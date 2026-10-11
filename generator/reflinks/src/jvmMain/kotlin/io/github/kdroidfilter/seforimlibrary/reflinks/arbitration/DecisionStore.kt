package io.github.kdroidfilter.seforimlibrary.reflinks.arbitration

import co.touchlab.kermit.Logger
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.io.FileOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption

/**
 * One paid LLM answer. [choice] is the 1-based candidate index (0 = no link); [target] is the chosen candidate as
 * "title|heRef", never an internal id, so a decision survives database rebuilds.
 */
@Serializable
data class Decision(
    val key: String,
    val model: String,
    val prompt: String,
    val status: Status,
    val choice: Int = 0,
    val target: String? = null,
    val confidence: String? = null,
    val costUsd: Double = 0.0,
    val at: String,
    /** INVALID only: finish reason and the start of the reply, to diagnose why it could not be read. */
    val detail: String? = null,
) {
    @Serializable
    enum class Status {
        /** A well-formed answer. */
        ANSWERED,

        /** The model was paid but its reply could not be parsed: treated as "no link", retried only on request. */
        INVALID,

        /** Manual correction from ref-overrides.jsonl: always wins over the model. */
        OVERRIDE,
    }
}

/**
 * Append-only JSONL store of LLM decisions, keyed by a content hash of the case (see [ArbitrationCase.cacheKey]).
 *
 * Durability: every [append] writes one line and fsyncs before returning, so a crash loses at most the request in
 * flight, never an answer already paid for. [load] skips unreadable lines (a line cut by a crash), whose case is then
 * simply asked again. Errors (no answer received) are never stored.
 */
class DecisionStore private constructor(
    private val file: Path,
    private val decisions: MutableMap<String, Decision>,
    private val overrides: Map<String, Decision>,
) {
    private val log = Logger.withTag("DecisionStore")
    private val lock = Any()
    private val out: FileOutputStream by lazy {
        Files.createDirectories(file.parent)
        FileOutputStream(file.toFile(), true)
    }

    val size: Int get() = synchronized(lock) { decisions.size }

    /** The decision for [key]: a manual override first, then the model's answer. */
    operator fun get(key: String): Decision? = overrides[key] ?: synchronized(lock) { decisions[key] }

    /** True when no request is needed for [key]. INVALID answers are retried only when [retryInvalid] is set. */
    fun isSettled(key: String, retryInvalid: Boolean): Boolean {
        val d = get(key) ?: return false
        return !(retryInvalid && d.status == Decision.Status.INVALID)
    }

    fun append(decision: Decision) {
        val line = (json.encodeToString(Decision.serializer(), decision) + "\n").toByteArray(StandardCharsets.UTF_8)
        synchronized(lock) {
            out.write(line)
            out.flush()
            out.fd.sync()
            decisions[decision.key] = decision
        }
    }

    /**
     * Rewrites the file sorted by key with one line per key (last answer wins), for stable git diffs.
     * Atomic: written to a temp file, fsynced, then moved over the original.
     */
    fun compact() {
        synchronized(lock) {
            val tmp = file.resolveSibling(file.fileName.toString() + ".tmp")
            FileOutputStream(tmp.toFile()).use { fos ->
                decisions.values.sortedBy { it.key }.forEach {
                    fos.write((json.encodeToString(Decision.serializer(), it) + "\n").toByteArray(StandardCharsets.UTF_8))
                }
                fos.flush()
                fos.fd.sync()
            }
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
            log.i { "Compacted ${decisions.size} decisions into $file" }
        }
    }

    fun close() = synchronized(lock) { out.close() }

    companion object {
        private val json = Json { ignoreUnknownKeys = true; encodeDefaults = false }

        /** Opens (or creates) the store at [file]; [overridesFile] holds manual corrections, if present. */
        fun open(file: Path, overridesFile: Path? = null): DecisionStore {
            val log = Logger.withTag("DecisionStore")
            val decisions = readAll(file, log)
            val overrides = overridesFile?.let { readAll(it, log) }.orEmpty()
                .mapValues { it.value.copy(status = Decision.Status.OVERRIDE) }
            log.i { "Loaded ${decisions.size} decisions and ${overrides.size} overrides" }
            return DecisionStore(file, decisions, overrides)
        }

        private fun readAll(file: Path, log: Logger): MutableMap<String, Decision> {
            val map = LinkedHashMap<String, Decision>()
            if (!Files.exists(file)) return map
            var skipped = 0
            Files.newBufferedReader(file, StandardCharsets.UTF_8).useLines { lines ->
                lines.filter { it.isNotBlank() }.forEach { line ->
                    runCatching { json.decodeFromString(Decision.serializer(), line) }
                        .onSuccess { map[it.key] = it }
                        .onFailure { skipped++ }
                }
            }
            if (skipped > 0) log.w { "$file: skipped $skipped unreadable line(s), their cases will be asked again" }
            return map
        }
    }
}
