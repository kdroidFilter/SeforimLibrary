package io.github.kdroidfilter.seforimlibrary.reflinks.arbitration

import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.json.Json
import java.security.MessageDigest

/** A target the model may choose: identified by title + heRef, with the text of its first segment. */
@Serializable
data class Candidate(
    val title: String,
    val heRef: String,
    val snippet: String,
    val note: String? = null,
    /** Target line in the database being scanned; not part of the cache key. */
    @Transient val lineId: Long = 0,
) {
    val target: String get() = "$title|$heRef"
}

@Serializable
data class Antecedent(val citation: String, val excerpt: String)

/** Which prompt handles the case: explicit references and ibid (שם) are separate problems. */
@Serializable
enum class CaseKind { REFERENCE, IBID }

/**
 * One ambiguous citation to arbitrate. [context] is the passage with the citation wrapped in ⟦ ⟧.
 * The cache key covers everything the answer depends on, and nothing else, so it only changes when the decision
 * could change: new text, new candidates, another prompt version or another model.
 */
@Serializable
data class ArbitrationCase(
    val kind: CaseKind,
    val sourceBook: String,
    val context: String,
    val candidates: List<Candidate>,
    val antecedent: Antecedent? = null,
) {
    fun cacheKey(model: String, promptVersion: String): String {
        val payload = KeyPayload(
            v = promptVersion,
            model = model,
            kind = kind,
            context = context,
            antecedent = antecedent,
            candidates = candidates.map { listOf(it.title, it.heRef, sha1(it.snippet), it.note.orEmpty()) },
        )
        return sha256(json.encodeToString(KeyPayload.serializer(), payload))
    }

    @Serializable
    private data class KeyPayload(
        val v: String,
        val model: String,
        val kind: CaseKind,
        val context: String,
        val antecedent: Antecedent?,
        val candidates: List<List<String>>,
    )

    private companion object {
        val json = Json { encodeDefaults = true }

        fun sha1(s: String) = hex(MessageDigest.getInstance("SHA-1").digest(s.toByteArray()))
        fun sha256(s: String) = hex(MessageDigest.getInstance("SHA-256").digest(s.toByteArray()))
        fun hex(bytes: ByteArray) = bytes.joinToString("") { "%02x".format(it) }
    }
}
