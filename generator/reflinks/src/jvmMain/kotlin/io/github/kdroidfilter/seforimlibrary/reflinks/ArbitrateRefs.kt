package io.github.kdroidfilter.seforimlibrary.reflinks

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.reflinks.arbitration.ArbitrationCase
import io.github.kdroidfilter.seforimlibrary.reflinks.arbitration.Arbitrator
import io.github.kdroidfilter.seforimlibrary.reflinks.arbitration.DecisionStore
import io.github.kdroidfilter.seforimlibrary.reflinks.arbitration.OpenRouterModel
import io.github.kdroidfilter.seforimlibrary.reflinks.arbitration.Prompts
import io.github.kdroidfilter.seforimlibrary.reflinks.corpus.CaseClass
import io.github.kdroidfilter.seforimlibrary.reflinks.corpus.CitationScanner
import io.github.kdroidfilter.seforimlibrary.reflinks.corpus.HeRefIndex
import io.github.kdroidfilter.seforimlibrary.reflinks.corpus.Method
import io.github.kdroidfilter.seforimlibrary.reflinks.corpus.Occurrence
import io.github.kdroidfilter.seforimlibrary.reflinks.corpus.SefariaSchemas
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.system.exitProcess

/** Arbitration order (holdout accuracy and value of the links): bare ibid last. */
private val CLASS_PRIORITY = listOf(
    CaseClass.KINGS_OR_COMMENTARY,
    CaseClass.IBID_WITH_LOCATION,
    CaseClass.TUR_PART_ONLY,
    CaseClass.TANAKH_NAME_BEFORE,
    CaseClass.PEREK_OR_DAF,
    CaseClass.IBID_BARE,
)

private const val DEFAULT_SEFARIA_SCHEMAS = "generator/sefariasqlite/build/sefaria/export/schemas"

private const val DEFAULT_MODEL = "meta/muse-spark-1.3-contributor"

/** Average billed cost per case measured on the corpus run (Muse 1.3 contributor, prompt v4, reasoning on). */
private const val COST_PER_CASE_USD = 0.00047

/** Credit kept untouched on the account, so the run stops before OpenRouter starts refusing mid-request. */
private const val CREDIT_SAFETY_MARGIN_USD = 0.25

/**
 * Fills the LLM decision cache for the ambiguous citations of the corpus.
 *
 * Without --confirm it only prints the estimate and exits, spending nothing. With --confirm it calls the model for the
 * cases missing from the cache, within min(--max-spend, remaining credit − margin), and stops cleanly on no credit,
 * a down server (circuit breaker) or the budget. Every paid answer is fsynced as it arrives; a later run resumes.
 */
fun main(args: Array<String>): Unit = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val log = Logger.withTag("ArbitrateRefs")
    val opts = args.filter { it.startsWith("--") }
        .associate { it.removePrefix("--").substringBefore('=') to it.substringAfter('=', "") }
    val dbPath = opts["db"] ?: Paths.get("build", "seforim.db").toString()
    val decisionsPath = Path.of(opts["decisions"] ?: "generator/data/ref-arbitration.jsonl")
    val model = opts["model"]?.ifEmpty { null } ?: DEFAULT_MODEL
    val maxSpend = opts["max-spend"]?.toDoubleOrNull() ?: 5.0
    val maxCases = opts["max-cases"]?.toIntOrNull() ?: Int.MAX_VALUE
    val workers = opts["workers"]?.toIntOrNull() ?: 16
    val confirm = "confirm" in opts
    val retryInvalid = "retry-invalid" in opts
    // Optional restriction: --classes=IBID_WITH_LOCATION,PEREK_OR_DAF (default: every class)
    val classes = opts["classes"]?.ifEmpty { null }?.split(',')?.map { CaseClass.valueOf(it.trim()) }?.toSet()

    require(Files.isRegularFile(Path.of(dbPath))) { "Database not found: $dbPath" }

    // 1. Collect the ambiguous cases (deduplicated by cache key)
    val cases = LinkedHashMap<String, ArbitrationCase>()
    val classOf = HashMap<String, CaseClass>()
    val byClass = mutableMapOf<CaseClass, Int>()
    val linkedBy = mutableMapOf<Method, Int>()
    val sefariaRefs = SefariaRefParser(SefariaSchemas.load(Path.of(opts["sefaria-schemas"] ?: DEFAULT_SEFARIA_SCHEMAS)))
    HeRefIndex(dbPath).use { index ->
        CitationScanner(index, sefariaRefs).scan { occ ->
            if (occ is Occurrence.Linked) linkedBy.merge(occ.method, 1, Int::plus)
            if (occ is Occurrence.Ambiguous && (classes == null || occ.caseClass in classes)) {
                val key = occ.case.cacheKey(model, Prompts.VERSION)
                cases.putIfAbsent(key, occ.case)
                classOf.putIfAbsent(key, occ.caseClass)
                byClass.merge(occ.caseClass, 1, Int::plus)
            }
        }
    }

    // 2. What is missing from the cache
    val store = DecisionStore.open(decisionsPath, decisionsPath.resolveSibling("ref-overrides.jsonl"))
    // When the budget cannot cover everything, the most useful and most reliable classes go first.
    val missing = cases.filterKeys { !store.isSettled(it, retryInvalid) }
        .entries.sortedBy { CLASS_PRIORITY.indexOf(classOf.getValue(it.key)) }
        .associate { it.key to it.value }
    val toSend = minOf(missing.size, maxCases)
    val estimate = toSend * COST_PER_CASE_USD
    log.i { "Deterministic links by method: $linkedBy (total ${linkedBy.values.sum()})" }
    log.i { "Ambiguous occurrences by class: $byClass" }
    log.i { "Distinct cases: ${cases.size}, already decided: ${cases.size - missing.size}, missing: ${missing.size}" }
    log.i { "This run: $toSend case(s) with $model, prompt ${Prompts.VERSION}, estimated ≈ ${"%.2f".format(estimate)} USD" }

    if (toSend == 0) {
        log.i { "Nothing to do." }
        store.close()
        return@runBlocking
    }

    val keyFile = opts["key-file"]?.ifEmpty { null }
    if (keyFile == null) {
        log.w { "No --key-file: estimate only." }
        store.close()
        return@runBlocking
    }
    val llm = OpenRouterModel(Files.readString(Path.of(keyFile.replaceFirst(Regex("^~"), System.getProperty("user.home")))).trim(), model)

    // 3. Pre-flight: never start a run the account cannot pay for
    val credit = llm.remainingCredit()
    val budget = if (credit != null) minOf(maxSpend, credit - CREDIT_SAFETY_MARGIN_USD) else maxSpend
    log.i { "Remaining credit: ${credit?.let { "%.2f USD".format(it) } ?: "unknown"}; budget for this run: ${"%.2f".format(budget)} USD" }
    if (budget <= 0) {
        log.e { "Not enough credit to start. Nothing was sent." }
        store.close()
        exitProcess(2)
    }
    if (!confirm) {
        log.w { "Dry run: add -Pconfirm to send the requests." }
        store.close()
        return@runBlocking
    }

    // 4. Run; whatever happens, what was paid is already on disk
    val report = try {
        Arbitrator(
            llm, model, store,
            Arbitrator.Config(workers = workers, maxSpendUsd = budget, maxCases = maxCases, retryInvalid = retryInvalid),
        ).run(missing.values.toList())
    } finally {
        store.compact()
        store.close()
    }
    log.i {
        "Done: answered ${report.answered}, invalid ${report.invalid}, rejected ${report.rejected}, " +
            "gave up ${report.gaveUp}, not sent ${report.notSent}, spent ${"%.3f".format(report.spentUsd)} USD"
    }
    report.stopReason?.let {
        log.w { "Stopped early: $it. Run the task again later to resume where it stopped." }
        exitProcess(3)
    }
}
