package io.github.kdroidfilter.seforimlibrary.reflinks.arbitration

import kotlinx.coroutines.test.runTest
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.appendText
import kotlin.io.path.readLines
import kotlin.io.path.writeText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ArbitrationTest {

    private val dir: Path = Files.createTempDirectory("arbitration")
    private val file: Path = dir.resolve("decisions.jsonl")

    private fun case(i: Int) = ArbitrationCase(
        kind = CaseKind.REFERENCE,
        sourceBook = "book",
        context = "passage $i ⟦(בראשית א, ד)⟧",
        candidates = listOf(Candidate("בראשית", "בראשית א, ד", "וירא אלהים את האור")),
    )

    private fun cases(n: Int) = (1..n).map(::case)

    /** A fake model: answers with [outcome] for the n-th call, counting calls. */
    private class FakeModel(val outcome: (Int) -> Outcome) : ChatModel {
        val calls = AtomicInteger(0)
        override suspend fun complete(system: String, user: String) = outcome(calls.incrementAndGet())
    }

    private fun answer(cost: Double = 0.001) = Outcome.Answer(1, "high", cost)

    private fun arbitrator(model: ChatModel, store: DecisionStore, config: Arbitrator.Config = Arbitrator.Config(workers = 1)) =
        Arbitrator(model, "test-model", store, config)

    @Test
    fun storesEveryAnswerAndNeverAsksTwice() = runTest {
        val model = FakeModel { answer() }
        val store = DecisionStore.open(file)
        val report = arbitrator(model, store).run(cases(5))
        assertEquals(5, report.answered)
        assertEquals("בראשית|בראשית א, ד", store[case(1).cacheKey("test-model", Prompts.VERSION)]?.target)
        store.close()

        // A new run on a reopened store sends nothing.
        val again = DecisionStore.open(file)
        val second = arbitrator(model, again).run(cases(5))
        assertEquals(0, second.requested)
        assertEquals(5, model.calls.get())
    }

    @Test
    fun noCreditStopsImmediatelyAndKeepsWhatWasPaid() = runTest {
        val model = FakeModel { n -> if (n <= 3) answer() else Outcome.Fatal("no credit left (402)") }
        val store = DecisionStore.open(file)
        val report = arbitrator(model, store).run(cases(20))
        assertEquals(3, report.answered)
        assertEquals("no credit left (402)", report.stopReason)
        assertEquals(4, model.calls.get(), "no request after the 402")
        assertEquals(16, report.notSent)
        assertEquals(3, file.readLines().size)
    }

    @Test
    fun downServerOpensTheCircuitBreaker() = runTest {
        val model = FakeModel { Outcome.Transient("HTTP 503") }
        val store = DecisionStore.open(file)
        val config = Arbitrator.Config(workers = 4, breakerThreshold = 6, maxAttempts = 10)
        val report = arbitrator(model, store, config).run(cases(100))
        assertNotNull(report.stopReason)
        assertTrue(report.stopReason!!.startsWith("circuit breaker"))
        assertTrue(model.calls.get() <= 6 + 4, "stops after the threshold (plus requests already in flight)")
        assertEquals(0, store.size, "failures are never stored")
    }

    @Test
    fun rateLimitsSlowDownWithoutTrippingTheBreaker() = runTest {
        val model = FakeModel { n -> if (n % 3 != 0) Outcome.Transient("rate limited", 1_000, rateLimited = true) else answer() }
        val store = DecisionStore.open(file)
        val config = Arbitrator.Config(workers = 4, breakerThreshold = 3)
        val report = arbitrator(model, store, config).run(cases(10))
        assertNull(report.stopReason)
        assertEquals(10, report.answered)
    }

    @Test
    fun endlessRateLimitingStops() = runTest {
        val model = FakeModel { Outcome.Transient("rate limited", 1_000, rateLimited = true) }
        val store = DecisionStore.open(file)
        val report = arbitrator(model, store, Arbitrator.Config(workers = 2, rateLimitThreshold = 20)).run(cases(5))
        assertTrue(report.stopReason!!.startsWith("rate limited"))
        assertEquals(0, store.size)
    }

    @Test
    fun budgetIsNeverExceeded() = runTest {
        val model = FakeModel { answer(cost = 0.01) }
        val store = DecisionStore.open(file)
        val config = Arbitrator.Config(workers = 1, maxSpendUsd = 0.05, initialCostEstimateUsd = 0.01)
        val report = arbitrator(model, store, config).run(cases(50))
        assertTrue(report.spentUsd <= 0.05 + 1e-9, "spent ${report.spentUsd}")
        assertTrue(report.stopReason!!.startsWith("budget"))
    }

    @Test
    fun transientErrorsAreRetriedThenStoredOnce() = runTest {
        val model = FakeModel { n -> if (n <= 2) Outcome.Transient("HTTP 307") else answer() }
        val store = DecisionStore.open(file)
        val report = arbitrator(model, store).run(cases(1))
        assertEquals(1, report.answered)
        assertEquals(3, model.calls.get())
        assertEquals(1, file.readLines().size)
    }

    @Test
    fun rejectedRequestsAreSkippedNotStored() = runTest {
        val model = FakeModel { Outcome.Rejected("HTTP 400") }
        val store = DecisionStore.open(file)
        val report = arbitrator(model, store).run(cases(3))
        assertEquals(3, report.rejected)
        assertNull(report.stopReason)
        assertEquals(0, store.size)
    }

    @Test
    fun invalidAnswersArePaidOnceUnlessRetryIsAsked() = runTest {
        val model = FakeModel { Outcome.Invalid("garbage", 0.001) }
        val store = DecisionStore.open(file)
        arbitrator(model, store).run(cases(2))
        assertEquals(2, model.calls.get())
        arbitrator(model, store).run(cases(2))
        assertEquals(2, model.calls.get(), "INVALID is settled by default")
        arbitrator(model, store, Arbitrator.Config(workers = 1, retryInvalid = true)).run(cases(2))
        assertEquals(4, model.calls.get())
    }

    @Test
    fun aLineCutByACrashIsIgnored() {
        val store = DecisionStore.open(file)
        store.append(Decision("k1", "m", "v4", Decision.Status.ANSWERED, 1, "t|h", "high", 0.001, "2026-01-01T00:00:00Z"))
        store.close()
        file.appendText("{\"key\":\"k2\",\"model\":\"m\",\"pro")
        val reopened = DecisionStore.open(file)
        assertEquals(1, reopened.size)
        assertNotNull(reopened["k1"])
    }

    @Test
    fun compactSortsAndKeepsTheLastAnswer() {
        val store = DecisionStore.open(file)
        listOf("b", "a", "b").forEachIndexed { i, k ->
            store.append(Decision(k, "m", "v4", Decision.Status.ANSWERED, i, null, null, 0.0, "t$i"))
        }
        store.compact()
        val lines = file.readLines()
        assertEquals(2, lines.size)
        assertTrue(lines[0].contains("\"key\":\"a\""))
        assertTrue(lines[1].contains("\"t2\""))
    }

    @Test
    fun overridesWin() {
        val overrides = dir.resolve("overrides.jsonl")
        overrides.writeText("{\"key\":\"k\",\"model\":\"human\",\"prompt\":\"-\",\"status\":\"ANSWERED\",\"choice\":0,\"at\":\"x\"}\n")
        val store = DecisionStore.open(file, overrides)
        store.append(Decision("k", "m", "v4", Decision.Status.ANSWERED, 1, "t|h", null, 0.0, "y"))
        assertEquals(Decision.Status.OVERRIDE, store["k"]?.status)
        assertEquals(0, store["k"]?.choice)
    }

    @Test
    fun httpClassification() {
        assertTrue(OpenRouterModel.classify(402, "", null) is Outcome.Fatal)
        assertTrue(OpenRouterModel.classify(401, "", null) is Outcome.Fatal)
        val limited = OpenRouterModel.classify(429, "", "7") as Outcome.Transient
        assertEquals(7000, limited.retryAfterMs)
        assertTrue(OpenRouterModel.classify(503, "", null) is Outcome.Transient)
        assertTrue(OpenRouterModel.classify(400, "bad", null) is Outcome.Rejected)
        val ok = OpenRouterModel.classify(
            200,
            """{"choices":[{"message":{"content":"```json\n{\"choice\": 2, \"confidence\": \"high\"}\n```"}}],"usage":{"cost":0.0004}}""",
            null,
        ) as Outcome.Answer
        assertEquals(2, ok.choice)
        assertEquals(0.0004, ok.costUsd)
        val empty = OpenRouterModel.classify(200, """{"choices":[{"message":{"content":""}}],"usage":{"cost":0.0003}}""", null)
        assertTrue(empty is Outcome.Invalid && empty.costUsd == 0.0003, "a paid empty reply is kept as INVALID")
    }
}
