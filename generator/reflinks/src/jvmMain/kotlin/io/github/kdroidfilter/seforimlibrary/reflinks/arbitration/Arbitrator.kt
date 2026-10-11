package io.github.kdroidfilter.seforimlibrary.reflinks.arbitration

import co.touchlab.kermit.Logger
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.random.Random

/**
 * Sends the cases missing from the [store] to the [model] and stores every paid answer as soon as it arrives.
 *
 * Token safety: a decided case is never sent again; 402/401 stop every worker; consecutive server failures open a
 * circuit breaker; the budget is checked before each request (spent + in flight); 429 pauses all workers.
 * Stopping loses nothing: what was paid is on disk, the rest is asked on the next run.
 */
class Arbitrator(
    private val model: ChatModel,
    private val modelId: String,
    private val store: DecisionStore,
    private val config: Config = Config(),
    private val now: () -> Long = System::currentTimeMillis,
) {
    data class Config(
        val workers: Int = 16,
        val maxSpendUsd: Double = 5.0,
        val maxCases: Int = Int.MAX_VALUE,
        val maxAttempts: Int = 5,
        /** Consecutive server failures that stop the run. */
        val breakerThreshold: Int = 12,
        /** Consecutive 429s that stop the run: throttling means "slow down", not "server down". */
        val rateLimitThreshold: Int = 300,
        val baseBackoffMs: Long = 2_000,
        val maxBackoffMs: Long = 60_000,
        /** Cost assumed for a request in flight until real answers give an average. */
        val initialCostEstimateUsd: Double = 0.001,
        val retryInvalid: Boolean = false,
    )

    data class Report(
        val requested: Int,
        val answered: Int,
        val invalid: Int,
        val rejected: Int,
        val gaveUp: Int,
        val notSent: Int,
        val spentUsd: Double,
        val stopReason: String?,
    )

    private val log = Logger.withTag("Arbitrator")

    suspend fun run(cases: List<ArbitrationCase>): Report {
        val todo = cases.asSequence()
            .map { it to it.cacheKey(modelId, Prompts.VERSION) }
            .distinctBy { it.second }
            .filterNot { store.isSettled(it.second, config.retryInvalid) }
            .take(config.maxCases)
            .toList()
        val state = RunState()
        val queue = Channel<Pair<ArbitrationCase, String>>(Channel.UNLIMITED)
        todo.forEach { queue.trySend(it) }
        queue.close()

        coroutineScope {
            repeat(config.workers.coerceAtLeast(1)) {
                launch {
                    for ((case, key) in queue) {
                        if (state.stopReason.get() != null) break
                        if (!state.withinBudget()) {
                            state.stop("budget of ${config.maxSpendUsd} USD reached")
                            break
                        }
                        state.started.incrementAndGet()
                        arbitrate(case, key, state)
                    }
                }
            }
        }
        return Report(
            requested = todo.size,
            answered = state.answered.get(),
            invalid = state.invalid.get(),
            rejected = state.rejected.get(),
            gaveUp = state.gaveUp.get(),
            notSent = todo.size - state.started.get(),
            spentUsd = state.spentMicros.get() / 1e6,
            stopReason = state.stopReason.get(),
        )
    }

    /** Sends one case until it is answered, rejected, given up, or the run is stopped. */
    private suspend fun arbitrate(case: ArbitrationCase, key: String, state: RunState) {
        var attempts = 0
        while (state.stopReason.get() == null) {
            val wait = state.pausedUntil.get() - now()
            if (wait > 0) delay(wait)
            state.inFlight.incrementAndGet()
            val outcome = try {
                model.complete(Prompts.system(case), Prompts.user(case))
            } finally {
                state.inFlight.decrementAndGet()
            }
            when (outcome) {
                is Outcome.Answer -> {
                    state.paid(outcome.costUsd)
                    state.answered.incrementAndGet()
                    // An out-of-range index is an unusable answer: kept as "no link"
                    val target = case.candidates.getOrNull(outcome.choice - 1)?.target
                    store.append(
                        decision(key, Decision.Status.ANSWERED, outcome.costUsd).copy(
                            choice = if (target != null) outcome.choice else 0,
                            target = target,
                            confidence = outcome.confidence,
                        ),
                    )
                    return
                }

                is Outcome.Invalid -> {
                    state.paid(outcome.costUsd)
                    state.invalid.incrementAndGet()
                    store.append(
                        decision(key, Decision.Status.INVALID, outcome.costUsd)
                            .copy(detail = "finish=${outcome.finishReason}: ${outcome.raw.take(200)}"),
                    )
                    return
                }

                is Outcome.Fatal -> return state.stop(outcome.reason)

                is Outcome.Rejected -> {
                    state.consecutiveFailures.set(0)
                    state.rejected.incrementAndGet()
                    log.w { "Request rejected, case skipped: ${outcome.reason}" }
                    return
                }

                is Outcome.Transient -> {
                    if (outcome.rateLimited) {
                        if (state.consecutiveRateLimits.incrementAndGet() >= config.rateLimitThreshold) {
                            return state.stop("rate limited ${config.rateLimitThreshold} times in a row")
                        }
                    } else {
                        attempts++
                        if (state.consecutiveFailures.incrementAndGet() >= config.breakerThreshold) {
                            return state.stop("circuit breaker: ${config.breakerThreshold} consecutive failures (last: ${outcome.reason})")
                        }
                        if (attempts >= config.maxAttempts) {
                            state.gaveUp.incrementAndGet()
                            return
                        }
                    }
                    val backoff = outcome.retryAfterMs
                        ?: (config.baseBackoffMs shl attempts.coerceAtLeast(1) - 1).coerceAtMost(config.maxBackoffMs)
                    val jittered = backoff + Random.nextLong(backoff / 4 + 1)
                    if (outcome.rateLimited) state.pausedUntil.accumulateAndGet(now() + jittered) { a, b -> maxOf(a, b) }
                    delay(jittered)
                }
            }
        }
    }

    private fun decision(key: String, status: Decision.Status, cost: Double) = Decision(
        key = key, model = modelId, prompt = Prompts.VERSION, status = status, costUsd = cost,
        at = Instant.ofEpochMilli(now()).toString(),
    )

    /** Counters shared by the workers of one run. */
    private inner class RunState {
        val stopReason = AtomicReference<String?>(null)
        val spentMicros = AtomicLong(0)
        val answered = AtomicInteger(0)
        val invalid = AtomicInteger(0)
        val rejected = AtomicInteger(0)
        val gaveUp = AtomicInteger(0)
        val started = AtomicInteger(0)
        val inFlight = AtomicInteger(0)
        val consecutiveFailures = AtomicInteger(0)
        val consecutiveRateLimits = AtomicInteger(0)
        val pausedUntil = AtomicLong(0)

        fun stop(reason: String) {
            if (stopReason.compareAndSet(null, reason)) log.w { "Stopping: $reason" }
        }

        fun paid(costUsd: Double) {
            consecutiveFailures.set(0)
            consecutiveRateLimits.set(0)
            spentMicros.addAndGet((costUsd * 1e6).toLong())
        }

        fun withinBudget(): Boolean {
            val paidCount = answered.get() + invalid.get()
            val average = if (paidCount == 0) config.initialCostEstimateUsd else spentMicros.get() / 1e6 / paidCount
            return spentMicros.get() / 1e6 + (inFlight.get() + 1) * average <= config.maxSpendUsd
        }
    }
}
