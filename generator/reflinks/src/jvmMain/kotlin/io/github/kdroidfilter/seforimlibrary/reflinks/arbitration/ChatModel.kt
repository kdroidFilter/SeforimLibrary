package io.github.kdroidfilter.seforimlibrary.reflinks.arbitration

import kotlinx.coroutines.future.await
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration

/** What a single request produced; the arbitrator decides what to do with each outcome. */
sealed interface Outcome {
    /** Paid and well-formed. */
    data class Answer(val choice: Int, val confidence: String?, val costUsd: Double) : Outcome

    /** Paid but unparseable: stored as INVALID so it is not paid again by accident. */
    data class Invalid(val raw: String, val costUsd: Double, val finishReason: String? = null) : Outcome

    /** Worth retrying later (server error, timeout, redirect, rate limit). [retryAfterMs] comes from the server. */
    data class Transient(val reason: String, val retryAfterMs: Long? = null, val rateLimited: Boolean = false) : Outcome

    /** Stop everything: no credit (402), bad or revoked key (401/403 from the auth layer). */
    data class Fatal(val reason: String) : Outcome

    /** This request will never succeed (400): skip the case, do not retry. */
    data class Rejected(val reason: String) : Outcome
}

fun interface ChatModel {
    suspend fun complete(system: String, user: String): Outcome
}

/**
 * OpenAI-compatible chat completions on OpenRouter, reasoning enabled. The key is held in memory only and never logged.
 * Costs come from OpenRouter's `usage.cost` (exact billed amount).
 */
class OpenRouterModel(
    private val apiKey: String,
    val model: String,
    private val baseUrl: String = "https://openrouter.ai/api/v1",
    /** Room for the reasoning plus the JSON answer: at 4000 about 10 % of the replies were cut before the answer. */
    private val maxTokens: Int = 8000,
    /** Generous on purpose: a request cut client-side may still be billed server-side. */
    private val requestTimeout: Duration = Duration.ofMinutes(5),
) : ChatModel {

    private val http: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .followRedirects(HttpClient.Redirect.NEVER)
        .build()

    override suspend fun complete(system: String, user: String): Outcome {
        val body = buildJsonObject {
            put("model", model)
            put("temperature", 0)
            put("max_tokens", maxTokens)
            put("response_format", buildJsonObject { put("type", "json_object") })
            put("reasoning", buildJsonObject { put("enabled", true) })
            put("usage", buildJsonObject { put("include", true) })
            put(
                "messages",
                buildJsonArray {
                    add(buildJsonObject { put("role", "system"); put("content", system) })
                    add(buildJsonObject { put("role", "user"); put("content", user) })
                },
            )
        }
        val request = HttpRequest.newBuilder(URI.create("$baseUrl/chat/completions"))
            .timeout(requestTimeout)
            .header("Authorization", "Bearer $apiKey")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
            .build()
        val response = try {
            http.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
        } catch (e: HttpTimeoutException) {
            return Outcome.Transient("timeout")
        } catch (e: java.io.IOException) {
            return Outcome.Transient("network: ${e.javaClass.simpleName}")
        }
        return classify(response.statusCode(), response.body(), response.headers().firstValue("retry-after").orElse(null))
    }

    /** Remaining credit in USD, or null when the account has no limit or the call fails. */
    suspend fun remainingCredit(): Double? {
        val request = HttpRequest.newBuilder(URI.create("$baseUrl/key"))
            .timeout(Duration.ofSeconds(30))
            .header("Authorization", "Bearer $apiKey")
            .GET().build()
        return runCatching {
            val r = http.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
            if (r.statusCode() != 200) return null
            json.parseToJsonElement(r.body()).jsonObject["data"]?.jsonObject?.get("limit_remaining")?.jsonPrimitive?.doubleOrNull
        }.getOrNull()
    }

    companion object {
        private val json = Json { ignoreUnknownKeys = true }
        private val choiceObject = Regex("\\{[^{}]*\"choice\"[^{}]*\\}")

        internal fun classify(status: Int, body: String, retryAfter: String?): Outcome = when {
            status == 200 -> parseAnswer(body)
            status == 402 -> Outcome.Fatal("no credit left (402)")
            status == 401 -> Outcome.Fatal("invalid API key (401)")
            status == 429 -> Outcome.Transient("rate limited", retryAfter?.toDoubleOrNull()?.let { (it * 1000).toLong() }, rateLimited = true)
            // OpenRouter answers 403 both for a revoked key and for transient provider moderation: the arbitrator
            // escalates it to fatal when it repeats (circuit breaker).
            status == 403 -> Outcome.Transient("forbidden (403)")
            status == 400 || status == 404 || status == 413 -> Outcome.Rejected("HTTP $status: ${body.take(200)}")
            else -> Outcome.Transient("HTTP $status")
        }

        private fun parseAnswer(body: String): Outcome {
            val root = runCatching { json.parseToJsonElement(body).jsonObject }.getOrElse { return Outcome.Transient("malformed response") }
            // Some providers report errors inside a 200
            root["error"]?.let { return Outcome.Transient("provider error: ${it.toString().take(200)}") }
            val cost = (root["usage"] as? JsonObject)?.get("cost")?.jsonPrimitive?.doubleOrNull ?: 0.0
            val choice = root["choices"]?.jsonArray?.firstOrNull()?.jsonObject
            val finishReason = choice?.get("finish_reason")?.jsonPrimitive?.content
            val content = choice?.get("message")?.jsonObject?.get("content")?.jsonPrimitive?.content.orEmpty()
            val obj = choiceObject.find(content)?.value ?: return Outcome.Invalid(content.take(300), cost, finishReason)
            return runCatching {
                val d = json.parseToJsonElement(obj).jsonObject
                Outcome.Answer(d.getValue("choice").jsonPrimitive.int, d["confidence"]?.jsonPrimitive?.content, cost)
            }.getOrElse { Outcome.Invalid(content.take(300), cost, finishReason) }
        }
    }
}
