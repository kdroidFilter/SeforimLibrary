package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.int

internal fun JsonElement?.stringOrNull(): String? = (this as? JsonPrimitive)?.contentOrNull

internal fun JsonPrimitive.intOrNullSafe(): Int? = runCatching { this.int }.getOrNull()

internal fun JsonElement?.isTriviallyEmpty(): Boolean {
    return when (this) {
        null, JsonNull -> true
        is JsonPrimitive -> this.contentOrNull.isNullOrBlank() && this.booleanOrNull == null
        is JsonArray -> this.isEmpty() || this.all { it.isTriviallyEmpty() }
        is JsonObject -> this.isEmpty()
    }
}

