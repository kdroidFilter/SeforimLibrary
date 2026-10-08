package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.jsonObject

/**
 * Hebrew names for Sefaria authors whose topic has an empty `he` title upstream,
 * keyed by Sefaria topic slug. Without them the book gets a blank author.
 */
private val missingHebrewAuthorNames = mapOf(
    "abraham-cohen" to "אברהם כהן",
    "joseph-albo" to "יוסף אלבו",
    "meir-posner" to "מאיר פוזנר",
    "meshullam-feivush-heller" to "משולם פייבוש הלר",
    "nissim-chaim-moshe-mizrachi" to "ניסים חיים משה מזרחי",
    "nissim-gaon" to "ניסים גאון",
    "sherira-gaon" to "שרירא גאון",
    "shmuel-bornsztain-of-sochatchov" to "שמואל בורנשטיין מסוכטשוב",
    "yissachar-eilenburg" to "יששכר בער איילנבורג",
    "yisrael-friedman1" to "ישראל פרידמן מרוז'ין",
    "yitzchak-adarbi" to "יצחק אדרבי",
)

/**
 * Resolves the Hebrew name of a Sefaria `authors` entry: its `he` title, else a known
 * override for its slug, else its English name. Returns null when none is usable.
 */
internal fun resolveSefariaAuthorName(author: JsonElement): String? {
    val obj = author.jsonObject
    return obj["he"]?.stringOrNull()?.trim()?.takeIf { it.isNotEmpty() }
        ?: obj["slug"]?.stringOrNull()?.let { missingHebrewAuthorNames[it] }
        ?: obj["en"]?.stringOrNull()?.trim()?.takeIf { it.isNotEmpty() }
}
