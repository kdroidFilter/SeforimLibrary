package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.common.licenses.Licenses
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.io.path.readText

/** One Hebrew version record of `versions.json`, as dumped raw by SefariaExport. */
@Serializable
internal data class SefariaVersionRecord(
    val title: String,
    val versionTitle: String,
    val versionTitleInHebrew: String? = null,
    val versionSource: String? = null,
    val license: String? = null,
)

/** The license of one book's version, after canonicalization and edition inheritance. */
internal data class ResolvedVersionLicense(
    val versionTitle: String,
    val heVersionTitle: String?,
    val versionSource: String?,
    val licenseCode: String,
    val inferred: Boolean,
)

/**
 * Resolves the license of each Sefaria version from `versions.json`.
 *
 * Sefaria declares licenses per (book, version). When a version is `unknown` but every
 * known version of the same edition (versionTitle + versionSource) shares a single license,
 * that license is inherited and flagged as inferred. Anything else stays unknown.
 */
internal class SefariaVersionLicenses(records: List<SefariaVersionRecord>) {
    private val byVersion = records.associateBy { it.title to it.versionTitle }

    private val editionLicense: Map<Pair<String, String?>, String> = records
        .map { (it.versionTitle to it.versionSource) to Licenses.canonicalCode(it.license) }
        .filter { (_, code) -> code != Licenses.UNKNOWN }
        .groupBy({ it.first }, { it.second })
        .mapNotNull { (edition, codes) -> codes.toSet().singleOrNull()?.let { edition to it } }
        .toMap()

    /** Returns null when the version is absent from `versions.json`. */
    fun resolve(title: String, versionTitle: String): ResolvedVersionLicense? {
        val record = byVersion[title to versionTitle] ?: return null
        val declared = Licenses.canonicalCode(record.license)
        val inherited = editionLicense[record.versionTitle to record.versionSource]
            .takeIf { declared == Licenses.UNKNOWN }
        return ResolvedVersionLicense(
            versionTitle = record.versionTitle,
            heVersionTitle = record.versionTitleInHebrew?.takeIf { it.isNotBlank() },
            versionSource = record.versionSource?.takeIf { it.isNotBlank() },
            licenseCode = inherited ?: declared,
            inferred = inherited != null,
        )
    }

    companion object {
        const val FILE_NAME = "versions.json"

        fun load(dbRoot: Path, json: Json): SefariaVersionLicenses {
            val file = dbRoot.resolve(FILE_NAME)
            require(file.exists()) {
                "$FILE_NAME missing from the Sefaria export at $dbRoot " +
                    "(produced by SefariaExport since the 2026-10-08 release)"
            }
            return SefariaVersionLicenses(json.decodeFromString<List<SefariaVersionRecord>>(file.readText()))
        }
    }
}
