package io.github.kdroidfilter.seforimlibrary.common.licenses

/** What a license allows. Only known for the licenses listed in [Licenses]. */
data class LicensePermissions(
    val attribution: Boolean,
    val shareAlike: Boolean,
    val commercial: Boolean,
    val derivatives: Boolean,
)

/**
 * Single source of truth for license codes, shared by every importer.
 *
 * Licenses are get-or-create: any code met upstream is stored as-is (after [canonicalCode]
 * folds spelling variants of the same license), and only the codes below carry permission
 * flags. A new upstream code is stored with unknown permissions until it is added here.
 */
object Licenses {
    const val UNKNOWN = "unknown"
    const val PUBLIC_DOMAIN = "Public Domain"
    const val CC_BY_SA = "CC-BY-SA"

    private val ALIASES = mapOf(
        "pd" to PUBLIC_DOMAIN,
        "public domain" to PUBLIC_DOMAIN,
        "unknown" to UNKNOWN,
    )

    private val PERMISSIONS = mapOf(
        PUBLIC_DOMAIN to LicensePermissions(attribution = false, shareAlike = false, commercial = true, derivatives = true),
        "CC0" to LicensePermissions(attribution = false, shareAlike = false, commercial = true, derivatives = true),
        "CC-BY" to LicensePermissions(attribution = true, shareAlike = false, commercial = true, derivatives = true),
        CC_BY_SA to LicensePermissions(attribution = true, shareAlike = true, commercial = true, derivatives = true),
        "CC-BY-NC" to LicensePermissions(attribution = true, shareAlike = false, commercial = false, derivatives = true),
        "CC-BY-NC-SA" to LicensePermissions(attribution = true, shareAlike = true, commercial = false, derivatives = true),
        "CC-BY-ND" to LicensePermissions(attribution = true, shareAlike = false, commercial = true, derivatives = false),
        "CC-BY-NC-ND" to LicensePermissions(attribution = true, shareAlike = false, commercial = false, derivatives = false),
    )

    /** Folds upstream spellings of the same license into one code; null/blank → [UNKNOWN]. */
    fun canonicalCode(raw: String?): String {
        val trimmed = raw?.trim().orEmpty()
        if (trimmed.isEmpty()) return UNKNOWN
        return ALIASES[trimmed.lowercase()] ?: trimmed
    }

    fun permissionsOf(code: String): LicensePermissions? = PERMISSIONS[code]
}
