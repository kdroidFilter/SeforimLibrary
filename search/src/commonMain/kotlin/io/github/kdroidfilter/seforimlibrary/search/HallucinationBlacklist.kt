package io.github.kdroidfilter.seforimlibrary.search

/**
 * Blacklist of hallucinated dictionary mappings: LLM-generated mappings that are incorrect and should be ignored.
 * Key: normalized token, Value: set of incorrect base forms to reject.
 *
 * TODO: This is a temporary workaround. The proper fix is to correct the
 *       hallucinated mappings directly in the lexical dictionary (lexical.db)
 *       so this blacklist becomes unnecessary.
 */
internal object HallucinationBlacklist {
    // token to base
    private val ENTRIES =
        listOf(
            "לחתוכ" to "כנ",
            "לחתוך" to "כנ",
            "לבקש" to "רצה",
            "מתפלל" to "בעה",
            "לשבת" to "נתנ",
            "לעבוד" to "עשה",
            "להעמיד" to "קומ",
            "כנ" to "כנ",
            "בנ" to "בנ",
            "לשימ" to "שמ",
            "לשים" to "שמ",
            "לדונ" to "יד",
            "לדון" to "יד",
            "אמ" to "אמ",
            "להרגיע" to "יושב",
            "לחייכ" to "חוה",
            "לחייך" to "חוה",
            "שנ" to "שנ",
            "עצ" to "עצ",
            "דנ" to "דנ",
            "להיכנס" to "עלה",
            "לשקול" to "לקח",
            "לבנות" to "בנ",
            "לשחרר" to "שרה",
            "להתיר" to "שרה",
        )

    private val bases: Map<String, Set<String>> by lazy {
        ENTRIES
            .groupBy({ HebrewTextUtils.normalizeHebrew(it.first) }, { HebrewTextUtils.normalizeHebrew(it.second) })
            .mapValues { it.value.toSet() }
    }

    /** Whether [expansion] reaches [token] only through a blacklisted base. */
    fun rejects(
        token: String,
        expansion: MagicDictionaryIndex.Expansion,
    ): Boolean {
        val blacklisted = bases[HebrewTextUtils.normalizeHebrew(token)] ?: return false
        return expansion.base.any { HebrewTextUtils.normalizeHebrew(it) in blacklisted }
    }
}
