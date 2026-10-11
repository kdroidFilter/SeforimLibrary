package io.github.kdroidfilter.seforimlibrary.reflinks

/**
 * Surface forms of the books the parser recognizes, mapped to their `book.title`.
 * Explicit spelling variants only: normalizing yod/vav globally caused false hits ("הובא למעלה" → "מעילה").
 */
internal object BookAliases {

    val tanakh: Map<String, List<String>> = mapOf(
        "בראשית" to listOf("בראשית", "בר'", "ברא'"),
        "שמות" to listOf("שמות"),
        "ויקרא" to listOf("ויקרא", "ויק'"),
        "במדבר" to listOf("במדבר", "במד'"),
        "דברים" to listOf("דברים", "דב'"),
        "יהושע" to listOf("יהושע", "יהו'"),
        "שופטים" to listOf("שופטים", "שופ'"),
        "שמואל א" to listOf("שמואל א", "ש\"א", "שמ\"א", "שמואל א'", "שמואל ראשון"),
        "שמואל ב" to listOf("שמואל ב", "ש\"ב", "שמ\"ב", "שמואל ב'", "שמואל שני"),
        "מלכים א" to listOf("מלכים א", "מ\"א", "מל\"א", "מלכים א'"),
        "מלכים ב" to listOf("מלכים ב", "מ\"ב", "מל\"ב", "מלכים ב'"),
        "ישעיהו" to listOf("ישעיהו", "ישעיה", "ישעי'"),
        "ירמיהו" to listOf("ירמיהו", "ירמיה", "ירמי'"),
        "יחזקאל" to listOf("יחזקאל", "יחזק'", "יחז'"),
        "הושע" to listOf("הושע"), "יואל" to listOf("יואל"), "עמוס" to listOf("עמוס"),
        "עובדיה" to listOf("עובדיה", "עבדיה"), "יונה" to listOf("יונה"), "מיכה" to listOf("מיכה"),
        "נחום" to listOf("נחום"), "חבקוק" to listOf("חבקוק"), "צפניה" to listOf("צפניה"), "חגי" to listOf("חגי"),
        "זכריה" to listOf("זכריה", "זכרי'"), "מלאכי" to listOf("מלאכי"),
        "תהילים" to listOf("תהלים", "תהילים", "תהל'", "תהי'"),
        "משלי" to listOf("משלי"), "איוב" to listOf("איוב"),
        "שיר השירים" to listOf("שיר השירים", "שה\"ש", "שיר השיר'"),
        "רות" to listOf("רות"), "איכה" to listOf("איכה"), "קהלת" to listOf("קהלת", "קוהלת"),
        "אסתר" to listOf("אסתר"), "דניאל" to listOf("דניאל"), "עזרא" to listOf("עזרא"), "נחמיה" to listOf("נחמיה"),
        "דברי הימים א" to listOf("דברי הימים א", "דה\"א", "דהי\"א", "דברי הימים א'"),
        "דברי הימים ב" to listOf("דברי הימים ב", "דה\"ב", "דהי\"ב", "דברי הימים ב'"),
    )

    /** Bavli tractates come first: [bavli] is the subset that has a Babylonian Talmud. */
    val tractates: Map<String, List<String>> = linkedMapOf(
        "ברכות" to listOf("ברכות"), "שבת" to listOf("שבת"), "עירובין" to listOf("עירובין", "ערובין"),
        "פסחים" to listOf("פסחים"), "יומא" to listOf("יומא"), "סוכה" to listOf("סוכה"), "ביצה" to listOf("ביצה"),
        "ראש השנה" to listOf("ראש השנה", "ר\"ה"), "תענית" to listOf("תענית"), "מגילה" to listOf("מגילה"),
        "מועד קטן" to listOf("מועד קטן", "מו\"ק"), "חגיגה" to listOf("חגיגה"), "יבמות" to listOf("יבמות"),
        "כתובות" to listOf("כתובות"), "נדרים" to listOf("נדרים"), "נזיר" to listOf("נזיר"), "סוטה" to listOf("סוטה"),
        "גיטין" to listOf("גיטין"), "קידושין" to listOf("קידושין", "קדושין"),
        "בבא קמא" to listOf("בבא קמא", "ב\"ק"), "בבא מציעא" to listOf("בבא מציעא", "ב\"מ"),
        "בבא בתרא" to listOf("בבא בתרא", "ב\"ב"), "סנהדרין" to listOf("סנהדרין"), "מכות" to listOf("מכות"),
        "שבועות" to listOf("שבועות"), "עבודה זרה" to listOf("עבודה זרה", "ע\"ז"), "הוריות" to listOf("הוריות"),
        "זבחים" to listOf("זבחים"), "מנחות" to listOf("מנחות"), "חולין" to listOf("חולין"), "בכורות" to listOf("בכורות"),
        "ערכין" to listOf("ערכין"), "תמורה" to listOf("תמורה"), "כריתות" to listOf("כריתות"), "מעילה" to listOf("מעילה"),
        "תמיד" to listOf("תמיד"), "נדה" to listOf("נדה", "נידה"),
        "פאה" to listOf("פאה"), "דמאי" to listOf("דמאי"), "כלאים" to listOf("כלאים"), "שביעית" to listOf("שביעית"),
        "תרומות" to listOf("תרומות"), "מעשרות" to listOf("מעשרות"), "מעשר שני" to listOf("מעשר שני"),
        "חלה" to listOf("חלה"), "ערלה" to listOf("ערלה"), "ביכורים" to listOf("ביכורים", "בכורים"),
        "שקלים" to listOf("שקלים"), "עדויות" to listOf("עדויות", "עדיות"), "אבות" to listOf("אבות"),
        "מדות" to listOf("מדות", "מידות"), "קינים" to listOf("קינים", "קנים"), "כלים" to listOf("כלים"),
        "אהלות" to listOf("אהלות"), "נגעים" to listOf("נגעים"), "פרה" to listOf("פרה"), "טהרות" to listOf("טהרות"),
        "מקואות" to listOf("מקואות", "מקוואות"), "מכשירין" to listOf("מכשירין"), "זבים" to listOf("זבים"),
        "טבול יום" to listOf("טבול יום"), "ידים" to listOf("ידים", "ידיים"), "עוקצים" to listOf("עוקצים", "עוקצין"),
    )

    val bavli: Set<String> = tractates.keys.take(37).toSet()

    val rabba: Map<String, List<String>> = mapOf(
        "בראשית רבה" to listOf("בראשית רבה", "ב\"ר", "בר\"ר", "בראשית רבא"),
        "שמות רבה" to listOf("שמות רבה", "שמו\"ר", "ש\"ר", "שמ\"ר"),
        "ויקרא רבה" to listOf("ויקרא רבה", "ויק\"ר", "וי\"ר"),
        "במדבר רבה" to listOf("במדבר רבה", "במד\"ר", "במ\"ר"),
        "דברים רבה" to listOf("דברים רבה", "דב\"ר"),
        "שיר השירים רבה" to listOf("שיר השירים רבה", "שהש\"ר", "שה\"ש רבה"),
        "רות רבה" to listOf("רות רבה", "רו\"ר"),
        "איכה רבה" to listOf("איכה רבה", "איכ\"ר", "איכה רבתי"),
        "קוהלת רבה" to listOf("קהלת רבה", "קה\"ר", "קוהלת רבה"),
        "אסתר רבה" to listOf("אסתר רבה", "אסת\"ר", "אס\"ר"),
    )

    /** The four parts of Tur / Shulchan Arukh. */
    val shulchanArukhParts: Map<String, List<String>> = mapOf(
        "אורח חיים" to listOf("או\"ח", "אורח חיים"),
        "יורה דעה" to listOf("יו\"ד", "יורה דעה"),
        "אבן העזר" to listOf("אה\"ע", "אבה\"ע", "אבן העזר"),
        "חושן משפט" to listOf("חו\"מ", "חושן משפט", "חשן משפט"),
    )

    val misc: Map<String, List<String>> = mapOf(
        "משנה ברורה" to listOf("משנה ברורה", "משנ\"ב"),
        "מדרש תנחומא" to listOf("תנחומא", "מדרש תנחומא"),
        "ספרא" to listOf("ספרא", "תורת כהנים"),
        "ספרי במדבר" to listOf("ספרי במדבר"),
        "ספרי דברים" to listOf("ספרי דברים"),
        "ספר החינוך" to listOf("ספר החינוך", "החינוך"),
        "ילקוט שמעוני על התורה" to listOf("ילקוט שמעוני", "ילק\"ש"),
    )

    /** Short Rambam hilchot names that differ from the title's own wording. */
    val rambamExtra: Map<String, List<String>> = mapOf(
        "קריאת שמע" to listOf("ק\"ש"),
        "יסודי התורה" to listOf("יסוה\"ת", "יסודי תורה"),
        "מאכלות אסורות" to listOf("מאכ\"א"),
        "איסורי ביאה" to listOf("איסו\"ב"),
        "תפילה וברכת כהנים" to listOf("תפלה", "תפילה"),
        "עבודה זרה וחוקות הגויים" to listOf("ע\"ז", "עכו\"ם", "עבודה זרה"),
        "תלמוד תורה" to listOf("ת\"ת"),
        "שביתת יום טוב" to listOf("יום טוב", "יו\"ט"),
        "מלוה ולוה" to listOf("מלוה"),
        "גזילה ואבידה" to listOf("גזילה", "אבידה"),
        "סנהדרין והעונשין המסורין להם" to listOf("סנהדרין"),
        "מלכים ומלחמות" to listOf("מלכים"),
        "רוצח ושמירת נפש" to listOf("רוצח"),
        "זכייה ומתנה" to listOf("זכיה ומתנה"),
        "ערכים וחרמין" to listOf("ערכין"),
        "שמיטה ויובל" to listOf("שמיטה"),
        "קידוש החודש" to listOf("קדוש החדש"),
        "יבום וחליצה" to listOf("יבום", "חליצה"),
        "אבל" to listOf("אבלות"),
    )

    /** Words that introduce a citation without being part of it ("ועיין", "ראה", …), longest first. */
    val introWords: List<String> = listOf(
        "ועיין עוד", "עיין עוד", "ועיין", "עיין", "ועי'", "עי'", "וע\"ע", "ע\"ע", "וע'", "ע'", "וראה", "ראה", "עיי'",
        "וכמ\"ש", "כמ\"ש", "כדאיתא", "דאיתא", "והובא", "הובא", "מובא", "ע\"פ", "על פי", "עפ\"י", "וכן", "יעויין",
        "ויעויין", "וראו", "ראו",
    )
}
