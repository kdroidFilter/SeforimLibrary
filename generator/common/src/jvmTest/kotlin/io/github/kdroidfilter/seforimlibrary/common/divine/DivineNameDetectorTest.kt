package io.github.kdroidfilter.seforimlibrary.common.divine

import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DivineNameDetectorTest {
    private val overrides = DivineNameOverrides.parse(
        listOf(
            DivineNameOverrides.HEADER,
            listOf("שמות 3:14", "EHYEH", "sacred", "", "x"),
            listOf("בראשית 19:18", "ADNUT", "sacred", "", "x"),
            listOf("בראשית 19:1-38", "*", "profane", "", "x"),
            listOf("שופטים 17:1-18:31", "*", "profane", "", "x"),
        ),
    ).overrides!!
    private val detector = DivineNameDetector(overrides)

    // Vocalized forms spelled with explicit marks so the tests say which point decides.
    private val el = "אֵל" // אֵל
    private val elSegol = "אֶל" // אֶל
    private val adnut = "אֲדֹנָי" // אֲדֹנָי
    private val adnutPatah = "אֲדֹנַי" // אֲדֹנַי
    private val shaddai = "שַׁדַּי" // שַׁדַּי
    private val sadai = "שָׂדַי" // שָׂדַי
    private val eloah = "אֱלוֹהַּ" // אֱלוֹהַּ
    private val elle = "אֵלֶּה" // אֵלֶּה
    private val hvyh = "יְהוָה" // יְהוָה
    private val ehyeh = "אֶהְיֶה" // אֶהְיֶה

    private data class Hit(val text: String, val kind: DivineKind, val source: DivineSource)

    private fun hits(content: String, verse: VerseRef? = null, nonHvyh: Boolean = true, elScope: Boolean = true): List<Hit> =
        detector.detect(content, verse, nonHvyh, elScope).spans.map { Hit(content.substring(it.rawStart, it.rawEnd), it.kind, it.source) }

    private fun kinds(content: String, verse: VerseRef? = null) = hits(content, verse).map { it.kind to it.source }

    // ── HVYH ──

    @Test
    fun `hvyh follows the app regex and excludes the prefix`() {
        val text = "לַיהוָה"
        assertEquals(listOf(Hit("יהוָה", DivineKind.HVYH, DivineSource.HVYH_RULE)), hits(text))
    }

    @Test
    fun `hvyh inside a longer word is skipped like the app does`() {
        assertEquals(emptyList(), hits("ויגביהוהו"))
        assertEquals(emptyList(), DivineNameDetector.appHvyhMatches("ויגביהוהו"))
    }

    @Test
    fun `hvyh is emitted for every app match outside markup`() {
        val text = "<b>$hvyh</b> אמר ל$hvyh &nbsp;יהוה"
        val result = detector.detect(text)
        assertEquals(result.appHvyh.size, result.spans.count { it.kind == DivineKind.HVYH })
        assertEquals(3, result.appHvyh.size)
    }

    // ── NIKUD ──

    @Test
    fun `adnut needs qamats under the nun`() {
        assertEquals(listOf(DivineKind.ADNUT to DivineSource.NIKUD_RULE), kinds("אל נא $adnut"))
        assertEquals(emptyList(), kinds("הנה נא $adnutPatah"))
        assertEquals(listOf(Hit(adnut, DivineKind.ADNUT, DivineSource.NIKUD_RULE)), hits("וַ$adnut".replace("וַאֲ", "וַא")).map { it.copy(text = adnut) })
    }

    @Test
    fun `shaddai needs a shin dot and a dagesh`() {
        assertEquals(listOf(DivineKind.SHADDAI to DivineSource.NIKUD_RULE), kinds("וְ$shaddai"))
        assertEquals(emptyList(), kinds(sadai))
        assertEquals(emptyList(), kinds("שֶׁדַּי לוֹ"))
    }

    @Test
    fun `el needs tsere and never takes a heh prefix`() {
        assertEquals(listOf(DivineKind.EL to DivineSource.NIKUD_RULE), kinds("$el גדול"))
        assertEquals(emptyList(), kinds("$elSegol משה"))
        assertEquals(emptyList(), kinds("הָ$el"))
    }

    @Test
    fun `leel yad is power not a name`() {
        assertEquals(emptyList(), kinds("יֵשׁ לְ$el יָדִי"))
    }

    @Test
    fun `el with a prefix keeps the prefix out of the span`() {
        val text = "לְ$el עֶלְיוֹן"
        assertEquals(listOf(Hit(el, DivineKind.EL, DivineSource.NIKUD_RULE)), hits(text))
    }

    @Test
    fun `beit el is a place`() {
        assertEquals(emptyList(), kinds("בֵּית־$el"))
    }

    @Test
    fun `el before other gods or a negation is skipped`() {
        assertEquals(emptyList(), kinds("לְ$el אַחֵר"))
        assertEquals(emptyList(), kinds("בְלֹא־$el"))
    }

    @Test
    fun `eloah needs a mapiq and no foreign context`() {
        // Disabled pending rabbinic approval (ELOAH_NIKUD_ENABLED): even a well-formed אֱלוֹהַּ is not written.
        assertEquals(false, DivineNameDetector.ELOAH_NIKUD_ENABLED)
        assertEquals(emptyList(), kinds("מִי $eloah"))
        assertEquals(emptyList(), kinds("מִי $eloah", VerseRef("איוב", 3, 4)))
        assertEquals(emptyList(), kinds("כָּל־$eloah"))
        assertEquals(emptyList(), kinds("$eloah נֵכָר"))
        assertEquals(emptyList(), kinds(elle))
    }

    @Test
    fun `el and eloah by nikud only in tanach and the prayer book`() {
        assertEquals(emptyList(), hits("$el גדול", elScope = false))
        assertEquals(emptyList(), hits("מִי $eloah", elScope = false))
        assertEquals(listOf(DivineKind.ADNUT), hits("אל נא $adnut", elScope = false).map { it.kind })
    }

    @Test
    fun `el by nikud outside a tanach verse is not written but the great el collocation is`() {
        assertEquals(false, DivineNameDetector.EL_NIKUD_IN_PRAYER_BOOK)
        val siddur = "$el מֶלֶךְ יוֹשֵׁב עַל כִּסֵּא רַחֲמִים"
        assertEquals(emptyList(), hits(siddur, elScope = false))
        val great = "הָ$el הַגָּדוֹל הַגִּבּוֹר וְהַנּוֹרָא"
        assertEquals(listOf(Hit(el, DivineKind.EL, DivineSource.COLLOCATION)), hits(great, elScope = false))
    }

    @Test
    fun `tzurishaddai is a personal name`() {
        assertEquals(emptyList(), kinds("צוּרִי שַׁדָּי"))
        assertEquals(emptyList(), kinds("צוּרִישַׁדָּי"))
        assertEquals(listOf(DivineKind.SHADDAI), kinds("וְשַׁדָּי").map { it.first })
    }

    @Test
    fun `hvyh is written only where the app accepts the word boundaries`() {
        assertEquals(1, DivineNameDetector.appHvyhMatches("אביהוה").size)
        assertEquals(emptyList(), hits("אביהוה"))
        assertEquals(emptyList(), hits("יהוהו"))
        assertEquals(listOf(DivineKind.HVYH), hits("וביהוה").map { it.kind })
        assertEquals(listOf(DivineKind.HVYH), hits("<b>ל</b>יהוה").map { it.kind })
    }

    @Test
    fun `a neighbour word split by a tag is read whole`() {
        // Exodus 34:14 as Sefaria stores it: אַחֵ<big>ר</big>.
        val text = "לְ$el אַחֵ<big>ר</big> כִּי"
        assertEquals(emptyList(), kinds(text, VerseRef("שמות", 34, 14)))
        val words = ScannedLine(text).words
        assertEquals("אחר", WordContext(words).skeleton(0, 1))
        assertEquals("כי", WordContext(words).skeleton(0, 2))
    }

    @Test
    fun `presentation forms keep a word whole and are never marked`() {
        // שׁוֹאֵל with U+FB2A, אֵלּוּ with U+FB35, שְׁמוּאֵל with U+FB35.
        assertEquals(emptyList(), kinds("שׁוֹ$el"))
        assertEquals(emptyList(), kinds("אֵלּוּ הדברים"))
        assertEquals(emptyList(), kinds("שְׁמוּ$el"))
        assertEquals(emptyList(), kinds("בֵּית $el"))
    }

    @Test
    fun `a prefix letter needs its own vowel`() {
        assertEquals(emptyList(), kinds("שׁוֹ$el"))
    }

    @Test
    fun `eloah shape rules out aramaic and lookalikes`() {
        assertEquals(emptyList(), kinds("אֱלָהּ")) // Aramaic אֱלָהּ
        assertEquals(emptyList(), kinds("בִּשְׂמֹאלָהּ")) // בִּשְׂמֹאלָהּ
        assertEquals(emptyList(), kinds("וֶאֱלֹהַּ"))
    }

    @Test
    fun `el before a negation or after ben is skipped`() {
        assertEquals(emptyList(), kinds("אֶל־$el לֹא יוֹשִׁיעַ"))
        assertEquals(emptyList(), kinds("בֶּן־לָ$el"))
    }

    @Test
    fun `elohim is never decided by nikud alone`() {
        assertEquals(emptyList(), kinds("בָּרָא אֱלֹהִים"))
    }

    // ── COLLOCATIONS ──

    @Test
    fun `anchor plus elohim keeps the suffix and drops the vav`() {
        assertEquals(listOf(Hit("אלהיך", DivineKind.ELOHIM, DivineSource.COLLOCATION)), hits("ה' אלהיך"))
        val vav = "$hvyh וֵאלֹהֵינּו"
        assertEquals(
            listOf(DivineKind.HVYH to DivineSource.HVYH_RULE, DivineKind.ELOHIM to DivineSource.COLLOCATION),
            kinds(vav),
        )
        assertEquals("אלֹהֵינּו", hits(vav).last().text)
    }

    @Test
    fun `other gods are never marked`() {
        assertEquals(emptyList(), kinds("ה' אלהים אחרים"))
        assertEquals(emptyList(), kinds("ויעבדו ה' אלהי מצרים"))
    }

    @Test
    fun `heh with geresh after a reference word is the number five`() {
        assertEquals(emptyList(), kinds("סימן ה' אלהים"))
        assertEquals(emptyList(), kinds("ד' ה' אלהים"))
    }

    @Test
    fun `elohei and a patriarch`() {
        assertEquals(listOf(Hit("אלהי", DivineKind.ELOHIM, DivineSource.COLLOCATION)), hits("אלהי ישראל"))
        assertEquals(emptyList(), kinds("אלהי נחור"))
    }

    @Test
    fun `tzevaot after an anchor or elohei`() {
        assertEquals(listOf(Hit("צבאות", DivineKind.TZEVAOT, DivineSource.COLLOCATION)), hits("ה' צבאות"))
        assertEquals(
            listOf(Hit("אלהי", DivineKind.ELOHIM, DivineSource.COLLOCATION), Hit("צבאות", DivineKind.TZEVAOT, DivineSource.COLLOCATION)),
            hits("אלהי הצבאות"),
        )
    }

    @Test
    fun `el shaddai el elyon and the great el`() {
        assertEquals(
            listOf(DivineKind.EL to DivineSource.COLLOCATION, DivineKind.SHADDAI to DivineSource.COLLOCATION),
            kinds("באל שדי"),
        )
        assertEquals(emptyList(), kinds("להשיבו אל שדי אמו"))
        assertEquals(emptyList(), kinds("שמואל שדי ליה לכלביה"))
        assertEquals(emptyList(), kinds("שרי. שמואל שדי ליה"))
        assertEquals(
            listOf(DivineKind.EL to DivineSource.COLLOCATION, DivineKind.SHADDAI to DivineSource.COLLOCATION),
            kinds("ויאמר אני אל שדי"),
        )
        assertEquals(listOf(Hit("אל", DivineKind.EL, DivineSource.COLLOCATION)), hits("לאל עליון"))
        assertEquals(emptyList(), kinds("מתחתון אל עליון"))
        assertEquals(listOf(Hit("אל", DivineKind.EL, DivineSource.COLLOCATION)), hits("האל הגדול הגבור והנורא"))
    }

    @Test
    fun `a vocalized collocation word must agree with its points`() {
        assertEquals(emptyList(), kinds("$elSegol שָׂדַי"))
    }

    // ── EXCEPTION LIST ──

    @Test
    fun `ehyeh only in exodus 3-14`() {
        val text = "$ehyeh אשר $ehyeh ... $ehyeh שלחני"
        assertEquals(3, hits(text, VerseRef("שמות", 3, 14)).count { it.kind == DivineKind.EHYEH && it.source == DivineSource.EXCEPTION_LIST })
        assertEquals(emptyList(), kinds(text))
        assertEquals(emptyList(), kinds(text, VerseRef("שמות", 3, 13)))
    }

    @Test
    fun `profane verse suppresses everything but hvyh`() {
        val text = "$hvyh אלהים ו$hvyh אלהי ישראל"
        val inVerse = kinds(text, VerseRef("שופטים", 17, 3))
        assertTrue(inVerse.all { it.first == DivineKind.HVYH }, inVerse.toString())
        assertEquals(2, inVerse.size)
        assertTrue(kinds(text, VerseRef("שופטים", 16, 3)).any { it.first == DivineKind.ELOHIM })
    }

    @Test
    fun `lot is profane except 19-18 adnut which the list forces`() {
        assertEquals(emptyList(), kinds("הנה נא $adnut", VerseRef("בראשית", 19, 2)))
        assertEquals(listOf(DivineKind.ADNUT to DivineSource.EXCEPTION_LIST), kinds("אל נא $adnut", VerseRef("בראשית", 19, 18)))
    }

    @Test
    fun `a tanach line without a verse gets hvyh only`() {
        assertEquals(listOf(DivineKind.HVYH to DivineSource.HVYH_RULE), detector.detect("$hvyh אלהי ישראל", null, nonHvyh = false).spans.map { it.kind to it.source })
    }

    // ── MARKUP ──

    @Test
    fun `a name split by a tag is skipped`() {
        assertEquals(emptyList(), kinds("ה' <b>אל</b>היך"))
        assertEquals(emptyList(), kinds("<big>אֵ</big>ל גדול"))
    }

    @Test
    fun `visible offsets count an entity once and tags as nothing`() {
        val text = "<b>א</b>&nbsp;ה' אלהינו"
        val span = detector.detect(text).spans.single()
        assertEquals(DivineKind.ELOHIM, span.kind)
        assertEquals(5, span.charStart)
        assertEquals(11, span.charEnd)
        assertEquals(countVisibleChars(text, span.rawStart), span.charStart)
        assertEquals(countVisibleChars(text, span.rawEnd), span.charEnd)
    }

    @Test
    fun `soft separators keep a collocation together`() {
        val text = "$hvyh&thinsp;<small>׀</small>&thinsp;אֱלֹהִים"
        assertTrue(kinds(text).contains(DivineKind.ELOHIM to DivineSource.COLLOCATION))
        assertEquals(emptyList(), kinds("ה', אלהים"))
    }

    @Test
    fun `visible range refuses a char that is part of an entity`() {
        val line = ScannedLine("a&amp;b")
        assertNull(line.visibleRange(1, 2))
        assertEquals(2 until 3, line.visibleRange(6, 7))
    }

    // ── TANACH REFS ──

    @Test
    fun `heRef parsing`() {
        assertEquals(VerseRef("בראשית", 19, 18), parseTanachHeRef("בראשית, יט, יח", "בראשית"))
        assertEquals(VerseRef("תהילים", 119, 15), parseTanachHeRef("תהילים, קיט, טו", "תהילים"))
        assertNull(parseTanachHeRef("שמות, ג, יד", "בראשית"))
        assertNull(parseTanachHeRef("בראשית, א", "בראשית"))
    }

    @Test
    fun `override list rejects hvyh and unknown refs as a whole`() {
        val bad = DivineNameOverrides.parse(
            listOf(
                DivineNameOverrides.HEADER,
                listOf("בראשית 1:1", "HVYH", "profane", "", "x"),
                listOf("בראשית 50:1-49:1", "EL", "profane", "", "x"),
                listOf("Genesis 1:1", "EL", "profane", "", "x"),
                listOf("בראשית 1:1", "FOO", "profane", "", "x"),
                listOf("בראשית 1:1-2", "EL", "sacred", "", "x"),
            ),
        )
        assertNull(bad.overrides)
        assertEquals(5, bad.problems.size, bad.problems.toString())
        assertEquals(VerseRef("שמואל א", 28, 13) to VerseRef("שמואל א", 28, 13), DivineNameOverrides.parseRef("שמואל א 28:13"))
        assertEquals(VerseRef("שופטים", 17, 1) to VerseRef("שופטים", 18, 31), DivineNameOverrides.parseRef("שופטים 17:1-18:31"))
    }

    @Test
    fun `without an override list a tanach verse gets hvyh only`() {
        val bare = DivineNameDetector(null)
        val text = "$ehyeh אשר $ehyeh $hvyh אלהי ישראל $adnut"
        assertEquals(
            listOf(DivineKind.HVYH),
            bare.detect(text, VerseRef("שמות", 3, 14)).spans.map { it.kind },
        )
        assertTrue(bare.detect(text).spans.any { it.kind == DivineKind.ELOHIM })
    }
}
