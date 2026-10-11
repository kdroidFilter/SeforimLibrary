"""Prototype citation parser: candidate text inside () or [] -> (family, title, sections, kind)."""
import re
from heb import gem, name_key, QUOTES, FINALS

TANAKH = {
    "בראשית": ["בראשית", "בר'", "ברא'"],
    "שמות": ["שמות", "שמ'"],
    "ויקרא": ["ויקרא", "ויק'"],
    "במדבר": ["במדבר", "במד'", "במ'"],
    "דברים": ["דברים", "דב'"],
    "יהושע": ["יהושע", "יהו'"],
    "שופטים": ["שופטים", "שופ'"],
    "שמואל א": ["שמואל א", "ש\"א", "שמ\"א", "שמואל א'", "שמואל ראשון"],
    "שמואל ב": ["שמואל ב", "ש\"ב", "שמ\"ב", "שמואל ב'", "שמואל שני"],
    "מלכים א": ["מלכים א", "מ\"א", "מל\"א", "מלכים א'"],
    "מלכים ב": ["מלכים ב", "מ\"ב", "מל\"ב", "מלכים ב'"],
    "ישעיהו": ["ישעיהו", "ישעיה", "ישעי'", "יש'"],
    "ירמיהו": ["ירמיהו", "ירמיה", "ירמי'", "יר'"],
    "יחזקאל": ["יחזקאל", "יחזק'", "יחז'"],
    "הושע": ["הושע"], "יואל": ["יואל"], "עמוס": ["עמוס"], "עובדיה": ["עובדיה"], "יונה": ["יונה"],
    "מיכה": ["מיכה"], "נחום": ["נחום"], "חבקוק": ["חבקוק"], "צפניה": ["צפניה"], "חגי": ["חגי"],
    "זכריה": ["זכריה", "זכרי'"], "מלאכי": ["מלאכי"],
    "תהילים": ["תהלים", "תהילים", "תהל'", "תהי'", "תה'"],
    "משלי": ["משלי", "מש'"], "איוב": ["איוב"], "שיר השירים": ["שיר השירים", "שה\"ש", "שיר השיר'"],
    "רות": ["רות"], "איכה": ["איכה"], "קהלת": ["קהלת", "קוהלת"], "אסתר": ["אסתר"], "דניאל": ["דניאל"],
    "עזרא": ["עזרא"], "נחמיה": ["נחמיה"],
    "דברי הימים א": ["דברי הימים א", "דה\"א", "דהי\"א", "דברי הימים א'"],
    "דברי הימים ב": ["דברי הימים ב", "דה\"ב", "דהי\"ב", "דברי הימים ב'"],
}

TRACTATES = {
    "ברכות": ["ברכות"], "שבת": ["שבת"], "עירובין": ["עירובין", "ערובין"], "פסחים": ["פסחים"],
    "יומא": ["יומא"], "סוכה": ["סוכה"], "ביצה": ["ביצה"], "ראש השנה": ["ראש השנה", "ר\"ה"],
    "תענית": ["תענית"], "מגילה": ["מגילה"], "מועד קטן": ["מועד קטן", "מו\"ק"], "חגיגה": ["חגיגה"],
    "יבמות": ["יבמות"], "כתובות": ["כתובות"], "נדרים": ["נדרים"], "נזיר": ["נזיר"], "סוטה": ["סוטה"],
    "גיטין": ["גיטין"], "קידושין": ["קידושין", "קדושין"],
    "בבא קמא": ["בבא קמא", "ב\"ק"], "בבא מציעא": ["בבא מציעא", "ב\"מ"], "בבא בתרא": ["בבא בתרא", "ב\"ב"],
    "סנהדרין": ["סנהדרין"], "מכות": ["מכות"], "שבועות": ["שבועות"], "עבודה זרה": ["עבודה זרה", "ע\"ז"],
    "הוריות": ["הוריות"], "זבחים": ["זבחים"], "מנחות": ["מנחות"], "חולין": ["חולין"], "בכורות": ["בכורות"],
    "ערכין": ["ערכין"], "תמורה": ["תמורה"], "כריתות": ["כריתות"], "מעילה": ["מעילה"], "תמיד": ["תמיד"],
    "נדה": ["נדה", "נידה"],
    # Mishna-only tractates
    "פאה": ["פאה"], "דמאי": ["דמאי"], "כלאים": ["כלאים"], "שביעית": ["שביעית"], "תרומות": ["תרומות"],
    "מעשרות": ["מעשרות"], "מעשר שני": ["מעשר שני"], "חלה": ["חלה"], "ערלה": ["ערלה"], "ביכורים": ["ביכורים", "בכורים"],
    "שקלים": ["שקלים"], "עדויות": ["עדויות", "עדיות"], "אבות": ["אבות"], "מדות": ["מדות", "מידות"], "קינים": ["קינים", "קנים"],
    "כלים": ["כלים"], "אהלות": ["אהלות", "אהלים"], "נגעים": ["נגעים"], "פרה": ["פרה"], "טהרות": ["טהרות"],
    "מקואות": ["מקואות", "מקוואות"], "מכשירין": ["מכשירין"], "זבים": ["זבים"], "טבול יום": ["טבול יום"],
    "ידים": ["ידים", "ידיים"], "עוקצים": ["עוקצים", "עוקצין"],
}
BAVLI = set(list(TRACTATES)[:37])

RABBA = {
    "בראשית רבה": ["בראשית רבה", "ב\"ר", "בר\"ר", "בראשית רבא", "בר\"ר"],
    "שמות רבה": ["שמות רבה", "שמו\"ר", "ש\"ר", "שמ\"ר"],
    "ויקרא רבה": ["ויקרא רבה", "ויק\"ר", "וי\"ר", "ויקר\"ר"],
    "במדבר רבה": ["במדבר רבה", "במד\"ר", "במ\"ר", "במדב\"ר"],
    "דברים רבה": ["דברים רבה", "דב\"ר", "דבר\"ר"],
    "שיר השירים רבה": ["שיר השירים רבה", "שהש\"ר", "שה\"ש רבה", "שיר השירים רבא"],
    "רות רבה": ["רות רבה", "רו\"ר", "רות רבא"],
    "איכה רבה": ["איכה רבה", "איכ\"ר", "איכה רבתי", "א\"ר"],
    "קוהלת רבה": ["קהלת רבה", "קה\"ר", "קוהלת רבה", "קהלת רבא"],
    "אסתר רבה": ["אסתר רבה", "אסת\"ר", "אס\"ר", "אסתר רבא"],
}

SA_PARTS = {
    "אורח חיים": ["או\"ח", "אורח חיים", "אורח חים"],
    "יורה דעה": ["יו\"ד", "יורה דעה"],
    "אבן העזר": ["אה\"ע", "אבה\"ע", "אבן העזר", "אב\"ה"],
    "חושן משפט": ["חו\"מ", "חושן משפט", "חשן משפט"],
}

MISC = {  # title: aliases  (book with generic "N, M" or "סי' N" addressing)
    "משנה ברורה": ["משנה ברורה", "משנ\"ב", "מ\"ב"],
    "מדרש תנחומא": ["תנחומא", "מדרש תנחומא", "תנחומא ורשא"],
    "ספרא": ["ספרא", "תורת כהנים", "ת\"כ"],
    "ספרי במדבר": ["ספרי במדבר"],
    "ספרי דברים": ["ספרי דברים"],
    "ספר החינוך": ["ספר החינוך", "החינוך", "חינוך"],
    "ילקוט שמעוני על התורה": ["ילקוט שמעוני", "ילקוט", "ילק\"ש", "יל\"ש"],
    "ספר הזהר": ["זוהר", "זהר", "זוה\"ק", "זהר הקדוש", "ספר הזהר"],
    "קיצור שולחן ערוך": ["קיצור שולחן ערוך", "קצשו\"ע", "קיצור שו\"ע"],
}

PREFIX_WORDS = ["ועיין עוד", "ועיין", "עיין", "ועי'", "עי'", "וע\"ע", "ע\"ע", "וע'", "ע'", "וראה", "ראה", "עיי'",
                "כמ\"ש", "וכמ\"ש", "כדאיתא", "דאיתא", "הובא", "והובא", "מובא", "ע\"פ", "על פי", "וכן", "כן הוא",
                "יעויין", "ויעויין", "ראו", "וראו", "עפ\"י", "מקור", "מקורו", "צ\"ע", "כנ\"ל", "עיין עוד"]


def norm(s):
    s = s.translate(QUOTES)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def build_aliases():
    """name_key(alias) -> list of (family, canonical)."""
    a = {}

    def add(alias, fam, canon):
        a.setdefault(name_key(alias), []).append((fam, canon))

    for canon, al in TANAKH.items():
        for x in al:
            add(x, "tanakh", canon)
    for canon, al in TRACTATES.items():
        for x in al:
            add(x, "tractate", canon)
            add("מסכת " + x, "tractate", canon)
            add("משנה " + x, "mishna", canon)
            add("מתני' " + x, "mishna", canon)
            add("ירושלמי " + x, "yerushalmi", canon)
            add("ירוש' " + x, "yerushalmi", canon)
            add("תוספתא " + x, "tosefta", canon)
            add("תוספתא דמסכת " + x, "tosefta", canon)
            add("גמ' " + x, "tractate", canon)
            add("גמרא " + x, "tractate", canon)
            add("בבלי " + x, "tractate", canon)
            add("רש\"י " + x, "rashi_bavli", canon)
            add("תוס' " + x, "tosafot", canon)
            add("תוספות " + x, "tosafot", canon)
    for canon, al in RABBA.items():
        for x in al:
            add(x, "rabba", canon)
            add("מדרש " + x, "rabba", canon)
    for canon, al in SA_PARTS.items():
        for x in al:
            add(x, "sa", canon)
            add("שו\"ע " + x, "sa", canon)
            add("שולחן ערוך " + x, "sa", canon)
            add("ש\"ע " + x, "sa", canon)
            add("טוש\"ע " + x, "sa", canon)
            add("טור " + x, "tur", canon)
            add("ט" + x, "tur", canon) if x.endswith('"ח') else None
            add("מ\"ב " + x, "misc", "משנה ברורה") if canon == "אורח חיים" else None
    add("שו\"ע", "sa", None)
    add("שולחן ערוך", "sa", None)
    add("טור", "tur", None)
    add("רמב\"ם", "rambam", None)
    add("רמבם", "rambam", None)
    add("הרמב\"ם", "rambam", None)
    add("משנה תורה", "rambam", None)
    add("יד החזקה", "rambam", None)
    for canon, al in MISC.items():
        for x in al:
            add(x, "misc", canon)
    for x in ["זח\"א", "זוהר ח\"א", "זהר ח\"א", "זוה\"ק ח\"א"]:
        add(x, "zohar_daf", 1)
    for x in ["זח\"ב", "זוהר ח\"ב", "זהר ח\"ב", "זוה\"ק ח\"ב"]:
        add(x, "zohar_daf", 2)
    for x in ["זח\"ג", "זוהר ח\"ג", "זהר ח\"ג", "זוה\"ק ח\"ג"]:
        add(x, "zohar_daf", 3)
    return a


ALIASES = build_aliases()
MAX_ALIAS_WORDS = max(len(k.split()) for k in ALIASES)

# ---------- location grammars ----------
NUM = r"[א-ת]{1,4}['\"]?[א-ת]?"
SEP = r"\s*[,:]?\s*"


def n(tok):
    return gem(tok) if tok else None


def strip_prefix(body):
    changed = True
    while changed:
        changed = False
        for p in PREFIX_WORDS:
            if body.startswith(p + " "):
                body = body[len(p) + 1:].strip()
                changed = True
    return body


def match_name(body):
    """Longest-prefix alias match. Also tries stripping a leading ב/ו/וב/ל/מ letter. Returns (cands, rest, alias)."""
    words = body.split()
    variants = [(words, "")]
    if words and len(words[0]) > 2 and words[0][0] in "בוד":
        variants.append(([words[0][1:]] + words[1:], words[0][0]))
        if words[0][:2] in ("וב", "וד") and len(words[0]) > 3:
            variants.append(([words[0][2:]] + words[1:], words[0][:2]))
    for ws, pre in variants:
        for k in range(min(MAX_ALIAS_WORDS, len(ws)), 0, -1):
            key = name_key(" ".join(ws[:k]))
            if key in ALIASES:
                return ALIASES[key], " ".join(ws[k:]).strip(" ,."), " ".join(ws[:k])
    return None, body, None


def clean_loc(rest):
    rest = rest.translate(QUOTES)
    rest = re.sub(r"\s+", " ", rest).strip(" ,;")
    return rest


def loc_chapter_verse(rest):
    """'א, ד' | 'א ד' | 'פרק א פסוק ד' | 'א' | 'א, ד-ו' -> (chap, verse|None, verse_end|None)."""
    r = rest
    r = re.sub(r"^(פרק|פ')\s*", "", r)
    r = re.sub(r"\s*(פסוק|פס')\s*", " ", r)
    m = re.fullmatch(rf"({NUM})(?:{SEP}({NUM})(?:\s*[-–]\s*({NUM}))?)?\.?", r)
    if not m:
        return None
    if any(g in ('ע"א', 'ע"ב') for g in m.groups() if g):
        return None
    c, v, e = n(m.group(1)), n(m.group(2)), n(m.group(3))
    if c is None or (m.group(2) and v is None):
        return None
    return c, v, e


def loc_daf(rest):
    """'דף כא:' | 'כא.' | 'כא ע"ב' | 'דף כא ע"א' | 'כא, א' | 'כא' -> (daf, amud)."""
    r = rest
    r = re.sub(r"^(דף|ד')\s*", "", r)
    m = re.fullmatch(rf"((?:{NUM})?[א-ת]'?)\s*([.:])?(?:\s*,?\s*(?:ע\"|עמוד\s*|ס?ע\")([אב]))?(?:\s*(?:ד\"ה|בד\"ה).*)?\.?", r)
    if not m:
        m2 = re.fullmatch(rf"({NUM})\s*[,.]?\s*([אב])'?\.?", r)
        if not m2:
            return None
        d = n(m2.group(1))
        return (d, "a" if m2.group(2) == "א" else "b", True, False) if d else None
    d = n(m.group(1))
    if not d or d < 2 or d > 180:
        return None
    amud = "a"
    if m.group(2) == ":" or m.group(3) == "ב":
        amud = "b"
    return d, amud, bool(m.group(2) or m.group(3)), "ד\"ה" in r


def loc_perek_mishna(rest):
    """'פ"א מ"ב' | 'פרק א משנה ב' | 'א, ב' | 'פ"א' -> (perek, mishna|None)."""
    r = rest
    m = re.fullmatch(rf"(?:פ\"?|פרק\s*)({NUM})(?:\s*(?:מ\"|משנה\s*|ה\"|הלכה\s*)({NUM}))?", r.replace("פ\"", "פ\"", 1))
    if m:
        p = n(m.group(1).lstrip('"')) if m.group(1) else None
        mm = n(m.group(2)) if m.group(2) else None
        if p:
            return p, mm
    cv = loc_chapter_verse(r)
    if cv:
        return cv[0], cv[1]
    return None






def parse_siman(r):
    """'סי' רנא ס"ב' | 'סימן רנא סעיף ב' | 'רנא, ב' | 'סי' רנא ס"ק ג' -> (siman, seif|None, is_sk)."""
    r = r.strip()
    m = re.match(rf"^(?:סי'|סימן|ס'|סי)\s*({NUM})\b\s*,?\s*(.*)$", r)
    if m:
        si, rest = n(m.group(1)), m.group(2).strip()
    else:
        m = re.fullmatch(rf"({NUM}){SEP}({NUM})?", r)
        if not m:
            return None
        si, rest = n(m.group(1)), (m.group(2) or "")
        return (si, n(rest) if rest else None, False) if si else None
    if not si:
        return None
    if not rest:
        return si, None, False
    if re.match(r"^(?:ס\"ק|סק\"|סעיף קטן|ס\"ק)", rest) and not re.fullmatch(rf"(?:ס\"ק|סק\"|סעיף קטן)\s*({NUM})\.?", rest):
        return None
    m = re.fullmatch(rf"(?:ס\"ק|סק\"|סעיף קטן)\s*({NUM})\.?", rest)
    if m:
        return si, n(m.group(1)), True
    m = re.fullmatch(rf"(?:סעיף|סעי'|ס\"?|סע')\s*({NUM})\.?", rest)
    if m and n(m.group(1)):
        return si, n(m.group(1)), False
    m = re.fullmatch(rf"ס\"([א-ת]{{1,2}})\.?", rest)
    if m:
        return si, n(m.group(1)), False
    m = re.fullmatch(rf"({NUM})\.?", rest)
    if m and n(m.group(1)):
        return si, n(m.group(1)), False
    return None


def marker_num(tok, markers):
    """'פ"ג' / 'פי"ג' / 'מי"ג' / 'ה"ב' -> number when tok starts with one of the marker letters."""
    tok = tok.translate(QUOTES).rstrip(".,")
    if len(tok) < 3 or tok[0] not in markers or '"' not in tok:
        return None
    rest = tok[1:]
    if rest.startswith('"'):
        rest = rest[1:]
        return gem(rest) if len(rest) <= 2 else None
    if rest.count('"') == 1 and rest.index('"') == len(rest) - 2:
        return gem(rest)
    return None


def parse_perek_token(r):
    ws = r.split(maxsplit=1)
    if ws:
        v = marker_num(ws[0], "פ")
        if v:
            return v, (ws[1] if len(ws) > 1 else "")
    m = re.match(rf"^(?:פרק|פ'|פר')\s*({NUM})(?=\s|$|,)\s*,?\s*(.*)$", r)
    if m and n(m.group(1)):
        return n(m.group(1)), m.group(2)
    return None, r


def parse_sub_token(r, letters):
    ws = r.split(maxsplit=1)
    if ws:
        v = marker_num(ws[0], "".join(letters))
        if v:
            return v, (ws[1] if len(ws) > 1 else "")
    m = re.match(rf"^(?:משנה|הלכה|הל'|מ'|ה'|סימן|סי'|אות|ס')\s*({NUM})(?=\s|$|,)\s*(.*)$", r)
    if m and n(m.group(1)):
        return n(m.group(1)), m.group(2)
    return None, r


RAMBAM_HILCHOT = None


def set_rambam_titles(titles):
    """titles: book titles like 'משנה תורה, הלכות שבת'."""
    global RAMBAM_HILCHOT
    RAMBAM_HILCHOT = {}
    extra = {
        "קריאת שמע": ["ק\"ש", "קר\"ש"], "יסודי התורה": ["יסוה\"ת", "יסודי תורה", "יסוד התורה"],
        "מאכלות אסורות": ["מאכ\"א", "מא\"א", "מאכלות אסורים"], "איסורי ביאה": ["איסו\"ב", "א\"ב"],
        "תפילה וברכת כהנים": ["תפלה", "תפילה", "נשיאות כפים"], "שבת": [], "עבודה זרה וחוקות הגויים": ["ע\"ז", "עכו\"ם", "עבודת כוכבים", "עבודה זרה"],
        "תשובה": [], "דעות": ["דעות"], "תלמוד תורה": ["ת\"ת"], "חמץ ומצה": ["חו\"מ", "חמץ", "חמץ ומצה"],
        "שופר וסוכה ולולב": ["שופר", "סוכה", "לולב"], "מגילה וחנוכה": ["מגילה", "חנוכה"],
        "שביתת עשור": ["שביתת עשור"], "שביתת יום טוב": ["יום טוב", "יו\"ט"], "ברכות": [],
        "תפילין ומזוזה וספר תורה": ["תפילין", "מזוזה", "ספר תורה", "ס\"ת"], "שאר אבות הטומאות": ["שאר אבה\"ט"],
        "מלוה ולוה": ["מלוה", "מלוה ולוה"], "גזילה ואבידה": ["גזילה", "אבידה", "גזלה ואבדה"], "אישות": [],
        "גירושין": ["גרושין"], "סנהדרין והעונשין המסורין להם": ["סנהדרין"], "מלכים ומלחמות": ["מלכים"],
        "שכנים": [], "שלוחין ושותפין": ["שלוחין", "שותפין"], "טוען ונטען": [], "עדות": [], "רוצח ושמירת נפש": ["רוצח"],
        "גניבה": ["גנבה"], "נזקי ממון": [], "חובל ומזיק": [], "מכירה": [], "זכייה ומתנה": ["זכיה ומתנה", "זכיה"],
        "שכירות": [], "שאלה ופקדון": ["שאלה ופקדון"], "נחלות": [], "מעשה הקרבנות": ["מעה\"ק"], "בית הבחירה": ["בית הבחירה", "ביהב\"ח"],
        "כלי המקדש והעובדין בו": ["כלי המקדש"], "תמידים ומוספין": ["תמידין ומוספין"], "פסולי המוקדשין": [],
        "איסורי מזבח": ["איסורי מזבח"], "ציצית": [], "עירובין": [], "תעניות": ["תענית"], "שחיטה": [],
        "כלאים": [], "מתנות עניים": ["מ\"ע"], "תרומות": [], "מעשר": [], "מעשר שני ונטע רבעי": ["מעשר שני"],
        "ביכורים ושאר מתנות כהונה שבגבולין": ["ביכורים", "בכורים"], "שמיטה ויובל": ["שמיטה", "שמיטה ויובל", "שמטה ויובל"],
        "נדרים": [], "נזירות": [], "שבועות": [], "ערכים וחרמין": ["ערכין", "ערכין וחרמין"], "מילה": [],
        "טומאת מת": [], "טומאת צרעת": [], "מקואות": [], "פרה אדומה": [], "אבל": ["אבלות"], "ממרים": [],
        "נערה בתולה": [], "סוטה": [], "יבום וחליצה": ["יבום", "חליצה"], "עבדים": [], "חמץ ומצה ": [],
        "קידוש החודש": ["קדוש החדש", "קה\"ח"], "בית הבחירה ": [], "מעילה": [], "קרבן פסח": [], "חגיגה": [],
        "בכורות": [], "שגגות": [], "מחוסרי כפרה": [], "תמורה": [], "טומאת אוכלין": [], "כלים": [],
        "משכב ומושב": [], "מטמאי משכב ומושב": ["מטמאי משכב ומושב"], "שקלים": [], "לולב": [], "סוכה": [], "שופר": [],
        "אבות הטומאה": ["אבות הטומאות"], "דעות ": [], "אסורי ביאה": [], "עבודת יום הכפורים": ["עבודת יוה\"כ", "יוה\"כ"],
        "ביאת מקדש": ["ביאת המקדש"], "מעשה הקרבנות ": [], "מתנות עניים ": [], "שאלה ופקדון ": [], "מלוה ולוה ": [],
        "נזקי ממון ": [], "מכירה ": [], "עבודה זרה": ["ע\"ז"], "תפילה": [], "חובל ומזיק ": [],
    }
    for t in titles:
        name = t.split("הלכות", 1)[1].strip() if "הלכות" in t else None
        if not name:
            continue
        RAMBAM_HILCHOT.setdefault(name_key(name), t)
        for al in extra.get(name, []):
            RAMBAM_HILCHOT.setdefault(name_key(al), t)
        # first word of a compound name ('תפילין ומזוזה...' -> 'תפילין')
        RAMBAM_HILCHOT.setdefault(name_key(name.split(" ו")[0]), t)


def match_hilchot(words):
    for k in range(min(5, len(words)), 0, -1):
        key = name_key(" ".join(words[:k]))
        if key in RAMBAM_HILCHOT:
            return RAMBAM_HILCHOT[key], words[k:]
    return None, words


def parse_rambam(rest):
    """'הל' שבת פ"ב ה"ג' | 'פ"ב מהל' שבת ה"ג' | 'הלכות שבת ב, ג' -> (title, perek, halacha)."""
    r = rest.strip(" ,")
    p, h, title = None, None, None
    p0, r2 = parse_perek_token(r)
    if p0:
        m = re.match(r"^(?:מהל'|מהלכות|מה'|הל'|הלכות|בהל')\s*(.*)$", r2)
        if not m:
            return None
        title, ws = match_hilchot(m.group(1).split())
        if not title:
            return None
        rr = " ".join(ws).strip(" ,")
        h, rr = parse_sub_token(rr, ["ה"]) if rr else (None, "")
        if rr.strip(" ."):
            return None
        return title, p0, h
    m = re.match(r"^(?:הל'|הלכות|הלכ'|ה')\s*(.*)$", r)
    if not m:
        return None
    title, ws = match_hilchot(m.group(1).split())
    if not title:
        return None
    rr = " ".join(ws).strip(" ,")
    if not rr:
        return title, None, None
    p, rr2 = parse_perek_token(rr)
    if p:
        h, rr3 = parse_sub_token(rr2.strip(" ,"), ["ה"]) if rr2.strip(" ,") else (None, "")
        if rr3.strip(" ."):
            return None
        return title, p, h
    cv = loc_chapter_verse(rr)
    if cv:
        return title, cv[0], cv[1]
    return None


def parse(raw):
    """Return dict describing the first citation of the group, or None if it is not recognized."""
    body = norm(raw[1:].rstrip(")]"))
    seg = re.split(r"\s*;\s*", body)[0]
    r = parse_segment(seg)
    if r:
        r["segments"] = len(re.split(r"\s*;\s*", body))
    return r


def parse_segment(body):
    body = strip_prefix(body)
    cands, rest, alias = match_name(body)
    if not cands:
        return None
    rest = clean_loc(rest)
    toks = rest.split()
    out = []
    for fam, canon in cands:
        r = None
        for k in range(len(toks), 0 if not toks else 0, -1):
            r = try_family(fam, canon, " ".join(toks[:k]).strip(" ,.") if k else "")
            if r and (k > 0 or not toks):
                r["trailing"] = len(toks) - k
                break
            r = None
        if r:
            r["alias"] = alias
            if not any(o["title"] == r["title"] and o["fam"] == r["fam"] for o in out):
                out.append(r)
    if not out:
        return {"fam": cands[0][0], "status": "name_only_or_bad_loc", "alias": alias, "rest": rest,
                "title": cands[0][1]}
    if len(out) > 1:
        out.sort(key=lambda x: x.get("prio", 0), reverse=True)
        if out[0].get("prio", 0) == out[1].get("prio", 0):
            out[0]["ambiguous"] = [o["title"] for o in out[1:]]
    return out[0]


def try_family(fam, canon, rest):
    if fam == "tanakh":
        if not rest:
            return {"fam": fam, "title": canon, "secs": [], "status": "book_only", "prio": 1}
        cv = loc_chapter_verse(rest)
        if not cv:
            return None
        c, v, e = cv
        if v is None:
            return {"fam": fam, "title": canon, "secs": [c], "status": "chapter", "prio": 2}
        return {"fam": fam, "title": canon, "secs": [c, v], "end": e, "status": "verse", "prio": 3}
    if fam == "tractate":
        if not rest:
            return {"fam": "bavli", "title": canon, "secs": [], "status": "book_only", "prio": 1}
        d = loc_daf(rest)
        if d and not d[2] and marker_num(rest.split()[0], "פ") and rest.split()[0][1:2] == '"':
            d = None  # 'פ"א' without amud: chapter, not folio 81
        if d and canon in BAVLI:
            daf, amud, explicit, dh = d
            return {"fam": "bavli", "title": canon, "secs": [f"{daf}{amud}"], "status": "daf" if explicit else "daf_no_amud",
                    "dh": dh, "prio": 3}
        pm = loc_perek_mishna_strict(rest)
        if not pm:
            cv = loc_chapter_verse(rest)
            if cv and cv[1] is not None:
                pm = (cv[0], cv[1])
        if pm:
            p, m = pm
            return {"fam": "mishna", "title": "משנה " + canon, "secs": [p] + ([m] if m else []),
                    "status": "mishna" if m else "perek", "prio": 2}
        return None
    if fam in ("mishna", "yerushalmi", "tosefta"):
        title = {"mishna": "משנה ", "yerushalmi": "תלמוד ירושלמי ", "tosefta": "תוספתא "}[fam] + canon
        if not rest:
            return {"fam": fam, "title": title, "secs": [], "status": "book_only", "prio": 1}
        pm = loc_perek_mishna_strict(rest) or loc_perek_mishna(rest)
        if pm:
            p, m = pm
            return {"fam": fam, "title": title, "secs": [p] + ([m] if m else []), "status": "full" if m else "perek", "prio": 3}
        if fam == "yerushalmi" and loc_daf(rest):
            return {"fam": fam, "title": title, "secs": [], "status": "yer_daf_unsupported", "prio": 1}
        return None
    if fam in ("rashi_bavli", "tosafot"):
        d = loc_daf(rest)
        if d and canon in BAVLI:
            daf, amud, explicit, dh = d
            title = ("רש\"י על " if fam == "rashi_bavli" else "תוספות על ") + canon
            return {"fam": fam, "title": title, "secs": [f"{daf}{amud}"], "status": "daf", "dh": dh, "prio": 3}
        return None
    if fam == "rabba":
        if not rest:
            return {"fam": fam, "title": canon, "secs": [], "status": "book_only", "prio": 1}
        p, r2 = parse_perek_token(re.sub(r"^(?:פרשה|פר')\s*", "פרק ", rest))
        if p:
            r2s = r2.strip(" ,")
            s, r3 = parse_sub_token(r2s, ["ס", "א"]) if r2s else (None, "")
            if s is None and r2s and re.fullmatch(rf"{NUM}\.?", r2s) and n(r2s.rstrip(".")):
                s, r3 = n(r2s.rstrip(".")), ""
            if r3.strip(" ."):
                return None
            return {"fam": fam, "title": canon, "secs": [p] + ([s] if s else []), "status": "parasha", "prio": 3}
        cv = loc_chapter_verse(rest)
        if cv:
            return {"fam": fam, "title": canon, "secs": [cv[0]] + ([cv[1]] if cv[1] else []), "status": "parasha", "prio": 3}
        return None
    if fam in ("sa", "tur"):
        part = canon
        r = rest
        if part is None:
            for pc, al in SA_PARTS.items():
                for x in al:
                    if r.startswith(x):
                        part, r = pc, r[len(x):].strip(" ,")
                        break
                if part:
                    break
            if part is None:
                return None
        s = parse_siman(r)
        if not s:
            return None
        si, se, sk = s
        if sk:
            return None
        title = f"שולחן ערוך, {part}" if fam == "sa" else "טור"
        secs = ([part] if fam == "tur" else []) + [si] + ([se] if se else [])
        return {"fam": fam, "title": title, "secs": secs, "status": "seif" if se else "siman", "prio": 3}
    if fam == "rambam":
        rr = parse_rambam(rest)
        if not rr:
            return None
        t, p, h = rr
        return {"fam": fam, "title": t, "secs": ([p] if p else []) + ([h] if h else []),
                "status": "halacha" if h else ("perek" if p else "book_only"), "prio": 3}
    if fam == "misc":
        if not rest:
            return {"fam": fam, "title": canon, "secs": [], "status": "book_only", "prio": 1}
        if canon == "משנה ברורה":
            s = parse_siman(rest)
            if s:
                return {"fam": fam, "title": canon, "secs": [s[0]] + ([s[1]] if s[1] else []), "status": "sk", "prio": 3}
            return None
        if canon == "ילקוט שמעוני על התורה":
            m = re.fullmatch(rf"(?:.*?\s)?(?:רמז|רמ')\s*({NUM})\.?", rest)
            if m and n(m.group(1)):
                return {"fam": fam, "title": canon, "secs": [n(m.group(1))], "status": "remez", "prio": 3,
                        "note": "torah/nach numbering ambiguity"}
            return None
        if canon == "ספר החינוך":
            m = re.fullmatch(rf"(?:מצוה|מצוות|מצ')\s*({NUM})\.?", rest)
            if m and n(m.group(1)):
                return {"fam": fam, "title": canon, "secs": [n(m.group(1))], "status": "mitzva", "prio": 3}
            return None
        if canon == "מדרש תנחומא":
            ws = rest.split()
            if len(ws) >= 2 and n(ws[-1]):
                return {"fam": fam, "title": canon, "secs": [" ".join(ws[:-1]), n(ws[-1])], "status": "parasha_siman", "prio": 3}
            if len(ws) >= 1 and not n(ws[0]):
                return {"fam": fam, "title": canon, "secs": [" ".join(ws)], "status": "parasha", "prio": 2}
            return None
        cv = loc_chapter_verse(rest)
        if cv:
            return {"fam": fam, "title": canon, "secs": [cv[0]] + ([cv[1]] if cv[1] else []), "status": "generic", "prio": 2}
        return None
    if fam == "zohar_daf":
        d = loc_daf(rest)
        if d:
            return {"fam": fam, "title": "ספר הזהר", "secs": [], "vol": canon, "daf": f"{d[0]}{d[1]}", "status": "zohar_daf", "prio": 3}
        return None
    return None


def loc_perek_mishna_strict(rest):
    """Only explicit perek/mishna markers: 'פ"א מ"ב', 'פרק א משנה ב', 'פ"א'."""
    p, r2 = parse_perek_token(rest)
    if not p:
        return None
    r2 = r2.strip(" ,")
    if not r2:
        return p, None
    m, r3 = parse_sub_token(r2, ["מ", "ה"])
    if m and not r3.strip(" ."):
        return p, m
    return None
