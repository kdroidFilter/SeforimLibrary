"""Hebrew numerals, normalization helpers and the target index."""
import re, sqlite3

VAL = {c: v for c, v in zip("אבגדהוזחטיכלמנסעפצקרשת", [1,2,3,4,5,6,7,8,9,10,20,30,40,50,60,70,80,90,100,200,300,400])}
FINALS = str.maketrans("ךםןףץ", "כמנפצ")
QUOTES = str.maketrans({"׳": "'", "״": '"', "״": '"', "`": "'", "’": "'", "”": '"', "“": '"'})
NIKUD = re.compile(r"[֑-ׇ]")
TAG = re.compile(r"<[^>]+>")


def gem(tok: str):
    """Parse a Hebrew numeral; return int or None. Enforces descending order (except טו/טז)."""
    t = tok.translate(QUOTES).replace("'", "").replace('"', "").translate(FINALS)
    if not t or len(t) > 5 or any(c not in VAL for c in t):
        return None
    if t in ("יה", "יו"):
        return None
    vals = [VAL[c] for c in t]
    for i in range(len(vals) - 1):
        if vals[i] < vals[i + 1] and not (t[i:i + 2] in ("טו", "טז")):
            return None
    # thousands like 'תתר' are fine (400+400+200)
    return sum(vals)


def name_key(s: str) -> str:
    """Spelling-insensitive key: strip quotes/nikud, finals, internal yod/vav."""
    s = NIKUD.sub("", s).translate(QUOTES).replace("-", " ")
    s = s.translate(FINALS)
    out = []
    for w in s.split():
        out.append(w)
    return " ".join(out)


def daf_key(n: int, amud: str) -> str:
    return f"{n}{amud}"


def parse_heref_token(tok: str):
    tok = tok.strip()
    m = re.fullmatch(r"([א-ת]+)([.:])", tok)
    if m:
        g = gem(m.group(1))
        if g:
            return daf_key(g, "a" if m.group(2) == "." else "b")
    g = gem(tok)
    if g is not None and re.fullmatch(r"[א-ת]+", tok):
        return g
    return NIKUD.sub("", tok)


class TargetIndex:
    """Per-book map: section tuple -> (lineId, lineIndex) of first line; plus line text loader."""

    def __init__(self, db_path):
        self.db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        self.books = {t: i for i, t in self.db.execute("select id, title from book")}
        self.cache = {}

    def get(self, title):
        if title in self.cache:
            return self.cache[title]
        bid = self.books.get(title)
        idx = {}
        if bid is not None:
            rows = self.db.execute(
                "select id, lineIndex, heRef from line where bookId=? and heRef is not null order by lineIndex", (bid,))
            for lid, li, h in rows:
                if not h.startswith(title):
                    continue
                rest = h[len(title):].strip().lstrip(",").strip()
                toks = tuple(parse_heref_token(x) for x in rest.split(",") if x.strip())
                # register every prefix -> first line
                for k in range(1, len(toks) + 1):
                    idx.setdefault(toks[:k], (lid, li))
        self.cache[title] = (bid, idx)
        return bid, idx

    def lookup(self, title, secs):
        bid, idx = self.get(title)
        if not idx:
            return None
        return idx.get(tuple(secs))

    def lines(self, bid, start_index, count):
        return self.db.execute(
            "select id, lineIndex, content, heRef from line where bookId=? and lineIndex>=? order by lineIndex limit ?",
            (bid, start_index, count)).fetchall()
