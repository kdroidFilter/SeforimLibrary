"""Extract citation candidates from the whole corpus (read-only) into cands.db."""
import re, sqlite3, sys, time

SRC = sys.argv[1]
OUT = sys.argv[2]

TAG = re.compile(r"<[^>]+>")
NIKUD = re.compile(r"[֑-ׇ]")
# Parenthesized / bracketed groups, no nesting, bounded length.
GROUP = re.compile(r"[(\[]([^()\[\]]{1,80})[)\]]")
REFLINK = re.compile(r'<a [^>]*class="refLink"[^>]*>', re.I)
DATAREF = re.compile(r'data-ref="([^"]*)"')
ANY_A = re.compile(r"<a\b[^>]*>", re.I)

src = sqlite3.connect(f"file:{SRC}?mode=ro", uri=True)
out = sqlite3.connect(OUT)
out.executescript("""
DROP TABLE IF EXISTS cand; DROP TABLE IF EXISTS anchor;
CREATE TABLE cand(lineId INT, bookId INT, pos INT, raw TEXT, after TEXT, before TEXT);
CREATE TABLE anchor(lineId INT, bookId INT, tag TEXT, dataRef TEXT, inner TEXT);
""")

cur = src.execute("SELECT id, bookId, content FROM line")
n = 0; t0 = time.time(); buf = []; abuf = []
for lid, bid, content in cur:
    n += 1
    if "<a" in content:
        for m in ANY_A.finditer(content):
            tag = m.group(0)
            end = content.find("</a>", m.end())
            inner = TAG.sub("", content[m.end():end]) if end > 0 else ""
            dr = DATAREF.search(tag)
            abuf.append((lid, bid, tag[:300], dr.group(1) if dr else None, inner[:120]))
    text = NIKUD.sub("", TAG.sub("", content))
    for m in GROUP.finditer(text):
        raw = m.group(1).strip()
        # keep only groups containing Hebrew letters
        if not re.search(r"[א-ת]", raw):
            continue
        buf.append((lid, bid, m.start(), m.group(0)[0] + raw, text[m.end():m.end() + 60], text[max(0, m.start() - 40):m.start()]))
    if len(buf) > 200000:
        out.executemany("INSERT INTO cand VALUES(?,?,?,?,?,?)", buf); buf.clear()
    if len(abuf) > 50000:
        out.executemany("INSERT INTO anchor VALUES(?,?,?,?,?)", abuf); abuf.clear()
    if n % 500000 == 0:
        print(n, round(time.time() - t0), flush=True)
out.executemany("INSERT INTO cand VALUES(?,?,?,?,?,?)", buf)
out.executemany("INSERT INTO anchor VALUES(?,?,?,?,?)", abuf)
out.commit()
print("done", n, round(time.time() - t0))
