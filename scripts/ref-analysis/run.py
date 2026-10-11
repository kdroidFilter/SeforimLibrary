"""Parse every short candidate, resolve against heRef index, validate. Writes table `res` into cands.db."""
import sqlite3, sys, re, time, json, random, collections
from heb import TargetIndex, NIKUD, TAG, FINALS
import refparse

DB = sys.argv[1]
c = sqlite3.connect("/tmp/refan/cands.db")
ti = TargetIndex(DB)
refparse.set_rambam_titles([t for t in ti.books if t.startswith("משנה תורה, הלכות")])

c.executescript("""DROP TABLE IF EXISTS res;
CREATE TABLE res(lineId INT, bookId INT, raw TEXT, after TEXT, fam TEXT, status TEXT, title TEXT, secs TEXT,
 alias TEXT, targetBook INT, targetLine INT, resolved INT, ambiguous TEXT, quote REAL);""")

WORD = re.compile(r"[א-ת]+")


def words(s):
    s = NIKUD.sub("", TAG.sub(" ", s)).replace("־", " ").translate(FINALS)
    return WORD.findall(s)


verse_cache = {}


def text_of(lid):
    if lid not in verse_cache:
        row = ti.db.execute("select content from line where id=?", (lid,)).fetchone()
        verse_cache[lid] = set(words(row[0])) if row else set()
    return verse_cache[lid]


def quote_score(after, lid):
    q = words(after)[:4]
    q = [w for w in q if len(w) > 1 and w not in ("וגו", "וגומר", "וכו", "וגו'")]
    if len(q) < 2:
        return None
    vw = text_of(lid)
    return sum(1 for w in q if w in vw) / len(q)


t0 = time.time(); stats = collections.Counter(); buf = []
rows = c.execute("select lineId, bookId, raw, after from cand").fetchall()
print("cands", len(rows), flush=True)
for i, (lid, bid, raw, after) in enumerate(rows):
    body = raw[1:]
    if len(body.split()) > 8:
        continue
    p = refparse.parse(raw)
    if not p:
        continue
    tb = tl = None; resolved = 0; q = None
    if p.get("secs") is not None and p.get("title") and p.get("status") not in ("name_only_or_bad_loc",):
        if p["secs"]:
            r = ti.lookup(p["title"], p["secs"])
            tbid, _ = ti.get(p["title"])
            tb = tbid
            if r:
                tl = r[0]; resolved = 1
                if p["fam"] == "tanakh" and p["status"] == "verse":
                    q = quote_score(after, tl)
        else:
            tbid, _ = ti.get(p["title"])
            tb = tbid
            resolved = 2 if tbid else 0  # book-level only
    buf.append((lid, bid, raw, after, p["fam"], p["status"], p.get("title"), json.dumps(p.get("secs"), ensure_ascii=False),
                p.get("alias"), tb, tl, resolved, json.dumps(p.get("ambiguous"), ensure_ascii=False) if p.get("ambiguous") else None, q))
    if len(buf) >= 100000:
        c.executemany("insert into res values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", buf); buf.clear()
        print(i, round(time.time() - t0), flush=True)
c.executemany("insert into res values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", buf)
c.commit()
print("done", round(time.time() - t0))
