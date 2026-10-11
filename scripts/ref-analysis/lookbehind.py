"""Generic lookbehind: book name written just before the parentheses, location inside.
'בישעיה (מ, ג)', 'בשו"ע (סי' רנא)', 'בטור (סי' ...)'. Validated with Tanakh quotes."""
import sqlite3, sys, re, collections, random, json
from heb import TargetIndex, QUOTES, NIKUD, TAG, FINALS
import refparse

DB = sys.argv[1]
ti = TargetIndex(DB)
refparse.set_rambam_titles([t for t in ti.books if t.startswith("משנה תורה, הלכות")])
c = sqlite3.connect("/tmp/refan/cands.db")
parsed = set((l, r) for l, r in c.execute("select lineId, raw from res"))
WORD = re.compile(r"[א-ת]+")
STOP = {"וגו", "וגומר", "וכו", "כו", "וכתיב", "שנאמר", "דכתיב", "כתיב", "ואומר", "כמו", "וכן", "הוא", "זה", "אמר"}


def words(s):
    return WORD.findall(NIKUD.sub("", TAG.sub(" ", s).replace("־", " ")).translate(FINALS))


def qscore(before, after, lid):
    vw = set(words(ti.db.execute("select content from line where id=?", (lid,)).fetchone()[0]))
    best = None
    for q in (words(before)[-6:-2], words(after)[:4]):
        q = [w for w in q if len(w) > 1 and w not in STOP]
        if len(q) >= 2:
            s = sum(w in vw for w in q) / len(q)
            best = s if best is None else max(best, s)
    return best


stats = collections.Counter(); samples = collections.defaultdict(list)
c.executescript("drop table if exists lb; create table lb(lineId INT, raw TEXT, before TEXT, after TEXT, fam TEXT, title TEXT, secs TEXT, targetLine INT);")
lbbuf = []
for lid, bid, raw, before, after in c.execute("select lineId, bookId, raw, before, after from cand"):
    if (lid, raw) in parsed:
        continue
    body = refparse.strip_prefix(refparse.norm(raw[1:]))
    if not body or len(body.split()) > 6 or body.startswith("שם"):
        continue
    ws = before.translate(QUOTES).rstrip().split()[-3:]
    if not ws or before.rstrip() != before.rstrip(" ") or before.endswith(("(", ")", ".", ":", ",")):
        pass
    hit = None
    for k in (3, 2, 1):
        if len(ws) < k:
            continue
        cand = ws[-k:]
        for strip in ("", "ב", "ו", "וב", "ד", "ל", "מ", "כ", "וכ", "שב"):
            if not cand[0].startswith(strip) or len(cand[0]) <= len(strip) + 1:
                continue
            name = " ".join([cand[0][len(strip):]] + cand[1:]).rstrip(",:")
            p = refparse.parse("(" + name + " " + body)
            if p and p.get("status") not in ("name_only_or_bad_loc", "book_only") and p.get("alias") == name and not p.get("trailing"):
                hit = p; break
        if hit:
            break
    if not hit:
        continue
    fam = hit["fam"]
    stats[f"{fam}"] += 1
    r = ti.lookup(hit["title"], hit["secs"]) if hit.get("secs") else None
    stats[f"{fam}/resolved"] += bool(r)
    if r:
        lbbuf.append((lid, raw, before, after, fam, hit["title"], json.dumps(hit["secs"], ensure_ascii=False), r[0]))
    if r and fam == "tanakh" and hit["status"] == "verse":
        s = qscore(before, after, r[0])
        if s is not None:
            stats["tanakh/quoted"] += 1; stats["tanakh/quote_ok"] += s >= 0.5
    if len(samples[fam]) < 2000:
        samples[fam].append((before[-25:], raw, hit["title"], hit.get("secs")))
c.executemany("insert into lb values(?,?,?,?,?,?,?,?)", lbbuf); c.commit()
for k in sorted(stats):
    print(f"{k:30} {stats[k]}")
random.seed(2)
for k, v in samples.items():
    print("==", k)
    for s in random.sample(v, min(8, len(v))):
        print("   ", s)
