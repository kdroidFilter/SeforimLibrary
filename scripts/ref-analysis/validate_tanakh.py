"""Precision estimate for Tanakh citations using the adjacent quote (before or after the citation)."""
import sqlite3, sys, re, random, collections, json
from heb import NIKUD, TAG, FINALS

DB = sys.argv[1]
src = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
c = sqlite3.connect("/tmp/refan/cands.db")
WORD = re.compile(r"[א-ת]+")
STOP = {"וגו", "וגומר", "וכו", "וגו'", "כו", "וכתיב", "שנאמר", "דכתיב", "כתיב", "ואומר", "שנא", "כדכתיב", "כמו", "כמש", "וכן", "הוא", "זה", "אמר"}


def words(s):
    s = TAG.sub(" ", s).replace("־", " ").replace("׀", " ")
    s = NIKUD.sub("", s).translate(FINALS)
    return [w for w in WORD.findall(s)]


# Load all Tanakh verses: (title, chap, verse) -> set(words), and per book word -> verses
tanakh_titles = [r[0] for r in src.execute(
    "select b.title from book b join category c on c.id=b.categoryId join category p on p.id=c.parentId where p.title like 'תנ%' and c.title in ('תורה','נביאים','כתובים')")]
verses = {}
line2v = {}
for t in tanakh_titles:
    for lid, h, content in src.execute("select l.id, l.heRef, l.content from line l join book b on b.id=l.bookId where b.title=? and l.heRef is not null", (t,)):
        verses[lid] = set(words(content))
        line2v[lid] = (t, h)
by_book = collections.defaultdict(list)
for lid, (t, h) in line2v.items():
    by_book[t].append(lid)
print("verses", len(verses))


def score(q, vw):
    q = [w for w in q if len(w) > 1 and w not in STOP]
    if len(q) < 2:
        return None
    return sum(1 for w in q if w in vw) / len(q)


rows = c.execute("""select r.raw, r.title, r.targetLine, c.before, c.after, r.status from res r join cand c
                    on c.lineId=r.lineId and c.raw=r.raw and c.after=r.after
                    where r.fam='tanakh' and r.status='verse' and r.resolved=1""").fetchall()
random.seed(1)
sample = random.sample(rows, 20000)
stat = collections.Counter(); bad = []
for raw, title, tl, before, after, st in sample:
    vb = words(before)[-4:]
    va = words(after)[:4]
    vw = verses.get(tl, set())
    sb, sa = score(vb, vw), score(va, vw)
    best = max([x for x in (sb, sa) if x is not None], default=None)
    if best is None:
        stat["no_quote"] += 1; continue
    if best >= 0.5:
        stat["match"] += 1; continue
    # does the quote strongly match another verse of the same book?
    alt = None
    for q in (vb, va):
        qq = [w for w in q if len(w) > 1 and w not in STOP]
        if len(qq) < 3:
            continue
        for lid in by_book[title]:
            if all(w in verses[lid] for w in qq):
                alt = lid; break
        if alt:
            break
    if alt:
        stat["mismatch_other_verse_matches"] += 1
        bad.append((raw, line2v[tl][1], line2v[alt][1], before[-40:], after[:40]))
    else:
        stat["mismatch_no_quote_found"] += 1
print(stat)
for b in bad[:40]:
    print(b)
