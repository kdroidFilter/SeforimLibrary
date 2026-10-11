"""Agreement between parsed citations and the existing `link` table (Sefaria auto-linker + Otzaria links)."""
import sqlite3, sys, random, collections, json

DB = sys.argv[1]
src = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
c = sqlite3.connect("/tmp/refan/cands.db")
random.seed(7)


def section_prefix(heref):
    # drop the last comma-separated token: 'שבת כא:, ה' -> 'שבת כא:' ; 'בראשית א, ד' -> 'בראשית א'
    return heref.rsplit(",", 1)[0].strip() if heref and "," in heref else heref


groups = c.execute("select fam||'/'||status, count(*) from res where resolved=1 group by 1 having count(*)>200").fetchall()
print(f"{'family':30} {'sample':>6} {'link→book':>10} {'same-sect|book':>15} {'exact|book':>11}")
for g, _ in groups:
    rows = c.execute("select lineId, targetBook, targetLine from res where fam||'/'||status=? and resolved=1", (g,)).fetchall()
    sample = random.sample(rows, min(3000, len(rows)))
    has_book = same_sec = exact = 0
    for lid, tb, tl in sample:
        tgt = src.execute("""select targetLineId from link where sourceLineId=? and targetBookId=?
                             union select sourceLineId from link where targetLineId=? and sourceBookId=?""",
                          (lid, tb, lid, tb)).fetchall()
        if not tgt:
            continue
        has_book += 1
        ids = [t[0] for t in tgt]
        if tl in ids:
            exact += 1; same_sec += 1; continue
        want = section_prefix(src.execute("select heRef from line where id=?", (tl,)).fetchone()[0])
        hs = [r[0] for r in src.execute(f"select heRef from line where id in ({','.join('?'*len(ids))})", ids)]
        if any(h and (h == want or h.startswith(want + ",") or section_prefix(h) == want) for h in hs):
            same_sec += 1
    hb = max(has_book, 1)
    print(f"{g:30} {len(sample):>6} {100*has_book/len(sample):>9.1f}% {100*same_sec/hb:>14.1f}% {100*exact/hb:>10.1f}%")
