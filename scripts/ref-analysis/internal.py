"""Internal refs (לעיל/לקמן/להלן/ע"ל + location) resolved inside the source book; otzaria:// links; single-letter groups."""
import sqlite3, sys, re, collections, random, urllib.parse
from heb import TargetIndex, gem
import refparse
DB = sys.argv[1]
ti = TargetIndex(DB)
c = sqlite3.connect("/tmp/refan/cands.db")
titles = {b: t for t, b in ti.books.items()}
st = collections.Counter(); smp = collections.defaultdict(list)
for lid, bid, raw in c.execute("select lineId, bookId, raw from cand"):
    body = refparse.norm(raw[1:])
    m = re.match(r"^(?:ועיין |עיין |ועי' |עי' |וע' |ע' |ראה |וראה )?(לעיל|לקמן|להלן|ע\"ל|למעלה|למטה)\b[,:]?\s*(.*)$", body)
    if not m:
        continue
    rest = m.group(2).strip(" ,.")
    if not rest:
        st["internal/bare"] += 1; continue
    # explicit other book after לעיל/לקמן ('לקמן גיטין דף יז.')
    p = refparse.parse("(" + rest)
    if p and p.get("status") not in ("name_only_or_bad_loc",):
        st["internal/names_other_book"] += 1; continue
    title = titles.get(bid)
    _, idx = ti.get(title)
    if not idx:
        st["internal/source_book_has_no_heref"] += 1; continue
    # try: siman/seif, daf, chapter/verse, plain number(s), 'אות X', 'סעיף X'
    r = re.sub(r"^(?:סי'|סימן|ס'|אות|סעיף|סעי'|פרק|פ'|דף|ד')\s*", "", rest)
    toks = [t for t in re.split(r"[\s,]+", r) if t]
    secs = []
    for t in toks[:3]:
        if t in ('ס"ק', 'סק', "אות", "סעיף", "סעי'", "הלכה", "ס\"", "משנה"):
            continue
        d = refparse.loc_daf(t)
        if d and d[2]:
            secs.append(f"{d[0]}{d[1]}"); continue
        mn = refparse.marker_num(t, "סמהפא")
        g = mn or gem(t)
        if g:
            secs.append(g)
        else:
            break
    if not secs:
        st["internal/unparsed"] += 1; smp["unparsed"].append(raw); continue
    hit = idx.get(tuple(secs)) or idx.get(tuple(secs[:1]))
    st["internal/parsed"] += 1
    st["internal/resolved_in_self"] += bool(hit)
    if not hit:
        smp["unresolved"].append((title, raw, secs))
for k in sorted(st): print(f"{k:45} {st[k]}")
random.seed(4)
for k, v in smp.items():
    print("==", k); [print("   ", x) for x in random.sample(v, min(10, len(v)))]

# ---------- otzaria:// inline links ----------
print("\n== otzaria inline links")
ok = tot = 0; ex = []
for (tag,) in c.execute("select tag from anchor where tag like '%otzaria://inline-link%'"):
    href = re.search(r'href="([^"]+)"', tag)
    if not href: continue
    q = urllib.parse.parse_qs(urllib.parse.urlparse(href.group(1)).query)
    path = q.get("path", [""])[0]; idx = int(q.get("index", ["-1"])[0])
    t = path.rsplit("/", 1)[-1].removesuffix(".txt")
    bid = ti.books.get(t)
    tot += 1
    if bid is not None:
        row = ti.db.execute("select count(*) from line where bookId=? and lineIndex=?", (bid, idx - 1)).fetchone()[0]
        ok += row > 0
    elif len(ex) < 5: ex.append(t)
print("total", tot, "book found & line exists", ok, "missing titles", ex)

# ---------- single-letter groups ----------
print("\n== single-letter groups by book (top)")
cnt = collections.Counter()
for bid, n in c.execute("select bookId, count(*) from cand where length(raw) <= 5 and raw not like '% %' group by bookId order by 2 desc limit 15"):
    print("   ", n, titles.get(bid))
