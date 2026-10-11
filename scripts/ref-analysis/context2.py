"""(דף X) without tractate: lookbehind on the preceding words (tractate alias / perek name / שם),
then base tractate, then last cited. Plus Zohar volume+daf via alt_toc."""
import sqlite3, sys, re, json, collections, random
from heb import TargetIndex, name_key, QUOTES
import refparse

DB = sys.argv[1]
ti = TargetIndex(DB)
c = sqlite3.connect("/tmp/refan/cands.db")
titles = {bid: t for t, bid in ti.books.items()}

# Bavli perek names -> tractate (from alt_toc 'Chapters')
perek = collections.defaultdict(set)
for name, title in ti.db.execute("""select t.text, b.title from alt_toc_entry e join alt_toc_structure s on s.id=e.structureId
        join book b on b.id=s.bookId join tocText t on t.id=e.textId where s.key='Chapters'"""):
    if title in refparse.BAVLI:
        perek[name_key(name.strip())].add(title)
print("perek names", len(perek), "ambiguous", sum(len(v) > 1 for v in perek.values()))

tract_alias = {}
for canon, al in refparse.TRACTATES.items():
    if canon in refparse.BAVLI:
        for a in al:
            tract_alias[name_key(a)] = canon
TRACT_RE = re.compile(r"על (?:מסכת )?(" + "|".join(sorted(refparse.BAVLI, key=len, reverse=True)) + r")$")


def lookbehind(before):
    ws = before.translate(QUOTES).replace(",", " ").split()[-4:]
    # 'בפרק מרובה' / 'פ' המוציא'
    for i in range(len(ws)):
        w = ws[i]
        if w.lstrip("וב") in ("פרק", "פ'", "פר'") and i + 1 < len(ws):
            for k in range(len(ws) - i - 1, 0, -1):
                key = name_key(" ".join(ws[i + 1:i + 1 + k]))
                if key in perek and len(perek[key]) == 1:
                    return next(iter(perek[key])), "perek_name"
    for k in (2, 1):
        if len(ws) < k:
            continue
        cand = ws[-k:]
        for strip in ("", "ב", "ו", "וב", "ד", "מ", "ל"):
            if cand[0].startswith(strip) and len(cand[0]) > len(strip) + 1:
                key = name_key(" ".join([cand[0][len(strip):]] + cand[1:]))
                if key in tract_alias:
                    return tract_alias[key], "lookbehind_name"
    return None, None


stats = collections.Counter(); samples = collections.defaultdict(list)
explicit = {}
for lid, raw, fam, title in c.execute("select lineId, raw, fam, title from res where fam in ('bavli','rashi_bavli','tosafot')"):
    explicit[(lid, raw)] = title.replace('רש"י על ', "").replace("תוספות על ", "") if title else None
last = None; last_book = None
for lid, bid, raw, before, after in c.execute("select lineId, bookId, raw, before, after from cand order by bookId, lineId, pos"):
    if bid != last_book:
        last, last_book = None, bid
    if (lid, raw) in explicit:
        last = (explicit[(lid, raw)], lid); continue
    body = refparse.strip_prefix(refparse.norm(raw[1:]))
    if not re.match(r"^(?:דף|ד') ", body):
        continue
    d = refparse.loc_daf(body.split(" ד\"ה")[0].split(" ובד\"ה")[0].strip(" ,."))
    if not d:
        stats["unparsed"] += 1; samples["unparsed"].append(raw); continue
    tr, how = lookbehind(before)
    if not tr and before.rstrip().endswith("שם") and last:
        tr, how = last[0], "lookbehind_sham"
    if not tr:
        m = TRACT_RE.search(titles.get(bid, ""))
        if m:
            tr, how = m.group(1), "base_tractate"
    if not tr and last and last[1] == lid:
        tr, how = last[0], "last_cited_same_line"
    if not tr and last:
        tr, how = last[0], "last_cited_prev_lines"
    if not tr:
        stats["no_context"] += 1; continue
    stats[how] += 1
    r = ti.lookup(tr, [f"{d[0]}{d[1]}"])
    stats[how + "/resolved"] += bool(r)
    if len(samples[how]) < 4000:
        samples[how].append((titles.get(bid), before[-30:], raw, tr))

for k in sorted(stats):
    print(f"{k:40} {stats[k]}")
random.seed(5)
for k in ("base_tractate", "last_cited_prev_lines", "lookbehind_name", "perek_name"):
    print("==", k)
    for s in random.sample(samples[k], min(12, len(samples[k]))):
        print("   ", s)

# ---------- Zohar volume + daf via alt_toc ----------
zohar = {}
for vol_text, daf_text, lid in ti.db.execute("""
    with recursive t(id, vol, lvl) as (
      select e.id, tt.text, 0 from alt_toc_entry e join tocText tt on tt.id=e.textId where e.structureId=369 and e.level=0
      union all select e.id, t.vol, e.level from alt_toc_entry e join t on e.parentId=t.id)
    select t.vol, tt.text, e.lineId from t join alt_toc_entry e on e.id=t.id join tocText tt on tt.id=e.textId where tt.text like 'דף %'"""):
    vol = {"כרך א": 1, "כרך ב": 2, "כרך ג": 3}.get(vol_text)
    dd = refparse.loc_daf(daf_text)
    if vol and dd and lid:
        zohar.setdefault((vol, f"{dd[0]}{dd[1]}"), lid)
print("zohar daf keys", len(zohar))
zs = collections.Counter()
for raw, in c.execute("select raw from res where fam='zohar_daf' and status='zohar_daf'"):
    p = refparse.parse(raw)
    zs["total"] += 1
    zs["resolved"] += (p.get("vol"), p.get("daf")) in zohar
print(zs)
