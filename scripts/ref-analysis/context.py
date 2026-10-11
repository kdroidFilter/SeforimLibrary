"""Context-dependent citations: (שם ...), (דף X) without tractate, (סי' X) without book.
Resolves them against the previous explicit citation / the source book, and validates."""
import sqlite3, sys, re, json, collections, random
from heb import TargetIndex, gem, NIKUD, TAG, FINALS, QUOTES
import refparse

DB = sys.argv[1]
ti = TargetIndex(DB)
c = sqlite3.connect("/tmp/refan/cands.db")
titles = {bid: t for t, bid in ti.books.items()}
WORD = re.compile(r"[א-ת]+")
STOP = {"וגו", "וגומר", "וכו", "כו", "וכתיב", "שנאמר", "דכתיב", "כתיב", "ואומר", "כמו", "וכן", "הוא", "זה", "אמר"}


def words(s):
    s = TAG.sub(" ", s).replace("־", " ")
    return WORD.findall(NIKUD.sub("", s).translate(FINALS))


vcache = {}


def vwords(lid):
    if lid not in vcache:
        r = ti.db.execute("select content from line where id=?", (lid,)).fetchone()
        vcache[lid] = set(words(r[0])) if r else set()
    return vcache[lid]


def qscore(before, after, lid):
    best = None
    for q in (words(before)[-4:], words(after)[:4]):
        q = [w for w in q if len(w) > 1 and w not in STOP]
        if len(q) < 2:
            continue
        s = sum(w in vwords(lid) for w in q) / len(q)
        best = s if best is None else max(best, s)
    return best


# explicit citations already resolved, keyed by (lineId, raw)
explicit = {}
for lid, raw, fam, title, secs, tl in c.execute(
        "select lineId, raw, fam, title, secs, targetLine from res"):
    explicit[(lid, raw)] = (fam, title, json.loads(secs) if secs else [], tl)
linkdb = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)


def link_agrees(lid, tb, tl):
    """None if no link to that book from this line; else whether a link hits the same daf/chapter."""
    tgt = [r[0] for r in linkdb.execute("select targetLineId from link where sourceLineId=? and targetBookId=? union select sourceLineId from link where targetLineId=? and sourceBookId=?", (lid, tb, lid, tb))]
    if not tgt:
        return None
    want = linkdb.execute("select heRef from line where id=?", (tl,)).fetchone()[0].split(",")[0].rstrip(".:")
    hs = [r[0] for r in linkdb.execute(f"select heRef from line where id in ({','.join('?'*len(tgt))})", tgt)]
    return any(h and h.split(",")[0].rstrip(".:") == want for h in hs)

TRACT_RE = re.compile(r"על (?:מסכת )?(" + "|".join(sorted(refparse.BAVLI, key=len, reverse=True)) + r")$")


def base_tractate(book_title):
    if book_title in refparse.BAVLI:
        return book_title
    m = TRACT_RE.search(book_title or "")
    return m.group(1) if m else None


stats = collections.Counter()
samples = collections.defaultdict(list)
last = None; last_book = None; last_line_gap = 0; prev_lid = None
rows = c.execute("select c.lineId, c.bookId, c.raw, c.before, c.after from cand c order by c.bookId, c.lineId, c.pos")
for lid, bid, raw, before, after in rows:
    if bid != last_book:
        last = None; last_book = bid; gap = 0
    body = refparse.norm(raw[1:])
    body0 = refparse.strip_prefix(body)
    key = (lid, raw)
    if key in explicit:
        last = (explicit[key], lid)
        continue
    # ---------- ibid ----------
    if body0 == "שם" or body0.startswith("שם ") or body0.startswith("שם,"):
        rest = body0[2:].strip(" ,")
        if last is None:
            stats["ibid/no_previous"] += 1; continue
        (fam, title, secs, tl), llid = last
        dist = "same_line" if llid == lid else "prev_lines"
        if tl is None and fam not in ("bavli",):
            stats[f"ibid/prev_unresolved"] += 1; continue
        if fam == "tanakh":
            if not rest:
                s = qscore(before, after, tl)
                stats[f"ibid/tanakh/bare/{dist}"] += 1
                if s is not None:
                    stats["ibid/tanakh/bare/quoted"] += 1
                    stats["ibid/tanakh/bare/quote_matches_prev_verse"] += s >= 0.5
                continue
            cv = refparse.loc_chapter_verse(rest)
            if cv and cv[1] is not None:
                r = ti.lookup(title, [cv[0], cv[1]])
                stats["ibid/tanakh/chap_verse"] += 1
                if r:
                    stats["ibid/tanakh/chap_verse/resolved"] += 1
                    s = qscore(before, after, r[0])
                    if s is not None:
                        stats[f"ibid/tanakh/chap_verse/quoted/{dist}"] += 1
                        stats[f"ibid/tanakh/chap_verse/quote_ok/{dist}"] += s >= 0.5
                        if s < 0.5 and len(samples["ibid_cv_bad"]) < 3000:
                            samples["ibid_cv_bad"].append((title, secs, raw, before[-30:], after[:30]))
                continue
            if cv and cv[1] is None:
                # 'שם N' : chapter N or verse N of the same chapter ?
                stats["ibid/tanakh/single_num"] += 1
                a = ti.lookup(title, [cv[0]])            # chapter N, verse 1 (prefix)
                b = ti.lookup(title, [secs[0], cv[0]]) if secs else None  # verse N same chapter
                if b:
                    s = qscore(before, after, b[0])
                    if s is not None:
                        stats["ibid/tanakh/single_num/quoted"] += 1
                        stats["ibid/tanakh/single_num/as_verse_same_chapter_ok"] += s >= 0.5
                if a:
                    # any verse of chapter N matching the quote?
                    bid_t, idx = ti.get(title)
                    k = 1; hit = False
                    while (cv[0], k) in idx and k < 200:
                        s = qscore(before, after, idx[(cv[0], k)][0])
                        if s is not None and s >= 0.75:
                            hit = True; break
                        k += 1
                    stats["ibid/tanakh/single_num/as_chapter_quote_found"] += hit
                continue
            stats["ibid/tanakh/other"] += 1
            samples["ibid/tanakh/other"].append(raw)
            continue
        if fam == "bavli":
            if not rest:
                stats[f"ibid/bavli/bare/{dist}"] += 1; continue
            d = refparse.loc_daf(rest)
            if d:
                stats["ibid/bavli/daf"] += 1
                r = ti.lookup(title, [f"{d[0]}{d[1]}"])
                stats["ibid/bavli/daf/resolved"] += bool(r)
                if r and random.random() < 0.2:
                    ag = link_agrees(lid, ti.books[title], r[0])
                    if ag is not None:
                        stats["ibid/bavli/daf/linkcheck"] += 1
                        stats["ibid/bavli/daf/linkcheck_ok"] += ag
                continue
            stats["ibid/bavli/other"] += 1; samples["ibid/bavli/other"].append(raw); continue
        stats[f"ibid/{fam}/{'bare' if not rest else 'with_loc'}"] += 1
        if rest:
            samples[f"ibid/{fam}/with_loc"].append(raw)
        continue
    # ---------- folio without tractate ----------
    if re.match(r"^(?:דף|ד') ", body0) or re.fullmatch(r"(?:[א-ת]{1,3}['\"]?[א-ת]?)\s*[.:]", body0) or re.fullmatch(r"(?:[א-ת]{1,3}['\"]?[א-ת]?)\s*ע\"[אב]", body0):
        d = refparse.loc_daf(body0.split(" ד\"ה")[0].strip())
        if not d:
            stats["daf_only/unparsed"] += 1; samples["daf_only/unparsed"].append(raw); continue
        bt = base_tractate(titles.get(bid))
        how = "base_tractate" if bt else None
        if not bt and last and last[0][0] in ("bavli", "rashi_bavli", "tosafot"):
            bt = last[0][1].replace('רש"י על ', "").replace("תוספות על ", ""); how = "last_cited"
        if not bt:
            stats["daf_only/no_context"] += 1
            samples["daf_only/no_context"].append((titles.get(bid), raw)); continue
        r = ti.lookup(bt, [f"{d[0]}{d[1]}"])
        stats[f"daf_only/{how}"] += 1
        stats[f"daf_only/{how}/resolved"] += bool(r)
        if r and random.random() < 0.05:
            ag = link_agrees(lid, ti.books[bt], r[0])
            if ag is not None:
                stats[f"daf_only/{how}/linkcheck"] += 1
                stats[f"daf_only/{how}/linkcheck_ok"] += ag
        continue
    # ---------- siman / seif without book ----------
    if re.match(r"^(?:סי'|סימן|ס\"ק|סעיף|סעי') ", body0):
        stats["siman_only/" + ("self_book_has_simanim" if ti.get(titles.get(bid, ""))[1].get((gem(body0.split()[1]) or -1,)) else "other")] += 1
        continue
    if re.match(r"^(?:לעיל|לקמן|להלן|למעלה|למטה|עיל|ע\"ל)\b", body0):
        stats["internal/" + body0.split()[0]] += 1
        samples["internal"].append(raw)
        continue
    if re.match(r"^(?:ד\"ה|בד\"ה)", body0):
        stats["dh_only"] += 1
        continue

for k in sorted(stats):
    print(f"{k:60} {stats[k]}")
json.dump(samples["ibid_cv_bad"], open("/tmp/refan/ibid_cv_bad.json","w"), ensure_ascii=False)
for k, v in samples.items():
    if k == "ibid_cv_bad": continue
    print("==", k, random.sample(v, min(15, len(v))))
