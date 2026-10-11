"""Quote check for Bavli citations: words around the citation vs. the words of the target daf (both amudim)."""
import sqlite3, sys, re, random, collections
from heb import NIKUD, TAG, FINALS
DB = sys.argv[1]
src = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
c = sqlite3.connect("/tmp/refan/cands.db")
WORD = re.compile(r"[א-ת]+")
STOP = set("אמר רבי רב אלא הכא התם דאמר דתנן תניא איתא אמרינן דאמרינן שם וכו כו גמרא בגמרא ד\"ה עיין ועיין הוא היא זה לא על את של".split())
def words(s): return WORD.findall(NIKUD.sub("", TAG.sub(" ", s)).translate(FINALS))
dafcache = {}
def daf_words(tl):
    bid, h = src.execute("select bookId, heRef from line where id=?", (tl,)).fetchone()
    key = (bid, h.split(",")[0].rstrip(".:"))
    if key not in dafcache:
        pref = key[1]
        ws = set()
        for (ct,) in src.execute("select content from line where bookId=? and (heRef like ? or heRef like ?)", (bid, pref + ".,%", pref + ":,%")):
            ws.update(words(ct))
        dafcache[key] = ws
    return dafcache[key]
def score(before, after, ws):
    best = None
    for q in (words(before)[-5:], words(after)[:5]):
        q = [w for w in q if len(w) > 2 and w not in STOP]
        if len(q) >= 3:
            s = sum(w in ws for w in q) / len(q); best = s if best is None else max(best, s)
    return best
random.seed(11)
alltl = [r[0] for r in c.execute("select targetLine from res where fam='bavli' and resolved=1 limit 200000")]
sets = {
  "explicit daf (amud given)": "select c.before, c.after, r.targetLine from res r join cand c on c.lineId=r.lineId and c.raw=r.raw and c.after=r.after where r.fam='bavli' and r.status='daf' and r.resolved=1",
  "explicit daf (no amud)": "select c.before, c.after, r.targetLine from res r join cand c on c.lineId=r.lineId and c.raw=r.raw and c.after=r.after where r.fam='bavli' and r.status='daf_no_amud' and r.resolved=1",
  "lookbehind tractate": "select before, after, targetLine from lb where fam='bavli'",
}
for name, q in sets.items():
    rows = random.sample(c.execute(q).fetchall(), 2500)
    st = collections.Counter()
    for be, af, tl in rows:
        s = score(be, af, daf_words(tl))
        ctrl = score(be, af, daf_words(random.choice(alltl)))
        if s is None: continue
        st["n"] += 1; st["ok"] += s >= 0.6; st["ctrl_ok"] += (ctrl or 0) >= 0.6
    print(f"{name:28} quoted={st['n']:5}  match={100*st['ok']/st['n']:.1f}%  random-daf control={100*st['ctrl_ok']/st['n']:.1f}%")
