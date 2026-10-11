import sqlite3, sys, re, random, collections
from heb import TargetIndex, NIKUD, TAG, FINALS
DB=sys.argv[1]; ti=TargetIndex(DB); c=sqlite3.connect("/tmp/refan/cands.db")
WORD=re.compile(r"[א-ת]+")
STOP={"וגו","וגומר","וכו","כו","וכתיב","שנאמר","דכתיב","כתיב","ואומר","כמו","וכן","הוא","זה","אמר","שנא"}
def words(s): return WORD.findall(NIKUD.sub("",TAG.sub(" ",s).replace("־"," ")).translate(FINALS))
rows=c.execute("select r.title, r.secs, c.before, c.after from res r join cand c on c.lineId=r.lineId and c.raw=r.raw and c.after=r.after where r.fam='tanakh' and r.status='chapter' and r.resolved=1").fetchall()
random.seed(9); st=collections.Counter()
for title, secs, be, af in random.sample(rows, 5000):
    ch=int(secs.strip("[]")); bid, idx=ti.get(title)
    verses=[]; k=1
    while (ch,k) in idx: verses.append(idx[(ch,k)][0]); k+=1
    best=[]
    for q in (words(af)[:5], words(be)[-5:]):
        q=[w for w in q if len(w)>1 and w not in STOP]
        if len(q)<3: continue
        hits=[]
        for v in verses:
            vw=set(words(ti.db.execute("select content from line where id=?",(v,)).fetchone()[0]))
            if sum(w in vw for w in q)/len(q)>=0.75: hits.append(v)
        if hits: best=hits; break
    st["n"]+=1
    st["unique_verse" if len(best)==1 else ("multi" if best else "none")]+=1
print(st)
