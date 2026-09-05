#!/usr/bin/env python3
import gzip, hashlib, json, os, pathlib, re, sqlite3, sys, time
from collections import Counter

IN=os.environ.get("CAP_ATOMS_DIR","capability-atoms")
OUT=pathlib.Path(os.environ.get("CAP_CONSOLIDATED_OUT","usable-consolidated"))
OUT.mkdir(parents=True,exist_ok=True)
DB=OUT/"capability-index.sqlite3"

GENERIC=set("skill skills agent expert guide workflow process system tool tools use using create build implement design manage management helper utilities utility developer development engineering code coding project projects".split())
ACTION_MAP={
 "test":"test","testing":"test","validate":"test","validation":"test","verify":"test","verification":"test","qa":"test",
 "review":"review","audit":"review","inspect":"review","check":"review","analyze":"analyze","analyse":"analyze","analysis":"analyze",
 "build":"build","create":"build","implement":"build","develop":"build","generate":"build",
 "deploy":"deploy","release":"deploy","publish":"deploy","delivery":"deploy",
 "debug":"debug","fix":"debug","troubleshoot":"debug","repair":"debug",
 "plan":"design","design":"design","architect":"design","architecture":"design",
 "migrate":"migrate","migration":"migrate","upgrade":"migrate","update":"migrate",
 "monitor":"observe","observability":"observe","logging":"observe","trace":"observe","metrics":"observe",
 "document":"document","documentation":"document","docs":"document","write":"document",
 "secure":"security","security":"security","auth":"security","authentication":"security","authorization":"security",
 "research":"research","search":"research","discover":"research","discovery":"research",
 "optimize":"optimize","optimise":"optimize","performance":"optimize","refactor":"optimize",
 "automate":"automate","automation":"automate","integrate":"integrate","integration":"integrate",
 "configure":"configure","configuration":"configure","install":"configure","setup":"configure"
}
DOMAIN_PATTERNS=[
 ("frontend",{"frontend","react","vue","angular","svelte","component","css","tailwind","ui"}),
 ("backend",{"backend","server","service","microservice"}),
 ("api",{"api","rest","openapi","graphql","endpoint","webhook"}),
 ("database",{"database","sql","postgres","postgresql","mysql","sqlite","mongodb","redis","schema"}),
 ("git",{"git","github","gitlab","branch","commit","pull","request","pr"}),
 ("cicd",{"ci","cd","pipeline","actions","jenkins","circleci"}),
 ("container",{"docker","container","containers","compose"}),
 ("kubernetes",{"kubernetes","k8s","helm","kubectl"}),
 ("cloud",{"cloud","aws","azure","gcp","lambda","serverless"}),
 ("infrastructure",{"terraform","iac","infrastructure","ansible"}),
 ("security",{"security","secure","vulnerability","owasp","secret","credential","auth"}),
 ("testing",{"test","testing","pytest","jest","vitest","playwright","cypress","e2e","unit"}),
 ("observability",{"monitor","monitoring","observability","logging","metrics","tracing","sentry"}),
 ("data",{"data","dataset","etl","pipeline","analytics","warehouse","lakehouse"}),
 ("ai-agent",{"agent","agents","llm","prompt","claude","codex","openai","gemini","mcp"}),
 ("mobile",{"mobile","ios","android","swift","kotlin","flutter","react-native"}),
 ("docs",{"docs","documentation","readme","markdown","writing"}),
 ("content",{"content","copywriting","seo","social","blog","marketing","video","image","design"}),
 ("project",{"project","requirements","planning","roadmap","product","ticket","issue"})
]
WORD=re.compile(r"[a-z0-9][a-z0-9+._/-]*")
def toks(s):
    return [x for x in WORD.findall(str(s or "").lower().replace("_","-")) if len(x)>1]
def stem(x):
    for suf in ("ing","tion","ment","ness","ers","er","ed","s"):
        if len(x)>5 and x.endswith(suf): return x[:-len(suf)]
    return x
def family_signature(a):
    title=toks(a.get("title","")); desc=toks(a.get("description",""))
    allw=title+desc[:20]
    action="operate"
    for w in allw:
        if w in ACTION_MAP: action=ACTION_MAP[w]; break
    domains=[]
    aset=set(allw)
    for name,keys in DOMAIN_PATTERNS:
        if aset & keys: domains.append(name)
    domains=sorted(set(domains))[:2]
    consumed=set(GENERIC)|set(ACTION_MAP)|{y for _,ks in DOMAIN_PATTERNS for y in ks}
    quals=[]
    for w in title:
        sw=stem(w)
        if w in consumed or sw in consumed or len(sw)<3: continue
        if sw not in quals: quals.append(sw)
    keep=1 if domains else 2
    quals=sorted(quals[:keep])
    raw=action+"|"+("+".join(domains) if domains else "general")+"|"+("+".join(quals) if quals else "core")
    return hashlib.sha256(raw.encode()).hexdigest()[:24],raw,action,domains,quals
def quality(a):
    return min(8,len(a.get("actions") or []))*5+min(5,len(a.get("validate") or []))*3+min(4,len(a.get("safety") or []))*2+min(6,len(toks(a.get("description",""))))
def atom_files():
    return sorted([p for p in pathlib.Path(IN).rglob("*.ndjson.gz") if p.is_file()])

if DB.exists(): DB.unlink()
db=sqlite3.connect(DB)
db.execute("pragma journal_mode=wal");db.execute("pragma synchronous=normal");db.execute("pragma temp_store=memory")
db.executescript("""
create table content(content_hash text primary key,family_id text not null,signature text not null,atom_json text not null,quality integer not null);
create table occurrence(source_system text,source_key text,item_key text,content_hash text,family_id text);
create index occ_family_idx on occurrence(family_id);
create index content_family_idx on content(family_id);
""")
source_occ=0;parsed=0
for fp in atom_files():
    with gzip.open(fp,"rt",encoding="utf-8") as f:
        batch_c=[];batch_o=[]
        for line in f:
            if not line.strip(): continue
            try:a=json.loads(line)
            except: continue
            h=str(a.get("content_hash") or "")
            if len(h)!=64: continue
            fid,sig,act,dom,qual=family_signature(a)
            a["family_signature"]=sig;a["family_action"]=act;a["family_domains"]=dom;a["family_qualifiers"]=qual
            q=quality(a);aj=json.dumps(a,separators=(",",":"),ensure_ascii=False)
            batch_c.append((h,fid,sig,aj,q))
            batch_o.append((str(a.get("source_system","")),str(a.get("source_key","")),str(a.get("item_key","")),h,fid))
            parsed+=1;source_occ+=1
            if len(batch_c)>=5000:
                db.executemany("insert into content values(?,?,?,?,?) on conflict(content_hash) do update set atom_json=case when excluded.quality>content.quality then excluded.atom_json else content.atom_json end,quality=max(content.quality,excluded.quality),family_id=content.family_id,signature=content.signature",batch_c)
                db.executemany("insert into occurrence values(?,?,?,?,?)",batch_o);db.commit();batch_c=[];batch_o=[]
        if batch_c:
            db.executemany("insert into content values(?,?,?,?,?) on conflict(content_hash) do update set atom_json=case when excluded.quality>content.quality then excluded.atom_json else content.atom_json end,quality=max(content.quality,excluded.quality),family_id=content.family_id,signature=content.signature",batch_c)
            db.executemany("insert into occurrence values(?,?,?,?,?)",batch_o);db.commit()
    print(json.dumps({"event":"consolidate_ingest","file":fp.name,"parsed":parsed}),flush=True)

families_gz=gzip.open(OUT/"capability-families.ndjson.gz","wt",encoding="utf-8",compresslevel=6)
members_gz=gzip.open(OUT/"family-members.ndjson.gz","wt",encoding="utf-8",compresslevel=6)
coverage_gz=gzip.open(OUT/"coverage-map.ndjson.gz","wt",encoding="utf-8",compresslevel=6)
family_count=0;max_family=0;member_count=0
family_rows=db.execute("select family_id,signature,count(*) from content group by family_id,signature order by family_id")
for fid,sig,count in family_rows:
    title_c=Counter();desc_c=Counter();actions=[];valid=[];safety=[];tokens=Counter();sdlc=0;social=0;risk=0;provider=0;domains=Counter();act="operate"
    cur=db.execute("select content_hash,atom_json from content where family_id=? order by quality desc,content_hash",(fid,))
    for h,aj in cur:
        a=json.loads(aj);member_count+=1;members_gz.write(json.dumps({"content_hash":h,"family_id":fid})+"\n")
        t=str(a.get("title") or "").strip();d=str(a.get("description") or "").strip()
        if t:title_c[t]+=1
        if d:desc_c[d]+=1
        for x in a.get("tokens") or []:tokens[x]+=1
        for x in a.get("actions") or []:
            if x not in actions and len(actions)<32:actions.append(x)
        for x in a.get("validate") or []:
            if x not in valid and len(valid)<16:valid.append(x)
        for x in a.get("safety") or []:
            if x not in safety and len(safety)<12:safety.append(x)
        sdlc|=int(a.get("sdlc_mask") or 0);social|=int(a.get("social_mask") or 0);risk|=int(a.get("risk_mask") or 0);provider|=int(a.get("provider_mask") or 0)
        for x in a.get("family_domains") or []:domains[x]+=1
        act=str(a.get("family_action") or act)
    fam={"v":1,"family_id":fid,"signature":sig,"members":count,"action":act,"domains":[x for x,_ in domains.most_common(3)],
         "title":title_c.most_common(1)[0][0] if title_c else fid,
         "description":desc_c.most_common(1)[0][0] if desc_c else "",
         "tokens":[x for x,_ in tokens.most_common(24)],"actions":actions,"validate":valid,"safety":safety,
         "sdlc_mask":sdlc,"social_mask":social,"risk_mask":risk,"provider_mask":provider}
    families_gz.write(json.dumps(fam,separators=(",",":"),ensure_ascii=False)+"\n");family_count+=1;max_family=max(max_family,count)
families_gz.close();members_gz.close()

for row in db.execute("select source_system,source_key,item_key,content_hash,family_id from occurrence order by rowid"):
    coverage_gz.write(json.dumps({"source_system":row[0],"source_key":row[1],"item_key":row[2],"content_hash":row[3],"family_id":row[4]},separators=(",",":"))+"\n")
coverage_gz.close()
unique_content=db.execute("select count(*) from content").fetchone()[0]
stats={"generatedAt":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),"sourceOccurrences":source_occ,"uniqueContentHashes":unique_content,"families":family_count,"familyMembers":member_count,"maxFamilySize":max_family,"atomFiles":len(atom_files()),"policy":"preserve every occurrence and unique content hash; semantic family merge unions unique procedure/validation/safety capabilities"}
(OUT/"consolidation-stats.json").write_text(json.dumps(stats,indent=2)+"\n",encoding="utf-8")
print(json.dumps({"event":"consolidation_complete",**stats},indent=2))
db.close()
