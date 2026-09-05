#!/usr/bin/env python3
import bisect, gzip, json, os, pathlib, shutil, sys, tempfile, time
import requests
import fsspec
import pyarrow.parquet as pq

PLAN=os.environ.get("BULK_PLAN","bulk-plan-v5.json")
PART=int(os.environ.get("BULK_PARTITION","0"))
SUB=int(os.environ.get("BULK_ITEM_SUBPART","-1"))
SUBS=max(1,int(os.environ.get("BULK_ITEM_SUBPARTS","1")))
OUT_DIR=pathlib.Path(os.environ.get("BULK_OUT_DIR","bulk-v5-out"))
OUT_DIR.mkdir(parents=True,exist_ok=True)
SUFFIX=(f"-s{SUB:02d}-of-{SUBS:02d}" if SUB>=0 else "")
RAW=OUT_DIR/f"raw-git-b2-bulk-p{PART:02d}{SUFFIX}.ndjson.gz"
UNIT=OUT_DIR/f"units-git-b2-bulk-p{PART:02d}{SUFFIX}.json"
DATASET="mvaccargiu/gitskills"

with open(PLAN,"r",encoding="utf-8") as f: plan=json.load(f)
part=next((x for x in plan["partitions"] if int(x["partition"])==PART),None)
if part is None: raise SystemExit(f"partition {PART} missing")
packs=part["packs"]
if not packs:
    RAW.write_bytes(gzip.compress(b""))
    UNIT.write_text(json.dumps({"partition":PART,"packs":[]},indent=2)+"\n")
    print(json.dumps({"event":"bulk_extract_empty","partition":PART}))
    raise SystemExit(0)

def get_json(url,n=6):
    last=None
    for i in range(n):
        try:
            r=requests.get(url,timeout=60,headers={"user-agent":"skillset-final-audit-bulk/5.0"})
            if r.status_code==200:return r.json()
            last=RuntimeError(f"{r.status_code}: {r.text[:200]}")
        except Exception as e:last=e
        time.sleep(min(2**i,20))
    raise last

catalog=get_json(f"https://datasets-server.huggingface.co/parquet?dataset={DATASET}")
files=[x for x in catalog.get("parquet_files",[]) if x.get("config")=="artifacts" and x.get("split")=="train"]
files.sort(key=lambda x:x["filename"])
if len(files)<1: raise RuntimeError("no artifacts parquet files")

# Read only parquet footers over HTTP to map global row offsets to physical files.
http=fsspec.filesystem("https",block_size=1024*1024)
cursor=0
mapped=[]
for x in files:
    with http.open(x["url"],"rb") as fh:
        pf=pq.ParquetFile(fh)
        n=pf.metadata.num_rows
    mapped.append({**x,"start":cursor,"end":cursor+n})
    cursor+=n

intervals=[]
pack_by_source={}
for p in packs:
    pack_by_source[p["sourceKey"]]=p
    for r in p["ranges"]:
        intervals.append((int(r["start"]),int(r["end"]),p["sourceKey"]))
intervals.sort()
starts=[x[0] for x in intervals]

def owner(gidx):
    i=bisect.bisect_right(starts,gidx)-1
    if i>=0 and intervals[i][0] <= gidx < intervals[i][1]:
        return intervals[i][2]
    return None

needed_files=[]
minrow=min(p["minRow"] for p in packs)
maxrow=max(p["maxRow"] for p in packs)
for x in mapped:
    if x["end"]<=minrow or x["start"]>maxrow: continue
    # exact overlap with at least one target interval
    if any(a < x["end"] and b > x["start"] for a,b,_ in intervals):
        needed_files.append(x)

skills={p["sourceKey"]:0 for p in packs}
selected_rows={p["sourceKey"]:0 for p in packs}
tmpdir=pathlib.Path(tempfile.mkdtemp(prefix=f"gitskills-bulk-{PART}-"))

def download(url,dest,n=8):
    last=None
    for i in range(n):
        try:
            with requests.get(url,stream=True,timeout=(30,300),headers={"user-agent":"skillset-final-audit-bulk/5.0"}) as r:
                if r.status_code!=200:
                    raise RuntimeError(f"download {r.status_code}: {r.text[:200]}")
                with open(dest,"wb") as f:
                    for chunk in r.iter_content(chunk_size=4*1024*1024):
                        if chunk:f.write(chunk)
            return
        except Exception as e:
            last=e
            time.sleep(min(2**i,30))
    raise last

with gzip.open(RAW,"wt",encoding="utf-8",compresslevel=5) as out:
    for fi,x in enumerate(needed_files,1):
        local=tmpdir/x["filename"]
        print(json.dumps({"event":"parquet_download","partition":PART,"file":x["filename"],"size":x.get("size"),"fileIndex":fi,"files":len(needed_files)}),flush=True)
        download(x["url"],local)
        pf=pq.ParquetFile(local)
        names=set(pf.schema_arrow.names)
        cols=[c for c in ["dedup_primary","content","repo_full_name","path","filename"] if c in names]
        if "dedup_primary" not in cols or "content" not in cols:
            raise RuntimeError(f"required columns missing in {x['filename']}: {sorted(names)}")
        rg_global=x["start"]
        for rg in range(pf.num_row_groups):
            rg_n=pf.metadata.row_group(rg).num_rows
            rg_start=rg_global
            rg_end=rg_start+rg_n
            rg_global=rg_end
            if not any(a < rg_end and b > rg_start for a,b,_ in intervals):
                continue
            local_off=0
            for batch in pf.iter_batches(row_groups=[rg],columns=cols,batch_size=1024):
                rows=batch.to_pylist()
                for j,row in enumerate(rows):
                    gidx=rg_start+local_off+j
                    sk=owner(gidx)
                    if sk is None: continue
                    if SUB>=0 and (gidx % SUBS)!=SUB: continue
                    selected_rows[sk]+=1
                    dp=row.get("dedup_primary")
                    if not (dp is True or dp==1): continue
                    text=row.get("content")
                    if text is None or str(text)=="": continue
                    text=str(text)
                    repo=str(row.get("repo_full_name") or "")
                    pth=str(row.get("path") or row.get("filename") or "")
                    item={"sourceKey":sk,"itemKey":str(gidx),"text":text,"repo":repo,"path":pth,"locator":f"hf-parquet://{DATASET}/artifacts/{gidx}"}
                    out.write(json.dumps(item,ensure_ascii=False,separators=(",",":"))+"\n")
                    skills[sk]+=1
                local_off+=len(rows)
        local.unlink(missing_ok=True)

# Fail closed if any planned global row range was not actually traversed.
units=[]
bad=[]
def sub_expected(p):
    if SUB<0: return int(p["expectedRows"])
    n=0
    for rr in p["ranges"]:
        a,b=int(rr["start"]),int(rr["end"])
        first=a+((SUB-(a%SUBS))%SUBS)
        if first<b:n+=1+((b-1-first)//SUBS)
    return n

for p in packs:
    expected=sub_expected(p)
    got=selected_rows[p["sourceKey"]]
    if got!=expected:
        bad.append({"sourceKey":p["sourceKey"],"expectedRows":expected,"selectedRows":got})
    ok=(got==expected)
    status=("partial" if SUB>=0 and ok else ("done" if ok else "error"))
    units.append({"source_key":p["sourceKey"],"source_system":"gitskills-b2","status":status,"rows_scanned":got,"skills_indexed":skills[p["sourceKey"]],"expected_rows":expected})
shutil.rmtree(tmpdir,ignore_errors=True)
summary={"partition":PART,"subpart":SUB,"subparts":SUBS,"packs":units,"parquetFiles":[x["filename"] for x in needed_files],"selectedRows":sum(selected_rows.values()),"skills":sum(skills.values()),"bad":bad}
UNIT.write_text(json.dumps(summary,indent=2)+"\n",encoding="utf-8")
print(json.dumps({"event":"bulk_extract_complete","partition":PART,"packs":len(packs),"files":len(needed_files),"selectedRows":summary["selectedRows"],"skills":summary["skills"],"bad":len(bad),"badDetails":bad[:20]}),flush=True)
if bad: raise SystemExit(3)
