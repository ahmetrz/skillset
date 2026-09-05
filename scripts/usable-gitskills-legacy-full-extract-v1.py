#!/usr/bin/env python3
import gzip, json, os, pathlib, tempfile, time, hashlib
import requests, fsspec
import pyarrow.parquet as pq

PART=int(os.environ.get("LEGACY_PARTITION","0"))
PARTS=max(1,int(os.environ.get("LEGACY_PARTITIONS","8")))
OUT=pathlib.Path(os.environ.get("LEGACY_OUT_DIR","usable-gitskills-legacy"))
OUT.mkdir(parents=True,exist_ok=True)
TOTAL=19132*100
LO=(PART*TOTAL)//PARTS
HI=((PART+1)*TOTAL)//PARTS
DATASET="mvaccargiu/gitskills"

def get_json(url,n=6):
    last=None
    for i in range(n):
        try:
            r=requests.get(url,timeout=60,headers={"user-agent":"skillset-usable-legacy/1.0"})
            if r.status_code==200:return r.json()
            last=RuntimeError(f"{r.status_code}: {r.text[:200]}")
        except Exception as e:last=e
        time.sleep(min(2**i,20))
    raise last

catalog=get_json(f"https://datasets-server.huggingface.co/parquet?dataset={DATASET}")
files=[x for x in catalog.get("parquet_files",[]) if x.get("config")=="artifacts" and x.get("split")=="train"]
files.sort(key=lambda x:x["filename"])
http=fsspec.filesystem("https",block_size=1024*1024)
cursor=0;mapped=[]
for x in files:
    with http.open(x["url"],"rb") as fh:
        pf=pq.ParquetFile(fh);n=pf.metadata.num_rows
    mapped.append({**x,"start":cursor,"end":cursor+n});cursor+=n
if cursor<TOTAL: raise RuntimeError(f"dataset_too_short:{cursor}<{TOTAL}")

tmpdir=pathlib.Path(tempfile.mkdtemp(prefix=f"usable-legacy-{PART}-"))
raw=OUT/f"raw-gitskills-legacy-full-p{PART:02d}-of-{PARTS:02d}.ndjson.gz"
manifest=OUT/f"manifest-gitskills-legacy-full-p{PART:02d}-of-{PARTS:02d}.json"
selected=0;skills=0;units={}
def download(url,dest,n=6):
    last=None
    for i in range(n):
        try:
            with requests.get(url,stream=True,timeout=(30,300),headers={"user-agent":"skillset-usable-legacy/1.0"}) as r:
                if r.status_code!=200: raise RuntimeError(f"download {r.status_code}")
                with open(dest,"wb") as f:
                    for ch in r.iter_content(4*1024*1024):
                        if ch:f.write(ch)
            return
        except Exception as e:last=e;time.sleep(min(2**i,20))
    raise last

with gzip.open(raw,"wt",encoding="utf-8",compresslevel=5) as out:
    for x in mapped:
        if x["end"]<=LO or x["start"]>=HI: continue
        local=tmpdir/x["filename"];download(x["url"],local)
        pf=pq.ParquetFile(local);names=set(pf.schema_arrow.names)
        cols=[c for c in ["dedup_primary","content","repo_full_name","path","filename"] if c in names]
        base=x["start"]
        for rg in range(pf.num_row_groups):
            n=pf.metadata.row_group(rg).num_rows;rg_lo=base;base+=n;rg_hi=base
            if rg_hi<=LO or rg_lo>=HI: continue
            off=0
            for batch in pf.iter_batches(row_groups=[rg],columns=cols,batch_size=2048):
                rows=batch.to_pylist()
                for j,row in enumerate(rows):
                    gidx=rg_lo+off+j
                    if gidx<LO or gidx>=HI: continue
                    selected+=1
                    shard=100000+(gidx//100);sk=f"hf:gitskills:{shard}"
                    units.setdefault(sk,0)
                    dp=row.get("dedup_primary")
                    if not (dp is True or dp==1): continue
                    text=row.get("content")
                    if text is None or str(text)=="": continue
                    text=str(text);skills+=1;units[sk]+=1
                    rec={"sourceSystem":"gitskills-legacy-hf","sourceKey":sk,"itemKey":str(gidx),"text":text,"repo":str(row.get("repo_full_name") or ""),"path":str(row.get("path") or row.get("filename") or ""),"locator":f"hf-parquet://{DATASET}/artifacts/{gidx}"}
                    out.write(json.dumps(rec,ensure_ascii=False,separators=(",",":"))+"\n")
                off+=len(rows)
        local.unlink(missing_ok=True)

expected_raw=HI-LO
if selected!=expected_raw: raise RuntimeError(f"selected_rows_{selected}_expected_{expected_raw}")
# Include zero-skill shards in manifest.
first_shard=100000+(LO//100);last_shard=100000+((HI-1)//100)
for sh in range(first_shard,last_shard+1): units.setdefault(f"hf:gitskills:{sh}",0)
m={"generatedAt":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),"partition":PART,"partitions":PARTS,"lo":LO,"hi":HI,"selectedRawRows":selected,"skills":skills,"units":len(units),"unitSkillCounts":units}
manifest.write_text(json.dumps(m,indent=2)+"\n",encoding="utf-8")
print(json.dumps({"event":"legacy_full_extract","partition":PART,"selectedRawRows":selected,"skills":skills,"units":len(units)},indent=2))
