import { createHash } from 'node:crypto';
import { gzipSync } from 'node:zlib';

const HF_ROWS='https://datasets-server.huggingface.co/rows';
const need=n=>{const v=process.env[n];if(!v)throw new Error(`Missing ${n}`);return v};
const ingest=need('GITSKILLS_INGEST_URL');
const lane=Number(need('WORKER'));
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const expected=new Map([[135012,1674],[135108,1494],[135236,1634],[135268,1441],[135300,1561],[135332,1568],[135364,1567],[135396,1609],[135428,1521],[135460,1275],[135556,1726],[135588,1799],[135652,1568],[135684,1756],[135716,1717],[135748,1818],[135780,1764],[135812,1339],[135844,1405],[135876,1540],[135908,1730],[135940,1519],[135972,1651],[136004,1547],[136036,1531],[136068,1704],[136100,1631],[136132,null],[136164,null]]);
const requested=(process.env.GITSKILLS_RESCUE_STARTS||'').split(',').map(Number).filter(Number.isInteger);
const starts=requested.length?requested:[...expected.keys()];
for(const start of starts)if(!expected.has(start))throw new Error(`unknown_rescue_start_${start}`);

async function retry(label,fn,attempts=8){let last;for(let i=0;i<attempts;i++){try{return await fn()}catch(e){last=e;if(i===attempts-1)break;const is429=/\b429\b/.test(String(e));const delay=Math.min((is429?5000:1000)*2**i,is429?60000:15000)+Math.floor(Math.random()*750);console.warn(JSON.stringify({event:'retry',label,attempt:i+1,delay,error:String(e).slice(0,240)}));await sleep(delay)}}throw last}

async function oidc(){
  const u=new URL(need('ACTIONS_ID_TOKEN_REQUEST_URL'));
  u.searchParams.set('audience','gitskills-b2-supabase-ingest');
  const r=await fetch(u,{headers:{authorization:`Bearer ${need('ACTIONS_ID_TOKEN_REQUEST_TOKEN')}`}});
  if(!r.ok)throw new Error(`oidc_${r.status}`);
  return (await r.json()).value;
}

function representatives(src){const out=[];for(const x of src){const r=x.row||{};if(r.dedup_primary!==1&&r.dedup_primary!==true)continue;out.push({row_idx:x.row_idx,repo_full_name:r.repo_full_name,path:r.path,filename:r.filename,location_class:r.location_class,file_sha:r.file_sha,discovered_at:r.discovered_at,dedup_primary:r.dedup_primary,content:r.content,content_fetched:r.content_fetched,content_sha_ok:r.content_sha_ok,frontmatter_valid:r.frontmatter_valid,name:r.name,description:r.description,body_chars:r.body_chars,sibling_count:r.sibling_count,sibling_bytes:r.sibling_bytes,has_scripts:r.has_scripts,has_references:r.has_references,composition_fetched:r.composition_fetched,composition_truncated:r.composition_truncated})}return out}

async function fetchShard(shardId){
  const u=new URL(HF_ROWS);u.searchParams.set('dataset','mvaccargiu/gitskills');u.searchParams.set('config','artifacts');u.searchParams.set('split','train');u.searchParams.set('offset',String((shardId-100000)*100));u.searchParams.set('length','100');
  const rows=await retry(`hf_${shardId}`,async()=>{const r=await fetch(u,{signal:AbortSignal.timeout(25000),headers:{'user-agent':'skillset-corpus-rescue/1.0'}});if(!r.ok)throw new Error(`HF ${r.status}: ${(await r.text()).slice(0,200)}`);const b=await r.json();if((b.rows||[]).length!==100)throw new Error(`scan_count_mismatch_${shardId}_${(b.rows||[]).length}`);return b.rows});
  const reps=representatives(rows);
  return {shardId,startOffset:(shardId-100000)*100,scannedRows:rows.length,representativeRows:reps.length,rows:reps};
}

async function mapLimit(items,limit,fn){let cursor=0;const out=new Array(items.length);await Promise.all(Array.from({length:Math.min(limit,items.length)},async()=>{while(cursor<items.length){const i=cursor++;out[i]=await fn(items[i])}}));return out}

async function upload(token,path,payload,reps){
  const bytes=gzipSync(Buffer.from(JSON.stringify(payload)),{level:9});
  const digest=createHash('sha256').update(bytes).digest('hex');
  await retry(`upload_${path}`,async()=>{const r=await fetch(ingest,{method:'POST',headers:{authorization:`Bearer ${token}`,'content-type':'application/gzip','x-storage-path':path,'x-sha256':digest,'x-representatives':String(reps)},body:bytes,signal:AbortSignal.timeout(150000)});const text=await r.text();if(!r.ok)throw new Error(`ingest_${r.status}:${text.slice(0,500)}`)});
  console.log(JSON.stringify({event:'split_upload',path,reps,bytes:bytes.length}));
}

async function rescue(token,start){
  const shards=await mapLimit(Array.from({length:32},(_,i)=>start+i),4,fetchShard);
  const actual=shards.reduce((n,s)=>n+s.representativeRows,0);
  const required=expected.get(start);
  if(required!==null&&actual!==required)throw new Error(`source_count_mismatch_${start}_${actual}_${required}`);
  const groups=[];let current=[],count=0;
  for(const shard of shards){if(current.length&&count+shard.representativeRows>250){groups.push([current,count]);current=[];count=0}current.push(shard);count+=shard.representativeRows}
  if(current.length)groups.push([current,count]);
  for(let i=0;i<groups.length;i++){const [part,reps]=groups[i];const path=`gitskills/discovery-b2-split-v1/pack-${start}-part-${String(i+1).padStart(2,'0')}.json.gz`;await upload(token,path,{version:2,source:'GitSkills',dataset:'mvaccargiu/gitskills',config:'artifacts',scan:'all_rows_local_dedup_primary',exactContentIncluded:true,storage:'supabase',generatedAt:new Date().toISOString(),shards:part},reps)}
  console.log(JSON.stringify({event:'source_complete',start,parts:groups.length,reps:actual}));
}

const token=await oidc();
for(const [i,start] of starts.entries())if(i%16===lane-1)await rescue(token,start);
