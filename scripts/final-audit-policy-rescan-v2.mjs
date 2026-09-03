import { createClient } from '@libsql/client';
import { S3Client,ListObjectsV2Command,GetObjectCommand } from '@aws-sdk/client-s3';
import { createHash } from 'node:crypto';
import { gunzipSync } from 'node:zlib';

const SRC=process.env.AUDIT_SOURCE||'',PART=Number(process.env.AUDIT_PARTITION||0),PARTS=Math.max(1,Number(process.env.AUDIT_PARTITIONS||1));
const HF='https://datasets-server.huggingface.co/rows',FIRST=100000,LAST=119131;
const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const turso=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const endpoint=need('B2_ENDPOINT').replace(/\/$/,'');
const region=endpoint.match(/s3[.-]([a-z0-9-]+)\.backblazeb2\.com/i)?.[1]||'us-east-1';
const b2=new S3Client({endpoint,region,forcePathStyle:true,credentials:{accessKeyId:need('B2_ACCESS_KEY_ID'),secretAccessKey:need('B2_SECRET_ACCESS_KEY')}});
const bucket=need('B2_BUCKET'),now=()=>new Date().toISOString(),sleep=ms=>new Promise(r=>setTimeout(r,ms)),sha=b=>createHash('sha256').update(b).digest('hex');
const clip=(s,n=300)=>{s=String(s||'').replace(/\s+/g,' ').trim();return s.length>n?s.slice(0,n-1)+'…':s};
async function retry(label,fn,n=6){let last;for(let i=0;i<n;i++){try{return await fn(i)}catch(e){last=e;if(i===n-1)break;const m=String(e?.message||e),base=/429|503|504|timeout|aborted/i.test(m)?1500:500;await sleep(Math.min(base*(2**i),15000)+Math.floor(Math.random()*300))}}throw new Error(label+': '+String(last?.message||last))}
async function ensure(){await turso.batch([
  {sql:"CREATE TABLE IF NOT EXISTS final_audit_policy_v2 (content_hash TEXT NOT NULL,policy_key TEXT NOT NULL,policy_value TEXT NOT NULL,evidence TEXT NOT NULL,sample_locator TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(content_hash,policy_key,policy_value))",args:[]},
  {sql:"CREATE INDEX IF NOT EXISTS idx_final_audit_policy_key_v2 ON final_audit_policy_v2(policy_key,policy_value)",args:[]},
  {sql:"CREATE TABLE IF NOT EXISTS final_audit_policy_unit_v2 (source_key TEXT PRIMARY KEY,source_system TEXT NOT NULL,status TEXT NOT NULL,skills_scanned INTEGER NOT NULL DEFAULT 0,facts INTEGER NOT NULL DEFAULT 0,error TEXT,updated_at TEXT NOT NULL)",args:[]},
  {sql:"CREATE INDEX IF NOT EXISTS idx_final_audit_policy_unit_status_v2 ON final_audit_policy_unit_v2(source_system,status)",args:[]}
],'write')}
async function done(k){const r=await turso.execute({sql:"SELECT status FROM final_audit_policy_unit_v2 WHERE source_key=?",args:[k]});return String(r.rows[0]?.status||'')==='done'}
async function mark(k,sys,status,skills=0,facts=0,error=null){await turso.execute({sql:"INSERT INTO final_audit_policy_unit_v2(source_key,source_system,status,skills_scanned,facts,error,updated_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(source_key) DO UPDATE SET status=excluded.status,skills_scanned=excluded.skills_scanned,facts=excluded.facts,error=excluded.error,updated_at=excluded.updated_at",args:[k,sys,status,skills,facts,error,now()]})}
function normalizeKey(k){k=k.toLowerCase().replace(/\s+/g,'_');if(/^retr/.test(k))return 'retry';if(/attempt/.test(k))return 'max_attempts';return k}
function numericFacts(text){
  const out=[],seen=new Set(),segs=String(text).split(/\n+|(?<=[.!?])\s+/).filter(x=>x.length<1200);
  const re=/\b(retr(?:y|ies)|timeout|coverage|score|threshold|max(?:imum)? attempts?)\b.{0,70}?\b(\d+(?:\.\d+)?)\s*(%|ms|milliseconds?|s|sec|seconds?|m|min|minutes?|h|hours?)?/ig;
  for(const s of segs){
    if(!/\b(must|required|always|set|minimum|maximum|at least|at most|no more than|exactly|threshold|target|limit|timeout|coverage|retries?|attempts?)\b/i.test(s))continue;
    re.lastIndex=0;let m;
    while((m=re.exec(s))&&out.length<32){
      const key=normalizeKey(m[1]),value=(m[2]+(m[3]||'')).toLowerCase(),id=key+'='+value;
      if(seen.has(id))continue;seen.add(id);out.push({key,value,evidence:clip(s,300)});
    }
  }
  const tools=['ripgrep','rg','grep','sed','awk','jq','git','gh','curl','wget','npm','pnpm','yarn','bun','pytest','jest','vitest','playwright','docker','kubectl','terraform','ansible','supabase','turso'];
  for(const tool of tools){
    const never=new RegExp("(never|do not|don't|must not).{0,50}(use|run).{0,25}\\b"+tool+"\\b","i");
    const prefer=new RegExp("(prefer|default to|always use|must use|required to use).{0,30}\\b"+tool+"\\b","i");
    const mn=String(text).match(never),mp=String(text).match(prefer);
    if(mn){const id='tool:'+tool+'=never';if(!seen.has(id)){seen.add(id);out.push({key:'tool:'+tool,value:'never',evidence:clip(mn[0],300)})}}
    if(mp){const id='tool:'+tool+'=prefer';if(!seen.has(id)){seen.add(id);out.push({key:'tool:'+tool,value:'prefer',evidence:clip(mp[0],300)})}}
  }
  return out;
}
async function write(sys,k,items){
  let facts=0;
  for(let off=0;off<items.length;off+=80){
    const stmts=[];
    for(const x of items.slice(off,off+80)){
      const h=sha(Buffer.from(x.text,'utf8'));
      for(const f of numericFacts(x.text)){facts++;stmts.push({sql:"INSERT OR IGNORE INTO final_audit_policy_v2(content_hash,policy_key,policy_value,evidence,sample_locator,created_at) VALUES(?,?,?,?,?,?)",args:[h,f.key,f.value,f.evidence,x.locator,now()]})}
    }
    if(stmts.length)await retry('policy_write',()=>turso.batch(stmts,'write'),8);
  }
  await mark(k,sys,'done',items.length,facts);return facts;
}
async function hfShard(id){const k='hf:gitskills:'+id;if(await done(k))return {skip:1};try{const u=new URL(HF);u.searchParams.set('dataset','mvaccargiu/gitskills');u.searchParams.set('config','artifacts');u.searchParams.set('split','train');u.searchParams.set('offset',String((id-FIRST)*100));u.searchParams.set('length','100');const body=await retry('hf_'+id,async()=>{const r=await fetch(u,{headers:{'user-agent':'skillset-policy-rescan/1.0'},signal:AbortSignal.timeout(30000)});if(!r.ok)throw new Error('HF '+r.status);return r.json()},8);const items=[];for(const x of body.rows||[]){const r=x.row||{};if(r.dedup_primary!==1&&r.dedup_primary!==true)continue;const text=String(r.content||'');if(text)items.push({text,locator:'hf://mvaccargiu/gitskills/artifacts/'+x.row_idx})}return {facts:await write('gitskills-legacy-hf',k,items),skills:items.length}}catch(e){await mark(k,'gitskills-legacy-hf','error',0,0,clip(e?.message||e));throw e}}
async function list(prefix){let token,out=[];do{const r=await b2.send(new ListObjectsV2Command({Bucket:bucket,Prefix:prefix,ContinuationToken:token,MaxKeys:1000}));out.push(...(r.Contents||[]).map(x=>String(x.Key)));token=r.IsTruncated?r.NextContinuationToken:undefined}while(token);return out.sort()}
async function get(key){const r=await retry('b2_get',()=>b2.send(new GetObjectCommand({Bucket:bucket,Key:key})),6);return JSON.parse(gunzipSync(Buffer.from(await r.Body.transformToByteArray())).toString('utf8'))}
async function gitPack(key){const k='b2:'+key;if(await done(k))return {skip:1};try{const p=await get(key),items=[];for(const s of p.shards||[])for(const r of s.rows||[]){const text=String(r.content||'');if(text)items.push({text,locator:'b2://'+bucket+'/'+key+'#row='+String(r.row_idx??items.length)})}return {facts:await write('gitskills-b2',k,items),skills:items.length}}catch(e){await mark(k,'gitskills-b2','error',0,0,clip(e?.message||e));throw e}}
async function skillsPack(key){const k='b2:'+key;if(await done(k))return {skip:1};try{const p=await get(key),items=[];for(const f of p.files||[]){const bytes=Buffer.from(String(f.contentBase64||''),'base64');if(bytes.length)items.push({text:bytes.toString('utf8'),locator:'b2://'+bucket+'/'+key+'#file='+encodeURIComponent(String(f.path||''))})}return {facts:await write('skills-sh-b2',k,items),skills:items.length}}catch(e){await mark(k,'skills-sh-b2','error',0,0,clip(e?.message||e));throw e}}
async function runLegacy(){const ids=[];for(let id=FIRST;id<=LAST;id++)if((id-FIRST)%PARTS===PART)ids.push(id);let doneN=0,skip=0,fail=0,facts=0,skills=0;for(let i=0;i<ids.length;i+=4){const rs=await Promise.allSettled(ids.slice(i,i+4).map(hfShard));for(const r of rs){if(r.status==='fulfilled'){if(r.value.skip)skip++;else{doneN++;facts+=r.value.facts||0;skills+=r.value.skills||0}}else fail++}}return {done:doneN,skip,fail,facts,skills}}
async function runB2(prefix,kind){const all=await list(prefix),keys=all.filter((_,i)=>i%PARTS===PART);let doneN=0,skip=0,fail=0,facts=0,skills=0;for(const key of keys){try{const r=kind==='git'?await gitPack(key):await skillsPack(key);if(r.skip)skip++;else{doneN++;facts+=r.facts||0;skills+=r.skills||0}}catch{fail++}}return {objects:keys.length,done:doneN,skip,fail,facts,skills}}
await ensure();
let result;if(SRC==='git-legacy')result=await runLegacy();else if(SRC==='git-b2')result=await runB2('gitskills/discovery-b2-v2/','git');else if(SRC==='skills-b2')result=await runB2('skills-sh/exact-b2-v1/','skills');else throw new Error('Unknown AUDIT_SOURCE '+SRC);
console.log(JSON.stringify({event:'policy_rescan_complete',source:SRC,partition:PART,partitions:PARTS,result}));
