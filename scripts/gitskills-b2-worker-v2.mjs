import { createClient } from '@libsql/client';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { createHash } from 'node:crypto';
import { gzipSync } from 'node:zlib';

const HF_ROWS='https://datasets-server.huggingface.co/rows';
const PACK_SIZE=Math.max(1,Math.min(Number(process.env.GITSKILLS_PACK_SIZE||24),48));
const FETCH_CONCURRENCY=Math.max(1,Math.min(Number(process.env.GITSKILLS_FETCH_CONCURRENCY||4),8));
const MAX_ATTEMPTS=64;
const ERROR_COOLDOWN_MS=45_000;
const LEGACY_DONE_SHARDS=19132;
const LEGACY_REPRESENTATIVES=1047750;

function need(n){const v=process.env[n];if(!v)throw new Error(`Missing required environment variable: ${n}`);return v;}
const turso=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const endpoint=need('B2_ENDPOINT').replace(/\/$/,'');
const region=endpoint.match(/s3[.-]([a-z0-9-]+)\.backblazeb2\.com/i)?.[1]||'us-east-1';
const b2=new S3Client({endpoint,region,forcePathStyle:true,credentials:{accessKeyId:need('B2_ACCESS_KEY_ID'),secretAccessKey:need('B2_SECRET_ACCESS_KEY')}});
const bucket=need('B2_BUCKET');
const sleep=(ms)=>new Promise(r=>setTimeout(r,ms));
const now=()=>new Date().toISOString();
const sha256=(b)=>createHash('sha256').update(b).digest('hex');

async function retry(label,fn,attempts=6){let last;for(let i=0;i<attempts;i++){try{return await fn(i);}catch(e){last=e;if(i===attempts-1)break;const m=String(e?.message||e);const is429=/\b429\b/.test(m);const base=is429?5000:1000;const cap=is429?60000:15000;const delay=Math.min(base*(2**i),cap)+Math.floor(Math.random()*750);console.warn(JSON.stringify({event:'retry',label,attempt:i+1,delay,error:m.slice(0,240)}));await sleep(delay);}}throw last;}

async function ensureSchema(){
  await turso.batch([
    `CREATE TABLE IF NOT EXISTS gitskills_shards (shard_id INTEGER PRIMARY KEY,start_offset INTEGER NOT NULL,row_limit INTEGER NOT NULL,status TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,claimed_at TEXT,updated_at TEXT NOT NULL,rows_fetched INTEGER NOT NULL DEFAULT 0,storage_path TEXT,error TEXT)`,
    `CREATE INDEX IF NOT EXISTS idx_gitskills_shards_status ON gitskills_shards(status, shard_id)`,
    `CREATE TABLE IF NOT EXISTS gitskills_packs (path TEXT PRIMARY KEY,shard_count INTEGER NOT NULL,representatives INTEGER NOT NULL,bytes INTEGER NOT NULL,sha256 TEXT NOT NULL,created_at TEXT NOT NULL)`,
    `CREATE TABLE IF NOT EXISTS gitskills_state (key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL)`
  ],'write');
  const t=now();
  await turso.batch([
    {sql:`INSERT INTO gitskills_state(key,value,updated_at) VALUES('legacy_done_shards',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`,args:[String(LEGACY_DONE_SHARDS),t]},
    {sql:`INSERT INTO gitskills_state(key,value,updated_at) VALUES('legacy_representatives',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`,args:[String(LEGACY_REPRESENTATIVES),t]}
  ],'write');
}

async function seedRemaining(){const c=await turso.execute(`SELECT COUNT(*) AS n FROM gitskills_shards`);if(Number(c.rows[0]?.n||0)>0)return;const ids=[119117,...Array.from({length:31},(_,i)=>119132+i),...Array.from({length:18808},(_,i)=>119164+i)];for(let i=0;i<ids.length;i+=400){const t=now();await retry('turso_seed',()=>turso.batch(ids.slice(i,i+400).map(id=>({sql:`INSERT OR IGNORE INTO gitskills_shards(shard_id,start_offset,row_limit,status,updated_at) VALUES(?,?,?,'pending',?)`,args:[id,(id-100000)*100,id===137971?17:100,t]})),'write'));}console.log(JSON.stringify({event:'seeded',shards:ids.length}));}

async function recover(){await ensureSchema();await seedRemaining();const t=now();const a=await turso.execute({sql:`UPDATE gitskills_shards SET status='error',claimed_at=NULL,updated_at=?,error=COALESCE(error,'recovered_after_cancel') WHERE status='processing'`,args:[t]});const b=await turso.execute({sql:`UPDATE gitskills_shards SET attempts=0,updated_at=?,error='hf_429_recovered' WHERE status<>'done' AND (error LIKE 'HF 429:%' OR error='hf_429_recovered' OR attempts>=8)`,args:[t]});console.log(JSON.stringify({event:'recover',processingReset:a.rowsAffected,retryBudgetReset:b.rowsAffected}));}

async function claim(){const stale=new Date(Date.now()-8*60_000).toISOString();const cooldown=new Date(Date.now()-ERROR_COOLDOWN_MS).toISOString();await turso.execute({sql:`UPDATE gitskills_shards SET status='error',error=COALESCE(error,'stale_processing'),claimed_at=NULL,updated_at=? WHERE status='processing' AND claimed_at<?`,args:[now(),stale]});const t=now();const r=await turso.execute({sql:`UPDATE gitskills_shards SET status='processing',attempts=attempts+1,claimed_at=?,updated_at=?,error=NULL WHERE shard_id IN (SELECT shard_id FROM gitskills_shards WHERE attempts<? AND (status='pending' OR (status='error' AND updated_at<?)) ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END,attempts,shard_id LIMIT ?) RETURNING shard_id,start_offset,row_limit,attempts`,args:[t,t,MAX_ATTEMPTS,cooldown,PACK_SIZE]});return r.rows.map(x=>({shard_id:Number(x.shard_id),start_offset:Number(x.start_offset),row_limit:Number(x.row_limit),attempts:Number(x.attempts)}));}

async function fetchPage(s){const u=new URL(HF_ROWS);u.searchParams.set('dataset','mvaccargiu/gitskills');u.searchParams.set('config','artifacts');u.searchParams.set('split','train');u.searchParams.set('offset',String(s.start_offset));u.searchParams.set('length',String(s.row_limit));return retry(`hf_${s.shard_id}`,async()=>{const res=await fetch(u,{signal:AbortSignal.timeout(25000),headers:{'user-agent':'skillset-corpus-b2/2.0'}});if(!res.ok){const text=(await res.text()).slice(0,240);throw new Error(`HF ${res.status}: ${text}`);}const body=await res.json();const rows=body.rows||[];if(rows.length!==s.row_limit)throw new Error(`scan_count_mismatch:${rows.length}/${s.row_limit}`);return rows;},8);}
function reps(src){const out=[];for(const x of src){const r=x.row||{};if(r.dedup_primary!==1&&r.dedup_primary!==true)continue;out.push({row_idx:x.row_idx,repo_full_name:r.repo_full_name,path:r.path,filename:r.filename,location_class:r.location_class,file_sha:r.file_sha,discovered_at:r.discovered_at,dedup_primary:r.dedup_primary,content:r.content,content_fetched:r.content_fetched,content_sha_ok:r.content_sha_ok,frontmatter_valid:r.frontmatter_valid,name:r.name,description:r.description,body_chars:r.body_chars,sibling_count:r.sibling_count,sibling_bytes:r.sibling_bytes,has_scripts:r.has_scripts,has_references:r.has_references,composition_fetched:r.composition_fetched,composition_truncated:r.composition_truncated});}return out;}
async function mapLimit(items,limit,fn){let c=0;const out=new Array(items.length);await Promise.all(Array.from({length:Math.min(limit,items.length||1)},async()=>{while(c<items.length){const i=c++;try{out[i]={ok:true,shard:items[i],rows:await fn(items[i])};}catch(e){out[i]={ok:false,shard:items[i],error:String(e?.message||e).slice(0,1200)};}}}));return out;}
async function finishFailures(bad){if(!bad.length)return;const t=now();await turso.batch(bad.map(x=>{const is429=/^HF 429:/.test(x.error);return {sql:`UPDATE gitskills_shards SET status='error',claimed_at=NULL,updated_at=?,error=?,attempts=CASE WHEN ? THEN MAX(attempts-1,0) ELSE attempts END WHERE shard_id=?`,args:[t,x.error,is429?1:0,x.shard.shard_id]};}),'write');}

async function runOnePack(){const claimed=await retry('turso_claim',claim);if(!claimed.length)return {claimed:0};const settled=await mapLimit(claimed,FETCH_CONCURRENCY,fetchPage);const good=settled.filter(x=>x.ok).map(x=>({...x,reps:reps(x.rows)}));const bad=settled.filter(x=>!x.ok);await finishFailures(bad);if(!good.length)return {claimed:claimed.length,success:0,failed:bad.length};const ids=good.map(x=>x.shard.shard_id);const payload={version:2,source:'GitSkills',dataset:'mvaccargiu/gitskills',config:'artifacts',scan:'all_rows_local_dedup_primary',exactContentIncluded:true,storage:'backblaze-b2',generatedAt:now(),shards:good.map(x=>({shardId:x.shard.shard_id,startOffset:x.shard.start_offset,scannedRows:x.rows.length,representativeRows:x.reps.length,rows:x.reps}))};const gz=gzipSync(Buffer.from(JSON.stringify(payload)),{level:9});const digest=sha256(gz);const path=`gitskills/discovery-b2-v2/pack-${ids.join('-')}.json.gz`;await retry('b2_put',()=>b2.send(new PutObjectCommand({Bucket:bucket,Key:path,Body:gz,ContentType:'application/gzip',Metadata:{sha256:digest}})),7);const n=good.reduce((a,x)=>a+x.reps.length,0),t=now();await retry('turso_finish',()=>turso.batch([{sql:`INSERT INTO gitskills_packs(path,shard_count,representatives,bytes,sha256,created_at) VALUES(?,?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET shard_count=excluded.shard_count,representatives=excluded.representatives,bytes=excluded.bytes,sha256=excluded.sha256`,args:[path,good.length,n,gz.length,digest,t]},...good.map(x=>({sql:`UPDATE gitskills_shards SET status='done',rows_fetched=?,storage_path=?,error=NULL,claimed_at=NULL,updated_at=? WHERE shard_id=?`,args:[x.reps.length,path,t,x.shard.shard_id]}))],'write'));return {claimed:claimed.length,success:good.length,failed:bad.length,representatives:n,bytes:gz.length,path,sha256:digest};}
async function status(){const q=await turso.execute(`SELECT status,COUNT(*) AS n,COALESCE(SUM(rows_fetched),0) AS reps FROM gitskills_shards GROUP BY status ORDER BY status`);const p=await turso.execute(`SELECT COUNT(*) AS n,COALESCE(SUM(representatives),0) AS reps,COALESCE(SUM(bytes),0) AS bytes FROM gitskills_packs`);const terminal=await turso.execute({sql:`SELECT COUNT(*) AS n FROM gitskills_shards WHERE status<>'done' AND attempts>=?`,args:[MAX_ATTEMPTS]});return {legacy:{doneShards:LEGACY_DONE_SHARDS,representatives:LEGACY_REPRESENTATIVES},queue:q.rows,packs:p.rows[0],terminal:Number(terminal.rows[0]?.n||0)};}

async function main(){if(process.argv.includes('--recover')){await recover();console.log(JSON.stringify({event:'status',...(await status())}));return;}await ensureSchema();await seedRemaining();if(process.argv.includes('--status')){console.log(JSON.stringify({event:'status',...(await status())}));return;}const packs=Math.max(1,Math.min(Number(process.env.GITSKILLS_PACKS_PER_RUN||200),400));for(let i=0;i<packs;i++){const r=await runOnePack();console.log(JSON.stringify({event:'pack',index:i+1,...r}));if(!r.claimed)break;}console.log(JSON.stringify({event:'status',...(await status())}));}
main().catch(e=>{console.error(JSON.stringify({event:'fatal',error:String(e?.stack||e)}));process.exit(1);});
