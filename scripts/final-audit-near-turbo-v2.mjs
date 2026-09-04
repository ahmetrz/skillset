import {createClient} from '@libsql/client';

const PART=Number(process.env.NEAR_PARTITION||0),PARTS=Math.max(1,Number(process.env.NEAR_PARTITIONS||20));
const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const turso=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const now=()=>new Date().toISOString(),sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function retry(label,fn,n=7){let last;for(let i=0;i<n;i++){try{return await fn(i)}catch(e){last=e;if(i===n-1)break;await sleep(Math.min(300*(2**i),8000))}}throw new Error(label+': '+String(last?.message||last))}
async function q(sql,args=[]){return (await turso.execute({sql,args})).rows}
async function ensure(){await turso.batch([
 {sql:"CREATE TABLE IF NOT EXISTS final_audit_near_v1 (a TEXT NOT NULL,b TEXT NOT NULL,hamming INTEGER NOT NULL,length_ratio REAL NOT NULL,relation TEXT NOT NULL,band TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(a,b))",args:[]},
 {sql:"CREATE INDEX IF NOT EXISTS idx_final_audit_near_relation_v1 ON final_audit_near_v1(relation,hamming)",args:[]},
 {sql:"CREATE TABLE IF NOT EXISTS final_audit_near_partition_v2 (partition_id INTEGER PRIMARY KEY,status TEXT NOT NULL,tasks INTEGER NOT NULL DEFAULT 0,groups_n INTEGER NOT NULL DEFAULT 0,pairs_n INTEGER NOT NULL DEFAULT 0,unresolved_groups INTEGER NOT NULL DEFAULT 0,max_group INTEGER NOT NULL DEFAULT 0,error TEXT,updated_at TEXT NOT NULL)",args:[]},
 {sql:"CREATE TABLE IF NOT EXISTS final_audit_near_oversized_v2 (band TEXT PRIMARY KEY,group_size INTEGER NOT NULL,reason TEXT NOT NULL,updated_at TEXT NOT NULL)",args:[]},
 {sql:"CREATE TABLE IF NOT EXISTS final_audit_reduce_state_v1 (key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL)",args:[]}
],'write')}
async function ready(){
 const [g,s,di,dp]=await Promise.all([
   q("SELECT count(*) n FROM gitskills_packs WHERE path LIKE 'gitskills/discovery-b2-v2/%'"),
   q("SELECT count(*) n FROM skills_sh_external_exact_v1 WHERE status='done' AND b2_path IS NOT NULL"),
   q("SELECT source_system,count(*) n FROM final_audit_unit_v1 WHERE status='done' GROUP BY source_system"),
   q("SELECT source_system,count(*) n FROM final_audit_policy_unit_v2 WHERE status='done' GROUP BY source_system")
 ]);
 const im=new Map(di.map(x=>[String(x.source_system),Number(x.n)])),pm=new Map(dp.map(x=>[String(x.source_system),Number(x.n)]));
 const exp={legacy:19132,git:Number(g[0]?.n||0),skills:Number(s[0]?.n||0)};
 return im.get('gitskills-legacy-hf')===exp.legacy&&im.get('gitskills-b2')===exp.git&&im.get('skills-sh-b2')===exp.skills&&pm.get('gitskills-legacy-hf')===exp.legacy&&pm.get('gitskills-b2')===exp.git&&pm.get('skills-sh-b2')===exp.skills;
}
function popcount64(hex){let x=BigInt('0x'+hex),n=0;while(x){x&=x-1n;n++}return n}
function ham(a,b){return popcount64((BigInt('0x'+a)^BigInt('0x'+b)).toString(16))}
async function mark(status,stats,error=null){await turso.execute({sql:"INSERT INTO final_audit_near_partition_v2(partition_id,status,tasks,groups_n,pairs_n,unresolved_groups,max_group,error,updated_at) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(partition_id) DO UPDATE SET status=excluded.status,tasks=excluded.tasks,groups_n=excluded.groups_n,pairs_n=excluded.pairs_n,unresolved_groups=excluded.unresolved_groups,max_group=excluded.max_group,error=excluded.error,updated_at=excluded.updated_at",args:[PART,status,stats.tasks,stats.groups,stats.pairs,stats.unresolved,stats.maxGroup,error,now()]})}
async function processGroup(axis,key,g,stats){
 stats.groups++;stats.maxGroup=Math.max(stats.maxGroup,g.length);
 if(g.length>3000){
   stats.unresolved++;
   await turso.execute({sql:"INSERT OR REPLACE INTO final_audit_near_oversized_v2(band,group_size,reason,updated_at) VALUES(?,?,?,?)",args:['axis'+axis+':'+key,g.length,'group_gt_3000',now()]});
   return;
 }
 const stmts=[];
 for(let i=0;i<g.length;i++)for(let j=i+1;j<g.length;j++){
   const x=g[i],y=g[j],dist=ham(String(x.simhash_hex),String(y.simhash_hex));
   if(dist>8)continue;
   const lr=Math.min(Number(x.char_count),Number(y.char_count))/Math.max(1,Math.max(Number(x.char_count),Number(y.char_count)));
   if(lr<0.55)continue;
   const a=String(x.content_hash)<String(y.content_hash)?String(x.content_hash):String(y.content_hash);
   const b=String(x.content_hash)<String(y.content_hash)?String(y.content_hash):String(x.content_hash);
   const rel=dist<=4&&lr>=0.90?'near_duplicate':'coverage_candidate';
   stmts.push({sql:"INSERT OR IGNORE INTO final_audit_near_v1(a,b,hamming,length_ratio,relation,band,created_at) VALUES(?,?,?,?,?,?,?)",args:[a,b,dist,lr,rel,'axis'+axis+':'+key,now()]});
   if(stmts.length>=300){const n=stmts.length;await retry('near_write',()=>turso.batch(stmts.splice(0),'write'));stats.pairs+=n}
 }
 if(stmts.length){const n=stmts.length;await retry('near_write',()=>turso.batch(stmts,'write'));stats.pairs+=n}
}
async function finalizeIfComplete(){
 const r=await q("SELECT count(*) n,coalesce(sum(unresolved_groups),0) unresolved FROM final_audit_near_partition_v2 WHERE status='done'");
 if(Number(r[0]?.n)!==PARTS)return;
 const unresolved=Number(r[0]?.unresolved||0);
 const vals=[['near_axis','3'],['near_bucket','0'],['near_complete','1'],['near_oversized_groups',String(unresolved)]];
 for(const [k,v] of vals)await turso.execute({sql:"INSERT INTO final_audit_reduce_state_v1(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",args:[k,v,now()]});
 console.log(JSON.stringify({event:'near_parallel_complete',partitions:PARTS,unresolved}));
}
await ensure();
const already=await q("SELECT status FROM final_audit_near_partition_v2 WHERE partition_id=?",[PART]);
if(String(already[0]?.status||'')==='done'){await finalizeIfComplete();process.exit(0)}
if(!(await ready())){console.log(JSON.stringify({event:'near_not_ready',partition:PART}));process.exit(0)}
const stats={tasks:0,groups:0,pairs:0,unresolved:0,maxGroup:0};
try{
 await mark('running',stats);
 for(let task=PART;task<768;task+=PARTS){
   const axis=Math.floor(task/256),bucketN=task%256,col=axis===0?'c0':axis===1?'c1':'c2',lo=bucketN*256,hi=lo+255;
   const rows=await q("SELECT content_hash,simhash_hex,char_count,token_count,c0,c1,c2,c3 FROM final_audit_exact_v1 WHERE (sdlc_mask<>0 OR social_mask<>0) AND "+col+" BETWEEN ? AND ?",[lo,hi]);
   const groups=new Map();
   for(const r of rows){const k=axis===0?r.c0+':'+r.c1:axis===1?r.c1+':'+r.c2:r.c2+':'+r.c3;if(!groups.has(k))groups.set(k,[]);groups.get(k).push(r)}
   for(const [key,g] of groups)if(g.length>1)await processGroup(axis,key,g,stats);
   stats.tasks++;if(stats.tasks%5===0)await mark('running',stats);
 }
 await mark('done',stats);await finalizeIfComplete();
 console.log(JSON.stringify({event:'near_partition_done',partition:PART,parts:PARTS,stats}));
}catch(e){await mark('error',stats,String(e?.message||e).slice(0,600));throw e}
