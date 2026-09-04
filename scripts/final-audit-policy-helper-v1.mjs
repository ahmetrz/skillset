import {createHash} from 'node:crypto';

const now=()=>new Date().toISOString();
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const sha=b=>createHash('sha256').update(b).digest('hex');
const clip=(s,n=300)=>{s=String(s||'').replace(/\s+/g,' ').trim();return s.length>n?s.slice(0,n-1)+'…':s};

async function retry(label,fn,n=7){let last;for(let i=0;i<n;i++){try{return await fn(i)}catch(e){last=e;if(i===n-1)break;const m=String(e?.message||e),base=/429|503|504|timeout|aborted|rate/i.test(m)?1200:300;await sleep(Math.min(base*(2**i),12000)+Math.floor(Math.random()*250))}}throw new Error(label+': '+String(last?.message||last))}

export async function ensurePolicy(turso){await turso.batch([
  {sql:"CREATE TABLE IF NOT EXISTS final_audit_policy_v2 (content_hash TEXT NOT NULL,policy_key TEXT NOT NULL,policy_value TEXT NOT NULL,evidence TEXT NOT NULL,sample_locator TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(content_hash,policy_key,policy_value))",args:[]},
  {sql:"CREATE INDEX IF NOT EXISTS idx_final_audit_policy_key_v2 ON final_audit_policy_v2(policy_key,policy_value)",args:[]},
  {sql:"CREATE TABLE IF NOT EXISTS final_audit_policy_unit_v2 (source_key TEXT PRIMARY KEY,source_system TEXT NOT NULL,status TEXT NOT NULL,skills_scanned INTEGER NOT NULL DEFAULT 0,facts INTEGER NOT NULL DEFAULT 0,error TEXT,updated_at TEXT NOT NULL)",args:[]},
  {sql:"CREATE INDEX IF NOT EXISTS idx_final_audit_policy_unit_status_v2 ON final_audit_policy_unit_v2(source_system,status)",args:[]}
],'write')}

export async function policyDone(turso,k){const r=await turso.execute({sql:"SELECT status FROM final_audit_policy_unit_v2 WHERE source_key=?",args:[k]});return String(r.rows[0]?.status||'')==='done'}

async function mark(turso,k,sys,status,skills=0,facts=0,error=null){await turso.execute({sql:"INSERT INTO final_audit_policy_unit_v2(source_key,source_system,status,skills_scanned,facts,error,updated_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(source_key) DO UPDATE SET status=excluded.status,skills_scanned=excluded.skills_scanned,facts=excluded.facts,error=excluded.error,updated_at=excluded.updated_at",args:[k,sys,status,skills,facts,error,now()]})}

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

export async function writePolicy(turso,sys,k,items){
  let facts=0;
  for(let off=0;off<items.length;off+=100){
    const stmts=[];
    for(const x of items.slice(off,off+100)){
      const h=sha(Buffer.from(x.text,'utf8'));
      for(const f of numericFacts(x.text)){facts++;stmts.push({sql:"INSERT OR IGNORE INTO final_audit_policy_v2(content_hash,policy_key,policy_value,evidence,sample_locator,created_at) VALUES(?,?,?,?,?,?)",args:[h,f.key,f.value,f.evidence,x.locator,now()]})}
    }
    if(stmts.length)await retry('policy_write',()=>turso.batch(stmts,'write'),8);
  }
  await mark(turso,k,sys,'done',items.length,facts);
  return facts;
}

export async function markPolicyError(turso,k,sys,e){await mark(turso,k,sys,'error',0,0,clip(e?.message||e,600))}
