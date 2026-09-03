import { createClient } from '@libsql/client';
import { S3Client,ListObjectsV2Command } from '@aws-sdk/client-s3';
import fs from 'node:fs/promises';

const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const turso=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const endpoint=need('B2_ENDPOINT').replace(/\/$/,'');
const region=endpoint.match(/s3[.-]([a-z0-9-]+)\.backblazeb2\.com/i)?.[1]||'us-east-1';
const b2=new S3Client({endpoint,region,forcePathStyle:true,credentials:{accessKeyId:need('B2_ACCESS_KEY_ID'),secretAccessKey:need('B2_SECRET_ACCESS_KEY')}});
const bucket=need('B2_BUCKET'), now=()=>new Date().toISOString();
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function retry(label,fn,n=6){let last;for(let i=0;i<n;i++){try{return await fn(i)}catch(e){last=e;if(i===n-1)break;await sleep(Math.min(500*(2**i),8000))}}throw new Error(label+': '+String(last?.message||last))}
async function listCount(prefix){let token,count=0;do{const r=await b2.send(new ListObjectsV2Command({Bucket:bucket,Prefix:prefix,ContinuationToken:token,MaxKeys:1000}));count+=(r.Contents||[]).length;token=r.IsTruncated?r.NextContinuationToken:undefined}while(token);return count}
async function q(sql,args=[]){return (await turso.execute({sql,args})).rows}
async function ensure(){await turso.batch([
  {sql:"CREATE TABLE IF NOT EXISTS final_audit_near_v1 (a TEXT NOT NULL,b TEXT NOT NULL,hamming INTEGER NOT NULL,length_ratio REAL NOT NULL,relation TEXT NOT NULL,band TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(a,b))",args:[]},
  {sql:"CREATE INDEX IF NOT EXISTS idx_final_audit_near_relation_v1 ON final_audit_near_v1(relation,hamming)",args:[]},
  {sql:"CREATE TABLE IF NOT EXISTS final_audit_reduce_state_v1 (key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL)",args:[]}
],'write')}
async function getState(k){const r=await q("SELECT value FROM final_audit_reduce_state_v1 WHERE key=?",[k]);return r[0]?.value||null}
async function setState(k,v){await turso.execute({sql:"INSERT INTO final_audit_reduce_state_v1(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",args:[k,String(v),now()]})}
function popcount64(hex){let x=BigInt('0x'+hex),n=0;while(x){x&=x-1n;n++}return n}
function ham(a,b){return popcount64((BigInt('0x'+a)^BigInt('0x'+b)).toString(16))}
async function readiness(){
  const [git,skills]=await Promise.all([listCount('gitskills/discovery-b2-v2/'),listCount('skills-sh/exact-b2-v1/')]);
  const rows=await q("SELECT source_system,status,count(*) units,coalesce(sum(skills_indexed),0) skills FROM final_audit_unit_v1 GROUP BY source_system,status ORDER BY source_system,status");
  let policyRows=[];
  try{policyRows=await q("SELECT source_system,status,count(*) units,coalesce(sum(skills_scanned),0) skills,coalesce(sum(facts),0) facts FROM final_audit_policy_unit_v2 GROUP BY source_system,status ORDER BY source_system,status")}catch{}
  const m=new Map(rows.filter(x=>x.status==='done').map(x=>[String(x.source_system),Number(x.units)]));
  const pm=new Map(policyRows.filter(x=>x.status==='done').map(x=>[String(x.source_system),Number(x.units)]));
  const errorSamples=await q("SELECT source_key,source_system,error,updated_at FROM final_audit_unit_v1 WHERE status='error' ORDER BY updated_at DESC LIMIT 20");
  let policyErrorSamples=[];try{policyErrorSamples=await q("SELECT source_key,source_system,error,updated_at FROM final_audit_policy_unit_v2 WHERE status='error' ORDER BY updated_at DESC LIMIT 20")}catch{}
  return {
    expected:{legacy:19132,gitB2:git,skillsB2:skills},
    done:{legacy:m.get('gitskills-legacy-hf')||0,gitB2:m.get('gitskills-b2')||0,skillsB2:m.get('skills-sh-b2')||0},
    policyDone:{legacy:pm.get('gitskills-legacy-hf')||0,gitB2:pm.get('gitskills-b2')||0,skillsB2:pm.get('skills-sh-b2')||0},
    rows,policyRows,errorSamples,policyErrorSamples
  };
}
async function summary(){
  const [a,b,c,d,e]=await Promise.all([
    q("SELECT count(*) n FROM final_audit_exact_v1"),
    q("SELECT count(*) n FROM final_audit_occurrence_v1"),
    q("SELECT count(*) groups_n,coalesce(sum(n-1),0) reducible FROM (SELECT content_hash,count(*) n FROM final_audit_occurrence_v1 GROUP BY content_hash HAVING count(*)>1)"),
    q("SELECT sum(case when sdlc_mask<>0 then 1 else 0 end) sdlc,sum(case when social_mask<>0 then 1 else 0 end) social,sum(case when sdlc_mask<>0 or social_mask<>0 then 1 else 0 end) in_scope FROM final_audit_exact_v1"),
    q("SELECT count(*) n FROM final_audit_exact_v1 WHERE risk_mask<>0")
  ]);
  return {exactUnique:a[0],occurrences:b[0],exactDuplicate:c[0],scope:d[0],riskFlagged:e[0]};
}
async function near(){
  let axis=Number(await getState('near_axis')||0),bucketN=Number(await getState('near_bucket')||0),inserted=Number(await getState('near_inserted')||0),oversized=Number(await getState('near_oversized_groups')||0);
  for(;axis<3;axis++){
    for(;bucketN<256;bucketN++){
      const col=axis===0?'c0':axis===1?'c1':'c2',lo=bucketN*256,hi=lo+255;
      const rows=await q("SELECT content_hash,simhash_hex,char_count,token_count,c0,c1,c2,c3 FROM final_audit_exact_v1 WHERE (sdlc_mask<>0 OR social_mask<>0) AND "+col+" BETWEEN ? AND ?",[lo,hi]);
      const groups=new Map();
      for(const r of rows){const k=axis===0?r.c0+':'+r.c1:axis===1?r.c1+':'+r.c2:r.c2+':'+r.c3;if(!groups.has(k))groups.set(k,[]);groups.get(k).push(r)}
      const stmts=[];
      for(const [key,g] of groups){
        if(g.length<2)continue;
        if(g.length>120){oversized++;continue}
        for(let i=0;i<g.length;i++)for(let j=i+1;j<g.length;j++){
          const x=g[i],y=g[j],dist=ham(String(x.simhash_hex),String(y.simhash_hex));
          const lr=Math.min(Number(x.char_count),Number(y.char_count))/Math.max(1,Math.max(Number(x.char_count),Number(y.char_count)));
          if(dist>8||lr<0.55)continue;
          const a=String(x.content_hash)<String(y.content_hash)?String(x.content_hash):String(y.content_hash);
          const b=String(x.content_hash)<String(y.content_hash)?String(y.content_hash):String(x.content_hash);
          const rel=dist<=4&&lr>=0.90?'near_duplicate':'coverage_candidate';
          stmts.push({sql:"INSERT OR IGNORE INTO final_audit_near_v1(a,b,hamming,length_ratio,relation,band,created_at) VALUES(?,?,?,?,?,?,?)",args:[a,b,dist,lr,rel,'axis'+axis+':'+key,now()]});
          if(stmts.length>=250){await retry('near_write',()=>turso.batch(stmts.splice(0),'write'));inserted+=250}
        }
      }
      if(stmts.length){const n=stmts.length;await retry('near_write',()=>turso.batch(stmts,'write'));inserted+=n}
      await setState('near_axis',axis);await setState('near_bucket',bucketN+1);await setState('near_inserted',inserted);await setState('near_oversized_groups',oversized);
    }
    bucketN=0;await setState('near_axis',axis+1);await setState('near_bucket',0);
  }
  const n=await q("SELECT relation,count(*) n FROM final_audit_near_v1 GROUP BY relation ORDER BY relation");
  await setState('near_complete','1');
  return {rows:n,attemptedInserts:inserted,oversizedGroups:oversized};
}
function recommendation(key,left,right){
  if(key==='git_destructive')return 'forbid';
  if(key==='production')return 'approval';
  if(key==='priority')return 'security';
  if(key==='provider')return 'agnostic';
  if(key==='branch')return 'pr';
  if(key==='testing')return 'first';
  if(key==='clarification')return 'proceed, except materially ambiguous/high-risk actions';
  if(key==='memory')return 'stateless by default; persistent only when explicitly scoped';
  return left;
}
async function conflicts(){
  const counts=new Map(),examples=new Map();let last='';
  while(true){
    const rows=await q("SELECT content_hash,policy_sig,sample_locator FROM final_audit_exact_v1 WHERE (sdlc_mask<>0 OR social_mask<>0) AND policy_sig<>'' AND content_hash>? ORDER BY content_hash LIMIT 5000",[last]);
    if(!rows.length)break;
    for(const r of rows){last=String(r.content_hash);for(const s of String(r.policy_sig).split(';').filter(Boolean)){counts.set(s,(counts.get(s)||0)+1);if(!examples.has(s))examples.set(s,{contentHash:r.content_hash,locator:r.sample_locator})}}
  }
  const pairs=[['clarification','ask','proceed'],['testing','first','after'],['git_destructive','forbid','allow'],['production','approval','auto'],['priority','security','speed'],['memory','stateless','persistent'],['branch','pr','direct_main']];
  const out=[];
  for(const [k,a,b] of pairs){const ka=k+'='+a,kb=k+'='+b;if((counts.get(ka)||0)&&(counts.get(kb)||0))out.push({topic:k,left:{stance:a,count:counts.get(ka),example:examples.get(ka)},right:{stance:b,count:counts.get(kb),example:examples.get(kb)},recommendation:recommendation(k,a,b),yourSelection:''})}
  const ag=counts.get('provider=agnostic')||0,forced=[...counts.entries()].filter(([k])=>k.startsWith('provider=forced:')).sort((a,b)=>b[1]-a[1]);
  if(ag&&forced.length)out.push({topic:'provider',left:{stance:'agnostic',count:ag,example:examples.get('provider=agnostic')},right:{stance:'provider_forced',count:forced.reduce((s,x)=>s+x[1],0),variants:forced.slice(0,30).map(([x,n])=>({stance:x.slice('provider='.length),count:n,example:examples.get(x)}))},recommendation:'agnostic',yourSelection:''});
  const numeric=await q("SELECT p.policy_key,p.policy_value,count(*) n,min(p.content_hash) example_hash,min(p.sample_locator) example_locator FROM final_audit_policy_v2 p JOIN final_audit_exact_v1 e ON e.content_hash=p.content_hash WHERE (e.sdlc_mask<>0 OR e.social_mask<>0) GROUP BY p.policy_key,p.policy_value ORDER BY p.policy_key,n DESC");
  const byKey=new Map();
  for(const r of numeric){const k=String(r.policy_key);if(!byKey.has(k))byKey.set(k,[]);byKey.get(k).push({value:String(r.policy_value),count:Number(r.n),example:{contentHash:r.example_hash,locator:r.example_locator}})}
  for(const [k,vals] of byKey){
    if(vals.length<2)continue;
    const isTool=k.startsWith('tool:');
    const conflictVals=isTool?vals.filter(v=>v.value==='never'||v.value==='prefer'):vals;
    if(conflictVals.length<2)continue;
    out.push({
      topic:k,
      variants:conflictVals.slice(0,50),
      recommendation:isTool?'prefer portable/default tooling; never-ban only when safety or compatibility requires it':'avoid one global hard-coded value; choose a context/risk-specific default with an override',
      yourSelection:''
    });
  }
  return {generatedAt:now(),status:'COMPLETE_TEXT_LEVEL_POLICY_CONFLICTS',conflicts:out};
}
async function main(){
  await ensure();const ready=await readiness();
  const ingestOk=ready.done.legacy===ready.expected.legacy&&ready.done.gitB2===ready.expected.gitB2&&ready.done.skillsB2===ready.expected.skillsB2;
  const policyOk=ready.policyDone.legacy===ready.expected.legacy&&ready.policyDone.gitB2===ready.expected.gitB2&&ready.policyDone.skillsB2===ready.expected.skillsB2;
  const ok=ingestOk&&policyOk;
  await fs.mkdir('audit',{recursive:true});
  if(!ok){const out={generatedAt:now(),ready:false,ingestReady:ingestOk,policyReady:policyOk,readiness:ready};await fs.writeFile('audit/final-audit-reduce-status.json',JSON.stringify(out,null,2)+'\n');console.log(JSON.stringify(out,null,2));return}
  const sum=await summary();const nearOut=await near();const conflict=await conflicts();
  const out={generatedAt:now(),ready:true,readiness:ready,summary:sum,near:nearOut,conflictRegistryStatus:conflict.status};
  await fs.writeFile('audit/final-audit-reduce-status.json',JSON.stringify(out,null,2)+'\n');
  await fs.writeFile('audit/conflict-registry-preliminary.json',JSON.stringify(conflict,null,2)+'\n');
  console.log(JSON.stringify(out,null,2));
}
main().catch(e=>{console.error(e?.stack||String(e));process.exit(1)});
