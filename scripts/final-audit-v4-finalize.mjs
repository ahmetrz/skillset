import {createClient} from '@libsql/client';
import fs from 'node:fs/promises';
import path from 'node:path';
import {createGunzip} from 'node:zlib';
import readline from 'node:readline';

const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const turso=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const input=process.env.AUDIT_V4_INPUT||'final-assets',outDir=process.env.AUDIT_V4_FINAL_OUT||'audit';
async function q(sql,args=[]){return (await turso.execute({sql,args})).rows}
async function files(dir){const out=[];for(const e of await fs.readdir(dir,{withFileTypes:true})){const p=path.join(dir,e.name);if(e.isDirectory())out.push(...await files(p));else out.push(p)}return out}
async function* ndjsonGz(file){
  const fh=await fs.open(file,'r'),stream=fh.createReadStream().pipe(createGunzip());
  const rl=readline.createInterface({input:stream,crlfDelay:Infinity});
  try{for await(const line of rl)if(line.trim())yield JSON.parse(line)}
  finally{await fh.close().catch(()=>{})}
}
await fs.mkdir(outDir,{recursive:true});
const all=await files(input);
const statusFile=all.find(x=>path.basename(x)==='final-audit-v4-status.json');
if(!statusFile)throw new Error('v4 source status missing');
const sourceStatus=JSON.parse(await fs.readFile(statusFile,'utf8'));
const nearManifests=[];
for(const p of all.filter(x=>/^near-manifest-.*\.json$/.test(path.basename(x))))nearManifests.push(JSON.parse(await fs.readFile(p,'utf8')));
const nearPartitions=new Set(nearManifests.map(x=>Number(x.partition)));
const unresolved=nearManifests.reduce((s,x)=>s+(Array.isArray(x.oversized)?x.oversized.length:Number(x.oversized||0)),0);
const nearCounts={
  partitions:nearPartitions.size,
  candidates:nearManifests.reduce((s,x)=>s+Number(x.candidates||0),0),
  nearDuplicates:nearManifests.reduce((s,x)=>s+Number(x.nearDuplicates||0),0),
  coverageCandidates:nearManifests.reduce((s,x)=>s+Number(x.coverageCandidates||0),0),
  unresolvedOversizedGroups:unresolved
};

const featureFiles=all.filter(x=>/^features-.*\.ndjson\.gz$/.test(path.basename(x)));
const newPolicy=new Map(),newFacts=new Map(),newExamples=new Map(),seenHashes=new Set();
function add(map,k,n=1){map.set(k,(map.get(k)||0)+n)}
for(const p of featureFiles){
  for await(const r of ndjsonGz(p)){
    if((Number(r.sdlc_mask)||0)===0&&(Number(r.social_mask)||0)===0)continue;
    const h=String(r.content_hash||'');if(!h||seenHashes.has(h))continue;seenHashes.add(h);
    for(const s of String(r.policy_sig||'').split(';').filter(Boolean)){add(newPolicy,s);if(!newExamples.has(s))newExamples.set(s,{contentHash:h,locator:r.sample_locator||r.locator})}
    for(const f of r.policy_facts||[]){const k=String(f.key)+'='+String(f.value);add(newFacts,k);if(!newExamples.has(k))newExamples.set(k,{contentHash:h,locator:r.sample_locator||r.locator,evidence:f.evidence})}
  }
}
async function oldStanceCount(stance){
  const rows=await q("SELECT count(*) n,min(content_hash) example_hash,min(sample_locator) example_locator FROM final_audit_exact_v1 WHERE (sdlc_mask<>0 OR social_mask<>0) AND (';'||policy_sig||';') LIKE ?",['%;'+stance+';%']);
  return {count:Number(rows[0]?.n||0),example:rows[0]?.example_hash?{contentHash:rows[0].example_hash,locator:rows[0].example_locator}:null};
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
const pairs=[['clarification','ask','proceed'],['testing','first','after'],['git_destructive','forbid','allow'],['production','approval','auto'],['priority','security','speed'],['memory','stateless','persistent'],['branch','pr','direct_main']];
const conflicts=[];
for(const [k,a,b] of pairs){
  const ka=k+'='+a,kb=k+'='+b,[oa,ob]=await Promise.all([oldStanceCount(ka),oldStanceCount(kb)]);
  const ca=oa.count+(newPolicy.get(ka)||0),cb=ob.count+(newPolicy.get(kb)||0);
  if(ca&&cb)conflicts.push({topic:k,left:{stance:a,count:ca,example:oa.example||newExamples.get(ka)},right:{stance:b,count:cb,example:ob.example||newExamples.get(kb)},recommendation:recommendation(k,a,b),yourSelection:''});
}
const providerNames=['claude','opus','sonnet','anthropic','openai','codex','gemini','cursor','copilot','cline','aider'];
const ag=await oldStanceCount('provider=agnostic');
const forced=[];
for(const p of providerNames){
  const stance='provider=forced:'+p,o=await oldStanceCount(stance),n=o.count+(newPolicy.get(stance)||0);
  if(n)forced.push({stance:'forced:'+p,count:n,example:o.example||newExamples.get(stance)});
}
const agCount=ag.count+(newPolicy.get('provider=agnostic')||0);
if(agCount&&forced.length)conflicts.push({topic:'provider',left:{stance:'agnostic',count:agCount,example:ag.example||newExamples.get('provider=agnostic')},right:{stance:'provider_forced',count:forced.reduce((s,x)=>s+x.count,0),variants:forced},recommendation:'agnostic',yourSelection:''});

const oldFacts=await q("SELECT policy_key,policy_value,count(*) n,min(content_hash) example_hash,min(sample_locator) example_locator FROM final_audit_policy_v2 GROUP BY policy_key,policy_value ORDER BY policy_key,n DESC");
const byKey=new Map();
for(const r of oldFacts){
  const k=String(r.policy_key),v=String(r.policy_value),id=k+'='+v;
  if(!byKey.has(k))byKey.set(k,new Map());
  byKey.get(k).set(v,{value:v,count:Number(r.n||0),example:{contentHash:r.example_hash,locator:r.example_locator}});
}
for(const [id,n] of newFacts){
  const pos=id.indexOf('='),k=id.slice(0,pos),v=id.slice(pos+1);
  if(!byKey.has(k))byKey.set(k,new Map());
  const vm=byKey.get(k),cur=vm.get(v)||{value:v,count:0,example:newExamples.get(id)};
  cur.count+=n;if(!cur.example)cur.example=newExamples.get(id);vm.set(v,cur);
}
for(const [k,vm] of byKey){
  let vals=[...vm.values()];
  if(k.startsWith('tool:'))vals=vals.filter(v=>v.value==='never'||v.value==='prefer');
  if(vals.length<2)continue;
  conflicts.push({topic:k,variants:vals.sort((a,b)=>b.count-a.count).slice(0,50),recommendation:k.startsWith('tool:')?'prefer portable/default tooling; never-ban only when safety or compatibility requires it':'avoid one global hard-coded value; choose a context/risk-specific default with an override',yourSelection:''});
}

const sourceReady=sourceStatus.complete===true,nearReady=nearPartitions.size===20&&unresolved===0;
const registry={generatedAt:new Date().toISOString(),status:sourceReady&&nearReady?'COMPLETE_TEXT_LEVEL_POLICY_CONFLICTS':'BLOCKED_INCOMPLETE_AUDIT',sourceReady,nearReady,conflicts};
const validation={
  generatedAt:new Date().toISOString(),
  status:sourceReady?(nearReady?'AWAITING_USER_CONFLICT_SELECTIONS':'REDUCING'):'INGESTING',
  guards:{
    source_inventory_reconciled:true,
    ingest_complete:sourceReady,
    policy_rescan_complete:sourceReady,
    near_candidate_pass_complete:nearPartitions.size===20,
    oversized_lsh_groups_resolved:unresolved===0,
    conflict_registry_complete:sourceReady&&nearReady,
    conflict_selections_complete:false,
    destructive_master_build_allowed:false
  },
  sourceStatus,near:nearCounts,conflictCount:conflicts.length,selectionsFilePresent:false
};
await fs.writeFile(path.join(outDir,'conflict-registry-preliminary.json'),JSON.stringify(registry,null,2)+'\n');
await fs.writeFile(path.join(outDir,'final-validation-status.json'),JSON.stringify(validation,null,2)+'\n');
await fs.writeFile(path.join(outDir,'final-audit-v4-final-summary.json'),JSON.stringify({generatedAt:new Date().toISOString(),sourceReady,nearReady,near:nearCounts,conflictCount:conflicts.length,newUniquePolicyHashesScanned:seenHashes.size},null,2)+'\n');
console.log(JSON.stringify(validation,null,2));
