import {createClient} from '@libsql/client';
import fs from 'node:fs/promises';
import path from 'node:path';
import {gzipSync} from 'node:zlib';
import {feature,numericFacts} from './final-audit-feature-core-v4.mjs';
import {rebuildGitSkillsPack,rebuildSkillsShPackHistorical} from './final-audit-source-fallback-v1.mjs';

const SRC=process.env.AUDIT_SOURCE||'',PART=Number(process.env.AUDIT_PARTITION||0),PARTS=Math.max(1,Number(process.env.AUDIT_PARTITIONS||1));
const FIRST=100000,LAST=119131;
const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const turso=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const bucket=need('B2_BUCKET');
const outDir=process.env.AUDIT_OUT_DIR||'audit-v4-out';
const skipDir=process.env.AUDIT_SKIP_MANIFEST_DIR||'';
const sleep=ms=>new Promise(r=>setTimeout(r,ms));

async function q(sql,args=[]){return (await turso.execute({sql,args})).rows}
async function releaseDoneSet(sys){
  const set=new Set();
  if(!skipDir)return set;
  async function walk(dir){
    let es=[];try{es=await fs.readdir(dir,{withFileTypes:true})}catch{return}
    for(const e of es){
      const p=path.join(dir,e.name);
      if(e.isDirectory())await walk(p);
      else if(/^manifest-.*\.json$/i.test(e.name)){
        try{
          const m=JSON.parse(await fs.readFile(p,'utf8'));
          for(const u of m.units||[])if(String(u.source_system)===sys&&u.status==='done')set.add(String(u.source_key));
        }catch{}
      }
    }
  }
  await walk(skipDir);
  return set;
}
async function doneSets(sys){
  const [a,b]=await Promise.all([
    q("SELECT source_key FROM final_audit_unit_v1 WHERE source_system=? AND status='done'",[sys]),
    q("SELECT source_key FROM final_audit_policy_unit_v2 WHERE source_system=? AND status='done'",[sys])
  ]);
  return {ingest:new Set(a.map(x=>String(x.source_key))),policy:new Set(b.map(x=>String(x.source_key)))};
}
function needed(k,sets,releaseDone){return !releaseDone.has(k)&&(!sets.ingest.has(k)||!sets.policy.has(k))}
async function units(){
  if(SRC==='git-legacy'){
    const sys='gitskills-legacy-hf',sets=await doneSets(sys),releaseDone=await releaseDoneSet(sys),all=[];
    for(let id=FIRST;id<=LAST;id++){const k='hf:gitskills:'+id;if(needed(k,sets,releaseDone))all.push({key:String(id),sourceKey:k})}
    return {sys,all:all.filter((_,i)=>i%PARTS===PART),totalMissing:all.length};
  }
  if(SRC==='git-b2'){
    const sys='gitskills-b2',sets=await doneSets(sys),releaseDone=await releaseDoneSet(sys);
    const rows=await q("SELECT path FROM gitskills_packs WHERE path LIKE 'gitskills/discovery-b2-v2/%' ORDER BY path");
    const all=rows.map(x=>String(x.path)).filter(k=>needed('b2:'+k,sets,releaseDone)).map(k=>({key:k,sourceKey:'b2:'+k}));
    return {sys,all:all.filter((_,i)=>i%PARTS===PART),totalMissing:all.length};
  }
  if(SRC==='skills-b2'){
    const sys='skills-sh-b2',sets=await doneSets(sys),releaseDone=await releaseDoneSet(sys);
    const rows=await q("SELECT b2_path FROM skills_sh_external_exact_v1 WHERE status='done' AND b2_path IS NOT NULL ORDER BY b2_path");
    const prefix='b2://'+bucket+'/';
    const all=rows.map(x=>String(x.b2_path)).filter(x=>x.startsWith(prefix)).map(x=>x.slice(prefix.length)).filter(k=>needed('b2:'+k,sets,releaseDone)).map(k=>({key:k,sourceKey:'b2:'+k}));
    return {sys,all:all.filter((_,i)=>i%PARTS===PART),totalMissing:all.length};
  }
  throw new Error('Unknown AUDIT_SOURCE '+SRC);
}
async function loadUnit(u){
  if(SRC==='git-legacy'){
    const p=await rebuildGitSkillsPack('gitskills/discovery-b2-v2/pack-'+u.key+'.json.gz');
    const s=p.shards?.[0]||{rows:[]},items=[];
    for(const r of s.rows||[]){const text=String(r.content||'');if(text)items.push({itemKey:String(r.row_idx??items.length),text,repo:r.repo_full_name||'',path:r.path||r.filename||'',locator:'hf://mvaccargiu/gitskills/artifacts/'+String(r.row_idx??items.length)})}
    return {items,rowsScanned:100,snapshot:null};
  }
  if(SRC==='git-b2'){
    const p=await rebuildGitSkillsPack(u.key),items=[];let rowsScanned=0;
    for(const s of p.shards||[])for(const r of s.rows||[]){rowsScanned++;const text=String(r.content||'');if(text)items.push({itemKey:String(r.row_idx??(s.shardId+':'+items.length)),text,repo:r.repo_full_name||'',path:r.path||r.filename||'',locator:'hf://mvaccargiu/gitskills/artifacts/'+String(r.row_idx??items.length)})}
    return {items,rowsScanned,snapshot:null};
  }
  const p=await rebuildSkillsShPackHistorical(turso,u.key,bucket),items=[];
  for(const f of p.files||[]){const bytes=Buffer.from(String(f.contentBase64||''),'base64');if(!bytes.length)continue;const fp=String(f.path||'');items.push({itemKey:fp,text:bytes.toString('utf8'),repo:p.repoSource||((p.owner||'')+'/'+(p.repo||'')),path:fp,locator:'git://'+String(p.owner||'')+'/'+String(p.repo||'')+'@'+String(p.commit||'')+'/'+fp})}
  return {items,rowsScanned:items.length,snapshot:{commit:p.commit,acquiredAt:p.acquiredAt,repo:p.repoSource}};
}
await fs.mkdir(outDir,{recursive:true});
const plan=await units(),records=[],unitResults=[];
let skills=0,inScope=0,factsN=0,failed=0;
for(let idx=0;idx<plan.all.length;idx++){
  const u=plan.all[idx];
  try{
    const loaded=await loadUnit(u),before=records.length;
    for(const x of loaded.items){
      const f=feature(x.text,x.locator),facts=numericFacts(x.text);
      if(f.sdlc_mask!==0||f.social_mask!==0)inScope++;
      factsN+=facts.length;skills++;
      records.push({
        source_key:u.sourceKey,item_key:String(x.itemKey),source_system:plan.sys,repo:x.repo||null,path:x.path||null,locator:x.locator,
        content_hash:f.h,simhash_hex:f.simhash_hex,c0:f.c0,c1:f.c1,c2:f.c2,c3:f.c3,char_count:f.char_count,token_count:f.token_count,
        sdlc_mask:f.sdlc_mask,social_mask:f.social_mask,risk_mask:f.risk_mask,provider_mask:f.provider_mask,policy_sig:f.policy_sig,
        skill_name:f.skill_name||null,sample_locator:f.sample_locator,policy_facts:facts
      });
    }
    unitResults.push({source_key:u.sourceKey,source_system:plan.sys,status:'done',rows_scanned:loaded.rowsScanned,skills_indexed:loaded.items.length,records:records.length-before,snapshot:loaded.snapshot});
  }catch(e){
    failed++;unitResults.push({source_key:u.sourceKey,source_system:plan.sys,status:'error',error:String(e?.message||e).slice(0,800)});
  }
  if((idx+1)%10===0)console.log(JSON.stringify({event:'progress',source:SRC,part:PART,done:idx+1,total:plan.all.length,skills,failed}));
  if(idx%25===0)await sleep(20);
}
const base=SRC+'-p'+String(PART).padStart(2,'0')+'-of-'+String(PARTS).padStart(2,'0');
const ndjson=records.map(x=>JSON.stringify(x)).join('\n')+(records.length?'\n':'');
await fs.writeFile(path.join(outDir,'features-'+base+'.ndjson.gz'),gzipSync(Buffer.from(ndjson),{level:6}));
const summary={
  generatedAt:new Date().toISOString(),source:SRC,sourceSystem:plan.sys,partition:PART,partitions:PARTS,
  totalMissingAtStart:plan.totalMissing,assignedUnits:plan.all.length,completedUnits:unitResults.filter(x=>x.status==='done').length,
  failedUnits:failed,skills,inScope,policyFacts:factsN,records:records.length,units:unitResults
};
await fs.writeFile(path.join(outDir,'manifest-'+base+'.json'),JSON.stringify(summary,null,2)+'\n');
console.log(JSON.stringify({event:'shard_complete',...summary,units:undefined}));
if(failed>Math.max(5,Math.ceil(plan.all.length*0.10)))process.exitCode=2;
