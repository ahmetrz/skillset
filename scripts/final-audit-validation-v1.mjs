import { createClient } from '@libsql/client';
import fs from 'node:fs/promises';

const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const turso=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const now=()=>new Date().toISOString();
async function q(sql,args=[]){try{return (await turso.execute({sql,args})).rows}catch(e){return [{error:String(e?.message||e)}]}}
async function exists(path){try{await fs.access(path);return true}catch{return false}}
async function readJson(path){try{return JSON.parse(await fs.readFile(path,'utf8'))}catch{return null}}

const units=await q("SELECT source_system,status,count(*) units FROM final_audit_unit_v1 GROUP BY source_system,status");
const policyUnits=await q("SELECT source_system,status,count(*) units FROM final_audit_policy_unit_v2 GROUP BY source_system,status");
const m=new Map(units.filter(x=>x.status==='done').map(x=>[String(x.source_system),Number(x.units)]));
const pm=new Map(policyUnits.filter(x=>x.status==='done').map(x=>[String(x.source_system),Number(x.units)]));
const reduceCheckpoint=await readJson('audit/final-audit-reduce-status.json');
const expected=reduceCheckpoint?.readiness?.expected||{legacy:19132,gitB2:642,skillsB2:7557};
const ingestDone={legacy:m.get('gitskills-legacy-hf')||0,gitB2:m.get('gitskills-b2')||0,skillsB2:m.get('skills-sh-b2')||0};
const policyDone={legacy:pm.get('gitskills-legacy-hf')||0,gitB2:pm.get('gitskills-b2')||0,skillsB2:pm.get('skills-sh-b2')||0};
const ingestReady=Object.keys(expected).every(k=>ingestDone[k]===expected[k]);
const policyReady=Object.keys(expected).every(k=>policyDone[k]===expected[k]);
const state=await q("SELECT key,value FROM final_audit_reduce_state_v1");
const sm=new Map(state.map(x=>[String(x.key),String(x.value)]));
const nearReady=sm.get('near_complete')==='1';
const oversizedGroups=Number(sm.get('near_oversized_groups')||0);
const conflict=await readJson('audit/conflict-registry-preliminary.json');
const conflictReady=conflict?.status==='COMPLETE_TEXT_LEVEL_POLICY_CONFLICTS';
const selectionPath='audit/conflict-selections.json';
const selections=await readJson(selectionPath);
const conflicts=Array.isArray(conflict?.conflicts)?conflict.conflicts:[];
const selMap=new Map((selections?.selections||[]).map(x=>[String(x.topic),x.selection]));
const selectionsComplete=conflictReady&&conflicts.length>0&&conflicts.every(x=>selMap.has(String(x.topic))&&String(selMap.get(String(x.topic))||'').trim()!=='');
const exact=await q("SELECT count(*) n FROM final_audit_exact_v1");
const occ=await q("SELECT count(*) n FROM final_audit_occurrence_v1");
const near=await q("SELECT relation,count(*) n FROM final_audit_near_v1 GROUP BY relation ORDER BY relation");
const guards={
  ingest_complete:ingestReady,
  policy_rescan_complete:policyReady,
  near_candidate_pass_complete:nearReady,
  oversized_lsh_groups_resolved:nearReady&&oversizedGroups===0,
  conflict_registry_complete:conflictReady,
  conflict_selections_complete:selectionsComplete,
  destructive_master_build_allowed:ingestReady&&policyReady&&nearReady&&oversizedGroups===0&&conflictReady&&selectionsComplete
};
let status='BLOCKED';
if(!ingestReady||!policyReady)status='INGESTING';
else if(!nearReady||oversizedGroups>0)status='REDUCING';
else if(!conflictReady)status='BUILDING_CONFLICT_REGISTRY';
else if(!selectionsComplete)status='AWAITING_USER_CONFLICT_SELECTIONS';
else status='READY_FOR_MASTER_BUILD';
const out={generatedAt:now(),status,guards,expected,ingestDone,policyDone,oversizedGroups,exactUnique:exact[0],occurrences:occ[0],near,conflictCount:conflicts.length,selectionsFilePresent:await exists(selectionPath)};
await fs.mkdir('audit',{recursive:true});
await fs.writeFile('audit/final-validation-status.json',JSON.stringify(out,null,2)+'\n');
console.log(JSON.stringify(out,null,2));
if(status==='READY_FOR_MASTER_BUILD')process.exit(0);
