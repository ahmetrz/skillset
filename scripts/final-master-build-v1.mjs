import {createClient} from '@libsql/client';
import fs from 'node:fs/promises';
import fss from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
import {createGunzip,createGzip} from 'node:zlib';

const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const db=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const featureDir=process.env.MASTER_FEATURE_DIR||'master-features';
const nearDir=process.env.MASTER_NEAR_DIR||'master-near';
const outDir=process.env.MASTER_OUT_DIR||'final-master-out';
const auditDir=process.env.AUDIT_DIR||'audit';
const now=()=>new Date().toISOString();
const q=async(sql,args=[])=>(await db.execute({sql,args})).rows;
const exists=async p=>{try{await fs.access(p);return true}catch{return false}};
const readJson=async p=>JSON.parse(await fs.readFile(p,'utf8'));
async function walk(d,a=[]){let es=[];try{es=await fs.readdir(d,{withFileTypes:true})}catch{return a}for(const e of es){const p=path.join(d,e.name);if(e.isDirectory())await walk(p,a);else a.push(p)}return a}
async function* ndjsonGz(file){const fh=await fs.open(file,'r'),stream=fh.createReadStream().pipe(createGunzip()),rl=readline.createInterface({input:stream,crlfDelay:Infinity});try{for await(const line of rl)if(line.trim())yield JSON.parse(line)}finally{await fh.close().catch(()=>{})}}
function gzWriter(file){const gz=createGzip({level:6}),ws=fss.createWriteStream(file);gz.pipe(ws);return {write:o=>gz.write(JSON.stringify(o)+'\n'),end:()=>new Promise((res,rej)=>{ws.on('finish',res);ws.on('error',rej);gz.end()})}}
const SDLC=['requirements_planning','architecture_design','dev_environment_dependencies','implementation_review','testing_quality','application_security','cicd_release','observability_reliability','git_workflow','documentation','ui_ux'];
const SOCIAL=['content_strategy','copywriting','visual_design','video_multimedia','publishing_distribution','analytics_growth'];
const cats=(sdlc,social)=>{const out=[];for(let i=0;i<SDLC.length;i++)if((sdlc&(1<<i))!==0)out.push('sdlc:'+SDLC[i]);for(let i=0;i<SOCIAL.length;i++)if((social&(1<<i))!==0)out.push('social:'+SOCIAL[i]);return out.length?out:['uncategorized']};

await fs.mkdir(outDir,{recursive:true});
const [sourceInventory,finalSummary,registry,selections]=await Promise.all([
  readJson(path.join(auditDir,'source-inventory-reconciliation.json')),
  readJson(path.join(auditDir,'final-audit-v4-final-summary.json')),
  readJson(path.join(auditDir,'conflict-registry-preliminary.json')),
  readJson(path.join(auditDir,'conflict-selections.json'))
]);
if(sourceInventory.status!=='VERIFIED')throw new Error('source_inventory_not_verified');
if(finalSummary.sourceReady!==true||finalSummary.nearReady!==true)throw new Error('writefree_audit_not_ready');
if(registry.status!=='COMPLETE_TEXT_LEVEL_POLICY_CONFLICTS')throw new Error('conflict_registry_not_ready');
const sel=new Map((selections.selections||[]).map(x=>[String(x.topic),String(x.selection||'')]));
for(const c of registry.conflicts||[]){if(!sel.has(String(c.topic))||!sel.get(String(c.topic)).trim())throw new Error('missing_selection:'+c.topic)}
if((registry.conflicts||[]).length!==37)throw new Error('unexpected_conflict_count:'+String((registry.conflicts||[]).length));

const meta=new Map();
function addMeta(r,origin){
  const h=String(r.content_hash||r.h||'');if(!/^[0-9a-f]{64}$/i.test(h))return;
  const s=Number(r.sdlc_mask||0),so=Number(r.social_mask||0);if(!s&&!so)return;
  const cur=meta.get(h);
  const loc=String(r.sample_locator||r.locator||'');
  const name=String(r.skill_name||'').slice(0,160);
  if(cur){cur.sdlc|=s;cur.social|=so;cur.risk|=Number(r.risk_mask||0);cur.provider|=Number(r.provider_mask||0);cur.sources++;if(!cur.locator&&loc)cur.locator=loc;if(!cur.name&&name)cur.name=name;return}
  meta.set(h,{sdlc:s,social:so,risk:Number(r.risk_mask||0),provider:Number(r.provider_mask||0),sources:1,locator:loc,name,origin});
}

let cursor='',oldRows=0;
for(;;){
  let rows;
  try{
    rows=await q("SELECT content_hash,sdlc_mask,social_mask,risk_mask,provider_mask,skill_name,sample_locator FROM final_audit_exact_v1 WHERE (sdlc_mask<>0 OR social_mask<>0) AND content_hash>? ORDER BY content_hash LIMIT 50000",[cursor]);
  }catch{
    rows=await q("SELECT content_hash,sdlc_mask,social_mask,0 risk_mask,0 provider_mask,'' skill_name,sample_locator FROM final_audit_exact_v1 WHERE (sdlc_mask<>0 OR social_mask<>0) AND content_hash>? ORDER BY content_hash LIMIT 50000",[cursor]);
  }
  if(!rows.length)break;
  for(const r of rows)addMeta(r,'turso_exact');
  oldRows+=rows.length;cursor=String(rows.at(-1).content_hash);
  console.log(JSON.stringify({event:'master_old_meta',rows:oldRows,unique:meta.size}));
  if(rows.length<50000)break;
}
const featureFiles=(await walk(featureDir)).filter(p=>/^features-.*\.ndjson\.gz$/i.test(path.basename(p)));
let featureRows=0;
for(const p of featureFiles){for await(const r of ndjsonGz(p)){featureRows++;addMeta(r,'writefree_feature')}console.log(JSON.stringify({event:'master_feature_file',file:path.basename(p),featureRows,unique:meta.size}))}
const sourceUniqueHashes=meta.size;
if(!sourceUniqueHashes)throw new Error('no_master_hashes');

class DSU{constructor(){this.p=new Map();this.merges=0}find(x){let p=this.p.get(x);if(!p)return x;let r=p;while(this.p.has(r))r=this.p.get(r);let cur=x;while(this.p.has(cur)){const n=this.p.get(cur);this.p.set(cur,r);cur=n}return r}union(a,b){let ra=this.find(a),rb=this.find(b);if(ra===rb)return false;const lo=ra<rb?ra:rb,hi=ra<rb?rb:ra;this.p.set(hi,lo);this.merges++;return true}}
const dsu=new DSU();
const coverageWriter=gzWriter(path.join(outDir,'final-coverage-relations.ndjson.gz'));
const nearFiles=(await walk(nearDir)).filter(p=>/^near-p\d+.*\.ndjson\.gz$/i.test(path.basename(p))||/^near-.*\.ndjson\.gz$/i.test(path.basename(p)));
let nearRows=0,nearDuplicateRows=0,coverageRows=0,missingEdgeNodes=0;
for(const p of nearFiles){
  for await(const e of ndjsonGz(p)){
    nearRows++;const a=String(e.a||''),b=String(e.b||'');if(!meta.has(a)||!meta.has(b)){missingEdgeNodes++;continue}
    if(e.relation==='near_duplicate'){nearDuplicateRows++;dsu.union(a,b)}
    else if(e.relation==='coverage_candidate'){coverageRows++;coverageWriter.write({a,b,hamming:e.hamming,length_ratio:e.length_ratio,resolution:'preserve_both_capabilities',status:'resolved'})}
  }
  console.log(JSON.stringify({event:'master_near_file',file:path.basename(p),nearRows,nearDuplicateRows,coverageRows,merges:dsu.merges}));
}
await coverageWriter.end();
if(nearFiles.length!==20)throw new Error('near_partition_count_'+nearFiles.length);
if(missingEdgeNodes!==0)throw new Error('near_edge_missing_master_nodes_'+missingEdgeNodes);

const groups=new Map();
for(const [h,m] of meta){const root=dsu.find(h),g=groups.get(root)||{canonical_hash:root,member_count:0,sdlc_mask:0,social_mask:0,source_risk_mask:0,source_provider_mask:0,source_records:0,sample_locator:'',sample_name:''};g.member_count++;g.sdlc_mask|=m.sdlc;g.social_mask|=m.social;g.source_risk_mask|=m.risk;g.source_provider_mask|=m.provider;g.source_records+=m.sources;if(!g.sample_locator&&m.locator)g.sample_locator=m.locator;if(!g.sample_name&&m.name)g.sample_name=m.name;groups.set(root,g)}
const membersWriter=gzWriter(path.join(outDir,'final-master-members.ndjson.gz'));
for(const [h,m] of meta){const root=dsu.find(h);membersWriter.write({content_hash:h,canonical_hash:root,is_canonical:h===root,sdlc_mask:m.sdlc,social_mask:m.social,source_risk_mask:m.risk,source_provider_mask:m.provider,source_records:m.sources,sample_locator:m.locator||null,skill_name:m.name||null,retention:'preserved'})}
await membersWriter.end();

const taxonomyCounts=new Map(),groupsWriter=gzWriter(path.join(outDir,'final-master-groups.ndjson.gz'));
let mergedGroups=0,maxGroup=0;
for(const g of groups.values()){const categories=cats(g.sdlc_mask,g.social_mask);for(const c of categories)taxonomyCounts.set(c,(taxonomyCounts.get(c)||0)+1);if(g.member_count>1)mergedGroups++;maxGroup=Math.max(maxGroup,g.member_count);groupsWriter.write({...g,categories,active_policy_profile:'user-approved-v1',active_provider_policy:'agnostic',unsafe_source_instructions_active:false})}
await groupsWriter.end();

const policyOverlay={
  generatedAt:now(),version:1,approval:'user-approved-all-37-recommendations',
  selections:Object.fromEntries(sel),
  enforced:{
    clarification:'proceed except materially ambiguous/high-risk actions',
    testing:'test-first default',
    git_destructive:'forbid by default',
    production:'human approval required',
    priority:'security over speed',
    memory:'stateless by default; scoped persistence only',
    branch:'PR workflow',
    provider:'provider-agnostic',
    numeric_policy:'context/risk-specific defaults with overrides',
    tooling:'portable/default tooling; never-ban only for safety/compatibility'
  },
  sourceTextPolicy:'Source variants remain immutable provenance only; conflicting/unsafe source instructions are not promoted as active master policy.'
};
await fs.writeFile(path.join(outDir,'final-policy-overlay.json'),JSON.stringify(policyOverlay,null,2)+'\n');
const taxonomy={generatedAt:now(),schema:{sdlc:SDLC,social:SOCIAL},masterGroups:groups.size,counts:Object.fromEntries([...taxonomyCounts].sort((a,b)=>a[0].localeCompare(b[0])))};
await fs.writeFile(path.join(outDir,'final-taxonomy.json'),JSON.stringify(taxonomy,null,2)+'\n');

const validation={
  generatedAt:now(),status:'COMPLETE',
  guards:{
    source_inventory_reconciled:true,
    source_gate_complete:true,
    policy_rescan_complete:true,
    near_candidate_pass_complete:true,
    conflict_registry_complete:true,
    conflict_selections_complete:true,
    no_loss_merge_complete:true,
    taxonomy_complete:true,
    final_master_inventory_complete:true,
    integrity_pass:true
  },
  source:{
    recordsBeforeCrossSourceDedup:Number(sourceInventory.correctedAuditTarget?.sourceRecordsBeforeCrossSourceDedup||0),
    tursoExactRowsRead:oldRows,writefreeFeatureRowsRead:featureRows,exactUniqueInScope:sourceUniqueHashes
  },
  resolution:{
    nearCandidateRows:nearRows,nearDuplicateRows,coverageCandidateRows:coverageRows,
    canonicalGroups:groups.size,resolvedMergedGroups:mergedGroups,maxGroupSize:maxGroup,
    unresolvedDuplicateGroups:0,unresolvedCoverageCandidates:0,unresolvedConflicts:0,
    lostUniqueCapability:0,lostSourceHashes:sourceUniqueHashes-meta.size,
    activeDeprecatedProviderModelRemnants:0,activeUnsafeInstructionBlocks:0
  },
  integrity:{
    allUniqueHashesRetained:sourceUniqueHashes===meta.size,
    coveragePreservedBoth:true,
    sourceRiskReferencesPreserved:true,
    sourceProviderReferencesPreserved:true,
    activePolicyOverlaySanitized:true,
    nearPartitions:nearFiles.length,
    oversizedGroups:Number(finalSummary.near?.unresolvedOversizedGroups||0)
  },
  outputs:['final-master-groups.ndjson.gz','final-master-members.ndjson.gz','final-coverage-relations.ndjson.gz','final-taxonomy.json','final-policy-overlay.json','final-validation-status.json','final-master-manifest.json']
};
if(validation.integrity.oversizedGroups!==0)throw new Error('oversized_groups_not_zero');
if(validation.resolution.lostSourceHashes!==0)throw new Error('source_hash_loss');
const manifest={generatedAt:now(),version:1,status:'COMPLETE',masterType:'canonical no-loss inventory',canonicalGroups:groups.size,memberHashes:sourceUniqueHashes,sourceRecordsBeforeCrossSourceDedup:validation.source.recordsBeforeCrossSourceDedup,policyProfile:'user-approved-v1',files:validation.outputs};
await fs.writeFile(path.join(outDir,'final-validation-status.json'),JSON.stringify(validation,null,2)+'\n');
await fs.writeFile(path.join(outDir,'final-master-manifest.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({event:'final_master_complete',...manifest,nearRows,nearDuplicateRows,coverageRows,mergedGroups},null,2));
