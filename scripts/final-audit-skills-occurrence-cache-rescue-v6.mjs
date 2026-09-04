import {createClient} from '@libsql/client';
import fs from 'node:fs/promises';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {gzipSync} from 'node:zlib';
import {feature,numericFacts} from './final-audit-feature-core-v4.mjs';

const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const db=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const dir=process.env.MANIFEST_DIR||'tail-manifests';
const outDir=process.env.RESCUE_OUT||'occ-rescue-out';
const bucket=need('B2_BUCKET');
const sha256=s=>createHash('sha256').update(s).digest('hex');
async function q(sql,args=[]){return (await db.execute({sql,args})).rows}
async function walk(d,out=[]){let es=[];try{es=await fs.readdir(d,{withFileTypes:true})}catch{return out}for(const e of es){const p=path.join(d,e.name);if(e.isDirectory())await walk(p,out);else if(/^manifest-.*\.json$/i.test(e.name))out.push(p)}return out}
function multiset(xs){const m=new Map();for(const x of xs)m.set(x,(m.get(x)||0)+1);return m}
function sameMulti(a,b){if(a.size!==b.size)return false;for(const [k,v] of a)if(b.get(k)!==v)return false;return true}
const releaseDone=new Set();
for(const p of await walk(dir)){try{const m=JSON.parse(await fs.readFile(p,'utf8'));for(const u of m.units||[])if(String(u.source_system)==='skills-sh-b2'&&u.status==='done')releaseDone.add(String(u.source_key))}catch{}}
const [di,dp,src]=await Promise.all([
 q("SELECT source_key FROM final_audit_unit_v1 WHERE source_system='skills-sh-b2' AND status='done'"),
 q("SELECT source_key FROM final_audit_policy_unit_v2 WHERE source_system='skills-sh-b2' AND status='done'"),
 q("SELECT source,owner,repo,b2_path,discovered_skills FROM skills_sh_external_exact_v1 WHERE status='done' AND b2_path IS NOT NULL ORDER BY b2_path")
]);
const ingest=new Set(di.map(x=>String(x.source_key))),policy=new Set(dp.map(x=>String(x.source_key)));
const prefix='b2://'+bucket+'/';
const unresolved=[];
for(const r of src){const b=String(r.b2_path||'');if(!b.startsWith(prefix))continue;const sk='b2:'+b.slice(prefix.length);if(releaseDone.has(sk)||(ingest.has(sk)&&policy.has(sk)))continue;unresolved.push({sourceKey:sk,source:String(r.source||''),owner:String(r.owner||''),repo:String(r.repo||''),expected:Number(r.discovered_skills||0)})}
await fs.mkdir(outDir,{recursive:true});
const records=[],units=[],report=[];let totalSkills=0,inScope=0,policyFacts=0;
for(const u of unresolved){
  const occ=await q("SELECT item_key,content_hash,repo,path,locator FROM final_audit_occurrence_v1 WHERE source_key=? ORDER BY item_key",[u.sourceKey]);
  if(occ.length!==u.expected){report.push({...u,occurrenceRows:occ.length,status:'skip_no_complete_occurrence_hashes'});continue}
  const html=await (await fetch('https://skills.sh/'+encodeURIComponent(u.owner)+'/'+encodeURIComponent(u.repo),{headers:{'user-agent':'skillset-occurrence-rescue/6.0'}})).text();
  const slugs=new Set();
  for(const m of html.matchAll(/href="\/([^"/]+)\/([^"/]+)\/([^"/?#]+)"/g))if(m[1].toLowerCase()===u.owner.toLowerCase()&&m[2].toLowerCase()===u.repo.toLowerCase())slugs.add(m[3]);
  const slugify=s=>String(s||'').toLowerCase().replace(/[\s_]+/g,'-').replace(/[^a-z0-9-]/g,'').replace(/-+/g,'-').replace(/^-|-$/g,'');
  for(const o of occ){
    const parts=String(o.item_key||'').replace(/\\/g,'/').split('/').filter(Boolean);
    if(parts.length>=2)slugs.add(slugify(parts[parts.length-2]));
  }
  slugs.add(slugify(u.repo));
  slugs.delete('');
  const downloaded=[];const errors=[];
  for(const slug of slugs){
    try{const res=await fetch('https://skills.sh/api/download/'+encodeURIComponent(u.owner)+'/'+encodeURIComponent(u.repo)+'/'+encodeURIComponent(slug),{headers:{'user-agent':'skillset-occurrence-rescue/6.0'}});if(!res.ok){errors.push({slug,status:res.status});continue}const j=await res.json();const sf=(Array.isArray(j.files)?j.files:[]).filter(x=>/(^|\/)SKILL\.md$/i.test(String(x.path||'')));if(sf.length!==1){errors.push({slug,error:'skill_md_count_'+sf.length});continue}const text=String(sf[0].contents||'');downloaded.push({slug,text,hash:sha256(text),snapshotHash:String(j.hash||'')})}catch(e){errors.push({slug,error:String(e?.message||e)})}
  }
  const byHash=new Map();for(const d of downloaded)if(!byHash.has(d.hash))byHash.set(d.hash,d);
  const missingHashes=[...new Set(occ.map(x=>String(x.content_hash)))].filter(h=>!byHash.has(h));
  if(missingHashes.length){report.push({...u,occurrenceRows:occ.length,candidateSlugs:slugs.size,downloaded:downloaded.length,missingHashes,errors,status:'skip_hash_coverage_mismatch'});continue}
  for(const o of occ){const h=String(o.content_hash),d=byHash.get(h);const f=feature(d.text,String(o.locator||'')),facts=numericFacts(d.text);if(f.h!==h)throw new Error('feature_hash_mismatch '+u.source);if(f.sdlc_mask!==0||f.social_mask!==0)inScope++;policyFacts+=facts.length;totalSkills++;records.push({source_key:u.sourceKey,item_key:String(o.item_key),source_system:'skills-sh-b2',repo:String(o.repo||u.source),path:String(o.path||o.item_key),locator:String(o.locator||''),content_hash:f.h,simhash_hex:f.simhash_hex,c0:f.c0,c1:f.c1,c2:f.c2,c3:f.c3,char_count:f.char_count,token_count:f.token_count,sdlc_mask:f.sdlc_mask,social_mask:f.social_mask,risk_mask:f.risk_mask,provider_mask:f.provider_mask,policy_sig:f.policy_sig,skill_name:f.skill_name||null,sample_locator:f.sample_locator,policy_facts:facts})}
  units.push({source_key:u.sourceKey,source_system:'skills-sh-b2',status:'done',rows_scanned:occ.length,skills_indexed:occ.length,records:occ.length,snapshot:{transport:'skills-sh-cache-occurrence-hash-verified',hashesVerified:occ.length}});
  report.push({...u,occurrenceRows:occ.length,candidateSlugs:slugs.size,downloaded:downloaded.length,status:'done_exact_hash_coverage'});
}
const body=records.map(x=>JSON.stringify(x)).join('\n')+(records.length?'\n':'');
await fs.writeFile(path.join(outDir,'features-skills-b2-occurrence-rescue-v6.ndjson.gz'),gzipSync(Buffer.from(body),{level:6}));
const manifest={generatedAt:new Date().toISOString(),source:'skills-sh-cache-occurrence-hash-verified',sourceSystem:'skills-sh-b2',totalMissingAtStart:unresolved.length,assignedUnits:unresolved.length,completedUnits:units.length,failedUnits:unresolved.length-units.length,skills:totalSkills,inScope,policyFacts,records:records.length,units,report};
await fs.writeFile(path.join(outDir,'manifest-skills-b2-occurrence-rescue-v6.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({event:'occurrence_rescue_complete',totalMissingAtStart:unresolved.length,completedUnits:units.length,remaining:unresolved.length-units.length,skills:totalSkills,report},null,2));