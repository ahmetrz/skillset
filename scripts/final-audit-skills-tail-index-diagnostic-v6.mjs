import {createClient} from '@libsql/client';
import fs from 'node:fs/promises';
import path from 'node:path';

const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const db=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const dir=process.env.MANIFEST_DIR||'tail-manifests';
async function q(sql,args=[]){return (await db.execute({sql,args})).rows}
async function walk(d,out=[]){let es=[];try{es=await fs.readdir(d,{withFileTypes:true})}catch{return out}for(const e of es){const p=path.join(d,e.name);if(e.isDirectory())await walk(p,out);else if(/^manifest-.*\.json$/i.test(e.name))out.push(p)}return out}
const releaseDone=new Set();
for(const p of await walk(dir)){try{const m=JSON.parse(await fs.readFile(p,'utf8'));for(const u of m.units||[])if(String(u.source_system)==='skills-sh-b2'&&u.status==='done')releaseDone.add(String(u.source_key))}catch{}}
const [di,dp,src,schema]=await Promise.all([
 q("SELECT source_key FROM final_audit_unit_v1 WHERE source_system='skills-sh-b2' AND status='done'"),
 q("SELECT source_key FROM final_audit_policy_unit_v2 WHERE source_system='skills-sh-b2' AND status='done'"),
 q("SELECT source,owner,repo,b2_path,discovered_skills,pack_sha256,bytes FROM skills_sh_external_exact_v1 WHERE status='done' AND b2_path IS NOT NULL ORDER BY b2_path"),
 q("SELECT name,sql FROM sqlite_master WHERE type='table' AND (lower(name) LIKE '%skill%' OR lower(name) LIKE '%audit%' OR lower(name) LIKE '%exact%' OR lower(name) LIKE '%occurrence%') ORDER BY name")
]);
const ingest=new Set(di.map(x=>String(x.source_key))),policy=new Set(dp.map(x=>String(x.source_key)));
const bucket=need('B2_BUCKET'),prefix='b2://'+bucket+'/';
const unresolved=[];
for(const r of src){const b=String(r.b2_path||'');if(!b.startsWith(prefix))continue;const sk='b2:'+b.slice(prefix.length);if(releaseDone.has(sk)||(ingest.has(sk)&&policy.has(sk)))continue;unresolved.push({sourceKey:sk,source:String(r.source||''),expectedSkills:Number(r.discovered_skills||0),packSha256:String(r.pack_sha256||'')})}
const tableInfo={};
for(const t of ['final_audit_occurrence_v1','final_audit_exact_v1','final_audit_policy_v2','skills_sh_external_exact_v1']){try{tableInfo[t]=await q('PRAGMA table_info('+t+')')}catch(e){tableInfo[t]={error:String(e)}}}
const partial=[];
for(const u of unresolved){
 let occ=[],exact=[];
 try{occ=await q("SELECT * FROM final_audit_occurrence_v1 WHERE source_key=? LIMIT 500",[u.sourceKey])}catch{}
 try{exact=await q("SELECT * FROM final_audit_exact_v1 WHERE source_key=? LIMIT 500",[u.sourceKey])}catch{}
 partial.push({...u,occurrenceRows:occ.length,exactRows:exact.length,occurrenceSample:occ.slice(0,3),exactSample:exact.slice(0,3),occurrenceHashes:[...new Set(occ.map(r=>String(r.content_hash||'')).filter(Boolean))]});
}
const out={generatedAt:new Date().toISOString(),unresolved:unresolved.length,releaseDone:releaseDone.size,tableInfo,schema,partial};
console.log(JSON.stringify(out,null,2));
await fs.writeFile(process.env.DIAG_OUT||'skills-tail-index-diagnostic-v6.json',JSON.stringify(out,null,2)+'\n');