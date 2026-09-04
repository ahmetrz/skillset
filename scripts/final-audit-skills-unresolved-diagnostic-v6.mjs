import {createClient} from '@libsql/client';
import fs from 'node:fs/promises';
import path from 'node:path';

const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const turso=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const bucket=need('B2_BUCKET');
const skipDir=process.env.AUDIT_SKIP_MANIFEST_DIR||'diag-skip';
const outFile=process.env.DIAG_OUT||'skills-sh-unresolved-diagnostic.json';

async function q(sql,args=[]){return (await turso.execute({sql,args})).rows}
async function walk(dir,out=[]){let es=[];try{es=await fs.readdir(dir,{withFileTypes:true})}catch{return out}
  for(const e of es){const p=path.join(dir,e.name);if(e.isDirectory())await walk(p,out);else if(/^manifest-.*\.json$/i.test(e.name))out.push(p)}return out}
const releaseDone=new Set();
for(const p of await walk(skipDir)){
  try{const m=JSON.parse(await fs.readFile(p,'utf8'));for(const u of m.units||[])if(String(u.source_system)==='skills-sh-b2'&&u.status==='done')releaseDone.add(String(u.source_key))}catch{}
}
const [di,dp,rows]=await Promise.all([
  q("SELECT source_key FROM final_audit_unit_v1 WHERE source_system='skills-sh-b2' AND status='done'"),
  q("SELECT source_key FROM final_audit_policy_unit_v2 WHERE source_system='skills-sh-b2' AND status='done'"),
  q("SELECT source,owner,repo,updated_at,b2_path,status FROM skills_sh_external_exact_v1 WHERE status='done' AND b2_path IS NOT NULL ORDER BY b2_path")
]);
const ingest=new Set(di.map(x=>String(x.source_key))),policy=new Set(dp.map(x=>String(x.source_key)));
const prefix='b2://'+bucket+'/';
const unresolved=[];
for(const r of rows){
  const b2=String(r.b2_path||'');if(!b2.startsWith(prefix))continue;
  const sk='b2:'+b2.slice(prefix.length);
  if(releaseDone.has(sk)||(ingest.has(sk)&&policy.has(sk)))continue;
  const source=String(r.source||''),owner=r.owner==null?'':String(r.owner),repo=r.repo==null?'':String(r.repo),at=r.updated_at==null?'':String(r.updated_at);
  let parsed=null;
  const m=source.match(/github\.com\/([^/]+)\/([^/#?]+)/i);
  if(m)parsed={owner:m[1],repo:m[2].replace(/\.git$/,'')};
  unresolved.push({sourceKey:sk,source,owner,repo,updatedAt:at,parsed,missing:{source:!source,owner:!owner,repo:!repo,updatedAt:!at}});
}
const summary={generatedAt:new Date().toISOString(),releaseDone:releaseDone.size,existingIngest:ingest.size,existingPolicy:policy.size,unresolved:unresolved.length,missingOwner:unresolved.filter(x=>x.missing.owner).length,missingRepo:unresolved.filter(x=>x.missing.repo).length,missingUpdatedAt:unresolved.filter(x=>x.missing.updatedAt).length,githubParsable:unresolved.filter(x=>x.parsed).length,rows:unresolved};
await fs.writeFile(outFile,JSON.stringify(summary,null,2)+'\n');
console.log(JSON.stringify(summary,null,2));
