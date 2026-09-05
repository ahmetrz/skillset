import {createClient} from '@libsql/client';
import {S3Client,GetObjectCommand} from '@aws-sdk/client-s3';
import fs from 'node:fs/promises';
import path from 'node:path';
import {gunzipSync,gzipSync} from 'node:zlib';
import {createHash} from 'node:crypto';
import {feature,numericFacts} from './final-audit-feature-core-v4.mjs';

const PART=Number(process.env.AUDIT_PARTITION||0),PARTS=Math.max(1,Number(process.env.AUDIT_PARTITIONS||4)),TARGET_SOURCE=String(process.env.RESCUE_SOURCE||'').trim().toLowerCase(),RESCUE_LABEL=String(process.env.RESCUE_LABEL||'').replace(/[^A-Za-z0-9._-]/g,'_');
const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const turso=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const endpoint=need('B2_ENDPOINT').replace(/\/$/,'');
const region=endpoint.match(/s3[.-]([a-z0-9-]+)\.backblazeb2\.com/i)?.[1]||'us-east-1';
const b2=new S3Client({endpoint,region,forcePathStyle:true,credentials:{accessKeyId:need('B2_ACCESS_KEY_ID'),secretAccessKey:need('B2_SECRET_ACCESS_KEY')}});
const bucket=need('B2_BUCKET');
const outDir=process.env.AUDIT_OUT_DIR||'skills-b2-rescue-out';
const skipDir=process.env.AUDIT_SKIP_MANIFEST_DIR||'';
const sha256=b=>createHash('sha256').update(b).digest('hex');

async function q(sql,args=[]){return (await turso.execute({sql,args})).rows}
async function walk(dir,out=[]){let es=[];try{es=await fs.readdir(dir,{withFileTypes:true})}catch{return out}
  for(const e of es){const p=path.join(dir,e.name);if(e.isDirectory())await walk(p,out);else if(/^manifest-.*\.json$/i.test(e.name))out.push(p)}return out}
async function readBody(body){if(!body)throw new Error('b2_empty_body');if(typeof body.transformToByteArray==='function')return Buffer.from(await body.transformToByteArray());const chunks=[];for await(const c of body)chunks.push(Buffer.from(c));return Buffer.concat(chunks)}
const releaseDone=new Set();
for(const p of await walk(skipDir)){try{const m=JSON.parse(await fs.readFile(p,'utf8'));for(const u of m.units||[])if(String(u.source_system)==='skills-sh-b2'&&u.status==='done')releaseDone.add(String(u.source_key))}catch{}}
const [di,dp,rows]=await Promise.all([
  q("SELECT source_key FROM final_audit_unit_v1 WHERE source_system='skills-sh-b2' AND status='done'"),
  q("SELECT source_key FROM final_audit_policy_unit_v2 WHERE source_system='skills-sh-b2' AND status='done'"),
  q("SELECT source,owner,repo,b2_path,pack_sha256,bytes,discovered_skills FROM skills_sh_external_exact_v1 WHERE status='done' AND b2_path IS NOT NULL ORDER BY b2_path")
]);
const ingest=new Set(di.map(x=>String(x.source_key))),policy=new Set(dp.map(x=>String(x.source_key)));
const prefix='b2://'+bucket+'/';
const unresolved=[];
for(const r of rows){
  const b2p=String(r.b2_path||'');if(!b2p.startsWith(prefix))continue;
  const key=b2p.slice(prefix.length),sk='b2:'+key;
  if(releaseDone.has(sk)||(ingest.has(sk)&&policy.has(sk)))continue;
  unresolved.push({key,sourceKey:sk,source:String(r.source||''),owner:String(r.owner||''),repo:String(r.repo||''),packSha256:String(r.pack_sha256||''),expectedBytes:Number(r.bytes||0),expectedSkills:Number(r.discovered_skills||0)});
}
const assigned=TARGET_SOURCE?unresolved.filter(u=>u.source.toLowerCase()===TARGET_SOURCE):unresolved.filter((_,i)=>i%PARTS===PART);
await fs.mkdir(outDir,{recursive:true});
const records=[],units=[];let skills=0,inScope=0,policyFacts=0,failed=0;
for(const u of assigned){
  try{
    const obj=await b2.send(new GetObjectCommand({Bucket:bucket,Key:u.key}));
    const gz=await readBody(obj.Body);
    const digest=sha256(gz);
    if(u.packSha256&&digest!==u.packSha256)throw new Error('b2_sha256_mismatch expected='+u.packSha256+' got='+digest);
    const payload=JSON.parse(gunzipSync(gz).toString('utf8'));
    if(String(payload.repoSource||'')!==u.source)throw new Error('b2_repo_source_mismatch');
    if(!Array.isArray(payload.files))throw new Error('b2_files_missing');
    if(Number(u.expectedSkills)!==payload.files.length)throw new Error('b2_skill_count_mismatch expected='+u.expectedSkills+' got='+payload.files.length);
    let n=0;
    for(const x of payload.files){
      const text=Buffer.from(String(x.contentBase64||''),'base64').toString('utf8');
      if(!text)continue;
      const locator='b2-exact://'+u.key+'#'+String(x.path||n);
      const f=feature(text,locator),facts=numericFacts(text);
      if(f.sdlc_mask!==0||f.social_mask!==0)inScope++;
      policyFacts+=facts.length;skills++;n++;
      records.push({
        source_key:u.sourceKey,item_key:String(x.path||n),source_system:'skills-sh-b2',repo:u.source,path:String(x.path||''),locator,
        content_hash:f.h,simhash_hex:f.simhash_hex,c0:f.c0,c1:f.c1,c2:f.c2,c3:f.c3,char_count:f.char_count,token_count:f.token_count,
        sdlc_mask:f.sdlc_mask,social_mask:f.social_mask,risk_mask:f.risk_mask,provider_mask:f.provider_mask,policy_sig:f.policy_sig,
        skill_name:f.skill_name||null,sample_locator:f.sample_locator,policy_facts:facts
      });
    }
    units.push({source_key:u.sourceKey,source_system:'skills-sh-b2',status:'done',rows_scanned:payload.files.length,skills_indexed:payload.files.length,records:n,snapshot:{transport:'b2-exact-rescue',packSha256:digest}});
    console.log(JSON.stringify({event:'b2_rescue_unit_done',partition:PART,source:u.source,skills:payload.files.length,bytes:gz.length}));
  }catch(e){
    failed++;units.push({source_key:u.sourceKey,source_system:'skills-sh-b2',status:'error',error:String(e?.message||e).slice(0,800)});
    console.warn(JSON.stringify({event:'b2_rescue_unit_error',partition:PART,source:u.source,error:String(e?.message||e).slice(0,300)}));
  }
}
const base=RESCUE_LABEL?'skills-b2-exact-rescue-'+RESCUE_LABEL:'skills-b2-exact-rescue-p'+String(PART).padStart(2,'0')+'-of-'+String(PARTS).padStart(2,'0');
const ndjson=records.map(x=>JSON.stringify(x)).join('\n')+(records.length?'\n':'');
await fs.writeFile(path.join(outDir,'features-'+base+'.ndjson.gz'),gzipSync(Buffer.from(ndjson),{level:6}));
const manifest={generatedAt:new Date().toISOString(),source:'skills-b2-exact-rescue',sourceSystem:'skills-sh-b2',partition:PART,partitions:PARTS,totalMissingAtStart:unresolved.length,assignedUnits:assigned.length,completedUnits:units.filter(x=>x.status==='done').length,failedUnits:failed,skills,inScope,policyFacts,records:records.length,units};
await fs.writeFile(path.join(outDir,'manifest-'+base+'.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({event:'b2_rescue_complete',partition:PART,...manifest,units:undefined}));
if(failed&&manifest.completedUnits===0)process.exitCode=2;
