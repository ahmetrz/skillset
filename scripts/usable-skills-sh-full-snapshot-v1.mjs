import {createClient} from '@libsql/client';
import {S3Client,GetObjectCommand} from '@aws-sdk/client-s3';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {gunzipSync,createGzip} from 'node:zlib';
import {feature,numericFacts} from './final-audit-feature-core-v4.mjs';

const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const PART=Number(process.env.SNAPSHOT_PARTITION||0),PARTS=Math.max(1,Number(process.env.SNAPSHOT_PARTITIONS||16));
const outDir=process.env.SNAPSHOT_OUT_DIR||'usable-skills-sh-full';
const db=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const endpoint=need('B2_ENDPOINT').replace(/\/$/,'');
const region=endpoint.match(/s3[.-]([a-z0-9-]+)\.backblazeb2\.com/i)?.[1]||'us-east-1';
const b2=new S3Client({endpoint,region,forcePathStyle:true,credentials:{accessKeyId:need('B2_ACCESS_KEY_ID'),secretAccessKey:need('B2_SECRET_ACCESS_KEY')}});
const bucket=need('B2_BUCKET');
const sha=b=>createHash('sha256').update(b).digest('hex');
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function retry(fn,n=6){let last;for(let i=0;i<n;i++){try{return await fn()}catch(e){last=e;if(i===n-1)break;await sleep(Math.min(500*(2**i),8000))}}throw last}
function writer(file){const gz=createGzip({level:6}),ws=fs.createWriteStream(file);gz.pipe(ws);return {write:o=>gz.write(JSON.stringify(o)+'\n'),end:()=>new Promise((res,rej)=>{ws.on('finish',res);ws.on('error',rej);gz.on('error',rej);gz.end()})}}

await fsp.mkdir(outDir,{recursive:true});
const tag='p'+String(PART).padStart(2,'0')+'-of-'+String(PARTS).padStart(2,'0');
const fw=writer(path.join(outDir,'features-skills-sh-full-'+tag+'.ndjson.gz'));
const rows=(await db.execute("SELECT source,b2_path,pack_sha256,bytes,discovered_skills FROM skills_sh_external_exact_v1 WHERE status='done' AND b2_path IS NOT NULL ORDER BY b2_path")).rows;
const assigned=rows.filter((_,i)=>i%PARTS===PART);
let repos=0,files=0,inScope=0,failed=0;
const units=[];
for(const r of assigned){
  const b2path=String(r.b2_path||''),prefix='b2://'+bucket+'/';
  if(!b2path.startsWith(prefix))throw new Error('unexpected_b2_path:'+b2path);
  const key=b2path.slice(prefix.length),sourceKey='b2:'+key;
  try{
    const obj=await retry(()=>b2.send(new GetObjectCommand({Bucket:bucket,Key:key})));
    const gz=Buffer.from(await obj.Body.transformToByteArray());
    if(String(r.pack_sha256||'')&&sha(gz)!==String(r.pack_sha256))throw new Error('pack_sha_mismatch');
    if(Number(r.bytes||0)&&gz.length!==Number(r.bytes))throw new Error('pack_bytes_mismatch');
    const pack=JSON.parse(gunzipSync(gz).toString('utf8')),xs=Array.isArray(pack.files)?pack.files:[];
    if(xs.length!==Number(r.discovered_skills||0))throw new Error('file_count_'+xs.length+'_expected_'+r.discovered_skills);
    for(const x of xs){
      const p=String(x.path||''),bytes=Buffer.from(String(x.contentBase64||''),'base64'),text=bytes.toString('utf8');
      const locator='b2-exact://'+key+'#'+encodeURIComponent(p),f=feature(text,locator);
      fw.write({source_key:sourceKey,item_key:p,source_system:'skills-sh-b2',repo:String(r.source||''),path:p,locator,content_hash:f.h,simhash_hex:f.simhash_hex,c0:f.c0,c1:f.c1,c2:f.c2,c3:f.c3,char_count:f.char_count,token_count:f.token_count,sdlc_mask:f.sdlc_mask,social_mask:f.social_mask,risk_mask:f.risk_mask,provider_mask:f.provider_mask,policy_sig:f.policy_sig,skill_name:f.skill_name||null,sample_locator:f.sample_locator,policy_facts:numericFacts(text)});
      files++;if(f.sdlc_mask||f.social_mask)inScope++;
    }
    repos++;units.push({source_key:sourceKey,status:'done',expected:Number(r.discovered_skills||0),files:xs.length,pack_sha256:String(r.pack_sha256||'')});
    if(repos%50===0)console.log(JSON.stringify({event:'skills_sh_exact_progress',partition:PART,repos,files}));
  }catch(e){
    failed++;units.push({source_key:sourceKey,status:'error',expected:Number(r.discovered_skills||0),error:String(e?.message||e).slice(0,240)});
    console.warn(JSON.stringify({event:'skills_sh_exact_error',partition:PART,source:r.source,error:String(e?.message||e).slice(0,160)}));
  }
}
await fw.end();
const manifest={generatedAt:new Date().toISOString(),partition:PART,partitions:PARTS,assignedRepos:assigned.length,completedRepos:repos,failedRepos:failed,files,inScope,uncategorized:files-inScope,units};
await fsp.writeFile(path.join(outDir,'manifest-skills-sh-full-'+tag+'.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({event:'skills_sh_exact_complete',partition:PART,repos,files,inScope,uncategorized:files-inScope,failed}));
if(failed)process.exitCode=3;
