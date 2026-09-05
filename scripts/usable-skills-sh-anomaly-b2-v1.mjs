import {createClient} from '@libsql/client';
import {S3Client,GetObjectCommand} from '@aws-sdk/client-s3';
import fs from 'node:fs/promises';
import path from 'node:path';
import {gunzipSync,gzipSync} from 'node:zlib';
import {createHash} from 'node:crypto';
import {feature,numericFacts} from './final-audit-feature-core-v4.mjs';

const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const db=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const endpoint=need('B2_ENDPOINT').replace(/\/$/,'');
const region=endpoint.match(/s3[.-]([a-z0-9-]+)\.backblazeb2\.com/i)?.[1]||'us-east-1';
const b2=new S3Client({endpoint,region,forcePathStyle:true,credentials:{accessKeyId:need('B2_ACCESS_KEY_ID'),secretAccessKey:need('B2_SECRET_ACCESS_KEY')}});
const bucket=need('B2_BUCKET');
const gate=JSON.parse(await fs.readFile(process.env.SKILLS_GATE_IN||'audit/usable-skills-sh-source-gate.json','utf8'));
const outDir=process.env.ANOMALY_OUT||'skills-sh-anomaly-out';
const sha=b=>createHash('sha256').update(b).digest('hex');
await fs.mkdir(outDir,{recursive:true});

const anomalies=[...(gate.missing||[]),...(gate.extra||[])];
const byKey=new Map(anomalies.map(x=>[String(x.sourceKey),x]));
const records=[],report=[];
for(const [sourceKey,a] of byKey){
  const key=sourceKey.replace(/^b2:/,'');
  try{
    const meta=(await db.execute({sql:"SELECT source,owner,repo,b2_path,pack_sha256,bytes,discovered_skills,updated_at FROM skills_sh_external_exact_v1 WHERE status='done' AND b2_path=? LIMIT 1",args:['b2://'+bucket+'/'+key]})).rows[0];
    if(!meta)throw new Error('metadata_missing');
    const obj=await b2.send(new GetObjectCommand({Bucket:bucket,Key:key}));
    const gz=Buffer.from(await obj.Body.transformToByteArray());
    if(String(meta.pack_sha256||'')&&sha(gz)!==String(meta.pack_sha256))throw new Error('pack_sha_mismatch');
    if(Number(meta.bytes||0)&&gz.length!==Number(meta.bytes))throw new Error('pack_bytes_mismatch');
    const pack=JSON.parse(gunzipSync(gz).toString('utf8'));
    const files=Array.isArray(pack.files)?pack.files:[];
    if(files.length!==Number(meta.discovered_skills||0))throw new Error('file_count_'+files.length+'_expected_'+meta.discovered_skills);
    for(const x of files){
      const p=String(x.path||''),bytes=Buffer.from(String(x.contentBase64||''),'base64'),text=bytes.toString('utf8'),locator='b2-exact://'+key+'#'+encodeURIComponent(p),f=feature(text,locator);
      records.push({source_key:sourceKey,item_key:p,source_system:'skills-sh-b2',repo:String(meta.source||''),path:p,locator,content_hash:f.h,simhash_hex:f.simhash_hex,c0:f.c0,c1:f.c1,c2:f.c2,c3:f.c3,char_count:f.char_count,token_count:f.token_count,sdlc_mask:f.sdlc_mask,social_mask:f.social_mask,risk_mask:f.risk_mask,provider_mask:f.provider_mask,policy_sig:f.policy_sig,skill_name:f.skill_name||null,sample_locator:f.sample_locator,policy_facts:numericFacts(text)});
    }
    report.push({sourceKey,source:String(meta.source||''),status:'exact_b2_verified',files:files.length,packSha256:String(meta.pack_sha256||'')});
    console.log(JSON.stringify({event:'anomaly_b2_exact',sourceKey,source:meta.source,files:files.length}));
  }catch(e){
    report.push({sourceKey,source:a.source,status:'failed',error:String(e?.message||e).slice(0,240)});
    console.warn(JSON.stringify({event:'anomaly_b2_failed',sourceKey,error:String(e?.message||e).slice(0,160)}));
  }
}
const body=records.map(JSON.stringify).join('\n')+(records.length?'\n':'');
await fs.writeFile(path.join(outDir,'features-skills-sh-anomaly-exact-b2-v1.ndjson.gz'),gzipSync(Buffer.from(body),{level:6}));
const ok=report.filter(x=>x.status==='exact_b2_verified').length;
const manifest={generatedAt:new Date().toISOString(),status:ok===byKey.size?'ALL_ANOMALIES_EXACT_B2_VERIFIED':'PARTIAL_B2_BLOCKED',anomalySources:byKey.size,completedSources:ok,failedSources:byKey.size-ok,records:records.length,report};
await fs.writeFile(path.join(outDir,'manifest-skills-sh-anomaly-exact-b2-v1.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify(manifest,null,2));
if(ok!==byKey.size)process.exitCode=3;
