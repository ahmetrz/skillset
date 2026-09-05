import {createClient} from '@libsql/client';
import {S3Client,GetObjectCommand} from '@aws-sdk/client-s3';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import {createGzip,gunzipSync} from 'node:zlib';
import {createHash} from 'node:crypto';

const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const part=Number(process.env.SKILLS_PARTITION||0),parts=Math.max(1,Number(process.env.SKILLS_PARTITIONS||16));
const outDir=process.env.SKILLS_RAW_OUT_DIR||'usable-skills-sh-raw';
await fsp.mkdir(outDir,{recursive:true});
const rawOut=path.join(outDir,'raw-skills-sh-p'+String(part).padStart(2,'0')+'-of-'+String(parts).padStart(2,'0')+'.ndjson.gz');
const manifestOut=path.join(outDir,'manifest-skills-sh-p'+String(part).padStart(2,'0')+'-of-'+String(parts).padStart(2,'0')+'.json');

const db=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const endpoint=need('B2_ENDPOINT').replace(/\/$/,'');
const region=endpoint.match(/s3[.-]([a-z0-9-]+)\.backblazeb2\.com/i)?.[1]||'us-east-1';
const b2=new S3Client({endpoint,region,forcePathStyle:true,credentials:{accessKeyId:need('B2_ACCESS_KEY_ID'),secretAccessKey:need('B2_SECRET_ACCESS_KEY')}});
const bucket=need('B2_BUCKET');
const sha=b=>createHash('sha256').update(b).digest('hex');

const rows=(await db.execute("SELECT source,b2_path,pack_sha256,bytes,discovered_skills FROM skills_sh_external_exact_v1 WHERE status='done' AND b2_path IS NOT NULL ORDER BY source,b2_path")).rows;
const assigned=rows.filter((_,i)=>i%parts===part);
const gz=createGzip({level:5}),out=fs.createWriteStream(rawOut);gz.pipe(out);
let files=0,expected=0,verifiedPacks=0;const errors=[];
async function get(key){
  let last;
  for(let i=0;i<4;i++){
    try{
      const obj=await b2.send(new GetObjectCommand({Bucket:bucket,Key:key}));
      return Buffer.from(await obj.Body.transformToByteArray());
    }catch(e){last=e;await new Promise(r=>setTimeout(r,500*(i+1)))}
  }
  throw last;
}
for(const r of assigned){
  expected+=Number(r.discovered_skills||0);
  const bp=String(r.b2_path||''),m=bp.match(/^b2:\/\/[^/]+\/(.+)$/);
  if(!m){errors.push({source:r.source,error:'bad_b2_path'});continue}
  const key=m[1];
  try{
    const packGz=await get(key);
    if(String(r.pack_sha256||'')&&sha(packGz)!==String(r.pack_sha256))throw new Error('pack_sha_mismatch');
    if(Number(r.bytes||0)&&packGz.length!==Number(r.bytes))throw new Error('pack_bytes_mismatch');
    const pack=JSON.parse(gunzipSync(packGz).toString('utf8'));
    const list=Array.isArray(pack.files)?pack.files:[];
    if(list.length!==Number(r.discovered_skills||0))throw new Error('file_count_'+list.length+'_expected_'+r.discovered_skills);
    for(const x of list){
      const p=String(x.path||''),text=Buffer.from(String(x.contentBase64||''),'base64').toString('utf8');
      gz.write(JSON.stringify({sourceSystem:'skills-sh-b2',sourceKey:'b2:'+key,itemKey:p,repo:String(r.source||''),path:p,locator:'b2-exact://'+key+'#'+encodeURIComponent(p),text})+'\n');
      files++;
    }
    verifiedPacks++;
  }catch(e){errors.push({source:String(r.source||''),key,error:String(e?.message||e).slice(0,240)})}
}
gz.end();
await new Promise((res,rej)=>{out.on('finish',res);out.on('error',rej);gz.on('error',rej)});
const manifest={generatedAt:new Date().toISOString(),partition:part,partitions:parts,assignedPacks:assigned.length,verifiedPacks,expectedFiles:expected,files,errors};
await fsp.writeFile(manifestOut,JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({event:'skills_sh_raw_complete',partition:part,assignedPacks:assigned.length,verifiedPacks,expectedFiles:expected,files,errors:errors.length}));
if(errors.length||files!==expected||verifiedPacks!==assigned.length)process.exitCode=3;
