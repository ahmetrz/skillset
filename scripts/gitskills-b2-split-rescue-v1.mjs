import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { createHash } from 'node:crypto';
import { gunzipSync, gzipSync } from 'node:zlib';

const need=n=>{const v=process.env[n];if(!v)throw new Error(`Missing ${n}`);return v};
const endpoint=need('B2_ENDPOINT').replace(/\/$/,'');
const region=endpoint.match(/s3[.-]([a-z0-9-]+)\.backblazeb2\.com/i)?.[1]||'us-east-1';
const b2=new S3Client({endpoint,region,forcePathStyle:true,credentials:{accessKeyId:need('B2_ACCESS_KEY_ID'),secretAccessKey:need('B2_SECRET_ACCESS_KEY')}});
const bucket=need('B2_BUCKET');
const ingest=need('GITSKILLS_INGEST_URL');
const lane=Number(need('WORKER'));
const starts=[135012,135108,135236,135268,135300,135332,135364,135396,135428,135460,135556,135588,135652,135684,135716,135748,135780,135812,135844,135876,135908,135940,135972,136004,136036,136068,136100];

async function oidc(){
  const u=new URL(need('ACTIONS_ID_TOKEN_REQUEST_URL'));
  u.searchParams.set('audience','gitskills-b2-supabase-ingest');
  const r=await fetch(u,{headers:{authorization:`Bearer ${need('ACTIONS_ID_TOKEN_REQUEST_TOKEN')}`}});
  if(!r.ok)throw new Error(`oidc_${r.status}`);
  return (await r.json()).value;
}

async function find(start){
  const prefix=`gitskills/discovery-b2-v2/pack-${start}-`;
  const r=await b2.send(new ListObjectsV2Command({Bucket:bucket,Prefix:prefix}));
  const keys=(r.Contents||[]).map(x=>x.Key).filter(Boolean);
  if(keys.length!==1)throw new Error(`source_lookup_${start}_${keys.length}`);
  return keys[0];
}

async function upload(token,path,payload,reps){
  const bytes=gzipSync(Buffer.from(JSON.stringify(payload)));
  const digest=createHash('sha256').update(bytes).digest('hex');
  const r=await fetch(ingest,{method:'POST',headers:{authorization:`Bearer ${token}`,'content-type':'application/gzip','x-storage-path':path,'x-sha256':digest,'x-representatives':String(reps)},body:bytes,signal:AbortSignal.timeout(150000)});
  const text=await r.text();
  if(!r.ok)throw new Error(`ingest_${r.status}:${text.slice(0,500)}`);
  console.log(JSON.stringify({event:'split_upload',path,reps,bytes:bytes.length}));
}

async function rescue(token,start){
  const key=await find(start);
  const o=await b2.send(new GetObjectCommand({Bucket:bucket,Key:key}));
  const payload=JSON.parse(gunzipSync(Buffer.from(await o.Body.transformToByteArray())));
  const groups=[];let current=[],count=0;
  for(const shard of payload.shards||[]){
    const n=Number(shard.representativeRows||shard.rows?.length||0);
    if(current.length&&count+n>250){groups.push([current,count]);current=[];count=0}
    current.push(shard);count+=n;
  }
  if(current.length)groups.push([current,count]);
  let total=0;
  for(let i=0;i<groups.length;i++){
    const [shards,reps]=groups[i];total+=reps;
    const path=`gitskills/discovery-b2-split-v1/pack-${start}-part-${String(i+1).padStart(2,'0')}.json.gz`;
    await upload(token,path,{...payload,shards},reps);
  }
  const expected=(payload.shards||[]).reduce((n,s)=>n+Number(s.representativeRows||s.rows?.length||0),0);
  if(total!==expected)throw new Error(`split_count_mismatch_${start}_${total}_${expected}`);
  console.log(JSON.stringify({event:'source_complete',start,key,parts:groups.length,reps:total}));
}

const token=await oidc();
for(const [i,start] of starts.entries())if(i%16===lane-1)await rescue(token,start);
