import { createClient } from '@libsql/client';
import { S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3';
import fs from 'node:fs/promises';

function need(name){const v=process.env[name];if(!v)throw new Error('Missing '+name);return v;}
const turso=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const endpoint=need('B2_ENDPOINT').replace(/\/$/,'');
const region=endpoint.match(/s3[.-]([a-z0-9-]+)\.backblazeb2\.com/i)?.[1]||'us-east-1';
const b2=new S3Client({endpoint,region,forcePathStyle:true,credentials:{accessKeyId:need('B2_ACCESS_KEY_ID'),secretAccessKey:need('B2_SECRET_ACCESS_KEY')}});
const bucket=need('B2_BUCKET');

async function q(sql,args=[]){
  try{
    const r=await turso.execute({sql,args});
    return {ok:true,rows:r.rows,rowsAffected:r.rowsAffected};
  }catch(e){
    return {ok:false,error:String(e?.message||e)};
  }
}
async function scalar(sql){
  const r=await q(sql);
  if(!r.ok)return r;
  return {ok:true,value:r.rows?.[0]??null};
}
async function prefixStats(prefix){
  let token=undefined,count=0,bytes=0,latest=null,pages=0;
  do{
    const r=await b2.send(new ListObjectsV2Command({Bucket:bucket,Prefix:prefix,ContinuationToken:token,MaxKeys:1000}));
    pages++;
    for(const o of r.Contents||[]){
      count++;
      bytes+=Number(o.Size||0);
      const lm=o.LastModified?new Date(o.LastModified).toISOString():null;
      if(lm&&(!latest||lm>latest))latest=lm;
    }
    token=r.IsTruncated?r.NextContinuationToken:undefined;
  }while(token);
  return {prefix,count,bytes,pages,latest};
}
const tursoStats={
  gitskills_shards:await scalar("select count(*) n, sum(case when status='done' then 1 else 0 end) done, sum(case when status<>'done' then 1 else 0 end) remaining, coalesce(sum(rows_fetched),0) reps from gitskills_shards"),
  gitskills_packs:await scalar("select count(*) n, coalesce(sum(representatives),0) reps, coalesce(sum(bytes),0) bytes from gitskills_packs"),
  gitskills_state:await q("select key,value,updated_at from gitskills_state order by key"),
  skills_sh_external_exact:await q("select status,count(*) n,coalesce(sum(discovered_skills),0) skills,coalesce(sum(bytes),0) bytes from skills_sh_external_exact_v1 group by status order by status")
};
const b2Stats=[];
for(const p of ['gitskills/','skills-sh/'])b2Stats.push(await prefixStats(p));
const out={generatedAt:new Date().toISOString(),bucket,turso:tursoStats,b2:b2Stats};
await fs.mkdir('audit',{recursive:true});
await fs.writeFile('audit/external-status.json',JSON.stringify(out,null,2)+'\n');
console.log(JSON.stringify(out,null,2));
