import { createClient } from '@libsql/client';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import tar from 'tar-stream';
import { createGunzip, gzipSync } from 'node:zlib';
import { Readable } from 'node:stream';
import { createHash } from 'node:crypto';

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://cxvvfgwdqgxczxmomztw.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_PUBLISHABLE_KEY;
const BATCH_SIZE = Math.max(1, Math.min(Number(process.env.SKILLS_SH_BATCH_SIZE || 32), 128));
let CONCURRENCY = Math.max(1, Math.min(Number(process.env.SKILLS_SH_CONCURRENCY || 16), 32));
const MIN_CONCURRENCY = Math.max(4, Math.min(Number(process.env.SKILLS_SH_MIN_CONCURRENCY || 12), CONCURRENCY));
const MAX_CONCURRENCY = Math.max(CONCURRENCY, Math.min(Number(process.env.SKILLS_SH_MAX_CONCURRENCY || 24), 32));
const MAX_BATCHES = Math.max(1, Math.min(Number(process.env.SKILLS_SH_MAX_BATCHES || 200), 1000));
const MAX_SKILL_BYTES = 16 * 1024 * 1024;
const MAX_TOTAL_SKILL_BYTES = 256 * 1024 * 1024;
const MAX_SKILL_FILES = 10000;
const UA = 'skillset-skills-sh-external-b2/1.0';

function need(n){const v=process.env[n]; if(!v) throw new Error('Missing '+n); return v;}
if(!SUPABASE_KEY) throw new Error('Missing SUPABASE_PUBLISHABLE_KEY');

const turso = createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const endpoint = need('B2_ENDPOINT').replace(/\/$/,'');
const region = endpoint.match(/s3[.-]([a-z0-9-]+)\.backblazeb2\.com/i)?.[1] || 'us-east-1';
const b2 = new S3Client({endpoint,region,forcePathStyle:true,credentials:{accessKeyId:need('B2_ACCESS_KEY_ID'),secretAccessKey:need('B2_SECRET_ACCESS_KEY')}});
const bucket = need('B2_BUCKET');

const sleep=(ms)=>new Promise(r=>setTimeout(r,ms));
const sha256=(b)=>createHash('sha256').update(b).digest('hex');
const safe=(s)=>String(s).replace(/[^A-Za-z0-9._-]/g,'_');

async function rpc(name,args={}){
  const r=await fetch(SUPABASE_URL+'/rest/v1/rpc/'+name,{
    method:'POST',
    headers:{apikey:SUPABASE_KEY,Authorization:'Bearer '+SUPABASE_KEY,'Content-Type':'application/json'},
    body:JSON.stringify(args),
    signal:AbortSignal.timeout(30000)
  });
  const text=await r.text();
  if(!r.ok) throw new Error('rpc '+name+' '+r.status+': '+text.slice(0,600));
  if(!text) return null;
  try{return JSON.parse(text)}catch{return text}
}

async function ensureSchema(){
  await turso.batch([
    `CREATE TABLE IF NOT EXISTS skills_sh_external_exact_v1 (
      source TEXT PRIMARY KEY, owner TEXT NOT NULL, repo TEXT NOT NULL,
      bucket_id INTEGER, repo_index INTEGER, status TEXT NOT NULL,
      discovered_skills INTEGER NOT NULL DEFAULT 0, b2_path TEXT,
      pack_sha256 TEXT, bytes INTEGER NOT NULL DEFAULT 0,
      error TEXT, updated_at TEXT NOT NULL
    )`,
    `CREATE INDEX IF NOT EXISTS idx_skills_sh_external_exact_status ON skills_sh_external_exact_v1(status,source)`
  ],'write');
}

async function retry(label,fn,attempts=4){
  let last;
  for(let i=0;i<attempts;i++){
    try{return await fn(i)}catch(e){
      last=e;
      if(i===attempts-1) break;
      const m=String(e?.message||e);
      const delay=Math.min((/429|503|504|timeout/i.test(m)?1500:500)*(2**i),12000)+Math.floor(Math.random()*300);
      console.warn(JSON.stringify({event:'retry',label,attempt:i+1,delay,error:m.slice(0,240)}));
      await sleep(delay);
    }
  }
  throw last;
}

async function openArchive(owner,repo){
  const urls=[
    `https://github.com/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/archive/HEAD.tar.gz`,
    `https://codeload.github.com/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/tar.gz/refs/heads/main`,
    `https://codeload.github.com/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/tar.gz/refs/heads/master`
  ];
  let lastStatus=0;
  for(const url of urls){
    const r=await fetch(url,{headers:{'user-agent':UA},redirect:'follow',signal:AbortSignal.timeout(45000)});
    lastStatus=r.status;
    if(r.ok && r.body) return {response:r,url};
    if(r.status===404) continue;
    throw new Error('archive_http_'+r.status);
  }
  const e=new Error(lastStatus===404?'archive_not_found':'archive_http_'+lastStatus);
  e.code=lastStatus===404?'NOT_FOUND':'HTTP';
  throw e;
}

async function extractSkills(response){
  return await new Promise((resolve,reject)=>{
    const extract=tar.extract();
    const gunzip=createGunzip();
    const files=[];
    let total=0;
    let settled=false;

    const fail=(e)=>{if(!settled){settled=true; reject(e)}};

    extract.on('entry',(header,stream,next)=>{
      const path=String(header.name||'').replace(/\\/g,'/');
      const isSkill=header.type==='file' && /(^|\/)SKILL\.md$/i.test(path);
      if(!isSkill){stream.resume(); stream.on('end',next); return;}

      if(Number(header.size||0)>MAX_SKILL_BYTES){
        stream.resume();
        stream.on('end',()=>fail(new Error('skill_file_oversize:'+path+':'+header.size)));
        return;
      }

      const chunks=[]; let n=0;
      stream.on('data',c=>{
        n+=c.length;
        if(n>MAX_SKILL_BYTES){stream.destroy(new Error('skill_file_oversize:'+path+':'+n)); return;}
        chunks.push(c);
      });
      stream.on('error',fail);
      stream.on('end',()=>{
        if(settled) return;
        const bytes=Buffer.concat(chunks);
        total+=bytes.length;
        if(total>MAX_TOTAL_SKILL_BYTES) return fail(new Error('repo_skill_bytes_oversize:'+total));
        files.push({path,bytes});
        if(files.length>MAX_SKILL_FILES) return fail(new Error('repo_skill_files_oversize:'+files.length));
        next();
      });
    });
    extract.on('finish',()=>{if(!settled){settled=true; resolve(files)}});
    extract.on('error',fail);
    gunzip.on('error',fail);

    Readable.fromWeb(response.body).pipe(gunzip).pipe(extract);
  });
}

async function finish(item,outcome,{discovered=0,path=null,error=null}={}){
  await rpc('skillset_skills_sh_d3_archive_finish_external_v1',{
    p_source:item.source,p_outcome:outcome,p_discovered:discovered,p_storage_path:path,p_error:error
  });
}

async function record(item,status,{discovered=0,path=null,digest=null,bytes=0,error=null}={}){
  const t=new Date().toISOString();
  await turso.execute({sql:`
    INSERT INTO skills_sh_external_exact_v1(source,owner,repo,bucket_id,repo_index,status,discovered_skills,b2_path,pack_sha256,bytes,error,updated_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(source) DO UPDATE SET status=excluded.status,discovered_skills=excluded.discovered_skills,
      b2_path=excluded.b2_path,pack_sha256=excluded.pack_sha256,bytes=excluded.bytes,error=excluded.error,updated_at=excluded.updated_at
  `,args:[item.source,item.owner,item.repo,Number(item.bucket_id||0),Number(item.repo_index||0),status,discovered,path,digest,bytes,error,t]});
}

async function processOne(item){
  try{
    const {response}=await retry('archive:'+item.source,()=>openArchive(item.owner,item.repo),4);
    const files=await extractSkills(response);
    const packed=files.map(f=>({
      path:f.path.replace(/^[^/]+\//,''),
      contentHash:sha256(f.bytes),
      originalBytes:f.bytes.length,
      contentBase64:f.bytes.toString('base64')
    }));
    const payload={
      version:1,source:'skills.sh',transport:'github-archive-direct-b2',
      repoSource:item.source,owner:item.owner,repo:item.repo,
      generatedAt:new Date().toISOString(),files:packed
    };
    const gz=gzipSync(Buffer.from(JSON.stringify(payload)),{level:9});
    const digest=sha256(gz);
    const key=`skills-sh/exact-b2-v1/bucket-${String(item.bucket_id||0).padStart(2,'0')}/${String(item.repo_index||0).padStart(7,'0')}-${safe(item.owner)}-${safe(item.repo)}.json.gz`;
    await retry('b2:'+item.source,()=>b2.send(new PutObjectCommand({Bucket:bucket,Key:key,Body:gz,ContentType:'application/gzip',Metadata:{sha256:digest,source:'skills-sh'}})),5);
    const b2path='b2://'+bucket+'/'+key;
    await record(item,'done',{discovered:packed.length,path:b2path,digest,bytes:gz.length});
    await finish(item,'done',{discovered:packed.length,path:b2path});
    return {ok:true,source:item.source,skills:packed.length,bytes:gz.length};
  }catch(e){
    const msg=String(e?.message||e);
    if(/archive_not_found/.test(msg)){
      await record(item,'not_found',{error:msg});
      await finish(item,'not_found',{error:msg});
      return {ok:true,notFound:true,source:item.source};
    }
    if(/oversize/.test(msg)){
      await record(item,'oversize',{error:msg});
      await finish(item,'oversize',{error:msg});
      return {ok:true,oversize:true,source:item.source};
    }
    await record(item,'error',{error:msg.slice(0,1200)}).catch(()=>{});
    await finish(item,'error',{error:msg.slice(0,1200)}).catch(()=>{});
    return {ok:false,source:item.source,error:msg.slice(0,300)};
  }
}

async function mapLimit(items,limit,fn){
  let next=0; const out=new Array(items.length);
  await Promise.all(Array.from({length:Math.min(limit,items.length||1)},async()=>{
    while(true){const i=next++; if(i>=items.length) return; out[i]=await fn(items[i]);}
  }));
  return out;
}

async function status(){
  const r=await turso.execute(`SELECT status,COUNT(*) n,COALESCE(SUM(discovered_skills),0) skills,COALESCE(SUM(bytes),0) bytes FROM skills_sh_external_exact_v1 GROUP BY status ORDER BY status`);
  return r.rows;
}

async function main(){
  await ensureSchema();
  if(process.argv.includes('--status')){console.log(JSON.stringify({event:'status',rows:await status()}));return;}
  let empty=0, completed=0, failures=0, cleanBatches=0;
  for(let b=0;b<MAX_BATCHES;b++){
    const claimed=await retry('claim',()=>rpc('skillset_skills_sh_d3_archive_claim_batch_external_v1',{p_limit:BATCH_SIZE}),5);
    const items=Array.isArray(claimed)?claimed:[];
    if(!items.length){empty++; if(empty>=3) break; await sleep(500); continue;}
    empty=0;
    const results=await mapLimit(items,CONCURRENCY,processOne);
    const ok=results.filter(x=>x?.ok).length;
    const bad=results.length-ok;
    completed+=ok; failures+=bad;

    if(bad===0){
      cleanBatches++;
      if(cleanBatches>=3 && CONCURRENCY<MAX_CONCURRENCY){
        CONCURRENCY=Math.min(MAX_CONCURRENCY,CONCURRENCY+4);
        cleanBatches=0;
        console.log(JSON.stringify({event:'adaptive_up',concurrency:CONCURRENCY}));
      }
    }else{
      cleanBatches=0;
      if(bad*5>=items.length && CONCURRENCY>MIN_CONCURRENCY){
        CONCURRENCY=Math.max(MIN_CONCURRENCY,CONCURRENCY-4);
        console.warn(JSON.stringify({event:'adaptive_down',concurrency:CONCURRENCY,bad,total:items.length}));
      }
    }

    console.log(JSON.stringify({event:'batch',batch:b+1,claimed:items.length,ok,bad,completed,failures,concurrency:CONCURRENCY}));
    if(bad>Math.ceil(items.length*0.25)){
      console.error(JSON.stringify({event:'breaker',reason:'failure_ratio',bad,total:items.length}));
      process.exitCode=2; break;
    }
  }
  console.log(JSON.stringify({event:'status',rows:await status(),completed,failures}));
}
main().catch(e=>{console.error(JSON.stringify({event:'fatal',error:String(e?.stack||e)}));process.exit(1);});
