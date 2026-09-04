import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import tar from 'tar-stream';
import {createGunzip} from 'node:zlib';
import {Readable} from 'node:stream';

const exec=promisify(execFile);
const HF='https://datasets-server.huggingface.co/rows';
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function retry(label,fn,n=7){let last;for(let i=0;i<n;i++){try{return await fn(i)}catch(e){last=e;if(i===n-1)break;const m=String(e?.message||e),base=/429|503|504|timeout|aborted|rate/i.test(m)?1200:300;await sleep(Math.min(base*(2**i),12000)+Math.floor(Math.random()*250))}}throw new Error(label+': '+String(last?.message||last))}
async function hfShard(id){
  const u=new URL(HF);
  u.searchParams.set('dataset','mvaccargiu/gitskills');
  u.searchParams.set('config','artifacts');
  u.searchParams.set('split','train');
  u.searchParams.set('offset',String((Number(id)-100000)*100));
  u.searchParams.set('length','100');
  const body=await retry('hf_fallback_'+id,async()=>{
    const r=await fetch(u,{headers:{'user-agent':'skillset-final-audit-turbo/3.0'},signal:AbortSignal.timeout(30000)});
    if(!r.ok)throw new Error('HF '+r.status+': '+(await r.text()).slice(0,200));
    return r.json();
  },8);
  return {shardId:Number(id),rows:(body.rows||[]).map(x=>({...(x.row||{}),row_idx:x.row_idx})).filter(r=>r.dedup_primary===1||r.dedup_primary===true)};
}
export async function rebuildGitSkillsPack(key){
  const base=String(key).split('/').pop()||'';
  const m=base.match(/^pack-([0-9-]+)\.json\.gz$/);
  if(!m)throw new Error('cannot_parse_gitskills_pack:'+key);
  const ids=m[1].split('-').map(Number).filter(Number.isFinite);
  const shards=[];
  for(let i=0;i<ids.length;i+=2)shards.push(...await Promise.all(ids.slice(i,i+2).map(hfShard)));
  return {version:1,source:'gitskills-hf-fallback',shards};
}
async function git(args,cwd,timeout=120000){
  const r=await exec('git',args,{cwd,timeout,maxBuffer:64*1024*1024,env:{...process.env,GIT_TERMINAL_PROMPT:'0'}});
  return String(r.stdout||'');
}
async function historicalSha(owner,repo,at,dir){
  const url='https://github.com/'+owner+'/'+repo+'.git';
  const sym=await git(['ls-remote','--symref',url,'HEAD'],dir,60000);
  const branch=sym.match(/^ref:\s+refs\/heads\/([^\t\r\n]+)\s+HEAD/m)?.[1];
  if(!branch)throw new Error('default_branch_unresolved:'+owner+'/'+repo);
  await git(['init','-q'],dir);
  await git(['remote','add','origin',url],dir);
  const fetchDepth=async depth=>git(['fetch','-q','--filter=blob:none','--depth='+depth,'origin','refs/heads/'+branch],dir,180000);
  await fetchDepth(200);
  let sha=(await git(['rev-list','-1','--before='+at,'FETCH_HEAD'],dir)).trim();
  if(!sha){await fetchDepth(2000);sha=(await git(['rev-list','-1','--before='+at,'FETCH_HEAD'],dir)).trim()}
  if(!sha){
    await git(['fetch','-q','--filter=blob:none','--unshallow','origin','refs/heads/'+branch],dir,300000).catch(()=>{});
    sha=(await git(['rev-list','-1','--before='+at,'FETCH_HEAD'],dir)).trim();
  }
  if(!sha)throw new Error('historical_commit_unresolved:'+owner+'/'+repo+'@'+at);
  return sha;
}
async function codeloadSkills(owner,repo,sha){
  const url='https://codeload.github.com/'+encodeURIComponent(owner)+'/'+encodeURIComponent(repo)+'/tar.gz/'+encodeURIComponent(sha);
  const r=await fetch(url,{headers:{'user-agent':'skillset-final-audit-turbo/3.0'},signal:AbortSignal.timeout(90000)});
  if(!r.ok)throw new Error('codeload_'+r.status);
  const extract=tar.extract(),gunzip=createGunzip();
  const files=[],MAX_FILE=64*1024*1024,MAX_TOTAL=512*1024*1024;
  let total=0,settled=false;
  return await new Promise((resolve,reject)=>{
    const fail=e=>{if(!settled){settled=true;reject(e)}};
    extract.on('entry',(header,stream,next)=>{
      const name=String(header.name||'');
      if(header.type!=='file'||!/(^|\/)SKILL\.md$/i.test(name)){stream.resume();stream.on('end',next);return}
      const chunks=[];let n=0;
      stream.on('data',c=>{n+=c.length;total+=c.length;if(n>MAX_FILE||total>MAX_TOTAL){stream.destroy(new Error('skill_archive_oversize'));return}chunks.push(Buffer.from(c))});
      stream.on('end',()=>{files.push({path:name.replace(/^[^/]+\//,''),bytes:Buffer.concat(chunks)});next()});
      stream.on('error',fail);
    });
    extract.on('finish',()=>{if(!settled){settled=true;resolve(files)}});
    extract.on('error',fail);gunzip.on('error',fail);
    Readable.fromWeb(r.body).on('error',fail).pipe(gunzip).pipe(extract);
  });
}
async function sparseSkills(owner,repo,sha,dir){
  await git(['fetch','-q','--filter=blob:none','--depth=1','origin',sha],dir,180000).catch(()=>{});
  const raw=await exec('git',['ls-tree','-r','-z','--name-only',sha],{cwd:dir,timeout:120000,maxBuffer:128*1024*1024});
  const paths=Buffer.from(raw.stdout).toString('utf8').split('\0').filter(p=>/(^|\/)SKILL\.md$/i.test(p));
  const out=[];
  for(const p of paths){
    const r=await exec('git',['show',sha+':'+p],{cwd:dir,timeout:120000,maxBuffer:64*1024*1024});
    out.push({path:p,bytes:Buffer.from(r.stdout)});
  }
  return out;
}
export async function rebuildSkillsShPackHistorical(turso,key,bucket){
  const b2path='b2://'+bucket+'/'+key;
  const q=await turso.execute({sql:"SELECT source,owner,repo,updated_at FROM skills_sh_external_exact_v1 WHERE b2_path=? AND status='done' LIMIT 1",args:[b2path]});
  const row=q.rows[0];
  if(!row)throw new Error('skills_sh_locator_missing:'+key);
  const owner=String(row.owner),repo=String(row.repo),source=String(row.source),at=String(row.updated_at);
  const dir=await fs.mkdtemp(path.join(os.tmpdir(),'skillset-hist-'));
  try{
    const commit=await historicalSha(owner,repo,at,dir);
    let files;
    try{files=await codeloadSkills(owner,repo,commit)}
    catch(e){files=await sparseSkills(owner,repo,commit,dir)}
    return {version:1,source:'skills.sh-historical-fallback',repoSource:source,owner,repo,acquiredAt:at,commit,files:files.map(f=>({path:f.path,contentBase64:f.bytes.toString('base64')}))};
  }finally{await fs.rm(dir,{recursive:true,force:true}).catch(()=>{})}
}
