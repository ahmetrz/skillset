import { createHash } from 'node:crypto';
import { gunzipSync, gzipSync } from 'node:zlib';

const need=n=>{const v=process.env[n];if(!v)throw new Error(`Missing ${n}`);return v};
const base=need('GITSKILLS_PROJECTION_RESCUE_URL');
const worker=Number(need('WORKER'));
const workers=Number(need('WORKERS'));
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const MAX_GZIP=1_500_000;

async function retry(label,fn,attempts=8){let last;for(let i=0;i<attempts;i++){try{return await fn()}catch(e){last=e;if(i===attempts-1)break;const delay=Math.min(1000*2**i,30000)+Math.floor(Math.random()*500);console.warn(JSON.stringify({event:'retry',label,attempt:i+1,delay,error:String(e).slice(0,300)}));await sleep(delay)}}throw last}
async function oidc(){const u=new URL(need('ACTIONS_ID_TOKEN_REQUEST_URL'));u.searchParams.set('audience','gitskills-projection-rescue');const r=await fetch(u,{headers:{authorization:`Bearer ${need('ACTIONS_ID_TOKEN_REQUEST_TOKEN')}`}});if(!r.ok)throw new Error(`oidc_${r.status}`);return (await r.json()).value}
const digest=bytes=>createHash('sha256').update(bytes).digest('hex');
const key=x=>`${String(x.row_idx)}|${String(x.file_sha||'')}`;

async function api(token,operation,body,headers={},url=base){return retry(operation,async()=>{const r=await fetch(url,{method:body===undefined?'GET':'POST',headers:{authorization:`Bearer ${token}`,...headers},body,signal:AbortSignal.timeout(180000)});const text=await r.text();if(!r.ok)throw new Error(`${operation}_${r.status}:${text.slice(0,500)}`);return text?JSON.parse(text):{}})}
async function download(label,url){return retry(label,async()=>{const r=await fetch(url,{signal:AbortSignal.timeout(180000)});if(!r.ok)throw new Error(`${label}_${r.status}`);return Buffer.from(await r.arrayBuffer())})}
async function upload(token,path,bytes){const sha=digest(bytes);await api(token,`upload_${path}`,bytes,{'content-type':'application/gzip','x-operation':'upload','x-storage-path':path,'x-sha256':sha});return sha}

function encodeInput(template,shards){return gzipSync(Buffer.from(JSON.stringify({...template,generatedAt:new Date().toISOString(),shards})),{level:6})}
function boundedParts(template,shards){
  const out=[];
  const splitShard=shard=>{
    const bytes=encodeInput(template,[shard]);
    if(bytes.length<=MAX_GZIP){out.push({shards:[shard],inputBytes:bytes});return}
    const rows=Array.isArray(shard.rows)?shard.rows:[];
    if(rows.length<=1)throw new Error(`single_row_too_large:${bytes.length}`);
    const mid=Math.ceil(rows.length/2);
    splitShard({...shard,rows:rows.slice(0,mid)});
    splitShard({...shard,rows:rows.slice(mid)});
  };
  const split=group=>{
    const bytes=encodeInput(template,group);
    if(bytes.length<=MAX_GZIP){out.push({shards:group,inputBytes:bytes});return}
    if(group.length===1){splitShard(group[0]);return}
    const mid=Math.ceil(group.length/2);
    split(group.slice(0,mid));
    split(group.slice(mid));
  };
  for(let i=0;i<shards.length;i+=4)split(shards.slice(i,i+4));
  return out;
}

async function rescue(token,parent){
  const [inputGz,prefilterGz]=await Promise.all([download('input',parent.input_url),download('prefilter',parent.prefilter_url)]);
  const input=JSON.parse(gunzipSync(inputGz));
  const pf=JSON.parse(gunzipSync(prefilterGz));
  if(!Array.isArray(input.shards)||!input.shards.length||!Array.isArray(pf.decisions))throw new Error('invalid_parent_payload');
  const template={...input};delete template.shards;
  const parts=boundedParts(template,input.shards);
  const sourceKeys=new Set();
  for(const shard of input.shards)for(const row of shard.rows||[]){const k=key(row);if(sourceKeys.has(k))throw new Error(`duplicate_source_row_key:${k}`);sourceKeys.add(k)}
  const rowToPart=new Map();
  parts.forEach((part,i)=>part.shards.forEach(shard=>(shard.rows||[]).forEach(row=>{const k=key(row);if(rowToPart.has(k))throw new Error(`duplicate_row_key:${k}`);rowToPart.set(k,i)})));
  if(rowToPart.size!==sourceKeys.size)throw new Error(`split_row_count_mismatch:${rowToPart.size}/${sourceKeys.size}`);
  for(const k of sourceKeys)if(!rowToPart.has(k))throw new Error(`missing_split_row_key:${k}`);
  const decisions=Array.from({length:parts.length},()=>[]);
  for(const d of pf.decisions){const i=rowToPart.get(key(d));if(i===undefined)throw new Error(`orphan_prefilter_decision:${key(d)}`);decisions[i].push(d)}
  if(decisions.reduce((n,x)=>n+x.length,0)!==pf.decisions.length)throw new Error('decision_count_mismatch');
  const retain=decisions.reduce((n,x)=>n+x.filter(d=>d.retain).length,0);
  if(retain!==Number(pf.retain||0))throw new Error(`retain_count_mismatch:${retain}/${pf.retain}`);

  const id=createHash('sha256').update(parent.input_path).digest('hex').slice(0,16);
  const children=[];
  for(let i=0;i<parts.length;i++){
    const suffix=String(i+1).padStart(3,'0');
    const inputPath=`gitskills/discovery-projection-split-v1/${id}-part-${suffix}.json.gz`;
    const prefilterPath=`gitskills/prefilter-projection-split-v1/${id}-part-${suffix}.json.gz`;
    const childPf={...pf,inputPath,generatedAt:new Date().toISOString(),decisions:decisions[i],retain:decisions[i].filter(d=>d.retain).length};
    const pfBytes=gzipSync(Buffer.from(JSON.stringify(childPf)),{level:6});
    await Promise.all([upload(token,inputPath,parts[i].inputBytes),upload(token,prefilterPath,pfBytes)]);
    children.push({input_path:inputPath,prefilter_path:prefilterPath});
  }
  await api(token,'finalize',JSON.stringify({parent_input:parent.input_path,parent_prefilter:parent.prefilter_path,children}),{'content-type':'application/json','x-operation':'finalize'});
  console.log(JSON.stringify({event:'rescued',parent:parent.input_path,children:children.length,input_bytes:inputGz.length,decisions:pf.decisions.length,retain}));
}

const token=await oidc();
const manifestUrl=new URL(base);
manifestUrl.searchParams.set('worker',String(worker));
manifestUrl.searchParams.set('workers',String(workers));
const manifest=await api(token,'manifest',undefined,{},manifestUrl);
console.log(JSON.stringify({event:'manifest',worker,workers,parents:manifest.parents?.length||0}));
for(const parent of manifest.parents||[])await rescue(token,parent);
