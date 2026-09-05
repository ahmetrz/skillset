import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline';
import {createGunzip,createGzip} from 'node:zlib';

const dir=process.env.EXACT_INPUT_DIR||'all-source-features';
const outDir=process.env.EXACT_OUT_DIR||'usable-exact-out';
const gatePath=process.env.EXACT_GATE_OUT||'audit/usable-exact-dedup-gate.json';
const expected=Number(process.env.EXACT_SOURCE_RECORDS||2051586);
async function walk(d,a=[]){let es=[];try{es=await fsp.readdir(d,{withFileTypes:true})}catch{return a}for(const e of es){const p=path.join(d,e.name);if(e.isDirectory())await walk(p,a);else a.push(p)}return a}
async function* ndjsonGz(file){const fh=await fsp.open(file,'r'),stream=fh.createReadStream().pipe(createGunzip()),rl=readline.createInterface({input:stream,crlfDelay:Infinity});try{for await(const line of rl)if(line.trim())yield JSON.parse(line)}finally{await fh.close().catch(()=>{})}}
function writer(file){const gz=createGzip({level:6}),ws=fs.createWriteStream(file);gz.pipe(ws);return {write:o=>gz.write(JSON.stringify(o)+'\n'),end:()=>new Promise((res,rej)=>{ws.on('finish',res);ws.on('error',rej);gz.on('error',rej);gz.end()})}}

await fsp.mkdir(outDir,{recursive:true});
const files=(await walk(dir)).filter(p=>/^features-.*\.ndjson\.gz$/i.test(path.basename(p))).sort();
if(!files.length)throw new Error('no_feature_files');
const canonical=new Map();
const occW=writer(path.join(outDir,'all-source-exact-occurrences.ndjson.gz'));
let sourceRecords=0,malformed=0;
const sourceCounts=new Map();
for(const p of files){
  for await(const r of ndjsonGz(p)){
    const h=String(r.content_hash||'').toLowerCase();
    if(!/^[0-9a-f]{64}$/.test(h)){malformed++;continue}
    sourceRecords++;
    const sys=String(r.source_system||'');
    sourceCounts.set(sys,(sourceCounts.get(sys)||0)+1);
    occW.write({source_system:sys,source_key:String(r.source_key||''),item_key:String(r.item_key||''),repo:r.repo||null,path:r.path||null,content_hash:h});
    const tuple=[sys,String(r.repo||''),String(r.path||''),String(r.locator||'')].join('\u0000');
    let x=canonical.get(h);
    if(!x){
      x={content_hash:h,occurrences:0,systems:new Set(),sdlc_mask:0,social_mask:0,risk_mask:0,provider_mask:0,char_count:Number(r.char_count||0),token_count:Number(r.token_count||0),simhash_hex:String(r.simhash_hex||''),c0:Number(r.c0||0),c1:Number(r.c1||0),c2:Number(r.c2||0),c3:Number(r.c3||0),skill_name:String(r.skill_name||''),sample_locator:String(r.sample_locator||r.locator||''),rep_tuple:tuple,rep_source_system:sys,rep_repo:r.repo||null,rep_path:r.path||null};
      canonical.set(h,x);
    }
    x.occurrences++;
    x.systems.add(sys);
    x.sdlc_mask|=Number(r.sdlc_mask||0);x.social_mask|=Number(r.social_mask||0);x.risk_mask|=Number(r.risk_mask||0);x.provider_mask|=Number(r.provider_mask||0);
    if(tuple<x.rep_tuple){x.rep_tuple=tuple;x.rep_source_system=sys;x.rep_repo=r.repo||null;x.rep_path=r.path||null;x.sample_locator=String(r.sample_locator||r.locator||'');if(r.skill_name)x.skill_name=String(r.skill_name)}
  }
  console.log(JSON.stringify({event:'exact_file_done',file:path.basename(p),sourceRecords,unique:canonical.size}));
}
await occW.end();
const canW=writer(path.join(outDir,'all-source-exact-canonical.ndjson.gz'));
let crossSourceHashes=0,maxOccurrences=0;
const systemHashCounts=new Map();
for(const x of canonical.values()){
  const systems=[...x.systems].sort();
  if(systems.length>1)crossSourceHashes++;
  for(const s of systems)systemHashCounts.set(s,(systemHashCounts.get(s)||0)+1);
  maxOccurrences=Math.max(maxOccurrences,x.occurrences);
  canW.write({content_hash:x.content_hash,occurrences:x.occurrences,source_systems:systems,simhash_hex:x.simhash_hex,c0:x.c0,c1:x.c1,c2:x.c2,c3:x.c3,char_count:x.char_count,token_count:x.token_count,sdlc_mask:x.sdlc_mask,social_mask:x.social_mask,risk_mask:x.risk_mask,provider_mask:x.provider_mask,skill_name:x.skill_name||null,sample_locator:x.sample_locator||null,representative:{source_system:x.rep_source_system,repo:x.rep_repo,path:x.rep_path}});
}
await canW.end();
const ok=malformed===0&&sourceRecords===expected;
const result={generatedAt:new Date().toISOString(),status:ok?'ALL_SOURCE_EXACT_DEDUP_VERIFIED':'BLOCKED_EXACT_DEDUP',inputFeatureFiles:files.length,sourceRecordsExpected:expected,sourceRecordsObserved:sourceRecords,uniqueContentHashes:canonical.size,exactDuplicateRecords:sourceRecords-canonical.size,crossSourceHashes,maxOccurrences,malformedHashes:malformed,sourceRecordCounts:Object.fromEntries([...sourceCounts].sort()),sourceUniqueHashMembership:Object.fromEntries([...systemHashCounts].sort()),hardGate:{allSourceRecordsRead:sourceRecords===expected,noMalformedHashes:malformed===0,exactDedupComplete:ok,noSourceLoss:sourceRecords===expected,usableNearPipelineAllowed:ok}};
await fsp.mkdir(path.dirname(gatePath),{recursive:true});await fsp.writeFile(gatePath,JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result,null,2));if(!ok)process.exitCode=3;
