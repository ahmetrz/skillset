import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline';
import {createGunzip,createGzip} from 'node:zlib';
import {feature,numericFacts} from './final-audit-feature-core-v4.mjs';

const PART=Number(process.env.BULK_PARTITION||0);
const PARTS=Math.max(1,Number(process.env.BULK_PARTITIONS||8));
const SUB=Number(process.env.BULK_ITEM_SUBPART??-1);
const SUBS=Math.max(1,Number(process.env.BULK_ITEM_SUBPARTS||1));
const DIR=process.env.BULK_OUT_DIR||'bulk-v5-out';
const suffix=SUB>=0?'-s'+String(SUB).padStart(2,'0')+'-of-'+String(SUBS).padStart(2,'0'):'';
const raw=path.join(DIR,'raw-git-b2-bulk-p'+String(PART).padStart(2,'0')+suffix+'.ndjson.gz');
const unitFile=path.join(DIR,'units-git-b2-bulk-p'+String(PART).padStart(2,'0')+suffix+'.json');
const featureFile=path.join(DIR,'features-git-b2-bulk-p'+String(PART).padStart(2,'0')+suffix+'-of-'+String(PARTS).padStart(2,'0')+'.ndjson.gz');
const manifestFile=path.join(DIR,'manifest-partial-git-b2-bulk-p'+String(PART).padStart(2,'0')+suffix+'-of-'+String(PARTS).padStart(2,'0')+'.json');

const plan=JSON.parse(await fsp.readFile(unitFile,'utf8'));
const counts=new Map(),records=new Map();
for(const u of plan.packs||[]){counts.set(String(u.source_key),0);records.set(String(u.source_key),0)}

const source=fs.createReadStream(raw).pipe(createGunzip());
const rl=readline.createInterface({input:source,crlfDelay:Infinity});
const gz=createGzip({level:6}),out=fs.createWriteStream(featureFile);gz.pipe(out);
let total=0,inScope=0,factsN=0;
for await(const line of rl){
  if(!line.trim())continue;
  const x=JSON.parse(line),sk=String(x.sourceKey),text=String(x.text||'');
  if(!text)continue;
  const f=feature(text,String(x.locator||'')),facts=numericFacts(text);
  if(f.sdlc_mask!==0||f.social_mask!==0)inScope++;
  factsN+=facts.length;total++;
  counts.set(sk,(counts.get(sk)||0)+1);
  records.set(sk,(records.get(sk)||0)+1);
  const rec={
    source_key:sk,item_key:String(x.itemKey),source_system:'gitskills-b2',
    repo:x.repo||null,path:x.path||null,locator:String(x.locator||''),
    content_hash:f.h,simhash_hex:f.simhash_hex,c0:f.c0,c1:f.c1,c2:f.c2,c3:f.c3,
    char_count:f.char_count,token_count:f.token_count,sdlc_mask:f.sdlc_mask,social_mask:f.social_mask,
    risk_mask:f.risk_mask,provider_mask:f.provider_mask,policy_sig:f.policy_sig,
    skill_name:f.skill_name||null,sample_locator:f.sample_locator,policy_facts:facts
  };
  gz.write(JSON.stringify(rec)+'\n');
  if(total%10000===0)console.log(JSON.stringify({event:'bulk_transform_progress',partition:PART,total,inScope,facts:factsN}));
}
gz.end();
await new Promise((resolve,reject)=>{out.on('finish',resolve);out.on('error',reject);gz.on('error',reject)});

const units=(plan.packs||[]).map(u=>({
  source_key:String(u.source_key),source_system:'gitskills-b2',status:u.status,
  rows_scanned:Number(u.rows_scanned||0),skills_indexed:counts.get(String(u.source_key))||0,
  records:records.get(String(u.source_key))||0,error:u.status==='done'?null:'parquet_extract_incomplete'
}));
const manifest={
  generatedAt:new Date().toISOString(),source:'git-b2-parquet-bulk',sourceSystem:'gitskills-b2',
  partition:PART,partitions:PARTS,subpart:SUB,subparts:SUBS,assignedUnits:units.length,
  completedUnits:units.filter(x=>x.status==='done'||x.status==='partial').length,
  failedUnits:units.filter(x=>x.status==='error').length,skills:total,inScope,policyFacts:factsN,records:total,units
};
await fsp.writeFile(manifestFile,JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({event:'bulk_transform_complete',partition:PART,parts:PARTS,units:units.length,skills:total,inScope,policyFacts:factsN,failedUnits:manifest.failedUnits}));
if(manifest.failedUnits)process.exitCode=3;
