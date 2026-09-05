import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import {createGunzip,createGzip} from 'node:zlib';

const DIR=process.env.REPAIR_DIR||'gitskills-b2-repair';
const PART=6,PARTS=8,SUBS=4;
const partBase='p06';
const subFeatureFiles=[];
const subManifestFiles=[];
for(let s=0;s<SUBS;s++){
  const tag='-s'+String(s).padStart(2,'0')+'-of-'+String(SUBS).padStart(2,'0')+'-of-'+String(PARTS).padStart(2,'0');
  subFeatureFiles.push(path.join(DIR,'features-git-b2-bulk-'+partBase+tag+'.ndjson.gz'));
  subManifestFiles.push(path.join(DIR,'manifest-partial-git-b2-bulk-'+partBase+tag+'.json'));
}
for(const p of [...subFeatureFiles,...subManifestFiles])await fsp.access(p);

const outFeature=path.join(DIR,'features-git-b2-bulk-p06-of-08.ndjson.gz');
const gz=createGzip({level:6});
const ws=fs.createWriteStream(outFeature);
gz.pipe(ws);
for(const p of subFeatureFiles){
  const src=fs.createReadStream(p).pipe(createGunzip());
  for await(const chunk of src)gz.write(chunk);
}
gz.end();
await new Promise((res,rej)=>{ws.on('finish',res);ws.on('error',rej);gz.on('error',rej)});

const manifests=[];
for(const p of subManifestFiles)manifests.push(JSON.parse(await fsp.readFile(p,'utf8')));
if(new Set(manifests.map(x=>Number(x.subpart))).size!==SUBS)throw new Error('subpart_identity_mismatch');
if(manifests.some(x=>Number(x.failedUnits||0)!==0))throw new Error('subpart_failure');

const units=new Map();
let skills=0,inScope=0,policyFacts=0,records=0;
for(const m of manifests){
  skills+=Number(m.skills||0);inScope+=Number(m.inScope||0);policyFacts+=Number(m.policyFacts||0);records+=Number(m.records||0);
  for(const u of m.units||[]){
    const k=String(u.source_key);
    const cur=units.get(k)||{source_key:k,source_system:'gitskills-b2',status:'done',rows_scanned:0,skills_indexed:0,records:0,error:null};
    cur.rows_scanned+=Number(u.rows_scanned||0);
    cur.skills_indexed+=Number(u.skills_indexed||0);
    cur.records+=Number(u.records||0);
    units.set(k,cur);
  }
}
const arr=[...units.values()];
const manifest={
  generatedAt:new Date().toISOString(),source:'usable-gitskills-b2-full-repair-subparts',sourceSystem:'gitskills-b2',
  partition:PART,partitions:PARTS,subparts:SUBS,assignedUnits:arr.length,completedUnits:arr.length,
  failedUnits:0,skills,inScope,policyFacts,records,units:arr
};
await fsp.writeFile(path.join(DIR,'manifest-partial-git-b2-bulk-p06-of-08.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({event:'b2_p6_repair_combined',units:arr.length,skills,inScope,records,outFeature}));
