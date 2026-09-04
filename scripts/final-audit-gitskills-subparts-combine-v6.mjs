import fs from 'node:fs/promises';
import path from 'node:path';

const DIR=process.env.SUBPART_MANIFEST_DIR||'p6-parts';
const OUT=process.env.SUBPART_COMBINED_OUT||'p6-combined';
const PART=Number(process.env.BULK_PARTITION||6),PARTS=Math.max(1,Number(process.env.BULK_PARTITIONS||8)),SUBS=Math.max(1,Number(process.env.BULK_ITEM_SUBPARTS||4));
const files=(await fs.readdir(DIR)).filter(x=>/^manifest-partial-git-b2-bulk-p06-s\d+-of-04-of-08\.json$/.test(x)).sort();
if(files.length!==SUBS)throw new Error('expected '+SUBS+' partial manifests, found '+files.length);
const manifests=[];for(const f of files)manifests.push(JSON.parse(await fs.readFile(path.join(DIR,f),'utf8')));
const gotSubs=new Set(manifests.map(x=>Number(x.subpart)));
if(gotSubs.size!==SUBS)throw new Error('duplicate/missing subparts');
if(manifests.some(x=>Number(x.failedUnits||0)!==0))throw new Error('partial manifest contains failures');
const units=new Map();
let skills=0,inScope=0,policyFacts=0,records=0;
for(const m of manifests){
  skills+=Number(m.skills||0);inScope+=Number(m.inScope||0);policyFacts+=Number(m.policyFacts||0);records+=Number(m.records||0);
  for(const u of m.units||[]){
    const k=String(u.source_key);
    const cur=units.get(k)||{source_key:k,source_system:'gitskills-b2',status:'done',rows_scanned:0,skills_indexed:0,records:0,error:null};
    cur.rows_scanned+=Number(u.rows_scanned||0);cur.skills_indexed+=Number(u.skills_indexed||0);cur.records+=Number(u.records||0);
    units.set(k,cur);
  }
}
await fs.mkdir(OUT,{recursive:true});
const arr=[...units.values()];
const manifest={generatedAt:new Date().toISOString(),source:'git-b2-parquet-bulk-subparts',sourceSystem:'gitskills-b2',partition:PART,partitions:PARTS,subparts:SUBS,assignedUnits:arr.length,completedUnits:arr.length,failedUnits:0,skills,inScope,policyFacts,records,units:arr};
const out=path.join(OUT,'manifest-git-b2-bulk-p06-of-08.json');
await fs.writeFile(out,JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({event:'p6_subparts_combined',units:arr.length,skills,inScope,policyFacts,records}));
