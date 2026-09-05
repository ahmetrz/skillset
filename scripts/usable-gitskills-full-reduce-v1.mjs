import fs from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline';
import {createGunzip} from 'node:zlib';

const dir=process.env.GITSKILLS_FULL_DIR||'gitskills-full';
const out=process.env.GITSKILLS_GATE_OUT||'audit/usable-gitskills-b2-full-gate.json';
const expected=Number(process.env.GITSKILLS_B2_EXPECTED||830231);
async function walk(d,a=[]){let es=[];try{es=await fs.readdir(d,{withFileTypes:true})}catch{return a}for(const e of es){const p=path.join(d,e.name);if(e.isDirectory())await walk(p,a);else a.push(p)}return a}
async function* ndjsonGz(file){const fh=await fs.open(file,'r'),stream=fh.createReadStream().pipe(createGunzip()),rl=readline.createInterface({input:stream,crlfDelay:Infinity});try{for await(const line of rl)if(line.trim())yield JSON.parse(line)}finally{await fh.close().catch(()=>{})}}
const all=await walk(dir);
const manifests=[],manifestFiles=all.filter(p=>/^manifest-partial-git-b2-bulk-p\d{2}-of-08\.json$/i.test(path.basename(p)));
for(const p of manifestFiles)manifests.push(JSON.parse(await fs.readFile(p,'utf8')));
const featureFiles=all.filter(p=>/^features-git-b2-bulk-p\d{2}-of-08\.ndjson\.gz$/i.test(path.basename(p)));
const hashes=new Set(),sourceKeys=new Set();let records=0,inScope=0;
for(const p of featureFiles)for await(const r of ndjsonGz(p)){records++;const h=String(r.content_hash||'').toLowerCase();if(/^[0-9a-f]{64}$/.test(h))hashes.add(h);sourceKeys.add(String(r.source_key||''));if(Number(r.sdlc_mask||0)||Number(r.social_mask||0))inScope++}
const units=new Map();let failedUnits=0;
for(const m of manifests){failedUnits+=Number(m.failedUnits||0);for(const u of m.units||[])units.set(String(u.source_key),u)}
const status=featureFiles.length===8&&manifestFiles.length===8&&units.size===642&&failedUnits===0&&hashes.size===expected?'ALL_GITSKILLS_B2_REPRESENTATIVES_VERIFIED':'BLOCKED_GITSKILLS_B2_MISMATCH';
const result={generatedAt:new Date().toISOString(),status,expectedDistinctRepresentatives:expected,observedDistinctContentHashes:hashes.size,records,duplicatesWithinFullSnapshot:records-hashes.size,inScope,uncategorized:records-inScope,featureFiles:featureFiles.length,manifestFiles:manifestFiles.length,sourceUnits:units.size,failedUnits,sourceKeysWithRecords:sourceKeys.size,hardGate:{all642PacksExtracted:units.size===642&&failedUnits===0,allExpectedRepresentativesPresent:hashes.size===expected,noScopeExclusion:true,usablePipelineAllowed:status==='ALL_GITSKILLS_B2_REPRESENTATIVES_VERIFIED'}};
await fs.mkdir(path.dirname(out),{recursive:true});await fs.writeFile(out,JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result,null,2));if(status!=='ALL_GITSKILLS_B2_REPRESENTATIVES_VERIFIED')process.exitCode=3;
