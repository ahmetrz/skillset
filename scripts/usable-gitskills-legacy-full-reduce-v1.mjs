import fs from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline';
import {createGunzip} from 'node:zlib';

const dir=process.env.LEGACY_FULL_DIR||'gitskills-legacy-full';
const out=process.env.LEGACY_GATE_OUT||'audit/usable-gitskills-legacy-full-gate.json';
async function walk(d,a=[]){let es=[];try{es=await fs.readdir(d,{withFileTypes:true})}catch{return a}for(const e of es){const p=path.join(d,e.name);if(e.isDirectory())await walk(p,a);else a.push(p)}return a}
async function* ndjsonGz(file){const fh=await fs.open(file,'r'),stream=fh.createReadStream().pipe(createGunzip()),rl=readline.createInterface({input:stream,crlfDelay:Infinity});try{for await(const line of rl)if(line.trim())yield JSON.parse(line)}finally{await fh.close().catch(()=>{})}}
const all=await walk(dir),featureFiles=all.filter(p=>/^features-gitskills-legacy-full-p\d{2}-of-08\.ndjson\.gz$/i.test(path.basename(p))),manifestFiles=all.filter(p=>/^manifest-gitskills-legacy-full-p\d{2}-of-08\.json$/i.test(path.basename(p)));
const hashes=new Set(),sourceKeys=new Set();let records=0,inScope=0,rawRows=0;
for(const p of featureFiles)for await(const r of ndjsonGz(p)){records++;const h=String(r.content_hash||'').toLowerCase();if(/^[0-9a-f]{64}$/.test(h))hashes.add(h);sourceKeys.add(String(r.source_key||''));if(Number(r.sdlc_mask||0)||Number(r.social_mask||0))inScope++}
const manifestUnits=new Set();
for(const p of manifestFiles){const m=JSON.parse(await fs.readFile(p,'utf8'));rawRows+=Number(m.selectedRawRows||0);for(const k of Object.keys(m.unitSkillCounts||{}))manifestUnits.add(k)}
const status=featureFiles.length===8&&manifestFiles.length===8&&rawRows===1913200&&manifestUnits.size===19132&&records===hashes.size?'ALL_GITSKILLS_LEGACY_RANGE_VERIFIED':'BLOCKED_GITSKILLS_LEGACY_MISMATCH';
const result={generatedAt:new Date().toISOString(),status,rawRowsExpected:1913200,rawRowsObserved:rawRows,sourceUnitsExpected:19132,sourceUnitsObserved:manifestUnits.size,records,distinctContentHashes:hashes.size,duplicatesWithinDedupPrimarySnapshot:records-hashes.size,inScope,uncategorized:records-inScope,featureFiles:featureFiles.length,manifestFiles:manifestFiles.length,hardGate:{all19132ShardsScanned:rawRows===1913200&&manifestUnits.size===19132,dedupPrimaryUnique:records===hashes.size,noScopeExclusion:true,usablePipelineAllowed:status==='ALL_GITSKILLS_LEGACY_RANGE_VERIFIED'}};
await fs.mkdir(path.dirname(out),{recursive:true});await fs.writeFile(out,JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result,null,2));if(status!=='ALL_GITSKILLS_LEGACY_RANGE_VERIFIED')process.exitCode=3;
