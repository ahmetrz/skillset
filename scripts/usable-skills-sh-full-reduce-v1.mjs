import fs from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline';
import {createGunzip} from 'node:zlib';

const dir=process.env.SKILLS_FULL_DIR||'skills-sh-full';
const out=process.env.SKILLS_FULL_GATE_OUT||'audit/usable-skills-sh-full-snapshot-gate.json';
async function walk(d,a=[]){let es=[];try{es=await fs.readdir(d,{withFileTypes:true})}catch{return a}for(const e of es){const p=path.join(d,e.name);if(e.isDirectory())await walk(p,a);else a.push(p)}return a}
async function* ndjsonGz(file){const fh=await fs.open(file,'r'),stream=fh.createReadStream().pipe(createGunzip()),rl=readline.createInterface({input:stream,crlfDelay:Infinity});try{for await(const line of rl)if(line.trim())yield JSON.parse(line)}finally{await fh.close().catch(()=>{})}}
const all=await walk(dir);
const featureFiles=all.filter(p=>/^features-skills-sh-full-p\d{2}-of-16\.ndjson\.gz$/i.test(path.basename(p)));
const manifestFiles=all.filter(p=>/^manifest-skills-sh-full-p\d{2}-of-16\.json$/i.test(path.basename(p)));
const hashes=new Set(),occ=new Set(),sourceKeys=new Set();let records=0,inScope=0;
for(const p of featureFiles)for await(const r of ndjsonGz(p)){records++;hashes.add(String(r.content_hash||''));sourceKeys.add(String(r.source_key||''));occ.add(String(r.source_key||'')+'\u0000'+String(r.item_key||''));if(Number(r.sdlc_mask||0)||Number(r.social_mask||0))inScope++}
let assignedRepos=0,completedRepos=0,failedRepos=0,manifestFilesCount=0;
for(const p of manifestFiles){const m=JSON.parse(await fs.readFile(p,'utf8'));manifestFilesCount++;assignedRepos+=Number(m.assignedRepos||0);completedRepos+=Number(m.completedRepos||0);failedRepos+=Number(m.failedRepos||0)}
const ok=featureFiles.length===16&&manifestFilesCount===16&&assignedRepos===7555&&completedRepos===7555&&failedRepos===0&&records===188725&&occ.size===188725&&sourceKeys.size===7555;
const result={generatedAt:new Date().toISOString(),status:ok?'ALL_SKILLS_SH_EXACT_SNAPSHOT_VERIFIED':'BLOCKED_SKILLS_SH_EXACT_SNAPSHOT',featureFiles:featureFiles.length,manifestFiles:manifestFilesCount,repositories:{assigned:assignedRepos,completed:completedRepos,failed:failedRepos,sourceKeys:sourceKeys.size},records,occurrences:occ.size,distinctContentHashes:hashes.size,exactDuplicateRecords:records-hashes.size,inScope,uncategorized:records-inScope,hardGate:{all7555Repos:true&&sourceKeys.size===7555,all188725Files:records===188725&&occ.size===188725,noScopeExclusion:true,usablePipelineAllowed:ok}};
await fs.mkdir(path.dirname(out),{recursive:true});await fs.writeFile(out,JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result,null,2));if(!ok)process.exitCode=3;
