import {createClient} from '@libsql/client';
import fs from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline';
import {createGunzip} from 'node:zlib';

const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const db=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const featureDir=process.env.COVERAGE_FEATURE_DIR||'coverage-features';
const out=process.env.SKILLS_GATE_OUT||'audit/usable-skills-sh-source-gate.json';
async function walk(d,a=[]){let es=[];try{es=await fs.readdir(d,{withFileTypes:true})}catch{return a}for(const e of es){const p=path.join(d,e.name);if(e.isDirectory())await walk(p,a);else a.push(p)}return a}
async function* ndjsonGz(file){const fh=await fs.open(file,'r'),stream=fh.createReadStream().pipe(createGunzip()),rl=readline.createInterface({input:stream,crlfDelay:Infinity});try{for await(const line of rl)if(line.trim())yield JSON.parse(line)}finally{await fh.close().catch(()=>{})}}

const rows=(await db.execute("SELECT source,b2_path,discovered_skills FROM skills_sh_external_exact_v1 WHERE status='done' AND b2_path IS NOT NULL ORDER BY b2_path")).rows;
const expected=new Map();
for(const r of rows){
  const p=String(r.b2_path||''),m=p.match(/^b2:\/\/[^/]+\/(.+)$/);if(!m)continue;
  expected.set('b2:'+m[1],{source:String(r.source||''),expected:Number(r.discovered_skills||0)});
}
const seen=new Map();
function add(sk,ik){if(!expected.has(sk)||!ik)return;if(!seen.has(sk))seen.set(sk,new Set());seen.get(sk).add(ik)}
let last=0,tursoRows=0;
for(;;){
  const rs=(await db.execute({sql:"SELECT rowid rid,source_key,item_key FROM final_audit_occurrence_v1 WHERE source_system='skills-sh-b2' AND rowid>? ORDER BY rowid LIMIT 50000",args:[last]})).rows;
  if(!rs.length)break;
  for(const r of rs)add(String(r.source_key||''),String(r.item_key||''));
  tursoRows+=rs.length;last=Number(rs.at(-1).rid);if(rs.length<50000)break;
}
const files=(await walk(featureDir)).filter(p=>/^features-skills-.*\.ndjson\.gz$/i.test(path.basename(p)));
let featureRows=0;
for(const p of files)for await(const r of ndjsonGz(p)){featureRows++;add(String(r.source_key||''),String(r.item_key||''))}
let expectedSkills=0,observedSkills=0;const missing=[],extra=[],ok=[];
for(const [sk,e] of expected){const got=seen.get(sk)?.size||0;expectedSkills+=e.expected;observedSkills+=got;const d=got-e.expected;if(d<0)missing.push({sourceKey:sk,source:e.source,expected:e.expected,observed:got,missing:-d});else if(d>0)extra.push({sourceKey:sk,source:e.source,expected:e.expected,observed:got,extra:d});else ok.push(sk)}
const result={generatedAt:new Date().toISOString(),status:missing.length===0?'ALL_SKILLS_SH_ACQUISITION_FILES_PRESENT':'BLOCKED_SKILLS_SH_MISSING',expectedRepos:expected.size,expectedSkills,observedSkillsInCurrentSourceKeys:observedSkills,completeRepos:ok.length,missingRepos:missing.length,extraRepos:extra.length,missingSkills:missing.reduce((s,x)=>s+x.missing,0),extraSkills:extra.reduce((s,x)=>s+x.extra,0),tursoRowsRead:tursoRows,featureRowsRead:featureRows,featureFiles:files.length,missing:missing.slice(0,500),extra:extra.slice(0,500),hardGate:{allCurrentExactSourceKeysCovered:missing.length===0,allSkillsShIncluded:missing.length===0&&expectedSkills===188725}};
await fs.mkdir(path.dirname(out),{recursive:true});await fs.writeFile(out,JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result,null,2));if(missing.length)process.exitCode=3;
