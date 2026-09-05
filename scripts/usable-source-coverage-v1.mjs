import {createClient} from '@libsql/client';
import fs from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline';
import {createGunzip} from 'node:zlib';
import {createHash} from 'node:crypto';

const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const db=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const featureDir=process.env.COVERAGE_FEATURE_DIR||'coverage-features';
const auditDir=process.env.AUDIT_DIR||'audit';
const out=process.env.COVERAGE_OUT||'audit/usable-source-coverage.json';
const readJson=async p=>JSON.parse(await fs.readFile(p,'utf8'));
async function walk(d,a=[]){let es=[];try{es=await fs.readdir(d,{withFileTypes:true})}catch{return a}for(const e of es){const p=path.join(d,e.name);if(e.isDirectory())await walk(p,a);else a.push(p)}return a}
async function* ndjsonGz(file){const fh=await fs.open(file,'r'),stream=fh.createReadStream().pipe(createGunzip()),rl=readline.createInterface({input:stream,crlfDelay:Infinity});try{for await(const line of rl)if(line.trim())yield JSON.parse(line)}finally{await fh.close().catch(()=>{})}}
const keyHash=s=>createHash('sha256').update(s).digest('hex');

const inv=await readJson(path.join(auditDir,'source-inventory-reconciliation.json'));
if(inv.status!=='VERIFIED')throw new Error('source_inventory_not_verified');
const expected={
  'gitskills-legacy-hf':Number(inv.verifiedExactSourceRecords?.gitskills?.legacyRepresentatives||0),
  'gitskills-b2':Number(inv.verifiedExactSourceRecords?.gitskills?.b2Representatives||0),
  'skills-sh-b2':Number(inv.verifiedExactSourceRecords?.skillsSh?.exactSkillsInB2||0)
};
const expectedTotal=Object.values(expected).reduce((a,b)=>a+b,0);
if(expectedTotal!==Number(inv.correctedAuditTarget?.sourceRecordsBeforeCrossSourceDedup||0))throw new Error('inventory_total_mismatch');

const occSeen=new Set(),hashSeen=new Set(),perSystem=new Map();
function add(r){
  const sys=String(r.source_system||''),sk=String(r.source_key||''),ik=String(r.item_key||''),h=String(r.content_hash||'').toLowerCase();
  if(!sys||!sk||!ik||!/^[0-9a-f]{64}$/.test(h))return;
  const oid=keyHash(sys+'\0'+sk+'\0'+ik);
  if(!occSeen.has(oid)){occSeen.add(oid);perSystem.set(sys,(perSystem.get(sys)||0)+1)}
  hashSeen.add(h);
}

let lastRowid=0,tursoRows=0;
for(;;){
  const rows=(await db.execute({
    sql:'SELECT rowid rid,source_system,source_key,item_key,content_hash FROM final_audit_occurrence_v1 WHERE rowid>? ORDER BY rowid LIMIT 50000',
    args:[lastRowid]
  })).rows;
  if(!rows.length)break;
  for(const r of rows)add(r);
  tursoRows+=rows.length;
  lastRowid=Number(rows.at(-1).rid);
  console.log(JSON.stringify({event:'turso_occurrence_progress',rows:tursoRows,union:occSeen.size}));
  if(rows.length<50000)break;
}

const files=(await walk(featureDir)).filter(p=>/^features-.*\.ndjson\.gz$/i.test(path.basename(p)));
let featureRows=0;
for(const p of files){
  for await(const r of ndjsonGz(p)){featureRows++;add(r)}
  console.log(JSON.stringify({event:'feature_coverage_progress',file:path.basename(p),featureRows,union:occSeen.size,uniqueHashes:hashSeen.size}));
}

const observed=Object.fromEntries([...perSystem.entries()].sort((a,b)=>a[0].localeCompare(b[0])));
const checks={};
for(const [sys,n] of Object.entries(expected))checks[sys]={expected:n,observed:Number(observed[sys]||0),complete:Number(observed[sys]||0)===n};
const unknownSystems=Object.keys(observed).filter(x=>!(x in expected));
const complete=Object.values(checks).every(x=>x.complete)&&occSeen.size===expectedTotal&&unknownSystems.length===0;
const result={
  generatedAt:new Date().toISOString(),
  status:complete?'ALL_SOURCE_RECORDS_VERIFIED':'BLOCKED_SOURCE_COVERAGE_MISMATCH',
  policy:'No source skill may be excluded by SDLC/social classification. Zero-mask skills are retained as uncategorized.',
  expectedTotalSourceRecords:expectedTotal,
  observedUnionSourceRecords:occSeen.size,
  exactUniqueContentHashesAcrossAllSources:hashSeen.size,
  tursoOccurrenceRowsRead:tursoRows,
  writefreeFeatureRowsRead:featureRows,
  featureFiles:files.length,
  expected,observed,checks,unknownSystems,
  hardGate:{
    allGitSkillsIncluded:checks['gitskills-legacy-hf'].complete&&checks['gitskills-b2'].complete,
    allSkillsShIncluded:checks['skills-sh-b2'].complete,
    noScopeExclusion:true,
    usablePipelineAllowed:complete
  }
};
await fs.mkdir(path.dirname(out),{recursive:true});
await fs.writeFile(out,JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify(result,null,2));
if(!complete)process.exitCode=3;
