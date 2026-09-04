import {createClient} from '@libsql/client';
import fs from 'node:fs/promises';
import path from 'node:path';

const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const turso=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const manifestDir=process.env.AUDIT_SKIP_MANIFEST_DIR||'bulk-skip';
const outFile=process.env.BULK_PLAN_OUT||'bulk-plan.json';
const parts=Math.max(1,Number(process.env.BULK_PARTITIONS||8));

async function q(sql,args=[]){return (await turso.execute({sql,args})).rows}
async function walk(dir,out=[]){let es=[];try{es=await fs.readdir(dir,{withFileTypes:true})}catch{return out}
  for(const e of es){const p=path.join(dir,e.name);if(e.isDirectory())await walk(p,out);else if(/^manifest-.*\.json$/i.test(e.name))out.push(p)}return out}

const releaseDone=new Set();
for(const p of await walk(manifestDir)){
  try{const m=JSON.parse(await fs.readFile(p,'utf8'));for(const u of m.units||[])if(u.status==='done'&&String(u.source_system)==='gitskills-b2')releaseDone.add(String(u.source_key))}catch{}
}
const [paths,di,dp]=await Promise.all([
  q("SELECT path FROM gitskills_packs WHERE path LIKE 'gitskills/discovery-b2-v2/%' ORDER BY path"),
  q("SELECT source_key FROM final_audit_unit_v1 WHERE source_system='gitskills-b2' AND status='done'"),
  q("SELECT source_key FROM final_audit_policy_unit_v2 WHERE source_system='gitskills-b2' AND status='done'")
]);
const ingest=new Set(di.map(x=>String(x.source_key))),policy=new Set(dp.map(x=>String(x.source_key)));
function parseIds(k){const b=String(k).split('/').pop()||'',m=b.match(/^pack-([0-9-]+)\.json\.gz$/);if(!m)throw new Error('bad pack '+k);return m[1].split('-').map(Number).filter(Number.isFinite)}
const unresolved=[];
for(const row of paths){
  const key=String(row.path),sk='b2:'+key;
  if(releaseDone.has(sk)||(ingest.has(sk)&&policy.has(sk)))continue;
  const shardIds=parseIds(key),ranges=shardIds.map(id=>({start:(id-100000)*100,end:(id-100000)*100+100}));
  unresolved.push({key,sourceKey:sk,shardIds,ranges,minRow:Math.min(...ranges.map(x=>x.start)),maxRow:Math.max(...ranges.map(x=>x.end))-1,expectedRows:shardIds.length*100});
}
unresolved.sort((a,b)=>a.minRow-b.minRow);
const partitions=Array.from({length:parts},(_,i)=>({partition:i,packs:[],minRow:null,maxRow:null,expectedRows:0}));
const totalRows=unresolved.reduce((s,p)=>s+p.expectedRows,0),target=Math.max(1,totalRows/parts);
let pi=0,acc=0;
for(const p of unresolved){
  if(pi<parts-1&&acc>=target){pi++;acc=0}
  partitions[pi].packs.push(p);partitions[pi].expectedRows+=p.expectedRows;acc+=p.expectedRows;
  partitions[pi].minRow=partitions[pi].minRow===null?p.minRow:Math.min(partitions[pi].minRow,p.minRow);
  partitions[pi].maxRow=partitions[pi].maxRow===null?p.maxRow:Math.max(partitions[pi].maxRow,p.maxRow);
}
const plan={generatedAt:new Date().toISOString(),parts,totalPacks:paths.length,releaseDone:releaseDone.size,existingIngest:ingest.size,existingPolicy:policy.size,unresolvedPacks:unresolved.length,totalExpectedRows:totalRows,partitions};
await fs.writeFile(outFile,JSON.stringify(plan,null,2)+'\n');
console.log(JSON.stringify({event:'bulk_plan',...plan,partitions:partitions.map(p=>({partition:p.partition,packs:p.packs.length,minRow:p.minRow,maxRow:p.maxRow,expectedRows:p.expectedRows}))},null,2));
