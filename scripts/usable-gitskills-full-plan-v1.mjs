import {createClient} from '@libsql/client';
import fs from 'node:fs/promises';

const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const db=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const out=process.env.BULK_PLAN_OUT||'usable-gitskills-b2-full-plan.json';
const parts=Math.max(1,Number(process.env.BULK_PARTITIONS||8));
const rows=(await db.execute("SELECT path FROM gitskills_packs WHERE path LIKE 'gitskills/discovery-b2-v2/%' ORDER BY path")).rows;
function parseIds(k){const b=String(k).split('/').pop()||'',m=b.match(/^pack-([0-9-]+)\.json\.gz$/);if(!m)throw new Error('bad_pack:'+k);return m[1].split('-').map(Number).filter(Number.isFinite)}
const packs=rows.map(r=>{const key=String(r.path),sourceKey='b2:'+key,shardIds=parseIds(key),ranges=shardIds.map(id=>({start:(id-100000)*100,end:(id-100000)*100+100}));return {key,sourceKey,shardIds,ranges,minRow:Math.min(...ranges.map(x=>x.start)),maxRow:Math.max(...ranges.map(x=>x.end))-1,expectedRows:shardIds.length*100}}).sort((a,b)=>a.minRow-b.minRow);
const ps=Array.from({length:parts},(_,i)=>({partition:i,packs:[],minRow:null,maxRow:null,expectedRows:0}));
const totalRows=packs.reduce((s,p)=>s+p.expectedRows,0),target=Math.max(1,totalRows/parts);
let pi=0,acc=0;
for(const p of packs){if(pi<parts-1&&acc>=target){pi++;acc=0}const z=ps[pi];z.packs.push(p);z.expectedRows+=p.expectedRows;acc+=p.expectedRows;z.minRow=z.minRow===null?p.minRow:Math.min(z.minRow,p.minRow);z.maxRow=z.maxRow===null?p.maxRow:Math.max(z.maxRow,p.maxRow)}
const plan={generatedAt:new Date().toISOString(),mode:'ALL_GITSKILLS_B2_PACKS',parts,totalPacks:packs.length,totalExpectedRawRows:totalRows,partitions:ps};
if(packs.length!==642)throw new Error('expected_642_packs_got_'+packs.length);
await fs.writeFile(out,JSON.stringify(plan,null,2)+'\n');
console.log(JSON.stringify({event:'usable_gitskills_full_plan',totalPacks:packs.length,totalExpectedRawRows:totalRows,partitions:ps.map(x=>({partition:x.partition,packs:x.packs.length,expectedRows:x.expectedRows,minRow:x.minRow,maxRow:x.maxRow}))},null,2));
