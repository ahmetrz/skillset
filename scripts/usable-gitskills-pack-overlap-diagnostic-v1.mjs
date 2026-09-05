import {createClient} from '@libsql/client';
import fs from 'node:fs/promises';
const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const db=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const rows=(await db.execute("SELECT path FROM gitskills_packs WHERE path LIKE 'gitskills/discovery-b2-v2/%' ORDER BY path")).rows;
const byShard=new Map(),packs=[];
function ids(k){const b=String(k).split('/').pop()||'',m=b.match(/^pack-([0-9-]+)\.json\.gz$/);if(!m)throw new Error('bad:'+k);return m[1].split('-').map(Number)}
for(const r of rows){const key=String(r.path),a=ids(key);packs.push({key,ids:a});for(const id of a){if(!byShard.has(id))byShard.set(id,[]);byShard.get(id).push(key)}}
const dup=[...byShard.entries()].filter(([,v])=>v.length>1).map(([shardId,keys])=>({shardId,keys,count:keys.length})).sort((a,b)=>a.shardId-b.shardId);
const allIds=[...byShard.keys()].sort((a,b)=>a-b),gaps=[];for(let i=1;i<allIds.length;i++)if(allIds[i]!==allIds[i-1]+1)gaps.push({after:allIds[i-1],before:allIds[i],gap:allIds[i]-allIds[i-1]-1});
const out={generatedAt:new Date().toISOString(),packCount:packs.length,uniqueShardIds:byShard.size,totalShardRefs:packs.reduce((s,x)=>s+x.ids.length,0),duplicateShardIds:dup.length,duplicateShardRefs:dup.reduce((s,x)=>s+x.count-1,0),duplicates:dup.slice(0,500),minShard:allIds[0],maxShard:allIds.at(-1),gaps:gaps.slice(0,500)};
await fs.mkdir('audit',{recursive:true});await fs.writeFile('audit/usable-gitskills-pack-overlap-diagnostic.json',JSON.stringify(out,null,2)+'\n');console.log(JSON.stringify(out,null,2));
