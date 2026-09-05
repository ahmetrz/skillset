import {createClient} from '@libsql/client';
import fs from 'node:fs/promises';
const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const db=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const info=(await db.execute("PRAGMA table_info(gitskills_packs)")).rows;
const rows=(await db.execute("SELECT * FROM gitskills_packs WHERE path LIKE 'gitskills/discovery-b2-v2/%' ORDER BY path")).rows;
const numericSums={};
for(const c of info){const n=String(c.name);let ok=true,sum=0;for(const r of rows){const v=r[n];if(v===null||v===undefined||v==='')continue;const x=Number(v);if(!Number.isFinite(x)){ok=false;break}sum+=x}if(ok)numericSums[n]=sum}
const jsonKeys={};
for(const r of rows.slice(0,20))for(const [k,v] of Object.entries(r)){if(typeof v!=='string'||v.length<2)continue;try{const j=JSON.parse(v);jsonKeys[k]=[...new Set([...(jsonKeys[k]||[]),...Object.keys(j||{})])]}catch{}}
const out={generatedAt:new Date().toISOString(),rowCount:rows.length,columns:info,sample:rows.slice(0,5),numericSums,jsonKeys};
await fs.mkdir('audit',{recursive:true});await fs.writeFile('audit/usable-gitskills-b2-metadata-diagnostic.json',JSON.stringify(out,null,2)+'\n');console.log(JSON.stringify(out,null,2));