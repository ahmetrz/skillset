import {createClient} from '@libsql/client';
import fs from 'node:fs/promises';
import path from 'node:path';
import {createGunzip,gzipSync} from 'node:zlib';
import readline from 'node:readline';

const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const turso=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const input=process.env.AUDIT_V4_INPUT||'v4-assets',outDir=process.env.AUDIT_V4_MERGE_OUT||'v4-merge-out';
const PARTS=Math.max(1,Number(process.env.NEAR_PARTITIONS||20));
async function q(sql,args=[]){return (await turso.execute({sql,args})).rows}
async function files(dir){const out=[];for(const e of await fs.readdir(dir,{withFileTypes:true})){const p=path.join(dir,e.name);if(e.isDirectory())out.push(...await files(p));else out.push(p)}return out}
async function* ndjsonGz(file){
  const input=await fs.open(file,'r'),stream=input.createReadStream().pipe(createGunzip());
  const rl=readline.createInterface({input:stream,crlfDelay:Infinity});
  try{for await(const line of rl)if(line.trim())yield JSON.parse(line)}
  finally{await input.close().catch(()=>{})}
}
await fs.mkdir(outDir,{recursive:true});
const all=await files(input),manifestFiles=all.filter(x=>/manifest-.*\.json$/.test(path.basename(x))),featureFiles=all.filter(x=>/features-.*\.ndjson\.gz$/.test(path.basename(x)));
const manifests=[];
for(const p of manifestFiles)manifests.push(JSON.parse(await fs.readFile(p,'utf8')));
const releaseDone=new Map(),releaseErrors=[];
for(const m of manifests)for(const u of m.units||[]){
  const k=String(u.source_system)+'|'+String(u.source_key);
  if(u.status==='done')releaseDone.set(k,u);
  else releaseErrors.push(u);
}
const [g,s,di,dp]=await Promise.all([
  q("SELECT count(*) n FROM gitskills_packs WHERE path LIKE 'gitskills/discovery-b2-v2/%'"),
  q("SELECT count(*) n FROM skills_sh_external_exact_v1 WHERE status='done' AND b2_path IS NOT NULL"),
  q("SELECT source_system,source_key FROM final_audit_unit_v1 WHERE status='done'"),
  q("SELECT source_system,source_key FROM final_audit_policy_unit_v2 WHERE status='done'")
]);
const ingestSets=new Map(),policySets=new Map(),releaseSets=new Map();
for(const r of di){const sys=String(r.source_system);if(!ingestSets.has(sys))ingestSets.set(sys,new Set());ingestSets.get(sys).add(String(r.source_key))}
for(const r of dp){const sys=String(r.source_system);if(!policySets.has(sys))policySets.set(sys,new Set());policySets.get(sys).add(String(r.source_key))}
for(const k of releaseDone.keys()){const pos=k.indexOf('|'),sys=k.slice(0,pos),sourceKey=k.slice(pos+1);if(!releaseSets.has(sys))releaseSets.set(sys,new Set());releaseSets.get(sys).add(sourceKey)}
const expected={'gitskills-legacy-hf':19132,'gitskills-b2':Number(g[0]?.n||0),'skills-sh-b2':Number(s[0]?.n||0)};
const effective={};
for(const sys of Object.keys(expected)){
  const oldI=ingestSets.get(sys)||new Set(),oldP=policySets.get(sys)||new Set(),rel=releaseSets.get(sys)||new Set();
  const ingestUnion=new Set([...oldI,...rel]),policyUnion=new Set([...oldP,...rel]);
  effective[sys]={expected:expected[sys],existingIngest:oldI.size,existingPolicy:oldP.size,releaseDone:rel.size,ingestEffective:ingestUnion.size,policyEffective:policyUnion.size};
}
const writers=Array.from({length:PARTS},()=>[]);
const seen=new Set();let rows=0,inScope=0;
for(const p of featureFiles){
  for await(const r of ndjsonGz(p)){
    rows++;
    if((Number(r.sdlc_mask)||0)===0&&(Number(r.social_mask)||0)===0)continue;
    inScope++;
    const h=String(r.content_hash||'');
    if(!h)continue;
    const sig=h+'|'+String(r.source_key)+'|'+String(r.item_key);
    if(seen.has(sig))continue;seen.add(sig);
    const cs=[Number(r.c0),Number(r.c1),Number(r.c2)];
    for(let axis=0;axis<3;axis++){
      const task=axis*256+Math.floor(cs[axis]/256),part=task%PARTS;
      writers[part].push(JSON.stringify({task,axis,content_hash:h,simhash_hex:r.simhash_hex,char_count:r.char_count,token_count:r.token_count,c0:r.c0,c1:r.c1,c2:r.c2,c3:r.c3}));
    }
  }
}
for(let i=0;i<PARTS;i++){
  const body=writers[i].join('\n')+(writers[i].length?'\n':'');
  await fs.writeFile(path.join(outDir,'near-new-p'+String(i).padStart(2,'0')+'.ndjson.gz'),gzipSync(Buffer.from(body),{level:6}));
}
const complete=Object.values(effective).every(x=>x.ingestEffective===x.expected&&x.policyEffective===x.expected);
const status={generatedAt:new Date().toISOString(),complete,sourceAssets:{manifests:manifestFiles.length,featureFiles:featureFiles.length,rows,inScope},effective,releaseErrors:releaseErrors.slice(0,200),nearPartitions:PARTS};
await fs.writeFile(path.join(outDir,'final-audit-v4-status.json'),JSON.stringify(status,null,2)+'\n');
console.log(JSON.stringify(status,null,2));
if(!complete)process.exitCode=3;
