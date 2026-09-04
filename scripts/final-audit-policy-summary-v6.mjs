import fs from 'node:fs/promises';
import path from 'node:path';
import {createGunzip} from 'node:zlib';
import readline from 'node:readline';

const input=process.env.AUDIT_V6_POLICY_INPUT||'policy-assets';
const outDir=process.env.AUDIT_V6_POLICY_OUT||'policy-out';
async function files(dir){const out=[];for(const e of await fs.readdir(dir,{withFileTypes:true})){const p=path.join(dir,e.name);if(e.isDirectory())out.push(...await files(p));else out.push(p)}return out}
async function* ndjsonGz(file){
  const fh=await fs.open(file,'r'),stream=fh.createReadStream().pipe(createGunzip());
  const rl=readline.createInterface({input:stream,crlfDelay:Infinity});
  try{for await(const line of rl)if(line.trim())yield JSON.parse(line)}
  finally{await fh.close().catch(()=>{})}
}
function add(map,k,n=1){map.set(k,(map.get(k)||0)+n)}
await fs.mkdir(outDir,{recursive:true});
const all=await files(input),featureFiles=all.filter(x=>/^features-.*\.ndjson\.gz$/.test(path.basename(x)));
const policy=new Map(),facts=new Map(),examples=new Map(),seen=new Set();
let rows=0,inScope=0;
for(const p of featureFiles){
  for await(const r of ndjsonGz(p)){
    rows++;
    if((Number(r.sdlc_mask)||0)===0&&(Number(r.social_mask)||0)===0)continue;
    inScope++;
    const h=String(r.content_hash||'');if(!h||seen.has(h))continue;seen.add(h);
    for(const s of String(r.policy_sig||'').split(';').filter(Boolean)){
      add(policy,s);if(!examples.has(s))examples.set(s,{contentHash:h,locator:r.sample_locator||r.locator});
    }
    for(const f of r.policy_facts||[]){
      const k=String(f.key)+'='+String(f.value);add(facts,k);
      if(!examples.has(k))examples.set(k,{contentHash:h,locator:r.sample_locator||r.locator,evidence:f.evidence});
    }
  }
}
const out={
  generatedAt:new Date().toISOString(),featureFiles:featureFiles.length,rows,inScope,uniqueHashes:seen.size,
  policy:Object.fromEntries(policy),facts:Object.fromEntries(facts),examples:Object.fromEntries(examples)
};
await fs.writeFile(path.join(outDir,'final-audit-v6-policy-summary.json'),JSON.stringify(out)+'\n');
console.log(JSON.stringify({event:'policy_summary_complete',featureFiles:featureFiles.length,rows,inScope,uniqueHashes:seen.size,policyKeys:policy.size,factKeys:facts.size}));
