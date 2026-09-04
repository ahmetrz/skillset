import {createClient} from '@libsql/client';
import fs from 'node:fs/promises';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {gzipSync} from 'node:zlib';
import {feature,numericFacts} from './final-audit-feature-core-v4.mjs';

const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const db=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const outDir=process.env.OUT_DIR||'stable-rescue-out';
const targets=new Set([
 'b2:skills-sh/exact-b2-v1/bucket-19/0015060-facukoren-pm-agent.json.gz',
 'b2:skills-sh/exact-b2-v1/bucket-25/0022459-xiaojiaenen-code2biz.json.gz'
]);
const sha256=s=>createHash('sha256').update(s).digest('hex');
async function q(sql,args=[]){return (await db.execute({sql,args})).rows}
await fs.mkdir(outDir,{recursive:true});
const records=[],units=[];let inScope=0,policyFacts=0;
for(const sk of targets){
 const occ=await q('SELECT item_key,content_hash,repo,path,locator FROM final_audit_occurrence_v1 WHERE source_key=? ORDER BY item_key',[sk]);
 if(!occ.length)throw new Error('missing acquisition occurrences '+sk);
 const repo=String(occ[0].repo||'');const [owner,name]=repo.split('/');
 const parent=String(occ[0].item_key||'').replace(/\\/g,'/').split('/').filter(Boolean).slice(-2,-1)[0]||name;
 const slug=parent.toLowerCase().replace(/[\s_]+/g,'-').replace(/[^a-z0-9-]/g,'').replace(/-+/g,'-').replace(/^-|-$/g,'');
 const res=await fetch('https://skills.sh/api/download/'+encodeURIComponent(owner)+'/'+encodeURIComponent(name)+'/'+encodeURIComponent(slug),{headers:{'user-agent':'skillset-stable-rescue/6.0'}});
 if(!res.ok)throw new Error('cache '+res.status+' '+repo+' '+slug);const j=await res.json();
 const sf=(Array.isArray(j.files)?j.files:[]).filter(x=>/(^|\/)SKILL\.md$/i.test(String(x.path||'')));
 if(sf.length!==1)throw new Error('skill md count '+sf.length+' '+repo);
 const text=String(sf[0].contents||''),h=sha256(text);
 if(occ.length!==1||h!==String(occ[0].content_hash))throw new Error('acquisition hash mismatch '+repo);
 const f=feature(text,String(occ[0].locator||'')),facts=numericFacts(text);if(f.h!==h)throw new Error('feature hash mismatch '+repo);
 if(f.sdlc_mask!==0||f.social_mask!==0)inScope++;policyFacts+=facts.length;
 records.push({source_key:sk,item_key:String(occ[0].item_key),source_system:'skills-sh-b2',repo,path:String(occ[0].path||occ[0].item_key),locator:String(occ[0].locator||''),content_hash:f.h,simhash_hex:f.simhash_hex,c0:f.c0,c1:f.c1,c2:f.c2,c3:f.c3,char_count:f.char_count,token_count:f.token_count,sdlc_mask:f.sdlc_mask,social_mask:f.social_mask,risk_mask:f.risk_mask,provider_mask:f.provider_mask,policy_sig:f.policy_sig,skill_name:f.skill_name||null,sample_locator:f.sample_locator,policy_facts:facts});
 units.push({source_key:sk,source_system:'skills-sh-b2',status:'done',rows_scanned:1,skills_indexed:1,records:1,snapshot:{transport:'skills-sh-cache-acquisition-hash-verified-stable',contentHash:h}});
}
const body=records.map(x=>JSON.stringify(x)).join('\n')+'\n';
await fs.writeFile(path.join(outDir,'features-skills-b2-stable-hash-rescue-v6.ndjson.gz'),gzipSync(Buffer.from(body),{level:6}));
const manifest={generatedAt:new Date().toISOString(),source:'skills-sh-cache-acquisition-hash-verified-stable',sourceSystem:'skills-sh-b2',assignedUnits:units.length,completedUnits:units.length,failedUnits:0,skills:records.length,inScope,policyFacts,records:records.length,units};
await fs.writeFile(path.join(outDir,'manifest-skills-b2-stable-hash-rescue-v6.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({event:'stable_hash_rescue_complete',completedUnits:units.length,skills:records.length}));