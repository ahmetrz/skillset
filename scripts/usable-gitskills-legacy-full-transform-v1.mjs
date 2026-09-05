import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline';
import {createGunzip,createGzip} from 'node:zlib';
import {feature,numericFacts} from './final-audit-feature-core-v4.mjs';

const PART=Number(process.env.LEGACY_PARTITION||0),PARTS=Math.max(1,Number(process.env.LEGACY_PARTITIONS||8));
const DIR=process.env.LEGACY_OUT_DIR||'usable-gitskills-legacy';
const raw=path.join(DIR,'raw-gitskills-legacy-full-p'+String(PART).padStart(2,'0')+'-of-'+String(PARTS).padStart(2,'0')+'.ndjson.gz');
const featureFile=path.join(DIR,'features-gitskills-legacy-full-p'+String(PART).padStart(2,'0')+'-of-'+String(PARTS).padStart(2,'0')+'.ndjson.gz');
const manifestFile=path.join(DIR,'manifest-gitskills-legacy-full-p'+String(PART).padStart(2,'0')+'-of-'+String(PARTS).padStart(2,'0')+'.json');
const source=fs.createReadStream(raw).pipe(createGunzip()),rl=readline.createInterface({input:source,crlfDelay:Infinity});
const gz=createGzip({level:6}),out=fs.createWriteStream(featureFile);gz.pipe(out);
let total=0,inScope=0,policyFacts=0;const units=new Set();
for await(const line of rl){if(!line.trim())continue;const x=JSON.parse(line),text=String(x.text||'');if(!text)continue;const f=feature(text,String(x.locator||'')),facts=numericFacts(text);if(f.sdlc_mask||f.social_mask)inScope++;policyFacts+=facts.length;total++;units.add(String(x.sourceKey));gz.write(JSON.stringify({source_key:String(x.sourceKey),item_key:String(x.itemKey),source_system:'gitskills-legacy-hf',repo:x.repo||null,path:x.path||null,locator:String(x.locator||''),content_hash:f.h,simhash_hex:f.simhash_hex,c0:f.c0,c1:f.c1,c2:f.c2,c3:f.c3,char_count:f.char_count,token_count:f.token_count,sdlc_mask:f.sdlc_mask,social_mask:f.social_mask,risk_mask:f.risk_mask,provider_mask:f.provider_mask,policy_sig:f.policy_sig,skill_name:f.skill_name||null,sample_locator:f.sample_locator,policy_facts:facts})+'\n')}
gz.end();await new Promise((res,rej)=>{out.on('finish',res);out.on('error',rej);gz.on('error',rej)});
const base=JSON.parse(await fsp.readFile(manifestFile,'utf8'));base.records=total;base.skills=total;base.inScope=inScope;base.uncategorized=total-inScope;base.policyFacts=policyFacts;base.sourceKeysWithSkills=units.size;await fsp.writeFile(manifestFile,JSON.stringify(base,null,2)+'\n');
console.log(JSON.stringify({event:'legacy_full_transform',partition:PART,records:total,inScope,uncategorized:total-inScope,sourceKeysWithSkills:units.size}));
