import fs from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline';
import {createGunzip} from 'node:zlib';
import {createHash} from 'node:crypto';

const dir=process.env.CONTROL_DIR||'controls';
const sha256=s=>createHash('sha256').update(s).digest('hex');
async function* gzLines(file){
  const fh=await fs.open(file,'r');
  const rl=readline.createInterface({input:fh.createReadStream().pipe(createGunzip()),crlfDelay:Infinity});
  try{for await(const l of rl)if(l.trim())yield JSON.parse(l)}finally{await fh.close().catch(()=>{})}
}
const files=(await fs.readdir(dir)).filter(x=>/^features-skills-b2-p\d+-of-04\.ndjson\.gz$/.test(x)).sort();
const groups=new Map();
for(const f of files)for await(const r of gzLines(path.join(dir,f))){
  const sk=String(r.source_key||''); if(!sk)continue;
  if(!groups.has(sk))groups.set(sk,{sourceKey:sk,repo:String(r.repo||''),hashes:new Set(),locators:[]});
  const g=groups.get(sk); g.hashes.add(String(r.content_hash)); g.locators.push(String(r.locator||''));
}
const results=[];
for(const g of groups.values()){
  if(!g.repo||!g.repo.includes('/')){results.push({sourceKey:g.sourceKey,error:'missing_repo'});continue}
  const [owner,repo]=g.repo.split('/');
  const pageUrl='https://skills.sh/'+encodeURIComponent(owner)+'/'+encodeURIComponent(repo);
  const pr=await fetch(pageUrl,{headers:{'user-agent':'skillset-cache-control/6.0'}});
  const html=await pr.text();
  const slugs=new Set();
  for(const m of html.matchAll(/href="\/([^"/]+)\/([^"/]+)\/([^"/?#]+)"/g)){
    if(m[1].toLowerCase()===owner.toLowerCase()&&m[2].toLowerCase()===repo.toLowerCase())slugs.add(m[3]);
  }
  const cachedHashes=new Set(),downloadErrors=[];
  for(const slug of slugs){
    const u='https://skills.sh/api/download/'+encodeURIComponent(owner)+'/'+encodeURIComponent(repo)+'/'+encodeURIComponent(slug);
    try{
      const res=await fetch(u,{headers:{'user-agent':'skillset-cache-control/6.0'}});
      if(!res.ok){downloadErrors.push({slug,status:res.status});continue}
      const j=await res.json();
      const skillFiles=(Array.isArray(j.files)?j.files:[]).filter(x=>/(^|\/)SKILL\.md$/i.test(String(x.path||'')));
      if(skillFiles.length!==1){downloadErrors.push({slug,error:'skill_md_count_'+skillFiles.length});continue}
      cachedHashes.add(sha256(String(skillFiles[0].contents||'')));
    }catch(e){downloadErrors.push({slug,error:String(e?.message||e)})}
  }
  const expected=[...g.hashes].sort(),cached=[...cachedHashes].sort();
  const missing=expected.filter(x=>!cachedHashes.has(x)),extra=cached.filter(x=>!g.hashes.has(x));
  results.push({sourceKey:g.sourceKey,repo:g.repo,acquiredSkills:expected.length,pageSlugs:slugs.size,cachedSkillHashes:cached.length,matched:expected.length-missing.length,missing,extra,downloadErrors,exactSetMatch:missing.length===0&&extra.length===0&&cached.length===expected.length});
}
const summary={generatedAt:new Date().toISOString(),controls:results.length,exactMatches:results.filter(x=>x.exactSetMatch).length,allExact:results.length>0&&results.every(x=>x.exactSetMatch),results};
console.log(JSON.stringify(summary,null,2));
await fs.writeFile(process.env.CONTROL_OUT||'skills-cache-control-v6.json',JSON.stringify(summary,null,2)+'\n');
if(!summary.allExact)process.exitCode=2;