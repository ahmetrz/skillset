import {createClient} from '@libsql/client';
import {createHash} from 'node:crypto';
import {gzipSync} from 'node:zlib';

const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const db=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const token=process.env.GITHUB_TOKEN||'';
const targets=['kenbin64/kensgames','yamato720/project-xplus'];
const sha256=b=>createHash('sha256').update(b).digest('hex');
async function q(sql,args=[]){return (await db.execute({sql,args})).rows}
async function gh(path){const h={'user-agent':'skillset-pack-sha-control/6.0','accept':'application/vnd.github+json'};if(token)h.authorization='Bearer '+token;const r=await fetch('https://api.github.com'+path,{headers:h});if(!r.ok)throw new Error('gh '+r.status+' '+path);return r.json()}
function slugify(s){return String(s||'').toLowerCase().replace(/[\s_]+/g,'-').replace(/[^a-z0-9-]/g,'').replace(/-+/g,'-').replace(/^-|-$/g,'')}
const results=[];
for(const source of targets){
 const rows=await q('SELECT source,owner,repo,updated_at,pack_sha256,discovered_skills,bytes FROM skills_sh_external_exact_v1 WHERE source=?',[source]);
 if(!rows.length){results.push({source,error:'metadata_missing'});continue} const m=rows[0],owner=String(m.owner),repo=String(m.repo);
 const page=await (await fetch('https://skills.sh/'+owner+'/'+repo,{headers:{'user-agent':'skillset-pack-sha-control/6.0'}})).text();
 const slugs=new Set();for(const x of page.matchAll(/href="\/([^"/]+)\/([^"/]+)\/([^"/?#]+)"/g))if(x[1].toLowerCase()===owner.toLowerCase()&&x[2].toLowerCase()===repo.toLowerCase())slugs.add(x[3]);
 const downloads=[];for(const slug of slugs){const r=await fetch('https://skills.sh/api/download/'+owner+'/'+repo+'/'+encodeURIComponent(slug));if(!r.ok)continue;const j=await r.json();const sf=(Array.isArray(j.files)?j.files:[]).filter(x=>/(^|\/)SKILL\.md$/i.test(String(x.path||'')));if(sf.length===1){const text=String(sf[0].contents||'');downloads.push({slug,text,hash:sha256(Buffer.from(text))})}}
 if(downloads.length!==1||Number(m.discovered_skills)!==1){results.push({source,error:'control_not_single_skill',slugs:[...slugs],downloads:downloads.length,expected:Number(m.discovered_skills)});continue}
 const info=await gh('/repos/'+owner+'/'+repo);const branch=String(info.default_branch||'main');const tree=await gh('/repos/'+owner+'/'+repo+'/git/trees/'+encodeURIComponent(branch)+'?recursive=1');
 const skillPaths=(Array.isArray(tree.tree)?tree.tree:[]).filter(x=>x.type==='blob'&&/(^|\/)SKILL\.md$/i.test(String(x.path||''))).map(x=>String(x.path));
 const matchedPaths=[];for(const p of skillPaths){const rr=await fetch('https://raw.githubusercontent.com/'+owner+'/'+repo+'/'+encodeURIComponent(branch)+'/'+p.split('/').map(encodeURIComponent).join('/'));if(!rr.ok)continue;const b=Buffer.from(await rr.arrayBuffer());if(sha256(b)===downloads[0].hash)matchedPaths.push({path:p,bytes:b})}
 if(matchedPaths.length!==1){results.push({source,error:'path_match_count_'+matchedPaths.length,skillPaths,cacheHash:downloads[0].hash});continue}
 const b=matchedPaths[0].bytes;const packed=[{path:matchedPaths[0].path,contentHash:sha256(b),originalBytes:b.length,contentBase64:b.toString('base64')}];
 const updated=Date.parse(String(m.updated_at));const target=String(m.pack_sha256);let found=null;let checked=0;
 const transports=['github-archive-direct-b2','github-api-tree-blob-fallback'];
 const start=updated-20000,end=updated+1000;
 outer:for(const transport of transports){for(let ms=start;ms<=end;ms++){const generatedAt=new Date(ms).toISOString();const payload={version:1,source:'skills.sh',transport,repoSource:source,owner,repo,generatedAt,files:packed};const gz=gzipSync(Buffer.from(JSON.stringify(payload)),{level:9});checked++;if(sha256(gz)===target){found={generatedAt,deltaMs:updated-ms,transport,path:packed[0].path,gzBytes:gz.length};break outer}}}
 results.push({source,updatedAt:String(m.updated_at),targetPackSha:target,cacheContentHash:downloads[0].hash,path:packed[0].path,checked,found});
}
console.log(JSON.stringify({generatedAt:new Date().toISOString(),results},null,2));