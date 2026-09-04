const HF='https://datasets-server.huggingface.co/rows';
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function retry(label,fn,n=6){let last;for(let i=0;i<n;i++){try{return await fn(i)}catch(e){last=e;if(i===n-1)break;const m=String(e?.message||e),base=/429|503|504|timeout|aborted/i.test(m)?1500:500;await sleep(Math.min(base*(2**i),15000)+Math.floor(Math.random()*300))}}throw new Error(label+': '+String(last?.message||last))}
async function hfShard(id){
  const u=new URL(HF);
  u.searchParams.set('dataset','mvaccargiu/gitskills');
  u.searchParams.set('config','artifacts');
  u.searchParams.set('split','train');
  u.searchParams.set('offset',String((Number(id)-100000)*100));
  u.searchParams.set('length','100');
  const body=await retry('hf_fallback_'+id,async()=>{
    const r=await fetch(u,{headers:{'user-agent':'skillset-final-audit-fallback/1.0'},signal:AbortSignal.timeout(30000)});
    if(!r.ok)throw new Error('HF '+r.status+': '+(await r.text()).slice(0,200));
    return r.json();
  },8);
  return {shardId:Number(id),rows:(body.rows||[]).map(x=>({...(x.row||{}),row_idx:x.row_idx})).filter(r=>r.dedup_primary===1||r.dedup_primary===true)};
}
export async function rebuildGitSkillsPack(key){
  const base=String(key).split('/').pop()||'';
  const m=base.match(/^pack-([0-9-]+)\.json\.gz$/);
  if(!m)throw new Error('cannot_parse_gitskills_pack:'+key);
  const ids=m[1].split('-').map(Number).filter(Number.isFinite);
  const shards=[];
  for(let i=0;i<ids.length;i+=4){
    shards.push(...await Promise.all(ids.slice(i,i+4).map(hfShard)));
  }
  return {version:1,source:'gitskills-hf-fallback',shards};
}
async function githubJson(path){
  const headers={'user-agent':'skillset-final-audit-fallback/1.0','accept':'application/vnd.github+json'};
  const token=process.env.GITHUB_TOKEN||'';
  if(token)headers.authorization='Bearer '+token;
  const r=await fetch('https://api.github.com'+path,{headers,signal:AbortSignal.timeout(30000)});
  const text=await r.text();
  if(!r.ok)throw new Error('github_api_'+r.status+':'+text.slice(0,300));
  return JSON.parse(text);
}
export async function rebuildSkillsShPack(turso,key,bucket){
  const b2path='b2://'+bucket+'/'+key;
  const q=await turso.execute({sql:"SELECT source,owner,repo FROM skills_sh_external_exact_v1 WHERE b2_path=? LIMIT 1",args:[b2path]});
  const row=q.rows[0];
  if(!row)throw new Error('skills_sh_locator_missing:'+key);
  const owner=String(row.owner),repo=String(row.repo),source=String(row.source);
  const info=await retry('github_repo_'+source,()=>githubJson('/repos/'+encodeURIComponent(owner)+'/'+encodeURIComponent(repo)),4);
  const branch=String(info.default_branch||'main');
  const tree=await retry('github_tree_'+source,()=>githubJson('/repos/'+encodeURIComponent(owner)+'/'+encodeURIComponent(repo)+'/git/trees/'+encodeURIComponent(branch)+'?recursive=1'),4);
  if(tree.truncated)throw new Error('github_tree_truncated:'+source);
  const entries=(Array.isArray(tree.tree)?tree.tree:[]).filter(x=>x?.type==='blob'&&/(^|\/)SKILL\.md$/i.test(String(x.path||'')));
  const files=[];
  for(let i=0;i<entries.length;i+=6){
    const batch=await Promise.all(entries.slice(i,i+6).map(async e=>{
      if(Number(e.size||0)>64*1024*1024)throw new Error('skill_file_oversize:'+e.path);
      const blob=await retry('github_blob_'+source,()=>githubJson('/repos/'+encodeURIComponent(owner)+'/'+encodeURIComponent(repo)+'/git/blobs/'+encodeURIComponent(e.sha)),4);
      if(blob.encoding!=='base64'||typeof blob.content!=='string')throw new Error('github_blob_encoding:'+e.path);
      const b64=blob.content.replace(/\s/g,'');
      return {path:String(e.path),contentBase64:b64};
    }));
    files.push(...batch);
  }
  return {version:1,source:'skills.sh-github-fallback',repoSource:source,owner,repo,files};
}
