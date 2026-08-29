import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';
const enc=new TextEncoder();
async function sha256(text:string){const d=await crypto.subtle.digest('SHA-256',enc.encode(text));return Array.from(new Uint8Array(d)).map(b=>b.toString(16).padStart(2,'0')).join('');}
async function gzipBytes(text:string){const s=new Blob([text]).stream().pipeThrough(new CompressionStream('gzip'));return new Uint8Array(await new Response(s).arrayBuffer());}
Deno.serve(async(req)=>{
 const su=Deno.env.get('SUPABASE_URL'),sk=Deno.env.get('SUPABASE_SERVICE_ROLE_KEY'); if(!su||!sk)return new Response('config',{status:500});
 const db=createClient(su,sk,{auth:{persistSession:false}}); let body:any={};try{body=await req.json();}catch{}
 const {data:auth,error:ae}=await db.rpc('skillset_consume_job_token',{p_token:body.token??'',p_purpose:'repo-discovery'}); if(ae||auth!==true)return new Response('Unauthorized',{status:401});
 const {data:rows,error:ce}=await db.rpc('skillset_claim_repository'); if(ce)return new Response(JSON.stringify({ok:false,error:ce.message}),{status:500});
 const row=rows?.[0]; if(!row)return new Response(JSON.stringify({ok:true,processed:0}),{headers:{'content-type':'application/json'}});
 const source=String(row.source),owner=String(row.owner),repo=String(row.repo); let ref:string|null=null,files=0,newSkills=0;
 try{
  const headers={'accept':'application/vnd.github+json','user-agent':'skillset-corpus/1.0'};
  const tree=await fetch(`https://api.github.com/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/git/trees/HEAD?recursive=1`,{headers,signal:AbortSignal.timeout(20000)});
  if(!tree.ok)throw new Error(`tree_http_${tree.status}`); const tj=await tree.json(); ref=String(tj.sha||'HEAD');
  const paths=(Array.isArray(tj.tree)?tj.tree:[]).filter((x:any)=>x?.type==='blob'&&typeof x?.path==='string'&&(x.path==='SKILL.md'||x.path.endsWith('/SKILL.md'))).map((x:any)=>String(x.path)).slice(0,500); files=paths.length;
  const bucket='skillset-corpus'; const concurrency=6;
  for(let start=0;start<paths.length;start+=concurrency){await Promise.all(paths.slice(start,start+concurrency).map(async(path:string)=>{
    const raw=`https://raw.githubusercontent.com/${owner}/${repo}/${encodeURIComponent(ref!)}/${path.split('/').map(encodeURIComponent).join('/')}`;
    const rr=await fetch(raw,{headers:{'user-agent':'skillset-corpus/1.0'},signal:AbortSignal.timeout(12000)}); if(!rr.ok)return; const text=await rr.text(); if(!text.trim())return;
    const hash=await sha256(text),gz=await gzipBytes(text),objectKey=`sha256/${hash.slice(0,2)}/${hash}.md.gz`;
    const {error:upErr}=await db.storage.from(bucket).upload(objectKey,gz,{contentType:'application/gzip',upsert:false,cacheControl:'31536000'}); if(upErr&&!/already|duplicate/i.test(String(upErr.message)))return;
    const parent=path==='SKILL.md'?'root':path.slice(0,-'/SKILL.md'.length); const skillName=parent==='root'?repo:parent.split('/').filter(Boolean).pop()!;
    const {data:inserted,error:ue}=await db.rpc('skillset_upsert_repo_discovered_skill',{p_source:source,p_skill_name:skillName,p_repo_path:parent,p_owner:owner,p_repo:repo,p_source_url:`https://github.com/${owner}/${repo}/blob/${ref}/${path}`,p_sha:hash,p_raw_bytes:enc.encode(text).byteLength,p_object_key:objectKey,p_gzip_bytes:gz.byteLength}); if(!ue&&inserted===true)newSkills++;
  }));}
  await db.rpc('skillset_finish_repository',{p_source:source,p_branch:ref,p_files:files,p_new:newSkills,p_error:null});
  return new Response(JSON.stringify({ok:true,source,ref,files,newSkills}),{headers:{'content-type':'application/json'}});
 }catch(e){const m=e instanceof Error?e.message:String(e);await db.rpc('skillset_finish_repository',{p_source:source,p_branch:ref,p_files:files,p_new:newSkills,p_error:m});return new Response(JSON.stringify({ok:false,source,error:m}),{status:500,headers:{'content-type':'application/json'}});}
});