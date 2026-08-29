import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const enc=new TextEncoder();
async function sha256(text:string){const d=await crypto.subtle.digest('SHA-256',enc.encode(text));return Array.from(new Uint8Array(d)).map(b=>b.toString(16).padStart(2,'0')).join('');}
async function gzipBytes(text:string){const s=new Blob([text]).stream().pipeThrough(new CompressionStream('gzip'));return new Uint8Array(await new Response(s).arrayBuffer());}
function pickSkillMd(files:any[]){const c=(files??[]).filter(f=>typeof f?.path==='string'&&typeof f?.contents==='string'&&(f.path==='SKILL.md'||f.path.endsWith('/SKILL.md'))).sort((a,b)=>a.path.length-b.path.length);return c[0]??null;}

Deno.serve(async(req)=>{
  const url=Deno.env.get('SUPABASE_URL'),key=Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');
  if(!url||!key)return new Response('Server configuration error',{status:500});
  const client=createClient(url,key,{auth:{persistSession:false}});
  let body:{token?:string;limit?:number}={};try{body=await req.json();}catch{}
  const {data:authorized,error:authError}=await client.rpc('skillset_consume_job_token',{p_token:body.token??'',p_purpose:'content-batch'});
  if(authError||authorized!==true)return new Response('Unauthorized',{status:401});
  const bucket='skillset-corpus';
  const {data:buckets}=await client.storage.listBuckets();
  if(!(buckets??[]).some((b:any)=>b.name===bucket)){
    const {error:e}=await client.storage.createBucket(bucket,{public:false,fileSizeLimit:1048576});
    if(e&&!/already/i.test(String(e.message)))return new Response(JSON.stringify({ok:false,error:e.message}),{status:500,headers:{'content-type':'application/json'}});
  }
  const limit=Math.max(1,Math.min(Number(body.limit??60),100));
  const {data:rows,error:claimError}=await client.rpc('skillset_claim_pending',{p_limit:limit});
  if(claimError)return new Response(JSON.stringify({ok:false,error:claimError.message}),{status:500,headers:{'content-type':'application/json'}});
  let okCount=0,errorCount=0,uploadedBytes=0;const concurrency=8;
  for(let start=0;start<(rows??[]).length;start+=concurrency){
    await Promise.all((rows??[]).slice(start,start+concurrency).map(async(row:any)=>{
      try{
        const source=String(row.source??'').replace(/^https?:\/\/github\.com\//,'').replace(/\.git$/,'');
        const parts=source.split('/').filter(Boolean);if(parts.length<2)throw new Error('invalid_source');
        const slug=String(row.id).startsWith(`${source}/`)?String(row.id).slice(source.length+1):String(row.skill_name??'').trim();if(!slug)throw new Error('missing_slug');
        const dl=`https://skills.sh/api/download/${encodeURIComponent(parts[0])}/${encodeURIComponent(parts[1])}/${encodeURIComponent(slug)}`;
        const res=await fetch(dl,{headers:{'user-agent':'skillset-corpus/1.0'},signal:AbortSignal.timeout(12000)});if(!res.ok)throw new Error(`download_http_${res.status}`);
        const snap=await res.json();const file=pickSkillMd(Array.isArray(snap?.files)?snap.files:[]);if(!file)throw new Error('skill_md_not_found_in_snapshot');
        const text=String(file.contents),hash=await sha256(text),rawBytes=enc.encode(text).byteLength,gz=await gzipBytes(text);
        const objectKey=`sha256/${hash.slice(0,2)}/${hash}.md.gz`;
        const {error:upErr}=await client.storage.from(bucket).upload(objectKey,gz,{contentType:'application/gzip',upsert:false,cacheControl:'31536000'});
        if(upErr&&!/already|duplicate/i.test(String(upErr.message)))throw new Error(`upload:${upErr.message}`);
        const {error:storeErr}=await client.rpc('skillset_store_object_content',{p_id:row.id,p_content_sha256:hash,p_content_bytes:rawBytes,p_object_key:objectKey,p_compressed_bytes:gz.byteLength,p_source_url:`https://github.com/${source}`,p_backend:'supabase-storage'});
        if(storeErr)throw new Error(`store:${storeErr.message}`);okCount++;uploadedBytes+=gz.byteLength;
      }catch(e){const m=e instanceof Error?e.message:String(e);await client.rpc('skillset_mark_error',{p_id:row.id,p_error:m});errorCount++;}
    }));
  }
  return new Response(JSON.stringify({ok:true,processed:(rows??[]).length,okCount,errorCount,uploadedBytes}),{headers:{'content-type':'application/json'}});
});