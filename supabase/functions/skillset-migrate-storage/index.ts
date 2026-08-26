import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

async function gzipBytes(text:string): Promise<Uint8Array> {
  const stream=new Blob([text]).stream().pipeThrough(new CompressionStream('gzip'));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

Deno.serve(async(req)=>{
  const url=Deno.env.get('SUPABASE_URL'), key=Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');
  if(!url||!key) return new Response('Server configuration error',{status:500});
  const client=createClient(url,key,{auth:{persistSession:false}});
  let body:{token?:string;limit?:number}={}; try{body=await req.json();}catch{}
  const {data:authorized,error:authError}=await client.rpc('skillset_consume_job_token',{p_token:body.token??'',p_purpose:'migrate-storage'});
  if(authError||authorized!==true) return new Response('Unauthorized',{status:401});
  const bucket='skillset-corpus';
  const {data:buckets}=await client.storage.listBuckets();
  if(!(buckets??[]).some((b:any)=>b.name===bucket)){
    const {error:e}=await client.storage.createBucket(bucket,{public:false,fileSizeLimit:1048576});
    if(e&&!String(e.message).toLowerCase().includes('already')) return new Response(JSON.stringify({ok:false,error:`bucket:${e.message}`}),{status:500,headers:{'content-type':'application/json'}});
  }
  const limit=Math.max(1,Math.min(Number(body.limit??200),250));
  const {data:rows,error}=await client.rpc('skillset_next_unmigrated_hashes',{p_limit:limit});
  if(error) return new Response(JSON.stringify({ok:false,error:error.message}),{status:500,headers:{'content-type':'application/json'}});
  let migratedHashes=0,affectedRows=0,uploadedBytes=0,errors=0;
  const concurrency=10;
  for(let start=0;start<(rows??[]).length;start+=concurrency){
    await Promise.all((rows??[]).slice(start,start+concurrency).map(async(row:any)=>{
      try{
        const sha=String(row.content_sha256), gz=await gzipBytes(String(row.skill_md??''));
        const objectKey=`sha256/${sha.slice(0,2)}/${sha}.md.gz`;
        const {error:upErr}=await client.storage.from(bucket).upload(objectKey,gz,{contentType:'application/gzip',upsert:false,cacheControl:'31536000'});
        if(upErr&&!/already|duplicate/i.test(String(upErr.message))) throw new Error(`upload:${upErr.message}`);
        const {data:count,error:markErr}=await client.rpc('skillset_mark_object_stored',{p_sha:sha,p_object_key:objectKey,p_compressed_bytes:gz.byteLength,p_backend:'supabase-storage'});
        if(markErr) throw new Error(`mark:${markErr.message}`);
        migratedHashes++; affectedRows+=Number(count??0); uploadedBytes+=gz.byteLength;
      }catch{errors++;}
    }));
  }
  return new Response(JSON.stringify({ok:true,selected:(rows??[]).length,migratedHashes,affectedRows,uploadedBytes,errors}),{headers:{'content-type':'application/json'}});
});