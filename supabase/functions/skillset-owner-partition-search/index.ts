import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';
Deno.serve(async(req)=>{
 const su=Deno.env.get('SUPABASE_URL'),sk=Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');if(!su||!sk)return new Response('config',{status:500});
 const db=createClient(su,sk,{auth:{persistSession:false}});let body:any={};try{body=await req.json();}catch{}
 const {data:auth,error:ae}=await db.rpc('skillset_consume_job_token',{p_token:body.token??'',p_purpose:'owner-partition-search'});if(ae||auth!==true)return new Response('Unauthorized',{status:401});
 const limit=Math.max(1,Math.min(Number(body.limit??10),25));const {data:jobs,error:ce}=await db.rpc('skillset_claim_owner_partitions',{p_limit:limit});
 if(ce)return new Response(JSON.stringify({ok:false,error:ce.message}),{status:500,headers:{'content-type':'application/json'}});
 let okCount=0,errorCount=0,rowsFound=0,expanded=0,saturated=0;const concurrency=5;
 async function run(job:any){
  const owner=String(job.owner),token=String(job.token);
  try{
   const u=`https://skills.sh/api/search?q=${encodeURIComponent(token)}&owner=${encodeURIComponent(owner)}&limit=200`;
   const r=await fetch(u,{headers:{'user-agent':'skillset-corpus/1.0'},signal:AbortSignal.timeout(15000)});if(!r.ok)throw new Error(`search_http_${r.status}`);
   const j=await r.json();const rows=Array.isArray(j?.skills)?j.skills:[];
   const {data:newTokens,error:xe}=await db.rpc('skillset_expand_owner_partition',{p_owner:owner,p_parent_token:token,p_rows:rows});if(xe)throw new Error(`expand:${xe.message}`);
   await db.rpc('skillset_finish_owner_partition',{p_owner:owner,p_token:token,p_count:rows.length,p_error:null});
   okCount++;rowsFound+=rows.length;expanded+=Number(newTokens??0);if(rows.length>=200)saturated++;
  }catch(e){const m=e instanceof Error?e.message:String(e);await db.rpc('skillset_finish_owner_partition',{p_owner:owner,p_token:token,p_count:0,p_error:m});errorCount++;}
 }
 for(let i=0;i<(jobs??[]).length;i+=concurrency)await Promise.all((jobs??[]).slice(i,i+concurrency).map(run));
 return new Response(JSON.stringify({ok:true,processed:(jobs??[]).length,okCount,errorCount,rowsFound,expanded,saturated}),{headers:{'content-type':'application/json'}});
});