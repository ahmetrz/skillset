import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';
Deno.serve(async(req)=>{
 const su=Deno.env.get('SUPABASE_URL'),sk=Deno.env.get('SUPABASE_SERVICE_ROLE_KEY'); if(!su||!sk)return new Response('config',{status:500});
 const db=createClient(su,sk,{auth:{persistSession:false}}); let body:any={};try{body=await req.json();}catch{}
 const {data:auth,error:ae}=await db.rpc('skillset_consume_job_token',{p_token:body.token??'',p_purpose:'owner-search'}); if(ae||auth!==true)return new Response('Unauthorized',{status:401});
 const {data:owner,error:ce}=await db.rpc('skillset_claim_owner_search'); if(ce)return new Response(JSON.stringify({ok:false,error:ce.message}),{status:500,headers:{'content-type':'application/json'}});
 if(!owner)return new Response(JSON.stringify({ok:true,processed:0}),{headers:{'content-type':'application/json'}});
 try{
  const u=`https://skills.sh/api/search?q=${encodeURIComponent(String(owner))}&owner=${encodeURIComponent(String(owner))}&limit=200`;
  const r=await fetch(u,{headers:{'user-agent':'skillset-corpus/1.0'},signal:AbortSignal.timeout(15000)}); if(!r.ok)throw new Error(`search_http_${r.status}`);
  const j=await r.json(); const rows=Array.isArray(j?.skills)?j.skills:[];
  const {data:count,error:se}=await db.rpc('skillset_seed_search_results',{p_rows:rows}); if(se)throw new Error(`seed:${se.message}`);
  await db.rpc('skillset_finish_owner_search',{p_owner:String(owner),p_count:rows.length,p_error:null});
  return new Response(JSON.stringify({ok:true,owner,rows:rows.length,upserted:Number(count??0),saturated:rows.length>=200}),{headers:{'content-type':'application/json'}});
 }catch(e){const m=e instanceof Error?e.message:String(e);await db.rpc('skillset_finish_owner_search',{p_owner:String(owner),p_count:0,p_error:m});return new Response(JSON.stringify({ok:false,owner,error:m}),{status:500,headers:{'content-type':'application/json'}});}
});