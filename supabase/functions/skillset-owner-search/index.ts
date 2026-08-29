import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';
Deno.serve(async(req)=>{
 const su=Deno.env.get('SUPABASE_URL'),sk=Deno.env.get('SUPABASE_SERVICE_ROLE_KEY'); if(!su||!sk)return new Response('config',{status:500});
 const db=createClient(su,sk,{auth:{persistSession:false}}); let body:any={};try{body=await req.json();}catch{}
 const {data:auth,error:ae}=await db.rpc('skillset_consume_job_token',{p_token:body.token??'',p_purpose:'owner-search'}); if(ae||auth!==true)return new Response('Unauthorized',{status:401});
 const limit=Math.max(1,Math.min(Number(body.limit??5),10));
 const {data:claimed,error:ce}=await db.rpc('skillset_claim_owner_search_batch',{p_limit:limit}); if(ce)return new Response(JSON.stringify({ok:false,error:ce.message}),{status:500,headers:{'content-type':'application/json'}});
 const owners=(claimed??[]).map((x:any)=>String(x.owner)).filter(Boolean); if(!owners.length)return new Response(JSON.stringify({ok:true,processed:0}),{headers:{'content-type':'application/json'}});
 let okCount=0,errorCount=0,saturated=0,upserted=0;
 await Promise.all(owners.map(async(owner:string)=>{
  try{
   const u=`https://skills.sh/api/search?q=${encodeURIComponent(owner)}&owner=${encodeURIComponent(owner)}&limit=200`;
   const r=await fetch(u,{headers:{'user-agent':'skillset-corpus/1.0'},signal:AbortSignal.timeout(15000)}); if(!r.ok)throw new Error(`search_http_${r.status}`);
   const j=await r.json(); const rows=Array.isArray(j?.skills)?j.skills:[];
   const {data:count,error:se}=await db.rpc('skillset_seed_search_results',{p_rows:rows}); if(se)throw new Error(`seed:${se.message}`);
   await db.rpc('skillset_finish_owner_search',{p_owner:owner,p_count:rows.length,p_error:null});
   okCount++;upserted+=Number(count??0);if(rows.length>=200)saturated++;
  }catch(e){const m=e instanceof Error?e.message:String(e);await db.rpc('skillset_finish_owner_search',{p_owner:owner,p_count:0,p_error:m});errorCount++;}
 }));
 return new Response(JSON.stringify({ok:true,processed:owners.length,okCount,errorCount,saturated,upserted}),{headers:{'content-type':'application/json'}});
});