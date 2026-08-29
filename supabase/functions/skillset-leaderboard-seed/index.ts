import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';
Deno.serve(async(req)=>{
  const url=Deno.env.get('SUPABASE_URL'),key=Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');
  if(!url||!key)return new Response('config',{status:500});
  const db=createClient(url,key,{auth:{persistSession:false}});
  let body:any={};try{body=await req.json();}catch{}
  const {data:auth,error:ae}=await db.rpc('skillset_consume_job_token',{p_token:body.token??'',p_purpose:'leaderboard-seed'});
  if(ae||auth!==true)return new Response('Unauthorized',{status:401});
  const {data:page,error:pe}=await db.rpc('skillset_claim_leaderboard_page');
  if(pe)return new Response(JSON.stringify({ok:false,error:pe.message}),{status:500,headers:{'content-type':'application/json'}});
  if(page===null||page===undefined)return new Response(JSON.stringify({ok:true,processed:0}),{headers:{'content-type':'application/json'}});
  try{
    const r=await fetch(`https://skills.sh/api/skills/all-time/${page}`,{headers:{'user-agent':'skillset-corpus/1.0'},signal:AbortSignal.timeout(15000)});
    if(!r.ok)throw new Error(`leaderboard_http_${r.status}`);
    const j=await r.json();
    if(!Array.isArray(j?.skills))throw new Error('invalid_leaderboard_payload');
    const {data:count,error:se}=await db.rpc('skillset_seed_leaderboard_page',{p_rows:j.skills,p_page:Number(j.page??page),p_total:Number(j.total??0),p_has_more:Boolean(j.hasMore)});
    if(se)throw new Error(`seed:${se.message}`);
    return new Response(JSON.stringify({ok:true,page:Number(page),rows:j.skills.length,total:Number(j.total??0),hasMore:Boolean(j.hasMore),upserted:Number(count??0)}),{headers:{'content-type':'application/json'}});
  }catch(e){const m=e instanceof Error?e.message:String(e);await db.rpc('skillset_fail_leaderboard_page',{p_page:Number(page),p_error:m});return new Response(JSON.stringify({ok:false,page,error:m}),{status:500,headers:{'content-type':'application/json'}});}
});