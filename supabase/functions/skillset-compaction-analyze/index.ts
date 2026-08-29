import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';
function cov(v:string[],sets:string[][]){const u=new Set<string>();for(const s of sets)for(const x of s)u.add(x);let covered=0;const missing:string[]=[];for(const x of v){if(u.has(x))covered++;else missing.push(x);}return {covered,missing,ratio:v.length?covered/v.length:1};}
Deno.serve(async(req)=>{
 const url=Deno.env.get('SUPABASE_URL'),key=Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');if(!url||!key)return new Response('config',{status:500});
 const c=createClient(url,key,{auth:{persistSession:false}});let b:any={};try{b=await req.json();}catch{}
 const {data:authorized,error:ae}=await c.rpc('skillset_consume_job_token',{p_token:b.token??'',p_purpose:'compaction-analyze'});if(ae||authorized!==true)return new Response('Unauthorized',{status:401});
 const force=b.force===true,limit=Math.max(1,Math.min(Number(b.limit??10),50));
 const {data:victims,error:ve}=await c.rpc('skillset_claim_compaction_victims',{p_limit:limit,p_force:force});if(ve)return Response.json({ok:false,error:ve.message},{status:500});
 let proposed=0,scanned=0;
 for(const v of victims??[]){scanned++;const vh=[...(v.unit_hashes??[])];if(!vh.length)continue;const {data:ss,error:se}=await c.rpc('skillset_compaction_survivors',{p_victim_file_id:v.file_id,p_limit:12});if(se||!ss?.length)continue;
   let best:any=null;
   for(const s of ss){if(Number(s.unit_count??0)<Number(v.unit_count??0))continue;const r=cov(vh,[s.unit_hashes??[]]);if(!best||r.ratio>best.ratio)best={...r,survivors:[s.file_id],survivorUnits:[s.unit_count]};}
   for(let i=0;i<ss.length;i++)for(let j=i+1;j<ss.length;j++){const r=cov(vh,[ss[i].unit_hashes??[],ss[j].unit_hashes??[]]);if(!best||r.ratio>best.ratio)best={...r,survivors:[ss[i].file_id,ss[j].file_id],survivorUnits:[ss[i].unit_count,ss[j].unit_count]};}
   if(!best)continue;const maxUnique=Math.max(3,Math.floor(vh.length*0.05));let action:string|null=null;if(best.missing.length===0)action='delete_covered';else if(best.ratio>=0.75&&best.missing.length<=maxUnique)action='merge_then_delete';if(!action)continue;
   const {error:re}=await c.rpc('skillset_record_compaction_candidate',{p_victim_file_id:v.file_id,p_survivor_file_ids:best.survivors,p_action:action,p_coverage:best.ratio,p_victim_units:vh.length,p_covered_units:best.covered,p_unique_hashes:best.missing,p_estimated_saved:Number(v.compressed_bytes??0),p_validation:{stage:'hash-unit-analysis',profile_version:1,max_unique:maxUnique,survivor_count:best.survivors.length,victim_unit_count:v.unit_count,survivor_unit_counts:best.survivorUnits}});if(!re)proposed++;
 }
 return Response.json({ok:true,scanned,proposed,force});
});