import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';
const enc=new TextEncoder();
async function hash16(s:string){const d=await crypto.subtle.digest('SHA-256',enc.encode(s));return Array.from(new Uint8Array(d)).slice(0,8).map(b=>b.toString(16).padStart(2,'0')).join('');}
function norm(s:string){return s.toLowerCase().replace(/\r/g,'').replace(/[`*_>#~]/g,' ').replace(/\s+/g,' ').trim();}
function semanticUnits(md:string){
  const lines=md.replace(/\r/g,'').split('\n');
  const units:string[]=[];let headings:string[]=[];let para:string[]=[];let code:string[]=[];let inCode=false;let fm=false;
  const flushPara=()=>{if(!para.length)return;const t=norm(para.join(' '));if(t.length>=18)units.push((headings.join(' > ')+' || '+t).trim());para=[];};
  const flushCode=()=>{if(!code.length)return;const t=code.join('\n').trim();if(t.length>=6)units.push((headings.join(' > ')+' || code || '+t).trim());code=[];};
  for(let i=0;i<lines.length;i++){
    const raw=lines[i],tr=raw.trim();
    if(i===0&&tr==='---'){fm=true;continue;}if(fm){if(tr==='---'){fm=false;continue;}if(/^description\s*:/i.test(tr)){const t=norm(tr.replace(/^description\s*:/i,''));if(t.length>=10)units.push('frontmatter description || '+t);}continue;}
    if(/^```/.test(tr)){if(inCode){flushCode();inCode=false;}else{flushPara();inCode=true;}continue;}if(inCode){code.push(raw);continue;}
    const hm=tr.match(/^(#{1,6})\s+(.+)$/);if(hm){flushPara();const level=hm[1].length;headings=headings.slice(0,level-1);headings[level-1]=norm(hm[2]);continue;}
    const lm=tr.match(/^[-*+]\s+(.+)$/)||tr.match(/^\d+[.)]\s+(.+)$/);if(lm){flushPara();const t=norm(lm[1]);if(t.length>=10)units.push((headings.join(' > ')+' || item || '+t).trim());continue;}
    if(!tr){flushPara();continue;}para.push(tr);
  }
  flushPara();if(inCode)flushCode();return Array.from(new Set(units));
}
async function ungzip(bytes:Uint8Array){return await new Response(new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'))).text();}
Deno.serve(async(req)=>{
  const url=Deno.env.get('SUPABASE_URL'),key=Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');if(!url||!key)return new Response('config',{status:500});
  const c=createClient(url,key,{auth:{persistSession:false}});let body:any={};try{body=await req.json();}catch{}
  const {data:authorized,error:aerr}=await c.rpc('skillset_consume_job_token',{p_token:body.token??'',p_purpose:'compaction-profile'});if(aerr||authorized!==true)return new Response('Unauthorized',{status:401});
  const limit=Math.max(1,Math.min(Number(body.limit??25),100));const {data:ver}=await c.rpc('skillset_compaction_profile_version');
  const {data:rows,error:claimError}=await c.rpc('skillset_claim_compaction_profiles',{p_limit:limit,p_force:body.force===true});if(claimError)return Response.json({ok:false,error:claimError.message},{status:500});
  let profiled=0,errors=0,totalUnits=0;
  for(const row of rows??[]){try{const {data:b,error:de}=await c.storage.from('skillset-corpus').download(row.content_object_key);if(de||!b)throw new Error(de?.message??'download_failed');const text=await ungzip(new Uint8Array(await b.arrayBuffer()));const units=semanticUnits(text);const hs:string[]=[];for(const u of units)hs.push(await hash16(u));hs.sort();const {error:se}=await c.rpc('skillset_store_compaction_profile',{p_file_id:row.file_id,p_sha:row.content_sha256,p_unit_hashes:hs,p_unit_count:hs.length,p_normalized_bytes:enc.encode(units.join('\n')).byteLength,p_profile_version:Number(ver??1)});if(se)throw new Error(se.message);profiled++;totalUnits+=hs.length;}catch{errors++;}}
  return Response.json({ok:true,processed:(rows??[]).length,profiled,errors,totalUnits});
});