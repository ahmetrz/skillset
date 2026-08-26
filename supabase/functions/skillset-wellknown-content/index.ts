import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';
const enc=new TextEncoder();
async function sha256(text:string){const d=await crypto.subtle.digest('SHA-256',enc.encode(text));return Array.from(new Uint8Array(d)).map(b=>b.toString(16).padStart(2,'0')).join('');}
async function gzipBytes(text:string){const s=new Blob([text]).stream().pipeThrough(new CompressionStream('gzip'));return new Uint8Array(await new Response(s).arrayBuffer());}
function looksLikeSkillMd(text:string){const t=text.trimStart();return t.startsWith('---')&&!/^<!doctype html|^<html/i.test(t);}
Deno.serve(async(req)=>{
 const su=Deno.env.get('SUPABASE_URL'),sk=Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');if(!su||!sk)return new Response('config',{status:500});
 const db=createClient(su,sk,{auth:{persistSession:false}});let body:any={};try{body=await req.json();}catch{}
 const {data:auth,error:ae}=await db.rpc('skillset_consume_job_token',{p_token:body.token??'',p_purpose:'wellknown-content'});if(ae||auth!==true)return new Response('Unauthorized',{status:401});
 const limit=Math.max(1,Math.min(Number(body.limit??10),25));
 const {data:rows,error:ce}=await db.rpc('skillset_claim_wellknown',{p_limit:limit});if(ce)return new Response(JSON.stringify({ok:false,error:ce.message}),{status:500,headers:{'content-type':'application/json'}});
 let okCount=0,errorCount=0;const bucket='skillset-corpus';
 for(const row of rows??[]){
  try{
   const host=String(row.source??'').trim();const skill=String(row.skill_name??'').trim();if(!host||!skill)throw new Error('invalid_wellknown_source');
   const bases=[`https://${host}/.well-known/agent-skills/${encodeURIComponent(skill)}/SKILL.md`,`https://${host}/.well-known/skills/${encodeURIComponent(skill)}/SKILL.md`];
   let text:string|null=null,sourceUrl:string|null=null,lastStatus=0;
   for(const u of bases){const r=await fetch(u,{headers:{'user-agent':'skillset-corpus/1.0'},signal:AbortSignal.timeout(12000)});lastStatus=r.status;if(!r.ok)continue;const candidate=await r.text();if(looksLikeSkillMd(candidate)){text=candidate;sourceUrl=u;break;}}
   if(text===null||sourceUrl===null)throw new Error(`wellknown_not_found_${lastStatus||0}`);
   const hash=await sha256(text),gz=await gzipBytes(text),objectKey=`sha256/${hash.slice(0,2)}/${hash}.md.gz`;
   const {error:upErr}=await db.storage.from(bucket).upload(objectKey,gz,{contentType:'application/gzip',upsert:false,cacheControl:'31536000'});if(upErr&&!/already|duplicate/i.test(String(upErr.message)))throw new Error(`upload:${upErr.message}`);
   const {error:se}=await db.rpc('skillset_store_object_content',{p_id:row.id,p_content_sha256:hash,p_content_bytes:enc.encode(text).byteLength,p_object_key:objectKey,p_compressed_bytes:gz.byteLength,p_source_url:sourceUrl,p_backend:'supabase-storage'});if(se)throw new Error(`store:${se.message}`);okCount++;
  }catch(e){const m=e instanceof Error?e.message:String(e);await db.rpc('skillset_mark_error',{p_id:row.id,p_error:m});errorCount++;}
 }
 return new Response(JSON.stringify({ok:true,processed:(rows??[]).length,okCount,errorCount}),{headers:{'content-type':'application/json'}});
});