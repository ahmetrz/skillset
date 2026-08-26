import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';
const enc=new TextEncoder();
const V2_SCHEMA='https://schemas.agentskills.io/discovery/0.2.0/schema.json';
async function sha256Bytes(bytes:Uint8Array){const d=await crypto.subtle.digest('SHA-256',bytes);return Array.from(new Uint8Array(d)).map(b=>b.toString(16).padStart(2,'0')).join('');}
async function sha256(text:string){return sha256Bytes(enc.encode(text));}
async function gzipBytes(text:string){const s=new Blob([text]).stream().pipeThrough(new CompressionStream('gzip'));return new Uint8Array(await new Response(s).arrayBuffer());}
function looksLikeSkillMd(text:string){const t=text.trimStart();return t.startsWith('---')&&!/^<!doctype html|^<html/i.test(t);}
type IndexHit={kind:'v1';base:string;path:string}|{kind:'v2';url:string;digest:string};
async function loadHostIndex(host:string):Promise<Map<string,IndexHit>>{
 const out=new Map<string,IndexHit>();
 const candidates=[{path:'.well-known/agent-skills',url:`https://${host}/.well-known/agent-skills/index.json`},{path:'.well-known/skills',url:`https://${host}/.well-known/skills/index.json`}];
 for(const c of candidates){
  try{
   const r=await fetch(c.url,{headers:{'user-agent':'skillset-corpus/1.0'},signal:AbortSignal.timeout(12000)});if(!r.ok)continue;
   const ct=r.headers.get('content-type')??'';if(!ct.toLowerCase().includes('json'))continue;
   const j=await r.json();if(!Array.isArray(j?.skills))continue;
   if(j?.$schema===V2_SCHEMA){
    for(const e of j.skills){if(typeof e?.name!=='string'||e?.type!=='skill-md'||typeof e?.url!=='string'||typeof e?.digest!=='string')continue;try{out.set(e.name,{kind:'v2',url:new URL(e.url,c.url).toString(),digest:e.digest});}catch{}}
   }else if(j?.$schema===undefined){
    for(const e of j.skills){if(typeof e?.name!=='string'||!Array.isArray(e?.files)||!e.files.some((f:any)=>typeof f==='string'&&f.toLowerCase()==='skill.md'))continue;out.set(e.name,{kind:'v1',base:`https://${host}`,path:c.path});}
   }
  }catch{}
 }
 return out;
}
Deno.serve(async(req)=>{
 const su=Deno.env.get('SUPABASE_URL'),sk=Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');if(!su||!sk)return new Response('config',{status:500});
 const db=createClient(su,sk,{auth:{persistSession:false}});let body:any={};try{body=await req.json();}catch{}
 const {data:auth,error:ae}=await db.rpc('skillset_consume_job_token',{p_token:body.token??'',p_purpose:'wellknown-content'});if(ae||auth!==true)return new Response('Unauthorized',{status:401});
 const limit=Math.max(1,Math.min(Number(body.limit??10),25));
 const {data:rows,error:ce}=await db.rpc('skillset_claim_wellknown',{p_limit:limit});if(ce)return new Response(JSON.stringify({ok:false,error:ce.message}),{status:500,headers:{'content-type':'application/json'}});
 let okCount=0,errorCount=0,digestVerified=0;const bucket='skillset-corpus';const indexCache=new Map<string,Promise<Map<string,IndexHit>>>();
 for(const row of rows??[]){
  try{
   const host=String(row.source??'').trim();const skill=String(row.skill_name??'').trim();if(!host||!skill)throw new Error('invalid_wellknown_source');
   if(!indexCache.has(host))indexCache.set(host,loadHostIndex(host));const index=await indexCache.get(host)!;const hit=index.get(skill);
   let text:string|null=null,sourceUrl:string|null=null;
   if(hit?.kind==='v2'){
    const r=await fetch(hit.url,{headers:{'user-agent':'skillset-corpus/1.0'},signal:AbortSignal.timeout(12000)});if(r.ok){const bytes=new Uint8Array(await r.arrayBuffer());const digest=await sha256Bytes(bytes);const expected=hit.digest.replace(/^sha256:/,'').toLowerCase();if(digest!==expected)throw new Error('wellknown_digest_mismatch');const candidate=new TextDecoder().decode(bytes);if(looksLikeSkillMd(candidate)){text=candidate;sourceUrl=hit.url;digestVerified++;}}
   }else if(hit?.kind==='v1'){
    const u=`${hit.base}/${hit.path}/${encodeURIComponent(skill)}/SKILL.md`;const r=await fetch(u,{headers:{'user-agent':'skillset-corpus/1.0'},signal:AbortSignal.timeout(12000)});if(r.ok){const candidate=await r.text();if(looksLikeSkillMd(candidate)){text=candidate;sourceUrl=u;}}
   }
   if(text===null){
    const direct=[`https://${host}/.well-known/agent-skills/${encodeURIComponent(skill)}/SKILL.md`,`https://${host}/.well-known/skills/${encodeURIComponent(skill)}/SKILL.md`];
    for(const u of direct){const r=await fetch(u,{headers:{'user-agent':'skillset-corpus/1.0'},signal:AbortSignal.timeout(12000)});if(!r.ok)continue;const candidate=await r.text();if(looksLikeSkillMd(candidate)){text=candidate;sourceUrl=u;break;}}
   }
   if(text===null||sourceUrl===null)throw new Error('wellknown_not_found');
   const hash=await sha256(text),gz=await gzipBytes(text),objectKey=`sha256/${hash.slice(0,2)}/${hash}.md.gz`;
   const {error:upErr}=await db.storage.from(bucket).upload(objectKey,gz,{contentType:'application/gzip',upsert:false,cacheControl:'31536000'});if(upErr&&!/already|duplicate/i.test(String(upErr.message)))throw new Error(`upload:${upErr.message}`);
   const {error:se}=await db.rpc('skillset_store_object_content',{p_id:row.id,p_content_sha256:hash,p_content_bytes:enc.encode(text).byteLength,p_object_key:objectKey,p_compressed_bytes:gz.byteLength,p_source_url:sourceUrl,p_backend:'supabase-storage'});if(se)throw new Error(`store:${se.message}`);okCount++;
  }catch(e){const m=e instanceof Error?e.message:String(e);await db.rpc('skillset_mark_error',{p_id:row.id,p_error:m});errorCount++;}
 }
 return new Response(JSON.stringify({ok:true,processed:(rows??[]).length,okCount,errorCount,digestVerified}),{headers:{'content-type':'application/json'}});
});