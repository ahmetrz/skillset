import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const BUCKET="skill-discovery-v1";
const DEADLINE=42000;

function b64(s:string){
  const bin=atob((s||"").replace(/\s/g,""));
  const a=new Uint8Array(bin.length);
  for(let i=0;i<bin.length;i++)a[i]=bin.charCodeAt(i);
  return a;
}
async function gunzipBlob(x:Blob){
  return await new Response(x.stream().pipeThrough(new DecompressionStream("gzip"))).text();
}
async function gunzipBytes(x:Uint8Array){
  return await new Response(new Blob([x]).stream().pipeThrough(new DecompressionStream("gzip"))).text();
}
async function sha(text:string){
  const d=await crypto.subtle.digest("SHA-256",new TextEncoder().encode(text));
  return Array.from(new Uint8Array(d),x=>x.toString(16).padStart(2,"0")).join("");
}
function clip(s:string,n=420){
  s=(s||"").replace(/\s+/g," ").trim();
  return s.length>n?s.slice(0,n-1)+"…":s;
}
function stripCode(s:string){
  return s.replace(/\x60\x60\x60[\s\S]*?\x60\x60\x60/g," ").replace(/\x60[^\x60]*\x60/g," ");
}
function fm(md:string){
  const l=md.split(/\r?\n/);
  if(l[0]?.trim()!=="---")return {name:"",description:"",body:md};
  let i=1;while(i<l.length&&l[i].trim()!=="---")i++;
  if(i>=l.length)return {name:"",description:"",body:md};
  const h=l.slice(1,i).join("\n");
  return {
    name:clip((h.match(/^name\s*:\s*(.+)$/mi)?.[1]||"").replace(/^["']|["']$/g,""),220),
    description:clip((h.match(/^description\s*:\s*(.+)$/mi)?.[1]||"").replace(/^["']|["']$/g,""),320),
    body:l.slice(i+1).join("\n")
  };
}
const STOP=new Set("the a an and or but if then else of to in on for from with without by as is are was were be been being this that these those it its into over under before after when where what how why which who your you we our they their can could should would may might do does did done use using used via per each any all some more most less than not no yes".split(/\s+/));
function toks(text:string){
  const m=stripCode(text).replace(/https?:\/\/\S+/g," URL ").toLowerCase().match(/[a-z0-9][a-z0-9_+.#-]{1,40}/g)||[];
  const o:string[]=[];
  for(const t of m){
    const x=t.replace(/^[#._-]+|[#._-]+$/g,"");
    if(x.length>=2&&!STOP.has(x)&&!(/^\d+$/.test(x)))o.push(x);
    if(o.length>=10000)break;
  }
  return o;
}
function fnv(s:string){
  let h=1469598103934665603n;
  for(let i=0;i<s.length;i++){
    h^=BigInt(s.charCodeAt(i));
    h=BigInt.asUintN(64,h*1099511628211n);
  }
  return h;
}
function sim(ts:string[]){
  const v=new Int32Array(64);
  const seen=new Set<string>();
  let n=0;
  function add(f:string){
    if(seen.has(f)||n>=8000)return;
    seen.add(f);n++;
    const h=fnv(f);
    for(let b=0;b<64;b++)v[b]+=(((h>>BigInt(b))&1n)===1n)?1:-1;
  }
  if(ts.length>=3)for(let i=0;i<ts.length-2;i++)add(ts[i]+" "+ts[i+1]+" "+ts[i+2]);else ts.forEach(add);
  let h=0n;
  for(let b=0;b<64;b++)if(v[b]>=0)h|=(1n<<BigInt(b));
  h=BigInt.asUintN(64,h);
  return {bin:h.toString(2).padStart(64,"0"),c0:Number(h&65535n),c1:Number((h>>16n)&65535n),c2:Number((h>>32n)&65535n),c3:Number((h>>48n)&65535n)};
}
function providers(t:string){
  const p:[string,RegExp][]=[
    ["anthropic",/\banthropic\b/i],["claude",/\bclaude\b/i],["opus",/\bopus\b/i],["sonnet",/\bsonnet\b/i],
    ["openai",/\bopenai\b/i],["codex",/\bcodex\b/i],["gpt",/\bgpt\b|\bgpt[- .]?\d/i],["gemini",/\bgemini\b/i],
    ["cursor",/\bcursor\b/i],["copilot",/\bcopilot\b/i],["cline",/\bcline\b/i],["aider",/\baider\b/i]
  ];
  return p.filter(x=>x[1].test(t)).map(x=>x[0]);
}
function risks(t:string){
  t=stripCode(t);const o:string[]=[];
  const p:[string,RegExp][]=[
    ["reasoning_leakage",/(show|reveal|print|write out|display).{0,90}(chain[- ]of[- ]thought|internal reasoning|hidden reasoning|private reasoning|thinking process)/is],
    ["destructive_git",/\b(git reset --hard|git clean -f|git push --force|force[- ]push)\b/i],
    ["secrets_exposure",/(print|echo|show|log|commit).{0,80}(secret|api key|token|password|credential)/is],
    ["test_bypass",/(skip|bypass|disable|ignore).{0,80}(test|tests|test suite|quality gate)/is],
    ["auto_production_change",/(automatic|automatically|without approval|without confirmation).{0,100}(deploy|release|change).{0,80}(prod|production)|(deploy|release).{0,80}(prod|production).{0,80}(without approval|without confirmation)/is],
    ["silent_error_continue",/(ignore|swallow|silently continue|continue anyway).{0,80}(error|failure|exception)/is],
    ["deprecated_model",/\b(claude[- ]?(?:2|3(?:\.\d)?|4(?:\.\d)?)|gpt-3\.5|gpt-4o)\b/i]
  ];
  for(const [n,r] of p)if(r.test(t))o.push(n);
  return o;
}
type Fact={topic:string,key:string,stance:string,value?:string,evidence:string};
function facts(text:string){
  const s=stripCode(text).split(/\n+|(?<=[.!?])\s+/).map(x=>x.trim()).filter(x=>x.length>=16&&x.length<=900);
  const o:Fact[]=[];
  const add=(topic:string,key:string,stance:string,e:string,value?:string)=>{
    if(o.length>=40)return;
    const evidence=clip(e,420);
    if(!o.some(x=>x.topic===topic&&x.key===key&&x.stance===stance&&x.value===value&&x.evidence===evidence))o.push({topic,key,stance,value,evidence});
  };
  for(const z0 of s){
    const z=z0.replace(/^[-*]\s+/,""),l=z.toLowerCase();
    if(/\b(must|always|required|should)\b/.test(l)&&/(ask|clarif|confirm)/.test(l)&&/(user|ambigu|uncertain|missing|unclear)/.test(l))add("clarification","user_interaction","ask_when_ambiguous",z);
    if(/(do not|don't|never)\s+(ask|pause|wait|clarif|confirm)|proceed without (asking|clarification|confirmation)|continue without (asking|clarification)/i.test(z))add("clarification","user_interaction","proceed_without_asking",z);
    if(/\b(test[- ]first|tdd|write (?:the )?tests? first|tests? before (?:implementation|coding|code))\b/i.test(z))add("testing_order","implementation_sequence","test_first",z);
    if(/(?:implement|write (?:the )?code|code).{0,90}(?:then|after).{0,50}tests?|tests?\s+after\s+(?:implementation|coding|code)/i.test(z))add("testing_order","implementation_sequence","test_after",z);
    if(/(never|do not|don't|must not|forbid|prohibit).{0,100}(git reset --hard|git clean -f|git push --force|force[- ]push|destructive git)/i.test(z))add("destructive_git","destructive_git","forbid",z);
    if(/(may|can|allowed|use|run).{0,100}(git reset --hard|git clean -f|git push --force|force[- ]push)/i.test(z)&&!/(never|do not|don't|must not|only with|approval|confirm)/i.test(z))add("destructive_git","destructive_git","allow",z);
    if(/(?:prod|production).{0,120}(approval|approve|confirm|permission|gate)|(?:approval|approve|confirm|permission|gate).{0,120}(?:prod|production)/i.test(z))add("production_deploy","production_change","approval_required",z);
    if(/(?:automatic|automatically|auto[- ]?deploy|without approval|without confirmation).{0,120}(?:deploy|release|change).{0,100}(?:prod|production)|(?:deploy|release).{0,100}(?:prod|production).{0,100}(?:without approval|without confirmation)/i.test(z))add("production_deploy","production_change","auto_without_approval",z);
    if(/security[- ]first|security (?:takes|has) priority|prioriti[sz]e security|never sacrifice security|security over speed/i.test(z))add("priority","security_vs_speed","security_first",z);
    if(/speed[- ]first|prioriti[sz]e speed|speed over security|move fast.{0,80}(?:security|safety).{0,40}(?:later|after)/i.test(z))add("priority","security_vs_speed","speed_first",z);
    if(/\bstateless\b|do not (?:persist|retain|remember).{0,60}(?:state|memory|context)/i.test(z))add("memory_mode","agent_state","stateless",z);
    if(/persistent memory|must (?:persist|retain|remember).{0,60}(?:state|memory|context)|requires? persistent (?:state|memory)/i.test(z))add("memory_mode","agent_state","persistent_required",z);
    if(/(?:feature|topic) branch|pull request|merge request|never (?:commit|push) directly to (?:main|master)|protected branch/i.test(z))add("branch_policy","integration_path","feature_branch_pr",z);
    if(/(?:commit|push|merge) directly (?:to|into) (?:main|master)|work directly on (?:main|master)/i.test(z)&&!/(never|do not|don't|must not)/i.test(z))add("branch_policy","integration_path","direct_main",z);
    if(/model[- ]agnostic|provider[- ]agnostic|avoid provider[- ]specific|do not depend on a specific (?:model|provider)/i.test(z))add("model_scope","provider_binding","model_agnostic",z);
    const f=z.match(/(?:must|always|required|only)\s+(?:use|select|route to|run|with).{0,70}\b(claude|opus|sonnet|anthropic|openai|codex|gpt[- .]?[a-z0-9.]*|gemini|cursor|copilot|cline|aider)\b/i);
    if(f)add("model_scope","provider_binding","provider_forced",z,f[1].toLowerCase());
    const n=z.match(/\b(retr(?:y|ies)|timeout|coverage|score|threshold|max(?:imum)? attempts?|minimum|limit)\b.{0,45}?\b(\d+(?:\.\d+)?)\s*(%|ms|s|sec|seconds?|m|min|minutes?|h|hours?)?/i);
    if(n&&/\b(must|required|always|set|minimum|maximum|at least|at most|no more than|exactly)\b/i.test(z))add("numeric_policy",n[1].toLowerCase().replace(/\s+/g,"_"),"fixed_value",z,(n[2]+(n[3]||"")).toLowerCase());
  }
  return o;
}
function feature(hash:string,ph:string,text:string){
  const h=fm(text),ts=toks(text),sh=sim(ts),body=h.body||text;
  return {
    canonical_hash:hash,projected_content_hash:ph,skill_name:h.name||null,skill_description:h.description||null,
    first_heading:clip(body.match(/^#{1,3}\s+(.+)$/m)?.[1]||"",220)||null,char_count:text.length,token_count:ts.length,
    block_count:text.split(/\n{2,}/).filter(x=>x.trim().length>=28).length,simhash:sh.bin,sim_chunk0:sh.c0,sim_chunk1:sh.c1,
    sim_chunk2:sh.c2,sim_chunk3:sh.c3,policy_facts:facts(text),provider_refs:providers(text),risk_flags:risks(text),evidence_excerpt:clip(text,420)
  };
}

Deno.serve(async()=>{
  const sb=createClient(Deno.env.get("SUPABASE_URL")!,Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,{auth:{persistSession:false}});
  const start=Date.now();let claims=0,features=0,failures=0;
  while(Date.now()-start<DEADLINE){
    const c=await sb.rpc("skillset_final_audit_claim_v1");
    if(c.error)return Response.json({ok:false,stage:"claim",error:c.error.message,claims,features,failures},{status:500});
    const claim=c.data;if(!claim||claim.kind==="none")break;claims++;
    let hashes:string[]=[];let path:string|null=null;
    try{
      const rows:any[]=[];
      if(claim.kind==="pack"){
        path=String(claim.projection_path);hashes=(claim.hashes||[]).map(String);
        const q=await sb.schema("skillset").from("final_audit_target_v1").select("canonical_hash,projected_content_hash").eq("run_id",claim.run_id).in("canonical_hash",hashes);
        if(q.error)throw new Error("targets:"+q.error.message);
        const expected=new Map((q.data||[]).map((x:any)=>[String(x.canonical_hash),String(x.projected_content_hash)]));
        const found=new Set<string>();
        const d=await sb.storage.from(BUCKET).download(path);if(d.error||!d.data)throw new Error("download:"+(d.error?.message||"missing"));
        const payload=JSON.parse(await gunzipBlob(d.data));
        for(const r of payload.rows||[]){
          const h=String(r.source_content_hash||"");
          if(!expected.has(h)||found.has(h)||r.decision!=="keep"||typeof r.projected_text!=="string")continue;
          const ph=String(r.projected_content_hash||"");
          if(ph!==expected.get(h)||await sha(r.projected_text)!==ph)throw new Error("hash_mismatch:"+h);
          rows.push(feature(h,ph,r.projected_text));found.add(h);
        }
        if(found.size!==hashes.length)throw new Error("missing_targets:"+found.size+"/"+hashes.length);
      }else if(claim.kind==="db"){
        const rr=claim.rows||[];hashes=rr.map((x:any)=>String(x.canonical_hash));
        for(const r of rr){
          const text=await gunzipBytes(b64(String(r.content_gzip_base64||""))),ph=String(r.projected_content_hash||"");
          if(await sha(text)!==ph)throw new Error("db_hash:"+r.canonical_hash);
          rows.push(feature(String(r.canonical_hash),ph,text));
        }
      }else throw new Error("unknown_kind:"+claim.kind);
      const f=await sb.rpc("skillset_final_audit_finish_v1",{p_run_id:claim.run_id,p_kind:claim.kind,p_path:path,p_rows:rows,p_claimed_hashes:hashes,p_error:null});
      if(f.error)throw new Error("finish:"+f.error.message);
      features+=rows.length;
    }catch(e){
      failures++;
      await sb.rpc("skillset_final_audit_finish_v1",{p_run_id:claim.run_id,p_kind:claim.kind,p_path:path,p_rows:[],p_claimed_hashes:hashes,p_error:String(e)});
    }
  }
  return Response.json({ok:true,claims,features,failures,elapsed_ms:Date.now()-start});
});