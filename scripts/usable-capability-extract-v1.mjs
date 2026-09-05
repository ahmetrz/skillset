import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline';
import {createGunzip,createGzip} from 'node:zlib';
import {createHash} from 'node:crypto';
import {feature} from './final-audit-feature-core-v4.mjs';

const input=process.env.CAP_INPUT;
const output=process.env.CAP_OUTPUT||'capability-atoms.ndjson.gz';
const sourceSystem=process.env.CAP_SOURCE_SYSTEM||'unknown';
if(!input) throw new Error('CAP_INPUT required');

const sha=s=>createHash('sha256').update(s).digest('hex');
const norm=s=>String(s||'').toLowerCase()
  .replace(/https?:\/\/\S+/g,' ')
  .replace(/[*_#>|[\](){},.;:!?'"\\]/g,' ')
  .replace(/[^a-z0-9+\-./ ]+/g,' ')
  .replace(/\s+/g,' ').trim();
const STOP=new Set('the a an and or to of for with in on at by from is are be use using when this that your you it as into via skill skills agent expert guide workflow process should must can will do does'.split(' '));
function words(s){return norm(s).split(' ').filter(x=>x.length>1&&!STOP.has(x))}
function frontmatter(text){
  const m=text.match(/^---\s*\n([\s\S]{0,6000}?)\n---\s*(?:\n|$)/);
  if(!m)return {};
  const out={};
  for(const line of m[1].split(/\r?\n/)){
    const z=line.match(/^([A-Za-z0-9_-]+)\s*:\s*(.*)$/);
    if(z)out[z[1].toLowerCase()]=z[2].replace(/^['"]|['"]$/g,'').trim();
  }
  return out;
}
function titleOf(text,fm,p){
  if(fm.name)return fm.name;
  const h=text.match(/^#{1,2}\s+(.+)$/m);if(h)return h[1].trim();
  const b=String(p||'').split('/').filter(Boolean).pop()||'skill';
  return b.replace(/\.md$/i,'').replace(/[-_]+/g,' ');
}
function descriptionOf(text,fm,title){
  if(fm.description)return fm.description;
  const body=text.replace(/^---[\s\S]*?\n---\s*/,'').split(/\r?\n/).map(x=>x.trim()).filter(Boolean);
  for(const x of body){
    if(/^#/.test(x)||/^[-*]\s/.test(x))continue;
    if(x.length>=24)return x.slice(0,600);
  }
  return title;
}
function sections(text){
  const res=[];let cur={name:'body',lines:[]};
  for(const raw of text.split(/\r?\n/)){
    const m=raw.match(/^#{1,4}\s+(.+?)\s*$/);
    if(m){if(cur.lines.length)res.push(cur);cur={name:norm(m[1]),lines:[]};}
    else cur.lines.push(raw);
  }
  if(cur.lines.length)res.push(cur);return res;
}
const ACTION=/\b(build|create|implement|design|review|test|validate|verify|deploy|debug|fix|migrate|monitor|analy[sz]e|generate|configure|install|upgrade|refactor|document|publish|audit|secure|optimi[sz]e|plan|research|manage|integrate|convert|transform|extract|compare|check|run|write|read|query|search|remove|add|update)\b/i;
const VALID=/\b(test|validate|verify|check|assert|lint|typecheck|build|smoke|review|confirm)\b/i;
const SAFE=/\b(secret|credential|token|password|production|prod|destructive|force[- ]push|reset --hard|rm -rf|delete|drop|security|approval|permission|least privilege|backup)\b/i;
function bulletsFrom(sec,re,max=14){
  const out=[];
  for(const s of sec){
    for(let line of s.lines){
      line=line.trim().replace(/^[-*+]\s+/,'').replace(/^\d+[.)]\s+/,'').trim();
      if(line.length<12||line.length>360||/^https?:/.test(line))continue;
      if(re.test(line)){
        const n=line.replace(/\s+/g,' ');
        if(!out.some(x=>norm(x)===norm(n)))out.push(n.slice(0,360));
      }
      if(out.length>=max)return out;
    }
  }
  return out;
}
function simTokens(title,desc,actions){
  const arr=[...words(title),...words(desc),...actions.flatMap(words)];
  const counts=new Map();for(const w of arr)counts.set(w,(counts.get(w)||0)+1);
  return [...counts.entries()].sort((a,b)=>b[1]-a[1]||a[0].localeCompare(b[0])).slice(0,24).map(x=>x[0]);
}
const fh=fs.createReadStream(input).pipe(createGunzip());
const rl=readline.createInterface({input:fh,crlfDelay:Infinity});
await fsp.mkdir(path.dirname(output),{recursive:true});
const gz=createGzip({level:6}),out=fs.createWriteStream(output);gz.pipe(out);
let rows=0,empty=0;
for await(const line of rl){
  if(!line.trim())continue;
  let r;try{r=JSON.parse(line)}catch{continue}
  const text=String(r.text||r.content||'');if(!text.trim()){empty++;continue}
  const fm=frontmatter(text),title=titleOf(text,fm,r.path),desc=descriptionOf(text,fm,title),sec=sections(text);
  const actions=bulletsFrom(sec,ACTION,16),validate=bulletsFrom(sec,VALID,8),safety=bulletsFrom(sec,SAFE,8);
  const toks=simTokens(title,desc,actions);
  const capabilityKey=sha(toks.slice(0,12).sort().join('\u0000')).slice(0,24);
  const ff=feature(text,String(r.locator||''));
  const contentHash=String(r.content_hash||r.contentHash||ff.h||sha(text));
  const atom={
    v:1,source_system:String(r.source_system||r.sourceSystem||sourceSystem),
    source_key:String(r.source_key||r.sourceKey||''),item_key:String(r.item_key||r.itemKey||''),
    repo:r.repo||null,path:r.path||null,locator:r.locator||null,content_hash:contentHash,
    title:title.slice(0,180),description:desc.slice(0,600),
    capability_key:capabilityKey,tokens:toks,actions,validate,safety,
    sdlc_mask:Number(r.sdlc_mask??ff.sdlc_mask??0),social_mask:Number(r.social_mask??ff.social_mask??0),
    risk_mask:Number(r.risk_mask??ff.risk_mask??0),provider_mask:Number(r.provider_mask??ff.provider_mask??0)
  };
  gz.write(JSON.stringify(atom)+'\n');rows++;
}
gz.end();
await new Promise((res,rej)=>{out.on('finish',res);out.on('error',rej);gz.on('error',rej)});
console.log(JSON.stringify({event:'capability_atoms_complete',input,output,rows,empty}));
