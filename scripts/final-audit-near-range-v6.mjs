import {createClient} from '@libsql/client';
import fs from 'node:fs/promises';
import path from 'node:path';
import {createGunzip,gzipSync} from 'node:zlib';
import readline from 'node:readline';

const PART=Number(process.env.NEAR_PARTITION||0),PARTS=Math.max(1,Number(process.env.NEAR_PARTITIONS||20));
const need=n=>{const v=process.env[n];if(!v)throw new Error('Missing '+n);return v};
const turso=createClient({url:need('TURSO_DATABASE_URL'),authToken:need('TURSO_AUTH_TOKEN')});
const input=process.env.NEAR_NEW_FILE||'',outDir=process.env.NEAR_OUT_DIR||'near-v6-out';

async function q(sql,args=[]){return (await turso.execute({sql,args})).rows}
function popcount64(hex){let x=BigInt('0x'+hex),n=0;while(x){x&=x-1n;n++}return n}
function ham(a,b){return popcount64((BigInt('0x'+a)^BigInt('0x'+b)).toString(16))}
async function* ndjsonGz(file){
  const fh=await fs.open(file,'r'),stream=fh.createReadStream().pipe(createGunzip());
  const rl=readline.createInterface({input:stream,crlfDelay:Infinity});
  try{for await(const line of rl)if(line.trim())yield JSON.parse(line)}
  finally{await fh.close().catch(()=>{})}
}

const bucketLo=Math.floor(PART*256/PARTS);
const bucketHi=Math.floor((PART+1)*256/PARTS)-1;
const newByAxisBand=[new Map(),new Map(),new Map()];
if(input){
  for await(const r of ndjsonGz(input)){
    const axis=Number(r.axis),task=Number(r.task),bucket=task-axis*256;
    if(axis<0||axis>2||bucket<bucketLo||bucket>bucketHi)continue;
    const key=axis===0?String(r.c0)+':'+String(r.c1):axis===1?String(r.c1)+':'+String(r.c2):String(r.c2)+':'+String(r.c3);
    if(!newByAxisBand[axis].has(key))newByAxisBand[axis].set(key,[]);
    newByAxisBand[axis].get(key).push(r);
  }
}

await fs.mkdir(outDir,{recursive:true});
const pairs=new Map(),oversized=[];
let groupsN=0,compared=0,maxGroup=0,oldRows=0;

for(let axis=0;axis<3;axis++){
  const col=axis===0?'c0':axis===1?'c1':'c2';
  const lo=bucketLo*256,hi=(bucketHi+1)*256-1;
  const old=await q("SELECT content_hash,simhash_hex,char_count,token_count,c0,c1,c2,c3 FROM final_audit_exact_v1 WHERE (sdlc_mask<>0 OR social_mask<>0) AND "+col+" BETWEEN ? AND ?",[lo,hi]);
  oldRows+=old.length;
  const groups=new Map(newByAxisBand[axis]);
  for(const r of old){
    const key=axis===0?String(r.c0)+':'+String(r.c1):axis===1?String(r.c1)+':'+String(r.c2):String(r.c2)+':'+String(r.c3);
    if(!groups.has(key))groups.set(key,[]);
    groups.get(key).push(r);
  }
  for(const [key,g0] of groups){
    if(g0.length<2)continue;
    const dedup=new Map();for(const r of g0)dedup.set(String(r.content_hash),r);
    const g=[...dedup.values()];
    if(g.length<2)continue;
    groupsN++;maxGroup=Math.max(maxGroup,g.length);
    if(g.length>3000){oversized.push({axis,band:key,groupSize:g.length,reason:'group_gt_3000'});continue}
    for(let i=0;i<g.length;i++)for(let j=i+1;j<g.length;j++){
      compared++;
      const x=g[i],y=g[j],dist=ham(String(x.simhash_hex),String(y.simhash_hex));
      if(dist>8)continue;
      const lr=Math.min(Number(x.char_count),Number(y.char_count))/Math.max(1,Math.max(Number(x.char_count),Number(y.char_count)));
      if(lr<0.55)continue;
      const a=String(x.content_hash)<String(y.content_hash)?String(x.content_hash):String(y.content_hash);
      const b=String(x.content_hash)<String(y.content_hash)?String(y.content_hash):String(x.content_hash);
      const id=a+'|'+b,relation=dist<=4&&lr>=0.90?'near_duplicate':'coverage_candidate';
      const cur=pairs.get(id);
      if(!cur||dist<cur.hamming)pairs.set(id,{a,b,hamming:dist,length_ratio:lr,relation,band:'axis'+axis+':'+key});
    }
  }
  console.log(JSON.stringify({event:'near_v6_axis_done',partition:PART,axis,bucketLo,bucketHi,oldRows:old.length,groups:groupsN,pairs:pairs.size,oversized:oversized.length}));
}
const rows=[...pairs.values()],body=rows.map(x=>JSON.stringify(x)).join('\n')+(rows.length?'\n':'');
const base='p'+String(PART).padStart(2,'0')+'-of-'+String(PARTS).padStart(2,'0');
await fs.writeFile(path.join(outDir,'near-'+base+'.ndjson.gz'),gzipSync(Buffer.from(body),{level:6}));
const manifest={generatedAt:new Date().toISOString(),engine:'range-v6',partition:PART,partitions:PARTS,bucketLo,bucketHi,oldRows,groups:groupsN,compared,candidates:rows.length,nearDuplicates:rows.filter(x=>x.relation==='near_duplicate').length,coverageCandidates:rows.filter(x=>x.relation==='coverage_candidate').length,oversized,maxGroup};
await fs.writeFile(path.join(outDir,'near-manifest-'+base+'.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({event:'near_v6_complete',...manifest,oversized:oversized.length}));
