import fs from 'node:fs/promises';

const urls=[
  'https://skills.sh/facukoren/pm-agent/pm-workflow',
  'https://skills.sh/k-park/ks-dev-proc/fix-root-cause',
  'https://skills.sh/zoharvan12/kolbo-code/video-prompting-guide'
];
const needles=['"hash"','"files"','"contents"','"content"','markdown','snapshot','rawContent','skillContent','Show more','api/v1/skills','/api/'];
for(const url of urls){
  const html=await (await fetch(url,{headers:{'user-agent':'skillset-audit-probe/6.1'}})).text();
  console.log(JSON.stringify({event:'page',url,bytes:html.length}));
  for(const n of needles){
    let from=0,count=0;
    while(true){
      const i=html.indexOf(n,from); if(i<0)break;
      count++; if(count<=5)console.log(JSON.stringify({event:'hit',url,needle:n,index:i,snippet:html.slice(Math.max(0,i-350),Math.min(html.length,i+1200))}));
      from=i+n.length;
    }
    if(count)console.log(JSON.stringify({event:'count',url,needle:n,count}));
  }
  const srcs=[...html.matchAll(/<script[^>]+src="([^"]+)"/g)].map(m=>new URL(m[1],url).href);
  console.log(JSON.stringify({event:'scripts',url,count:srcs.length,srcs}));
  for(const src of srcs){
    let js='';try{js=await (await fetch(src,{headers:{'user-agent':'skillset-audit-probe/6.1'}})).text()}catch{continue}
    const hitNeedles=['Show more','api/v1/skills','contents','snapshot','skillContent','rawContent','files'];
    for(const n of hitNeedles){
      const i=js.indexOf(n);
      if(i>=0)console.log(JSON.stringify({event:'js_hit',src,needle:n,index:i,snippet:js.slice(Math.max(0,i-700),Math.min(js.length,i+1800))}));
    }
  }
}