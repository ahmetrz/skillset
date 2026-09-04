const urls=[
 'https://skills.sh/facukoren/pm-agent/pm-workflow',
 'https://skills.sh/k-park/ks-dev-proc/fix-root-cause',
 'https://skills.sh/zoharvan12/kolbo-code/video-prompting-guide'
];
for(const url of urls){
 const html=await (await fetch(url,{headers:{'user-agent':'skillset-audit-probe/6.2'}})).text();
 console.log(JSON.stringify({event:'page',url,bytes:html.length}));
 const needles=['previewHtml','restHtml','hash','sha256','contentHash','rawMarkdown','markdownContent','readme','skillName'];
 for(const n of needles){
   const arr=[];let p=0;
   while(true){const i=html.indexOf(n,p);if(i<0)break;arr.push(i);p=i+n.length}
   if(arr.length)console.log(JSON.stringify({event:'needle',url,needle:n,count:arr.length,hits:arr.slice(0,10).map(i=>({i,s:html.slice(Math.max(0,i-700),Math.min(html.length,i+2200))}))}));
 }
 const hex=[...html.matchAll(/[a-f0-9]{64}/gi)].map(m=>({v:m[0],i:m.index}));
 console.log(JSON.stringify({event:'hex64',url,count:hex.length,hits:hex.slice(0,20)}));
 const phrase=url.includes('fix-root')?'근본 원인':url.includes('video-')?'Universal Prompt Formula':'Phase 0: INTAKE';
 const p=html.indexOf(phrase); console.log(JSON.stringify({event:'phrase',url,phrase,index:p,snippet:p>=0?html.slice(Math.max(0,p-1500),Math.min(html.length,p+10000)):''}));
}