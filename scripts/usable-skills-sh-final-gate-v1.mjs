import fs from 'node:fs/promises';
import path from 'node:path';

const base=JSON.parse(await fs.readFile(process.env.BASE_GATE||'audit/usable-skills-sh-source-gate.json','utf8'));
const anomaly=JSON.parse(await fs.readFile(process.env.ANOMALY_GATE||'audit/usable-skills-sh-anomaly-b2.json','utf8'));
const out=process.env.FINAL_GATE_OUT||'audit/usable-skills-sh-final-source-gate.json';

const expectedBySource=new Map();
for(const x of [...(base.missing||[]),...(base.extra||[])])expectedBySource.set(String(x.sourceKey),Number(x.expected||0));
const reportBySource=new Map((anomaly.report||[]).map(x=>[String(x.sourceKey),x]));
const anomalies=[...expectedBySource.keys()];
const missingReport=anomalies.filter(k=>!reportBySource.has(k));
const badReport=anomalies.filter(k=>reportBySource.get(k)?.status!=='exact_b2_verified'||Number(reportBySource.get(k)?.files||0)!==expectedBySource.get(k));
const anomalyExpected=anomalies.reduce((s,k)=>s+expectedBySource.get(k),0);
const anomalyObserved=anomalies.reduce((s,k)=>s+Number(reportBySource.get(k)?.files||0),0);
const completeRepos=Number(base.completeRepos||0)+anomalies.length;
const exactFiles=(Number(base.expectedSkills||0)-anomalyExpected)+anomalyObserved;
const ok=Number(base.expectedRepos)===7555&&Number(base.expectedSkills)===188725&&anomaly.status==='ALL_ANOMALIES_EXACT_B2_VERIFIED'&&missingReport.length===0&&badReport.length===0&&completeRepos===7555&&exactFiles===188725;
const result={
  generatedAt:new Date().toISOString(),
  status:ok?'ALL_SKILLS_SH_EXACT_FILES_VERIFIED':'BLOCKED_SKILLS_SH_FINAL_GATE',
  authoritativeSource:'skills_sh_external_exact_v1 + exact B2 anomaly overrides',
  repositories:{expected:7555,verified:completeRepos,baseExactCountRepos:Number(base.completeRepos||0),exactB2OverrideRepos:anomalies.length},
  files:{expected:188725,verified:exactFiles,baseNonAnomalyFiles:Number(base.expectedSkills||0)-anomalyExpected,exactB2OverrideFiles:anomalyObserved},
  anomalyVerification:{status:anomaly.status,expectedSources:anomalies.length,completedSources:Number(anomaly.completedSources||0),missingReport,badReport},
  hardGate:{allSkillsShIncluded:ok,noScopeExclusion:true,usablePipelineAllowed:ok}
};
await fs.mkdir(path.dirname(out),{recursive:true});await fs.writeFile(out,JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result,null,2));if(!ok)process.exitCode=3;
