import fs from 'node:fs/promises';
import path from 'node:path';

const read=async p=>JSON.parse(await fs.readFile(p,'utf8'));
const legacy=await read(process.env.LEGACY_GATE||'audit/usable-gitskills-legacy-full-gate.json');
const b2=await read(process.env.B2_GATE||'audit/usable-gitskills-b2-full-gate.json');
const skills=await read(process.env.SKILLS_GATE||'audit/usable-skills-sh-final-source-gate.json');
const out=process.env.OUT||'audit/usable-all-source-final-gate.json';

const legacyOk=legacy.status==='ALL_GITSKILLS_LEGACY_RANGE_VERIFIED'&&legacy.hardGate?.usablePipelineAllowed===true;
const b2Ok=b2.status==='ALL_GITSKILLS_B2_REPRESENTATIVES_VERIFIED'&&b2.hardGate?.usablePipelineAllowed===true;
const skillsOk=skills.status==='ALL_SKILLS_SH_EXACT_FILES_VERIFIED'&&skills.hardGate?.usablePipelineAllowed===true;

const gitLegacy=Number(legacy.distinctContentHashes||0);
const gitB2Records=Number(b2.observedRepresentativeRecords||0);
const gitB2Distinct=Number(b2.observedDistinctContentHashes||0);
const skillsFiles=Number(skills.files?.verified||0);
const sourceRecords=gitLegacy+gitB2Records+skillsFiles;
const result={
  generatedAt:new Date().toISOString(),
  status:legacyOk&&b2Ok&&skillsOk?'ALL_GITSKILLS_AND_SKILLS_SH_VERIFIED':'BLOCKED_SOURCE_PLATFORM_GATE',
  policy:'Every acquired GitSkills representative and every exact skills.sh SKILL.md is included. SDLC/social masks are classification only and never exclusion filters.',
  gitSkills:{
    legacy:{verified:legacyOk,representatives:gitLegacy,rawRows:Number(legacy.rawRowsObserved||0),shards:Number(legacy.sourceUnitsObserved||0)},
    b2:{verified:b2Ok,representativeRecords:gitB2Records,distinctContentHashes:gitB2Distinct,exactDuplicateRecords:gitB2Records-gitB2Distinct,packs:Number(b2.sourceUnits||0)},
    totalSourceRepresentativeRecords:gitLegacy+gitB2Records,
    preCrossPartitionDistinctUpperBound:gitLegacy+gitB2Distinct
  },
  skillsSh:{verified:skillsOk,repositories:Number(skills.repositories?.verified||0),exactFiles:skillsFiles},
  totalSourceRecordsBeforeCrossSourceDedup:sourceRecords,
  hardGate:{
    allGitSkillsIncluded:legacyOk&&b2Ok,
    allSkillsShIncluded:skillsOk,
    noScopeExclusion:true,
    usablePipelineAllowed:legacyOk&&b2Ok&&skillsOk
  },
  authoritativeInputs:{
    legacy:'full HF parquet range reconstruction',
    gitskillsB2:'all 642 acquisition pack ranges reconstructed from HF parquet',
    skillsSh:'7,555 current exact source keys with SHA-256 verified B2 overrides for all anomalies'
  }
};
await fs.mkdir(path.dirname(out),{recursive:true});await fs.writeFile(out,JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result,null,2));
if(!result.hardGate.usablePipelineAllowed)process.exitCode=3;
