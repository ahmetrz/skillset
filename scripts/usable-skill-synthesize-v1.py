#!/usr/bin/env python3
import gzip, hashlib, io, json, os, pathlib, re, tarfile, time

IN=pathlib.Path(os.environ.get("CAP_CONSOLIDATED_DIR","usable-consolidated"))
OUT=pathlib.Path(os.environ.get("USABLE_PACKAGE_OUT","usable-package"))
POLICY=pathlib.Path(os.environ.get("POLICY_OVERLAY","audit/final-policy-overlay.json"))
OUT.mkdir(parents=True,exist_ok=True)

MODEL_RE=re.compile(r"\b(?:claude\s*(?:opus|sonnet|haiku)?\s*\d*(?:\.\d+)?|gpt[- ]?(?:3\.5|4(?:o|\.1)?|5(?:\.\d+)?)|gemini\s*(?:pro|flash|ultra)?\s*\d*(?:\.\d+)?|o[134](?:-mini)?)\b",re.I)
FILLER_RE=re.compile(r"^(?:you are|act as|as an expert|your role is|important:|note:)\b",re.I)
DANGEROUS_RE=re.compile(r"\b(?:ignore previous instructions|disable security|bypass (?:security|approval|validation)|skip (?:all )?tests|force[- ]push|git reset --hard|rm -rf|drop database|delete production)\b",re.I)
NEG_RE=re.compile(r"\b(?:do not|don't|never|avoid|forbid|without approval|requires? approval)\b",re.I)
SPACE=re.compile(r"\s+")

def clean(s,limit=360):
    s=SPACE.sub(" ",str(s or "").strip())
    s=MODEL_RE.sub("available model",s)
    if FILLER_RE.search(s): return ""
    return s[:limit].strip()

def safe_line(s):
    s=clean(s)
    if not s:return ""
    if DANGEROUS_RE.search(s) and not NEG_RE.search(s): return ""
    return s

def slugify(s):
    s=MODEL_RE.sub("",s.lower())
    s=re.sub(r"[^a-z0-9]+","-",s).strip("-")
    return s[:56].strip("-") or "skill"

def category(f):
    ds=set(f.get("domains") or []);act=f.get("action","operate")
    if "security" in ds or act=="security":return "security"
    if "testing" in ds or act=="test":return "testing"
    if ds & {"cicd","container","kubernetes","cloud","infrastructure"} or act=="deploy":return "devops"
    if "database" in ds:return "data-database"
    if "data" in ds:return "data-analytics"
    if "frontend" in ds:return "frontend-ui"
    if "backend" in ds or "api" in ds:return "backend-api"
    if "git" in ds:return "git-workflow"
    if "observability" in ds or act=="observe":return "observability"
    if "ai-agent" in ds:return "ai-agents"
    if "mobile" in ds:return "mobile"
    if "docs" in ds or act=="document":return "documentation"
    if "content" in ds:return "content-design"
    if "project" in ds or act=="design":return "planning-architecture"
    return "general"

def dedup(lines,maxn):
    out=[];seen=set()
    for x in lines or []:
        x=safe_line(x);k=re.sub(r"[^a-z0-9]+"," ",x.lower()).strip()
        if not x or not k or k in seen:continue
        seen.add(k);out.append(x)
        if len(out)>=maxn:break
    return out

def imperative(line):
    if not line:return line
    line=line.strip()
    if line[-1:] not in ".!?":line+="."
    return line[0].upper()+line[1:]

def skill_text(f,name,desc):
    proc=dedup(f.get("actions"),12)
    val=dedup(f.get("validate"),5)
    saf=dedup(f.get("safety"),4)
    if not proc:
        proc=["Inspect the relevant project context and existing conventions before changing anything.",
              "Apply the smallest complete change that satisfies the requested outcome."]
    if not val:
        val=["Run the repository's relevant existing checks and verify the requested outcome."]
    risk=int(f.get("risk_mask") or 0)
    if risk and not any("approval" in x.lower() for x in saf):
        saf.append("Require explicit approval for production or materially destructive actions.")
    if not any("destructive" in x.lower() or "force" in x.lower() for x in saf):
        saf.append("Avoid destructive Git or data-loss operations; prefer reversible changes.")
    saf=dedup(saf,5)
    lines=[
        "---",f"name: {name}",f"description: {desc}","---","",
        f"# {name.replace('-',' ').title()}","",
        "## Goal",clean(f.get("description") or desc,280) or desc,"",
        "## Procedure"
    ]
    lines += [f"{i}. {imperative(x)}" for i,x in enumerate(proc,1)]
    lines += ["","## Validate"]+[f"- {imperative(x)}" for x in val]
    if saf:
        lines += ["","## Safety"]+[f"- {imperative(x)}" for x in saf]
    return "\n".join(lines).rstrip()+"\n"

policy=json.loads(POLICY.read_text(encoding="utf-8")) if POLICY.exists() else {}
policy_md="""# Portable defaults

- Proceed without unnecessary clarification unless the action is materially ambiguous or high-risk.
- Prefer test-first behavior and validate the requested outcome.
- Security takes priority over speed.
- Do not perform destructive Git operations by default.
- Require human approval for production changes.
- Be provider-agnostic unless the task genuinely depends on a provider-specific capability.
- Use the repository's existing tooling and conventions; do not migrate tools without a concrete need.
- Use context- and risk-specific thresholds, retries, timeouts, and coverage targets.
- Keep memory stateless by default; persist only when explicitly scoped.
- Prefer pull-request workflows for repository changes.
"""

family_file=IN/"capability-families.ndjson.gz"
if not family_file.exists():raise SystemExit("missing "+str(family_file))
tar_path=OUT/"final-usable-skillset.tar.gz"
catalog_path=OUT/"catalog.ndjson.gz"
catalog=gzip.open(catalog_path,"wt",encoding="utf-8",compresslevel=6)
skills=0;procedures=0;validations=0;safety_rules=0;categories={}
seen_names=set()

with tarfile.open(tar_path,"w:gz",compresslevel=6) as tar:
    def add_bytes(name,data):
        b=data.encode("utf-8");ti=tarfile.TarInfo(name);ti.size=len(b);ti.mtime=0;tar.addfile(ti,io.BytesIO(b))
    add_bytes("README.md","# Final Usable Skill Set\n\nGenerated from the fully verified GitSkills + skills.sh corpus. Skills are canonical, compact, provider-agnostic by default, and traceable through the audit maps.\n")
    add_bytes("policy/portable-defaults.md",policy_md)
    with gzip.open(family_file,"rt",encoding="utf-8") as f:
        for line in f:
            if not line.strip():continue
            fam=json.loads(line);fid=str(fam["family_id"]);cat=category(fam)
            sig=str(fam.get("signature") or "")
            parts=[x for x in sig.split("|") if x and x not in {"general","core"}]
            base="-".join(parts[:3]) or clean(fam.get("title"),80)
            base=slugify(base)
            name=(base+"-"+fid[:6])[:63].strip("-")
            if name in seen_names:name=(base+"-"+fid[:10])[:63].strip("-")
            seen_names.add(name)
            ds=", ".join(fam.get("domains") or []) or "general software and knowledge work"
            desc=clean(f"Use for {fam.get('action','operate')} tasks involving {ds}.",180)
            body=skill_text(fam,name,desc)
            add_bytes(f"skills/{cat}/{name}/SKILL.md",body)
            proc=len(dedup(fam.get("actions"),12));val=len(dedup(fam.get("validate"),5));saf=len(dedup(fam.get("safety"),5))
            catalog.write(json.dumps({"id":fid,"name":name,"description":desc,"category":cat,"action":fam.get("action"),"domains":fam.get("domains") or [],"members":int(fam.get("members") or 0),"sdlc_mask":int(fam.get("sdlc_mask") or 0),"social_mask":int(fam.get("social_mask") or 0),"risk_mask":int(fam.get("risk_mask") or 0),"path":f"skills/{cat}/{name}/SKILL.md"},separators=(",",":"))+"\n")
            skills+=1;procedures+=proc;validations+=val;safety_rules+=saf;categories[cat]=categories.get(cat,0)+1
catalog.close()

manifest={
 "generatedAt":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),"status":"SYNTHESIZED",
 "skills":skills,"categories":dict(sorted(categories.items())),"procedureItems":procedures,
 "validationItems":validations,"safetyItems":safety_rules,
 "policyApproval":policy.get("approval","unknown"),"policyProfile":"portable-user-approved-v1",
 "runtimeArchive":tar_path.name,"catalog":catalog_path.name,
 "sourceTextPolicy":"Source text remains provenance; unsafe/conflicting instructions are not promoted."
}
(OUT/"usable-manifest.json").write_text(json.dumps(manifest,indent=2)+"\n",encoding="utf-8")
print(json.dumps({"event":"usable_synthesis_complete",**manifest},indent=2))
