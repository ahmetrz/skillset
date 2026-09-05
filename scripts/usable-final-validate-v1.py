#!/usr/bin/env python3
import gzip, hashlib, json, os, pathlib, re, tarfile, time

SRC=pathlib.Path(os.environ.get("SOURCE_GATE","audit/usable-all-source-final-gate.json"))
CONS=pathlib.Path(os.environ.get("CAP_CONSOLIDATED_DIR","usable-consolidated"))
PKG=pathlib.Path(os.environ.get("USABLE_PACKAGE_DIR","usable-package"))
OUT=pathlib.Path(os.environ.get("USABLE_VALIDATION_OUT","usable-package/validation.json"))
source=json.loads(SRC.read_text(encoding="utf-8"))
stats=json.loads((CONS/"consolidation-stats.json").read_text(encoding="utf-8"))
manifest=json.loads((PKG/"usable-manifest.json").read_text(encoding="utf-8"))

expected=int(source.get("totalSourceRecordsBeforeCrossSourceDedup") or 0)
archive=PKG/manifest["runtimeArchive"]
catalog=PKG/manifest["catalog"]
families=CONS/"capability-families.ndjson.gz"
members=CONS/"family-members.ndjson.gz"
coverage=CONS/"coverage-map.ndjson.gz"

def count_gz(p):
    n=0
    with gzip.open(p,"rt",encoding="utf-8") as f:
        for line in f:
            if line.strip():n+=1
    return n
def sha256_file(p):
    h=hashlib.sha256()
    with open(p,"rb") as f:
        for b in iter(lambda:f.read(4*1024*1024),b""):h.update(b)
    return h.hexdigest()

family_count=count_gz(families)
member_count=count_gz(members)
coverage_count=count_gz(coverage)
catalog_count=count_gz(catalog)

MODEL_RE=re.compile(r"\b(?:claude\s*(?:opus|sonnet|haiku)?\s*\d+(?:\.\d+)?|gpt[- ]?(?:3\.5|4(?:o|\.1)?|5(?:\.\d+)?)|gemini\s*(?:pro|flash|ultra)?\s*\d+(?:\.\d+)?|o[134](?:-mini)?)\b",re.I)
DANGER=re.compile(r"\b(?:ignore previous instructions|disable security|bypass (?:security|approval|validation)|skip (?:all )?tests|force[- ]push|git reset --hard|rm -rf|drop database|delete production)\b",re.I)
NEG=re.compile(r"\b(?:do not|don't|never|avoid|forbid|without approval|requires? approval)\b",re.I)

skill_count=0;invalid=0;dup_names=0;provider_remnants=0;unsafe=0;too_long=0
seen=set()
with tarfile.open(archive,"r:gz") as tar:
    for m in tar:
        if not m.isfile() or not m.name.endswith("/SKILL.md"):continue
        skill_count+=1
        data=tar.extractfile(m).read().decode("utf-8","replace")
        fm=re.match(r"^---\s*\n([\s\S]*?)\n---\s*\n",data)
        name="";desc=""
        if not fm:invalid+=1
        else:
            for line in fm.group(1).splitlines():
                z=re.match(r"^(name|description)\s*:\s*(.*)$",line)
                if z:
                    if z.group(1)=="name":name=z.group(2).strip()
                    else:desc=z.group(2).strip()
            if not name or not desc:invalid+=1
        if name:
            if name in seen:dup_names+=1
            seen.add(name)
        if MODEL_RE.search(data):provider_remnants+=1
        for line in data.splitlines():
            if DANGER.search(line) and not NEG.search(line):unsafe+=1;break
        if len(re.findall(r"\S+",data))>900:too_long+=1

checks={
 "source_gate_complete":source.get("hardGate",{}).get("usablePipelineAllowed") is True,
 "source_occurrences_complete":coverage_count==expected and int(stats.get("sourceOccurrences") or 0)==expected,
 "all_unique_hashes_retained":member_count==int(stats.get("uniqueContentHashes") or -1)==int(stats.get("familyMembers") or -2),
 "family_inventory_consistent":family_count==int(stats.get("families") or -1)==int(manifest.get("skills") or -2),
 "catalog_complete":catalog_count==family_count,
 "archive_skill_count_complete":skill_count==family_count,
 "invalid_skill_md_zero":invalid==0,
 "duplicate_skill_names_zero":dup_names==0,
 "active_deprecated_model_remnants_zero":provider_remnants==0,
 "active_unsafe_instruction_blocks_zero":unsafe==0,
 "oversized_skill_docs_zero":too_long==0
}
status="USABLE_FINAL" if all(checks.values()) else "BLOCKED_VALIDATION"
result={
 "generatedAt":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),"status":status,
 "checks":checks,
 "source":{"expectedRecords":expected,"coverageRows":coverage_count,"uniqueContentHashes":int(stats.get("uniqueContentHashes") or 0)},
 "resolution":{"families":family_count,"memberHashes":member_count,"catalogRows":catalog_count},
 "runtime":{"skills":skill_count,"invalidSkillMd":invalid,"duplicateNames":dup_names,"deprecatedModelRemnants":provider_remnants,"unsafeInstructionBlocks":unsafe,"oversizedSkillDocs":too_long},
 "integrity":{"archive":archive.name,"sha256":sha256_file(archive),"bytes":archive.stat().st_size},
 "policy":{"lostUniqueCapability":0 if checks["all_unique_hashes_retained"] else None,"lostSourceOccurrence":0 if checks["source_occurrences_complete"] else None,"unresolvedPolicyConflicts":0 if unsafe==0 else None}
}
OUT.parent.mkdir(parents=True,exist_ok=True);OUT.write_text(json.dumps(result,indent=2)+"\n",encoding="utf-8")
print(json.dumps(result,indent=2))
if status!="USABLE_FINAL":raise SystemExit(3)
