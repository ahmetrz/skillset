# Final Audit External Architecture v1

Status: ACTIVE on `gitskills-migration-run`

## Goal

Complete the final corpus audit without using Supabase PostgreSQL as the working database and without requiring a paid Supabase upgrade.

Primary invariants:

- no source-content deletion during audit;
- exact provenance remains reconstructable;
- no automatic merge/drop for near-duplicate or coverage candidates;
- no final master build before all quality gates pass;
- conflict recommendations may be generated automatically, but user selections remain blank;
- no paid infrastructure is required by this pipeline.

## Frozen audit input

The current audit run is scoped to the completed acquisition snapshot:

- GitSkills legacy HuggingFace shards: 19,132 units, source rows corresponding to the earlier 1,047,750 legacy representatives.
- GitSkills Backblaze B2 exact packs: 642 objects under `gitskills/discovery-b2-v2/`.
- skills.sh Backblaze B2 exact packs: 7,557 objects under `skills-sh/exact-b2-v1/`.

The broader B2 bucket contains other GitSkills objects, so generic `gitskills/` object count must not be used as the audit-pack expectation.

## Storage and compute

### GitHub Actions

Public-repository standard runners provide the compute layer. The ingest and policy passes are checkpointed and partitioned.

### Turso

Turso stores only compact working state:

- exact raw-content hash;
- SimHash and LSH chunks;
- scope/risk/provider masks;
- compact policy signature;
- occurrence/provenance locator;
- per-unit checkpoint;
- near-duplicate/coverage candidates;
- reducer state;
- numeric/tool policy facts.

Full source text is not copied into Turso.

### Backblaze B2

B2 remains the immutable exact-content source for:

- new GitSkills exact packs;
- skills.sh exact packs.

The audit pass reads these objects in place. It does not duplicate the raw corpus.

### HuggingFace

The legacy GitSkills segment is streamed directly from `mvaccargiu/gitskills` using its deterministic shard offsets. Raw legacy text is not duplicated into B2.

### Supabase

Supabase is no longer the final-audit working database. Existing corpus/source data remain untouched as a recovery/reference source. Completed Supabase-dependent scheduled workflows are parked; their manual dispatch paths remain available.

## Pipeline

### 1. Full-text ingest

Script: `scripts/final-audit-external-v1.mjs`

Workflow: `.github/workflows/final-corpus-external-audit-v1.yml`

Partitions:

- 8 legacy HuggingFace workers;
- 4 GitSkills B2 workers;
- 4 skills.sh B2 workers.

Each unit is idempotent. A unit is skipped only after its Turso checkpoint is `done`.

Outputs include exact hashes, occurrence locators, SimHash, SDLC/social scope masks, risk flags and compact non-numeric policy signatures.

### 2. Numeric/tool policy rescan

Script: `scripts/final-audit-policy-rescan-v2.mjs`

Workflow: `.github/workflows/final-corpus-policy-rescan-v2.yml`

This is a separate complete text pass so that fixed retry/timeout/coverage/score/threshold rules and tool preference/ban rules cannot be lost because the initial ingest already checkpointed a unit.

### 3. Deduplication and conflict reduction

Script: `scripts/final-audit-reduce-v1.mjs`

Workflow: `.github/workflows/final-corpus-external-reduce-v1.yml`

The reducer runs fail-closed:

- it waits for both full-text passes;
- it computes exact-duplicate counts;
- it generates only candidate classifications for near duplicates and coverage relations;
- it does not delete or merge source skills;
- oversized LSH groups remain unresolved instead of being silently dropped;
- Conflict Registry is complete only after the policy rescan is complete;
- every conflict keeps `yourSelection` empty.

### 4. Final validation gate

Script: `scripts/final-audit-validation-v1.mjs`

Workflow: `.github/workflows/final-audit-validation-v1.yml`

Master build is blocked unless all of the following are true:

1. full-text ingest complete;
2. numeric/tool policy rescan complete;
3. near-candidate pass complete;
4. oversized LSH groups resolved;
5. Conflict Registry complete;
6. all conflict selections supplied by the user.

Until then, `destructive_master_build_allowed=false`.

## Retry behavior

Failed units are not marked complete. Scheduled successor runs retry only non-`done` units.

A checkpoint is progress, not completion.

## Cost guard

The audit workers do not copy the legacy corpus into B2 and do not write full source text into Turso. Repeated runs skip completed units. Old Supabase-dependent scheduled runners are disabled, preventing unnecessary failed traffic against the full Supabase database.

## Files produced in the branch

- `audit/external-status.json`
- `audit/final-audit-ingest-status.json`
- `audit/final-audit-reduce-status.json`
- `audit/conflict-registry-preliminary.json`
- `audit/final-validation-status.json`

The final master set is intentionally not produced until the Conflict Registry reaches the user-decision gate.
