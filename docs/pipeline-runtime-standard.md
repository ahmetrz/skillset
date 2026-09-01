# Pipeline Runtime Standard

Every unattended pipeline stage defaults to the fastest measured configuration that
preserves correctness, security, and the free-tier cost boundary.

## Required execution pattern

- Claim work atomically with `FOR UPDATE SKIP LOCKED`.
- Commit claims before network or storage work; never hold database locks across I/O.
- Make writes idempotent and checkpoint every completed item.
- Reclaim stale claims automatically. Retry boundedly, then quarantine the single
  failing item without blocking the queue.
- Keep terminal failures, not-found items, oversize items, and conflicts separately
  observable. Never silently discard them.
- Stop only when open work and processing work are both zero and the integrity gate
  passes.

## Safe maximum tuning

1. Establish a live throughput and error-rate baseline.
2. Change only one control at a time: cadence, then concurrency, then batch size.
3. Measure each candidate under live load for at least one full observation window.
4. Keep a candidate only when throughput rises and correctness gates remain clean.
5. Roll back immediately when throughput falls, conflicts appear, or resource errors
   increase.
6. Persist the winning setting in a migration; do not leave a dashboard-only change.

## Default health gates

- Added cost: zero.
- Conflicts or integrity loss: zero.
- Unbounded retries: zero.
- Stale claims: automatically recovered.
- Resource-limit failures: bounded retry plus quarantine.
- User intervention: not required unless credentials, permissions, or an irreversible
  product decision blocks progress.

These rules apply to every subsequent acquisition, normalization, analysis, export,
and integrity stage unless a stricter stage-specific constraint is documented.

## Automatic stage transitions

The skills.sh completion chain runs inside Supabase and does not depend on an open
Codex session or a long-running GitHub runner:

1. Verify both archive passes are terminal. The exact-storage pass is authoritative
   when the redundant novel pass has only a manifest transport error.
2. Drain SDLC projection with its database capacity gate and four-attempt worker
   contract. A terminal batch receives one bounded rescue round, then blocks loudly.
3. Start evaluation in 5,000-content-hash deltas and drain each delta before opening
   the next one.
4. Pass integrity only when archive, projection, in-flight HTTP work, running
   evaluations, failed evaluations, and unevaluated canonical hashes are all clean.
5. Mark the pipeline completed and unschedule the controller automatically.

The controller is `public.skillset_skills_sh_completion_tick_v1()` and is scheduled
as `skills-sh-completion-autopilot-v1` every three seconds while work remains. Its
control table and RPCs are restricted to `service_role`.

## Unified canonical corpus autopilot

After the source pipelines pass their gates, the unified corpus continues entirely
inside Postgres. It does not require an open Codex session or a GitHub runner:

1. Union the accepted GitSkills and skills.sh content hashes through a
   storage-neutral view; do not duplicate the canonical corpus physically.
2. Expose every known source alias through a provenance view so source mapping
   remains queryable without copying the member tables.
3. Build and fingerprint all 256 deterministic hash-prefix export partitions.
4. Rank deterministic SDLC, global-quality, and social-media review candidates.
5. Pass integrity only when expected and actual canonical counts match, every
   canonical hash has provenance, export rows match the catalog, all partitions
   have fingerprints, and synthesis candidates exist.
6. Mark the run completed and unschedule the controller automatically.

The controller is `public.skillset_unified_corpus_tick_v1()` and is scheduled as
`unified-corpus-autopilot-v1` while work remains. Every physical write is idempotent;
canonical and provenance access are view-backed. The catalog, provenance, export manifest,
synthesis candidates, control table, and RPCs are restricted to `service_role`.
Social-media candidates are discovery inputs marked for domain review; they are not
treated as authored or approved skills without that review.

## Final synthesis continuation

Completion is a transition, not a reason to wait for a user message. When the unified
corpus gate passes, the final synthesis controller must start automatically and run
independently of an open Codex session:

1. Deduplicate the 2,200 ranked memberships to 1,594 canonical source skills.
2. Materialize projected database content and selected GitSkills archive content into
   hash-verified, provenance-preserving storage packs.
3. Compose one deterministic bundle for every ranked target/category pair. Bundles
   retain the original components and add a narrow routing `SKILL.md`; they do not
   silently rewrite or merge safety constraints.
4. Pass integrity only when every canonical source is materialized, every category
   bundle contains its expected component count, and every artifact has a digest.
5. Stop the cron automatically only after that gate passes. Bounded terminal failures
   block loudly rather than dropping candidates.

The controller is `public.skillset_unified_final_synthesis_tick_v1()` and the worker
is deployed in the retired `skills-sh-search-behavior-probe` function slot to remain
inside the existing free-plan function quota. The slot is JWT-protected; the cron JWT
is stored in Vault, not in repository files.
