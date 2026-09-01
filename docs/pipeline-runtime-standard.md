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
