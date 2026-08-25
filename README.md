# Skillset Corpus

A zero-cost-first pipeline for discovering, fetching, deduplicating, and analyzing public AI agent skills and their `SKILL.md` files.

## Goal

Build a verifiable corpus of publicly accessible skills referenced by skills.sh and related public GitHub repositories, with resumable crawling, compressed content storage, and content-hash deduplication.

## Architecture

1. **skills.sh leaderboard discovery** — official `/api/v1/skills` with Vercel OIDC.
2. **Adaptive search expansion** — systematically queries the official search index and recursively splits saturated query buckets.
3. **SKILL.md ingestion** — official detail endpoint first; legacy public download endpoint as fallback.
4. **Persistent storage** — Turso/libSQL. `SKILL.md` is gzip-compressed before storage.
5. **Resumable state** — every phase stores its cursor/status in the database.
6. **Execution loop** — GitHub Actions repeatedly triggers the Vercel crawler endpoint while staying within a zero-cost-first schedule.

## Required configuration

### Vercel

Connect this repository to a Vercel project and enable **Settings → OIDC Federation**.

Set these environment variables:

- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`
- `CRAWL_SECRET`

### GitHub repository settings

Create repository variable:

- `CRAWL_URL` = production Vercel URL, for example `https://skillset.example.vercel.app`

Create repository secret:

- `CRAWL_SECRET` = the same random secret configured on Vercel

## Endpoints

- `GET /api/crawl?mode=auto` — run the next resumable batch. Requires `Authorization: Bearer <CRAWL_SECRET>`.
- `GET /api/crawl?mode=leaderboard` — ingest one leaderboard page.
- `GET /api/crawl?mode=content&limit=100` — fetch pending `SKILL.md` files.
- `GET /api/crawl?mode=search&batch=8` — process adaptive search-discovery queries.
- `GET /api/status` — corpus counts, compressed/raw size, discovery progress, and queue state.

## Important completeness rule

The skills.sh API exposes a leaderboard and ranked search, but does not currently expose a documented endpoint that enumerates every one of the 600k+ searchable skills. Therefore the project treats `leaderboard total == fetched` only as a **baseline**, not as proof of full-catalog completeness.

The crawler uses multiple discovery channels and tracks saturated search buckets. Repository-level recursive discovery is the next coverage layer before the corpus can be declared complete.

## Branch

Current implementation work is on `feature/full-skill-corpus` until deployment and database tests pass.
