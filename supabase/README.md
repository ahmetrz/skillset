# Skillset crawler runtime

This directory mirrors the crawler currently running on Supabase.

## Discovery layers

1. Mastra historical snapshot seed (`skillset-seed-mastra`)
2. skills.sh public all-time leaderboard pagination (`skillset-leaderboard-seed`)
3. skills.sh owner search expansion (`skillset-owner-search`)
4. recursive GitHub repository discovery of every `SKILL.md` (`skillset-repo-discovery`)
5. skills.sh download/content retrieval (`skillset-content-batch`)
6. gzip + SHA-256 content-addressed storage (`skillset-migrate-storage`)

The sources deliberately overlap. Coverage is built by unioning independent discovery paths rather than treating one API as authoritative.

## Content storage

Full `SKILL.md` text is not intended to remain inline in Postgres. Content is SHA-256 hashed, gzip-compressed, and stored in the private `skillset-corpus` Storage bucket under:

`sha256/<first-two-hash-chars>/<sha256>.md.gz`

Rows in `skillset.skills` retain metadata, hash, raw/compressed sizes, and the object key. Duplicate content therefore uses one object.

A database-side capacity guard stops content/repository workers before estimated object storage reaches 850 MB.

## Scheduling

The production runtime currently uses pg_cron:

- storage migration: every minute
- content batch: every minute
- repository discovery: every 3 minutes
- leaderboard seed: every minute until complete
- owner search: every minute

Calls are made through `pg_net`. Each trigger creates a short-lived single-use token in `skillset.job_tokens`; Edge Functions consume the token before doing work. No application secret is committed here.

## Retry policy

Content retrieval is bounded. `retrieval_attempts` is incremented atomically when a row is claimed.

- HTTP 404: fail fast; repository discovery may still recover the skill independently.
- HTTP 429: delayed retry, capped at six attempts.
- HTTP 5xx/timeouts: delayed retry, capped at three attempts.
- missing `SKILL.md` in a download snapshot: two attempts.

This prevents an unreachable record from cycling forever through the queue.

## Important coverage note

`/api/skills/all-time/{page}` is a public leaderboard endpoint, not proof of the entire skills ecosystem. Public search ignores `page`/`offset`. The crawler therefore combines leaderboard, owner expansion, historical snapshot, and repository-level discovery. A corpus count must not be described as complete unless independent coverage evidence supports that claim.
