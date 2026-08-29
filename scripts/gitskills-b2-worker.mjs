import { createClient } from '@libsql/client';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { createHash } from 'node:crypto';
import { gzipSync } from 'node:zlib';

const HF_ROWS = 'https://datasets-server.huggingface.co/rows';
const PACK_SIZE = Math.max(1, Math.min(Number(process.env.GITSKILLS_PACK_SIZE || 12), 32));
const FETCH_CONCURRENCY = Math.max(1, Math.min(Number(process.env.GITSKILLS_FETCH_CONCURRENCY || 4), 8));
const MAX_ATTEMPTS = 8;
const LEGACY_DONE_SHARDS = 19132;
const LEGACY_REPRESENTATIVES = 1047750;

function need(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
}

const turso = createClient({ url: need('TURSO_DATABASE_URL'), authToken: need('TURSO_AUTH_TOKEN') });
const b2Endpoint = need('B2_ENDPOINT').replace(/\/$/, '');
const region = b2Endpoint.match(/s3[.-]([a-z0-9-]+)\.backblazeb2\.com/i)?.[1] || 'us-east-1';
const b2 = new S3Client({
  endpoint: b2Endpoint,
  region,
  forcePathStyle: true,
  credentials: { accessKeyId: need('B2_ACCESS_KEY_ID'), secretAccessKey: need('B2_SECRET_ACCESS_KEY') },
});
const bucket = need('B2_BUCKET');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => new Date().toISOString();
const sha256 = (buf) => createHash('sha256').update(buf).digest('hex');

async function retry(label, fn, attempts = 6) {
  let last;
  for (let i = 0; i < attempts; i++) {
    try { return await fn(); } catch (e) {
      last = e;
      if (i === attempts - 1) break;
      const delay = Math.min(1000 * (2 ** i), 15000) + Math.floor(Math.random() * 400);
      console.warn(JSON.stringify({ event: 'retry', label, attempt: i + 1, delay, error: String(e?.message || e).slice(0, 300) }));
      await sleep(delay);
    }
  }
  throw last;
}

async function ensureSchema() {
  await turso.batch([
    `CREATE TABLE IF NOT EXISTS gitskills_shards (
      shard_id INTEGER PRIMARY KEY,
      start_offset INTEGER NOT NULL,
      row_limit INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      attempts INTEGER NOT NULL DEFAULT 0,
      claimed_at TEXT,
      updated_at TEXT NOT NULL,
      rows_fetched INTEGER NOT NULL DEFAULT 0,
      storage_path TEXT,
      error TEXT
    )`,
    `CREATE INDEX IF NOT EXISTS idx_gitskills_shards_status ON gitskills_shards(status, shard_id)`,
    `CREATE TABLE IF NOT EXISTS gitskills_packs (
      path TEXT PRIMARY KEY,
      shard_count INTEGER NOT NULL,
      representatives INTEGER NOT NULL,
      bytes INTEGER NOT NULL,
      sha256 TEXT NOT NULL,
      created_at TEXT NOT NULL
    )`,
    `CREATE TABLE IF NOT EXISTS gitskills_state (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)`,
  ], 'write');
  const t = now();
  await turso.batch([
    { sql: `INSERT INTO gitskills_state(key,value,updated_at) VALUES('legacy_done_shards',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`, args: [String(LEGACY_DONE_SHARDS), t] },
    { sql: `INSERT INTO gitskills_state(key,value,updated_at) VALUES('legacy_representatives',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`, args: [String(LEGACY_REPRESENTATIVES), t] },
  ], 'write');
}

async function seedRemaining() {
  const count = await turso.execute(`SELECT COUNT(*) AS n FROM gitskills_shards`);
  if (Number(count.rows[0]?.n || 0) > 0) return;
  const ids = [119117, ...Array.from({ length: 31 }, (_, i) => 119132 + i), ...Array.from({ length: 18808 }, (_, i) => 119164 + i)];
  for (let i = 0; i < ids.length; i += 400) {
    const t = now();
    const statements = ids.slice(i, i + 400).map((id) => ({
      sql: `INSERT OR IGNORE INTO gitskills_shards(shard_id,start_offset,row_limit,status,updated_at) VALUES(?,?,?,'pending',?)`,
      args: [id, (id - 100000) * 100, id === 137971 ? 17 : 100, t],
    }));
    await retry('turso_seed', () => turso.batch(statements, 'write'));
  }
  console.log(JSON.stringify({ event: 'seeded', shards: ids.length }));
}

async function claim() {
  const stale = new Date(Date.now() - 15 * 60 * 1000).toISOString();
  await turso.execute({
    sql: `UPDATE gitskills_shards SET status='error', error='stale_processing', claimed_at=NULL, updated_at=? WHERE status='processing' AND claimed_at < ?`,
    args: [now(), stale],
  });
  const result = await turso.execute({
    sql: `UPDATE gitskills_shards
          SET status='processing', attempts=attempts+1, claimed_at=?, updated_at=?, error=NULL
          WHERE shard_id IN (
            SELECT shard_id FROM gitskills_shards
            WHERE status IN ('pending','error') AND attempts < ?
            ORDER BY shard_id LIMIT ?
          )
          RETURNING shard_id,start_offset,row_limit,attempts`,
    args: [now(), now(), MAX_ATTEMPTS, PACK_SIZE],
  });
  return result.rows.map((r) => ({ shard_id: Number(r.shard_id), start_offset: Number(r.start_offset), row_limit: Number(r.row_limit), attempts: Number(r.attempts) }));
}

async function fetchPage(shard) {
  const u = new URL(HF_ROWS);
  u.searchParams.set('dataset', 'mvaccargiu/gitskills');
  u.searchParams.set('config', 'artifacts');
  u.searchParams.set('split', 'train');
  u.searchParams.set('offset', String(shard.start_offset));
  u.searchParams.set('length', String(shard.row_limit));
  return retry(`hf_${shard.shard_id}`, async () => {
    const res = await fetch(u, { signal: AbortSignal.timeout(20000), headers: { 'user-agent': 'skillset-corpus-b2/1.0' } });
    if (!res.ok) throw new Error(`HF ${res.status}: ${(await res.text()).slice(0, 240)}`);
    const body = await res.json();
    const rows = body.rows || [];
    if (rows.length !== shard.row_limit) throw new Error(`scan_count_mismatch:${rows.length}/${shard.row_limit}`);
    return rows;
  }, 5);
}

function representatives(src) {
  const out = [];
  for (const x of src) {
    const r = x.row || {};
    if (r.dedup_primary !== 1 && r.dedup_primary !== true) continue;
    out.push({
      row_idx: x.row_idx, repo_full_name: r.repo_full_name, path: r.path, filename: r.filename,
      location_class: r.location_class, file_sha: r.file_sha, discovered_at: r.discovered_at,
      dedup_primary: r.dedup_primary, content: r.content, content_fetched: r.content_fetched,
      content_sha_ok: r.content_sha_ok, frontmatter_valid: r.frontmatter_valid, name: r.name,
      description: r.description, body_chars: r.body_chars, sibling_count: r.sibling_count,
      sibling_bytes: r.sibling_bytes, has_scripts: r.has_scripts, has_references: r.has_references,
      composition_fetched: r.composition_fetched, composition_truncated: r.composition_truncated,
    });
  }
  return out;
}

async function mapLimit(items, limit, fn) {
  let cursor = 0;
  const out = new Array(items.length);
  await Promise.all(Array.from({ length: Math.min(limit, items.length || 1) }, async () => {
    while (cursor < items.length) {
      const i = cursor++;
      try { out[i] = { ok: true, shard: items[i], rows: await fn(items[i]) }; }
      catch (e) { out[i] = { ok: false, shard: items[i], error: String(e?.message || e).slice(0, 1500) }; }
    }
  }));
  return out;
}

async function finishFailures(bad) {
  if (!bad.length) return;
  await turso.batch(bad.map((x) => ({
    sql: `UPDATE gitskills_shards SET status='error',claimed_at=NULL,updated_at=?,error=? WHERE shard_id=?`,
    args: [now(), x.error, x.shard.shard_id],
  })), 'write');
}

async function runOnePack() {
  const claimed = await retry('turso_claim', claim);
  if (!claimed.length) return { claimed: 0 };
  const settled = await mapLimit(claimed, FETCH_CONCURRENCY, fetchPage);
  const good = settled.filter((x) => x.ok).map((x) => ({ ...x, reps: representatives(x.rows) }));
  const bad = settled.filter((x) => !x.ok);
  await finishFailures(bad);
  if (!good.length) return { claimed: claimed.length, success: 0, failed: bad.length };

  const ids = good.map((x) => x.shard.shard_id);
  const payload = {
    version: 1,
    source: 'GitSkills',
    dataset: 'mvaccargiu/gitskills',
    config: 'artifacts',
    scan: 'all_rows_local_dedup_primary',
    exactContentIncluded: true,
    storage: 'backblaze-b2',
    generatedAt: now(),
    shards: good.map((x) => ({ shardId: x.shard.shard_id, startOffset: x.shard.start_offset, scannedRows: x.rows.length, representativeRows: x.reps.length, rows: x.reps })),
  };
  const gz = gzipSync(Buffer.from(JSON.stringify(payload)), { level: 9 });
  const digest = sha256(gz);
  const path = `gitskills/discovery-b2-v1/pack-${ids.join('-')}.json.gz`;
  await retry('b2_put', () => b2.send(new PutObjectCommand({ Bucket: bucket, Key: path, Body: gz, ContentType: 'application/gzip', Metadata: { sha256: digest } })), 6);

  const reps = good.reduce((n, x) => n + x.reps.length, 0);
  const t = now();
  const statements = [
    { sql: `INSERT INTO gitskills_packs(path,shard_count,representatives,bytes,sha256,created_at) VALUES(?,?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET shard_count=excluded.shard_count,representatives=excluded.representatives,bytes=excluded.bytes,sha256=excluded.sha256`, args: [path, good.length, reps, gz.length, digest, t] },
    ...good.map((x) => ({ sql: `UPDATE gitskills_shards SET status='done',rows_fetched=?,storage_path=?,error=NULL,claimed_at=NULL,updated_at=? WHERE shard_id=?`, args: [x.reps.length, path, t, x.shard.shard_id] })),
  ];
  await retry('turso_finish', () => turso.batch(statements, 'write'));
  return { claimed: claimed.length, success: good.length, failed: bad.length, representatives: reps, bytes: gz.length, path, sha256: digest };
}

async function status() {
  const q = await turso.execute(`SELECT status,COUNT(*) AS n,COALESCE(SUM(rows_fetched),0) AS reps FROM gitskills_shards GROUP BY status ORDER BY status`);
  const packs = await turso.execute(`SELECT COUNT(*) AS n,COALESCE(SUM(representatives),0) AS reps,COALESCE(SUM(bytes),0) AS bytes FROM gitskills_packs`);
  const terminal = await turso.execute({ sql: `SELECT COUNT(*) AS n FROM gitskills_shards WHERE status<>'done' AND attempts>=?`, args: [MAX_ATTEMPTS] });
  return { legacy: { doneShards: LEGACY_DONE_SHARDS, representatives: LEGACY_REPRESENTATIVES }, queue: q.rows, packs: packs.rows[0], terminal: Number(terminal.rows[0]?.n || 0) };
}

async function main() {
  await ensureSchema();
  await seedRemaining();
  const mode = process.argv.includes('--status') ? 'status' : 'run';
  if (mode === 'status') { console.log(JSON.stringify({ event: 'status', ...(await status()) })); return; }
  const packs = Math.max(1, Math.min(Number(process.env.GITSKILLS_PACKS_PER_RUN || 1), 200));
  for (let i = 0; i < packs; i++) {
    const result = await runOnePack();
    console.log(JSON.stringify({ event: 'pack', index: i + 1, ...result }));
    if (!result.claimed) break;
  }
  console.log(JSON.stringify({ event: 'status', ...(await status()) }));
}

main().catch((e) => { console.error(JSON.stringify({ event: 'fatal', error: String(e?.stack || e) })); process.exit(1); });
