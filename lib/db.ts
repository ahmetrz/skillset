import { createClient, type Client } from '@libsql/client';
import { gzipSync } from 'node:zlib';

let client: Client | null = null;

export function db(): Client {
  if (client) return client;

  const url = process.env.TURSO_DATABASE_URL;
  const authToken = process.env.TURSO_AUTH_TOKEN;

  if (!url || !authToken) {
    throw new Error('TURSO_DATABASE_URL and TURSO_AUTH_TOKEN are required');
  }

  client = createClient({ url, authToken });
  return client;
}

export async function ensureSchema() {
  const c = db();
  await c.batch([
    `CREATE TABLE IF NOT EXISTS skills (
      id TEXT PRIMARY KEY,
      source TEXT NOT NULL,
      slug TEXT NOT NULL,
      name TEXT,
      installs INTEGER NOT NULL DEFAULT 0,
      source_type TEXT,
      install_url TEXT,
      page_url TEXT,
      is_duplicate INTEGER NOT NULL DEFAULT 0,
      content_hash TEXT,
      skill_md_gzip BLOB,
      content_bytes INTEGER,
      compressed_bytes INTEGER,
      retrieval_status TEXT NOT NULL DEFAULT 'discovered',
      discovery_source TEXT,
      source_path TEXT,
      first_seen TEXT NOT NULL,
      last_seen TEXT NOT NULL,
      retrieved_at TEXT,
      error TEXT
    )`,
    `CREATE INDEX IF NOT EXISTS idx_skills_status ON skills(retrieval_status)`,
    `CREATE INDEX IF NOT EXISTS idx_skills_source ON skills(source)`,
    `CREATE INDEX IF NOT EXISTS idx_skills_hash ON skills(content_hash)`,
    `CREATE TABLE IF NOT EXISTS repositories (
      source TEXT PRIMARY KEY,
      owner TEXT,
      repo TEXT,
      first_seen TEXT NOT NULL,
      last_seen TEXT NOT NULL,
      expansion_status TEXT NOT NULL DEFAULT 'pending',
      expanded_at TEXT,
      discovered_skill_count INTEGER NOT NULL DEFAULT 0,
      error TEXT
    )`,
    `CREATE TABLE IF NOT EXISTS crawl_state (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )`,
  ], 'write');
}

export type DiscoveredSkill = {
  id: string;
  source: string;
  slug: string;
  name?: string | null;
  installs?: number | null;
  sourceType?: string | null;
  installUrl?: string | null;
  url?: string | null;
  isDuplicate?: boolean | null;
};

export async function upsertDiscoveredSkill(skill: DiscoveredSkill, discoverySource: string) {
  const now = new Date().toISOString();
  const c = db();
  await c.execute({
    sql: `INSERT INTO skills (
      id, source, slug, name, installs, source_type, install_url, page_url,
      is_duplicate, retrieval_status, discovery_source, first_seen, last_seen
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'discovered', ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      name = COALESCE(excluded.name, skills.name),
      installs = MAX(skills.installs, excluded.installs),
      source_type = COALESCE(excluded.source_type, skills.source_type),
      install_url = COALESCE(excluded.install_url, skills.install_url),
      page_url = COALESCE(excluded.page_url, skills.page_url),
      is_duplicate = MAX(skills.is_duplicate, excluded.is_duplicate),
      last_seen = excluded.last_seen`,
    args: [
      skill.id,
      skill.source,
      skill.slug,
      skill.name ?? null,
      skill.installs ?? 0,
      skill.sourceType ?? null,
      skill.installUrl ?? null,
      skill.url ?? null,
      skill.isDuplicate ? 1 : 0,
      discoverySource,
      now,
      now,
    ],
  });

  if (skill.sourceType === 'github' || /^[-\w.]+\/[-\w.]+$/.test(skill.source)) {
    const [owner, repo] = skill.source.split('/');
    await c.execute({
      sql: `INSERT INTO repositories (source, owner, repo, first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(source) DO UPDATE SET last_seen = excluded.last_seen`,
      args: [skill.source, owner ?? null, repo ?? null, now, now],
    });
  }
}

export async function storeSkillContent(params: {
  id: string;
  hash?: string | null;
  skillMd: string;
  sourcePath?: string | null;
}) {
  const compressed = gzipSync(Buffer.from(params.skillMd, 'utf8'), { level: 9 });
  const now = new Date().toISOString();
  await db().execute({
    sql: `UPDATE skills SET
      content_hash = ?,
      skill_md_gzip = ?,
      content_bytes = ?,
      compressed_bytes = ?,
      source_path = ?,
      retrieval_status = 'complete',
      retrieved_at = ?,
      error = NULL,
      last_seen = ?
      WHERE id = ?`,
    args: [
      params.hash ?? null,
      compressed,
      Buffer.byteLength(params.skillMd, 'utf8'),
      compressed.byteLength,
      params.sourcePath ?? 'SKILL.md',
      now,
      now,
      params.id,
    ],
  });
}

export async function markSkillError(id: string, error: unknown) {
  const message = error instanceof Error ? error.message : String(error);
  await db().execute({
    sql: `UPDATE skills SET retrieval_status = 'error', error = ?, last_seen = ? WHERE id = ?`,
    args: [message.slice(0, 2000), new Date().toISOString(), id],
  });
}

export async function getState(key: string): Promise<string | null> {
  const result = await db().execute({ sql: 'SELECT value FROM crawl_state WHERE key = ?', args: [key] });
  return result.rows[0]?.value?.toString() ?? null;
}

export async function setState(key: string, value: string) {
  const now = new Date().toISOString();
  await db().execute({
    sql: `INSERT INTO crawl_state (key, value, updated_at) VALUES (?, ?, ?)
          ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
    args: [key, value, now],
  });
}

export async function corpusStats() {
  const result = await db().execute(`SELECT
    COUNT(*) AS discovered,
    SUM(CASE WHEN retrieval_status = 'complete' THEN 1 ELSE 0 END) AS complete,
    SUM(CASE WHEN retrieval_status = 'error' THEN 1 ELSE 0 END) AS errors,
    SUM(CASE WHEN is_duplicate = 1 THEN 1 ELSE 0 END) AS flagged_duplicates,
    COUNT(DISTINCT CASE WHEN content_hash IS NOT NULL THEN content_hash END) AS unique_hashes,
    COALESCE(SUM(content_bytes), 0) AS raw_bytes,
    COALESCE(SUM(compressed_bytes), 0) AS compressed_bytes
  FROM skills`);
  return result.rows[0] ?? {};
}
