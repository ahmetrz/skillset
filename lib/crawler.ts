import { db, ensureSchema, getState, markSkillError, setState, storeSkillContent, upsertDiscoveredSkill } from './db';
import { fetchLeaderboardPage, fetchLegacyDownload, fetchSkillDetail, findSkillMd, searchSkills } from './skills-api';

const SEARCH_CHARS = 'abcdefghijklmnopqrstuvwxyz0123456789-';

async function ensureSearchQueue() {
  await db().execute(`CREATE TABLE IF NOT EXISTS search_queries (
    query TEXT PRIMARY KEY,
    depth INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    result_count INTEGER,
    processed_at TEXT,
    error TEXT
  )`);

  const count = await db().execute(`SELECT COUNT(*) AS n FROM search_queries`);
  if (Number(count.rows[0]?.n ?? 0) > 0) return;

  const statements = [];
  for (const a of SEARCH_CHARS) {
    for (const b of SEARCH_CHARS) {
      statements.push({
        sql: `INSERT OR IGNORE INTO search_queries(query, depth) VALUES (?, 2)`,
        args: [`${a}${b}`],
      });
    }
  }
  for (let i = 0; i < statements.length; i += 100) {
    await db().batch(statements.slice(i, i + 100), 'write');
  }
}

export async function discoverLeaderboardBatch() {
  await ensureSchema();
  const page = Number((await getState('leaderboard_page')) ?? '0');
  const response = await fetchLeaderboardPage(page, 500);

  for (const skill of response.data) {
    await upsertDiscoveredSkill(skill, 'skills.sh:leaderboard');
  }

  await setState('leaderboard_total_reported', String(response.pagination.total));
  await setState('leaderboard_page', String(page + 1));
  if (!response.pagination.hasMore) await setState('leaderboard_complete', 'true');

  return {
    phase: 'leaderboard',
    page,
    fetched: response.data.length,
    totalReported: response.pagination.total,
    hasMore: response.pagination.hasMore,
  };
}

async function fetchOneSkill(id: string) {
  try {
    let detail;
    try {
      detail = await fetchSkillDetail(id);
    } catch {
      detail = await fetchLegacyDownload(id);
    }

    const file = findSkillMd(detail.files);
    if (!file) throw new Error('SKILL.md not present in snapshot');

    await storeSkillContent({
      id,
      hash: detail.hash ?? null,
      skillMd: file.contents,
      sourcePath: file.path,
    });
    return { ok: true as const, id };
  } catch (error) {
    await markSkillError(id, error);
    return { ok: false as const, id, error: error instanceof Error ? error.message : String(error) };
  }
}

export async function fetchContentBatch(limit = 100, concurrency = 20) {
  await ensureSchema();
  const pending = await db().execute({
    sql: `SELECT id FROM skills WHERE retrieval_status = 'discovered' ORDER BY installs DESC, id LIMIT ?`,
    args: [Math.max(1, Math.min(limit, 500))],
  });
  const ids = pending.rows.map((row) => String(row.id));

  let cursor = 0;
  const results: Awaited<ReturnType<typeof fetchOneSkill>>[] = [];
  const workers = Array.from({ length: Math.min(concurrency, ids.length || 1) }, async () => {
    while (cursor < ids.length) {
      const index = cursor++;
      results.push(await fetchOneSkill(ids[index]!));
    }
  });
  await Promise.all(workers);

  return {
    phase: 'content',
    attempted: ids.length,
    complete: results.filter((r) => r.ok).length,
    errors: results.filter((r) => !r.ok).length,
  };
}

export async function discoverSearchBatch(batchSize = 8) {
  await ensureSchema();
  await ensureSearchQueue();

  const rows = await db().execute({
    sql: `SELECT query, depth FROM search_queries WHERE status = 'pending' ORDER BY depth, query LIMIT ?`,
    args: [Math.max(1, Math.min(batchSize, 25))],
  });

  let discovered = 0;
  let saturated = 0;

  for (const row of rows.rows) {
    const query = String(row.query);
    const depth = Number(row.depth);
    try {
      const response = await searchSkills(query, 200);
      for (const skill of response.data) {
        await upsertDiscoveredSkill(skill, `skills.sh:search:${query}`);
      }
      discovered += response.data.length;

      if (response.data.length >= 200 && depth < 4) {
        saturated++;
        const children = [...SEARCH_CHARS].map((char) => ({
          sql: `INSERT OR IGNORE INTO search_queries(query, depth) VALUES (?, ?)`,
          args: [`${query}${char}`, depth + 1],
        }));
        await db().batch(children, 'write');
      }

      await db().execute({
        sql: `UPDATE search_queries SET status = 'complete', result_count = ?, processed_at = ?, error = NULL WHERE query = ?`,
        args: [response.data.length, new Date().toISOString(), query],
      });
    } catch (error) {
      await db().execute({
        sql: `UPDATE search_queries SET status = 'error', processed_at = ?, error = ? WHERE query = ?`,
        args: [new Date().toISOString(), error instanceof Error ? error.message.slice(0, 1000) : String(error).slice(0, 1000), query],
      });
    }
  }

  const remaining = await db().execute(`SELECT COUNT(*) AS n FROM search_queries WHERE status = 'pending'`);
  return {
    phase: 'search-expansion',
    queriesProcessed: rows.rows.length,
    resultRowsSeen: discovered,
    saturatedQueries: saturated,
    pendingQueries: Number(remaining.rows[0]?.n ?? 0),
  };
}

export async function runAutoBatch() {
  await ensureSchema();
  const leaderboardComplete = (await getState('leaderboard_complete')) === 'true';
  if (!leaderboardComplete) return discoverLeaderboardBatch();

  const pending = await db().execute(`SELECT COUNT(*) AS n FROM skills WHERE retrieval_status = 'discovered'`);
  if (Number(pending.rows[0]?.n ?? 0) > 0) return fetchContentBatch(100, 20);

  return discoverSearchBatch(8);
}
