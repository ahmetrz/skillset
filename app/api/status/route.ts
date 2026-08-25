import { NextResponse } from 'next/server';
import { corpusStats, db, ensureSchema, getState } from '../../../lib/db';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET() {
  try {
    await ensureSchema();
    const [stats, leaderboardComplete, leaderboardPage, leaderboardTotal, searchPending, searchComplete, repoPending] = await Promise.all([
      corpusStats(),
      getState('leaderboard_complete'),
      getState('leaderboard_page'),
      getState('leaderboard_total_reported'),
      db().execute(`SELECT COUNT(*) AS n FROM search_queries WHERE status = 'pending'`).catch(() => ({ rows: [{ n: 0 }] } as any)),
      db().execute(`SELECT COUNT(*) AS n FROM search_queries WHERE status = 'complete'`).catch(() => ({ rows: [{ n: 0 }] } as any)),
      db().execute(`SELECT COUNT(*) AS n FROM repositories WHERE expansion_status = 'pending'`).catch(() => ({ rows: [{ n: 0 }] } as any)),
    ]);

    return NextResponse.json({
      ok: true,
      stats,
      discovery: {
        leaderboardComplete: leaderboardComplete === 'true',
        leaderboardPage: Number(leaderboardPage ?? 0),
        leaderboardTotalReported: Number(leaderboardTotal ?? 0),
        searchQueriesPending: Number(searchPending.rows[0]?.n ?? 0),
        searchQueriesComplete: Number(searchComplete.rows[0]?.n ?? 0),
        repositoriesPendingExpansion: Number(repoPending.rows[0]?.n ?? 0),
      },
      at: new Date().toISOString(),
    });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : String(error) },
      { status: 500 },
    );
  }
}
