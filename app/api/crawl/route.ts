import { NextRequest, NextResponse } from 'next/server';
import { discoverLeaderboardBatch, discoverSearchBatch, fetchContentBatch, runAutoBatch } from '../../../lib/crawler';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';
export const maxDuration = 60;

function isAuthorized(request: NextRequest) {
  const secret = process.env.CRAWL_SECRET;
  if (!secret) return false;
  const bearer = request.headers.get('authorization');
  const direct = request.headers.get('x-crawl-secret');
  return bearer === `Bearer ${secret}` || direct === secret;
}

export async function GET(request: NextRequest) {
  if (!isAuthorized(request)) {
    return NextResponse.json({ error: 'unauthorized' }, { status: 401 });
  }

  try {
    const mode = request.nextUrl.searchParams.get('mode') ?? 'auto';
    let result;

    if (mode === 'leaderboard') result = await discoverLeaderboardBatch();
    else if (mode === 'content') {
      const limit = Number(request.nextUrl.searchParams.get('limit') ?? '100');
      result = await fetchContentBatch(limit, 20);
    } else if (mode === 'search') {
      const batch = Number(request.nextUrl.searchParams.get('batch') ?? '8');
      result = await discoverSearchBatch(batch);
    } else if (mode === 'auto') result = await runAutoBatch();
    else return NextResponse.json({ error: 'invalid_mode' }, { status: 400 });

    return NextResponse.json({ ok: true, result, at: new Date().toISOString() });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : String(error),
        at: new Date().toISOString(),
      },
      { status: 500 },
    );
  }
}
