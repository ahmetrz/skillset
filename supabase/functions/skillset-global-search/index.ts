import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

Deno.serve(async (req) => {
  const su = Deno.env.get('SUPABASE_URL');
  const sk = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');
  if (!su || !sk) return new Response('config', { status: 500 });
  const db = createClient(su, sk, { auth: { persistSession: false } });
  let body: any = {};
  try { body = await req.json(); } catch {}
  const { data: auth, error: ae } = await db.rpc('skillset_consume_job_token', {
    p_token: body.token ?? '', p_purpose: 'global-search'
  });
  if (ae || auth !== true) return new Response('Unauthorized', { status: 401 });

  const limit = Math.max(1, Math.min(Number(body.limit ?? 25), 100));
  const { data: jobs, error: ce } = await db.rpc('skillset_claim_global_search', { p_limit: limit });
  if (ce) return Response.json({ ok: false, error: ce.message }, { status: 500 });
  const rows = jobs ?? [];

  let ok = 0, failed = 0, saturated = 0, totalResults = 0;
  const chunks: any[][] = [];
  for (let i = 0; i < rows.length; i += 5) chunks.push(rows.slice(i, i + 5));

  for (const chunk of chunks) {
    await Promise.all(chunk.map(async (job: any) => {
      const token = String(job.token);
      try {
        const url = `https://skills.sh/api/search?q=${encodeURIComponent(token)}&limit=200`;
        const r = await fetch(url, { headers: { 'user-agent': 'skillset-corpus/1.0' }, signal: AbortSignal.timeout(15000) });
        if (!r.ok) throw new Error(`search_http_${r.status}`);
        const j = await r.json();
        const skills = Array.isArray(j?.skills) ? j.skills : [];
        totalResults += skills.length;
        if (skills.length >= 200) saturated++;
        const { error: se } = await db.rpc('skillset_seed_search_results', { p_rows: skills });
        if (se) throw new Error(`seed:${se.message}`);
        const { error: fe } = await db.rpc('skillset_finish_global_search', { p_token: token, p_count: skills.length, p_error: null });
        if (fe) throw new Error(`finish:${fe.message}`);
        ok++;
      } catch (e) {
        const m = e instanceof Error ? e.message : String(e);
        await db.rpc('skillset_finish_global_search', { p_token: token, p_count: 0, p_error: m });
        failed++;
      }
    }));
  }

  return Response.json({ ok: true, processed: rows.length, okCount: ok, failedCount: failed, saturatedCount: saturated, totalResults });
});
