import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const SNAPSHOT_URL = 'https://raw.githubusercontent.com/mastra-ai/skills-api/main/src/registry/scraped-skills.json';

Deno.serve(async (req) => {
  const supabaseUrl = Deno.env.get('SUPABASE_URL');
  const serviceRole = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');
  if (!supabaseUrl || !serviceRole) return new Response('Server configuration error', { status: 500 });
  const client = createClient(supabaseUrl, serviceRole, { auth: { persistSession: false } });

  let body: { token?: string } = {};
  try { body = await req.json(); } catch { /* invalid body handled below */ }
  const { data: authorized, error: authError } = await client.rpc('skillset_consume_job_token', { p_token: body.token ?? '', p_purpose: 'seed-mastra' });
  if (authError || authorized !== true) return new Response('Unauthorized', { status: 401 });

  try {
    const response = await fetch(SNAPSHOT_URL, { headers: { 'user-agent': 'skillset-corpus/1.0' } });
    if (!response.ok) throw new Error(`snapshot_fetch_failed:${response.status}`);
    const data = await response.json();
    const skills = Array.isArray(data?.skills) ? data.skills : [];
    await client.rpc('skillset_seed_status', { p_source: 'mastra-skills-api', p_total: skills.length, p_processed: 0, p_status: 'running' });

    const chunkSize = 500;
    let processed = 0;
    for (let i = 0; i < skills.length; i += chunkSize) {
      const chunk = skills.slice(i, i + chunkSize);
      const { error } = await client.rpc('skillset_bulk_seed', { p_rows: chunk });
      if (error) throw new Error(`bulk_seed_failed:${error.message}`);
      processed += chunk.length;
      if (processed % 2500 === 0 || processed === skills.length) {
        await client.rpc('skillset_seed_status', { p_source: 'mastra-skills-api', p_total: skills.length, p_processed: processed, p_status: 'running' });
      }
    }

    await client.rpc('skillset_seed_status', { p_source: 'mastra-skills-api', p_total: skills.length, p_processed: processed, p_status: 'complete' });
    return new Response(JSON.stringify({ ok: true, total: skills.length, processed }), { headers: { 'content-type': 'application/json' } });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await client.rpc('skillset_seed_status', { p_source: 'mastra-skills-api', p_total: 0, p_processed: 0, p_status: `error:${message.slice(0,300)}` });
    return new Response(JSON.stringify({ ok: false, error: message }), { status: 500, headers: { 'content-type': 'application/json' } });
  }
});