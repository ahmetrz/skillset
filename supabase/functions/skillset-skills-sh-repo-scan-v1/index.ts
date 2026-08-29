import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SEARCH_BASE = "https://tessera-cyan-eta.vercel.app/api/skills";
const BUCKET = "skills-sh-repo-scan-v1";
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function sha256Hex(bytes: Uint8Array) {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), (x) => x.toString(16).padStart(2, "0")).join("");
}

async function gzipBytes(text: string) {
  const input = new TextEncoder().encode(text);
  const stream = new Blob([input]).stream().pipeThrough(new CompressionStream("gzip"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

async function gunzipJson(blob: Blob) {
  const stream = blob.stream().pipeThrough(new DecompressionStream("gzip"));
  return JSON.parse(await new Response(stream).text());
}

async function queryRepo(owner: string, repo: string, attempt: number) {
  const primary = repo && repo.length >= 2 ? repo : owner;
  const queries = [primary, ...(primary !== owner ? [owner] : [])];
  const startedAt = Date.now();
  let last: any = null;

  for (const query of queries) {
    const url = new URL(SEARCH_BASE);
    url.searchParams.set("mode", "search");
    url.searchParams.set("q", query);
    url.searchParams.set("limit", "200");
    url.searchParams.set("owner", owner);
    try {
      const response = await fetch(url, {
        headers: { Accept: "application/json" },
        signal: AbortSignal.timeout(30_000),
      });
      const text = await response.text();
      let body: any = null;
      try {
        body = JSON.parse(text);
      } catch {
        // The structured error below retains a bounded response excerpt.
      }
      if (!response.ok || !body) {
        last = {
          owner,
          repo,
          query,
          status: response.status,
          error: body?.error || text.slice(0, 500),
          durationMs: Date.now() - startedAt,
          matches: [],
          upstreamCount: 0,
          saturated: false,
          attempt,
        };
        if (response.status === 400 && query !== owner) continue;
        return last;
      }
      const data = Array.isArray(body.data) ? body.data : [];
      const target = `${owner}/${repo}`.toLowerCase();
      const matches = data.filter((x: any) => String(x?.source || "").toLowerCase() === target);
      const upstreamCount = Number(body.count ?? data.length ?? 0);
      return {
        owner,
        repo,
        query,
        status: response.status,
        durationMs: Date.now() - startedAt,
        upstreamCount,
        saturated: upstreamCount >= 200,
        attempt,
        matches: matches.map((x: any) => ({
          id: x.id,
          slug: x.slug,
          name: x.name,
          source: x.source,
          installs: x.installs,
          sourceType: x.sourceType,
          installUrl: x.installUrl,
          url: x.url,
          isDuplicate: x.isDuplicate === true,
        })),
      };
    } catch (error) {
      last = {
        owner,
        repo,
        query,
        status: 0,
        error: String(error),
        durationMs: Date.now() - startedAt,
        matches: [],
        upstreamCount: 0,
        saturated: false,
        attempt,
      };
    }
  }
  return last || {
    owner,
    repo,
    query: primary,
    status: 0,
    error: "no_query_result",
    durationMs: Date.now() - startedAt,
    matches: [],
    upstreamCount: 0,
    saturated: false,
    attempt,
  };
}

async function searchRepo(owner: string, repo: string) {
  let last: any = null;
  for (let attempt = 1; attempt <= 4; attempt++) {
    last = await queryRepo(owner, repo, attempt);
    if (last.status === 200) return last;
    const retryable = last.status === 0 || last.status === 408 || last.status === 429 || last.status >= 500;
    if (!retryable) break;
    if (attempt < 4) await sleep(200 * 2 ** (attempt - 1) + Math.floor(Math.random() * 250));
  }
  return last;
}

async function mapLimit<T, R>(items: T[], limit: number, fn: (item: T) => Promise<R>) {
  const output = new Array<R>(items.length);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length || 1) }, async () => {
    while (true) {
      const index = next++;
      if (index >= items.length) return;
      output[index] = await fn(items[index]);
    }
  }));
  return output;
}

Deno.serve(async () => {
  const sb = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
    { auth: { persistSession: false } },
  );
  let batch: any = null;
  try {
    const { data: claims, error: claimError } = await sb.rpc("skillset_skills_sh_repo_scan_claim_v1");
    if (claimError) return Response.json({ ok: false, stage: "claim", error: claimError.message }, { status: 500 });
    if (!claims?.length) return Response.json({ ok: true, version: 5, done: true, claimed: 0 });
    batch = claims[0];

    const [{ data: repos, error: reposError }, { data: state, error: stateError }] = await Promise.all([
      sb.rpc("skillset_skills_sh_repo_scan_repos_v1", {
        p_start_offset: batch.start_offset,
        p_limit: batch.batch_size,
      }),
      sb.schema("skillset").from("skills_sh_repo_scan_batches_v1")
        .select("pack_path,pack_sha256,error_count")
        .eq("batch_id", batch.batch_id)
        .maybeSingle(),
    ]);
    if (reposError) throw new Error("repos:" + reposError.message);
    if (stateError) throw new Error("state:" + stateError.message);

    try {
      const { data: existingBucket } = await sb.storage.getBucket(BUCKET);
      if (!existingBucket) await sb.storage.createBucket(BUCKET, { public: false, fileSizeLimit: 10_485_760 });
    } catch {
      // A concurrent worker may have created the bucket.
    }

    let prior: any = null;
    if (state?.pack_path) {
      const { data: priorBlob } = await sb.storage.from(BUCKET).download(state.pack_path);
      if (priorBlob) {
        try {
          prior = await gunzipJson(priorBlob);
        } catch {
          prior = null;
        }
      }
    }
    const priorByRepo = new Map<string, any>();
    for (const row of prior?.repos || []) {
      priorByRepo.set(`${String(row.owner).toLowerCase()}/${String(row.repo).toLowerCase()}`, row);
    }

    let reusedCount = 0;
    let retriedCount = 0;
    const all = await mapLimit(repos || [], 8, async (item: any) => {
      const key = `${String(item.owner).toLowerCase()}/${String(item.repo).toLowerCase()}`;
      const previous = priorByRepo.get(key);
      if (previous?.status === 200) {
        reusedCount++;
        return previous;
      }
      retriedCount++;
      return await searchRepo(String(item.owner), String(item.repo));
    });

    const repoCount = all.length;
    const skillMatches = all.reduce((n, x) => n + (x.matches?.length || 0), 0);
    const saturated = all.filter((x) => x.saturated).length;
    const errors = all.filter((x) => x.status !== 200).length;
    const histogram: Record<string, number> = {};
    for (const row of all) histogram[String(row.status || 0)] = (histogram[String(row.status || 0)] || 0) + 1;

    const payload = {
      version: 5,
      batchId: batch.batch_id,
      startOffset: batch.start_offset,
      batchSize: batch.batch_size,
      generatedAt: new Date().toISOString(),
      repoCount,
      skillMatches,
      saturatedRepos: saturated,
      errorCount: errors,
      statusHistogram: histogram,
      recovery: { reusedCount, retriedCount, priorPack: state?.pack_path || null },
      repos: all,
    };
    const gz = await gzipBytes(JSON.stringify(payload));
    const hash = await sha256Hex(gz);
    const path = `packs-v3/${String(batch.batch_id).padStart(5, "0")}.json.gz`;
    const { error: uploadError } = await sb.storage.from(BUCKET).upload(path, gz, {
      contentType: "application/gzip",
      upsert: true,
    });
    if (uploadError) throw new Error("upload:" + uploadError.message);
    const { data: verifyBlob, error: verifyError } = await sb.storage.from(BUCKET).download(path);
    if (verifyError || !verifyBlob) throw new Error("download_verify:" + (verifyError?.message || "missing"));
    const verify = await sha256Hex(new Uint8Array(await verifyBlob.arrayBuffer()));
    if (verify !== hash) throw new Error("sha256_mismatch");

    const { error: finishError } = await sb.rpc("skillset_skills_sh_repo_scan_finish_v1", {
      p_batch_id: batch.batch_id,
      p_repo_count: repoCount,
      p_skill_matches: skillMatches,
      p_saturated_repos: saturated,
      p_error_count: errors,
      p_pack_path: path,
      p_pack_sha256: hash,
    });
    if (finishError) throw new Error("finish:" + finishError.message);
    const response = {
      ok: errors === 0,
      version: 5,
      batchId: batch.batch_id,
      repoCount,
      skillMatches,
      saturatedRepos: saturated,
      errorCount: errors,
      statusHistogram: histogram,
      reusedCount,
      retriedCount,
      packPath: path,
      packBytes: gz.length,
      sha256: hash,
    };
    return Response.json(response, { status: errors > 0 ? 503 : 200 });
  } catch (error) {
    if (batch?.batch_id) {
      await sb.rpc("skillset_skills_sh_repo_scan_release_v1", {
        p_batch_id: batch.batch_id,
        p_error: String(error),
      }).catch(() => {});
    }
    return Response.json({ ok: false, version: 5, stage: "worker", batchId: batch?.batch_id || null, error: String(error) }, { status: 500 });
  }
});
