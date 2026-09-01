import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const BUCKET = "skill-discovery-v1";

function json(body: unknown, status = 200) {
  return Response.json(body, { status });
}

function errorText(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

async function gunzipBytes(bytes: Uint8Array) {
  const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"));
  return await new Response(stream).text();
}

async function gunzipBlob(blob: Blob) {
  return await gunzipBytes(new Uint8Array(await blob.arrayBuffer()));
}

async function gzipBytes(text: string) {
  const stream = new Blob([new TextEncoder().encode(text)]).stream()
    .pipeThrough(new CompressionStream("gzip"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

async function sha256Text(text: string) {
  const bytes = new TextEncoder().encode(text);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

async function sha256Bytes(bytes: Uint8Array) {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

function fromBase64(value: string) {
  const raw = atob(value);
  return Uint8Array.from(raw, (c) => c.charCodeAt(0));
}

function sourceRows(payload: any) {
  if (Array.isArray(payload?.shards)) {
    return payload.shards.flatMap((shard: any) => Array.isArray(shard?.rows) ? shard.rows : []);
  }
  return Array.isArray(payload?.rows) ? payload.rows : [];
}

function slug(value: string) {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

async function uploadPack(sb: any, path: string, payload: unknown) {
  const gz = await gzipBytes(JSON.stringify(payload));
  const { error } = await sb.storage.from(BUCKET).upload(path, gz, {
    upsert: true,
    contentType: "application/gzip",
    cacheControl: "31536000",
  });
  if (error) throw new Error(`upload:${error.message}`);
  return { bytes: gz.length, sha256: await sha256Bytes(gz) };
}

Deno.serve(async (req) => {
  if (req.method !== "POST") return json({ ok: false, error: "method_not_allowed" }, 405);
  const sb = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
    { auth: { persistSession: false } },
  );
  let claim: any = null;
  try {
    const result = await sb.rpc("skillset_unified_synthesis_claim_v1");
    if (result.error) throw new Error(`claim:${result.error.message}`);
    claim = result.data;
    if (!claim || claim.kind === "none" || claim.kind === "blocked") {
      return json({ ok: true, claimed: 0, ...claim });
    }

    if (claim.kind === "source_projected") {
      const rows = [];
      for (const row of claim.rows || []) {
        const content = await gunzipBytes(fromBase64(row.content_gzip_base64));
        const materializedHash = await sha256Text(content);
        if (materializedHash !== row.projected_content_hash) {
          throw new Error(`projected_hash_mismatch:${row.canonical_hash}`);
        }
        rows.push({
          canonical_hash: row.canonical_hash,
          materialized_hash: materializedHash,
          source_kind: "projected_db",
          content,
        });
      }
      const suffix = (await sha256Text(rows.map((r) => r.canonical_hash).join("|"))).slice(0, 24);
      const outputPath = `unified-synthesis/source-v1/projected-${suffix}.json.gz`;
      await uploadPack(sb, outputPath, { version: 1, source_kind: "projected_db", rows });
      const finish = await sb.rpc("skillset_unified_synthesis_source_finish_v1", {
        p_claimed_hashes: claim.hashes,
        p_rows: rows.map((r) => ({
          canonical_hash: r.canonical_hash,
          materialized_hash: r.materialized_hash,
          content_bytes: new TextEncoder().encode(r.content).length,
        })),
        p_output_path: outputPath,
        p_error: null,
      });
      if (finish.error) throw new Error(`finish:${finish.error.message}`);
      return json({ ok: true, kind: claim.kind, ready: rows.length, outputPath });
    }

    if (claim.kind === "source_gitskills") {
      const download = await sb.storage.from(BUCKET).download(claim.input_path);
      if (download.error || !download.data) throw new Error(`download:${download.error?.message || "missing"}`);
      const payload = JSON.parse(await gunzipBlob(download.data));
      const wanted = new Set<string>(claim.hashes || []);
      const rows = [];
      for (const row of sourceRows(payload)) {
        if (typeof row?.content !== "string") continue;
        const hash = await sha256Text(row.content);
        if (!wanted.has(hash)) continue;
        rows.push({
          canonical_hash: hash,
          materialized_hash: hash,
          source_kind: "gitskills_pack",
          repo_full_name: row.repo_full_name || null,
          path: row.path || null,
          content: row.content,
        });
      }
      const suffix = (await sha256Text(claim.input_path)).slice(0, 24);
      const outputPath = `unified-synthesis/source-v1/gitskills-${suffix}.json.gz`;
      if (rows.length) await uploadPack(sb, outputPath, { version: 1, source_kind: "gitskills_pack", rows });
      const finish = await sb.rpc("skillset_unified_synthesis_source_finish_v1", {
        p_claimed_hashes: claim.hashes,
        p_rows: rows.map((r) => ({
          canonical_hash: r.canonical_hash,
          materialized_hash: r.materialized_hash,
          content_bytes: new TextEncoder().encode(r.content).length,
        })),
        p_output_path: rows.length ? outputPath : null,
        p_error: rows.length ? null : "no_target_content_in_pack",
      });
      if (finish.error) throw new Error(`finish:${finish.error.message}`);
      return json({ ok: true, kind: claim.kind, expected: wanted.size, found: rows.length, outputPath });
    }

    if (claim.kind === "compose") {
      const byHash = new Map<string, any>();
      const paths = [...new Set<string>((claim.candidates || []).map((x: any) => x.output_path).filter(Boolean))];
      for (const path of paths) {
        const download = await sb.storage.from(BUCKET).download(path);
        if (download.error || !download.data) throw new Error(`component_download:${path}:${download.error?.message || "missing"}`);
        const payload = JSON.parse(await gunzipBlob(download.data));
        for (const row of payload.rows || []) byHash.set(row.canonical_hash, row);
      }
      const components = [];
      for (const candidate of claim.candidates || []) {
        const row = byHash.get(candidate.canonical_hash);
        if (!row?.content) throw new Error(`component_missing:${candidate.canonical_hash}`);
        components.push({
          rank: candidate.rank,
          canonical_hash: candidate.canonical_hash,
          materialized_hash: row.materialized_hash,
          score: candidate.score,
          occurrence_count: candidate.occurrence_count,
          source_kind: row.source_kind,
          repo_full_name: row.repo_full_name || null,
          path: row.path || null,
          content: row.content,
        });
      }
      const name = slug(`${claim.target_area}-${claim.category}`);
      const indexSkill = [
        "---",
        `name: ${name}`,
        `description: Curated ${claim.target_area} skill bundle for ${claim.category}; routes work to provenance-preserving components.`,
        "---",
        "",
        `# ${claim.category.replaceAll("_", " ")}`,
        "",
        "Select the narrowest component that matches the task. Prefer the higher-ranked component when multiple components overlap. Preserve each component's safety constraints and verify repository-specific commands before execution.",
        "",
        "## Components",
        ...components.map((c) => `- Rank ${c.rank}: \`${c.canonical_hash}\` (score ${c.score}, occurrences ${c.occurrence_count})`),
        "",
      ].join("\n");
      const artifact = {
        version: 1,
        target_area: claim.target_area,
        category: claim.category,
        generated_at: new Date().toISOString(),
        index_skill: indexSkill,
        components,
      };
      const outputPath = `unified-synthesis/final-v1/${slug(claim.target_area)}/${slug(claim.category)}.json.gz`;
      const uploaded = await uploadPack(sb, outputPath, artifact);
      const finish = await sb.rpc("skillset_unified_synthesis_bundle_finish_v1", {
        p_target_area: claim.target_area,
        p_category: claim.category,
        p_output_path: outputPath,
        p_component_count: components.length,
        p_artifact_sha256: uploaded.sha256,
        p_error: null,
      });
      if (finish.error) throw new Error(`bundle_finish:${finish.error.message}`);
      return json({ ok: true, kind: claim.kind, target_area: claim.target_area, category: claim.category, components: components.length });
    }

    throw new Error(`unknown_claim_kind:${claim.kind}`);
  } catch (error) {
    const message = errorText(error);
    if (claim?.kind?.startsWith("source_")) {
      await sb.rpc("skillset_unified_synthesis_source_finish_v1", {
        p_claimed_hashes: claim.hashes || [], p_rows: [], p_output_path: null, p_error: message,
      });
    } else if (claim?.kind === "compose") {
      await sb.rpc("skillset_unified_synthesis_bundle_finish_v1", {
        p_target_area: claim.target_area, p_category: claim.category,
        p_output_path: null, p_component_count: 0, p_artifact_sha256: null, p_error: message,
      });
    }
    return json({ ok: false, error: message, kind: claim?.kind || null }, 500);
  }
});
