import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const BUCKET = "skill-discovery-v1";
const PAGE_SIZE = 500;

async function gzipBytes(text: string) {
  const stream = new Blob([new TextEncoder().encode(text)]).stream()
    .pipeThrough(new CompressionStream("gzip"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

Deno.serve(async () => {
  const sb = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
    { auth: { persistSession: false } },
  );
  let claim: { prefix: string } | null = null;
  try {
    const { data, error } = await sb.rpc("skillset_gitskills_canonical_export_claim_v1");
    if (error) throw error;
    if (!data?.length) {
      return Response.json({ ok: true, version: 2, claimed: 0, waitingForUpstream: true });
    }
    claim = data[0];
    const prefix = String(claim.prefix);
    const lo = prefix + "0".repeat(62);
    const prefixNumber = Number.parseInt(prefix, 16);
    const next = prefixNumber < 255
      ? (prefixNumber + 1).toString(16).padStart(2, "0") + "0".repeat(62)
      : null;

    const rows: Record<string, unknown>[] = [];
    for (let offset = 0;; offset += PAGE_SIZE) {
      let query = sb.from("gitskills_final_accept_canonical_v1")
        .select("*")
        .gte("source_content_hash", lo)
        .order("source_content_hash", { ascending: true })
        .range(offset, offset + PAGE_SIZE - 1);
      if (next) query = query.lt("source_content_hash", next);
      const { data: page, error: queryError } = await query;
      if (queryError) throw new Error(`query:${queryError.message}`);
      rows.push(...(page || []));
      if ((page?.length || 0) < PAGE_SIZE) break;
    }

    const payload = {
      version: 2,
      source: "GitSkills",
      policy: "canonical_sha256_accepts_only",
      prefix,
      generatedAt: new Date().toISOString(),
      count: rows.length,
      rows,
    };
    const bytes = await gzipBytes(JSON.stringify(payload));
    const path = `gitskills/final-canonical-v1/prefix-${prefix}.json.gz`;
    const { error: uploadError } = await sb.storage.from(BUCKET).upload(path, bytes, {
      upsert: true,
      contentType: "application/gzip",
    });
    if (uploadError) throw uploadError;
    const { error: finishError } = await sb.rpc("skillset_gitskills_canonical_export_finish_v1", {
      p_prefix: prefix,
      p_path: path,
      p_count: rows.length,
      p_error: null,
    });
    if (finishError) throw finishError;
    return Response.json({ ok: true, version: 2, prefix, count: rows.length, path, bytes: bytes.length });
  } catch (error) {
    if (claim?.prefix) {
      await sb.rpc("skillset_gitskills_canonical_export_finish_v1", {
        p_prefix: claim.prefix,
        p_path: null,
        p_count: 0,
        p_error: String(error),
      }).catch(() => {});
    }
    return Response.json({ ok: false, version: 2, error: String(error) }, { status: 500 });
  }
});
