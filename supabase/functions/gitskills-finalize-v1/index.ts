import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const BUCKET = "skill-discovery-v1";

async function gunzip(blob: Blob) {
  const stream = blob.stream().pipeThrough(new DecompressionStream("gzip"));
  return await new Response(stream).text();
}

async function gzipBytes(text: string) {
  const input = new TextEncoder().encode(text);
  const stream = new Blob([input]).stream().pipeThrough(new CompressionStream("gzip"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

function errorText(error: unknown) {
  if (error && typeof error === "object" && "message" in error) {
    return String((error as { message?: unknown }).message || error);
  }
  return String(error);
}

Deno.serve(async () => {
  const sb = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
    { auth: { persistSession: false } },
  );
  let claim: any = null;

  try {
    const { data, error } = await sb.rpc("skillset_gitskills_final_claim_v1");
    if (error) throw new Error(error.message);
    if (!data?.length) return Response.json({ ok: true, version: 4, claimed: 0 });
    claim = data[0];

    const { data: blob, error: downloadError } = await sb.storage
      .from(BUCKET)
      .download(claim.rubric_path);
    if (downloadError || !blob) {
      throw new Error("download_failed:" + (downloadError?.message || "missing"));
    }

    const rubric = JSON.parse(await gunzip(blob));
    let accept = 0;
    let reviewReject = 0;
    let safetyHold = 0;
    let reject = 0;
    const rows: any[] = [];
    const acceptByHash = new Map<string, any>();

    for (const row of rubric.rows || []) {
      let finalDecision = "reject";
      let reason = "rubric_reject";
      if (row.decision === "accept") {
        if (row.safety_review_required) {
          finalDecision = "safety_hold";
          reason = "safety_review_required";
          safetyHold++;
        } else {
          finalDecision = "accept";
          reason = "dual_view_ge_70";
          accept++;
          const candidate = {
            source_content_hash: row.source_content_hash,
            projected_content_hash: row.projected_content_hash,
            repo_full_name: row.repo_full_name,
            path: row.path,
            score: row.score,
            raw_structural_score: row.raw_structural_score,
            projection_score: row.projection_score,
          };
          const prior = acceptByHash.get(candidate.source_content_hash);
          const candidateRank = [
            candidate.score ?? -1,
            candidate.projection_score ?? -1,
            candidate.raw_structural_score ?? -1,
          ];
          const priorRank = prior
            ? [prior.score ?? -1, prior.projection_score ?? -1, prior.raw_structural_score ?? -1]
            : null;
          const candidateTie = [
            candidate.repo_full_name,
            candidate.path,
            candidate.projected_content_hash,
          ].join("\u0000");
          const priorTie = prior
            ? [prior.repo_full_name, prior.path, prior.projected_content_hash].join("\u0000")
            : "";
          const better = (() => {
            if (!prior || !priorRank) return true;
            for (let index = 0; index < candidateRank.length; index++) {
              if (candidateRank[index] !== priorRank[index]) {
                return candidateRank[index] > priorRank[index];
              }
            }
            return candidateTie < priorTie;
          })();
          if (better) acceptByHash.set(candidate.source_content_hash, candidate);
        }
      } else if (row.decision === "review") {
        reason = "precision_first_unresolved_65_69";
        reviewReject++;
      } else {
        reject++;
      }
      rows.push({
        row_idx: row.row_idx,
        file_sha: row.file_sha,
        repo_full_name: row.repo_full_name,
        path: row.path,
        source_content_hash: row.source_content_hash,
        projected_content_hash: row.projected_content_hash,
        score: row.score,
        raw_structural_score: row.raw_structural_score,
        projection_score: row.projection_score,
        rubric_decision: row.decision,
        final_decision: finalDecision,
        final_reason: reason,
        safety_review_required: Boolean(row.safety_review_required),
      });
    }

    const acceptRows = [...acceptByHash.values()];
    const output = {
      // Keep the persisted artifact schema at v2; v4 identifies only this worker revision.
      version: 2,
      policy: "precision_first_final_resolution",
      rubricVersion: "3.1h",
      inputPath: claim.input_path,
      rubricPath: claim.rubric_path,
      accept,
      precisionRejectReview: reviewReject,
      safetyHold,
      reject,
      reviewRemaining: 0,
      generatedAt: new Date().toISOString(),
      rows,
    };
    const gz = await gzipBytes(JSON.stringify(output));
    const safe = claim.input_path.split("/").pop()?.replace(/\.json\.gz$/, "") || crypto.randomUUID();
    const outputPath = `gitskills/final-resolution-v1/${safe}.json.gz`;
    const { error: uploadError } = await sb.storage.from(BUCKET).upload(outputPath, gz, {
      upsert: true,
      contentType: "application/gzip",
    });
    if (uploadError) throw new Error("upload:" + uploadError.message);

    const { error: registryError } = await sb.rpc("skillset_gitskills_final_accept_replace_v1", {
      p_input_path: claim.input_path,
      p_rows: acceptRows,
    });
    if (registryError) throw new Error("accept_registry:" + registryError.message);

    const { error: finishError } = await sb.rpc("skillset_gitskills_final_finish_v1", {
      p_input_path: claim.input_path,
      p_output_path: outputPath,
      p_accept: accept,
      p_review_reject: reviewReject,
      p_safety_hold: safetyHold,
      p_reject: reject,
      p_error: null,
    });
    if (finishError) throw new Error("finish:" + finishError.message);

    return Response.json({
      ok: true,
      version: 4,
      accept,
      precisionRejectReview: reviewReject,
      safetyHold,
      reject,
      reviewRemaining: 0,
      canonicalCandidatesWritten: acceptRows.length,
      input: claim.input_path,
      output: outputPath,
    });
  } catch (error) {
    const message = errorText(error);
    if (claim?.input_path) {
      try {
        await sb.rpc("skillset_gitskills_final_finish_v1", {
          p_input_path: claim.input_path,
          p_output_path: null,
          p_accept: 0,
          p_review_reject: 0,
          p_safety_hold: 0,
          p_reject: 0,
          p_error: message,
        });
      } catch {
        // Preserve the original failure if the release call also fails.
      }
    }
    return Response.json({ ok: false, version: 4, error: message }, { status: 500 });
  }
});
