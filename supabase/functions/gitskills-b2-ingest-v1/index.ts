import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { createRemoteJWKSet, jwtVerify } from "npm:jose@5.9.6";

const BUCKET = "skill-discovery-v1";
const ISSUER = "https://token.actions.githubusercontent.com";
const AUDIENCE = "gitskills-b2-supabase-ingest";
const JWKS = createRemoteJWKSet(new URL(`${ISSUER}/.well-known/jwks`));
const PATH = /^gitskills\/(?:discovery-b2-v[12]\/pack-[0-9-]+|discovery-b2-split-v1\/pack-[0-9]+-part-[0-9]+)\.json\.gz$/;

function json(body: unknown, status = 200) {
  return Response.json(body, { status });
}

async function authorize(req: Request) {
  const token = req.headers.get("authorization")?.replace(/^Bearer\s+/i, "");
  if (!token) throw new Error("missing_oidc_token");
  const { payload } = await jwtVerify(token, JWKS, { issuer: ISSUER, audience: AUDIENCE });
  if (payload.repository !== "ahmetrz/skillset") throw new Error("repository_denied");
  if (payload.ref !== "refs/heads/gitskills-migration-run") throw new Error("ref_denied");
  const workflow = String(payload.workflow_ref || "");
  if (!workflow.includes("/.github/workflows/gitskills-b2-import.yml@refs/heads/gitskills-migration-run") &&
      !workflow.includes("/.github/workflows/gitskills-b2-split-rescue.yml@refs/heads/gitskills-migration-run")) {
    throw new Error("workflow_denied");
  }
}

Deno.serve(async (req: Request) => {
  try {
    if (req.method !== "POST") return json({ ok: false, error: "method_not_allowed" }, 405);
    await authorize(req);
    const path = req.headers.get("x-storage-path") || "";
    const expectedHash = req.headers.get("x-sha256") || "";
    const representatives = Number(req.headers.get("x-representatives") || "0");
    if (!PATH.test(path) || !/^[a-f0-9]{64}$/.test(expectedHash) || representatives < 0) {
      return json({ ok: false, error: "invalid_metadata" }, 400);
    }
    const bytes = new Uint8Array(await req.arrayBuffer());
    const digest = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)))
      .map((b) => b.toString(16).padStart(2, "0")).join("");
    if (digest !== expectedHash) return json({ ok: false, error: "sha256_mismatch" }, 400);

    const sb = createClient(Deno.env.get("SUPABASE_URL")!, Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!, { auth: { persistSession: false } });
    const { error: uploadError } = await sb.storage.from(BUCKET).upload(path, bytes, {
      upsert: true, contentType: "application/gzip", cacheControl: "31536000"
    });
    if (uploadError) throw new Error(`storage_upload:${JSON.stringify(uploadError)}`);
    const { error: finishError } = await sb.rpc("skillset_gitskills_b2_ingest_finish_v1", {
      p_path: path, p_representatives: representatives
    });
    if (finishError) throw new Error(`ingest_finish:${JSON.stringify(finishError)}`);
    return json({ ok: true, path, bytes: bytes.length, representatives, sha256: digest });
  } catch (error) {
    return json({ ok: false, error: String(error) }, 401);
  }
});
