import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { createRemoteJWKSet, jwtVerify } from "npm:jose@5.9.6";

const BUCKET = "skill-discovery-v1";
const ISSUER = "https://token.actions.githubusercontent.com";
const AUDIENCE = "gitskills-projection-rescue";
const JWKS = createRemoteJWKSet(new URL(`${ISSUER}/.well-known/jwks`));
const INPUT_PATH = /^gitskills\/discovery-projection-split-v1\/[a-f0-9]{16}-part-[0-9]{3}[.]json[.]gz$/;
const PREFILTER_PATH = /^gitskills\/prefilter-projection-split-v1\/[a-f0-9]{16}-part-[0-9]{3}[.]json[.]gz$/;

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
  if (!workflow.includes("/.github/workflows/gitskills-fast-finish.yml@refs/heads/gitskills-migration-run")) {
    throw new Error("workflow_denied");
  }
}

async function sha256(bytes: Uint8Array) {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

Deno.serve(async (req: Request) => {
  try {
    await authorize(req);
    const sb = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
      { auth: { persistSession: false } },
    );

    if (req.method === "GET") {
      const url = new URL(req.url);
      const worker = Number(url.searchParams.get("worker") || "0");
      const workers = Number(url.searchParams.get("workers") || "0");
      const { data, error } = await sb.rpc("skillset_gitskills_projection_rescue_manifest_v1", {
        p_worker: worker, p_workers: workers, p_limit: 100,
      });
      if (error) throw new Error(`manifest:${error.message}`);
      const parents = [];
      for (const row of data || []) {
        const [input, prefilter] = await Promise.all([
          sb.storage.from(BUCKET).createSignedUrl(row.input_path, 1800),
          sb.storage.from(BUCKET).createSignedUrl(row.prefilter_path, 1800),
        ]);
        if (input.error || prefilter.error || !input.data || !prefilter.data) {
          throw new Error(`signed_url_failed:${row.input_path}`);
        }
        parents.push({
          input_path: row.input_path,
          prefilter_path: row.prefilter_path,
          input_url: input.data.signedUrl,
          prefilter_url: prefilter.data.signedUrl,
        });
      }
      return json({ ok: true, parents });
    }

    if (req.method !== "POST") return json({ ok: false, error: "method_not_allowed" }, 405);
    const operation = req.headers.get("x-operation") || "";
    if (operation === "upload") {
      const path = req.headers.get("x-storage-path") || "";
      const expected = req.headers.get("x-sha256") || "";
      if ((!INPUT_PATH.test(path) && !PREFILTER_PATH.test(path)) || !/^[a-f0-9]{64}$/.test(expected)) {
        return json({ ok: false, error: "invalid_upload_metadata" }, 400);
      }
      const bytes = new Uint8Array(await req.arrayBuffer());
      if (bytes.length < 20 || bytes.length > 3_000_000) return json({ ok: false, error: "invalid_upload_size" }, 400);
      const actual = await sha256(bytes);
      if (actual !== expected) return json({ ok: false, error: "sha256_mismatch" }, 400);
      const { error } = await sb.storage.from(BUCKET).upload(path, bytes, {
        upsert: true, contentType: "application/gzip", cacheControl: "31536000",
      });
      if (error) throw new Error(`upload:${error.message}`);
      return json({ ok: true, path, bytes: bytes.length, sha256: actual });
    }

    if (operation === "finalize") {
      const body = await req.json();
      const { data, error } = await sb.rpc("skillset_gitskills_projection_rescue_finish_v1", {
        p_parent_input: body.parent_input,
        p_parent_prefilter: body.parent_prefilter,
        p_children: body.children,
      });
      if (error) throw new Error(`finalize:${error.message}`);
      return json({ ok: true, children: data });
    }
    return json({ ok: false, error: "invalid_operation" }, 400);
  } catch (error) {
    return json({ ok: false, error: String(error) }, 401);
  }
});
