import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { build } from "esbuild";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";

const directory = dirname(fileURLToPath(import.meta.url));
const built = await build({ entryPoints: [join(directory, "test-worker.ts")], bundle: true, write: false,
  format: "esm", platform: "browser", target: "es2022", external: ["cloudflare:workers"] });
const hash = "a".repeat(64);
const tables = ["v1_snapshot_activecampaign_contact", "v1_snapshot_activecampaign_contact_tag", "v1_snapshot_activecampaign_tags",
  "v1_snapshot_stripe_charge", "v1_snapshot_stripe_customer", "v1_snapshot_stripe_payment_intent", "v1_snapshot_stripe_kajabi_charge",
  "v1_snapshot_stripe_kajabi_customer", "v1_snapshot_stripe_kajabi_payment_intent"];
const seal = { format: "identity-only-v1", baselineId: "identity-1", tenantId: "boom", identityVersion: 1,
  sourceSeal: hash, publicationHash: "b".repeat(64), nativeImportProofSha256: "c".repeat(64), inputs: { fivetran: { snapshotAt: "2026-09-05T22:28:00.000Z",
    tables: tables.map(table => ({ table, expectedPhysicalRows: 0 })) } },
  counts: { facts: 0, identifiers: 0, components: 0, recordLookups: 0 },
  membership: { version: 1, buckets: 65536, pageSize: 256, pageHashes: Array(256).fill(hash) } };
const canonical = value => JSON.stringify(value, Object.keys(value).sort());
// Match the repository's recursive canonical JSON.
function ordered(value) { if (Array.isArray(value)) return value.map(ordered); if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.keys(value).sort().map(key => [key, ordered(value[key])])); }
const sealText = JSON.stringify(ordered(seal));
const sealHash = createHash("sha256").update(sealText).digest("hex");

async function runtime(t, mode) {
  const storage = await mkdtemp(join(tmpdir(), "identity-only-activation-"));
  const miniflare = new Miniflare({ ...convertV4MiniflareOptions({ name: "identity-only", modules: true,
    script: built.outputFiles[0].text, compatibilityDate: "2026-09-05",
    durableObjects: { IDENTITY: { className: "TestIdentity", useSQLite: true } },
    serviceBindings: { IDENTITY_BASELINE: { name: "identity-only", entrypoint: "TestBaseline" } },
    bindings: { TENANT_ID: "boom", TINYBIRD_URL: "https://api.us-east.tinybird.co", TINYBIRD_TOKEN: "test",
      IDENTITY_BASELINE_ID: "identity-1", IDENTITY_BASELINE_SEAL: sealHash },
    outboundService: async request => {
      const query = new URLSearchParams(await request.text()).get("q");
      assert.match(query, /v1_identity_bootstrap_manifests/);
      const row = { payload_json: sealText, payload_hash: sealHash };
      return Response.json({ data: mode === "missing" ? [] : mode === "ambiguous" ? [row, row] : [row] });
    },
  }), isolatedResourcePersistencePath: storage, resourcePersistencePath: storage });
  t.after(async () => { await miniflare.dispose(); await rm(storage, { recursive: true, force: true }); });
  return async (path, body) => {
    const response = await miniflare.dispatchFetch(`https://test.invalid${path}`, { method: "POST", body: body && JSON.stringify(body) });
    const text = await response.text();
    let parsed = null;
    try { parsed = text ? JSON.parse(text) : null; } catch { parsed = text; }
    return { status: response.status, body: parsed };
  };
}

test("actual Durable Object blocks version zero and activates one exact seal", async t => {
  const call = await runtime(t, "valid");
  assert.equal((await call("/enqueue", [])).status, 400);
  const activated = await call("/activate");
  assert.equal(activated.status, 200);
  assert.equal(activated.body.identityVersion, 1);
  assert.equal((await call("/enqueue", [])).status, 200);
});

for (const mode of ["missing", "ambiguous"]) test(`activation rejects ${mode} baseline proof`, async t => {
  const call = await runtime(t, mode);
  assert.equal((await call("/activate")).status, 400);
  assert.equal((await call("/status")).body.publishedVersion, 0);
});
