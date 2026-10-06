import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { build } from "esbuild";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";

const directory = dirname(fileURLToPath(import.meta.url));
const built = await build({ entryPoints: [join(directory, "fivetran-worker.ts")], bundle: true, write: false,
  format: "esm", platform: "browser", target: "es2022", external: ["cloudflare:workers", "node:crypto"] });
const snapshot = "2026-09-05T22:28:00.000000Z";

async function run(t, enabled) {
  const storage = await mkdtemp(join(tmpdir(), "identity-fivetran-"));
  const options = { ...convertV4MiniflareOptions({ name: "identity-fivetran", modules: true, script: built.outputFiles[0].text,
    compatibilityDate: "2026-09-05", durableObjects: {
      FACTS: { className: "TestFivetranIdentity", useSQLite: true },
      IDENTITY: { className: "TestIdentityBinding", useSQLite: true },
    },
    bindings: { TENANT_ID: "boom", TINYBIRD_URL: "https://api.us-east.tinybird.co", TINYBIRD_TOKEN: "test",
      FIVETRAN_SNAPSHOT_AT: snapshot, IDENTITY_BASELINE_ID: "identity-1", IDENTITY_BASELINE_SEAL: "a".repeat(64),
      IDENTITY_INGESTION_ENABLED: enabled ? "true" : "false" },
    serviceBindings: { IDENTITY_BASELINE: { name: "identity-fivetran", entrypoint: "TestBaselineBinding" } },
  }), isolatedResourcePersistencePath: storage, resourcePersistencePath: storage };
  const mf = new Miniflare(options);
  t.after(async () => { await mf.dispose(); await rm(storage, { recursive: true, force: true }); });
  return async (path, body) => {
    const response = await mf.dispatchFetch(`https://test.invalid${path}`, { method: "POST", body: JSON.stringify(body) });
    return { status: response.status, text: await response.text() };
  };
}

test("actual Fivetran identity coordinator starts only behind its independent gate", async t => {
  const off = await run(t, false);
  const identity = { pipeline: "activecampaign", snapshotId: "identity-1", snapshotAt: snapshot };
  assert.equal((await off("/initialize", identity)).status, 200);
  assert.match((await off("/start", { pipeline: "activecampaign", snapshotId: "identity-1", activationId: "identity-1" })).text, /disabled/);

  const on = await run(t, true);
  assert.equal((await on("/initialize", identity)).status, 200);
  const started = await on("/start", { pipeline: "activecampaign", snapshotId: "identity-1", activationId: "identity-1" });
  assert.equal(started.status, 200, started.text);
  assert.equal(JSON.parse(started.text).activated, true);
});
