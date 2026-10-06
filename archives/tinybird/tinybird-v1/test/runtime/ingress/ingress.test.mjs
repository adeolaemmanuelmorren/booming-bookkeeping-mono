import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";

const directory = dirname(fileURLToPath(import.meta.url));
const bundle = await build({ entryPoints: [join(directory, "test-worker.ts")], bundle: true, write: false, format: "esm", platform: "browser", target: "es2022", external: ["cloudflare:workers"] });

function envelope(count = 1) {
  return {
    schema_version: "jitsu_events_api_v1", producer_id: "jitsu:boom",
    events: Array.from({ length: count }, (_, i) => ({
      tenant_id: "boom", producer_id: "jitsu:boom", producer_sequence: i + 1,
      message_id: `message-${i}`, delivery_event_id: `delivery-${i}`, event_kind: "page_view",
      observed_at: "2026-09-05T01:02:03.123Z", ingested_at: "2026-09-05T01:02:04.456Z",
      source_fact_version: 1788570123123000, source_deleted: 0,
      fact_payload: JSON.stringify({ messageId: `message-${i}`, anonymousId: `visitor-${i}`, type: "page", timestamp: "2026-09-05T01:02:03.123456Z", sentAt: "2026-09-05T01:02:03.654321Z", properties: { url: "https://boom.invalid/original", custom: "retain this raw field" } }),
      extra_original_field: { keep: "untouched" },
    })),
  };
}

function storageKey(input) {
  const hash = createHash("sha256").update(JSON.stringify(input)).digest("hex");
  return `jitsu/envelopes/${hash}.json`;
}

async function runtime(t, bindings = {}) {
  const persistence = await mkdtemp(join(tmpdir(), "browser-ingress-test-"));
  const failures = { stage: null, occurrence: 1, seen: 0, pause: null, entered: null, calls: [] };
  const options = {
    ...convertV4MiniflareOptions({
      name: "ingress-test", modules: true, script: bundle.outputFiles[0].text, compatibilityDate: "2026-09-05",
      r2Buckets: { BROWSER_BUFFER: "browser-buffer-test" },
      durableObjects: { ROUTER: { className: "TestRouter", useSQLite: true } },
      bindings: { TENANT_ID: "boom", ...bindings },
      serviceBindings: {
        INGRESS: { name: "ingress-test", entrypoint: "TestIngress" },
        TEST_CONTROL: async request => {
          const call = await request.json();
          failures.calls.push(call);
          if (call.stage !== failures.stage) return Response.json(null);
          failures.seen++;
          if (failures.seen !== failures.occurrence) return Response.json(null);
          failures.entered?.();
          if (failures.pause) await failures.pause;
          return new Response(`Injected ${call.stage} failure`, { status: 503 });
        },
      },
    }),
    resourcePersistencePath: persistence,
    isolatedResourcePersistencePath: persistence,
  };
  let mf = new Miniflare(options);
  t.after(async () => { await mf.dispose(); await rm(persistence, { recursive: true, force: true }); });
  async function call(path, body) {
    const response = await mf.dispatchFetch(`https://test.invalid${path}`, { method: body === undefined ? "GET" : "POST", body: body === undefined ? undefined : JSON.stringify(body) });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error);
    return result;
  }
  return {
    call, failures,
    bucket: () => mf.getR2Bucket("BROWSER_BUFFER"),
    async restart() { await mf.dispose(); mf = new Miniflare(options); },
  };
}

const live = { BROWSER_INGRESS_MODE: "live", BROWSER_BASELINE_ID: "sealed-history", BROWSER_BASELINE_SEQUENCE: "100" };

test("default buffer archives exact queue JSON, verifies it, and never initializes the router", async t => {
  const r = await runtime(t, { DISABLE_ROUTER: "true" });
  const input = envelope();
  const receipt = await r.call("/receive", input);
  assert.equal(receipt.status, "buffered");
  assert.equal(receipt.key, storageKey(input));
  assert.equal(receipt.eventCount, 1);
  const stored = await (await r.bucket()).get(receipt.key);
  assert.equal(await stored.text(), JSON.stringify(input));
  assert.equal(stored.customMetadata.sha256, receipt.sha256);
  assert.deepEqual(r.failures.calls.map(call => call.stage), ["beforeGet", "beforePut", "afterPut", "beforeGet"]);
});

test("duplicate and concurrent retries retain one immutable object", async t => {
  const r = await runtime(t, { BROWSER_INGRESS_MODE: "buffer", DISABLE_ROUTER: "true" });
  const input = envelope();
  const receipts = await Promise.all(Array.from({ length: 4 }, () => r.call("/receive", input)));
  assert.ok(receipts.every(receipt => receipt.key === storageKey(input)));
  const bucket = await r.bucket();
  assert.equal((await bucket.list()).objects.length, 1);
  assert.equal(await (await bucket.get(storageKey(input))).text(), JSON.stringify(input));
});

test("R2 archive survives complete runtime restart and replay does not rewrite it", async t => {
  const r = await runtime(t, { DISABLE_ROUTER: "true" });
  const input = envelope();
  await r.call("/receive", input);
  const beforeVersion = (await (await r.bucket()).head(storageKey(input))).version;
  await r.restart();
  assert.equal((await r.call("/receive", input)).status, "buffered");
  const after = await (await r.bucket()).get(storageKey(input));
  assert.equal(after.version, beforeVersion);
  assert.equal(await after.text(), JSON.stringify(input));
});

test("failed R2 write cannot acknowledge; retry stores the envelope", async t => {
  const r = await runtime(t);
  r.failures.stage = "beforePut";
  const input = envelope();
  assert.deepEqual(await r.call("/consume", { envelope: input, behavior: "actual" }), { acknowledged: 0, retries: [{ delaySeconds: 60 }], forwarded: input });
  assert.equal((await (await r.bucket()).list()).objects.length, 0);
  const result = await r.call("/consume", { envelope: input, behavior: "actual" });
  assert.equal(result.acknowledged, 1);
  assert.equal((await (await r.bucket()).list()).objects.length, 1);
});

test("lost response after durable R2 write retries the same immutable key", async t => {
  const r = await runtime(t);
  r.failures.stage = "afterPut";
  const input = envelope();
  await assert.rejects(r.call("/receive", input), /afterPut failure/);
  const written = await (await r.bucket()).head(storageKey(input));
  assert.ok(written);
  assert.equal((await r.call("/receive", input)).status, "buffered");
  assert.equal((await (await r.bucket()).head(storageKey(input))).version, written.version);
});

test("conflicting preexisting archive fails exact readback without overwriting data", async t => {
  const r = await runtime(t);
  const input = envelope();
  const bucket = await r.bucket();
  await bucket.put(storageKey(input), "corrupt previous content");
  await assert.rejects(r.call("/receive", input), /exact readback/);
  assert.equal(await (await bucket.get(storageKey(input))).text(), "corrupt previous content");
});

test("live ingress requires a sealed baseline configuration before obtaining a router", async t => {
  for (const baseline of [{}, { BROWSER_BASELINE_ID: "empty", BROWSER_BASELINE_SEQUENCE: "1" }, { BROWSER_BASELINE_ID: "sealed", BROWSER_BASELINE_SEQUENCE: "0" }]) {
    const r = await runtime(t, { BROWSER_INGRESS_MODE: "live", DISABLE_ROUTER: "true", ...baseline });
    const input = envelope();
    await assert.rejects(r.call("/receive", input), /baseline has not been activated/);
    assert.equal((await (await r.bucket()).list()).objects.length, 1);
  }
});

test("unknown modes retain the raw envelope and fail closed", async t => {
  const r = await runtime(t, { BROWSER_INGRESS_MODE: "typo", DISABLE_ROUTER: "true" });
  await assert.rejects(r.call("/receive", envelope()), /must be buffer, collect or live/);
  assert.equal((await (await r.bucket()).list()).objects.length, 1);
});

test("live preserves original timestamps and IDs, and awaits durable routing", async t => {
  const r = await runtime(t, live);
  const input = envelope();
  const receipt = await r.call("/receive", input);
  assert.equal(receipt.status, "accepted");
  const { events, calls } = await r.call("/status");
  assert.equal(calls.length, 1);
  assert.equal(events.length, 1);
  assert.equal(events[0].source.delivery_event_id, input.events[0].delivery_event_id);
  assert.equal(events[0].source.original_payload, input.events[0].fact_payload);
  assert.equal(events[0].source.ingested_at, "2026-09-05T01:02:04.456000Z");
  assert.equal(events[0].pageRevision.page.page_view_timestamp, "2026-09-05T01:02:03.123456Z");
  assert.equal(events[0].pageRevision.page.page_view_id, "message-0");
  assert.equal(events[0].identity.factKey, "segment_page_view:message-0");
});

test("malformed envelope is archived but cannot acknowledge or route", async t => {
  const r = await runtime(t, live);
  for (const input of [{ ...envelope(), schema_version: "wrong" }, { ...envelope(), events: [] }, { ...envelope(), producer_id: "different" }]) {
    await assert.rejects(r.call("/receive", input), /Invalid|no events|ownership/);
    assert.equal(await (await (await r.bucket()).get(storageKey(input))).text(), JSON.stringify(input));
  }
  assert.equal(r.failures.calls.filter(call => call.stage === "beforeRoute").length, 0);
});

test("invalid final event prevents every live batch, with the complete raw envelope retained", async t => {
  const r = await runtime(t, live);
  const input = envelope(201);
  input.events[200].fact_payload = "not JSON";
  await assert.rejects(r.call("/receive", input));
  assert.equal(r.failures.calls.filter(call => call.stage === "beforeRoute").length, 0);
  assert.equal(await (await (await r.bucket()).get(storageKey(input))).text(), JSON.stringify(input));
});

test("single oversized normalized event fails explicitly before routing and remains recoverable", async t => {
  const r = await runtime(t, live);
  const input = envelope(2);
  const payload = JSON.parse(input.events[1].fact_payload);
  payload.raw_large_field = "界".repeat(180_000);
  input.events[1].fact_payload = JSON.stringify(payload);
  await assert.rejects(r.call("/receive", input), /exceeds 512000 bytes/);
  assert.equal(r.failures.calls.filter(call => call.stage === "beforeRoute").length, 0);
  assert.equal(await (await (await r.bucket()).get(storageKey(input))).text(), JSON.stringify(input));
});

test("ambiguous second batch preserves first acceptance and retry delivers stable IDs after restart", async t => {
  const r = await runtime(t, live);
  const input = envelope(201);
  r.failures.stage = "afterRoute";
  r.failures.occurrence = 2;
  await assert.rejects(r.call("/receive", input), /afterRoute failure/);
  const first = await r.call("/status");
  assert.equal(first.events.length, 201);
  assert.ok(first.calls.length >= 2);
  assert.ok(first.calls.every(batch => batch.count <= 200 && batch.bytes <= 512_000));
  await r.restart();
  assert.equal((await r.call("/receive", input)).status, "accepted");
  const replay = await r.call("/status");
  assert.equal(replay.events.length, 201);
  assert.deepEqual(replay.events, first.events);
  assert.equal(replay.calls.length, first.calls.length * 2);
});

test("queue acknowledges only accepted or buffered receipts and forwards the original body", async t => {
  const r = await runtime(t);
  const input = envelope();
  for (const behavior of ["buffered", "stored", "accepted"]) {
    const result = await r.call("/consume", { envelope: input, behavior });
    assert.deepEqual(result, { acknowledged: 1, retries: [], forwarded: input });
  }
  for (const behavior of ["missing", "throw", "unexpected"]) {
    const result = await r.call("/consume", { envelope: input, behavior });
    assert.equal(result.acknowledged, 0);
    assert.deepEqual(result.retries, [{ delaySeconds: 60 }]);
  }
});


test("readback failure cannot acknowledge even after a successful durable write", async t => {
  const r = await runtime(t);
  r.failures.stage = "beforeGet";
  r.failures.occurrence = 2;
  const input = envelope();
  const failed = await r.call("/consume", { envelope: input, behavior: "actual" });
  assert.equal(failed.acknowledged, 0);
  assert.deepEqual(failed.retries, [{ delaySeconds: 60 }]);
  assert.equal(await (await (await r.bucket()).get(storageKey(input))).text(), JSON.stringify(input));
  assert.equal((await r.call("/consume", { envelope: input, behavior: "actual" })).acknowledged, 1);
});

test("consumer waits for the archive operation before acknowledging", async t => {
  const r = await runtime(t);
  const input = envelope();
  r.failures.stage = "beforePut";
  let unblock;
  const entered = new Promise(resolve => { r.failures.entered = resolve; });
  r.failures.pause = new Promise(resolve => { unblock = resolve; });
  let settled = false;
  const pending = r.call("/consume", { envelope: input, behavior: "actual" }).then(result => { settled = true; return result; });
  await entered;
  assert.equal(settled, false);
  assert.equal((await (await r.bucket()).list()).objects.length, 0);
  unblock();
  const result = await pending;
  assert.equal(result.acknowledged, 0);
  assert.deepEqual(result.retries, [{ delaySeconds: 60 }]);
});
