import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { createHash } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";

const directory = dirname(fileURLToPath(import.meta.url));
const bundle = await build({ entryPoints: [join(directory, "replay-worker.ts")], bundle: true, write: false, format: "esm", platform: "browser", target: "es2022", external: ["cloudflare:workers"] });
function envelope(id) {
  const event = { tenant_id: "boom", producer_id: "jitsu", message_id: id, delivery_event_id: `delivery:${id}`, event_kind: "page_view", observed_at: "2026-09-05T01:00:00Z", ingested_at: "2026-09-05T01:00:01Z", source_fact_version: 1, source_deleted: 0, fact_payload: JSON.stringify({ messageId: id, anonymousId: `visitor:${id}`, timestamp: "2026-09-05T01:00:00.123456Z" }) };
  return { schema_version: "jitsu_events_api_v1", producer_id: "jitsu", events: [event] };
}
function archived(id) {
  const body = JSON.stringify(envelope(id));
  return { body, key: `jitsu/envelopes/${createHash("sha256").update(body).digest("hex")}.json` };
}
async function runtime(t, bindings = {}) {
  const persistence = await mkdtemp(join(tmpdir(), "buffer-replay-test-"));
  const control = { stage: null, hits: 0, occurrence: 1, calls: [], pause: null, entered: null, fail: true };
  const options = {
    ...convertV4MiniflareOptions({
      name: "replay-test", modules: true, script: bundle.outputFiles[0].text, compatibilityDate: "2026-09-05",
      r2Buckets: { BROWSER_BUFFER: "buffer-test" },
      durableObjects: { REPLAY: { className: "TestReplay", useSQLite: true }, BROWSER_ROUTER: { className: "TestRouter", useSQLite: true } },
      bindings: { TENANT_ID: "boom", BROWSER_INGRESS_MODE: "live", BROWSER_BASELINE_ID: "sealed", BROWSER_BASELINE_SEQUENCE: "1", AUTO_PUBLISH: "true", ...bindings },
      serviceBindings: {
        BROWSER_INGRESS: { name: "replay-test", entrypoint: "BrowserIngress" },
        CONTROL: async request => {
          const call = await request.json(); control.calls.push(call);
          if (call.stage !== control.stage) return Response.json(null);
          control.hits++;
          if (control.hits !== control.occurrence) return Response.json(null);
          control.entered?.(call);
          if (control.pause) await control.pause;
          return control.fail ? new Response("Injected failure", { status: 503 }) : Response.json(null);
        },
      },
    }),
    resourcePersistencePath: persistence, isolatedResourcePersistencePath: persistence,
  };
  let mf = new Miniflare(options);
  t.after(async () => { await mf.dispose(); await rm(persistence, { recursive: true, force: true }); });
  const call = async (path, input = {}) => {
    const response = await mf.dispatchFetch(`https://test.invalid${path}`, { method: "POST", body: JSON.stringify(input) });
    const value = await response.json(); if (!response.ok) throw new Error(value.error); return value;
  };
  return {
    call, control, bucket: () => mf.getR2Bucket("BROWSER_BUFFER"),
    async put(item) { await (await mf.getR2Bucket("BROWSER_BUFFER")).put(item.key, item.body); },
    async restart() { await mf.dispose(); mf = new Miniflare(options); },
  };
}
async function drain(r, maxSteps = 30) {
  for (let i = 0; i < maxSteps; i++) {
    await r.call("/step");
    if (!(await (await r.bucket()).list()).objects.length) return i + 1;
  }
  throw new Error("Buffer did not drain within bounded test steps");
}

test("replay cannot start in buffer mode or without the sealed baseline", async t => {
  for (const settings of [{ BROWSER_INGRESS_MODE: "buffer" }, { BROWSER_BASELINE_ID: "empty" }, { BROWSER_BASELINE_SEQUENCE: "0" }]) {
    const r = await runtime(t, settings);
    await assert.rejects(r.call("/start"), /requires/);
    assert.equal((await r.call("/status")).enabled, false);
    assert.equal(r.control.calls.filter(call => call.stage === "beforeReceive").length, 0);
  }
});

test("retains the envelope until a fixed router publication fence is satisfied", async t => {
  const r = await runtime(t, { AUTO_PUBLISH: "false" }); const item = archived("one");
  await r.put(item); await r.call("/start");
  assert.equal((await r.call("/step")).counts.awaiting, 1);
  assert.ok(await (await r.bucket()).head(item.key));
  await r.call("/step");
  assert.equal((await r.call("/router")).calls, 1);
  assert.ok(await (await r.bucket()).head(item.key));
  await r.call("/publish"); await r.call("/step");
  assert.equal(await (await r.bucket()).head(item.key), null);
  assert.equal((await r.call("/status")).counts.verified, 1);
});

test("checkpoint and fixed publication fence survive a complete runtime restart", async t => {
  const r = await runtime(t, { AUTO_PUBLISH: "false" }); const item = archived("restart");
  await r.put(item); await r.call("/start"); await r.call("/step");
  await r.restart();
  assert.equal((await r.call("/status")).counts.awaiting, 1);
  await r.call("/publish"); await r.call("/step");
  assert.equal((await r.call("/router")).calls, 1);
  assert.equal(await (await r.bucket()).head(item.key), null);
});

test("ambiguous router acceptance retries stable source facts without duplicate events", async t => {
  const r = await runtime(t); const item = archived("ambiguous");
  r.control.stage = "afterReceive";
  await r.put(item); await r.call("/start"); await r.call("/step");
  assert.ok(await (await r.bucket()).head(item.key));
  assert.equal((await r.call("/router")).count, 1);
  await r.restart(); await drain(r);
  assert.equal((await r.call("/router")).count, 1);
  assert.equal((await r.call("/status")).counts.verified, 1);
});

test("ambiguous deletion resumes from its saved verified receipt", async t => {
  const r = await runtime(t); const item = archived("delete");
  r.control.stage = "afterDelete";
  await r.put(item); await r.call("/start"); await r.call("/step"); await r.call("/step");
  assert.equal(await (await r.bucket()).head(item.key), null);
  assert.equal((await r.call("/status")).counts.verified, 1);
  await r.restart(); await r.call("/step");
  assert.equal((await r.call("/status")).pending, 0);
  assert.equal((await r.call("/router")).calls, 1);
});

test("an already-published hash recreated by a late retry is removed without replaying", async t => {
  const r = await runtime(t); const item = archived("recreated");
  await r.put(item); await r.call("/start"); await drain(r);
  const before = await r.call("/router"); await r.put(item); await drain(r);
  assert.equal((await r.call("/router")).calls, before.calls);
  assert.equal((await r.call("/status")).counts.verified, 1);
});

test("a corrupted key or malformed envelope remains visible and does not block other keys", async t => {
  const r = await runtime(t); const good = archived("good"); const bad = archived("bad");
  await r.put(good); await r.put({ ...bad, body: "wrong hash" });
  const invalidBody = JSON.stringify({ schema_version: "invalid" });
  const invalid = { key: `jitsu/envelopes/${createHash("sha256").update(invalidBody).digest("hex")}.json`, body: invalidBody };
  await r.put(invalid); await r.call("/start"); await r.call("/step"); await r.call("/step");
  assert.equal(await (await r.bucket()).head(good.key), null);
  assert.ok(await (await r.bucket()).head(bad.key));
  assert.ok(await (await r.bucket()).head(invalid.key));
  assert.equal((await r.call("/status")).failures.length, 2);
});

test("late hash inserted behind the saved listing cursor is found on the next full sweep", async t => {
  const r = await runtime(t);
  const items = Array.from({ length: 151 }, (_, i) => archived(`many-${i}`)).sort((a, b) => a.key.localeCompare(b.key));
  const late = items.shift();
  await Promise.all(items.map(item => r.put(item))); await r.call("/start");
  const first = await r.call("/step"); assert.ok(first.cursor);
  await r.put(late);
  const steps = await drain(r, 25);
  assert.equal((await r.call("/router")).count, 151);
  assert.ok((await r.call("/status")).cycle >= 2);
  assert.ok(steps <= 20);
});

test("an old buffer request finishing after repeated empty sweeps is eventually replayed", async t => {
  const r = await runtime(t); const item = archived("old-in-flight");
  let release;
  const entered = new Promise(resolve => { r.control.entered = resolve; });
  r.control.stage = "oldBeforePut"; r.control.fail = false;
  r.control.pause = new Promise(resolve => { release = resolve; });
  const oldRequest = r.call("/old-buffer", item); await entered;
  await r.call("/start");
  for (let i = 0; i < 3; i++) assert.equal((await r.call("/step")).pending, 0);
  release(); assert.equal((await oldRequest).status, "buffered");
  await drain(r);
  assert.equal((await r.call("/router")).count, 1);
  assert.equal((await r.call("/status")).counts.verified, 1);
});

test("expired lease and an overlapping stale run cannot overwrite a new checkpoint", async t => {
  const r = await runtime(t); const item = archived("lease"); await r.put(item); await r.call("/start");
  let release;
  const entered = new Promise(resolve => { r.control.entered = resolve; });
  r.control.stage = "afterList"; r.control.fail = false;
  r.control.pause = new Promise(resolve => { release = resolve; });
  const stalled = r.call("/step"); await entered;
  const oldLease = (await r.call("/lease")).lease_id;
  await r.call("/expire"); await r.call("/step");
  assert.equal((await r.call("/status")).counts.awaiting, 1);
  release(); await stalled; await drain(r);
  assert.equal((await r.call("/router")).count, 1);
  assert.equal((await r.call("/status")).pending, 0);
  assert.ok(oldLease);
});

test("each run bounds listings to 100 and newly delivered envelopes to 25", async t => {
  const r = await runtime(t);
  await Promise.all(Array.from({ length: 130 }, (_, i) => r.put(archived(`bounded-${i}`))));
  await r.call("/start"); await r.call("/step");
  assert.equal((await r.call("/router")).count, 25);
  const listing = r.control.calls.find(call => call.stage === "afterList");
  assert.equal(listing.input.keys.length, 100);
  assert.equal(listing.input.truncated, true);
  const status = await r.call("/status");
  assert.equal(status.pending, 100);
  assert.equal(status.counts.awaiting, 25);
});


test("real alarms drain the current 42-envelope buffer scale without manual wakeups", async t => {
  const r = await runtime(t, { REAL_ALARMS: "true" });
  const objects = Array.from({ length: 42 }, (_, batch) => {
    const value = envelope(`volume-${batch}`);
    value.events = Array.from({ length: 40 }, (_, event) => {
      const row = envelope(`volume-${batch}-${event}`).events[0];
      const payload = JSON.parse(row.fact_payload);
      payload.original_field = "x".repeat(1_850);
      return { ...row, fact_payload: JSON.stringify(payload) };
    });
    const body = JSON.stringify(value);
    return { body, key: `jitsu/envelopes/${createHash("sha256").update(body).digest("hex")}.json` };
  });
  await Promise.all(objects.map(item => r.put(item)));
  const started = Date.now();
  await r.call("/start");
  let remaining = 42;
  while (remaining && Date.now() - started < 55_000) {
    await delay(250);
    remaining = (await (await r.bucket()).list()).objects.length;
  }
  const elapsedMs = Date.now() - started;
  assert.equal(remaining, 0);
  assert.equal((await r.call("/router")).count, 1680);
  assert.equal((await r.call("/status")).counts.verified, 42);
  t.diagnostic(JSON.stringify({ envelopes: objects.length, events: 1680, bytes: objects.reduce((sum, item) => sum + Buffer.byteLength(item.body), 0), elapsedMs }));
});


test("a failed listing discards the opaque cursor and resumes from the prefix", async t => {
  const r = await runtime(t);
  await Promise.all(Array.from({ length: 105 }, (_, i) => r.put(archived(`cursor-${i}`))));
  await r.call("/start");
  assert.ok((await r.call("/step")).cursor);
  r.control.stage = "beforeList";
  const failed = await r.call("/step");
  assert.equal(failed.cursor, null);
  assert.ok(failed.lastError);
  await drain(r);
  assert.equal((await r.call("/router")).count, 105);
});


test("repeated start preserves the active run's durable watchdog alarm", async t => {
  const r = await runtime(t, { REAL_ALARMS: "true" });
  await r.put(archived("restart-watchdog")); await r.call("/start");
  let release;
  const entered = new Promise(resolve => { r.control.entered = resolve; });
  r.control.stage = "afterList"; r.control.fail = false;
  r.control.pause = new Promise(resolve => { release = resolve; });
  const active = r.call("/step"); await entered;
  const lease = await r.call("/lease");
  const restarted = await r.call("/start");
  assert.equal(restarted.alarmAt, lease.lease_until);
  release(); await active; await drain(r);
  assert.equal((await r.call("/status")).pending, 0);
});
