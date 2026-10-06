import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";

const directory = dirname(fileURLToPath(import.meta.url));
const bundle = await build({ entryPoints: [join(directory, "test-worker.ts")], bundle: true, write: false, format: "esm", platform: "browser", target: "es2022", external: ["cloudflare:workers"] });

function input(id, visitor, version = 1, options = {}) {
  return {
    tenantId: "boom", source: "jitsu_data", kind: "page_view", ingestedAt: "2026-09-05T00:00:00Z",
    record: { id, anonymous_id: visitor, timestamp: "2026-09-01T00:00:00Z", loaded_at: `2026-09-02T00:00:${String(version).padStart(2, "0")}Z` },
    ...options,
  };
}

function fakeRemote() {
  return {
    sources: new Map(), records: new Map(), commits: new Map(), groups: new Map(), identities: new Map(), calls: [],
    baselineHeads: new Map(), baselineMembers: new Map(), baselineVisitors: new Map(),
    failAfter: null, failVisitor: null, partialRecords: false, partialSources: false, hideGroup: false, block: null, started: null,
    async handle(request) {
      const { method, input } = await request.json();
      this.calls.push({ method, input: structuredClone(input) });
      if (method === "loadSourceHeads") return Response.json(input.map(key => ({ key, heads: this.baselineHeads.get(JSON.stringify([key.event_kind, key.source_record_id])) ?? [] })));
      if (method === "loadMembers") return Response.json(input.flatMap(key => this.baselineMembers.has(key) ? [this.baselineMembers.get(key)] : []));
      if (method === "loadVisitor") return Response.json(this.baselineVisitors.get(input.visitorKey) ?? null);
      if (method === "readSources") return Response.json(input.flatMap(id => this.sources.has(id) ? [this.sources.get(id)] : []));
      if (method === "readRecords") return Response.json(this.records.get(input.publication_id) ?? []);
      if (method === "readCommit") return Response.json(this.commits.get(input.publication_id) ?? null);
      if (method === "readGroup") return Response.json(this.hideGroup ? null : this.groups.get(input.groupId) ?? null);
      if (method === "appendSources") {
        this.started?.();
        if (this.block) await this.block;
        for (const row of this.partialSources ? input.slice(0, 1) : input) this.sources.set(row.fact_id, structuredClone(row));
      }
      if (method === "identity") {
        for (const fact of input) {
          const previous = this.identities.get(fact.eventId);
          if (previous) assert.deepEqual(previous, fact);
          this.identities.set(fact.eventId, structuredClone(fact));
        }
      }
      if (method === "appendRecords") {
        const added = this.partialRecords ? input.slice(0, 1) : input;
        const id = input[0].publication_id;
        this.records.set(id, [...this.records.get(id) ?? [], ...structuredClone(added)]);
      }
      if (method === "appendCommit") this.commits.set(input.publication_id, structuredClone(input));
      if (method === "appendGroup") this.groups.set(input.group_id, structuredClone(input));
      if (method === this.failAfter && (!this.failVisitor || input.visitorKey === this.failVisitor)) {
        this.failAfter = null;
        return new Response(`Lost response after ${method}`, { status: 503 });
      }
      return Response.json(null);
    },
  };
}

async function runtime(t, remote = fakeRemote(), baseline = false) {
  const storage = await mkdtemp(join(tmpdir(), "browser-router-test-"));
  const publisher = { name: "browser-test", entrypoint: "TestPublisher" };
  const options = {
    ...convertV4MiniflareOptions({
      name: "browser-test", modules: true, script: bundle.outputFiles[0].text, compatibilityDate: "2026-09-05",
      durableObjects: { ROUTER: { className: "TestRouter", useSQLite: true }, VISITORS: { className: "TestVisitor", useSQLite: true }, IDENTITY: { className: "TestIdentity", useSQLite: true } },
      durableObjectsPersist: storage,
      bindings: { TENANT_ID: "boom", BROWSER_FLUSH_DELAY_MS: "3600000", SESSION_FLUSH_DELAY_MS: "3600000", ...(baseline ? { BROWSER_BASELINE_ID: "seed-verified", BROWSER_BASELINE_SEQUENCE: "10" } : {}) },
      serviceBindings: { BROWSER_SOURCE_PUBLISHER: publisher, SESSION_PUBLISHER: publisher, GROUP_PUBLISHER: publisher, TEST_CONTROL: request => remote.handle(request), ...(baseline ? { BROWSER_BASELINE: publisher, SESSION_BASELINE: publisher } : {}) },
    }),
    unsafeInspectDurableObjects: true,
    // Miniflare 5's compatibility converter drops the old durableObjectsPersist option.
    isolatedResourcePersistencePath: storage,
    resourcePersistencePath: storage,
  };
  let mf = new Miniflare(options);
  t.after(async () => { await mf.dispose(); await rm(storage, { recursive: true, force: true }); });
  async function call(path, body) {
    const response = await mf.dispatchFetch(`https://test.invalid${path}`, { method: body === undefined ? "GET" : "POST", body: body === undefined ? undefined : JSON.stringify(body) });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error);
    return result;
  }
  async function visible(groups = [...remote.groups.values()], records = [...remote.records.values()].flat()) { return call("/visible", { groups, records }); }
  async function evict() { await mf.unsafeEvictDurableObject("browser-test", "TestRouter", { name: "boom" }); }
  async function restart() { await mf.dispose(); mf = new Miniflare(options); }
  return { mf, remote, call, visible, evict, restart };
}

function pageCount(view) { return Object.values(view.visitors).flat().reduce((sum, session) => sum + session.page_view_count, 0); }

test("durable receive survives restart and delivers raw, identity and one complete session group", async (t) => {
  const { call, visible, evict, remote } = await runtime(t);
  assert.equal((await call("/receive", [input("page-a", "visitor-a")])).accepted, 1);
  await evict();
  assert.equal((await call("/status")).pending, 1);
  assert.ok((await call("/status")).alarmAt > Date.now());
  assert.equal((await call("/run")).publishedSequence, 1);
  assert.equal(remote.sources.size, 1);
  assert.equal(remote.identities.size, 1);
  const view = await visible();
  assert.equal(view.sequence, 1);
  assert.equal(pageCount(view), 1);
  assert.equal(view.visitors["visitor-a"][0].first_page_view_id, "page-a");
});

test("duplicate deliveries and newer arrival timestamps never enqueue new logical work", async (t) => {
  const { call, remote } = await runtime(t);
  const page = input("page-a", "visitor-a");
  await call("/receive", [page]);
  await call("/run");
  const retry = await call("/receive", [{ ...page, ingestedAt: "2026-09-06T00:00:00Z" }]);
  assert.equal(retry.accepted, 0);
  assert.equal((await call("/status")).pending, 0);
  assert.equal(remote.identities.size, 1);
});

test("cross-visitor move stays invisible after ambiguous second prepare, then switches both visitors together", async (t) => {
  const { call, visible, remote, evict, mf } = await runtime(t);
  await call("/receive", [input("page-a", "visitor-a")]);
  await call("/run");
  remote.failAfter = "afterPrepare";
  remote.failVisitor = "visitor-b";
  await call("/receive", [input("page-a", "visitor-b", 2)]);
  const failed = await call("/run");
  assert.equal(failed.publishedSequence, 1);
  assert.match(failed.lastError, /Lost response/);
  assert.equal((await visible()).visitors["visitor-a"].length, 1);
  assert.equal((await visible()).visitors["visitor-b"], undefined);
  await evict();
  await mf.unsafeEvictDurableObject("browser-test", "TestVisitor", { name: JSON.stringify(["boom", "visitor-b"]) });
  assert.equal((await call("/run")).publishedSequence, 2);
  const view = await visible();
  assert.deepEqual(view.visitors["visitor-a"], []);
  assert.equal(view.visitors["visitor-b"][0].first_page_view_id, "page-a");
  assert.equal(pageCount(view), 1);
});

test("stale replay and source downgrade cannot resurrect a page under the old visitor", async (t) => {
  const { call, visible, remote } = await runtime(t);
  await call("/receive", [input("page-a", "visitor-a")]); await call("/run");
  await call("/receive", [input("page-a", "visitor-b", 2)]); await call("/run");
  await call("/receive", [input("page-a", "visitor-a", 3, { source: "boom_domains" })]); await call("/run");
  await call("/receive", [input("page-a", "visitor-a")]);
  const view = await visible();
  assert.deepEqual(view.visitors["visitor-a"], []);
  assert.equal(pageCount(view), 1);
  assert.equal(remote.sources.size, 3);
  assert.equal(remote.identities.size, 2);
});

test("source conflicts are retained and quarantined without replacing selected heads", async (t) => {
  const { call, visible, remote } = await runtime(t);
  await call("/receive", [input("page-a", "visitor-a")]); await call("/run");
  const conflict = await call("/receive", [input("page-a", "visitor-b")]);
  assert.equal(conflict.quarantined, 1);
  await call("/run");
  assert.equal(remote.sources.size, 2);
  assert.ok([...remote.sources.values()].some(row => row.rejection));
  assert.equal((await visible()).visitors["visitor-a"][0].first_page_view_id, "page-a");
});

for (const failure of ["appendSources", "identity", "appendRecords", "appendCommit", "appendGroup"]) {
  test(`ambiguous ${failure} retries the saved delivery after interruption`, async (t) => {
    const { call, evict, restart, remote, visible } = await runtime(t);
    remote.failAfter = failure;
    await call("/receive", [input("page-a", "visitor-a")]);
    assert.equal((await call("/run")).pending, 1);
    // Miniflare graceful eviction retains an active reference after a failed nested DO RPC.
    // Restart workerd against the same SQLite directory to test recovery in this case.
    if (failure === "identity") await restart();
    else await evict();
    assert.equal((await call("/run")).pending, 0);
    assert.equal(remote.sources.size, 1);
    assert.equal(remote.identities.size, 1);
    assert.equal(remote.groups.size, 1);
    assert.equal(pageCount(await visible()), 1);
  });
}

test("partial snapshot rows prevent group commit until actual complete rows are visible", async (t) => {
  const { call, remote, visible } = await runtime(t);
  const early = input("early", "visitor-a");
  const late = input("late", "visitor-a"); late.record.timestamp = "2026-09-01T01:00:00Z";
  remote.partialRecords = true;
  await call("/receive", [early, late]);
  assert.equal((await call("/run")).pending, 1);
  assert.equal(remote.groups.size, 0);
  remote.partialRecords = false;
  await call("/run");
  assert.equal((await visible()).visitors["visitor-a"].length, 2);
});

test("reader delays an entire move and all later groups if one earlier group member is not visible", async (t) => {
  const { call, visible, remote } = await runtime(t);
  await call("/receive", [input("page-a", "visitor-a")]); await call("/run");
  await call("/receive", [input("page-a", "visitor-b", 2)]); await call("/run");
  await call("/receive", [input("page-c", "visitor-b", 3)]); await call("/run");
  const move = [...remote.groups.values()].find(group => group.sequence === 2);
  const delayed = move.members.find(member => member.visitor_key === "visitor-b").publication_id;
  const records = [...remote.records.values()].flat().filter(row => row.publication_id !== delayed);
  const view = await visible([...remote.groups.values()], records);
  assert.equal(view.sequence, 1);
  assert.equal(pageCount(view), 1);
  assert.equal(view.visitors["visitor-b"], undefined);
  const recovered = await visible();
  assert.equal(recovered.sequence, 3);
  assert.deepEqual(recovered.visitors["visitor-a"], []);
  assert.equal(pageCount(recovered), 2);
});

test("deletion commits an empty visitor snapshot and historical replay cannot undo the tombstone", async (t) => {
  const { call, visible } = await runtime(t);
  await call("/receive", [input("page-a", "visitor-a")]); await call("/run");
  await call("/receive", [input("page-a", "visitor-a", 2, { deleted: true })]); await call("/run");
  await call("/receive", [input("page-a", "visitor-a", 4, { source: "boom_domains" })]); await call("/run");
  assert.deepEqual((await visible()).visitors["visitor-a"], []);
});

test("new input during publication remains queued behind the immutable active group", async (t) => {
  const { call, remote, visible } = await runtime(t);
  let unblock;
  const started = new Promise(resolve => { remote.started = resolve; });
  remote.block = new Promise(resolve => { unblock = resolve; });
  await call("/receive", [input("page-a", "visitor-a")]);
  const publishing = call("/run");
  await started;
  await call("/receive", [input("page-a", "visitor-b", 2)]);
  assert.equal((await call("/status")).pending, 2);
  remote.block = null; unblock();
  assert.equal((await publishing).publishedSequence, 1);
  assert.equal((await visible()).visitors["visitor-a"].length, 1);
  await call("/run");
  assert.deepEqual((await visible()).visitors["visitor-a"], []);
  assert.equal(pageCount(await visible()), 1);
});

test("grouped visitor rejects direct writes and stale or conflicting prepare requests", async (t) => {
  const { call, remote } = await runtime(t);
  await call("/receive", [input("page-a", "visitor-a")]); await call("/run");
  const first = remote.calls.find(call => call.method === "beforePrepare").input;
  assert.equal((await call("/prepare", first)).revision, "1");
  await assert.rejects(call("/direct-receive", { visitor: "visitor-a", revisions: [] }), /controlled by BrowserRouter/);
  await assert.rejects(call("/prepare", { ...first, revisions: [] }), /Conflicting session group retry/);
  await assert.rejects(call("/prepare", { ...first, groupId: "different" }), /Stale session group sequence/);
});

test("constructor restores a missing lease watchdog and expired leases retry after restart", async (t) => {
  const { call, evict } = await runtime(t);
  await call("/receive", [input("page-a", "visitor-a")]);
  await call("/orphan");
  assert.equal((await call("/status")).alarmAt, null);
  await evict();
  assert.ok((await call("/status")).alarmAt > Date.now());
  assert.equal((await call("/run")).publishedSequence, 0);
  await call("/expire");
  assert.equal((await call("/run")).publishedSequence, 1);
});

test("a page can move back at a newer revision without resurrecting either stale visitor head", async (t) => {
  const { call, visible } = await runtime(t);
  await call("/receive", [input("page-a", "visitor-a")]); await call("/run");
  await call("/receive", [input("page-a", "visitor-b", 2)]); await call("/run");
  await call("/receive", [input("page-a", "visitor-a", 3)]); await call("/run");
  await call("/receive", [input("page-a", "visitor-b", 2)]);
  assert.equal(pageCount(await visible()), 1);
  assert.deepEqual((await visible()).visitors["visitor-b"], []);
});

test("two pages swapping visitors in one batch are published in one group", async (t) => {
  const { call, visible, remote } = await runtime(t);
  await call("/receive", [input("page-a", "visitor-a"), input("page-b", "visitor-b")]); await call("/run");
  await call("/receive", [input("page-a", "visitor-b", 2), input("page-b", "visitor-a", 2)]); await call("/run");
  const view = await visible();
  assert.equal(view.visitors["visitor-a"][0].first_page_view_id, "page-b");
  assert.equal(view.visitors["visitor-b"][0].first_page_view_id, "page-a");
  assert.equal(pageCount(view), 2);
  assert.equal([...remote.groups.values()].at(-1).members.length, 2);
});

test("superseded prepare retries fail without changing the visitor's newer state", async (t) => {
  const { call, remote } = await runtime(t);
  await call("/receive", [input("page-a", "visitor-a")]); await call("/run");
  const first = remote.calls.find(call => call.method === "beforePrepare").input;
  await call("/receive", [input("page-a", "visitor-b", 2)]); await call("/run");
  await assert.rejects(call("/prepare", first), /superseded after publication/);
});

test("source IDs shared across event kinds retain distinct heads and identity keys", async (t) => {
  const { call, remote, visible } = await runtime(t);
  const page = input("same-id", "visitor-a");
  const identify = input("same-id", "visitor-a", 1, { kind: "identify" });
  identify.record.email = "one@example.com";
  await call("/receive", [page, identify]); await call("/run");
  assert.equal(remote.identities.size, 2);
  assert.deepEqual([...remote.identities.values()].map(fact => fact.factKey).sort(), ["segment_identify:same-id", "segment_page_view:same-id"]);
  assert.equal((await call("/heads")).length, 2);
  assert.equal(pageCount(await visible()), 1);
});

test("live Jitsu outranks later historical updates while retaining both source facts", async (t) => {
  const { call, remote, visible } = await runtime(t);
  const jitsu = { tenant_id: "boom", message_id: "same-id", event_kind: "page_view", delivery_event_id: "delivery-1", producer_id: "jitsu", observed_at: "2026-09-01T00:00:00Z", ingested_at: "2026-09-05T00:00:00Z", source_fact_version: 1, source_deleted: 0,
    fact_payload: JSON.stringify({ type: "page", messageId: "same-id", anonymousId: "live-visitor", timestamp: "2026-09-01T00:00:00Z" }) };
  await call("/receive", [{ jitsu }]); await call("/run");
  await call("/receive", [input("same-id", "historical-visitor", 4)]); await call("/run");
  assert.equal(remote.sources.size, 2);
  assert.equal(remote.identities.size, 1);
  assert.equal((await visible()).visitors["live-visitor"][0].first_page_view_id, "same-id");
  assert.equal((await visible()).visitors["historical-visitor"], undefined);
});

test("reader rejects corrupt actual payloads even when supplied hash columns still match", async (t) => {
  const { call, visible, remote } = await runtime(t);
  await call("/receive", [input("page-a", "visitor-a")]); await call("/run");
  const rows = structuredClone([...remote.records.values()].flat());
  rows[0].payload_json = JSON.stringify({ visitor_key: "wrong" });
  await assert.rejects(visible([...remote.groups.values()], rows), /payload hash mismatch/);
});

test("incomplete raw ingestion cannot acknowledge identity or session delivery", async (t) => {
  const { call, remote } = await runtime(t);
  remote.partialSources = true;
  await call("/receive", [input("page-a", "visitor-a"), input("page-b", "visitor-b")]);
  assert.match((await call("/run")).lastError, /not.*fully visible/);
  assert.equal(remote.identities.size, 0);
  assert.equal(remote.groups.size, 0);
  remote.partialSources = false;
  assert.equal((await call("/run")).publishedSequence, 1);
});

test("successful append with delayed group visibility remains pending until verified readback", async (t) => {
  const { call, remote } = await runtime(t);
  remote.hideGroup = true;
  await call("/receive", [input("page-a", "visitor-a")]);
  assert.equal((await call("/run")).publishedSequence, 0);
  assert.equal(remote.groups.size, 1);
  remote.hideGroup = false;
  assert.equal((await call("/run")).publishedSequence, 1);
  assert.equal(remote.calls.filter(call => call.method === "appendGroup").length, 1);
});

test("unknown event kinds retain source facts without producing identity or session state", async (t) => {
  const { call, remote, visible } = await runtime(t);
  await call("/receive", [input("event-a", "visitor-a", 1, { kind: "unknown_kind" })]);
  await call("/run");
  assert.equal(remote.sources.size, 1);
  assert.equal(remote.identities.size, 0);
  assert.deepEqual(await visible(), { sequence: 1, visitors: {} });
});

test("authoritative read plans switch both visitors together without reading group history", async (t) => {
  const { call, remote } = await runtime(t);
  await call("/receive", [input("page-a", "visitor-a")]); await call("/run");
  const before = await call("/plan", ["visitor-a", "visitor-b"]);
  await call("/receive", [input("page-a", "visitor-b", 2)]); await call("/run");
  const after = await call("/plan", ["visitor-a", "visitor-b"]);
  assert.equal(before.sequence, 1); assert.equal(after.sequence, 2);
  const records = [...remote.records.values()].flat();
  assert.equal((await call("/read-plan", { plan: before, records }))["visitor-a"].length, 1);
  const current = await call("/read-plan", { plan: after, records });
  assert.deepEqual(current["visitor-a"], []);
  assert.equal(current["visitor-b"].length, 1);
  const newId = after.members.find(member => member.visitor_key === "visitor-b").publication_id;
  await assert.rejects(call("/read-plan", { plan: after, records: records.filter(row => row.publication_id !== newId) }), /not.*fully visible/);
});

test("historical baseline hydrates only touched keys and visitors before a live page move", async (t) => {
  const { call, remote, mf, evict } = await runtime(t, fakeRemote(), true);
  const fixture = await call("/make-seed", { visitor: "historical-a", inputs: [input("old-page", "historical-a")] });
  remote.baselineHeads.set(JSON.stringify(["page_view", "old-page"]), fixture.events);
  remote.baselineMembers.set("historical-a", fixture.member);
  remote.baselineVisitors.set("historical-a", fixture.seed);
  remote.records.set(fixture.member.publication_id, fixture.records);
  assert.deepEqual(await mf.listDurableObjectIds("TestVisitor", "browser-test"), []);
  const baselinePlan = await call("/plan", ["historical-a", "live-b"]);
  assert.equal(baselinePlan.sequence, 10);
  await call("/receive", [input("old-page", "live-b", 2)]);
  await evict();
  assert.equal((await call("/run")).publishedSequence, 11);
  const plan = await call("/plan", ["historical-a", "live-b"]);
  const view = await call("/read-plan", { plan, records: [...remote.records.values()].flat() });
  assert.deepEqual(view["historical-a"], []);
  assert.equal(view["live-b"][0].first_page_view_id, "old-page");
  assert.equal(plan.members.find(member => member.visitor_key === "historical-a").revision, "2");
  assert.equal(remote.calls.filter(call => call.method === "loadSourceHeads").length, 1);
  await call("/receive", [input("old-page", "historical-a")]);
  assert.equal(remote.calls.filter(call => call.method === "loadSourceHeads").length, 1);
});

test("baseline snapshot mismatch blocks hydration without losing the queued correction", async (t) => {
  const { call, remote } = await runtime(t, fakeRemote(), true);
  const fixture = await call("/make-seed", { visitor: "historical-a", inputs: [input("old-page", "historical-a")] });
  remote.baselineHeads.set(JSON.stringify(["page_view", "old-page"]), fixture.events);
  remote.baselineVisitors.set("historical-a", { ...fixture.seed, heads: [] });
  await call("/receive", [input("old-page", "live-b", 2)]);
  assert.match((await call("/run")).lastError, /Baseline pages do not match/);
  assert.equal((await call("/status")).pending, 1);
  remote.baselineVisitors.set("historical-a", fixture.seed);
  assert.equal((await call("/run")).publishedSequence, 11);
});

test("large raw payloads use bounded outbox rows and never enter permanent routing heads", async (t) => {
  const { call } = await runtime(t);
  const event = input("large-page", "visitor-a");
  event.record.unused_raw_context = "界".repeat(60000);
  await call("/receive", [event]);
  const sizes = await call("/sizes");
  t.diagnostic(`Large raw observation storage bytes: ${JSON.stringify(sizes)}`);
  assert.ok(sizes.outboxChunk > 0 && sizes.outboxChunk <= 98304, JSON.stringify(sizes));
  assert.equal(sizes.sourceFingerprint, 64);
  assert.ok(sizes.logicalHead < 10000, JSON.stringify(sizes));
  assert.equal((await call("/run")).publishedSequence, 1);
});

test("oversized normalized batches reject before acknowledging any observation", async (t) => {
  const { call } = await runtime(t);
  const event = input("huge-page", "visitor-a");
  event.record.unused_raw_context = "x".repeat(600000);
  await assert.rejects(call("/receive", [event]), /exceeds 512000 bytes/);
  const status = await call("/status");
  assert.equal(status.pending, 0);
  assert.equal(status.retainedFacts, 0);
});

test("batch splitting respects actual UTF-8 bytes and retains every observation", async (t) => {
  const { call } = await runtime(t);
  const events = [input("a", "visitor-a"), input("b", "visitor-b"), input("c", "visitor-c")];
  for (const event of events) event.record.unused = "界".repeat(90000);
  const batches = await call("/split", events);
  assert.equal(batches.length, 3);
  assert.deepEqual(batches.flatMap(batch => batch.ids), ["a", "b", "c"]);
  assert.ok(batches.every(batch => batch.bytes <= 512000));
});

test("1213-session preparation stores snapshots in bounded SQLite rows", async (t) => {
  const { call } = await runtime(t);
  const revisions = Array.from({ length: 1213 }, (_, index) => ({
    page_view_id: `page-${index}`, source_priority: 2, source_revision: "1",
    page: { page_view_id: `page-${index}`, visitor_key: "largest-visitor", page_view_timestamp: new Date(Date.UTC(2026, 0, 1) + index * 3600000).toISOString() },
  }));
  const snapshot = await call("/prepare", { groupId: "large-prepare", sequence: 1, visitorKey: "largest-visitor", revisions });
  assert.equal(snapshot.sessions.length, 1213);
  const sizes = await call("/visitor-sizes", { visitor: "largest-visitor" });
  t.diagnostic(`1213-session prepared snapshot storage: ${JSON.stringify(sizes)}`);
  assert.ok(sizes.chunks > 1);
  assert.ok(sizes.max_bytes <= 98304, JSON.stringify(sizes));
});
