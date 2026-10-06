import assert from "node:assert/strict";
import { test } from "node:test";
import { createHash } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { build } from "esbuild";
const directory = dirname(fileURLToPath(import.meta.url));
const bundled = await build({
  entryPoints: [join(directory, "adapter-test-worker.ts")], bundle: true, write: false,
  format: "esm", platform: "browser", target: "es2022", external: ["cloudflare:workers"],
});
const script = bundled.outputFiles[0].text;

function page(id, time = "00:00:00", visitor = "visitor-1") {
  return {
    page_view_id: id, source_priority: 3, source_revision: "1",
    page: { page_view_id: id, visitor_key: visitor, page_view_timestamp: `2026-09-01T${time}Z` },
  };
}

function fakePublisher() {
  return {
    records: new Map(), commits: new Map(), calls: [], failAfter: null, partialAppend: false,
    corruptRead: false, blockAppend: null, appendStarted: null,
    async handle(request) {
      const { method, input } = await request.json();
      this.calls.push({ method, input: structuredClone(input) });
      if (method === "readRecords") {
        const records = structuredClone(this.records.get(input.publication_id) ?? []);
        if (this.corruptRead && records.length) records[0].payload_json = "{\"corrupt\":true}";
        return Response.json(records);
      }
      if (method === "readCommit") return Response.json(this.commits.get(input.publication_id) ?? null);
      if (method === "appendRecords") {
        this.appendStarted?.();
        if (this.blockAppend) await this.blockAppend;
        const publicationId = input[0].publication_id;
        const added = this.partialAppend ? input.slice(0, 1) : input;
        this.records.set(publicationId, [...this.records.get(publicationId) ?? [], ...structuredClone(added)]);
      }
      if (method === "appendCommit") this.commits.set(input.publication_id, structuredClone(input));
      if (this.failAfter === method) {
        this.failAfter = null;
        return new Response("Simulated response lost after durable remote write", { status: 503 });
      }
      return Response.json(null);
    },
  };
}

async function runtime(t, remote = fakePublisher(), flushDelay = "60000") {
  const storage = await mkdtemp(join(tmpdir(), "session-adapter-test-"));
  const options = {
    name: "session-test", modules: true, script, compatibilityDate: "2026-09-05",
    durableObjects: { VISITORS: { className: "TestVisitorSessions", useSQLite: true } },
    durableObjectsPersist: storage,
    bindings: { TENANT_ID: "boom", SESSION_FLUSH_DELAY_MS: flushDelay },
    serviceBindings: {
      SESSION_PUBLISHER: { name: "session-test", entrypoint: "TestPublisher" },
      TEST_CONTROL: (request) => remote.handle(request),
    },
  };
  const miniflare = new Miniflare({ ...convertV4MiniflareOptions(options), unsafeInspectDurableObjects: true });
  t.after(async () => { await miniflare.dispose(); await rm(storage, { recursive: true, force: true }); });
  async function call(path, body, name = "visitor-1") {
    const response = await miniflare.dispatchFetch(`https://test.invalid${path}?name=${encodeURIComponent(name)}`, {
      method: body ? "POST" : "GET", body: body ? JSON.stringify(body) : undefined,
    });
    if (!response.ok) throw new Error(await response.text());
    return response.json();
  }
  return { miniflare, remote, call };
}

test("SQLite transaction persists pages, revision, outbox, and alarm through eviction", async (t) => {
  const { miniflare, remote, call } = await runtime(t);
  const accepted = await call("/receive", { visitor: "visitor-1", revisions: [page("a")] });
  assert.equal(accepted.revision, "1");
  assert.equal(accepted.publicationQueued, true);
  const before = await call("/status");
  assert.equal(before.pendingSnapshots, 1);
  assert.equal(before.pageHeads, 1);
  assert.ok(before.alarmAt > Date.now());
  await miniflare.unsafeEvictDurableObject("session-test", "TestVisitorSessions", { name: "visitor-1" });
  const after = await call("/status");
  assert.equal(after.pendingSnapshots, 1);
  assert.equal(after.revision, "1");
  assert.equal(after.pageHeads, 1);
  const delivered = await call("/run");
  assert.equal(delivered.publishedRevision, "1");
  assert.equal(delivered.pendingSnapshots, 0);
  assert.equal(delivered.alarmAt, null);
  const [commit] = remote.commits.values();
  const [record] = remote.records.get(commit.publication_id);
  assert.equal(commit.row_count, 1);
  assert.equal(record.tenant_id, "boom");
  assert.equal(record.payload_hash, createHash("sha256").update(record.payload_json).digest("hex"));
  assert.equal(JSON.parse(record.payload_json).first_page_view_id, "a");
});

test("same source revision retries and unchanged higher revisions do not enqueue snapshots", async (t) => {
  const { call } = await runtime(t);
  const a = page("a");
  await call("/receive", { visitor: "visitor-1", revisions: [a] });
  const retry = await call("/receive", { visitor: "visitor-1", revisions: [a] });
  assert.equal(retry.revision, "1");
  assert.equal(retry.publicationQueued, false);
  const newer = await call("/receive", { visitor: "visitor-1", revisions: [{ ...a, source_revision: "2" }] });
  assert.equal(newer.publicationQueued, false);
  assert.equal((await call("/status")).pendingSnapshots, 1);
});

test("ambiguous row upload retries read actual rows and avoid another append", async (t) => {
  const { call, remote } = await runtime(t);
  remote.failAfter = "appendRecords";
  await call("/receive", { visitor: "visitor-1", revisions: [page("a")] });
  const failed = await call("/run");
  assert.equal(failed.pendingSnapshots, 1);
  assert.equal(failed.attempts, 1);
  assert.equal(remote.commits.size, 0);
  const completed = await call("/run");
  assert.equal(completed.pendingSnapshots, 0);
  assert.equal(remote.commits.size, 1);
  assert.equal(remote.calls.filter((call) => call.method === "appendRecords").length, 1);
});

test("ambiguous commit retries preserve the exact manifest and do not republish", async (t) => {
  const { call, remote } = await runtime(t);
  remote.failAfter = "appendCommit";
  await call("/receive", { visitor: "visitor-1", revisions: [page("a")] });
  assert.equal((await call("/run")).pendingSnapshots, 1);
  const saved = structuredClone([...remote.commits.values()][0]);
  assert.equal((await call("/run")).publishedRevision, "1");
  assert.deepEqual([...remote.commits.values()][0], saved);
  assert.equal(remote.calls.filter((call) => call.method === "appendCommit").length, 1);
});

test("partial visibility never commits and duplicate retry rows validate by actual payload", async (t) => {
  const { call, remote } = await runtime(t);
  remote.partialAppend = true;
  await call("/receive", { visitor: "visitor-1", revisions: [page("a"), page("b", "01:00:00")] });
  const failed = await call("/run");
  assert.equal(failed.pendingSnapshots, 1);
  assert.match(failed.lastError, /not.*fully visible/);
  assert.equal(remote.commits.size, 0);
  remote.partialAppend = false;
  assert.equal((await call("/run")).pendingSnapshots, 0);
  const [commit] = remote.commits.values();
  assert.equal(commit.row_count, 2);
  assert.equal(remote.records.get(commit.publication_id).length, 3);
});

test("changed payload with an unchanged claimed hash never commits", async (t) => {
  const { call, remote } = await runtime(t);
  remote.corruptRead = true;
  await call("/receive", { visitor: "visitor-1", revisions: [page("a")] });
  const failed = await call("/run");
  assert.equal(failed.pendingSnapshots, 1);
  assert.match(failed.lastError, /unexpected or conflicting row/);
  assert.equal(remote.commits.size, 0);
});

test("deleting the last page publishes an explicit zero-row revision", async (t) => {
  const { call, remote } = await runtime(t);
  await call("/receive", { visitor: "visitor-1", revisions: [page("a")] });
  await call("/run");
  const deleted = { ...page("a"), page: null, source_revision: "2" };
  await call("/receive", { visitor: "visitor-1", revisions: [deleted] });
  const completed = await call("/run");
  assert.equal(completed.publishedRevision, "2");
  assert.equal(completed.sessions, 0);
  assert.equal(completed.pageHeads, 1);
  assert.deepEqual([...remote.commits.values()].map((commit) => [commit.revision, commit.row_count]), [["1", 1], ["2", 0]]);
  assert.equal(remote.calls.filter((call) => call.method === "appendRecords").length, 1);
  const replay = await call("/receive", { visitor: "visitor-1", revisions: [page("a")] });
  assert.equal(replay.publicationQueued, false);
  assert.equal(replay.sessionCount, 0);
});

test("new input during pending publication preserves each complete revision", async (t) => {
  const { call, remote } = await runtime(t);
  remote.failAfter = "appendRecords";
  await call("/receive", { visitor: "visitor-1", revisions: [page("a")] });
  await call("/run");
  const second = await call("/receive", { visitor: "visitor-1", revisions: [page("b", "00:10:00")] });
  assert.equal(second.revision, "2");
  assert.equal((await call("/status")).pendingSnapshots, 2);
  assert.equal((await call("/run")).publishedRevision, "1");
  assert.equal((await call("/run")).publishedRevision, "2");
  const commits = [...remote.commits.values()];
  const counts = commits.map((commit) => JSON.parse(remote.records.get(commit.publication_id)[0].payload_json).page_view_count);
  assert.deepEqual(counts, [1, 2]);
});

test("new input can arrive during remote I/O without changing the leased payload", async (t) => {
  const { call, remote } = await runtime(t);
  let release;
  remote.blockAppend = new Promise((resolve) => { release = resolve; });
  const started = new Promise((resolve) => { remote.appendStarted = resolve; });
  await call("/receive", { visitor: "visitor-1", revisions: [page("a")] });
  const active = call("/run");
  await started;
  const second = await call("/receive", { visitor: "visitor-1", revisions: [page("b", "00:10:00")] });
  assert.equal(second.revision, "2");
  await call("/run");
  assert.equal(remote.calls.filter((call) => call.method === "appendRecords").length, 1);
  release();
  assert.equal((await active).publishedRevision, "1");
  remote.blockAppend = null;
  assert.equal((await call("/run")).publishedRevision, "2");
  const [first] = remote.commits.values();
  assert.equal(JSON.parse(remote.records.get(first.publication_id)[0].payload_json).page_view_count, 1);
});

test("misrouting and conflicting revisions leave all durable state unchanged", async (t) => {
  const { call } = await runtime(t);
  await call("/receive", { visitor: "visitor-1", revisions: [page("a")] });
  await assert.rejects(call("/receive", { visitor: "other", revisions: [page("b", "00:00:00", "other")] }), /already bound/);
  await assert.rejects(call("/receive", { visitor: "visitor-1", revisions: [page("b", "00:00:00", "other")] }), /Misrouted/);
  await assert.rejects(call("/receive", { visitor: "visitor-1", revisions: [page("b"), page("a", "00:01:00")] }), /Conflicting payload/);
  const state = await call("/status");
  assert.equal(state.pageHeads, 1);
  assert.equal(state.revision, "1");
  assert.equal(state.pendingSnapshots, 1);
});

test("failure after local page writes rolls the complete receipt transaction back", async (t) => {
  const { miniflare, call } = await runtime(t);
  await call("/status");
  const storage = await miniflare.unsafeGetDurableObjectStorage("session-test", "TestVisitorSessions", { name: "visitor-1" });
  await storage.exec("CREATE TRIGGER reject_outbox BEFORE INSERT ON session_outbox BEGIN SELECT RAISE(ABORT, 'Injected outbox failure'); END");
  await assert.rejects(call("/receive", { visitor: "visitor-1", revisions: [page("a")] }), /Injected outbox failure/);
  const state = await call("/status");
  assert.equal(state.revision, "0");
  assert.equal(state.pageHeads, 0);
  assert.equal(state.sessions, 0);
  assert.equal(state.pendingSnapshots, 0);
  assert.equal(state.alarmAt, null);
});

test("empty bootstrap still publishes a zero-row snapshot", async (t) => {
  const { call, remote } = await runtime(t);
  const accepted = await call("/receive", { visitor: "visitor-1", revisions: [] });
  assert.equal(accepted.revision, "1");
  assert.equal(accepted.publicationQueued, true);
  assert.equal((await call("/run")).publishedRevision, "1");
  assert.equal([...remote.commits.values()][0].row_count, 0);
  assert.equal(remote.calls.filter((call) => call.method === "appendRecords").length, 0);
});

test("a scheduled runtime alarm publishes without an explicit drain request", async (t) => {
  const { call, remote } = await runtime(t, fakePublisher(), "10");
  await call("/receive", { visitor: "visitor-1", revisions: [page("a")] });
  const deadline = Date.now() + 5000;
  let status = await call("/status");
  while (status.publishedRevision !== "1" && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 10));
    status = await call("/status");
  }
  assert.equal(status.publishedRevision, "1");
  assert.equal(status.pendingSnapshots, 0);
  assert.equal(remote.commits.size, 1);
});
