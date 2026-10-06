import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { Miniflare, convertV4MiniflareOptions } from "../../../tinybird-v1/node_modules/miniflare/dist/src/index.js";
import { build } from "../../../tinybird-v1/node_modules/esbuild/lib/main.js";

const output = await build({ entryPoints: [fileURLToPath(new URL("./publication-worker.ts", import.meta.url))],
  bundle: true, write: false, format: "esm", platform: "browser", target: "es2022", external: ["cloudflare:workers", "node:crypto"] });
const baselineId = `b_${"a".repeat(24)}`;
function replacement(id, email, phone, version = 1, rows) {
  return { source: "activecampaign", source_account: "default", scope_id: id,
    replacement_id: `${id}:${version}`, observed_at: "2026-09-05T23:00:00Z", observation_sequence: version,
    evidence_inbox_ids: [], source_evidence: {}, rows: rows ?? [{ form_submission_id: id,
      email, phone, first_name: "Test", last_name: "", occurred_at: "2026-09-05T22:00:00Z", is_deleted: false }] };
}
async function runtime(t) {
  const storage = await mkdtemp(join(tmpdir(), "realtime-publication-test-"));
  const options = { name: "publication-test", script: output.outputFiles[0].text,
    modules: true, compatibilityFlags: ["nodejs_compat"], compatibilityDate: "2026-09-05",
    durableObjects: { SOURCE_COORDINATOR: { className: "SourceCoordinator", useSQLite: true }, PUBLICATION: { className: "Publication", useSQLite: true }, BROWSER_SOURCE: { className: "BrowserSource", useSQLite: true } },
    r2Buckets: ["BROWSER_BUFFER"], durableObjectsPersist: storage };
  const mf = new Miniflare({ ...convertV4MiniflareOptions(options), unsafeInspectDurableObjects: true });
  t.after(async () => { await mf.dispose(); await rm(storage, { recursive: true, force: true }); });
  const call = async (path, body = {}) => {
    const result = await mf.dispatchFetch(`https://test.invalid${path}`, { method: "POST", body: JSON.stringify(body) });
    const value = await result.json();
    if (!result.ok) throw new Error(value.error);
    return value;
  };
  async function batch() {
    const manifest = await call("/prepare", { baselineId });
    const rows = [];
    while (rows.length < manifest.rowCount) {
      const page = await call("/chunk", { batchId: manifest.batchId, offset: rows.length });
      rows.push(...page.rows);
    }
    assert.equal(createHash("sha256").update(rows.join("\n")).digest("hex"), manifest.contentHash);
    return { manifest, rows: rows.map(JSON.parse) };
  }
  return { call, batch, mf };
}
function browserObservation() {
  return { tenant_id: "boom", message_id: "browser-test", event_kind: "identify", delivery_event_id: "delivery-1",
    producer_id: "test", observed_at: "2026-09-05T22:00:00Z", ingested_at: "2026-09-05T22:01:00Z",
    source_fact_version: 1, source_deleted: 0,
    fact_payload: JSON.stringify({ messageId: "browser-test", anonymousId: "visitor-test",
      timestamp: "2026-09-05T22:00:00Z", traits: { email: "browser@example.com" } }) };
}
test("historical handover work cannot consume reserved live-source capacity", async (t) => {
  const { call } = await runtime(t);
  for (let offset = 0; offset < 525; offset += 25) {
    await call("/enqueue", Array.from({ length: 25 }, (_, index) => ({
      source: "history", source_account: "default", baseline_id: baselineId,
      scope_id: `history:${offset + index}`, replacement_id: `history:${offset + index}`,
      observation_sequence: 0, observed_at: "2026-09-05T20:00:00Z",
      rows: [], evidence_inbox_ids: [], source_evidence: {}, facts: [],
    })));
  }
  assert.deepEqual(await call("/enqueue", [replacement("live-contact", "test@example.com", null)]), { accepted: 1 });
});
test("hourly handover reuses identical history but still publishes changed identity evidence", async (t) => {
  const { call, batch } = await runtime(t);
  const event = { source_system: "stripe", source_record_id: "history-test",
    observed_at: "2025-01-01T00:00:00Z", anonymous_id: null, user_id: null,
    email: "before@example.com", phone: null, first_name: null, last_name: null };
  await call("/history-inputs", { baselineId, snapshotTime: "2026-09-05T20:00:00Z" });
  await call("/history-load", { baselineId, events: [JSON.stringify(event)] });
  await call("/ack", (await batch()).manifest);
  const next = `b_${"b".repeat(24)}`;
  await call("/history-inputs", { baselineId: next, snapshotTime: "2026-09-05T21:00:00Z" });
  const checked = await call("/history-load", { baselineId: next, events: [JSON.stringify(event)] });
  assert.ok(checked.factKeys.includes("stripe:history-test"));
  assert.equal((await call("/status")).pending, 0);
  await call("/history-load", { baselineId: next, events: [JSON.stringify({ ...event, email: "after@example.com" })] });
  assert.equal((await call("/status")).pending, 1);
  const result = await batch();
  const facts = result.rows.filter(row => row.kind === "identity" && row.payload.state_kind === "fact")
    .map(row => JSON.parse(row.payload.payload_json));
  assert.ok(facts.some(fact => fact.factKey === "stripe:history-test" && fact.evidenceKeys.includes("email:after@example.com")));
});
test("already queued unchanged history commits without rebuilding customer identities", async (t) => {
  const { call, batch } = await runtime(t);
  await call("/history-inputs", { baselineId, snapshotTime: "2026-09-05T20:00:00Z" });
  await call("/history-load", { baselineId, events: [JSON.stringify({ source_system: "stripe",
    source_record_id: "unchanged", observed_at: "2025-01-01T00:00:00Z",
    anonymous_id: null, user_id: null, email: "test@example.com", phone: null,
    first_name: null, last_name: null })] });
  const first = await batch();
  await call("/ack", first.manifest);
  const source = first.rows.find(row => row.kind === "source").payload;
  await call("/enqueue", [{ ...source, scope_id: "queued-history", replacement_id: "queued-history",
    facts: source.facts.map(fact => ({ ...fact, sourceFactVersion: fact.sourceFactVersion + 3600000 })) }]);
  const next = await batch();
  assert.equal(next.manifest.sourceCount, 1);
  assert.equal(next.manifest.identityCount, 0);
  await call("/ack", next.manifest);
  assert.equal((await call("/status")).pending, 0);
});
test("hourly handover refreshes historical evidence without resetting live facts", async (t) => {
  const { call, batch } = await runtime(t);
  await call("/history-inputs", { baselineId, snapshotTime: "2026-09-05T20:00:00Z" });
  await call("/enqueue", [replacement("live", "live@example.com", "+12025561234")]);
  await call("/history-load", { baselineId, events: [JSON.stringify({ source_system: "stripe",
    source_record_id: "old", observed_at: "2025-01-01T00:00:00Z", email: "old@example.com", phone: "+12025561234",
    anonymous_id: null, user_id: null, first_name: null, last_name: null })] });
  await call("/ack", (await batch()).manifest);
  const next = `b_${"b".repeat(24)}`;
  const inputs = await call("/history-inputs", { baselineId: next, snapshotTime: "2026-09-05T21:00:00Z" });
  assert.equal(inputs.baselineId, next);
  assert.ok(inputs.factKeys.includes("stripe:old"));
  await call("/history-load", { baselineId: next, events: [], removedFactKeys: ["stripe:old"] });
  const result = await batch();
  const facts = result.rows.filter(row => row.kind === "identity" && row.payload.state_kind === "fact")
    .map(row => JSON.parse(row.payload.payload_json));
  assert.ok(facts.some(fact => fact.factKey === "stripe:old" && fact.factDeleted));
  assert.ok(!facts.some(fact => fact.factKey === "activecampaign:live" && fact.factDeleted));
  await call("/ack", result.manifest);
  assert.equal((await call("/history-inputs", { baselineId, snapshotTime: "2026-09-05T20:00:00Z" })).baselineId, next);
});
test("historical payment evidence joins a new registration before a complete publication", async (t) => {
  const { call, batch } = await runtime(t);
  await call("/enqueue", [replacement("new-contact", "new@example.com", "+12025561234")]);
  const inputs = await call("/history-inputs", { baselineId });
  const loaded = await call("/history-load", { baselineId, events: [JSON.stringify({
    source_system: "stripe", source_record_id: "old-payment", observed_at: "2025-01-01T00:00:00Z",
    anonymous_id: null, user_id: "old@example.com", email: "old@example.com", phone: "+12025561234",
    first_name: "Old", last_name: "Customer", contact_id: null,
  })] });
  await call("/history-checked", { baselineId, keys: [...inputs.keys, ...loaded.keys],
    factKeys: [...inputs.factKeys, ...loaded.factKeys], scopes: inputs.scopes });
  const result = await batch();
  assert.equal(result.manifest.historyComplete, true);
  const mappings = result.rows.filter(row => row.kind === "identity" && row.payload.state_kind === "mapping")
    .map(row => JSON.parse(row.payload.payload_json)).filter(row => row.identifierType === "email");
  assert.equal(mappings.length, 2);
  assert.equal(new Set(mappings.map(row => row.profileId)).size, 1);
  await call("/ack", result.manifest);
});
test("empty contact replacements retract registrations found only in historical data", async (t) => {
  const { call, batch } = await runtime(t);
  await call("/enqueue", [replacement("activecampaign:contact:removed-contact", null, null, 1, [])]);
  const inputs = await call("/history-inputs", { baselineId });
  assert.deepEqual(inputs.scopes, ["activecampaign:contact:removed-contact"]);
  const loaded = await call("/history-load", { baselineId, events: [JSON.stringify({
    source_system: "activecampaign", source_record_id: "old-tag", contact_id: "removed-contact",
    observed_at: "2025-01-01T00:00:00Z", anonymous_id: null, user_id: "old@example.com",
    email: "old@example.com", phone: null, first_name: "Old", last_name: "Customer",
  })] });
  await call("/history-checked", { baselineId, keys: loaded.keys, factKeys: loaded.factKeys, scopes: inputs.scopes });
  const result = await batch();
  const facts = result.rows.filter(row => row.kind === "identity" && row.payload.state_kind === "fact");
  assert.equal(facts.length, 1);
  assert.equal(JSON.parse(facts[0].payload.payload_json).factDeleted, true);
  assert.equal(result.manifest.historyComplete, true);
});
test("browser redelivery preserves one source replacement and links its identifiers", async (t) => {
  const { call, batch } = await runtime(t);
  const observation = browserObservation();
  const first = await call("/browser-contract", observation);
  const retry = await call("/browser-contract", { ...observation, delivery_event_id: "delivery-2", ingested_at: "2026-09-05T22:02:00Z" });
  assert.equal(first.replacement_id, retry.replacement_id);
  assert.equal((await call("/enqueue", [first, retry])).accepted, 1);
  const prepared = await batch();
  assert.ok(prepared.rows.some(row => row.kind === "identity" && row.payload.state_kind === "mapping"));
  await call("/ack", prepared.manifest);
  assert.equal((await call("/enqueue", [retry])).accepted, 0);
});
test("browser buffer replay survives eviction and leaves original objects intact", async (t) => {
  const { call, mf } = await runtime(t);
  const bucket = await mf.getR2Bucket("BROWSER_BUFFER");
  const body = JSON.stringify({ schema_version: "jitsu_events_api_v1", producer_id: "test", events: [browserObservation()] });
  const key = `jitsu/envelopes/${createHash("sha256").update(body).digest("hex")}.json`;
  await bucket.put(key, body);
  const first = await call("/browser-start");
  assert.equal(first.acceptedEvents, 1);
  await call("/browser-pause");
  await mf.unsafeEvictDurableObject("publication-test", "BrowserSource", { name: "boom" });
  const retry = await call("/browser-start");
  assert.equal(retry.acceptedEvents, 1);
  assert.equal(retry.completedObjects, 1);
  assert.equal(await (await bucket.get(key)).text(), body);
  await call("/browser-pause");
});
test("durable source and identity publication survives eviction and requires exact acknowledgement", async (t) => {
  const { call, batch, mf } = await runtime(t);
  const input = replacement("contact-a", "a@example.com", null);
  assert.equal((await call("/enqueue", [input, input])).accepted, 1);
  const first = await batch();
  assert.equal(first.manifest.sourceCount, 1);
  assert.ok(first.manifest.identityCount > 0);
  assert.equal((await call("/status")).version, 0);
  await mf.unsafeEvictDurableObject("publication-test", "Publication", { name: "test" });
  const retry = await call("/prepare", { baselineId: `b_${"b".repeat(24)}` });
  assert.deepEqual(retry, first.manifest);
  await assert.rejects(call("/ack", { ...first.manifest, contentHash: "0".repeat(64) }), /does not match/);
  assert.equal((await call("/status")).version, 0);
  await call("/ack", first.manifest);
  await call("/ack", first.manifest);
  assert.equal((await call("/status")).version, 1);
  assert.equal((await call("/status")).pending, 0);
  assert.equal((await call("/enqueue", [input])).accepted, 0);
  await assert.rejects(call("/enqueue", [{ ...input, rows: [] }]), /Conflicting source retry/);
});
test("removing a registration retracts identity evidence and splits an existing merged profile", async (t) => {
  const { call, batch } = await runtime(t);
  await call("/enqueue", [replacement("a", "a@example.com", "+12025561234"), replacement("b", "b@example.com", "+14156661234")]);
  const initial = await batch();
  await call("/ack", initial.manifest);
  await call("/enqueue", [replacement("bridge", "a@example.com", "+14156661234")]);
  const merged = await batch();
  const mappings = merged.rows.filter((row) => row.kind === "identity" && row.payload.state_kind === "mapping")
    .map((row) => JSON.parse(row.payload.payload_json));
  assert.equal(new Set(mappings.filter((row) => row.identifierType === "email").map((row) => row.profileId)).size, 1);
  await call("/ack", merged.manifest);
  await call("/enqueue", [replacement("bridge", null, null, 2, [])]);
  const split = await batch();
  const current = new Map();
  for (const row of [...initial.rows, ...merged.rows, ...split.rows]) {
    if (row.kind !== "identity" || row.payload.state_kind !== "mapping") continue;
    if (row.payload.is_deleted) current.delete(row.payload.state_key);
    else current.set(row.payload.state_key, JSON.parse(row.payload.payload_json));
  }
  const after = [...current.values()];
  assert.equal(new Set(after.filter((row) => row.identifierType === "email").map((row) => row.profileId)).size, 2);
  assert.ok(split.rows.some((row) => row.kind === "identity" && row.payload.state_kind === "fact" && JSON.parse(row.payload.payload_json).factDeleted));
  await call("/ack", split.manifest);
  assert.equal((await call("/status")).version, 3);
});

test("new arrivals wait for the next checkpoint without blocking or being acknowledged with history", async (t) => {
  const { call, batch, mf } = await runtime(t);
  await call("/enqueue", [replacement("first", "first@example.com", null)]);
  const inputs = await call("/history-inputs", { baselineId, snapshotTime: "2026-09-05T20:00:00Z" });
  await call("/enqueue", [replacement("later", "later@example.com", null)]);
  const loaded = await call("/history-load", { baselineId, events: [JSON.stringify({
    source_system: "stripe", source_record_id: "old", email: "first@example.com",
    anonymous_id: null, user_id: null, phone: null, first_name: null, last_name: null,
    observed_at: "2025-01-01T00:00:00Z"
  })] });
  await call("/history-checked", { baselineId, keys: [...inputs.keys, ...loaded.keys],
    factKeys: [...inputs.factKeys, ...loaded.factKeys], scopes: inputs.scopes });
  await mf.unsafeEvictDurableObject("publication-test", "Publication", { name: "test" });
  const next = `b_${"b".repeat(24)}`;
  assert.equal((await call("/history-inputs", { baselineId: next, snapshotTime: "2026-09-05T21:00:00Z" })).baselineId, baselineId);
  const first = await batch();
  assert.equal(first.manifest.historyComplete, true);
  assert.equal(first.manifest.sourceCount, 2);
  assert.ok(!first.rows.some(row => row.kind === "source" && row.payload.scope_id === "later"));
  await call("/ack", first.manifest);
  assert.equal((await call("/status")).pending, 1);
  const second = await batch();
  assert.ok(second.rows.some(row => row.kind === "source" && row.payload.scope_id === "later"));
  await call("/ack", second.manifest);
  assert.equal((await call("/status")).pending, 0);
});

test("one provider cannot consume another provider's publication capacity", async (t) => {
  const { call } = await runtime(t);
  for (let offset = 0; offset < 100; offset += 25) {
    await call("/enqueue", Array.from({ length: 25 }, (_, index) => replacement(`contact-${offset + index}`, null, null)));
  }
  await assert.rejects(call("/enqueue", [replacement("contact-over-limit", null, null)]), /Source publication backlog/);
  const browser = await call("/browser-contract", browserObservation());
  assert.equal((await call("/enqueue", [browser])).accepted, 1);
  assert.equal((await call("/status")).pending, 101);
});

test("remote fetchers share the durable lease and preserve saved cursors", async (t) => {
  const { call } = await runtime(t);
  const route = { source: "stripe", account: "main" };
  const lease = "11111111-1111-4111-8111-111111111111";
  const other = "22222222-2222-4222-8222-222222222222";
  const run = (operation, args = [], owner = lease) => call("/fetcher", { route, operation, args, lease: owner });
  assert.equal((await run("acquire")).acquired, true);
  assert.equal((await run("acquire", [], other)).acquired, false);
  await run("setCursor", ["stripe:main:events-cursor", { completedThrough: 123 }]);
  await assert.rejects(run("setCursor", ["stripe:main:events-cursor", {}], other), /lease expired/);
  await assert.rejects(run("initializeSchema"), /Unsupported/);
  await run("release", [true]);
  assert.equal((await run("acquire", [], other)).acquired, true);
  assert.deepEqual(await run("getCursor", ["stripe:main:events-cursor"], other), { completedThrough: 123 });
  await run("release", [true], other);
});

test("changed ActiveCampaign payloads sharing an update timestamp do not stall discovery", async (t) => {
  const { call } = await runtime(t);
  const route = { source: "activecampaign", account: "default" };
  const lease = "11111111-1111-4111-8111-111111111111";
  const run = (operation, args = []) => call("/fetcher", { route, operation, args, lease });
  await run("acquire");
  const record = { id: "same-contact-update-time", source: "activecampaign", sourceAccount: "default",
    kind: "dirty_contact", contactId: "123", receivedAt: "2026-09-06T01:00:00Z",
    immutablePayload: { trigger: { kind: "contact_update_poll", contact: { id: "123", score: "1" } } } };
  assert.equal(await run("putInboxIfAbsent", [record]), true);
  assert.equal(await run("putInboxIfAbsent", [record]), false);
  const changed = structuredClone(record); changed.immutablePayload.trigger.contact.score = "2";
  assert.equal(await run("putInboxIfAbsent", [changed]), true);
  const pending = await run("listPendingInbox", [{ source: "activecampaign", sourceAccount: "default", limit: 10 }]);
  assert.equal(pending.length, 2);
  await run("release", [true]);
});

test("a checkpoint finishes after 100 source updates even when more updates are waiting", async (t) => {
  const { call, batch } = await runtime(t);
  for (let offset = 0; offset < 100; offset += 25) {
    await call("/enqueue", Array.from({ length: 25 }, (_, index) => replacement(`contact-${offset + index}`, null, null, 1, [])));
  }
  await call("/enqueue", [await call("/browser-contract", browserObservation())]);
  const inputs = await call("/history-inputs", { baselineId });
  await call("/history-checked", { baselineId, keys: inputs.keys, factKeys: inputs.factKeys, scopes: inputs.scopes });
  const result = await batch();
  assert.equal(result.manifest.sourceCount, 100);
  assert.equal(result.manifest.historyComplete, true);
  await call("/ack", result.manifest);
  assert.equal((await call("/status")).pending, 1);
});

test("larger batches keep historical fact work bounded and retain the remainder", async (t) => {
  const { call, batch } = await runtime(t);
  await call("/history-inputs", { baselineId });
  for (let offset = 0; offset < 2600; offset += 1000) {
    const events = Array.from({ length: Math.min(1000, 2600 - offset) }, (_, index) => JSON.stringify({
      source_system: "stripe", source_record_id: `history-${offset + index}`, observed_at: "2025-01-01T00:00:00Z",
      anonymous_id: null, user_id: null, email: null, phone: null, first_name: null, last_name: null
    }));
    const loaded = await call("/history-load", { baselineId, events });
    await call("/history-checked", { baselineId, keys: loaded.keys, factKeys: loaded.factKeys });
  }
  const first = await batch();
  assert.equal(first.manifest.sourceCount, 25);
  assert.equal(first.manifest.historyComplete, false);
  await call("/ack", first.manifest);
  assert.equal((await call("/status")).pending, 1);
  const last = await batch();
  assert.equal(last.manifest.sourceCount, 1);
  assert.equal(last.manifest.historyComplete, true);
});

test("manual reconciliation queues bounded provider reads without taking the fetcher lease", async (t) => {
  const { call } = await runtime(t);
  const route = { source: "activecampaign", account: "default" };
  const lease = "11111111-1111-4111-8111-111111111111";
  const run = (operation, args = []) => call("/fetcher", { route, operation, args, lease });
  await run("acquire");
  await run("setCursor", ["activecampaign:updated-contacts-cursor", { completedThrough: "2026-09-06T04:00:00Z" }]);
  assert.deepEqual(await call("/reconcile", { contactIds: ["123", "123", "456"] }), { queued: 2 });
  const pending = await run("listPendingInbox", [{ source: "activecampaign", sourceAccount: "default", limit: 10 }]);
  assert.deepEqual(pending.map(row => row.contactId).sort(), ["123", "456"]);
  assert.ok(pending.every(row => row.kind === "dirty_contact" && !row.progress));
  assert.deepEqual(await run("getCursor", ["activecampaign:updated-contacts-cursor"]), { completedThrough: "2026-09-06T04:00:00Z" });
  await assert.rejects(call("/reconcile", { contactIds: ["invalid"] }));
  await assert.rejects(call("/reconcile", { contactIds: Array(26).fill("123") }));
  await run("release", [true]);
});

test("history accepts one bounded 5,000-row page and preserves every fact", async (t) => {
  const { call } = await runtime(t);
  await call('/history-inputs', { baselineId });
  const events = Array.from({ length: 5000 }, (_, i) => JSON.stringify({
    source_system: 'stripe', source_record_id: `large-page-${i}`,
    observed_at: '2026-09-05T00:00:00Z', anonymous_id: null, user_id: null,
    email: null, phone: null, first_name: null, last_name: null,
  }));
  const started = Date.now();
  const loaded = await call('/history-load', { baselineId, events });
  assert.equal(loaded.factKeys.length, 5000);
  assert.equal(new Set(loaded.factKeys).size, 5000);
  assert.equal((await call('/status')).pending, 50);
  await assert.rejects(call('/history-load', { baselineId, events: [...events, events[0]] }), /Invalid history batch/);
  console.log(JSON.stringify({ event: 'history_page_test', rows: 5000, durationMs: Date.now() - started }));
});


test('history discovery returns committed evidence keys and excludes checked keys', async t => {
  const {call,batch} = await runtime(t);
  await call('/history-inputs', {baselineId, snapshotTime:'2026-09-05T20:00:00Z'});
  await call('/history-load', {baselineId, events:Array.from({length:40}, (_,i)=>JSON.stringify({
    source_system:'stripe', source_record_id:`indexed-${i}`, observed_at:'2025-01-01T00:00:00Z',
    email:`person${i}@example.com`, anonymous_id:`visitor-${i}`, user_id:null, phone:null,
    first_name:null, last_name:null
  }))});
  const committed = await batch();
  const expected = new Set(committed.rows.filter(r=>r.kind==='identity' && r.payload.state_kind==='fact')
    .flatMap(r=>JSON.parse(r.payload.payload_json).evidenceKeys));
  await call('/ack', committed.manifest);
  const next=`b_${'c'.repeat(24)}`;
  const input=await call('/history-inputs', {baselineId:next, snapshotTime:'2026-09-05T21:00:00Z'});
  assert.deepEqual(new Set(input.keys), expected);
  const checked=[...expected].slice(0,10);
  await call('/history-checked',{baselineId:next,keys:checked,factKeys:[],scopes:[]});
  const remaining=await call('/history-inputs',{baselineId:next});
  assert.deepEqual(new Set(remaining.keys),new Set([...expected].filter(k=>!checked.includes(k))));
});


test('Stripe event metadata changes preserve both observations without blocking retries', async t => {
  const {call}=await runtime(t);
  const route={source:'stripe',account:'main'};
  const lease='11111111-1111-4111-8111-111111111111';
  const run=(operation,args=[])=>call('/fetcher',{route,operation,args,lease});
  await run('acquire');
  const record={id:'stripe:main:event:evt_test',source:'stripe',sourceAccount:'main',kind:'stripe_event',
    chargeId:'ch_test',receivedAt:'2026-09-10T00:00:00Z',
    immutablePayload:{event:{id:'evt_test',pending_webhooks:1,data:{object:{id:'ch_test'}}}}};
  assert.equal(await run('putInboxIfAbsent',[record]),true);
  assert.equal(await run('putInboxIfAbsent',[record]),false);
  const changed=structuredClone(record);changed.immutablePayload.event.pending_webhooks=0;
  assert.equal(await run('putInboxIfAbsent',[changed]),true);
  assert.equal(await run('putInboxIfAbsent',[changed]),false);
  const pending=await run('listPendingInbox',[{source:'stripe',sourceAccount:'main',limit:10}]);
  assert.equal(pending.length,2);
  assert.ok(pending.every(item=>item.chargeId==='ch_test'));
  await run('release',[true]);
});


test('historical retraction of an already deleted contact version is a no-op', async t => {
  const {call,batch}=await runtime(t);
  const scope='activecampaign:contact:123';
  await call('/enqueue',[replacement(scope,'live@example.com',null,1)]);
  await call('/ack',(await batch()).manifest);
  await call('/enqueue',[replacement(scope,'live@example.com',null,2,[])]);
  const deletion=await batch();
  await call('/ack',deletion.manifest);
  await call('/history-inputs',{baselineId,snapshotTime:'2026-09-05T23:00:00Z'});
  await call('/history-load',{baselineId,events:[JSON.stringify({
    source_system:'activecampaign',source_record_id:scope,contact_id:'123',
    observed_at:'2026-09-05T22:00:00Z',email:'frozen@example.com',phone:null,
    anonymous_id:null,user_id:null,first_name:'Different frozen name',last_name:null
  })]});
  assert.equal((await call('/status')).pending,0);
  const tombstone=deletion.rows.find(r=>r.kind==='identity' && r.payload.state_kind==='fact');
  const prior=JSON.parse(tombstone.payload.payload_json);
  const legacyFact={factKey:prior.factKey,factKind:prior.factKind,producerId:'frozen-history',
    sourcePriority:1,sourceFactVersion:2,factDeleted:true,evidenceKeys:[],
    factPayload:'{}',factPayloadHash:createHash('sha256').update('{}').digest('hex'),
    observedAt:prior.factObservedAt,ingestedAt:'2026-09-05T23:00:00Z',eventId:'legacy-retraction'};
  await call('/enqueue',[{source:'history',source_account:'default',baseline_id:baselineId,
    scope_id:'legacy',replacement_id:'legacy',observation_sequence:0,observed_at:'1970-01-01T00:00:00.000Z',
    rows:[],evidence_inbox_ids:[],source_evidence:{},facts:[legacyFact]}]);
  const result=await batch();
  assert.equal(result.manifest.identityCount,0);
  await call('/ack',result.manifest);
});

test('compacted committed batches preserve retry checks and pending work', async (t) => {
  const { call, batch } = await runtime(t);
  const done = replacement('compact-contact', 'done@example.com', null);
  await call('/enqueue', [done]);
  const {manifest} = await batch();
  await call('/ack', manifest);
  const pending = replacement('pending-contact', 'pending@example.com', null);
  await call('/enqueue', [pending]);
  await call('/compact');
  await call('/enqueue', [done]);
  await assert.rejects(call('/enqueue', [{...done, rows: []}]), /Conflicting source retry/);
  const next = await batch();
  assert.ok(next.rows.some(row => row.kind === 'source' && row.payload.replacement_id === pending.replacement_id));
});

test("new browser notifications pass the older hash-ordered backlog without deleting it", async (t) => {
  const { call, mf } = await runtime(t);
  const bucket = await mf.getR2Bucket("BROWSER_BUFFER");
  const objects = Array.from({ length: 12 }, (_, index) => {
    const observation = { ...browserObservation(), message_id: `event-${index}`,
      fact_payload: JSON.stringify({ messageId: `event-${index}`, anonymousId: `visitor-${index}` }) };
    const body = JSON.stringify({ schema_version: "jitsu_events_api_v1", events: [observation] });
    return { body, id: observation.message_id,
      key: `jitsu/envelopes/${createHash("sha256").update(body).digest("hex")}.json` };
  }).sort((a, b) => a.key.localeCompare(b.key));
  for (const object of objects) await bucket.put(object.key, object.body);
  await call("/browser-notify", { keys: objects.slice(0, -1).map(object => object.key) });
  const newest = objects.at(-1);
  await call("/browser-notify", { keys: [newest.key] });
  const status = await call("/browser-start");
  await call("/browser-pause");
  assert.equal(status.completedObjects, 10);
  assert.equal(status.objects, 12);
  const page = await call("/browser-published", { baselineId });
  const sources = page.rows.map(JSON.parse).filter(row => row.kind === "source");
  assert.ok(sources.some(row => row.payload.browser.source.source_record_id === newest.id));
  assert.equal((await bucket.list()).objects.length, 12);
});

test("realtime contact recovery prioritizes recent work and preserves per-contact publication order", async (t) => {
  const { call } = await runtime(t);
  const route = { source: "activecampaign", account: "default" };
  const lease = "33333333-3333-4333-8333-333333333333";
  const run = (operation, args = []) => call("/fetcher", { route, operation, args, lease });
  await run("acquire");
  for (const [id, receivedAt] of [["older", "2026-09-05T00:00:00Z"], ["recent", "2026-09-14T00:00:00Z"]]) {
    await run("putInboxIfAbsent", [{ id, source: "activecampaign", sourceAccount: "default",
      kind: "dirty_contact", contactId: id, receivedAt, immutablePayload: {} }]);
  }
  const input = { source: "activecampaign", sourceAccount: "default", limit: 1 };
  assert.equal((await run("listPendingInbox", [input]))[0].id, "recent");
  await run("saveInboxProgress", ["older", { nextStep: "resume" }]);
  assert.equal((await run("listPendingInbox", [input]))[0].id, "older");
  for (const [scope, version] of [["a", 1], ["a", 2], ["z", 3]]) {
    const contract = replacement(`activecampaign:contact:${scope}`, `${scope}@example.com`, null, version);
    await run("putOutboxIfAbsent", [{ id: `${scope}:${version}`, source: "activecampaign",
      sourceAccount: "default", contract, inboxIds: [], stateUpdates: [] }]);
  }
  assert.equal((await run("listPendingOutbox", [input]))[0].id, "z:3");
  await run("commitPublishedOutbox", ["z:3", "2026-09-14T01:00:00Z"]);
  assert.equal((await run("listPendingOutbox", [input]))[0].id, "a:1");
  await run("commitPublishedOutbox", ["a:1", "2026-09-14T01:00:01Z"]);
  assert.equal((await run("listPendingOutbox", [input]))[0].id, "a:2");
  await run("release", [true]);
});

test('verified handover retains unchanged coverage and invalidates changed history', async t => {
  const { call, batch } = await runtime(t);
  await call('/history-inputs', { baselineId, snapshotTime: '2026-09-05T20:00:00Z' });
  const events = ['same', 'changed'].map(id => JSON.stringify({ source_system: 'stripe',
    source_record_id: id, observed_at: '2025-01-01T00:00:00Z', email: `${id}@example.com`,
    phone: null, anonymous_id: null, user_id: null, first_name: null, last_name: null }));
  const coverage = await call('/history-load', { baselineId, events });
  await call('/history-checked', { baselineId, ...coverage });
  await call('/ack', (await batch()).manifest);
  const next = `b_${'b'.repeat(24)}`;
  const advance = await call('/history-advance', { previousBaselineId: baselineId,
    baselineId: next, snapshotTime: '2026-09-05T21:00:00Z', invalidations: [
      { kind: 'fact', value: 'stripe:changed' }, { kind: 'key', value: 'email:changed@example.com' },
    ] });
  assert.equal(advance.advanced, true);
  const inputs = await call('/history-inputs', { baselineId: next, snapshotTime: '2026-09-05T21:00:00Z' });
  assert.ok(inputs.factKeys.includes('stripe:changed'));
  assert.ok(inputs.keys.includes('email:changed@example.com'));
  assert.ok(!inputs.factKeys.includes('stripe:same'));
  assert.ok(!inputs.keys.includes('email:same@example.com'));
  await call('/history-load', { baselineId: next, events: [], removedFactKeys: ['stripe:changed'] });
  const result = await batch();
  assert.ok(result.rows.some(row => row.kind === 'identity' && row.payload.state_kind === 'fact'
    && JSON.parse(row.payload.payload_json).factDeleted));
});

test('verified handover cannot change an active publication or accept a stale comparison', async t => {
  const { call } = await runtime(t);
  await call('/history-inputs', { baselineId, snapshotTime: '2026-09-05T20:00:00Z' });
  const next = `b_${'b'.repeat(24)}`;
  const input = { previousBaselineId: next, baselineId: next, snapshotTime: '2026-09-05T21:00:00Z', invalidations: [] };
  assert.equal((await call('/history-advance', input)).advanced, false);
  await call('/enqueue', [replacement('pending', 'test@example.com', null)]);
  await call('/history-inputs', { baselineId });
  assert.equal((await call('/history-advance', { ...input, previousBaselineId: baselineId })).advanced, false);
  assert.equal((await call('/history-root')).baselineId, baselineId);
});

test("storage diagnostic is bounded and readable", async t => { const {call} = await runtime(t); const usage = await call("/storage-usage"); assert.ok(usage.usage.length); });

test("unchanged identity values are not rewritten for a newer source revision", async t => {
  const {call,batch} = await runtime(t);
  await call("/enqueue", [replacement("stable", "stable@example.com", null)]);
  const first = await batch();
  await call("/ack", first.manifest);
  await call("/enqueue", [replacement("stable", "stable@example.com", null, 2)]);
  const second = await batch();
  assert.ok(second.rows.some(row => row.kind === "source"));
  assert.ok(second.rows.some(row => row.kind === "identity" && row.payload.state_kind === "fact"));
  assert.equal(second.rows.filter(row => row.kind === "identity" && ["mapping","profile","evidence"].includes(row.payload.state_kind)).length, 0);
  await call("/ack", second.manifest);
});

test("replacement object preserves checkpoint, pending data and identity removal through parent", async t => {
  const {call} = await runtime(t);
  await call("/old/history-inputs", {baselineId,snapshotTime:"2026-09-05T20:00:00Z"});
  await call("/old/enqueue", [replacement("migration-contact","migration@example.com",null)]);
  const first = await call("/old/prepare",{baselineId});
  await call("/old/ack",first);
  await call("/old/enqueue",[replacement("migration-contact",null,null,2,[])]);
  assert.equal((await call("/new/seed")).version,1);
  assert.equal((await call("/new/import")).count,1);
  assert.equal((await call("/new/status")).pending,1);
  const second=await call("/new/prepare",{baselineId});
  assert.equal(second.version,2);
  const chunk=await call("/new/chunk",{batchId:second.batchId,offset:0});
  const rows=chunk.rows.map(JSON.parse);
  assert.ok(rows.some(row=>row.kind==="identity" && row.payload.state_kind==="fact" && JSON.parse(row.payload.payload_json).factDeleted));
  await call("/new/ack",second);
  assert.equal((await call("/new/status")).version,2);
  assert.equal((await call("/old/status")).version,1);
  assert.equal((await call("/old/status")).pending,1);
  await assert.rejects(call("/new/seed"),/not empty/);
});
