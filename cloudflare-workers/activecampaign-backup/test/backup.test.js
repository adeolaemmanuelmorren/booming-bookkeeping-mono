import test from "node:test";
import assert from "node:assert/strict";
import { contactRecords, read } from "../src/activecampaign.js";
import { runBackup, windowBounds } from "../src/runner.js";

const start = Date.parse("2026-11-16T00:00:00Z") / 1000;
const bounds = { start, end: start + 3600 };
const contact = { id: "12", udate: "2026-11-16T00:10:00Z", email: "fixture@example.test", contactTags: ["71"] };
const assignment = { id: "71", contact: "12", tag: "2", cdate: "2026-11-16T00:10:00Z" };

test("a contact version includes every referenced tag and preserves assignment IDs and dates", () => {
  const records = contactRecords({ contacts: [contact], contactTags: [assignment] }, "0", bounds);
  assert.deepEqual(records[0].payload.contactTags, [assignment]);
  assert.throws(() => contactRecords({ contacts: [contact], contactTags: [] }, "0", bounds), /incomplete/);
  assert.throws(() => contactRecords({ contacts: [contact], contactTags: [assignment] }, "12", bounds), /pagination/);
  assert.throws(() => contactRecords({ contacts: [{ ...contact, udate: "2026-11-18T00:00:00Z" }], contactTags: [assignment] }, "0", bounds), /outside/);
});

test("a complete empty tag set represents removal instead of retaining old assignments", () => {
  const records = contactRecords({ contacts: [{ ...contact, contactTags: [] }], contactTags: [] }, "0", bounds);
  assert.deepEqual(records[0].payload.contactTags, []);
  assert.deepEqual(contactRecords({ contacts: [] }, "0", bounds), []);
});

test("windows overlap, remain bounded and cannot skip unfinished history", () => {
  const initial = "2026-11-16T00:00:00Z";
  const first = windowBounds(initial, null, start + 86400);
  assert.equal(first.end, start + 6 * 3600);
  const second = windowBounds(initial, new Date(first.end * 1000).toISOString(), start + 86400);
  assert.equal(second.start, first.end - 600);
  assert.throws(() => windowBounds("invalid", null, start), /Invalid/);
});

test("the source client permits only fixed read endpoints", async () => {
  await assert.rejects(read({}, "/contacts/12/delete"), /Unsupported/);
  await assert.rejects(read({}, "/contacts"), /token/);
});

test("publication failure leaves the cursor unchanged; a retry publishes the complete window once", async () => {
  const originalFetch = globalThis.fetch;
  let failWrite = true;
  const calls = [];
  const saved = new Map();
  const step = { do: async (name, options, action) => {
    if (saved.has(name)) return saved.get(name);
    const result = await (action ?? options)();
    saved.set(name, result);
    return result;
  } };
  globalThis.fetch = async (input, options) => {
    const url = new URL(input);
    calls.push(url.pathname);
    if (url.pathname === "/api/3/contacts") {
      assert.equal(options.method, "GET");
      assert.equal(url.origin, "https://boomingbookkeeping.api-us1.com");
      return Response.json({ contacts: [contact], contactTags: [assignment] });
    }
    if (url.pathname === "/api/3/tags") return Response.json({ tags: [{ id: "2", tag: "Fixture" }], meta: { total: "1" } });
    if (url.pathname === "/window") return Response.json({ windowEnd: null });
    if (url.pathname === "/records" && failWrite) return new Response("", { status: 503 });
    if (url.pathname === "/complete") {
      const body = JSON.parse(options.body);
      assert.equal(body.contactCount, 1);
      assert.equal(body.tagCount, 1);
    }
    return Response.json({ ok: true });
  };
  const env = { START_AT: "2026-11-16T00:00:00Z", ACTIVE_CAMPAIGN_TOKEN: "fixture", WAREHOUSE_URL: "https://receiver.test", WAREHOUSE_TOKEN: "fixture" };
  try {
    await assert.rejects(runBackup(env, { instanceId: "fixture" }, step), /503/);
    assert.equal(calls.includes("/complete"), false);
    failWrite = false;
    await runBackup(env, { instanceId: "fixture" }, step);
    assert.equal(calls.at(-1), "/complete");
    assert.equal(calls.filter(path => path === "/complete").length, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("an incomplete catalog cannot publish its window", async () => {
  const originalFetch = globalThis.fetch;
  let completed = false;
  globalThis.fetch = async input => {
    const path = new URL(input).pathname;
    if (path === "/window") return Response.json({ windowEnd: null });
    if (path === "/api/3/contacts") return Response.json({ contacts: [] });
    if (path === "/api/3/tags") return Response.json({ tags: [{ id: "2", tag: "Fixture" }], meta: { total: "2" } });
    if (path === "/complete") completed = true;
    return Response.json({ ok: true });
  };
  try {
    const env = { START_AT: "2026-11-16T00:00:00Z", ACTIVE_CAMPAIGN_TOKEN: "fixture", WAREHOUSE_URL: "https://receiver.test", WAREHOUSE_TOKEN: "fixture" };
    const step = { do: (name, options, action) => (action ?? options)() };
    await assert.rejects(runBackup(env, { instanceId: "fixture" }, step), /catalog is incomplete/);
    assert.equal(completed, false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
