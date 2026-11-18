import test from "node:test";
import assert from "node:assert/strict";
import { runBackup } from "../src/runner.js";

test("failed publication cannot advance the cursor; retry resumes the saved discovery page", async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  let rejectPublication = true;
  let listCalls = 0;
  globalThis.fetch = async (url, options) => {
    const path = new URL(url).pathname;
    calls.push(path);
    if (path === "/window") return Response.json({ windowEnd: null });
    if (path === "/records" && rejectPublication) return new Response("", { status: 503 });
    return Response.json({ accepted: 1 });
  };
  const saved = new Map();
  const step = { do: async (name, options, action) => {
    if (saved.has(name)) return saved.get(name);
    const result = await action();
    saved.set(name, result);
    return result;
  } };
  const env = {
    START_AT: new Date(Date.now() - 3600000).toISOString(),
    WAREHOUSE_URL: "https://receiver.test", WAREHOUSE_TOKEN: "fixture",
    STRIPE_SOURCE: { read: async (account, path) => {
      if (path === "/charges") { listCalls++; return { status: 200, body: { data: [{ id: "ch_1" }], has_more: false } }; }
      if (path === "/charges/ch_1") return { status: 200, body: { id: "ch_1", paid: true, status: "succeeded" } };
      if (path === "/events") return { status: 200, body: { data: [
        { type: "charge.succeeded", data: { object: { id: "ch_1", object: "charge" } } },
      ], has_more: false } };
      throw new Error(`Unexpected path: ${path}`);
    } },
  };
  try {
    const event = { instanceId: "fixture", payload: { account: "stripe" } };
    await assert.rejects(runBackup(env, event, step), /503/);
    assert.equal(calls.includes("/complete"), false);
    rejectPublication = false;
    await runBackup(env, event, step);
    assert.equal(calls.at(-1), "/complete");
    assert.equal(listCalls, 1);
    assert.equal(calls.filter(path => path === "/records").length, 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
