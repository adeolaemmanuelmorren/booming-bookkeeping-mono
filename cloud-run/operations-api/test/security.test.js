import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { createHandler } from "../src/app.js";
import { createGoogleProvider } from "../src/google.js";
import { createMetaProvider } from "../src/meta.js";
import { createJitsuProvider } from "../src/jitsu.js";
import { redact } from "../src/errors.js";

const apiKey = "test-key-with-more-than-thirty-two-characters";

async function withServer(providers, run) {
  const server = createServer(createHandler({ apiKey, ...providers }));
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  try { await run(`http://127.0.0.1:${server.address().port}`); }
  finally { await new Promise(resolve => server.close(resolve)); }
}

test("all data routes require the API key before dispatch", async () => {
  let calls = 0;
  await withServer({ google: { datasets() { calls++; return {}; } } }, async base => {
    for (const headers of [{}, { "X-Api-Key": "wrong" }]) {
      assert.equal((await fetch(`${base}/v1/bigquery/datasets`, { headers })).status, 401);
    }
    assert.equal((await fetch(`${base}/v1/bigquery/datasets?apiKey=${apiKey}`)).status, 401);
    assert.equal(calls, 0);
    assert.equal((await fetch(`${base}/health`)).status, 200);
    assert.equal((await fetch(`${base}/v1/bigquery/datasets`, { headers: { "X-Api-Key": apiKey } })).status, 200);
    assert.equal(calls, 1);
  });
});

test("status retains failed and unavailable provider checks", async () => {
  await withServer({ google: { status: async () => ({ status: "degraded", latest: { state: "FAILED" } }),
    bigqueryStatus: async () => ({ status: "ok" }), metaReconciliation: async () => ({ status: "warning" }) },
    jitsu: { status: async () => { throw new Error("secret"); } },
    meta: { status: async () => ({ status: "ok" }) } }, async base => {
    const response = await fetch(`${base}/v1/status`, { headers: { "X-Api-Key": apiKey } });
    assert.equal(response.status, 503);
    const body = await response.json();
    assert.equal(body.checks.dataform.latest.state, "FAILED");
    assert.equal(body.checks.jitsu.status, "unavailable");
    assert.ok(!JSON.stringify(body).includes("secret"));
  });
});

test("Meta reconciliation is advisory and does not turn a successful pipeline into a failure", async () => {
  const reconciliation = { status: "warning", mismatchCount: 21, differences: [] };
  const ok = async () => ({ status: "ok" });
  await withServer({ google: { status: ok, bigqueryStatus: ok, metaReconciliation: async () => reconciliation },
    jitsu: { status: ok }, meta: { status: ok } }, async base => {
    const headers = { "X-Api-Key": apiKey };
    assert.equal((await fetch(`${base}/v1/meta/reconciliation`)).status, 401);
    const response = await fetch(`${base}/v1/status`, { headers });
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(body.status, "warning");
    assert.equal(body.checks.dataform.status, "ok");
    assert.equal(body.checks.metaReconciliation.mismatchCount, 21);
    const detail = await fetch(`${base}/v1/meta/reconciliation`, { headers });
    assert.equal(detail.status, 200);
    assert.deepEqual(await detail.json(), reconciliation);
  });
});

function queryProvider(statistics, onExecute) {
  return createGoogleProvider({ auth: {}, projectId: "business", datasets: ["analytics"], repository: "repo",
    bigquery: { createQueryJob: async options => {
      if (options.dryRun) return [{ metadata: { statistics: { query: statistics } } }];
      onExecute(options);
      return [{ id: "boom_health_123", getQueryResults: async () => [[{ count: 1 }], null, { totalRows: "1" }] }];
    } } });
}

test("SQL cannot mutate data, exceed scan budget, or escape the allowed dataset", async () => {
  for (const statistics of [
    { statementType: "DELETE" }, { statementType: "SCRIPT" },
    { statementType: "SELECT", totalBytesProcessed: "1000000001" },
    { statementType: "SELECT", referencedTables: [{ projectId: "other", datasetId: "analytics" }] },
    { statementType: "SELECT", referencedTables: [{ projectId: "business", datasetId: "private" }] },
  ]) {
    let executed = false;
    const provider = queryProvider(statistics, () => { executed = true; });
    await assert.rejects(provider.query({ sql: "test" }));
    assert.equal(executed, false);
  }
  const provider = queryProvider({ statementType: "SELECT", totalBytesProcessed: "100",
    referencedTables: [{ projectId: "business", datasetId: "analytics" }] }, options => {
    assert.equal(options.maximumBytesBilled, "1000000000");
    assert.equal(options.jobTimeoutMs, 25000);
  });
  assert.deepEqual((await provider.query({ sql: "SELECT 1" })).rows, [{ count: 1 }]);
  await assert.rejects(provider.query({ sql: "SELECT 1", limit: 201 }));
});

test("credentials are redacted recursively, including URL parameters", () => {
  const result = redact({ privateKey: "secret", data: [{ authorization: "Bearer secret", message: "known-token" }],
    url: "https://graph.facebook.com?access_token=sensitive&after=cursor" }, ["known-token"]);
  assert.equal(result.privateKey, "[redacted]");
  assert.equal(result.data[0].message, "[redacted]");
  assert.ok(!JSON.stringify(result).includes("sensitive"));
  assert.deepEqual(redact({ pageToken: "opaque", nextPageToken: "opaque-next" }),
    { pageToken: "opaque", nextPageToken: "opaque-next" });
});

test("Jitsu uses workspace-scoped logs and bounded validated filters", async () => {
  const provider = createJitsuProvider({ apiKey: "jitsu-key", workspaceId: "boom", fetchImpl: async (url, options) => {
    assert.equal(url.pathname, "/api/boom/log/bulker_stream/actor");
    assert.equal(url.searchParams.get("limit"), "20");
    assert.equal(options.headers.Authorization, "Bearer jitsu-key");
    return new Response('{"date":"2026-10-10","content":{"status":"SUCCESS"}}\n');
  } });
  assert.equal((await provider.deliveries(new URLSearchParams({ mode: "stream", actorId: "actor", limit: "20" }))).records.length, 1);
  await assert.rejects(provider.invocations(new URLSearchParams({ actorId: "../other" })));
  await assert.rejects(provider.deliveries(new URLSearchParams({ limit: "201" })));
});

test("Meta bounds dates and strips token-bearing pagination URLs", async () => {
  const provider = createMetaProvider({ accessToken: "meta-token", fetchImpl: async () => new Response(JSON.stringify({
    data: [], paging: { next: "https://graph.facebook.com/?access_token=meta-token", cursors: { after: "opaque" } },
  })) });
  const result = await provider.insights(new URLSearchParams({ since: "2026-10-01", until: "2026-10-02" }));
  assert.equal(result.paging.hasMore, true);
  assert.equal(result.paging.cursors.after, "opaque");
  assert.equal(result.paging.next, undefined);
  await assert.rejects(provider.insights(new URLSearchParams({ since: "2026-01-01", until: "2026-10-10" })));
  await assert.rejects(provider.insights(new URLSearchParams({ since: "2026-99-01" })), /valid YYYY-MM-DD/);
});
