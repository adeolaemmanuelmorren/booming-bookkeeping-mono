import { createHash, timingSafeEqual, randomUUID } from "node:crypto";
import { ApiError, redact } from "./errors.js";

export const routes = [
  "GET /v1", "GET /v1/status",
  "GET /v1/dataform/workflows", "GET /v1/dataform/invocations?limit=50&pageToken=...",
  "GET /v1/dataform/invocations/{id}", "GET /v1/dataform/invocations/{id}/actions?limit=100&pageToken=...",
  "GET /v1/dataform/compilations/{id}", "GET /v1/bigquery/datasets",
  "GET /v1/bigquery/datasets/{dataset}/tables?limit=100&pageToken=...",
  "GET /v1/bigquery/datasets/{dataset}/tables/{table}",
  "GET /v1/bigquery/datasets/{dataset}/tables/{table}/rows?limit=20",
  "POST /v1/bigquery/query", "GET /v1/bigquery/query/{jobId}?limit=100&pageToken=...",
  "GET /v1/jitsu/workspace", "GET /v1/jitsu/connections", "GET /v1/jitsu/metrics",
  "GET /v1/jitsu/invocations?limit=50&actorId=...&levels=error&start=...&end=...",
  "GET /v1/jitsu/deliveries?mode=batch&limit=50&actorId=...&start=...&end=...",
  "GET /v1/meta/account", "GET /v1/meta/reconciliation",
  "GET /v1/meta/insights?since=YYYY-MM-DD&until=YYYY-MM-DD&level=account&limit=50&after=...",
];

export function createHandler({ apiKey, google, jitsu, meta, secrets = [] }) {
  if (!apiKey || apiKey.length < 32) throw new Error("OPERATIONS_API_KEY must contain at least 32 characters.");
  const expected = createHash("sha256").update(apiKey).digest();

  return async function handle(request, response) {
    const requestId = randomUUID();
    const send = (status, body) => {
      response.writeHead(status, { "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff", "X-Request-Id": requestId });
      response.end(JSON.stringify(redact(body, [apiKey, ...secrets])));
    };
    try {
      const url = new URL(request.url, "http://localhost");
      if (url.pathname === "/health" && request.method === "GET") return send(200, { status: "ok" });
      const header = request.headers["x-api-key"];
      const supplied = typeof header === "string" ? header : "";
      const suppliedHash = createHash("sha256").update(supplied).digest();
      if (!supplied || !timingSafeEqual(suppliedHash, expected)) return send(401, { error: "Invalid or missing X-Api-Key." });
      if (!["GET", "POST"].includes(request.method)) return send(405, { error: "Method not allowed." });
      const parts = url.pathname.split("/").filter(Boolean).map(decodeURIComponent);
      const params = url.searchParams;
      let result;

      if (request.method === "POST") {
        if (url.pathname !== "/v1/bigquery/query") return send(405, { error: "Only read-only BigQuery queries accept POST." });
        result = await google.query(await readBody(request));
        return send(200, result);
      }
      if (url.pathname === "/v1") return send(200, { service: "boom-operations-api", routes });
      if (url.pathname === "/v1/status") {
        const checks = { dataform: () => google.status(), bigquery: () => google.bigqueryStatus(),
          jitsu: () => jitsu.status(), meta: () => meta.status(),
          metaReconciliation: () => google.metaReconciliation() };
        const entries = await Promise.all(Object.entries(checks).map(async ([name, check]) => {
          try { return [name, await check()]; }
          catch (error) { return [name, { status: "unavailable", error: safeError(error) }]; }
        }));
        const failed = entries.some(([, check]) => !["ok", "warning"].includes(check.status));
        const warning = entries.some(([, check]) => check.status === "warning");
        const status = failed ? "degraded" : warning ? "warning" : "ok";
        return send(failed ? 503 : 200, { status, checkedAt: new Date().toISOString(),
          checks: Object.fromEntries(entries) });
      }
      if (parts[0] !== "v1") return send(404, { error: "Route not found." });
      if (parts[1] === "dataform") {
        if (parts.length === 3 && parts[2] === "workflows") result = await google.workflows();
        if (parts.length === 3 && parts[2] === "invocations") result = await google.invocations(params);
        if (parts.length === 4 && parts[2] === "invocations") result = await google.invocation(parts[3]);
        if (parts.length === 5 && parts[2] === "invocations" && parts[4] === "actions") result = await google.actions(parts[3], params);
        if (parts.length === 4 && parts[2] === "compilations") result = await google.compilation(parts[3]);
      }
      if (parts[1] === "bigquery") {
        if (parts.length === 3 && parts[2] === "datasets") result = await google.datasets();
        if (parts.length === 5 && parts[2] === "datasets" && parts[4] === "tables") result = await google.tables(parts[3], params);
        if (parts.length === 6 && parts[2] === "datasets" && parts[4] === "tables") result = await google.table(parts[3], parts[5]);
        if (parts.length === 7 && parts[2] === "datasets" && parts[4] === "tables" && parts[6] === "rows") result = await google.rows(parts[3], parts[5], params);
        if (parts.length === 4 && parts[2] === "query") result = await google.query({ jobId: parts[3],
          limit: params.get("limit") ?? 100, pageToken: params.get("pageToken") ?? undefined });
      }
      if (parts[1] === "jitsu" && parts.length === 3) {
        if (parts[2] === "workspace") result = await jitsu.workspace();
        if (parts[2] === "connections") result = await jitsu.connections();
        if (parts[2] === "metrics") result = await jitsu.metrics();
        if (parts[2] === "invocations") result = await jitsu.invocations(params);
        if (parts[2] === "deliveries") result = await jitsu.deliveries(params);
      }
      if (parts[1] === "meta" && parts.length === 3) {
        if (parts[2] === "account") result = await meta.account();
        if (parts[2] === "reconciliation") result = await google.metaReconciliation();
        if (parts[2] === "insights") result = await meta.insights(params);
      }
      if (result === undefined) return send(404, { error: "Route not found." });
      return send(200, result);
    } catch (error) {
      // Provider errors may include credential-bearing URLs. Log only classification.
      console.error(JSON.stringify({ requestId, errorType: error.constructor.name, status: error.status ?? error.code ?? 502 }));
      const status = error instanceof ApiError ? error.status : [400, 403, 404].includes(Number(error.code)) ? Number(error.code) : 502;
      return send(status, { error: safeError(error), requestId });
    }
  };
}

function safeError(error) {
  if (error instanceof ApiError) return error.message;
  if (Number(error.code) === 404) return "Upstream resource was not found.";
  if (Number(error.code) === 403) return "Upstream access was denied.";
  if (Number(error.code) === 400) return "Upstream rejected the request. Check the resource, SQL or parameters.";
  return "Upstream request failed. Retry or inspect provider permissions.";
}

async function readBody(request) {
  if (!request.headers["content-type"]?.startsWith("application/json")) throw new ApiError(415, "Use application/json.");
  let size = 0;
  const chunks = [];
  for await (const chunk of request) {
    size += chunk.length;
    if (size > 40_000) throw new ApiError(413, "Request body is too large.");
    chunks.push(chunk);
  }
  try {
    const body = JSON.parse(Buffer.concat(chunks).toString());
    if (!body || Array.isArray(body) || typeof body !== "object") throw new Error("Expected an object.");
    return body;
  }
  catch { throw new ApiError(400, "Invalid JSON body."); }
}
