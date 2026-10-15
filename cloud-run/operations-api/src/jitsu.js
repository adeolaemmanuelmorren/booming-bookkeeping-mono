import { ApiError, identifier, integer } from "./errors.js";

export function createJitsuProvider({ apiKey, workspaceId, fetchImpl = fetch }) {
  let canonicalId;
  async function get(path, params = {}) {
    if (!apiKey) throw new ApiError(503, "Jitsu management API key is not configured.");
    if (!workspaceId) throw new ApiError(503, "Jitsu workspace ID is not configured.");
    const url = new URL(`https://use.jitsu.com/api/${path}`);
    for (const [key, value] of Object.entries(params)) url.searchParams.set(key, value);
    const response = await fetchImpl(url, {
      headers: { Authorization: `Bearer ${apiKey}` }, signal: AbortSignal.timeout(25_000),
    });
    if (!response.ok) throw new ApiError(502, `Jitsu request failed with HTTP ${response.status}.`);
    return response;
  }

  async function logs(type, params) {
    const actor = params.get("actorId");
    const path = `${workspaceId}/log/${type}${actor ? `/${identifier(actor)}` : ""}`;
    const query = { limit: integer(params.get("limit"), 50) };
    for (const key of ["start", "end"]) {
      const value = params.get(key);
      if (!value) continue;
      if (!Number.isFinite(Date.parse(value))) throw new ApiError(400, `Invalid ${key} timestamp.`);
      query[key] = new Date(value).toISOString();
    }
    if (query.start && query.end && query.start >= query.end) throw new ApiError(400, "start must precede end.");
    if (params.get("levels")) {
      if (!/^(debug|info|warn|error)(,(debug|info|warn|error))*$/.test(params.get("levels"))) {
        throw new ApiError(400, "levels must contain debug, info, warn or error separated by commas.");
      }
      query.levels = params.get("levels");
    }
    if (params.get("search")) query.search = params.get("search").slice(0, 500);
    const response = await get(path, query);
    const body = await response.text();
    const records = body.split("\n").filter(line => line.trim()).map(line => JSON.parse(line));
    return { workspaceId, type, records, limit: query.limit,
      note: "Operational log records within Jitsu retention. Function logs are not a complete execution ledger." };
  }

  return {
    workspace: async () => (await get(`workspace/${workspaceId}`)).json(),
    metrics: async () => ({ format: "prometheus", text: await (await get(`${workspaceId}/metrics`)).text() }),
    async connections() {
      if (!canonicalId) {
        const workspace = await (await get(`workspace/${workspaceId}`)).json();
        canonicalId = workspace.id ?? workspace.workspace?.id;
        if (!canonicalId) throw new ApiError(502, "Jitsu did not return a workspace ID.");
      }
      // Configuration can contain source and destination credentials. Only select operational fields.
      const data = await (await get(`${canonicalId}/config/link`)).json();
      const records = Array.isArray(data) ? data : data.links ?? data.objects ?? [];
      return { connections: records.map(item => ({ id: item.id, name: item.name,
        fromId: item.fromId, toId: item.toId, disabled: item.disabled, updatedAt: item.updatedAt })) };
    },
    invocations: params => logs("function", params),
    deliveries: params => {
      const mode = params.get("mode") ?? "batch";
      if (!["batch", "stream"].includes(mode)) throw new ApiError(400, "Delivery mode must be batch or stream.");
      return logs(`bulker_${mode}`, params);
    },
    async status() {
      const params = new URLSearchParams({ limit: "20", levels: "error",
        start: new Date(Date.now() - 3_600_000).toISOString() });
      const [metrics, batches, streams, functions] = await Promise.all([
        (await get(`${workspaceId}/metrics`)).text(),
        logs("bulker_batch", params), logs("bulker_stream", params), logs("function", params),
      ]);
      const count = batches.records.length + streams.records.length + functions.records.length;
      return { status: count ? "degraded" : "ok", metrics: { format: "prometheus", text: metrics },
        recentErrors: { batches: batches.records, streams: streams.records, functions: functions.records },
        note: "API access and last-hour function, batch and stream error logs checked. Metrics counters are cumulative." };
    },
  };
}
