import { timingSafeEqual } from "node:crypto";

const STREAMS = new Set([
  "charges", "customers", "payment_intents", "invoices", "invoice_line_items",
  "checkout_sessions", "checkout_sessions_line_items", "subscriptions",
  "products", "prices", "plans", "coupons", "promotion_codes",
]);

export function validate(path, body) {
  if (!["stripe", "stripe_kajabi"].includes(body.account)) return false;
  if (path === "/window") return true;
  if (path === "/complete") {
    return /^[a-zA-Z0-9_-]{1,100}$/.test(body.workflowId)
      && Number.isSafeInteger(body.start) && Number.isSafeInteger(body.end)
      && body.start > 0 && body.end > body.start && body.end - body.start <= 25 * 3600
      && body.end <= Math.floor(Date.now() / 1000);
  }
  if (path !== "/records") return false;
  if (!/^[a-zA-Z0-9_:-]{1,180}$/.test(body.batchId)) return false;
  if (!Number.isFinite(Date.parse(body.observedAt))) return false;
  if (Date.parse(body.observedAt) > Date.now() + 60_000) return false;
  if (!Array.isArray(body.records) || !body.records.length || body.records.length > 2000) return false;
  return body.records.every(record => STREAMS.has(record.stream)
    && typeof record.id === "string" && /^[a-zA-Z0-9_-]{1,200}$/.test(record.id)
    && record.payload?.id === record.id && !record.payload.client_secret);
}

export function handler({ token, warehouse }) {
  if (!token || token.length < 32) throw new Error("Receiver token is missing");
  return async (request, response) => {
    response.setHeader("Cache-Control", "no-store");
    response.setHeader("Content-Type", "application/json");
    const send = (status, body) => { response.writeHead(status); response.end(JSON.stringify(body)); };
    const supplied = Buffer.from(request.headers.authorization ?? "");
    const expected = Buffer.from(`Bearer ${token}`);
    if (supplied.length !== expected.length || !timingSafeEqual(supplied, expected)) return send(401, { error: "Unauthorized" });
    if (request.method !== "POST" || !["/window", "/records", "/complete"].includes(request.url)) return send(404, { error: "Not found" });
    try {
      let size = 0;
      const chunks = [];
      for await (const chunk of request) {
        size += chunk.length;
        if (size > 4_000_000) return send(413, { error: "Batch too large" });
        chunks.push(chunk);
      }
      let body;
      try { body = JSON.parse(Buffer.concat(chunks).toString("utf8")); }
      catch { return send(400, { error: "Invalid JSON" }); }
      if (!body || !validate(request.url, body)) return send(400, { error: "Invalid batch" });
      const result = await warehouse(request.url, body);
      return send(200, result);
    } catch (error) {
      // Never log Stripe records, access tokens, or SQL parameters.
      console.error(JSON.stringify({ event: "backup_receiver_failed", code: error.code ?? "unknown" }));
      return send(503, { error: "Warehouse request failed" });
    }
  };
}
