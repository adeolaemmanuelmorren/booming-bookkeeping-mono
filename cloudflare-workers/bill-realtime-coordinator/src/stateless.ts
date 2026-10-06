import { timingSafeEqual } from "node:crypto";
import { readAuditSource } from "./audit-read.js";

// Reporting state and calculation now live in BigQuery. Keep only the
// authenticated source-read endpoint for existing diagnostic callers.
export default {
  async fetch(request: Request, env: { ADMIN_TOKEN: string }): Promise<Response> {
    const path = new URL(request.url).pathname;
    if (request.method === "GET" && path === "/healthz") {
      return Response.json({ ok: true, engine: "warehouse", durableObjectReporting: false });
    }
    const expected = new TextEncoder().encode(`Bearer ${env.ADMIN_TOKEN ?? ""}`);
    const provided = new TextEncoder().encode(request.headers.get("Authorization") ?? "");
    if (!env.ADMIN_TOKEN || expected.length !== provided.length || !timingSafeEqual(expected, provided)) {
      return new Response("Unauthorized", { status: 401 });
    }
    if (request.method !== "POST" || path !== "/admin/audit/source") {
      return Response.json({ error: "Legacy reporting retired. Use the warehouse reporting API." }, { status: 410 });
    }
    try {
      return Response.json(await readAuditSource(env, await request.json()), {
        headers: { "Cache-Control": "no-store" },
      });
    } catch {
      return Response.json({ error: "Source read failed." }, { status: 502 });
    }
  },
};
