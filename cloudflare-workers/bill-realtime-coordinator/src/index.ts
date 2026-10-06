import { timingSafeEqual } from "node:crypto";
import { checkSources } from "./source-check.js";
import { readAuditSource } from "./audit-read.js";
import type { StripeSourceBinding, ActiveCampaignSourceBinding } from "../../../tinybird-v1/worker/sources/contracts.ts";
import type { SourceRoute } from "../../../tinybird-v1/worker/sources/contracts.ts";
import { SourceCoordinator } from "./sources.ts";
import { Publication, type Manifest } from "./publication.ts";
import { BrowserSource } from "./browser.ts";
export { BrowserSource } from "./browser.ts";
export { SourceCoordinator } from "./sources.ts";
export { Publication, SourcePublisher } from "./publication.ts";

interface Env {
  ADMIN_TOKEN: string;
  PUBLICATION_TOKEN?: string;
  FETCHER_TOKEN?: string;
  STRIPE_SOURCE: StripeSourceBinding;
  ACTIVECAMPAIGN_SOURCE: ActiveCampaignSourceBinding & { testTagIncremental(): Promise<unknown> };
  SOURCE_COORDINATOR: DurableObjectNamespace<SourceCoordinator>;
  PUBLICATION: DurableObjectNamespace<Publication>;
  PUBLICATION_NAME?: string;
  BROWSER_SOURCE: DurableObjectNamespace<BrowserSource>;
}

function authorized(request: Request, expected: string): boolean {
  if (!expected) return false;
  const provided = new TextEncoder().encode(request.headers.get("Authorization") ?? "");
  const token = new TextEncoder().encode(`Bearer ${expected}`);
  return provided.byteLength === token.byteLength && timingSafeEqual(provided, token);
}

export default {
  async queue(batch: MessageBatch<{ bucket: string; object: { key: string } }>, env: Env) {
    const keys = batch.messages.map(message => {
      if (message.body.bucket !== "boom-tinybird-v1-browser-buffer") throw new Error("Unexpected browser bucket.");
      return message.body.object.key;
    });
    await env.BROWSER_SOURCE.getByName("boom").notify(keys);
    batch.ackAll();
  },
  async fetch(request: Request, env: Env): Promise<Response> {
    const path = new URL(request.url).pathname;
    if (path === "/healthz" && request.method === "GET") {
      return Response.json({ ok: true, realtimeEnabled: false });
    }
    const publicationAccess = path.startsWith("/admin/publication/") &&
      authorized(request, env.PUBLICATION_TOKEN ?? "");
    const fetcherAccess = path === "/admin/sources/fetcher" && authorized(request, env.FETCHER_TOKEN ?? "");
    if (!authorized(request, env.ADMIN_TOKEN) && !publicationAccess && !fetcherAccess) return new Response("Unauthorized", { status: 401 });
    if (request.method !== "POST") {
      return new Response("Not found", { status: 404 });
    }
    const headers = { "Cache-Control": "no-store" };
    try {
      if (path === "/admin/check-sources") {
        const result = await checkSources(env);
        return Response.json(result, { status: result.ok ? 200 : 502, headers });
      }
      if (path === "/admin/audit/activecampaign-tag-incremental") {
        return Response.json(await env.ACTIVECAMPAIGN_SOURCE.testTagIncremental(), { headers });
      }
      if (path === "/admin/audit/source") {
        return Response.json(await readAuditSource(env, await request.json()), { headers });
      }
      if (path.startsWith("/admin/recovery/")) {
        const target = env.PUBLICATION.getByName("boom-recovery-20260920");
        if (path.endsWith("/export-import")) {
          const body = await request.json<{old_id?:string;profile_id?:string}>();
          return Response.json(await target.importRecoveryExport(body.old_id,body.profile_id), {headers});
        }
        if (path.endsWith("/verify")) {
          const old = await env.PUBLICATION.getByName("boom").recoveryDigest();
          const next = await target.recoveryDigest();
          return Response.json({old,next,matches:old.version===next.version && old.pendingHash===next.pendingHash}, {headers});
        }
        if (path.endsWith("/storage")) return Response.json(await target.storageUsage(), {headers});
        if (path.endsWith("/prepare")) {
          const body = await request.json<{baselineId:string}>();
          return Response.json(await target.prepare(body.baselineId), {headers});
        }
        if (path.endsWith("/seed")) return Response.json(await target.seedRecovery(), {headers});
        if (path.endsWith("/import")) {
          const body = await request.json<{after?:number}>();
          return Response.json(await target.importRecoveryPending(body.after), {headers});
        }
        if (path.endsWith("/status")) return Response.json(await target.status(), {headers});
      }
      const publication = env.PUBLICATION.getByName(env.PUBLICATION_NAME ?? "boom");
      if (path.startsWith("/admin/browser/")) {
        const browser = env.BROWSER_SOURCE.getByName("boom");
        if (path.endsWith("/start")) return Response.json(await browser.start(), { headers });
        if (path.endsWith("/pause")) return Response.json(await browser.pause(), { headers });
        if (path.endsWith("/wake")) return Response.json(await browser.wake(), { headers });
        if (path.endsWith("/status")) return Response.json(await browser.status(), { headers });
      }
      if (path === "/admin/publication/identity-export/status") return Response.json(await publication.identityExportStatus(), { headers });
      if (path === "/admin/publication/identity-export/start") return Response.json(await publication.startIdentityExport(), { headers });
      if (path === "/admin/publication/storage-usage") return Response.json(await publication.storageUsage(), { headers });
      if (path === "/admin/publication/compact-heads") {
        const body = await request.json<{after?: string}>();
        return Response.json(await publication.compactSourceHeads(body.after), { headers });
      }
      if (path === "/admin/publication/compact") return Response.json(await publication.compactAcknowledged(), { headers });
      if (path === "/admin/publication/status") return Response.json(await publication.status(), { headers });
      if (path === "/admin/publication/history-root") return Response.json(await publication.historyRootStatus(), { headers });
      if (path === "/admin/publication/history-advance") {
        return Response.json(await publication.advanceHistory(await request.json()), { headers });
      }
      if (path === "/admin/publication/history-inputs") {
        const body = await request.json<{ baselineId: string; snapshotTime?: string }>();
        return Response.json(await publication.historyInputs(body.baselineId, body.snapshotTime), { headers });
      }
      if (path === "/admin/publication/history-load") {
        const body = await request.json<{ baselineId: string; events: string[]; removedFactKeys?: string[] }>();
        return Response.json(await publication.loadHistory(body.baselineId, body.events, body.removedFactKeys), { headers });
      }
      if (path === "/admin/publication/history-checked") {
        const body = await request.json<{ baselineId: string; keys: string[]; factKeys: string[]; scopes?: string[] }>();
        return Response.json(await publication.markHistoryChecked(body.baselineId, body.keys, body.factKeys, body.scopes), { headers });
      }
      if (path === "/admin/publication/prepare") {
        const body = await request.json<{ baselineId: string }>();
        return Response.json({ manifest: await publication.prepare(body.baselineId) }, { headers });
      }
      if (path === "/admin/publication/chunk") {
        const body = await request.json<{ batchId: string; offset: number }>();
        return Response.json(await publication.chunk(body.batchId, body.offset), { headers });
      }
      if (path === "/admin/publication/acknowledge") {
        return Response.json(await publication.acknowledge(await request.json<Manifest>()), { headers });
      }
      if (path.startsWith("/admin/sources/")) {
        const body = await request.json<{ route: SourceRoute; createdGte?: number; operation?: string; lease?: string; args?: unknown[]; contactIds?: string[] }>();
        const route = body.route;
        const valid = route?.source === "stripe" && ["main", "kajabi"].includes(route.account)
          || route?.source === "activecampaign" && route.account === "default";
        if (!valid) return Response.json({ error: "Invalid source route." }, { status: 400, headers });
        const name = `${route.source}:${route.account}`;
        const source = env.SOURCE_COORDINATOR.getByName(name);
        if (path === "/admin/sources/reconcile" && route.source === "activecampaign") {
          return Response.json(await source.reconcileContacts(body.contactIds!), { headers });
        }
        if (path === "/admin/sources/fetcher") return Response.json(await source.fetcher(route, body.operation!, body.lease!, body.args), { headers });
        if (path === "/admin/sources/status") return Response.json(await source.status(route), { headers });
        if (path === "/admin/sources/pause") return Response.json(await source.pause(), { headers });
        if (path === "/admin/sources/wake") return Response.json(await source.wake(route), { headers });
        if (path === "/admin/sources/start") {
          if (!Number.isSafeInteger(body.createdGte) || body.createdGte! < 0) {
            return Response.json({ error: "An explicit backfill start is required." }, { status: 400, headers });
          }
          return Response.json(await source.startBackfill(route, { backfillId: "realtime-v1", stripeCreatedGte: body.createdGte }), { headers });
        }
      }
      return new Response("Not found", { status: 404 });
    } catch (error) {
      const message = error instanceof Error ? error.message : "";
      const publicationErrors: Record<string, string> = {
        "Exceeded the maximum database size.": "database_full",
        "Invalid history batch.": "invalid_history_batch",
        "Historical source key missing.": "missing_history_key",
        "Invalid source replacement.": "invalid_source_replacement",
        "Invalid source version.": "invalid_source_version",
        "Source replacement exceeds the size limit.": "source_too_large",
        "Conflicting source retry.": "conflicting_source_retry",
        "Publication backlog is full; retry after warehouse acknowledgement.": "publication_full",
        "Identity reverse evidence points to a missing fact": "identity_missing_fact",
        "Identity component read is incomplete": "identity_incomplete_component",
        "Affected identity component exceeds the publication limit.": "identity_component_too_large",
        "Publication lease was replaced.": "publication_lease_replaced",
      };
      const code = message.startsWith("immutable inbox ID conflict:") ? "immutable_inbox_conflict"
        : message.startsWith("Conflicting identity source revision for ") ? "identity_revision_conflict"
        : message.startsWith("Profile member is missing:") ? "identity_missing_member"
        : publicationErrors[message] ?? "operation_failed";
      const frames = error instanceof Error ? error.stack?.split("\n").slice(1, 5).map(line => line.trim()) : [];
      console.error(JSON.stringify({ event: "realtime_operation_failed", code, frames }));
      // Source exceptions can contain provider data. Keep it out of public logs.
      return Response.json({ error: "Realtime operation failed; pending work is retained.", code }, { status: 502, headers });
    }
  },
};
