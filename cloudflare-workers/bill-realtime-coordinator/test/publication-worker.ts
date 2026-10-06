import { SourceCoordinator } from "../src/sources.ts";
export { SourceCoordinator } from "../src/sources.ts";
import { Publication } from "../src/publication.ts";
import { BrowserSource, browserReplacement } from "../src/browser.ts";
export { BrowserSource } from "../src/browser.ts";
export { Publication } from "../src/publication.ts";

export default {
  async fetch(request: Request, env: { SOURCE_COORDINATOR: DurableObjectNamespace<SourceCoordinator>; PUBLICATION: DurableObjectNamespace<Publication>; BROWSER_SOURCE: DurableObjectNamespace<BrowserSource> }) {
    const originalPath = new URL(request.url).pathname;
    const path = originalPath.replace(/^\/(old|new)/, "");
    const stub = env.PUBLICATION.getByName(originalPath.startsWith("/old/") ? "boom" : originalPath.startsWith("/new/") ? "boom-recovery-20260920" : "test");
    const body = await request.json<any>();
    try {
      if (path === "/reconcile") return Response.json(await env.SOURCE_COORDINATOR.getByName("fetcher-test").reconcileContacts(body.contactIds));
      if (path === "/fetcher") return Response.json(await env.SOURCE_COORDINATOR.getByName("fetcher-test").fetcher(body.route, body.operation, body.lease, body.args));
      if (path === "/browser-contract") return Response.json(await browserReplacement(body));
      if (path === "/history-root") return Response.json(await stub.historyRootStatus());
      if (path === "/history-advance") return Response.json(await stub.advanceHistory(body));
      if (path === "/history-inputs") return Response.json(await stub.historyInputs(body.baselineId, body.snapshotTime));
      if (path === "/seed") return Response.json(await stub.seedRecovery());
      if (path === "/import") return Response.json(await stub.importRecoveryPending(body.after));
      if (path === "/storage-usage") return Response.json(await stub.storageUsage());
      if (path === "/compact-heads") return Response.json(await stub.compactSourceHeads(body.after));
      if (path === "/compact") return Response.json(await stub.compactAcknowledged());
      if (path === "/history-load") return Response.json(await stub.loadHistory(body.baselineId, body.events, body.removedFactKeys));
      if (path === "/history-checked") return Response.json(await stub.markHistoryChecked(body.baselineId, body.keys, body.factKeys, body.scopes));
      if (path === "/browser-notify") return Response.json(await env.BROWSER_SOURCE.getByName("boom").notify(body.keys));
      if (path === "/browser-published") {
        const published = env.PUBLICATION.getByName("boom");
        const manifest = await published.prepare(body.baselineId);
        return Response.json(await published.chunk(manifest!.batchId, 0));
      }
      if (path === "/browser-start") return Response.json(await env.BROWSER_SOURCE.getByName("boom").start());
      if (path === "/browser-pause") return Response.json(await env.BROWSER_SOURCE.getByName("boom").pause());
      if (path === "/browser-status") return Response.json(await env.BROWSER_SOURCE.getByName("boom").status());
      if (path === "/enqueue") return Response.json(await stub.enqueue(body));
      if (path === "/prepare") return Response.json(await stub.prepare(body.baselineId));
      if (path === "/chunk") return Response.json(await stub.chunk(body.batchId, body.offset));
      if (path === "/ack") return Response.json(await stub.acknowledge(body));
      return Response.json(await stub.status());
    } catch (error) {
      return Response.json({ error: String(error) }, { status: 409 });
    }
  },
};
