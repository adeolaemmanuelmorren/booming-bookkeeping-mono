import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import type { PendingIdentityFact } from "../../../worker/identity/engine.ts";
import { FivetranFactCoordinator, type FivetranFactCoordinatorEnv } from "../../../worker/fivetran/runtime-coordinator.ts";

export class TestFivetranIdentity extends FivetranFactCoordinator {}
export class TestIdentityBinding extends DurableObject {
  async status() { return { publishedVersion: 1, baseline: { identityVersion: 1, baselineId: "identity-1",
    sealHash: "a".repeat(64), snapshotAt: "2026-09-05T22:28:00.000000Z" } }; }
  async enqueue(_facts: PendingIdentityFact[]) { return { accepted: 0 }; }
}
export class TestBaselineBinding extends WorkerEntrypoint {
  async readScopes(input: { scopeIds: string[] }) { return Object.fromEntries(input.scopeIds.map(scope => [scope, []])); }
}
interface Env extends FivetranFactCoordinatorEnv { FACTS: DurableObjectNamespace<TestFivetranIdentity> }
export default {
  async fetch(request: Request, env: Env) {
    const stub = env.FACTS.getByName("fivetran:activecampaign");
    try {
      const body = request.method === "POST" ? await request.json() : undefined;
      const path = new URL(request.url).pathname;
      if (path === "/initialize") return Response.json(await stub.initializeIdentityPipeline(body as never));
      if (path === "/start") return Response.json(await stub.start(body as never));
      return Response.json(await stub.status("activecampaign"));
    } catch (error) { return new Response(error instanceof Error ? error.message : "failed", { status: 400 }); }
  },
};
