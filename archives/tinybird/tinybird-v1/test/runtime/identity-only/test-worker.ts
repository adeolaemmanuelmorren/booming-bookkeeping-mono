import { IdentityCoordinator } from "../../../worker/identity/coordinator.ts";
import { WorkerEntrypoint } from "cloudflare:workers";

export class TestIdentity extends IdentityCoordinator {}
export class TestBaseline extends WorkerEntrypoint { async readRecords() { return []; } }
interface Env { IDENTITY: DurableObjectNamespace<TestIdentity> }

export default {
  async fetch(request: Request, env: Env) {
    const identity = env.IDENTITY.getByName("boom");
    try {
      const path = new URL(request.url).pathname;
      if (path === "/activate") return Response.json(await identity.activateBaseline());
      if (path === "/enqueue") return Response.json(await identity.enqueue(await request.json()));
      return Response.json(await identity.status());
    } catch (error) {
      return new Response(error instanceof Error ? error.message : "failed", { status: 400 });
    }
  },
};
