import { DurableObject } from "cloudflare:workers";
import { IdentityTouchpointReplay } from "../../../worker/identity/touchpoint-replay.ts";
export class TestReplay extends IdentityTouchpointReplay { async run() { await this.alarm(); return this.status(); } }
export class TestIdentity extends DurableObject {
  async status() { return { publishedVersion: 1, baseline: { baselineId: "identity-1", identityVersion: 1 } }; }
  async enqueue(facts: unknown[]) { this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS accepted(n INTEGER)"); this.ctx.storage.sql.exec("INSERT INTO accepted VALUES(?)", facts.length); }
}
interface Env { REPLAY: DurableObjectNamespace<TestReplay>; BROWSER_BUFFER: R2Bucket }
export default { async fetch(request: Request, env: Env) {
  const url = new URL(request.url); const replay = env.REPLAY.getByName("boom");
  try {
    if (url.pathname === "/put") { const body = await request.text(); const hash = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(body));
      const key = `jitsu/envelopes/${[...new Uint8Array(hash)].map(v => v.toString(16).padStart(2,"0")).join("")}.json`; await env.BROWSER_BUFFER.put(key, body); return Response.json({ key }); }
    if (url.pathname === "/stage") return Response.json(await replay.stage(await request.json()));
    if (url.pathname === "/start") return Response.json(await replay.start());
    if (url.pathname === "/run") return Response.json(await replay.run());
    return Response.json(await replay.status());
  } catch (error) { return new Response(error instanceof Error ? error.message : "failed", { status: 400 }); }
} };
