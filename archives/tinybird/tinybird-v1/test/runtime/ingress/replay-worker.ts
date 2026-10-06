import { DurableObject } from "cloudflare:workers";
import { BrowserIngress, type BrowserQueueEnvelope } from "../../../worker/browser/ingress.ts";
import { BrowserBufferReplay, type BrowserBufferReplayEnv } from "../../../worker/browser/buffer-replay.ts";
import type { NormalizedBrowserEvent } from "../../../worker/browser/normalize.ts";

interface Env extends BrowserBufferReplayEnv {
  REPLAY: DurableObjectNamespace<TestReplay>;
  BROWSER_ROUTER: DurableObjectNamespace<TestRouter>;
  CONTROL: Fetcher;
  AUTO_PUBLISH?: string;
  REAL_ALARMS?: string;
}
async function control(env: Env, stage: string, input: unknown = null) {
  const response = await env.CONTROL.fetch("https://control.invalid", { method: "POST", body: JSON.stringify({ stage, input }) });
  if (!response.ok) throw new Error(`Injected ${stage} failure`);
}

export { BrowserIngress };
export class TestReplay extends BrowserBufferReplay {
  constructor(ctx: DurableObjectState, env: Env) {
    const bucket = env.BROWSER_BUFFER;
    super(ctx, { ...env, BROWSER_BUFFER: {
      async list(options: R2ListOptions) {
        await control(env, "beforeList", options);
        const result = await bucket.list(options);
        await control(env, "afterList", { keys: result.objects.map(object => object.key), truncated: result.truncated });
        return result;
      },
      async get(key: string) { await control(env, "beforeGet", key); return bucket.get(key); },
      async delete(key: string) {
        await control(env, "beforeDelete", key);
        await bucket.delete(key);
        await control(env, "afterDelete", key);
      },
    } as R2Bucket });
  }
  async start() { const result = await super.start(); await this.parkAlarm(); return result; }
  async alarm() { await super.alarm(); await this.parkAlarm(); }
  async step(scan = true) {
    this.ctx.storage.sql.exec("UPDATE buffer_replay_items SET next_attempt = 0");
    if (scan) this.ctx.storage.sql.exec("UPDATE buffer_replay_meta SET next_scan_at = 0");
    await this.alarm();
    return this.status();
  }
  async expireLease() {
    this.ctx.storage.sql.exec("UPDATE buffer_replay_meta SET lease_until = 0");
  }
  async pendingLease() { return this.ctx.storage.sql.exec("SELECT lease_id, lease_until FROM buffer_replay_meta").one(); }
  private async parkAlarm() {
    if ((this.env as Env).REAL_ALARMS === "true") return;
    await this.ctx.storage.setAlarm(Date.now() + 3_600_000);
  }
}

export class TestRouter extends DurableObject<Env> {
  async receive(events: NormalizedBrowserEvent[]) {
    await control(this.env, "beforeReceive", events.map(event => event.source.delivery_event_id));
    const result = await this.ctx.storage.transaction(async storage => {
      let count = 0;
      for (const event of events) {
        const key = `event:${event.source.delivery_event_id}`;
        if (await storage.get(key)) continue;
        await storage.put(key, event);
        count++;
      }
      let sequence = await storage.get<number>("sequence") ?? 1;
      if (count) sequence++;
      await storage.put("sequence", sequence);
      if (this.env.AUTO_PUBLISH === "true") await storage.put("published", sequence);
      await storage.put("calls", (await storage.get<number>("calls") ?? 0) + 1);
      await storage.put("count", (await storage.get<number>("count") ?? 0) + count);
      return { sequence, accepted: count, quarantined: 0 };
    });
    await control(this.env, "afterReceive", result);
    return result;
  }
  async status() {
    await control(this.env, "beforeStatus");
    return { sequence: await this.ctx.storage.get<number>("sequence") ?? 1, publishedSequence: await this.ctx.storage.get<number>("published") ?? 1 };
  }
  async publish() { await this.ctx.storage.put("published", await this.ctx.storage.get<number>("sequence") ?? 1); }
  async inspect() { return { ...await this.status(), count: await this.ctx.storage.get<number>("count") ?? 0, calls: await this.ctx.storage.get<number>("calls") ?? 0 }; }
}

export default {
  async fetch(request: Request, env: Env) {
    const replay = env.REPLAY.getByName("boom");
    const path = new URL(request.url).pathname;
    try {
      if (path === "/start") return Response.json(await replay.start());
      if (path === "/step") return Response.json(await replay.step((await request.json<{ scan?: boolean }>()).scan ?? true));
      if (path === "/status") return Response.json(await replay.status());
      if (path === "/pause") return Response.json(await replay.pause());
      if (path === "/expire") { await replay.expireLease(); return Response.json(null); }
      if (path === "/lease") return Response.json(await replay.pendingLease());
      if (path === "/publish") { await env.BROWSER_ROUTER.getByName("boom").publish(); return Response.json(null); }
      if (path === "/router") return Response.json(await env.BROWSER_ROUTER.getByName("boom").inspect());
      if (path === "/old-buffer") {
        const input = await request.json<{ key: string; body: string }>();
        await control(env, "oldBeforePut");
        await env.BROWSER_BUFFER.put(input.key, input.body);
        return Response.json({ status: "buffered" });
      }
      return new Response("Not found", { status: 404 });
    } catch (error) { return Response.json({ error: String(error) }, { status: 500 }); }
  },
};
