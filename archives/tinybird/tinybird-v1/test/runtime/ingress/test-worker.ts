import { DurableObject } from "cloudflare:workers";
import { BrowserIngress as ActualBrowserIngress, type BrowserIngressEnv, type BrowserQueueEnvelope } from "../../../worker/browser/ingress.ts";
import type { NormalizedBrowserEvent } from "../../../worker/browser/normalize.ts";
import { consumeJitsuQueue } from "../../../../cloudflare-workers/jitsu-tinybird-ingest/src/browser.ts";
import type { QueueEnvelope, WorkerEnv } from "../../../../cloudflare-workers/jitsu-tinybird-ingest/src/types.ts";

interface TestEnv extends BrowserIngressEnv {
  INGRESS: { receive(envelope: BrowserQueueEnvelope): Promise<unknown> };
  ROUTER: DurableObjectNamespace<TestRouter>;
  TEST_CONTROL: Fetcher;
  DISABLE_ROUTER?: string;
}

async function control(env: TestEnv, stage: string, input: unknown = null): Promise<void> {
  const response = await env.TEST_CONTROL.fetch("https://control.invalid", {
    method: "POST", body: JSON.stringify({ stage, input }),
  });
  if (!response.ok) throw new Error(await response.text());
}

/** Only the test wrapper injects failures. The production entrypoint stays unchanged. */
export class TestIngress extends ActualBrowserIngress {
  constructor(ctx: ExecutionContext, env: TestEnv) {
    const bucket = env.BROWSER_BUFFER;
    const intercepted = {
      async get(key: string) {
        await control(env, "beforeGet", key);
        return bucket.get(key);
      },
      async put(key: string, body: string, options: R2PutOptions) {
        await control(env, "beforePut", { key, body });
        const result = await bucket.put(key, body, options);
        await control(env, "afterPut", key);
        return result;
      },
    } as R2Bucket;
    super(ctx, {
      ...env,
      BROWSER_BUFFER: intercepted,
      BROWSER_ROUTER: {
        getByName(name: string) {
          if (env.DISABLE_ROUTER === "true") throw new Error("Router must not be initialized");
          return env.ROUTER.getByName(name);
        },
      },
    });
  }
}

/** Durable mock models router acceptance; the actual router has its own adverse-case suite. */
export class TestRouter extends DurableObject<TestEnv> {
  async receive(events: NormalizedBrowserEvent[]) {
    await control(this.env, "beforeRoute", events);
    await this.ctx.storage.transaction(async storage => {
      const calls = await storage.get<unknown[]>("calls") ?? [];
      calls.push({ count: events.length, bytes: new TextEncoder().encode(JSON.stringify(events)).byteLength });
      await storage.put("calls", calls);
      for (const event of events) await storage.put(`event:${event.source.delivery_event_id}`, event);
    });
    await control(this.env, "afterRoute", events);
    return { accepted: events.length };
  }

  async status() {
    return {
      calls: await this.ctx.storage.get("calls") ?? [],
      events: [...(await this.ctx.storage.list({ prefix: "event:" })).values()],
    };
  }
}

export default {
  async fetch(request: Request, env: TestEnv): Promise<Response> {
    try {
      const path = new URL(request.url).pathname;
      if (path === "/receive") return Response.json(await env.INGRESS.receive(await request.json()));
      if (path === "/status") return Response.json(await env.ROUTER.getByName("boom").status());
      if (path === "/consume") {
        const { envelope, behavior } = await request.json<{ envelope: QueueEnvelope; behavior: string }>();
        let acknowledged = 0;
        const retries: unknown[] = [];
        let forwarded: unknown = null;
        const message = {
          id: "queue-delivery", attempts: 2, body: envelope,
          ack() { acknowledged++; }, retry(options: unknown) { retries.push(options); },
        } as Message<QueueEnvelope>;
        const service = behavior === "missing" ? undefined : {
          async receive(input: QueueEnvelope) {
            // Object identity confirms the consumer did not rebuild its body or timestamps.
            if (input !== envelope) throw new Error("Consumer changed envelope reference");
            forwarded = input;
            if (behavior === "throw") throw new Error("Ambiguous service response");
            if (behavior === "actual") return env.INGRESS.receive(input);
            return { status: behavior };
          },
        };
        await consumeJitsuQueue({ messages: [message] } as unknown as MessageBatch<QueueEnvelope>, { BROWSER_INGRESS: service } as WorkerEnv);
        return Response.json({ acknowledged, retries, forwarded });
      }
      return new Response("Not found", { status: 404 });
    } catch (error) {
      return Response.json({ error: String(error) }, { status: 500 });
    }
  },
};
