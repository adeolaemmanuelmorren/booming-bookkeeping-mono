import { handleJitsuWebhook } from "./ingress";
import { consumeJitsuQueue } from "./tinybird";
import type { QueueEnvelope, WorkerEnv } from "./types";

const WEBHOOK_PATH = "/webhooks/jitsu";

export default {
  async fetch(request: Request, env: WorkerEnv): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/health" && request.method === "GET") {
      return Response.json({ ok: true, service: "jitsu-tinybird-ingest" });
    }

    if (url.pathname !== WEBHOOK_PATH) {
      return Response.json({ error: "Not found" }, { status: 404 });
    }

    if (request.method !== "POST") {
      return Response.json(
        { error: "Method not allowed" },
        { status: 405, headers: { Allow: "POST" } },
      );
    }

    return handleJitsuWebhook(request, env);
  },

  async queue(
    batch: MessageBatch<QueueEnvelope>,
    env: WorkerEnv,
  ): Promise<void> {
    await consumeJitsuQueue(batch, env);
  },
};

