import { constantTimeEqual, RequestError } from "./json";
import {
  buildJitsuDelivery,
  parseJitsuRequest,
  queueEnvelopeBytes,
} from "./jitsu";
import type { QueueEnvelope, WorkerEnv } from "./types";

const MAX_SEND_BATCH_MESSAGES = 100;
const MAX_SEND_BATCH_BYTES = 240_000;

export async function handleJitsuWebhook(
  request: Request,
  env: WorkerEnv,
): Promise<Response> {
  if (!env.JITSU_WEBHOOK_TOKEN) {
    return jsonResponse({ error: "Webhook authentication is not configured" }, 503);
  }

  if (!(await isAuthorized(request, env.JITSU_WEBHOOK_TOKEN))) {
    return jsonResponse({ error: "Unauthorized" }, 401);
  }

  try {
    const events = await parseJitsuRequest(request);
    const delivery = await buildJitsuDelivery(events, env.TENANT_ID);
    await enqueueEnvelopes(env.JITSU_EVENTS_QUEUE, delivery.envelopes);

    return jsonResponse(
      {
        accepted: delivery.observations.length,
        producerId: delivery.producerId,
        queueMessages: delivery.envelopes.length,
      },
      200,
    );
  } catch (error) {
    if (error instanceof RequestError) {
      return jsonResponse({ error: error.message }, error.status);
    }

    console.error("jitsu_webhook_enqueue_failed", safeError(error));
    return jsonResponse({ error: "Queue delivery failed" }, 503);
  }
}

export async function enqueueEnvelopes(
  queue: Queue<QueueEnvelope>,
  envelopes: QueueEnvelope[],
): Promise<void> {
  let messages: Array<{ body: QueueEnvelope; contentType: "json" }> = [];
  let messageBytes = 0;

  for (const envelope of envelopes) {
    const envelopeBytes = queueEnvelopeBytes(envelope);
    const batchIsFull = messages.length === MAX_SEND_BATCH_MESSAGES;
    const batchIsTooLarge = messageBytes + envelopeBytes > MAX_SEND_BATCH_BYTES;

    if (messages.length > 0 && (batchIsFull || batchIsTooLarge)) {
      await queue.sendBatch(messages);
      messages = [];
      messageBytes = 0;
    }

    messages.push({ body: envelope, contentType: "json" });
    messageBytes += envelopeBytes;
  }

  if (messages.length > 0) {
    await queue.sendBatch(messages);
  }
}

function jsonResponse(body: unknown, status: number): Response {
  return Response.json(body, { status });
}

async function isAuthorized(request: Request, expectedToken: string): Promise<boolean> {
  const authorization = request.headers.get("authorization") ?? "";
  const prefix = "Bearer ";

  if (!authorization.startsWith(prefix)) {
    return false;
  }

  return constantTimeEqual(authorization.slice(prefix.length), expectedToken);
}

function safeError(error: unknown): string {
  return error instanceof Error ? error.message : "Unknown error";
}
