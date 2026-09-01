import { isJsonObject } from "./json";
import { QUEUE_SCHEMA_VERSION } from "./jitsu";
import type {
  Fetcher,
  QueueEnvelope,
  WorkerEnv,
} from "./types";

const REQUEST_TIMEOUT_MS = 15_000;

interface TinybirdResult {
  ok: boolean;
  status: number;
  retryAfterSeconds: number;
  error?: string;
}

export async function consumeJitsuQueue(
  batch: MessageBatch<QueueEnvelope>,
  env: WorkerEnv,
  fetcher: Fetcher = fetch,
): Promise<void> {
  await Promise.all(
    batch.messages.map((message) => consumeMessage(message, env, fetcher)),
  );
}

export async function sendToTinybird(
  envelope: QueueEnvelope,
  env: WorkerEnv,
  fetcher: Fetcher = fetch,
  deliveredAt = new Date().toISOString(),
): Promise<TinybirdResult> {
  const url = tinybirdEventsUrl(env);
  const body = envelope.events
    .map((event) => JSON.stringify({ ...event, ingested_at: deliveredAt }))
    .join("\n");
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  let response: Response;
  let responseBody: string;

  try {
    response = await fetcher(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${env.TINYBIRD_APPEND_TOKEN}`,
        "Content-Type": "application/x-ndjson",
      },
      body,
      signal: controller.signal,
    });
    responseBody = await response.text();
  } finally {
    clearTimeout(timeout);
  }

  if (response.status !== 200) {
    return {
      ok: false,
      status: response.status,
      retryAfterSeconds: retryDelay(response),
      error:
        responseBody.trim() === ""
          ? "Tinybird returned an empty error response"
          : "Tinybird returned an error response",
    };
  }

  const quarantinedRows = readQuarantinedRows(responseBody);

  if (quarantinedRows > 0) {
    return {
      ok: false,
      status: response.status,
      retryAfterSeconds: 300,
      error: `Tinybird quarantined ${quarantinedRows} rows`,
    };
  }

  return {
    ok: true,
    status: response.status,
    retryAfterSeconds: 0,
  };
}

async function consumeMessage(
  message: Message<QueueEnvelope>,
  env: WorkerEnv,
  fetcher: Fetcher,
): Promise<void> {
  if (!isQueueEnvelope(message.body)) {
    console.error("jitsu_queue_message_invalid", {
      messageId: message.id,
      attempts: message.attempts,
    });
    message.retry({ delaySeconds: 300 });
    return;
  }

  let result: TinybirdResult;

  try {
    result = await sendToTinybird(message.body, env, fetcher);
  } catch (error) {
    console.error("tinybird_request_failed", {
      messageId: message.id,
      producerId: message.body.producer_id,
      attempts: message.attempts,
      error: error instanceof Error ? error.message : "Unknown error",
    });
    message.retry({ delaySeconds: 60 });
    return;
  }

  if (result.ok) {
    message.ack();
    return;
  }

  console.error("tinybird_write_rejected", {
    messageId: message.id,
    producerId: message.body.producer_id,
    attempts: message.attempts,
    status: result.status,
    error: result.error,
  });
  message.retry({ delaySeconds: result.retryAfterSeconds });
}

export function isQueueEnvelope(value: unknown): value is QueueEnvelope {
  if (!isJsonObject(value)) {
    return false;
  }

  if (value.schema_version !== QUEUE_SCHEMA_VERSION) {
    return false;
  }

  if (typeof value.producer_id !== "string" || value.producer_id === "") {
    return false;
  }

  return Array.isArray(value.events) && value.events.length > 0;
}

function tinybirdEventsUrl(env: WorkerEnv): URL {
  if (!env.TINYBIRD_APPEND_TOKEN) {
    throw new Error("TINYBIRD_APPEND_TOKEN is not configured");
  }

  if (!env.TINYBIRD_DATASOURCE) {
    throw new Error("TINYBIRD_DATASOURCE is not configured");
  }

  const baseUrl = new URL(env.TINYBIRD_API_URL);

  if (baseUrl.protocol !== "https:") {
    throw new Error("TINYBIRD_API_URL must use HTTPS");
  }

  const url = new URL("/v0/events", baseUrl);
  url.searchParams.set("name", env.TINYBIRD_DATASOURCE);
  url.searchParams.set("wait", "true");
  return url;
}

function readQuarantinedRows(body: string): number {
  if (body.trim() === "") {
    return 0;
  }

  try {
    const parsed: unknown = JSON.parse(body);

    if (!isJsonObject(parsed)) {
      return 0;
    }

    return typeof parsed.quarantined_rows === "number"
      ? parsed.quarantined_rows
      : 0;
  } catch {
    return 0;
  }
}

function retryDelay(response: Response): number {
  const retryAfter = response.headers.get("retry-after");

  if (retryAfter) {
    const seconds = Number(retryAfter);

    if (Number.isFinite(seconds) && seconds >= 0) {
      return Math.min(Math.ceil(seconds), 3_600);
    }

    const retryAt = Date.parse(retryAfter);

    if (Number.isFinite(retryAt)) {
      return Math.min(
        Math.max(Math.ceil((retryAt - Date.now()) / 1_000), 0),
        3_600,
      );
    }
  }

  if (response.status === 429 || response.status === 503) {
    return 30;
  }

  if (response.status >= 500) {
    return 60;
  }

  return 300;
}
