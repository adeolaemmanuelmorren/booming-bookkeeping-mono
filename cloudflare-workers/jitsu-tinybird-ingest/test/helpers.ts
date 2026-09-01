import { vi } from "vitest";
import type { QueueEnvelope, WorkerEnv } from "../src/types";

export function testEnv(
  queue: Queue<QueueEnvelope> = testQueue(),
): WorkerEnv {
  return {
    JITSU_EVENTS_QUEUE: queue,
    JITSU_WEBHOOK_TOKEN: "test-webhook-token",
    TENANT_ID: "boom",
    TINYBIRD_API_URL: "https://api.us-east.tinybird.co",
    TINYBIRD_APPEND_TOKEN: "test-append-token",
    TINYBIRD_DATASOURCE: "jitsu_events_api_observations",
  };
}

export function testQueue(): Queue<QueueEnvelope> {
  return {
    send: vi.fn(),
    sendBatch: vi.fn().mockResolvedValue(undefined),
  } as unknown as Queue<QueueEnvelope>;
}

export function testMessage(
  body: QueueEnvelope,
  id = crypto.randomUUID(),
): Message<QueueEnvelope> {
  return {
    id,
    timestamp: new Date("2026-08-26T12:00:00.000Z"),
    body,
    attempts: 1,
    ack: vi.fn(),
    retry: vi.fn(),
  } as unknown as Message<QueueEnvelope>;
}

export function testBatch(
  messages: Array<Message<QueueEnvelope>>,
): MessageBatch<QueueEnvelope> {
  return {
    queue: "jitsu-tinybird-events",
    messages,
    ackAll: vi.fn(),
    retryAll: vi.fn(),
  } as unknown as MessageBatch<QueueEnvelope>;
}

export function pageEvent(messageId = "page-1") {
  return {
    type: "page",
    messageId,
    timestamp: "2026-08-26T11:59:59.123Z",
    receivedAt: "2026-08-26T12:00:00.456Z",
    anonymousId: "anonymous-1",
    userId: "person@example.com",
    properties: {
      url: "https://thebookkeepingchallenge.com/register",
      path: "/register",
      referrer: "https://facebook.com/",
    },
    context: {
      traits: {
        email: "person@example.com",
      },
      attribution: {
        utm_source: "facebook",
        utm_medium: "paid_social",
        utm_campaign: "challenge",
        fbclid: "fb-click-1",
      },
    },
  };
}

export function formEvent(messageId = "form-1") {
  return {
    type: "track",
    event: "Form Submitted",
    messageId,
    timestamp: "2026-08-26T12:00:01.123Z",
    receivedAt: "2026-08-26T12:00:02.456Z",
    anonymousId: "anonymous-1",
    properties: {
      form_id: "registration",
      form_name: "Challenge registration",
      form_action: "submit",
      submitted_at: "2026-08-26T11:58:00.123Z",
      email: "person@example.com",
      phone: "+1 808 555 0199",
    },
  };
}
