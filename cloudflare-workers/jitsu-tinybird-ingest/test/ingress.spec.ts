import { describe, expect, it, vi } from "vitest";
import { handleJitsuWebhook } from "../src/ingress";
import {
  buildJitsuDelivery,
  MAX_QUEUE_MESSAGE_BYTES,
  parseJitsuRequest,
  queueEnvelopeBytes,
} from "../src/jitsu";
import type { QueueEnvelope } from "../src/types";
import { formEvent, pageEvent, testEnv, testQueue } from "./helpers";

describe("Jitsu webhook ingress", () => {
  it("rejects requests without the shared bearer token", async () => {
    const queue = testQueue();
    const request = new Request("https://example.com/webhooks/jitsu", {
      method: "POST",
      body: JSON.stringify({ batch: [pageEvent()] }),
      headers: { "Content-Type": "application/json" },
    });

    const response = await handleJitsuWebhook(request, testEnv(queue));

    expect(response.status).toBe(401);
    expect(queue.sendBatch).not.toHaveBeenCalled();
  });

  it("accepts a Jitsu JSON batch and enqueues normalized observations", async () => {
    const queue = testQueue();
    const request = new Request("https://example.com/webhooks/jitsu", {
      method: "POST",
      body: JSON.stringify({ batch: [formEvent(), pageEvent()] }),
      headers: {
        Authorization: "Bearer test-webhook-token",
        "Content-Type": "application/json",
      },
    });

    const response = await handleJitsuWebhook(request, testEnv(queue));
    const result = (await response.json()) as {
      accepted: number;
      producerId: string;
      queueMessages: number;
    };

    expect(response.status).toBe(200);
    expect(result.accepted).toBe(2);
    expect(result.queueMessages).toBe(1);
    expect(result.producerId).toMatch(/^jitsu-webhook-v1:[A-F0-9]{64}$/);
    expect(queue.sendBatch).toHaveBeenCalledTimes(1);

    const firstCall = vi.mocked(queue.sendBatch).mock.calls[0][0];
    const envelope = Array.from(firstCall)[0].body as QueueEnvelope;

    expect(envelope.events.map((event) => event.message_id)).toEqual([
      "form-1",
      "page-1",
    ]);
    expect(envelope.events.map((event) => event.producer_sequence)).toEqual([1, 2]);
    expect(envelope.events[0]).toMatchObject({
      event_kind: "client_form",
      email: "person@example.com",
      form_id: "registration",
      submitted_at: "2026-08-26T11:58:00.123Z",
      source_deleted: 0,
    });
    expect(envelope.events[1]).toMatchObject({
      event_kind: "page_view",
      utm_source: "facebook",
      fbclid: "fb-click-1",
    });
    expect(envelope.events[0].payload_hash).toMatch(/^[A-F0-9]{64}$/);
    expect(envelope.events[0].fact_payload_hash).toMatch(/^[A-F0-9]{64}$/);
    expect(envelope.events[0].source_fact_version).toBe(
      Date.parse("2026-08-26T12:00:02.456Z") * 1_000,
    );
    expect(Object.keys(envelope.events[0]).sort()).toEqual(
      [
        "ad_id",
        "adset_id",
        "amount",
        "anonymous_id",
        "campaign_id",
        "currency",
        "customer_name",
        "delivery_event_id",
        "email",
        "event_kind",
        "event_timestamp",
        "fact_payload",
        "fact_payload_hash",
        "fbclid",
        "first_name",
        "form_action",
        "form_id",
        "form_name",
        "ingested_at",
        "is_checkout_form",
        "is_payment_confirmed",
        "last_name",
        "message_id",
        "observed_at",
        "page_path",
        "page_referrer",
        "page_url",
        "payload_hash",
        "payment_status",
        "phone",
        "producer_id",
        "producer_sequence",
        "product_id",
        "product_name",
        "products",
        "source_deleted",
        "source_fact_version",
        "submitted_at",
        "tenant_id",
        "user_id",
        "utm_campaign",
        "utm_content",
        "utm_id",
        "utm_medium",
        "utm_source",
        "utm_term",
        "value",
      ].sort(),
    );
  });

  it("keeps Dataform timestamp and value facts separate", async () => {
    const event = {
      type: "track",
      event: "Order Completed",
      messageId: "order-1",
      timestamp: "2026-08-26T12:00:00.000Z",
      receivedAt: "2026-08-26T12:05:00.000Z",
      properties: {
        submitted_at: "2026-08-26T11:59:00.000Z",
        amount: 125,
        value: 99,
      },
    };

    const delivery = await buildJitsuDelivery([event], "boom");

    expect(delivery.observations[0]).toMatchObject({
      amount: 125,
      event_timestamp: "2026-08-26T12:00:00.000Z",
      observed_at: "2026-08-26T12:00:00.000Z",
      submitted_at: "2026-08-26T11:59:00.000Z",
      value: 99,
    });
  });

  it("parses NDJSON events", async () => {
    const request = new Request("https://example.com/webhooks/jitsu", {
      method: "POST",
      body: `${JSON.stringify(pageEvent())}\n${JSON.stringify(formEvent())}\n`,
      headers: { "Content-Type": "application/x-ndjson" },
    });

    const events = await parseJitsuRequest(request);

    expect(events).toHaveLength(2);
  });

  it("keeps semantic delivery metadata stable across Jitsu retries", async () => {
    const first = await buildJitsuDelivery(
      [pageEvent(), formEvent()],
      "boom",
    );
    const retried = await buildJitsuDelivery(
      [formEvent(), pageEvent()],
      "boom",
    );

    expect(retried.producerId).toBe(first.producerId);
    expect(retried.observations.map(({ ingested_at: _ingestedAt, ...event }) => event))
      .toEqual(first.observations.map(({ ingested_at: _ingestedAt, ...event }) => event));
  });

  it("splits a delivery into Queue-safe messages", async () => {
    const events = Array.from({ length: 20 }, (_, index) => ({
      ...pageEvent(`page-${index.toString().padStart(2, "0")}`),
      properties: {
        ...pageEvent().properties,
        title: "x".repeat(2_000),
      },
    }));
    const delivery = await buildJitsuDelivery(events, "boom", 10_000);

    expect(delivery.envelopes.length).toBeGreaterThan(1);
    expect(delivery.envelopes.flatMap((envelope) => envelope.events)).toEqual(
      delivery.observations,
    );

    for (const envelope of delivery.envelopes) {
      expect(queueEnvelopeBytes(envelope)).toBeLessThanOrEqual(10_000);
      expect(queueEnvelopeBytes(envelope)).toBeLessThan(MAX_QUEUE_MESSAGE_BYTES);
    }
  });

  it("rejects a single event that cannot fit in one Queue message", async () => {
    const baseEvent = pageEvent();
    const oversizedEvent = {
      ...baseEvent,
      properties: {
        ...baseEvent.properties,
        title: "x".repeat(130_000),
      },
    };
    const request = new Request("https://example.com/webhooks/jitsu", {
      method: "POST",
      body: JSON.stringify({ batch: [oversizedEvent] }),
      headers: {
        Authorization: "Bearer test-webhook-token",
        "Content-Type": "application/json",
      },
    });

    const response = await handleJitsuWebhook(request, testEnv());

    expect(response.status).toBe(413);
  });
});
