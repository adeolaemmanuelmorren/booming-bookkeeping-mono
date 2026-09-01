import { describe, expect, it, vi } from "vitest";
import { buildJitsuDelivery } from "../src/jitsu";
import { consumeJitsuQueue, sendToTinybird } from "../src/tinybird";
import type { Fetcher } from "../src/types";
import {
  formEvent,
  pageEvent,
  testBatch,
  testEnv,
  testMessage,
} from "./helpers";

describe("Tinybird Queue consumer", () => {
  it("writes NDJSON with wait=true and acknowledges the Queue message", async () => {
    const delivery = await buildJitsuDelivery(
      [pageEvent(), formEvent()],
      "boom",
    );
    const message = testMessage(delivery.envelopes[0], "queue-message-1");
    const fetcher = vi.fn<Fetcher>().mockResolvedValue(
      Response.json({ successful_rows: 2, quarantined_rows: 0 }, { status: 200 }),
    );

    await consumeJitsuQueue(testBatch([message]), testEnv(), fetcher);

    expect(message.ack).toHaveBeenCalledTimes(1);
    expect(message.retry).not.toHaveBeenCalled();
    expect(fetcher).toHaveBeenCalledTimes(1);

    const [url, init] = fetcher.mock.calls[0];
    const parsedUrl = new URL(String(url));
    const headers = new Headers(init?.headers);
    const lines = String(init?.body).split("\n");

    expect(parsedUrl.pathname).toBe("/v0/events");
    expect(parsedUrl.searchParams.get("name")).toBe(
      "jitsu_events_api_observations",
    );
    expect(parsedUrl.searchParams.get("wait")).toBe("true");
    expect(headers.get("authorization")).toBe("Bearer test-append-token");
    expect(headers.get("content-type")).toBe("application/x-ndjson");
    expect(lines).toHaveLength(2);
  });

  it("stamps the actual Tinybird delivery attempt without changing source version", async () => {
    const delivery = await buildJitsuDelivery([pageEvent()], "boom");
    const fetcher = vi.fn<Fetcher>().mockResolvedValue(
      Response.json({ successful_rows: 1, quarantined_rows: 0 }, { status: 200 }),
    );
    const deliveredAt = "2026-08-27T15:30:00.000Z";

    await sendToTinybird(delivery.envelopes[0], testEnv(), fetcher, deliveredAt);

    const [, init] = fetcher.mock.calls[0];
    const observation = JSON.parse(String(init?.body)) as {
      ingested_at: string;
      source_fact_version: number;
    };
    expect(observation.ingested_at).toBe(deliveredAt);
    expect(observation.source_fact_version).toBe(
      Date.parse("2026-08-26T12:00:00.456Z") * 1_000,
    );
  });

  it("acknowledges successful messages and retries failed messages separately", async () => {
    const firstDelivery = await buildJitsuDelivery([pageEvent("page-a")], "boom");
    const secondDelivery = await buildJitsuDelivery([pageEvent("page-b")], "boom");
    const firstMessage = testMessage(firstDelivery.envelopes[0], "queue-a");
    const secondMessage = testMessage(secondDelivery.envelopes[0], "queue-b");
    const fetcher = vi
      .fn<Fetcher>()
      .mockResolvedValueOnce(
        Response.json(
          { successful_rows: 1, quarantined_rows: 0 },
          { status: 200 },
        ),
      )
      .mockResolvedValueOnce(
        new Response("temporarily unavailable", {
          status: 503,
          headers: { "Retry-After": "45" },
        }),
      );
    vi.spyOn(console, "error").mockImplementation(() => undefined);

    await consumeJitsuQueue(
      testBatch([firstMessage, secondMessage]),
      testEnv(),
      fetcher,
    );

    expect(firstMessage.ack).toHaveBeenCalledTimes(1);
    expect(firstMessage.retry).not.toHaveBeenCalled();
    expect(secondMessage.ack).not.toHaveBeenCalled();
    expect(secondMessage.retry).toHaveBeenCalledWith({ delaySeconds: 45 });
  });

  it("retries a 200 response that quarantined rows", async () => {
    const delivery = await buildJitsuDelivery([pageEvent()], "boom");
    const result = await sendToTinybird(
      delivery.envelopes[0],
      testEnv(),
      vi.fn<Fetcher>().mockResolvedValue(
        Response.json(
          { successful_rows: 0, quarantined_rows: 1 },
          { status: 200 },
        ),
      ),
    );

    expect(result).toMatchObject({
      ok: false,
      status: 200,
      retryAfterSeconds: 300,
    });
  });
});
