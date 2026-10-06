import { WorkerEntrypoint } from "cloudflare:workers";
import { normalizeJitsu, type JitsuObservationInput, type NormalizedBrowserEvent } from "./normalize.ts";
import { browserBatches } from "./batches.ts";
import { sha256 } from "../storage/json.ts";
import { Tinybird, type TinybirdEnv } from "../storage/tinybird.ts";
import { publishRawObservations, rawObservations, type ArchivedEnvelope } from "./raw-storage.ts";

export interface BrowserQueueEnvelope {
  schema_version: "jitsu_events_api_v1";
  producer_id: string;
  events: JitsuObservationInput[];
}

export interface BrowserIngressReceipt {
  status: "buffered" | "stored" | "accepted";
  key: string;
  sha256: string;
  eventCount: number;
}

export interface BrowserIngressEnv extends Partial<TinybirdEnv> {
  TENANT_ID: string;
  BROWSER_BUFFER: R2Bucket;
  BROWSER_INGRESS_MODE?: string;
  BROWSER_BASELINE_ID?: string;
  BROWSER_BASELINE_SEQUENCE?: string;
  BROWSER_IDENTITY_REPLAY?: {
    getByName(name: string): { stage(keys: string[]): Promise<unknown> };
  };
  BROWSER_ROUTER?: {
    getByName(name: string): {
      receive(events: NormalizedBrowserEvent[]): Promise<unknown>;
    };
  };
}

/** Private service-binding entrypoint. The parent owns the Worker's public HTTP handler. */
export class BrowserIngress extends WorkerEntrypoint<BrowserIngressEnv> {
  async receive(envelope: BrowserQueueEnvelope): Promise<BrowserIngressReceipt> {
    return (await this.receiveBatch([envelope]))[0];
  }

  async receiveBatch(envelopes: BrowserQueueEnvelope[]): Promise<BrowserIngressReceipt[]> {
    if (!this.env.TENANT_ID) throw new Error("TENANT_ID is required");
    if (!Array.isArray(envelopes) || !envelopes.length || envelopes.length > 8) throw new Error("Invalid envelope batch size");
    const bytes = new TextEncoder().encode(JSON.stringify(envelopes)).byteLength;
    if (bytes > 1_000_000) throw new Error("Envelope batch exceeds the ingress limit");
    const archives: ArchivedEnvelope[] = [];
    const receipts: BrowserIngressReceipt[] = [];
    for (const envelope of envelopes) {
      const body = JSON.stringify(envelope);
      if (body === undefined) throw new Error("Queue envelope is not JSON serializable");
      const hash = await sha256(body);
      const key = `jitsu/envelopes/${hash}.json`;

      // The body is the exact serialized queue value. Do not rewrite arrival or source timestamps.
      const receivedAt = await this.archive(key, body, hash);
      validateEnvelope(envelope, this.env.TENANT_ID);
      archives.push({ envelope, hash, receivedAt });
      receipts.push({ key, sha256: hash, eventCount: envelope.events.length, status: "buffered" });
    }
    const mode = this.env.BROWSER_INGRESS_MODE ?? "buffer";
    if (mode === "buffer") return receipts;
    if (mode === "collect") {
      if (!this.env.TINYBIRD_URL || !this.env.TINYBIRD_TOKEN) throw new Error("Raw touchpoint storage is not configured");
      const client = new Tinybird({ TINYBIRD_URL: this.env.TINYBIRD_URL, TINYBIRD_TOKEN: this.env.TINYBIRD_TOKEN });
      await publishRawObservations(client, await rawObservations(archives));
      if (this.env.BROWSER_IDENTITY_REPLAY) {
        const keys = [...new Set(receipts.map(receipt => receipt.key))];
        this.ctx.waitUntil(this.env.BROWSER_IDENTITY_REPLAY.getByName(this.env.TENANT_ID).stage(keys).catch(() => {
          console.error('identity_staging_retry_needed', { envelopes: keys.length });
        }));
      }
      return receipts.map(receipt => ({ ...receipt, status: "stored" }));
    }
    if (mode !== "live") throw new Error("BROWSER_INGRESS_MODE must be buffer, collect or live");

    // Activation is a separate operator action after the baseline manifest has been sealed.
    const baselineSequence = Number(this.env.BROWSER_BASELINE_SEQUENCE ?? "0");
    if (!this.env.BROWSER_BASELINE_ID || this.env.BROWSER_BASELINE_ID === "empty" || !Number.isSafeInteger(baselineSequence) || baselineSequence < 1) {
      throw new Error("Historical baseline has not been activated");
    }
    if (!this.env.BROWSER_ROUTER) throw new Error("BROWSER_ROUTER is not configured");

    // Validate and split the complete envelope before sending its first event to the router.
    const events: NormalizedBrowserEvent[] = [];
    for (const envelope of envelopes) {
      for (const observation of envelope.events) events.push(await normalizeJitsu(observation));
    }
    const batches = browserBatches(events);
    const router = this.env.BROWSER_ROUTER.getByName(this.env.TENANT_ID);
    for (const batch of batches) await router.receive(batch);
    return receipts.map(receipt => ({ ...receipt, status: "accepted" }));
  }

  private async archive(key: string, body: string, hash: string): Promise<string> {
    let stored = await this.env.BROWSER_BUFFER.get(key);
    if (!stored) {
      await this.env.BROWSER_BUFFER.put(key, body, {
        onlyIf: new Headers({ "If-None-Match": "*" }),
        httpMetadata: { contentType: "application/json" },
        customMetadata: { sha256: hash, tenant_id: this.env.TENANT_ID },
      });
      stored = await this.env.BROWSER_BUFFER.get(key);
    }
    if (!stored || await stored.text() !== body) throw new Error("Buffered envelope failed exact readback verification");
    return stored.uploaded.toISOString();
  }
}

function validateEnvelope(value: BrowserQueueEnvelope, tenantId: string): void {
  if (!value || typeof value !== "object" || value.schema_version !== "jitsu_events_api_v1" || typeof value.producer_id !== "string" || !value.producer_id) {
    throw new Error("Invalid Jitsu queue envelope");
  }
  if (!Array.isArray(value.events) || !value.events.length) throw new Error("Jitsu queue envelope has no events");
  for (const event of value.events) {
    if (!event || typeof event !== "object" || event.tenant_id !== tenantId || event.producer_id !== value.producer_id) throw new Error("Jitsu envelope event ownership mismatch");
    for (const field of ["message_id", "event_kind", "delivery_event_id", "observed_at", "ingested_at", "fact_payload"] as const) {
      if (typeof event[field] !== "string" || !event[field]) throw new Error(`Jitsu event is missing ${field}`);
    }
    if (event.source_deleted !== 0 && event.source_deleted !== 1) throw new Error("Invalid Jitsu deletion flag");
  }
}
