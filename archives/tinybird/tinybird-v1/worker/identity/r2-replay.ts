import { normalizeJitsu } from "../browser/normalize.ts";
import type { BrowserQueueEnvelope } from "../browser/ingress.ts";
import type { PendingIdentityFact } from "./engine.ts";

export interface EnvelopeCursor { objectKey: string; eventIndex: number }
export interface EnvelopeObject { key: string; text(): Promise<string> }
export interface EnvelopeSource {
  list(cursor: string | null, limit: number): Promise<{ objects: EnvelopeObject[]; nextCursor: string | null }>;
}
export interface EnvelopeProgress {
  load(objectKey: string): Promise<number | null>;
  save(cursor: EnvelopeCursor): Promise<void>;
}
export interface IdentityReceiver { enqueue(facts: PendingIdentityFact[]): Promise<unknown> }

/** Replays retained hash-keyed envelopes in key/event order and checkpoints each accepted event. */
export async function replayIdentityEnvelopeSlice(input: {
  tenantId: string;
  source: EnvelopeSource;
  progress: EnvelopeProgress;
  receiver: IdentityReceiver;
  maxEvents?: number;
}): Promise<{ processed: number; complete: boolean; cursor: EnvelopeCursor | null }> {
  const limit = input.maxEvents ?? 200;
  if (!Number.isSafeInteger(limit) || limit < 1) throw new Error("Replay event limit is invalid");
  let cursor: EnvelopeCursor | null = null;
  let processed = 0;
  let listingCursor: string | null = null;
  for (;;) {
    const page = await input.source.list(listingCursor, 100);
    for (const object of page.objects.sort((left, right) => left.key.localeCompare(right.key))) {
    const envelope = parseEnvelope(await object.text(), input.tenantId);
    const savedIndex = await input.progress.load(object.key);
    const start = savedIndex === null ? 0 : savedIndex + 1;
    for (let index = start; index < envelope.events.length; index++) {
      if (processed >= limit) return { processed, complete: false, cursor };
      const identity = (await normalizeJitsu(envelope.events[index])).identity;
      if (identity) await input.receiver.enqueue([identity]);
      cursor = { objectKey: object.key, eventIndex: index };
      await input.progress.save(cursor);
      processed++;
    }
    }
    if (!page.nextCursor) return { processed, complete: true, cursor };
    if (page.nextCursor === listingCursor) throw new Error("R2 replay listing cursor did not advance");
    listingCursor = page.nextCursor;
  }
}

function parseEnvelope(text: string, tenantId: string): BrowserQueueEnvelope {
  let value: BrowserQueueEnvelope;
  try { value = JSON.parse(text) as BrowserQueueEnvelope; }
  catch { throw new Error("Buffered identity envelope is invalid JSON"); }
  if (value.schema_version !== "jitsu_events_api_v1" || !Array.isArray(value.events) || !value.events.length) throw new Error("Buffered identity envelope is invalid");
  if (value.events.some(event => event.tenant_id !== tenantId || event.producer_id !== value.producer_id)) throw new Error("Buffered identity envelope scope mismatch");
  return value;
}
