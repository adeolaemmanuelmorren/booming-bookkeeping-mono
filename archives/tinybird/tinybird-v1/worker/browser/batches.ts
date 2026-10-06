import type { NormalizedBrowserEvent } from "./normalize.ts";

export const MAX_BROWSER_BATCH_BYTES = 512_000;
const MAX_EVENTS = 200;

/** Split before RPC. An oversized individual observation fails explicitly; none are truncated. */
export function browserBatches(events: NormalizedBrowserEvent[]): NormalizedBrowserEvent[][] {
  const batches: NormalizedBrowserEvent[][] = [];
  let batch: NormalizedBrowserEvent[] = [];
  let bytes = 2;
  for (const event of events) {
    const eventBytes = new TextEncoder().encode(JSON.stringify(event)).byteLength;
    if (eventBytes + 2 > MAX_BROWSER_BATCH_BYTES) throw new Error(`Browser observation exceeds 512000 bytes: ${event.source.source_record_id}`);
    const separator = batch.length ? 1 : 0;
    if (batch.length === MAX_EVENTS || bytes + separator + eventBytes > MAX_BROWSER_BATCH_BYTES) {
      batches.push(batch);
      batch = [];
      bytes = 2;
    }
    bytes += (batch.length ? 1 : 0) + eventBytes;
    batch.push(event);
  }
  if (batch.length) batches.push(batch);
  return batches;
}
