import assert from "node:assert/strict";
import test from "node:test";
import { replayIdentityEnvelopeSlice, type EnvelopeCursor } from "../../worker/identity/r2-replay.ts";

function envelope(ids: string[]) {
  return JSON.stringify({ schema_version: "jitsu_events_api_v1", producer_id: "p", events: ids.map((id, index) => ({
    tenant_id: "boom", producer_id: "p", message_id: id, delivery_event_id: `d-${id}`,
    event_kind: "identify", observed_at: "2026-09-05T23:00:00Z", ingested_at: "2026-09-05T23:00:01Z",
    source_fact_version: index + 1, source_deleted: 0, fact_payload: JSON.stringify({ message_id: id, email: `${id}@example.com` }),
  })) });
}

test("checkpoints every event and resumes inside an envelope", async () => {
  const cursors = new Map<string, number>();
  const accepted: string[] = [];
  const source = { list: async () => ({ nextCursor: null, objects: [{ key: "jitsu/envelopes/a.json", text: async () => envelope(["one", "two"]) }] }) };
  const progress = { load: async (key: string) => cursors.get(key) ?? null, save: async (value: EnvelopeCursor) => { cursors.set(value.objectKey, value.eventIndex); } };
  const receiver = { enqueue: async (facts: Array<{ factKey: string }>) => { accepted.push(facts[0].factKey); } };
  const first = await replayIdentityEnvelopeSlice({ tenantId: "boom", source, progress, receiver, maxEvents: 1 });
  assert.equal(first.complete, false);
  const second = await replayIdentityEnvelopeSlice({ tenantId: "boom", source, progress, receiver, maxEvents: 2 });
  assert.equal(second.complete, true);
  assert.deepEqual(accepted, ["segment_identify:one", "segment_identify:two"]);
});

test("rejects an envelope from another tenant before enqueue", async () => {
  const wrong = JSON.parse(envelope(["one"]));
  wrong.events[0].tenant_id = "other";
  await assert.rejects(() => replayIdentityEnvelopeSlice({
    tenantId: "boom",
    source: { list: async () => ({ nextCursor: null, objects: [{ key: "jitsu/envelopes/a.json", text: async () => JSON.stringify(wrong) }] }) },
    progress: { load: async () => null, save: async () => undefined },
    receiver: { enqueue: async () => undefined },
  }), /scope mismatch/);
});

test("a later hash key behind prior work is found by a full rescan", async () => {
  const cursors = new Map<string, number>([["jitsu/envelopes/z.json", 0]]);
  const accepted: string[] = [];
  await replayIdentityEnvelopeSlice({
    tenantId: "boom",
    source: { list: async () => ({ nextCursor: null, objects: [
      { key: "jitsu/envelopes/a.json", text: async () => envelope(["late"] ) },
      { key: "jitsu/envelopes/z.json", text: async () => envelope(["old"] ) },
    ] }) },
    progress: { load: async key => cursors.get(key) ?? null, save: async value => { cursors.set(value.objectKey, value.eventIndex); } },
    receiver: { enqueue: async facts => { accepted.push(facts[0].factKey); } },
  });
  assert.deepEqual(accepted, ["segment_identify:late"]);
});

test("a rejected identity enqueue does not advance the object checkpoint", async () => {
  let saved = false;
  await assert.rejects(() => replayIdentityEnvelopeSlice({
    tenantId: "boom",
    source: { list: async () => ({ nextCursor: null, objects: [{ key: "jitsu/envelopes/a.json", text: async () => envelope(["one"]) }] }) },
    progress: { load: async () => null, save: async () => { saved = true; } },
    receiver: { enqueue: async () => { throw new Error("identity unavailable"); } },
  }), /identity unavailable/);
  assert.equal(saved, false);
});
