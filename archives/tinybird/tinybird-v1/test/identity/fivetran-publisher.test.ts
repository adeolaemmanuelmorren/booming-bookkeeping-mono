import assert from "node:assert/strict";
import test from "node:test";
import { IdentityOnlyReplacementPublisher } from "../../worker/identity/fivetran-publisher.ts";

test("publishes identity facts without publishing source or conversion rows", async () => {
  const batches: unknown[][] = [];
  const publisher = new IdentityOnlyReplacementPublisher({ enqueue: async facts => { batches.push(facts); } });
  await publisher.publishSourceReplacements([{
    source: "stripe", source_account: "main", scope_id: "stripe:main:charge:ch_1",
    replacement_id: "window:ch_1", observed_at: "2026-09-05T23:42:00Z", observation_sequence: 2,
    rows: [{ source: "stripe", source_account: "main", charge_id: "ch_1", email: "A@Example.com", name: "A Person", occurred_at: "2026-09-05T23:00:00Z", is_deleted: false }],
    evidence_inbox_ids: [], source_evidence: { identityFacts: [{
      eventId: "e1", producerId: "source:stripe", observedAt: null, ingestedAt: "2026-09-05T23:42:00Z",
      factKind: "stripe", factKey: "stripe:ch_1", sourceFactVersion: 2, factDeleted: false,
      factPayloadHash: "a".repeat(64), factPayload: "{}", evidenceKeys: [],
    }] },
  }]);
  assert.equal(batches.length, 1);
  assert.equal((batches[0][0] as { factKey: string }).factKey, "stripe:ch_1");
});
