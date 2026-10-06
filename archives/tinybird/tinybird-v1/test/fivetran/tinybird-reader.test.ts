import assert from "node:assert/strict";
import test from "node:test";

import { PIPELINE_TABLES, type JsonRecord } from "../../worker/fivetran/contracts.ts";
import {
  buildBootstrapScopeSql,
  buildChangedScopeSql,
  TinybirdFivetranFactReader,
  type TinybirdQueryClient,
} from "../../worker/fivetran/tinybird-reader.ts";

const SNAPSHOT = "2026-09-05T22:28:00.000000Z";
const ALL_SOURCES = [...new Set(Object.values(PIPELINE_TABLES).flat())].sort();

test("reader accepts only a complete nine-source transport receipt", async () => {
  const client = new QueueClient([{
    complete_through_exclusive: "2026-09-05 22:31:00.000",
    source_count: 9,
    manifest_json: JSON.stringify(
      ALL_SOURCES.map((source_name) => ({ source_name })),
    ),
  }]);
  const reader = new TinybirdFivetranFactReader(client, "boom", SNAPSHOT);

  const barrier = await reader.verifiedObservationThrough(
    PIPELINE_TABLES.stripe_main,
  );
  assert.equal(barrier, "2026-09-05T22:31:00.000000Z");
  assert.match(client.sql[0], /FROM v1_fivetran_source_receipts/);
  assert.doesNotMatch(client.sql[0], /max\(_fivetran_synced\)/i);

  const incomplete = new QueueClient([{
    complete_through_exclusive: "2026-09-05 22:31:00.000",
    source_count: 9,
    manifest_json: JSON.stringify(
      ALL_SOURCES.slice(1).map((source_name) => ({ source_name })),
    ),
  }]);
  await assert.rejects(
    new TinybirdFivetranFactReader(incomplete, "boom", SNAPSHOT)
      .verifiedObservationThrough(PIPELINE_TABLES.activecampaign),
    /wrong manifest size/,
  );
});

test("reader reports freshness only from a complete 3×6 reconciliation receipt", async () => {
  const sources = PIPELINE_TABLES.activecampaign;
  const manifest = sources.flatMap((source_name, sourceIndex) =>
    Array.from({ length: 6 }, (_, shard) => ({
      source_name,
      shard,
      observed_at: `2026-09-05 2${sourceIndex}:0${shard}:00.000`,
    }))
  );
  const client = new QueueClient([{
    complete_through_exclusive: "2026-09-05 23:28:00.000",
    source_count: 3,
    shard_count: 18,
    manifest_json: JSON.stringify(manifest),
  }]);
  const reader = new TinybirdFivetranFactReader(client, "boom", SNAPSHOT);
  const status = await reader.activeCampaignReconciliationStatus();

  assert.equal(status?.cycleCompletedAt, "2026-09-05T23:28:00.000000Z");
  assert.equal(status?.oldestShardObservedAt, "2026-09-05T20:00:00.000000Z");
  assert.equal(status?.newestShardObservedAt, "2026-09-05T22:05:00.000000Z");
});

test("dirty-scope SQL expands dependent customer, payment, and tag updates", () => {
  const customer = buildChangedScopeSql({
    pipeline: "stripe_main",
    table: "raw_stripe_customer",
    snapshotAt: SNAPSHOT,
    afterExclusive: SNAPSHOT,
    throughInclusive: "2026-09-05T22:29:00.000000Z",
    afterCursor: "",
    limit: 100,
  });
  assert.match(customer, /changed_ids/);
  assert.match(customer, /v1_snapshot_stripe_charge/);
  assert.match(customer, /v1_fivetran_stripe_charge/);
  assert.match(customer, /customer_id/);
  assert.match(customer, /tuple\(_v1_observed_at, _fivetran_synced\)/);
  assert.match(customer, /ifNull\(_v1_deleted, 0\)/);
  assert.match(customer, /charge\.is_deleted = 0/);
  assert.doesNotMatch(customer, /tuple\(_fivetran_synced, _v1_observed_at\)/);

  const paymentIntent = buildChangedScopeSql({
    pipeline: "stripe_kajabi",
    table: "raw_stripe_kajabi_payment_intent",
    snapshotAt: SNAPSHOT,
    afterExclusive: SNAPSHOT,
    throughInclusive: "2026-09-05T22:29:00.000000Z",
    afterCursor: "",
    limit: 100,
  });
  assert.match(paymentIntent, /payment_intent_id/);
  assert.match(paymentIntent, /stripe:kajabi:charge:/);

  const tag = buildChangedScopeSql({
    pipeline: "activecampaign",
    table: "raw_activecampaign_tags",
    snapshotAt: SNAPSHOT,
    afterExclusive: SNAPSHOT,
    throughInclusive: "2026-09-05T22:29:00.000000Z",
    afterCursor: "activecampaign:contact:10",
    limit: 100,
  });
  assert.match(tag, /current_assignments/);
  assert.match(tag, /v1_snapshot_activecampaign_contact_tag/);
  assert.match(tag, /_fivetran_deleted/);
  assert.match(tag, /_v1_deleted/);
  assert.match(tag, /tuple\(_v1_observed_at, _fivetran_synced\)/);
  assert.doesNotMatch(tag, /tuple\(_fivetran_synced, _v1_observed_at\)/);
  assert.match(tag, /_v1_observed_at/);
  assert.match(tag, /_v1_observed_at > /);
  assert.match(tag, /_v1_observed_at <= /);
});

test("bootstrap SQL enumerates qualifying facts instead of every contact", () => {
  const activeCampaign = buildBootstrapScopeSql({
    pipeline: "activecampaign",
    afterCursor: "",
    limit: 5_001,
  });
  assert.match(activeCampaign, /Registered for Webinar/);
  assert.match(activeCampaign, /Registered for Challenge/);
  assert.match(activeCampaign, /INNER JOIN v1_snapshot_activecampaign_tags/);
  assert.match(activeCampaign, /GROUP BY scope_id/);

  const stripe = buildBootstrapScopeSql({
    pipeline: "stripe_main",
    afterCursor: "",
    limit: 5_001,
  });
  assert.match(stripe, /paid = 1/);
  assert.match(stripe, /status = 'succeeded'/);
});

test("Stripe batch hydration uses raw snapshot rows and keeps source evidence keys", async () => {
  const client = new RoutingClient();
  const reader = new TinybirdFivetranFactReader(client, "boom", SNAPSHOT);
  const scopes = await reader.readStripeScopes({
    account: "main",
    chargeIds: ["ch_2", "ch_1"],
    throughInclusive: SNAPSHOT,
    snapshotOnly: true,
  });

  assert.deepEqual(scopes.map((scope) => scope.chargeId), ["ch_1", "ch_2"]);
  assert.equal(client.sql.length, 3);
  assert.equal(scopes[0].customerVersions[0].id, "cus_1");
  assert.equal(scopes[1].customerVersions.length, 0);
  assert.equal(scopes[0].paymentIntentVersions[0].id, "pi_1");
  assert.ok(scopes[0].evidenceRecordIds.every((id) => id.includes(":")));
  assert.ok(client.sql.every((sql) => !sql.includes("v1_fivetran_")));
});

class QueueClient implements TinybirdQueryClient {
  readonly sql: string[] = [];
  private readonly response: JsonRecord[];

  constructor(response: JsonRecord[]) {
    this.response = response;
  }

  async query<Row extends JsonRecord>(sql: string): Promise<Row[]> {
    this.sql.push(sql);
    return structuredClone(this.response) as Row[];
  }
}

class RoutingClient implements TinybirdQueryClient {
  readonly sql: string[] = [];

  async query<Row extends JsonRecord>(sql: string): Promise<Row[]> {
    this.sql.push(sql);

    if (sql.includes("v1_snapshot_stripe_charge")) {
      return [charge("ch_1", "cus_1", "pi_1"), charge("ch_2", null, null)] as Row[];
    }
    if (sql.includes("v1_snapshot_stripe_customer")) {
      return [{
        id: "cus_1",
        email: "person@example.com",
        _fivetran_synced: "2026-09-05T22:27:00.000000Z",
      }] as Row[];
    }
    if (sql.includes("v1_snapshot_stripe_payment_intent")) {
      return [{
        id: "pi_1",
        receipt_email: "receipt@example.com",
        _fivetran_synced: "2026-09-05T22:27:00.000000Z",
      }] as Row[];
    }

    throw new Error("unexpected Tinybird query");
  }
}

function charge(id: string, customerId: string | null, paymentIntentId: string | null) {
  return {
    id,
    customer_id: customerId,
    payment_intent_id: paymentIntentId,
    _fivetran_synced: "2026-09-05T22:27:00.000000Z",
  };
}
