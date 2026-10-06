import { conversionIdentityFacts } from '../../worker/conversions/identity.ts';
import type { SourceReplacement } from '../../worker/conversions/publication.ts';
import type { PendingIdentityFact } from '../../worker/identity/engine.ts';
import type { BootstrapConfig } from '../../worker/bootstrap/executor.ts';
import { CONVERSION_FACTS_TABLE, CONVERSION_MANIFESTS_TABLE } from '../../worker/bootstrap/conversion-identity.ts';
import { canonicalJson, compareStrings } from '../../worker/sessions/session-engine.ts';
import { hash, hashText } from '../../worker/bootstrap/hash.ts';
import type { JsonRecord } from '../../worker/storage/tinybird.ts';
import { TIME } from './bootstrap-fixture.ts';

/** Synthetic conversions use the same identity-fact adapter as source publication. */
export async function conversionFixtureFacts(): Promise<PendingIdentityFact[]> {
  const replacement = (source: SourceReplacement['source'], account: SourceReplacement['source_account'], rows: JsonRecord[]): SourceReplacement => ({
    source, source_account: account, rows, scope_id: 'synthetic', replacement_id: `synthetic:${source}:${account}`,
    observed_at: TIME, observation_sequence: 10, evidence_inbox_ids: [], source_evidence: {},
  });
  const groups = [
    replacement('stripe', 'main', [{ charge_id: 'synthetic-bridge', occurred_at: null, email: 'person@example.invalid', phone: '+12024440199', name: 'Ada Example' }]),
    replacement('activecampaign', 'default', [{ form_submission_id: 'synthetic-registration', occurred_at: TIME, email: 'third@example.invalid', first_name: 'Third' }]),
    replacement('stripe', 'kajabi', [{ charge_id: 'synthetic-deleted', occurred_at: TIME, email: 'deleted@example.invalid', is_deleted: true }]),
  ];
  return (await Promise.all(groups.map(conversionIdentityFacts))).flat()
    .sort((a, b) => compareStrings(a.factKind, b.factKind) || compareStrings(a.factKey, b.factKey));
}

export async function attachConversionSnapshot(
  config: BootstrapConfig, tables: Record<string, JsonRecord[]>, facts?: PendingIdentityFact[],
): Promise<PendingIdentityFact[]> {
  const selected = facts ?? await conversionFixtureFacts();
  const snapshotId = `${config.baselineId}:conversions`;
  const rows = await Promise.all(selected.map(async fact => {
    const canonical = canonicalJson(fact);
    return { tenant_id: config.tenantId, snapshot_id: snapshotId, fact_kind: fact.factKind, fact_key: fact.factKey,
      event_id: fact.eventId, producer_id: fact.producerId, observed_at: fact.observedAt, ingested_at: fact.ingestedAt,
      source_priority: fact.sourcePriority!, source_fact_version: fact.sourceFactVersion, fact_deleted: fact.factDeleted,
      fact_payload_hash: fact.factPayloadHash, fact_payload: fact.factPayload, evidence_keys: fact.evidenceKeys,
      canonical_fact_json: canonical, row_hash: await hashText(canonical) };
  }));
  config.conversionIdentity = { snapshotId, snapshotAt: TIME, expectedDistinctFactCount: selected.length,
    canonicalHash: await hashText(rows.map(row => row.canonical_fact_json).join('\n')) };
  config.sourceSeal = await hash({ browserSourceSeal: config.sourceSeal, conversionIdentity: config.conversionIdentity });
  tables[CONVERSION_FACTS_TABLE] = rows;
  tables[CONVERSION_MANIFESTS_TABLE] = [{ tenant_id: config.tenantId, snapshot_id: snapshotId, snapshot_at: TIME,
    expected_distinct_fact_count: selected.length, canonical_hash: config.conversionIdentity.canonicalHash, sealed_at: TIME }];
  return selected;
}
