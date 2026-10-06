import assert from 'node:assert/strict';
import { readFile, writeFile } from 'node:fs/promises';
import { tinybirdConfig, query } from './tinybird.mjs';
import { Tinybird } from '../worker/storage/tinybird.ts';
import { IdentityBootstrapStore } from '../worker/identity/bootstrap-store.ts';
import { readIdentityBootstrapSeal } from '../worker/identity/bootstrap-seal.ts';
import { hash } from '../worker/bootstrap/hash.ts';

const mode = process.argv[2] ?? 'status';
if (!['status', 'verify-baseline'].includes(mode)) throw new Error('Unknown identity check');
const root = new URL('../', import.meta.url);
const plan = JSON.parse(await readFile(new URL('restore/identity-plan.json', root), 'utf8'));
const { ADMIN_TOKEN } = JSON.parse(await readFile(new URL('.dev.vars.admin.json', root), 'utf8'));
const host = 'https://boom-tinybird-facts-v1.bill-3e3.workers.dev';
const config = await tinybirdConfig();
const client = new Tinybird({ TINYBIRD_URL: config.host, TINYBIRD_TOKEN: config.token });
const store = new IdentityBootstrapStore(client, plan.tenantId, plan.baselineId);

async function admin(path) {
  const response = await fetch(host + path, {
    method: 'POST', headers: { Authorization: `Bearer ${ADMIN_TOKEN}`, 'Content-Type': 'application/json' },
    body: '{}', signal: AbortSignal.timeout(60_000), redirect: 'manual',
  });
  if (!response.ok) { await response.body?.cancel(); throw new Error(`Identity check failed with HTTP ${response.status}`); }
  return response.json();
}

const sources = await query(`SELECT manifest_kind, manifest_key AS source,
  max(sequence) AS latest_sequence,
  argMax(JSONExtractUInt(payload_json,'physical'),sequence) AS physical,
  argMax(JSONExtractUInt(payload_json,'candidates'),sequence) AS candidates
  FROM v1_identity_bootstrap_manifests WHERE ${store.scope()}
  AND manifest_kind IN ('source','source-cursor')
  GROUP BY manifest_kind,manifest_key ORDER BY manifest_kind,manifest_key`);
const saved = await store.get('seal', 'complete');
const result = { checkedAt: new Date().toISOString(), sources: sources.data, sealed: Boolean(saved) };

if (mode === 'status') {
  const [health, identity, replay] = await Promise.all([
    fetch(host + '/health').then(response => response.json()),
    admin('/admin/identity/status'), admin('/admin/identity/browser-replay/status'),
  ]);
  Object.assign(result, { health, identity, replay });
} else {
  assert.ok(saved, 'The complete identity seal is not available');
  const seal = saved.payload;
  assert.equal(await hash(seal.inputs), plan.sourceSeal);
  assert.equal(seal.nativeImportProofSha256, plan.nativeImportProofSha256);
  const sealHash = await hash(seal);
  const verified = await readIdentityBootstrapSeal({ tenantId: plan.tenantId,
    baselineId: plan.baselineId, expectedSealHash: sealHash,
    reader: { read: async () => ({ payload: seal, payloadHash: sealHash }) },
  });
  const expected = [...plan.inputs.browser, plan.inputs.live,
    ...plan.inputs.fivetran.tables.filter(input => /_(charge|contact_tag)$/.test(input.table))];
  const complete = sources.data.filter(row => row.manifest_kind === 'source');
  assert.equal(complete.length, expected.length, 'Every identity source must have a complete receipt');
  for (const input of expected) {
    assert.equal(Number(complete.find(row => row.source === input.table)?.physical), input.expectedPhysicalRows);
  }
  const selected = await store.get('stage', 'selected');
  const publication = await store.get('publication', 'all');
  const scopes = await store.get('scope-proofs', 'complete');
  assert.equal(selected?.payload.facts, seal.counts.facts);
  assert.equal(publication?.payload.facts, seal.counts.facts);
  assert.equal(publication?.payload.components, seal.counts.components);
  assert.equal(publication?.payload.records, seal.counts.records);
  assert.equal(publication?.payload.chain, seal.publicationHash);
  assert.equal(scopes?.payload.facts, seal.counts.scopeFacts);
  assert.equal(scopes?.payload.scopes, seal.counts.scopeLookups);
  let verifiedLookups = 0;
  let largestLookupRows = 0;
  // Sorted bucket ranges keep this independent coverage check within bounded memory.
  for (let lower = 0; lower < 65_536; lower += 4096) {
    const first = String(lower).padStart(5, '0');
    const after = String(lower + 4096).padStart(5, '0');
    const capacity = await query(`SELECT uniqExact(manifest_key) AS lookups,
      max(JSONExtractUInt(payload_json,'count')) AS largest_lookup_rows
      FROM v1_identity_bootstrap_manifests WHERE ${store.scope()} AND manifest_kind='lookup' AND sequence=0
      AND manifest_key >= '${first}:' AND manifest_key < '${after}:'`);
    verifiedLookups += Number(capacity.data[0].lookups);
    largestLookupRows = Math.max(largestLookupRows, Number(capacity.data[0].largest_lookup_rows));
  }
  assert.equal(verifiedLookups, seal.counts.recordLookups + seal.counts.scopeLookups, 'Lookup coverage is incomplete');
  assert.ok(largestLookupRows <= 20_000, 'A sealed lookup exceeds the live identity reader limit');
  Object.assign(result, { verified: true, receipt: verified.receipt, counts: seal.counts, largestLookupRows });
}

await writeFile(new URL(`evidence/cutover/identity-${mode}.json`, root), JSON.stringify(result, null, 2));
console.log(JSON.stringify(result));
