import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const projectRoot = join(scriptDirectory, '..');

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

async function readProjectFile(relativePath) {
  return readFile(join(projectRoot, relativePath), 'utf8');
}

const sourceConnections = await readProjectFile(
  'pipes/adapters/registered_source_connections.pipe',
);
const inventoryDigests = [
  ...sourceConnections.matchAll(/'([0-9A-F]{64})' AS connection_inventory_digest/g),
].map((match) => match[1]);

assert(inventoryDigests.length === 9, 'Every Boom source connection must carry the inventory digest.');
assert(new Set(inventoryDigests).size === 1, 'Registered connections must share one inventory digest.');

const currentCutoffs = await readProjectFile(
  'pipes/domain/current_source_connection_cutoffs.pipe',
);
assert(
  currentCutoffs.includes('cutoffs.source_manifest_version = active.source_manifest_version'),
  'The predecessor cutoff must use the published manifest version.',
);
assert(
  currentCutoffs.includes(
    'previous.previous_active_source_generation_id = current.source_generation_id',
  ),
  'An active generation must keep its originally declared predecessor cutoff.',
);
assert(
  currentCutoffs.includes('uniqExact(tuple(')
    && currentCutoffs.includes('AS previous_revision_count')
    && currentCutoffs.includes('AS selected_revision_count')
    && currentCutoffs.includes('WHERE cutoffs.version = 1'),
  'Cutoff selection must detect amended rows.',
);

const readiness = await readProjectFile(
  'pipes/domain/source_generation_cutoff_readiness.pipe',
);
assert(
  readiness.includes('registered_connection_inventory_digest = expected_connection_inventory_digest'),
  'Cutoff readiness must match the deployed and generation-pinned connection inventories.',
);
assert(
  readiness.includes('AND version = 1')
    && readiness.includes('AND selected_revision_count = 1'),
  'A source generation must use one immutable version-1 cutoff row per connection.',
);
assert(
  readiness.includes('AS cutoff_set_digest')
    && readiness.includes('expected_connection_inventory_digest AS connection_inventory_digest'),
  'Cutoff readiness must publish the inventory and complete cutoff-set digests.',
);
assert(
  readiness.includes('NODE source_generation_cutoff_row_set_audit')
    && readiness.includes('extra_or_mismatched_cutoff_row_count')
    && readiness.includes("ifNull(expected.source_connection_id, '') = ''"),
  'Unregistered or wrong-manifest cutoff rows must make the generation unready.',
);

const identityEvents = await readProjectFile(
  'pipes/identity/source_generation_identity_events.pipe',
);
assert(
  !identityEvents.includes('latest_live_jitsu_fact_versions')
    && identityEvents.includes("AND startsWith(stable.producer_id, 'source_generation:')"),
  'A source-generation retry must not depend on unbounded direct Jitsu state.',
);
assert(
  identityEvents.match(/'__boom_source_connection_id'/g)?.length === 4
    && /JSONExtractString\(\s*stable\.fact_payload,\s*'__boom_source_connection_id'\s*\)/m.test(identityEvents)
    && identityEvents.includes("AND startsWith(stable.producer_id, 'source_generation:')")
    && identityEvents.includes('cutoff.current_cutoff AS source_version')
    && !identityEvents.includes('max(cutoff.current_cutoff) AS removal_cutoff'),
  'Generated-fact removals must use the owner cutoff, while direct Jitsu keeps its native version domain.',
);
assert(
  identityEvents.includes('NODE pre_generation_identity_fact_versions')
    && identityEvents.includes(
      'state.batch_version < generation.identity_stable_batch_version',
    )
    && identityEvents.includes('FROM pre_generation_identity_facts AS stable')
    && !identityEvents.includes('FROM current_identity_facts AS stable'),
  'Removal candidates must read the identity snapshot before the pinned target batch.',
);

const adapterCoverage = await readProjectFile(
  'pipes/adapters/bill_adapter_coverage_checks.pipe',
);
assert(
  adapterCoverage.includes(
    'source_fact_version != toUInt64(toUnixTimestamp64Micro(ingested_at))',
  ),
  'Direct Jitsu revisions must use epoch microseconds so cross-path fact versions are comparable.',
);

const normalizedIdentityInput = await readProjectFile(
  'pipes/identity/normalize_identity_input.pipe',
);
assert(
  normalizedIdentityInput.includes(
    'source_fact_version = toUInt64(toUnixTimestamp64Micro(ingested_at))',
  ),
  'Malformed direct Jitsu fact versions must be rejected before compaction and checkpointing.',
);

const identityCompaction = await readProjectFile(
  'copies/identity_compact_state.pipe',
);
assert(
  identityCompaction.includes('events.producer_id')
    && identityCompaction.includes('tupleElement(facts.current_fact, 7) AS producer_id'),
  'Fact compaction must retain producer provenance for source-owned removal handling.',
);
assert(
  !identityEvents.toLowerCase().includes('provider'),
  'Identity generation must use explicit source-connection lineage, not provider fallback inference.',
);

console.log(JSON.stringify({
  status: 'ok',
  checks: {
    registeredConnectionCount: inventoryDigests.length,
    immutableCutoffs: true,
    cutoffSetPinned: true,
    deterministicJitsuBoundary: true,
    perConnectionTombstones: true,
    pinnedRemovalBaseline: true,
    providerFallbackAbsent: true,
  },
}, null, 2));
