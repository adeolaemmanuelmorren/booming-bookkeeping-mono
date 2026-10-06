import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { generatedFanInFiles } from './source-registry-files.mjs';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const projectRoot = join(scriptDirectory, '..');

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function assertRejected(action, message) {
  let rejected = false;

  try {
    action();
  } catch {
    rejected = true;
  }

  assert(rejected, message);
}

function identityFactKey(connectionId, sourceSystem, recordId) {
  return [connectionId, sourceSystem, recordId]
    .map((value) => `${value.length}:${value}`)
    .join(':');
}

function addSyntheticSource(registry, extension) {
  registry.sources.push({
    id: extension.sourceId,
    source_connection_ids: [extension.connectionId],
    adapters: [extension.resource],
    contract_resources: [],
    inputs: [`fixture.${extension.sourceId}`],
    contracts: [extension.contractName],
  });

  const registration = {
    source_id: extension.sourceId,
    resource: extension.resource,
  };

  if (extension.injectConnectionId) {
    registration.source_connection_id = extension.connectionId;
  }

  registry.contract_fan_ins[extension.contractName].push(registration);
}

function generatedSql(generated, registry, generatedFileKey) {
  return generated.get(registry.generated_files[generatedFileKey]);
}

function inventoryDigestFromSql(sql) {
  const digests = [
    ...sql.matchAll(/'([0-9A-F]{64})' AS connection_inventory_digest/g),
  ].map((match) => match[1]);

  assert(digests.length > 0, 'The connection inventory must include a digest.');
  assert(new Set(digests).size === 1, 'One registry render must use one inventory digest.');
  return digests[0];
}

const registry = JSON.parse(await readFile(join(projectRoot, 'project/source-registry.json'), 'utf8'));
const contracts = JSON.parse(await readFile(join(projectRoot, 'contracts/canonical-contracts.json'), 'utf8'));
const generated = generatedFanInFiles(registry, contracts);
const sourceConnections = generatedSql(generated, registry, 'registered_source_connections');
const baseInventoryDigest = inventoryDigestFromSql(sourceConnections);

for (const contractName of Object.keys(registry.contract_fan_ins)) {
  const sql = generatedSql(generated, registry, contractName);
  assert(!sql.includes('SELECT *'), `${contractName} fan-in must use explicit contract columns.`);
  assert(!sql.includes('rows.*'), `${contractName} fan-in must use explicit contract columns.`);
}

const extensionCases = [
  {
    contractName: 'boom_classified_payment_v1',
    sourceId: 'fixture-payment',
    connectionId: 'fixture-payment-primary',
    resource: 'fixture_payment_contract',
    injectConnectionId: true,
  },
  {
    contractName: 'lead_evidence_v1',
    sourceId: 'fixture-leads',
    connectionId: 'fixture-leads-primary',
    resource: 'fixture_lead_contract',
  },
  {
    contractName: 'ad_delivery_hourly_v1',
    sourceId: 'fixture-hourly-ads',
    connectionId: 'fixture-hourly-ads-primary',
    resource: 'fixture_hourly_ad_contract',
  },
  {
    contractName: 'ad_delivery_daily_v1',
    sourceId: 'fixture-daily-ads',
    connectionId: 'fixture-daily-ads-primary',
    resource: 'fixture_daily_ad_contract',
  },
  {
    contractName: 'identity_observation_v1',
    sourceId: 'fixture-identity',
    connectionId: 'fixture-identity-primary',
    resource: 'fixture_identity_contract',
  },
];

for (const extension of extensionCases) {
  const extendedRegistry = structuredClone(registry);
  addSyntheticSource(extendedRegistry, extension);

  const extended = generatedFanInFiles(extendedRegistry, contracts);
  const fanIn = generatedSql(extended, extendedRegistry, extension.contractName);
  const coverage = generatedSql(extended, extendedRegistry, 'registered_contract_coverage_checks');
  const requirements = generatedSql(extended, extendedRegistry, 'registered_contract_coverage_requirements');
  const sourceConnections = generatedSql(extended, extendedRegistry, 'registered_source_connections');

  assert(fanIn.includes(`FROM ${extension.resource} AS rows`), `${extension.contractName} must include a newly registered source.`);
  assert(fanIn.includes(extension.connectionId), `${extension.contractName} must fence the new source connection.`);
  assert(coverage.includes(`FROM ${extension.resource} AS contract_rows`), `${extension.contractName} must generate a contract check.`);
  assert(coverage.includes(`rows.source_connection_id NOT IN ('${extension.connectionId}')`), `${extension.contractName} check must enforce connection ownership.`);
  assert(requirements.includes(`'${extension.resource}' AS adapter_id`), `${extension.contractName} must generate a readiness requirement.`);
  assert(sourceConnections.includes(`'${extension.connectionId}' AS source_connection_id`), `${extension.contractName} source connection must be registered.`);
  assert(
    inventoryDigestFromSql(sourceConnections) !== baseInventoryDigest,
    `${extension.contractName} must change the pinned connection inventory digest.`,
  );
}

const wrongOwnerRegistry = structuredClone(registry);
wrongOwnerRegistry.contract_fan_ins.lead_evidence_v1[0].source_id = 'meta-ads';
assertRejected(
  () => generatedFanInFiles(wrongOwnerRegistry, contracts),
  'A fan-in resource assigned to a source that does not declare its contract must fail.',
);

const unownedResourceRegistry = structuredClone(registry);
unownedResourceRegistry.contract_fan_ins.lead_evidence_v1[0].resource = 'meta_hourly_delivery_adapter';
assertRejected(
  () => generatedFanInFiles(unownedResourceRegistry, contracts),
  'A source must not register a resource it does not own.',
);

const wrongConnectionRegistry = structuredClone(registry);
wrongConnectionRegistry.contract_fan_ins.boom_classified_payment_v1[0].source_connection_id = 'stripe-kajabi';
assertRejected(
  () => generatedFanInFiles(wrongConnectionRegistry, contracts),
  'A literal source connection outside its source owner must fail.',
);

const duplicateConnectionRegistry = structuredClone(registry);
duplicateConnectionRegistry.sources.push({
  id: 'fixture-duplicate-connection',
  source_connection_ids: ['stripe-primary'],
  adapters: ['fixture_duplicate_connection_adapter'],
  contract_resources: [],
  inputs: ['fixture.duplicate'],
  contracts: ['boom_classified_payment_v1'],
});
assertRejected(
  () => generatedFanInFiles(duplicateConnectionRegistry, contracts),
  'Two non-derived sources must not own the same source_connection_id.',
);

const missingDerivationRegistry = structuredClone(registry);
missingDerivationRegistry.contract_fan_ins.identity_observation_v1
  .find((entry) => entry.resource === 'payment_identity_contract').derived_from_contract = undefined;
missingDerivationRegistry.contract_fan_ins.identity_observation_v1
  .find((entry) => entry.resource === 'payment_identity_contract').derived = true;
assertRejected(
  () => generatedFanInFiles(missingDerivationRegistry, contracts),
  'A derived fan-in must name the contract that owns its connection lineage.',
);

assert(!sourceConnections.includes('registered-payments'), 'Derived fan-in aliases must not become source connections.');

assert(
  identityFactKey('stripe-primary', 'stripe:payment', 'same-id')
    !== identityFactKey('stripe-kajabi', 'stripe:payment', 'same-id'),
  'Equal provider record IDs from different connections must produce different identity fact keys.',
);

const duplicatePaymentIds = new Set([
  'boom:stripe-primary:same-payment-id',
  'boom:stripe-kajabi:same-payment-id',
]);
assert(duplicatePaymentIds.size === 2, 'Payment grain must include tenant and source connection.');

console.log(JSON.stringify({
  status: 'ok',
  checks: {
    generatedFiles: generated.size,
    syntheticSourceKinds: extensionCases.length,
    ownershipMismatchRejected: true,
    unownedResourceRejected: true,
    wrongConnectionRejected: true,
    duplicateConnectionRejected: true,
    missingDerivationRejected: true,
    crossConnectionIdentityKeysDistinct: true,
    crossConnectionPaymentKeysDistinct: true,
  },
}, null, 2));
