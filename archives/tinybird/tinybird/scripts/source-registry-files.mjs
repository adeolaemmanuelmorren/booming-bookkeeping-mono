import { createHash } from 'node:crypto';

const GENERATED_CONTRACTS = [
  {
    contractName: 'boom_classified_payment_v1',
    nodeName: 'registered_current_payment_rows',
    description: [
      '    GENERATED from the source registry and canonical contracts. Boom-classified',
      '    payment evidence keeps tenant and source-connection lineage.',
    ],
  },
  {
    contractName: 'lead_evidence_v1',
    nodeName: 'registered_current_lead_rows',
    description: [
      '    GENERATED from the source registry and canonical contracts. Typed lead evidence',
      '    preserves tenant and source-connection lineage.',
    ],
  },
  {
    contractName: 'ad_delivery_hourly_v1',
    nodeName: 'registered_current_hourly_ad_delivery_rows',
    description: [
      '    GENERATED from the source registry and canonical contracts. Hourly ad delivery',
      '    keeps provider connection lineage while exposing one reusable shape.',
    ],
  },
  {
    contractName: 'ad_delivery_daily_v1',
    nodeName: 'registered_current_daily_ad_delivery_rows',
    description: [
      '    GENERATED from the source registry and canonical contracts. Daily ad delivery',
      '    preserves provider-native level, reach, and frequency fields.',
    ],
  },
  {
    contractName: 'identity_observation_v1',
    nodeName: 'registered_identity_observation_rows',
    description: [
      '    GENERATED from the source registry and canonical contracts. First-party identity',
      '    observations keep tenant and source-connection lineage.',
    ],
  },
];

const GENERATED_RESOURCE_KEYS = [
  'registered_source_connections',
  'registered_contract_coverage_checks',
  'registered_contract_coverage_requirements',
];

const CONTRACT_VALIDATION_NAME = 'canonical_required_fields_and_grain';

function sqlString(value) {
  return `'${value.replaceAll("'", "''")}'`;
}

function requireValue(condition, message) {
  if (!condition) throw new Error(message);
}

function unique(values) {
  return [...new Set(values)];
}

function canonicalContract(canonicalContracts, contractName) {
  const contract = canonicalContracts.contracts?.[contractName];
  requireValue(contract, `Unknown canonical contract ${contractName}.`);

  const columns = contract.columns ?? [];
  const columnNames = columns.map((column) => column.name);

  for (const column of columns) {
    requireValue(column.name && column.type, `Canonical contract ${contractName} has an incomplete column.`);
  }

  requireValue(columns.length > 0, `Canonical contract ${contractName} has no columns.`);
  requireValue(new Set(columnNames).size === columnNames.length, `Canonical contract ${contractName} has duplicate columns.`);
  requireValue(columnNames.includes('source_generation_id'), `Canonical contract ${contractName} is missing source_generation_id.`);
  requireValue(columnNames.includes('tenant_id'), `Canonical contract ${contractName} is missing tenant_id.`);
  requireValue(columnNames.includes('source_connection_id'), `Canonical contract ${contractName} is missing source_connection_id.`);

  for (const grainColumn of contract.grain ?? []) {
    requireValue(columnNames.includes(grainColumn), `Canonical contract ${contractName} grain references ${grainColumn}.`);
  }

  for (const requiredColumn of contract.required_nonempty ?? []) {
    requireValue(columnNames.includes(requiredColumn), `Canonical contract ${contractName} requires unknown column ${requiredColumn}.`);
  }

  requireValue((contract.grain ?? []).length > 0, `Canonical contract ${contractName} has no grain.`);

  return contract;
}

function buildRegistryContext(registry, canonicalContracts) {
  requireValue(typeof registry.tenant_id === 'string' && registry.tenant_id !== '', 'Source registry is missing tenant_id.');
  requireValue(Array.isArray(registry.sources) && registry.sources.length > 0, 'Source registry has no sources.');

  const sourceById = new Map();
  const connectionOwners = new Map();

  for (const source of registry.sources) {
    requireValue(typeof source.id === 'string' && source.id !== '', 'Source registry contains an empty source ID.');
    requireValue(!sourceById.has(source.id), `Source registry contains duplicate source ID ${source.id}.`);
    requireValue(Array.isArray(source.source_connection_ids) && source.source_connection_ids.length > 0, `Source ${source.id} has no source_connection_ids.`);
    requireValue(Array.isArray(source.adapters) && source.adapters.length > 0, `Source ${source.id} has no concrete adapters.`);
    requireValue(Array.isArray(source.contracts), `Source ${source.id} has no contracts list.`);

    for (const contractName of source.contracts) {
      canonicalContract(canonicalContracts, contractName);
    }

    for (const connectionId of source.source_connection_ids) {
      requireValue(typeof connectionId === 'string' && connectionId !== '', `Source ${source.id} has an empty source_connection_id.`);
      requireValue(!connectionOwners.has(connectionId), `source_connection_id ${connectionId} belongs to more than one source.`);
      connectionOwners.set(connectionId, source.id);
    }

    sourceById.set(source.id, source);
  }

  return { sourceById, connectionOwners };
}

function sortedEntries(registry, contractName) {
  const entries = registry.contract_fan_ins?.[contractName] ?? [];
  requireValue(entries.length > 0, `Contract ${contractName} has no registered resources.`);

  const resources = entries.map((entry) => entry.resource);
  requireValue(resources.every(Boolean), `Contract ${contractName} contains an entry without a resource.`);
  requireValue(new Set(resources).size === resources.length, `Contract ${contractName} contains duplicate resources.`);

  return [...entries].sort((left, right) => {
    const sourceOrder = left.source_id.localeCompare(right.source_id);
    if (sourceOrder !== 0) return sourceOrder;
    return left.resource.localeCompare(right.resource);
  });
}

function directEntryConnections(entry, contractName, context) {
  const source = context.sourceById.get(entry.source_id);
  requireValue(source, `Contract ${contractName} references unknown source ${entry.source_id}.`);
  requireValue(source.contracts.includes(contractName), `Source ${entry.source_id} does not declare contract ${contractName}.`);

  const ownedResources = new Set([
    ...(source.adapters ?? []),
    ...(source.contract_resources ?? []),
  ]);
  requireValue(ownedResources.has(entry.resource), `Source ${entry.source_id} does not own resource ${entry.resource}.`);

  if (!entry.source_connection_id) return source.source_connection_ids;

  requireValue(
    source.source_connection_ids.includes(entry.source_connection_id),
    `Contract ${contractName} resource ${entry.resource} uses connection ${entry.source_connection_id} outside source ${entry.source_id}.`,
  );

  return [entry.source_connection_id];
}

function entryConnections(registry, contractName, entry, context, contractStack = []) {
  if (!entry.derived_from_contract) {
    requireValue(entry.derived !== true, `Derived resource ${entry.resource} must declare derived_from_contract.`);
    return directEntryConnections(entry, contractName, context);
  }

  const parentContract = entry.derived_from_contract;
  requireValue(parentContract !== contractName, `Contract ${contractName} cannot derive from itself.`);
  requireValue(!contractStack.includes(parentContract), `Derived contract cycle includes ${parentContract}.`);

  const parentEntries = sortedEntries(registry, parentContract);
  const nextStack = [...contractStack, contractName];
  const connectionIds = parentEntries.flatMap((parentEntry) => (
    entryConnections(registry, parentContract, parentEntry, context, nextStack)
  ));

  requireValue(connectionIds.length > 0, `Derived resource ${entry.resource} has no source connections.`);
  return unique(connectionIds).sort();
}

function requiredGeneratedFileKeys() {
  return [
    ...GENERATED_CONTRACTS.map(({ contractName }) => contractName),
    ...GENERATED_RESOURCE_KEYS,
  ];
}

export function validateRegistryContracts(registry, canonicalContracts) {
  const context = buildRegistryContext(registry, canonicalContracts);
  const generatedContractNames = new Set(GENERATED_CONTRACTS.map(({ contractName }) => contractName));

  for (const contractName of Object.keys(registry.contract_fan_ins ?? {})) {
    requireValue(generatedContractNames.has(contractName), `Contract ${contractName} has no fan-in generator.`);
  }

  for (const generatedContract of GENERATED_CONTRACTS) {
    canonicalContract(canonicalContracts, generatedContract.contractName);

    for (const entry of sortedEntries(registry, generatedContract.contractName)) {
      requireValue(typeof entry.source_id === 'string' && entry.source_id !== '', `Contract ${generatedContract.contractName} has an entry without source_id.`);
      entryConnections(registry, generatedContract.contractName, entry, context);
    }
  }

  const generatedPaths = [];
  for (const generatedFileKey of requiredGeneratedFileKeys()) {
    const path = registry.generated_files?.[generatedFileKey];
    requireValue(typeof path === 'string' && path !== '', `Generated resource ${generatedFileKey} has no file path.`);
    generatedPaths.push(path);
  }

  requireValue(new Set(generatedPaths).size === generatedPaths.length, 'Generated resource file paths must be unique.');
  return context;
}

function renderColumn(column, entry, isLast) {
  const suffix = isLast ? '' : ',';

  if (column.name === 'source_connection_id' && entry.source_connection_id) {
    return `        CAST(${sqlString(entry.source_connection_id)} AS ${column.type}) AS source_connection_id${suffix}`;
  }

  return `        CAST(rows.${column.name} AS ${column.type}) AS ${column.name}${suffix}`;
}

function renderConnectionFence(entry, connectionIds) {
  if (entry.source_connection_id) return [];

  const allowedConnections = connectionIds.map(sqlString).join(', ');
  return [`      AND rows.source_connection_id IN (${allowedConnections})`];
}

function renderFanInQuery(entry, contract, tenantId, connectionIds) {
  const projection = contract.columns.map((column, index) => (
    renderColumn(column, entry, index === contract.columns.length - 1)
  ));

  return [
    '    SELECT',
    ...projection,
    `    FROM ${entry.resource} AS rows`,
    `    WHERE rows.tenant_id = ${sqlString(tenantId)}`,
    ...renderConnectionFence(entry, connectionIds),
  ].join('\n');
}

function renderFanIn(registry, canonicalContracts, generatedContract, context) {
  const contract = canonicalContract(canonicalContracts, generatedContract.contractName);
  const entries = sortedEntries(registry, generatedContract.contractName);
  const queries = entries.map((entry) => {
    const connectionIds = entryConnections(registry, generatedContract.contractName, entry, context);
    return renderFanInQuery(entry, contract, registry.tenant_id, connectionIds);
  });

  return [
    'DESCRIPTION >',
    ...generatedContract.description,
    '',
    `NODE ${generatedContract.nodeName}`,
    'SQL >',
    queries.join('\n    UNION ALL\n'),
    '',
  ].join('\n');
}

function sortedSourceConnections(registry) {
  return registry.sources
    .flatMap((source) => source.source_connection_ids.map((connectionId) => ({
      sourceId: source.id,
      connectionId,
    })))
    .sort((left, right) => {
      const sourceOrder = left.sourceId.localeCompare(right.sourceId);
      if (sourceOrder !== 0) return sourceOrder;
      return left.connectionId.localeCompare(right.connectionId);
    });
}

function connectionInventoryDigest(registry) {
  const inventory = sortedSourceConnections(registry).map(({ sourceId, connectionId }) => [
    sourceId,
    connectionId,
  ]);

  return createHash('sha256')
    .update(JSON.stringify(inventory), 'utf8')
    .digest('hex')
    .toUpperCase();
}

function renderSourceConnections(registry) {
  const inventoryDigest = connectionInventoryDigest(registry);
  const rows = sortedSourceConnections(registry).map(({ sourceId, connectionId }) => [
    '    SELECT',
    `        ${sqlString(registry.tenant_id)} AS tenant_id,`,
    `        ${sqlString(sourceId)} AS source_id,`,
    `        ${sqlString(connectionId)} AS source_connection_id,`,
    `        ${sqlString(inventoryDigest)} AS connection_inventory_digest`,
  ].join('\n'));

  return [
    'DESCRIPTION >',
    '    GENERATED non-derived source connections. Generation cutoffs and source-owned',
    '    validation gates use this registry projection instead of provider-specific lists.',
    '',
    'NODE registered_source_connection_rows',
    'SQL >',
    rows.join('\n    UNION ALL\n'),
    '',
  ].join('\n');
}

function contractColumnByName(contract) {
  return new Map(contract.columns.map((column) => [column.name, column]));
}

function validationInputColumns(contract) {
  return unique([
    'source_generation_id',
    'tenant_id',
    'source_connection_id',
    ...(contract.required_nonempty ?? []),
    ...(contract.grain ?? []),
  ]);
}

function renderValidationInputColumn(column, entry) {
  if (column.name === 'source_connection_id' && entry.source_connection_id) {
    return `            CAST(${sqlString(entry.source_connection_id)} AS ${column.type}) AS source_connection_id,`;
  }

  return `            CAST(contract_rows.${column.name} AS ${column.type}) AS ${column.name},`;
}

function renderRequiredFieldChecks(contract) {
  const requiredColumns = unique([
    'source_generation_id',
    ...(contract.required_nonempty ?? []),
  ]);

  return requiredColumns.map((columnName) => `empty(ifNull(toString(rows.${columnName}), ''))`);
}

function renderContractCoverageQuery(contractName, entry, contract, connectionIds) {
  const columnsByName = contractColumnByName(contract);
  const inputColumns = validationInputColumns(contract).map((columnName) => columnsByName.get(columnName));
  const projection = inputColumns.map((column) => renderValidationInputColumn(column, entry));
  const allowedConnections = connectionIds.map(sqlString).join(', ');
  const invalidChecks = [
    ...renderRequiredFieldChecks(contract),
    'rows.tenant_id != generation.tenant_id',
    'rows.source_generation_id != generation.source_generation_id',
    `rows.source_connection_id NOT IN (${allowedConnections})`,
  ];
  const invalidCheckLines = invalidChecks.map((check, index) => (
    `                    ${index === 0 ? '' : 'OR '}${check}`
  ));
  const grain = contract.grain.map((columnName) => `rows.${columnName}`).join(', ');

  projection.push('            toUInt8(1) AS contract_row_present');

  return [
    '    SELECT',
    '        generation.tenant_id,',
    '        generation.source_generation_id,',
    `        ${sqlString(entry.resource)} AS adapter_id,`,
    `        ${sqlString(contractName)} AS contract_name,`,
    `        ${sqlString(CONTRACT_VALIDATION_NAME)} AS validation_name,`,
    '        toUInt64(',
    '            countIf(',
    '                rows.contract_row_present = 1',
    '                AND (',
    ...invalidCheckLines,
    '                )',
    '            )',
    '            + countIf(rows.contract_row_present = 1)',
    `            - uniqExactIf(tuple(${grain}), rows.contract_row_present = 1)`,
    '        ) AS invalid_row_count',
    '    FROM current_source_generation AS generation',
    '    LEFT JOIN (',
    '        SELECT',
    ...projection,
    `        FROM ${entry.resource} AS contract_rows`,
    '    ) AS rows ON 1 = 1',
    '    GROUP BY generation.tenant_id, generation.source_generation_id',
  ].join('\n');
}

function registeredContractEntries(registry, canonicalContracts, context) {
  const registrations = [];

  for (const generatedContract of GENERATED_CONTRACTS) {
    const contractName = generatedContract.contractName;
    const contract = canonicalContract(canonicalContracts, contractName);

    for (const entry of sortedEntries(registry, contractName)) {
      registrations.push({
        contractName,
        contract,
        entry,
        connectionIds: entryConnections(registry, contractName, entry, context),
      });
    }
  }

  return registrations.sort((left, right) => {
    const contractOrder = left.contractName.localeCompare(right.contractName);
    if (contractOrder !== 0) return contractOrder;
    return left.entry.resource.localeCompare(right.entry.resource);
  });
}

function renderContractCoverageChecks(registry, canonicalContracts, context) {
  const queries = registeredContractEntries(registry, canonicalContracts, context).map((registration) => (
    renderContractCoverageQuery(
      registration.contractName,
      registration.entry,
      registration.contract,
      registration.connectionIds,
    )
  ));

  return [
    'DESCRIPTION >',
    '    GENERATED contract checks for every registered fan-in resource. Required fields,',
    '    declared grain, source generation, tenant, and allowed connections fail closed.',
    '',
    'NODE registered_contract_validation_rows',
    'SQL >',
    queries.join('\n    UNION ALL\n'),
    '',
  ].join('\n');
}

function renderContractCoverageRequirements(registry, canonicalContracts, context) {
  const rows = registeredContractEntries(registry, canonicalContracts, context).map(({ contractName, entry }) => [
    '    SELECT',
    `        ${sqlString(entry.resource)} AS adapter_id,`,
    `        ${sqlString(contractName)} AS contract_name,`,
    `        ${sqlString(CONTRACT_VALIDATION_NAME)} AS validation_name`,
  ].join('\n'));

  return [
    'DESCRIPTION >',
    '    GENERATED readiness requirements for every source-registry fan-in entry.',
    '    Registering a source therefore adds a publication gate in the same change.',
    '',
    'NODE required_registered_contract_validations',
    'SQL >',
    rows.join('\n    UNION ALL\n'),
    '',
  ].join('\n');
}

export function generatedFanInFiles(registry, canonicalContracts) {
  const context = validateRegistryContracts(registry, canonicalContracts);
  const files = new Map();

  for (const generatedContract of GENERATED_CONTRACTS) {
    const path = registry.generated_files[generatedContract.contractName];
    files.set(path, renderFanIn(registry, canonicalContracts, generatedContract, context));
  }

  files.set(
    registry.generated_files.registered_source_connections,
    renderSourceConnections(registry),
  );
  files.set(
    registry.generated_files.registered_contract_coverage_checks,
    renderContractCoverageChecks(registry, canonicalContracts, context),
  );
  files.set(
    registry.generated_files.registered_contract_coverage_requirements,
    renderContractCoverageRequirements(registry, canonicalContracts, context),
  );

  return files;
}
