import { readFile, readdir } from "node:fs/promises";
import { basename, dirname, extname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { generatedFanInFiles } from "./source-registry-files.mjs";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const projectRoot = resolve(scriptDirectory, "..");
const repositoryRoot = resolve(projectRoot, "..");
const failures = [];

function fail(message) {
  failures.push(message);
}

async function readJson(relativePath) {
  const path = join(projectRoot, relativePath);
  const source = await readFile(path, "utf8");

  try {
    return JSON.parse(source);
  } catch (error) {
    fail(`${relativePath}: invalid JSON (${error.message})`);
    return null;
  }
}

async function walk(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const paths = [];

  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      paths.push(...(await walk(path)));
      continue;
    }

    paths.push(path);
  }

  return paths;
}

function resourceName(path) {
  return basename(path, extname(path));
}

function validateNoIndentedProperties(path, source) {
  const pattern = /^[ \t]+(DESCRIPTION|SCHEMA|ENGINE|NODE|SQL|TYPE|TARGET_DATASOURCE|COPY_MODE|COPY_SCHEDULE)\b/gm;
  const match = pattern.exec(source);
  if (match) {
    fail(`${relative(projectRoot, path)}: property ${match[1]} must not be indented`);
  }
}

function validateDatasource(path, source) {
  const label = relative(projectRoot, path);
  if (!source.includes("SCHEMA >")) {
    fail(`${label}: missing SCHEMA block`);
  }

  if (!/^ENGINE\s+/m.test(source)) {
    fail(`${label}: missing ENGINE`);
  }

  if (/\bDateTime64\b(?!\s*\()/g.test(source)) {
    fail(`${label}: DateTime64 must declare precision`);
  }

  const schemaStart = source.indexOf("SCHEMA >");
  const schemaEnd = source.indexOf("\nENGINE", schemaStart);
  if (schemaStart >= 0 && schemaEnd > schemaStart) {
    const schema = source.slice(schemaStart, schemaEnd);
    const columns = schema.split("\n").filter((line) => line.trimStart().startsWith("`"));
    const nullableColumns = [];

    for (const column of columns) {
      const isDerivedColumn = /\s(?:DEFAULT|ALIAS)\s/.test(column);
      if (!column.includes("`json:") && !isDerivedColumn) {
        fail(`${label}: schema column is missing a JSON path: ${column.trim()}`);
      }

      const name = column.match(/^\s*`([^`]+)`/)?.[1];
      if (name && column.includes("Nullable(")) {
        nullableColumns.push(name);
      }
    }

    const sortingKey = source.match(/^ENGINE_SORTING_KEY\s+"([^"]*)"/m)?.[1] ?? "";
    for (const nullableColumn of nullableColumns) {
      const reference = new RegExp(`\\b${escapeRegularExpression(nullableColumn)}\\b`);
      if (reference.test(sortingKey)) {
        fail(`${label}: sorting key references nullable column ${nullableColumn}`);
      }
    }
  }
}

function escapeRegularExpression(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function validatePipe(path, source, allResourceNames) {
  const label = relative(projectRoot, path);
  const nodes = [...source.matchAll(/^NODE\s+([A-Za-z0-9_]+)/gm)].map((match) => match[1]);
  if (nodes.length === 0) {
    fail(`${label}: missing NODE`);
  }

  const duplicateNodes = nodes.filter((node, index) => nodes.indexOf(node) !== index);
  for (const node of new Set(duplicateNodes)) {
    fail(`${label}: duplicate node ${node}`);
  }

  const pipeName = resourceName(path);
  for (const node of nodes) {
    if (node === pipeName || allResourceNames.has(node)) {
      fail(`${label}: node ${node} collides with a resource name`);
    }
  }

  if (/\b(CREATE|INSERT|UPDATE|DELETE|TRUNCATE|DROP|ALTER)\s+/i.test(source)) {
    fail(`${label}: Pipes must contain SELECT-only SQL`);
  }

  if (source.includes("{{") && !/SQL\s*>\s*\n\s*%/m.test(source)) {
    fail(`${label}: parameterized SQL must start with %`);
  }

  const isEndpoint = path.includes(`${join("", "endpoints")}/`);
  if (isEndpoint && !/^TYPE\s+endpoint\s*$/mi.test(source)) {
    fail(`${label}: endpoint is missing TYPE endpoint`);
  }

  const isCopy = path.includes(`${join("", "copies")}/`);
  if (isCopy) {
    if (!/^TYPE\s+copy\s*$/mi.test(source)) {
      fail(`${label}: Copy is missing TYPE COPY`);
    }
    if (!/^TARGET_DATASOURCE\s+/mi.test(source)) {
      fail(`${label}: Copy is missing TARGET_DATASOURCE`);
    }
    if (!/^COPY_MODE\s+(append|replace)\s*$/mi.test(source)) {
      fail(`${label}: Copy must declare append or replace mode`);
    }
  }
}

const actionDispositions = new Set([
  "ported",
  "collapsed_into_resource",
  "dependency_eliminated_for_bill_output",
]);

const assertionScopes = new Set([
  "bill_release",
  "shared_dependency",
  "outside_bill_output",
]);

const assertionDispositions = new Set([
  "structural_rule_lock_plus_branch_gate",
  "branch_gate_only",
  "unimplemented_release_gate",
  "outside_bill_output",
]);

async function validateCoverageFile(relativePath, label) {
  if (typeof relativePath !== "string" || relativePath === "") {
    fail(`${label}: file path must be a non-empty string`);
    return;
  }

  const path = resolve(projectRoot, relativePath);
  if (relative(projectRoot, path).startsWith("..")) {
    fail(`${label}: file must stay inside the Tinybird project`);
    return;
  }

  try {
    await readFile(path, "utf8");
  } catch {
    fail(`${label}: missing file ${relativePath}`);
  }
}

function validateCoverageResources(label, resources, resourcePaths) {
  if (!Array.isArray(resources) || resources.length === 0) {
    fail(`${label}: tinybird_resources must contain at least one resource`);
    return;
  }

  if (new Set(resources).size !== resources.length) {
    fail(`${label}: tinybird_resources contains duplicates`);
  }

  for (const resource of resources) {
    if (typeof resource !== "string" || resource === "") {
      fail(`${label}: Tinybird resource names must be non-empty strings`);
      continue;
    }

    if (!resourcePaths.has(resource)) {
      fail(`${label}: references missing Tinybird resource ${resource}`);
    }
  }
}

async function validateEvidenceProfiles(coverage) {
  const profiles = coverage.evidence_profiles;
  const parityGates = coverage.parity_gates;

  if (!profiles || typeof profiles !== "object" || Array.isArray(profiles)) {
    fail("model coverage must declare evidence_profiles");
    return;
  }

  if (!parityGates || typeof parityGates !== "object" || Array.isArray(parityGates)) {
    fail("model coverage must declare parity_gates");
    return;
  }

  for (const [gateName, description] of Object.entries(parityGates)) {
    if (typeof description !== "string" || description.trim() === "") {
      fail(`model coverage parity gate ${gateName} needs a description`);
    }
  }

  for (const [profileName, profile] of Object.entries(profiles)) {
    const label = `model coverage evidence profile ${profileName}`;
    if (!profile || typeof profile !== "object" || Array.isArray(profile)) {
      fail(`${label}: profile must be an object`);
      continue;
    }

    if (!Array.isArray(profile.structural_files) || profile.structural_files.length === 0) {
      fail(`${label}: structural_files must contain at least one file`);
    } else {
      for (const path of profile.structural_files) {
        await validateCoverageFile(path, label);
      }
    }

    if (!Array.isArray(profile.parity_gates) || profile.parity_gates.length === 0) {
      fail(`${label}: parity_gates must contain at least one gate`);
      continue;
    }

    for (const gateName of profile.parity_gates) {
      if (!Object.hasOwn(parityGates, gateName)) {
        fail(`${label}: references unknown parity gate ${gateName}`);
      }
    }
  }
}

function validateEvidenceReference(label, profileName, evidenceProfiles) {
  if (typeof profileName !== "string" || profileName === "") {
    fail(`${label}: evidence_profile must be a non-empty string`);
    return;
  }

  if (!Object.hasOwn(evidenceProfiles, profileName)) {
    fail(`${label}: references unknown evidence profile ${profileName}`);
  }
}

async function validateDataformFile(relativePath, label) {
  if (typeof relativePath !== "string" || relativePath === "") {
    fail(`${label}: Dataform path must be a non-empty string`);
    return;
  }

  const path = resolve(repositoryRoot, "dataform", relativePath);
  const dataformRoot = resolve(repositoryRoot, "dataform");
  if (relative(dataformRoot, path).startsWith("..")) {
    fail(`${label}: Dataform path must stay inside dataform/`);
    return;
  }

  try {
    await readFile(path, "utf8");
  } catch {
    fail(`${label}: references missing file dataform/${relativePath}`);
  }
}

async function validateAssertionInventory(assertions) {
  const assertionDirectory = join(repositoryRoot, "dataform", "definitions", "assertions");
  const actualAssertions = (await readdir(assertionDirectory))
    .filter((name) => name.endsWith(".sqlx"))
    .map((name) => `definitions/assertions/${name}`);
  const coveredAssertions = new Set(assertions.map((entry) => entry?.assertion));

  for (const assertion of actualAssertions) {
    if (!coveredAssertions.has(assertion)) {
      fail(`model coverage is missing Dataform assertion ${assertion}`);
    }
  }

  for (const assertion of coveredAssertions) {
    if (!actualAssertions.includes(assertion)) {
      fail(`model coverage includes non-assertion or missing Dataform file ${assertion}`);
    }
  }
}

async function validateCoverage(resourcePaths) {
  const coverage = await readJson("project/model-coverage.json");
  if (!coverage) {
    return;
  }

  if (coverage.schema_version !== 2) {
    fail("model coverage schema_version must be 2");
  }
  if (coverage.expected_action_count !== 60) {
    fail("model coverage expected_action_count must remain 60 for Bill's runtime closure");
  }
  if (coverage.expected_assertion_count !== 18) {
    fail("model coverage expected_assertion_count must remain 18 for the current Dataform assertion inventory");
  }

  await validateEvidenceProfiles(coverage);
  const evidenceProfiles = coverage.evidence_profiles ?? {};
  const actions = Array.isArray(coverage.actions) ? coverage.actions : [];
  const assertions = Array.isArray(coverage.assertions) ? coverage.assertions : [];

  if (!Array.isArray(coverage.actions)) {
    fail("model coverage actions must be an array");
  }
  if (actions.length !== coverage.expected_action_count) {
    fail(`model coverage contains ${actions.length} actions; expected ${coverage.expected_action_count}`);
  }

  const actionPaths = actions.map((entry) => entry?.action);
  if (new Set(actionPaths).size !== actionPaths.length) {
    fail("model coverage contains duplicate Dataform actions");
  }

  for (const entry of actions) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      fail("model coverage actions must contain objects");
      continue;
    }

    const label = `model coverage action ${entry.action ?? "<missing>"}`;
    await validateDataformFile(entry.action, label);
    validateCoverageResources(label, entry.tinybird_resources, resourcePaths);
    validateEvidenceReference(label, entry.evidence_profile, evidenceProfiles);

    if (typeof entry.action === "string" && entry.action.includes("/assertions/")) {
      fail(`${label}: assertions belong in the assertions inventory`);
    }
    if (!actionDispositions.has(entry.disposition)) {
      fail(`${label}: unsupported disposition ${entry.disposition}`);
    }
  }

  if (!Array.isArray(coverage.assertions)) {
    fail("model coverage assertions must be an array");
  }
  if (assertions.length !== coverage.expected_assertion_count) {
    fail(`model coverage contains ${assertions.length} assertions; expected ${coverage.expected_assertion_count}`);
  }

  const assertionPaths = assertions.map((entry) => entry?.assertion);
  if (new Set(assertionPaths).size !== assertionPaths.length) {
    fail("model coverage contains duplicate Dataform assertions");
  }

  await validateAssertionInventory(assertions);
  for (const entry of assertions) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      fail("model coverage assertions must contain objects");
      continue;
    }

    const label = `model coverage assertion ${entry.assertion ?? "<missing>"}`;
    await validateDataformFile(entry.assertion, label);
    validateCoverageResources(label, entry.tinybird_resources, resourcePaths);
    validateEvidenceReference(label, entry.evidence_profile, evidenceProfiles);

    if (!assertionScopes.has(entry.scope)) {
      fail(`${label}: unsupported scope ${entry.scope}`);
    }
    if (!assertionDispositions.has(entry.disposition)) {
      fail(`${label}: unsupported disposition ${entry.disposition}`);
    }
    if (entry.scope === "outside_bill_output" && entry.disposition !== "outside_bill_output") {
      fail(`${label}: outside_bill_output scope must use the outside_bill_output disposition`);
    }
    if (entry.scope !== "outside_bill_output" && entry.disposition === "outside_bill_output") {
      fail(`${label}: outside_bill_output disposition requires the matching scope`);
    }
    if (
      entry.disposition === "unimplemented_release_gate" &&
      !entry.operational_equivalent?.toLowerCase().includes("not implemented")
    ) {
      fail(`${label}: unimplemented release gates must say that they are not implemented`);
    }
    if (typeof entry.operational_equivalent !== "string" || entry.operational_equivalent.trim() === "") {
      fail(`${label}: operational_equivalent must explain the replacement or exclusion`);
    }
  }
}

async function validateEndpointContracts(files) {
  const contracts = await readJson("contracts/endpoint-contracts.json");
  if (!contracts) {
    return;
  }

  const endpointNames = new Set(
    files
      .filter((path) => path.includes(`${join("", "endpoints")}/`) && extname(path) === ".pipe")
      .map(resourceName),
  );

  for (const endpointName of Object.keys(contracts)) {
    if (!endpointNames.has(endpointName)) {
      fail(`missing contracted endpoint endpoints/${endpointName}.pipe`);
    }
  }
}

function validateTestResourceNames(files, pipeResourceNames) {
  const testFiles = files.filter((path) => {
    const isTestFile = path.includes(`${join("", "tests")}/`);
    const extension = extname(path);

    return isTestFile && [".yaml", ".yml"].includes(extension);
  });

  for (const path of testFiles) {
    const testName = resourceName(path);
    if (pipeResourceNames.has(testName)) {
      continue;
    }

    fail(`${relative(projectRoot, path)}: no matching Pipe resource ${testName}.pipe`);
  }
}

async function validateSourceRegistry(resourcePaths) {
  const registry = await readJson("project/source-registry.json");
  const canonicalContracts = await readJson("contracts/canonical-contracts.json");
  if (!registry || !canonicalContracts) {
    return;
  }
  const allResourceNames = new Set(resourcePaths.keys());

  const sourceIds = registry.sources.map((source) => source.id);
  if (new Set(sourceIds).size !== sourceIds.length) {
    fail("source registry contains duplicate source IDs");
  }

  const connectionIds = registry.sources.flatMap((source) => source.source_connection_ids ?? []);
  if (registry.sources.some((source) => !Array.isArray(source.source_connection_ids) || source.source_connection_ids.length === 0)) {
    fail("every source registry entry must declare source_connection_ids");
  }
  if (connectionIds.some((connectionId) => typeof connectionId !== "string" || connectionId === "")) {
    fail("source registry contains an empty source connection ID");
  }
  if (new Set(connectionIds).size !== connectionIds.length) {
    fail("source registry contains duplicate source_connection_id values");
  }

  const registeredResources = [
    ...registry.sources.flatMap((source) => source.adapters ?? []),
    ...registry.sources.flatMap((source) => source.contract_resources ?? []),
    ...(registry.shared_domains ?? []),
  ];

  for (const resource of registeredResources) {
    if (!allResourceNames.has(resource)) {
      fail(`source registry references missing Tinybird resource ${resource}`);
    }
  }

  if (new Set(registeredResources).size !== registeredResources.length) {
    fail("source registry contains duplicate Tinybird resources");
  }

  for (const source of registry.sources) {
    if (!Array.isArray(source.adapters) || source.adapters.length === 0) {
      fail(`source registry entry ${source.id} has no concrete adapters`);
    }
  }

  const fanIns = registry.contract_fan_ins ?? {};
  for (const contractName of [
    "boom_classified_payment_v1",
    "lead_evidence_v1",
    "ad_delivery_hourly_v1",
    "ad_delivery_daily_v1",
    "identity_observation_v1",
  ]) {
    const entries = fanIns[contractName];
    if (!Array.isArray(entries) || entries.length === 0) {
      fail(`source registry contract ${contractName} has no registered fan-in resources`);
      continue;
    }

    const resources = entries.map((entry) => entry.resource);
    if (new Set(resources).size !== resources.length) {
      fail(`source registry contract ${contractName} contains duplicate resources`);
    }

    for (const entry of entries) {
      if (!allResourceNames.has(entry.resource)) {
        fail(`source registry contract ${contractName} references missing resource ${entry.resource}`);
      }

      if (!entry.derived_from_contract && !sourceIds.includes(entry.source_id)) {
        fail(`source registry contract ${contractName} references unknown source ${entry.source_id}`);
      }

      if (contractName !== "boom_classified_payment_v1") {
        const projectionPath = resourcePaths.get(entry.resource);
        if (projectionPath) {
          const projection = await readFile(projectionPath, "utf8");
          if (!projection.includes("source_connection_id")) {
            fail(`${relative(projectRoot, projectionPath)}: canonical contract projection is missing source_connection_id`);
          }
        }
      }
    }
  }

  for (const entry of fanIns.boom_classified_payment_v1 ?? []) {
    const source = registry.sources.find((candidate) => candidate.id === entry.source_id);
    if (source && !source.source_connection_ids.includes(entry.source_connection_id)) {
      fail(`payment fan-in ${entry.resource} has the wrong source_connection_id`);
    }
  }

  try {
    for (const [relativePath, expected] of generatedFanInFiles(registry, canonicalContracts)) {
      const current = await readFile(join(projectRoot, relativePath), "utf8");
      if (current !== expected) {
        fail(`${relativePath}: generated source fan-in is stale`);
      }
    }
  } catch (error) {
    fail(`source registry fan-in generation failed (${error.message})`);
  }
}

async function validateRawManifest(files) {
  const candidateNames = [
    "contracts/raw-bigquery-schemas.json",
    "contracts/raw-sources.json",
    "contracts/raw-source-schemas.json",
    "contracts/raw-schema-manifest.json",
  ];
  let manifest = null;

  for (const candidate of candidateNames) {
    try {
      manifest = await readJson(candidate);
      if (manifest) {
        break;
      }
    } catch {
      // The schema renderer may use another documented name. Validation below
      // still checks every rendered Data Source structurally.
    }
  }

  if (!manifest) {
    return;
  }

  const sources = manifest.sources ?? manifest.tables ?? [];
  if (sources.length !== 49) {
    fail(`raw schema manifest contains ${sources.length} tables; expected 49`);
  }

  const rawFiles = files.filter(
    (path) => path.includes(`${join("datasources", "raw")}/`) && extname(path) === ".datasource",
  );
  if (rawFiles.length !== sources.length) {
    fail(`rendered raw Data Source count ${rawFiles.length} does not match manifest count ${sources.length}`);
  }
}

async function validateRawColumnReferences(files) {
  let manifest;
  try {
    manifest = await readJson("contracts/raw-bigquery-schemas.json");
  } catch {
    return;
  }

  if (!manifest) {
    return;
  }

  const fieldsByResource = new Map(
    manifest.tables.map((table) => [
      table.resourceName,
      new Set(table.fields.map((field) => field.name)),
    ]),
  );

  for (const path of files.filter((candidate) => extname(candidate) === ".pipe")) {
    const source = await readFile(path, "utf8");
    const nodeBlocks = source.split(/^NODE\s+/m).slice(1);

    for (const block of nodeBlocks) {
      const bindings = new Map();
      const relationPattern = /\b(?:FROM|JOIN)\s+(raw_[A-Za-z0-9_]+)\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*)/gi;

      for (const match of block.matchAll(relationPattern)) {
        bindings.set(match[2], match[1]);
      }

      for (const [alias, resource] of bindings) {
        const knownFields = fieldsByResource.get(resource);
        if (!knownFields) {
          fail(`${relative(projectRoot, path)}: references raw resource ${resource} missing from schema manifest`);
          continue;
        }

        const referencePattern = new RegExp(`\\b${alias}\\.([A-Za-z_][A-Za-z0-9_]*)`, "g");
        for (const reference of block.matchAll(referencePattern)) {
          if (!knownFields.has(reference[1])) {
            fail(`${relative(projectRoot, path)}: ${alias}.${reference[1]} is not a field in ${resource}`);
          }
        }
      }
    }
  }
}

const files = await walk(projectRoot);
const datafiles = files.filter((path) => [".datasource", ".pipe", ".connection"].includes(extname(path)));
const names = new Map();

for (const path of datafiles) {
  const name = resourceName(path);
  const existing = names.get(name);
  if (existing) {
    fail(`${relative(projectRoot, path)}: resource name also used by ${relative(projectRoot, existing)}`);
    continue;
  }
  names.set(name, path);
}

const allResourceNames = new Set(names.keys());
const pipeResourceNames = new Set(
  datafiles.filter((path) => extname(path) === ".pipe").map(resourceName),
);

for (const path of datafiles) {
  const source = await readFile(path, "utf8");
  const label = relative(projectRoot, path);

  if (source.trim() === "") {
    fail(`${label}: file is empty`);
    continue;
  }

  validateNoIndentedProperties(path, source);
  if (extname(path) === ".datasource") {
    validateDatasource(path, source);
  }
  if (extname(path) === ".pipe") {
    validatePipe(path, source, allResourceNames);
  }
}

await validateCoverage(names);
await validateEndpointContracts(files);
validateTestResourceNames(files, pipeResourceNames);
await validateSourceRegistry(names);
await validateRawManifest(files);
await validateRawColumnReferences(files);

if (failures.length > 0) {
  console.error(`Tinybird project validation failed with ${failures.length} problem(s):`);
  for (const failure of failures) {
    console.error(`- ${failure}`);
  }
  process.exitCode = 1;
} else {
  const datasourceCount = datafiles.filter((path) => extname(path) === ".datasource").length;
  const pipeCount = datafiles.filter((path) => extname(path) === ".pipe").length;
  console.log(`Validated ${datasourceCount} Data Sources and ${pipeCount} Pipes.`);
  console.log("Static validation passed. Tinybird compilation remains a branch gate.");
}
