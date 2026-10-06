import assert from "node:assert/strict";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  loadOutputContract,
  selectOutputs,
} from "../lib/parity-contract.mjs";
import {
  buildBigQueryDigestSql,
  buildBigQueryDiagnosticSql,
  buildBigQuerySchemaSql,
  buildTinybirdDigestSql,
  buildTinybirdDiagnosticSql,
  metricDefinitions,
} from "../lib/parity-sql.mjs";
import {
  areTypesCompatible,
  parseBigQueryType,
  parseTinybirdType,
} from "../lib/parity-types.mjs";
import { resolveTinybirdTarget } from "../lib/tinybird-target.mjs";

const projectRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
  "..",
);
const contractPath = path.join(
  projectRoot,
  "scripts",
  "parity",
  "output-contract.json",
);

test("the contract covers all 40 outputs in dependency order", () => {
  const contract = loadOutputContract(projectRoot, contractPath);
  const countsByLayer = Object.groupBy(
    contract.outputs,
    (output) => output.layer,
  );

  assert.equal(contract.outputs.length, 40);
  assert.deepEqual(
    Object.fromEntries(
      Object.entries(countsByLayer).map(([layer, outputs]) => [layer, outputs.length]),
    ),
    { 1: 9, 2: 12, 3: 6, 4: 6, 5: 5, 6: 2 },
  );
  assert.equal(
    contract.outputs.filter((output) => output.unique_key.length > 0).length,
    19,
  );
  assert.deepEqual(
    contract.outputs
      .filter((output) => output.bigquery_kind === "view")
      .map((output) => output.name),
    [
      "mart_conversions_multi_touch_pages",
      "mart_conversions_all_performance",
    ],
  );

  const touchpoints = contract.outputs.find(
    (output) => output.name === "mart_touchpoints_all",
  );
  assert.equal(touchpoints.digest_shards, 8);
  assert.equal(touchpoints.digest_shard_key, "touchpoint_id");
  assert.equal(touchpoints.digest_shard_strategy, "hex_range");
});

test("output selection rejects ambiguous or unknown requests", () => {
  const contract = loadOutputContract(projectRoot, contractPath);

  assert.equal(selectOutputs(contract, { outputs: [], layer: 6 }).length, 2);
  assert.throws(
    () => selectOutputs(contract, { outputs: ["missing"], layer: null }),
    /Unknown output/,
  );
  assert.throws(
    () => selectOutputs(contract, { outputs: ["mart_payments"], layer: 1 }),
    /not both/,
  );
});

test("production is the default Tinybird target", () => {
  assert.deepEqual(resolveTinybirdTarget({}), {
    name: "cloud",
    branch: null,
    cliArguments: ["--cloud"],
  });
  assert.deepEqual(resolveTinybirdTarget({ target: "branch", branch: "review" }), {
    name: "branch",
    branch: "review",
    cliArguments: ["--branch", "review"],
  });
  assert.deepEqual(resolveTinybirdTarget({ target: "staging" }), {
    name: "staging",
    branch: null,
    cliArguments: ["--cloud", "--staging"],
  });
  assert.throws(
    () => resolveTinybirdTarget({ target: "branch" }),
    /requires --branch/,
  );
  assert.throws(
    () => resolveTinybirdTarget({ target: "cloud", branch: "review" }),
    /cannot be used/,
  );
  assert.throws(
    () => resolveTinybirdTarget({ target: "staging", branch: "review" }),
    /cannot be used/,
  );
});

test("recursive BigQuery and Tinybird types compare by logical shape", () => {
  const bigQueryType = parseBigQueryType(
    "ARRAY<STRUCT<id STRING, quantity INT64, item_price NUMERIC>>",
  );
  const tinybirdType = parseTinybirdType(
    "Array(Tuple(id String, quantity Int64, item_price Decimal(38, 9)))",
  );

  assert.equal(bigQueryType.kind, "array");
  assert.equal(tinybirdType.kind, "array");
  assert.equal(areTypesCompatible(bigQueryType, tinybirdType).compatible, true);
  assert.equal(
    areTypesCompatible("TIMESTAMP", "DateTime64(3)").compatible,
    false,
  );
  assert.equal(areTypesCompatible("BOOL", "UInt8").compatible, true);

  const nestedPayload = parseBigQueryType(
    "STRUCT<event_name STRING, content_ids ARRAY<STRING>, address STRUCT<city STRING, country STRING>>",
  );
  assert.equal(nestedPayload.fields[1].type.kind, "array");
  assert.equal(nestedPayload.fields[2].type.kind, "struct");
});

test("digest SQL is recursive, length-prefixed, and schema-driven", () => {
  const columns = [
    { name: "id", data_type: "STRING" },
    { name: "amount", data_type: "NUMERIC" },
    {
      name: "items",
      data_type: "ARRAY<STRUCT<id STRING, quantity INT64, item_price NUMERIC>>",
    },
  ];
  const common = {
    columns,
    bucketCount: 256,
    floatScale: 9,
    arrayPolicies: { items: "set" },
  };
  const bigQuerySql = buildBigQueryDigestSql({
    ...common,
    source: {
      reference: "able-folio-499722.booming_data_analytics.example",
      timeTravel: true,
    },
  });
  const tinybirdSql = buildTinybirdDigestSql({
    ...common,
    resource: "example",
  });

  assert.match(bigQuerySql, /FOR SYSTEM_TIME AS OF TIMESTAMP\(@snapshot_at\)/);
  assert.match(bigQuerySql, /BYTE_LENGTH/);
  assert.match(bigQuerySql, /ORDER BY encoded_item/);
  assert.match(tinybirdSql, /arraySort\(arrayMap/);
  assert.match(tinybirdSql, /tupleElement/);
  assert.deepEqual(metricDefinitions(columns), [
    { name: "id", null_alias: "n_0", numeric_alias: null },
    { name: "amount", null_alias: "n_1", numeric_alias: "s_1" },
    { name: "items", null_alias: "n_2", numeric_alias: null },
  ]);
});

test("wide digest SQL can split sorted hexadecimal keys into pruned ranges", () => {
  const common = {
    columns: [{ name: "touchpoint_id", data_type: "STRING" }],
    bucketCount: 256,
    floatScale: 9,
    shardCount: 8,
    shardNumber: 3,
    shardKey: "touchpoint_id",
    shardStrategy: "hex_range",
  };
  const bigQuerySql = buildBigQueryDigestSql({
    ...common,
    source: {
      reference: "able-folio-499722.booming_data_analytics.mart_touchpoints_all",
      timeTravel: true,
    },
  });
  const tinybirdSql = buildTinybirdDigestSql({
    ...common,
    resource: "mart_touchpoints_all",
  });

  assert.match(bigQuerySql, /`touchpoint_id` >= '6' AND `touchpoint_id` < '8'/);
  assert.match(tinybirdSql, /`touchpoint_id` >= '6' AND `touchpoint_id` < '8'/);
});

test("hashed-row diagnostics normalize SHA-256 hex case across engines", () => {
  const common = {
    columns: [{ name: "payment_id", data_type: "STRING" }],
    bucketCount: 256,
    buckets: [0],
    diagnosticKey: ["payment_id"],
    floatScale: 9,
    limit: 50,
  };
  const bigQuerySql = buildBigQueryDiagnosticSql({
    ...common,
    source: {
      reference: "able-folio-499722.booming_data_analytics.mart_payments",
      timeTravel: true,
    },
  });
  const tinybirdSql = buildTinybirdDiagnosticSql({
    ...common,
    resource: "mart_payments",
  });

  assert.match(bigQuerySql, /LOWER\(TO_HEX\(SHA256\(canonical_key\)\)\)/);
  assert.match(tinybirdSql, /lower\(hex\(SHA256\(canonical_key\)\)\)/);
});

test("BigQuery schema SQL addresses INFORMATION_SCHEMA tables safely", () => {
  const sql = buildBigQuerySchemaSql(
    "able-folio-499722.booming_data_analytics.mart_payments",
  );

  assert.match(
    sql,
    /`able-folio-499722\.booming_data_analytics\.INFORMATION_SCHEMA\.COLUMNS`/,
  );
  assert.match(
    sql,
    /`able-folio-499722\.booming_data_analytics\.INFORMATION_SCHEMA\.TABLES`/,
  );
});
