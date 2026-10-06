import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";

const evidencePath = stringArgument("evidence");
const snapshot = stringArgument("snapshot");
const maxBatchVersion = numberArgument("max-batch-version");
const buckets = mismatchBuckets(evidencePath);

const bigqueryRows = queryBigQuery(bigquerySql(buckets));
const tinybirdRows = queryTinybird(tinybirdSql(buckets));
const result = compareNames(bigqueryRows, tinybirdRows);

const output = {
  snapshot,
  max_batch_version: maxBatchVersion,
  compared_buckets: buckets,
  bigquery_rows: bigqueryRows.length,
  tinybird_rows: tinybirdRows.length,
  ...result,
};
const serialized = `${JSON.stringify(output, null, 2)}\n`;
const outputPath = optionalStringArgument("output");
if (outputPath) writeFileSync(outputPath, serialized, { mode: 0o600 });
console.log(serialized);

function mismatchBuckets(path) {
  const evidence = JSON.parse(readFileSync(path, "utf8"));
  const profiles = evidence.surfaces.find((surface) => surface.name === "profiles");
  if (!profiles?.digest?.bucket_mismatches) {
    throw new Error("The evidence file has no profile bucket mismatches.");
  }
  return profiles.digest.bucket_mismatches.map((row) => Number(row.bucket));
}

function bigquerySql(selectedBuckets) {
  return `
    WITH profiles AS (
      SELECT *
      FROM \`able-folio-499722.booming_data_analytics.int_resolved_identifiers\`
        FOR SYSTEM_TIME AS OF TIMESTAMP(@snapshot_at)
    ),
    projected AS (
      SELECT
        'boom' AS tenant_id,
        profile_id,
        COALESCE(first_name, '') AS first_name,
        COALESCE(last_name, '') AS last_name
      FROM profiles
    )
    SELECT profile_id, first_name, last_name
    FROM projected
    WHERE ${bigqueryBucket()} IN (${selectedBuckets.join(",")})
    ORDER BY profile_id
  `;
}

function tinybirdSql(selectedBuckets) {
  return `
    WITH base AS (
      SELECT tenant_id, state_key, profile_id, first_name, last_name
      FROM identity_state_seed_enriched
      WHERE tenant_id = 'boom' AND state_kind = 'profile'
    ),
    delta AS (
      SELECT
        journal.tenant_id,
        journal.state_key,
        journal.profile_id,
        journal.first_name,
        journal.last_name,
        journal.is_deleted
      FROM identity_state_delta_versions AS journal
      INNER JOIN activated_identity_batches AS activated
        ON journal.tenant_id = activated.tenant_id
        AND journal.batch_version = activated.batch_version
        AND journal.batch_id = activated.batch_id
      WHERE
        journal.tenant_id = 'boom'
        AND journal.state_kind = 'profile'
        AND journal.batch_version <= ${maxBatchVersion}
      QUALIFY row_number() OVER (
        PARTITION BY journal.tenant_id, journal.lookup_key, journal.sub_key
        ORDER BY journal.batch_version DESC, journal.committed_at DESC, journal.row_hash DESC
      ) = 1
    ),
    projected AS (
      SELECT
        if(delta.state_key = '', base.tenant_id, delta.tenant_id) AS tenant_id,
        if(delta.state_key = '', base.profile_id, delta.profile_id) AS profile_id,
        if(delta.state_key = '', base.first_name, delta.first_name) AS first_name,
        if(delta.state_key = '', base.last_name, delta.last_name) AS last_name
      FROM base
      FULL OUTER JOIN delta
        ON base.tenant_id = delta.tenant_id
        AND base.state_key = delta.state_key
      WHERE delta.state_key = '' OR delta.is_deleted = 0
    )
    SELECT profile_id, first_name, last_name
    FROM projected
    WHERE ${tinybirdBucket()} IN (${selectedBuckets.join(",")})
    ORDER BY profile_id
  `;
}

function bigqueryBucket() {
  return `MOD(MOD(FARM_FINGERPRINT(CONCAT(
    'V4:boom',
    'V', CAST(BYTE_LENGTH(profile_id) AS STRING), ':', profile_id
  )), 256) + 256, 256)`;
}

function tinybirdBucket() {
  return `farmFingerprint64(concat(
    'V4:boom',
    'V', toString(length(profile_id)), ':', profile_id
  )) % 256`;
}

function queryBigQuery(sql) {
  const output = execFileSync("bq", [
    "query",
    "--quiet",
    "--use_legacy_sql=false",
    "--project_id=able-folio-499722",
    "--location=US",
    "--format=json",
    "--max_rows=1000000",
    "--maximum_bytes_billed=20000000000",
    `--parameter=snapshot_at:STRING:${snapshot}`,
    sql,
  ], { encoding: "utf8", maxBuffer: 256 * 1024 * 1024 });
  return JSON.parse(output);
}

function queryTinybird(sql) {
  const output = execFileSync("tb", [
    "--cloud",
    "--output",
    "json",
    "sql",
    "--rows-limit",
    "1000000",
    sql,
  ], { encoding: "utf8", maxBuffer: 256 * 1024 * 1024 });
  return parseJsonOutput(output).data;
}

function compareNames(bigqueryRows, tinybirdRows) {
  const expected = new Map(bigqueryRows.map((row) => [row.profile_id, row]));
  const actual = new Map(tinybirdRows.map((row) => [row.profile_id, row]));
  const profileIds = [...new Set([...expected.keys(), ...actual.keys()])].sort();
  const mismatches = profileIds.flatMap((profileId) => {
    const bigquery = expected.get(profileId);
    const tinybird = actual.get(profileId);
    if (!bigquery || !tinybird) {
      return [{ profile_id: profileId, kind: bigquery ? "missing_tinybird" : "missing_bigquery" }];
    }
    if (
      String(bigquery.first_name) === String(tinybird.first_name)
      && String(bigquery.last_name) === String(tinybird.last_name)
    ) return [];
    return [{
      profile_id: profileId,
      kind: "trait",
      bigquery_first_name: String(bigquery.first_name),
      bigquery_last_name: String(bigquery.last_name),
      tinybird_first_name: String(tinybird.first_name),
      tinybird_last_name: String(tinybird.last_name),
    }];
  });
  return { mismatch_count: mismatches.length, mismatches };
}

function parseJsonOutput(output) {
  for (const match of output.matchAll(/[\[{]/g)) {
    try {
      return JSON.parse(output.slice(match.index).trim());
    } catch {
      continue;
    }
  }
  throw new Error("Command did not return JSON.");
}

function numberArgument(name) {
  const value = Number(stringArgument(name));
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`--${name} is invalid.`);
  return value;
}

function stringArgument(name) {
  const prefix = `--${name}=`;
  const values = process.argv.filter((argument) => argument.startsWith(prefix));
  if (values.length !== 1) throw new Error(`Supply --${name}=... exactly once.`);
  return values[0].slice(prefix.length);
}

function optionalStringArgument(name) {
  const prefix = `--${name}=`;
  const values = process.argv.filter((argument) => argument.startsWith(prefix));
  if (values.length > 1) throw new Error(`Supply --${name}=... at most once.`);
  return values[0]?.slice(prefix.length) ?? null;
}
