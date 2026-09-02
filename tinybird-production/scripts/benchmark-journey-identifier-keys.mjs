import { readFile } from "node:fs/promises";

const requestedKeyCount = Number(process.argv[2] ?? "50");
if (!Number.isSafeInteger(requestedKeyCount) || requestedKeyCount < 1 || requestedKeyCount > 500) {
  throw new Error("key count must be an integer from 1 through 500.");
}

const tinybirdConfigUrl = new URL("../.tinyb", import.meta.url);
const tinybirdConfig = JSON.parse(await readFile(tinybirdConfigUrl, "utf8"));
const identifierMappings = await sampleIdentifierKeys(tinybirdConfig, requestedKeyCount);
const identifierKeys = identifierMappings.map((mapping) => mapping.identifierKey);
const identifierProfileIds = identifierMappings.map((mapping) => mapping.profileId);
if (identifierKeys.length !== requestedKeyCount) {
  throw new Error(`Expected ${requestedKeyCount} identifier keys but found ${identifierKeys.length}.`);
}

const url = new URL("/v0/pipes/reporting_profile_journey_build.json", tinybirdConfig.host);
const parameters = new URLSearchParams({
  p_identifier_keys_delimited: delimitedParameter(identifierKeys),
  p_identifier_profile_ids_delimited: delimitedParameter(identifierProfileIds),
});

const startedAt = performance.now();
const response = await fetch(url, {
  method: "POST",
  headers: {
    Authorization: `Bearer ${tinybirdConfig.token}`,
    "Content-Type": "application/x-www-form-urlencoded",
  },
  body: parameters,
});
const body = await response.json();
const keyList = sqlStrings(identifierKeys);
const components = await Promise.all([
  measureSql(tinybirdConfig, "mapping_seed", `
    SELECT count()
    FROM identity_mapping_serving_seed
    WHERE tenant_id = 'boom' AND identifier_key IN (${keyList})
  `),
  measureSql(tinybirdConfig, "mapping_delta", `
    SELECT count()
    FROM identity_mapping_delta_lookup
    WHERE tenant_id = 'boom' AND identifier_key IN (${keyList})
  `),
  measureSql(tinybirdConfig, "mapping_delta_activated", `
    SELECT count()
    FROM identity_mapping_delta_lookup AS journal
    INNER JOIN activated_identity_batches AS activated
      ON journal.tenant_id = activated.tenant_id
      AND journal.batch_version = activated.batch_version
      AND journal.batch_id = activated.batch_id
    WHERE journal.tenant_id = 'boom' AND journal.identifier_key IN (${keyList})
  `),
  measureSql(tinybirdConfig, "conversion_facts", `
    SELECT count()
    FROM reporting_conversion_facts_v2_current
    WHERE tenant_id = 'boom' AND identity_anchor_key IN (${keyList})
  `),
  measureSql(tinybirdConfig, "touchpoint_facts", `
    SELECT count()
    FROM mart_touchpoints_all_facts_v3_current
    WHERE identity_anchor_key IN (${keyList})
  `),
]);

console.log(JSON.stringify({
  httpStatus: response.status,
  requestedIdentifierKeys: identifierKeys.length,
  elapsedSeconds: (performance.now() - startedAt) / 1_000,
  outputRows: Array.isArray(body.data) ? body.data.length : null,
  statistics: body.statistics ?? null,
  components,
  error: body.error ?? null,
}, null, 2));

if (!response.ok) process.exitCode = 1;

async function sampleIdentifierKeys(config, limit) {
  const response = await fetch(`${config.host}/v0/sql`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      q: `
        SELECT profile_id, arrayJoin(member_identifier_keys) AS identifier_key
        FROM
          (
            SELECT profile_id, member_identifier_keys
            FROM current_identity_profiles
            WHERE length(member_identifier_keys) >= 3
            ORDER BY cityHash64(profile_id)
            LIMIT 100
          )
        ORDER BY cityHash64(identifier_key)
        LIMIT ${limit}
        FORMAT JSON
      `,
    }),
  });
  if (!response.ok) {
    throw new Error(`Tinybird identifier sample failed with HTTP ${response.status}.`);
  }
  const payload = await response.json();
  return payload.data.map((row) => ({
    identifierKey: String(row.identifier_key),
    profileId: String(row.profile_id),
  }));
}

async function measureSql(config, name, sql) {
  const response = await fetch(`${config.host}/v0/sql`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ q: `${sql} FORMAT JSON` }),
  });
  const body = await response.json();
  return {
    name,
    httpStatus: response.status,
    matchedRows: body.data?.[0]?.["count()"] ?? null,
    statistics: body.statistics ?? null,
    error: body.error ?? null,
  };
}

function sqlStrings(values) {
  return values.map((value) => `'${value.replaceAll("'", "''")}'`).join(", ");
}

function delimitedParameter(values) {
  const delimiter = "|";
  if (values.some((value) => value.includes(delimiter))) {
    throw new Error(`Journey parameter values cannot contain ${delimiter}.`);
  }
  return values.join(delimiter);
}
