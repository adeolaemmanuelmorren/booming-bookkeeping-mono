import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const projectRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../..",
);

test("journey payload rows are versioned and atomically activated per conversion", async () => {
  const versions = await resource("datasources/state/reporting_journey_versions.datasource");
  const commits = await resource("datasources/state/reporting_journey_commits.datasource");
  const current = await resource(
    "pipes/models/attribution/reporting_current_journey_base_rows.pipe",
  );

  assert.match(versions, /ENGINE ReplacingMergeTree/);
  assert.match(versions, /journey_row_key/);
  assert.match(commits, /ENGINE ReplacingMergeTree/);
  assert.match(commits, /ENGINE_SORTING_KEY tenant_id, conversion_id/);
  assert.match(commits, /ENGINE_VER batch_version/);
  assert.match(commits, /journey_row_count/);
  assert.match(current, /FROM reporting_journey_commits/);
  assert.match(current, /versions\.batch_version = commits\.active_batch_version/);
  assert.match(current, /versions\.batch_id = commits\.active_batch_id/);
  assert.match(current, /WHERE commits\.active_is_deleted = 0/);
  assert.match(current, /FROM reporting_journey_seed_current AS seed/);
  assert.match(current, /LEFT ANTI JOIN latest_journey_commit_versions/);
  assert.match(current, /FROM current_seed_journey_rows[\s\S]*UNION ALL/);
  assert.match(current, /nullIf\(journey\.touchpoint_id, ''\) AS touchpoint_id/);
});

test("only the Worker-facing endpoint executes journey windows", async () => {
  const endpoint = await resource(
    "pipes/models/attribution/reporting_profile_journey_window_build.pipe",
  );
  const storedReader = await resource(
    "pipes/models/attribution/mart_conversions_with_touchpoints_build.pipe",
  );
  const multiTouch = await resource(
    "pipes/models/attribution/mart_conversions_multi_touch_build.pipe",
  );
  const revenue = await resource(
    "pipes/outputs/attribution/mart_revenue_attribution_build.pipe",
  );

  assert.match(endpoint, /TYPE ENDPOINT/);
  assert.match(endpoint, /p_identifier_key/);
  assert.match(endpoint, /p_identifier_keys/);
  assert.match(endpoint, /p_identifier_profile_ids/);
  assert.match(endpoint, /p_identifier_keys_delimited/);
  assert.match(endpoint, /p_identifier_profile_ids_delimited/);
  assert.match(endpoint, /split_to_array\([^)]*separator='\|'\)/);
  assert.match(endpoint, /arrayZip/);
  assert.match(endpoint, /p_conversion_ids/);
  assert.match(endpoint, /FROM reporting_conversion_facts_v2_current/);
  assert.match(endpoint, /FROM mart_touchpoints_all_facts_v3_current/);
  assert.match(endpoint, /identity_anchor_key IN/);
  assert.match(storedReader, /FROM reporting_current_journey_rows/);
  assert.match(multiTouch, /FROM reporting_current_journey_base_rows/);
  assert.match(multiTouch, /FROM reporting_latest_ad_names_current/);
  assert.match(revenue, /FROM reporting_current_journey_rows/);
  assert.doesNotMatch(storedReader, /reporting_profile_journey_window_build/);
  assert.doesNotMatch(multiTouch, /reporting_profile_journey_window_build/);
  assert.doesNotMatch(revenue, /reporting_profile_journey_window_build/);
});

test("journey point lookups keep bounded physical indexes", async () => {
  const mappingSeed = await resource(
    "datasources/seed/identity_mapping_serving_seed.datasource",
  );
  const mappingDelta = await resource(
    "datasources/state/identity_mapping_delta_lookup.datasource",
  );
  const activationLookup = await resource(
    "datasources/state/identity_activation_lookup.datasource",
  );
  const journeyWindow = await resource(
    "pipes/models/attribution/reporting_profile_journey_window_build.pipe",
  );
  const conversions = await resource(
    "datasources/state/reporting_conversion_facts_v2_current.datasource",
  );

  assert.match(mappingSeed, /ENGINE_SETTINGS "index_granularity=128"/);
  assert.match(mappingDelta, /ENGINE_SETTINGS "index_granularity=128"/);
  assert.match(activationLookup, /ENGINE_SETTINGS "index_granularity=128"/);
  assert.match(journeyWindow, /INNER JOIN identity_activation_lookup AS activated/);
  assert.doesNotMatch(journeyWindow, /INNER JOIN activated_identity_batches AS activated/);
  assert.match(
    conversions,
    /INDEX conversion_id_bloom conversion_id TYPE bloom_filter\(0\.01\) GRANULARITY 1/,
  );
});

test("expensive stable revenue and channel calculations happen before storage", async () => {
  const endpoint = await resource("endpoints/reporting_profile_journey_build.pipe");
  const revenue = await resource(
    "pipes/outputs/attribution/mart_revenue_attribution_build.pipe",
  );
  const current = await resource("pipes/models/attribution/reporting_current_journey_rows.pipe");

  assert.match(endpoint, /revenue_multi_touch_decimal/);
  assert.match(endpoint, /touchpoint_channel_label/);
  assert.match(revenue, /revenue_multi_touch_decimal/);
  assert.match(revenue, /touchpoint_channel_label/);
  assert.doesNotMatch(revenue, /match\(lowerUTF8/);
  assert.match(current, /FROM reporting_latest_ad_names_current/);
});

test("the immutable journey snapshot is the seed instead of a historical recompute", async () => {
  const current = await resource(
    "pipes/models/attribution/reporting_current_journey_base_rows.pipe",
  );

  assert.match(current, /'journey_seed' AS batch_id/);
  assert.match(current, /toUInt64\(0\) AS batch_version/);
  assert.match(current, /LEFT ANTI JOIN latest_journey_commit_versions/);
});

test("the conversion change endpoint limits the combined entity streams", async () => {
  const endpoint = await resource(
    "endpoints/reporting_cdc_changed_conversions.pipe",
  );

  assert.match(
    endpoint,
    /SELECT \*\s+FROM \([\s\S]*SELECT \* FROM changed_client_form_entities[\s\S]*SELECT \* FROM changed_server_payment_entities\s+\)\s+ORDER BY last_ingested_at ASC, entity_kind ASC, entity_id ASC\s+LIMIT \{\{ UInt64\(p_limit, 2000\) \}\}/,
  );
});

test("server forms and payments use the same bounded conversion CDC path", async () => {
  const changed = await resource(
    "endpoints/reporting_cdc_changed_conversions.pipe",
  );
  const build = await resource(
    "endpoints/reporting_conversion_facts_cdc_build.pipe",
  );

  assert.match(changed, /p_entity_scope/);
  assert.match(changed, /'server_form' AS entity_kind/);
  assert.match(changed, /'server_payment' AS entity_kind/);
  assert.match(changed, /FROM activecampaign_contact_tags_adapter/);
  assert.match(changed, /FROM activecampaign_registration_build/);
  assert.match(changed, /current_server_form_fact_heads/);
  assert.match(changed, /current_registration_form_facts/);
  assert.match(changed, /changed_or_new_server_form_ids/);
  assert.match(changed, /removed_or_superseded_server_form_ids/);
  assert.match(changed, /LEFT ANTI JOIN current_server_form_fact_heads/);
  assert.match(changed, /FROM stripe_identity_facts_adapter/);
  assert.match(build, /p_server_form_submission_ids/);
  assert.match(build, /p_server_payment_keys/);
  assert.match(build, /FROM all_stripe_payments_build AS payments/);
  assert.match(build, /SELECT \* FROM cdc_server_form_conversion_facts/);
  assert.match(build, /SELECT \* FROM cdc_server_payment_conversion_facts/);
});

async function resource(relativePath) {
  return readFile(path.join(projectRoot, relativePath), "utf8");
}
