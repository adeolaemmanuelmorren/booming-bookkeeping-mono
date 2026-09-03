import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const projectRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);

const expectedRawDataSourceCount = 50;
const expectedLiveDataSource = "jitsu_events_api_observations.datasource";
const expectedSyncTokenNames = [
  "TOKEN boom_bigquery_tinybird_sync APPEND",
  'TOKEN "boom_bigquery_tinybird_sync" APPEND',
];

const rawDirectory = path.join(projectRoot, "datasources", "raw");
const liveDirectory = path.join(projectRoot, "datasources", "live");
const seedDirectory = path.join(projectRoot, "datasources", "seed");
const stateDirectory = path.join(projectRoot, "datasources", "state");
const pipeDirectories = [
  path.join(projectRoot, "pipes"),
  path.join(projectRoot, "endpoints"),
  path.join(projectRoot, "materializations"),
  path.join(projectRoot, "copies"),
];

const rawFiles = (await readdir(rawDirectory)).filter((file) =>
  file.endsWith(".datasource"),
);

if (rawFiles.length !== expectedRawDataSourceCount) {
  fail(
    `Expected ${expectedRawDataSourceCount} raw Data Sources, found ${rawFiles.length}`,
  );
}

const liveFiles = await readdir(liveDirectory);

if (!liveFiles.includes(expectedLiveDataSource)) {
  fail(`Missing live Data Source ${expectedLiveDataSource}`);
}

const stateFiles = new Set(await readdir(stateDirectory));
const seedFiles = new Set(await readdir(seedDirectory));
const requiredJitsuStateDataSources = [
  "jitsu_attribution_param_versions.datasource",
  "jitsu_form_submission_versions.datasource",
  "jitsu_identify_versions.datasource",
  "jitsu_order_completed_versions.datasource",
  "jitsu_page_view_versions.datasource",
];

const requiredIdentityStateDataSources = [
  "identity_batch_output_rows.datasource",
  "identity_compaction_changes_stage.datasource",
  "identity_compaction_components_stage.datasource",
  "identity_compaction_scope_stage.datasource",
  "identity_events_cursor.datasource",
  "identity_state_delta_versions.datasource",
];

const requiredIdentitySeedDataSources = [
  "identity_fact_evidence_seed.datasource",
  "identity_mapping_evidence_seed.datasource",
  "identity_state_seed.datasource",
  "identity_state_seed_enriched.datasource",
];

const requiredReportingSeedDataSources = [
  "reporting_journey_seed_current.datasource",
];

const requiredRevenueStateDataSources = [
  "int_payment_plan_timing_current.datasource",
  "mart_revenue_attribution_current.datasource",
  "reporting_journey_commits.datasource",
  "reporting_journey_versions.datasource",
];

const requiredPaymentStateDataSources = [
  "int_stripe_browser_product_resolution_current.datasource",
];

const requiredSegmentStateDataSources = [
  "segretl_repeatable_conversions_current.datasource",
];

for (const file of requiredJitsuStateDataSources) {
  if (!stateFiles.has(file)) {
    fail(`Missing Jitsu state Data Source ${file}`);
  }
}

for (const file of requiredIdentityStateDataSources) {
  if (!stateFiles.has(file)) {
    fail(`Missing identity state Data Source ${file}`);
  }
}

for (const file of requiredIdentitySeedDataSources) {
  if (!seedFiles.has(file)) {
    fail(`Missing identity seed Data Source ${file}`);
  }
}

for (const file of requiredReportingSeedDataSources) {
  if (!seedFiles.has(file)) {
    fail(`Missing reporting seed Data Source ${file}`);
  }
}

for (const file of requiredRevenueStateDataSources) {
  if (!stateFiles.has(file)) {
    fail(`Missing revenue attribution state Data Source ${file}`);
  }
}

for (const file of requiredPaymentStateDataSources) {
  if (!stateFiles.has(file)) {
    fail(`Missing payment model state Data Source ${file}`);
  }
}

for (const file of requiredSegmentStateDataSources) {
  if (!stateFiles.has(file)) {
    fail(`Missing Segment output state Data Source ${file}`);
  }
}

for (const file of rawFiles) {
  const contents = await readFile(path.join(rawDirectory, file), "utf8");

  if (!contents.includes("IMPORT_CONNECTION_NAME boom_bigquery_gcs")) {
    fail(`${file} is not bound to boom_bigquery_gcs`);
  }

  if (!contents.includes("gs://booming-data/tinybird/")) {
    fail(`${file} does not use the approved GCS prefix`);
  }

  if (!contents.includes("IMPORT_FORMAT parquet")) {
    fail(`${file} is not configured for Parquet`);
  }

  if (!expectedSyncTokenNames.some((token) => contents.includes(token))) {
    fail(`${file} does not grant the resource-scoped sync token append access`);
  }
}

const pipeFiles = await collectFiles(pipeDirectories, ".pipe");
const resourceNames = new Set();

for (const file of pipeFiles) {
  const resourceName = path.basename(file, ".pipe");

  if (resourceNames.has(resourceName)) {
    fail(`Duplicate Tinybird resource name ${resourceName}`);
  }

  resourceNames.add(resourceName);
}

const requiredResources = [
  "meta_ad_dimensions",
  "model_mart_ad_performance_hourly",
  "model_mart_meta_delivery_daily",
  "mart_ad_performance_hourly",
  "mart_meta_delivery_daily",
  "all_stripe_payments_adapter",
  "all_stripe_payments_build",
  "snapshot_all_stripe_payments",
  "activated_identity_batches",
  "current_identity_batch_versions",
  "identity_state_base",
  "current_identity_delta_state",
  "current_identity_state",
  "current_identity_evidence",
  "current_identity_facts",
  "current_identity_mappings",
  "current_identity_profiles",
  "source_identity_facts",
  "enqueue_identity_changes",
  "identity_compaction_prepare_build",
  "prepare_identity_compaction",
  "identity_compaction_changes_build",
  "classify_identity_compaction_changes",
  "identity_compaction_profiles_build",
  "seed_identity_compaction_profiles",
  "identity_compaction_touched_profiles_build",
  "freeze_identity_compaction_touched_profiles",
  "identity_compaction_scope_build",
  "scope_identity_compaction",
  "identity_compaction_fact_keys_build",
  "select_identity_compaction_fact_keys",
  "identity_compaction_facts_build",
  "expand_identity_compaction_facts",
  "identity_compaction_components_build",
  "build_identity_compaction_components",
  "identity_compaction_build",
  "compact_identity_state",
  "identity_compaction_manifest",
  "identity_worker_pending_facts",
  "identity_worker_fact_heads",
  "identity_worker_mapping_heads",
  "identity_worker_profile_heads",
  "identity_worker_evidence_heads",
  "identity_worker_written_rows",
  "current_identity_compaction_cursor",
  "identity_generation",
  "resolve_identity",
  "jitsu_attribution_params",
  "jitsu_attribution_params_build",
  "jitsu_attribution_params_live_build",
  "jitsu_form_submissions",
  "jitsu_form_submissions_build",
  "jitsu_form_submissions_live_build",
  "jitsu_identifies",
  "jitsu_identifies_build",
  "jitsu_identifies_live_build",
  "jitsu_order_completed",
  "jitsu_order_completed_build",
  "jitsu_order_completed_live_build",
  "jitsu_page_views",
  "jitsu_page_views_build",
  "jitsu_page_views_live_build",
  "sessionized_page_views",
  "website_sessions",
  "website_session_touchpoints",
  "backfill_activecampaign_contacts_current",
  "backfill_activecampaign_contact_tags_current",
  "backfill_activecampaign_tags_current",
  "backfill_jitsu_attribution_param_versions",
  "backfill_jitsu_form_submission_versions",
  "backfill_jitsu_identify_versions",
  "backfill_jitsu_order_completed_versions",
  "backfill_jitsu_page_view_versions",
  "materialize_jitsu_attribution_param_versions_live",
  "materialize_jitsu_form_submission_versions_live",
  "materialize_jitsu_identify_versions_live",
  "materialize_jitsu_order_completed_versions_live",
  "materialize_jitsu_page_view_versions_live",
  "int_payment_plan_timing_build",
  "snapshot_int_payment_plan_timing",
  "int_payment_plan_timing",
  "int_stripe_browser_product_resolution_build",
  "snapshot_int_stripe_browser_product_resolution",
  "int_stripe_browser_product_resolution",
  "mart_revenue_attribution_build",
  "mart_revenue_attribution",
  "reporting_profile_journey_window_build",
  "reporting_profile_journey_build",
  "reporting_journey_profile_page",
  "reporting_current_journey_rows",
  "reporting_current_journey_base_rows",
  "mart_conversions_with_touchpoints_build",
  "mart_conversions_multi_touch_build",
  "segretl_repeatable_conversions_build",
  "segretl_repeatable_conversions",
  "snapshot_segretl_repeatable_conversions",
];

for (const resourceName of requiredResources) {
  if (!resourceNames.has(resourceName)) {
    fail(`Missing required resource ${resourceName}`);
  }
}

console.log(
  `Production project check passed: ${rawFiles.length} raw Data Sources, ` +
    `${liveFiles.length} live Data Source, ${pipeFiles.length} pipes/endpoints.`,
);

async function collectFiles(directories, extension) {
  const matches = [];

  for (const directory of directories) {
    await walk(directory, matches, extension);
  }

  return matches;
}

async function walk(directory, matches, extension) {
  const entries = await readdir(directory, { withFileTypes: true });

  for (const entry of entries) {
    const entryPath = path.join(directory, entry.name);

    if (entry.isDirectory()) {
      await walk(entryPath, matches, extension);
      continue;
    }

    if (entry.name.endsWith(extension)) {
      matches.push(entryPath);
    }
  }
}

function fail(message) {
  console.error(message);
  process.exitCode = 1;
  throw new Error(message);
}
