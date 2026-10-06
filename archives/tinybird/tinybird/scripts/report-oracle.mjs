import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const projectDirectory = dirname(scriptDirectory);
const fixturePath = join(projectDirectory, "fixtures/reports/bill_report_cases.json");
const endpointDirectory = join(projectDirectory, "endpoints");
const adapterDirectory = join(projectDirectory, "pipes/adapters");
const domainDirectory = join(projectDirectory, "pipes/domain");
const copiesDirectory = join(projectDirectory, "copies");
const cases = JSON.parse(readFileSync(fixturePath, "utf8"));

function readProjectFile(...parts) {
  return readFileSync(join(projectDirectory, ...parts), "utf8");
}

function rawUrlParameter(url, parameterName) {
  const query = url.split("?")[1] ?? "";
  const escapedName = parameterName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = query.match(new RegExp(`(?:^|&)${escapedName}=([^&=]*)`, "i"));
  return match?.[1] || null;
}

function isInsideHalfOpenRange(timestamp, start, end) {
  const value = Date.parse(timestamp);
  return value >= Date.parse(start) && value < Date.parse(end);
}

function isStrictPacificMinute(value) {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$/.exec(value);
  if (!match) return false;

  const [, yearText, monthText, dayText, hourText, minuteText] = match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const daysInMonth = new Date(Date.UTC(year, month, 0)).getUTCDate();

  return month >= 1 && month <= 12
    && day >= 1 && day <= daysInMonth
    && hour >= 0 && hour <= 23
    && minute >= 0 && minute <= 59;
}

function latestGenerationStatus(rows) {
  const ordered = [...rows].sort((left, right) => {
    if (left.version !== right.version) return right.version - left.version;
    return Date.parse(right.activated_at) - Date.parse(left.activated_at);
  });
  return ordered[0]?.status ?? null;
}

function deduplicateCurrentEvents(rows) {
  const current = new Map();

  for (const row of rows) {
    const key = `${row.event_kind}:${String(row.source_row_id)}`;
    const prior = current.get(key);
    if (!prior || row.source_priority > prior.source_priority
      || (row.source_priority === prior.source_priority && row.source_version > prior.source_version)) {
      current.set(key, row);
    }
  }

  return [...current.values()];
}

function pacificDate(timestamp) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Los_Angeles",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date(timestamp));
}

function latestApplicableRegistration(registrations, paymentTime) {
  const upperBound = paymentTime ? Date.parse(paymentTime) : Number.POSITIVE_INFINITY;
  return [...registrations]
    .filter((registration) => Date.parse(registration.submitted_at) <= upperBound)
    .sort((left, right) => Date.parse(right.submitted_at) - Date.parse(left.submitted_at))[0];
}

function firstQualifyingFiveK(payments, earliestRegistrationTime) {
  return [...payments]
    .filter((payment) => payment.payment_category === "mentorship")
    .filter((payment) => !payment.is_repeat_payment)
    .filter((payment) => payment.net_amount > 900)
    .filter((payment) => Date.parse(payment.payment_time) >= Date.parse(earliestRegistrationTime))
    .sort((left, right) => Date.parse(left.payment_time) - Date.parse(right.payment_time))[0];
}

function isImmediateVip(payment, registration) {
  const acceptedRules = new Set(["keyboard_rich_challenge_vip", "structured_basic_vip"]);
  return acceptedRules.has(payment.product_rule)
    && !payment.is_repeat_payment
    && Date.parse(payment.payment_time) >= Date.parse(registration.submitted_at)
    && pacificDate(payment.payment_time) === pacificDate(registration.submitted_at);
}

function averageDailyDelivery(rows) {
  const representedDayCount = new Set(rows.map((row) => row.date)).size;
  const totals = new Map();

  for (const row of rows) {
    const current = totals.get(row.key) ?? { reach: 0, frequency: 0 };
    current.reach += row.reach;
    current.frequency += row.frequency;
    totals.set(row.key, current);
  }

  return new Map([...totals].map(([key, value]) => [key, {
    reach: value.reach / representedDayCount,
    frequency: value.frequency / representedDayCount,
  }]));
}

function assertOrdered(text, orderedTokens, context) {
  let cursor = -1;
  for (const token of orderedTokens) {
    const next = text.indexOf(token, cursor + 1);
    assert.notEqual(next, -1, `${context} is missing ${token}`);
    assert.ok(next > cursor, `${context} places ${token} out of order`);
    cursor = next;
  }
}

for (const testCase of cases.url_parameters) {
  assert.equal(rawUrlParameter(testCase.url, testCase.name), testCase.expected);
}

for (const [timestamp, expected] of cases.half_open_range.cases) {
  assert.equal(
    isInsideHalfOpenRange(timestamp, cases.half_open_range.start, cases.half_open_range.end),
    expected,
  );
}

for (const [value, expected] of cases.strict_dates) {
  assert.equal(isStrictPacificMinute(value), expected);
}

assert.equal(latestGenerationStatus(cases.generation_lifecycle.queryable), "validated");
assert.equal(latestGenerationStatus(cases.generation_lifecycle.revoked), "revoked");

const deduplicatedNullEvents = deduplicateCurrentEvents(cases.null_event_ids);
assert.equal(deduplicatedNullEvents.length, 2);
assert.equal(deduplicatedNullEvents.find((row) => row.event_kind === "client_form").source_priority, 3);

const earliestRegistration = cases.registrations[0];
const firstFiveK = firstQualifyingFiveK(cases.payments, earliestRegistration.submitted_at);
assert.equal(firstFiveK.payment_id, "first-qualifying-5k");
assert.equal(latestApplicableRegistration(cases.registrations, firstFiveK.payment_time).registration_id, "registration-applicable");
assert.equal(isImmediateVip(cases.payments[0], cases.registrations[1]), true);
assert.equal(1 * 4997, 4997);

const paidPreRegistrationTouches = cases.touchpoints.filter((touchpoint) =>
  touchpoint.source === "meta"
  && touchpoint.medium === "paid"
  && Date.parse(touchpoint.timestamp) < Date.parse(cases.registrations[1].submitted_at));
assert.equal(paidPreRegistrationTouches.length, 1);
assert.equal(paidPreRegistrationTouches[0].touchpoint_id, "paid-before");

const dailyAverages = averageDailyDelivery(cases.daily_delivery.rows);
for (const [key, expected] of Object.entries(cases.daily_delivery.expected)) {
  assert.deepEqual(dailyAverages.get(key), expected);
}

const endpointContracts = {
  bill_report_window: [
    "AS available_start_pacific",
    "AS previous_week_start_pacific",
    "AS active_week_start_pacific",
    "AS data_through_pacific",
  ],
  bill_live_acquisition: [
    "AS hour_start_pacific", "product.campaign_id", "product.adset_id", "product.ad_id",
    "product.campaign_name", "product.adset_name", "product.ad_name", "product.spend",
    "product.impressions", "product.clicks", "product.last_touch_registrations",
    "product.last_touch_immediate_vips", "product.first_touch_registrations",
    "product.first_touch_immediate_vips", "product.solo_touch_registrations",
    "product.solo_touch_immediate_vips",
  ],
  bill_5k_performance: [
    "AS hour_start_pacific", "product.campaign_id", "product.adset_id", "product.ad_id",
    "product.last_touch_5k_purchasers", "product.last_touch_5k_revenue",
    "product.last_touch_immediate_vip_5k_purchasers", "product.first_touch_5k_purchasers",
    "product.first_touch_5k_revenue", "product.solo_touch_5k_purchasers",
    "product.solo_touch_5k_revenue",
  ],
  bill_meta_delivery_daily: [
    "AS date", "product.level", "product.campaign_id", "product.adset_id", "product.ad_id",
    "product.impressions", "product.reach", "product.frequency",
  ],
};

for (const [endpointName, columns] of Object.entries(endpointContracts)) {
  const endpoint = readFileSync(join(endpointDirectory, `${endpointName}.pipe`), "utf8");
  const outputNode = endpoint.slice(endpoint.lastIndexOf("NODE "));
  assertOrdered(outputNode, columns, endpointName);
  assert.match(endpoint, /String\(p_report_generation\)/);
  assert.match(endpoint, /FROM published_report_build_stages/);
  assert.match(endpoint, /product_materialized_at/);
  assert.match(endpoint, /\.tenant_id = requested\.tenant_id/);
  assert.doesNotMatch(endpoint, /max\([^)]*materialized_at|argMax\([^)]*materialized_at/);
  assert.doesNotMatch(endpoint, /String\(report_generation\)/);
  assert.doesNotMatch(endpoint, /BestEffortOrNull/);
}

for (const endpointName of ["bill_live_acquisition", "bill_5k_performance", "bill_meta_delivery_daily"]) {
  const endpoint = readFileSync(join(endpointDirectory, `${endpointName}.pipe`), "utf8");
  assert.match(endpoint, /String\(p_range_start_pacific\)/);
  assert.match(endpoint, /String\(p_range_end_pacific\)/);
  assert.match(endpoint, /parseDateTime64BestEffort\(/);
}

const pageAdapter = readFileSync(join(adapterDirectory, "web_page_view_adapter.pipe"), "utf8");
assert.doesNotMatch(pageAdapter, /extractURLParameter/);
assert.match(pageAdapter, /\(\?i\)\(\?:\^\|&\)utm_source=\(\[\^&=\]\*\)/);
assert.match(pageAdapter, /WHERE 1 = 1/);

const conversionAdapter = readFileSync(join(adapterDirectory, "web_conversion_adapter.pipe"), "utf8");
assert.match(conversionAdapter, /WHERE 1 = 1/);
const identityAdapter = readFileSync(join(adapterDirectory, "web_identity_observation_adapter.pipe"), "utf8");
assert.match(identityAdapter, /nullIf\(trim\(source\.phone\), ''\) AS phone/);
assert.doesNotMatch(identityAdapter, /phone_number/);

const stripeAdapter = readFileSync(join(adapterDirectory, "stripe_primary_payment_adapter.pipe"), "utf8");
assertOrdered(stripeAdapter, ["billing_detail_phone", "customers.phone", "charges.metadata_phone"], "Stripe phone fallback");
assert.match(stripeAdapter, /coalesce\(catalog_product_id, 'mentorship_full_payment'\)/);

const acquisition = readProjectFile("pipes", "domain", "bill_krc_acquisition_product.pipe");
assert.match(acquisition, /CROSS JOIN current_report_policy AS policy/);
assert.match(acquisition, /payments\.net_amount > policy\.five_k_net_threshold/);
assert.match(acquisition, /touchpoints\.click_timestamp < registrations\.submitted_at/);
assert.match(acquisition, /payments\.is_repeat_payment = 0/);
assert.match(acquisition, /toDate\(payments\.payment_time, 'America\/Los_Angeles'\) = registrations\.registration_date/);
assert.match(acquisition, /FROM current_daily_ad_delivery/);
assert.match(acquisition, /argMax\(campaign_name, tuple\(date, source_version\)\)/);
assert.doesNotMatch(acquisition, /FROM meta_hourly_delivery_adapter/);
assert.doesNotMatch(acquisition, /fbclid.*profile_id|gclid.*profile_id|click_id.*profile_id/is);

const registrationPolicy = readProjectFile("pipes", "policies", "boom_registration_policy.pipe");
assert.match(registrationPolicy, /FROM current_lead_evidence/);
assert.match(registrationPolicy, /WHERE lead_type IN \('krc', 'webinar'\)/);
const profiledRegistrations = readProjectFile("pipes", "domain", "profiled_krc_registrations.pipe");
assert.match(profiledRegistrations, /FROM boom_registration_policy/);
assert.doesNotMatch(profiledRegistrations, /FROM activecampaign_registration_adapter/);

const genericConversionPresence = readProjectFile("pipes", "domain", "generic_conversion_row_presence.pipe");
assert.match(genericConversionPresence, /FROM current_daily_ad_delivery/);
assert.match(genericConversionPresence, /argMax\(campaign_name, tuple\(date, source_version\)\)/);
assert.doesNotMatch(genericConversionPresence, /FROM meta_hourly_delivery_adapter/);

const hourlyProduct = readProjectFile("pipes", "reports", "bill_hourly_product.pipe");
assert.match(hourlyProduct, /FULL OUTER JOIN generic_conversion_row_presence/);
assert.match(hourlyProduct, /five_k_reported_value/);
assert.match(hourlyProduct, /FROM current_hourly_ad_delivery/);
assert.match(hourlyProduct, /WHERE source = 'meta'/);
const dailyProduct = readProjectFile("pipes", "reports", "bill_meta_delivery_daily_product.pipe");
assert.match(dailyProduct, /FROM current_daily_ad_delivery/);
assert.match(dailyProduct, /WHERE source = 'meta'/);

const productValidation = readProjectFile("pipes", "domain", "bill_product_build_validation.pipe");
assert.match(productValidation, /expected_bill_product_signatures/);
assert.match(productValidation, /materialized_bill_product_signatures/);
assert.match(productValidation, /latest_materialized_bill_product_attempts/);
assert.match(productValidation, /product\.materialized_at = attempt\.product_materialized_at/);
assert.match(productValidation, /toString\(product_materialized_at\)/);
for (const signature of productValidation.matchAll(/toJSONString\(tuple\(([\s\S]*?)\)\) AS row_signature/g)) {
  assert.doesNotMatch(signature[1], /materialized_at/);
}

const currentBuildValidation = readProjectFile("pipes", "domain", "current_report_build_validation.pipe");
for (const stageId of [
  "hourly_performance",
  "krc_acquisition",
  "meta_delivery_daily",
  "meta_hour_bounds",
]) {
  assert.match(currentBuildValidation, new RegExp(`'${stageId}'`));
}
assert.match(currentBuildValidation, /arraySort\(groupUniqArray\(stages\.stage_id\)\) = \[/);
assert.match(currentBuildValidation, /AND count\(\) = 4/);
assert.match(currentBuildValidation, /uniqExact\(stages\.build_digest\) = 1/);

const publishedBuildStages = readProjectFile("pipes", "domain", "published_report_build_stages.pipe");
for (const pin of [
  "source_manifest_version",
  "source_manifest_digest",
  "connection_inventory_digest",
  "cutoff_set_digest",
  "policy_digest",
  "build_digest",
  "product_materialized_at",
]) {
  assert.match(publishedBuildStages, new RegExp(pin));
}
assert.match(publishedBuildStages, /published\.build_digest = stages\.build_digest/);

const reportActivation = readProjectFile("copies", "activate_report_generation.pipe");
for (const gate of [
  "current_report_activation_request",
  "current_identity_state",
  "current_session_state",
  "current_report_build_validation",
  "bill_build_readiness",
]) {
  assert.match(reportActivation, new RegExp(gate));
}
assert.match(reportActivation, /request\.version > coalesce\(published\.latest_publication_version/);
assert.match(reportActivation, /request\.build_digest = build\.build_digest/);
assert.match(reportActivation, /request\.policy_digest = build\.policy_digest/);
assert.match(reportActivation, /request\.policy_digest = readiness\.policy_digest/);
assert.match(reportActivation, /request\.connection_inventory_digest = build\.connection_inventory_digest/);
assert.match(reportActivation, /request\.cutoff_set_digest = build\.cutoff_set_digest/);
assert.match(reportActivation, /request\.cutoff_set_digest = readiness\.cutoff_set_digest/);
assert.match(reportActivation, /existing\.report_generation = request\.report_generation/);

const activationRequest = readProjectFile("datasources", "control", "report_activation_requests.datasource");
assert.match(activationRequest, /`report_generation` String/);
assert.match(activationRequest, /`build_digest` FixedString\(64\)/);
assert.match(activationRequest, /`policy_digest` FixedString\(64\)/);
assert.match(activationRequest, /`connection_inventory_digest` FixedString\(64\)/);
assert.match(activationRequest, /`cutoff_set_digest` FixedString\(64\)/);

const sourceManifest = readProjectFile("datasources", "control", "source_generation_manifests.datasource");
assert.match(sourceManifest, /`policy_digest` FixedString\(64\)/);
assert.match(sourceManifest, /`connection_inventory_digest` FixedString\(64\)/);
const reportPolicy = readProjectFile("pipes", "domain", "current_report_policy.pipe");
assert.match(reportPolicy, /hex\(SHA256\(toJSONString\(tuple\(/);
assert.match(reportPolicy, /AS policy_digest/);

const readiness = readFileSync(join(domainDirectory, "bill_build_readiness.pipe"), "utf8");
assert.match(readiness, /coverage\.is_covered = 1/);
assert.match(readiness, /coverage\.invalid_row_count = 0/);
assert.match(readiness, /coverage\.adapter_id IS NULL/);
assert.match(readiness, /policy\.policy_digest = generation\.policy_digest/);
assert.match(readiness, /cutoff_readiness\.cutoff_set_digest/);
assert.match(readiness, /coverage\.source_manifest_version = generation\.version/);
assert.match(readiness, /coverage\.source_manifest_digest = generation\.source_manifest_digest/);
assert.match(readiness, /coverage\.connection_inventory_digest = generation\.connection_inventory_digest/);
assert.match(readiness, /coverage\.cutoff_set_digest = cutoff_readiness\.cutoff_set_digest/);
const coverageRequirements = readFileSync(join(domainDirectory, "bill_adapter_coverage_requirements.pipe"), "utf8");
assert.match(coverageRequirements, /FROM registered_contract_coverage_requirements/);
const coverageChecks = readFileSync(join(adapterDirectory, "bill_adapter_coverage_checks.pipe"), "utf8");
assert.match(coverageChecks, /FROM registered_contract_coverage_checks/);
assert.match(coverageChecks, /generation\.version AS source_manifest_version/);
assert.match(coverageChecks, /generation\.source_manifest_digest/);
assert.match(coverageChecks, /generation\.connection_inventory_digest/);
assert.match(coverageChecks, /cutoff_readiness\.cutoff_set_digest/);
const coverageSchema = readProjectFile("datasources", "control", "adapter_coverage_validations.datasource");
assert.match(coverageSchema, /`source_manifest_version` UInt64/);
assert.match(coverageSchema, /`source_manifest_digest` String/);
assert.match(coverageSchema, /`connection_inventory_digest` FixedString\(64\)/);
assert.match(coverageSchema, /`cutoff_set_digest` FixedString\(64\)/);

const cutoffReadiness = readProjectFile("pipes", "domain", "source_generation_cutoff_readiness.pipe");
assert.match(cutoffReadiness, /expected_connection_inventory_digest AS connection_inventory_digest/);
assert.match(cutoffReadiness, /AS cutoff_set_digest/);
assert.match(cutoffReadiness, /selected_revision_count = 1/);
for (const filename of readdirSync(copiesDirectory)) {
  assert.ok(!filename.endsWith(".copy"), `${filename} must use the native .pipe extension`);
  if (!filename.startsWith("build_bill_")) continue;
  const copy = readFileSync(join(copiesDirectory, filename), "utf8");
  assert.match(copy, /INNER JOIN bill_build_readiness/);
  assert.match(copy, /readiness\.is_ready = 1/);
}

console.log("Bill report oracle passed: compatibility, modularity, and publication assertions.");
