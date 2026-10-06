import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";

const projectRoot = fileURLToPath(new URL("../..", import.meta.url));
const repositoryRoot = fileURLToPath(new URL("../../..", import.meta.url));

test("ActiveCampaign emits versioned live facts and tombstones from current source rows", () => {
  const adapter = readProjectFile(
    "pipes/adapters/activecampaign/activecampaign_identity_facts_adapter.pipe",
  );
  const source = readProjectFile("pipes/models/identity/source_identity_facts.pipe");

  assert.match(adapter, /contact_tag\.source_version AS assignment_source_version/);
  assert.match(adapter, /contact\.contact_updated_at AS contact_source_version/);
  assert.match(adapter, /tag\.source_version AS tag_source_version/);
  assert.match(adapter, /NODE activecampaign_tag_semantic_transitions/);
  assert.match(adapter, /semantic_hash != previous_semantic_hash/);
  assert.match(adapter, /semantic_transition\.source_version AS tag_semantic_source_version/);
  assert.doesNotMatch(
    adapter,
    /greatest\([\s\S]{0,180}ifNull\(tag\.source_version/,
  );
  assert.match(adapter, /NODE recently_changed_activecampaign_contact_ids/);
  assert.match(adapter, /max\(local_source_version\) OVER \(PARTITION BY contact_id\)/);
  assert.match(adapter, /assignment\.tag_match_type = 'fallback'/);
  assert.match(adapter, /max\([\s\S]*OVER \(PARTITION BY contact_id, registration_type\) AS has_live_primary/);
  assert.match(adapter, /assignment\.has_live_primary = 0/);
  assert.match(adapter, /toUInt8\(NOT is_live\) AS fact_deleted/);
  assert.match(adapter, /if\(is_live, email, CAST\(NULL AS Nullable\(String\)\)\)/);

  assert.match(source, /FROM activecampaign_identity_facts_adapter/);
  assert.match(source, /source_version\s*>= parseDateTime64BestEffort\(/);
  assert.doesNotMatch(
    source,
    /FROM activecampaign_registration_adapter[\s\S]*?NODE payment_fact_candidates/,
  );
});

test("Stripe status changes and related-record corrections produce newer fact versions", () => {
  const adapter = readProjectFile(
    "pipes/adapters/payments/stripe_identity_facts_adapter.pipe",
  );
  const source = readProjectFile("pipes/models/identity/source_identity_facts.pipe");

  assert.match(adapter, /FROM raw_stripe_charge/);
  assert.match(adapter, /FROM raw_stripe_kajabi_charge/);
  assert.match(adapter, /charge\.charge_source_version/);
  assert.match(adapter, /customer\.customer_source_version/);
  assert.match(adapter, /customer\.customer_is_deleted/);
  assert.match(adapter, /intent\.intent_source_version/);
  assert.match(
    adapter,
    /coalesce\(charge\.paid, false\)\s*AND ifNull\(charge\.status, ''\) = 'succeeded' AS is_live/,
  );
  assert.match(adapter, /toUInt8\(NOT is_live\) AS fact_deleted/);
  assert.match(source, /FROM stripe_identity_facts_adapter/);
  assert.doesNotMatch(
    source,
    /FROM all_stripe_payments_adapter[\s\S]*?NODE all_identity_fact_candidates/,
  );

  assert.equal(stripeFactDeleted({ paid: true, status: "succeeded" }), 0);
  assert.equal(stripeFactDeleted({ paid: false, status: "failed" }), 1);
  assert.equal(newestVersion(100, 140, 120), 140);
});

test("all Fivetran identity producers use the bounded generation overlap", () => {
  const copyPlan = readFileSync(
    `${repositoryRoot}/cloudflare-workers/bigquery-tinybird-sync/src/copy-plan.ts`,
    "utf8",
  );

  for (const producerId of [
    "source_identity:activecampaign",
    "source_identity:stripe",
    "source_identity:stripe_kajabi",
  ]) {
    assert.match(
      copyPlan,
      new RegExp(
        `producerId: "${producerId}",[\\s\\S]{0,80}usesGenerationOverlapCutoff: true`,
      ),
    );
  }
});

test("Jitsu remains append-only while the shared output forwards source tombstones", () => {
  const source = readProjectFile("pipes/models/identity/source_identity_facts.pipe");
  const jitsuSection = source.slice(
    0,
    source.indexOf("p_identity_producer == 'source_identity:activecampaign'"),
  );
  const outputSection = source.slice(source.indexOf("NODE current_source_identity_facts"));

  assert.equal((jitsuSection.match(/toUInt8\(0\) AS fact_deleted/g) ?? []).length, 5);
  assert.match(source, /\{% else %\}[\s\S]*?WHERE 0[\s\S]*?\{% end %\}/);
  assert.match(outputSection, /normalized_source_fact_version AS source_fact_version/);
  assert.match(outputSection, /\n        fact_deleted,/);
  assert.doesNotMatch(outputSection, /toUInt8\(0\) AS fact_deleted/);
});

function readProjectFile(relativePath) {
  return readFileSync(`${projectRoot}/${relativePath}`, "utf8");
}

function stripeFactDeleted({ paid, status }) {
  return Number(!(paid && status === "succeeded"));
}

function newestVersion(...versions) {
  return Math.max(...versions);
}
