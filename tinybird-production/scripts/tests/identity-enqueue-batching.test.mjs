import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";

const projectRoot = fileURLToPath(new URL("../..", import.meta.url));

const jitsuAdapters = [
  ["jitsu_identifies.pipe", "source_identity:segment_identify"],
  ["jitsu_form_submissions.pipe", "source_identity:segment_form"],
  ["jitsu_order_completed.pipe", "source_identity:segment_order_completed"],
  ["jitsu_page_views.pipe", "source_identity:segment_page_view"],
  ["jitsu_attribution_params.pipe", "source_identity:segment_attribution"],
];

test("each Jitsu identity producer prunes recently ingested keys before ranking", () => {
  for (const [fileName, producerId] of jitsuAdapters) {
    const contents = readProjectFile(`pipes/adapters/jitsu/${fileName}`);

    assert.match(contents, /NODE (?:recently_ingested|recent)_[a-z_]+_keys/);
    assert.match(contents, /source_ingested_at\s*>?= toDateTime64\(/);
    assert.match(contents, /String\(p_source_ingested_from,/);
    assert.ok(contents.includes(`= '${producerId}'`));
    assert.match(
      contents,
      /IN \(\s*SELECT tenant_id, __tb_state_key FROM (?:recently_ingested|recent)_/,
    );
    assert.match(contents, /String\(p_identity_producer, 'all'\) }} = 'all'/);

    const publicOutput = contents.slice(contents.lastIndexOf("NODE current_"));
    assert.doesNotMatch(publicOutput, /source_ingested_at|__tb_state_key/);
  }
});

test("the reusable enqueue Copy prunes the seed and activated delta to candidate fact keys", () => {
  const contents = readProjectFile("copies/identity/enqueue_identity_changes.pipe");

  assert.match(
    contents,
    /WHERE producer_id = {{ String\(p_identity_producer, ''\) }}/,
  );
  assert.match(contents, /FROM source_identity_facts/);
  assert.match(contents, /bigquery_identity_seed:/);
  assert.match(contents, /FROM identity_state_seed_enriched AS seed/);
  assert.match(contents, /FROM identity_state_delta_versions AS journal/);
  assert.match(contents, /journal\.state_kind = 'fact'/);
  assert.match(contents, /activated_identity_batches AS activated/);
  assert.match(contents, /journal\.batch_version = activated\.batch_version/);
  assert.match(contents, /journal\.batch_id = activated\.batch_id/);
  assert.doesNotMatch(contents, /journal\.batch_version <=/);
  assert.match(
    contents,
    /\(journal\.tenant_id, journal\.lookup_key\)\s*IN \(SELECT tenant_id, fact_key FROM candidate_identity_fact_keys\)/,
    "the delta read must be restricted to the current producer's candidate fact keys",
  );
  assert.match(contents, /concat\('fact:', fact_key\) AS state_key/);
  assert.match(
    contents,
    /\(seed\.tenant_id, seed\.state_key\)\s*IN \(SELECT tenant_id, state_key FROM candidate_identity_fact_keys\)/,
    "the flat seed read must use its sorted state key for the same candidate facts",
  );
  assert.doesNotMatch(contents, /identity_state_versions/);
  assert.doesNotMatch(contents, /FROM current_identity_facts/);
  assert.doesNotMatch(contents, /FROM current_identity_state/);
  assert.doesNotMatch(contents, /SELECT source\.\*/);
  assert.match(contents, /source\.tenant_id AS tenant_id/);
  assert.match(contents, /source\.evidence_keys AS evidence_keys/);
  assert.match(
    contents,
    /argMax\(\s*event_id,\s*tuple\(source_fact_version, ingested_at, event_id\)/,
    "retries must compare with only the latest queued version of each fact",
  );
  assert.match(contents, /source\.fact_key = queued\.fact_key/);
  assert.match(contents, /queued\.event_id != source\.event_id/);
  assert.match(contents, /tenant_id = 'boom'/);
  assert.match(
    contents,
    /\(tenant_id, fact_kind, fact_key\)\s*IN \(SELECT tenant_id, fact_kind, fact_key FROM candidate_identity_fact_keys/,
    "queued history must be pruned to the current producer's candidate fact keys",
  );
  assert.doesNotMatch(contents, /GROUP BY tenant_id, event_id/);
  assert.doesNotMatch(contents, /COPY_SCHEDULE/);
  assert.match(contents, /COPY_MODE append/);
});

test("stable and queued comparisons preserve retries and A-to-B-to-A", () => {
  assert.equal(shouldEnqueue("A", "A", []), false);
  assert.equal(shouldEnqueue("B", "A", []), true);

  const queuedB = [{ eventId: "B", sourceVersion: 10, ingestedAt: 20 }];
  assert.equal(shouldEnqueue("B", "A", queuedB), false);
  assert.equal(shouldEnqueue("A", "A", queuedB), true);
  assert.equal(shouldEnqueue("A", "B", queuedB), true);

  const queuedA = [
    ...queuedB,
    { eventId: "A", sourceVersion: 10, ingestedAt: 30 },
  ];
  assert.equal(shouldEnqueue("A", "A", queuedA), false);
});

test("the shared source renders exactly one requested producer", () => {
  const contents = readProjectFile("pipes/models/identity/source_identity_facts.pipe");
  const producerIds = [
    "source_identity:segment_identify",
    "source_identity:segment_form",
    "source_identity:segment_order_completed",
    "source_identity:segment_page_view",
    "source_identity:segment_attribution",
    "source_identity:activecampaign",
    "source_identity:stripe",
    "source_identity:stripe_kajabi",
  ];

  assert.match(contents, /NODE selected_identity_fact_candidates/);
  assert.match(contents, /\{% if defined\(p_identity_producer\)/);
  assert.match(contents, /\{% elif defined\(/);
  assert.match(contents, /\{% else %\}[\s\S]*?WHERE 0[\s\S]*?\{% end %\}/);
  assert.doesNotMatch(contents, /String\(p_identity_producer, 'all'\)/);
  assert.doesNotMatch(contents, /NODE all_identity_fact_candidates|UNION ALL/);
  for (const producerId of producerIds) assert.ok(contents.includes(producerId));
  assert.match(
    contents,
    /concat\(\s*fact_key,\s*':',\s*toString\(normalized_source_fact_version\),/,
    "event_id must distinguish A-to-B-to-A corrections by source version",
  );
});

function readProjectFile(relativePath) {
  return readFileSync(`${projectRoot}/${relativePath}`, "utf8");
}

function shouldEnqueue(sourceEventId, stableEventId, queuedVersions) {
  const latest = [...queuedVersions].sort((left, right) => (
    right.sourceVersion - left.sourceVersion
    || right.ingestedAt - left.ingestedAt
    || right.eventId.localeCompare(left.eventId)
  ))[0];

  if (latest) return latest.eventId !== sourceEventId;
  return stableEventId !== sourceEventId;
}
