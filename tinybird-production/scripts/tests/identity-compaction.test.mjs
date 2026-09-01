import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";

const projectRoot = fileURLToPath(new URL("../..", import.meta.url));

test("retired Copy compaction assets remain rollback-checkable", () => {
  const prepare = readProjectFile(
    "pipes/models/identity/identity_compaction_prepare_build.pipe",
  );
  const scope = readProjectFile(
    "pipes/models/identity/identity_compaction_scope_build.pipe",
  );
  const changes = readProjectFile(
    "pipes/models/identity/identity_compaction_changes_build.pipe",
  );
  const profiles = readProjectFile(
    "pipes/models/identity/identity_compaction_profiles_build.pipe",
  );
  const touchedProfiles = readProjectFile(
    "pipes/models/identity/identity_compaction_touched_profiles_build.pipe",
  );
  const build = readProjectFile("pipes/models/identity/identity_compaction_build.pipe");
  const components = readProjectFile(
    "pipes/models/identity/identity_compaction_components_build.pipe",
  );
  const facts = readProjectFile(
    "pipes/models/identity/identity_compaction_facts_build.pipe",
  );
  const prepareCopy = readProjectFile(
    "copies/identity/prepare_identity_compaction.pipe",
  );
  const scopeCopy = readProjectFile(
    "copies/identity/scope_identity_compaction.pipe",
  );
  const changesCopy = readProjectFile(
    "copies/identity/classify_identity_compaction_changes.pipe",
  );
  const profilesCopy = readProjectFile(
    "copies/identity/seed_identity_compaction_profiles.pipe",
  );
  const touchedProfilesCopy = readProjectFile(
    "copies/identity/freeze_identity_compaction_touched_profiles.pipe",
  );
  const componentsCopy = readProjectFile(
    "copies/identity/build_identity_compaction_components.pipe",
  );
  const factsCopy = readProjectFile(
    "copies/identity/expand_identity_compaction_facts.pipe",
  );
  const compactCopy = readProjectFile("copies/identity/compact_identity_state.pipe");
  const changesStage = readProjectFile(
    "datasources/state/identity_compaction_changes_stage.datasource",
  );
  const scopeStage = readProjectFile(
    "datasources/state/identity_compaction_scope_stage.datasource",
  );
  const componentsStage = readProjectFile(
    "datasources/state/identity_compaction_components_stage.datasource",
  );
  const queue = readProjectFile(
    "datasources/state/identity_events_cursor.datasource",
  );
  const enqueue = readProjectFile(
    "copies/identity/enqueue_identity_changes.pipe",
  );

  assert.match(prepare, /FROM identity_events_cursor AS raw/);
  assert.match(prepare, /p_cursor_ingested_at/);
  assert.match(prepare, /p_cursor_event_id/);
  assert.match(
    prepare,
    /\(raw\.ingested_at, toString\(raw\.event_id\)\)\s*> \(request\.cursor_ingested_at, request\.cursor_event_id\)/,
  );
  assert.match(prepare, /ORDER BY raw\.ingested_at, raw\.event_id/);
  assert.match(prepare, /LIMIT {{ UInt64\(p_batch_limit, 500\) }}/);
  assert.match(prepare, /'input_fact' AS stage_kind/);
  assert.doesNotMatch(prepare, /FROM identity_state_seed/);
  assert.doesNotMatch(prepare, /FROM identity_state_delta_versions/);
  assert.match(changes, /queued\.source_fact_version > stable\.source_fact_version/);
  assert.match(changes, /queued\.source_fact_version = stable\.source_fact_version/);
  assert.match(changes, /queued\.fact_deleted != stable\.fact_deleted/);
  assert.match(changes, /queued\.fact_payload_hash != stable\.fact_payload_hash/);
  assert.match(profiles, /'prior_profile' AS stage_kind/);
  assert.match(touchedProfiles, /'touched_profile' AS stage_kind/);
  assert.match(touchedProfiles, /p_touched_profile_shard_count/);
  assert.match(touchedProfiles, /p_touched_profile_shard/);
  assert.match(touchedProfiles, /cityHash64\(prepared\.prior_profile_id\)/);
  assert.match(prepare, /active_batch_version/);
  assert.match(prepare, /active_batch_id/);
  assert.equal(
    (changes.match(/FROM identity_state_delta_versions AS journal/g) ?? []).length,
    (changes.match(/changes_activated_identity_batches AS activated/g) ?? []).length + 1,
  );
  assert.equal(
    (scope.match(/FROM identity_state_delta_versions AS journal/g) ?? []).length,
    (scope.match(/scope_activated_identity_batches AS activated/g) ?? []).length + 1,
  );
  assert.equal(
    (profiles.match(/FROM identity_state_delta_versions AS journal/g) ?? []).length,
    (profiles.match(/profiles_activated_identity_batches AS activated/g) ?? []).length + 1,
  );
  assert.equal(
    (touchedProfiles.match(/FROM identity_state_delta_versions AS journal/g) ?? []).length,
    (touchedProfiles.match(/touched_profiles_activated_batches AS activated/g) ?? []).length
      + 1,
  );
  assert.equal(
    (facts.match(/FROM identity_state_delta_versions AS journal/g) ?? []).length,
    (facts.match(/facts_activated_identity_batches AS activated/g) ?? []).length + 1,
  );
  for (const stagedBuild of [changes, profiles, touchedProfiles, scope, facts]) {
    assert.match(stagedBuild, /journal\.batch_version = activated\.batch_version/);
    assert.match(stagedBuild, /journal\.batch_id = activated\.batch_id/);
    assert.doesNotMatch(stagedBuild, /journal\.batch_version <=/);
    assert.doesNotMatch(stagedBuild, /identity_state_versions/);
  }

  assert.match(scope, /prepared\.input_hash = metadata\.input_hash/);
  assert.match(scope, /journal\.state_kind = 'activation_audit'/);
  assert.match(scope, /journal\.is_deleted = 0/);
  assert.match(scope, /activated\.batch_version < activated\.active_batch_version/);
  assert.match(scope, /activated\.batch_id = activated\.active_batch_id/);
  assert.match(scope, /PARTITION BY journal\.tenant_id, journal\.batch_version/);
  assert.match(
    scope,
    /ORDER BY\s+journal\.committed_at DESC,\s+journal\.row_hash DESC,\s+journal\.batch_id DESC/,
  );
  assert.doesNotMatch(scope, /FROM activated_identity_batches AS activated/);
  assert.match(build, /FROM identity_compaction_changes_stage AS prepared/);
  assert.match(build, /FROM identity_compaction_scope_stage AS scoped/);
  assert.match(build, /FROM identity_compaction_components_stage AS components/);
  assert.doesNotMatch(build, /identity_events_cursor/);
  assert.doesNotMatch(build, /identity_state_delta_versions/);
  assert.doesNotMatch(build, /identity_state_seed/);
  assert.doesNotMatch(build, /activation_audit/);

  for (const [copy, target] of [
    [prepareCopy, "identity_compaction_changes_stage"],
    [changesCopy, "identity_compaction_changes_stage"],
    [profilesCopy, "identity_compaction_changes_stage"],
    [touchedProfilesCopy, "identity_compaction_scope_stage"],
    [scopeCopy, "identity_compaction_scope_stage"],
    [factsCopy, "identity_compaction_scope_stage"],
    [componentsCopy, "identity_compaction_components_stage"],
    [compactCopy, "identity_state_delta_versions"],
  ]) {
    assert.match(copy, new RegExp(`TARGET_DATASOURCE ${target}`));
    assert.match(copy, /COPY_MODE append/);
    assert.doesNotMatch(copy, /COPY_SCHEDULE/);
  }

  for (const datasource of [changesStage, scopeStage, componentsStage]) {
    assert.match(datasource, /ENGINE MergeTree/);
    assert.match(
      datasource,
      /ENGINE_SORTING_KEY tenant_id, batch_version, batch_id, stage_kind, stage_key, staged_at, stage_hash/,
    );
  }
  assert.match(changesStage, /`input_hash` FixedString\(64\)/);
  assert.match(changesStage, /`active_batch_version` UInt64/);
  assert.match(changesStage, /`active_batch_id` String/);
  assert.match(scopeStage, /`input_hash` FixedString\(64\)/);
  assert.match(componentsStage, /`component_label` UInt64/);

  assert.equal(
    [prepare, changes, profiles, touchedProfiles, scope, facts, components, build].filter((contents) =>
      /p_cursor_ingested_at/.test(contents)
    ).length,
    1,
  );
  for (const stagedBuild of [
    prepare,
    changes,
    profiles,
    touchedProfiles,
    scope,
    facts,
    components,
    build,
  ]) {
    assert.equal((stagedBuild.match(/^NODE /gm) ?? []).length, 1);
    assert.match(stagedBuild, /\n    WITH\n/);
  }
  assert.match(
    prepare,
    /if\(summary\.tenant_id = '', toUInt64\(0\), summary\.input_event_count\)/,
  );

  assert.match(queue, /ENGINE_PARTITION_KEY toYYYYMM\(ingested_at\)/);
  assert.match(
    queue,
    /ENGINE_SORTING_KEY tenant_id, ingested_at, event_id, producer_id, fact_kind, fact_key/,
  );
  assert.match(enqueue, /FROM identity_events_cursor/);
  assert.match(enqueue, /TARGET_DATASOURCE identity_events_cursor/);
  assert.doesNotMatch(enqueue, /identity_events_raw/);
});

test("Worker identity reads use literal keys and a batch-keyed validation index", () => {
  const pending = readProjectFile("endpoints/identity_worker_pending_facts.pipe");
  const facts = readProjectFile("endpoints/identity_worker_fact_heads.pipe");
  const mappings = readProjectFile("endpoints/identity_worker_mapping_heads.pipe");
  const profiles = readProjectFile("endpoints/identity_worker_profile_heads.pipe");
  const evidence = readProjectFile("endpoints/identity_worker_evidence_heads.pipe");
  const written = readProjectFile("endpoints/identity_worker_written_rows.pipe");
  const batchIndex = readProjectFile(
    "datasources/state/identity_batch_output_rows.datasource",
  );
  const manifest = readProjectFile("endpoints/identity_compaction_manifest.pipe");

  assert.match(pending, /FROM identity_events_cursor/);
  assert.match(pending, /ORDER BY ingested_at, event_id/);
  assert.match(pending, /LIMIT {{ UInt64\(p_batch_limit, 1500\) }}/);

  assert.match(facts, /state_key IN {{ Array\(p_state_keys, 'String'\) }}/);
  assert.match(facts, /lookup_key IN {{ Array\(p_fact_keys, 'String'\) }}/);
  assert.match(facts, /JSONExtractString\(fact_payload, 'first_name'\)/);
  assert.doesNotMatch(facts.split("TYPE ENDPOINT")[0].split("SELECT\n        fact_kind").at(-1), /fact_payload,/);
  assert.match(mappings, /lookup_key IN {{ Array\(p_identifier_keys, 'String'\) }}/);
  assert.match(profiles, /lookup_key IN {{ Array\(p_profile_ids, 'String'\) }}/);
  assert.match(evidence, /identifier_key IN {{ Array\(p_identifier_keys, 'String'\) }}/);
  assert.match(evidence, /journal\.lookup_key IN {{ Array\(p_identifier_keys, 'String'\) }}/);
  assert.match(written, /lookup_key IN {{ Array\(p_lookup_keys, 'String'\) }}/);

  assert.match(
    batchIndex,
    /ENGINE_SORTING_KEY tenant_id, batch_version, batch_id, state_kind, lookup_key, sub_key, row_hash/,
  );
  assert.match(manifest, /FROM identity_batch_output_rows/);
  assert.doesNotMatch(
    manifest.slice(manifest.indexOf("NODE distinct_identity_compaction_output_rows")),
    /FROM identity_state_delta_versions/,
  );
});

test("affected scope and component members are frozen before final serialization", () => {
  const factKeys = readProjectFile(
    "pipes/models/identity/identity_compaction_fact_keys_build.pipe",
  );
  const touchedProfiles = readProjectFile(
    "pipes/models/identity/identity_compaction_touched_profiles_build.pipe",
  );
  const scope = readProjectFile(
    "pipes/models/identity/identity_compaction_scope_build.pipe",
  );
  const build = readProjectFile(
    "pipes/models/identity/identity_compaction_build.pipe",
  );
  const components = readProjectFile(
    "pipes/models/identity/identity_compaction_components_build.pipe",
  );
  const facts = readProjectFile(
    "pipes/models/identity/identity_compaction_facts_build.pipe",
  );

  assert.match(scope, /stable\.stable_evidence_keys/);
  assert.match(scope, /incoming\.evidence_keys/);
  assert.match(scope, /FROM selected_frozen_touched_profiles AS profiles/);
  assert.match(scope, /profiles\.member_identifier_keys/);
  assert.doesNotMatch(scope, /FROM identity_fact_evidence_seed AS base/);
  assert.doesNotMatch(scope, /journal\.state_kind = 'evidence'/);
  assert.doesNotMatch(scope, /'retained_fact' AS stage_kind/);
  assert.match(factKeys, /FROM identity_fact_evidence_seed AS base/);
  assert.match(factKeys, /journal\.state_kind = 'evidence'/);
  assert.match(scope, /IN \(SELECT tenant_id, identifier_key FROM affected_identity_identifier_keys\)/);
  assert.match(facts, /FROM identity_compaction_fact_keys_stage AS staged/);
  assert.doesNotMatch(scope, /identity_mapping_evidence_seed/);
  assert.match(scope, /'scope_metadata' AS stage_kind/);
  assert.match(scope, /'current_mapping' AS stage_kind/);
  assert.doesNotMatch(scope, /'touched_profile' AS stage_kind/);
  assert.match(touchedProfiles, /'touched_profile' AS stage_kind/);
  assert.match(factKeys, /JOIN scoped\.member_identifier_keys AS identifier_key/);
  assert.match(factKeys, /WHERE stage_kind = 'current_mapping'/);
  assert.match(facts, /'facts_metadata' AS stage_kind/);
  assert.match(facts, /'retained_fact' AS stage_kind/);

  assert.match(components, /WHERE stage_kind = 'retained_fact'/);
  assert.match(components, /arrayFold\(/);
  assert.match(components, /'component_metadata' AS stage_kind/);
  assert.match(components, /'component_member' AS stage_kind/);
  assert.match(components, /FROM staged_identity_component_members AS members/);

  assert.match(build, /WHERE stage_kind = 'current_mapping'/);
  assert.match(build, /WHERE stage_kind = 'touched_profile'/);
  assert.match(build, /WHERE stage_kind = 'component_member'/);
  assert.doesNotMatch(build, /arrayFold\(/);
  assert.doesNotMatch(build, /retained_identity_facts_after_overlay/);
  assert.match(build, /FROM changed_evidence_delta_values AS evidence/);
  assert.match(build, /FROM recomputed_mapping_delta_values AS mappings/);
  assert.match(build, /FROM displaced_profile_delta_values AS profiles/);

  const componentRollup = build.slice(
    build.indexOf("affected_identity_component_rollups AS"),
    build.indexOf("affected_component_history AS"),
  );
  assert.match(componentRollup, /FROM affected_identity_component_members AS members/);
  assert.match(componentRollup, /min\(members\.first_seen_at\) AS first_seen_at/);
  assert.match(componentRollup, /max\(members\.last_seen_at\) AS last_seen_at/);
  assert.match(componentRollup, /members\.first_name/);
  assert.match(componentRollup, /members\.last_name/);

  assert.doesNotMatch(build, /FROM current_identity_facts/);
  assert.doesNotMatch(build, /FROM current_identity_mappings/);
  assert.doesNotMatch(build, /FROM current_identity_profiles/);
});

test("bounded scope and fact seed reads use the physical state sorting key", () => {
  const touchedProfiles = readProjectFile(
    "pipes/models/identity/identity_compaction_touched_profiles_build.pipe",
  );
  const scope = readProjectFile(
    "pipes/models/identity/identity_compaction_scope_build.pipe",
  );
  const facts = readProjectFile(
    "pipes/models/identity/identity_compaction_facts_build.pipe",
  );

  assert.match(
    touchedProfiles,
    /concat\('profile:', toString\(length\(profile_id\)\), ':', profile_id\) AS state_key/,
  );
  assert.match(
    scope,
    /concat\(\s*'mapping:', toString\(length\(identifier_key\)\), ':', identifier_key\s*\) AS state_key/,
  );
  assert.equal(
    (scope.match(/AND \(seed\.tenant_id, seed\.state_key\)/g) ?? []).length,
    1,
  );
  assert.equal(
    (touchedProfiles.match(/AND \(seed\.tenant_id, seed\.state_key\)/g) ?? []).length,
    1,
  );
  assert.match(
    touchedProfiles,
    /IN \(SELECT tenant_id, state_key FROM selected_prior_profile_state_keys\)/,
  );
  assert.match(
    scope,
    /IN \(SELECT tenant_id, state_key FROM affected_identity_mapping_state_keys\)/,
  );
  assert.doesNotMatch(scope, /\(seed\.tenant_id, seed\.profile_id\)/);
  assert.doesNotMatch(scope, /\(seed\.tenant_id, seed\.identifier_key\)/);

  assert.match(facts, /SELECT tenant_id, concat\('fact:', fact_key\) AS state_key/);
  assert.match(facts, /AND \(seed\.tenant_id, seed\.state_key\)/);
  assert.match(
    facts,
    /IN \(SELECT tenant_id, state_key FROM retained_identity_fact_state_keys\)/,
  );
  assert.doesNotMatch(facts, /\(seed\.tenant_id, seed\.fact_key\)/);
});

test("classification Copies read seed facts and mappings through the sorted state key", () => {
  const prepare = readProjectFile(
    "pipes/models/identity/identity_compaction_prepare_build.pipe",
  );
  const changes = readProjectFile(
    "pipes/models/identity/identity_compaction_changes_build.pipe",
  );
  const profiles = readProjectFile(
    "pipes/models/identity/identity_compaction_profiles_build.pipe",
  );
  const seed = readProjectFile(
    "datasources/seed/identity_state_seed_enriched.datasource",
  );

  assert.match(
    seed,
    /ENGINE_SORTING_KEY tenant_id, state_kind, state_key, batch_version, committed_at, row_hash/,
  );
  assert.doesNotMatch(prepare, /FROM identity_state_seed/);
  assert.match(changes, /concat\('fact:', fact_key\) AS state_key/);
  assert.match(
    changes,
    /\(seed\.tenant_id, seed\.state_key\)\s+IN \(SELECT tenant_id, state_key FROM selected_input_fact_keys\)/,
  );
  assert.match(
    profiles,
    /concat\(\s*'mapping:', toString\(length\(identifier_key\)\), ':', identifier_key\s*\) AS state_key/,
  );
  assert.match(
    profiles,
    /\(seed\.tenant_id, seed\.state_key\)\s+IN \(SELECT tenant_id, state_key FROM changed_identity_identifier_keys\)/,
  );
  assert.doesNotMatch(changes, /\(seed\.tenant_id, seed\.fact_kind, seed\.fact_key\)/);
  assert.doesNotMatch(profiles, /\(seed\.tenant_id, seed\.identifier_key\)/);
});

test("staging retries and empty batches fail closed", () => {
  const scope = readProjectFile(
    "pipes/models/identity/identity_compaction_scope_build.pipe",
  );
  const components = readProjectFile(
    "pipes/models/identity/identity_compaction_components_build.pipe",
  );
  const facts = readProjectFile(
    "pipes/models/identity/identity_compaction_facts_build.pipe",
  );
  const build = readProjectFile("pipes/models/identity/identity_compaction_build.pipe");

  assert.match(scope, /PARTITION BY prepared\.tenant_id, prepared\.stage_kind, prepared\.stage_key/);
  assert.match(scope, /ORDER BY prepared\.staged_at, prepared\.stage_hash/);
  assert.match(facts, /WHERE scoped\.stage_kind = 'scope_metadata'/);
  assert.match(facts, /scoped\.input_hash = metadata\.input_hash/);
  assert.match(facts, /'facts_metadata' AS stage_kind/);
  assert.match(
    facts,
    /PARTITION BY scoped\.tenant_id, scoped\.stage_kind, scoped\.stage_key/,
  );
  assert.match(components, /WHERE scoped\.stage_kind = 'scope_metadata'/);
  assert.match(components, /WHERE scoped\.stage_kind = 'facts_metadata'/);
  assert.match(components, /scoped\.input_hash = metadata\.input_hash/);
  assert.match(components, /'component_metadata' AS stage_kind/);
  assert.match(
    components,
    /PARTITION BY scoped\.tenant_id, scoped\.stage_kind, scoped\.stage_key/,
  );
  assert.match(build, /WHERE scoped\.stage_kind = 'scope_metadata'/);
  assert.match(build, /scoped\.input_hash = metadata\.input_hash/);
  assert.match(build, /PARTITION BY scoped\.tenant_id, scoped\.stage_kind, scoped\.stage_key/);
  assert.match(build, /WHERE components\.stage_kind = 'component_metadata'/);
  assert.match(build, /components\.input_hash = metadata\.input_hash/);
  assert.match(
    build,
    /PARTITION BY components\.tenant_id, components\.stage_kind, components\.stage_key/,
  );
  assert.match(build, /__manifest_placeholder/);
  assert.match(build, /countIf\(rows\.state_kind != '__manifest_placeholder'\) OVER \(\s*\)/);
  assert.match(build, /groupArrayIf\(/);
  assert.equal((build.match(/FROM identity_compaction_delta_rows/g) ?? []).length, 1);

  const attempts = [
    { kind: "metadata", key: "metadata", inputHash: "first", stagedAt: 1 },
    { kind: "changed_fact", key: "fact:a", inputHash: "first", stagedAt: 1 },
    { kind: "metadata", key: "metadata", inputHash: "second", stagedAt: 2 },
    { kind: "changed_fact", key: "fact:b", inputHash: "second", stagedAt: 2 },
  ];
  assert.deepEqual(selectPreparedAttempt(attempts), [
    { kind: "changed_fact", key: "fact:a", inputHash: "first", stagedAt: 1 },
    { kind: "metadata", key: "metadata", inputHash: "first", stagedAt: 1 },
  ]);
});

test("manifest and cursor endpoints expose the coordinator contract", () => {
  const datasource = readProjectFile(
    "datasources/state/identity_state_delta_versions.datasource",
  );
  const manifest = readProjectFile("endpoints/identity_compaction_manifest.pipe");
  const cursor = readProjectFile(
    "endpoints/current_identity_compaction_cursor.pipe",
  );

  for (const field of [
    "checkpoint_ingested_at",
    "checkpoint_event_id",
    "output_row_count",
    "output_hash",
  ]) {
    assert.match(datasource, new RegExp(`\\b${field}\\b`));
  }

  assert.match(manifest, /String\(p_tenant_id, 'boom'\)/);
  assert.match(manifest, /UInt64\(p_batch_version, 0\)/);
  assert.match(manifest, /String\(p_batch_id, ''\)/);
  assert.match(manifest, /expected_output_row_count/);
  assert.match(manifest, /actual_output_row_count/);
  assert.match(manifest, /AS is_valid/);
  assert.match(manifest, /SELECT DISTINCT state_kind, lookup_key, sub_key, row_hash/);
  assert.match(manifest, /FROM identity_batch_output_rows/);
  assert.match(manifest, /TYPE ENDPOINT/);

  assert.match(cursor, /String\(p_tenant_id, 'boom'\)/);
  assert.match(cursor, /active_batch_version/);
  assert.match(cursor, /active_batch_id/);
  assert.match(cursor, /checkpoint_ingested_at/);
  assert.match(cursor, /checkpoint_event_id/);
  assert.match(cursor, /FROM activated_identity_batches AS activated/);
  assert.match(cursor, /FROM requested_activated_identity_batches AS activated/);
  assert.doesNotMatch(cursor, /FROM identity_state_delta_versions/);
  assert.doesNotMatch(cursor, /current_identity_batch_versions/);
  assert.match(cursor, /TYPE ENDPOINT/);
});

test("request-time resolution is a small activated-state lookup", () => {
  const resolver = readProjectFile("endpoints/resolve_identity.pipe");

  assert.ok(Buffer.byteLength(resolver) < 64 * 1024);
  assert.match(resolver, /FROM identity_state_seed_enriched AS seed/);
  assert.match(resolver, /FROM identity_state_delta_versions AS journal/);
  assert.match(resolver, /NODE resolver_activated_identity_batches/);
  assert.match(resolver, /resolver_activated_identity_batches AS activated/);
  assert.match(resolver, /journal\.batch_version = activated\.batch_version/);
  assert.match(resolver, /journal\.batch_id = activated\.batch_id/);
  assert.doesNotMatch(resolver, /journal\.batch_version <=/);
  assert.doesNotMatch(resolver, /current_identity_batch_versions/);
  assert.equal(
    (resolver.match(/FROM identity_state_delta_versions AS journal/g) ?? []).length,
    (resolver.match(/INNER JOIN\s+resolver_activated_identity_batches AS activated/g) ?? []).length,
  );
  assert.match(resolver, /requested_identity_identifier_candidates/);
  assert.match(resolver, /AS mapping_state_key/);
  assert.match(
    resolver,
    /NODE candidate_requested_mapping_versions\s+SQL >\s+%\s+SELECT/,
  );
  assert.match(
    resolver,
    /seed\.tenant_id = {{ String\(p_tenant_id, 'boom'\) }}[\s\S]{0,120}seed\.state_kind = 'mapping'[\s\S]{0,120}seed\.state_key IN \(\s*SELECT mapping_state_key/,
  );
  assert.match(resolver, /AS profile_state_key/);
  assert.match(
    resolver,
    /NODE candidate_requested_profile_versions\s+SQL >\s+%\s+SELECT/,
  );
  assert.match(
    resolver,
    /seed\.tenant_id = {{ String\(p_tenant_id, 'boom'\) }}[\s\S]{0,120}seed\.state_kind = 'profile'[\s\S]{0,120}seed\.state_key IN \(\s*SELECT profile_state_key/,
  );
  assert.match(
    resolver,
    /journal\.tenant_id = {{ String\(p_tenant_id, 'boom'\) }}[\s\S]{0,120}journal\.state_kind = 'mapping'[\s\S]{0,160}journal\.lookup_key IN \(\s*SELECT identifier_key/,
  );
  assert.match(
    resolver,
    /journal\.tenant_id = {{ String\(p_tenant_id, 'boom'\) }}[\s\S]{0,120}journal\.state_kind = 'profile'[\s\S]{0,160}journal\.lookup_key IN \(SELECT profile_id/,
  );
  assert.equal((resolver.match(/journal\.sub_key = ''/g) ?? []).length, 2);
  assert.doesNotMatch(resolver, /journal\.state_key\s+IN/);
  assert.doesNotMatch(resolver, /\(seed\.tenant_id, seed\.state_key\)/);
  assert.doesNotMatch(resolver, /\(journal\.tenant_id, journal\.lookup_key\)/);
  assert.doesNotMatch(resolver, /seed\.identifier_key\)\s+IN/);
  assert.doesNotMatch(resolver, /seed\.profile_id\)\s+IN/);
  assert.doesNotMatch(resolver, /lengthUTF8/);
  assert.doesNotMatch(resolver, /identity_live_pending_snapshot_current/);
  assert.doesNotMatch(resolver, /live_pending/);
  assert.doesNotMatch(resolver, /identity_rebuild/);
  assert.doesNotMatch(resolver, /FROM current_identity_mappings/);
  assert.doesNotMatch(resolver, /FROM current_identity_profiles/);
});

test("resolver state keys use the seed's byte-length encoding", () => {
  assert.equal(
    identityStateKey("mapping", "canonical_email:josé@example.com"),
    "mapping:33:canonical_email:josé@example.com",
  );
  assert.equal(
    identityStateKey("profile", "0123456789abcdef0123456789abcdef"),
    "profile:32:0123456789abcdef0123456789abcdef",
  );
});

test("same-version corrections apply only when deletion state or payload changes", () => {
  const stable = { version: 10, deleted: 0, hash: "A" };

  assert.equal(shouldApplyFact({ version: 11, deleted: 0, hash: "A" }, stable), true);
  assert.equal(shouldApplyFact({ version: 10, deleted: 1, hash: "A" }, stable), true);
  assert.equal(shouldApplyFact({ version: 10, deleted: 0, hash: "B" }, stable), true);
  assert.equal(shouldApplyFact({ version: 10, deleted: 0, hash: "A" }, stable), false);
  assert.equal(shouldApplyFact({ version: 9, deleted: 1, hash: "B" }, stable), false);
});

test("scope ranks exact delta activation markers before applying the pinned bound", () => {
  const markers = [
    { version: 2, id: "older", committedAt: 1, hash: "a" },
    { version: 2, id: "winner", committedAt: 2, hash: "b" },
    { version: 3, id: "pinned", committedAt: 3, hash: "c" },
    { version: 4, id: "future", committedAt: 4, hash: "d" },
  ];

  assert.deepEqual(selectPinnedDeltaActivations(markers, 3, "pinned"), [
    { version: 2, id: "winner", committedAt: 2, hash: "b" },
    { version: 3, id: "pinned", committedAt: 3, hash: "c" },
  ]);

  const conflictingMarkers = [
    ...markers,
    { version: 3, id: "new-winner", committedAt: 5, hash: "e" },
  ];
  assert.deepEqual(selectPinnedDeltaActivations(conflictingMarkers, 3, "pinned"), [
    { version: 2, id: "winner", committedAt: 2, hash: "b" },
  ]);
});

test("source event identity includes fact deletion in the canonical payload and hash", () => {
  const source = readProjectFile("pipes/models/identity/source_identity_facts.pipe");
  const canonical = source.slice(
    source.indexOf("NODE identity_fact_payload_values"),
    source.indexOf("NODE current_source_identity_facts"),
  );
  const output = source.slice(source.indexOf("NODE current_source_identity_facts"));

  assert.match(canonical, /toString\(length\(toString\(fact_deleted\)\)\)/);
  assert.match(canonical, /toString\(fact_deleted\)/);
  assert.match(
    output,
    /toString\(normalized_source_fact_version\)[\s\S]{0,120}toString\(fact_deleted\)[\s\S]{0,120}SHA256\(canonical_payload\)/,
  );
  assert.match(output, /'fact_deleted',[\s\S]{0,60}toString\(fact_deleted\)/);
});

test("deleting the only bridge splits the old profile exactly", () => {
  const stableFacts = [
    fact("left", ["email:a", "user_id:b"]),
    fact("bridge", ["user_id:b", "phone:c"]),
    fact("right", ["phone:c", "anonymous_id:d"]),
  ];
  const profile = {
    id: "old",
    members: ["email:a", "user_id:b", "phone:c", "anonymous_id:d"],
  };
  const mappings = Object.fromEntries(profile.members.map((key) => [key, "old"]));
  const changes = [fact("bridge", [], true, 2)];

  assert.deepEqual(
    recomputeAffectedComponents(stableFacts, mappings, [profile], changes),
    [
      ["anonymous_id:d", "phone:c"],
      ["email:a", "user_id:b"],
    ],
  );
});

test("one update can split its old profile and join another profile", () => {
  const stableFacts = [
    fact("left", ["email:a", "user_id:b"]),
    fact("moving", ["user_id:b", "phone:c"]),
    fact("other", ["anonymous_id:x", "email:y"]),
  ];
  const profiles = [
    { id: "one", members: ["email:a", "user_id:b", "phone:c"] },
    { id: "two", members: ["anonymous_id:x", "email:y"] },
  ];
  const mappings = {
    "email:a": "one",
    "user_id:b": "one",
    "phone:c": "one",
    "anonymous_id:x": "two",
    "email:y": "two",
  };
  const changes = [fact("moving", ["phone:c", "anonymous_id:x"], false, 2)];

  assert.deepEqual(
    recomputeAffectedComponents(stableFacts, mappings, profiles, changes),
    [
      ["anonymous_id:x", "email:y", "phone:c"],
      ["email:a", "user_id:b"],
    ],
  );
});

test("manifest validation ignores exact retry duplicates but rejects conflicts", () => {
  const rows = [
    { kind: "fact", lookup: "a", sub: "", hash: "1" },
    { kind: "mapping", lookup: "email:a", sub: "", hash: "2" },
  ];
  const expected = summarize(rows);

  assert.deepEqual(summarize([...rows, ...rows]), expected);
  assert.notDeepEqual(
    summarize([...rows, { kind: "fact", lookup: "a", sub: "", hash: "changed" }]),
    expected,
  );
});

function fact(key, evidence, deleted = false, version = 1) {
  return { key, evidence, deleted, version };
}

function shouldApplyFact(incoming, stable) {
  if (incoming.version > stable.version) return true;
  if (incoming.version !== stable.version) return false;
  return incoming.deleted !== stable.deleted || incoming.hash !== stable.hash;
}

function identityStateKey(kind, value) {
  return `${kind}:${Buffer.byteLength(value, "utf8")}:${value}`;
}

function selectPinnedDeltaActivations(markers, activeVersion, activeId) {
  const winnerByVersion = new Map();

  for (const marker of markers) {
    const current = winnerByVersion.get(marker.version);
    if (!current || compareActivationMarkers(marker, current) < 0) {
      winnerByVersion.set(marker.version, marker);
    }
  }

  return [...winnerByVersion.values()]
    .filter((marker) =>
      marker.version < activeVersion
      || (marker.version === activeVersion && marker.id === activeId)
    )
    .sort((left, right) => left.version - right.version);
}

function compareActivationMarkers(left, right) {
  if (left.committedAt !== right.committedAt) return right.committedAt - left.committedAt;
  if (left.hash !== right.hash) return right.hash.localeCompare(left.hash);
  return right.id.localeCompare(left.id);
}

function recomputeAffectedComponents(stableFacts, mappings, profiles, changes) {
  const stableByKey = new Map(stableFacts.map((row) => [row.key, row]));
  const seedIdentifiers = new Set();

  for (const change of changes) {
    for (const identifier of stableByKey.get(change.key)?.evidence ?? []) {
      seedIdentifiers.add(identifier);
    }
    if (!change.deleted) {
      for (const identifier of change.evidence) seedIdentifiers.add(identifier);
    }
  }

  const touchedProfileIds = new Set(
    [...seedIdentifiers].map((key) => mappings[key]).filter(Boolean),
  );
  const affectedIdentifiers = new Set(seedIdentifiers);
  for (const profile of profiles) {
    if (!touchedProfileIds.has(profile.id)) continue;
    for (const identifier of profile.members) affectedIdentifiers.add(identifier);
  }

  const retainedKeys = new Set(changes.map((row) => row.key));
  for (const stable of stableFacts) {
    if (stable.evidence.some((identifier) => affectedIdentifiers.has(identifier))) {
      retainedKeys.add(stable.key);
    }
  }

  const overlay = new Map(
    stableFacts.filter((row) => retainedKeys.has(row.key)).map((row) => [row.key, row]),
  );
  for (const change of changes) overlay.set(change.key, change);

  const parent = new Map();
  const find = (key) => {
    const current = parent.get(key);
    if (current === key) return key;
    const root = find(current);
    parent.set(key, root);
    return root;
  };
  const union = (left, right) => {
    const leftRoot = find(left);
    const rightRoot = find(right);
    if (leftRoot !== rightRoot) parent.set(rightRoot, leftRoot);
  };

  for (const current of overlay.values()) {
    if (current.deleted || current.evidence.length === 0) continue;
    for (const identifier of current.evidence) {
      if (!parent.has(identifier)) parent.set(identifier, identifier);
      union(current.evidence[0], identifier);
    }
  }

  const components = new Map();
  for (const identifier of parent.keys()) {
    const root = find(identifier);
    const members = components.get(root) ?? [];
    members.push(identifier);
    components.set(root, members);
  }

  return [...components.values()]
    .map((members) => members.sort())
    .sort((left, right) => left.join("|").localeCompare(right.join("|")));
}

function selectPreparedAttempt(rows) {
  const metadata = rows
    .filter((row) => row.kind === "metadata")
    .sort((left, right) => left.stagedAt - right.stagedAt)[0];
  const selected = new Map();

  for (const row of rows) {
    if (row.inputHash !== metadata.inputHash) continue;
    const key = `${row.kind}:${row.key}`;
    const current = selected.get(key);
    if (!current || row.stagedAt < current.stagedAt) selected.set(key, row);
  }

  return [...selected.values()].sort((left, right) =>
    `${left.kind}:${left.key}`.localeCompare(`${right.kind}:${right.key}`),
  );
}

function summarize(rows) {
  const distinct = new Set(
    rows.map((row) => [row.kind, row.lookup, row.sub, row.hash].join(":")),
  );
  const values = [...distinct].sort();
  return { count: values.length, values };
}

function readProjectFile(relativePath) {
  return readFileSync(`${projectRoot}/${relativePath}`, "utf8");
}
