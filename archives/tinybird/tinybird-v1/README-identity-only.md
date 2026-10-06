Identity-only initializer
=========================

Run `scripts/bootstrap-identity.ts <plan> <proof> <build-manifest>` in one supervised Node 22 process. Root owns packaging, schema deployment, job execution and activation. This unit made no provider calls.

The new files are listed with SHA-256 in `identity-only-handoff.json`. Copy only that allowlist. In particular, do not copy the isolated `worker/bootstrap/executor.ts` type shim or overwrite shared identity storage/runtime files.

Production input gate
---------------------

Generate the plan with `identityBootstrapPlan(inputs, baselineId, nativeImportProofSha256)` using `restore/identity-inputs.json`. Baseline ID must begin `boom-identity-`. The production validator pins the entire input description and the exact approved native-import proof digest. The CLI verifies plan/proof byte hashes against build-manifest entries `config/identity-plan.json` and `config/native-import-verification.json`, validates the complete 44-table receipt, verifies the destination workspace, and reads current counts for all 44 tables before writing.

The manifest uses the existing `{snapshotAt,files:[{path,bytes,sha256}]}` shape. TINYBIRD_URL and TINYBIRD_TOKEN come from the existing job secret environment. No credentials belong in the image or configuration.

There are 10 historical browser tables, one preserved live table, and nine fixed Fivetran snapshots. Browser live ends at 2026-09-05 21:41:23.124153; Fivetran snapshots are at 2026-09-05T22:28:00Z. Version 1 is NOT a common-time snapshot. Retained R2/raw-observation replay must cover the browser gap and continue from immutable envelope/event keys. No R2 objects are excluded merely because their hash sorts before a cursor.

Execution
---------

1. Read compact source projections in physical-key ranges. Check original IDs and projected range completeness. Preserve all four live delivery-attempt keys: producer, sequence, delivery ID, ingestion time.
2. Retain compact identity candidates. Select one current fact by source priority and revision. A missing candidate page fails coverage; its selection attempt is discarded before any graph or scope publication.
3. Build a compact complete connected-component index, then partition selected facts by component. Publish complete components through the existing identity engine and journal commit rules at version 1. There is no session, browser-group, conversion-record, per-visitor DO or per-scope publication stage.
4. Write current qualifying Fivetran scope facts and authenticated lookup proofs, then seal only after source, selection, partition, graph, scope and membership counts agree. Checkpoints are stored remotely after verified writes. Retries retain immutable rows; no TTL or cleanup is part of this unit.

Fivetran facts use observation-time microseconds + 1, priority 1. Successful paid charges retain identity after refunds. AC primary tags suppress fallback per contact and registration type. Only `_fivetran_deleted` excludes AC contacts, matching the current Dataform filter; the unrelated `deleted` column is not an additional filter.

Activation and continuous input
-------------------------------

The completion log returns `sealHash`, the SHA-256 of canonical complete seal JSON. Configure:

- TENANT_ID: the sealed tenant.
- IDENTITY_BASELINE_ID: the sealed baseline ID.
- IDENTITY_BASELINE_SEAL: that complete `sealHash`, not `sourceSeal`.
- IDENTITY_BASELINE service binding: exported `IdentityBaseline` WorkerEntrypoint.

Private methods are `readRecords({kind,keys})` and `readScopes({tenantId,baselineId,scopeIds})`. The latter returns a plain object with every requested key; authenticated absence returns `[]`. Both verify the configured complete seal, its membership descriptors and exact returned row counts/hashes. Missing proof data retries/fails. Requests are limited to 200 keys and 20,000 authenticated rows; callers must split larger requests, and an individually larger lookup requires explicit handling rather than truncation.

No per-scope PreparedScope seeding is required. Each identity source coordinator starts at the sealed Fivetran snapshot T and lazily hydrates previous qualifying facts. Its raw scope reader must overlay snapshot rows with CHANGES rows through an exact complete nine-source receipt, preserving unchanged dependency rows. Runtime activation, receipt consumption, fan-out and the R2 catch-up proof belong to root's identity-runtime unit.

Capacity and validation
-----------------------

Defaults: 10,000 source IDs per range, 100,000 maximum projected rows per range, 20,000 fact write/scan waves, maximum 500,000 facts per complete component and 32MB of component input per publication batch. Uploads are at most 8MB; new bootstrap verification predicates are below 200KB with at most four concurrent reads. Membership writes use 128 bucket leaves per wave. Live service limits are unchanged.

A local 100k-fact synthetic graph had 200k identifiers and 100k components. Indexing took 560ms and added about 27MiB retained heap above the source array. A complete 10k-fact batch produced 60k journal rows in 1.96s; process RSS was 321MiB after the batch. This excludes Tinybird I/O. Start with a 16GiB process / 12GiB Node heap and retain the explicit bounds. No full-history time or memory result is claimed.

Tests cover source projection/qualification, cross-page AC fallback suppression, repeated live delivery attempts, source and selection gaps, lost acknowledgements, more than 600 complete components, null-time profile selection, authenticated positive/negative reads, a 20k-row bounded/concurrent upload wave, and production proof rejection. Real Tinybird execution of the new projected-range SQL remains an integration check for root; local source tests are synthetic.

Reused source dependencies
--------------------------

The runner reuses browser normalization/identity, session timestamp/canonical/MD5 utilities, the identity engine/state/storage, bootstrap identity/hash utilities, Fivetran raw-collapse/timestamp/json/contracts, ActiveCampaign tag rules/shared utilities, and `scripts/bootstrap-import-proof.ts`. Only type imports reference the old bootstrap executor; it is not executed. Root must preserve its newer shared IdentityStorage baseline overlay and apply its own byte-bound/concurrency change to journal verification reads.
