# Tinybird V1 execution record

V1 replaces the existing Tinybird implementation with source facts, accurate incremental sessions, incremental identity records, and Stripe/ActiveCampaign conversions. There is no attribution, report aggregation, activation, or Dataform execution in V1. The latest user correction requires retaining the existing Fivetran/GCP source feed. Its current transport uses BigQuery as the raw landing/export step before GCS and Tinybird.

Stripe and ActiveCampaign must use the existing Fivetran → GCP sync. The user explicitly rejected both conversion webhooks and direct provider API polling. API polling implementation has been stopped and is not enabled in production. The existing Jitsu transport remains in use.

The existing conversion path is Fivetran → BigQuery raw tables → GCS exports → Tinybird's native GCS connector. The production reset and historical raw restore are complete. The replacement raw exporter is enabled and tracks BigQuery commit-time changes. Its old attribution/report Copy plan remains retired.

## Current user direction

The continuous raw Fivetran → BigQuery → GCS → Tinybird native-connector sync is complete and remains enabled. The user has now authorized restoring Jitsu → the existing Cloudflare Queue → Tinybird with roughly one-minute batching, replaying retained R2 envelopes, and initializing identity from history before continuously applying touchpoint and Fivetran changes. Raw touchpoint delivery must run independently of identity initialization. Retain R2 originals for independent identity replay. The stopped combined conversion/session backfill must remain stopped; use a separate identity-only initializer. No conversion, session, attribution, activation or report models are enabled by this work.

## Work in progress, touchpoints and identity

- Raw collection is live in Worker version `2fb9122c-2f45-4a66-897e-2814d2d64e15`. It writes complete original observations to `v1_jitsu_observations`, verifies hashes and occurrence keys before acknowledging, and retains R2 originals. Tinybird deployments 130 and 131 added the table and the existing runtime token's access.
- The separate raw replay completed two full scans with 723 envelopes and 12,129 verified events, zero pending and no errors at 01:39:19 UTC. It is now paused; the live queue continues independently. Evidence: `evidence/cutover/raw-touchpoints-pause-complete.json`.
- Queue version `95fac3f8-8123-4c5b-82d6-92a822bc2b3c` uses a maximum of 100 envelopes or 60 seconds. Full batches can flush sooner during bursts. Real touchpoint arrival age was 79 seconds at 01:31:52 UTC.
- Queue retries share the same raw occurrence key. New upstream delivery attempts can have different occurrence keys and must collapse to stable semantic source heads. The actual producer/normalizer retry fixture passes. Physical identical retries do not change identity facts.
- Identity-only initialization is running in the separate Cloud Run job `tinybird-v1-identity-bootstrap`, execution `tinybird-v1-identity-bootstrap-wkgz9`, from `/private/tmp/boom-identity-build-20260906-v5`. Every previous execution was confirmed cancelled before its replacement resumed the saved checkpoints. The source plan, native import proof and baseline ID are unchanged. Actual historical Tinybird projections are working.
- Source collection uses three independent collectors with separate durable receipts. Candidate and selected scans fix the empty component prefix so paging follows the physical sort order. Fact verification uses the full sort tuple; every returned column, including full JSON strings, is checked exactly. Requests stay below 200KB with at most four reads in flight per collector. All active collectors drain before a failure is reported.
- Verification retains exact immutable row receipts across attempts when different replicas expose different subsets. It never treats an unobserved row as present. Source, selection, index and publication counts remain mandatory. Twenty-five focused tests and strict TypeScript pass, including alternating replica visibility, lost acknowledgement recovery, full JSON differences and different-key origin conflicts. Each change passed independent review. The job reports only aggregate I/O timings, counters and memory.
- Identity activation is still disabled. New R2 keys are staged without blocking raw delivery. After the complete graph seal is verified, pin its full hash, activate version 1, initialize the three fresh `identity-fivetran:<pipeline>` coordinators, then enable identity replay. Require full retained R2 catch-up and current nine-source receipt consumption before claiming identity is current. The v1 browser cutoff precedes Fivetran snapshot T, so replay must include all retained R2 objects.
- At 01:59 UTC, raw Fivetran transport was verified through 01:54 UTC across all nine sources. Three consecutive native-import windows had no gaps or rejected data. Evidence: `evidence/cutover/raw-sync-during-identity.json`.

## Current state, 00:55 UTC September 6

- All 44 historical/snapshot tables are restored and verified: 23,710,098 physical rows, 9,044 native files, zero missing files or rejected rows.
- All nine live Tinybird GCS connections are attached with the correct recursive paths. Production has 74 V1 tables and zero pipes.
- The separate model backfill was cancelled and remains stopped. Its partial prepared records are not an activated baseline. The facts Worker stays disabled; Jitsu stays buffered.
- The bucket-only custom role now grants the existing exporter exactly storage.objects.list. The first failed export was verified to have written no files and recovered using the same query, data window and output path with a new job ID.
- Raw sync is enabled in exporter version b2bb918b-71eb-46b9-9650-34720f53c315. Its minute Cron wakes the durable coordinator, which exports each source and triggers its native GCS connector after export completion.
- The initial catch-up completed for all nine sources: nine native files, 48 changed records, zero rejected rows. Its common checkpoint advanced from 22:28 to 23:42 UTC. The first automatic window through 00:13 UTC also completed: nine files and 50 changed records. The next run started automatically, targeting 00:49 UTC.
- Receipt queries explicitly request JSON. Accepted import responses without a job ID continue through existing-job discovery or exact file receipts without repeating the import request. TypeScript, 38 unit tests and 20 Worker tests passed.
- New windows now take all available elapsed time up to the existing one-hour limit. The earlier halving of the available time window caused avoidable catch-up lag. The in-progress window and all persisted export/import identities remain unchanged.
- Continuous raw sync is verified. The initial bridge and two subsequent automatic windows completed across all nine sources: 27 files, 98 changed records, zero rejected rows, no duplicate file appends, and no gaps between windows. At 00:54:57 UTC, the common verified cutoff was 00:49 UTC, about six minutes behind the clock. Another automatic window through 00:54 UTC was already running.
- The current sync-only scope is complete. Keep the raw exporter enabled and the separate model workers disabled.
- Fivetran intervals remain 60 minutes for both Stripe accounts and 360 minutes for ActiveCampaign. No provider API polling or conversion webhooks are used.

Evidence is under `evidence/cutover/`, including `live-gcs-connectors-verified.json`, `gcs-list-permission-added.json`, `failed-export-recovered.json`, `raw-catchup-deploy.log`, `raw-sync-progress.json`, and `raw-continuous-sync-verified.json`.

## Current completion requirements

- [x] Restore and verify the historical raw data in Tinybird.
- [x] Attach the nine live native GCS connectors.
- [x] Stop the separate model backfill and leave model ingestion disabled.
- [x] Correct the export service account's missing bucket-list permission.
- [x] Recover the failed first export without changing its data window or duplicating files.
- [x] Complete the nine-source catch-up from the historical snapshot.
- [x] Observe consecutive automatic raw-sync cycles with matching exported/imported rows and zero rejected rows.
- [x] Keep Fivetran as the source sync; use no conversion webhooks or direct provider polling.

Session, identity, and conversion-model backfills are deferred. They are not prerequisites for completing this raw sync and must not resume automatically.

## Execution order

1. Ground source access, archive recoverability, state size, and exact session/identity/conversion semantics.
2. Compare independent designs and define source-record and current-state contracts.
3. Build the verification fixtures before changing live state.
4. Implement and test ingestion, sessions, identity publication, and conversion updates as independently verifiable units.
5. Pause old writers, preserve source data, reset the confirmed workspace, and deploy the replacement.
6. Backfill, reconcile, resume live sources, and verify freshness and correctness.

The destructive reset is authorized by the user. It follows evidence that required history is recoverable and new deliveries are buffered. No additional report computation belongs in this project.

## Scope and current evidence

The configured target is the `booming_bookkeeping` Tinybird workspace in US East. The legacy `bigquery-tinybird-sync` Worker was retired at 21:10 UTC, with all four namespaces preserved and Cron removed. Its old source, identity, journey and reporting code no longer accepts work. Jitsu has a separate queued delivery Worker. Source connection and live inventories must be checked before relying on old documentation.

The repository contains unrelated user changes. Archive the exact selected implementation trees without altering them during preparation. Preserve authentication outside code archives.

The initial shell network check failed in the sandbox. Subsequent remote checks use narrowly scoped authenticated requests with redacted errors. Private provider read gateways are deployed in existing Workers. The new V1 Worker is deployed with ingestion disabled and browser buffering enabled. Live checks verify both distinct Stripe accounts and ActiveCampaign. The approved production reset has replaced the old Tinybird resources. Required source backups remain in GCS. Jitsu now writes to the V1 R2 buffer. The legacy exporter is retired and its previously submitted export jobs have drained.

Decision trail: `../.audit/tinybird-v1.tsv`.

## Latest verification

- Live Jitsu backups: 789,028 observations through 20:24:51.606074 UTC plus 4,142 tail observations through 21:41:23.124153 UTC. All 793,170 source payload hashes, backup hashes, cloud object checksums and interval boundaries passed verification. See `evidence/cutover/preserved-jitsu-{local,cloud}-verified.json`.
- Historical source-content recoverability is verified for all ten sources through frozen objects, import receipts and old object generations. The extra 660 page rows and 16 attribution rows were duplicate file deliveries. See `evidence/history-recoverability/recoverability-verdict.json`.
- Historical plus cutoff live pages: 3,169,255 distinct pages, 1,619,015 visitors, maximum 1,213 pages per visitor. Bulk bootstrap must avoid one remote publication per visitor.
- Legacy exporter retirement is deployed and verified: HTTP 410, no Cron, all four namespace IDs unchanged. See `evidence/retirement/verified.json`. No pending or running legacy export jobs remain; see `evidence/retirement/export-jobs-drained.json`.
- Actual Tinybird validation passes for identity payload publication, current-state SQL, null times, merges, splits and stale replay. UInt64 uploads use exact JSON numbers. Acknowledged writes can need bounded readback retries.
- Actual Tinybird validation passes for batched provider facts, original evidence, both account namespaces, refunds, registration removal and current conversion reads. The new provider tables exist only in the validation branch. These API transport proofs are superseded by the user's requirement to retain Fivetran ingestion.
- Integrated unit suite passed 114 cases after initial browser bootstrap integration. Browser/session runtime passed 45 cases; ingress runtime passed 16; the newly integrated ongoing buffer replay passed 14 cases. These tests do not prove production flow is complete.
- Actual browser bootstrap passed in the isolated Tinybird branch, including a lost commit acknowledgement and restart. It produced 5 page heads, 4 visitors, 4 sessions and 9 identity facts. The second actual branch smoke also passed the authenticated absence proofs and restart, with the same output counts. Bounded bulk scans are being finalized after real wide-source probes found server timeouts on full-table deduplication.
- Browser routing and source coordinators are integrated locally. The new V1 Worker remains deployed with ingestion disabled. New deliveries are buffered in `boom-tinybird-v1-browser-buffer`; the 22:35 UTC check showed 171 envelopes, 15.7 MB, and a stable old Tinybird count of 793,170.
- Stripe source-content recoverability passed for all 21 source tables. All import paths match preserved files; 32 files without import receipts are empty. See `evidence/stripe-history/recoverability-verdict.json`.
- The finite restore plan contains 34 source families, 8,576 selected Parquet objects and 16,183,572 physical rows before logical deduplication. The plan preserves one object per exact content group. All 8,576 same-bucket GCS copies passed pinned-generation checksum verification at 22:09 UTC. See `restore/preservation-result.json`. The main replacement schema also passed cloud validation; the approved reset completed in deployment 127 at 22:54 UTC.
- The new signed ActiveCampaign receiver was removed immediately after the user's correction. No new conversion webhook was created, configured or deployed. Unused internal conversion webhook paths have also been removed. Subsequent direct API polling work was stopped after the user clarified that conversions must continue through Fivetran/GCP.
- Automatic approval review rejected a broad local raw-data download and a native historical import into a validation branch. Read-only lineage checks resolved the historical discrepancy without either operation. No branch GCS import was run.

- Corrected Fivetran baseline: nine raw tables exported at the common 2026-09-05 22:28 UTC snapshot. All export jobs completed, totaling 6,733,355 rows in 196 Parquet files. GCS generation/checksum inventory is frozen in `restore/fivetran-snapshot/manifest.json`. Native Tinybird imports are underway. Earlier progress figures are historical checkpoints, not completion claims.
- Direct provider API bindings and routes are removed from the deployed V1 Worker. The unused SourceCoordinator namespace is preserved by an inert class. Typecheck and dry bundle passed. Fivetran fact routes are deployed with ingestion disabled.

## Production reset and bootstrap preparation, 23:14 UTC

The user explicitly approved replacing the 120 old tables and 201 pipes after automatic review rejected the first attempt. Deployment 127 completed, leaving 74 V1 tables and zero pipes. Native GCS restore jobs are progressing. All 793,170 preserved live Jitsu observations are restored and verified. New Jitsu deliveries remain in the R2 buffer.

The combined browser and conversion identity fixture passed in the real validation workspace, including restart recovery. It sealed 12 identity facts in five components. The production baseline is not built or activated. The combined one-time job and durable source restart state are being integrated. No live source coordinator is enabled.
