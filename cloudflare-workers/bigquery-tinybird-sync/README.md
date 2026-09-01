# BigQuery to Tinybird sync Worker

This Worker coordinates native BigQuery exports, Tinybird GCS imports, and the
ordered Tinybird Copy publication. It does not move file contents through
Cloudflare.

```text
Operator run now; optional Cloudflare Cron only after production validation
  -> enqueue one deterministic shard of at most five sources
  -> one PublicationCoordinator Durable Object builds a ten-shard raw generation
  -> submit BigQuery EXPORT DATA queries
  -> poll each BigQuery job to DONE
  -> reserve a workspace-wide Tinybird rate slot in a Durable Object
  -> trigger the matching Tinybird GCS Data Source sync
  -> wait until Tinybird has no working GCS or import jobs
  -> run and poll the ordered Tinybird Copy jobs one at a time
```

The checked-in configuration intentionally has no Cron Trigger. Controlled runs
use `POST /run` until the complete pipeline passes production validation. No live
secret is checked in.

## Cadence and paths

The Worker syncs 49 physical source-table exports plus one derived Stripe auxiliary
export. Resources are sorted by Tinybird name and assigned by index modulo 10. The
ten one-minute shards contain:

```text
5, 5, 5, 5, 5, 5, 5, 5, 5, 5 resources
```

The publication coordinator groups one successful run of each of the ten slots
into a logical raw generation. It persists scheduled minutes, so a Copy
publication can hold raw imports without losing queued deliveries. After the
publication finishes, it drains the queued minutes into the next generation.
Only one raw generation and one publication can be active.

Ten shards describe the healthy-path coverage window, not a guaranteed
ten-minute publication interval. Ten uninterrupted one-minute triggers supply
one run of every raw slot. The same coordinator stops processing raw slots while
it waits for imports and runs the ordered Copy plan. Those scheduled arrivals
remain queued. Each catch-up shard already consumes the full five-call Tinybird
allowance, so the Worker cannot process backlog faster than one shard per
rolling minute. With sub-minute exports, ten scheduled minutes is only the raw
coverage floor. Slow BigQuery jobs add delay, followed by import settling and
Copy time. Do not treat this design as a strict ten-minute freshness SLA.

BigQuery completion can drift across minute boundaries, so a separate
SQLite-backed rate-gate Durable Object coordinates every Tinybird scheduling
call. It persists a rolling 60-second window, admits no more than five calls,
and protects the limit across overlapping or duplicate invocations. A `429`
response honors `Retry-After` and makes one retry through the same rate gate.

Each query exports the interval `[scheduled time - 20 minutes, scheduled time)`. The overlap protects against a late upstream sync. Files land at an immutable scheduled-minute prefix:

```text
gs://booming-data/tinybird/<dataset>/<normalized-table>/incremental/
  run_date=YYYY-MM-DD/run_time=HHMMSS/part-*.parquet
```

The five `boom_domains` ingestion-time-partitioned tables use a versioned prefix
because their hidden BigQuery `_PARTITIONTIME` is part of the staged data contract:

```text
gs://booming-data/tinybird/boom_domains/<normalized-table>/partition_time_v1/incremental/
  run_date=YYYY-MM-DD/run_time=HHMMSS/part-*.parquet
```

Those exports include `_PARTITIONTIME AS source_partition_time`. Their initial
backfills live under the matching `partition_time_v1/backfill/` prefix, so the
native Tinybird loader never mixes the earlier incomplete files with corrected
ones.

The generated manifest at `src/table-manifest.generated.ts` contains 50 resource
names, source metadata, version columns, and export projections. The 49 physical
tables come from `../../tinybird/contracts/raw-bigquery-schemas.json`. The single
derived export comes from `../../tinybird/contracts/derived-bigquery-exports.json`.
Those files are schema contracts only. The matching deployable Data Sources live
in `../../tinybird-production/datasources/raw/`.

Manifest generation also checks `../../tinybird/project/source-registry.json`.
Every physical BigQuery input must belong to exactly one registry source. An
input can register a whole dataset, such as `stripe`, or one table, such as
`activecampaign.contact`. Registry-only inputs such as `jitsu_events_api` are
valid because they do not flow through this Worker.

`raw_stripe_auxiliary` is one typed `UNION ALL` export for the two Stripe
subscription-history tables and five Kajabi checkout/catalog tables. They share
one Parquet schema with `record_type` and `payment_source` discriminators and land
under `tinybird/stripe_combined/auxiliary/`. This keeps the workspace at exactly
five Tinybird sync requests per minute while supplying all seven missing inputs.

The one-time historical load uses `buildBackfillExportPlan`. It writes a complete
snapshot below
`tinybird/stripe_combined/auxiliary/backfill/snapshot_at=YYYYMMDDHHMMSS/` and has
no watermark predicate. Running that BigQuery plan does not trigger Tinybird. An
operator must verify the Parquet snapshot before separately starting the native
GCS Data Source sync.

Keep exactly one full snapshot below that `backfill/` wildcard. A second full
snapshot would make the native GCS import read the same historical rows twice.
Rows arriving after the snapshot belong in the normal incremental export path.

BigQuery `JSON` columns are exported with `TO_JSON_STRING`. `GEOGRAPHY` columns are exported with `ST_ASWKT`. The watermark uses the temporal entries in `versionColumns`; non-temporal tie-breakers remain in the manifest but are not compared to timestamps. Partitioned sources also receive direct bounds on their field partition or `_PARTITIONTIME`. Those bounds are rounded outward to the partition granularity, so they preserve the exact watermark window while allowing BigQuery to prune unrelated partitions.

## Retry behavior

The scheduled minute and resource name form both the BigQuery job ID and GCS output prefix. If Cloudflare delivers the same minute twice, BigQuery returns the existing job instead of starting a second export. The Worker polls that job and never calls Tinybird before its state is `DONE` without an error.

The same BigQuery job ID is the Tinybird rate gate's request key. The Durable
Object stores that key before external I/O. An active duplicate returns
`in_flight`; a completed duplicate returns `already_synced`; neither sends
another Tinybird request. An `in_flight` result does not complete a raw shard.
The queued minute retries and receives `already_synced` after the first request
finishes. Failed leases can be reclaimed after five minutes. Tinybird's GCS
connector still provides the final safeguard by importing only newly discovered
files.

The rate gate records every outgoing attempt, including the one allowed retry after a `429`. Each Tinybird request has a 30-second timeout, and the whole gate operation has a three-minute deadline. A `Retry-After` that cannot fit inside that deadline fails without retrying early; the next scheduled connector run can discover the exported file. The five-minute lease is therefore always longer than live outbound work. A single Durable Object instance is used because the protected quota is shared by the Tinybird workspace. This is intentionally a low-throughput coordination point, not a general data-processing service.

After all ten slots complete, the publication coordinator polls Tinybird's
`gcs_sync` and `import` jobs and requires two consecutive idle polls. It then
runs the Copy plan serially and polls `/v0/jobs/<job-id>` before advancing. Later
generations use the recurring plan from `tinybird-production/PARITY_RUNBOOK.md`.
All Copy jobs use normal shared compute. The bootstrap retains the two completed
`boom_domains` page-view windows at indexes 8 and 9 so an in-flight publication
can resume safely. It does not schedule later `boom_domains` windows,
`jitsu_data`, or `jitsu_events_api`. Those histories are loaded outside this
Copy plan. Index 10 is `snapshot_all_stripe_payments`.

Identity maintenance has four steps: enqueue source changes, run one bounded
affected-component compaction, validate the embedded batch manifest, and append
one activation marker with the Tinybird Events API using `wait=true`. The
compaction writes only to `identity_state_delta_versions`; readers ignore those
rows until the separate activation succeeds. The legacy live-pending, rebuild
gate, and 11 shadow-rebuild Copies are retired from automatic execution.

Recurring `enqueue_identity_changes` runs eight times, once for each producer.
The bootstrap runs seven producers and omits `source_identity:activecampaign`.
The maximum ActiveCampaign source version is `2026-08-26 15:00:21.449` UTC,
which is before the immutable identity seed cutoff of `2026-08-26 23:05:00`
UTC. The seed therefore already contains every ActiveCampaign version that the
bootstrap could enqueue. Bootstrap index 5 now selects `source_identity:stripe`.
Recurring publications still include ActiveCampaign so later source versions
are processed.

Bootstrap producers start at the immutable identity seed `committed_at`,
configured as `IDENTITY_BOOTSTRAP_SOURCE_INGESTED_FROM` in Tinybird
`YYYY-MM-DD HH:MM:SS` format. Its `>=` predicate includes the boundary;
duplicate evidence is safe. Recurring publications use the generation's
earliest raw scheduled minute minus the configured export overlap. The stable
adapters first find keys at or after the selected cutoff, then resolve the
authoritative version for only those keys. The Durable Object stores the batch
index, cutoff, immutable compaction cursor and batch ID, submitted parameters,
manifest, and Tinybird job ID before advancing. Retries poll a known Copy job.
Activation retries are safe because duplicate activation markers expose the
same deterministic batch.

The 20-minute window intentionally repeats source versions across adjacent
Parquet batches. The downstream Tinybird adapters must continue resolving the
latest logical version; raw row counts are not expected to equal distinct
source-key counts.

## Operator endpoints

Every operator endpoint requires:

```text
Authorization: Bearer <SYNC_ADMIN_TOKEN>
```

- `GET /health` reports manifest size, shard sizes, current slot, whether
  required secrets are present, and the healthy-path ten-minute raw coverage
  window. It explicitly reports that the end-to-end publication interval is
  not guaranteed and never reveals secret values.
- `POST /run` queues the current minute's shard and returns `202`.
- `POST /run` with `{"slot": 9}` queues a specific shard while preserving the five-table ceiling.
- `POST /run` with `{"slot": 9, "scheduledAt": "2026-08-26T12:34:00Z"}` queues a deterministic replay. The request key deduplicates the same minute and slot.
- `GET /publication/status` reports the raw queue, active generation, active Copy job and parameters, frozen identity batch, and last completed publication.
- `POST /publication/bootstrap` with `{"mode":"run"}` requests the complete bootstrap plan after the current raw generation settles.
- `POST /publication/bootstrap` with `{"mode":"acknowledge_existing"}` marks a manually completed bootstrap as complete. Use this only after checking every required bootstrap job.
- `POST /publication/recover` with `{"action":"retry_known_job_or_raw"}` retries a failed raw/import check or resumes polling a known Copy job. It never resubmits that Copy.
- `POST /publication/recover` with `{"action":"retry_copy_after_confirming_no_job"}` retries a Copy step with no stored job ID. Use it only after checking Tinybird and confirming the first submission created no job.
- `POST /publication/recover` with `{"action":"retry_terminal_copy"}` asks Tinybird to verify that the stored Copy job is `error` or `cancelled`, records that terminal result, then resubmits the same coordinator step. It refuses jobs that are still active or already done.

`SYNC_ADMIN_TOKEN` protects these HTTP endpoints only. It is never sent to
Tinybird.

## Local verification

```sh
npm install
npm run manifest:generate
npm run cf-typegen
npm run check
npm run validate:bundle
```

Copy `.dev.vars.example` to `.dev.vars` only for local execution. Never commit `.dev.vars`.

## Provisioning when deployment is approved

The export service account needs:

- `roles/bigquery.jobUser` on `able-folio-499722`.
- `roles/bigquery.dataViewer` on each source dataset.
- `roles/storage.objectAdmin` limited to the `booming-data/tinybird/` object prefix where possible. BigQuery requires object create and delete permissions for an overwrite export.

Tinybird's separate GCS reader identity needs `roles/storage.objectViewer` on the exported prefix. The existing `boom_bigquery_gcs` connection owns that credential.

Tinybird currently requires an `ADMIN` token for the native GCS scheduling API.
The same `TINYBIRD_ADMIN_TOKEN` also submits and polls Copy jobs, reads the
compaction cursor and manifest endpoints, and appends the activation marker. The
existing Data Source `APPEND` token cannot do all of this.
Cloudflare metadata confirms that `TINYBIRD_ADMIN_TOKEN` and the separate
`SYNC_ADMIN_TOKEN` are installed. Their values were not inspected. Keep them as
different secrets.

The live no-Cron Worker predates the fact-expansion Copy. Version
`1980769d-2cf7-4db6-9384-d58eacbf792e` contains the current five-stage sequence
and is uploaded but inactive. After Tinybird release 33 is live, keep this
Worker inactive while `identity_fact_evidence_seed` is imported from the pinned
seed generation and its count and digest are validated. The live identity read
path does not depend on that reverse-evidence seed, but compaction does. Deploy
the Worker only after the import passes, then run a controlled generation before
adding a Cron Trigger.

Keep the Cron Trigger inactive during the first rollout:

1. Pipe each secret from a mode-`600` file into `wrangler secret put`. The first
   command creates an inert draft Worker when the service does not exist. Do not
   place secret values in command arguments.
2. Upload the checked-in `wrangler.jsonc`. It includes the coordinator Durable
   Object migrations and no Cron Trigger.
3. Validate the version preview, then use `wrangler versions deploy` to promote
   that exact version.
4. Verify `/health`, queue controlled raw runs, and inspect
   `/publication/status`. Confirm BigQuery, GCS, Tinybird imports, and the Copy
   job sequence.
5. Add and deploy a Cron Trigger only after the controlled run, identity parity,
   and failure tests pass. If approved, the sharding plan requires `* * * * *`.
   A slower trigger makes the raw coverage window longer than ten minutes. No
   Cron schedule is currently checked in.

## Runtime caveats

- A scheduled trigger, when deliberately enabled later, only queues work. A Durable Object alarm processes one raw slot or one
  Copy state transition at a time. BigQuery polling still stops after eight
  minutes, and Tinybird rate waits stop after three minutes.
- A submitted Copy has a one-hour polling timeout. The coordinator stops instead
  of guessing when a job errors, is cancelled, or exceeds that timeout.
- Tinybird's Copy submission API does not expose an idempotency key. If the
  network fails after Tinybird accepts a Copy but before the Worker stores its
  job ID, the coordinator stops in `failed`. An operator must inspect Tinybird
  jobs before changing state or retrying. Blind resubmission could duplicate an
  append-mode Copy.
- Producer batching and a cursor-bound limit keep normal identity work bounded.
  Exact deletions and profile splits are handled by expanding only the touched
  published profiles through the identifier-to-fact evidence index.
- This configuration assumes Cloudflare Workers Paid. A one-minute Cron has a 30-second CPU limit on Paid, while Free has only 10 ms and 50 subrequests. Five concurrent BigQuery polling loops can exceed the Free subrequest allowance.
- Service-account JSON is a long-lived private key. Store it only as a Worker secret, restrict its IAM roles, audit its use, and rotate it. The Worker exchanges a WebCrypto-signed RS256 JWT for a short-lived Google OAuth token on every invocation.
- `EXPORT DATA` query processing is billable, even though the export operation itself is not. Monitor bytes processed per table.
- BigQuery requires the source datasets and `booming-data` bucket to be in compatible locations.
- Timestamp watermarks cannot prove deletion completeness when an upstream connector removes data without advancing a row version. Keep periodic key-level reconciliation or full replacement in the parity plan for those sources.

References: [Cloudflare Worker limits](https://developers.cloudflare.com/workers/platform/limits/), [BigQuery `EXPORT DATA`](https://cloud.google.com/bigquery/docs/reference/standard-sql/export-statements), [BigQuery export permissions](https://cloud.google.com/bigquery/docs/exporting-data), and [Tinybird GCS sync](https://www.tinybird.co/docs/forward/ingest-data/connectors/gcs).
