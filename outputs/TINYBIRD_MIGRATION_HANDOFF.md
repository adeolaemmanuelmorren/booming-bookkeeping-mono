# Tinybird migration handoff

Last updated: 2026-08-28 UTC

## 1. Executive summary

The migration is not finished.

| Area | Status | Current truth |
|---|---|---|
| Jitsu live ingestion | Working | Events reach Tinybird through Cloudflare Queue. Latest observed lag was about two minutes. |
| Historical Jitsu page data | Loaded | The missing tail was imported and an exact 65-column sample matched BigQuery. |
| BigQuery raw bootstrap | Loaded | One generation exported and imported all 50 registered source tables. |
| Marketing data refresh | Not running | The recurring Cloudflare schedule was never enabled. Tinybird is behind BigQuery. |
| Identity seed | Loaded | Tinybird has the fixed 2026-08-26 23:05 UTC identity snapshot. |
| Incremental identity updates | Blocked | Preparation passes. The next scope Copy exceeds Tinybird's 30-second limit. |
| Final marts | Unverified | The output files exist, but live values have not passed final parity against Dataform. |

The main failure was the identity design. I built a guarded incremental updater with several serial Copy jobs. It protected the visible identity state from failed runs, but it was too expensive for Tinybird's 30-second Copy limit. I kept optimizing one failing stage at a time instead of stopping and replacing the design. That consumed most of the time.

No background work is running now.

## 2. Architecture that was implemented

### 2.1 Live Jitsu path

```text
Jitsu, five-minute batches
    -> Cloudflare webhook Worker
    -> Cloudflare Queue
    -> Tinybird Events API
    -> live Jitsu Data Source
    -> page, form, identify, order, and attribution adapters
```

Code:

- `cloudflare-workers/jitsu-tinybird-ingest`

Behavior:

- The Worker authenticates the webhook.
- It normalizes Jitsu events into the Tinybird landing schema.
- It assigns deterministic delivery identifiers.
- Cloudflare Queue handles delivery retries.
- Tinybird receives the events through its Events API.

### 2.2 Fivetran marketing and payment path

```text
Fivetran
    -> BigQuery
    -> Cloudflare BigQuery export Worker
    -> GCS Parquet files under gs://booming-data/tinybird/
    -> Tinybird native GCS imports
    -> raw Tinybird Data Sources
    -> adapters and reporting models
```

Code:

- `cloudflare-workers/bigquery-tinybird-sync`
- `tinybird-production`

The Worker registers 50 BigQuery source tables. It divides them into ten groups of five. A Durable Object prevents overlapping import requests. A second Durable Object records the active generation, Copy jobs, identity stage, failures, and recovery state.

### 2.3 Historical Jitsu path

Historical Jitsu data came from BigQuery. The original plan was BigQuery to GCS Parquet to Tinybird. Tinybird's attempted local Parquet append failed before writing any rows, so the missing tail was converted to gzip NDJSON and sent through the Tinybird Events API.

### 2.4 Identity path

The identity seed came from the fixed BigQuery/Dataform snapshot at `2026-08-26 23:05:00 UTC`.

The incremental design has these steps:

1. Enqueue changed facts from Jitsu, ActiveCampaign, Stripe, and Stripe-Kajabi.
2. Prepare a bounded batch of changed facts.
3. Find the existing profiles and mappings affected by those facts.
4. Collect the retained facts for those profiles.
5. Rebuild connected identity components.
6. Write the new state to a hidden delta.
7. Validate the batch manifest.
8. Add an activation marker that makes the batch visible.

Failed batches stay hidden. That protection worked. The new identity state never reached activation because the scope step failed.

## 3. What completed successfully

### 3.1 Jitsu live delivery

The live path works.

The first webhook version returned HTTP `202`. Jitsu treated the request as failed even though the response body said every event was accepted and queued. The Worker now returns HTTP `200`.

Evidence after that fix:

- 5,200 events arrived after the corrected deployment during the first verification.
- A later read-only query found 7,936 live Events API observations.
- Latest live page and identify records were about two minutes behind the query time.

Jitsu Worker version with the `200` response:

- `702b808f-283b-485e-8050-9a16d3fc608c`

### 3.2 Initial BigQuery to Tinybird bootstrap

One complete raw generation ran across all ten five-table groups.

Generation:

- `generation_raw_20260828005300000_slot_0`

This populated the 50 registered raw Data Sources, including:

- ActiveCampaign
- Facebook Ads
- Google Ads
- Stripe
- Stripe-Kajabi
- historical Jitsu and `boom_domains`
- manual attribution inputs

The bootstrap does not prove that those tables are current. It only proves that the initial generation loaded.

### 3.3 Historical page-view migration

Canonical logical counts at the fixed cutoff:

| BigQuery source | Logical page views |
|---|---:|
| `boom_domains` | 854,341 |
| `jitsu_data` | 1,946,689 |
| Total | 2,801,030 |

The missing Jitsu tail contained 64,782 logical rows. The overlap export contained 103,006 source versions, including 38,224 versions already present. That overlap prevented a boundary gap.

The tail was split into 52 gzip NDJSON chunks. Tinybird acknowledged every chunk.

Validation result:

- 6,446 deterministic sampled rows
- all 65 columns compared
- zero missing rows
- zero column mismatches
- identical SHA-256 digest: `aa77dd474b494adb1a3f3240335d5780d546307d203ad548697d85ed1c82bb9c`

### 3.4 Raw page and identify inputs

The latest read-only query returned:

| Input | Tinybird rows | Latest record | Lag at query time |
|---|---:|---|---:|
| Page versions | 3,389,153 | 2026-08-28 04:30:39 UTC | 2 minutes |
| Identify versions | 2,380,954 | 2026-08-28 04:30:31 UTC | 2 minutes |
| Live Events API observations | 7,936 | 2026-08-28 04:30:39 UTC | 2 minutes |

These are input records. They do not mean resolved identity is current.

### 3.5 Local project checks

The checked-in model and contract tests pass:

- Tinybird project check passed for 50 raw Data Sources, one live Data Source, and 153 pipes and endpoints.
- Twenty-seven Tinybird identity tests passed.
- Fifty-seven Cloudflare BigQuery sync Worker tests passed after the final batch-size change.
- Nineteen parity contract tests passed.

These tests validate files, contracts, and deterministic logic. They do not replace live mart parity.

## 4. Problems and attempted fixes

### 4.1 Direct GCS and Parquet confusion

The earlier investigation moved through Fivetran Iceberg, S3, GCS, Airbyte, and BigQuery export options before settling on BigQuery to GCS Parquet to Tinybird.

What blocked the direct historical import attempt:

- Tinybird's local Parquet append failed with `'NoneType' object is not iterable` before writing rows.
- The managed connector path required a deployed Tinybird connection.

Result:

- Historical Jitsu used the Events API instead.
- The scheduled raw pipeline still uses BigQuery export, GCS, and Tinybird's native GCS imports.

### 4.2 Jitsu false failure alerts

Problem:

- Cloudflare returned `202` after accepting and queueing the batch.
- Jitsu classified that response as a failed job.

Evidence from the alert:

- `accepted: 8`
- `queueMessages: 1`
- HTTP status `202`

Fix:

- Return `200` without changing queue behavior.

Result:

- Resolved.
- The alert did not indicate data loss.

### 4.3 Large historical page Copies

The first bootstrap plan included page history already loaded through the separate historical process.

Problems:

- Large page Copies exceeded Tinybird's 30-second limit.
- Recopying the same historical sources would waste compute and risk duplicate versions.

Attempts:

- Split `boom_domains` into small date windows.
- Two windows completed.
- Removed the remaining redundant historical page Copies from the bootstrap plan.

Result:

- The coordinator advanced to payment snapshots and identity processing.

### 4.4 ActiveCampaign bootstrap Copies

The ActiveCampaign contact-tag backfill exceeded the Copy limit after processing about 1.6 million version rows.

An on-demand compute attempt was made. It still hit the Copy limit. The on-demand setting was then removed.

Current state:

- No on-demand compute remains configured.
- Existing ActiveCampaign raw tables are loaded from the bootstrap.

### 4.5 Identity enqueue scanned the wrong seed columns

The identity seed uses this physical order:

```text
tenant_id, state_kind, state_key, batch_version, committed_at, row_hash
```

The first enqueue query filtered `fact_key` instead of `state_key`. That forced broad reads of the seed.

Fix:

- Derive the exact `state_key` for each candidate fact.
- Filter seed rows using that sorted key.

Result:

- Jitsu identity producers completed.
- At least 61,047 identity change events entered the identity queue during bootstrap.

Tinybird release:

- Release 34

### 4.6 ActiveCampaign identity enqueue

The ActiveCampaign identity adapter still timed out after the general enqueue fix.

Investigation found that every relevant ActiveCampaign source version was earlier than the identity seed cutoff:

| Source | Maximum source version |
|---|---|
| Contacts | 2026-08-26 15:00:20.123 UTC |
| Contact tags | 2026-08-26 15:00:20.145 UTC |
| Tags | 2026-08-26 15:00:21.449 UTC |
| Identity seed cutoff | 2026-08-26 23:05:00 UTC |

There were zero post-seed ActiveCampaign candidates.

Attempts:

- Limit the adapter to contacts touched after the cutoff.
- Replace repeated grouped joins with window calculations.
- Reduce the adapter read from about 102.5 million rows to about 41 million rows.

The Copy still timed out.

Decision:

- Skip ActiveCampaign only during this bootstrap because the seed already contains all its versions.
- Keep ActiveCampaign in every recurring publication so future source versions are not omitted.

Tinybird release:

- Release 36

### 4.7 Identity preparation

The first prepared identity batch used 5,000 queued events.

Original performance:

- 6,507 output rows
- 52,207,566 rows read
- about 3.9 GB read
- about 14.6 seconds as a read-only query
- more than 30 seconds as a Copy

Cause:

- Fact and mapping seed lookups again ignored `state_key`.
- The query repeated the seed work across several output branches.

Fix:

- Use the exact physical keys for fact, mapping, profile, scope, and retained-fact lookups.

Verification:

- Old and new queries returned the same 6,507 rows.
- Output hash matched after excluding the nondeterministic staging timestamp.
- Read-only time fell to about 5.5 seconds.
- Reads fell to about 17.2 million rows and 1.6 GB.

The 5,000-event Copy still timed out.

Second change:

- Reduce the identity batch from 5,000 events to 500.
- Confirm both failed Copies wrote zero staging rows before retrying.

Result:

- The 500-event prepare Copy completed.

Tinybird release:

- Release 37

Cloudflare Worker version:

- `74139854-c469-4a01-ba7e-4548b02ab520`

### 4.8 Identity scope

After preparation passed, the coordinator advanced to scope.

Scope determines which existing profiles and mappings could change because of the 500 prepared facts.

Result:

- Scope exceeded Tinybird's 30-second Copy limit.
- No identity activation occurred.
- Visible identity remains on the seed snapshot.
- The prepared batch remains hidden.

Exact stop point:

| Field | Value |
|---|---|
| Coordinator section | `identity` |
| Identity phase | `scope` |
| Batch ID | `generation_raw_20260828005300000_slot_0_identity_1787878380000` |
| Failed Tinybird job | `18c2986f-3ebf-493f-bed2-d84f540276ca` |

Do not blindly retry this job. The scope design needs to be simplified or replaced first.

### 4.9 Older page-view seed drift

The newly imported historical tail matched exactly. A separate full-history sample found 11 older differences out of 2,749 rows.

Differences:

- Nine `NULL` versus `0` values for the Meta-click indicator.
- Two old `$direct/referral` labels versus normalized `direct/none` labels.

This may change session, touchpoint, and channel outputs. It must be resolved before declaring mart parity.

### 4.10 Tinybird vCPU overage

Repeated broad identity queries and failed Copies triggered a Tinybird vCPU overage warning.

The account email stated that seconds over the plan allowance cost `$0.0002` each.

Current controls:

- No on-demand compute.
- No recurring Cloudflare schedule.
- No background jobs started by this handoff.

## 5. Why marketing data is stale

Fivetran is still updating BigQuery. Tinybird is stale because the recurring Cloudflare BigQuery export schedule was never enabled.

The bootstrap ran once. Nothing then triggered the next BigQuery export, GCS write, and Tinybird import.

Read-only comparison:

| Source family | Tinybird newest | BigQuery newest |
|---|---|---|
| ActiveCampaign | 2026-08-26 around 15:00 UTC | 2026-08-28 around 03:00 UTC |
| Facebook Ads | 2026-08-26 around 19:00 UTC | 2026-08-28 around 04:00 UTC |
| Google Ads | mostly 2026-08-26 | 2026-08-27 to 2026-08-28 |

Sample row counts:

| Table | Tinybird | BigQuery | Missing from Tinybird |
|---|---:|---:|---:|
| ActiveCampaign contacts | 1,173,570 | 1,175,424 | 1,854 |
| ActiveCampaign contact tags | 4,546,808 | 4,552,545 | 5,737 |
| Facebook basic ad hourly | 218,306 | 222,076 | 3,770 |
| Google ad stats | 4,527 | 4,569 | 42 |

All 18 queried ActiveCampaign, Facebook Ads, and Google Ads tables exist in Tinybird and contain data. They are not current.

## 6. Identity data status

### 6.1 Present and current enough to ingest

- Raw page events are present and flowing.
- Raw identify events are present and flowing.
- Form and order inputs are present.
- ActiveCampaign and Stripe identity inputs exist.
- The fixed identity seed exists.

### 6.2 Not current

- The resolved profile and mapping state still reflects the 2026-08-26 23:05 UTC seed.
- At least 61,047 bootstrap identity changes were queued.
- The first 500-event batch reached preparation but failed at scope.
- No queued batch reached activation.

Identity seed size at the last check:

- 16,463,564 rows total
- 9,219,703 fact rows
- 4,968,701 mapping rows
- 2,275,158 profile rows
- 2 audit rows

## 7. Final marts and parity status

The Tinybird project defines about 40 outputs across six layers. The local parity contract suite passes, but live parity has not run to completion.

Reasons:

- Marketing data is stale.
- Resolved identity is stale.
- Post-identity outputs have not completed.
- Two BigQuery outputs are views and need physical frozen copies for an exact cutoff comparison.
- The older page-view drift remains unresolved.

The two BigQuery views that need frozen tables are:

- `booming_data_analytics.mart_conversions_multi_touch_pages`
- `booming_data_analytics.mart_conversions_all_performance`

The intended parity snapshot dataset does not exist yet:

- `able-folio-499722:booming_data_parity_snapshots`

Do not report the migration as complete until the identity comparison and all 40 output comparisons pass at the same quiet cutoff.

## 8. What should happen next

### Priority 1. Restore raw data flow

1. Run one controlled catch-up generation from BigQuery through GCS into Tinybird.
2. Compare every registered source watermark between BigQuery and Tinybird.
3. Resolve any table-specific gap.
4. Enable the recurring Cloudflare schedule at the requested five-minute interval.
5. Observe at least two successive runs before calling the flow healthy.

### Priority 2. Replace or simplify identity scope

Do not continue the pattern of optimizing and retrying each Copy stage.

The next person should first choose a simpler identity update method that fits Tinybird's time limit. The current design has too many broad seed reads. Preserve these requirements:

- deterministic output
- retry safety
- no partial identity publication
- one visible profile per identifier
- support for future lead and payment sources

After choosing the simpler method:

1. Process the queued changes.
2. Activate a current identity state.
3. Confirm page, lead, and payment records resolve to the same profiles as BigQuery.

### Priority 3. Rebuild outputs and prove parity

1. Hold Jitsu briefly for the final quiet cut.
2. Catch identity up through that cutoff.
3. Run every post-identity Tinybird output.
4. Refresh Dataform outputs at the same cutoff.
5. Freeze the two BigQuery views.
6. Compare identity state.
7. Compare all 40 marts.
8. Resolve the older page-view differences.
9. Resume Jitsu and recurring exports.

## 9. Mistakes that caused the delay

This section is blunt because the next person should not repeat them.

1. I pursued too many storage alternatives before locking the BigQuery to GCS design.
2. I built an incremental identity system with too many serial stages for Tinybird's Copy limit.
3. I optimized one timeout at a time instead of benchmarking the complete identity path first.
4. I briefly tried on-demand compute even though it did not solve the enforced Copy limit.
5. I left the recurring export schedule disabled while spending time on identity. That allowed marketing data to fall behind.
6. I did not stop early enough when the identity design was clearly becoming the whole project.
7. I did not give concise, accurate status updates soon enough.

## 10. Important files

| Purpose | Path |
|---|---|
| This handoff | `outputs/TINYBIRD_MIGRATION_HANDOFF.md` |
| Jitsu Worker | `cloudflare-workers/jitsu-tinybird-ingest` |
| BigQuery and GCS Worker | `cloudflare-workers/bigquery-tinybird-sync` |
| Tinybird project | `tinybird-production` |
| Tinybird parity runbook | `tinybird-production/PARITY_RUNBOOK.md` |
| Decision and evidence log | `.audit/tinybird-migration.tsv` |
| Independent identity review | `outputs/research/2026-08-24-tinybird-identity-resolution-independent-review.md` |

## 11. Security and access

- The user pasted an admin-scoped Tinybird token into the conversation.
- The Tinybird CLI configuration also contains that token.
- Rotate the Tinybird admin token after handoff.
- Cloudflare stores the Jitsu webhook token, Tinybird ingestion tokens, GCP credentials, and Worker operator token as secrets.
- Do not print any of those values while debugging.

## 12. Final state in one paragraph

Jitsu live ingestion works. Historical page and identify inputs are loaded and continue arriving. The initial 50-table BigQuery bootstrap loaded, but marketing data is stale because the recurring Cloudflare schedule was never enabled. The identity seed exists, but the incremental updater is blocked at the scope Copy and no new identity batch has been activated. Final reporting marts have not passed live parity. The next person should restore the raw schedule first, replace or simplify identity scope, then run one controlled quiet-cut comparison across identity and all 40 outputs.
