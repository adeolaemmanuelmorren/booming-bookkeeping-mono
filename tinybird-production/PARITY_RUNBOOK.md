# Production parity runbook

This runbook compares all 40 Dataform outputs in
`able-folio-499722.booming_data_analytics` with the same-named resources in the
production Tinybird project. It assumes the complete Tinybird resource graph is
already deployed.

Do not start parity while raw imports or Copy jobs are still running. Tinybird has
no time travel for these output Pipes. The checked-in Cloudflare coordinator
serializes BigQuery/GCS raw generations and Copy publication, but it has not been
deployed and it cannot pause the separate Jitsu Events API path. Exact comparison
therefore still needs a quiet cut:

1. Stop new raw sync cycles and hold Jitsu delivery.
2. Wait for all BigQuery export, Tinybird import, materialization, and Copy jobs to
   finish.
3. Record the BigQuery snapshot timestamp, Tinybird deployment, raw import jobs,
   and Copy jobs in one run manifest.
4. Refresh the physical state in the order below.
5. Run all six parity layers before releasing held delivery.

A bounded historical window is enough for date-filtered reports, but it is not
enough to prove all-time outputs such as customers, payments, or first/last
touchpoints.

## Automated publication coordinator

`cloudflare-workers/bigquery-tinybird-sync` now contains a
`PublicationCoordinator` Durable Object. The live Worker has no Cron and runs an
earlier coordinator version. Cloudflare Worker version
`1980769d-2cf7-4db6-9384-d58eacbf792e` contains the current five-stage identity
sequence and is uploaded but not deployed.

Release 33 can be promoted while `identity_fact_evidence_seed` is still empty:
the resolver and reporting read path uses `identity_state_seed` plus activated
deltas, not the reverse-evidence seed. Keep the coordinator paused after
promotion. Import and validate the reverse-evidence seed before deploying the
new Worker version or submitting any identity Copy job:

```sh
tb --cloud datasource sync identity_fact_evidence_seed --yes
```

The source must be the pinned
`snapshot_at=20260826230500` prefix used by the other two seeds. Require a
nonzero imported row count and compare its count and digest with the pinned
BigQuery export. An empty or partial reverse index can make compaction omit
unchanged facts without failing its internal manifest check.

After that import passes and Worker version
`1980769d-2cf7-4db6-9384-d58eacbf792e` is deployed, the coordinator:

1. Persists every scheduled raw slot instead of dropping minutes while Copies run.
2. Requires one successful run of all ten raw shards.
3. Requires two consecutive checks with no working Tinybird `gcs_sync` or `import`
   jobs.
4. Holds later BigQuery/GCS raw runs while it submits and polls one Copy at a time.
5. Runs the bootstrap order or recurring order documented below.
6. Repeats bounded identity compaction until the prepared batch contains no events.
7. Activates a batch only after its manifest proves the expected and actual row
   counts and hashes match.

The coordinator needs a dedicated `TINYBIRD_ADMIN_TOKEN`. Tinybird rejects the
native GCS scheduling call with the resource-scoped `APPEND` token. Cloudflare's
live Worker metadata confirms that this secret and the separate
`SYNC_ADMIN_TOKEN` are installed; their values were not inspected. The latter
protects HTTP operator endpoints and is never sent to Tinybird.

Tinybird's Copy submission endpoint does not accept an idempotency key. If the
network fails after Tinybird accepts a Copy but before the coordinator stores the
job ID, the coordinator stops. Inspect Tinybird jobs before any retry, especially
for an append-mode Copy. The recovery endpoint can resume a known job without
resubmitting it. Retrying a step with no stored job ID requires an explicit
confirmation that Tinybird created no job.

## Physical state refresh order

The live BigQuery-to-Tinybird Worker currently exports and imports raw data only.
The checked-in coordinator automates the sequence below after deployment. None of
the Copy files has a `COPY_SCHEDULE`; the coordinator remains the sole scheduler.

Run the one-time bootstrap Copies only when their targets are empty:

```sh
tb --cloud copy run backfill_activecampaign_contacts_current
tb --cloud copy run backfill_activecampaign_contact_tags_current
tb --cloud copy run backfill_activecampaign_tags_current

tb --cloud copy run backfill_jitsu_attribution_param_versions
tb --cloud copy run backfill_jitsu_form_submission_versions
tb --cloud copy run backfill_jitsu_identify_versions
tb --cloud copy run backfill_jitsu_order_completed_versions
```

Do not run `backfill_jitsu_page_view_versions` without parameters. The Worker
runs it on normal shared compute using explicit `p_source_system`, `p_start`, and
`p_end` values. The bootstrap retains only the two completed `boom_domains`
windows, 2026-06-22 to 2026-06-24 and 2026-06-24 to 2026-06-26. They remain at
publication indexes 8 and 9 so the current bootstrap can resume safely. The plan
does not schedule later `boom_domains` windows, historical `jitsu_data`, or
`jitsu_events_api`; those page views are loaded outside this Copy plan. Index 10
is `snapshot_all_stripe_payments`.

Identity does not have a bootstrap Copy. Its three immutable seed Data Sources are
loaded from the same completed BigQuery generation in GCS:

- `identity_state_seed`
- `identity_mapping_evidence_seed`
- `identity_fact_evidence_seed`

Never replace one seed independently. Import all three from one pinned generation,
verify their counts and hashes, and then keep them immutable. The recurring path is:

```sh
# Use the same overlap cutoff for all five Jitsu producers. It must be the
# earliest raw scheduled minute in the generation minus EXPORT_OVERLAP_MINUTES.
IDENTITY_CUTOFF='YYYY-MM-DD HH:MM:SS'
tb --cloud copy run enqueue_identity_changes --param p_identity_producer=source_identity:segment_identify --param "p_source_ingested_from=${IDENTITY_CUTOFF}"
tb --cloud copy run enqueue_identity_changes --param p_identity_producer=source_identity:segment_form --param "p_source_ingested_from=${IDENTITY_CUTOFF}"
tb --cloud copy run enqueue_identity_changes --param p_identity_producer=source_identity:segment_order_completed --param "p_source_ingested_from=${IDENTITY_CUTOFF}"
tb --cloud copy run enqueue_identity_changes --param p_identity_producer=source_identity:segment_page_view --param "p_source_ingested_from=${IDENTITY_CUTOFF}"
tb --cloud copy run enqueue_identity_changes --param p_identity_producer=source_identity:segment_attribution --param "p_source_ingested_from=${IDENTITY_CUTOFF}"
tb --cloud copy run enqueue_identity_changes --param p_identity_producer=source_identity:activecampaign
tb --cloud copy run enqueue_identity_changes --param p_identity_producer=source_identity:stripe
tb --cloud copy run enqueue_identity_changes --param p_identity_producer=source_identity:stripe_kajabi
```

The coordinator then freezes and drains one batch at a time. The five dependent
Copies must use the same `tenant_id`, `batch_version`, and `batch_id`:

```sh
BATCH_VERSION='<active batch version + 1>'
BATCH_ID='<generation>_identity_<batch version>'
CURSOR_AT='<current_identity_compaction_cursor.checkpoint_ingested_at>'
CURSOR_EVENT_ID='<current_identity_compaction_cursor.checkpoint_event_id>'

tb --cloud copy run prepare_identity_compaction \
  --param p_tenant_id=boom \
  --param "p_batch_version=${BATCH_VERSION}" \
  --param "p_batch_id=${BATCH_ID}" \
  --param "p_cursor_ingested_at=${CURSOR_AT}" \
  --param "p_cursor_event_id=${CURSOR_EVENT_ID}" \
  --param p_batch_limit=5000
tb --cloud copy run scope_identity_compaction \
  --param p_tenant_id=boom \
  --param "p_batch_version=${BATCH_VERSION}" \
  --param "p_batch_id=${BATCH_ID}"
tb --cloud copy run expand_identity_compaction_facts \
  --param p_tenant_id=boom \
  --param "p_batch_version=${BATCH_VERSION}" \
  --param "p_batch_id=${BATCH_ID}"
tb --cloud copy run build_identity_compaction_components \
  --param p_tenant_id=boom \
  --param "p_batch_version=${BATCH_VERSION}" \
  --param "p_batch_id=${BATCH_ID}"
tb --cloud copy run compact_identity_state \
  --param p_tenant_id=boom \
  --param "p_batch_version=${BATCH_VERSION}" \
  --param "p_batch_id=${BATCH_ID}"
```

Read `identity_compaction_manifest` for that exact batch. The coordinator sends the
Events API activation row only when `is_valid = 1`. If the batch contains events,
start the next batch from the returned checkpoint. If `input_event_count = 0`, do
not activate it; identity is caught up and report publication can continue.

The fact-expansion Copy always writes a batch-keyed `facts_metadata` readiness row,
including for an empty batch. The component Copy requires that marker, so a failed
or skipped fact expansion cannot silently produce a valid manifest.

The former live-pending and shadow-rebuild Pipes are retired. Their physical Data
Sources remain rollback-only and are never part of automatic execution. Source
tombstones and affected-component recomputation handle routine deletions without a
global rebuild.

Wait for each submitted job to finish before starting a dependent stage. Once
identity has caught up, the recurring replace-mode output chain is:

```sh
# Stage A. These three are independent.
tb --cloud copy run snapshot_activecampaign_registrations
tb --cloud copy run snapshot_all_stripe_payments
tb --cloud copy run snapshot_mart_ad_performance

# Stage B. Run after Stage A and the activated identity state.
tb --cloud copy run snapshot_int_stripe_browser_product_resolution
tb --cloud copy run snapshot_mart_form_submissions_client_side
tb --cloud copy run snapshot_mart_form_submissions_server_side
tb --cloud copy run snapshot_mart_payments
tb --cloud copy run snapshot_mart_touchpoints_all

# Stage C.
tb --cloud copy run snapshot_mart_payments_client_side
tb --cloud copy run snapshot_int_payment_plan_timing
tb --cloud copy run snapshot_segretl_repeatable_conversions

# Stage D.
tb --cloud copy run snapshot_mart_conversions_with_touchpoints

# Stage E. These three are independent after Stage D.
tb --cloud copy run snapshot_mart_conversions_multi_touch
tb --cloud copy run snapshot_mart_revenue_attribution
```

`snapshot_mart_revenue_attribution` also requires the completed payment-plan Copy.
`snapshot_segretl_repeatable_conversions` requires completed browser-product,
payment, touchpoint, server-form, and Jitsu-order state.

## Direct snapshot-backed outputs

Ten of the 40 same-named output Pipes are shallow readers over replace-mode Data
Sources:

| Output | Data Source | Refresh Copy |
| --- | --- | --- |
| `mart_ad_performance` | `mart_ad_performance_current` | `snapshot_mart_ad_performance` |
| `mart_form_submissions_client_side` | `mart_form_submissions_client_side_current` | `snapshot_mart_form_submissions_client_side` |
| `mart_form_submissions_server_side` | `mart_form_submissions_server_side_current` | `snapshot_mart_form_submissions_server_side` |
| `mart_payments` | `mart_payments_current` | `snapshot_mart_payments` |
| `mart_payments_client_side` | `mart_payments_client_side_current` | `snapshot_mart_payments_client_side` |
| `mart_touchpoints_all` | `mart_touchpoints_all_current` | `snapshot_mart_touchpoints_all` |
| `mart_conversions_with_touchpoints` | `mart_conversions_with_touchpoints_current` | `snapshot_mart_conversions_with_touchpoints` |
| `mart_conversions_multi_touch` | `mart_conversions_multi_touch_current` | `snapshot_mart_conversions_multi_touch` |
| `mart_revenue_attribution` | `mart_revenue_attribution_current` | `snapshot_mart_revenue_attribution` |
| `segretl_repeatable_conversions` | `segretl_repeatable_conversions_current` | `snapshot_segretl_repeatable_conversions` |

Four other replace-mode Data Sources are physical prerequisites rather than
same-named outputs:

| Intermediate | Data Source | Refresh Copy |
| --- | --- | --- |
| ActiveCampaign registration adapter | `activecampaign_registrations_current` | `snapshot_activecampaign_registrations` |
| Shared Stripe payment adapter | `all_stripe_payments_current` | `snapshot_all_stripe_payments` |
| Browser product resolution | `int_stripe_browser_product_resolution_current` | `snapshot_int_stripe_browser_product_resolution` |
| Payment-plan timing | `int_payment_plan_timing_current` | `snapshot_int_payment_plan_timing` |

Identity, Jitsu, and ActiveCampaign version Data Sources also sit upstream of most
customer, funnel, attribution, and reverse-ETL outputs. Calling an output
"virtual" below means only that it is not a direct shallow reader. It can still
depend on physical state transitively.

## Output comparison order

Compare a layer only after every output in the prior layer passes. `[snapshot]`
marks a direct snapshot-backed output from the table above.

### Layer 1

- `mart_ad_cost_by_landing_page_daily`
- `mart_ad_performance` `[snapshot]`
- `mart_ad_performance_hourly`
- `mart_form_submissions_client_side` `[snapshot]`
- `mart_form_submissions_server_side` `[snapshot]`
- `mart_manual_attribution_profile_search_candidates`
- `mart_meta_delivery_daily`
- `mart_payments` `[snapshot]`
- `mart_touchpoints_all` `[snapshot]`

### Layer 2

- `mart_checkout_started_client_side`
- `mart_contact_first_conversions`
- `mart_customers`
- `mart_krc_acquisition`
- `mart_manual_attribution_payment_status`
- `mart_payment_info_submitted_client_side`
- `mart_payments_client_side` `[snapshot]`
- `mart_product_sales`
- `mart_touchpoints_first`
- `mart_touchpoints_last`
- `mart_unattributed_payments`
- `segretl_form_submitted`

### Layer 3

- `mart_booming_mentorship_vip_customers`
- `mart_conversions_with_touchpoints` `[snapshot]`
- `mart_krc_post_registration_paid_clicks`
- `segretl_first_conversions`
- `segretl_form_submitted_flattened`
- `segretl_repeatable_conversions` `[snapshot]`

### Layer 4

- `mart_conversions_ad_performance_hourly`
- `mart_conversions_multi_touch` `[snapshot]`
- `mart_revenue_attribution` `[snapshot]`
- `segretl_first_conversions_flattened`
- `segretl_order_completed`
- `segretl_repeatable_conversions_flattened`

### Layer 5

- `mart_conversions_ad_performance`
- `mart_conversions_multi_touch_pages`
- `mart_conversions_organic_performance`
- `mart_meta_krc_reporting_hourly`
- `segretl_order_completed_flattened`

### Layer 6

- `mart_conversions_all_performance`
- `mart_landing_page_performance_daily`

This is the topological order of the checked-in Dataform output dependencies. All
40 names have one same-named Tinybird resource. `mart_ad_performance_hourly` and
`mart_meta_delivery_daily` are API endpoints. The other 38 are queryable Pipes,
not public endpoints.

## Comparison contract

Each output must pass all of these checks against the BigQuery table at the
recorded snapshot timestamp:

1. Exact column names and order.
2. Compatible types and nullability.
3. Declared unique-key uniqueness in both systems.
4. Row count, per-column null counts, and numeric totals.
5. A full-row multiset digest split into 256 hash buckets.
6. A keyed row diff when a declared key exists, otherwise a canonical-row diff
   for the mismatched buckets.

`mart_conversions_all_performance` and `mart_conversions_multi_touch_pages` are
BigQuery views. BigQuery cannot time-travel the view object as one table. During
the quiet cut, materialize each view result into a temporary comparison table, or
rewrite its query so every physical input uses the same `FOR SYSTEM_TIME AS OF`
timestamp. The other 38 outputs are BigQuery tables.

The bucket digest should length-prefix every canonical field and use BigQuery
`FARM_FINGERPRINT` with Tinybird `farmFingerprint64`. Compare bucket row count,
unsigned fingerprint sum, and XOR. This reuses the method already proved by
`compare-activecampaign-registration.mjs` without downloading millions of rows.

Canonicalization rules must be explicit:

- Timestamps use UTC with six fractional digits.
- Dates use `YYYY-MM-DD`.
- `NUMERIC` and Tinybird Decimal values use nine fixed decimal places.
- `FLOAT64` values use a declared comparison scale and also receive independent
  aggregate checks. The two existing Meta comparators use nine decimal places.
- Booleans use `0` or `1`.
- Null uses a value that cannot collide with a non-null field because fields are
  length-prefixed.
- Arrays preserve order unless the output contract explicitly marks them as a
  set. Nested Segment payload tuples and the KRC paid-touchpoint array need a
  recursive canonicalizer. JSON text alone is not a cross-engine canonical form.

Nineteen outputs have Dataform-declared unique keys. The generic runner should
read these declarations rather than duplicate them by hand. The checked-in
runner does that. The other 21 have separately reviewed diagnostic keys in
`scripts/parity/output-contract.json`.

## Production comparison command

The comparator is read-only. It never creates BigQuery tables, changes Tinybird
resources, or pauses ingestion. The operator owns the quiet cut and supplies its
generation identifier:

```sh
npm run parity:production -- \
  --snapshot="2026-08-27T03:05:00Z" \
  --generation="quiet-cut-20260827t0305z" \
  --quiet-cut-confirmed=true \
  --view-snapshot="mart_conversions_multi_touch_pages=able-folio-499722.booming_data_parity_snapshots.mart_conversions_multi_touch_pages__quiet_cut_20260827t0305z" \
  --view-snapshot="mart_conversions_all_performance=able-folio-499722.booming_data_parity_snapshots.mart_conversions_all_performance__quiet_cut_20260827t0305z"
```

Run `npm run parity:plan` first to inspect all 40 outputs without querying either
system. `--layer=3` selects one layer. Repeat `--output=<name>` to select specific
outputs. The default Tinybird target is production. Use the pending deployment
explicitly when validating a release before promotion:

```sh
node scripts/compare-dataform-outputs.mjs \
  --target=staging \
  --plan
```

A branch must also be explicit:

```sh
node scripts/compare-dataform-outputs.mjs \
  --target=branch \
  --branch=<branch-name> \
  --plan
```

The runner stops after the first failing layer and writes one incremental audit
record to `.parity-evidence/<generation>/run.json`. That record contains schema
hashes and mismatches, uniqueness results, row counts, per-column null counts,
scaled numeric totals, and the 256-bucket multiset digest. It does not contain
raw rows. `--diagnostics=hashed-rows` adds bounded SHA-256 key and row hashes for
mismatched buckets; the default `summary` mode emits no row-level values.

### Freeze the two BigQuery views

BigQuery cannot time-travel a view result. After ingestion is held and all jobs
finish, materialize each view into a dedicated comparison dataset. Use a unique
table name for the quiet-cut generation and set an expiration. For example:

```sql
CREATE TABLE `able-folio-499722.booming_data_parity_snapshots.mart_conversions_multi_touch_pages__quiet_cut_20260827t0305z`
OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))
AS
SELECT *
FROM `able-folio-499722.booming_data_analytics.mart_conversions_multi_touch_pages`;

CREATE TABLE `able-folio-499722.booming_data_parity_snapshots.mart_conversions_all_performance__quiet_cut_20260827t0305z`
OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))
AS
SELECT *
FROM `able-folio-499722.booming_data_analytics.mart_conversions_all_performance`;
```

Pass those two tables with `--view-snapshot`. The runner verifies that each
reference resolves to a table rather than a view. It refuses to compare either
view without a frozen table. The other 38 BigQuery outputs use `FOR SYSTEM_TIME
AS OF` at the supplied `--snapshot` timestamp.

## Existing scripts to reuse

- `scripts/compare-meta-marts.mjs` already proves full-row parity for
  `mart_ad_performance_hourly` and `mart_meta_delivery_daily` over a fixed range.
- `scripts/compare-activecampaign-registration.mjs` is the upstream gate for
  server-side form outputs and contains the scalable bucket-digest pattern.
- `scripts/compare-all-stripe-payments.mjs` is the upstream gate for payment
  outputs and contains the recursive per-type canonicalization pattern.
- `dataform/validation/reconcile_mart_payments.sql`,
  `validate_documented_product_rules.sql`, and
  `validate_payment_occurrence.sql` remain payment business-rule gates. They do
  not compare Tinybird with BigQuery.

All three upstream scripts accept `--target=cloud|branch`. Cloud is the default;
branch mode requires `--branch=<branch-name>`.

## Remaining automation

The comparison command now covers all 40 outputs. These controls still sit
outside the comparator:

1. Promote Tinybird release 33 while leaving the current no-Cron Worker live.
   Import and validate `identity_fact_evidence_seed` from the pinned seed
   generation. Only then deploy inactive Cloudflare Worker version
   `1980769d-2cf7-4db6-9384-d58eacbf792e` and validate one controlled generation.
   Its source waits for all ten raw shards, polls imports and Copy jobs, holds
   later raw runs, and publishes one generation at a time.
2. Add a Jitsu hold or generation pin. The coordinator provides the quiet cut for
   BigQuery/GCS, but live Jitsu rows can still change while parity runs.
3. Add freshness checks for all 50 raw imports, the five Jitsu version stores,
   ActiveCampaign current state, the identity generation, and every replace-mode
   Data Source. A successful deployment does not prove data is loaded or current.
4. Join deployment, import, and Copy job IDs from the future coordinator into the
   comparator's run manifest. The comparator already records the snapshot,
   generation, row counts, schema hashes, bucket digests, duration, and result.
5. Add public endpoints or a documented SQL-query service for the 38 outputs that
   are currently regular Pipes if external consumers must query them directly.

Until items 1 through 4 exist and the coordinator is deployed, the comparison
itself is repeatable, but starting a race-free production generation remains an
operator-controlled procedure.
