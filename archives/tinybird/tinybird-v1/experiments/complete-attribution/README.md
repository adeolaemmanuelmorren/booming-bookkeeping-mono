# Attribution and report proof

This is an executable prototype, not a deployed replacement. It starts with normalized touchpoints, normalized conversions and an externally supplied identity map. `results/` contains the measured results. No production data or models were changed by these experiments.

## Measured results

| Check | Result |
| --- | --- |
| 1,000 randomized profiles, 144 metrics | Passed against the independent per-conversion reference |
| Persisted update/retry/fault suite | 16 checks passed, including refunds, identity changes, shared sessions and missing chunks |
| 100,000 touches, 150,000 conversions, 1,000 profiles | All 2,400 persisted report rows matched; Tinybird query 0.625–0.661 s, HTTP request 0.844–0.875 s |
| 1 million touches, 150,000 conversions, 50,000 profiles | External calculation 6.36 s, peak process RSS 126.2 MiB |
| One profile, 10,000 touches and 30,000 conversions | External calculation 171 ms, peak process RSS 132.6 MiB |
| Empty publication and malformed manifest identity | 3 checks passed |
| Existing V1 local test suite | 178 passed |
| Actual Dataform attribution SQL comparison | Passed: 25 profiles, 375 touchpoints, 469 conversions, 89 report rows; all 144 metrics and session counts matched |

The external benchmarks used a 128 MiB V8 heap limit. Total process memory can exceed that limit, as the RSS values show. They do not establish Cloudflare Worker memory/CPU suitability. The large profiles reuse synthetic input patterns; this is not a measured production distribution. Persisted reporting used a 512 MiB query limit. Scale data was reused for the final query test, so publication latency is not reported.

## The proposed complete system

1. Fivetran remains the source-data sync. BigQuery receives its raw tables. Export committed changes to GCS, with source revisions and deletion information, and let Tinybird's native GCS connector ingest the files. BigQuery does not calculate attribution. End-to-end freshness cannot be faster than the upstream Fivetran sync.
2. Jitsu sends browser events to the Cloudflare buffer. Batch delivery, retries and durable receipts absorb bursts. Keep the raw events for replay. The one-to-two-minute batch target must be measured with real traffic.
3. A session worker maintains each visitor's ordered page history and emits corrected sessions/touchpoints. Preserve the Dataform rule exactly: `TIMESTAMP_DIFF(..., MINUTE) > 30`, with page ID as the tie breaker. A late event can renumber subsequent session IDs, so replace all affected outputs, not just the arriving event.
4. Conversion normalization maintains current source records and their derived conversion facts. Refunds, deletes, first-payment/customer classifications and linked-source corrections can change earlier facts. The 24 metric definitions come from Dataform. A source update must identify every affected conversion, including dependent earlier/later records.
5. The identity service computes the graph outside Tinybird. Publish versioned mapping changes. Route old and new affected profiles to attribution, including retired profiles after a merge and both sides of a split. Ordered identity deltas require durable replay of gaps; a larger version number alone does not prove all earlier deltas arrived.
6. On a fact or identity change, rebuild the affected profiles' attribution contributions using `engine.mjs`. It walks sorted timelines and accumulates middle-touch credit. It never allocates the full touchpoint-by-conversion join. Batch multiple changes to the same profile before rebuilding.
7. Stage immutable contribution chunks in Tinybird. Verify their contents, then publish one manifest covering all affected and retired profiles. A durable single publisher must assign versions and reject stale calculations against newer source/identity revisions. That production coordinator is not implemented by this prototype.
8. A report performs one Tinybird query over the latest committed contributions. It aggregates metrics and exact session memberships, checks publication integrity, then filters the click-date window. Attribution eligibility is computed before the report filter, so later conversions can contribute to earlier click dates.
9. Fetch Google/Meta performance on demand through complete, paginated provider responses. Normalize account, IDs, currency and reporting dates before joining. Return separate attribution and paid-ad reports with their own freshness metadata. Never claim provider delivery statistics are current merely because the request ran now.
10. Activation models should consume versioned profile facts from the external processing layer. Attribution report aggregates cannot replace the underlying profile histories needed for activation rules.

The existing session/conversion/identity components are possible integration targets, not evidence that this full chain is active or correct. This experiment does not enable them.

## What differs from the failed attempts

Earlier query-time joins and arrays expanded or retained too much per-profile history inside a report request. Some special cases passed while dense and distinct-conversion cases hit the 512 MiB query limit.

Here attribution is calculated when an affected profile changes, outside Tinybird. Tinybird stores report contributions and aggregates them on demand. This deliberately changes the earlier pure query-time proposal. It avoids whole-warehouse refreshes, but a long profile history still has a rebuild cost.

An insert-triggered materialized view with `argMin` alone cannot handle this contract. Corrections, refunds, deletions, changed identity and conversion-specific eligibility require replacing affected results. No identity graph is computed in Tinybird.

## Correctness contract

- 24 metrics, first/last/40-20-40, and their three paid variants. `named-metrics.mjs` explicitly maps all 144 positions to Dataform column names.
- Touch time determines eligibility. Session time determines ordering and reporting date, falling back to conversion time. Touchpoint ID breaks ties.
- The first direct visit remains eligible; later direct visits receive zero credit but remain in session counts.
- Paid variants retain the original model weights on qualifying paid touches. They do not renormalize a paid-only journey.
- Offline conversions remain in the detail report. Unknown identities are not all joined into one shared profile.
- Full detail retains date, source, medium, account, campaign/adset/ad IDs and names, UTM content/term, landing host/path and currency.
- Account and currency are deliberate additional isolation dimensions compared with the existing Dataform attribution mart. Null IDs remain distinct from empty strings. This is not an unchanged output schema.
- The paid-ad report follows the existing paid mart: case-sensitive source/medium filter, sums paid variants, groups across paid media, and labels output medium `cpc`. The underlying paid-weight eligibility is case-insensitive and requires non-null campaign/ad IDs. Null-ID groups therefore have zero paid credit.
- Exact session IDs are retained for global distinct counts. They increase storage and query memory; the scale test measures that cost.
- Numeric text survives ingestion without a changed content hash. Query arithmetic remains Float64, with measured tolerances. This is not exact decimal arithmetic.
- Data and integrity metadata flow from one commit scan. Missing chunks remain visible through metadata sentinels even outside the date window. Conflicting version transactions cause the reader to reject the report.

## Executable checks

Use Node 22 with `--experimental-strip-types` where the test imports existing TypeScript helpers.

```
node experiments/complete-attribution/test-engine.mjs
node --experimental-strip-types experiments/complete-attribution/test-contract.mjs
node experiments/complete-attribution/test-service.mjs
node --experimental-strip-types experiments/complete-attribution/test-persisted.mjs
node --experimental-strip-types experiments/complete-attribution/test-persisted-scale.mjs
node --experimental-strip-types experiments/complete-attribution/test-commit-conflicts.mjs
node experiments/complete-attribution/test-bigquery-reference.mjs
```

The persistence scripts accept only the dedicated `v1_attribution_proof` branch belonging to the expected workspace. Schemas are in `schemas/`. V1 tables record abandoned proof formats; the active client uses V2. Native builds were made from a temporary full schema project. No connector was attached to the proof tables.

`test-persisted-scale.mjs <existing-scenario>` reuses stored synthetic data and repeats the report/reference comparison without another import. Its publication time is then null. Report timings exclude source ingestion, profile computation and provider fetching.

The BigQuery reference script extracts the actual Dataform joined-through-weighted SELECT and feeds synthetic literals. It runs no Dataform workflow and reads no production tables. The authenticated comparison passed against 25 synthetic profiles, 375 touchpoints and 469 conversions. All 89 report rows matched across 144 named metrics and exact session counts, with maximum absolute numeric difference 3.03e-9. BigQuery processed zero source-table bytes. This proves the extracted attribution SELECT on these fixtures, not source normalization or ad-name enrichment.

## Remaining production proof

The prototype does not establish production completeness or a one-to-fifteen-minute SLA. Required gates remain:

- Compare real normalized source/session outputs and all metrics with the known-correct Dataform results. The extracted attribution SELECT has now passed the synthetic BigQuery comparison.
- Connect committed Fivetran/GCS revisions to normalized conversion changes, with source deletes, corrections and multi-table dependency replay.
- Connect session output and ordered identity publications to a durable affected-profile index. Test crashes, restart recovery, stale workers and out-of-order deltas.
- Integrate the existing authenticated Google/Meta query services, verify all requested accounts/pages and account time zones, and measure API latency and freshness. `report-service.mjs` currently tests an adapter contract with fixtures, not live providers.
- Measure continuous updates and report concurrency on representative real profile distributions. Bound manifest size, profile history size, old-version retention and garbage collection. One profile with millions of events can still exceed an external worker's memory.
- Validate ad-name enrichment and historical name corrections. The core prototype receives normalized names; it does not reproduce the Dataform latest-name lookup.
- Finish and verify identity backfill, then enable production models through the existing activation checks. No backfill status was polled for this proof.

Tinybird's Events API acknowledgement is documented at https://www.tinybird.co/docs/forward/ingest-data/events-api . The experiment still verifies visibility and content rather than treating an acknowledgement as a complete report publication.
