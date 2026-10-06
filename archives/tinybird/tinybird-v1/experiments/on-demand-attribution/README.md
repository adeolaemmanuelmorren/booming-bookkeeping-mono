# Query-time attribution experiment

Read-only synthetic queries executed in the production Tinybird workspace on September 6, 2026. No tables, pipes, graph state, or backfill settings were changed. Queries ran sequentially with a 20-second execution limit and a 512 MiB requested memory cap.

## Results

| Synthetic touchpoints | Conversions | Profiles | End-to-end time, two runs |
| --- | --- | --- | --- |
| 10,000 | 1,500 | 500 | 0.171–0.172 s |
| 100,000 | 15,000 | 5,000 | 0.302–0.305 s |
| 1,000,000 | 150,000 | 50,000 | 1.481–1.510 s |
| 1,000,000 | 15,000 | 5,000 | 1.316–1.444 s |
| 100,000 | 150,000 | 500 | Memory-limit failure |

Each scale query resolves separate touchpoint and conversion identifiers through a supplied identity mapping, joins earlier touchpoints to conversions, computes first-touch, last-touch, and 40/20/40 credit, and aggregates revenue and conversion credit. The mapping is synthetic input representing an externally computed graph. Tinybird performs no graph computation.

The failure case has 200 touchpoints and 300 conversion events per profile. The multiplication of matching rows overwhelms the query despite having fewer touchpoints than the successful million-row cases. It does not establish a general Tinybird capacity limit, and the benchmark does not establish cost per report.

## Correctness checks

The fixture's expected credit values are written explicitly, independently of the SQL. All 15 expected conversion/touchpoint rows matched. It checks:

- First direct touch is retained; subsequent direct touches receive zero credit.
- One eligible touch receives full credit; two receive halves; three receive 40/20/40.
- Touchpoints after conversion time are excluded; equal times are included.
- Touchpoint IDs break equal-time ordering ties.
- Conversions without earlier touches receive offline credit, including a profile with only a future touch.
- Multiple conversions for a profile have separate journeys.
- A conversion supplied with zero current net revenue contributes zero revenue.
- Click-date filtering occurs after weighting, so earlier touches outside the report window still affect credit and conversions after the report window can contribute.
- All three models conserve total conversion credit in the successful scale cases; multi-touch conserves the supplied net revenue.

The core attribution rules were checked against both local Dataform copies' `mart_conversions_with_touchpoints.sqlx`. Both use all prior touchpoints without a fixed lookback. Adding a 30-day lookback solely for speed would change those results. This experiment is not full Dataform parity validation.

Mock ad results were merged after aggregation. A spend-only ad remained present and total spend was preserved without multiplication. No real ad API request, pagination, latency, quota, or freshness was tested. The fixture uses one account/date window/currency; production merge keys must also include account, reporting date and the relevant currency/dimension boundaries.

## What remains untested

Inputs come from `numbers()` and inline literals, not persisted production sources. These timings exclude production disk reads, wide rows, current-version selection, deduplication, real identifier strings, identity journal lookups, and ingestion lag. The supplied mapping is complete and one-to-one per identifier; missing mappings and actual graph changes need production correctness checks. The test also does not build sessions or normalize conversion source records.

Only two sequential samples were taken, during the background backfill. They are not p95 estimates or concurrent dashboard capacity tests. Real external API fetching remains separate from the single Tinybird attribution query.

The next meaningful test needs completed identity plus prepared session/touchpoint and conversion records. Measure real per-profile touchpoint/conversion counts and test the largest journeys, typical report windows, and concurrent requests. Preserve full-journey weighting while restricting candidates to profiles relevant to the requested report. If dense journeys remain expensive, evaluate a different query shape or selectively precompute attribution for those journeys before increasing resource limits.

## Reproduce and inspect

Run `node experiments/on-demand-attribution/run.mjs` from `tinybird-v1` with existing Tinybird authentication. It creates no remote resources. The generated SQL, first-run results, final evidence, and run log are in `results/`. Failed queries stop the scale sequence; the final deliberate stress failure is recorded in `evidence.json`.

## Follow-up: ASOF and shared journeys

`test-journey.mjs` tests two read-only rewrites under the same execution and memory limits. Both obtain each conversion's latest prior touch timestamp with ASOF, then group conversions by profile and that timestamp before calculating credit. Offline conversions retain their individual conversion times for click-date grouping.

The initial array variant builds ordered profile arrays, filters each distinct journey boundary, and expands the grouped journeys. It passed the fixtures but hit memory limits on both the million-touchpoint case and the repeat-conversion stress case. Arrays are not a demonstrated solution here.

The final default variant uses ordinary joins and window functions after ASOF grouping, without journey arrays:

| Synthetic workload | Original query | ASOF + grouped relational query |
| --- | --- | --- |
| 10K touches / 1.5K conversions / 500 profiles | 0.17 s | 0.23 s |
| 100K touches / 15K conversions / 5K profiles | 0.30 s | 0.45 s |
| 1M touches / 150K conversions / 50K profiles | 1.48–1.51 s | Memory-limit failure |
| 1M touches / 15K conversions / 5K profiles | 1.32–1.44 s | Memory-limit failure |
| 100K touches / 150K conversions / 500 profiles, repeated boundaries | Memory-limit failure | 0.38–0.42 s |
| 100K touches / 100K conversions / 500 profiles, distinct boundaries | Not tested | Memory-limit failure |

Successful scale outputs match the original benchmark's first-, last-, and multi-touch conversion totals and multi-touch revenue by source/campaign/ad. The repeated-boundary stress output matches the proportional reference from the successful 200-touchpoint journey baseline. The distinct-boundary control deliberately removes work-sharing opportunities; it still exceeds the memory cap.

An independent JavaScript row-by-row oracle also validates the full fixture, a click-date window, and a revised fixture with a supplied identity merge plus a late touch. These add checks for touch eligibility time differing from session ordering time, multiple touches at the same eligibility timestamp, and offline conversions on different dates. Identity mappings are supplied test inputs; no graph is computed in Tinybird. Full production parity remains unverified.

This demonstrates a useful optimization for repeated journeys, with regressions on other workloads. It does not justify replacing the original query everywhere. A workload-selection strategy, persisted lookup optimization, or bounded execution approach would need its own test before adoption.

Run `node experiments/on-demand-attribution/test-journey.mjs` to reproduce. `--stress-only` runs correctness fixtures and the two stress cases. `journey-query.mjs` exports the query builder; its `useArrays` option retains the unsuccessful variant for inspection. Evidence, generated final SQL, and saved earlier results are in `journey-results/`. All queries are synthetic and no production schema, identity worker, or backfill configuration was changed.

## Follow-up: aggregate states for first touch

`test-states.mjs` exercises `argMinState` grouped by simulated insert batch and profile, then `argMinMerge` grouped by profile. All operations execute within read-only SQL queries. This tests aggregate-state semantics and early aggregation, not an actual persisted AggregatingMergeTree or materialized-view deployment.

Verified behaviors:

- Separate batches combine to the correct earliest touch.
- An earlier touch arriving in a later batch replaces the previous minimum.
- Timestamp plus touchpoint ID gives deterministic tie handling; identical retries preserve this minimum.
- Appending a correction does not retract the old minimum. The counterexample stays stale until the affected profile's state is rebuilt from current source facts.
- A lifetime minimum is not generally equivalent to Dataform attribution. Dataform tests eligibility using touch time, but ranks by session start. A touch with an earlier session start can arrive later than a conversion; another touch may be eligible for that conversion. Keeping only the lifetime minimum loses that alternative. The fixture explicitly demonstrates this failure.

First-touch-only timings, one sequential sample each:

| Synthetic workload | Expanded-join first touch | Build/merge states and join one summary per profile |
| --- | --- | --- |
| 1M touches / 150K conversions / 50K profiles | 0.841 s | 0.546 s |
| 100K touches / 150K conversions / 500 profiles | Memory-limit failure | 0.165 s |

The benchmark cases use matching session/touch timestamps, complete current identity mappings, and conversions after each profile's first touch. Both compact-summary outputs match the expected 150,000 conversions and 750,000,000 cents; the successful expanded-join output matches exactly. The corrected-source and time-eligibility counterexamples are separate tests of the shortcut's limits.

These are not three-model attribution timings. They cannot be compared as equivalent work against the earlier 40/20/40, first-touch and last-touch benchmarks. State preparation is included in the measured query, and persisted-state read performance, correction propagation, identity regrouping and concurrent requests remain untested. No full attribution solution is deployed or proven by this experiment.

Run `node experiments/on-demand-attribution/test-states.mjs`; SQL and results are in `state-results/`. A viable production design must retain enough history or time-specific summaries to preserve eligibility and must repair affected state after corrections or identity changes. An append-only lifetime minimum alone fails those requirements.
