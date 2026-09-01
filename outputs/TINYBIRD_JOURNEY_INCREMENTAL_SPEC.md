# Incremental journey spec

Date: 2026-08-30. Identity is done — nothing here changes it. This covers the three slow reporting builds. They all share one root cause: every query re-derives every customer's journey from scratch. The shared fix is one stored **journey table**, built incrementally. Each mart's section says exactly what changes for it.

## The shared foundation: the journey table

One row per (conversion, earlier touchpoint of the same person), with the nine numberings (sequence number, non-direct count, paid-ad count, payment sequence, etc.) and the 40/20/40 weights already filled in as columns.

Built by **one SQL pipe** (not JavaScript): give it a list of profile ids (max 500), it returns finished journey rows for exactly those people — the touchpoint join and all nine window calcs, scoped to just them. Over a handful of profiles that's milliseconds; the same math was only slow because it ran over all of history every query. The Worker just calls this pipe and appends the answer via the Events API.

Kept current by three events:

- **New conversion arrives** → build that conversion's journey rows once. The past doesn't change, so they're final.
- **Identity merge** → the identity worker already outputs which profiles changed; rebuild only those profiles' rows as replacements (higher batch version, reads take newest — same trick as the identity journal). Nobody else's rows are ever touched.
- **One-time backfill** → run the pipe over every existing profile, 500 at a time, through the same path. This replaces the paid on-demand rebuild — don't buy that.

## Slow build 1: mart_conversions_with_touchpoints (14.18s)

This build *is* the expansion join plus the nine window calcs. It stops existing as a query.

- The expansion and the windows move into the build pipe above and run per-profile at write time.
- The endpoint that used to run this build now just reads stored journey rows. Expected: sub-second.

## Slow build 2: mart_conversions_multi_touch (times out at 20s)

The 144 sums were never the problem — summing is what ClickHouse is best at. They timed out because they ran on top of the live expansion.

- Point the aggregation at the stored journey table: plain `sum(metric × weight)` and group-by over precomputed columns.
- Measure it. Only if it still exceeds ~10s, add a materialized view to keep the sums current on ingest. Don't build the MV preemptively.

## Slow build 3: mart_revenue_attribution (blocked)

It was blocked because it re-ran the whole journey pipeline before its own work. Two changes:

- Read stored journey rows instead of recomputing them — the blocker disappears.
- The per-row processing it repeats every query goes upstream, once, at enrichment: **channel classification (regex/lowercase) is a property of the touchpoint itself** — classify it when the touchpoint fact is stored (same as stg_page_views → int_events_enriched), and the journey pipe just carries the channel column through. Decimal-safe revenue values likewise get computed once on the payment fact. The remaining query is a payments join plus sums.

## Ad performance: spend, clicks, impressions

Join at query time — never bake into journey rows. Spend arrives per (day, campaign, adset, ad) and the platforms restate it for days afterward; if it were distributed into journey rows, every restatement would force journey rebuilds — the disease we just cured, back from a second direction. Journey rows change for identity reasons only.

The query-time join is cheap because it's aggregate-to-aggregate, not row-level:

1. Sum journey rows by (date, campaign, adset, ad) → attributed conversions and revenue per ad-day.
2. Sum the ad table the same way → spend, clicks, impressions per ad-day. Tiny table, milliseconds.
3. Join the two small results on the ad-day keys. CPA/ROAS is division after the join.

## Touchpoint enrichment (the int_events_enriched pattern)

Split it in two:

- **At ingestion, store what never changes**: raw UTM values, click ids (gclid/fbclid), referrer, and the channel class derived from them. Frozen properties of the event.
- **At query time, join campaign/adset/ad metadata** (names, structure). Cheap: the ad dim is 100–1,000 rows — ClickHouse hash-joins a small right side in memory, so it adds ~nothing to the scan you're already paying. Cheaper still: group journey rows by campaign_id (or normalized utm_campaign when there's no id) first, then join the dim onto the few hundred aggregated rows.

Payoff: campaign renames cost nothing — names are never stored on touchpoints, so every report (historical included) shows current names with no rebuild. Same principle as spend: the ad table changes on its own schedule, so nothing that changes with it gets baked into stored rows.

The rule the whole design follows: **precompute what depends on identity (journeys) · store at ingestion what's a frozen property of the event (raw utms, click ids, channel) · join at query time what changes on its own schedule (spend, campaign names).**

## How we know it's right

1. The build pipe reads only the requested profiles' rows (pipe_stats_rt).
2. A normal cycle (new conversions + touched profiles) finishes in a few seconds.
3. All three report endpoints answer in ≤2 seconds.
4. Sums over the journey table match BigQuery's multi-touch marts at a quiet cutoff.
5. After one identity merge, only that merge's profiles have new rows.

## Scalability fixes after the 429 incident

Three defects caused it; fix in this order.

1. **The build pipe must prune — gate 1 is mandatory.** Today it filters on `profile_id`, a column *computed by joins*, so every call scans the whole touchpoint table (sort key is only `touchpoint_id`), the whole conversion table, and the full mapping delta. Fix: the Worker passes each profile's member **identifier keys as literals**; the pipe filters on stored anchor columns; re-key the tables so those columns lead the sort key (conversions by `identity_anchor_key`; touchpoints v3 by the anonymous/user anchor key). Join the mapping and compute profile_id only over that pruned subset, then run the windows on it. Nothing ships until pipe_stats_rt shows reads proportional to keys.
2. **429 is backpressure, not failure.** On 429/timeout/5xx: never enter `failed` — back off (honor Retry-After), re-alarm, resume from the existing checkpoint (`journey_profile_index` / backfill cursor already persist). Pace build calls (max N per minute) to stay inside 1 vCPU sustained. `failed` is reserved for correctness errors only.
3. **Decouple reporting from ingestion.** Journey compute moves to its own Durable Object, downstream of identity activation, consuming touched-profile lists. The raw/identity coordinator no longer references journeys. A reporting stall may delay reports — it can never block raw publication or identity again.

Buying more compute is rejected: with fix 1 the same batch costs orders of magnitude less; with fixes 2–3 hitting any limit degrades to slower, never down.

## Not doing

Full-history rebuilds. On-demand compute. Rewriting window math in JavaScript. Computing journeys at query time (that's the slow thing being deleted). Plan resize.
