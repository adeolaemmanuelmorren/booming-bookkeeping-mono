# Reporting facts CDC — spec and deploy runbook

Date: 2026-09-01. This closes the staleness the parity check exposed: journey builds were incremental, but the fact tables they read were frozen migration seeds (touchpoints stop Aug 28; conversion facts keep one superseded form). This is the third application of the seed + delta pattern — identity, journeys, now facts.

## Design

**Base tables stay frozen. Changes land in versioned delta journals.**

- `mart_touchpoints_all_fact_deltas` and `reporting_conversion_fact_deltas`: same layouts as the v3/v2 fact tables plus `row_version`, `is_deleted`, `batch_id`, `committed_at`. Sorted by anchor key (deltas keep the pruning contract). ReplacingMergeTree on `row_version`.
- The journey window-build pipe now reads base ∪ deltas per scoped node, takes the latest `row_version` per (anchor, entity), and drops `is_deleted` rows. With empty deltas it returns exactly what it returned before — the deploy is backward-compatible.

**A new `ReportingFactsCoordinator` Durable Object keeps the deltas current:**

1. Every cycle it reads *changed entities* from the raw version tables, windowed by `source_ingested_at` cursor: visitors with new page views (`reporting_cdc_changed_visitors`), and client form / client order entities (`reporting_cdc_changed_conversions`).
2. It rebuilds **whole entities**: a visitor is fully re-sessionized (`reporting_touchpoint_facts_cdc_build`, scoped by literal anonymous/user ids — bloom indexes added to `jitsu_page_view_versions`; form/order entities rebuilt via `reporting_conversion_facts_cdc_build`, scoped by `__tb_state_key`, which is already the sort key). The journey paging lesson is baked in: an entity's data is never split across builds.
3. It diffs the rebuild against stored heads (`reporting_touchpoint_fact_heads`, `reporting_conversion_fact_heads`) and **tombstones what the rebuild no longer produces**. This matters twice: `session_id = MD5(visitor_key | session_number)`, so a late page view renumbers a visitor's later sessions and the old ids must die; and a conversion whose anchor changed (new email) gets its old row tombstoned *under the old anchor* — the exact "superseded server form" parity bug.
4. **Late data triggers journey repair.** Before a window's cursor advances, every affected anchor key is resolved to current profiles and pushed into the journey coordinator's existing repair queue. Fresh facts with stale journeys would just recreate the disease one layer up.
5. Cursor semantics: changed-entity reads are ordered by `last_ingested_at` ascending; the cursor advances to the window's high-water mark minus one second, so a page cut mid-timestamp re-processes instead of skipping. Rebuilds are idempotent, so overlap is free.
6. Same operational contract as the journey DO: 429/5xx/timeout are backpressure (Retry-After honored, checkpoint kept); `failed` is for correctness errors; `GET /reporting-facts/status`, `POST /reporting-facts/recover`.

**The backfill is the cursor's starting position.** Both cursors initialize to the immutable seed cutoff (`2026-08-26 23:05:00`). The first pass replays everything since the seed through the normal CDC loop — no separate backfill tooling, and it emits the journey repairs for the whole gap as it goes.

**Raw queue compacts itself.** The publication coordinator now auto-runs the existing backlog compaction (latest run per slot, widened export overlap, active-generation slots protected) whenever queued runs exceed 3× the slot count and no publication is active. The 139-run pileup can't recur; a backlog degrades to one widened catch-up run per slot.

## Deploy sequence

1. **Tinybird first**: two delta datasources, bloom indexes on `jitsu_page_view_versions` (anonymous_id, user_id), six new endpoint pipes, the updated `reporting_profile_journey_window_build`. Safe before the Worker: empty deltas change nothing.
2. **Worker** (`npm run check && npm run deploy`): new DO class (migration v4), cron tick, routes, auto-compaction.
3. Watch `/reporting-facts/status`: cursors should march from Aug 26 toward now. Journey repairs will flow as windows complete.

## Verification gates

1. `pipe_stats_rt` for `reporting_touchpoint_facts_cdc_build`: read_rows proportional to the visitors passed, not the page-view table.
2. Cursors reach ~now (both streams) with `lastError` null.
3. Re-run the 50-profile parity: touchpoints past Aug 28 present; the superseded form gone from Tinybird totals.
4. Re-check the 1-in-50 outlier profile — likely fixed by the above; if not, diagnose it, don't amnesty it.
5. Queue: watch one scheduled cycle confirm `raw_backlog_autocompacted` fires if queued > 30 and the queue stays bounded.

## Explicitly not covered yet (extend with the same pattern)

- **Server-side sources are still frozen**: `mart_form_submissions_server_side_facts_current` (ActiveCampaign registrations) and `all_stripe_payments_current` (Stripe/Kajabi payments) are bootstrap snapshots. If parity still misses *server* payments or forms after client CDC lands, this is why. The extension is mechanical: two more branches in the changed-entities endpoint, two more scoped node groups in the build pipe, reading from the live adapter pipes.
- Journey versions compaction (unchanged roadmap item).
- Read-side cutover of the three snapshot Copies (unchanged; gated on parity).
