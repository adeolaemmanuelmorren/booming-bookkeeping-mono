# Journey paging fix — deploy runbook

Date: 2026-08-31. The code is written and tested (87 tests pass, `npm run check` clean). This is the deploy + verification sequence. Do not reorder.

## What changed (already committed to the working tree)

**The defect:** the journey handoff flattened every touched profile's identifier keys into one lexically sorted array and sliced it 500 at a time, so one profile's keys landed on different pages and journeys were built from partial history — silently wrong windows and weights.

**The fix — Worker (`cloudflare-workers/bigquery-tinybird-sync`):**
- `identity-worker.ts` — the queue row now carries `profile_ids` (from `affectedJourneyProfileIds`, which already existed) plus `identifier_keys` re-scoped to *orphaned* keys only (deleted mappings no current profile owns; new `orphanedJourneyIdentifierKeys`).
- `journey-worker.ts` — new `takeProfilePage`: packs whole profiles into ≤500-key pages, never splits one profile, throws on a single profile wider than the limit.
- `journey-coordinator.ts` — pages in strict order: conversion-only pages first, orphan-key pages next (each key re-verified against current mappings; still-mapped keys are dropped because their profile is rebuilt later), profile pages last so their higher batch_version wins any overlap. Expands profiles→keys at build time via the pruned `identity_worker_profile_heads` endpoint. Backpressure handling unchanged (429/5xx/timeout → backoff honoring Retry-After, resume from checkpoint). Idle poll stretched 60s → 10min. New `recoverFailed()`.
- `publication-coordinator.ts` — the dead `journey` phase no longer calls the journey DO at all (it only drains); after identity activation the coordinator ticks the journey DO (wrapped in try/catch — a journey failure can never fail publication).
- `http.ts` — new authorized routes: `GET /journey/status`, `POST /journey/recover`.

**Tinybird (`tinybird-production`):**
- `datasources/state/reporting_journey_identity_queue.datasource` — added `profile_ids Array(String)`.
- `endpoints/reporting_journey_pending_batch.pipe` — selects `profile_ids`.

## Deploy sequence

1. **Deploy the Tinybird changes first** (queue column + pending pipe). Old worker keeps writing rows without `profile_ids`; they read back as `[]`, harmless.
2. **Drain the handoff before deploying the Worker.** Pause publication (operator pause), let the journey DO reach `idle` with the queue cursor at head (`GET /journey/status`: phase `idle`, no active batch). A legacy-format row processed by the new worker is *safe but incomplete* (orphan verification drops still-mapped keys, so that batch's merge repair is skipped) — if one slips through, note its `batch_id` and re-run that repair by hand after deploy.
3. **Deploy the Worker** (`npm run check && npm run deploy`). Unpause publication.

## Verification gates — in order, before calling it done

1. **Pruning proof (mandatory, unchanged from the spec).** After the first real journey page, check `pipe_stats_rt` for `reporting_profile_journey_window_build`: `read_rows` must be proportional to the keys passed, nowhere near table row counts. If it is not, stop.
2. **One full identity cycle.** `GET /journey/status` goes `running` → `idle`, cursor advances to the activated batch, no `lastError`.
3. **Whole-profile spot check.** Pick one profile with 3+ identifier keys (anonymous_id + user_id + email) touched by that cycle. Its journey rows in `reporting_current_journey_base_rows` must match BigQuery's multi-touch mart for that profile at a quiet cutoff — touchpoint counts, payment numbering, weights.
4. **Failure isolation.** Confirm the publication coordinator completed its cycle regardless of journey state (the tick is fire-and-forget; `journey_tick_failed` in logs is acceptable, publication `failed` because of journeys is not).

## Recovery

- Journey DO stuck `failed` → fix the cause, then `POST /journey/recover`. `failed` now only means a correctness error (e.g. a >500-key profile, multi-profile conversion); 429/timeout/5xx never land there.
- Wrong or partial journey rows from before this fix: re-enqueue the affected identity batch's profiles (any later merge touching them also repairs them, since profile pages always rebuild whole profiles).

## Still on the roadmap (unchanged, see the Notion review doc)

Do not start these until all four gates above pass. The read-side migration points reports at the journey table, so the journey table must first be proven correct and cheap to maintain.

1. Point report endpoints at stored journey rows; delete the three snapshot Copies (`snapshot_mart_conversions_with_touchpoints` / `_multi_touch` / `_revenue_attribution`) — they still rebuild full marts from all history every cycle and will re-hit the 30s Copy cap as history grows.
2. Journey-versions compaction (seed + delta fold, same pattern as the identity journal) before the versions table dwarfs live rows.
