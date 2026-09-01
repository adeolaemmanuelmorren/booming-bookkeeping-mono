# Identity worker-compactor spec

Date: 2026-08-30. Supersedes the Copy-pipeline identity phases in TINYBIRD_IDENTITY_UNBLOCK_SPEC.md (items 1–4 of that spec are done; its budget rules still apply to what remains).

## Decision

Identity compaction moves into the coordinator Durable Object. Tinybird keeps storage and serving only: raw events, seed, journal, activation gating, read pipes. No Copy jobs remain on the identity path.

Rationale (measured): Copies run ~5× slower than read-only queries; dynamic IN-subqueries do not prune (9.55M rows) while literal keys do (2,048 rows); retries duplicate staging rows. The Worker loop uses only contractual behavior: literal-param endpoint reads, Events API appends, activation markers.

## The loop (one DO alarm cycle, identical for backlog drain and steady state)

1. Read pending batch (existing `readPipeRows` + cursor/pending pipes). Freeze deterministic `batch_version` / `batch_id` (existing scheme).
2. Fetch affected state via parameterized endpoint reads, keys as **literals**, ≤ 500 keys per request, narrow columns:
   - mappings by identifier_key
   - profiles by profile_id
   - retained fact heads by identifier_key via the reverse evidence index (keys + evidence + timestamps + kind only — never `fact_payload`; changed payloads arrive in the batch itself)
3. Compute in JS: union (DisjointSet), winner selection, fact heads, journal row diff.
4. Append journal rows via Events API (NDJSON, chunked ≤ ~5 MB per request, all rows carry the batch id), verify the manifest endpoint matches expected counts/hash, then append the activation marker (existing `appendIdentityActivation` + `validateManifestForActivation`).

## Work items

1. **Shared identity lib.** Extract DisjointSet, winner ordering, normalization, and fact-head semantics from `tinybird/scripts/identity-oracle.mjs` into one module consumed by BOTH the coordinator and the parity scripts. One implementation; the engine cannot disagree with the checker.
2. **Lookup endpoints.** New/adjusted pipes taking literal key-array params for mappings, profiles, and retained-fact heads. Gate: `pipe_stats_rt.read_rows` scales with keys requested (thousands), not table size (millions).
3. **Coordinator.** Replace identity phases `prepare|scope|facts|components|compact` with one `compute` phase implementing the loop. Keep: enqueue Copies (they work), batch-id scheme, manifest validation, activation append, recovery actions. Idempotency: any failure before activation leaves the batch invisible; re-run recomputes the same batch id; duplicate unactivated journal rows are inert.
4. **Delete.** The five compaction Copy pipes, their staging datasources, shard parameters, and the `p_fact_shard*` plumbing. Retire via the existing `RETIRED_IDENTITY_COPY_SET` mechanism.
5. **Drain then cadence.** Drain the backlog with large cycles (5k–25k events) through the same code path. Then enable the recurring schedule at 5–10 min.

## Guardrails

≤ 500 keys/request; ≤ 1,000 subrequests/invocation; no fact payloads in retained reads; measure DO CPU per cycle (30 s allowance — alert at 10 s).

## Acceptance gates

1. Pruning: every lookup endpoint reads O(keys) rows, verified in `pipe_stats_rt`.
2. Cycle time: ≤ 60 s wall for a 5k-event batch, ≤ 15 s for steady-state (~1k events).
3. Parity: drained state passes the existing BigQuery parity scripts at a quiet cutoff.
4. Drain: manifest `inputEventCount = 0`; activated batch version > seed version.
5. Stability: two consecutive recurring generations complete clean, identity activating each cycle.

## Non-goals

Re-seeding as transport; pending overlay (read-time recompute); click-ID bindings; profile splits; plan resize.
