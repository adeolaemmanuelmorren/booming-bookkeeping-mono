# Identity Worker unblock spec

Date: 2026-08-28. Decision: the coordinator Worker owns identity graph computation. Tinybird stores state, serves literal-key reads, validates written rows, and gates visibility with activation markers.

## Why this is the stable boundary

Production measurements ruled out Tinybird Copy jobs as the graph engine:

- The same SQL ran about five times slower as a Copy than as a read-only query.
- Dynamic `IN (subquery)` filters read 6.2M seed rows for fact lookups and 9.6M reverse-index rows for identifier lookups.
- A literal point lookup read 2,048 rows.
- Retry output accumulated in append-only staging tables.
- Final compaction needed on-demand compute even after 32 fact shards.

The Worker design depends only on observed, stable contracts: literal primary-key endpoint reads, append-only Events API writes, deterministic row hashes, manifest validation, and activation-marker gating.

## One batch loop

Every recurring generation first pages each of the eight normalized source producers
through `identity_worker_source_facts`. The overlap cutoff is fixed for the generation,
pages contain at most 500 facts, and the Worker compares those literal fact keys with
the activated heads before appending actual changes to `identity_events_cursor`.
There are no recurring Tinybird Copy jobs.

Every backlog and steady-state graph batch then uses the same path:

1. Read up to 1,500 pending facts after the active cursor.
2. Read current fact heads for those literal fact keys and classify actual changes.
3. Read mappings and touched profiles by literal keys.
4. Expand retained fact keys through the reverse evidence index, again with literal keys.
5. Read narrow retained fact fields. Full retained payloads stay in Tinybird; endpoints return evidence, timestamps, and extracted names.
6. Compute exact connected components, winner IDs, mapping/profile history, fact overlays, splits, merges, and tombstones in the Worker.
7. Append deterministic fact, evidence, mapping, and profile journal rows.
8. Read those exact literal keys back and reject missing or conflicting rows.
9. Append a narrow batch-output index and deterministic manifest.
10. Validate count and hash through the batch-keyed manifest endpoint.
11. Append the activation marker last. Until this step, none of the new journal rows are visible.

Retries rerun the same cursor window, batch ID, row hashes, and output hash. Exact duplicates collapse logically during verification and manifest validation. A conflicting retry fails closed before activation.

## Implementation

- Worker graph engine: `cloudflare-workers/bigquery-tinybird-sync/src/identity-engine.ts`
- Worker batch orchestration: `cloudflare-workers/bigquery-tinybird-sync/src/identity-worker.ts`
- Tinybird client: `cloudflare-workers/bigquery-tinybird-sync/src/tinybird-api.ts`
- Coordinator state machine: identity phases are `enqueue`, `compute`, `validate`, and `activate`.
- The old enqueue and compaction Copies remain checked in only as rollback evidence and are excluded from automatic execution.
- Tinybird literal-key endpoints are prefixed `identity_worker_`.
- `identity_batch_output_rows` is sorted by batch and keeps manifest validation bounded as the serving journal grows.

## Correctness contract

- Winner order is identifier priority, first observation time, then identifier value.
- Profile ID is lowercase MD5 of the winner identifier key, matching the BigQuery seed.
- Fact version, deletion, and same-version correction behavior matches the deployed compaction contract.
- Removing a bridge recomputes the full affected prior profile and can split it exactly.
- Historical profile IDs flatten across merges.
- Journal row hashes and manifest hashes match the former SQL serialization.
- Only an exact activated `(tenant, batch version, batch ID)` changes visible graph state.

## Resource envelope

Measured steady-state input is about 880 events per ten minutes, with roughly 520 touched profiles at p99 and about 39 facts per profile at p99. Reads and writes are chunked at no more than 500 literal keys or rows per request. The Worker has a 30-second CPU allowance and 128 MB memory; network waits do not consume CPU time.

## Production repair and source coverage

The immutable seed at `2026-08-26 23:05:00 UTC` matched BigQuery exactly at
9,219,703 facts. The first post-seed drain exposed a source-feed gap: Tinybird had
received 61,047 of the 138,502 net facts added by the frozen BigQuery snapshot.
The repair compared 139,007 normalized candidates through literal fact-head reads,
skipped 56,972 already-current rows, and appended 82,034 upserts plus one deletion.
The replay input hash is
`c49d902d3baf878d50d116d7803a7f5877aeaff92d83830705dea970c410099e`.

The same replay-safe tool later reconciled the complete 12:01 BigQuery/Dataform run
while the first replay was still active. It pages pending cursor rows before reading
active heads, so a concurrently activated fact is present in at least one snapshot.
The final dry run and execute run both selected exactly 1,088 rows and produced the
same input hash,
`996d83bea5a4fbc721815d1e356fe80799efdd6781c8995cc2d42ad1f8f90c0a`.
The coherent source cut used for the first multi-surface comparison is
`2026-08-28 12:04:51 UTC`, after the same Dataform run finished facts, profiles,
and profile lookup. That preserved run proved exact mappings and reverse evidence.
Profiles had 88 trait differences, and the logical-fact query timed out; it was not
a four-surface pass.

The source coordinator now runs one of ten raw slots every minute. Ten completed
slots form one frozen generation, and subsequent arrivals wait for the next
generation. Interrupted raw runs are returned to the queue after a five-minute
lease. Identity publication runs only after all ten slots and their imports finish.

## Release gates

1. Worker engine fixture, split/merge, fact winner, retry, and hash tests pass.
2. Tinybird project checks and cloud deployment validation pass.
3. Live literal-key endpoints show index-pruned reads.
4. One frozen batch produces the same manifest as the retired SQL implementation, or any difference is explained and approved.
5. Activate one batch, compare Tinybird identity state with BigQuery, then drain the backlog through the same loop.
6. Run two recurring cycles with no compaction Copy submissions and sub-hour freshness.

## Rollback

Before activation, do not write the activation marker. Unactivated Worker rows are
invisible, so the enriched seed and last activated journal batch remain the serving
state.

After activation, activation markers are immutable and must not be deleted. Roll
back the Worker to the last known-good Cloudflare version and pause its recurring
trigger. Read the affected fact keys from the bad batch's frozen cursor window,
then replay those facts with a higher source fact version through the normal pending
Facts API. The known-good Worker computes a compensating batch with a higher batch
version. Validate its manifest and journal counts before activating it, run the
parity checks, and only then restore the recurring trigger. The compensating batch
supersedes the bad state while retaining the complete audit trail. The retired Copy
assets are preserved but cannot run automatically and are not the rollback path.

## Final production proof — 2026-08-28

- The original repair generation activated 101,915 pending facts through 68 nonempty manifests plus one terminal empty manifest. Every manifest count and output hash matched its activated journal rows.
- Frozen BigQuery fact reconciliation at `2026-08-28 13:01:18 UTC` compared 166,827 post-seed candidates and returned zero missing or queued rows after the final seven-row repair. The retained dry-run summary is `tinybird-production/.identity-parity-evidence-worker-engine-final-1304/generation_raw_20260828114600000_slot_6/fact-gap-readback-after.json`.
- The earlier common-cut digest proved exact mappings and reverse evidence. Profiles had 88 name-trait differences, while its logical-fact surface timed out. The Worker now ranks each non-empty name by that name fact's timestamp, and the retained literal readback `tinybird-production/.identity-parity-evidence-worker-engine-final/generation_raw_20260828111400000_slot_3/profile-trait-readback-after.json` confirms all 88 profiles have zero mismatches.
- The optimized logical-fact digest compares canonical stored payload hashes, observed timestamps, and evidence keys without JSON extraction.
- A later live digest stayed generation-stable and proved the complete manifest chain, but correctly reported Tinybird ahead of the hourly BigQuery cut by 4,676 facts, 6,420 evidence edges, 1,089 mappings, and 792 profiles. Those rows are post-cut freshness, not a graph-semantic mismatch.
- The final evidence is therefore component-wise common-cut proof plus a generation-stable live run, not one four-surface live pass: an hourly BigQuery snapshot cannot remain row-for-row equal while Tinybird continues its sub-hour feed.
- Recurring identity uses no Tinybird Copy jobs and no Tinybird on-demand compute.
