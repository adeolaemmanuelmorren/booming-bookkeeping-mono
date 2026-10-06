# Browser source normalizer

Copy `worker/browser/` and `test/browser.test.ts` into the V1 project. The modules import the existing session `PageView`, `PageRevision`, timestamp parser, visitor-key rule, and canonical JSON helper. The local session copies only make this isolated test directory executable.

`normalizeJitsu(observation)` accepts the existing ingress observation shape. `normalizeHistorical({tenantId, source, kind, record, ingestedAt, deleted?})` accepts raw flat rows from `jitsu_data` or `boom_domains`. Both return `{source, pageRevision, identity}`. Calls are async only for SHA-256 hashing.

## Contracts

- Source priority is live Jitsu 3, historical Jitsu 2, Boom domains 1. Page and identity records carry it. Select heads by priority first, then source revision. Keep tombstone heads. Reject conflicting content at the same priority and revision.
- Source revision is the source update timestamp in exact microseconds plus one. A missing source timestamp has revision zero. Original ingress `source_fact_version` is not reused because its old parser discards microseconds. The raw payload retains the original timestamps and identifiers. Normalized timestamps are UTC with six decimal places, so timestamp strings compare chronologically.
- Page and browser identity IDs retain Dataform's original `COALESCE(message_id, id)` identity. Empty IDs fail explicitly rather than taking a different fallback. Live records lacking an upstream ID use the existing ingress-generated message ID.
- Pageviews contribute anonymous and user identifiers only. Identifies, forms, orders, and attribution events follow the distinct fallbacks in `int_identity_events`. Unknown event kinds remain source facts without adding graph edges.
- Identity includes canonical email evidence and exact North American phone filtering from Dataform. No additional email validation, international phone fallback, or probabilistic matching is added.
- Missing observed time remains null. Dataform includes such facts in identity. The identity engine needs nullable times and null-aware winner ordering. Arrival time is operational metadata only.
- If a correction changes event kind, the caller must retract the previous kind's derived page or identity record. A new non-page kind does not produce a page tombstone by itself.

## V1 boundary

Page traffic fields retain observed values and source URL extraction. The raw payload retains all additional campaign parameters, click evidence, and referrer data. Paid-click classification, traffic-channel classification, ad-network names, and campaign-type enrichment are not computed. Those PageView fields are null. Therefore this module claims parity for source identity, timestamps, visitor keys, device/geo fields, raw UTM/click extraction, and identity evidence, not every enriched `stg_page_views` output column.

Use raw exported source tables for historical input. The old `migration_seed/jitsu_page_view_versions` parquet is enriched staging output and cannot reconstruct original context fields or original raw UTM values.

Page membership, boundaries, counts, duration, and session IDs use the same session engine as live traffic. The 16 tests exercise source precedence, replay, late precision-sensitive boundaries, null times, corrections, deletions, source-specific identity, phone exclusions, email aliases, and Dataform's SQL URL parameter behavior. Host parsing covers [BigQuery's documented examples](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#nethost). Arbitrary malformed URL best-effort behavior still requires source-data comparison.

Pre-epoch update revisions, unsafe numeric identity revisions, invalid typed fields, and invalid source timestamps fail explicitly. The caller must retain and quarantine failures, never drop them or silently substitute ingestion values.

## Verification

Run `node --experimental-strip-types --test test/browser.test.ts`. Type-check with the project's existing TypeScript configuration. All checks passed in the isolated directory. No deployment or provider changes were made.
