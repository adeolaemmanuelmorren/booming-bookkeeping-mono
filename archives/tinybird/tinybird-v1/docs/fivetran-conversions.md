# Fivetran conversion facts for Tinybird V1

This package builds source facts only. It reads the nine approved Fivetran raw
tables from Tinybird, normalizes Stripe payments and ActiveCampaign
registrations, and publishes complete source-scope replacements. It contains no
attribution or reporting logic.

## Fixed baseline

The immutable baseline is `fivetran-20260905T222800Z` at
`2026-09-05T22:28:00.000000Z`.

`runBulkBootstrapAndSeal` processes three keyset streams:

1. `stripe_main`
2. `stripe_kajabi`
3. `activecampaign`

Each 5,000-scope page is hydrated with batched Tinybird queries. The bulk
publisher writes source records, immutable identity facts, and source commits in
byte-bounded uploads. It verifies each wave before the durable checkpoint moves.
After all three streams finish, it seals the identity fact count and ordered
SHA-256 manifest. Bootstrap publication never calls the live identity queue.

Use the existing conversion transforms as injected dependencies:

- `sourceRecords` from `worker/conversions/publication.ts`
- `conversionIdentityFacts` from `worker/conversions/identity.ts`
- `normalizeStripeChargeSnapshot` from `worker/conversions/stripe.mjs`
- `buildActiveCampaignContactReplacement` from
  `worker/conversions/activecampaign.mjs`

Keep the source coordinator paused until the source facts are committed, the
identity manifest is sealed, and the browser identity version 1 has consumed
that manifest.

## Incremental boundary

The transport receipt is the only completeness barrier. A row stamped at the
receipt's `target_end` belongs to `(previous barrier, target_end]`. Discovery
uses `_v1_observed_at > previous` and `<= target_end`; hydration includes every
row observed `<= target_end`. The machine consumes a complete verified batch,
including a long catch-up batch, then advances directly to its receipt boundary.

Raw state ordering is `(_v1_observed_at, _fivetran_synced)`. The source timestamp
is never rewritten. A later observation replaces prior state even when Fivetran
loaded a record with an older extraction timestamp. `_v1_deleted=true` records
physical absence at the verified cutoff; a later present row restores the ID.
Incremental source commits enqueue identity facts before committing the source cursor.

The raw transport uses BigQuery commit history and fixed-cutoff reads. Its first
verified batch bridges the immutable snapshot to the change-history starting
point. See `../fivetran-transport/README.md` for the nine-source barrier and
operator recovery procedure. The optional legacy reconciliation reader remains
inert; commit history supplies physical deletions in this deployment.

## Required Tinybird resources

- `tinybird/v1_bootstrap_conversion_identity_facts.datasource`
- `tinybird/v1_bootstrap_conversion_identity_manifests.datasource`
- the nine `v1_snapshot_*` raw tables
- the nine `v1_fivetran_*` live raw tables
- `v1_fivetran_source_receipts`
- `v1_fivetran_reconciliation_receipts`
- existing `v1_source_records` and `v1_source_commits`

## Verification

```sh
./node_modules/.bin/tsc -p tsconfig.json
/Users/adeola/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node --experimental-strip-types --test test/*.test.ts
```

The tests cover both Stripe namespaces, refunds, failed-charge tombstones,
ActiveCampaign fallback and removal behavior, same-version reconciliation
deletes, later resurrection, duplicate/late raw deliveries, ambiguous remote
writes, full-batch observation boundaries, idle final batches, and identity
manifest sealing.
