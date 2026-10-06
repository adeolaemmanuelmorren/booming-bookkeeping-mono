# Boom analytics on Tinybird

This folder is the Boom-only replacement for the Dataform path used by Bill's
two reporting pages. It keeps the current public query shapes while replacing
the 60-model execution graph with source-owned adapters, an identity/session
domain, and one Bill report product.

It is the implementation candidate for the imported-facts approach described in
the [migration research](../outputs/research/2026-08-24-dataform-to-tinybird-migration.md):
Fivetran and Jitsu land data before a report request, and the request reads only
Tinybird. The request-refreshed ad-API approach remains design-only; this folder
does not contain its snapshot producer, completion ledger, or gateway workflow.

## Current status

| Area | Status |
| --- | --- |
| Native resources | Committed: 68 Data Sources, 82 transformation Pipes, 15 Copy Pipes, and 5 endpoints |
| Local evidence | Parser, structural validation, generated-schema checks, and offline rule oracles |
| Source ingestion | Not bound: real GCS URIs, immutable delivery contract, and completion controller are still required |
| Application integration | Not wired: the Bill Reporting API does not yet translate its public routes to these endpoints |
| Executed parity | Not run: Tinybird branch build, production-shaped replay, BigQuery hash comparison, and load tests remain gates |
| Approach 2 | Not implemented: no request-time provider fetch or completed versioned ad snapshot exists |

These files are therefore an implementation candidate, not a deployed analytics
system. The resource count is recorded for plan review; its Tinybird plan budget
has not been approved or measured in a branch.

The implementation deliberately has two identity update paths:

- The identity-resolution endpoint overlays a bounded pending batch on stable state.
- Bill reports read only a fully built, generation-pinned identity/session snapshot.
- A correction path rebuilds a shadow universe before publishing deletions,
  identity splits, or other changes that cannot be undone by a merge-only graph.

This is the selected design because a merge-only identity graph would silently
change current Dataform behavior.

## Public serving contract

The trusted reporting API is designed to call these Tinybird endpoints:

1. `bill_report_generation` to obtain a consistency token.
2. `bill_report_window` for the four current Pacific report bounds.
3. `bill_live_acquisition` for live registration and Immediate VIP rows.
4. `bill_5k_performance` for purchaser rows and fixed $4,997 values.
5. `bill_meta_delivery_daily` for daily, hierarchy-native reach and frequency.

The four report result shapes are recorded in
[`contracts/endpoint-contracts.json`](contracts/endpoint-contracts.json). The
generation token is private orchestration data and does not change the browser
payload.

Each published token pins one immutable validated build: the exact KRC, hourly,
daily-delivery, and Meta-bound materialization attempts plus their source
manifest, connection-inventory, cutoff-set, and policy digests. Product retries
append new attempts; existing tokens never start reading those newer rows.

## Project layout

```text
contracts/     Exact source, endpoint, and BigQuery oracle contracts
datasources/   Raw source schemas plus control, domain, and versioned state
pipes/         Source adapters, named policies, identity/session logic, and report products
copies/        Explicit state compaction and correction jobs
endpoints/     The narrow API-facing query surface
fixtures/      Synthetic, non-PII edge cases
tests/         Tinybird endpoint scenario declarations
project/       Source registry, policy, coverage, and publication plan
scripts/       Offline rendering, oracles, and structural validation
```

## Safe local checks

Every Data Source and Pipe parses with the locally installed Tinybird parser.
The project has not been built in a Tinybird branch, executed, or deployed.

These checks are local and do not call Tinybird or any provider API:

```sh
cd tinybird
npm test
npm run plan:generation
```

`npm test` includes the installed Tinybird parser. If the CLI lives outside the
standard `uv` tool directory, set `TINYBIRD_PYTHONPATH` to its `site-packages`
directory.

The live schema drift command is read-only but does query BigQuery metadata:

```sh
cd tinybird
npm run check:schemas:live
```

## Deployment boundary

The committed GCS connection contains only a secret reference. Bucket URIs are
not guessed. Before a branch build, prove that Fivetran produces immutable flat
CSV, NDJSON, or Parquet objects with stable keys, versions, deletions, replay
behavior, and a signed completion manifest. Then bind each raw Data Source to
its actual URI with `IMPORT_SCHEDULE @on-demand`.

No report request calls Meta, Google, Stripe, ActiveCampaign, Fivetran, or GCS.
Jitsu writes events to Tinybird's Events API; report reads stay inside Tinybird.

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the invariants and
[`PARITY.md`](PARITY.md) for the release gates. The exact controller sequence is
in [`ORCHESTRATION.md`](ORCHESTRATION.md), and the add-a-source contract is in
[`REUSE.md`](REUSE.md).
