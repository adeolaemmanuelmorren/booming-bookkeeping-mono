# Reusable source pattern

This Boom project now has an executable source-registration pattern rather than
hand-maintained shared unions. It is intentionally local to this repository for
the first migration; KNYC can extract the same files into a versioned package
after the Boom parity gates pass.

## Stable interfaces

[`contracts/canonical-contracts.json`](contracts/canonical-contracts.json)
defines ordered columns, intended Tinybird types, required fields, and logical
grain for five boundaries:

- Boom-classified payment evidence
- lead evidence
- hourly ad delivery
- daily ad delivery, including native reach and frequency
- first-party identity observations

[`project/source-registry.json`](project/source-registry.json) registers the
concrete contract resources and their connection IDs. The renderer emits explicit
column projections for every union; it never generates `SELECT *`. It also emits:

- `registered_source_connections`, the non-derived connection inventory
- one required-field, connection, and grain check per fan-in entry
- the matching publication-readiness requirement for every generated check

```sh
cd tinybird
npm run render:registry
npm run check:registry
```

The generated resources are committed, so a review shows exactly how a source
enters a shared domain. Static validation rejects stale output, unknown or
unowned resources, undeclared contracts, duplicate registrations, and connection
IDs that do not belong to the declared source. Generated SQL fences dynamic
connection columns to the source's allowed IDs.

The rendered connection list carries one deterministic inventory digest. Adding,
removing, or renaming a connection changes that digest. The controller must start
a new source generation and record one immutable version-1 cutoff row for every
rendered connection; a changed cutoff is a new generation, not an amendment.

## Boom-to-KNYC extraction boundary

This repository proves a pattern; it is not yet a reusable package. After Boom's
executed parity gates pass, the package candidate is:

- canonical contract descriptors and explicit-column fan-in rendering
- registry ownership, connection fencing, inventory hashing, and generated checks
- generation/cutoff pinning and publication barriers
- identity journal algorithms after tenant and policy inputs are parameterized

The following stays in the Boom application:

- raw BigQuery schema captures and Fivetran connection bindings
- Stripe product classification and `boom_registration_policy`
- the hard-coded `boom` tenant filters that still exist in this first implementation
- Bill report products, endpoints, fixtures, and BigQuery parity hashes

Before KNYC extraction, rename or parameterize the Boom-classified payment
contract, remove single-tenant literals, add JWT tenant fencing and two-tenant
fixtures, and publish the shared files as one versioned unit. Until then, KNYC
should copy the design decisions, not import this directory as if it were a stable
library.

## Add a payment source

1. Add the raw Data Sources and one source-owned adapter.
2. Make its final node match `boom_classified_payment_v1`. Provider reconstruction
   can be shared, but the source's product/category rules must be an explicit Boom
   policy binding rather than a generic rules engine.
3. Give the connection a stable ID and register the adapter under
   `boom_classified_payment_v1`.
4. Render the registry. Required-field and grain coverage is generated. Add
   provider-specific semantic checks and fixtures when the canonical checks are
   not enough.

The generated payment fan-in changes automatically. Payment identity also changes
automatically because `payment_identity_contract` reads that fan-in. Shared identity,
session, attribution, and endpoint SQL do not change.

The two migrated Stripe adapters still contain their existing Boom classification
nodes. They are marked with `boom-payment-classification-v1` in the registry. This
keeps exact Dataform behavior; it does not pretend those IDs or names are portable
provider semantics.

## Add a lead source

1. Add its native adapter.
2. Add a thin `lead_evidence_v1` projection and, when it carries identity, an
   `identity_observation_v1` projection.
3. Register both projection resources under the source that owns them.
4. Render and add deletion, correction, and identity fixtures. Canonical grain,
   required-field, and connection checks are generated.

No shared fan-in or identity Pipe is edited. Bill registration profiling reads the
generated lead fan-in through `boom_registration_policy`, so another source can
participate by emitting `lead_type = 'krc'` or `lead_type = 'webinar'`. ActiveCampaign's
tag selection remains source-specific; the named Boom policy controls participation
in Bill acquisition after canonicalization.

## Add an ad source

Register hourly and/or daily contract projections. The shared ad fan-ins preserve
`tenant_id`, `source_connection_id`, provider name, native hierarchy level, IDs,
names, delivery metrics, currency, and source revision. A provider without reach or
frequency emits nulls; those values are never reconstructed from another grain.

Bill's product reads the shared fan-ins and filters to Meta, preserving the current
report. Ad-name lookup uses the daily fan-in, which retains ads absent from hourly
delivery. A KNYC product can select multiple registered sources without changing
the fan-in compiler.

## Collision and tenant safety

Internal payment grain is `(tenant_id, source_connection_id, payment_id)`. A legacy
manual link that names only `payment_id` must resolve to exactly one connection or
the build fails. Identity fact and shadow-observation keys length-prefix connection,
source system, and record key, so equal provider IDs from separate connections do
not overwrite one another.

Boom remains intentionally single-tenant at the serving layer. The reusable
contracts carry tenant lineage, but KNYC still needs its own JWT tenant fence,
two-tenant fixtures, policy bindings, and deployment configuration.
