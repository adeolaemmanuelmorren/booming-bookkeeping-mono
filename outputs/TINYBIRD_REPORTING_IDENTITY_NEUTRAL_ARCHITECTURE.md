# Tinybird reporting identity-neutral architecture

## Caller contract

Reporting callers continue to query the existing model names and receive the
existing schemas. `profile_id`, profile traits, customer groupings, conversion
journeys, and attribution weights are resolved from the currently activated
identity graph when the query runs.

An identity activation must be able to change an affected reporting result
without writing or rebuilding any reporting table.

## Storage boundary

Persist only identity-neutral source facts and source-derived enrichment:

- Stripe payments keyed by `(payment_source, payment_id)`
- ActiveCampaign registrations keyed by `form_submission_id`
- Current Jitsu forms, orders, page views, and attribution events
- Website sessions/touchpoints keyed by stable event and session identifiers
- Manual payment-to-profile overrides, which are explicit business decisions
- Ad delivery, cost, and product-resolution state

Do not persist values derived from the current identity graph:

- `profile_id` or identity-profile names
- `is_identified`, `is_attributed`, or profile-based eligibility
- Profile-based ranks, first/last values, or payment-plan grouping
- Conversion-to-touchpoint membership
- Attribution weights or aggregates built from those journeys
- Segment payload traits derived from the current profile

The rule is broader than “do not store `profile_id`.” A table can be stale even
without that column when its rows or calculations were grouped by identity.

## Read path

```text
identity-neutral source facts
    -> source-specific normalized identifier candidates
    -> current_identity_mappings from activated graph state
    -> current_identity_profiles when traits are required
    -> fact-grain reporting models
    -> profile-grouped journeys and reporting aggregates
```

Source-specific precedence remains unchanged:

- Client form: email, phone, anonymous ID
- Server form: email, phone
- Server payment: manual override, then email, then phone
- Native order: email, anonymous ID
- Touchpoint: anonymous ID, then user/email identity
- Manual search candidate: email, phone

All profile-grouped calculations happen after current resolution. Manual
payment overrides remain literal and continue to win over automatic identity.

## Module boundary

Identity-neutral physical state stays below `pipes/models/resolution`.
Resolution models own identifier normalization and current graph joins.
Canonical output pipes keep their public schemas and read live build models.
The retired identity-dependent Copy pipes remain rollback-only and are not part
of recurring publication.

## Conversion journey module

### Caller

`mart_conversions_with_touchpoints` keeps its existing output schema. Callers do
not choose an identity generation and do not know which source produced a
conversion. The endpoint always uses the currently activated graph.

### Internal contract

The conversion layer first normalizes every source to one narrow row shape:

```text
conversion_id
conversion_time
conversion_source
conversion_type
identity_anchor_key
manual_profile_id
source metrics
payment grouping fields when applicable
```

Server forms, client forms, server payments, and client payments emit this
identity-neutral shape. One resolver joins the combined rows to current identity
mappings. Manual payment overrides still win. Payment customer and mentorship
ranks run only after that resolution.

Touchpoints keep their separate identity-neutral fact table and one current
mapping join. The journey join therefore expands current identity twice in
total: once for all conversion facts and once for all touchpoints. The previous
build expanded it once per source reader plus the touchpoint reader.

### Module map

```text
physical form/payment facts
    -> reporting_conversion_facts
    -> reporting_resolved_conversion_events

physical touchpoint facts
    -> resolved_session_touchpoint_facts

resolved conversion events + resolved touchpoints
    -> mart_conversions_with_touchpoints_build
    -> existing public output schema
```

`reporting_conversion_facts` owns source-specific normalization and metrics.
`reporting_resolved_conversion_events` owns current identity and post-resolution
payment ranking. The journey build owns only touchpoint eligibility, ordering,
and attribution weights.

### Rejected shapes

- Calling the four existing resolved outputs keeps four copies of the identity
  overlay in one query and already exceeds Tinybird's 20-second limit.
- Persisting resolved conversion events makes the table stale after every
  identity activation.
- Computing each source as a separate endpoint moves the union to callers and
  breaks the existing reporting contract.

## Designs considered

### A. Read-time resolution over identity-neutral facts — selected

This has one identity seam, never becomes stale after activation, and matches
the coordinator’s existing decision to retire recurring reporting Copies.
Queries must filter source facts before broad identity joins, and production
latency must be measured.

### B. Full snapshots per identity generation — rejected

Reads are fast, but every identity activation makes the snapshots stale and
requires the same full rebuild that caused this incident.

### C. Durable Object selective reporting invalidation — deferred

Affected-profile recomputation can preserve fast reads, but invalidation
cascades through customer rollups, payment plans, journey ranks, first/last
touches, and attribution weights. It adds a second distributed state machine.
Use it only if measured live-query latency cannot meet the serving target.

## Cutover and proof

1. Repoint fact-grain output readers to live resolution models.
2. Repoint identity-derived aggregates to their live build models.
3. Remove direct reads of frozen identity-baked reporting seeds.
4. Validate the Tinybird dependency graph and output schemas.
5. Run the existing six-layer, 40-output quiet-cut parity gate.
6. Resume ingestion and activate an identity-only change. Identity-dependent
   outputs must change without a reporting Copy; independent outputs must not.
7. Keep old physical snapshots read-only for rollback until the permanence test
   passes. Delete them only in a later cleanup.
