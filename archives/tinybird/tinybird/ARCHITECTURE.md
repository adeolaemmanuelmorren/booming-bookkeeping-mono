# Architecture and invariants

## Decision

The project uses a journal-backed stable-plus-pending design with a shadow
generation rebuild for non-monotonic corrections.

The rejected alternative rebuilt the complete graph and all sessions for every
analytics generation. It was easier to reason about, but it would put more than
nine million current identity observations and the complete page history on the
normal publication path. The selected design keeps that full rebuild as the
correction mechanism instead of the steady-state mechanism.

## Caller view

```text
trusted Bill API
  -> report generation token
  -> four pinned report queries
     -> Bill report product
        -> one validated source + identity + session generation
```

The caller supplies only report bounds and a consistency token. It does not
coordinate raw tables, identity batches, session repairs, or attribution roots.
Pending input is visible immediately only through `resolve_identity`; Bill output
changes after compaction, session replay, product validation, and publication.

## Source ownership

Each adapter owns the details that only its provider knows:

- stable row key and source version
- delete and correction behavior
- provider joins and latest-row selection
- units, money, timestamp, and timezone normalization
- provider-native product, tag, form, campaign, and account evidence
- conversion into a bounded canonical contract

Shared fan-ins are generated from the source registry and explicit canonical
contracts. They carry both tenant and connection lineage and never parse Stripe
metadata, ActiveCampaign tags, Fivetran system columns, or Jitsu payload variants.
The current Stripe and ActiveCampaign adapters retain the exact Boom taxonomy and
declare named Boom policy bindings; those rules are not presented as portable
provider logic.

Adding a payment, lead, or ad source means adding raw schemas, a contract projection,
one source owner plus its fan-in registration, provider-specific checks, and fixtures.
Rendering changes the typed fan-in and automatically adds canonical contract checks,
readiness requirements, connection fences, and the source-connection inventory. It
does not require editing final report endpoints.

## Input publication

Fivetran file ingestion is generation-aware. A completion manifest identifies
the exact object set, table counts, hashes, and source versions for a producer
sequence. A checkpoint cannot pass a missing or incomplete sequence.

Jitsu input is immutable and uses a durable event ID, producer sequence, payload
hash, source timestamp, and Tinybird ingestion timestamp. Legacy Segment history
uses the same typed contracts at lower source priority so the existing overlap
deduplication remains reproducible.

## Identity

Only first-party evidence participates in connected components:

- `anonymous_id`: trimmed, case preserved
- `user_id`: trimmed and lowercased
- raw `email`: trimmed and lowercased
- hidden canonical email: Gmail dots and plus suffix removed; Googlemail becomes Gmail
- `phone`: exact existing NANP validation, emitted as `+1...`

Click IDs remain attribution properties in the compatibility policy. They never
merge people and do not provide a unique-lookup fallback.

The public winner order is raw email, canonical email, user ID, anonymous ID,
then phone; earliest observation and lexical value break ties. `profile_id` is
the uppercase hexadecimal MD5 of `identifier_type:identifier_value`.

Stable mappings, profiles, facts, checkpoints, and change records are versioned.
Readers use `argMax` and explicit tombstones rather than relying on asynchronous
ReplacingMergeTree merges. Pending events contract known identifiers to their
stable roots and use an exact ordered `arrayFold` union for the bounded delta.

The normal path only merges. Evidence removal or any requested split triggers a
full shadow-universe rebuild. The old universe remains active until the rebuild,
session replay, report replay, and invariant checks all succeed.

Every shadow request, work row, and persisted result names the exact source
generation, manifest version/digest, connection-inventory digest, cutoff-set
digest, and shadow-request hash. The publisher accepts only that complete tuple
from the latest request and only while the same inputs remain current. This keeps
delayed work from an earlier request or cutoff set from becoming live.

## Sessions and touchpoints

Sessionization is deliberately independent of the identity graph:

```text
visitor_key = anonymous_id, else user_id, else page_view_id
```

Rows order by page timestamp and page-view ID. A new session begins when
BigQuery's whole-minute timestamp difference is greater than 30. Therefore a
30 minute 59 second gap remains in the same session, while 31 minutes starts a
new one.

A changed page dirties its prior and current visitor keys. The system rereads
the complete page history for each dirty visitor, because a late middle page can
merge sessions and renumber every later session. Touchpoints bind to current
identity after sessionization, using anonymous identity before user/email.

## Bill report product

The report product owns the current behaviors, including the awkward ones:

- only ActiveCampaign KRC tag assignments count as registrations
- the latest applicable registration is selected
- Meta paid touches must occur strictly before that registration
- first, last, and solo touches use timestamp plus touchpoint ID ordering
- a refunded successful Immediate VIP can count
- a $5K purchaser is a nonrepeat mentorship payment with net amount `> 900`
- a $997 deposit or $1,997 initial installment can qualify
- reported purchaser value is always count multiplied by 4,997
- the generic conversion branch can create a zero-delivery report row
- hourly availability is anchored only to Meta delivery
- report bounds are Pacific, start-inclusive, and end-exclusive
- daily reach and frequency come from each hierarchy's own daily rows

No application-side semantic SQL is required. The Node gateway continues to
authenticate, call endpoints, compact arrays, and serialize the current payload.

## Publication and consistency

An active universe pointer is the read barrier. Normal compaction writes one
identity batch atomically; attribution consumes only complete identity batches.
Adapter coverage is saved against the exact source-manifest version/digest,
generated connection-inventory digest, and deterministic digest of every
per-connection cutoff row.
The policy's semantic fields are hashed deterministically, and the manifest,
readiness result, approval, validation, and publication must carry the same
policy digest.

The exact four named product comparisons are persisted before approval. Their
composite build digest includes the manifest digest, connection-inventory digest,
cutoff-set digest, policy digest, and each stage's materialization timestamp and
content digest. The published token stores those pins, and endpoint readers
resolve the four saved timestamps rather than selecting the newest rows. A later
Copy retry or registry change therefore cannot mutate an already validated or
published token. The legacy `pending_input_digest` field on
the report token carries the pinned identity batch's complete input digest, not
an unmaterialized pending overlay.

The gateway must include that report generation in cache validation. The current
`dataThrough`-only ETag cannot detect an identity bridge, late payment, refund,
or name correction.

## Explicit non-goals

- The folder does not deploy or create live Tinybird resources.
- It does not guess GCS paths or Fivetran file semantics.
- It does not make request-time provider API calls.
- It does not enable click-ID identity lookup.
- It does not silently fix current DST, `ANY_VALUE`, or daily averaging behavior.
- It does not claim performance before a production-shaped Tinybird branch replay.
