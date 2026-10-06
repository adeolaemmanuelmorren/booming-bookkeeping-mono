# Source-machine contracts

Each machine runs inside one serialized durable coordinator. A call must pass one
deadline through every provider read and stop starting calls before 60 seconds.

## Durable store

The injected `store` implements these asynchronous methods:

```js
getCursor(key) -> object | null
setCursor(key, value) -> void

putInboxIfAbsent(record) -> boolean
listPendingInbox({ source, sourceAccount, limit }) -> record[]
saveInboxProgress(id, progress) -> void
markInboxOutboxed(id, outboxId) -> void
markInboxProcessed(id, outboxId) -> void

nextSequence(key) -> positive integer

putOutboxIfAbsent(record) -> boolean
listPendingOutbox({ source, sourceAccount, limit }) -> record[]
markOutboxPublished(id, publishedAt) -> void

getState(key) -> JSON value | null
setState(key, JSON value) -> void
getPublishedRows(scopeId) -> row[]
setPublishedRows(scopeId, rows) -> void
```

`putInboxIfAbsent`, `putOutboxIfAbsent`, and `nextSequence` must be atomic. Inbox
payloads are immutable; `saveInboxProgress` changes only processing state. Sequence
gaps are valid. Publication uses its replacement ID as an idempotency key, so retrying
after a crash is safe. `listPendingOutbox` must return increasing
`observation_sequence` within a scope.

## Publication

The injected function has one signature:

```js
publishSourceReplacement({
  source,
  source_account,
  scope_id,
  replacement_id,
  observed_at,
  observation_sequence,
  rows,
  evidence_inbox_ids,
}) -> Promise<void>
```

The publisher must make `replacement_id` idempotent and replace `scope_id` atomically.
The coordinator records local state only after publication succeeds.

## Stripe provider reads

Run one serialized machine for `main` and one for `kajabi`. The gateway supplies:

```js
provider.read.listEvents({
  account, types, createdGte, createdLte, startingAfter, limit, deadlineAtMs
}) -> { items, hasMore, nextCursor }

provider.read.listCharges({
  account, createdGte, createdLte, startingAfter, limit, deadlineAtMs
}) -> { items, hasMore, nextCursor }

provider.read.readChargeBundlePage({
  account, chargeId, cursor, deadlineAtMs
}) -> {
  canonicalCharge,
  relatedRecords,
  observedAt,
  complete,
  nextCursor
}
```

`nextCursor` is the last Stripe object ID when `hasMore` is true. A charge-bundle
cursor is opaque to the coordinator. The gateway uses it to finish every page of
refunds and any referenced raw objects being retained: Customer, PaymentIntent,
Invoice and lines, Checkout Session and lines, BalanceTransaction, and PaymentMethod.
Every collection is returned as an array under `relatedRecords`. No product or
purchase classification happens here.

The event inbox stores the untouched Stripe Event under the account-scoped Event ID.
Event time is audit evidence only. The coordinator refetches the current Charge and
assigns the monotonic observation sequence after all bundle pages are complete.

Stripe documents that Events are available for only 30 days and that list endpoints
use `starting_after` cursors. The event poll therefore provides a recent repair path;
the fixed-window Charges backfill supplies older current Charge state:

- https://docs.stripe.com/api/events/list
- https://docs.stripe.com/api/charges/list
- https://docs.stripe.com/api/pagination

## ActiveCampaign provider reads

The gateway supplies:

```js
provider.read.listContacts({
  updatedAfter, updatedBefore, idGreater, orderById, limit, deadlineAtMs
}) -> { items, hasMore }

provider.read.getContact({ contactId, deadlineAtMs }) -> contact | null
provider.read.listContactTags({ contactId, offset, limit, deadlineAtMs })
  -> { items, hasMore }
provider.read.getTag({ tagId, deadlineAtMs }) -> tag | null
provider.read.listTags({ offset, limit, deadlineAtMs })
  -> { items, hasMore }
```

`null` means an authoritative 404. Other provider failures must throw so the slice can
retry without advancing its cursor. The provider adapter translates the canonical ISO
filter timestamps to the exact format accepted by the account API. A live integration
probe confirmed sub-day precision for the account's `updated_after` and
`updated_before` filters.

The Contacts API supports `filters[updated_after]`, `filters[updated_before]`, and
recommends `orders[id]=ASC` plus `id_greater` for large-account pagination. That poll
detects contact-field changes only. It does not prove a tag assignment was removed.

Tag membership changes are discovered through direct API polling. Each relevant tag
gets a complete membership scan. Additions can be processed as they are observed;
removals are considered only after every page of that tag's scan completes. Contact
update timestamps are not evidence that tag membership is unchanged.

A completed membership scan also detects a deleted contact's disappearance from a
relevant tag. The next contact read must return an authoritative 404 before treating
that contact as deleted. API failures never count as deletion evidence.

The Contacts API does not provide snapshot isolation across pages. Repeated complete
scans repair membership changes that race a scan. Detection delay depends on the full
scan time and account API limits; measure it before claiming one-minute freshness.

- https://developers.activecampaign.com/reference/list-all-contacts
- https://developers.activecampaign.com/reference/remove-a-contacts-tag
- https://developers.activecampaign.com/reference/retrieve-all-tags
- https://developers.activecampaign.com/reference/pagination
