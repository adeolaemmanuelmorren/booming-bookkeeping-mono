# V1 source and state ownership

The new runtime accepts browser events, provider source records, and identity observations. It publishes pageviews, sessions, profiles, identifier mappings, and conversion facts. It has no reporting or attribution module.

## Design comparison

Two independent reviews considered session state in per-visitor Durable Objects and Tinybird-backed whole-visitor rereads. Use per-visitor durable page state for the live path. That makes late-event repair proportional to one visitor's history and avoids a recurring discovery query over all historical pageviews. Bootstrap uses the same normalizer and session rules in bulk.

The identity graph contains millions of identifiers. Keep its durable facts, reverse evidence, and published mappings in Tinybird with lookup-oriented sorting. A dedicated coordinator consumes direct identity observations and loads only the affected complete components. Extract the existing pure graph algorithm, not its old warehouse exporter, journey coordinator, or reporting calls. Before accepting this choice, measure rows read for identifier and reverse-evidence lookups against the seeded graph.

Reject changing the session ID to visitor plus first page ID in V1. Current correct outputs use MD5 of visitor plus chronological session ordinal. A late insertion can renumber later sessions; V1 must publish replacements and tombstones for them.

## Logical records

- Browser events retain source, source event ID, original payload, event timestamp, source revision, deletion flag, and durable arrival sequence.
- Pageviews retain original identifiers and the normalized first-party attribution properties needed by future reports. No profile assignment is stored on a pageview or session.
- Sessions retain visitor, exact session ID, start/end, first/last page evidence, duration, count, and a visitor publication revision.
- Identity observations retain original source-record identity and all supported normalized identifiers. Deleted or corrected evidence must be replaceable.
- Identity publication contains facts, reverse evidence, current identifier-to-profile mappings, profiles, and a validated commit record. No journey repair is emitted.
- Stripe conversions retain account-qualified charge identity, provider time, gross/refund/net amounts, currency, customer identifiers, and original provider evidence. Related provider objects remain available for exact source enrichment.
- ActiveCampaign conversions retain contact-tag assignment identity and assignment time. Registration eligibility follows the existing primary/fallback tag definitions. Contact or tag corrections can replace or remove records.

## Input continuity and reset

Historical object-store exports are finite migration inputs. The running system does not query BigQuery or invoke Dataform. Retain all browser event kinds needed for identity, not only pageviews.

Before deleting data: archive source code and deployed source, confirm historical object manifests, export the live Jitsu overlap/tail, buffer new deliveries, stop old exporters/alarms/Copies, and record source counts. Reset only the confirmed Boom Tinybird workspace. Replay and live delivery use identical source priority and logical deduplication.

## Publication and verification

Durable state and outboxes commit before remote delivery. Failed or ambiguous uploads retry the same publication. HTTP success with quarantined rows is a failure. Verify actual published records before advancing a checkpoint; an independently written output index is insufficient.

Session readers expose a complete visitor revision. Identity readers expose only complete validated batches. Delete markers must suppress older rows, including when a corrected event changes time or visitor.

Verification covers fixed-cut source contents, timestamp precision, session boundaries, late bridges, earlier session insertion, duplicate deliveries, identity merges/splits, source corrections, refunds, registration removal, and repeated live progress. Synthetic records use a separate test tenant and are removed before final acceptance.
