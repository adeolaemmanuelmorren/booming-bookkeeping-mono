# Simple identity backfill

Three reporting tables: `identifiers`, `profiles`, `profile_redirects`.

The job reuses the fully prepared historical candidate facts from `boom-identity-20260905-v1`. It verifies all 14 source receipts and each fact hash, selects the current revision of each fact using the existing identity revision rules, computes connected components outside Tinybird, and writes the identifier mappings and profile list. It verifies every output bucket against the generated row hashes. It does not create the old component journal, scope lookups or membership proofs.

Identifier types and normalization remain unchanged. Profile winners preserve the existing engine's identifier priority, null-observation ordering, earliest timestamp and Unicode comparison. Profile IDs retain the existing lowercase MD5 derivation. Tests compare the new graph with the existing engine.

`source` records one deterministic contributing fact kind, not every contributing observation. Validation fields and source_stub stay null because the backfill performs no external validation and does not assign Durable Object instances. Pub/Sub-specific fields are omitted. `snapshot_id` isolates repeated historical exports; downstream queries must use a completed snapshot and deduplicate exact retry rows.

This is the existing frozen historical baseline, not a current-time snapshot: browser history and Fivetran snapshots have separate cutoffs described in restore/identity-plan.json. Live catch-up and continuous identity processing remain separate work and are not automatically enabled. No existing production tables are deleted.

There was no previously published V1 identity baseline. The initial redirects table is therefore empty. Future merges can populate redirects; a split requires identifier reassignment and cannot be represented by a single redirect.

The job receives Tinybird credentials through the existing Secret Manager reference. Builds contain only bundled code and the fixed input plan. It logs counts and progress, never identifier values. A fixed export timestamp makes a manual retry of the same snapshot idempotent. A successful `complete` log is required before using that snapshot.

Local semantic test:

```
node --experimental-strip-types identity-simple/test.mjs
```

Deployment on 2026-09-07:

- Job: `tinybird-identity-simple`, project `able-folio-499722`, region `us-east4`.
- Snapshot: `identity-simple-20260907-v1`.
- Image digest: `sha256:c91ee81b9b02ed194a8924b887a1b77c2934b4e454b50324715e8e67da8a36d9`.
- 10,659,477 prepared candidate facts across 14 verified source receipts.
- 32 GiB memory, 24 GiB Node heap, one task, no automatic retries, three-hour execution limit.
- Real-data preflight read 1,000 candidates and resolved 999 profiles; no output writes in that preflight.
- Production deployment 133 created only the three new tables. Deployment 134, an additive token deployment, grants the existing runtime READ and APPEND on them.

## Original worker export

The replacement R2 identity-state job was cancelled and its Cloud Run job deleted
at the user's request. Its state builder, reader, publisher, and upload route
were removed. The replacement IdentityCoordinator is retired and cannot accept
facts or activate a baseline. Historical Tinybird output remains intact.

Identity is calculated by the existing `bill-realtime-coordinator` Publication
Durable Object. Its `identity_state` table already contains facts, evidence,
identifier assignments, and profiles. A delivery-only outbox now copies its
committed assignments and profiles to Tinybird. Redirects describe its existing
profile history; an ambiguous historical ID has no single redirect target.

The outbox retains failed deliveries and verifies actual stored fields before
writing a receipt to `v1_identity_commits`. It does not run an identity graph or
change the worker's provider configuration. Status is available through
`POST /admin/publication/identity-export/status` on the original worker.

Export rows use `snapshot_id=identity-worker:<version>:<batch_id>`. Readers must
include only versions with matching tenant, version, and batch ID in
`v1_identity_commits`, alongside the completed historical snapshot. Select the
latest version per identifier/profile/redirect key before excluding tombstones.
Tombstones are identifier action `removed`, profile `identifier_count=0`, and
redirect `new_profile_id=''`. Do not join the physical append log as current state.

See `original-worker-export.json` and `replacement-removal.json` for checked
operational status. The on-demand pages still use their configured older report
API; this identity export does not change their report backend.
