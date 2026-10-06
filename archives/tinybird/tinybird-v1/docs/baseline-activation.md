# Sealed baseline activation

This handoff adds two bootstrap files and changes the identity coordinator. It makes no cloud calls outside the local mock runtime and does not modify main.

## Copy list

- `worker/bootstrap/configured-baseline.ts` to the corresponding main path.
- `worker/bootstrap/baseline-service.ts` to the corresponding main path.
- Apply `identity-activation.patch`, or copy `worker/identity/coordinator.ts` after checking for concurrent main changes.
- Copy `integration/test/runtime/baseline/*` to `tinybird-v1/test/runtime/baseline/`. These tests import actual main implementations.

The other worker files are dependency copies. Do not copy them back. The browser bootstrap agent owns the hardened `membership.ts` and `tinybird-storage.ts` dependencies; those changes must be integrated before these files compile.

## Identity activation

`IdentityCoordinator.activateBaseline()` accepts no caller-supplied baseline object or verification flag. It reads the saved Tinybird manifest at `manifest_kind='seal'`, `manifest_key='complete'`, scoped to the configured tenant and baseline ID. The transport checks the actual stored manifest payload hash.

The helper requires matching `runId`, `tenantId`, `sourceSeal` and `finalSessionSequence`, plus the supported `identityVersion=1`, `visitorRevision='1'`, receipt/input hashes, valid counts and authenticated membership proof shape. It returns a receipt containing the exact canonical seal hash.

First activation requires published version 0, no inbox, job or outbox, and no prior committed inbox sequence. It checks this again after the Tinybird read and updates the baseline receipt and published version in one local transaction. The existing identity facts stay in Tinybird. The next batch reads those committed version 1 facts and publishes version 2.

Repeating activation with the same saved seal returns the same receipt. It never changes a later live published version or drops pending work. A changed seal, changed configuration or late first activation fails. Source ingestion and job claiming reject configured-but-unactivated baselines, so they cannot race the first activation.

With no baseline variables and no saved baseline receipt, the existing empty-start coordinator behavior remains available for existing tests. Production V1 must use the explicit sealed baseline configuration before allowing live producers.

The coordinator initializes the extra `identity_baseline` SQLite table in its existing constructor. No new identity namespace or Durable Object migration is needed. Preserve all existing namespace IDs and migrations.

## Private baseline reader

Export this WorkerEntrypoint from the main worker:

```ts
export { BootstrapBaseline } from './bootstrap/baseline-service.ts';
```

Add two private service bindings to the same worker and entrypoint:

```json
[
  { "binding": "BROWSER_BASELINE", "service": "boom-tinybird-facts-v1", "entrypoint": "BootstrapBaseline" },
  { "binding": "SESSION_BASELINE", "service": "boom-tinybird-facts-v1", "entrypoint": "BootstrapBaseline" }
]
```

The entrypoint exposes only `loadSourceHeads`, `loadMembers` and `loadVisitor`. Each call verifies the configured saved seal and delegates to `BootstrapBaselineReader`. It has no mutation methods or public fetch route.

Required variables:

- `TENANT_ID`
- `BROWSER_BASELINE_ID`, the bootstrap run ID
- `BROWSER_BASELINE_SEAL`, the exact source seal hash
- `BROWSER_BASELINE_SEQUENCE`, the seal's final session sequence

Tinybird URL/token bindings are unchanged.

## Activation order

1. Complete the bootstrap and verify its actual stored final seal, including membership proofs and all published identity/session output.
2. Deploy the helper, coordinator update, reader export, both private bindings and the exact baseline variables. Keep browser ingress in buffer mode and conversion source processing disabled.
3. Through the parent's authenticated admin route, call `env.IDENTITY.getByName(env.TENANT_ID).activateBaseline()`.
4. Verify the returned receipt and coordinator status show the expected baseline and published version 1. An activation rejection needs investigation; do not clear the inbox or reset published state to force it through.
5. Enable live browser ingress and the verified Fivetran source coordinators. The first identity live batch must use version 2. Start the browser buffer replay after live ingress is enabled, as described in its separate handoff. Conversion updates use the retained Fivetran, BigQuery raw export, GCS, and native Tinybird import route.

Root owns the authenticated activation/status routes and deployment configuration. No public unauthenticated activation route is introduced here.

## Evidence

`evidence/activation-runtime-tests.tap`: 10/10 actual Miniflare tests passed. They cover existing version 1 facts joining new version 2 facts, pre-activation input blocking, restart, repeated activation with pending and published work, changed scope/seal, prior inbox/job/outbox/history, input arriving during seal lookup, payload hash failure, authenticated absence and private reader scope enforcement.

`evidence/legacy-runtime-tests.tap`: all 13 existing identity coordinator runtime tests passed against the modified coordinator. Combined runtime result is 23/23.

`evidence/typecheck.txt`: strict TypeScript passed.

The tests simulate Tinybird responses while using its actual storage adapter, the actual identity engine/coordinator, and private Worker RPC. They do not claim to revalidate every production bootstrap row; the bootstrap writer and its seal verification own that work.
