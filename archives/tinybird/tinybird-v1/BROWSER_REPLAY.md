# Browser buffer replay

`BrowserBufferReplay` is a maintenance Durable Object per tenant. It is disabled until `start()` is called. It does not change the current Jitsu producer, private ingress or browser router.

It reads at most 100 R2 object names and processes at most 25 envelopes per alarm. Work continues on a five-second cadence. Once a scan reaches its end, the next full scan starts 30 seconds later. Every complete scan begins again at the prefix, so a new hash inserted behind an earlier cursor is discovered on a later scan. A rejected cursor also resets the scan to the beginning.

Each envelope has a durable checkpoint keyed by its content-hash R2 key. The states are `pending`, `awaiting` and `verified`. Raw envelope bodies remain in R2, never in SQLite. A partial index covers only unfinished work; completed receipts remain available for duplicate detection.

For a pending object, the runner reads its full body and verifies its SHA-256 against the key. It calls the existing `BrowserIngress.receive` and requires the matching `accepted` receipt. It then reads `BrowserRouter.status().sequence` and saves that fixed sequence as the object's required publication point. That sequence includes every browser batch accepted before the status call. An ambiguous response can cause another ingress call; the existing router uses stable facts to deduplicate it.

The runner waits until `publishedSequence` reaches the saved sequence. The router advances that value only after validating its published source rows and group data. The runner then saves a durable `verified` receipt before deleting the R2 object. Identity delivery is durably enqueued by the router; this receipt does not claim that the separate identity publisher has already completed.

An ambiguous R2 delete retries from the verified receipt. If an old request recreates the same hash later, another scan reads and validates its body, then removes it without sending it through ingress again. A corrupt object, invalid envelope or missing unverified object stays visible as unfinished work with an error. Other objects can continue.

A durable two-minute lease protects each alarm. The runner renews it before processing each envelope and rejects stale updates after an expired lease. A watchdog alarm survives a worker restart. Each run stops beginning new envelopes after 20 seconds; a single external RPC can last longer. This is not a hard timeout on Cloudflare RPC.

## Integration files

Copy only this implementation file:

- `worker/browser/buffer-replay.ts` to `tinybird-v1/worker/browser/buffer-replay.ts`.

Copy these tests to `tinybird-v1/test/runtime/ingress/`:

- `integration/test/runtime/ingress/replay.test.mjs`
- `integration/test/runtime/ingress/replay-worker.ts`

The integration tests import the current main ingress, normalizer and replay implementation. Do not replace the other dependency copies in this temporary directory. No ingress or router changes are required.

Export the class from the main worker:

```ts
export { BrowserBufferReplay } from './browser/buffer-replay.ts';
```

Add the binding while preserving all existing bindings and migration tags:

```json
{ "name": "BROWSER_REPLAY", "class_name": "BrowserBufferReplay" }
```

Append a new migration:

```json
{ "tag": "v1-browser-replay", "new_sqlite_classes": ["BrowserBufferReplay"] }
```

The class uses existing `BROWSER_BUFFER`, `BROWSER_INGRESS`, `BROWSER_ROUTER` and tenant/baseline/mode bindings. It needs no new credentials. Root owns the authenticated HTTP routes. Suggested mapping under the existing authorization check:

```ts
const replay = env.BROWSER_REPLAY.getByName(env.TENANT_ID);
// POST /admin/browser/replay/status -> replay.status()
// POST /admin/browser/replay/start  -> replay.start()
// POST /admin/browser/replay/wake   -> replay.wake()
// POST /admin/browser/replay/pause  -> replay.pause()
```

`start()` requires live ingress and a nonempty historical baseline ID with a positive sequence. It pins that baseline identity for this maintenance object. The deployment process must verify the actual sealed baseline before setting those variables.

## Activation order

1. Deploy the replay class, binding, migration and admin routes while ingress remains in buffer mode. Do not call `start`. The object remains disabled and does not initialize BrowserRouter.
2. Complete and verify the historical baseline and the matching browser/session baseline readers. Preserve the existing R2 buffer throughout the rebuild.
3. Deploy V1 with the verified `BROWSER_BASELINE_ID`, `BROWSER_BASELINE_SEQUENCE` and `BROWSER_INGRESS_MODE=live`. Existing Jitsu calls now archive and forward through the normal ingress. Requests still finishing in an older buffer-mode deployment may write later.
4. Call the authenticated replay `start` route once. The call is idempotent. Automatic alarms enumerate the buffer, replay its envelopes and clean up only after router publication.
5. Check replay status, router progress, errors and actual Tinybird facts. At the observed arrival rate of about two envelopes per minute, the configured maintenance capacity is ample. Confirm production publication latency separately.
6. Leave the sweeper active. An empty scan or a zero pending count is a current observation, not proof that no old request can finish later. Continuing full scans handles those late writes without an admission coordinator or a finite retirement assumption.

For a rollback that stops processing, call `pause` before reverting ingress to buffer mode. A network operation already in progress may finish, but stale runs cannot advance their checkpoints after the lease is invalidated. Restart with the same sealed baseline when live processing is ready again.

## Operational limits

Current evidence is a 42-envelope, 3,824,925-byte buffer at 2026-09-05 21:54:44 UTC, with roughly two new envelopes per minute. The runner processes at most 25 items per run. Each ordinary envelope needs an acceptance pass and a later publication/cleanup pass, so its nominal ceiling is roughly 150 envelopes per minute before I/O time and publication waiting. This is a capacity estimate, not a measured production throughput guarantee.

Under a small steady backlog, a newly completed old-mode write is discovered within the next full scan, normally 30 seconds, plus listing/processing time. Publication latency depends on the actual router and Tinybird. During a large backlog or an outage, inspect `oldestPendingAgeMs`, errors and completed scan timing. The runner does not claim a one-to-ten-minute SLA while its dependencies are unavailable.

The raw buffer is temporary. Its verified objects are removed after the router has published their source rows. Do not use the buffer as the permanent raw archive; Tinybird source records already retain the exact original payload. Permanent content-hash receipts stay in the replay DO. This unit does not add retention expiry or an administrative deletion tool.

R2 direct bindings provide strongly consistent reads and listings. Pagination does not create a frozen snapshot, which is why the runner repeats complete scans. See [R2 consistency](https://developers.cloudflare.com/r2/reference/consistency/).

## Validation

`evidence/replay-runtime-tests.tap` contains actual local Miniflare R2, Durable Object and service-binding tests. They cover publication gating, storage restart, ambiguous acceptance/deletion, recreated hashes, malformed inputs, a late key behind a cursor, an old request held through three empty scans, stale leases and bounded work. All 14 replay tests passed. The representative workload contained 42 envelopes, 1,680 events and 3,810,026 bytes. Real alarms drained it in 20.929 seconds without manual wakeups. This includes the initial five-second scheduling delay. Publication was mocked, so the timing measures the runner rather than production Tinybird latency.

The test router durably deduplicates and simulates publication. Real source/session publication remains covered by the parent's separate Tinybird and browser router suites. These replay tests make no cloud calls.

`evidence/replay-typecheck.txt` records the strict TypeScript result.
