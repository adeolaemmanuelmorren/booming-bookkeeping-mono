# Retire the legacy exporter

This is a separate deployment for the existing `bigquery-tinybird-sync` Worker. This retirement was deployed and verified at 21:10 UTC on 2026-09-05. See `../evidence/retirement/verified.json`. The production entrypoint is `src/index.ts`. Files under `test/` must never be deployed.

The Worker returns HTTP 410 for every path. All old job RPCs throw a retired error. Every Durable Object constructor and alarm handler deletes its own alarm. The Worker imports no legacy code, creates no application tables, writes no application state and makes no outbound calls. Its scheduled handler does nothing even if an old Cron event arrives.

`wrangler.jsonc` is the old config with only `triggers.crons` changed to `[]`. It preserves the Worker name, account, four class names, four bindings and migration tags v1 through v4. Do not rename classes, add deletion migrations, switch to the newer `exports` lifecycle format, remove bindings or delete the Worker. Any of those changes can change or destroy the namespaces whose state rollback needs.

## Before the parent deploys

1. Capture the currently deployed version ID, Worker settings, binding namespace IDs and Cron schedule. Compare all four live bindings against this config. Recheck that the latest version has not changed since the archive.
2. Retain the verified source archive and deployed multipart archive. The multipart contains `index.js` and `index.js.map`, but no settings or namespace metadata. It is not a complete settings backup.
3. Record active BigQuery export job IDs, Tinybird native import job IDs and Tinybird Copy job IDs. Capture progress from the existing operator endpoints before they become HTTP 410. Do not assume an export stopped when the Worker stopped polling it.
4. Deploy this directory at 100% to the existing Worker. Keep remote secrets and every namespace binding. Do not deploy the test workers. Verify the resulting live bindings and absence of Cron schedules.
5. Verify legacy HTTP paths return 410 and the active deployment has only retirement code. Allow in-flight requests from the old version to drain. Check for jobs created during deployment as well as those captured before it.
6. Poll every already submitted BigQuery and Tinybird job to a terminal state before wiping any source or destination. Verify there are no independent scheduled imports, copies or external writers left. A timeout or an empty local queue does not establish remote completion.

Cloudflare says Cron changes can take up to 15 minutes to propagate. The no-op scheduled handler covers late delivery. Existing sleeping object alarms can still wake objects once; the retired constructor and handler remove them without resuming stored work.

## What remains independent

`jitsu-tinybird-ingest` is another Worker. Its config has a `jitsu-tinybird-events` queue producer and consumer, and no service or Durable Object binding to this exporter. Its consumer sends directly to Tinybird's Events API. Retiring the exporter does not stop Jitsu. The parent must preserve or reroute live Jitsu ingress before wiping its existing destination. This retirement also does not stop `marketing-webhooks`, existing provider jobs, or another application's credentials.

## Rollback

The recorded pre-retirement deployed version is `f461e3a5-55c0-4907-98f5-8b51dfb23fe0`, in `tinybird-v1/evidence/before-reset/bigquery-tinybird-sync-deployments.txt`. The parent must verify it is the correct current archived version before deployment.

Rollback to that archived deployed version preserves the four namespaces because this deployment makes no lifecycle migrations. Application jobs, cursors and SQLite/KV contents remain present. Alarms removed during retirement are not restored by code rollback. Verify or restore the every-minute Cron separately, then deliberately resume the old coordinators using their original operator controls when their Tinybird/GCS destinations are ready. Queued old work can run again after rollback. Do not roll back casually after the old destination has been wiped or repurposed.

Do not use the current local legacy source as the exact rollback image. Its `publication-coordinator.ts` differs from the archived deployed source. The exported RPC method inventory matches, but the deployed version is the authoritative rollback target.

## Validation

Run `npm test` with Node 22 and the existing `miniflare` and `esbuild` dependencies. The local `node_modules` symlink points to the parent project's installed dependencies. No cloud request is made by the tests.

The tests exercise actual Workers RPC and SQLite storage. They verify HTTP 410, rejection of all public job methods from both local and deployed legacy sources, repeated late schedule delivery, alarm removal, and preservation of jobs, cursors and object IDs through a simulated code replacement, eviction and rollback. The rollback test uses representative SQLite/KV state rather than the complete live production database. It cannot prove live settings, deployment propagation or completion of remote jobs.

Strict TypeScript check passed for production and test Worker sources. Results are in `evidence/test-results.tap`. Sanitized archive and source checks are in `evidence/deployed-archive-check.json` and `evidence/deployed-source-check.json`.

## Sources inspected

- `/Users/adeola/Boom Bookkeeping/cloudflare-workers/bigquery-tinybird-sync/wrangler.jsonc`
- `/Users/adeola/Boom Bookkeeping/cloudflare-workers/bigquery-tinybird-sync/src/index.ts`
- `/Users/adeola/Boom Bookkeeping/cloudflare-workers/bigquery-tinybird-sync/src/http.ts`
- `/Users/adeola/Boom Bookkeeping/cloudflare-workers/bigquery-tinybird-sync/src/tinybird-gate.ts`
- `/Users/adeola/Boom Bookkeeping/cloudflare-workers/bigquery-tinybird-sync/src/publication-coordinator.ts`
- `/Users/adeola/Boom Bookkeeping/cloudflare-workers/bigquery-tinybird-sync/src/journey-coordinator.ts`
- `/Users/adeola/Boom Bookkeeping/cloudflare-workers/bigquery-tinybird-sync/src/reporting-facts-coordinator.ts`
- `/Users/adeola/Boom Bookkeeping/cloudflare-workers/bigquery-tinybird-sync/src/bigquery.ts`
- `/Users/adeola/Boom Bookkeeping/cloudflare-workers/bigquery-tinybird-sync/src/sync.ts`
- `/Users/adeola/Boom Bookkeeping/cloudflare-workers/jitsu-tinybird-ingest/wrangler.jsonc`
- `/Users/adeola/Boom Bookkeeping/cloudflare-workers/jitsu-tinybird-ingest/src/index.ts`
- `/Users/adeola/Boom Bookkeeping/cloudflare-workers/jitsu-tinybird-ingest/src/tinybird.ts`
- `/Users/adeola/Boom Bookkeeping/archives/tinybird-before-v1-20260905T202513Z/deployed-workers/manifest.json`

Official documentation: [Durable Object migrations](https://developers.cloudflare.com/durable-objects/reference/durable-objects-migrations/), [Cron propagation](https://developers.cloudflare.com/workers/configuration/cron-triggers/), [Worker rollbacks](https://developers.cloudflare.com/workers/versions-and-deployments/rollbacks/).
