# Realtime coordinator

This Worker owns saved source cursors, pending replacements, and the V1 identity graph for the isolated realtime reports. Existing hourly reporting remains separate.

## Source fetching

Three independent Cloud Run jobs poll every minute:

- `bill-realtime-fetch-stripe-main`
- `bill-realtime-fetch-stripe-kajabi`
- `bill-realtime-fetch-activecampaign-default`

The jobs run the existing provider adapters in Node. Provider credentials remain in the existing private Worker gateways. The fetchers use a scoped credential to read through those gateways and access their own durable source state. The dedicated Google account is `bill-realtime-fetcher@able-folio-499722.iam.gserviceaccount.com`.

Each source keeps its existing cursor, inbox, and outbox. A five-minute durable lease prevents overlapping jobs from processing the same source. On acquisition, the old source alarm is disabled; `paused: true` means that Worker alarm is disabled, not that its Cloud Run job is stopped. Manage the Cloud Scheduler job to pause future Cloud Run executions. An existing execution may finish its leased work.

Fetchers poll recent changes with overlap before hydrating pending records. They do not start full account backfills. Previously queued historical work is retained. Historical identity evidence for affected customers is loaded separately from the frozen baseline. Broader recurring reconciliation and exact baseline source-version retirement still need verification before launch.

ActiveCampaign observations include a content hash in the discovery key. The provider can change returned fields without changing its update timestamp. Exact retries deduplicate; changed observations no longer stall the cursor with an immutable-ID conflict.

Browser ingestion runs separately in `BrowserSource`. R2 notifications prioritize new objects. The existing buffer is read without deleting or modifying its objects.

## Identity and publication

The publication coordinator freezes a finite set of source replacements for each checkpoint. Historical evidence joins that set; later source arrivals wait for the next checkpoint. Each checkpoint pins its baseline. The identity graph survives hourly handovers and restarts.

Queue capacity is reserved for each source so browser traffic cannot consume all available room. The receiver stages immutable batches, checks row counts and SHA-256 hashes in BigQuery, commits, and then acknowledges the Worker. A failed transfer resumes the same batch. The final staging transaction also verifies and commits small batches, avoiding a separate warehouse job.

`publishedOutbox` means accepted by the durable queue. Warehouse commits prove BigQuery publication. `historyComplete` on a manifest means its required history was covered. `report_ready = FALSE` on an ingestion commit is expected: completed attribution reports have their own publication records.

## Verification and operation

Use Node 22. Wrangler currently comes from `tinybird-v1/node_modules`.

```
npm test
node inspect-progress.mjs
node admin.mjs status stripe main
node admin.mjs status stripe kajabi
node admin.mjs status activecampaign default
```

`inspect-progress.mjs` saves aggregate counters and recent commit metadata to `evidence/pipeline-progress.json`. It does not persist source records or credentials. `sync-local.mjs` uses the same publication lease as the deployed consumer and cannot compete with it.

The ignored `.dev.vars` contains operator credentials with owner-only permissions. `FETCHER_TOKEN` authorizes only the source-fetcher endpoint. `PUBLICATION_TOKEN` authorizes only publication endpoints. Neither provides general admin access.

The nineteen coordinator tests cover historical hydration, empty replacements, graph merges and splits, restart recovery, exact acknowledgements, finite checkpoints, reserved source capacity, remote fetcher leases, and ActiveCampaign timestamp collisions. A checkpoint includes at most 100 source updates. Each warehouse batch includes at most 100 replacement contracts, with a 2,500-fact target for historical batches. Live end-to-end freshness and final report rollout remain under verification.
