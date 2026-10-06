# V1 Fivetran source transport

This Worker carries the nine raw conversion dependencies from BigQuery to immutable GCS objects and then to the matching `v1_fivetran_*` Tinybird datasources. Browser delivery remains on the existing Jitsu queue.

Each immutable export uses its own directory: `raw_source/commit-<timestamp>/part-*.parquet` or `raw_source/bridge-<timestamp>/part-*.parquet`. The connectors use the recursive `raw_source/**/*.parquet` pattern. Export rows are globally ordered by primary ID to avoid BigQuery producing large numbers of empty Parquet shards for tiny windows.

## Commit windows

Each ongoing export covers one half-open BigQuery commit window. `CHANGES` enumerates the primary IDs changed in `[completedThrough, targetEnd)`. The export then reads those IDs from the source table at `targetEnd - 1 microsecond`. A present ID emits its exact cutoff row with `_v1_deleted=false`. An ID absent at the cutoff emits the most recent DELETE payload with `_v1_deleted=true`. Every emitted row preserves the source `_fivetran_synced` and records `targetEnd` in `_v1_observed_at`. Readers choose current state by `(_v1_observed_at, _fivetran_synced)` and treat missing snapshot `_v1_deleted` as false.

BigQuery rejects `CHANGES` and `FOR SYSTEM_TIME AS OF` on the same source in one statement. The deterministic export job therefore uses a script: it first materializes change rows and IDs in temporary tables, then performs the fixed-cutoff read, then exports. No permanent BigQuery table is created.

The coordinator performs one persisted transition per alarm. It records a deterministic BigQuery job before submission, yields while it runs, recovers a missing job by resubmitting the saved plan, preserves ambiguous Tinybird acknowledgements for receipt recovery, and admits at most five imports per rolling minute. A table completes only when BigQuery file and row statistics match Tinybird's per-file operations log with zero quarantined rows. All nine verified table receipts are required before the common barrier advances.

## One-time bridge

The existing coordinator already persists the frozen T cursor. On its first enabled run, it automatically selects the bridge for every source with:

- bridge start `T=2026-09-05T22:28:00.000Z`
- bridge end `C=2026-09-05T23:42:00.000Z`

The bridge script materializes the complete source at T and C in temporary tables and exports only new, changed, and physically deleted IDs. Each source passes the normal GCS file/count and Tinybird quarantine receipt checks. The durable cursor advances from T to C only after all nine receipts and the common barrier verify. No manual cursor update or separate bridge execution is allowed.

## Enablement handshake

Keep `INGESTION_ENABLED=false` until all of these values are installed and independently checked:

- `CHANGE_HISTORY_ENABLED_AT=2026-09-05T23:38:46.234Z`
- `CHANGE_HISTORY_RETENTION_HOURS=168`
- `COMPLETE_THROUGH=2026-09-05T22:28:00.000Z`
- `SOURCE_BRIDGE_THROUGH=2026-09-05T23:42:00.000Z`
- `SOURCE_CREATED_AT_JSON` with all nine exact entries from `restore/fivetran-change-history/status.json`

The runtime refuses a source missing from the creation-time map. It also refuses a window before change-history enablement, before source creation, outside the configured retention period, or longer than BigQuery's one-day `CHANGES` limit. A BigQuery transaction/change-history error leaves the durable window unchanged and therefore fails closed.

Every export script asserts the live table creation timestamp and change-history flag against the pins. The coordinator repeats the nine-table metadata check immediately before publishing a common barrier. A conflicting deterministic BigQuery job ID is accepted only when its project, location, and exact query match the saved plan. Ambiguous Tinybird scheduling is reconciled through the Jobs API and is never resubmitted blindly; multiple candidates or a known terminal failure move the coordinator to `attention` and stop its alarm.

## Attention recovery

When status reports `attention`, keep ingestion disabled and inspect the saved source, target window, phase, BigQuery job, Tinybird job, and exact operations-log receipt. Correct the external failure or establish the missing receipt first. Preserve the durable cursor and immutable GCS objects. Resume through a reviewed state repair only when the accepted job and receipt are unambiguous; never resubmit an unknown import or advance the cursor manually.

The existing four retired Durable Object classes stay inert. This package adds only `SourceExportCoordinator` with migration tag `v5-source-export`.
