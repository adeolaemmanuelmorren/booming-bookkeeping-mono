# BigQuery and Jitsu to Tinybird implementation

## Approved data paths

```text
Live product events
Jitsu webhook batch -> Cloudflare Queue -> Tinybird Events API

Warehouse and historical events
BigQuery -> immutable Parquet files in gs://booming-data/tinybird/<source>/<table>/
         -> Tinybird native GCS ingestion
```

Jitsu controls the five-minute webhook batch. The Cloudflare Queue is only the durable delivery boundary; its consumer forwards available messages without adding another batching delay.

BigQuery exports and Tinybird imports are separately retryable. Every export has an immutable run path. A small overlap in `_fivetran_synced` protects the five-minute boundary, and Tinybird resolves repeated versions by source primary key plus synchronization timestamp.

## Folder contract

```text
gs://booming-data/tinybird/
  activecampaign/<table>/run_date=YYYY-MM-DD/run_time=HHMMSS/*.parquet
  facebook_ads/<table>/run_date=YYYY-MM-DD/run_time=HHMMSS/*.parquet
  google_ads/<table>/run_date=YYYY-MM-DD/run_time=HHMMSS/*.parquet
  stripe/<table>/run_date=YYYY-MM-DD/run_time=HHMMSS/*.parquet
  stripe_kajabi/<table>/run_date=YYYY-MM-DD/run_time=HHMMSS/*.parquet
  jitsu/<table>/run_date=YYYY-MM-DD/run_time=HHMMSS/*.parquet
```

GCS folders are prefixes, so the export creates them when the first Parquet object is written.

## Completion checks

The migration is complete only when all of these are true:

1. A uniquely identified Jitsu test event is accepted by the webhook, survives the queue, and is queryable from Tinybird.
2. A BigQuery incremental export creates readable Parquet under the expected per-source GCS prefix.
3. Tinybird's native GCS connector imports those files without a custom file parser.
4. Historical Jitsu tables are exported through the same GCS layout and their Tinybird row counts and stable hashes match BigQuery.
5. The final Tinybird report queries match the current BigQuery/Dataform marts for fixed validation windows, including counts, totals, keys, and ordered result hashes.
6. Five-minute schedules, retry behavior, scoped credentials, monitoring, and a recovery runbook are deployed and documented.

## Safety rules

- Do not truncate BigQuery, GCS, or Tinybird production data during migration.
- Keep exports immutable and make retries idempotent at the Tinybird query/model layer.
- Test on a Tinybird branch before production deployment.
- Preserve the current BigQuery/Dataform reports until parity is proven.
