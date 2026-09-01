# Environment variables and secrets

Non-secret values are committed in `wrangler.jsonc`.

| Name | Value | Purpose |
| --- | --- | --- |
| `BIGQUERY_PROJECT_ID` | `able-folio-499722` | Project that owns the query jobs. |
| `BIGQUERY_LOCATION` | `US` | BigQuery job location. |
| `GCS_BUCKET` | `booming-data` | Existing export bucket. |
| `GCS_PREFIX` | `tinybird` | Root folder owned by these exports. |
| `TINYBIRD_API_URL` | `https://api.us-east.tinybird.co` | Bill workspace API host. |
| `TINYBIRD_FETCH_TIMEOUT_MS` | `30000` | Per-attempt Tinybird request timeout, kept below the persistent lease. |
| `TINYBIRD_GATE_TIMEOUT_MS` | `180000` | Total Tinybird gate budget, including rate and retry waits. |
| `COPY_POLL_INTERVAL_MS` | `5000` | Delay between Tinybird import and Copy job checks. |
| `COPY_JOB_TIMEOUT_MS` | `3600000` | One-hour ceiling for polling one submitted Copy job. |
| `IDENTITY_BOOTSTRAP_SOURCE_INGESTED_FROM` | `2026-08-26 23:05:00` | Immutable identity seed `committed_at` cutoff for the one-time bootstrap. The bootstrap includes the boundary with `>=`; duplicate evidence is safe. |
| `EXPORT_OVERLAP_MINUTES` | `20` | Incremental lookback window. |
| `BIGQUERY_POLL_INTERVAL_MS` | `5000` | Delay between job status checks. |
| `BIGQUERY_JOB_TIMEOUT_MS` | `480000` | Eight-minute per-job polling ceiling. |

Three values must be Cloudflare Worker secrets.

| Name | Owner | Purpose |
| --- | --- | --- |
| `GCP_SERVICE_ACCOUNT_JSON` | GCP and Cloudflare | Complete service-account key JSON used for Google OAuth and BigQuery REST. |
| `TINYBIRD_ADMIN_TOKEN` | Tinybird and Cloudflare | Dedicated Tinybird `ADMIN` token used for native GCS scheduling, job polling, Copy submission, manifest validation, and the activation append. |
| `SYNC_ADMIN_TOKEN` | Operators and Cloudflare | Different long random bearer token protecting the Worker's operator endpoints. It is never sent to Tinybird. |

Set production secrets only after deployment approval:

```sh
npx wrangler secret put GCP_SERVICE_ACCOUNT_JSON
npx wrangler secret put TINYBIRD_ADMIN_TOKEN
npx wrangler secret put SYNC_ADMIN_TOKEN
```

The Worker never logs these values. Use a dedicated GCP service account rather than a person-owned credential. Do not reuse `SYNC_ADMIN_TOKEN` as the Tinybird token.

`TINYBIRD_SYNC_GATE` and `PUBLICATION_COORDINATOR` are checked-in Durable Object bindings, not secrets. Their SQLite migrations are declared in the Wrangler configuration. The checked-in configuration has no Cron Trigger; enable one only after controlled production validation.

The existing resource-scoped `APPEND` token cannot call Tinybird's native GCS scheduling endpoint, which returns `403 token needs scope ADMIN`. Adding `TINYBIRD_ADMIN_TOKEN` to Cloudflare therefore needs explicit approval before deployment.
