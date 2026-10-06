# BigQuery export connection

`boom_bigquery_gcs.connection` uses a JSON key for the dedicated read-only
`tinybird-parquet-reader` GCP service account. It can list object names in the
`booming-data` bucket, but it can read object contents only under `tinybird/`.
The complete JSON document lives
in the `BOOM_GCS_SERVICE_ACCOUNT_CREDENTIALS_JSON` Tinybird Secret and is never
committed. The earlier HMAC connection probe authenticated, but Tinybird failed
when attaching a GCS Data Source, so HMAC is not used for production ingestion.

Every generated raw Data Source owns one non-overlapping recursive object prefix:

```text
gs://booming-data/tinybird/<dataset>/<normalized-table-name>/**/*.parquet
```

The initial deployment imports all matching historical files. GCS has no
automatic discovery in Tinybird, so the Cloudflare coordinator calls the native
Data Source sync endpoint after each successful BigQuery export. A sync sees only
objects added since the preceding successful run.
