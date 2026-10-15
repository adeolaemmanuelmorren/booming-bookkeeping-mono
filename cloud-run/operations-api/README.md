# Boom operations API

Live service: https://boom-operations-api-943586541190.us-central1.run.app

Send `X-Api-Key` on every `/v1` request. The key is stored in Google Secret
Manager as `boom-operations-api-key`, version 1. The ignored local
`.env.credentials` file also contains it and has file permissions `0600`.
Provider credentials never belong in this README or source control.

```sh
export OPERATIONS_API_KEY='your-key'
curl -H "X-Api-Key: $OPERATIONS_API_KEY" \
  https://boom-operations-api-943586541190.us-central1.run.app/v1/status
```

`GET /v1` returns the route list. `GET /health` is the only public route and
checks whether the HTTP process is running.

## Checks and investigation routes

| Route | Returns |
| --- | --- |
| `GET /v1/status` | Combined provider observations and timestamps. HTTP 503 if any check is degraded or unavailable. |
| `GET /v1/dataform/workflows` | Scheduled workflow configurations. |
| `GET /v1/dataform/invocations?limit=50` | Recent runs, newest first, with states and timing. |
| `GET /v1/dataform/invocations/{id}` | One invocation, including compilation and configuration references. |
| `GET /v1/dataform/invocations/{id}/actions?limit=100` | Table and assertion states, failure reasons, SQL and BigQuery job references. |
| `GET /v1/dataform/compilations/{id}` | Compilation errors and resolved Git revision. |
| `GET /v1/bigquery/datasets` | Allowed datasets and locations. |
| `GET /v1/bigquery/datasets/{dataset}/tables?limit=100` | Table list. |
| `GET /v1/bigquery/datasets/{dataset}/tables/{table}` | Schema, table type, row count and modification metadata where BigQuery provides them. |
| `GET /v1/bigquery/datasets/{dataset}/tables/{table}/rows?limit=20` | A bounded row sample, subject to the query scan budget. |
| `POST /v1/bigquery/query` | Bounded read-only SQL results. |
| `GET /v1/bigquery/query/{jobId}?limit=100` | Another page of a query created by this API. |
| `GET /v1/jitsu/workspace` | BBB workspace details. |
| `GET /v1/jitsu/connections` | Connection IDs, source/destination IDs and update times. |
| `GET /v1/jitsu/metrics` | Prometheus metrics text, including queue gauges and delivery counters. |
| `GET /v1/jitsu/invocations` | Function operational logs. |
| `GET /v1/jitsu/deliveries?mode=batch` | Warehouse batch delivery records. Use `mode=stream` for streaming records. |
| `GET /v1/meta/account` | Ad account status, currency, timezone and disable reason. |
| `GET /v1/meta/reconciliation` | Advisory daily-versus-hourly comparison, including mismatch count, dates, totals and spend differences. |
| `GET /v1/meta/insights?since=2026-10-10&until=2026-10-10&level=account` | Daily impressions, clicks, spend, reach, actions and action values. |

List routes accept `limit` up to 200. For Dataform and BigQuery, pass the returned
`nextPageToken` as `pageToken`. Query pagination also requires the returned
`jobId`. Meta uses `paging.cursors.after`; pass it as `after` when
`paging.hasMore` is true. Meta ranges may cover up to 31 days. Levels are
`account`, `campaign`, `adset` and `ad`.

Jitsu log routes accept `actorId`, `start`, `end`, `levels` and `search`.
`start` is inclusive and `end` is exclusive. Use ISO timestamps. `levels` is
a comma-separated list of `debug`, `info`, `warn`, `error`. Connection IDs
from `/v1/jitsu/connections` identify function and ordinary warehouse actors.
Jitsu function logs are operational records within Jitsu's retention window,
not a guaranteed ledger of every execution.

```sh
curl -H "X-Api-Key: $OPERATIONS_API_KEY" \
  'https://boom-operations-api-943586541190.us-central1.run.app/v1/jitsu/deliveries?mode=batch&levels=error&limit=20'

curl -H "X-Api-Key: $OPERATIONS_API_KEY" -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT MAX(date) AS latest_date FROM `able-folio-499722.booming_data_analytics.stg_facebook_ads`", "limit": 1}' \
  https://boom-operations-api-943586541190.us-central1.run.app/v1/bigquery/query
```

SQL must be a single SELECT. The API dry-runs it, checks referenced datasets,
and caps each query at 1 GB billed and 25 seconds. Scripts and writes are
rejected. Rows default to 100 per page, with a maximum of 200. Large table
samples can still exceed the scan ceiling; query fewer columns and a bounded
date range instead.

## Meaning of status

Dataform checks the latest completed `main` hourly workflow. A failed run or
completion older than three hours is degraded. It also returns the newest
scheduled invocation, which may still be running.

The Meta daily-versus-hourly reconciliation moved out of Dataform on October 14,
2026. The API computes it directly from the two staging tables, preserving its
original tolerances, three-day settling window and accepted historical
fingerprints. It does not read the old assertion view. Mismatches appear as
`checks.metaReconciliation.status: "warning"` in `/v1/status` and are listed by
`/v1/meta/reconciliation`, with up to 200 dates. An advisory warning keeps HTTP
200 and sets the overall status to `warning`. A failed or unavailable provider
check still returns HTTP 503. Dataform run status reflects its actual build
result independently of this source comparison.

BigQuery status verifies access to `booming_data_analytics`. It does not infer
freshness from dataset access or table modification metadata. Use source
timestamp queries for that.

Jitsu checks API access and last-hour error logs for functions, batch deliveries
and stream deliveries. Counters in its metrics are cumulative. An empty error
window alone does not prove recent events were delivered.

Meta status verifies token access and whether the account is active. The
Insights route separately verifies delivery metrics. An empty Insights page is
a valid response for an account or range without delivery.

## Scope and deployment

Project `able-folio-499722`, Dataform repository `dataform` in `us-central1`,
Jitsu workspace `cmrodbdxf0017221itkku4a4o`, Meta account `10151088695453802`.
The service account `boom-operations-api@able-folio-499722.iam.gserviceaccount.com`
has Dataform Viewer, BigQuery Job User, and dataset reader ACLs for:

- `booming_data_analytics` and `booming_data_analytics_assertions`
- `boom_domains` and `jitsu_data`
- `facebook_ads_airbyte` and `google_ads_airbyte`
- `activecampaign_airbyte`
- `stripe_booming_airbyte` and `stripe_kajabi_airbyte`
- `manual_attribution` and `bill_reports_realtime`

The user explicitly approved this 11-dataset scope. Existing dataset ACL
entries are preserved. The service has access only to its own API-key secret,
`boom-health-jitsu-api-key` and the existing `meta-ads-access-token` secret.
There are no workflow trigger, write-query or configuration-edit routes.

Run `npm ci`, then `npm test` locally. Use Node 22. Set environment variables
from `.env.example` and provide Google ADC when running locally.

Redeploy from the monorepo root, preserving existing environment and secrets:

```sh
gcloud run deploy boom-operations-api --project=able-folio-499722 \
  --region=us-central1 --source=cloud-run/operations-api --quiet
```

The service scales to zero, has at most two instances, and uses 512 MiB with
one CPU per instance. Cloud Run permits requests to reach the container; the
application enforces the API key before any data lookup. API keys are never
accepted in URLs. Responses use `Cache-Control: no-store`, redact credential
fields, and do not return Meta token-bearing pagination URLs.

Run `node scripts/smoke.mjs` after deployment. It writes a sanitized verification
record to `verification.json`. It checks real provider reads, missing-key
rejection, SQL writes rejected by dry run, and dataset scope enforcement.

Dataform methods follow the [official Dataform API](https://docs.cloud.google.com/dataform/reference/rest/v1/projects.locations.repositories.workflowInvocations/list).
Jitsu calls follow its [management API](https://jitsu.com/docs/api) and official
[log implementation](https://github.com/jitsucom/jitsu/blob/newjitsu/webapps/console/lib/server/events-log-stream.ts).
