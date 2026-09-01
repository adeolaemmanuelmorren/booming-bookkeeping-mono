# Boom Tinybird production project

This is the clean production migration project for replacing the Dataform serving layer.
The migration is still in progress. It deliberately does not copy Dataform's folder
hierarchy or the earlier deeply nested Tinybird draft.

The dependency direction is short and consistent:

```text
BigQuery Parquet in GCS -> raw Data Sources -> reusable source models -> mart endpoints
Jitsu webhook          -> Events API       -> reusable event models  -> mart endpoints
```

Provider adapters live one folder below `pipes/adapters`, so builds and deployments use
the checked-in `--max-depth 4` package scripts.

The raw GCS layer has 49 one-to-one BigQuery table mirrors plus one typed Stripe
auxiliary resource. That 50th resource combines two subscription-history tables
and five Kajabi checkout/catalog tables into one sync request. Repeated exports are
immutable, so source models select the newest `_fivetran_synced` row at each
logical source key before applying business logic.

The native GCS connection reads `BOOM_GCS_SERVICE_ACCOUNT_CREDENTIALS_JSON` from Tinybird
Secrets. That credential belongs to `tinybird-parquet-reader`, which can list object names
in `booming-data` but can read object contents only under `tinybird/`. The earlier HMAC
probe authenticated the connection but Tinybird's GCS linker failed when a Data Source was
attached, so HMAC is not the production credential path.

## First parity gate

The first two production endpoints mirror these BigQuery marts:

- `mart_ad_performance_hourly`
- `mart_meta_delivery_daily`

Both endpoints now match BigQuery row for row over the fixed parity window: 18,211 hourly
rows and 1,226 daily rows have identical SHA-256 hashes and totals. They share
`meta_ad_dimensions`, which is the reusable seam for campaign, ad-set, and ad
names. Adding another ad platform means adding another source adapter with the same output
contract; it does not require changing the raw ingestion framework.

The live Jitsu landing Data Source is `jitsu_events_api_observations`. Historical Jitsu
tables remain raw GCS imports and will be unioned with the live contract in a later source
adapter using `message_id` precedence.

## Full output parity

`scripts/parity/output-contract.json` lists all 40 Dataform outputs in six
dependency layers. It reads the 19 declared unique keys from the Dataform SQLX
files and records reviewed diagnostic keys for the other outputs.

```sh
npm run parity:check
npm run parity:plan
```

The production runner is read-only and defaults to Tinybird Cloud:

```sh
npm run parity:production -- \
  --snapshot=<quiet-cut-timestamp> \
  --generation=<generation-id> \
  --quiet-cut-confirmed=true \
  --view-snapshot=<view-name>=<frozen-project.dataset.table> \
  --view-snapshot=<view-name>=<frozen-project.dataset.table>
```

See `PARITY_RUNBOOK.md` for the two required frozen BigQuery view tables and the
physical-state refresh order.
