# Direct Google and Meta Ads access

We query both advertising APIs directly. The API services do not use a
scheduler or write results to BigQuery.

## Google Ads

- Manager account: `3855055503`
- Advertising account: `1941192637`
- Google Cloud service account: `google-ads-reader@able-folio-499722.iam.gserviceaccount.com`
- Developer token secret: `google-ads-developer-token`
- API version: `v25`
- OAuth scope: `https://www.googleapis.com/auth/adwords`

The private Google service requests current performance data from the Google
Ads API. It caches each response for five minutes.

```text
GET /v1/google-ads/performance?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
```

The Google service is connected to the reporting API.

## Meta Ads

- Advertising account: `act_10151088695453802`
- Meta permission: `ads_read`
- Authentication: Meta system-user token
- Token secret: `meta-ads-access-token`
- Google Cloud service account: `meta-ads-reader@able-folio-499722.iam.gserviceaccount.com`
- Graph API version: `v25.0`

The private Meta service requests Insights directly from Meta. It caches each
response for five minutes.

```text
GET /v1/meta-ads/performance?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
GET /v1/meta-ads/hourly-performance?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
GET /v1/meta-ads/delivery?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
```

The first endpoint follows the `stg_facebook_ads` shape. The hourly endpoint
follows `stg_facebook_ads_hourly`. Delivery returns campaign, ad-set, and
ad-level reach separately.

The Meta service is deployed and tested. It is not connected to the dashboard.

## Security and freshness

Both services are private Cloud Run services. Credentials stay in Google
Secret Manager and never appear in API responses. A calling service needs
Cloud Run Invoker permission.

Freshness depends on the five-minute cache and any reporting delay inside
Google or Meta.
