# Consolidated reporting source Worker

Deployment target: existing Worker `bill-realtime-ondemand-source-d7ef`.

Status: deployed October 5, 2026. Worker version
`2a5abe3f-6d5c-439b-a787-de95a0a5cb92`. Both the hourly reporting API and realtime
refresh job now use this Worker. All six ad endpoints passed live comparisons for
October 1 and September 28–October 4. Three existing source routes passed live checks.
Hourly report rows and dimensions matched before/after cutover. End-to-end realtime
refresh verification is still in progress.

Old Google/Meta Cloud Run services remain deployed for rollback and any separate
callers; the main dashboard's switched callers no longer fetch ads through them.

## Responsibilities

- Stripe and ActiveCampaign: existing authenticated `POST /read` and private service bindings.
- Google Ads: `GET /v1/google-ads/performance` and `/hourly-performance`.
- Meta Ads: `GET /v1/meta-ads/performance`, `/hourly-performance`, `/delivery`, and `/dashboard`.
- Browser prototype compatibility: existing `POST /browser-page` and R2 binding preserved.
- No Durable Object reporting storage, alarms, or background sync.

The ad endpoints reuse the queries, mappers, date validation, and HTTP adapters in
`bill-ad-reports-site/server/reporting/src/sources/google` and `sources/meta`. Those modules therefore
remain shared source until a later source-layout cleanup. The Worker calls the actual
Google and Meta APIs, not the old Cloud Run fetchers.

## Authentication and caching

All endpoints require the existing ADMIN_TOKEN. Google calls additionally require
X-Google-Ads-Access-Token, a short-lived adwords-scoped token minted by the reporting
caller's service account. No Google private key is placed in Cloudflare.

Required additional Worker secrets:
- GOOGLE_ADS_DEVELOPER_TOKEN, from existing Google Secret Manager secret.
- META_ADS_ACCESS_TOKEN, from existing Google Secret Manager secret.

Successful ad results are cached for five minutes with Cloudflare Cache API.
Authorization is checked before cache lookup. Provider failures are never cached.
Meta report polling IDs also remain in isolate memory for retries; isolate eviction
can lose pending IDs, as it can when a Cloud Run instance stops.

## Activation gates

1. Inspect the deployed Google and Meta environment settings and match these vars.
2. Provision the two secrets securely and deploy this existing Worker.
3. Grant each reporting caller only the needed access to mint adwords tokens for
   google-ads-reader, if it does not already have that permission.
4. Run `node scripts/compare-ads-source.js` from the realtime reporting image
   using the existing refresh job service account. This is the verified comparison
   path; the local Python comparison requires equivalent service-account permissions.
   Compare both a settled day and a multi-day range. Timestamp metadata is excluded;
   all other fields and duplicate row counts must match, and results must be nonempty.
5. Build the hourly reporting API and realtime API/job from their updated source.
   Set ADS_SOURCE_URL to this Worker and ADS_SOURCE_TOKEN to the existing source
   gateway secret. The realtime job can reuse ONDEMAND_ADMIN_TOKEN.
6. Verify both hourly and realtime report responses, and complete a realtime refresh.
7. Only after caller verification, retire the two old ad services if no other callers
   remain. Removing ADS_SOURCE_URL restores the original caller destinations.

Run fixture tests from repo root:
`node --test cloudflare-workers/bill-realtime-source/test/*.test.js`.


Prepared Cloud Build images on October 5:
- Realtime: worker-ads-20261005, build 1ccda85c-8f8f-4331-9004-b4b25f15d28f.
- Hourly: worker-ads-20261005, build cfa6da3e-28f4-43f2-bbfd-aac2a0d2b2d6.

Verified deployed ad settings match the Worker account IDs, API versions, manager
account, cache duration, and maximum date range. Both reporting accounts now have the custom billAdsAccessTokenMinter role on the
Google reader account, containing only iam.serviceAccounts.getAccessToken.
The hourly reporting account also has access to the existing source gateway secret.


Production switch:
- Hourly revision: bill-reporting-api-00025-958.
- Realtime refresh image: worker-ads-verified-20261005.
- Worker endpoint: https://bill-realtime-ondemand-source-d7ef.bill-3e3.workers.dev.
- Realtime job uses ADS_SOURCE_URL plus its existing ONDEMAND_ADMIN_TOKEN.
- Hourly API uses ADS_SOURCE_URL and ADS_SOURCE_TOKEN from bill-ondemand-source-token.
- The realtime serving API needs no redeploy: the existing API starts the updated job.
- Evidence: outputs/verification/worker-ads-consolidation-2026-10-05/ in the repo root.
