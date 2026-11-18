# Stripe backup for Dataform

This Worker uses Cloudflare Workflows to read the main and Kajabi Stripe accounts through the existing private `StripeSource` service binding. It sends complete purchase batches to the authenticated Cloud Run receiver in `cloud-run/stripe-backup-receiver`.

The receiver writes only to `able-folio-499722.stripe_backup`. Its Google service identity stays in GCP. The Worker needs only the receiver-specific `WAREHOUSE_TOKEN` secret. No public Worker endpoint is exposed.

## Deployment

The Worker is deployed with `ENABLED=true` and a five-minute cron. The receiver is deployed in `us-central1`. Production Dataform uses commit `a6a5a4e5c3bff38726e0e4550faf93a3928ca450` from the separate Dataform repository.

The final production build passed all 212 actions and all 84 assertions on November 18, 2026. It restored 99 eligible purchase events, 59 from the main account and 40 from Kajabi. Event IDs were unique; all 9,593 shared events retained their amounts, currencies and product content. The one older baseline event aged out of the existing 90-day output window. Live sample batches were replayed twice without duplicate source rows. The unused Google private key from the initial setup was revoked; only the approved ingestion token is stored in Cloudflare.

## How records reach the existing models

The nested Dataform repository first freezes complete backup batches in `stripe_backup_snapshot`. Its `src_stripe_*` and `src_stripe_kajabi_*` tables combine that snapshot with Airbyte and select the newest observation per Stripe ID before any downstream joins. These inputs stay fixed for the rest of the build. Account and stream filters keep the two accounts separate. Existing staging and purchase models keep their transformations, eligibility rules and `purchase_<charge_id>` event IDs.

The schema projection is in `dataform/includes/stripe_backup.js`, with the inspected Airbyte column types in `stripe_backup_schema.json`. New Airbyte columns do not automatically alter the existing staging contract. If a consumed column changes type, update the snapshot and validate the projection.

The backup runs continuously once enabled. It fills missing data during an Airbyte outage without needing an outage detector. It also remains valid when Airbyte returns. Retries may append physical copies, but the source tables reduce those copies to one row before purchase totals are calculated. Raw backup row counts are not purchase counts. Payment reconciliation uses these same frozen combined inputs, including refunds that Airbyte has not yet ingested. Kajabi product resolution also uses the backup-aware inputs.

## Fetching and recovery

- The cron runs every five minutes for both accounts.
- Each Workflow covers up to six hours from the last completed window, with a ten-minute overlap and a one-minute delay at the newest boundary.
- `START_AT` is the initial recovery boundary. It is used only until a completed window exists.
- Charge creation scans discover payments. Charge success, update, capture and refund events discover later changes to older charges. Events supply IDs only; objects are reread at the gateway's pinned Stripe API version.
- Hydration includes customers, PaymentIntents, invoices, all invoice and Checkout line pages, subscriptions, prices, products, and embedded plans and coupons. New standalone promotion-code catalog entries are not independently polled. Purchase values continue to use the existing charge amounts.
- A warehouse insert is atomic for each hydrated batch. A failed source read publishes none of that batch. A failed or ambiguous warehouse response retries the batch.
- The completed cursor advances only after every source page and warehouse write succeeds. Failure and replay tests cover this ordering.
- More than 29 days without a completed window stops with an explicit reconciliation error because Stripe event history is limited. Truncated subscription items and oversized child collections also fail rather than silently omit records.

Airbyte history remains available for installment sequencing. This is a purchase-ingestion backup, not a replacement for ActiveCampaign, browser attribution, or all Stripe catalog synchronization. Dataform's existing hourly schedule remains the publication cadence. The existing flattened order output still has its 26-hour conversion window.

## Commands

Use Node 22 or newer. Install with `npm ci`, then run `npm test` and `npm run check`.

Read-only live hydration check from the monorepo root:

```sh
node cloudflare-workers/stripe-backup/scripts/verify-live.cjs --read-only
```

Omitting `--read-only` writes live Stripe samples twice to the backup receiver to verify replay deduplication. It requires the configured receiver token. It never sends CAPI events directly.

`scripts/provision.cjs` creates the separate datasets and dedicated service identity. `scripts/set-secret.cjs` installs only the receiver token from the ignored, owner-readable `.dev.vars`. The token is never included in source control or command output.

For Git builds, use root directory `cloudflare-workers/stripe-backup`, build command `npm run check && npm test`, deploy command `npm run deploy`, and watch path `cloudflare-workers/stripe-backup/*`.

## Rollout checks for future changes

1. Install the approved ingestion token and deploy the final Worker with `ENABLED=false`.
2. Run manual Workflow instances for both accounts and verify warehouse acknowledgements and completed windows.
3. Replay a batch and verify one source row per account and Stripe ID.
4. Build the changed Dataform project in validation schemas. Compare purchase IDs, amounts, installment exclusions and unique-key assertions, including records overlapping Airbyte.
5. Publish the Dataform change through its separate repository and production release, rebuild the existing models, then enable the Worker schedule.
6. Check cursor freshness, latest warehouse charges and final purchase timestamps. Cloudflare Workflow errors and receiver failures require operational attention; no new external alert channel is configured.

To pause fetching, set `ENABLED=false` and redeploy. Existing Workflow instances can finish. Restoring the former staging table references rolls Dataform back to Airbyte alone. Keep backup history and cursors during rollback.
