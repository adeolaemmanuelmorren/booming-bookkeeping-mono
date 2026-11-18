# ActiveCampaign backup for Bill

This Worker reads Bill's ActiveCampaign account at `boomingbookkeeping.api-us1.com` and publishes contacts with their complete tag sets to `able-folio-499722.activecampaign_backup`. It supports the existing KRC and webinar registration models, audience models, and contact joins. It never calls Segment or advertising destinations directly.

## Operation

Cloudflare Workflows runs every five minutes. Each instance reads up to six hours of modified contacts, with a ten-minute overlap and a one-minute delay. The initial boundary is November 16, 2026 UTC. ID-based pagination reads 100 contacts at a time. Every contact must include all referenced tag assignments, and every returned modification timestamp must fall within the requested window. Missing references, stuck pagination, unexpected timestamps, and incomplete tag catalogs fail the workflow.

A complete tag catalog is fetched on every run. The receiver acknowledges each atomic page insert. Only after all pages succeed does it verify the distinct contact and tag counts and publish a completed-window entry. Dataform ignores records belonging to unfinished workflows. Failed runs leave the cursor unchanged; the next scheduled instance retries that interval. Workflow retries can append physical copies, but Dataform selects one contact or assignment per source ID before downstream joins.

Dataform freezes the completed-window list and backup snapshot for each build. It chooses the current contact version from Airbyte or backup and uses that version's complete tag set. An empty tag set removes old assignments. The latest complete tag catalog replaces the older catalog. Registration rules and the existing `form_submission_...` event IDs are unchanged.

The backup runs continuously, including when Airbyte is healthy. A newer full Airbyte contact snapshot supersedes older backup observations, including contacts absent after deletion. The modified-contact feed does not reliably expose whole-contact deletions during an outage. Those reconcile with the next full Airbyte refresh. This is a registration ingestion backup, not a full replica of every ActiveCampaign resource.

## Credentials and deployment

The AC token is read from Doppler project `booming-bookkeeping`, config `dev`, secret `ACTIVE_CAMPAIGN_TOKEN`, and installed as an encrypted Worker secret. Source calls are GET-only and restricted to `/contacts` and `/tags` on the fixed Bill account origin. No public Worker endpoint is exposed.

The Worker also holds a dedicated `WAREHOUSE_TOKEN`. The authenticated receiver is `boom-activecampaign-backup-receiver` in `us-central1`. Its runtime service account `activecampaign-backup-writer` can run BigQuery jobs and write the backup dataset. Google credentials stay in GCP through the runtime identity. The receiver token is stored in Secret Manager as `activecampaign-backup-receiver-token` and locally in an ignored, owner-readable `.dev.vars` file.

Use Node 22. Run `npm ci`, `npm test`, and `npm run check` in this Worker folder. Run the receiver tests in `cloud-run/activecampaign-backup-receiver` as well.

- `scripts/provision.cjs` creates the isolated datasets, tables, and restricted runtime identity using the operator's gcloud login.
- `scripts/set-secrets.cjs` installs the Doppler token and existing receiver token without printing them.
- `scripts/recover.cjs` runs sequential recovery windows until the completed cursor is less than ten minutes behind. An optional first argument resumes monitoring an existing instance.
- Deploy the receiver from `cloud-run/activecampaign-backup-receiver`, mount its Secret Manager token as `RECEIVER_TOKEN`, and use its dedicated service account.
- Deploy initially with `ENABLED=false`. Complete recovery and a Dataform validation build before enabling the five-minute schedule.

For Cloudflare Git builds, use root directory `cloudflare-workers/activecampaign-backup`, build command `npm run check && npm test`, deploy command `npm run deploy`, and watch path `cloudflare-workers/activecampaign-backup/*`. Deploy the receiver when its own directory changes. Dataform remains in its separately versioned repository.

Dataform's existing hourly build publishes the combined inputs to the existing models. Segment's existing registration subscriptions also run hourly. Five-minute ingestion does not change those downstream schedules.

## Rollout evidence

The Airbyte-only shadow build passed all 222 actions on November 18, 2026. It preserved all 1,235,095 contacts, 4,769,668 tag assignments, and 1,013,627 registration event IDs. No registration was added, removed, duplicated, or given a different conversion identity before introducing backup observations.

## Verification and rollback

Before rollout, compare an isolated build with a saved registration baseline. Check unique IDs, unchanged conversion identities for shared events, new registrations after the Airbyte cutoff, tag removals, and source retry deduplication. Check all Dataform assertions and the next scheduled Workflow completion. Read Segment sync receipts after publication; warehouse presence alone does not prove destination acceptance.

To pause ingestion, set `ENABLED=false` and redeploy. Existing instances can finish. Restore the previous three ActiveCampaign staging views to roll Dataform back to Airbyte alone. Retain backup records and completed windows for recovery. Workflow and receiver errors are visible in Cloudflare and GCP; this deployment does not add an external alert channel.
