# Stripe backup receiver

The Cloudflare Stripe backup calls this service to publish hydrated batches into the separate `stripe_backup` BigQuery dataset. The Cloud Run service account is `stripe-backup-writer@able-folio-499722.iam.gserviceaccount.com`. It has BigQuery Job User at project level and write access to the backup dataset only.

Every endpoint requires `Authorization: Bearer <RECEIVER_TOKEN>`. The token comes from the GCP secret `stripe-backup-receiver-token`. The service accepts no arbitrary SQL, destination table names or source URLs.

- `POST /window` reads the completed cursor for one of the two fixed Stripe accounts.
- `POST /records` validates the batch and inserts its records in one BigQuery statement.
- `POST /complete` records a successfully processed source window.

Raw copies from retries are allowed. Dataform freezes a snapshot and deduplicates by account, stream and Stripe ID before downstream joins. The receiver never calls Stripe or any conversion destination. Payloads, tokens and SQL parameters are excluded from application logs.

Run `npm ci` and `npm test`. Deploy from this directory or pass it as the Cloud Run source directory. The Dockerfile copies only the package files and `src`; local credentials and tests are excluded. Runtime Google authentication uses the service identity, with no exported private key.

The service was deployed with a maximum of three instances, concurrency ten, 512 MiB memory and a 180-second request timeout. Cloud Run allows requests to reach the container; the application authenticates them before parsing bodies or calling BigQuery.
