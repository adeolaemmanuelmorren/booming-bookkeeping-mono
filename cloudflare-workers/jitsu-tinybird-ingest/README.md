# Jitsu to Tinybird ingestion Worker

This Worker is the narrow delivery boundary between Jitsu and Tinybird.

```text
Jitsu five-minute webhook batch
  -> POST /webhooks/jitsu
  -> jitsu-tinybird-events Queue
  -> Tinybird Events API with wait=true
  -> jitsu_events_api_observations
```

Jitsu owns the five-minute batching interval. The Cloudflare Queue does not wait five minutes or hold events to build another batch. It provides retry isolation and delivers available messages to the consumer within its configured one-second maximum wait.

This project is separate from the browser-facing Jitsu proxy and the existing
marketing webhook Worker. The production Worker is deployed at
`https://jitsu-tinybird-ingest.bill-3e3.workers.dev`. Use `wrangler deployments
status` for the current version instead of recording a version ID here.

## Ingress contract

Send an authenticated `POST` request to `/webhooks/jitsu`:

```text
Authorization: Bearer <JITSU_WEBHOOK_TOKEN>
```

The endpoint accepts:

- A Jitsu or Segment-compatible JSON object with a `batch` array.
- A JSON object with an `events` array.
- A JSON array of events.
- NDJSON with one event or batch envelope per line.
- A single JSON event.

Every event needs a valid `receivedAt`, `received_at`, `sentAt`, `sent_at`, or `timestamp` value. Standard Jitsu events already have these fields.

The ingress normalizes each event to the checked-in Tinybird schema. It preserves Jitsu `messageId` as `message_id`, then calculates:

- A stable producer ID from the complete batch contents.
- Gap-free producer sequence numbers after deterministic sorting.
- A stable delivery event ID for each producer sequence.
- SHA-256 hashes for the semantic normalized fields and original Jitsu payload.
- A source fact version equal to Jitsu's normalized received, sent, or event timestamp in epoch microseconds.

The Queue consumer stamps `ingested_at` immediately before every Tinybird delivery attempt. That landing timestamp is excluded from the stable hashes, so a delayed or retried message is included by the next incremental identity scan without becoming a conflicting logical version. If Jitsu retries a request after a partial Queue write, the Worker still produces the same IDs and hashes.

## Queue limits

Cloudflare limits one Queue message to 128 KB and one `sendBatch` call to 100 messages or 256 KB. The Worker keeps messages below 120 KB and `sendBatch` calls below 240 KB.

Large Jitsu batches are split across Queue messages. A single event that cannot fit in one message receives HTTP 413. The Worker does not add R2 or another reassembly layer.

The consumer writes one NDJSON request per Queue message to:

```text
POST /v0/events?name=jitsu_events_api_observations&wait=true
```

Only HTTP 200 with no quarantined rows is acknowledged. Every other response or network failure retries that Queue message. After five retries, Cloudflare moves it to `jitsu-tinybird-events-dlq`.

## Local verification

```sh
npm install
npm run cf-typegen
npm run check
npx wrangler deploy --dry-run --outdir /tmp/jitsu-tinybird-ingest-dry-run
```

See [docs/environment-variables.md](docs/environment-variables.md) for runtime values and secrets.

## Production provisioning state

The production queues `jitsu-tinybird-events` and `jitsu-tinybird-events-dlq` already exist with 14-day retention.

Both Worker secrets are configured, and a synthetic event passed through the webhook, Queue, and Tinybird before the test row was removed. The remaining step is to configure the Jitsu Webhook destination with the deployed `/webhooks/jitsu` URL, the bearer header, and its five-minute batch setting.

Do not add a Cloudflare Cron trigger. This path is driven by Jitsu.
