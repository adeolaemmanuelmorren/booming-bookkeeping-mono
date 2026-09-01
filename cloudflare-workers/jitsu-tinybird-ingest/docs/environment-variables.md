# Environment variables

The Worker has three committed, non-secret values in `wrangler.jsonc`.

| Name | Current value | Purpose |
| --- | --- | --- |
| `TENANT_ID` | `boom` | Matches the tenant key in the Tinybird project. |
| `TINYBIRD_API_URL` | `https://api.us-east.tinybird.co` | Uses the API host for the Bill Tinybird workspace region. |
| `TINYBIRD_DATASOURCE` | `jitsu_events_api_observations` | Receives normalized live Jitsu events. |

Two values must be Cloudflare Worker secrets.

| Name | Owner | Purpose |
| --- | --- | --- |
| `JITSU_WEBHOOK_TOKEN` | Jitsu and Cloudflare | Shared bearer token on `POST /webhooks/jitsu`. Use at least 32 random bytes. |
| `TINYBIRD_APPEND_TOKEN` | Tinybird and Cloudflare | Resource-scoped token with append access to `jitsu_events_api_observations`. |

Set production secrets only when deployment is approved:

```sh
npx wrangler secret put JITSU_WEBHOOK_TOKEN
npx wrangler secret put TINYBIRD_APPEND_TOKEN
```

For local development, copy `.dev.vars.example` to `.dev.vars`. Never commit `.dev.vars`.

