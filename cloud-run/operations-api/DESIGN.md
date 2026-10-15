# Operations API design

## Work list

- [x] Ground existing reporting, Dataform, BigQuery and Meta integrations.
- [x] Compare extending reporting with a separate Cloud Run service.
- [x] Define the routes and credential boundaries.
- [x] Implement and test provider reads and authentication.
- [x] Deploy and verify real responses, including rejected unauthorized requests.

## Usage

Send `X-Api-Key` to `GET /v1/status` for a combined check. Use
`GET /v1/dataform/invocations` and `GET /v1/dataform/invocations/{id}/actions`
to investigate a failed warehouse run. Use the BigQuery routes to inspect
schemas, bounded row samples, or submit a bounded read-only SQL query.
Jitsu routes expose workspace metrics, function logs, and warehouse delivery
logs. Meta routes expose the configured ad account and bounded insights.

## Shape

A separate Node HTTP service owns authentication, route validation and HTTP
responses. Provider modules hide Google ADC, pagination, upstream credentials,
and response sanitization. Every provider is restricted to this business's
configured project, datasets, repository, Jitsu workspace and Meta ad account.
There are no workflow trigger or configuration mutation endpoints.

`createHandler({ apiKey, google, jitsu, meta })` receives provider objects so
security and failure behavior can be tested without real credentials.
`google` reads Dataform and BigQuery; `jitsu` reads live execution and delivery
records; `meta` reads Graph account and Insights endpoints.

## Decision and alternatives

Candidate A extends the existing reporting service and reuses its router and
reporting credentials. It hides the same provider complexity, but couples
operational checks to dashboard releases and grants the monitoring process the
reporting job's write permissions.

Candidate B uses a separate Cloud Run service and read-only service account.
It adds one small deployment while keeping health checks independent of report
execution. This is the chosen shape. Reuse the existing credential values
through Secret Manager, with access granted only to the required secrets.
The architect skill's arena helper is not installed; the comparison was made
from the traced code and a separate read-only exploration agent.

## Tradeoffs

- A shared API key authorizes business data reads. Store it in Secret Manager,
  compare it in constant time, and never accept it in a URL.
- BigQuery dry runs and dataset IAM enforce SELECT-only queries with a per-query
  billing ceiling. Result pages are bounded independently of bytes scanned.
- Status reports observations, timestamps and explicit unavailable checks.
  It must not report a disconnected provider as healthy.
- Missing Jitsu credentials leave its routes unavailable while other checks work.
- Meta daily/hourly reconciliation is an advisory API check. Its warning does
  not fail Dataform or produce an HTTP 503 when all provider checks pass.
