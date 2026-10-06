# Parity and release gates

## Offline structural rule locks

The local suite locks the intended structure and selected rules without running
Tinybird SQL. It checks:

- raw schema capture and deterministic Data Source rendering
- an explicit resource, disposition, structural-evidence file, and parity gate
  for all 60 Dataform actions in the Bill runtime closure
- unique Tinybird resource and node names
- SELECT-only Pipes and required parameter preambles
- JavaScript examples of identity normalization, profile selection, and MD5 casing
- graph bridges, replay, deletion/split fixtures, and singleton profiles
- session minute-boundary and late-page-resessionization fixtures
- KRC registration, Immediate VIP, and purchaser edge cases
- endpoint column order, row order, nullability intent, and range semantics
- the generic-conversion row-presence quirk
- daily reach/frequency compatibility behavior

Production PII is not checked into fixtures.

These checks do not compile or execute the Pipes, replay imported data, or prove
BigQuery/Tinybird equality. Only the required branch gates below establish
executed parity. Expected endpoint-test rows must be generated from branch
execution before those tests are treated as result evidence.

## Read-only BigQuery oracle

The checked oracle range is:

```text
[2026-08-17T10:00, 2026-08-24T10:00) America/Los_Angeles
```

Its expected row counts, totals, and ordered JSON hashes are stored in
[`contracts/bigquery-oracles.json`](contracts/bigquery-oracles.json). The live
Tinybird comparison must use the exact documented ordering and numeric
serialization.

## Capacity bounds to prove

The normal identity reader is hard-bounded to 5,000 contiguous deliveries per
producer per compaction. The shadow labeler stops before iteration 500. Those are
structural limits, not performance evidence.

Dirty-visitor replay currently reads complete visitor history, and the report
endpoints do not yet enforce a maximum custom date range. Before deployment, the
branch test must measure those paths, set and enforce the largest safe report
range, record the maximum dirty visitors and history rows per run, choose controller
Copy concurrency, and confirm the resource count fits the selected Tinybird plan.
The current candidate contains 68 Data Sources, 82 transformation Pipes, 15 Copy
Pipes, and 5 endpoints; its plan budget is unapproved.

## Required Tinybird branch gates

These are the executed parity gates. Before any cutover:

1. Bind a proved flat-file GCS delivery in a private branch.
2. Build every native datafile successfully.
3. Replay production-shaped history without changing active production state.
4. Match identity mappings, profiles, sessions, payments, KRC acquisition, and
   the four report query outputs to one completed BigQuery/Dataform generation.
5. Match the ordered oracle hashes, not only totals.
6. Inject failure after every Copy stage and prove the previous generation stays visible.
7. Prove a request pinned before publication keeps one generation across all four calls.
8. Prove identity and attribution Copies stay below 25 seconds at their largest bounded batches.
9. Prove cache-miss report p95 is at most 5 seconds and p99 is below 10 seconds.
10. Run a seven-day shadow burn without unexplained row drift, Copy backlog, 408,
    429, or memory protection errors.
11. Complete and sign off
    [`project/external-consumer-inventory.json`](project/external-consumer-inventory.json)
    before retiring any Dataform action.

## Known gates that local checks cannot close

- Tinybird SQL compilation and supported function details
- real GCS object format, delete semantics, and import completion
- Copy atomicity across the concrete project resources
- pending-union memory and runtime at production graph size
- full dirty-visitor session replay cost
- ClickHouse parsing of offset-free Pacific input around DST
- Tinybird JSON number serialization versus the BigQuery Node client
- historical-name selection when current BigQuery `ANY_VALUE` inputs disagree

Until those pass, the folder is an implementation candidate, not a production
deployment.
