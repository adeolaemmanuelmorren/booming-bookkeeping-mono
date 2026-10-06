# Complete attribution proof

The experiment consumes current, normalized touchpoints and conversions plus an externally supplied identity mapping. It does not replace source normalization, build sessions, compute identity, or deploy production models.

Pass criteria:

1. All 24 Dataform conversion metrics match an independent per-conversion reference for first-touch, last-touch, 40/20/40, and paid-only variants, grouped at the complete reporting grain.
2. Eligibility follows touch time; ordering follows session start with conversion-time fallback and deterministic touchpoint ID ties. Later direct visits have zero weight. Offline conversions, nullable times, currency separation and reporting time zone are preserved.
3. Late arrivals, corrections, refunds, source deletion, duplicate/stale source replay, external identity merges and splits yield the same result as a full reference rebuild.
4. The processor avoids allocating touchpoint-by-conversion rows on normal and dense workloads. Measure runtime and process memory, including a long single-profile journey.
5. Store contributions in dedicated tables in the dedicated v1_attribution_proof branch. Publish all affected profiles through one transaction manifest. Verify retries, staged-but-unpublished rows, missing committed chunks and profile retirement. Report queries must fail closed on incomplete data.
6. Aggregate persisted contributions in one Tinybird report query, then full-outer merge ad spend at matching dimensions. Verify spend-only and conversion-only groups, currency/account isolation and no multiplied spend.

Sequence: implement the independent reference and timeline processor; differential and change-replay tests; bounded-memory workload tests; isolated persisted publication/report proof; read-only audit and report limitations. No production mutation is authorized by this experiment.

Production source parity, real ad API authentication/freshness, sustained concurrency, the historical identity backfill and unimplemented production session/conversion models are separate gates. They cannot be represented as proven by synthetic fixtures.
