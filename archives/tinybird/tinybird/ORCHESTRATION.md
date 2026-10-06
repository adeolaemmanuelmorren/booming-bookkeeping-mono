# Generation orchestration

Tinybird owns every transformation and report query. A small trusted controller
still has to coordinate on-demand GCS imports, Copy jobs, parity approval, and
publication. It never runs provider SQL or calls a provider while serving a report.

The machine-readable dependency graph is
[`project/generation-plan.json`](project/generation-plan.json). Run
`npm run plan:generation` for the ordered view or
`node scripts/plan-generation.mjs --json` for controller input.

## Normal generation

Acquire the Boom generation lock and pause the scheduled identity-compaction and
session-replay writers before step 1. This prevents a Jitsu batch from consuming
the manifest's next identity batch number while the source generation is being
built.

1. Append one immutable `source_generation_manifests` row. Its report token,
   expected identity batch, semantic policy version, deterministic policy
   digest, source-manifest digest, generated connection-inventory digest, and
   monotonically increasing version are fixed for the complete run. The expected
   identity batch is exactly the next batch number; removal comparison reads the
   journal strictly before that target, so a retry sees the same baseline.
2. Trigger every bound Fivetran GCS Data Source import and verify the provider's
   completion manifest. Do not advance if a table or object is missing.
3. Append one exact, complete `source_generation_connection_cutoffs` row for
   every connection emitted by `registered_source_connections`. Require
   `source_generation_cutoff_readiness.is_ready = 1`; missing, extra,
   incomplete, or superseded cutoff rows stop the run. Retain its exact
   `cutoff_set_digest` for every later gate.
4. Run `identity_enqueue_source_generation`, then `identity_compact_state`.
   Stop on `correction_required`; that generation must use the shadow workflow.
5. Run `session_resessionize_dirty_visitors` until its batch audit equals the
   manifest's identity batch.
6. Run `validate_bill_adapters`. Every saved coverage row must name the exact
   source-manifest version/digest, connection-inventory digest, and cutoff-set
   digest. `bill_build_readiness.is_ready` must be `1` for those inputs and its
   pinned policy digest.
7. Run `build_bill_krc_acquisition` first. Then run
   `build_bill_hourly_product`. The daily-delivery and Meta-hour-bound Copies can
   run after adapter validation and in parallel with the KRC/hourly chain.
8. Run `validate_bill_product_build`. Require the exact four named passing
   stages (`krc_acquisition`, `hourly_performance`, `meta_delivery_daily`, and
   `meta_hour_bounds`). Retain its `build_digest`; it pins each stage's exact
   materialization attempt, manifest digest, connection-inventory digest,
   cutoff-set digest, and policy digest.
9. Compare identity, sessions, and the four report outputs with the approved
   parity oracle. Append one `report_activation_requests` row containing the
   exact manifest digest, connection-inventory digest, cutoff-set digest, policy
   digest, identity/session batch, build digest, three parity hashes, a globally
   increasing publication version, and `approved = 1`.
10. Run `identity_activate_candidate` only when the identity batch is hidden by a
   correction rebuild. It produces no row for an already-active normal batch.
11. Run `activate_report_generation`. Verify `bill_report_generation` returns
    the requested token, then resume the scheduled writers and release the lock
    before allowing the next source manifest. On abort, keep the candidate
    unpublished and release the lock only after the controller has stopped all
    in-flight Copies.

A later `approved = 0` request with a higher request version revokes an approval
that has not yet published. A published generation remains pinned to the exact
four validated materialization attempts. Rerunning a product Copy can create a
new attempt, but it cannot change the rows returned by an existing token.

## Connection cutoff contract

Read `connection_inventory_digest` from the generated
`registered_source_connections` resource and copy it unchanged into the source
manifest. For each registered connection, append exactly one cutoff row with
`version = 1`:

- `previous_cutoff` is that connection's cutoff from the published predecessor,
  or the UTC epoch for the first generation.
- `current_cutoff` is the highest fully imported source revision covered by the
  completion manifest. For Jitsu it is the accepted `ingested_at` boundary; for
  manual inputs it is the immutable snapshot boundary.
- `object_count`, `row_count`, and `content_digest` describe the exact completed
  delivery. The content digest is an uppercase SHA-256 value and cannot be zero.
- `recorded_at` is fixed when the row is created. It participates in the generated
  `cutoff_set_digest`.

Do not append a revised row under the same source generation. A second variant,
an unregistered connection, or a wrong manifest version/digest makes readiness
fail. Create a new source generation for any correction. Direct Jitsu facts use
`ingested_at` expressed as UInt64 UTC epoch microseconds for
`source_fact_version`; malformed versions remain behind their producer checkpoint.

## Correction generation

Pause the two scheduled identity/session writers before beginning:

1. Append one exact `identity_shadow_requests` row. Its source generation,
   manifest version/digest, connection-inventory digest, cutoff-set digest, and
   request hash identify this rebuild and cannot be reused by superseding work.
2. Run `identity_shadow_prepare` once.
3. Run `identity_shadow_label_step` until the latest status is `converged` with
   zero changed labels. Stop before the enforced 500-iteration limit.
4. Run `identity_shadow_rebuild`, identity parity, and manual-link checks.
5. Run `identity_shadow_publish`. It publishes only rows carrying the current
   request's exact source generation and request hash. Publication returns no
   rows when an active
   manual link names a profile token that the split makes ambiguous; relink or
   unlink that payment explicitly and rebuild.
6. Continue at normal-generation step 4. After approval, run
   `identity_activate_candidate` and then `activate_report_generation`.
7. Resume scheduled writers only after the report token is visible.

## Fail-closed rules

- Never infer a GCS URI, completion marker, deletion convention, or source version.
- Never publish when the current source manifest, manifest version, connection
  inventory, cutoff set, policy digest, or approval differs from the validated
  build.
- Never approve before the persisted product build digest exists.
- Never skip the KRC-before-hourly dependency.
- Never activate identity without the matching session audit and the exact four
  named product audits.
- Never publish shadow rows from a stale source generation or superseded request hash.
- Never move the report pointer without the active approved identity batch.
- Never reuse a report token or decrease a publication version.

The committed raw Data Sources are intentionally unbound until the real Fivetran
flat-file contract and bucket paths are supplied. That is a deployment input, not
a value this repository can safely invent.
