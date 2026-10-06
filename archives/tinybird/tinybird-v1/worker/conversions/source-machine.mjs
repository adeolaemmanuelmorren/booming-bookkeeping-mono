import { requireObject, requireString, toIsoTimestamp } from "./shared.mjs";

export const DEFAULT_SLICE_MS = 55_000;
export const MAX_SLICE_MS = 60_000;

export function createSliceBudget({
  clock = Date,
  maxDurationMs = DEFAULT_SLICE_MS,
  deadlineAtMs,
} = {}) {
  if (!Number.isFinite(maxDurationMs) || maxDurationMs <= 0) {
    throw new TypeError("maxDurationMs must be greater than zero");
  }

  if (maxDurationMs > MAX_SLICE_MS) {
    throw new TypeError("maxDurationMs cannot exceed 60000");
  }

  const startedAtMs = clock.now();
  const computedDeadline = startedAtMs + maxDurationMs;
  const deadline = Math.min(deadlineAtMs ?? computedDeadline, computedDeadline);

  return {
    clock,
    deadlineAtMs: deadline,
    canStartCall(minimumRemainingMs = 1_000) {
      return clock.now() + minimumRemainingMs < deadline;
    },
    nowIso() {
      return new Date(clock.now()).toISOString();
    },
  };
}

export async function enqueuePublication({
  store,
  contract,
  inboxIds = [],
  stateUpdates = [],
}) {
  validateReplacementContract(contract);

  const id = `publication:${contract.source}:${contract.replacement_id}`;
  const inserted = await store.putOutboxIfAbsent({
    id,
    source: contract.source,
    sourceAccount: contract.source_account,
    contract,
    inboxIds: [...new Set(inboxIds)],
    stateUpdates,
  });

  return { id, inserted };
}

export async function drainPublicationOutbox({
  source,
  sourceAccount,
  store,
  publishSourceReplacement,
  publishSourceReplacements,
  budget,
  limit = 10,
}) {
  const pending = await store.listPendingOutbox({ source, sourceAccount, limit });

  if (pending.length === 0 || !budget.canStartCall()) {
    return 0;
  }

  if (typeof publishSourceReplacements === "function") {
    await publishSourceReplacements(pending.map((outbox) => outbox.contract));
    const publishedAt = budget.nowIso();

    for (const outbox of pending) {
      await commitPublishedOutbox(store, outbox, publishedAt);
    }

    return pending.length;
  }

  if (typeof publishSourceReplacement !== "function") {
    throw new TypeError("a source publisher method is required");
  }

  let published = 0;

  for (const outbox of pending) {
    if (!budget.canStartCall()) {
      break;
    }

    await publishSourceReplacement(outbox.contract);

    await commitPublishedOutbox(store, outbox, budget.nowIso());
    published += 1;
  }

  return published;
}

async function commitPublishedOutbox(store, outbox, publishedAt) {
  if (typeof store.commitPublishedOutbox === "function") {
    await store.commitPublishedOutbox(outbox.id, publishedAt);
    return;
  }

  for (const update of outbox.stateUpdates ?? []) {
    await store.setState(update.key, update.value);
  }

  await store.setPublishedRows(outbox.contract.scope_id, outbox.contract.rows);

  for (const inboxId of outbox.inboxIds ?? []) {
    await store.markInboxProcessed(inboxId, outbox.id);
  }

  await store.markOutboxPublished(outbox.id, publishedAt);
}

export function replacementContract({
  source,
  sourceAccount,
  scopeId,
  replacementId,
  observedAt,
  observationSequence,
  rows,
  evidenceInboxIds,
  sourceEvidence,
}) {
  const contract = {
    source: requireString(source, "source"),
    source_account: requireString(sourceAccount, "sourceAccount"),
    scope_id: requireString(scopeId, "scopeId"),
    replacement_id: requireString(replacementId, "replacementId"),
    observed_at: toIsoTimestamp(observedAt, "observedAt"),
    observation_sequence: requirePositiveInteger(
      observationSequence,
      "observationSequence",
    ),
    rows,
    evidence_inbox_ids: [...new Set(evidenceInboxIds ?? [])],
    source_evidence: cloneSourceEvidence(sourceEvidence),
  };

  validateReplacementContract(contract);
  return contract;
}

function cloneSourceEvidence(value) {
  requireObject(value, "sourceEvidence");
  return structuredClone(value);
}

function validateReplacementContract(contract) {
  requireObject(contract, "contract");
  requireString(contract.source, "contract.source");
  requireString(contract.source_account, "contract.source_account");
  requireString(contract.scope_id, "contract.scope_id");
  requireString(contract.replacement_id, "contract.replacement_id");
  toIsoTimestamp(contract.observed_at, "contract.observed_at");
  requirePositiveInteger(
    contract.observation_sequence,
    "contract.observation_sequence",
  );

  if (!Array.isArray(contract.rows)) {
    throw new TypeError("contract.rows must be an array");
  }
}

function requirePositiveInteger(value, fieldName) {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${fieldName} must be a positive safe integer`);
  }

  return value;
}
