import {
  canonicalJson,
  cloneEvidence,
  extractId,
  requireObject,
  requireString,
} from "./shared.mjs";
import {
  normalizeStripeChargeSnapshot,
  normalizeStripeEvent,
} from "./stripe.mjs";
import {
  createSliceBudget,
  drainPublicationOutbox,
  enqueuePublication,
  replacementContract,
} from "./source-machine.mjs";

export const STRIPE_EVENT_TYPES = Object.freeze([
  "charge.succeeded",
  "charge.updated",
  "charge.refunded",
  "refund.created",
  "refund.updated",
  "refund.failed",
]);

const STRIPE_ACCOUNTS = new Set(["main", "kajabi"]);

export async function runStripeSourceSlice(dependencies, options) {
  const account = requireStripeAccount(options?.account);
  const budget = createSliceBudget({
    clock: dependencies.clock,
    maxDurationMs: options?.maxDurationMs,
  });
  const result = {
    account,
    published: 0,
    inboxPages: 0,
    eventPages: 0,
    backfillPages: 0,
  };

  result.published = await drainPublicationOutbox({
    source: "stripe",
    sourceAccount: account,
    store: dependencies.store,
    publishSourceReplacement: dependencies.publishSourceReplacement,
    publishSourceReplacements: dependencies.publishSourceReplacements,
    budget,
    limit: options?.publicationLimit ?? 10,
  });

  if (budget.canStartCall()) {
    result.inboxPages = await processStripeInbox({
      ...dependencies,
      account,
      budget,
      limit: options?.inboxLimit ?? 10,
    });
  }

  if (budget.canStartCall()) {
    result.eventPages = await pollStripeEvents({
      ...dependencies,
      account,
      budget,
      initialLookbackSeconds: options?.eventInitialLookbackSeconds,
      overlapSeconds: options?.eventOverlapSeconds,
      settleSeconds: options?.eventSettleSeconds,
      maxPages: options?.eventPageLimit ?? 2,
    });
  }

  if (budget.canStartCall()) {
    result.backfillPages = await backfillStripeCharges({
      ...dependencies,
      account,
      budget,
      backfillId: options?.backfillId ?? "v1",
      createdGte: options?.backfillCreatedGte ?? 0,
      maxPages: options?.backfillPageLimit ?? 1,
    });
  }

  return result;
}

export async function pollStripeEvents({
  account,
  provider,
  store,
  budget,
  initialLookbackSeconds = 3_600,
  overlapSeconds = 600,
  settleSeconds = 10,
  maxPages = 2,
}) {
  requireStripeAccount(account);

  const cursorKey = `stripe:${account}:events-cursor`;
  let cursor = await store.getCursor(cursorKey);

  if (!cursor) {
    const nowSeconds = Math.floor(budget.clock.now() / 1_000);
    cursor = {
      completedThrough: Math.max(0, nowSeconds - initialLookbackSeconds),
      activeWindow: null,
    };
  }

  if (!cursor.activeWindow) {
    const windowEnd = Math.floor(budget.clock.now() / 1_000) - settleSeconds;

    if (windowEnd <= cursor.completedThrough) {
      await store.setCursor(cursorKey, cursor);
      return 0;
    }

    cursor.activeWindow = {
      createdGte: Math.max(0, cursor.completedThrough - overlapSeconds),
      createdLte: windowEnd,
      startingAfter: null,
    };
    await store.setCursor(cursorKey, cursor);
  }

  let pages = 0;

  while (pages < maxPages && budget.canStartCall()) {
    const page = validateStripePage(
      await provider.read.listEvents({
        account,
        types: STRIPE_EVENT_TYPES,
        createdGte: cursor.activeWindow.createdGte,
        createdLte: cursor.activeWindow.createdLte,
        startingAfter: cursor.activeWindow.startingAfter,
        limit: 100,
        deadlineAtMs: budget.deadlineAtMs,
      }),
      "listEvents",
    );

    for (const event of page.items) {
      await enqueueStripeEvent({ account, event, store, receivedAt: budget.nowIso() });
    }

    pages += 1;

    if (page.hasMore) {
      cursor.activeWindow.startingAfter = getNextStripeCursor(page);
      await store.setCursor(cursorKey, cursor);
      continue;
    }

    cursor.completedThrough = cursor.activeWindow.createdLte;
    cursor.activeWindow = null;
    await store.setCursor(cursorKey, cursor);
    break;
  }

  return pages;
}

export async function backfillStripeCharges({
  account,
  provider,
  store,
  budget,
  backfillId = "v1",
  createdGte = 0,
  maxPages = 1,
}) {
  requireStripeAccount(account);

  const cursorKey = `stripe:${account}:charges-backfill:${backfillId}`;
  let cursor = await store.getCursor(cursorKey);

  if (cursor?.status === "complete") {
    return 0;
  }

  if (!cursor) {
    cursor = {
      status: "running",
      createdGte,
      createdLte: Math.floor(budget.clock.now() / 1_000),
      startingAfter: null,
    };
    await store.setCursor(cursorKey, cursor);
  }

  let pages = 0;

  while (pages < maxPages && budget.canStartCall()) {
    const page = validateStripePage(
      await provider.read.listCharges({
        account,
        createdGte: cursor.createdGte,
        createdLte: cursor.createdLte,
        startingAfter: cursor.startingAfter,
        limit: 100,
        deadlineAtMs: budget.deadlineAtMs,
      }),
      "listCharges",
    );

    for (const listedCharge of page.items) {
      if (listedCharge.paid !== true || listedCharge.status !== "succeeded") {
        continue;
      }

      const chargeId = requireString(listedCharge.id, "listedCharge.id");
      await store.putInboxIfAbsent({
        id: `stripe:${account}:backfill:${backfillId}:charge:${chargeId}`,
        source: "stripe",
        sourceAccount: account,
        kind: "charge_backfill",
        chargeId,
        receivedAt: budget.nowIso(),
        immutablePayload: { listedCharge: cloneEvidence(listedCharge) },
      });
    }

    pages += 1;

    if (page.hasMore) {
      cursor.startingAfter = getNextStripeCursor(page);
      await store.setCursor(cursorKey, cursor);
      continue;
    }

    cursor.status = "complete";
    cursor.startingAfter = null;
    cursor.completedAt = budget.nowIso();
    await store.setCursor(cursorKey, cursor);
    break;
  }

  return pages;
}

export async function processStripeInbox({
  account,
  provider,
  store,
  budget,
  limit = 10,
}) {
  requireStripeAccount(account);

  const pending = await store.listPendingInbox({
    source: "stripe",
    sourceAccount: account,
    limit,
  });
  let pages = 0;

  for (const inbox of pending) {
    if (!budget.canStartCall()) {
      break;
    }

    const progress = inbox.progress ?? emptyStripeProgress();
    const page = validateChargeBundlePage(
      await provider.read.readChargeBundlePage({
        account,
        chargeId: inbox.chargeId,
        cursor: progress.bundleCursor,
        deadlineAtMs: budget.deadlineAtMs,
      }),
    );
    const bundle = mergeChargeBundle(progress.bundle, page);
    pages += 1;

    if (!page.complete) {
      await store.saveInboxProgress(inbox.id, {
        ...progress,
        bundle,
        bundleCursor: page.nextCursor,
      });
      continue;
    }

    if (
      bundle.canonicalCharge?.paid !== true ||
      bundle.canonicalCharge?.status !== "succeeded"
    ) {
      await store.markInboxProcessed(inbox.id, null);
      continue;
    }

    const sequence =
      progress.observationSequence ??
      (await store.nextSequence(`stripe:${account}:charge-observations`));
    const observedAt = page.observedAt ?? budget.nowIso();
    const completedProgress = {
      ...progress,
      bundle,
      bundleCursor: null,
      observationSequence: sequence,
      observedAt,
    };
    await store.saveInboxProgress(inbox.id, completedProgress);

    const fact = normalizeCompletedStripeBundle({
      account,
      inbox,
      bundle,
      observedAt,
      observationSequence: sequence,
    });
    const replacementId = `${fact.source_fact_id}:observation:${sequence}`;
    const contract = replacementContract({
      source: "stripe",
      sourceAccount: account,
      scopeId: fact.source_fact_id,
      replacementId,
      observedAt,
      observationSequence: sequence,
      rows: [fact],
      evidenceInboxIds: [inbox.id],
      sourceEvidence: {
        kind: "stripe_charge_snapshot",
        inbox: {
          id: inbox.id,
          kind: inbox.kind,
          receivedAt: inbox.receivedAt,
          immutablePayload: cloneEvidence(inbox.immutablePayload),
        },
        canonicalCharge: cloneEvidence(bundle.canonicalCharge),
        relatedRecords: cloneEvidence(bundle.relatedRecords),
      },
    });
    const outbox = await enqueuePublication({
      store,
      contract,
      inboxIds: [inbox.id],
    });
    await store.markInboxOutboxed(inbox.id, outbox.id);
  }

  return pages;
}

async function enqueueStripeEvent({ account, event, store, receivedAt }) {
  requireObject(event, "event");

  const eventId = requireString(event.id, "event.id");
  const eventType = requireString(event.type, "event.type");
  const chargeId = getChargeIdFromStripeEvent(event);

  if (!STRIPE_EVENT_TYPES.includes(eventType) || !chargeId) {
    return false;
  }

  return store.putInboxIfAbsent({
    id: `stripe:${account}:event:${eventId}`,
    source: "stripe",
    sourceAccount: account,
    kind: "stripe_event",
    chargeId,
    receivedAt,
    immutablePayload: { event: cloneEvidence(event) },
  });
}

function getChargeIdFromStripeEvent(event) {
  const object = event.data?.object;

  if (object?.object === "charge") {
    return extractId(object);
  }

  if (object?.object === "refund") {
    return extractId(object.charge);
  }

  return null;
}

function validateStripePage(value, methodName) {
  const page = requireObject(value, methodName);

  if (!Array.isArray(page.items)) {
    throw new TypeError(`${methodName}.items must be an array`);
  }

  if (typeof page.hasMore !== "boolean") {
    throw new TypeError(`${methodName}.hasMore must be a boolean`);
  }

  return page;
}

function getNextStripeCursor(page) {
  const cursor = page.nextCursor ?? page.items.at(-1)?.id;

  if (!cursor) {
    throw new TypeError("a Stripe page with hasMore=true must return a cursor");
  }

  return String(cursor);
}

function validateChargeBundlePage(value) {
  const page = requireObject(value, "readChargeBundlePage");

  if (typeof page.complete !== "boolean") {
    throw new TypeError("readChargeBundlePage.complete must be a boolean");
  }

  if (!page.complete && !page.nextCursor) {
    throw new TypeError("an incomplete charge bundle page must return nextCursor");
  }

  if (page.relatedRecords && typeof page.relatedRecords !== "object") {
    throw new TypeError("relatedRecords must be an object of arrays");
  }

  return page;
}

function emptyStripeProgress() {
  return {
    bundleCursor: null,
    bundle: { canonicalCharge: null, relatedRecords: {} },
    observationSequence: null,
  };
}

function mergeChargeBundle(currentBundle, page) {
  const current = currentBundle ?? emptyStripeProgress().bundle;
  const relatedRecords = { ...current.relatedRecords };

  for (const [collection, records] of Object.entries(page.relatedRecords ?? {})) {
    if (!Array.isArray(records)) {
      throw new TypeError(`relatedRecords.${collection} must be an array`);
    }

    relatedRecords[collection] = mergeRecords(
      relatedRecords[collection] ?? [],
      records,
    );
  }

  return {
    canonicalCharge: page.canonicalCharge
      ? cloneEvidence(page.canonicalCharge)
      : current.canonicalCharge,
    relatedRecords,
  };
}

function mergeRecords(existing, additions) {
  const records = new Map();

  for (const record of [...existing, ...additions]) {
    const id = extractId(record);
    const key = id ? `${record.object ?? "record"}:${id}` : canonicalJson(record);
    records.set(key, cloneEvidence(record));
  }

  return [...records.values()];
}

function normalizeCompletedStripeBundle({
  account,
  inbox,
  bundle,
  observedAt,
  observationSequence,
}) {
  const charge = requireObject(bundle.canonicalCharge, "canonicalCharge");
  const records = bundle.relatedRecords;
  const related = {
    customer: findById(records.customers, charge.customer),
    paymentIntent: findById(records.paymentIntents, charge.payment_intent),
    refunds: { data: records.refunds ?? [] },
    rawRecords: cloneEvidence(records),
  };
  const event = inbox.immutablePayload.event;

  if (event) {
    return normalizeStripeEvent({
      account,
      event,
      canonicalCharge: charge,
      related,
      receivedAt: inbox.receivedAt,
      observedAt,
      observationSequence,
    });
  }

  return normalizeStripeChargeSnapshot({
    account,
    charge,
    related,
    receivedAt: observedAt,
    observedAt,
    observationSequence,
  });
}

function findById(records = [], idValue) {
  const id = extractId(idValue);
  return records.find((record) => extractId(record) === id) ?? null;
}

function requireStripeAccount(value) {
  const account = requireString(value, "account");

  if (!STRIPE_ACCOUNTS.has(account)) {
    throw new TypeError("account must be main or kajabi");
  }

  return account;
}
