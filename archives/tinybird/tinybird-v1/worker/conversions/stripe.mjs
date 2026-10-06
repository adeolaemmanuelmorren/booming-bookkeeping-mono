import {
  canonicalJson,
  cleanString,
  cloneEvidence,
  extractId,
  lowerString,
  requireNonNegativeInteger,
  requireObject,
  requireString,
  toIsoTimestamp,
  unixSecondsToIso,
} from "./shared.mjs";

const STRIPE_ACCOUNTS = new Set(["main", "kajabi"]);
const STRIPE_SOURCE_ADAPTERS = Object.freeze({
  main: "stripe",
  kajabi: "stripe_kajabi",
});

export function normalizeStripeEvent(input) {
  requireObject(input, "input");

  const event = requireObject(input.event, "event");
  const canonicalCharge = requireObject(input.canonicalCharge, "canonicalCharge");

  return normalizeStripeChargeSnapshot({
    account: input.account,
    charge: canonicalCharge,
    event,
    observationSequence: input.observationSequence,
    observedAt: input.observedAt,
    receivedAt: input.receivedAt,
    related: input.related,
  });
}

export function normalizeStripeChargeSnapshot(input) {
  requireObject(input, "input");

  const account = requireStripeAccount(input.account);
  const observationSequence = requirePositiveInteger(
    input.observationSequence,
    "observationSequence",
  );
  const charge = requireObject(input.charge, "charge");
  const chargeId = requireString(charge.id, "charge.id");

  if (charge.object && charge.object !== "charge") {
    throw new TypeError("charge.object must be charge");
  }

  if (charge.paid !== true || charge.status !== "succeeded") {
    throw new TypeError("only paid, succeeded Stripe charges can become conversion facts");
  }

  const amountMinor = requireNonNegativeInteger(charge.amount, "charge.amount");
  const refundedAmountMinor = requireNonNegativeInteger(
    charge.amount_refunded ?? 0,
    "charge.amount_refunded",
  );

  if (refundedAmountMinor > amountMinor) {
    throw new TypeError("charge.amount_refunded cannot exceed charge.amount");
  }

  const event = input.event ? requireObject(input.event, "event") : null;
  const related = input.related ? requireObject(input.related, "related") : {};
  const version = getStripeVersion({
    account,
    chargeId,
    event,
    observedAt: input.observedAt ?? input.receivedAt,
  });
  const currency = requireString(charge.currency, "charge.currency").toUpperCase();
  const customer = related.customer ?? expandedObject(charge.customer);
  const paymentIntent = related.paymentIntent ?? expandedObject(charge.payment_intent);

  return {
    source: "stripe",
    source_account: account,
    source_adapter: STRIPE_SOURCE_ADAPTERS[account],
    source_fact_id: `stripe:${account}:charge:${chargeId}`,
    source_observation_sequence: observationSequence,
    source_version_at: version.at,
    source_version_id: version.id,
    provider_event_id: event ? requireString(event.id, "event.id") : null,
    provider_event_key: event
      ? `stripe:${account}:event:${requireString(event.id, "event.id")}`
      : null,
    provider_event_created_at: event
      ? unixSecondsToIso(event.created, "event.created")
      : null,
    provider_event_type: event ? requireString(event.type, "event.type") : "api_snapshot",
    received_at: input.receivedAt
      ? toIsoTimestamp(input.receivedAt, "receivedAt")
      : version.at,
    occurred_at: unixSecondsToIso(charge.created, "charge.created"),
    is_deleted: false,

    charge_id: chargeId,
    customer_id: extractId(charge.customer),
    payment_intent_id: extractId(charge.payment_intent),
    invoice_id: extractId(charge.invoice),
    payment_method_id: extractId(charge.payment_method),
    balance_transaction_id: extractId(charge.balance_transaction),
    transfer_id: extractId(charge.transfer),

    currency,
    amount_minor: amountMinor,
    amount_captured_minor: optionalNonNegativeInteger(
      charge.amount_captured,
      "charge.amount_captured",
    ),
    amount_refunded_minor: refundedAmountMinor,
    net_amount_minor: amountMinor - refundedAmountMinor,
    has_refund: refundedAmountMinor > 0,
    is_fully_refunded: refundedAmountMinor === amountMinor && amountMinor > 0,
    provider_refunded: charge.refunded === true,
    refund_ids: getRefundIds(charge, related),

    email: firstValue(
      lowerString(charge.billing_details?.email),
      lowerString(charge.receipt_email),
      lowerString(customer?.email),
      lowerString(paymentIntent?.receipt_email),
    ),
    phone: firstValue(
      cleanString(charge.billing_details?.phone),
      cleanString(customer?.phone),
      account === "main" ? cleanString(charge.metadata?.phone) : null,
    ),
    name: firstValue(
      cleanString(charge.billing_details?.name),
      cleanString(customer?.name),
    ),
    receipt_email: lowerString(charge.receipt_email),
    receipt_number: cleanString(charge.receipt_number),
    receipt_url: cleanString(charge.receipt_url),
    billing_details: cloneEvidence(charge.billing_details ?? null),
    shipping: cloneEvidence(charge.shipping ?? null),

    description: cleanString(charge.description),
    calculated_statement_descriptor: cleanString(
      charge.calculated_statement_descriptor,
    ),
    statement_descriptor: cleanString(charge.statement_descriptor),
    statement_descriptor_suffix: cleanString(charge.statement_descriptor_suffix),
    payment_method_type: cleanString(charge.payment_method_details?.type),
    metadata: cloneEvidence(charge.metadata ?? {}),
    captured: charge.captured === true,
    livemode: charge.livemode === true,
    raw_evidence: {
      stripe_event: cloneEvidence(event),
      canonical_charge: cloneEvidence(charge),
      related: cloneEvidence(related),
    },
  };
}

export function normalizeStripeBatch(inputs) {
  if (!Array.isArray(inputs)) {
    throw new TypeError("inputs must be an array");
  }

  const factsByVersion = new Map();

  for (const input of inputs) {
    const fact = normalizeStripeEvent(input);
    const existing = factsByVersion.get(fact.source_version_id);

    if (!existing) {
      factsByVersion.set(fact.source_version_id, fact);
      continue;
    }

    assertSameStripeEvent(existing, fact);

    if (fact.received_at < existing.received_at) {
      factsByVersion.set(fact.source_version_id, fact);
    }
  }

  return [...factsByVersion.values()].sort((left, right) =>
    left.source_version_id.localeCompare(right.source_version_id),
  );
}

function requireStripeAccount(value) {
  const account = requireString(value, "account");

  if (!STRIPE_ACCOUNTS.has(account)) {
    throw new TypeError("account must be main or kajabi");
  }

  return account;
}

function getStripeVersion({ account, chargeId, event, observedAt }) {
  const observedAtIso = toIsoTimestamp(observedAt, "observedAt");

  if (event) {
    const eventId = requireString(event.id, "event.id");

    return {
      at: observedAtIso,
      id: `stripe:${account}:charge:${chargeId}:event:${eventId}`,
    };
  }

  return {
    at: observedAtIso,
    id: `stripe:${account}:charge:${chargeId}:api-snapshot:${observedAtIso}`,
  };
}

function optionalNonNegativeInteger(value, fieldName) {
  if (value === null || value === undefined) {
    return null;
  }

  return requireNonNegativeInteger(value, fieldName);
}

function requirePositiveInteger(value, fieldName) {
  const integer = requireNonNegativeInteger(value, fieldName);

  if (integer === 0) {
    throw new TypeError(`${fieldName} must be greater than zero`);
  }

  return integer;
}

function expandedObject(value) {
  if (!value || typeof value !== "object") {
    return null;
  }

  return value;
}

function firstValue(...values) {
  return values.find((value) => value !== null && value !== undefined) ?? null;
}

function getRefundIds(charge, related) {
  const refunds = related.refunds?.data ?? charge.refunds?.data ?? [];

  if (!Array.isArray(refunds)) {
    return [];
  }

  return [...new Set(refunds.map((refund) => extractId(refund)).filter(Boolean))].sort();
}

function assertSameStripeEvent(existing, duplicate) {
  if (
    existing.source_observation_sequence !== duplicate.source_observation_sequence
  ) {
    throw new TypeError(
      `duplicate Stripe event received different observation sequences: ${existing.source_version_id}`,
    );
  }

  const existingEvent = existing.raw_evidence.stripe_event;
  const duplicateEvent = duplicate.raw_evidence.stripe_event;

  if (canonicalJson(existingEvent) !== canonicalJson(duplicateEvent)) {
    throw new TypeError(
      `conflicting Stripe payloads share ${existing.source_version_id}`,
    );
  }

  if (existing.source_account !== duplicate.source_account) {
    return;
  }

  const existingCharge = existing.raw_evidence.canonical_charge;
  const duplicateCharge = duplicate.raw_evidence.canonical_charge;

  if (canonicalJson(existingCharge) !== canonicalJson(duplicateCharge)) {
    throw new TypeError(
      `conflicting charge snapshots share ${existing.source_version_id}`,
    );
  }
}
