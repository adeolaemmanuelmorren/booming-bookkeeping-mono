import type {
  JsonRecord,
  PreparedScope,
  ScopeState,
  SourceReplacement,
  StripeAccount,
  StripeChargeNormalizer,
  StripeRawScope,
} from "./contracts.ts";
import { sha256 } from "./json.ts";
import {
  booleanValue,
  collapseFivetranVersions,
  identifier,
  isDeleted,
  latestSourceVersion,
  optionalString,
  requiredInteger,
} from "./raw-collapse.ts";
import { parseTimestamp } from "./timestamp.ts";

export interface StripeBuildInput {
  raw: StripeRawScope;
  throughInclusive: string;
  windowId: string;
  observationSequence: number;
  previous: ScopeState | null;
  snapshotOnly?: boolean;
  normalizeCharge: StripeChargeNormalizer;
}

export function buildStripeFivetranScope(input: StripeBuildInput): PreparedScope {
  const { raw } = input;
  const scopeId = stripeScopeId(raw.account, raw.chargeId);
  const replacementId = `${scopeId}:fivetran:${input.windowId}`;
  const chargeRows = collapseFivetranVersions({
    rows: raw.chargeVersions,
    throughInclusive: input.snapshotOnly ? undefined : input.throughInclusive,
  });
  const customerRows = collapseFivetranVersions({
    rows: raw.customerVersions,
    throughInclusive: input.snapshotOnly ? undefined : input.throughInclusive,
  });
  const paymentIntentRows = collapseFivetranVersions({
    rows: raw.paymentIntentVersions,
    throughInclusive: input.snapshotOnly ? undefined : input.throughInclusive,
  });
  const chargeRaw = chargeRows.currentById.get(raw.chargeId) ?? null;
  const rows = buildStripeRows({
    ...input,
    chargeRaw,
    customerById: customerRows.currentById,
    paymentIntentById: paymentIntentRows.currentById,
    scopeId,
    replacementId,
  });
  const sourceEvidence = {
    transport: {
      kind: "fivetran_raw_landing",
      pipeline: raw.account === "main" ? "stripe_main" : "stripe_kajabi",
      observed_through_inclusive: input.throughInclusive,
      window_id: input.windowId,
    },
    raw_charge_versions: chargeRows.versions,
    raw_customer_versions: customerRows.versions,
    raw_payment_intent_versions: paymentIntentRows.versions,
  };
  const replacement: SourceReplacement = {
    source: "stripe",
    source_account: raw.account,
    scope_id: scopeId,
    replacement_id: replacementId,
    observed_at: input.throughInclusive,
    observation_sequence: input.observationSequence,
    rows,
    evidence_inbox_ids: sortedUnique(raw.evidenceRecordIds),
    source_evidence: sourceEvidence,
  };
  const compactState = {
    charge: chargeRaw,
    customer: relatedRow(chargeRaw?.customer_id, customerRows.currentById),
    payment_intent: relatedRow(
      chargeRaw?.payment_intent_id,
      paymentIntentRows.currentById,
    ),
    provider_source_version_at: latestSourceVersion([
      ...chargeRows.versions,
      ...customerRows.versions,
      ...paymentIntentRows.versions,
    ]),
  };

  return {
    windowId: input.windowId,
    scopeId,
    inputHash: sha256({ sourceEvidence, previousRows: input.previous?.rows ?? [] }),
    replacement,
    compactState,
  };
}

function buildStripeRows(input: StripeBuildInput & {
  chargeRaw: JsonRecord | null;
  customerById: Map<string, JsonRecord>;
  paymentIntentById: Map<string, JsonRecord>;
  scopeId: string;
  replacementId: string;
}): JsonRecord[] {
  const priorRows = input.previous?.rows ?? [];
  const chargeRaw = input.chargeRaw;

  if (!chargeRaw) {
    return stripeTombstones(priorRows, input, "charge_missing");
  }

  if (!booleanValue(chargeRaw.paid) || optionalString(chargeRaw.status) !== "succeeded") {
    return stripeTombstones(priorRows, input, "charge_no_longer_succeeded");
  }

  const charge = stripeCharge(chargeRaw);
  const customer = stripeCustomer(
    relatedRow(chargeRaw.customer_id, input.customerById),
  );
  const paymentIntent = stripePaymentIntent(
    relatedRow(chargeRaw.payment_intent_id, input.paymentIntentById),
  );
  const fact = input.normalizeCharge({
    account: input.raw.account,
    charge,
    observationSequence: input.observationSequence,
    observedAt: input.throughInclusive,
    receivedAt: input.throughInclusive,
    related: { customer, paymentIntent, refunds: { data: [] } },
  });
  const sourceFactId = String(fact.source_fact_id);
  const providerVersion = latestSourceVersion([
    chargeRaw,
    ...input.customerById.values(),
    ...input.paymentIntentById.values(),
  ]);

  return [{
    ...fact,
    source_observation_sequence: input.observationSequence,
    source_version_at: input.throughInclusive,
    source_version_id: `${sourceFactId}:${input.replacementId}`,
    replacement_scope_id: input.scopeId,
    replacement_version_id: input.replacementId,
    provider_event_id: null,
    provider_event_key: null,
    provider_event_created_at: null,
    provider_event_type: "fivetran_snapshot",
    provider_source_version_at: providerVersion,
    received_at: input.throughInclusive,
    is_deleted: false,
    tombstone_reason: null,
  }];
}

function stripeTombstones(
  previousRows: JsonRecord[],
  input: StripeBuildInput & { scopeId: string; replacementId: string },
  reason: string,
): JsonRecord[] {
  const tombstones: JsonRecord[] = previousRows
    .filter((row) => row.is_deleted !== true)
    .map((row) => ({
      ...structuredClone(row),
      source_observation_sequence: input.observationSequence,
      source_version_at: input.throughInclusive,
      source_version_id: `${String(row.source_fact_id)}:${input.replacementId}`,
      replacement_scope_id: input.scopeId,
      replacement_version_id: input.replacementId,
      received_at: input.throughInclusive,
      is_deleted: true,
      tombstone_reason: reason,
      tombstone_evidence: {
        observed_through_inclusive: input.throughInclusive,
        raw_charge_versions: structuredClone(input.raw.chargeVersions),
      },
    }));

  return tombstones.sort((left, right) =>
    String(left.source_fact_id).localeCompare(String(right.source_fact_id)),
  );
}

export function stripeScopeId(account: StripeAccount, chargeId: string): string {
  return `stripe:${account}:charge:${identifier(chargeId, "chargeId")}`;
}

function stripeCharge(raw: JsonRecord): JsonRecord {
  return {
    id: identifier(raw.id, "charge.id"),
    object: "charge",
    amount: requiredInteger(raw.amount, "charge.amount"),
    amount_captured: nullableInteger(raw.amount_captured, "charge.amount_captured"),
    amount_refunded: requiredInteger(raw.amount_refunded ?? 0, "charge.amount_refunded"),
    paid: booleanValue(raw.paid),
    captured: booleanValue(raw.captured),
    refunded: booleanValue(raw.refunded),
    status: optionalString(raw.status),
    currency: optionalString(raw.currency),
    created: unixSeconds(raw.created, "charge.created"),
    customer: optionalString(raw.customer_id),
    payment_intent: optionalString(raw.payment_intent_id),
    invoice: optionalString(raw.invoice_id),
    payment_method: optionalString(raw.payment_method_id),
    balance_transaction: optionalString(raw.balance_transaction_id),
    transfer: optionalString(raw.transfer_id),
    receipt_email: optionalString(raw.receipt_email),
    receipt_number: optionalString(raw.receipt_number),
    receipt_url: optionalString(raw.receipt_url),
    description: optionalString(raw.description),
    calculated_statement_descriptor: optionalString(raw.calculated_statement_descriptor),
    statement_descriptor: optionalString(raw.statement_descriptor),
    statement_descriptor_suffix: optionalString(raw.statement_descriptor_suffix),
    livemode: booleanValue(raw.livemode),
    metadata: jsonObject(raw.metadata, "charge.metadata"),
    billing_details: billingDetails(raw),
    shipping: shippingDetails(raw),
  };
}

function stripeCustomer(raw: JsonRecord | null): JsonRecord | null {
  if (!raw) return null;

  return {
    id: identifier(raw.id, "customer.id"),
    object: "customer",
    email: optionalString(raw.email),
    name: optionalString(raw.name),
    phone: optionalString(raw.phone),
    is_deleted: isDeleted(raw.is_deleted),
    metadata: jsonObject(raw.metadata, "customer.metadata"),
  };
}

function stripePaymentIntent(raw: JsonRecord | null): JsonRecord | null {
  if (!raw) return null;

  return {
    id: identifier(raw.id, "payment_intent.id"),
    object: "payment_intent",
    receipt_email: optionalString(raw.receipt_email),
    customer: optionalString(raw.customer_id),
    status: optionalString(raw.status),
    metadata: jsonObject(raw.metadata, "payment_intent.metadata"),
  };
}

function relatedRow(
  value: unknown,
  rows: Map<string, JsonRecord>,
): JsonRecord | null {
  const id = optionalString(value);
  return id ? rows.get(id) ?? null : null;
}

function billingDetails(raw: JsonRecord): JsonRecord {
  return {
    email: optionalString(raw.billing_detail_email),
    name: optionalString(raw.billing_detail_name),
    phone: optionalString(raw.billing_detail_phone),
    address: address(raw, "billing_detail_address"),
  };
}

function shippingDetails(raw: JsonRecord): JsonRecord | null {
  const shipping = {
    name: optionalString(raw.shipping_name),
    phone: optionalString(raw.shipping_phone),
    carrier: optionalString(raw.shipping_carrier),
    tracking_number: optionalString(raw.shipping_tracking_number),
    address: address(raw, "shipping_address"),
  };

  return hasValue(shipping) ? shipping : null;
}

function address(raw: JsonRecord, prefix: string): JsonRecord | null {
  const value = {
    city: optionalString(raw[`${prefix}_city`]),
    country: optionalString(raw[`${prefix}_country`]),
    line1: optionalString(raw[`${prefix}_line_1`]),
    line2: optionalString(raw[`${prefix}_line_2`]),
    postal_code: optionalString(raw[`${prefix}_postal_code`]),
    state: optionalString(raw[`${prefix}_state`]),
  };

  return hasValue(value) ? value : null;
}

function hasValue(value: JsonRecord): boolean {
  return Object.values(value).some((field) => {
    if (field && typeof field === "object") return hasValue(field as JsonRecord);
    return field !== null && field !== undefined && field !== "";
  });
}

function jsonObject(value: unknown, fieldName: string): JsonRecord {
  if (value === null || value === undefined || value === "") return {};

  if (typeof value === "object" && !Array.isArray(value)) {
    return structuredClone(value as JsonRecord);
  }

  if (typeof value !== "string") {
    throw new TypeError(`${fieldName} must be a JSON object`);
  }

  let parsed: unknown;

  try {
    parsed = JSON.parse(value);
  } catch {
    throw new TypeError(`${fieldName} must be valid JSON`);
  }

  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new TypeError(`${fieldName} must be a JSON object`);
  }

  return parsed as JsonRecord;
}

function unixSeconds(value: unknown, fieldName: string): number {
  const timestamp = parseTimestamp(value, fieldName);
  const seconds = timestamp.microseconds / 1_000_000n;
  const number = Number(seconds);

  if (!Number.isSafeInteger(number) || number < 0) {
    throw new TypeError(`${fieldName} is outside supported Unix seconds`);
  }

  return number;
}

function nullableInteger(value: unknown, fieldName: string): number | null {
  if (value === null || value === undefined || value === "") return null;
  return requiredInteger(value, fieldName);
}

function sortedUnique(values: readonly string[]): string[] {
  return [...new Set(values)].sort();
}
