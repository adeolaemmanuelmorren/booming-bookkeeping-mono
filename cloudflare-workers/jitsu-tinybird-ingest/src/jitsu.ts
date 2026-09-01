import {
  isJsonObject,
  RequestError,
  sha256Hex,
  stableStringify,
  utf8ByteLength,
} from "./json";
import type {
  JitsuObservation,
  JsonObject,
  JsonValue,
  QueueEnvelope,
} from "./types";

export const MAX_REQUEST_BYTES = 10_000_000;
export const MAX_EVENTS_PER_REQUEST = 10_000;
export const MAX_QUEUE_MESSAGE_BYTES = 120_000;
export const QUEUE_SCHEMA_VERSION = "jitsu_events_api_v1" as const;

type ObservationDraft = Omit<
  JitsuObservation,
  "delivery_event_id" | "producer_id" | "producer_sequence" | "tenant_id"
>;

export interface JitsuDelivery {
  producerId: string;
  observations: JitsuObservation[];
  envelopes: QueueEnvelope[];
}

export async function parseJitsuRequest(
  request: Request,
): Promise<JsonObject[]> {
  const contentLength = Number(request.headers.get("content-length"));

  if (Number.isFinite(contentLength) && contentLength > MAX_REQUEST_BYTES) {
    throw new RequestError("Request body exceeds 10 MB", 413);
  }

  const body = await request.text();

  if (utf8ByteLength(body) > MAX_REQUEST_BYTES) {
    throw new RequestError("Request body exceeds 10 MB", 413);
  }

  if (body.trim() === "") {
    throw new RequestError("Request body is empty", 400);
  }

  const contentType = request.headers.get("content-type")?.toLowerCase() ?? "";
  const events = contentType.includes("ndjson")
    ? parseNdjson(body)
    : parseJson(body);

  if (events.length === 0) {
    throw new RequestError("Request contains no Jitsu events", 400);
  }

  if (events.length > MAX_EVENTS_PER_REQUEST) {
    throw new RequestError("Request contains more than 10,000 events", 413);
  }

  return events;
}

export async function buildJitsuDelivery(
  events: JsonObject[],
  tenantId: string,
  maxQueueMessageBytes = MAX_QUEUE_MESSAGE_BYTES,
): Promise<JitsuDelivery> {
  const trimmedTenantId = tenantId.trim();

  if (trimmedTenantId === "") {
    throw new RequestError("TENANT_ID is not configured", 503);
  }

  const acceptedAt = new Date().toISOString();
  const drafts = await Promise.all(
    events.map((event, index) => buildObservationDraft(event, index, acceptedAt)),
  );
  drafts.sort(compareDrafts);
  rejectConflictingMessageIds(drafts);

  const batchFingerprint = drafts
    .map((draft) => `${draft.message_id}:${draft.payload_hash}`)
    .join("\n");
  const batchHash = await sha256Hex(
    `${QUEUE_SCHEMA_VERSION}\n${trimmedTenantId}\n${batchFingerprint}`,
  );
  const producerId = `jitsu-webhook-v1:${batchHash}`;
  const observations = await Promise.all(
    drafts.map(async (draft, index) => {
      const producerSequence = index + 1;
      const deliveryEventId = await sha256Hex(
        `${producerId}:${producerSequence}:${draft.message_id}:${draft.payload_hash}`,
      );

      return {
        tenant_id: trimmedTenantId,
        producer_id: producerId,
        producer_sequence: producerSequence,
        delivery_event_id: deliveryEventId,
        ...draft,
      } satisfies JitsuObservation;
    }),
  );

  return {
    producerId,
    observations,
    envelopes: buildQueueEnvelopes(
      producerId,
      observations,
      maxQueueMessageBytes,
    ),
  };
}

export function buildQueueEnvelopes(
  producerId: string,
  observations: JitsuObservation[],
  maxBytes = MAX_QUEUE_MESSAGE_BYTES,
): QueueEnvelope[] {
  const envelopes: QueueEnvelope[] = [];
  let events: JitsuObservation[] = [];

  for (const observation of observations) {
    const candidate = createEnvelope(producerId, [...events, observation]);

    if (queueEnvelopeBytes(candidate) <= maxBytes) {
      events = candidate.events;
      continue;
    }

    if (events.length === 0) {
      throw new RequestError(
        `Jitsu event ${observation.message_id} exceeds the Queue message limit`,
        413,
      );
    }

    envelopes.push(createEnvelope(producerId, events));
    const singleEventEnvelope = createEnvelope(producerId, [observation]);

    if (queueEnvelopeBytes(singleEventEnvelope) > maxBytes) {
      throw new RequestError(
        `Jitsu event ${observation.message_id} exceeds the Queue message limit`,
        413,
      );
    }

    events = [observation];
  }

  if (events.length > 0) {
    envelopes.push(createEnvelope(producerId, events));
  }

  return envelopes;
}

export function queueEnvelopeBytes(envelope: QueueEnvelope): number {
  return utf8ByteLength(JSON.stringify(envelope));
}

function parseJson(body: string): JsonObject[] {
  let parsed: unknown;

  try {
    parsed = JSON.parse(body);
  } catch {
    throw new RequestError("Request body is not valid JSON", 400);
  }

  return extractEvents(parsed);
}

function parseNdjson(body: string): JsonObject[] {
  const events: JsonObject[] = [];
  const lines = body.split(/\r?\n/);

  for (const [index, line] of lines.entries()) {
    if (line.trim() === "") {
      continue;
    }

    let parsed: unknown;

    try {
      parsed = JSON.parse(line);
    } catch {
      throw new RequestError(`NDJSON line ${index + 1} is not valid JSON`, 400);
    }

    events.push(...extractEvents(parsed));
  }

  return events;
}

function extractEvents(value: unknown): JsonObject[] {
  if (Array.isArray(value)) {
    return value.flatMap(extractEvents);
  }

  if (!isJsonObject(value)) {
    throw new RequestError("Every Jitsu event must be a JSON object", 400);
  }

  if (Array.isArray(value.batch)) {
    return value.batch.flatMap(extractEvents);
  }

  if (Array.isArray(value.events)) {
    return value.events.flatMap(extractEvents);
  }

  return [value];
}

async function buildObservationDraft(
  event: JsonObject,
  index: number,
  acceptedAt: string,
): Promise<ObservationDraft> {
  const properties = objectValue(event.properties);
  const context = objectValue(event.context);
  const traits = {
    ...objectValue(context.traits),
    ...objectValue(event.traits),
  };
  const page = objectValue(context.page);
  const attribution = objectValue(context.attribution);
  const campaign = objectValue(context.campaign);
  const factPayload = stableStringify(event);
  const factPayloadHash = await sha256Hex(factPayload);
  const messageId =
    firstString(
      event.messageId,
      event.message_id,
      event.id,
      properties.event_id,
      properties.message_id,
    ) ?? `jitsu_${factPayloadHash}`;
  const observedAt = firstTimestamp(
    event.timestamp,
    event.sentAt,
    event.sent_at,
    event.receivedAt,
    event.received_at,
    properties.timestamp,
  );
  const sourceVersionAt = firstTimestamp(
    event.receivedAt,
    event.received_at,
    event.sentAt,
    event.sent_at,
    event.timestamp,
  );

  if (!observedAt || !sourceVersionAt) {
    throw new RequestError(
      `Jitsu event ${messageId || index + 1} has no valid timestamp`,
      400,
    );
  }

  const sourceFactVersion = Date.parse(sourceVersionAt) * 1_000;
  const eventName = firstString(event.event, event.name, properties.event_name);
  const eventType = firstString(event.type, event.event_type);
  const bodyWithoutHash = {
    payload_hash: "",
    message_id: messageId,
    event_kind: eventKind(eventType, eventName),
    observed_at: observedAt,
    // The Queue consumer replaces this with the Tinybird delivery-attempt time.
    // Keeping it operational (rather than upstream-authored) makes incremental
    // scans include events that spent longer than the overlap window in Queue.
    ingested_at: acceptedAt,
    event_timestamp: firstTimestamp(event.timestamp),
    anonymous_id: firstString(
      event.anonymousId,
      event.anonymous_id,
      traits.anonymousId,
      traits.anonymous_id,
    ),
    user_id: firstString(event.userId, event.user_id, traits.userId, traits.user_id),
    email: firstString(event.email, traits.email, properties.email),
    phone: firstString(
      event.phone,
      traits.phone,
      traits.phone_number,
      traits.phoneNumber,
      properties.phone,
      properties.phone_number,
    ),
    first_name: firstString(
      event.first_name,
      event.firstName,
      traits.first_name,
      traits.firstName,
      properties.first_name,
      properties.firstName,
    ),
    last_name: firstString(
      event.last_name,
      event.lastName,
      traits.last_name,
      traits.lastName,
      properties.last_name,
      properties.lastName,
    ),
    customer_name: firstString(
      event.customer_name,
      traits.name,
      properties.customer_name,
      properties.customerName,
      properties.name,
    ),
    page_url: firstString(
      event.page_url,
      event.url,
      properties.page_url,
      properties.pageUrl,
      properties.url,
      page.url,
    ),
    page_path: firstString(
      event.page_path,
      event.path,
      properties.page_path,
      properties.pagePath,
      properties.path,
      page.path,
    ),
    page_referrer: firstString(
      event.page_referrer,
      event.referrer,
      properties.page_referrer,
      properties.referrer,
      page.referrer,
    ),
    form_id: firstString(event.form_id, properties.form_id, properties.formId),
    form_name: firstString(
      event.form_name,
      properties.form_name,
      properties.formName,
    ),
    form_action: firstString(
      event.form_action,
      properties.form_action,
      properties.formAction,
    ),
    submitted_at: firstTimestamp(
      event.submitted_at,
      event.submittedAt,
      properties.submitted_at,
      properties.submittedAt,
    ),
    is_checkout_form: firstBoolean(
      event.is_checkout_form,
      properties.is_checkout_form,
      properties.isCheckoutForm,
    ),
    is_payment_confirmed: firstBoolean(
      event.is_payment_confirmed,
      properties.is_payment_confirmed,
      properties.isPaymentConfirmed,
    ),
    payment_status: firstString(
      event.payment_status,
      properties.payment_status,
      properties.paymentStatus,
    ),
    amount: firstNumber(event.amount, properties.amount),
    value: firstNumber(event.value, properties.value),
    currency: firstString(event.currency, properties.currency),
    product_id: firstString(
      event.product_id,
      properties.product_id,
      properties.productId,
    ),
    product_name: firstString(
      event.product_name,
      properties.product_name,
      properties.productName,
    ),
    products: firstJsonString(event.products, properties.products, properties.contents),
    utm_source: attributionString("utm_source", event, properties, attribution),
    utm_medium: attributionString("utm_medium", event, properties, attribution),
    utm_campaign: attributionString("utm_campaign", event, properties, attribution),
    utm_content: attributionString("utm_content", event, properties, attribution),
    utm_term: attributionString("utm_term", event, properties, attribution),
    utm_id: attributionString("utm_id", event, properties, attribution),
    campaign_id: firstString(
      event.campaign_id,
      properties.campaign_id,
      attribution.campaign_id,
      campaign.id,
    ),
    adset_id: firstString(event.adset_id, properties.adset_id, attribution.adset_id),
    ad_id: firstString(event.ad_id, properties.ad_id, attribution.ad_id),
    fbclid: attributionString("fbclid", event, properties, attribution),
    source_fact_version: sourceFactVersion,
    source_deleted: 0 as const,
    fact_payload_hash: factPayloadHash,
    fact_payload: factPayload,
  } satisfies ObservationDraft;
  const payloadHashInput = stableStringify({
    ...bodyWithoutHash,
    // Delivery retries may change only this operational timestamp.
    ingested_at: "",
  } as unknown as JsonObject);

  return {
    ...bodyWithoutHash,
    payload_hash: await sha256Hex(payloadHashInput),
  };
}

function compareDrafts(left: ObservationDraft, right: ObservationDraft): number {
  return (
    compareText(left.message_id, right.message_id) ||
    compareText(left.payload_hash, right.payload_hash) ||
    compareText(left.fact_payload, right.fact_payload)
  );
}

function compareText(left: string, right: string): number {
  if (left < right) {
    return -1;
  }

  if (left > right) {
    return 1;
  }

  return 0;
}

function rejectConflictingMessageIds(drafts: ObservationDraft[]): void {
  const payloadsByMessageId = new Map<string, string>();

  for (const draft of drafts) {
    const existingPayloadHash = payloadsByMessageId.get(draft.message_id);

    if (existingPayloadHash && existingPayloadHash !== draft.payload_hash) {
      throw new RequestError(
        `Jitsu message_id ${draft.message_id} has conflicting payloads`,
        400,
      );
    }

    payloadsByMessageId.set(draft.message_id, draft.payload_hash);
  }
}

function eventKind(type: string | null, name: string | null): string {
  const normalizedType = normalizeLabel(type);
  const normalizedName = normalizeLabel(name);

  if (normalizedType === "page") {
    return "page_view";
  }

  if (normalizedType === "identify") {
    return "identify";
  }

  if (normalizedName === "form_submitted") {
    return "client_form";
  }

  if (normalizedName === "order_completed") {
    return "client_order";
  }

  if (normalizedName === "attr" || normalizedName === "attribution") {
    return "attribution";
  }

  return normalizedType || normalizedName || "event";
}

function normalizeLabel(value: string | null): string {
  return (value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function attributionString(
  key: string,
  event: JsonObject,
  properties: JsonObject,
  attribution: JsonObject,
): string | null {
  return firstString(event[key], properties[key], attribution[key]);
}

function objectValue(value: JsonValue | undefined): JsonObject {
  return isJsonObject(value) ? value : {};
}

function firstString(...values: Array<JsonValue | undefined>): string | null {
  for (const value of values) {
    if (typeof value !== "string") {
      continue;
    }

    const trimmed = value.trim();

    if (trimmed !== "") {
      return trimmed;
    }
  }

  return null;
}

function firstBoolean(...values: Array<JsonValue | undefined>): boolean | null {
  for (const value of values) {
    if (typeof value === "boolean") {
      return value;
    }

    if (value === 1 || value === "1" || value === "true") {
      return true;
    }

    if (value === 0 || value === "0" || value === "false") {
      return false;
    }
  }

  return null;
}

function firstNumber(...values: Array<JsonValue | undefined>): number | null {
  for (const value of values) {
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }

    if (typeof value !== "string" || value.trim() === "") {
      continue;
    }

    const parsed = Number(value);

    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return null;
}

function firstJsonString(...values: Array<JsonValue | undefined>): string | null {
  for (const value of values) {
    if (value === undefined || value === null) {
      continue;
    }

    if (typeof value === "string") {
      const trimmed = value.trim();
      return trimmed === "" ? null : trimmed;
    }

    return stableStringify(value);
  }

  return null;
}

function firstTimestamp(...values: Array<JsonValue | undefined>): string | null {
  for (const value of values) {
    const timestamp = parseTimestamp(value);

    if (timestamp) {
      return timestamp;
    }
  }

  return null;
}

function parseTimestamp(value: JsonValue | undefined): string | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    const milliseconds = Math.abs(value) < 100_000_000_000 ? value * 1_000 : value;
    const date = new Date(milliseconds);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
  }

  if (typeof value !== "string" || value.trim() === "") {
    return null;
  }

  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function createEnvelope(
  producerId: string,
  events: JitsuObservation[],
): QueueEnvelope {
  return {
    schema_version: QUEUE_SCHEMA_VERSION,
    producer_id: producerId,
    events,
  };
}
