import { deriveVisitorKey, type PageRevision } from "../sessions/page-revisions.ts";
import { canonicalJson, timestampMicros, type PageView } from "../sessions/session-engine.ts";
import { evidenceKeys, type BrowserIdentifiers } from "./identity.ts";

type Row = Record<string, unknown>;
export type HistoricalBrowserSource = "boom_domains" | "jitsu_data";
export type BrowserSource = HistoricalBrowserSource | "jitsu_events_api";
export type BrowserKind = "page_view" | "identify" | "client_form" | "client_order" | "attribution";

const SOURCE_PRIORITY: Record<BrowserSource, number> = {
  boom_domains: 1, jitsu_data: 2, jitsu_events_api: 3,
};
const IDENTITY_KIND: Record<BrowserKind, string> = {
  page_view: "segment_page_view",
  identify: "segment_identify",
  client_form: "segment_form",
  client_order: "segment_order_completed",
  attribution: "segment_attribution",
};

/** A structural subset of the existing ingress JitsuObservation. */
export interface JitsuObservationInput {
  tenant_id: string;
  message_id: string;
  event_kind: string;
  delivery_event_id: string;
  producer_id: string;
  observed_at: string;
  ingested_at: string;
  source_fact_version: number | string;
  source_deleted: 0 | 1;
  fact_payload: string;
}

export interface BrowserSourceFact extends BrowserIdentifiers {
  tenant_id: string;
  source_system: BrowserSource;
  source_priority: number;
  source_record_id: string;
  event_kind: string;
  source_revision: string;
  source_deleted: boolean;
  observed_at: string | null;
  source_updated_at: string | null;
  ingested_at: string;
  original_payload: string;
  original_payload_hash: string;
  delivery_event_id: string;
}

/** Only selected source heads may enter the identity engine. Priority stays separate. */
export interface BrowserIdentityFact {
  eventId: string;
  producerId: string;
  observedAt: string | null;
  ingestedAt: string;
  factKind: string;
  factKey: string;
  sourcePriority: number;
  sourceFactVersion: number;
  factDeleted: boolean;
  factPayloadHash: string;
  factPayload: string;
  evidenceKeys: string[];
}

export interface NormalizedBrowserEvent {
  source: BrowserSourceFact;
  pageRevision: PageRevision | null;
  identity: BrowserIdentityFact | null;
}

export interface HistoricalBrowserInput {
  tenantId: string;
  source: HistoricalBrowserSource;
  kind: BrowserKind;
  record: Row;
  ingestedAt: string;
  deleted?: boolean;
}

/** Raw payload timestamps retain precision discarded by the old ingress projection. */
export async function normalizeJitsu(observation: JitsuObservationInput): Promise<NormalizedBrowserEvent> {
  const payload = parseObject(observation.fact_payload);
  const row = flattenJitsu(payload);
  row.message_id = firstPresent(row.message_id, row.id, observation.message_id);
  return normalize({
    tenantId: observation.tenant_id,
    source: "jitsu_events_api",
    kind: observation.event_kind,
    row,
    ingestedAt: observation.ingested_at,
    deleted: observation.source_deleted === 1,
    originalPayload: observation.fact_payload,
    deliveryId: observation.delivery_event_id,
    producerId: observation.producer_id,
  });
}

/** Pass raw exported source rows, not the old enriched page-view migration seed. */
export async function normalizeHistorical(input: HistoricalBrowserInput): Promise<NormalizedBrowserEvent> {
  return normalize({
    tenantId: input.tenantId,
    source: input.source,
    kind: input.kind,
    row: input.record,
    ingestedAt: input.ingestedAt,
    deleted: input.deleted ?? false,
    originalPayload: canonicalJson(input.record),
    deliveryId: "",
    producerId: `historical:${input.source}`,
  });
}

interface Input {
  tenantId: string;
  source: BrowserSource;
  kind: string;
  row: Row;
  ingestedAt: string;
  deleted: boolean;
  originalPayload: string;
  deliveryId: string;
  producerId: string;
}

async function normalize(input: Input): Promise<NormalizedBrowserEvent> {
  if (!input.tenantId.trim()) throw new Error("Tenant ID is required");
  const recordId = requiredId(firstPresent(input.row.message_id, input.row.id));
  const observedAt = eventTime(input.row, input.kind);
  const updatedAt = sourceTime(input.row, input.kind);
  const revision = sourceRevision(updatedAt);
  const identifiers = normalizeIdentifiers(input.row, input.kind);
  const originalPayloadHash = await sha256(input.originalPayload);
  const source: BrowserSourceFact = {
    tenant_id: input.tenantId,
    source_system: input.source,
    source_priority: SOURCE_PRIORITY[input.source],
    source_record_id: recordId,
    event_kind: input.kind,
    source_revision: revision,
    source_deleted: input.deleted,
    observed_at: observedAt,
    source_updated_at: updatedAt,
    ingested_at: requiredTime(input.ingestedAt),
    original_payload: input.originalPayload,
    original_payload_hash: originalPayloadHash,
    delivery_event_id: input.deliveryId || `${input.source}:${input.kind}:${recordId}:${revision}:${originalPayloadHash}`,
    ...identifiers,
  };
  const pageRevision: PageRevision | null = input.kind === "page_view" ? {
    page_view_id: recordId,
    source_priority: source.source_priority,
    source_revision: revision,
    page: input.deleted ? null : normalizePage(input.row, source),
  } : null;
  if (!isSupportedKind(input.kind)) return { source, pageRevision, identity: null };
  const factPayload = canonicalJson({ ...identifiers, source_record_id: recordId, source_system: input.source });
  const factPayloadHash = await sha256(factPayload);
  const numericRevision = Number(revision);
  if (!Number.isSafeInteger(numericRevision)) throw new Error("Identity source revision exceeds safe integer range");
  return {
    source,
    pageRevision,
    identity: {
      eventId: source.delivery_event_id,
      producerId: input.producerId,
      observedAt,
      ingestedAt: source.ingested_at,
      factKind: IDENTITY_KIND[input.kind],
      factKey: `${IDENTITY_KIND[input.kind]}:${recordId}`,
      sourcePriority: source.source_priority,
      sourceFactVersion: numericRevision,
      factDeleted: input.deleted,
      factPayloadHash,
      factPayload,
      evidenceKeys: input.deleted ? [] : evidenceKeys(identifiers),
    },
  };
}

function normalizeIdentifiers(row: Row, kind: string): BrowserIdentifiers {
  const anonymousId = text(row.anonymous_id);
  const userId = lower(row.user_id);
  const base: BrowserIdentifiers = {
    anonymous_id: anonymousId, user_id: userId,
    email: null, phone: null, first_name: null, last_name: null,
  };
  if (kind === "page_view") return base;
  if (kind === "attribution") return { ...base, email: userId };
  if (kind === "identify") return {
    ...base,
    // The SQL coalesces before trimming. An empty email suppresses user_id fallback.
    email: lower(firstPresent(row.email, row.user_id)),
    phone: text(row.phone),
  };
  if (kind === "client_form") return {
    ...base,
    email: firstText(row.email, row.extra_submitted_fields_checkout_offer_member_email, row.context_traits_email, row.user_id)?.toLowerCase() ?? null,
    phone: firstText(row.phone, row.context_traits_phone),
    first_name: text(row.first_name),
  };
  if (kind === "client_order") {
    const name = text(row.name);
    return {
      ...base,
      email: firstText(row.email, row.context_traits_email, row.user_id)?.toLowerCase() ?? null,
      phone: firstText(row.phone, row.context_traits_phone),
      first_name: name?.split(" ")[0] ?? null,
      last_name: name?.split(" ").at(-1) ?? null,
    };
  }
  // Unknown kinds stay available as raw source facts. They do not create graph edges.
  return base;
}

function normalizePage(row: Row, source: BrowserSourceFact): PageView {
  const eventUrl = nullableString(row.url);
  const pageUrl = text(firstPresent(row.url, row.context_page_url));
  const pagePath = text(firstPresent(row.path, row.context_page_path));
  const pageHost = urlHost(pageUrl);
  const page: PageView = {
    page_view_id: source.source_record_id,
    visitor_key: deriveVisitorKey({ page_view_id: source.source_record_id, ...source }),
    page_view_timestamp: source.observed_at,
    anonymous_id: source.anonymous_id,
    user_id: source.user_id,
    page_url: pageUrl,
    page_host: pageHost,
    page_path: pagePath,
    page_path_host: (pagePath ?? "") + (pageHost ?? ""),
    ip_address: text(row.context_ip),
    user_agent: text(row.context_user_agent),
    device_platform: text(row.context_user_agent_data_platform),
    device_is_mobile: nullableBoolean(row.context_user_agent_data_mobile),
    geo_city: firstText(row.context_edge_city, row.context_geo_city_name),
    geo_region: firstText(row.context_edge_region, row.context_geo_region_name),
    geo_region_code: firstText(row.context_edge_region_code, row.context_geo_region_code),
    geo_country: firstText(row.context_edge_country, row.context_geo_country_code),
    geo_timezone: firstText(row.context_edge_timezone, row.context_geo_location_timezone),
    // V1 stores observed traffic fields. It does not classify traffic or join ads.
    is_paid_ad_click: null,
    campaign_name: null,
    adset_name: null,
    ad_name: null,
    google_campaign_type: null,
    channel_source: null,
    channel_medium: null,
  };
  const campaignFallbacks = { source: "source", medium: "medium", campaign: "name", content: "content", term: "term", id: "id" };
  for (const [suffix, fallback] of Object.entries(campaignFallbacks)) {
    const key = `utm_${suffix}` as "utm_source";
    page[key] = firstText(row[`context_attribution_${key}`], row[`context_campaign_${key}`], row[`context_campaign_${fallback}`], urlParameter(eventUrl, key));
  }
  for (const key of ["gclid", "gbraid", "wbraid", "ttclid", "li_fat_id", "rdt_cid", "twclid"] as const) {
    page[key] = firstText(row[`context_attribution_${key}`], urlParameter(eventUrl, key));
  }
  page.fbclid = firstText(urlParameter(nullableString(firstPresent(row.url, row.context_page_url)), "fbclid"), row.context_attribution_fbclid);
  for (const key of ["fbc", "fbp", "ttp", "uetsid", "uetvid"] as const) page[key] = text(row[`context_attribution_${key}`]);
  for (const key of ["campaign_id", "adset_id", "ad_id"] as const) page[key] = firstText(row[key], row[`context_attribution_${key}`]);
  page.google_keyword_id = urlParameter(eventUrl, "h_keyword_id");
  page.google_keyword = urlParameter(eventUrl, "h_keyword");
  const clickType = (["gclid", "gbraid", "wbraid", "fbclid", "ttclid", "li_fat_id", "rdt_cid", "twclid"] as const).find((key) => page[key] !== null);
  page.click_id_type = clickType ?? null;
  page.click_id = clickType ? page[clickType] ?? null : null;
  return page;
}

function flattenJitsu(payload: Row): Row {
  const event = flatten(payload);
  const properties = flatten(object(payload.properties));
  const row = { ...properties, ...event };
  if (text(payload.type)?.toLowerCase() === "identify") {
    const traits = { ...object(object(payload.context).traits), ...object(payload.traits) };
    Object.assign(row, { ...flatten(traits), ...row });
  }
  row.message_id = firstPresent(event.message_id, event.id, properties.event_id, properties.message_id);
  row.anonymous_id = firstPresent(event.anonymous_id, row.traits_anonymous_id, row.context_traits_anonymous_id);
  row.user_id = firstPresent(event.user_id, row.traits_user_id, row.context_traits_user_id);
  return row;
}

function flatten(value: Row, prefix = ""): Row {
  const result: Row = {};
  for (const [name, child] of Object.entries(value)) {
    const snake = name.replace(/([a-z0-9])([A-Z])/g, "$1_$2").toLowerCase();
    const key = prefix ? `${prefix}_${snake}` : snake;
    if (child && typeof child === "object" && !Array.isArray(child)) Object.assign(result, flatten(child as Row, key));
    else result[key] = child;
  }
  return result;
}

function eventTime(row: Row, kind: string): string | null {
  if (kind === "client_form") return optionalTime(firstPresent(row.submitted_at, row.timestamp, row.sent_at, row.received_at, row.loaded_at));
  if (kind === "client_order") return optionalTime(firstPresent(row.timestamp, row.submitted_at, row.sent_at, row.received_at, row.loaded_at));
  if (kind === "attribution") return optionalTime(firstPresent(row.timestamp, row.sent_at, row.received_at, row.loaded_at, row.original_timestamp, row.uuid_ts));
  return optionalTime(firstPresent(row.timestamp, row.sent_at, row.received_at, row.loaded_at));
}

function sourceTime(row: Row, kind: string): string | null {
  const candidates = [row.loaded_at, row.received_at, row.sent_at, row.timestamp];
  if (kind === "client_form" || kind === "client_order") candidates.push(row.submitted_at);
  if (kind === "attribution") candidates.push(row.original_timestamp, row.uuid_ts);
  return optionalTime(firstPresent(...candidates));
}

function sourceRevision(updatedAt: string | null): string {
  if (updatedAt === null) return "0";
  const micros = timestampMicros(updatedAt);
  if (micros < 0n) throw new Error("Pre-epoch source revisions require an explicit source ordering policy");
  return (micros + 1n).toString();
}

/** Matches the SQL helper, including no percent decoding and only the first '=' value. */
export function urlParameter(url: string | null, parameter: string): string | null {
  const query = url?.split("?")[1];
  if (query === undefined) return null;
  const match = query.split("&").find((part) => part.split("=")[0].toLowerCase() === parameter.toLowerCase());
  return text(match?.split("=")[1]);
}

function urlHost(url: string | null): string | null {
  if (url === null) return null;
  // Preserve the host spelling. URL.hostname would punycode or normalize it.
  if (url.startsWith("/") && !url.startsWith("//")) return null;
  const authority = url.replace(/^[a-z][a-z0-9+.-]*:\/\//i, "").replace(/^\/\//, "").split(/[/?#]/)[0];
  const withoutCredentials = authority.slice(authority.lastIndexOf("@") + 1);
  const host = withoutCredentials.startsWith("[")
    ? withoutCredentials.match(/^\[[^\]]+\]/)?.[0] ?? null
    : withoutCredentials.split(":")[0];
  if (!host || /\s/.test(host)) return null;
  return host.toLowerCase();
}

function firstPresent(...values: unknown[]): unknown { return values.find((value) => value !== undefined && value !== null) ?? null; }
function firstText(...values: unknown[]): string | null { return values.map(text).find((value) => value !== null) ?? null; }
function text(value: unknown): string | null { return nullableString(value)?.trim() || null; }
function lower(value: unknown): string | null { return text(value)?.toLowerCase() ?? null; }
function nullableString(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "string") throw new Error("Expected a string source field");
  return value;
}
function nullableBoolean(value: unknown): boolean | null {
  if (value === null || value === undefined) return null;
  if (value === true || value === 1) return true;
  if (value === false || value === 0) return false;
  throw new Error("Expected a boolean source field");
}
function requiredId(value: unknown): string {
  const id = nullableString(value);
  if (!id?.trim()) throw new Error("Source record ID is required");
  return id;
}
function optionalTime(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "number" && Number.isFinite(value)) {
    const milliseconds = Math.abs(value) < 100_000_000_000 ? value * 1000 : value;
    return formatTimestamp(timestampMicros(new Date(milliseconds).toISOString()));
  }
  const timestamp = nullableString(value)!;
  return formatTimestamp(timestampMicros(timestamp));
}
function requiredTime(value: string): string { return formatTimestamp(timestampMicros(value)); }
function formatTimestamp(micros: bigint): string {
  let seconds = micros / 1_000_000n;
  let fraction = micros % 1_000_000n;
  if (fraction < 0n) { seconds -= 1n; fraction += 1_000_000n; }
  return `${new Date(Number(seconds * 1_000n)).toISOString().slice(0, 19)}.${fraction.toString().padStart(6, "0")}Z`;
}
function isSupportedKind(kind: string): kind is BrowserKind { return Object.hasOwn(IDENTITY_KIND, kind); }
function object(value: unknown): Row { return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Row : {}; }
function parseObject(value: string): Row {
  const parsed: unknown = JSON.parse(value);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("Browser payload must be an object");
  return parsed as Row;
}
async function sha256(value: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}
