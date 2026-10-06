// ../../../../Boom Bookkeeping/tinybird-v1/worker/sessions/session-engine.ts
function compareStrings(left, right) {
  const leftPoints = Array.from(left, (value) => value.codePointAt(0));
  const rightPoints = Array.from(right, (value) => value.codePointAt(0));
  for (let index = 0; index < Math.min(leftPoints.length, rightPoints.length); index++) {
    if (leftPoints[index] !== rightPoints[index]) return leftPoints[index] - rightPoints[index];
  }
  return leftPoints.length - rightPoints.length;
}
function timestampMicros(value) {
  const match = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?(Z| UTC|[+-]\d{2}:\d{2})?$/.exec(value);
  if (!match) throw new Error(`Invalid timestamp: ${value}`);
  const zone = !match[4] || match[4] === " UTC" ? "Z" : match[4];
  const milliseconds = Date.parse(`${match[1]}T${match[2]}${zone}`);
  if (!Number.isFinite(milliseconds)) throw new Error(`Invalid timestamp: ${value}`);
  const dateOnly = /* @__PURE__ */ new Date(`${match[1]}T00:00:00Z`);
  if (dateOnly.toISOString().slice(0, 10) !== match[1] || match[2].slice(0, 2) === "24") {
    throw new Error(`Invalid timestamp: ${value}`);
  }
  return BigInt(milliseconds) * 1000n + BigInt((match[3] ?? "").padEnd(6, "0"));
}
function canonicalJson(value) {
  if (value === void 0) return "undefined";
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const record = value;
  const entries = Object.keys(record).filter((key) => record[key] !== void 0).sort(compareStrings);
  return `{${entries.map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`).join(",")}}`;
}

// ../../../../Boom Bookkeeping/tinybird-v1/worker/sessions/page-revisions.ts
function deriveVisitorKey(input) {
  if (!input.page_view_id) throw new Error("Page ID is required");
  return input.anonymous_id?.trim() || input.user_id?.trim().toLowerCase() || input.page_view_id;
}

// ../../../../Boom Bookkeeping/tinybird-v1/worker/browser/identity.ts
function normalizePhone(value) {
  if (value === null) return null;
  let digits = value.trim().replace(/[^0-9]/g, "");
  if (digits.length === 11 && digits.startsWith("1")) digits = digits.slice(1);
  if (!/^[2-9][0-9]{2}[2-9][0-9]{6}$/.test(digits)) return null;
  if (digits.slice(3, 6) === "555") return null;
  if (/(0000000|1111111|2222222|3333333|4444444|5555555|6666666|7777777|8888888|9999999)$/.test(digits)) return null;
  if (/^(0123456789|1234567890|234567890[0-9]|9876543210)$/.test(digits)) return null;
  return `+1${digits}`;
}
function canonicalEmail(value) {
  const email = value?.trim().toLowerCase() || null;
  if (!email) return null;
  const [local, domain] = email.split("@");
  if (domain !== "gmail.com" && domain !== "googlemail.com") return email;
  return `${local.replace(/[+].*$/, "").replace(/\./g, "")}@gmail.com`;
}
function evidenceKeys(identifiers) {
  const values = {
    anonymous_id: identifiers.anonymous_id,
    user_id: identifiers.user_id,
    email: identifiers.email,
    canonical_email: canonicalEmail(identifiers.email),
    phone: normalizePhone(identifiers.phone)
  };
  return Object.entries(values).filter(([, value]) => value !== null).map(([type, value]) => `${type}:${value}`).sort(compareStrings);
}

// ../../../../Boom Bookkeeping/tinybird-v1/worker/browser/normalize.ts
var SOURCE_PRIORITY = {
  boom_domains: 1,
  jitsu_data: 2,
  jitsu_events_api: 3
};
var IDENTITY_KIND = {
  page_view: "segment_page_view",
  identify: "segment_identify",
  client_form: "segment_form",
  client_order: "segment_order_completed",
  attribution: "segment_attribution"
};
async function normalizeJitsu(observation) {
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
    producerId: observation.producer_id
  });
}
async function normalizeHistorical(input) {
  return normalize({
    tenantId: input.tenantId,
    source: input.source,
    kind: input.kind,
    row: input.record,
    ingestedAt: input.ingestedAt,
    deleted: input.deleted ?? false,
    originalPayload: canonicalJson(input.record),
    deliveryId: "",
    producerId: `historical:${input.source}`
  });
}
async function normalize(input) {
  if (!input.tenantId.trim()) throw new Error("Tenant ID is required");
  const recordId = requiredId(firstPresent(input.row.message_id, input.row.id));
  const observedAt = eventTime(input.row, input.kind);
  const updatedAt = sourceTime(input.row, input.kind);
  const revision = sourceRevision(updatedAt);
  const identifiers = normalizeIdentifiers(input.row, input.kind);
  const originalPayloadHash = await sha256(input.originalPayload);
  const source = {
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
    ...identifiers
  };
  const pageRevision = input.kind === "page_view" ? {
    page_view_id: recordId,
    source_priority: source.source_priority,
    source_revision: revision,
    page: input.deleted ? null : normalizePage(input.row, source)
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
      evidenceKeys: input.deleted ? [] : evidenceKeys(identifiers)
    }
  };
}
function normalizeIdentifiers(row, kind) {
  const anonymousId = text(row.anonymous_id);
  const userId = lower(row.user_id);
  const base = {
    anonymous_id: anonymousId,
    user_id: userId,
    email: null,
    phone: null,
    first_name: null,
    last_name: null
  };
  if (kind === "page_view") return base;
  if (kind === "attribution") return { ...base, email: userId };
  if (kind === "identify") return {
    ...base,
    // The SQL coalesces before trimming. An empty email suppresses user_id fallback.
    email: lower(firstPresent(row.email, row.user_id)),
    phone: text(row.phone)
  };
  if (kind === "client_form") return {
    ...base,
    email: firstText(row.email, row.extra_submitted_fields_checkout_offer_member_email, row.context_traits_email, row.user_id)?.toLowerCase() ?? null,
    phone: firstText(row.phone, row.context_traits_phone),
    first_name: text(row.first_name)
  };
  if (kind === "client_order") {
    const name = text(row.name);
    return {
      ...base,
      email: firstText(row.email, row.context_traits_email, row.user_id)?.toLowerCase() ?? null,
      phone: firstText(row.phone, row.context_traits_phone),
      first_name: name?.split(" ")[0] ?? null,
      last_name: name?.split(" ").at(-1) ?? null
    };
  }
  return base;
}
function normalizePage(row, source) {
  const eventUrl = nullableString(row.url);
  const pageUrl = text(firstPresent(row.url, row.context_page_url));
  const pagePath = text(firstPresent(row.path, row.context_page_path));
  const pageHost = urlHost(pageUrl);
  const page = {
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
    channel_medium: null
  };
  const campaignFallbacks = { source: "source", medium: "medium", campaign: "name", content: "content", term: "term", id: "id" };
  for (const [suffix, fallback] of Object.entries(campaignFallbacks)) {
    const key = `utm_${suffix}`;
    page[key] = firstText(row[`context_attribution_${key}`], row[`context_campaign_${key}`], row[`context_campaign_${fallback}`], urlParameter(eventUrl, key));
  }
  for (const key of ["gclid", "gbraid", "wbraid", "ttclid", "li_fat_id", "rdt_cid", "twclid"]) {
    page[key] = firstText(row[`context_attribution_${key}`], urlParameter(eventUrl, key));
  }
  page.fbclid = firstText(urlParameter(nullableString(firstPresent(row.url, row.context_page_url)), "fbclid"), row.context_attribution_fbclid);
  for (const key of ["fbc", "fbp", "ttp", "uetsid", "uetvid"]) page[key] = text(row[`context_attribution_${key}`]);
  for (const key of ["campaign_id", "adset_id", "ad_id"]) page[key] = firstText(row[key], row[`context_attribution_${key}`]);
  page.google_keyword_id = urlParameter(eventUrl, "h_keyword_id");
  page.google_keyword = urlParameter(eventUrl, "h_keyword");
  const clickType = ["gclid", "gbraid", "wbraid", "fbclid", "ttclid", "li_fat_id", "rdt_cid", "twclid"].find((key) => page[key] !== null);
  page.click_id_type = clickType ?? null;
  page.click_id = clickType ? page[clickType] ?? null : null;
  return page;
}
function flattenJitsu(payload) {
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
function flatten(value, prefix = "") {
  const result = {};
  for (const [name, child] of Object.entries(value)) {
    const snake = name.replace(/([a-z0-9])([A-Z])/g, "$1_$2").toLowerCase();
    const key = prefix ? `${prefix}_${snake}` : snake;
    if (child && typeof child === "object" && !Array.isArray(child)) Object.assign(result, flatten(child, key));
    else result[key] = child;
  }
  return result;
}
function eventTime(row, kind) {
  if (kind === "client_form") return optionalTime(firstPresent(row.submitted_at, row.timestamp, row.sent_at, row.received_at, row.loaded_at));
  if (kind === "client_order") return optionalTime(firstPresent(row.timestamp, row.submitted_at, row.sent_at, row.received_at, row.loaded_at));
  if (kind === "attribution") return optionalTime(firstPresent(row.timestamp, row.sent_at, row.received_at, row.loaded_at, row.original_timestamp, row.uuid_ts));
  return optionalTime(firstPresent(row.timestamp, row.sent_at, row.received_at, row.loaded_at));
}
function sourceTime(row, kind) {
  const candidates = [row.loaded_at, row.received_at, row.sent_at, row.timestamp];
  if (kind === "client_form" || kind === "client_order") candidates.push(row.submitted_at);
  if (kind === "attribution") candidates.push(row.original_timestamp, row.uuid_ts);
  return optionalTime(firstPresent(...candidates));
}
function sourceRevision(updatedAt) {
  if (updatedAt === null) return "0";
  const micros = timestampMicros(updatedAt);
  if (micros < 0n) throw new Error("Pre-epoch source revisions require an explicit source ordering policy");
  return (micros + 1n).toString();
}
function urlParameter(url, parameter) {
  const query = url?.split("?")[1];
  if (query === void 0) return null;
  const match = query.split("&").find((part) => part.split("=")[0].toLowerCase() === parameter.toLowerCase());
  return text(match?.split("=")[1]);
}
function urlHost(url) {
  if (url === null) return null;
  if (url.startsWith("/") && !url.startsWith("//")) return null;
  const authority = url.replace(/^[a-z][a-z0-9+.-]*:\/\//i, "").replace(/^\/\//, "").split(/[/?#]/)[0];
  const withoutCredentials = authority.slice(authority.lastIndexOf("@") + 1);
  const host = withoutCredentials.startsWith("[") ? withoutCredentials.match(/^\[[^\]]+\]/)?.[0] ?? null : withoutCredentials.split(":")[0];
  if (!host || /\s/.test(host)) return null;
  return host.toLowerCase();
}
function firstPresent(...values) {
  return values.find((value) => value !== void 0 && value !== null) ?? null;
}
function firstText(...values) {
  return values.map(text).find((value) => value !== null) ?? null;
}
function text(value) {
  return nullableString(value)?.trim() || null;
}
function lower(value) {
  return text(value)?.toLowerCase() ?? null;
}
function nullableString(value) {
  if (value === null || value === void 0) return null;
  if (typeof value !== "string") throw new Error("Expected a string source field");
  return value;
}
function nullableBoolean(value) {
  if (value === null || value === void 0) return null;
  if (value === true || value === 1) return true;
  if (value === false || value === 0) return false;
  throw new Error("Expected a boolean source field");
}
function requiredId(value) {
  const id = nullableString(value);
  if (!id?.trim()) throw new Error("Source record ID is required");
  return id;
}
function optionalTime(value) {
  if (value === null || value === void 0) return null;
  if (typeof value === "number" && Number.isFinite(value)) {
    const milliseconds = Math.abs(value) < 1e11 ? value * 1e3 : value;
    return formatTimestamp(timestampMicros(new Date(milliseconds).toISOString()));
  }
  const timestamp = nullableString(value);
  return formatTimestamp(timestampMicros(timestamp));
}
function requiredTime(value) {
  return formatTimestamp(timestampMicros(value));
}
function formatTimestamp(micros) {
  let seconds = micros / 1000000n;
  let fraction = micros % 1000000n;
  if (fraction < 0n) {
    seconds -= 1n;
    fraction += 1000000n;
  }
  return `${new Date(Number(seconds * 1000n)).toISOString().slice(0, 19)}.${fraction.toString().padStart(6, "0")}Z`;
}
function isSupportedKind(kind) {
  return Object.hasOwn(IDENTITY_KIND, kind);
}
function object(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value : {};
}
function parseObject(value) {
  const parsed = JSON.parse(value);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("Browser payload must be an object");
  return parsed;
}
async function sha256(value) {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}
export {
  normalizeHistorical,
  normalizeJitsu,
  urlParameter
};
