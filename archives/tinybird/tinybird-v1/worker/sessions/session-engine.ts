import { md5Hex } from "./md5.ts";

export interface PageView {
  page_view_id: string;
  visitor_key: string;
  page_view_timestamp: string | null;
  anonymous_id?: string | null;
  user_id?: string | null;
  page_url?: string | null;
  page_host?: string | null;
  page_path?: string | null;
  page_path_host?: string | null;
  ip_address?: string | null;
  user_agent?: string | null;
  device_platform?: string | null;
  device_is_mobile?: boolean | null;
  geo_city?: string | null;
  geo_region?: string | null;
  geo_region_code?: string | null;
  geo_country?: string | null;
  geo_timezone?: string | null;
  utm_source?: string | null;
  utm_medium?: string | null;
  utm_campaign?: string | null;
  utm_content?: string | null;
  utm_term?: string | null;
  utm_id?: string | null;
  is_paid_ad_click?: boolean | null;
  campaign_id?: string | null;
  adset_id?: string | null;
  ad_id?: string | null;
  campaign_name?: string | null;
  adset_name?: string | null;
  ad_name?: string | null;
  google_campaign_type?: string | null;
  google_keyword_id?: string | null;
  google_keyword?: string | null;
  click_id?: string | null;
  click_id_type?: string | null;
  gclid?: string | null;
  gbraid?: string | null;
  wbraid?: string | null;
  fbclid?: string | null;
  ttclid?: string | null;
  li_fat_id?: string | null;
  rdt_cid?: string | null;
  twclid?: string | null;
  fbc?: string | null;
  fbp?: string | null;
  ttp?: string | null;
  uetsid?: string | null;
  uetvid?: string | null;
  channel_source?: string | null;
  channel_medium?: string | null;
}

const FIRST_PAGE_ATTRIBUTES = [
  "anonymous_id", "user_id", "ip_address", "user_agent", "device_platform",
  "device_is_mobile", "geo_city", "geo_region", "geo_region_code", "geo_country",
  "geo_timezone", "utm_source", "utm_medium", "utm_campaign", "utm_content",
  "utm_term", "utm_id", "is_paid_ad_click", "campaign_id", "adset_id", "ad_id",
  "campaign_name", "adset_name", "ad_name", "google_campaign_type",
  "google_keyword_id", "google_keyword", "click_id", "click_id_type", "gclid",
  "gbraid", "wbraid", "fbclid", "ttclid", "li_fat_id", "rdt_cid", "twclid",
  "fbc", "fbp", "ttp", "uetsid", "uetvid", "channel_source", "channel_medium",
] as const;

type FirstPageAttributes = {
  [Key in typeof FIRST_PAGE_ATTRIBUTES[number]]: Exclude<PageView[Key], undefined>;
};

export interface SessionFact extends FirstPageAttributes {
  session_id: string;
  visitor_key: string;
  session_start_timestamp: string;
  session_end_timestamp: string;
  session_duration_seconds: number;
  page_view_count: number;
  first_page_view_id: string;
  first_page_url: string | null;
  first_page_host: string | null;
  first_page_path: string | null;
  first_page_path_host: string | null;
  last_page_view_id: string;
  last_page_url: string | null;
  last_page_path: string | null;
}

interface TimedPage {
  page: PageView;
  timestampMicros: bigint;
}

/** Input must contain the complete current history for this visitor. */
export function buildSessions(visitorKey: string, pages: readonly PageView[]): SessionFact[] {
  if (!visitorKey) throw new Error("Visitor key is required");
  const uniquePages = new Map<string, PageView>();
  for (const page of pages) {
    if (page.visitor_key !== visitorKey) throw new Error("Page belongs to another visitor");
    if (!page.page_view_id) throw new Error("Page ID is required");
    const existing = uniquePages.get(page.page_view_id);
    if (existing && canonicalJson(existing) !== canonicalJson(page)) {
      throw new Error(`Conflicting current pages for ${page.page_view_id}`);
    }
    uniquePages.set(page.page_view_id, page);
  }

  const timedPages: TimedPage[] = [];
  for (const page of uniquePages.values()) {
    if (page.page_view_timestamp === null) continue;
    timedPages.push({ page, timestampMicros: timestampMicros(page.page_view_timestamp) });
  }
  timedPages.sort(comparePages);

  const sessions: SessionFact[] = [];
  let currentPages: TimedPage[] = [];
  for (const page of timedPages) {
    const previous = currentPages.at(-1);
    const startsSession = previous
      && (page.timestampMicros - previous.timestampMicros) / 60_000_000n > 30n;
    if (startsSession) {
      sessions.push(rollUp(visitorKey, sessions.length + 1, currentPages));
      currentPages = [];
    }
    currentPages.push(page);
  }
  if (currentPages.length) sessions.push(rollUp(visitorKey, sessions.length + 1, currentPages));
  return sessions;
}

function rollUp(visitorKey: string, ordinal: number, pages: TimedPage[]): SessionFact {
  const first = pages[0];
  const last = pages[pages.length - 1];
  const attributes = Object.fromEntries(
    FIRST_PAGE_ATTRIBUTES.map((key) => [key, first.page[key] ?? null]),
  ) as FirstPageAttributes;
  return {
    ...attributes,
    session_id: md5Hex(`${visitorKey}|${ordinal}`),
    visitor_key: visitorKey,
    session_start_timestamp: formatTimestamp(first.timestampMicros),
    session_end_timestamp: formatTimestamp(last.timestampMicros),
    session_duration_seconds: Number((last.timestampMicros - first.timestampMicros) / 1_000_000n),
    page_view_count: pages.length,
    first_page_view_id: first.page.page_view_id,
    first_page_url: first.page.page_url ?? null,
    first_page_host: first.page.page_host ?? null,
    first_page_path: first.page.page_path ?? null,
    first_page_path_host: first.page.page_path_host ?? null,
    last_page_view_id: last.page.page_view_id,
    last_page_url: last.page.page_url ?? null,
    last_page_path: last.page.page_path ?? null,
  };
}

function comparePages(left: TimedPage, right: TimedPage): number {
  if (left.timestampMicros < right.timestampMicros) return -1;
  if (left.timestampMicros > right.timestampMicros) return 1;
  return compareStrings(left.page.page_view_id, right.page.page_view_id);
}

/** BigQuery orders strings by Unicode code point, not the machine's locale. */
export function compareStrings(left: string, right: string): number {
  const leftPoints = Array.from(left, (value) => value.codePointAt(0)!);
  const rightPoints = Array.from(right, (value) => value.codePointAt(0)!);
  for (let index = 0; index < Math.min(leftPoints.length, rightPoints.length); index++) {
    if (leftPoints[index] !== rightPoints[index]) return leftPoints[index] - rightPoints[index];
  }
  return leftPoints.length - rightPoints.length;
}

/** Accept UTC/offset ISO timestamps or BigQuery's UTC space-separated format. */
export function timestampMicros(value: string): bigint {
  const match = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?(Z| UTC|[+-]\d{2}:\d{2})?$/.exec(value);
  if (!match) throw new Error(`Invalid timestamp: ${value}`);
  const zone = !match[4] || match[4] === " UTC" ? "Z" : match[4];
  const milliseconds = Date.parse(`${match[1]}T${match[2]}${zone}`);
  if (!Number.isFinite(milliseconds)) throw new Error(`Invalid timestamp: ${value}`);
  // Date.parse accepts impossible dates such as February 30. Reject them.
  const dateOnly = new Date(`${match[1]}T00:00:00Z`);
  if (dateOnly.toISOString().slice(0, 10) !== match[1] || match[2].slice(0, 2) === "24") {
    throw new Error(`Invalid timestamp: ${value}`);
  }
  return BigInt(milliseconds) * 1_000n + BigInt((match[3] ?? "").padEnd(6, "0"));
}

function formatTimestamp(micros: bigint): string {
  let seconds = micros / 1_000_000n;
  let fraction = micros % 1_000_000n;
  if (fraction < 0n) {
    seconds -= 1n;
    fraction += 1_000_000n;
  }
  const date = new Date(Number(seconds * 1_000n)).toISOString().slice(0, 19);
  return `${date}.${fraction.toString().padStart(6, "0")}Z`;
}

export function canonicalJson(value: unknown): string {
  if (value === undefined) return "undefined";
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const record = value as Record<string, unknown>;
  const entries = Object.keys(record).filter((key) => record[key] !== undefined).sort(compareStrings);
  return `{${entries.map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`).join(",")}}`;
}

export interface SessionUpsert {
  session_id: string;
  visitor_key: string;
  revision: string;
  is_deleted: false;
  session: SessionFact;
}

export interface SessionTombstone {
  session_id: string;
  visitor_key: string;
  revision: string;
  is_deleted: true;
  session: null;
}

/** The caller assigns one monotonically increasing revision in its transaction. */
export function diffSessions(
  previous: readonly SessionFact[],
  current: readonly SessionFact[],
  revision: string,
): Array<SessionUpsert | SessionTombstone> {
  if (!/^[1-9]\d*$/.test(revision)) throw new Error("Revision must be a positive integer string");
  const before = indexSessions(previous);
  const after = indexSessions(current);
  const changes: Array<SessionUpsert | SessionTombstone> = [];
  for (const session of current) {
    if (canonicalJson(before.get(session.session_id)) === canonicalJson(session)) continue;
    changes.push({ session_id: session.session_id, visitor_key: session.visitor_key, revision, is_deleted: false, session });
  }
  for (const session of previous) {
    if (after.has(session.session_id)) continue;
    changes.push({ session_id: session.session_id, visitor_key: session.visitor_key, revision, is_deleted: true, session: null });
  }
  return changes.sort((left, right) => compareStrings(left.session_id, right.session_id));
}

function indexSessions(sessions: readonly SessionFact[]): Map<string, SessionFact> {
  const result = new Map<string, SessionFact>();
  for (const session of sessions) {
    if (result.has(session.session_id)) throw new Error(`Duplicate session ID: ${session.session_id}`);
    result.set(session.session_id, session);
  }
  return result;
}
