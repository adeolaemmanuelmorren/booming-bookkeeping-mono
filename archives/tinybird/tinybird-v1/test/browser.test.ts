import assert from "node:assert/strict";
import test from "node:test";
import { normalizeHistorical, normalizeJitsu, urlParameter, type BrowserKind } from "../worker/browser/normalize.ts";
import { canonicalEmail, normalizePhone } from "../worker/browser/identity.ts";
import { applyPageRevisions } from "../worker/sessions/page-revisions.ts";
import { buildSessions } from "../worker/sessions/session-engine.ts";

const arrival = "2026-09-05T15:00:00.000000Z";
const sourceTime = "2026-09-05T14:00:00.123456Z";
const eventTime = "2026-09-05T12:00:00.654321Z";

function historical(record: Record<string, unknown>, kind: BrowserKind = "page_view", source: "boom_domains" | "jitsu_data" = "jitsu_data") {
  return normalizeHistorical({ tenantId: "test", source, kind, record: { id: "row-1", timestamp: eventTime, received_at: sourceTime, ...record }, ingestedAt: arrival });
}

function live(payload: Record<string, unknown>, kind = "page_view", extra = {}) {
  return normalizeJitsu({
    tenant_id: "test", message_id: "row-1", event_kind: kind,
    delivery_event_id: "delivery-1", producer_id: "producer-1",
    observed_at: "2026-09-05T12:00:00.654Z", ingested_at: arrival,
    source_fact_version: 1788616800123000, source_deleted: 0,
    fact_payload: JSON.stringify(payload), ...extra,
  });
}

test("raw source normalization matches fixed Dataform page source projections", async () => {
  const row = await historical({
    message_id: "original-message", anonymous_id: " VisitOR ", user_id: " USER@EXAMPLE.COM ",
    url: "https://EXAMPLE.com/path?utm_source=fb&utm_campaign=A%2BB&utm_content=x=y&gclid=google&fbclid=new",
    path: " /path ", context_attribution_fbclid: "old", context_attribution_utm_source: " ig ",
    context_user_agent_data_mobile: false, context_edge_city: " ", context_geo_city_name: "San Diego",
    context_attribution_fbp: " cookie ", email: "page@example.com", phone: "4153216789",
  });
  assert.equal(row.source.source_record_id, "original-message");
  assert.equal(row.source.source_revision, "1788616800123457");
  assert.equal(row.source.observed_at, eventTime);
  const page = row.pageRevision!.page!;
  assert.equal(page.visitor_key, "VisitOR");
  assert.equal(page.user_id, "user@example.com");
  assert.equal(page.page_host, "example.com");
  assert.equal(page.page_path_host, "/pathexample.com");
  assert.equal(page.device_is_mobile, false);
  assert.equal(page.geo_city, "San Diego");
  assert.equal(page.utm_source, "ig");
  assert.equal(page.utm_campaign, "A%2BB");
  assert.equal(page.utm_content, "x");
  assert.equal(page.fbclid, "new");
  assert.equal(page.click_id_type, "gclid");
  assert.equal(page.click_id, "google");
  assert.equal(page.fbp, "cookie");
  assert.deepEqual(row.identity!.evidenceKeys, ["anonymous_id:VisitOR", "user_id:user@example.com"]);
  assert.equal(page.is_paid_ad_click, null);
  assert.equal(page.campaign_name, null);
  assert.equal(page.channel_source, null);
});

test("live nested payload and flat history produce the same page and identity evidence", async () => {
  const old = await historical({
    message_id: "same", anonymous_id: " A ", user_id: " U ",
    url: "https://example.com/?utm_source=raw", path: "/",
    context_attribution_utm_medium: "cpc", context_attribution_gclid: "click",
    context_user_agent_data_mobile: false, context_geo_city_name: "Paris",
  });
  const current = await live({
    messageId: "same", type: "page", anonymousId: " A ", userId: " U ",
    timestamp: eventTime, receivedAt: sourceTime,
    properties: { url: "https://example.com/?utm_source=raw", path: "/" },
    context: { attribution: { utm_medium: "cpc", gclid: "click" }, userAgentData: { mobile: false }, geo: { cityName: "Paris" } },
  });
  assert.deepEqual(current.pageRevision!.page, old.pageRevision!.page);
  assert.deepEqual(current.identity!.evidenceKeys, old.identity!.evidenceKeys);
  assert.equal(current.source.source_revision, old.source.source_revision);
  assert.equal(current.source.source_priority, 3);
  assert.equal(current.pageRevision!.page!.page_view_timestamp, eventTime);
  assert.equal(JSON.parse(current.source.original_payload).timestamp, eventTime);
});

test("priority beats source time and repeated delivery does not alter sessions", async () => {
  const boom = await historical({ anonymous_id: "old", received_at: "2026-09-06T00:00:00Z" }, "page_view", "boom_domains");
  const jitsu = await historical({ anonymous_id: "historical" });
  const current = await live({ id: "row-1", anonymousId: "live", timestamp: eventTime, receivedAt: sourceTime });
  const result = applyPageRevisions(new Map(), [current.pageRevision!, boom.pageRevision!, jitsu.pageRevision!]);
  assert.equal(result.heads.get("row-1")!.page!.visitor_key, "live");
  const duplicate = await live({ id: "row-1", anonymousId: "live", timestamp: eventTime, receivedAt: sourceTime }, "page_view", { ingested_at: "2026-09-06T00:00:00Z" });
  const repeat = applyPageRevisions(result.heads, [duplicate.pageRevision!]);
  assert.deepEqual(repeat.affectedVisitors, []);
  assert.deepEqual(repeat.changedPageIds, []);
});

test("microseconds affect session boundaries after live normalization", async () => {
  const first = await live({ messageId: "first", anonymousId: "v", timestamp: "2026-09-05T12:00:00.000001Z" });
  const second = await live({ messageId: "second", anonymousId: "v", timestamp: "2026-09-05T12:31:00.000000Z" });
  const sessions = buildSessions("v", [first.pageRevision!.page!, second.pageRevision!.page!]);
  assert.equal(sessions.length, 1);
  assert.equal(sessions[0].page_view_count, 2);
  assert.equal(sessions[0].session_start_timestamp, "2026-09-05T12:00:00.000001Z");
});

test("time fallbacks follow source semantics and never use ingestion time", async () => {
  const noTime = await historical({ timestamp: null, received_at: null, sent_at: null, loaded_at: null, anonymous_id: "v" });
  assert.equal(noTime.source.observed_at, null);
  assert.equal(noTime.source.source_revision, "0");
  assert.equal(noTime.pageRevision!.page!.page_view_timestamp, null);
  assert.equal(noTime.identity!.observedAt, null);
  assert.equal(buildSessions("v", [noTime.pageRevision!.page!]).length, 0);
  const sent = await historical({ timestamp: null, sent_at: eventTime });
  assert.equal(sent.source.observed_at, eventTime);
  const form = await historical({ submitted_at: "2020-01-01T00:00:00Z" }, "client_form");
  assert.equal(form.source.observed_at, "2020-01-01T00:00:00.000000Z");
  const order = await historical({ submitted_at: "2020-01-01T00:00:00Z" }, "client_order");
  assert.equal(order.source.observed_at, eventTime);
});

test("identify, form, order, and attribution retain their different evidence rules", async () => {
  const source = { anonymous_id: "anon", user_id: " User@Example.COM ", email: " ", phone: "", context_traits_email: " Traits@Example.com ", context_traits_phone: "4153216789", first_name: " Ada ", name: "Ada  Lovelace" };
  const identify = await historical(source, "identify");
  assert.equal(identify.source.email, null);
  assert.equal(identify.source.phone, null);
  assert.equal(identify.source.first_name, null);
  const form = await historical({ ...source, extra_submitted_fields_checkout_offer_member_email: "Member@Example.COM" }, "client_form");
  assert.equal(form.source.email, "member@example.com");
  assert.equal(form.source.phone, "4153216789");
  assert.equal(form.source.first_name, "Ada");
  assert.equal(form.source.last_name, null);
  const order = await historical(source, "client_order");
  assert.equal(order.source.email, "traits@example.com");
  assert.equal(order.source.first_name, "Ada");
  assert.equal(order.source.last_name, "Lovelace");
  const attribution = await historical(source, "attribution");
  assert.equal(attribution.source.email, "user@example.com");
  assert.equal(attribution.source.phone, null);
  assert.equal(attribution.source.first_name, null);
});

test("nested identify traits map to historical fields with explicit top-level values winning", async () => {
  const result = await live({ type: "identify", messageId: "identify", timestamp: eventTime,
    context: { traits: { email: "old@example.com", userId: "old" } },
    traits: { email: "A.b+work@GoogleMail.com", phone: "+1 (415) 321-6789", userId: "new" },
  }, "identify");
  assert.equal(result.source.email, "a.b+work@googlemail.com");
  assert.deepEqual(result.identity!.evidenceKeys, ["canonical_email:ab@gmail.com", "email:a.b+work@googlemail.com", "phone:+14153216789", "user_id:new"]);
});

test("phone exclusions and canonical email behavior match fixed Dataform literals", () => {
  for (const input of ["4155551234", "4151111111", "2345678901", "9876543210", "+44 20 7946 0958", "1153216789", "4150216789", "4153216789 ext 123", null]) assert.equal(normalizePhone(input), null, `${input}`);
  assert.equal(normalizePhone("+1 (415) 321-6789"), "+14153216789");
  assert.equal(normalizePhone("415.321.6789"), "+14153216789");
  assert.equal(canonicalEmail("A.B+work@GoogleMail.com"), "ab@gmail.com");
  assert.equal(canonicalEmail("A.B+work@Example.com"), "a.b+work@example.com");
  assert.equal(canonicalEmail("not-an-email"), "not-an-email");
});

test("canonical UTC timestamps compare correctly while original payload time stays intact", async () => {
  const result = await historical({ timestamp: "2026-09-05T05:00:00.654321-07:00", received_at: "2026-09-05 14:00:00.123456 UTC" });
  assert.equal(result.source.observed_at, eventTime);
  assert.equal(result.source.source_updated_at, sourceTime);
  assert.equal(result.identity!.observedAt, eventTime);
  assert.equal(JSON.parse(result.source.original_payload).timestamp, "2026-09-05T05:00:00.654321-07:00");
});

test("correction replaces identifiers and deletion keeps source identity with a tombstone", async () => {
  const original = await live({ id: "row-1", timestamp: eventTime, receivedAt: sourceTime, anonymousId: "old" });
  const correction = await live({ id: "row-1", timestamp: eventTime, receivedAt: "2026-09-05T14:00:00.123457Z", anonymousId: "new" });
  const updated = applyPageRevisions(new Map([["row-1", original.pageRevision!]]), [correction.pageRevision!]);
  assert.deepEqual(updated.affectedVisitors, ["new", "old"]);
  const removed = await live({ id: "row-1", timestamp: eventTime, receivedAt: "2026-09-05T14:00:00.123458Z", anonymousId: "new" }, "page_view", { source_deleted: 1 });
  assert.equal(removed.pageRevision!.page, null);
  assert.equal(removed.source.source_deleted, true);
  assert.equal(removed.identity!.factDeleted, true);
  assert.deepEqual(removed.identity!.evidenceKeys, []);
  assert.equal(removed.identity!.factKey, "segment_page_view:row-1");
});

test("empty fields preserve SQL coalesce ordering; missing and false remain distinct", async () => {
  const result = await historical({ url: "", context_page_url: "https://fallback.example/?utm_source=fb", path: "", context_page_path: "/fallback", anonymous_id: " ", user_id: " MixedCase " });
  assert.equal(result.pageRevision!.page!.page_url, null);
  assert.equal(result.pageRevision!.page!.page_path, null);
  assert.equal(result.pageRevision!.page!.visitor_key, "mixedcase");
  assert.equal(result.pageRevision!.page!.utm_source, null);
  assert.equal(result.pageRevision!.page!.device_is_mobile, null);
  const noIds = await historical({ message_id: " Exact-ID ", anonymous_id: null, user_id: null });
  assert.equal(noIds.pageRevision!.page!.visitor_key, " Exact-ID ");
});

test("same source revision with changed page evidence is rejected, not arbitrarily picked", async () => {
  const first = await historical({ anonymous_id: "first" });
  const second = await historical({ anonymous_id: "second" });
  assert.throws(() => applyPageRevisions(new Map([["row-1", first.pageRevision!]]), [second.pageRevision!]), /Conflicting payload/);
});

test("unsupported event kinds are archived without new identity behavior", async () => {
  const result = await live({ id: "row-1", timestamp: eventTime, email: "new@example.com", properties: { sensitive: "retained" } }, "custom_track");
  assert.equal(result.pageRevision, null);
  assert.equal(result.identity, null);
  assert.equal(JSON.parse(result.source.original_payload).properties.sensitive, "retained");
});

test("invalid IDs or timestamps fail explicitly instead of silently changing source meaning", async () => {
  await assert.rejects(historical({ id: null }), /Source record ID/);
  await assert.rejects(historical({ message_id: "", id: "fallback" }), /Source record ID/);
  await assert.rejects(historical({ timestamp: "2026-02-30T00:00:00Z" }), /Invalid timestamp/);
  await assert.rejects(historical({ timestamp: "", sent_at: eventTime }), /Invalid timestamp/);
});

test("URL parameter helper retains SQL encoding and duplicate-parameter behavior", () => {
  assert.equal(urlParameter("https://example.com/?UTM_SOURCE=a+b&utm_source=second", "utm_source"), "a+b");
  assert.equal(urlParameter("https://example.com/?x=a=b", "x"), "a");
  assert.equal(urlParameter("https://example.com/?x=&x=second", "x"), null);
  assert.equal(urlParameter("https://example.com/?x=first?x=second", "x"), "first");
  assert.equal(urlParameter("https://example.com/?x=value#fragment", "x"), "value#fragment");
});

test("host extraction keeps the documented NET.HOST spelling without URL normalization", async () => {
  for (const [url, expected] of [
    ["http://例子.卷筒纸.中国", "例子.卷筒纸.中国"],
    ["//user:password@a.b:80/path?query", "a.b"],
    ["https://[::1]:80", "[::1]"],
    ["    www.Example.Co.UK    ", "www.example.co.uk"],
    ["mailto:?to=&subject=&body=", "mailto"],
  ]) {
    const result = await historical({ url });
    assert.equal(result.pageRevision!.page!.page_host, expected);
  }
});
