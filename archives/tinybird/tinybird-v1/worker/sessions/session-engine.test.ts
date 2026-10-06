import { createHash } from "node:crypto";
import { test } from "node:test";
import assert from "node:assert/strict";
import { buildSessions, diffSessions, timestampMicros, type PageView } from "./session-engine.ts";
import { applyPageRevisions, deriveVisitorKey, pagesForVisitor, type PageRevision } from "./page-revisions.ts";
import { md5Hex } from "./md5.ts";

function page(id: string, time: string, fields: Partial<PageView> = {}): PageView {
  return { page_view_id: id, visitor_key: "visitor-1", page_view_timestamp: `2026-09-01T${time}Z`, ...fields };
}

function revision(value: PageView | null, version = "1", pageId = value?.page_view_id ?? "a", priority = 3): PageRevision {
  return { page_view_id: pageId, source_priority: priority, source_revision: version, page: value };
}

function sessionId(visitor: string, ordinal: number): string {
  return createHash("md5").update(`${visitor}|${ordinal}`).digest("hex");
}

test("session boundary uses whole minutes, including microseconds", () => {
  for (const end of ["00:30:00", "00:30:59", "00:30:59.999999"]) {
    const sessions = buildSessions("visitor-1", [page("b", end), page("a", "00:00:00")]);
    assert.equal(sessions.length, 1, end);
    assert.equal(sessions[0].page_view_count, 2);
  }
  const sessions = buildSessions("visitor-1", [page("a", "00:00:00"), page("b", "00:31:00")]);
  assert.deepEqual(sessions.map((value) => value.first_page_view_id), ["a", "b"]);
});

test("uses consecutive inactivity, not total elapsed session time", () => {
  const sessions = buildSessions("visitor-1", [
    page("c", "01:00:00"), page("a", "00:00:00"), page("b", "00:30:00"),
  ]);
  assert.equal(sessions.length, 1);
  assert.equal(sessions[0].session_duration_seconds, 3600);
  assert.equal(sessions[0].page_view_count, 3);
});

test("first and last attributes follow event order and preserve false values", () => {
  const first = page("a", "00:00:00.800000", {
    anonymous_id: "CaseSensitive", user_id: "kept-as-supplied", page_url: "https://first.test/a",
    page_host: "first.test", page_path: "/a", page_path_host: "/afirst.test", device_is_mobile: false,
    is_paid_ad_click: false, campaign_id: "old-campaign", utm_source: "google", gclid: "first-click",
  });
  const last = page("b", "00:00:02.100000", {
    page_url: "https://last.test/b", page_path: "/b", device_is_mobile: true,
    campaign_id: "new-campaign", utm_source: "meta", gclid: "last-click",
  });
  const [session] = buildSessions("visitor-1", [last, first]);
  assert.equal(session.session_id, sessionId("visitor-1", 1));
  assert.equal(session.session_duration_seconds, 1);
  assert.equal(session.session_start_timestamp, "2026-09-01T00:00:00.800000Z");
  assert.equal(session.session_end_timestamp, "2026-09-01T00:00:02.100000Z");
  assert.equal(session.first_page_url, "https://first.test/a");
  assert.equal(session.first_page_host, "first.test");
  assert.equal(session.first_page_path, "/a");
  assert.equal(session.first_page_path_host, "/afirst.test");
  assert.equal(session.last_page_url, "https://last.test/b");
  assert.equal(session.last_page_path, "/b");
  assert.equal(session.anonymous_id, "CaseSensitive");
  assert.equal(session.user_id, "kept-as-supplied");
  assert.equal(session.device_is_mobile, false);
  assert.equal(session.is_paid_ad_click, false);
  assert.equal(session.campaign_id, "old-campaign");
  assert.equal(session.utm_source, "google");
  assert.equal(session.gclid, "first-click");
  assert.equal(session.geo_city, null);
});

test("late bridge merges sessions and removes the superseded session", () => {
  const before = buildSessions("visitor-1", [page("a", "00:00:00"), page("c", "01:00:00")]);
  const after = buildSessions("visitor-1", [page("a", "00:00:00"), page("c", "01:00:00"), page("b", "00:30:00")]);
  assert.equal(after.length, 1);
  assert.equal(after[0].page_view_count, 3);
  assert.equal(after[0].session_duration_seconds, 3600);
  const changes = diffSessions(before, after, "12");
  assert.equal(changes.length, 2);
  assert.deepEqual(changes.find((value) => value.is_deleted), {
    session_id: sessionId("visitor-1", 2), visitor_key: "visitor-1", revision: "12", is_deleted: true, session: null,
  });
  assert.equal(changes.find((value) => !value.is_deleted)?.session?.first_page_view_id, "a");
});

test("earlier session renumbers all following sessions without a history cutoff", () => {
  const initial = [page("later-a", "05:00:00"), page("later-b", "06:00:00")];
  const before = buildSessions("visitor-1", initial);
  const after = buildSessions("visitor-1", [
    ...initial, page("years-earlier", "00:00:00", { page_view_timestamp: "2018-01-01T00:00:00Z" }),
  ]);
  assert.deepEqual(after.map((value) => [value.session_id, value.first_page_view_id]), [
    [sessionId("visitor-1", 1), "years-earlier"],
    [sessionId("visitor-1", 2), "later-a"],
    [sessionId("visitor-1", 3), "later-b"],
  ]);
  const changes = diffSessions(before, after, "13");
  assert.equal(changes.length, 3);
  assert.ok(changes.every((value) => !value.is_deleted));
});

test("timestamp ties use stable Unicode code-point page IDs", () => {
  const sessions = buildSessions("visitor-1", [
    page("😀", "00:00:00"), page("\uffff", "00:00:00"), page("a", "00:00:00"), page("A", "00:00:00"),
  ]);
  assert.equal(sessions[0].first_page_view_id, "A");
  assert.equal(sessions[0].last_page_view_id, "😀");
  const precise = buildSessions("visitor-1", [page("z", "00:00:00.000001"), page("a", "00:00:00.000002")]);
  assert.equal(precise[0].first_page_view_id, "z");
});

test("identical duplicates do not inflate page counts and retries emit no diff", () => {
  const a = page("a", "00:00:00");
  const loaded = applyPageRevisions(new Map(), [revision(a), revision({ ...a })]);
  const retry = applyPageRevisions(loaded.heads, [revision(a)]);
  assert.deepEqual(retry.affectedVisitors, []);
  assert.deepEqual(retry.changedPageIds, []);
  const sessions = buildSessions("visitor-1", [a, { ...a }]);
  assert.equal(sessions[0].page_view_count, 1);
  assert.deepEqual(diffSessions(sessions, buildSessions("visitor-1", [a]), "2"), []);
});

test("timestamp correction replaces a page and repairs the complete visitor", () => {
  const initial = applyPageRevisions(new Map(), [revision(page("a", "00:00:00")), revision(page("b", "01:00:00"))]);
  const before = buildSessions("visitor-1", pagesForVisitor(initial.heads, "visitor-1"));
  const corrected = applyPageRevisions(initial.heads, [revision(page("b", "00:20:00"), "2")]);
  const after = buildSessions("visitor-1", pagesForVisitor(corrected.heads, "visitor-1"));
  assert.deepEqual(corrected.affectedVisitors, ["visitor-1"]);
  assert.equal(before.length, 2);
  assert.equal(after.length, 1);
  assert.equal(after[0].session_duration_seconds, 1200);
  assert.equal(diffSessions(before, after, "2").filter((value) => value.is_deleted).length, 1);
  assert.equal(pagesForVisitor(initial.heads, "visitor-1")[1].page_view_timestamp, "2026-09-01T01:00:00Z");
});

test("visitor correction repairs both the old and new visitor", () => {
  const original = page("a", "00:00:00", { visitor_key: "old" });
  const initial = applyPageRevisions(new Map(), [revision(original)]);
  const moved = applyPageRevisions(initial.heads, [revision({ ...original, visitor_key: "new" }, "2")]);
  assert.deepEqual(moved.affectedVisitors, ["new", "old"]);
  const oldBefore = buildSessions("old", pagesForVisitor(initial.heads, "old"));
  const oldAfter = buildSessions("old", pagesForVisitor(moved.heads, "old"));
  const newAfter = buildSessions("new", pagesForVisitor(moved.heads, "new"));
  assert.deepEqual(oldAfter, []);
  assert.equal(newAfter[0].session_id, sessionId("new", 1));
  assert.equal(newAfter[0].first_page_view_id, "a");
  assert.deepEqual(diffSessions(oldBefore, oldAfter, "2"), [{
    session_id: sessionId("old", 1), visitor_key: "old", revision: "2", is_deleted: true, session: null,
  }]);
});

test("deletion retains its revision head and blocks stale resurrection", () => {
  const original = page("a", "00:00:00");
  const initial = applyPageRevisions(new Map(), [revision(original, "2")]);
  const deleted = applyPageRevisions(initial.heads, [revision(null, "3", "a")]);
  const replayed = applyPageRevisions(deleted.heads, [revision(original, "2")]);
  assert.deepEqual(deleted.affectedVisitors, ["visitor-1"]);
  assert.deepEqual(pagesForVisitor(replayed.heads, "visitor-1"), []);
  assert.equal(replayed.heads.get("a")?.source_revision, "3");
  assert.equal(diffSessions(buildSessions("visitor-1", [original]), [], "3")[0].is_deleted, true);
  const restored = applyPageRevisions(replayed.heads, [revision(original, "4")]);
  assert.equal(pagesForVisitor(restored.heads, "visitor-1").length, 1);
});

test("source precedence is independent of arrival order and revision magnitude", () => {
  const historical = revision(page("a", "00:00:00"), "999999999999999999999", "a", 1);
  const live = revision(page("a", "00:05:00"), "1", "a", 3);
  for (const revisions of [[historical, live], [live, historical]]) {
    const result = applyPageRevisions(new Map(), revisions);
    assert.equal(result.heads.get("a")?.page?.page_view_timestamp, "2026-09-01T00:05:00Z");
    assert.deepEqual(result.affectedVisitors, ["visitor-1"]);
  }
});

test("no-change source updates persist revision but do not rebuild sessions", () => {
  const a = page("a", "00:00:00");
  const initial = applyPageRevisions(new Map(), [revision(a, "1")]);
  const next = applyPageRevisions(initial.heads, [revision(a, "2")]);
  assert.equal(next.heads.get("a")?.source_revision, "2");
  assert.deepEqual(next.changedPageIds, []);
  assert.deepEqual(next.affectedVisitors, []);
});

test("same source revision with conflicting content fails without changing prior state", () => {
  const a = page("a", "00:00:00");
  const initial = applyPageRevisions(new Map(), [revision(a)]);
  assert.throws(() => applyPageRevisions(initial.heads, [revision(page("a", "00:01:00"))]), /Conflicting payload/);
  assert.equal(initial.heads.get("a")?.page, a);
  assert.throws(() => buildSessions("visitor-1", [a, page("a", "00:01:00")]), /Conflicting current pages/);
});

test("null timestamps remain source facts but do not create sessions", () => {
  const input = page("a", "00:00:00", { page_view_timestamp: null });
  const result = applyPageRevisions(new Map(), [revision(input)]);
  assert.equal(result.heads.size, 1);
  assert.deepEqual(buildSessions("visitor-1", [input]), []);
  assert.deepEqual(buildSessions("visitor-1", []), []);
});

test("timestamp parser preserves offsets, microseconds and pre-epoch values", () => {
  assert.equal(timestampMicros("1970-01-01T00:00:00.000001Z"), 1n);
  assert.equal(timestampMicros("1970-01-01T01:00:00.000001+01:00"), 1n);
  assert.equal(timestampMicros("1970-01-01 00:00:00.000001 UTC"), 1n);
  assert.equal(timestampMicros("1969-12-31T23:59:59.999999Z"), -1n);
  assert.throws(() => timestampMicros("2026-02-30T00:00:00Z"), /Invalid timestamp/);
  assert.throws(() => timestampMicros("2026-09-01T24:00:00Z"), /Invalid timestamp/);
});

test("visitor fallback preserves IDs and matches anonymous, user, page priority", () => {
  assert.equal(deriveVisitorKey({ page_view_id: "OriginalPage", anonymous_id: " AbC ", user_id: "USER" }), "AbC");
  assert.equal(deriveVisitorKey({ page_view_id: "OriginalPage", anonymous_id: " ", user_id: " USER " }), "user");
  assert.equal(deriveVisitorKey({ page_view_id: "OriginalPage", user_id: " " }), "OriginalPage");
  const original = page("OriginalPage", "00:00:00", { visitor_key: "DoNotRewrite" });
  assert.equal(buildSessions("DoNotRewrite", [original])[0].visitor_key, "DoNotRewrite");
});

test("copied MD5 implementation matches the independent Node implementation", () => {
  for (const value of ["", "visitor-1|1", "CaseSensitive|12", "visitor-😀|999"] ) {
    assert.equal(md5Hex(value), createHash("md5").update(value).digest("hex"));
  }
});

test("deleting a bridge splits one session and restores later ordinals", () => {
  const initial = applyPageRevisions(new Map(), [
    revision(page("a", "00:00:00")), revision(page("b", "00:30:00")), revision(page("c", "01:00:00")),
  ]);
  const before = buildSessions("visitor-1", pagesForVisitor(initial.heads, "visitor-1"));
  const deleted = applyPageRevisions(initial.heads, [revision(null, "2", "b")]);
  const after = buildSessions("visitor-1", pagesForVisitor(deleted.heads, "visitor-1"));
  assert.equal(before.length, 1);
  assert.deepEqual(after.map((value) => [value.session_id, value.first_page_view_id]), [
    [sessionId("visitor-1", 1), "a"], [sessionId("visitor-1", 2), "c"],
  ]);
  assert.equal(diffSessions(before, after, "2").length, 2);
});
