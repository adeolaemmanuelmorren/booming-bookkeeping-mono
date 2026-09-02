import { describe, expect, it } from "vitest";
import {
  conversionDeltaRows,
  expectedConversionIds,
  maxLastIngestedAt,
  nextCursorFrom,
  takeConversionEntityPage,
  takeVisitorPage,
  tinybirdDateTime64,
  touchpointDeltaRows,
  visitorAnchorKeys,
} from "../src/reporting-facts-worker";
import type { ChangedVisitor } from "../src/tinybird-api";

const batch = {
  rowVersion: 1_790_000_000_000,
  batchId: "facts_touchpoints_0_1790000000000",
  committedAt: "2026-09-01T12:00:00.000Z",
};

describe("reporting facts paging", () => {
  it("splits visitors into kind-specific literal lists", () => {
    const page = takeVisitorPage([
      visitor("anonymous", "anon-1"),
      visitor("user", "User@Example.com"),
      visitor("page_view", "pv-1"),
    ]);

    expect(page.anonymousIds).toEqual(["anon-1"]);
    expect(page.userIds).toEqual(["User@Example.com"]);
    expect(page.pageViewIds).toEqual(["pv-1"]);
    expect(page.visitors).toHaveLength(3);
  });

  it("gives a comma-bearing visitor its own singleton page", () => {
    const first = takeVisitorPage([
      visitor("anonymous", "anon-1"),
      visitor("user", "weird,value"),
    ]);
    expect(first.visitors.map((entry) => entry.visitorValue)).toEqual(["anon-1"]);

    const second = takeVisitorPage([
      visitor("user", "weird,value"),
      visitor("anonymous", "anon-2"),
    ]);
    expect(second.visitors.map((entry) => entry.visitorValue)).toEqual(["weird,value"]);
  });

  it("caps pages at the visitor limit", () => {
    const visitors = Array.from({ length: 5 }, (_, index) => visitor("anonymous", `a-${index}`));
    expect(takeVisitorPage(visitors, 3).visitors).toHaveLength(3);
  });

  it("routes conversion entities by kind", () => {
    const page = takeConversionEntityPage([
      { entityKind: "client_form", entityId: "form-1", lastIngestedAt: "t" },
      { entityKind: "client_order", entityId: "order-1", lastIngestedAt: "t" },
    ]);
    expect(page.formSubmissionIds).toEqual(["form-1"]);
    expect(page.orderEventIds).toEqual(["order-1"]);
  });
});

describe("reporting facts anchors and cursors", () => {
  it("derives anchor keys exactly as sessions store them", () => {
    expect(visitorAnchorKeys([
      visitor("anonymous", "anon-1"),
      visitor("user", "User@Example.com"),
      visitor("page_view", "pv-1"),
    ])).toEqual(["anonymous_id:anon-1", "user_id:user@example.com"]);
  });

  it("expands entities to every conversion id they can emit", () => {
    expect(expectedConversionIds(
      { entityKind: "client_form", entityId: "f1", lastIngestedAt: "t" },
    )).toEqual(["client_form:f1", "client_payment:historical_form_f1"]);
    expect(expectedConversionIds(
      { entityKind: "client_order", entityId: "o1", lastIngestedAt: "t" },
    )).toEqual(["client_payment:segment_order_o1"]);
  });

  it("advances the cursor one second before the high-water mark", () => {
    expect(maxLastIngestedAt([
      { lastIngestedAt: "2026-09-01 10:00:01.000000" },
      { lastIngestedAt: "2026-09-01 10:00:05.500000" },
    ])).toBe("2026-09-01 10:00:05.500000");
    expect(nextCursorFrom("2026-09-01 10:00:05.500000"))
      .toBe("2026-09-01 10:00:04.500000");
    expect(tinybirdDateTime64(Date.UTC(2026, 8, 1, 10, 0, 0, 250)))
      .toBe("2026-09-01 10:00:00.250000");
  });
});

describe("touchpoint delta diffing", () => {
  it("versions rebuilt rows and tombstones vanished session ids", () => {
    const diff = touchpointDeltaRows(
      [
        touchpointRow("anonymous_id:a1", "tp-1"),
        touchpointRow("anonymous_id:a1", "tp-2"),
      ],
      [
        { identityAnchorKey: "anonymous_id:a1", touchpointId: "tp-1" },
        { identityAnchorKey: "anonymous_id:a1", touchpointId: "tp-ghost" },
      ],
      batch,
    );

    expect(diff.deltaRows).toHaveLength(3);
    const tombstone = diff.deltaRows.find((row) => row.is_deleted === 1);
    expect(tombstone).toMatchObject({
      identity_anchor_key: "anonymous_id:a1",
      touchpoint_id: "tp-ghost",
      row_version: batch.rowVersion,
    });
    expect(diff.affectedAnchorKeys).toEqual(["anonymous_id:a1"]);
  });

  it("never tombstones the empty aggregate anchor", () => {
    const diff = touchpointDeltaRows(
      [touchpointRow("", "tp-solo")],
      [],
      batch,
    );
    expect(diff.affectedAnchorKeys).toEqual([]);
    expect(diff.deltaRows).toHaveLength(1);
  });
});

describe("conversion delta diffing", () => {
  it("tombstones a superseded conversion under its old anchor", () => {
    const diff = conversionDeltaRows(
      [conversionRow("email:new@example.com", "client_form:f1")],
      [
        { identityAnchorKey: "email:old@example.com", conversionId: "client_form:f1" },
      ],
      batch,
    );

    expect(diff.deltaRows).toHaveLength(2);
    const tombstone = diff.deltaRows.find((row) => row.is_deleted === 1);
    expect(tombstone).toMatchObject({
      identity_anchor_key: "email:old@example.com",
      conversion_id: "client_form:f1",
      fact_kind: "tombstone",
    });
    expect(diff.affectedAnchorKeys).toEqual([
      "email:new@example.com",
      "email:old@example.com",
    ]);
  });

  it("leaves an unchanged head alone", () => {
    const diff = conversionDeltaRows(
      [conversionRow("email:a@example.com", "client_form:f1")],
      [{ identityAnchorKey: "email:a@example.com", conversionId: "client_form:f1" }],
      batch,
    );
    expect(diff.deltaRows.filter((row) => row.is_deleted === 1)).toHaveLength(0);
  });
});

function visitor(
  visitorKind: ChangedVisitor["visitorKind"],
  visitorValue: string,
): ChangedVisitor {
  return { visitorKind, visitorValue, lastIngestedAt: "2026-09-01 10:00:00.000000" };
}

function touchpointRow(anchorKey: string, touchpointId: string): Record<string, unknown> {
  return {
    identity_anchor_key: anchorKey,
    touchpoint_id: touchpointId,
    session_id: `session-${touchpointId}`,
  };
}

function conversionRow(anchorKey: string, conversionId: string): Record<string, unknown> {
  return {
    tenant_id: "boom",
    identity_anchor_key: anchorKey,
    conversion_id: conversionId,
    fact_kind: "client_form",
  };
}
