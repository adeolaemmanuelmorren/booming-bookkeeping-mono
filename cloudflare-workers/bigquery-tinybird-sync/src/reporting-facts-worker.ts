import type {
  ChangedConversionEntity,
  ChangedVisitor,
  ConversionFactHead,
  TouchpointFactHead,
} from "./tinybird-api";

export const CDC_VISITOR_PAGE_LIMIT = 200;
export const CDC_ENTITY_PAGE_LIMIT = 500;

export interface VisitorPage {
  visitors: ChangedVisitor[];
  anonymousIds: string[];
  userIds: string[];
  pageViewIds: string[];
}

export interface ConversionEntityPage {
  entities: ChangedConversionEntity[];
  formSubmissionIds: string[];
  orderEventIds: string[];
  serverFormSubmissionIds: string[];
  serverPaymentKeys: string[];
}

export interface FactDeltaBatch {
  rowVersion: number;
  batchId: string;
  committedAt: string;
}

// A visitor is re-sessionized completely per page, so a page only limits how
// many visitors ride one build call; splitting a single visitor is impossible
// by construction. A comma-bearing value gets its own singleton page to fit
// the literal-parameter convention.
export function takeVisitorPage(
  visitors: ChangedVisitor[],
  limit: number = CDC_VISITOR_PAGE_LIMIT,
): VisitorPage {
  const page: VisitorPage = { visitors: [], anonymousIds: [], userIds: [], pageViewIds: [] };

  for (const visitor of visitors) {
    if (page.visitors.length > 0 && visitor.visitorValue.includes(",")) break;
    page.visitors.push(visitor);
    if (visitor.visitorKind === "anonymous") page.anonymousIds.push(visitor.visitorValue);
    else if (visitor.visitorKind === "user") page.userIds.push(visitor.visitorValue);
    else page.pageViewIds.push(visitor.visitorValue);
    if (visitor.visitorValue.includes(",")) break;
    if (page.visitors.length >= limit) break;
  }

  return page;
}

export function takeConversionEntityPage(
  entities: ChangedConversionEntity[],
  limit: number = CDC_ENTITY_PAGE_LIMIT,
): ConversionEntityPage {
  const page: ConversionEntityPage = {
    entities: [],
    formSubmissionIds: [],
    orderEventIds: [],
    serverFormSubmissionIds: [],
    serverPaymentKeys: [],
  };

  for (const entity of entities) {
    if (page.entities.length > 0 && entity.entityId.includes(",")) break;
    page.entities.push(entity);
    if (entity.entityKind === "client_form") {
      page.formSubmissionIds.push(entity.entityId);
    } else if (entity.entityKind === "client_order") {
      page.orderEventIds.push(entity.entityId);
    } else if (entity.entityKind === "server_form") {
      page.serverFormSubmissionIds.push(entity.entityId);
    } else {
      page.serverPaymentKeys.push(entity.entityId);
    }
    if (entity.entityId.includes(",")) break;
    if (page.entities.length >= limit) break;
  }

  return page;
}

// Anchor keys under which a visitor's sessions are stored. A session keeps its
// own anonymous/user identity, so these are exact, not heuristic. Sessions of
// page-view-only visitors store the empty anchor and are excluded from head
// diffs: the empty anchor aggregates unrelated visitors.
export function visitorAnchorKeys(visitors: ChangedVisitor[]): string[] {
  const keys = new Set<string>();
  for (const visitor of visitors) {
    if (visitor.visitorKind === "anonymous") {
      keys.add(`anonymous_id:${visitor.visitorValue}`);
    } else if (visitor.visitorKind === "user") {
      keys.add(`user_id:${visitor.visitorValue.toLowerCase()}`);
    }
  }
  return [...keys].sort();
}

export function expectedConversionIds(entity: ChangedConversionEntity): string[] {
  if (entity.entityKind === "client_form") {
    return [
      `client_form:${entity.entityId}`,
      `client_payment:historical_form_${entity.entityId}`,
    ];
  }
  if (entity.entityKind === "client_order") {
    return [`client_payment:segment_order_${entity.entityId}`];
  }
  if (entity.entityKind === "server_form") {
    return [`server_form:${entity.entityId}`];
  }
  return [`server_payment:${entity.entityId}`];
}

export interface TouchpointDeltaDiff {
  deltaRows: Record<string, unknown>[];
  affectedAnchorKeys: string[];
}

export function touchpointDeltaRows(
  buildRows: Record<string, unknown>[],
  heads: TouchpointFactHead[],
  batch: FactDeltaBatch,
): TouchpointDeltaDiff {
  const liveKeys = new Set<string>();
  const affectedAnchorKeys = new Set<string>();
  const deltaRows: Record<string, unknown>[] = [];

  for (const row of buildRows) {
    const anchorKey = requiredString(row.identity_anchor_key ?? "", "identity_anchor_key");
    const touchpointId = requiredString(row.touchpoint_id, "touchpoint_id");
    liveKeys.add(`${anchorKey}|${touchpointId}`);
    if (anchorKey !== "") affectedAnchorKeys.add(anchorKey);
    deltaRows.push({
      ...row,
      identity_anchor_key: anchorKey,
      row_version: batch.rowVersion,
      batch_id: batch.batchId,
      committed_at: batch.committedAt,
      is_deleted: 0,
    });
  }

  for (const head of heads) {
    if (liveKeys.has(`${head.identityAnchorKey}|${head.touchpointId}`)) continue;
    if (head.identityAnchorKey !== "") affectedAnchorKeys.add(head.identityAnchorKey);
    deltaRows.push({
      identity_anchor_key: head.identityAnchorKey,
      touchpoint_id: head.touchpointId,
      row_version: batch.rowVersion,
      batch_id: batch.batchId,
      committed_at: batch.committedAt,
      is_deleted: 1,
      session_duration_seconds: 0,
      page_view_count: 0,
      first_page_path_host: "",
    });
  }

  return { deltaRows, affectedAnchorKeys: [...affectedAnchorKeys].sort() };
}

export interface ConversionDeltaDiff {
  deltaRows: Record<string, unknown>[];
  affectedAnchorKeys: string[];
}

export function conversionDeltaRows(
  buildRows: Record<string, unknown>[],
  heads: ConversionFactHead[],
  batch: FactDeltaBatch,
): ConversionDeltaDiff {
  const liveKeys = new Set<string>();
  const affectedAnchorKeys = new Set<string>();
  const deltaRows: Record<string, unknown>[] = [];

  for (const row of buildRows) {
    const anchorKey = requiredString(row.identity_anchor_key ?? "", "identity_anchor_key");
    const conversionId = requiredString(row.conversion_id, "conversion_id");
    liveKeys.add(`${anchorKey}|${conversionId}`);
    if (anchorKey !== "") affectedAnchorKeys.add(anchorKey);
    deltaRows.push({
      ...row,
      identity_anchor_key: anchorKey,
      row_version: batch.rowVersion,
      batch_id: batch.batchId,
      committed_at: batch.committedAt,
      is_deleted: 0,
    });
  }

  for (const head of heads) {
    if (liveKeys.has(`${head.identityAnchorKey}|${head.conversionId}`)) continue;
    if (head.identityAnchorKey !== "") affectedAnchorKeys.add(head.identityAnchorKey);
    deltaRows.push({
      tenant_id: "boom",
      identity_anchor_key: head.identityAnchorKey,
      conversion_id: head.conversionId,
      row_version: batch.rowVersion,
      batch_id: batch.batchId,
      committed_at: batch.committedAt,
      is_deleted: 1,
      fact_kind: "tombstone",
      conversion_source: "",
      conversion_type: "",
      is_repeat_payment: 0,
      net_amount: 0,
      client_payment_amount: 0,
    });
  }

  return { deltaRows, affectedAnchorKeys: [...affectedAnchorKeys].sort() };
}

export function maxLastIngestedAt(
  rows: readonly { lastIngestedAt: string }[],
): string | null {
  let latest: string | null = null;
  for (const row of rows) {
    if (latest === null || row.lastIngestedAt > latest) latest = row.lastIngestedAt;
  }
  return latest;
}

// Windows re-read from one second before the processed high-water mark, so a
// page cut in the middle of one ingestion timestamp re-processes those
// entities instead of skipping them. Rebuilds are idempotent, so overlap is
// free; a gap is not.
export function nextCursorFrom(lastIngestedAt: string): string {
  const parsed = Date.parse(`${lastIngestedAt.replace(" ", "T")}Z`);
  if (Number.isNaN(parsed)) {
    throw new Error(`CDC cursor timestamp is invalid: ${lastIngestedAt}`);
  }
  return tinybirdDateTime64(parsed - 1_000);
}

export function nextWindowCursor(
  rows: readonly { lastIngestedAt: string }[],
  windowEnd: string,
  windowLimit: number,
): string {
  if (rows.length < windowLimit) return windowEnd;

  const latest = maxLastIngestedAt(rows);
  if (latest === null) return windowEnd;
  return nextCursorFrom(latest);
}

export function tinybirdDateTime64(epochMs: number): string {
  const iso = new Date(epochMs).toISOString();
  return `${iso.slice(0, 10)} ${iso.slice(11, 23)}000`;
}

function requiredString(value: unknown, name: string): string {
  if (typeof value === "string") return value;
  throw new Error(`Reporting CDC row ${name} is invalid.`);
}
