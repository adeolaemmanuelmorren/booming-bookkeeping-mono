import { canonicalJson, compareStrings, timestampMicros, type PageView } from "./session-engine.ts";

export interface PageRevision {
  page_view_id: string;
  source_priority: number;
  source_revision: string;
  page: PageView | null;
}

export interface PageRevisionResult {
  heads: Map<string, PageRevision>;
  affectedVisitors: string[];
  changedPageIds: string[];
}

/** Higher source priority always wins. Keep deleted heads to block stale replay. */
export function applyPageRevisions(
  previous: ReadonlyMap<string, PageRevision>,
  incoming: readonly PageRevision[],
): PageRevisionResult {
  const heads = new Map(previous);
  for (const revision of incoming) {
    validateRevision(revision);
    const existing = heads.get(revision.page_view_id);
    if (!existing) {
      heads.set(revision.page_view_id, revision);
      continue;
    }
    const order = compareRevisions(revision, existing);
    if (order < 0) continue;
    if (order === 0) {
      if (canonicalJson(revision.page) !== canonicalJson(existing.page)) {
        throw new Error(`Conflicting payload at the same source revision: ${revision.page_view_id}`);
      }
      continue;
    }
    heads.set(revision.page_view_id, revision);
  }

  const affectedVisitors = new Set<string>();
  const changedPageIds: string[] = [];
  for (const [pageId, revision] of heads) {
    const before = previous.get(pageId)?.page ?? null;
    if (canonicalJson(before) === canonicalJson(revision.page)) continue;
    if (before) affectedVisitors.add(before.visitor_key);
    if (revision.page) affectedVisitors.add(revision.page.visitor_key);
    changedPageIds.push(pageId);
  }
  return {
    heads,
    affectedVisitors: [...affectedVisitors].sort(compareStrings),
    changedPageIds: changedPageIds.sort(compareStrings),
  };
}

export function pagesForVisitor(heads: ReadonlyMap<string, PageRevision>, visitorKey: string): PageView[] {
  const pages: PageView[] = [];
  for (const revision of heads.values()) {
    if (revision.page?.visitor_key === visitorKey) pages.push(revision.page);
  }
  return pages;
}

/** Use only when the source has not already supplied its canonical visitor key. */
export function deriveVisitorKey(input: {
  page_view_id: string;
  anonymous_id?: string | null;
  user_id?: string | null;
}): string {
  if (!input.page_view_id) throw new Error("Page ID is required");
  return input.anonymous_id?.trim() || input.user_id?.trim().toLowerCase() || input.page_view_id;
}

function validateRevision(revision: PageRevision): void {
  if (!revision.page_view_id) throw new Error("Page ID is required");
  if (!Number.isSafeInteger(revision.source_priority) || revision.source_priority < 0) {
    throw new Error("Source priority must be a nonnegative integer");
  }
  if (!/^(0|[1-9]\d*)$/.test(revision.source_revision)) {
    throw new Error("Source revision must be a nonnegative integer string");
  }
  if (revision.page === null) return;
  if (revision.page.page_view_id !== revision.page_view_id) throw new Error("Page revision ID mismatch");
  if (!revision.page.visitor_key) throw new Error("Visitor key is required");
  if (revision.page.page_view_timestamp !== null) timestampMicros(revision.page.page_view_timestamp);
}

function compareRevisions(left: PageRevision, right: PageRevision): number {
  if (left.source_priority !== right.source_priority) return left.source_priority - right.source_priority;
  const leftVersion = BigInt(left.source_revision);
  const rightVersion = BigInt(right.source_revision);
  if (leftVersion < rightVersion) return -1;
  if (leftVersion > rightVersion) return 1;
  return 0;
}
