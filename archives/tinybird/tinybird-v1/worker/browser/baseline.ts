import type { PageRevision } from "../sessions/page-revisions.ts";
import type { SessionSnapshot } from "../sessions/session-publication.ts";
import type { NormalizedBrowserEvent } from "./normalize.ts";
import type { GroupMember } from "./publication.ts";

export interface BrowserLogicalKey {
  event_kind: string;
  source_record_id: string;
}

export interface BaselineSourceHeads {
  key: BrowserLogicalKey;
  heads: NormalizedBrowserEvent[];
}

export interface VisitorBaseline {
  tenantId: string;
  visitorKey: string;
  heads: PageRevision[];
  snapshot: SessionSnapshot;
}

/** All reads refer to one sealed, verified historical bootstrap generation. */
export interface BrowserBaselineReader {
  loadSourceHeads(keys: BrowserLogicalKey[]): Promise<BaselineSourceHeads[]>;
  loadMembers(visitorKeys: string[]): Promise<GroupMember[]>;
}

export interface SessionBaselineReader {
  loadVisitor(tenantId: string, visitorKey: string): Promise<VisitorBaseline | null>;
}
