import { canonicalJson, compareStrings, type SessionFact } from "./session-engine.ts";
import { md5Hex } from "./md5.ts";
import { waitForReadback } from "../storage/tinybird.ts";

export interface SnapshotManifest {
  tenant_id: string;
  publication_id: string;
  visitor_key: string;
  revision: string;
  row_count: number;
  content_hash: string;
}

export interface SnapshotRecord {
  tenant_id: string;
  publication_id: string;
  visitor_key: string;
  revision: string;
  session_id: string;
  payload_json: string;
  payload_hash: string;
}

export interface SessionSnapshot {
  tenant_id: string;
  publication_id: string;
  visitor_key: string;
  revision: string;
  sessions: SessionFact[];
}

/** The parent implements this binding, including complete paginated reads. */
export interface SessionPublisher {
  publishSnapshots?(snapshots: SessionSnapshot[]): Promise<void>;
  appendRecords(records: SnapshotRecord[]): Promise<void>;
  readRecords(manifest: SnapshotManifest): Promise<SnapshotRecord[]>;
  appendCommit(manifest: SnapshotManifest): Promise<void>;
  readCommit(manifest: SnapshotManifest): Promise<SnapshotManifest | null>;
}

export function createSnapshot(tenantId: string, visitorKey: string, revision: string, sessions: SessionFact[]): SessionSnapshot {
  return {
    tenant_id: tenantId,
    publication_id: md5Hex(canonicalJson([tenantId, visitorKey, revision])),
    visitor_key: visitorKey,
    revision,
    sessions: [...sessions].sort((left, right) => compareStrings(left.session_id, right.session_id)),
  };
}

/** Reads actual rows before publishing the commit, including on ambiguous retries. */
export async function snapshotPublication(snapshot: SessionSnapshot): Promise<{ records: SnapshotRecord[]; manifest: SnapshotManifest }> {
  const records: SnapshotRecord[] = [];
  for (const session of snapshot.sessions) {
    const payload = canonicalJson(session);
    records.push({
      tenant_id: snapshot.tenant_id,
      publication_id: snapshot.publication_id,
      visitor_key: snapshot.visitor_key,
      revision: snapshot.revision,
      session_id: session.session_id,
      payload_json: payload,
      payload_hash: await sha256(payload),
    });
  }
  const manifest: SnapshotManifest = {
    tenant_id: snapshot.tenant_id,
    publication_id: snapshot.publication_id,
    visitor_key: snapshot.visitor_key,
    revision: snapshot.revision,
    row_count: records.length,
    content_hash: await sha256(canonicalJson(records)),
  };
  return { records, manifest };
}

/** Reads actual rows before publishing the commit, including on ambiguous retries. */
export async function publishSnapshot(snapshot: SessionSnapshot, publisher: SessionPublisher): Promise<void> {
  const { records, manifest } = await snapshotPublication(snapshot);
  let actual = await publisher.readRecords(manifest);
  if (!recordsMatch(records, actual)) {
    if (records.length) await publisher.appendRecords(records);
    actual = await waitForReadback(() => publisher.readRecords(manifest), rows => recordsMatch(records, rows));
  }
  if (!recordsMatch(records, actual)) throw new Error("Snapshot rows are not fully visible");

  let commit = await publisher.readCommit(manifest);
  if (commit && canonicalJson(commit) !== canonicalJson(manifest)) {
    throw new Error("Snapshot commit conflicts with the saved manifest");
  }
  if (!commit) {
    await publisher.appendCommit(manifest);
    commit = await waitForReadback(() => publisher.readCommit(manifest), row => {
      if (row && canonicalJson(row) !== canonicalJson(manifest)) throw new Error("Snapshot commit conflicts with the saved manifest");
      return row !== null;
    });
  }
  if (canonicalJson(commit) !== canonicalJson(manifest)) {
    throw new Error("Snapshot commit is not fully visible");
  }
}

export function recordsMatch(expectedRecords: SnapshotRecord[], actual: SnapshotRecord[]): boolean {
  const expected = new Map(expectedRecords.map((record) => [record.session_id, canonicalJson(record)]));
  const found = new Map<string, string>();
  for (const record of actual) {
    const content = canonicalJson(record);
    if (expected.get(record.session_id) !== content) {
      throw new Error("Snapshot contains an unexpected or conflicting row");
    }
    // Identical transport retries are harmless; conflicting versions were rejected above.
    found.set(record.session_id, content);
  }
  return found.size === expected.size;
}

async function sha256(value: string): Promise<string> {
  const bytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return Array.from(new Uint8Array(bytes), (byte) => byte.toString(16).padStart(2, "0")).join("");
}
