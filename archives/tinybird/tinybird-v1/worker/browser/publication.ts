import { canonicalJson, type SessionFact } from "../sessions/session-engine.ts";
import { publishSnapshot, type SessionPublisher, type SessionSnapshot, type SnapshotRecord } from "../sessions/session-publication.ts";
import type { BrowserSourceFact } from "./normalize.ts";
import { waitForReadback } from "../storage/tinybird.ts";

export interface RetainedBrowserFact {
  fact_id: string;
  source: BrowserSourceFact;
  rejection: string | null;
}

/** Read methods return complete, paginated results. Raw facts are never silently dropped. */
export interface BrowserSourcePublisher {
  appendSources(records: RetainedBrowserFact[]): Promise<void>;
  readSources(factIds: string[]): Promise<RetainedBrowserFact[]>;
}

export interface GroupMember {
  visitor_key: string;
  revision: string;
  publication_id: string;
  records: { session_id: string; payload_hash: string }[];
}

export interface BrowserGroupCommit {
  tenant_id: string;
  group_id: string;
  sequence: number;
  members: GroupMember[];
}

/** Issued atomically by BrowserRouter. Read every member before returning any sessions. */
export interface BrowserReadPlan {
  tenant_id: string;
  sequence: number;
  visitor_keys: string[];
  members: GroupMember[];
}

export interface BrowserGroupPublisher {
  appendGroup(commit: BrowserGroupCommit): Promise<void>;
  readGroup(tenantId: string, groupId: string): Promise<BrowserGroupCommit | null>;
}

export async function publishSources(records: RetainedBrowserFact[], publisher: BrowserSourcePublisher): Promise<void> {
  if (!records.length) return;
  const ids = records.map((record) => record.fact_id);
  if (sourcesMatch(records, await publisher.readSources(ids))) return;
  await publisher.appendSources(records);
  await waitForReadback(() => publisher.readSources(ids), rows => sourcesMatch(records, rows));
}

export async function publishGroup(
  tenantId: string, groupId: string, sequence: number, snapshots: SessionSnapshot[],
  sessions: SessionPublisher, groups: BrowserGroupPublisher,
): Promise<BrowserGroupCommit> {
  await publishSessionBatch(snapshots, sessions);
  const members: GroupMember[] = [];
  for (const snapshot of [...snapshots].sort((a, b) => a.visitor_key.localeCompare(b.visitor_key))) {
    members.push(await snapshotMember(snapshot));
  }
  const commit = { tenant_id: tenantId, group_id: groupId, sequence, members };
  await commitGroup(commit, groups);
  return commit;
}

export async function snapshotMember(snapshot: SessionSnapshot): Promise<GroupMember> {
  const records = [];
  for (const session of snapshot.sessions) records.push({ session_id: session.session_id, payload_hash: await sha256(canonicalJson(session)) });
  records.sort((a, b) => a.session_id.localeCompare(b.session_id));
  return { visitor_key: snapshot.visitor_key, revision: snapshot.revision, publication_id: snapshot.publication_id, records };
}

export async function publishSessionBatch(snapshots: SessionSnapshot[], publisher: SessionPublisher): Promise<void> {
  if (!snapshots.length) return;
  if (publisher.publishSnapshots) {
    await publisher.publishSnapshots(snapshots);
    return;
  }
  for (const snapshot of snapshots) await publishSnapshot(snapshot, publisher);
}

export async function commitGroup(commit: BrowserGroupCommit, groups: BrowserGroupPublisher): Promise<void> {
  let visible = await groups.readGroup(commit.tenant_id, commit.group_id);
  if (visible && canonicalJson(visible) !== canonicalJson(commit)) throw new Error("Conflicting browser group commit");
  if (!visible) {
    await groups.appendGroup(commit);
    visible = await waitForReadback(() => groups.readGroup(commit.tenant_id, commit.group_id), row => {
      if (row && canonicalJson(row) !== canonicalJson(commit)) throw new Error("Conflicting browser group commit");
      return row !== null;
    });
  }
  if (canonicalJson(visible) !== canonicalJson(commit)) throw new Error("Browser group commit is not fully visible");
}

/** Bounded reads use an authoritative plan instead of replaying historical groups. */
export async function readSnapshotPlan(plan: BrowserReadPlan, records: SnapshotRecord[]): Promise<Record<string, SessionFact[]>> {
  const synthetic = { tenant_id: plan.tenant_id, group_id: "read-plan", sequence: 1, members: plan.members };
  const result = await readCompletePrefix(plan.tenant_id, [synthetic], records);
  if (result.sequence !== 1) throw new Error("Read plan snapshots are not fully visible");
  return Object.fromEntries(plan.visitor_keys.map(key => [key, result.visitors[key] ?? []]));
}

/** Executable read specification. The parent implements this contract in its endpoint. */
export async function readCompletePrefix(
  tenantId: string, commits: BrowserGroupCommit[], records: SnapshotRecord[],
): Promise<{ sequence: number; visitors: Record<string, SessionFact[]> }> {
  const bySequence = new Map<number, BrowserGroupCommit>();
  for (const commit of commits) {
    if (commit.tenant_id !== tenantId) continue;
    const previous = bySequence.get(commit.sequence);
    if (previous && canonicalJson(previous) !== canonicalJson(commit)) throw new Error("Conflicting group sequence");
    bySequence.set(commit.sequence, commit);
  }
  const visitors: Record<string, SessionFact[]> = {};
  let sequence = 0;
  while (bySequence.has(sequence + 1)) {
    const group = bySequence.get(sequence + 1)!;
    const staged: Record<string, SessionFact[]> = {};
    let complete = true;
    for (const member of group.members) {
      const actual = records.filter((row) => row.tenant_id === tenantId && row.publication_id === member.publication_id);
      const expected = new Map(member.records.map((row) => [row.session_id, row.payload_hash]));
      const found = new Map<string, SessionFact>();
      for (const row of actual) {
        if (row.visitor_key !== member.visitor_key || row.revision !== member.revision || expected.get(row.session_id) !== row.payload_hash) {
          throw new Error("Unexpected or conflicting group snapshot row");
        }
        if (await sha256(row.payload_json) !== row.payload_hash) throw new Error("Group snapshot payload hash mismatch");
        const payload = JSON.parse(row.payload_json) as SessionFact;
        if (payload.session_id !== row.session_id || payload.visitor_key !== row.visitor_key) throw new Error("Group snapshot payload identity mismatch");
        found.set(row.session_id, payload);
      }
      if (found.size !== expected.size) { complete = false; break; }
      staged[member.visitor_key] = [...found.values()];
    }
    if (!complete) break;
    Object.assign(visitors, staged);
    sequence++;
  }
  return { sequence, visitors };
}

function sourcesMatch(expected: RetainedBrowserFact[], actual: RetainedBrowserFact[]): boolean {
  const wanted = new Map(expected.map((row) => [row.fact_id, canonicalJson(row)]));
  const found = new Set<string>();
  for (const row of actual) {
    if (wanted.get(row.fact_id) !== canonicalJson(row)) throw new Error("Unexpected or conflicting browser source row");
    found.add(row.fact_id);
  }
  return found.size === wanted.size;
}

export async function sha256(value: string): Promise<string> {
  const bytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return Array.from(new Uint8Array(bytes), (byte) => byte.toString(16).padStart(2, "0")).join("");
}
