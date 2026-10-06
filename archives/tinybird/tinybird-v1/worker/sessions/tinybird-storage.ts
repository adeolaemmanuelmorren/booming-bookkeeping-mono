import { canonicalJson } from '../storage/json.ts';
import { Tinybird, sqlString, sqlStrings, uint64Number, waitForReadback } from '../storage/tinybird.ts';
import { snapshotPublication, type SessionPublisher, type SessionSnapshot, type SnapshotManifest, type SnapshotRecord } from './session-publication.ts';

export class TinybirdSessionPublisher implements SessionPublisher {
  private client: Tinybird;
  private readPageRows: number;
  private maxVisitors: number;

  constructor(client: Tinybird, options: { readPageRows?: number; maxVisitors?: number } = {}) {
    this.client = client;
    this.readPageRows = options.readPageRows ?? 500;
    this.maxVisitors = options.maxVisitors ?? 500;
  }

  async appendRecords(records: SnapshotRecord[]): Promise<void> {
    await this.client.append('v1_session_records', records.map(row => ({ ...row, revision: uint64Number(row.revision) })));
  }

  async readRecords(manifest: SnapshotManifest): Promise<SnapshotRecord[]> {
    return this.readManyRecords([manifest]);
  }

  async appendCommit(manifest: SnapshotManifest): Promise<void> {
    await this.client.append('v1_session_commits', [{ ...manifest, revision: uint64Number(manifest.revision) }]);
  }

  async readCommit(manifest: SnapshotManifest): Promise<SnapshotManifest | null> {
    return (await this.readManyCommits([manifest]))[0] ?? null;
  }

  /** One bounded set of calls for all visitors in a browser group. */
  async publishSnapshots(snapshots: SessionSnapshot[]): Promise<void> {
    if (!snapshots.length) return;
    if (snapshots.length > this.maxVisitors) throw new Error('Session publication exceeds its visitor bound');
    const publications = await Promise.all(snapshots.map(snapshotPublication));
    const manifests = publications.map(item => item.manifest);
    const records = publications.flatMap(item => item.records);
    const expected = new Map(records.map(row => [recordKey(row), canonicalJson(row)]));
    const wantedCommits = new Map(manifests.map(row => [row.publication_id, canonicalJson(row)]));
    if (wantedCommits.size !== manifests.length) throw new Error('Duplicate session publication ID');
    const checkRecords = (actual: SnapshotRecord[]) => verifiedKeys(actual, expected, recordKey);
    const existing = checkRecords(await this.readManyRecords(manifests));
    const missing = records.filter(row => !existing.has(recordKey(row)));
    if (missing.length) await this.appendRecords(missing);
    await waitForReadback(() => this.readManyRecords(manifests), rows => checkRecords(rows).size === expected.size);

    const checkCommits = (actual: SnapshotManifest[]) => verifiedKeys(actual, wantedCommits, row => row.publication_id);
    const existingCommits = checkCommits(await this.readManyCommits(manifests));
    const missingCommits = manifests.filter(row => !existingCommits.has(row.publication_id));
    if (missingCommits.length) {
      await this.client.append('v1_session_commits', missingCommits.map(row => ({ ...row, revision: uint64Number(row.revision) })));
    }
    await waitForReadback(() => this.readManyCommits(manifests), rows => checkCommits(rows).size === wantedCommits.size);
  }

  private async readManyRecords(manifests: SnapshotManifest[]): Promise<SnapshotRecord[]> {
    const records: SnapshotRecord[] = [];
    for (let offset = 0; ; offset += this.readPageRows) {
      const page = await this.client.query<SnapshotRecord>(`
        SELECT DISTINCT tenant_id, visitor_key, toString(revision) AS revision,
          publication_id, session_id, payload_json, payload_hash
        FROM v1_session_records WHERE ${scope(manifests)}
        ORDER BY publication_id, session_id, payload_hash, payload_json LIMIT ${this.readPageRows} OFFSET ${offset}
      `);
      records.push(...page);
      if (page.length < this.readPageRows) return records;
    }
  }

  private async readManyCommits(manifests: SnapshotManifest[]): Promise<SnapshotManifest[]> {
    const result = new Map<string, SnapshotManifest>();
    for (let offset = 0; ; offset += this.readPageRows) {
      const rows = await this.client.query<SnapshotManifest>(`
        SELECT DISTINCT tenant_id, visitor_key, toString(revision) AS revision,
          publication_id, row_count, content_hash
        FROM v1_session_commits WHERE ${scope(manifests)}
        ORDER BY publication_id, content_hash LIMIT ${this.readPageRows} OFFSET ${offset}
      `);
      for (const raw of rows) {
        const row = { ...raw, row_count: Number(raw.row_count) };
        const previous = result.get(row.publication_id);
        if (previous && canonicalJson(previous) !== canonicalJson(row)) throw new Error('Conflicting session commit records');
        result.set(row.publication_id, row);
      }
      if (rows.length < this.readPageRows) return [...result.values()];
    }
  }
}

function scope(manifests: SnapshotManifest[]): string {
  const tenantId = manifests[0].tenant_id;
  if (manifests.some(row => row.tenant_id !== tenantId)) throw new Error('Session publication tenant mismatch');
  return `tenant_id = ${sqlString(tenantId)}
    AND visitor_key IN (${sqlStrings([...new Set(manifests.map(row => row.visitor_key))])})
    AND publication_id IN (${sqlStrings(manifests.map(row => row.publication_id))})`;
}

function recordKey(row: SnapshotRecord): string {
  return canonicalJson([row.publication_id, row.session_id]);
}

function verifiedKeys<T>(rows: T[], expected: Map<string, string>, key: (row: T) => string): Set<string> {
  const found = new Set<string>();
  for (const row of rows) {
    const id = key(row);
    if (expected.get(id) !== canonicalJson(row)) throw new Error('Conflicting session publication record');
    found.add(id);
  }
  return found;
}
