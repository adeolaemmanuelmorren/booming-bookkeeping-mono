import { canonicalJson, sha256 } from '../storage/json.ts';
import { Tinybird, sqlString, sqlStrings, uint64Number, waitForReadback } from '../storage/tinybird.ts';
import type { BrowserGroupCommit, BrowserGroupPublisher, BrowserSourcePublisher, GroupMember, RetainedBrowserFact } from './publication.ts';

/** Raw source evidence remains independent of the selected current page or identity. */
export class TinybirdBrowserSourcePublisher implements BrowserSourcePublisher {
  private client: Tinybird;
  private tenantId: string;

  constructor(client: Tinybird, tenantId: string) {
    this.client = client;
    this.tenantId = tenantId;
  }

  async appendSources(records: RetainedBrowserFact[]): Promise<void> {
    await this.client.append('v1_browser_source_records', records.map(record => {
      if (record.source.tenant_id !== this.tenantId) throw new Error('Browser source tenant mismatch');
      return {
        tenant_id: this.tenantId,
        fact_id: record.fact_id,
        source_system: record.source.source_system,
        event_kind: record.source.event_kind,
        source_record_id: record.source.source_record_id,
        source_revision: uint64Number(record.source.source_revision),
        payload_json: canonicalJson(record),
      };
    }));
  }

  async readSources(factIds: string[]): Promise<RetainedBrowserFact[]> {
    if (!factIds.length) return [];
    const result: RetainedBrowserFact[] = [];
    for (let offset = 0; ; offset += 500) {
      const rows = await this.client.query<{ fact_id: string; payload_json: string }>(`
        SELECT DISTINCT fact_id, payload_json FROM v1_browser_source_records
        WHERE tenant_id = ${sqlString(this.tenantId)} AND fact_id IN (${sqlStrings(factIds)})
        ORDER BY fact_id, payload_json LIMIT 500 OFFSET ${offset}
      `);
      for (const row of rows) {
        const fact = JSON.parse(row.payload_json) as RetainedBrowserFact;
        if (fact.fact_id !== row.fact_id || fact.source.tenant_id !== this.tenantId) {
          throw new Error('Browser source payload identity mismatch');
        }
        if (await sha256(fact.source.original_payload) !== fact.source.original_payload_hash) {
          throw new Error('Browser original payload hash mismatch');
        }
        result.push(fact);
      }
      if (rows.length < 500) return result;
    }
  }
}

interface StoredGroupCommit {
  tenant_id: string;
  group_id: string;
  sequence: number;
  member_count: number;
  content_hash: string;
}

interface StoredGroupMember {
  tenant_id: string;
  group_id: string;
  sequence: number;
  member_index: number;
  visitor_key: string;
  revision: string;
  publication_id: string;
  payload_json: string;
}

/** A group marker becomes visible only after every complete visitor reference exists. */
export class TinybirdBrowserGroupPublisher implements BrowserGroupPublisher {
  private client: Tinybird;
  private tenantId: string;
  private readPageRows: number;

  constructor(client: Tinybird, tenantId: string, options: { readPageRows?: number } = {}) {
    this.client = client;
    this.tenantId = tenantId;
    this.readPageRows = options.readPageRows ?? 500;
  }

  async appendGroup(group: BrowserGroupCommit): Promise<void> {
    this.checkTenant(group.tenant_id);
    const wanted = new Map(group.members.map(member => [member.visitor_key, canonicalJson(member)]));
    if (wanted.size !== group.members.length) throw new Error('Duplicate browser group visitor');
    const matches = (members: GroupMember[]) => {
      for (const member of members) {
        if (wanted.get(member.visitor_key) !== canonicalJson(member)) throw new Error('Conflicting browser group member');
      }
      return members.length === wanted.size;
    };
    if (!matches(await this.readMembers(group.tenant_id, group.group_id, group.sequence))) {
      await this.client.append('v1_browser_group_members', group.members.map((member, index) => ({
        tenant_id: group.tenant_id,
        group_id: group.group_id,
        sequence: uint64Number(group.sequence),
        member_index: index,
        visitor_key: member.visitor_key,
        revision: uint64Number(member.revision),
        publication_id: member.publication_id,
        payload_json: canonicalJson(member),
      })));
      await waitForReadback(() => this.readMembers(group.tenant_id, group.group_id, group.sequence), matches);
    }
    await this.client.append('v1_browser_group_commits', [{
      tenant_id: group.tenant_id,
      group_id: group.group_id,
      sequence: uint64Number(group.sequence),
      member_count: group.members.length,
      content_hash: await membersHash(group.members),
    }]);
  }

  async readGroup(tenantId: string, groupId: string): Promise<BrowserGroupCommit | null> {
    this.checkTenant(tenantId);
    const rows = await this.client.query<StoredGroupCommit>(`
      SELECT DISTINCT tenant_id, group_id, sequence, member_count, content_hash
      FROM v1_browser_group_commits WHERE ${groupScope(tenantId, groupId)}
    `);
    if (!rows.length) return null;
    const commits = rows.map(row => ({ ...row, sequence: uint64Number(row.sequence), member_count: Number(row.member_count) }));
    if (commits.some(row => canonicalJson(row) !== canonicalJson(commits[0]))) {
      throw new Error('Conflicting browser group commits');
    }
    const commit = commits[0];
    const members = await this.readMembers(tenantId, groupId, commit.sequence);
    if (members.length < commit.member_count) return null;
    if (members.length !== commit.member_count || await membersHash(members) !== commit.content_hash) {
      throw new Error('Browser group member manifest mismatch');
    }
    return { tenant_id: tenantId, group_id: groupId, sequence: commit.sequence, members };
  }

  private async readMembers(tenantId: string, groupId: string, sequence: number): Promise<GroupMember[]> {
    const members = new Map<string, { index: number; member: GroupMember }>();
    for (let offset = 0; ; offset += this.readPageRows) {
      const rows = await this.client.query<StoredGroupMember>(`
        SELECT DISTINCT tenant_id, group_id, sequence, member_index, visitor_key, toString(revision) AS revision,
          publication_id, payload_json FROM v1_browser_group_members
        WHERE ${groupScope(tenantId, groupId)}
        ORDER BY member_index, visitor_key, payload_json LIMIT ${this.readPageRows} OFFSET ${offset}
      `);
      for (const row of rows) {
        const member = JSON.parse(row.payload_json) as GroupMember;
        if (uint64Number(row.sequence) !== sequence || member.visitor_key !== row.visitor_key
          || member.revision !== row.revision || member.publication_id !== row.publication_id) {
          throw new Error('Browser group member identity mismatch');
        }
        const previous = members.get(member.visitor_key);
        const entry = { index: Number(row.member_index), member };
        if (previous && canonicalJson(previous) !== canonicalJson(entry)) throw new Error('Conflicting browser group member');
        members.set(member.visitor_key, entry);
      }
      if (rows.length < this.readPageRows) {
        const ordered = [...members.values()].sort((a, b) => a.index - b.index);
        if (new Set(ordered.map(row => row.index)).size !== ordered.length) throw new Error('Duplicate browser group member index');
        return ordered.map(row => row.member);
      }
    }
  }

  private checkTenant(tenantId: string): void {
    if (tenantId !== this.tenantId) throw new Error('Browser group tenant mismatch');
  }
}

function groupScope(tenantId: string, groupId: string): string {
  return `tenant_id = ${sqlString(tenantId)} AND group_id = ${sqlString(groupId)}`;
}

async function membersHash(members: GroupMember[]): Promise<string> {
  return sha256(members.map(canonicalJson).sort().join('\n'));
}
