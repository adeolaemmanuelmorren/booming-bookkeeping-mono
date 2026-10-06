import { canonicalJson, sha256 } from '../storage/json.ts';
import { Tinybird, sqlString, sqlStrings, uint64Number, waitForReadback, type JsonRecord } from '../storage/tinybird.ts';
import type { PendingIdentityFact } from '../identity/engine.ts';
import { conversionIdentityFacts } from './identity.ts';

export interface SourceReplacement {
  source: 'stripe' | 'activecampaign';
  source_account: 'main' | 'kajabi' | 'default';
  scope_id: string;
  replacement_id: string;
  observed_at: string;
  observation_sequence: number;
  rows: JsonRecord[];
  evidence_inbox_ids: string[];
  source_evidence: JsonRecord;
}

export interface SourceRecord extends JsonRecord {
  tenant_id: string;
  source: string;
  source_account: string;
  scope_id: string;
  replacement_id: string;
  observation_sequence: number;
  record_kind: 'conversion' | 'evidence';
  record_id: string;
  payload_json: string;
  payload_hash: string;
}

interface Commit extends JsonRecord {
  tenant_id: string;
  source: string;
  source_account: string;
  scope_id: string;
  replacement_id: string;
  observation_sequence: number;
  observed_at: string;
  row_count: number;
  content_hash: string;
}

export interface IdentityReceiver { enqueue(facts: PendingIdentityFact[]): Promise<unknown> }

/** Each scope is replaced in full, including a contact with zero registrations. */
export async function publishSourceReplacement(
  tenantId: string,
  replacement: SourceReplacement,
  client: Tinybird,
  identity: IdentityReceiver,
): Promise<void> {
  await publishSourceReplacements(tenantId, [replacement], client, identity);
}

/** Share ingestion and verification calls across the source coordinator's bounded outbox. */
export async function publishSourceReplacements(
  tenantId: string,
  replacements: SourceReplacement[],
  client: Tinybird,
  identity: IdentityReceiver,
): Promise<void> {
  if (!replacements.length) return;
  if (replacements.length > 100) throw new Error('Source publication batch exceeds 100 replacements');
  const ids = replacements.map(row => row.replacement_id);
  if (new Set(ids).size !== ids.length) throw new Error('Duplicate source replacement ID');
  const groups = await Promise.all(replacements.map(async replacement => {
    const records = await sourceRecords(tenantId, replacement);
    const manifest: Commit = {
      tenant_id: tenantId, source: replacement.source, source_account: replacement.source_account,
      scope_id: replacement.scope_id, replacement_id: replacement.replacement_id,
      observation_sequence: replacement.observation_sequence, observed_at: replacement.observed_at,
      row_count: records.length, content_hash: await sha256(records.map(canonicalJson).sort().join('\n')),
    };
    return { records, manifest };
  }));
  const records = groups.flatMap(group => group.records);
  const manifests = new Map(groups.map(group => [group.manifest.replacement_id, group.manifest]));
  const expected = new Map(records.map(row => [key(row), canonicalJson(row)]));
  const scope = `tenant_id = ${sqlString(tenantId)} AND replacement_id IN (${sqlStrings(ids)})`;
  const readRecords = async () => {
    const received = new Map<string, string>();
    for (let offset = 0; ; offset += 500) {
      const page = await client.query<SourceRecord>(`SELECT DISTINCT tenant_id, source, source_account,
        scope_id, replacement_id, observation_sequence, record_kind, record_id, payload_json, payload_hash
        FROM v1_source_records WHERE ${scope}
        ORDER BY replacement_id, record_kind, record_id, payload_hash, payload_json LIMIT 500 OFFSET ${offset}`);
      for (const row of page) {
        row.observation_sequence = uint64Number(row.observation_sequence);
        const text = canonicalJson(row);
        if (expected.get(key(row)) !== text) throw new Error('Conflicting source publication record');
        received.set(key(row), text);
      }
      if (page.length < 500) return received;
    }
  };
  const existing = await readRecords();
  await client.append('v1_source_records', records.filter(row => !existing.has(key(row))));
  await waitForReadback(readRecords, rows => rows.size === expected.size);

  // Durable identity receipt can safely precede its own minute publication cycle.
  const facts = (await Promise.all(replacements.map(conversionIdentityFacts))).flat();
  for (let offset = 0; offset < facts.length; offset += 500) {
    await identity.enqueue(facts.slice(offset, offset + 500));
  }

  const readCommits = () => client.query<Commit>(`SELECT tenant_id, source, source_account, scope_id,
    replacement_id, observation_sequence, toString(observed_at) AS observed_at, row_count, content_hash
    FROM v1_source_commits WHERE ${scope}`);
  const verifyCommits = (commits: Commit[]) => {
    const received = new Set<string>();
    for (const commit of commits) {
      const manifest = manifests.get(commit.replacement_id);
      if (!manifest || commit.scope_id !== manifest.scope_id
        || uint64Number(commit.observation_sequence) !== manifest.observation_sequence
        || Number(commit.row_count) !== manifest.row_count || commit.content_hash !== manifest.content_hash) {
        throw new Error('Conflicting source publication commit');
      }
      received.add(commit.replacement_id);
    }
    return received.size === manifests.size;
  };
  const existingCommits = await readCommits();
  if (verifyCommits(existingCommits)) return;
  const present = new Set(existingCommits.map(row => row.replacement_id));
  await client.append('v1_source_commits', [...manifests.values()].filter(row => !present.has(row.replacement_id)));
  await waitForReadback(readCommits, verifyCommits);
}

export async function sourceRecords(tenantId: string, replacement: SourceReplacement): Promise<SourceRecord[]> {
  validateReplacement(tenantId, replacement);
  const records: SourceRecord[] = [];
  const add = async (kind: SourceRecord['record_kind'], id: string, payload: unknown) => {
    const json = canonicalJson(payload);
    records.push({
      tenant_id: tenantId, source: replacement.source, source_account: replacement.source_account,
      scope_id: replacement.scope_id, replacement_id: replacement.replacement_id,
      observation_sequence: replacement.observation_sequence, record_kind: kind,
      record_id: id, payload_json: json, payload_hash: await sha256(json),
    });
  };
  for (const row of replacement.rows) {
    if (typeof row.source_fact_id !== 'string' || !row.source_fact_id) throw new Error('Conversion fact ID is required');
    if (row.source !== replacement.source || row.source_account !== replacement.source_account) {
      throw new Error('Conversion fact belongs to another source account');
    }
    await add('conversion', row.source_fact_id, row);
  }
  // Preserve original evidence even when none of its records qualify as a conversion.
  for (const [name, value] of Object.entries(replacement.source_evidence).sort()) {
    if (Array.isArray(value)) {
      for (let index = 0; index < value.length; index++) await add('evidence', `${name}:${index}`, value[index]);
      if (!value.length) await add('evidence', name, []);
    } else {
      await add('evidence', name, value);
    }
  }
  await add('evidence', '_receipt', { observed_at: replacement.observed_at, evidence_inbox_ids: replacement.evidence_inbox_ids });
  const keys = records.map(key);
  if (new Set(keys).size !== keys.length) throw new Error('Duplicate source publication record key');
  return records;
}

function key(row: SourceRecord): string { return canonicalJson([row.scope_id, row.replacement_id, row.record_kind, row.record_id]); }
function validateReplacement(tenantId: string, replacement: SourceReplacement): void {
  if (!tenantId || !replacement.scope_id || !replacement.replacement_id) throw new Error('Source replacement keys are required');
  const account = replacement.source_account;
  const valid = replacement.source === 'stripe' ? account === 'main' || account === 'kajabi'
    : replacement.source === 'activecampaign' && account === 'default';
  if (!valid) throw new Error('Unexpected source account');
  if (uint64Number(replacement.observation_sequence) === 0) throw new Error('Source sequence must be positive');
  if (!Number.isFinite(Date.parse(replacement.observed_at))) throw new Error('Source observation timestamp is required');
  if (!Array.isArray(replacement.rows) || !Array.isArray(replacement.evidence_inbox_ids)) throw new Error('Source replacement arrays are required');
  if (!replacement.source_evidence || Array.isArray(replacement.source_evidence) || typeof replacement.source_evidence !== 'object') {
    throw new Error('Original source evidence is required');
  }
}
