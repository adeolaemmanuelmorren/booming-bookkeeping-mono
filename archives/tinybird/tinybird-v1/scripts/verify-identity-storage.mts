import assert from 'node:assert/strict';
import { writeFile } from 'node:fs/promises';
import { Tinybird } from '../worker/storage/tinybird.ts';
import { sha256 } from '../worker/storage/json.ts';
import { computeIdentityBatch } from '../worker/identity/state.ts';
import { IdentityStorage } from '../worker/identity/storage.ts';
import type { PendingIdentityFact } from '../worker/identity/engine.ts';
import { tinybirdRequest } from './tinybird.mjs';

const response = await tinybirdRequest('/v1/environments');
const { environments } = await response.json();
const branch = environments.find((item: { name: string }) => item.name === 'v1_facts_validation');
if (!branch || branch.main !== '00c04079-d0b4-4d8b-8de6-6fa8072b85af') throw new Error('Validation branch is missing');
class VerificationClient extends Tinybird {
  async query<T>(sql: string): Promise<T[]> {
    const rows = await super.query<T>(sql);
    if (process.env.VERIFY_TRACE === '1') console.log(JSON.stringify({ read: sql.includes('SELECT DISTINCT tenant_id') ? 'publication' : sql.includes('toString(committed_at)') ? 'commit' : 'state', rows: rows.length }));
    return rows;
  }
}
const client = new VerificationClient({ TINYBIRD_URL: 'https://api.us-east.tinybird.co', TINYBIRD_TOKEN: branch.token });
const storage = new IdentityStorage(client);
const tenantId = `verification-${crypto.randomUUID()}`;
const now = new Date().toISOString();

async function fact(key: string, keys: string[], version = 1, deleted = false): Promise<PendingIdentityFact> {
  const payload = JSON.stringify({ first_name: 'Synthetic', last_name: 'Fixture', keys, deleted });
  return {
    eventId: `${key}:${version}`, producerId: 'storage-verification', observedAt: null, ingestedAt: now,
    factKind: 'test', factKey: key, sourcePriority: 3, sourceFactVersion: version,
    factDeleted: deleted, factPayloadHash: await sha256(payload), factPayload: payload, evidenceKeys: keys,
  };
}

async function publish(facts: PendingIdentityFact[], version: number) {
  const batch = { tenantId, version, id: `${tenantId}:${version}`, committedAt: now, facts };
  const result = await computeIdentityBatch(batch, storage.reader(tenantId, version - 1));
  if (process.env.VERIFY_TRACE === '1') console.log(JSON.stringify({ batch: version, expected_rows: result.rows.length }));
  await storage.publish(batch, result.rows);
  return result;
}

const left = await fact('left', ['email:left@example.invalid', 'anonymous_id:left']);
const right = await fact('right', ['email:right@example.invalid', 'anonymous_id:right']);
await publish([left, right], 1);
const initial = await storage.reader(tenantId, 1).mappings(['email:left@example.invalid', 'email:right@example.invalid']);
assert.equal(initial.length, 2);
assert.notEqual(initial[0].profileId, initial[1].profileId);
assert.ok(initial.every(row => row.firstSeenAt === null));

const bridge = await fact('bridge', ['anonymous_id:left', 'anonymous_id:right']);
await publish([bridge], 2);
const merged = await storage.reader(tenantId, 2).mappings(['email:left@example.invalid', 'email:right@example.invalid']);
assert.equal(merged[0].profileId, merged[1].profileId);
await publish([await fact('bridge', [], 2, true)], 3);
const split = await storage.reader(tenantId, 3).mappings(['email:left@example.invalid', 'email:right@example.invalid']);
assert.notEqual(split[0].profileId, split[1].profileId);
const replay = await publish([bridge], 4);
assert.equal(replay.changedFacts.length, 0);
const result = { branch_id: branch.id, tenant_id: tenantId, checked_at: new Date().toISOString(),
  verified: ['actual_payload_publication', 'current_state_sql', 'nullable_times', 'merge', 'split', 'stale_replay'] };
await writeFile(new URL('../evidence/identity-storage-verified.json', import.meta.url), JSON.stringify(result, null, 2) + '\n');
console.log(JSON.stringify(result));
