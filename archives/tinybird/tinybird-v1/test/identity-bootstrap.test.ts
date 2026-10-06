import assert from 'node:assert/strict';
import test from 'node:test';
import { createHash } from 'node:crypto';
import { canonicalJson, sha256 } from '../worker/storage/json.ts';
import { IdentityComponentIndex } from '../worker/bootstrap/identity.ts';
import { collectSource, collectSources, selectHeads, partitionComponents, publishComponents, buildScopeProofs, type IdentityBootstrapPlan } from '../worker/identity/bootstrap-runner.ts';
import { buildIdentityMembership, readIdentityBaselineRecords, readIdentityBaselineScopes, proveIdentityLookups, lookupKey } from '../worker/identity/bootstrap-membership.ts';
import { IdentityBootstrapStore, FACTS_TABLE } from '../worker/identity/bootstrap-store.ts';
import { Tinybird, type JsonRecord } from '../worker/storage/tinybird.ts';
import type { PendingIdentityFact } from '../worker/identity/engine.ts';
import { JournalClient, MemoryStore } from './identity-bootstrap-memory.ts';

const T = '2026-09-05T22:28:00.000000Z';
const progress = () => {};
function plan(): IdentityBootstrapPlan {
  return { baselineId: 'fixture', tenantId: 'test', startedAt: T, sourceSeal: 'a'.repeat(64),
    inputs: { browser: [], live: { table: 'v1_empty', cutoff: T, expectedPhysicalRows: 0 }, fivetran: { snapshotAt: T, tables: [] } },
    rangeIds: 1000, maxSourceRows: 20000, batchFacts: 5000, maxIdentifiers: 10000, maxComponentFacts: 10000, maxBatchBytes: 8000000 };
}
async function fact(key: string, evidenceKeys: string[], observedAt: string|null = T, overrides: Partial<PendingIdentityFact> = {}): Promise<PendingIdentityFact> {
  const payload = canonicalJson({ first_name: null, last_name: null });
  return { eventId: key, producerId: 'fixture', factKind: 'browser', factKey: key, sourcePriority: 2, sourceFactVersion: 2,
    observedAt, ingestedAt: T, factDeleted: false, factPayload: payload, factPayloadHash: await sha256(payload), evidenceKeys, ...overrides };
}

test('lost source checkpoint acknowledgement resumes without skipping or duplicating logical facts', async () => {
  const store = new MemoryStore();
  const rows = await Promise.all([1,2].map(async n => store.factRow(await fact(String(n), ['anonymous_id:'+n]), 'source', String(n))));
  const input = { key: 'source', expectedPhysical: 2, pages: async function* (after: string[]|null) {
    for (let n = after ? Number(after[0])+1 : 1; n<=2; n++) yield { cursor: [String(n)], physical: 1, rows: [rows[n-1]] };
  } };
  store.lostCheckpointAck = true;
  await assert.rejects(collectSource(store, input), /lost checkpoint/);
  const complete = await collectSource(store, input);
  assert.equal(complete.physical, 2); assert.equal(complete.candidates, 2);
  assert.equal(store.rows.get(FACTS_TABLE)!.length, 2);
});

test('a missing source page cannot seal or advance to a permanently incomplete source', async () => {
  const store = new MemoryStore();
  const input = { key: 'source', expectedPhysical: 1, pages: async function* () {} };
  await assert.rejects(collectSource(store,input), /not fully visible/);
  assert.equal(await store.get('source','source'), null);
  assert.equal((await store.get<{ physical:number }>('source-cursor','source'))!.payload.physical, 0);
});

test('a missing competing candidate restarts selection; older live priority wins when visible', async () => {
  const store = new MemoryStore();
  const old = await store.factRow(await fact('same', ['email:old@example.com'], T, { sourcePriority: 1, sourceFactVersion: 999 }), 'old','old');
  const live = await store.factRow(await fact('same', ['email:new@example.com'], T, { sourcePriority: 3, sourceFactVersion: 1 }), 'live','live');
  await store.writeFacts([old,live]); store.omitCandidateOrigin = 'live';
  await assert.rejects(selectHeads(plan(),store,2,progress), /incomplete/);
  assert.equal((await store.get<{ generation:number }>('selected-cursor','all'))!.payload.generation,1);
  store.omitCandidateOrigin = null;
  const selected = await selectHeads(plan(),store,2,progress);
  const rows = [];
  for await (const row of store.facts(selected.phase)) rows.push(row);
  assert.equal(rows.length,1);
  assert.deepEqual(JSON.parse(rows[0].fact_json).evidenceKeys,['email:new@example.com']);
  assert.equal(selected.phase,'selected:1');
});

test('same source priority/version conflict fails before graph publication', async () => {
  const store = new MemoryStore();
  await store.writeFacts([
    await store.factRow(await fact('same',['email:a@example.com']), 'x','one'),
    await store.factRow(await fact('same',['email:b@example.com']), 'x','two'),
  ]);
  await assert.rejects(selectHeads(plan(),store,2,progress), /Conflicting identity source revision/);
  assert.equal((store.client as JournalClient).tables.size,0);
});

test('complete components publish once across a lost commit acknowledgement and authenticate positive/negative reads', async () => {
  class LostCommitClient extends JournalClient {
    fail = true;
    async append(table: string, rows: readonly JsonRecord[]) {
      await super.append(table,rows);
      if (table === 'v1_identity_commits' && this.fail) { this.fail = false; throw new Error('lost commit acknowledgement'); }
    }
  }
  const client = new LostCommitClient(); const store = new MemoryStore(client); const config = plan();
  const inputs = [
    await fact('a',['anonymous_id:visitor','email:z@example.com'],null),
    await fact('b',['anonymous_id:visitor','email:a@example.com'],'2020-01-01T00:00:00.000000Z'),
    await fact('c',['email:z@example.com'],T,{ factKind:'stripe',factKey:'stripe:charge' }),
    ...await Promise.all(Array.from({length:600},(_,i)=>fact('single'+String(i).padStart(4,'0'),['anonymous_id:single'+i]))),
  ];
  await store.writeFacts(await Promise.all(inputs.map((value,i) => store.factRow(value,'input',String(i),value.factKind==='stripe'?'stripe:main:charge:charge':''))));
  const selected = await selectHeads(config,store,inputs.length,progress);
  const index = new IdentityComponentIndex(config.maxIdentifiers);
  for await (const row of store.facts(selected.phase)) index.addSelectedFact(JSON.parse(row.fact_json));
  const summary = index.seal(inputs.length);
  assert.equal(summary.components,601);
  await partitionComponents(config,store,index,selected,progress);
  await assert.rejects(publishComponents(config,store,index,progress), /lost commit/);
  const published = await publishComponents(config,store,index,progress);
  assert.equal(published.facts,603); assert.equal(published.components,601);
  assert.ok(published.records > 2000);
  const scopeCount = await buildScopeProofs(store,selected.scopeFacts,config.maxComponentFacts);
  const membership = await buildIdentityMembership(store,published.lookups+scopeCount);
  const result = await readIdentityBaselineRecords(store,membership,'mapping',['email:z@example.com','email:not-here@example.com']);
  assert.equal(result.length,1);
  const profile = JSON.parse(result[0].payload_json).profileId;
  assert.equal(profile,createHash('md5').update('email:z@example.com').digest('hex'));
  const scopes = await readIdentityBaselineScopes(store,membership,['stripe:main:charge:charge','stripe:main:charge:absent']);
  assert.equal(scopes.get('stripe:main:charge:charge')!.length,1);
  assert.deepEqual(scopes.get('stripe:main:charge:absent'),[]);
  const unknown = await proveIdentityLookups(store,membership,[lookupKey('scope','new')]);
  assert.equal(unknown.get(lookupKey('scope','new')),null);
  const page = membership.pageHashes.findIndex(Boolean);
  store.missingManifest = `membership-page:${page}:${membership.pageHashes[page]}`;
  // Force a key into this descriptor page, without relying on one fixed hash prefix.
  let missingKey = '';
  for (let i=0;;i++) { const key = lookupKey('scope','missing'+i); if (parseInt((await sha256(key)).slice(0,2),16)===page) { missingKey=key; break; } }
  await assert.rejects(proveIdentityLookups(store,membership,[missingKey]), /not visible/);
});

test('immutable write verifies a lost append acknowledgement before allowing a checkpoint', async () => {
  let reads=0; let appends=0;
  const expected = { tenant_id:'test',value:1 };
  const client = { query: async () => ++reads < 3 ? [] : [expected], append: async () => { appends++; throw new Error('lost append'); } } as unknown as Tinybird;
  const store = new IdentityBootstrapStore(client,'test','fixture');
  await store.write('v1_test',[expected],'1');
  assert.equal(appends,1); assert.equal(reads,3);
});

test('20,000 fact uploads use bounded concurrent verification queries without shrinking upload waves', async () => {
  let stored: JsonRecord[]=[];let active=0;let peak=0;let appended=0;const sizes:number[]=[];
  const client={query:async(sql:string)=>{
    sizes.push(new TextEncoder().encode(sql).byteLength);active++;peak=Math.max(peak,active);
    await new Promise(resolve=>setTimeout(resolve,0));active--;
    const wanted=new Set([...sql.matchAll(/'([0-9]{64})'/g)].map(match=>match[1]));
    return stored.filter(row=>wanted.has(String(row.origin_key)));
  },append:async(_table:string,rows:JsonRecord[])=>{appended++;stored.push(...rows);}} as unknown as Tinybird;
  const store=new IdentityBootstrapStore(client,'test','fixture');
  const template=await store.factRow(await fact('fixture',['anonymous_id:v']),'input','0'.repeat(64));
  const rows=Array.from({length:20000},(_,i)=>({...template,origin_key:String(i).padStart(64,'0'),fact_key:String(i)}));
  await store.writeFacts(rows);
  assert.equal(appended,1);assert.equal(stored.length,20000);
  assert.ok(peak>1 && peak<=4);assert.equal(sizes.length,32);assert.ok(sizes.every(bytes=>bytes<=200000));
});

test('parallel sources finish their separate receipts and resume without adding facts', async () => {
  const store = new MemoryStore();
  let active = 0; let peak = 0; let pages = 0;
  const inputs = await Promise.all(Array.from({ length: 7 }, async (_, index) => {
    const key = `source-${index}`;
    const row = await store.factRow(await fact(key, [`anonymous_id:${key}`]), key, key);
    return { key, expectedPhysical: 1, pages: async function* () {
      active++; peak = Math.max(peak, active); pages++;
      await new Promise(resolve => setTimeout(resolve, 5));
      yield { cursor: [key], physical: 1, rows: [row] };
      active--;
    } };
  }));
  assert.equal(await collectSources(store, inputs), 7);
  assert.equal(active, 0); assert.equal(peak, 3);
  assert.equal(await collectSources(store, inputs), 7);
  assert.equal(pages, 7); assert.equal(store.rows.get(FACTS_TABLE)!.length, 7);
});

test('a failed parallel source drains active work and leaves later sources for restart', async () => {
  const store = new MemoryStore();
  const started: string[] = []; const finished: string[] = [];
  const inputs = ['fails', 'active-one', 'active-two', 'later'].map(key => ({
    key, expectedPhysical: 0, pages: async function* () {
      started.push(key);
      if (key === 'fails') throw new Error('source unavailable');
      await new Promise(resolve => setTimeout(resolve, 10));
      finished.push(key);
    },
  }));
  await assert.rejects(collectSources(store, inputs), /source unavailable/);
  assert.deepEqual(started, ['fails', 'active-one', 'active-two']);
  assert.deepEqual(finished, ['active-one', 'active-two']);
  assert.equal(await store.get('source', 'fails'), null);
  assert.equal(await store.get('source', 'later'), null);
  assert.ok(await store.get('source', 'active-one'));
  assert.ok(await store.get('source', 'active-two'));
});

test('unpartitioned scans use their physical sort prefix and reject misplaced writes', async () => {
  const queries: string[] = [];
  const client = { query: async (sql: string) => { queries.push(sql); return []; },
    append: async () => { throw new Error('Unexpected append'); } } as unknown as Tinybird;
  const store = new IdentityBootstrapStore(client, 'test', 'fixture');
  for await (const _row of store.facts('candidate')) { /* Empty source. */ }
  for await (const _row of store.facts('selected:2')) { /* Empty source. */ }
  assert.equal(queries.length, 2);
  assert.ok(queries.every(sql => sql.includes("AND component_key=''")));
  const row = await store.factRow(await fact('bad', ['anonymous_id:v']), 'input', 'origin');
  await assert.rejects(store.writeFacts([{ ...row, component_key: 'unexpected' }]), /cannot have a component key/);
  assert.equal(queries.length, 2);
});

test('a conflicting origin with another physical key cannot complete source selection', async () => {
  const store = new MemoryStore();
  const previous = await store.factRow(await fact('old-key', ['anonymous_id:old']), 'source', 'same-origin');
  const incoming = await store.factRow(await fact('new-key', ['anonymous_id:new']), 'source', 'same-origin');
  await assert.rejects(store.writeFacts([previous, incoming]), /Duplicate identity origin/);
  // A retry with a different sort suffix can miss the previous row in targeted readback.
  // The full candidate scan must still reject it before any graph can be published.
  store.rows.set(FACTS_TABLE, [previous, incoming]);
  await assert.rejects(selectHeads(plan(), store, 1, progress), /input was incomplete/);
  assert.equal(await store.get('stage', 'selected'), null);
  assert.equal(await store.get('stage', 'partitioned'), null);
  assert.equal(await store.get('publication', 'all'), null);
});

test('metadata-indexed readback still compares full JSON payload bytes and retries missing rows only', async () => {
  const expected = [
    { id: 'first', revision: 1, payload_hash: 'same', payload_json: '{"value":"first"}' },
    { id: 'second', revision: 2, payload_hash: 'same', payload_json: '{"value":"second"}' },
  ];
  let stored: JsonRecord[] = [expected[0]];
  const appended: JsonRecord[] = [];
  const client = { query: async () => stored.map(row => ({ ...row, revision: String(row.revision) })),
    append: async (_table: string, rows: JsonRecord[]) => { appended.push(...rows); stored.push(...rows); },
  } as unknown as Tinybird;
  const store = new IdentityBootstrapStore(client, 'test', 'fixture');
  await store.write('v1_test', expected, '1');
  assert.deepEqual(appended, [expected[1]]);
  await store.write('v1_test', expected, '1');
  assert.equal(appended.length, 1);
  stored = [{ ...expected[0], payload_json: '{ "value": "first" }' }, expected[1]];
  await assert.rejects(store.write('v1_test', expected, '1'), /Conflicting immutable identity/);
  assert.equal(appended.length, 1);
});

test('immutable receipts survive reads from replicas with different visible subsets', async () => {
  const first = { id: 'first', value: 1 };
  const second = { id: 'second', value: 2 };
  let reads = 0; const appended: JsonRecord[] = [];
  const client = { query: async () => ++reads % 2 ? [first] : [second],
    append: async (_table: string, rows: JsonRecord[]) => { appended.push(...rows); },
  } as unknown as Tinybird;
  const store = new IdentityBootstrapStore(client, 'test', 'fixture');
  await store.write('v1_test', [first, second], '1');
  assert.equal(reads, 2);
  assert.deepEqual(appended, [second]);
});
