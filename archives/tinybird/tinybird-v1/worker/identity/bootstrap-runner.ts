import { canonicalJson, sha256 } from '../storage/json.ts';
import { compareStrings } from '../sessions/session-engine.ts';
import { sqlString, waitForReadback, uint64Number, type JsonRecord } from '../storage/tinybird.ts';
import { IdentityComponentIndex, buildIdentitySeedBatch, type CompleteIdentityComponent } from '../bootstrap/identity.ts';
import { hash } from '../bootstrap/hash.ts';
import { changedIdentityFacts, type CurrentIdentityFact, type PendingIdentityFact } from './engine.ts';
import { IdentityStorage, identityRecord } from './storage.ts';
import { IdentityBootstrapStore, FACTS_TABLE, type FactRow } from './bootstrap-store.ts';
import { buildIdentityMembership, recordLookupProofs, recordsHash, writeLookupProofs, lookupKey, SCOPES_TABLE, type ScopeRecord, type IdentityMembership } from './bootstrap-membership.ts';
import { verifyIdentityInputs, historicalStream, liveStream, fivetranStreams, type IdentityInputs, type SourceStream } from './bootstrap-sources.ts';

export interface IdentityBootstrapPlan {
  baselineId: string; tenantId: string; startedAt: string; nativeImportProofSha256?: string; sourceSeal: string; inputs: IdentityInputs;
  rangeIds: number; maxSourceRows: number; batchFacts: number; maxIdentifiers: number;
  maxComponentFacts: number; maxBatchBytes: number;
}
export interface IdentityBootstrapSeal {
  format: 'identity-only-v1'; baselineId: string; tenantId: string; identityVersion: 1;
  sourceSeal: string; inputs: IdentityInputs; nativeImportProofSha256?: string;
  counts: { facts: number; identifiers: number; components: number; records: number; recordLookups: number; scopeLookups: number; scopeFacts: number };
  publicationHash: string; membership: IdentityMembership;
}
type Progress = (stage: string, counts: Record<string, unknown>) => void;
interface SourceProgress { cursor: string[] | null; physical: number; candidates: number; chain: string }
interface SelectionProgress { cursor: [string, string] | null; candidates: number; facts: number; scopeFacts: number; generation: number; phase: string }
interface PublicationProgress { lastComponent: string | null; facts: number; components: number; records: number; lookups: number; chain: string }

/** One supervised initializer. Every output batch is verified before its cursor advances. */
export async function initializeIdentity(plan: IdentityBootstrapPlan, store: IdentityBootstrapStore, progress: Progress = () => {}): Promise<IdentityBootstrapSeal> {
  validatePlan(plan, store);
  if (await hash(plan.inputs) !== plan.sourceSeal) throw new Error('Identity source seal differs from frozen inputs');
  const saved = await store.get<IdentityBootstrapPlan>('plan', 'config');
  if (saved && canonicalJson(saved.payload) !== canonicalJson(plan)) throw new Error('Identity bootstrap plan changed');
  if (!saved) {
    const [existing] = await store.client.query<{ rows: number | string }>(`SELECT count() AS rows FROM v1_identity_commits WHERE tenant_id=${sqlString(plan.tenantId)}`);
    const [records] = await store.client.query<{ rows: number | string }>(`SELECT count() AS rows FROM v1_identity_records WHERE tenant_id=${sqlString(plan.tenantId)}`);
    if (uint64Number(existing.rows) || uint64Number(records.rows)) throw new Error('A fresh identity baseline requires an empty identity journal');
    await store.put('plan', 'config', plan);
  }
  const sealed = await store.get<IdentityBootstrapSeal>('seal', 'complete');
  if (sealed) return sealed.payload;
  await verifyIdentityInputs(store, plan.inputs);
  const streams: SourceStream[] = [
    ...plan.inputs.browser.map(input => historicalStream(store, input, plan.startedAt, plan.rangeIds, plan.maxSourceRows)),
    liveStream(store, plan.inputs.live, plan.rangeIds, plan.maxSourceRows),
    ...await fivetranStreams(store, plan.inputs, plan.rangeIds, plan.maxSourceRows),
  ];
  const candidates = await collectSources(store, streams, progress);
  const selected = await selectHeads(plan, store, candidates, progress);
  const indexed = await waitForReadback(async () => {
    const index = new IdentityComponentIndex(plan.maxIdentifiers);
    let count = 0;
    for await (const row of store.facts(selected.phase)) { index.addSelectedFact(await parseFact(row)); count++; }
    if (count > selected.facts) throw new Error('Unexpected selected identity facts');
    return { index, count };
  }, result => result.count === selected.facts);
  const summary = indexed.index.seal(selected.facts);
  progress('indexed', summary);
  await partitionComponents(plan, store, indexed.index, selected, progress);
  const published = await publishComponents(plan, store, indexed.index, progress);
  const scopeLookups = await buildScopeProofs(store, selected.scopeFacts, plan.maxComponentFacts);
  const membership = await buildIdentityMembership(store, published.lookups + scopeLookups);
  if (published.facts !== selected.facts || published.components !== summary.components) throw new Error('Identity publication does not cover the complete graph');
  const seal: IdentityBootstrapSeal = { format: 'identity-only-v1', baselineId: plan.baselineId, tenantId: plan.tenantId,
    identityVersion: 1, sourceSeal: plan.sourceSeal, inputs: plan.inputs,
    ...(plan.nativeImportProofSha256 ? { nativeImportProofSha256: plan.nativeImportProofSha256 } : {}),
    counts: { facts: selected.facts, identifiers: summary.identifiers, components: summary.components,
      records: published.records, recordLookups: published.lookups, scopeLookups, scopeFacts: selected.scopeFacts },
    publicationHash: published.chain, membership };
  await store.put('seal', 'complete', seal);
  progress('sealed', seal.counts);
  return seal;
}

/** Sources have separate receipts. Drain active work before surfacing any failure. */
export async function collectSources(store: IdentityBootstrapStore, streams: SourceStream[], progress: Progress = () => {}): Promise<number> {
  if (new Set(streams.map(stream => stream.key)).size !== streams.length) throw new Error('Identity source keys must be unique');
  let next = 0;
  let candidates = 0;
  let stopped = false;
  const collect = async () => {
    while (!stopped && next < streams.length) {
      const input = streams[next++];
      try {
        const receipt = await collectSource(store, input, progress);
        candidates += receipt.candidates;
      } catch (error) {
        stopped = true;
        throw error;
      }
    }
  };
  const results = await Promise.allSettled(Array.from({ length: Math.min(3, streams.length) }, collect));
  const failure = results.find(result => result.status === 'rejected');
  if (failure?.status === 'rejected') throw failure.reason;
  return candidates;
}

export async function collectSource(store: IdentityBootstrapStore, input: SourceStream, progress: Progress = () => {}): Promise<SourceProgress> {
  const complete = await store.get<SourceProgress>('source', input.key);
  if (complete) {
    if (complete.payload.physical !== input.expectedPhysical) throw new Error('Frozen source count changed');
    return complete.payload;
  }
  const saved = await store.get<SourceProgress>('source-cursor', input.key);
  let sequence = (saved?.sequence ?? 0) + 1;
  let state = saved?.payload ?? { cursor: null, physical: 0, candidates: 0, chain: '' };
  for await (const page of input.pages(state.cursor)) {
    page.rows.sort((left, right) => compareStrings(left.origin_key, right.origin_key));
    for (let offset = 0; offset < page.rows.length; offset += 20000) await store.writeFacts(page.rows.slice(offset, offset + 20000));
    state = { cursor: page.cursor, physical: state.physical + page.physical,
      candidates: state.candidates + page.rows.length, chain: await hash([state.chain, page.cursor, page.physical, page.rows.map(row => [row.origin_key, row.fact_hash])]) };
    if (state.physical > input.expectedPhysical) throw new Error('Frozen identity source contains extra rows');
    await store.put('source-cursor', input.key, state, sequence++);
    progress('source', { input: input.key, physical: state.physical, candidates: state.candidates });
  }
  if (state.physical !== input.expectedPhysical) {
    await store.put('source-cursor', input.key, { cursor: null, physical: 0, candidates: 0, chain: '' }, sequence);
    throw new Error('Frozen identity source was not fully visible; restart replays it safely');
  }
  await store.put('source', input.key, state);
  return state;
}

export async function selectHeads(plan: IdentityBootstrapPlan, store: IdentityBootstrapStore, expectedCandidates: number, progress: Progress): Promise<SelectionProgress> {
  const complete = await store.get<SelectionProgress>('stage', 'selected');
  if (complete) return complete.payload;
  const saved = await store.get<SelectionProgress>('selected-cursor', 'all');
  let state: SelectionProgress = saved?.payload ?? { cursor: null, candidates: 0, facts: 0, scopeFacts: 0, generation: 0, phase: 'selected:0' };
  let sequence = (saved?.sequence ?? 0) + 1;
  let winner: FactRow | null = null;
  let batch: FactRow[] = [];
  let consumed = 0;
  const finishHead = async () => {
    if (!winner) return;
    batch.push({ ...winner, phase: state.phase, origin_key: await hash([winner.fact_kind,winner.fact_key]) });
    winner = null;
  };
  const flush = async () => {
    if (!batch.length) return;
    await store.writeFacts(batch);
    const scopes = batch.filter(row => row.scope_id);
    const last = batch.at(-1)!;
    state = { ...state, cursor: [last.fact_kind,last.fact_key], candidates: state.candidates + consumed,
      facts: state.facts + batch.length, scopeFacts: state.scopeFacts + scopes.length };
    await store.put('selected-cursor', 'all', state, sequence++);
    progress('selected', { facts: state.facts, candidates: state.candidates });
    batch = []; consumed = 0;
  };
  try {
  for await (const row of store.facts('candidate', state.cursor ?? undefined)) {
    if (winner && (winner.fact_kind !== row.fact_kind || winner.fact_key !== row.fact_key)) {
      await finishHead();
      if (batch.length >= plan.batchFacts) await flush();
    }
    consumed++;
    if (!winner) { winner = row; continue; }
    const incoming = await parseFact(row);
    const prior = await parseFact(winner);
    if (changedIdentityFacts([incoming], [currentFact(prior)]).length) winner = row;
  }
  await finishHead(); await flush();
  if (state.candidates !== expectedCandidates) {
    throw new Error('Selected identity input was incomplete; restart the selection');
  }
  } catch (error) {
    await store.put('selected-cursor', 'all', { cursor: null, candidates: 0, facts: 0, scopeFacts: 0, generation: state.generation + 1, phase: `selected:${state.generation + 1}` }, sequence);
    throw error;
  }
  await store.put('stage', 'selected', state);
  return state;
}

export async function partitionComponents(plan: IdentityBootstrapPlan, store: IdentityBootstrapStore, index: IdentityComponentIndex, selected: SelectionProgress, progress: Progress): Promise<void> {
  if (await store.get('stage', 'partitioned')) return;
  const saved = await store.get<{ cursor: [string,string] | null; facts: number }>('partition-cursor', 'all');
  let sequence = (saved?.sequence ?? 0) + 1;
  let count = saved?.payload.facts ?? 0;
  let rows: FactRow[] = [];
  const flush = async () => {
    if (!rows.length) return;
    await store.writeFacts(rows);
    const scopes = rows.filter(row => row.scope_id).map(row => ({ tenant_id: store.tenantId, baseline_id: store.baselineId, ...scopeRecord(row) }));
    if (scopes.length) await store.write(SCOPES_TABLE, scopes,
      group => `${store.scope()} AND tuple(scope_id,fact_kind,fact_key) IN (${group.map(row => `tuple(${[row.scope_id,row.fact_kind,row.fact_key].map(value => sqlString(String(value))).join(',')})`).join(',')})`);
    count += rows.length;
    const last = rows.at(-1)!;
    await store.put('partition-cursor', 'all', { cursor: [last.fact_kind,last.fact_key], facts: count }, sequence++);
    progress('partitioned', { facts: count }); rows = [];
  };
  for await (const row of store.facts(selected.phase, saved?.payload.cursor ?? undefined)) {
    rows.push({ ...row, phase: 'component', component_key: index.componentKey(await parseFact(row)) });
    if (rows.length >= plan.batchFacts) await flush();
  }
  await flush();
  if (count !== selected.facts) {
    await store.put('partition-cursor', 'all', { cursor: null, facts: 0 }, sequence);
    throw new Error('Identity partition is incomplete; restart the stage');
  }
  await store.put('stage', 'partitioned', { facts: count });
}

export async function publishComponents(plan: IdentityBootstrapPlan, store: IdentityBootstrapStore, index: IdentityComponentIndex, progress: Progress): Promise<PublicationProgress> {
  const saved = await store.get<PublicationProgress>('publication', 'all');
  let sequence = (saved?.sequence ?? 0) + 1;
  let state = saved?.payload ?? { lastComponent: null, facts: 0, components: 0, records: 0, lookups: 0, chain: await hash([]) };
  let batch: CompleteIdentityComponent[] = [];
  let facts = 0; let bytes = 0;
  const publish = async () => {
    if (!batch.length) return;
    const id = `identity-bootstrap:${plan.baselineId}:${sequence}`;
    const result = await buildIdentitySeedBatch({ tenantId: plan.tenantId, batchId: id, sourceSeal: plan.sourceSeal,
      committedAt: plan.startedAt, components: batch }, index);
    await new IdentityStorage(store.client, { readPageRows: 5000 }).publish({ tenantId: plan.tenantId, version: 1, id,
      committedAt: plan.startedAt, facts: batch.flatMap(component => component.facts) }, result.rows);
    const records = result.rows.map(identityRecord);
    const lookups = await recordLookupProofs(store, records);
    state = { lastComponent: batch.at(-1)!.componentKey, facts: state.facts + facts, components: state.components + batch.length,
      records: state.records + records.length, lookups: state.lookups + lookups,
      chain: await hash([state.chain, id, await recordsHash(records)]) };
    await store.put('publication', 'all', state, sequence++);
    progress('published', { facts: state.facts, components: state.components, records: state.records, lookups: state.lookups }); batch = []; facts = 0; bytes = 0;
  };
  const pending = index.components().filter(row => state.lastComponent === null || compareStrings(row.key, state.lastComponent) > 0);
  if (pending.some(row => row.count > plan.maxComponentFacts)) throw new Error('A complete identity component exceeds the configured fact limit');
  for (let offset = 0; offset < pending.length;) {
    const group: { key: string; count: number }[] = [];
    let groupFacts = 0;
    while (offset < pending.length && group.length < 1000) {
      const next = pending[offset];
      if (group.length && groupFacts + next.count > plan.batchFacts) break;
      group.push(next); groupFacts += next.count; offset++;
    }
    const expected = new Map(group.map(row => [row.key,row.count]));
    const loaded = await waitForReadback(async () => {
      const groups = new Map(group.map(row => [row.key,[] as PendingIdentityFact[]]));
      for await (const row of store.scan<FactRow>(FACTS_TABLE, ['component_key','fact_kind','fact_key'], `${store.scope()} AND phase='component' AND component_key IN (${group.map(row => sqlString(row.key)).join(',')})`)) {
        const facts = groups.get(row.component_key);
        if (!facts) throw new Error('Unexpected identity component');
        facts.push(await parseFact(row));
        if (facts.length > expected.get(row.component_key)!) throw new Error('Unexpected component facts');
      }
      for (const [key, rows] of groups) if (rows.length > expected.get(key)!) throw new Error('Unexpected component facts');
      return groups;
    }, groups => [...groups].every(([key,rows]) => rows.length === expected.get(key)));
    for (const item of group) {
      const current = loaded.get(item.key)!;
      const size = new TextEncoder().encode(canonicalJson(current)).byteLength;
      if (size > plan.maxBatchBytes) throw new Error('A complete identity component exceeds the configured byte limit');
      if (batch.length && (facts + current.length > plan.batchFacts || bytes + size > plan.maxBatchBytes)) await publish();
      batch.push({ componentKey: item.key, sourceSeal: plan.sourceSeal, facts: current,
        expectedFactCount: item.count, expectedFactHash: await hash(current) });
      facts += current.length; bytes += size;
    }
  }
  await publish(); return state;
}

export async function buildScopeProofs(store: IdentityBootstrapStore, expectedFacts: number, maxScopeFacts: number): Promise<number> {
  const complete = await store.get<{ facts: number; scopes: number }>('scope-proofs', 'complete');
  if (complete) return complete.payload.scopes;
  let scope: string | null = null; let rows: ScopeRecord[] = []; let facts = 0; let count = 0;
  let proofs: { key: string; count: number; hash: string }[] = [];
  const flushScope = async () => {
    if (scope === null) return;
    proofs.push({ key: lookupKey('scope', scope), count: rows.length, hash: await recordsHash(rows) }); count++;

  };
  for await (const row of store.scan<ScopeRecord>(SCOPES_TABLE, ['scope_id','fact_kind','fact_key'], store.scope(), 'scope_id,fact_kind,fact_key,fact_json,fact_hash')) {
    if (scope !== row.scope_id) { await flushScope(); scope = row.scope_id; rows = []; }
    rows.push(row); facts++;
    if (rows.length > maxScopeFacts) throw new Error('Identity scope exceeds its configured fact limit');
  }
  await flushScope();
  if (facts !== expectedFacts) throw new Error('Identity scope proofs are incomplete; retry the stage');
  for (let offset = 0; offset < proofs.length; offset += 5000) await writeLookupProofs(store, proofs.slice(offset, offset + 5000));
  await store.put('scope-proofs', 'complete', { facts, scopes: count }); return count;
}

async function parseFact(row: FactRow): Promise<PendingIdentityFact> {
  if (await sha256(row.fact_json) !== row.fact_hash) throw new Error('Identity fact hash mismatch');
  const fact = JSON.parse(row.fact_json) as PendingIdentityFact;
  if (fact.factKind !== row.fact_kind || fact.factKey !== row.fact_key || fact.sourceFactVersion !== Number(row.source_version)
    || (fact.sourcePriority ?? 0) !== Number(row.source_priority)) throw new Error('Identity source columns disagree with their fact');
  return fact;
}
function currentFact(fact: PendingIdentityFact): CurrentIdentityFact {
  return { ...fact, factObservedAt: fact.observedAt, firstName: '', lastName: '', isDeleted: fact.factDeleted };
}
function scopeRecord(row: FactRow): ScopeRecord {
  return { scope_id: row.scope_id, fact_kind: row.fact_kind, fact_key: row.fact_key, fact_json: row.fact_json, fact_hash: row.fact_hash };
}
function validatePlan(plan: IdentityBootstrapPlan, store: IdentityBootstrapStore): void {
  if (!plan.baselineId || plan.baselineId !== store.baselineId || plan.tenantId !== store.tenantId || !/^[a-f0-9]{64}$/.test(plan.sourceSeal)) throw new Error('Invalid identity bootstrap scope');
  for (const value of [plan.rangeIds,plan.maxSourceRows,plan.batchFacts,plan.maxIdentifiers,plan.maxComponentFacts,plan.maxBatchBytes]) {
    if (!Number.isSafeInteger(value) || value < 1) throw new Error('Identity bootstrap bounds are required');
  }
}
