import assert from 'node:assert/strict';
import { writeFile } from 'node:fs/promises';
import { Tinybird, sqlString, waitForReadback } from '../worker/storage/tinybird.ts';
import { canonicalJson, sha256 } from '../worker/storage/json.ts';
import { normalizeHistorical } from '../worker/browser/normalize.ts';
import { TinybirdBrowserSourcePublisher, TinybirdBrowserGroupPublisher } from '../worker/browser/tinybird-storage.ts';
import { publishSources, publishGroup, readCompletePrefix, type RetainedBrowserFact } from '../worker/browser/publication.ts';
import { TinybirdSessionPublisher } from '../worker/sessions/tinybird-storage.ts';
import { buildSessions, type PageView } from '../worker/sessions/session-engine.ts';
import { createSnapshot, type SnapshotRecord } from '../worker/sessions/session-publication.ts';
import { tinybirdRequest } from './tinybird.mjs';

const { environments } = await (await tinybirdRequest('/v1/environments')).json();
const branch = environments.find((row: { name: string }) => row.name === 'v1_facts_validation');
if (branch?.main !== '00c04079-d0b4-4d8b-8de6-6fa8072b85af') throw new Error('Wrong validation branch');
const client = new Tinybird({ TINYBIRD_URL: 'https://api.us-east.tinybird.co', TINYBIRD_TOKEN: branch.token });
const tenant = `verification-browser-${crypto.randomUUID()}`;
const sources = new TinybirdBrowserSourcePublisher(client, tenant);
const groups = new TinybirdBrowserGroupPublisher(client, tenant);
const sessions = new TinybirdSessionPublisher(client);
const now = new Date().toISOString();
const visitors = ['Zulu', 'alpha', '9', ...Array.from({ length: 22 }, (_, index) => `visitor-${index}`)];
const normalized = await Promise.all(visitors.map((visitor, index) => normalizeHistorical({
  tenantId: tenant, source: 'jitsu_data', kind: 'page_view', ingestedAt: now,
  record: { id: `synthetic-page-${index}`, anonymous_id: visitor,
    timestamp: '2026-09-01T12:00:00.123456Z', received_at: '2026-09-01T12:00:01.123456Z' },
})));
const retained: RetainedBrowserFact[] = await Promise.all(normalized.map(async event => ({
  fact_id: await sha256(canonicalJson(event)), source: event.source, rejection: null,
})));
await publishSources(retained, sources);
await publishSources(retained, sources);
assert.equal((await sources.readSources(retained.map(row => row.fact_id))).length, visitors.length);
const snapshots = normalized.map(event => {
  const page = event.pageRevision!.page!;
  return createSnapshot(tenant, page.visitor_key, '1', buildSessions(page.visitor_key, [page]));
});
const first = await publishGroup(tenant, 'synthetic-group-1', 1, snapshots, sessions, groups);
assert.equal(first.members.length, visitors.length);
assert.deepEqual(await groups.readGroup(tenant, first.group_id), first);

// Move a page between visitors. Both complete replacements share one group.
const oldVisitor = visitors[0];
const newVisitor = visitors[1];
const movedPage: PageView = { ...normalized[0].pageRevision!.page!, visitor_key: newVisitor };
const destinationPage = normalized[1].pageRevision!.page!;
const second = await publishGroup(tenant, 'synthetic-group-2', 2, [
  createSnapshot(tenant, oldVisitor, '2', []),
  createSnapshot(tenant, newVisitor, '2', buildSessions(newVisitor, [movedPage, destinationPage])),
], sessions, groups);
await publishGroup(tenant, 'synthetic-group-2', 2, [
  createSnapshot(tenant, oldVisitor, '2', []),
  createSnapshot(tenant, newVisitor, '2', buildSessions(newVisitor, [movedPage, destinationPage])),
], sessions, groups);
const visible = await waitForReadback(async () => {
  const records = await client.query<SnapshotRecord>(`
    SELECT DISTINCT tenant_id, visitor_key, toString(revision) AS revision,
      publication_id, session_id, payload_json, payload_hash
    FROM v1_session_records WHERE tenant_id = ${sqlString(tenant)}
  `);
  return readCompletePrefix(tenant, [first, second], records);
}, result => result.sequence === 2);
assert.equal(visible.visitors[oldVisitor].length, 0);
assert.equal(visible.visitors[newVisitor].length, 1);
assert.equal(visible.visitors[newVisitor][0].page_view_count, 2);
await assert.rejects(() => groups.appendGroup({ ...second, members: [...second.members, second.members[0]] }), /Duplicate/);

const evidence = {
  tenant, verified_at: new Date().toISOString(), source_facts: retained.length,
  initial_visitors: visitors.length, group_sequence: visible.sequence,
  cross_visitor_move: true, empty_replacement: true, idempotent_replay: true,
  original_payload_verified: true, exact_member_order: true,
};
await writeFile(new URL('../evidence/browser-storage-verified.json', import.meta.url), JSON.stringify(evidence, null, 2) + '\n');
console.log(JSON.stringify(evidence));
