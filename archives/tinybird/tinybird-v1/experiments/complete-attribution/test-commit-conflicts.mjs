import assert from 'node:assert/strict';
import { writeFile } from 'node:fs/promises';
import { connect } from './tinybird-store.mjs';
const scenario = `manifest-fault:${crypto.randomUUID()}`;
const store = await connect(scenario);
const prepared = store.prepare(1, new Map([['retired-profile', []]]));
await store.stage(prepared);
await store.commit(prepared);
await store.commit(prepared);
let empty;
for (let attempt = 0; attempt < 40; attempt++) {
  try { empty = await store.report(undefined, undefined, 1); break; }
  catch (error) { if (!String(error).includes('visible')) throw error; }
  await new Promise(resolve => setTimeout(resolve, 1000));
}
assert.ok(empty);
assert.deepEqual(empty.rows, []);
// A nonconforming writer reuses both version and transaction ID with different content.
await store.appendCommit({ ...prepared.commit, profiles: ['different-profile'] });
let rejected = false;
for (let attempt = 0; attempt < 40; attempt++) {
  try { await store.report('2025-01-01', '2025-01-02', 1); }
  catch (error) {
    if (!String(error).includes('Conflicting committed')) throw error;
    rejected = true;
    break;
  }
  await new Promise(resolve => setTimeout(resolve, 1000));
}
assert.ok(rejected);
const evidence = { verified: true, scenario, checks: ['all-profiles-retired', 'duplicate-empty-commit', 'same-transaction-id-different-content-rejected'] };
await writeFile(new URL('results/commit-conflicts.json', import.meta.url), JSON.stringify(evidence, null, 2));
console.log(JSON.stringify(evidence));
