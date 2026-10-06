import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { loadIdentityBootstrapFiles } from '../scripts/bootstrap-identity.ts';
import { identityBootstrapPlan, FROZEN_NATIVE_IMPORT_PROOF_SHA256 } from '../worker/identity/bootstrap-plan.ts';
import { validateNativeImportProof } from '../scripts/bootstrap-import-proof.ts';
const digest=(bytes:Uint8Array)=>createHash('sha256').update(bytes).digest('hex');
const bytes=(value:unknown)=>Buffer.from(JSON.stringify(value));
const T='2026-09-05T22:28:00.000000Z';
const originalProofBytes=readFileSync(new URL('../restore/native-import-verification.json',import.meta.url));
function proof() { return JSON.parse(Buffer.from(originalProofBytes).toString('utf8')); }

async function files(value?:unknown) {
  const input=JSON.parse(await readFile(new URL('../restore/identity-inputs.json',import.meta.url),'utf8'));
  const proofBytes=value===undefined?originalProofBytes:bytes(value);const plan=await identityBootstrapPlan(input,'boom-identity-test-v1',FROZEN_NATIVE_IMPORT_PROOF_SHA256);
  const planBytes=bytes(plan);
  const manifest=bytes({snapshotAt:T,files:[{path:'config/identity-plan.json',bytes:planBytes.length,sha256:digest(planBytes)},
    {path:'config/native-import-verification.json',bytes:proofBytes.length,sha256:digest(proofBytes)}]});
  return [planBytes,proofBytes,manifest] as const;
}

test('production loader accepts exact frozen proof and retains its hash for the identity seal',async()=>{
  const inputs=await files();const {plan,proof}=await loadIdentityBootstrapFiles(...inputs);
  assert.equal(proof.tables.length,44);assert.equal(plan.nativeImportProofSha256,digest(inputs[1]));
});

test('production loader rejects an internally consistent reduced conversion snapshot and partial imports',async()=>{
  const reduced=proof();const table=reduced.tables.find((table:{table:string})=>table.table==='v1_snapshot_stripe_charge')!;
  table.rows--;table.expectedRows--;table.expectedNativeRows--;table.acceptedRows--;
  assert.throws(()=>validateNativeImportProof(reduced),/frozen source manifests/);
  await assert.rejects(loadIdentityBootstrapFiles(...await files(reduced)),/pinned digest/);
  const partial=proof();partial.tables[0].successfulFiles--;
  assert.throws(()=>validateNativeImportProof(partial),/incomplete table/);
  await assert.rejects(loadIdentityBootstrapFiles(...await files(partial)),/pinned digest/);
});

test('production loader rejects modified bytes and an unbound build manifest',async()=>{
  const [config,proofBytes,manifest]=await files();
  await assert.rejects(loadIdentityBootstrapFiles(config,Buffer.concat([proofBytes,Buffer.from(' ')]),manifest),/pinned digest/);
  await assert.rejects(loadIdentityBootstrapFiles(config,proofBytes,bytes({snapshotAt:T,files:[]})),/build manifest/);
});
