import { hash } from '../bootstrap/hash.ts';
import type { IdentityBootstrapPlan } from './bootstrap-runner.ts';
import type { IdentityInputs } from './bootstrap-sources.ts';

// Frozen ten browser exports, the preserved live source, and nine snapshots at T.
export const FROZEN_IDENTITY_INPUTS_HASH = 'fbe2f19cd5bdd51e521e0d7d89ab1094d999240493e296035a5da14c5332bad6';
export const FROZEN_NATIVE_IMPORT_PROOF_SHA256 = '8de9093ef7a40b21c51e760e8d3da356accbd29ee6d9337a748bd6e392ce7af8';
export const IDENTITY_WORKSPACE = { id: '00c04079-d0b4-4d8b-8de6-6fa8072b85af', name: 'booming_bookkeeping' };

export async function identityBootstrapPlan(inputs: IdentityInputs, baselineId: string, nativeImportProofSha256: string): Promise<IdentityBootstrapPlan> {
  const plan: IdentityBootstrapPlan = { baselineId, nativeImportProofSha256, tenantId: 'boom', startedAt: '2026-09-05T22:28:00.000000Z',
    sourceSeal: await hash(inputs), inputs, rangeIds: 10000, maxSourceRows: 100000, batchFacts: 20000,
    maxIdentifiers: 30000000, maxComponentFacts: 500000, maxBatchBytes: 32000000 };
  await validateProductionIdentityPlan(plan);
  return plan;
}

/** The reusable executor accepts fixtures; the production entrypoint accepts only frozen inputs. */
export async function validateProductionIdentityPlan(plan: IdentityBootstrapPlan): Promise<void> {
  if (plan.nativeImportProofSha256 !== FROZEN_NATIVE_IMPORT_PROOF_SHA256 || plan.tenantId !== 'boom' || !/^boom-identity-[a-z0-9-]+$/.test(plan.baselineId)
    || plan.startedAt !== '2026-09-05T22:28:00.000000Z'
    || plan.sourceSeal !== FROZEN_IDENTITY_INPUTS_HASH || await hash(plan.inputs) !== FROZEN_IDENTITY_INPUTS_HASH) {
    throw new Error('Identity plan differs from the frozen complete production inputs');
  }
}
