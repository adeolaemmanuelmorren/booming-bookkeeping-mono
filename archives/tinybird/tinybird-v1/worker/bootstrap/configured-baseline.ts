import { canonicalJson, sha256 } from '../storage/json.ts';
import { Tinybird, type TinybirdEnv } from '../storage/tinybird.ts';
import { TinybirdBootstrapStorage } from './tinybird-storage.ts';
import type { sealBootstrap } from './manifest.ts';
import { validateMembershipSeal, type MembershipSeal } from './membership.ts';

export interface BootstrapBaselineEnv extends TinybirdEnv {
  TENANT_ID: string;
  BROWSER_BASELINE_ID?: string;
  BROWSER_BASELINE_SEAL?: string;
  BROWSER_BASELINE_SEQUENCE?: string;
}
export type BootstrapSeal = Awaited<ReturnType<typeof sealBootstrap>> & { membership: MembershipSeal };
export interface BaselineScope {
  tenantId: string;
  baselineId: string;
  sourceSeal: string;
  sessionSequence: number;
}
export interface BaselineReceipt extends BaselineScope {
  identityVersion: number;
  sealHash: string;
}

export function configuredBaselineScope(env: BootstrapBaselineEnv): BaselineScope {
  const sessionSequence = Number(env.BROWSER_BASELINE_SEQUENCE ?? '0');
  if (!env.TENANT_ID || !env.BROWSER_BASELINE_ID || env.BROWSER_BASELINE_ID === 'empty') throw new Error('Baseline tenant and ID are required');
  if (!env.BROWSER_BASELINE_SEAL || !/^[a-f0-9]{64}$/.test(env.BROWSER_BASELINE_SEAL)) throw new Error('Baseline source seal is required');
  if (!Number.isSafeInteger(sessionSequence) || sessionSequence < 1) throw new Error('Baseline session sequence is required');
  return { tenantId: env.TENANT_ID, baselineId: env.BROWSER_BASELINE_ID, sourceSeal: env.BROWSER_BASELINE_SEAL, sessionSequence };
}

/** The saved manifest is fetched from the configured tenant/run and its actual payload hash is checked. */
export async function readConfiguredBaseline(env: BootstrapBaselineEnv) {
  const scope = configuredBaselineScope(env);
  const store = new TinybirdBootstrapStorage(new Tinybird(env), scope);
  const seal = await store.getManifest<BootstrapSeal>('seal', 'complete');
  if (!seal || seal.runId !== scope.baselineId || seal.tenantId !== scope.tenantId || seal.sourceSeal !== scope.sourceSeal) {
    throw new Error('Saved bootstrap seal does not match the configured baseline');
  }
  if (seal.finalSessionSequence !== scope.sessionSequence || seal.identityVersion !== 1 || seal.visitorRevision !== '1') {
    throw new Error('Saved bootstrap seal has incompatible publication versions');
  }
  if (!/^[a-f0-9]{64}$/.test(seal.inputManifestHash) || !/^[a-f0-9]{64}$/.test(seal.receiptsHash)) throw new Error('Saved bootstrap seal is incomplete');
  for (const key of ['visitors', 'pageHeads', 'sessions', 'identityFacts', 'identityComponents'] as const) {
    if (!Number.isSafeInteger(seal.counts?.[key]) || seal.counts[key] < 0) throw new Error('Saved bootstrap seal has invalid counts');
  }
  validateMembershipSeal(seal.membership);
  const receipt: BaselineReceipt = { ...scope, identityVersion: seal.identityVersion, sealHash: await sha256(canonicalJson(seal)) };
  return { store, seal, receipt };
}
