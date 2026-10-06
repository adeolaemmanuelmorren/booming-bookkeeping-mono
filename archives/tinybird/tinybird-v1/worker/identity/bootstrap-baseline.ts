import { WorkerEntrypoint } from 'cloudflare:workers';
import { hash } from '../bootstrap/hash.ts';
import { Tinybird, type TinybirdEnv } from '../storage/tinybird.ts';
import { IdentityBootstrapStore } from './bootstrap-store.ts';
import { readIdentityBaselineRecords, readIdentityBaselineScopes, validateIdentityMembership } from './bootstrap-membership.ts';
import type { IdentityBootstrapSeal } from './bootstrap-runner.ts';
import type { IdentityRecord } from './storage.ts';
import type { PendingIdentityFact } from './engine.ts';

interface Env extends TinybirdEnv {
  TENANT_ID: string;
  IDENTITY_BASELINE_ID: string;
  IDENTITY_BASELINE_SEAL: string;
}

/** Private service binding only. No HTTP route or caller-chosen tenant/baseline. */
export class IdentityBaseline extends WorkerEntrypoint<Env> {
  async readRecords(input: { kind: string; keys: string[] }): Promise<IdentityRecord[]> {
    const { kind, keys } = input;
    if (!['fact','evidence','mapping','profile'].includes(kind)) throw new Error('Invalid identity state kind');
    const { store, seal } = await this.baseline();
    return readIdentityBaselineRecords(store, seal.membership, kind, keys);
  }

  async readScopes(input: { tenantId: string; baselineId: string; scopeIds: string[] }): Promise<Record<string, PendingIdentityFact[]>> {
    if (input.tenantId !== this.env.TENANT_ID || input.baselineId !== this.env.IDENTITY_BASELINE_ID) throw new Error('Identity scope request belongs to another baseline');
    const { store, seal } = await this.baseline();
    return Object.fromEntries(await readIdentityBaselineScopes(store, seal.membership, input.scopeIds));
  }

  private async baseline(): Promise<{ store: IdentityBootstrapStore; seal: IdentityBootstrapSeal }> {
    const store = new IdentityBootstrapStore(new Tinybird(this.env), this.env.TENANT_ID, this.env.IDENTITY_BASELINE_ID);
    const values = await store.requireMany<IdentityBootstrapSeal>('seal', ['complete']);
    const seal = values.get('complete')!;
    if (seal.format !== 'identity-only-v1' || seal.identityVersion !== 1 || seal.tenantId !== this.env.TENANT_ID
      || seal.baselineId !== this.env.IDENTITY_BASELINE_ID
      || await hash(seal.inputs) !== seal.sourceSeal) throw new Error('Identity baseline seal differs from its configured scope');
    if (await hash(seal) !== this.env.IDENTITY_BASELINE_SEAL || !/^[a-f0-9]{64}$/.test(seal.nativeImportProofSha256 ?? '')) throw new Error('Identity baseline content is not the activated sealed publication');
    validateIdentityMembership(seal.membership);
    return { store, seal };
  }
}
