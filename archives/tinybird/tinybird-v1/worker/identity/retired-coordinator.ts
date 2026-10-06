import { DurableObject } from 'cloudflare:workers';

/** Preserves the unused namespace; identity belongs to bill-realtime-coordinator. */
export class RetiredIdentityCoordinator extends DurableObject<unknown> {
  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => { await ctx.storage.deleteAlarm(); });
  }
  async alarm() { await this.ctx.storage.deleteAlarm(); }
  async status() { return { retired: true, pending: 0, publishedVersion: 0, activeBatch: null, baseline: null }; }
  async activateBaseline(): Promise<never> { throw new Error('Replacement identity worker is retired'); }
  async enqueue(): Promise<never> { throw new Error('Replacement identity worker is retired'); }
  async wake() { return this.status(); }
}
