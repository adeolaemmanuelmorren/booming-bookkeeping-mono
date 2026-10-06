import { DurableObject } from 'cloudflare:workers';

/** Preserve the unused namespace while making direct provider ingestion impossible. */
export class DisabledProviderCoordinator extends DurableObject<Record<string, unknown>> {
  constructor(ctx: DurableObjectState, env: Record<string, unknown>) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(() => ctx.storage.deleteAlarm());
  }

  async alarm(): Promise<void> {
    await this.ctx.storage.deleteAlarm();
  }

  async status(): Promise<{ disabled: true; source: 'fivetran' }> {
    return { disabled: true, source: 'fivetran' };
  }

  async startBackfill(): Promise<never> {
    throw new Error('Direct provider ingestion is disabled; conversions use Fivetran');
  }

  async wake(): Promise<never> {
    throw new Error('Direct provider ingestion is disabled; conversions use Fivetran');
  }
}
