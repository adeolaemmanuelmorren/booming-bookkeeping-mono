import { IdentityCoordinator } from '../../../worker/identity/coordinator.ts';
import { WorkerEntrypoint } from 'cloudflare:workers';

export class EmptyBaseline extends WorkerEntrypoint { async readRecords() { return []; } }

export class TestIdentityCoordinator extends IdentityCoordinator {
  private failWake = false;
  async wake(): Promise<void> {
    if (this.failWake) throw new Error('Injected alarm scheduling failure');
    await super.wake();
  }
  async receiptWithAlarmFailure(facts: Parameters<IdentityCoordinator['enqueue']>[0]): Promise<void> {
    this.failWake = true;
    try { await this.enqueue(facts); }
    finally { this.failWake = false; }
  }
  async runAlarm(): Promise<void> { await this.alarm(); }
  async clearAlarm(): Promise<void> { await this.ctx.storage.deleteAlarm(); }
  async alarmTime(): Promise<number | null> { return this.ctx.storage.getAlarm(); }
  async primeLease(): Promise<string> {
    const cutoff = this.ctx.storage.sql.exec<{ cutoff: number }>('SELECT MAX(sequence) AS cutoff FROM identity_inbox').one().cutoff;
    this.ctx.storage.sql.exec(`INSERT INTO identity_job
      (singleton, version, id, committed_at, input_cutoff, lease_id, lease_until)
      VALUES (1, 1, 'crashed-batch', '2026-09-05T00:00:00.000Z', ?, 'crashed-worker', ?)`, cutoff, Date.now() + 60_000);
    return 'crashed-batch';
  }
  async expireLease(): Promise<void> {
    this.ctx.storage.sql.exec('UPDATE identity_job SET lease_until = 0');
  }
}

interface Env { IDENTITY: DurableObjectNamespace<TestIdentityCoordinator> }

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const stub = env.IDENTITY.getByName('boom');
    try {
      const path = new URL(request.url).pathname;
      if (path === '/activate') return Response.json(await stub.activateBaseline());
      if (path === '/enqueue') return Response.json(await stub.enqueue(await request.json()));
      if (path === '/fail-wake') await stub.receiptWithAlarmFailure(await request.json());
      if (path === '/run') await stub.runAlarm();
      if (path === '/clear-alarm') await stub.clearAlarm();
      if (path === '/prime-lease') await stub.primeLease();
      if (path === '/expire-lease') await stub.expireLease();
      return Response.json({ ...await stub.status(), alarmAt: await stub.alarmTime() });
    } catch (error) {
      return new Response(error instanceof Error ? error.message : 'Test call failed', { status: 400 });
    }
  },
};
