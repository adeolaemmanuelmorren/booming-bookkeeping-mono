import { IdentityCoordinator } from '../../../worker/identity/coordinator.ts';
import { BootstrapBaseline } from '../../../worker/bootstrap/baseline-service.ts';
import { computeIdentityBatch } from '../../../worker/identity/state.ts';
import { IdentityStorage } from '../../../worker/identity/storage.ts';
import { Tinybird } from '../../../worker/storage/tinybird.ts';
import type { PendingIdentityFact } from '../../../worker/identity/engine.ts';
import type { BootstrapBaselineEnv } from '../../../worker/bootstrap/configured-baseline.ts';

export { BootstrapBaseline };
export class TestIdentity extends IdentityCoordinator {
  async run() { await this.alarm(); return this.status(); }
  async injectPriorWork(kind: string) {
    if (kind === 'published') {
      this.ctx.storage.sql.exec("UPDATE identity_meta SET value = 1 WHERE key = 'published_version'");
      return;
    }
    if (kind === 'job') {
      this.ctx.storage.sql.exec("INSERT INTO identity_job(singleton,version,id,committed_at,input_cutoff) VALUES (1,1,'old-job','2026-09-01',1)");
      return;
    }
    if (kind === 'outbox') {
      this.ctx.storage.sql.exec("INSERT INTO identity_outbox VALUES ('old', '{}')");
      return;
    }
    this.ctx.storage.sql.exec("INSERT INTO identity_inbox(event_id,fingerprint,payload) VALUES ('old-input','old','{}')");
    if (kind === 'drained') this.ctx.storage.sql.exec('DELETE FROM identity_inbox');
  }
}
interface Env extends BootstrapBaselineEnv {
  IDENTITY: DurableObjectNamespace<TestIdentity>;
  BASELINE: {
    loadSourceHeads(keys: unknown[]): Promise<unknown>;
    loadMembers(keys: string[]): Promise<unknown>;
    loadVisitor(tenantId: string, visitorKey: string): Promise<unknown>;
  };
}
export default {
  async fetch(request: Request, env: Env) {
    try {
      const path = new URL(request.url).pathname;
      if (path === '/seed') {
        const facts = await request.json<PendingIdentityFact[]>();
        const batch = { tenantId: env.TENANT_ID, version: 1, id: 'baseline-identity', committedAt: '2026-09-01T00:00:00.000Z', facts };
        const empty = { facts: async () => [], evidence: async () => [], mappings: async () => [], profiles: async () => [] };
        const result = await computeIdentityBatch(batch, empty);
        await new IdentityStorage(new Tinybird(env)).publish(batch, result.rows);
        return Response.json({ rows: result.rows.length });
      }
      if (path === '/heads') return Response.json(await env.BASELINE.loadSourceHeads(await request.json()));
      if (path === '/members') return Response.json(await env.BASELINE.loadMembers(await request.json()));
      if (path === '/visitor') {
        const { tenant, visitor } = await request.json<{ tenant: string; visitor: string }>();
        return Response.json(await env.BASELINE.loadVisitor(tenant, visitor));
      }
      const identity = env.IDENTITY.getByName('boom');
      if (path === '/activate') return Response.json(await identity.activateBaseline());
      if (path === '/enqueue') return Response.json(await identity.enqueue(await request.json()));
      if (path === '/run') return Response.json(await identity.run());
      if (path === '/inject') { await identity.injectPriorWork((await request.json<{ kind: string }>()).kind); return Response.json(null); }
      return Response.json(await identity.status());
    } catch (error) { return Response.json({ error: String(error) }, { status: 400 }); }
  },
};
