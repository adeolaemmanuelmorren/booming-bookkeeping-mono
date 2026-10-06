import type { TinybirdEnv } from './storage/tinybird.ts';
import type { BrowserIngressReceipt, BrowserQueueEnvelope } from './browser/ingress.ts';
import type { BrowserBufferReplay } from './browser/buffer-replay.ts';
import type { IdentityCoordinator } from './identity/coordinator.ts';
import type { FivetranFactCoordinator } from './fivetran/runtime-coordinator.ts';
import { handleFivetranAdmin } from './fivetran/admin-routes.ts';
import type { RawTouchpointReplay } from './browser/raw-replay.ts';
import type { IdentityTouchpointReplay } from './identity/touchpoint-replay.ts';
import { handleIdentityFivetranAdmin } from './identity/fivetran-admin.ts';
export { RawTouchpointReplay } from './browser/raw-replay.ts';
export { IdentityTouchpointReplay } from './identity/touchpoint-replay.ts';
export { IdentityBaseline } from './identity/bootstrap-baseline.ts';
export { FivetranFactCoordinator } from './fivetran/runtime-coordinator.ts';
export { RetiredIdentityCoordinator as IdentityCoordinator } from './identity/retired-coordinator.ts';
export { VisitorSessions } from './sessions/visitor-sessions.ts';
export { SessionSnapshotPublisher } from './sessions/tinybird-publisher.ts';
export { DisabledProviderCoordinator as SourceCoordinator } from './sources/disabled-provider-coordinator.ts';
export { SourceReplacementPublisher } from './conversions/tinybird-publisher.ts';
export { BrowserRouter } from './browser/router.ts';
export { BrowserSourcePublisher, BrowserGroupPublisher } from './browser/tinybird-publisher.ts';
export { BrowserIngress } from './browser/ingress.ts';
export { BrowserBufferReplay } from './browser/buffer-replay.ts';
export { BootstrapBaseline } from './bootstrap/baseline-service.ts';

interface Env extends TinybirdEnv {
  TENANT_ID: string;
  ADMIN_TOKEN: string;
  INGESTION_ENABLED?: string;
  IDENTITY_INGESTION_ENABLED?: string;
  IDENTITY_BASELINE_ID?: string;
  FIVETRAN_SNAPSHOT_AT: string;
  BROWSER_INGRESS_MODE?: string;
  BROWSER_BUFFER: R2Bucket;
  BROWSER_INGRESS: { receive(envelope: BrowserQueueEnvelope): Promise<BrowserIngressReceipt> };
  BROWSER_REPLAY: DurableObjectNamespace<BrowserBufferReplay>;
  BROWSER_RAW_REPLAY: DurableObjectNamespace<RawTouchpointReplay>;
  BROWSER_IDENTITY_REPLAY: DurableObjectNamespace<IdentityTouchpointReplay>;
  IDENTITY: DurableObjectNamespace<IdentityCoordinator>;
  FIVETRAN_FACTS: DurableObjectNamespace<FivetranFactCoordinator>;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const path = new URL(request.url).pathname;
    if (path === '/health' && request.method === 'GET') {
      return Response.json({ service: 'boom-tinybird-facts-v1', ingestion_enabled: env.INGESTION_ENABLED === 'true',
        identity_ingestion_enabled: env.IDENTITY_INGESTION_ENABLED === 'true', browser_mode: env.BROWSER_INGRESS_MODE ?? 'buffer' });
    }
    if (!env.ADMIN_TOKEN || request.headers.get('Authorization') !== `Bearer ${env.ADMIN_TOKEN}`) {
      return new Response('Unauthorized', { status: 401 });
    }
    if (request.method !== 'POST') {
      return new Response('Not found', { status: 404 });
    }
    try {
      if (path.startsWith('/admin/identity/fivetran/')) return await handleIdentityFivetranAdmin(request, env);
      if (path.startsWith('/admin/identity/browser-replay/')) {
        const replay = env.BROWSER_IDENTITY_REPLAY.getByName(env.TENANT_ID);
        if (path === '/admin/identity/browser-replay/status') return Response.json(await replay.status());
        if (path === '/admin/identity/browser-replay/start') return Response.json(await replay.start());
        if (path === '/admin/identity/browser-replay/pause') return Response.json(await replay.pause());
        return new Response('Not found', { status: 404 });
      }
      if (path.startsWith('/admin/browser/raw-replay/')) {
        const replay = env.BROWSER_RAW_REPLAY.getByName(env.TENANT_ID);
        if (path.endsWith('/status')) return Response.json(await replay.status());
        if (path.endsWith('/start')) return Response.json(await replay.start());
        if (path.endsWith('/pause')) return Response.json(await replay.pause());
        return new Response('Not found', { status: 404 });
      }
      if (path.startsWith('/admin/fivetran/')) return await handleFivetranAdmin(request, env);
      if (path === '/admin/identity/status') return Response.json(await env.IDENTITY.getByName(env.TENANT_ID).status());
      if (path === '/admin/identity/activate-baseline') {
        if (env.INGESTION_ENABLED === 'true' || env.IDENTITY_INGESTION_ENABLED === 'true'
          || !['buffer', 'collect'].includes(env.BROWSER_INGRESS_MODE ?? 'buffer')) {
          return Response.json({ error: 'Baseline activation requires disabled identity and model ingestion' }, { status: 409 });
        }
        return Response.json(await env.IDENTITY.getByName(env.TENANT_ID).activateBaseline());
      }
      if (path.startsWith('/admin/browser/replay/')) {
        const replay = env.BROWSER_REPLAY.getByName(env.TENANT_ID);
        if (path === '/admin/browser/replay/status') return Response.json(await replay.status());
        if (path === '/admin/browser/replay/pause') return Response.json(await replay.pause());
        if (env.INGESTION_ENABLED !== 'true') return Response.json({ error: 'Ingestion is disabled' }, { status: 409 });
        if (path === '/admin/browser/replay/start') return Response.json(await replay.start());
        if (path === '/admin/browser/replay/wake') return Response.json(await replay.wake());
        return new Response('Not found', { status: 404 });
      }
      if (path === '/admin/browser/probe') {
        if (env.BROWSER_INGRESS_MODE !== 'buffer') return Response.json({ error: 'Probe requires buffer mode' }, { status: 409 });
        const producerId = `verification:${crypto.randomUUID()}`;
        const now = new Date().toISOString();
        const envelope: BrowserQueueEnvelope = { schema_version: 'jitsu_events_api_v1', producer_id: producerId, events: [{
          tenant_id: 'boom', producer_id: producerId, message_id: producerId, delivery_event_id: producerId,
          event_kind: 'v1_probe', observed_at: now, ingested_at: now, source_fact_version: 1, source_deleted: 0,
          fact_payload: JSON.stringify({ type: 'v1_probe', messageId: producerId }),
        }] };
        const receipt = await env.BROWSER_INGRESS.receive(envelope);
        const object = await env.BROWSER_BUFFER.get(receipt.key);
        const verified = receipt.status === 'buffered' && object !== null && await object.text() === JSON.stringify(envelope);
        await env.BROWSER_BUFFER.delete(receipt.key);
        return Response.json({ mode: 'buffer', verified, probe_removed: await env.BROWSER_BUFFER.head(receipt.key) === null }, { status: verified ? 200 : 502 });
      }
      if (path === '/admin/browser/receive') {
        return Response.json(await env.BROWSER_INGRESS.receive(await request.json() as BrowserQueueEnvelope));
      }
      if (path === '/admin/browser/buffer-status') {
        const listing = await env.BROWSER_BUFFER.list({ prefix: 'jitsu/envelopes/', limit: 1000 });
        return Response.json({ mode: env.BROWSER_INGRESS_MODE ?? 'buffer', listed_count: listing.objects.length,
          truncated: listing.truncated, listed_bytes: listing.objects.reduce((sum, object) => sum + object.size, 0),
          latest_upload: listing.objects.reduce((latest, object) => Math.max(latest, object.uploaded.getTime()), 0) });
      }
      return new Response('Not found', { status: 404 });
    } catch (error) {
      return Response.json({ error: error instanceof Error ? error.message : 'V1 operation failed' }, { status: 502 });
    }
  },
};
