import type { BulkBootstrapCheckpoint, PipelineId, PreparedScope } from './contracts.ts';
import type { FivetranFactCoordinator } from './runtime-coordinator.ts';
import type { IdentityCoordinator } from '../identity/coordinator.ts';

const MAX_BODY_BYTES = 950_000;
const METHODS = new Set(['initialize', 'checkpoint-load', 'checkpoint-save', 'seed', 'finalize', 'status', 'start']);
type Coordinator = Pick<FivetranFactCoordinator,
  'initializePipeline' | 'loadBulkBootstrapCheckpoint' | 'saveBulkBootstrapCheckpoint' |
  'seedBulkBootstrapScopes' | 'finalizeBulkBootstrap' | 'status' | 'start'>;

export interface FivetranAdminEnv {
  TENANT_ID: string;
  INGESTION_ENABLED?: string;
  FIVETRAN_FACTS: { getByName(name: string): Coordinator };
  IDENTITY: { getByName(name: string): Pick<IdentityCoordinator, 'status'> };
}

/** Called only after the main Worker has checked the admin token and POST method. */
export async function handleFivetranAdmin(request: Request, env: FivetranAdminEnv): Promise<Response> {
  const match = new URL(request.url).pathname.match(/^\/admin\/fivetran\/(stripe_main|stripe_kajabi|activecampaign)\/([a-z-]+)$/);
  if (!match || !METHODS.has(match[2])) return new Response('Not found', { status: 404 });
  const pipeline = match[1] as PipelineId;
  const method = match[2];
  let body: Record<string, unknown>;
  try {
    body = await readBody(request);
  } catch (error) {
    const tooLarge = error instanceof RangeError;
    return Response.json({ error: tooLarge ? 'Request exceeds the byte limit' : 'Expected a JSON object' }, { status: tooLarge ? 413 : 400 });
  }
  if (body.pipeline !== undefined && body.pipeline !== pipeline) {
    return Response.json({ error: 'Pipeline does not match the route' }, { status: 400 });
  }
  const coordinator = env.FIVETRAN_FACTS.getByName(`fivetran:${pipeline}`);
  switch (method) {
    case 'status':
      return Response.json(await coordinator.status(pipeline));
    case 'initialize':
      return Response.json(await coordinator.initializePipeline({ pipeline, snapshotId: text(body.snapshotId), snapshotAt: text(body.snapshotAt) }));
    case 'checkpoint-load':
      return Response.json(await coordinator.loadBulkBootstrapCheckpoint({ pipeline, snapshotId: text(body.snapshotId) }));
    case 'checkpoint-save':
      await coordinator.saveBulkBootstrapCheckpoint({ ...body, pipeline } as unknown as BulkBootstrapCheckpoint);
      return Response.json({ saved: true });
    case 'seed':
      if (!Array.isArray(body.prepared)) return Response.json({ error: 'Prepared scopes are required' }, { status: 400 });
      return Response.json(await coordinator.seedBulkBootstrapScopes({ pipeline, snapshotAt: text(body.snapshotAt), prepared: body.prepared as PreparedScope[] }));
    case 'finalize':
      return Response.json(await coordinator.finalizeBulkBootstrap({ pipeline, snapshotAt: text(body.snapshotAt) }));
    case 'start': {
      if (env.INGESTION_ENABLED !== 'true') return Response.json({ error: 'Ingestion is disabled' }, { status: 409 });
      const identity = await env.IDENTITY.getByName(env.TENANT_ID).status();
      if (!identity.baseline || identity.publishedVersion < 1) {
        return Response.json({ error: 'The combined identity baseline must be active first' }, { status: 409 });
      }
      if (body.activationId !== identity.baseline.sealHash) {
        return Response.json({ error: 'Activation must match the active identity baseline' }, { status: 409 });
      }
      return Response.json(await coordinator.start({ pipeline, snapshotId: text(body.snapshotId), activationId: identity.baseline.sealHash }));
    }
    default:
      return new Response('Not found', { status: 404 });
  }
}

async function readBody(request: Request): Promise<Record<string, unknown>> {
  if (Number(request.headers.get('Content-Length')) > MAX_BODY_BYTES) throw new RangeError('Request too large');
  const reader = request.body?.getReader();
  if (!reader) return {};
  const chunks: Uint8Array[] = [];
  let size = 0;
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    size += value.byteLength;
    if (size > MAX_BODY_BYTES) {
      await reader.cancel();
      throw new RangeError('Request too large');
    }
    chunks.push(value);
  }
  if (!size) return {};
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  const body: unknown = JSON.parse(new TextDecoder().decode(bytes));
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw new Error('Expected an object');
  return body as Record<string, unknown>;
}

function text(value: unknown): string {
  if (typeof value !== 'string' || !value) throw new Error('Required string is missing');
  return value;
}
