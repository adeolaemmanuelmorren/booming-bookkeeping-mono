interface PipelineIdentity { pipeline: 'stripe_main' | 'stripe_kajabi' | 'activecampaign'; snapshotId: string; snapshotAt: string }
interface IdentityFivetranCoordinator {
  initializeIdentityPipeline(input: PipelineIdentity): Promise<unknown>;
  status(pipeline: PipelineIdentity['pipeline']): Promise<unknown>;
  start(input: { pipeline: PipelineIdentity['pipeline']; snapshotId: string; activationId: string }): Promise<unknown>;
}
export interface IdentityFivetranAdminEnv {
  TENANT_ID: string;
  FIVETRAN_SNAPSHOT_AT: string;
  IDENTITY_BASELINE_ID?: string;
  IDENTITY_INGESTION_ENABLED?: string;
  FIVETRAN_FACTS: { getByName(name: string): IdentityFivetranCoordinator };
  IDENTITY: { getByName(name: string): { status(): Promise<{
    publishedVersion: number;
    baseline: { baselineId: string; sealHash: string } | null;
  }> } };
}

/** The parent has already verified the operator token and POST method. */
export async function handleIdentityFivetranAdmin(request: Request, env: IdentityFivetranAdminEnv): Promise<Response> {
  const match = new URL(request.url).pathname.match(/^\/admin\/identity\/fivetran\/(stripe_main|stripe_kajabi|activecampaign)\/(initialize|status|start)$/);
  if (!match) return new Response('Not found', { status: 404 });
  const pipeline = match[1] as PipelineIdentity['pipeline'];
  // The stopped conversion bootstrap has separate durable instances and remains untouched.
  const coordinator = env.FIVETRAN_FACTS.getByName(`identity-fivetran:${pipeline}`);
  if (match[2] === 'status') return Response.json(await coordinator.status(pipeline));
  const identity = await env.IDENTITY.getByName(env.TENANT_ID).status();
  if (!env.IDENTITY_BASELINE_ID || identity.publishedVersion < 1 || !identity.baseline
    || identity.baseline.baselineId !== env.IDENTITY_BASELINE_ID) {
    return Response.json({ error: 'The configured identity baseline must be active first' }, { status: 409 });
  }
  const input = { pipeline, snapshotId: env.IDENTITY_BASELINE_ID, snapshotAt: env.FIVETRAN_SNAPSHOT_AT };
  if (match[2] === 'initialize') return Response.json(await coordinator.initializeIdentityPipeline(input));
  if (env.IDENTITY_INGESTION_ENABLED !== 'true') return Response.json({ error: 'Identity ingestion is disabled' }, { status: 409 });
  return Response.json(await coordinator.start({ pipeline, snapshotId: input.snapshotId, activationId: identity.baseline.sealHash }));
}
