import { WorkerEntrypoint } from 'cloudflare:workers';
import { Tinybird, type TinybirdEnv } from '../storage/tinybird.ts';
import type { IdentityCoordinator } from '../identity/coordinator.ts';
import { publishSourceReplacement, publishSourceReplacements, type SourceReplacement } from './publication.ts';

interface Env extends TinybirdEnv {
  TENANT_ID: string;
  IDENTITY: DurableObjectNamespace<IdentityCoordinator>;
}

export class SourceReplacementPublisher extends WorkerEntrypoint<Env> {
  async publishSourceReplacements(replacements: SourceReplacement[]): Promise<void> {
    await publishSourceReplacements(this.env.TENANT_ID, replacements, new Tinybird(this.env),
      this.env.IDENTITY.getByName(this.env.TENANT_ID));
  }

  async publishSourceReplacement(replacement: SourceReplacement): Promise<void> {
    await publishSourceReplacement(this.env.TENANT_ID, replacement, new Tinybird(this.env),
      this.env.IDENTITY.getByName(this.env.TENANT_ID));
  }
}
