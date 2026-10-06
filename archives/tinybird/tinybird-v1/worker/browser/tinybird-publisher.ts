import { WorkerEntrypoint } from 'cloudflare:workers';
import { Tinybird, type TinybirdEnv } from '../storage/tinybird.ts';
import { TinybirdBrowserGroupPublisher, TinybirdBrowserSourcePublisher } from './tinybird-storage.ts';
import type { BrowserGroupCommit, RetainedBrowserFact } from './publication.ts';

interface Env extends TinybirdEnv { TENANT_ID: string }

export class BrowserSourcePublisher extends WorkerEntrypoint<Env> {
  appendSources(records: RetainedBrowserFact[]): Promise<void> {
    return new TinybirdBrowserSourcePublisher(new Tinybird(this.env), this.env.TENANT_ID).appendSources(records);
  }

  readSources(ids: string[]): Promise<RetainedBrowserFact[]> {
    return new TinybirdBrowserSourcePublisher(new Tinybird(this.env), this.env.TENANT_ID).readSources(ids);
  }
}

export class BrowserGroupPublisher extends WorkerEntrypoint<Env> {
  appendGroup(group: BrowserGroupCommit): Promise<void> {
    return new TinybirdBrowserGroupPublisher(new Tinybird(this.env), this.env.TENANT_ID).appendGroup(group);
  }

  readGroup(tenantId: string, groupId: string): Promise<BrowserGroupCommit | null> {
    return new TinybirdBrowserGroupPublisher(new Tinybird(this.env), this.env.TENANT_ID).readGroup(tenantId, groupId);
  }
}
