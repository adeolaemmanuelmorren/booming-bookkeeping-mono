import { WorkerEntrypoint } from 'cloudflare:workers';
import { BootstrapBaselineReader, type LogicalKey } from './baseline.ts';
import { readConfiguredBaseline, type BootstrapBaselineEnv } from './configured-baseline.ts';

/** Private read-only service used by the browser router and visitor session objects. */
export class BootstrapBaseline extends WorkerEntrypoint<BootstrapBaselineEnv> {
  async loadSourceHeads(keys: LogicalKey[]) {
    return (await this.reader()).loadSourceHeads(keys);
  }

  async loadMembers(visitorKeys: string[]) {
    return (await this.reader()).loadMembers(visitorKeys);
  }

  async loadVisitor(tenantId: string, visitorKey: string) {
    return (await this.reader()).loadVisitor(tenantId, visitorKey);
  }

  private async reader() {
    const { store, receipt } = await readConfiguredBaseline(this.env);
    return new BootstrapBaselineReader(receipt.tenantId, receipt.sourceSeal, store.baselineStore());
  }
}
