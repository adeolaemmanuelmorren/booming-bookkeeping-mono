import { DurableObject } from "cloudflare:workers";

const SERVICE = "bigquery-tinybird-sync";
const RETIRED = "bigquery-tinybird-sync is retired. No jobs were accepted.";

function retiredResponse(): Response {
  return Response.json({ service: SERVICE, retired: true }, { status: 410 });
}

/** Preserve existing namespaces and stored jobs without importing their old runners. */
class RetiredObject extends DurableObject<unknown> {
  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      await ctx.storage.deleteAlarm();
    });
  }

  async alarm(): Promise<void> {
    await this.ctx.storage.deleteAlarm();
  }

  async fetch(): Promise<Response> {
    await this.ctx.storage.deleteAlarm();
    return retiredResponse();
  }

  async status() {
    await this.ctx.storage.deleteAlarm();
    return { service: SERVICE, retired: true, alarmAt: await this.ctx.storage.getAlarm() };
  }

  protected async rejectJob(): Promise<never> {
    await this.ctx.storage.deleteAlarm();
    throw new Error(RETIRED);
  }
}

export class TinybirdSyncGate extends RetiredObject {
  async trigger(_request?: unknown) { return this.rejectJob(); }
}

export class PublicationCoordinator extends RetiredObject {
  async enqueueRawRun(_input?: unknown) { return this.rejectJob(); }
  async tick() { return this.rejectJob(); }
  async configureBootstrap(_mode?: unknown) { return this.rejectJob(); }
  async recoverFailed(_action?: unknown) { return this.rejectJob(); }
  async setOperatorPaused(_paused?: unknown) { return this.rejectJob(); }
  async startJourneyBackfill() { return this.rejectJob(); }
  async compactRawBacklog() { return this.rejectJob(); }
  async recoverExpiredRawRunLease() { return this.rejectJob(); }
  async pauseAfterPublication() { return this.rejectJob(); }
}

export class JourneyCoordinator extends RetiredObject {
  async tick() { return this.rejectJob(); }
  async recoverFailed() { return this.rejectJob(); }
  async enqueueRepair(_input?: unknown) { return this.rejectJob(); }
}

export class ReportingFactsCoordinator extends RetiredObject {
  async tick() { return this.rejectJob(); }
  async recoverFailed() { return this.rejectJob(); }
  async replayServerFromSeed() { return this.rejectJob(); }
}

export default {
  async fetch(): Promise<Response> {
    return retiredResponse();
  },

  // Cron removal propagates separately. Any late delivery must remain harmless.
  async scheduled(): Promise<void> {},
};
