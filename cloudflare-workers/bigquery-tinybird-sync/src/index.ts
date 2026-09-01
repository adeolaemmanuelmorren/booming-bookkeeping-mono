import { handleRequest } from "./http";
import type { WorkerEnv } from "./sync";

export { TinybirdSyncGate } from "./tinybird-gate";
export { PublicationCoordinator } from "./publication-coordinator";
export { JourneyCoordinator } from "./journey-coordinator";

const SERVICE_NAME = "bigquery-tinybird-sync";

export default {
  async fetch(request: Request, env: WorkerEnv): Promise<Response> {
    return handleRequest(request, env);
  },

  async scheduled(
    controller: ScheduledController,
    env: WorkerEnv,
  ): Promise<void> {
    const coordinator = env.PUBLICATION_COORDINATOR.getByName(
      "bill-publication-coordinator",
    );
    const [result] = await Promise.all([
      coordinator.enqueueRawRun({
        scheduledAt: new Date(controller.scheduledTime).toISOString(),
      }),
      env.JOURNEY_COORDINATOR.getByName("boom").tick(),
    ]);
    logRun(result);
  },
};

function logRun(result: object): void {
  console.log(JSON.stringify({
    service: SERVICE_NAME,
    event: "scheduled_run_queued",
    ...result,
  }));
}
