import {
  SourceExportCoordinator,
  transportContract,
  type FailedExportRecoveryRequest,
  type Env,
} from "./coordinator";
export {
  TinybirdSyncGate,
  PublicationCoordinator,
  JourneyCoordinator,
  ReportingFactsCoordinator,
} from "./retired";

export { SourceExportCoordinator };

interface WorkerEnv extends Env {
  ADMIN_TOKEN: string;
  INGESTION_ENABLED?: string;
  SOURCE_EXPORT_COORDINATOR: DurableObjectNamespace<SourceExportCoordinator>;
}

export default {
  async scheduled(
    _controller: ScheduledController,
    env: WorkerEnv,
  ): Promise<void> {
    if (env.INGESTION_ENABLED !== "true") return;
    const coordinator =
      env.SOURCE_EXPORT_COORDINATOR.getByName("v1-source-export");
    await coordinator.start();
  },
  async fetch(request: Request, env: WorkerEnv): Promise<Response> {
    if (!env.ADMIN_TOKEN || request.headers.get("Authorization") !== `Bearer ${env.ADMIN_TOKEN}`) {
      return new Response("Unauthorized", { status: 401 });
    }

    const path = new URL(request.url).pathname;
    if (request.method === "GET" && path === "/admin/source-export/contract") {
      return Response.json({ sources: transportContract() });
    }

    const coordinator =
      env.SOURCE_EXPORT_COORDINATOR.getByName("v1-source-export");
    if (request.method === "GET" && path === "/admin/source-export/status") {
      return Response.json(await coordinator.status());
    }
    if (request.method === "POST" && path === "/admin/source-export/start") {
      if (env.INGESTION_ENABLED !== "true") {
        return Response.json({ error: "Ingestion is disabled" }, { status: 409 });
      }
      return Response.json(await coordinator.start());
    }
    if (
      request.method === "POST" &&
      path === "/admin/source-export/recover-failed-export"
    ) {
      if (env.INGESTION_ENABLED !== "false") {
        return Response.json({ error: "Ingestion must be disabled" }, { status: 409 });
      }
      const body = (await request.json()) as FailedExportRecoveryRequest;
      return Response.json(await coordinator.recoverFailedExport(body));
    }

    return new Response("Not found", { status: 404 });
  },
};
