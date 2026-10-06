import { WorkerEntrypoint } from "cloudflare:workers";

import type { SourceReplacement } from "../../../worker/fivetran/contracts.ts";
import {
  FivetranFactCoordinator,
  type FivetranFactCoordinatorEnv,
} from "../../../worker/fivetran/runtime-coordinator.ts";

export class TestFivetranFactCoordinator extends FivetranFactCoordinator {
  async runAlarm(): Promise<void> {
    await this.alarm();
  }

  async clearAlarmForTest(): Promise<void> {
    await this.ctx.storage.deleteAlarm();
  }
}

interface TestEnv extends FivetranFactCoordinatorEnv {
  FACTS: DurableObjectNamespace<TestFivetranFactCoordinator>;
  TEST_CONTROL: Fetcher;
}

export class TestSourcePublisher extends WorkerEntrypoint<TestEnv> {
  async publishSourceReplacements(
    replacements: SourceReplacement[],
  ): Promise<void> {
    const response = await this.env.TEST_CONTROL.fetch("https://test.invalid", {
      method: "POST",
      body: JSON.stringify(replacements),
    });
    if (!response.ok) throw new Error("Test source publisher rejected a batch");
  }
}

export default {
  async fetch(request: Request, env: TestEnv): Promise<Response> {
    const url = new URL(request.url);
    const pipeline = url.searchParams.get("pipeline") ?? "activecampaign";
    const stub = env.FACTS.getByName(`fivetran:${pipeline}`);

    try {
      const body = request.method === "POST" ? await request.json() : undefined;
      if (url.pathname === "/initialize") {
        return Response.json(await stub.initializePipeline(body));
      }
      if (url.pathname === "/checkpoint-save") {
        await stub.saveBulkBootstrapCheckpoint(body);
        return Response.json({ saved: true });
      }
      if (url.pathname === "/checkpoint-load") {
        return Response.json(await stub.loadBulkBootstrapCheckpoint(body));
      }
      if (url.pathname === "/seed") {
        return Response.json(await stub.seedBulkBootstrapScopes(body));
      }
      if (url.pathname === "/finalize") {
        return Response.json(await stub.finalizeBulkBootstrap(body));
      }
      if (url.pathname === "/start") {
        return Response.json(await stub.start(body));
      }
      if (url.pathname === "/run") {
        await stub.runAlarm();
      }
      if (url.pathname === "/clear-alarm") {
        await stub.clearAlarmForTest();
        return Response.json({ cleared: true });
      }
      return Response.json(await stub.status(pipeline));
    } catch (error) {
      return new Response(
        error instanceof Error ? error.message : "Test coordinator call failed",
        { status: 400 },
      );
    }
  },
};
