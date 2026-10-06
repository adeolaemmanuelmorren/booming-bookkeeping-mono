import { WorkerEntrypoint } from "cloudflare:workers";
import { VisitorSessions, type SessionEnv } from "../../../worker/sessions/visitor-sessions.ts";
import type { SnapshotManifest, SnapshotRecord } from "../../../worker/sessions/session-publication.ts";

export class TestVisitorSessions extends VisitorSessions {
  async runDueAlarm(): Promise<void> {
    this.ctx.storage.sql.exec("UPDATE session_outbox SET next_attempt_at = 0 WHERE lease_id IS NULL AND revision = (SELECT MIN(revision) FROM session_outbox)");
    await this.alarm();
  }
}

interface TestEnv extends SessionEnv {
  VISITORS: DurableObjectNamespace<TestVisitorSessions>;
  TEST_CONTROL: Fetcher;
}

/** This bridge talks only to the Node test fake, never to an external service. */
export class TestPublisher extends WorkerEntrypoint<TestEnv> {
  async appendRecords(records: SnapshotRecord[]): Promise<void> {
    await this.call("appendRecords", records);
  }
  async readRecords(manifest: SnapshotManifest): Promise<SnapshotRecord[]> {
    return this.call("readRecords", manifest) as Promise<SnapshotRecord[]>;
  }
  async appendCommit(manifest: SnapshotManifest): Promise<void> {
    await this.call("appendCommit", manifest);
  }
  async readCommit(manifest: SnapshotManifest): Promise<SnapshotManifest | null> {
    return this.call("readCommit", manifest) as Promise<SnapshotManifest | null>;
  }
  private async call(method: string, input: unknown): Promise<unknown> {
    const response = await this.env.TEST_CONTROL.fetch("https://test.invalid", {
      method: "POST", body: JSON.stringify({ method, input }),
    });
    if (!response.ok) throw new Error(await response.text());
    return response.json();
  }
}

export default {
  async fetch(request: Request, env: TestEnv): Promise<Response> {
    const url = new URL(request.url);
    const name = url.searchParams.get("name") ?? "visitor-1";
    const stub = env.VISITORS.getByName(name);
    try {
      if (url.pathname === "/receive") {
        const body = await request.json<{ visitor: string; revisions: Parameters<TestVisitorSessions["receive"]>[1] }>();
        return Response.json(await stub.receive(body.visitor, body.revisions));
      }
      if (url.pathname === "/run") {
        await stub.runDueAlarm();
        return Response.json(await stub.status());
      }
      return Response.json(await stub.status());
    } catch (error) {
      return new Response(error instanceof Error ? error.message : "Test call failed", { status: 400 });
    }
  },
};
