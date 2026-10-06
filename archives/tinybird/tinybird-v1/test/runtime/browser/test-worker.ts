import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import { BrowserRouter, visitorObjectName } from "../../../worker/browser/router.ts";
import { VisitorSessions, type PrepareSessionGroup } from "../../../worker/sessions/visitor-sessions.ts";
import { normalizeHistorical, normalizeJitsu } from "../../../worker/browser/normalize.ts";
import { readCompletePrefix, readSnapshotPlan, snapshotMember } from "../../../worker/browser/publication.ts";
import { applyPageRevisions, pagesForVisitor } from "../../../worker/sessions/page-revisions.ts";
import { buildSessions, canonicalJson } from "../../../worker/sessions/session-engine.ts";
import { createSnapshot, publishSnapshot, type SessionSnapshot } from "../../../worker/sessions/session-publication.ts";
import { sha256 } from "../../../worker/storage/json.ts";
import { browserBatches } from "../../../worker/browser/batches.ts";

export class TestRouter extends BrowserRouter {
  async run() {
    this.ctx.storage.sql.exec("UPDATE browser_outbox SET next_attempt = 0 WHERE sequence = (SELECT MIN(sequence) FROM browser_outbox)");
    await this.alarm();
    return this.status();
  }
  async expireLease() { this.ctx.storage.sql.exec("UPDATE browser_outbox SET lease_until = 0, next_attempt = 0"); }
  async orphanLease() {
    this.ctx.storage.sql.exec("UPDATE browser_outbox SET lease_id = 'interrupted', lease_until = ?, next_attempt = 0 WHERE sequence = (SELECT MIN(sequence) FROM browser_outbox)", Date.now() + 3600000);
    await this.ctx.storage.deleteAlarm();
  }
  async heads() { return this.ctx.storage.sql.exec("SELECT * FROM browser_heads").toArray(); }
  async byteSizes() {
    const max = (table: string, column: string) => this.ctx.storage.sql.exec<{ bytes: number }>(`SELECT COALESCE(MAX(length(CAST(${column} AS BLOB))), 0) AS bytes FROM ${table}`).one().bytes;
    return { outboxChunk: max("browser_outbox_chunks", "payload"), logicalHead: max("browser_heads", "event_json"), sourceFingerprint: max("browser_source_heads", "fingerprint") };
  }
}

export class TestVisitor extends VisitorSessions {
  override async prepareGroup(input: PrepareSessionGroup) {
    const control = this.env as unknown as { TEST_CONTROL: Fetcher };
    await remote(control, "beforePrepare", input);
    const result = await super.prepareGroup(input);
    await remote(control, "afterPrepare", input);
    return result;
  }
  async byteSizes() {
    return this.ctx.storage.sql.exec("SELECT COUNT(*) AS chunks, COALESCE(MAX(length(CAST(payload AS BLOB))), 0) AS max_bytes FROM prepared_session_chunks").one();
  }
}

type TestEnv = { TEST_CONTROL: Fetcher };
export class TestIdentity extends DurableObject<TestEnv> {
  async enqueue(input: unknown) { return remote(this.env, "identity", input); }
}
export class TestPublisher extends WorkerEntrypoint<TestEnv> {
  async publishSnapshots(snapshots: SessionSnapshot[]) {
    await remote(this.env, "sessionBatch", { count: snapshots.length });
    for (const snapshot of snapshots) await publishSnapshot(snapshot, this);
  }
  async appendSources(input: unknown) { await remote(this.env, "appendSources", input); }
  async readSources(input: unknown) { return remote(this.env, "readSources", input); }
  async appendRecords(input: unknown) { await remote(this.env, "appendRecords", input); }
  async readRecords(input: unknown) { return remote(this.env, "readRecords", input); }
  async appendCommit(input: unknown) { await remote(this.env, "appendCommit", input); }
  async readCommit(input: unknown) { return remote(this.env, "readCommit", input); }
  async appendGroup(input: unknown) { await remote(this.env, "appendGroup", input); }
  async readGroup(tenantId: string, groupId: string) { return remote(this.env, "readGroup", { tenantId, groupId }); }
  async loadSourceHeads(input: unknown) { return remote(this.env, "loadSourceHeads", input); }
  async loadMembers(input: unknown) { return remote(this.env, "loadMembers", input); }
  async loadVisitor(tenantId: string, visitorKey: string) { return remote(this.env, "loadVisitor", { tenantId, visitorKey }); }
}

async function remote(env: TestEnv, method: string, input: unknown): Promise<any> {
  const response = await env.TEST_CONTROL.fetch("https://local-control.invalid", { method: "POST", body: JSON.stringify({ method, input }) });
  if (!response.ok) throw new Error(await response.text());
  return response.json();
}

export default {
  async fetch(request: Request, env: TestEnv & { ROUTER: DurableObjectNamespace<TestRouter>; VISITORS: DurableObjectNamespace<TestVisitor> }) {
    const url = new URL(request.url);
    const body = request.method === "POST" ? await request.json() as any : null;
    const router = env.ROUTER.getByName("boom");
    try {
      if (url.pathname === "/receive") {
        const events = [];
        for (const input of body) events.push(input.jitsu ? await normalizeJitsu(input.jitsu) : await normalizeHistorical(input));
        return Response.json(await router.receive(events));
      }
      if (url.pathname === "/run") return Response.json(await router.run());
      if (url.pathname === "/expire") { await router.expireLease(); return Response.json(null); }
      if (url.pathname === "/orphan") { await router.orphanLease(); return Response.json(null); }
      if (url.pathname === "/status") return Response.json(await router.status());
      if (url.pathname === "/heads") return Response.json(await router.heads());
      if (url.pathname === "/sizes") return Response.json(await router.byteSizes());
      if (url.pathname === "/plan") return Response.json(await router.readPlan(body));
      if (url.pathname === "/read-plan") return Response.json(await readSnapshotPlan(body.plan, body.records));
      if (url.pathname === "/split") {
        const events = [];
        for (const item of body) events.push(await normalizeHistorical(item));
        return Response.json(browserBatches(events).map(batch => ({ bytes: new TextEncoder().encode(JSON.stringify(batch)).byteLength, ids: batch.map(event => event.source.source_record_id) })));
      }
      if (url.pathname === "/visible") return Response.json(await readCompletePrefix("boom", body.groups, body.records));
      if (url.pathname === "/make-seed") {
        const events = [];
        for (const item of body.inputs) events.push(await normalizeHistorical(item));
        const heads = applyPageRevisions(new Map(), events.flatMap(event => event.pageRevision ? [event.pageRevision] : [])).heads;
        const snapshot = createSnapshot("boom", body.visitor, "1", buildSessions(body.visitor, pagesForVisitor(heads, body.visitor)));
        const member = await snapshotMember(snapshot);
        const records = [];
        for (const session of snapshot.sessions) {
          const payload = canonicalJson(session);
          records.push({ tenant_id: "boom", visitor_key: body.visitor, revision: "1", publication_id: snapshot.publication_id, session_id: session.session_id, payload_json: payload, payload_hash: await sha256(payload) });
        }
        return Response.json({ seed: { tenantId: "boom", visitorKey: body.visitor, heads: [...heads.values()], snapshot }, events, member, records });
      }
      if (url.pathname === "/direct-receive") {
        const visitor = env.VISITORS.getByName(visitorObjectName("boom", body.visitor));
        return Response.json(await visitor.receive(body.visitor, body.revisions));
      }
      if (url.pathname === "/prepare") {
        const visitor = env.VISITORS.getByName(visitorObjectName("boom", body.visitorKey));
        return Response.json(await visitor.prepareGroup(body));
      }
      if (url.pathname === "/visitor-sizes") return Response.json(await env.VISITORS.getByName(visitorObjectName("boom", body.visitor)).byteSizes());
      return new Response("Missing test route", { status: 404 });
    } catch (error) {
      return Response.json({ error: error instanceof Error ? error.message : String(error) }, { status: 409 });
    }
  },
};
