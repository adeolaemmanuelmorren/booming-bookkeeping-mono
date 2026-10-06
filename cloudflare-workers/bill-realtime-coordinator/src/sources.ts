import { SourceCoordinator as V1SourceCoordinator } from "../../../tinybird-v1/worker/sources/source-coordinator.ts";
import { SqliteSourceStore } from "../../../tinybird-v1/worker/sources/sqlite-source-store.ts";
import { canonicalJson, sha256 } from "../../../tinybird-v1/worker/storage/json.ts";
import { FETCHER_STORE_METHODS } from "./fetcher-store.ts";
import type { SourceRoute, StartBackfillOptions } from "../../../tinybird-v1/worker/sources/contracts.ts";

// Reuse the verified cursors and provider adapters with an explicit pause control
// for this deployment. The old V1 Worker remains untouched.
export class SourceCoordinator extends V1SourceCoordinator {
  async reconcileContacts(contactIds: string[]) {
    if (!Array.isArray(contactIds) || !contactIds.length || contactIds.length > 25 ||
      contactIds.some(id => typeof id !== "string" || !/^\d+$/.test(id))) {
      throw new Error("Expected 1 to 25 contact IDs.");
    }
    const store = new SqliteSourceStore(this.ctx.storage);
    store.bindRoute({ source: "activecampaign", account: "default" });
    const receivedAt = new Date().toISOString();
    const auditId = crypto.randomUUID();
    for (const contactId of new Set(contactIds)) {
      // Queue a fresh provider read. Never accept replacement facts from the caller.
      await store.putInboxIfAbsent({ id: `activecampaign:reconcile:${auditId}:${contactId}`,
        source: "activecampaign", sourceAccount: "default", kind: "dirty_contact", contactId,
        receivedAt, immutablePayload: { trigger: { kind: "manual_reconciliation" } } });
    }
    return { queued: new Set(contactIds).size };
  }

  async fetcher(route: SourceRoute, operation: string, lease: string, args: unknown[] = []) {
    const store = new SqliteSourceStore(this.ctx.storage);
    store.bindRoute(route);
    if (!/^[a-f0-9-]{36}$/.test(lease)) throw new Error("Invalid fetcher lease.");
    if (operation === "acquire") {
      const acquired = store.acquireLease(lease, Date.now(), Date.now() + 300_000);
      if (!acquired) return { acquired: false };
      // The Cloud Run scheduler takes over waking this same saved source state.
      await this.ctx.storage.put("realtime_paused", true);
      await this.ctx.storage.deleteAlarm();
      store.recordSliceStart(new Date().toISOString());
      return { acquired: true };
    }
    const assertLease = () => {
      const current = this.ctx.storage.sql.exec<{ lease_id: string; expires_at_ms: number }>(
        "SELECT lease_id, expires_at_ms FROM coordinator_lease WHERE id = 1").one();
      if (current.lease_id !== lease || current.expires_at_ms <= Date.now()) throw new Error("Fetcher lease expired.");
    };
    assertLease();
    if (operation === "release") {
      if (args[0] === true) store.recordSliceSuccess(new Date().toISOString());
      else store.recordSliceFailure("cloud_fetcher_incomplete", null);
      store.releaseLease(lease);
      return { released: true };
    }
    if (operation === "read") {
      const [path, parameters] = args as [string, Record<string, string>];
      if (typeof path !== "string" || !path.startsWith("/")) throw new Error("Invalid provider path.");
      return route.source === "stripe"
        ? this.env.STRIPE_SOURCE.read(route.account === "main" ? "stripe" : "stripe_kajabi", path, parameters)
        : this.env.ACTIVECAMPAIGN_SOURCE.read(path, parameters);
    }
    if (operation === "publish") {
      const contracts = args[0] as import("../../../tinybird-v1/worker/sources/contracts.ts").SourceReplacementContract[];
      if (!Array.isArray(contracts) || contracts.some(item => item.source !== route.source || item.source_account !== route.account)) {
        throw new Error("Fetcher source mismatch.");
      }
      await this.env.SOURCE_PUBLISHER.publishSourceReplacements!(contracts);
      return { accepted: true };
    }
    if (!FETCHER_STORE_METHODS.includes(operation as typeof FETCHER_STORE_METHODS[number])) {
      throw new Error("Unsupported fetcher operation.");
    }
    return this.ctx.blockConcurrencyWhile(async () => {
      assertLease();
      if (operation === "putInboxIfAbsent" && route.source === "activecampaign") {
        const record = args[0] as { id: string; immutablePayload: { trigger?: { kind?: string } } };
        if (record.immutablePayload?.trigger?.kind === "contact_update_poll") {
          // ActiveCampaign can return changed contact fields with the same
          // update timestamp. Preserve both observations and deduplicate exact retries.
          const hash = await sha256(canonicalJson(record.immutablePayload));
          args = [{ ...record, id: `${record.id}:${hash}` }];
        }
      }
      if (operation === "putInboxIfAbsent" && route.source === "stripe") {
        const record = args[0] as { id: string; kind: string; immutablePayload: unknown };
        if (record.kind === "stripe_event") {
          // Stripe can change delivery metadata on an existing event. Keep each
          // observation immutable; charge replacements still deduplicate payments.
          const hash = await sha256(canonicalJson(record.immutablePayload));
          args = [{ ...record, id: `${record.id}:${hash}` }];
        }
      }
      if (route.source === "activecampaign" && ["listPendingInbox", "listPendingOutbox"].includes(operation)) {
        // Fresh contacts should not wait behind the historical recovery queue.
        // The store still publishes each contact's versions in increasing order.
        args = [{ ...(args[0] as object), recentFirst: true }];
      }
      const method = store[operation as typeof FETCHER_STORE_METHODS[number]] as (...values: unknown[]) => Promise<unknown>;
      return await method.apply(store, args) ?? null;
    });
  }

  async status(route: SourceRoute) {
    const status = await super.status(route);
    const hydration = this.ctx.storage.sql.exec(`SELECT
      COALESCE(json_extract(progress_json, '$.bundleCursor.pendingTasks[0].kind'), 'initial') AS nextStep,
      COUNT(*) AS records
      FROM source_inbox WHERE status = 'pending' GROUP BY nextStep`).toArray();
    const cursorKey = route.source === "stripe" ? `stripe:${route.account}:events-cursor`
      : "activecampaign:updated-contacts-cursor";
    const cursor = this.ctx.storage.sql.exec<{ value_json: string }>(
      "SELECT value_json FROM source_cursors WHERE key = ?", cursorKey).toArray()[0];
    const completedThrough = cursor ? JSON.parse(cursor.value_json).completedThrough : null;
    const pending = this.ctx.storage.sql.exec(`SELECT priority, COUNT(*) records, MIN(received_at) oldestReceivedAt
      FROM source_inbox WHERE status = 'pending' GROUP BY priority`).toArray();
    return { ...status, paused: await this.ctx.storage.get("realtime_paused") !== false, hydration, pending,
      discoveredThrough: typeof completedThrough === "number" ? new Date(completedThrough * 1000).toISOString() : completedThrough };
  }
  async startBackfill(route: SourceRoute, options: StartBackfillOptions = {}) {
    await this.ctx.storage.put("realtime_paused", false);
    return super.startBackfill(route, options);
  }
  async wake(route: SourceRoute) {
    if (await this.ctx.storage.get("realtime_paused") === false) return super.wake(route);
    return super.status(route);
  }
  async alarm() {
    if (await this.ctx.storage.get("realtime_paused") !== false) return;
    await super.alarm();
  }
  async pause() {
    await this.ctx.storage.put("realtime_paused", true);
    await this.ctx.storage.deleteAlarm();
    return { paused: true };
  }
}
