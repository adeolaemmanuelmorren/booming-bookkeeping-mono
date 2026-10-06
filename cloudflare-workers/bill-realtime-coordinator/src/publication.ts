import { createHash } from "node:crypto";
import { IdentityExport, type ExportEnv } from './identity-export.ts';
import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import { computeIdentityBatch, type IdentityStateReader } from "../../../tinybird-v1/worker/identity/state.ts";
import { identityRecord } from "../../../tinybird-v1/worker/identity/storage.ts";
import { canonicalJson, sha256 } from "../../../tinybird-v1/worker/storage/json.ts";
import { evidenceKeys } from "../../../tinybird-v1/worker/browser/identity.ts";
import { compactSourceHead, replacementFacts, scopeKey, validateReplacement, fingerprint, compareVersion, type SourceReplacement } from "./replacements.ts";

export interface PublicationEnv extends ExportEnv { PUBLICATION: DurableObjectNamespace<Publication>; PUBLICATION_NAME?: string; PUBLICATION_WRITES_PAUSED?: string }
type SqlRow = Record<string, SqlStorageValue>;
type QueueRow = SqlRow & { sequence: number; payload: string };
type Job = SqlRow & { version: number; batch_id: string; baseline_id: string;
  cutoff: number; created_at: string; lease: string; lease_until: number; manifest: string | null };

function alreadyRetracted(prior: { factDeleted: boolean; sourcePriority?: number; sourceFactVersion: number } | undefined,
  incoming: { factDeleted: boolean; sourcePriority?: number; sourceFactVersion: number }) {
  if (!prior?.factDeleted || !incoming.factDeleted) return false;
  if ((prior.sourcePriority ?? 0) !== (incoming.sourcePriority ?? 0)) return false;
  return prior.sourceFactVersion >= incoming.sourceFactVersion;
}
export type Manifest = {
  schemaVersion: 1; tenantId: string; batchId: string; version: number; previousVersion: number;
  baselineId: string; createdAt: string; rowCount: number; contentHash: string;
  sourceCount: number; identityCount: number; historyComplete: boolean;
};

// One writer per tenant graph. Source coordinators retain their own independent
// cursors; this queue only serializes identity changes and warehouse publication.
export class Publication extends DurableObject<PublicationEnv> {
  private identityExport: IdentityExport;
  constructor(ctx: DurableObjectState, env: PublicationEnv) {
    super(ctx, env);
    this.identityExport = new IdentityExport(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      this.identityExport.initialize();
      if (!ctx.id.equals(env.PUBLICATION.idFromName("boom"))) ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS recovery_parent (singleton INTEGER PRIMARY KEY CHECK(singleton=1),name TEXT,version INTEGER)");
      ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS source_inbox (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT, replacement_id TEXT NOT NULL UNIQUE,
        fingerprint TEXT NOT NULL, payload TEXT NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0)`);
      if (!ctx.storage.sql.exec("PRAGMA table_info(source_inbox)").toArray().some(row => row.name === "round_member")) {
        ctx.storage.sql.exec("ALTER TABLE source_inbox ADD COLUMN round_member INTEGER NOT NULL DEFAULT 0");
      }
      ctx.storage.sql.exec(`CREATE INDEX IF NOT EXISTS source_inbox_pending
        ON source_inbox(acknowledged, round_member, sequence)`);
      ctx.storage.sql.exec(`CREATE INDEX IF NOT EXISTS source_inbox_pending_provider
        ON source_inbox(json_extract(payload, '$.source'), json_extract(payload, '$.source_account'))
        WHERE acknowledged = 0`);
      ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS publication_round (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), baseline_id TEXT NOT NULL)`);
      ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS identity_state (
        state_kind TEXT NOT NULL, state_key TEXT NOT NULL, lookup_key TEXT NOT NULL,
        is_deleted INTEGER NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(state_kind, state_key))`);
      ctx.storage.sql.exec(`CREATE INDEX IF NOT EXISTS identity_lookup ON identity_state(state_kind, lookup_key)`);
      ctx.storage.sql.exec(`CREATE INDEX IF NOT EXISTS identity_active_lookup
        ON identity_state(state_kind, lookup_key) WHERE is_deleted = 0`);
      ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS source_heads (scope_key TEXT PRIMARY KEY, payload TEXT NOT NULL)`);
      ctx.storage.sql.exec(`CREATE INDEX IF NOT EXISTS source_heads_contact
        ON source_heads(json_extract(payload, '$.source'), json_extract(payload, '$.scope_id'))`);
      ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS publication_meta (singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL)`);
      ctx.storage.sql.exec(`INSERT OR IGNORE INTO publication_meta VALUES (1, 0)`);
      ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS publication_job (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL,
        batch_id TEXT NOT NULL, baseline_id TEXT NOT NULL, cutoff INTEGER NOT NULL,
        created_at TEXT NOT NULL, lease TEXT NOT NULL, lease_until INTEGER NOT NULL, manifest TEXT)`);
      // Preserve an in-flight pre-migration manifest and its acknowledgement.
      if (this.job() && !this.round()) this.beginRound(this.job()!.baseline_id);
      ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS publication_rows (position INTEGER PRIMARY KEY, row_text TEXT NOT NULL)`);
      ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS publication_receipts (batch_id TEXT PRIMARY KEY, manifest TEXT NOT NULL)`);
      ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS history_checked (kind TEXT, value TEXT, PRIMARY KEY(kind, value));
        CREATE TABLE IF NOT EXISTS history_root (singleton INTEGER PRIMARY KEY CHECK(singleton=1), baseline_id TEXT NOT NULL);`);
      ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS history_schema (version INTEGER PRIMARY KEY)");
      if (!ctx.storage.sql.exec("PRAGMA table_info(history_root)").toArray().some(row => row.name === "snapshot_time")) {
        ctx.storage.sql.exec("ALTER TABLE history_root ADD COLUMN snapshot_time TEXT NOT NULL DEFAULT '1970-01-01T00:00:00.000Z'");
      }
      if (!ctx.storage.sql.exec("SELECT 1 FROM history_schema WHERE version = 2").toArray().length) {
        ctx.storage.sql.exec("DELETE FROM history_checked WHERE kind = 'scope'");
        ctx.storage.sql.exec("INSERT INTO history_schema VALUES (2)");
      }
    });
  }

  recoveryCheckpoint() {
    if (this.job()) throw new Error("Cannot migrate an active publication job");
    const root = this.ctx.storage.sql.exec("SELECT * FROM history_root").toArray()[0];
    const receipt = this.ctx.storage.sql.exec<{batch_id:string;manifest:string}>(
      "SELECT * FROM publication_receipts WHERE json_extract(manifest,'$.version')=?",this.version()).toArray()[0];
    return { version: this.version(), root, receipt };
  }

  recoveryExportPage(afterOld = "", afterProfile = "") {
    const status = this.identityExport.status();
    if (Number(status.pending.rows) || Number(status.pending.batches)) throw new Error("Parent export is not drained");
    const rows = this.ctx.storage.sql.exec<{old_id:string;profile_id:string}>(
      "SELECT old_id,profile_id FROM identity_tb_history WHERE (old_id,profile_id) > (?,?) ORDER BY old_id,profile_id LIMIT 500",
      afterOld,afterProfile).toArray();
    return {rows,status};
  }

  async importRecoveryExport(afterOld = "", afterProfile = "") {
    const parent = this.parent();
    if (!parent) throw new Error("Recovery parent is not configured");
    const {rows,status} = await parent.recoveryExportPage(afterOld,afterProfile);
    this.ctx.storage.transactionSync(() => {
      for (const row of rows) this.ctx.storage.sql.exec("INSERT OR IGNORE INTO identity_tb_history VALUES(?,?)",row.old_id,row.profile_id);
      if (!rows.length && status.enabled) {
        this.ctx.storage.sql.exec("INSERT OR IGNORE INTO identity_tb_config VALUES(1,1,?,?,NULL)",status.seedVersion,status.exportedVersion);
      }
    });
    return {count:rows.length,next:rows.at(-1) ?? null,complete:!rows.length};
  }

  recoveryDigest() {
    const hash = createHash("sha256");
    for (const row of this.ctx.storage.sql.exec<{replacement_id:string;payload:string}>(
      "SELECT replacement_id,payload FROM source_inbox WHERE acknowledged=0 ORDER BY replacement_id")) hash.update(row.replacement_id + "\n" + row.payload + "\n");
    return {version:this.version(),pendingHash:hash.digest("hex")};
  }

  recoveryPending(after = 0) {
    return this.ctx.storage.sql.exec<{sequence:number;payload:string}>(
      "SELECT sequence,payload FROM source_inbox WHERE acknowledged=0 AND sequence>? ORDER BY sequence LIMIT 10",after).toArray();
  }

  async seedRecovery() {
    if (this.version() || this.ctx.storage.sql.exec("SELECT 1 FROM source_inbox LIMIT 1").toArray().length) throw new Error("Recovery target is not empty");
    const checkpoint = await this.env.PUBLICATION.getByName("boom").recoveryCheckpoint();
    if (!checkpoint.root || !checkpoint.receipt) throw new Error("Missing recovery checkpoint");
    this.ctx.storage.transactionSync(() => {
      if (this.version()) throw new Error("Recovery was already seeded");
      this.ctx.storage.sql.exec("INSERT INTO recovery_parent VALUES(1,'boom',?)",checkpoint.version);
      this.ctx.storage.sql.exec("UPDATE publication_meta SET version=?",checkpoint.version);
      this.ctx.storage.sql.exec("INSERT INTO history_root VALUES(1,?,?)",checkpoint.root.baseline_id,checkpoint.root.snapshot_time);
      this.ctx.storage.sql.exec("INSERT INTO publication_receipts VALUES(?,?)",checkpoint.receipt.batch_id,checkpoint.receipt.manifest);
    });
    return {version:checkpoint.version};
  }

  async importRecoveryPending(after = 0) {
    const parent = this.parent();
    if (!parent) throw new Error("Recovery parent is not configured");
    const checkpoint = await parent.recoveryCheckpoint();
    const expected = this.ctx.storage.sql.exec<{version:number}>("SELECT version FROM recovery_parent").one().version;
    if (checkpoint.version !== expected) throw new Error("Recovery parent changed");
    const rows = await parent.recoveryPending(after);
    if (rows.length) await this.enqueue(rows.map(row => JSON.parse(row.payload)), true);
    return {count:rows.length,next:rows.at(-1)?.sequence ?? null};
  }

  private parent() {
    if (this.ctx.id.equals(this.env.PUBLICATION.idFromName("boom"))) return null;
    const row = this.ctx.storage.sql.exec<{name:string}>("SELECT name FROM recovery_parent").toArray()[0];
    return row ? this.env.PUBLICATION.getByName(row.name) : null;
  }

  async readIdentityRecords(kind: string, keys: string[]) {
    if (!["fact","mapping","profile","evidence"].includes(kind) || keys.length > 500) throw new Error("Invalid identity read");
    const local = this.ctx.storage.sql.exec<{state_kind:string;state_key:string;lookup_key:string;is_deleted:number;payload:string}>(
      "SELECT * FROM identity_state WHERE state_kind=? AND lookup_key IN (SELECT value FROM json_each(?))",kind,JSON.stringify(keys)).toArray();
    const parent = this.parent();
    if (!parent) return local;
    const merged = new Map((await parent.readIdentityRecords(kind,keys)).map(row => [row.state_key,row]));
    for (const row of local) merged.set(row.state_key,row);
    return [...merged.values()];
  }

  async readSourceHead(key: string): Promise<SourceReplacement | null> {
    const row = this.ctx.storage.sql.exec<{payload:string}>("SELECT payload FROM source_heads WHERE scope_key=?",key).toArray()[0];
    if (row) return compactSourceHead(JSON.parse(row.payload));
    return this.parent()?.readSourceHead(key) ?? null;
  }

  priorFingerprints(ids: string[]) {
    if (ids.length > 25) throw new Error("Too many retry IDs");
    return this.ctx.storage.sql.exec<{replacement_id:string;fingerprint:string}>(
      "SELECT replacement_id,fingerprint FROM source_inbox WHERE replacement_id IN (SELECT value FROM json_each(?))",JSON.stringify(ids)).toArray();
  }

  storageUsage() {
    const tables = ["source_inbox", "source_heads", "identity_state", "publication_rows", "publication_receipts", "history_checked", "identity_tb_rows"];
    const usage = tables.map(table => {
      const columns = this.ctx.storage.sql.exec<{name: string}>(`PRAGMA table_info(${table})`).toArray();
      const size = columns.map(column => `coalesce(length(${column.name}),0)`).join("+");
      return { table, ...this.ctx.storage.sql.exec(`SELECT count(*) total_rows FROM ${table}`).one(), ...this.ctx.storage.sql.exec(`SELECT count(*) sampled_rows, sum(${size}) sampled_bytes FROM (SELECT * FROM ${table} LIMIT 100)`).one() };
    });
    const sourceHeads = this.ctx.storage.sql.exec(`SELECT json_extract(payload, '$.source') source, count(*) rows
      FROM source_heads GROUP BY json_extract(payload, '$.source')`).toArray();
    return { usage, sourceHeads, export: this.identityExport.status() };
  }

  async compactSourceHeads(after: string = "") {
    if (typeof after !== "string" || after.length > 2048) throw new Error("Invalid compaction cursor");
    const rows = this.ctx.storage.sql.exec<{scope_key: string; payload: string}>(
      "SELECT scope_key,payload FROM source_heads WHERE scope_key > ? ORDER BY scope_key LIMIT 100", after).toArray();
    let savedBytes = 0;
    let changed = 0;
    for (const row of rows) {
      const value = JSON.parse(row.payload);
      if (value.source === "history" || "storedIdentityFacts" in value) continue;
      const compact = canonicalJson(await compactSourceHead(value));
      if (compact.length >= row.payload.length) continue;
      const update = this.ctx.storage.sql.exec("UPDATE source_heads SET payload=? WHERE scope_key=? AND payload=?", compact,row.scope_key,row.payload);
      if (update.rowsWritten) { changed++; savedBytes += row.payload.length - compact.length; }
    }
    return { scanned: rows.length, changed, savedBytes, next: rows.at(-1)?.scope_key ?? null };
  }

  compactAcknowledged() {
    // Keep IDs and fingerprint hashes for retry detection. The warehouse has
    // already committed these payloads; pending batches remain untouched.
    const rows = this.ctx.storage.sql.exec<{sequence: number; fingerprint: string}>(
      "SELECT sequence, fingerprint FROM source_inbox WHERE acknowledged = 1 AND payload != '{}' LIMIT 500").toArray();
    this.ctx.storage.transactionSync(() => {
      for (const row of rows) {
        const digest = /^[a-f0-9]{64}$/.test(row.fingerprint) ? row.fingerprint
          : createHash("sha256").update(row.fingerprint).digest("hex");
        this.ctx.storage.sql.exec("UPDATE source_inbox SET fingerprint = ?, payload = '{}' WHERE sequence = ? AND acknowledged = 1", digest, row.sequence);
      }
      // Frozen history uses unique batch scopes and is never a live source head.
      this.ctx.storage.sql.exec(`DELETE FROM source_heads WHERE scope_key IN (
        SELECT scope_key FROM source_heads WHERE json_extract(payload, '$.source') = 'history' LIMIT 500)`);
    });
    return { compacted: rows.length };
  }

  async startIdentityExport() { return this.identityExport.start(this.version()); }
  identityExportStatus() { return this.identityExport.status(); }
  async alarm() { await this.identityExport.alarm(); }

  async enqueue(contracts: SourceReplacement[], recoveryImport = false) {
    if (this.env.PUBLICATION_WRITES_PAUSED === "true" && !recoveryImport) throw new Error("Publication migration in progress; retry later");
    if (!contracts.length || contracts.length > 25) throw new Error("Expected 1 to 25 replacements.");
    for (const contract of contracts) validateReplacement(contract);
    const entries = contracts.map((contract) => ({ contract, text: canonicalJson(contract), digest: createHash("sha256").update(fingerprint(contract)).digest("hex") }));
    const parent = recoveryImport ? null : this.parent();
    const prior = parent ? await parent.priorFingerprints(contracts.map(contract => contract.replacement_id)) : [];
    return this.ctx.storage.transactionSync(() => {
      let count = 0;
      for (const { contract, text, digest } of entries) {
        const previous = this.ctx.storage.sql.exec<SqlRow & { fingerprint: string }>(
          "SELECT fingerprint FROM source_inbox WHERE replacement_id = ?", contract.replacement_id).toArray()[0] ?? prior.find(row => row.replacement_id === contract.replacement_id);
        if (previous) {
          if (previous.fingerprint !== digest && previous.fingerprint !== fingerprint(contract)) throw new Error("Conflicting source retry.");
          continue;
        }
        const pending = this.ctx.storage.sql.exec<SqlRow & { n: number }>(`SELECT COUNT(*) n FROM source_inbox INDEXED BY source_inbox_pending_provider
          WHERE acknowledged = 0 ${contract.source === "history" ? "" : "AND json_extract(payload, '$.source') != 'history'"}`).one().n;
        if (pending >= (contract.source === "history" ? 5000 : 500)) throw new Error("Publication backlog is full; retry after warehouse acknowledgement.");
        // Reserve room for each provider so browser traffic cannot starve
        // payment or contact updates while the receiver drains the queue.
        if (contract.source !== "history") {
          const sourcePending = this.ctx.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) n FROM source_inbox INDEXED BY source_inbox_pending_provider
            WHERE acknowledged = 0 AND json_extract(payload, '$.source') = ?
            AND json_extract(payload, '$.source_account') = ?`, contract.source, contract.source_account).one().n;
          const limit = contract.source === "browser" ? 200 : 100;
          if (sourcePending >= limit) throw new Error("Source publication backlog is full; retry after warehouse acknowledgement.");
        }
        this.ctx.storage.sql.exec("INSERT INTO source_inbox(replacement_id, fingerprint, payload, round_member) VALUES (?, ?, ?, ?)",
          contract.replacement_id, digest, text, contract.source === "history" && this.round() ? 1 : 0);
        count++;
      }
      return { accepted: count };
    });
  }

  status() {
    const counts = this.ctx.storage.sql.exec<SqlRow>(`SELECT COUNT(*) received,
      COALESCE(SUM(acknowledged = 0), 0) pending FROM source_inbox`).one();
    const job = this.job();
    const roundPending = this.ctx.storage.sql.exec<{ n: number }>(
      "SELECT COUNT(*) n FROM source_inbox WHERE acknowledged = 0 AND round_member = 1").one().n;
    const receipt = this.ctx.storage.sql.exec<{ manifest: string }>(
      "SELECT manifest FROM publication_receipts WHERE json_extract(manifest, '$.version') = ?", this.version()).toArray()[0];
    const latest = receipt ? JSON.parse(receipt.manifest) as Manifest : null;
    const pendingSources = this.ctx.storage.sql.exec(`SELECT json_extract(payload, '$.source') source,
      json_extract(payload, '$.source_account') account, COUNT(*) records FROM source_inbox
      WHERE acknowledged = 0 GROUP BY source, account`).toArray();
    return { ...counts, pendingSources, version: this.version(), activeBatch: job?.batch_id ?? null,
      prepared: Boolean(job?.manifest), roundPending,
      baselineId: this.round()?.baseline_id ?? latest?.baselineId ?? null,
      historyComplete: latest?.historyComplete ?? false };
  }

  historyRootStatus() {
    const root = this.ctx.storage.sql.exec<{ baseline_id: string; snapshot_time: string }>(
      "SELECT * FROM history_root").toArray()[0];
    return { baselineId: root?.baseline_id ?? null, snapshotTime: root?.snapshot_time ?? null,
      busy: Boolean(this.job() || this.round()) };
  }

  advanceHistory(input: { previousBaselineId: string; baselineId: string; snapshotTime: string;
    invalidations: { kind: string; value: string }[] }) {
    if (!/^b_[a-f0-9]{24}$/.test(input.baselineId) || !Number.isFinite(Date.parse(input.snapshotTime)) ||
        input.invalidations.length > 100_000 || input.invalidations.some(item =>
          !["key", "fact", "scope"].includes(item.kind) || typeof item.value !== "string")) {
      throw new Error("Invalid history handover.");
    }
    return this.ctx.storage.transactionSync(() => {
      const root = this.historyRootStatus();
      // Never change a baseline underneath a frozen or prepared publication.
      if (root.busy || root.baselineId !== input.previousBaselineId ||
          Date.parse(input.snapshotTime) <= Date.parse(root.snapshotTime ?? "")) return { advanced: false };
      for (const kind of ["key", "fact", "scope"]) {
        const values = input.invalidations.filter(item => item.kind === kind).map(item => item.value);
        for (let offset = 0; offset < values.length; offset += 1000) {
          this.ctx.storage.sql.exec(`DELETE FROM history_checked WHERE kind = ?
            AND value IN (SELECT value FROM json_each(?))`, kind, JSON.stringify(values.slice(offset, offset + 1000)));
        }
      }
      this.ctx.storage.sql.exec("UPDATE history_root SET baseline_id = ?, snapshot_time = ? WHERE singleton = 1",
        input.baselineId, input.snapshotTime);
      return { advanced: true, invalidated: input.invalidations.length };
    });
  }

  async historyInputs(baselineId: string, snapshotTime?: string) {
    if (!/^b_[a-f0-9]{24}$/.test(baselineId)) throw new Error("Invalid historical baseline.");
    if (snapshotTime && !Number.isFinite(Date.parse(snapshotTime))) throw new Error("Invalid baseline time.");
    this.ctx.storage.sql.exec("INSERT OR IGNORE INTO history_root(singleton, baseline_id) VALUES (1, ?)", baselineId);
    const previousRoot = this.ctx.storage.sql.exec<{ baseline_id: string; snapshot_time: string }>("SELECT * FROM history_root").one();
    if (snapshotTime && Date.parse(snapshotTime) > Date.parse(previousRoot.snapshot_time) && !this.job() && !this.round()) {
      this.ctx.storage.transactionSync(() => {
        this.ctx.storage.sql.exec("UPDATE history_root SET baseline_id = ?, snapshot_time = ?", baselineId, snapshotTime);
        this.ctx.storage.sql.exec("DELETE FROM history_checked");
      });
    }
    const root = this.ctx.storage.sql.exec<{ baseline_id: string }>("SELECT baseline_id FROM history_root").one().baseline_id;
    this.beginRound(root);
    const inputs = this.ctx.storage.sql.exec<QueueRow>(
      "SELECT sequence, payload FROM source_inbox WHERE acknowledged = 0 AND round_member = 1 ORDER BY sequence LIMIT 100").toArray();
    const pendingFacts = [];
    for (const row of inputs) {
      const value = JSON.parse(row.payload) as SourceReplacement;
      if (value.source === "history") continue;
      pendingFacts.push(...await replacementFacts(value, null));
    }
    const keys = new Set(pendingFacts.flatMap(fact => fact.evidenceKeys));
    const factKeys = new Set(pendingFacts.map(fact => fact.factKey));
    const scopes = new Set<string>();
    for (const row of inputs) {
      const value = JSON.parse(row.payload);
      if (value.source === "activecampaign") scopes.add(value.scope_id);
    }
    const contactHeads = this.ctx.storage.sql.exec<{ scope: string }>(`SELECT json_extract(payload, '$.scope_id') scope FROM source_heads
      WHERE json_extract(payload, '$.source') = 'activecampaign'
      AND NOT EXISTS (SELECT 1 FROM history_checked c WHERE c.kind = 'scope' AND c.value = json_extract(payload, '$.scope_id')) LIMIT 5000`).toArray();
    contactHeads.forEach(row => scopes.add(row.scope));
    const oldFacts = this.ctx.storage.sql.exec<{ lookup_key: string }>(`SELECT lookup_key FROM identity_state s
      WHERE state_kind = 'fact' AND NOT EXISTS (SELECT 1 FROM history_checked c WHERE c.kind = 'fact' AND c.value = s.lookup_key)
      LIMIT 5000`).toArray();
    oldFacts.forEach(row => factKeys.add(row.lookup_key));
    // The committed reverse index contains the same active evidence keys.
    // Reading its indexed column avoids decoding every historical fact on each pass.
    const oldKeys = this.ctx.storage.sql.exec<{ value: string }>(`SELECT DISTINCT lookup_key value FROM identity_state s
      WHERE state_kind = 'evidence' AND is_deleted = 0
      AND NOT EXISTS (SELECT 1 FROM history_checked c WHERE c.kind = 'key' AND c.value = s.lookup_key) LIMIT 5000`).toArray();
    oldKeys.forEach(row => keys.add(row.value));
    const unchecked = (kind: string, values: Set<string>) => {
      if (!values.size) return [];
      return this.ctx.storage.sql.exec<{ value: string }>(`SELECT incoming.value
        FROM json_each(?) incoming
        WHERE NOT EXISTS (SELECT 1 FROM history_checked checked
          WHERE checked.kind = ? AND checked.value = incoming.value)`,
        JSON.stringify([...values]), kind).toArray().map(row => row.value);
    };
    return { baselineId: root, keys: unchecked("key", keys), factKeys: unchecked("fact", factKeys), scopes: unchecked("scope", scopes) };
  }

  async loadHistory(baselineId: string, events: string[], removedFactKeys: string[] = []) {
    const root = this.ctx.storage.sql.exec<{ baseline_id: string; snapshot_time: string }>("SELECT * FROM history_root").toArray()[0];
    if (root?.baseline_id !== baselineId || events.length > 5000) throw new Error("Invalid history batch.");
    const facts = [];
    // Read pending contacts once. Historical profiles can contain many forms
    // for the same contact; scanning both source tables for every form stalls
    // an hourly handover as the retained inbox grows.
    const pendingContacts = this.ctx.storage.sql.exec<{ payload: string }>(`SELECT payload FROM source_inbox
      WHERE acknowledged = 0 AND round_member = 1
      AND json_extract(payload, '$.source') = 'activecampaign'`).toArray()
      .map(item => JSON.parse(item.payload) as SourceReplacement);
    const contacts = new Map<string, SourceReplacement | null>();
    for (const text of events) {
      const row = JSON.parse(text);
      if (!row.source_system || !row.source_record_id) throw new Error("Historical source key missing.");
      const identifiers = { anonymous_id: row.anonymous_id, user_id: row.user_id, email: row.email, phone: row.phone,
        first_name: row.first_name, last_name: row.last_name };
      const payload = canonicalJson(identifiers);
      const factKey = `${row.source_system}:${row.source_record_id}`;
      const fact = { eventId: `history:${baselineId}:${factKey}`, producerId: "frozen-history", factKind: row.source_system,
        factKey, sourcePriority: 0, sourceFactVersion: Date.parse(root.snapshot_time), factDeleted: false, factPayload: payload,
        factPayloadHash: await sha256(payload), observedAt: row.observed_at, ingestedAt: "1970-01-01T00:00:00.000Z",
        evidenceKeys: evidenceKeys(identifiers) };
      // An empty complete contact replacement must also retract registrations
      // that existed only in the hourly baseline, before this Worker saw them.
      if (row.source_system === "activecampaign" && row.contact_id) {
        const scope = `activecampaign:contact:${row.contact_id}`;
        if (!contacts.has(scope)) {
          const head = await this.readSourceHead(JSON.stringify(["activecampaign", "default", scope]));
          const candidates = pendingContacts.filter(contact => contact.scope_id === scope);
          if (head) candidates.push(head);
          candidates.sort((a, b) => b.observation_sequence - a.observation_sequence);
          contacts.set(scope, candidates[0] ?? null);
        }
        const contact = contacts.get(scope);
        if (contact && !contact.rows.some((item: { form_submission_id: string }) => item.form_submission_id === row.source_record_id)) {
          fact.factDeleted = true;
          fact.evidenceKeys = [];
          fact.sourcePriority = 1;
          fact.sourceFactVersion = contact.observation_sequence;
        }
      }
      facts.push(fact);
    }
    if (removedFactKeys.length > 1000) throw new Error("Historical deletion batch is too large.");
    for (const factKey of removedFactKeys) {
      const prior = (await this.reader().facts([factKey]))[0];
      if (!prior) continue;
      if (prior.sourcePriority !== 0 || prior.factDeleted) continue;
      const payload = canonicalJson({ first_name: prior.firstName, last_name: prior.lastName });
      facts.push({ producerId: "frozen-history", factKind: prior.factKind, factKey,
        eventId: `history:${baselineId}:removed:${factKey}`, sourcePriority: 0,
        sourceFactVersion: Date.parse(root.snapshot_time), factDeleted: true, evidenceKeys: [],
        factPayload: payload, factPayloadHash: await sha256(payload),
        observedAt: prior.factObservedAt, ingestedAt: root.snapshot_time });
    }
    const current = new Map((await this.reader().facts(facts.map(fact => fact.factKey)))
      .map(fact => [fact.factKey, fact]));
    const changed = facts.filter(fact => {
      const prior = current.get(fact.factKey);
      // Historical contact retractions cannot change an already deleted source
      // revision merely because its frozen name or email payload differs.
      if (alreadyRetracted(prior, fact)) return false;
      if (!prior || prior.isDeleted) return true;
      // A new baseline timestamp alone does not change a customer's identity.
      // Coverage still records every checked fact below. Publish only evidence
      // changes, preserving higher-priority live observations.
      if ((prior.sourcePriority ?? 0) > fact.sourcePriority) return false;
      if ((prior.sourcePriority ?? 0) !== fact.sourcePriority) return true;
      return prior.factDeleted !== fact.factDeleted || prior.factPayloadHash !== fact.factPayloadHash
        || prior.factObservedAt !== fact.observedAt
        || canonicalJson(prior.evidenceKeys) !== canonicalJson(fact.evidenceKeys);
    });
    for (let offset = 0; offset < changed.length; offset += 100) {
      const group = changed.slice(offset, offset + 100);
      const id = `history:${baselineId}:${await sha256(canonicalJson(group))}`;
      await this.enqueue([{ source: "history", source_account: "default", baseline_id: baselineId,
        scope_id: id, replacement_id: id, observation_sequence: 0, observed_at: "1970-01-01T00:00:00.000Z",
        rows: [], evidence_inbox_ids: [], source_evidence: {}, facts: group }]);
    }
    return { keys: [...new Set(facts.flatMap(fact => fact.evidenceKeys))], factKeys: facts.map(fact => fact.factKey) };
  }

  markHistoryChecked(baselineId: string, keys: string[], factKeys: string[], scopes: string[] = []) {
    const root = this.ctx.storage.sql.exec<{ baseline_id: string }>("SELECT baseline_id FROM history_root").one();
    if (root.baseline_id !== baselineId || keys.length + factKeys.length > 10_000) throw new Error("Invalid history coverage.");
    this.ctx.storage.transactionSync(() => {
      for (const value of keys) this.ctx.storage.sql.exec("INSERT OR IGNORE INTO history_checked VALUES ('key', ?)", value);
      for (const value of factKeys) this.ctx.storage.sql.exec("INSERT OR IGNORE INTO history_checked VALUES ('fact', ?)", value);
      for (const value of scopes) this.ctx.storage.sql.exec("INSERT OR IGNORE INTO history_checked VALUES ('scope', ?)", value);
    });
    return { checked: true };
  }

  async prepare(baselineId: string): Promise<Manifest | null> {
    if (!/^b_[a-f0-9]{24}$/.test(baselineId)) throw new Error("Invalid baseline ID.");
    const existing = this.job();
    if (existing?.manifest) {
      this.beginRound(existing.baseline_id);
      return JSON.parse(existing.manifest);
    }
    if (existing && existing.lease_until > Date.now()) return null;
    const round = this.beginRound(existing?.baseline_id ?? baselineId);
    const candidates = this.ctx.storage.sql.exec<QueueRow>(
      "SELECT sequence, payload FROM source_inbox WHERE acknowledged = 0 AND round_member = 1 ORDER BY sequence LIMIT 100").toArray();
    const pending: QueueRow[] = [];
    let facts = 0;
    for (const row of candidates) {
      const value = JSON.parse(row.payload) as SourceReplacement;
      const count = value.source === "history" ? value.facts.length : Math.max(1, value.rows.length);
      if (pending.length && facts + count > 2500) break;
      pending.push(row);
      facts += count;
    }
    if (!pending.length) return null;
    const lease = crypto.randomUUID();
    if (!existing) {
      this.ctx.storage.sql.exec("INSERT INTO publication_job VALUES (1, ?, ?, ?, ?, ?, ?, ?, NULL)",
        this.version() + 1, crypto.randomUUID(), round.baseline_id, pending.at(-1)!.sequence,
        new Date().toISOString(), lease, Date.now() + 1100_000);
    } else {
      this.ctx.storage.sql.exec("UPDATE publication_job SET lease = ?, lease_until = ? WHERE singleton = 1", lease, Date.now() + 1100_000);
    }
    const job = this.job()!;
    try {
      const contracts = this.ctx.storage.sql.exec<QueueRow>(
        "SELECT sequence, payload FROM source_inbox WHERE acknowledged = 0 AND round_member = 1 AND sequence <= ? ORDER BY sequence", job.cutoff)
        .toArray().map((row) => JSON.parse(row.payload) as SourceReplacement);
      const latest = new Map<string, SourceReplacement>();
      for (const contract of contracts) {
        const key = scopeKey(contract);
        const prior = latest.get(key);
        if (prior && compareVersion(prior, contract) === 0 && fingerprint(prior) !== fingerprint(contract)) {
          throw new Error("Conflicting source scope version.");
        }
        if (!prior || compareVersion(prior, contract) < 0) latest.set(key, contract);
      }
      const facts = [];
      for (const [key, contract] of latest) {
        const head = await this.readSourceHead(key);
        facts.push(...await replacementFacts(contract, head));
      }
      // Apply the same no-change check to history queued before this upgrade.
      // Keep repeated fact keys for the engine to resolve or reject together.
      const historyKeys = new Set(contracts.filter(contract => contract.source === "history")
        .flatMap(contract => contract.facts.map(fact => fact.factKey)));
      const counts = new Map<string, number>();
      for (const fact of facts) counts.set(fact.factKey, (counts.get(fact.factKey) ?? 0) + 1);
      const storedFacts = new Map((await this.reader().facts([...historyKeys])).map(fact => [fact.factKey, fact]));
      const changedFacts = facts.filter(fact => {
        if (!historyKeys.has(fact.factKey)) return true;
        const prior = storedFacts.get(fact.factKey);
        if (alreadyRetracted(prior, fact)) return false;
        if (counts.get(fact.factKey) !== 1) return true;
        if (!prior || prior.isDeleted || (prior.sourcePriority ?? 0) !== (fact.sourcePriority ?? 0)) return true;
        return prior.factDeleted !== fact.factDeleted || prior.factPayloadHash !== fact.factPayloadHash
          || prior.factObservedAt !== fact.observedAt
          || canonicalJson(prior.evidenceKeys) !== canonicalJson(fact.evidenceKeys);
      });
      const result = await computeIdentityBatch({ tenantId: "boom", version: job.version,
        id: job.batch_id, committedAt: job.created_at, facts: changedFacts }, this.reader());
      const rows = [
        ...contracts.map((payload) => canonicalJson({ kind: "source", payload })),
        ...(await this.changedIdentityRecords(result.rows.map(identityRecord))).map((payload) => canonicalJson({ kind: "identity", payload })),
      ].sort();
      if (rows.length > 100_000) throw new Error("Affected identity component exceeds the publication limit.");
      const root = this.ctx.storage.sql.exec<{ baseline_id: string }>("SELECT baseline_id FROM history_root").toArray()[0];
      const missing = root ? await this.historyInputs(root.baseline_id) : null;
      const remaining = this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) n FROM source_inbox WHERE acknowledged = 0 AND round_member = 1 AND sequence > ?", job.cutoff).one().n;
      const historyComplete = Boolean(missing && !missing.keys.length && !missing.factKeys.length && !missing.scopes.length && remaining === 0);
      const manifest: Manifest = { schemaVersion: 1, tenantId: "boom", batchId: job.batch_id,
        version: job.version, previousVersion: job.version - 1, baselineId: job.baseline_id,
        createdAt: job.created_at, rowCount: rows.length, contentHash: await sha256(rows.join("\n")),
        sourceCount: contracts.length, identityCount: rows.filter(row => JSON.parse(row).kind === "identity").length, historyComplete };
      this.ctx.storage.transactionSync(() => {
        this.assertLease(lease);
        this.ctx.storage.sql.exec("DELETE FROM publication_rows");
        rows.forEach((row, position) => this.ctx.storage.sql.exec("INSERT INTO publication_rows VALUES (?, ?)", position, row));
        this.ctx.storage.sql.exec("UPDATE publication_job SET manifest = ? WHERE singleton = 1", canonicalJson(manifest));
      });
      return manifest;
    } catch (error) {
      this.ctx.storage.sql.exec("UPDATE publication_job SET lease_until = 0 WHERE lease = ?", lease);
      throw error;
    }
  }

  chunk(batchId: string, offset: number) {
    const job = this.job();
    if (!job?.manifest || job.batch_id !== batchId) throw new Error("Publication is not prepared.");
    if (!Number.isSafeInteger(offset) || offset < 0) throw new Error("Invalid offset.");
    const candidates = this.ctx.storage.sql.exec<SqlRow & { row_text: string }>(
      "SELECT row_text FROM publication_rows WHERE position >= ? ORDER BY position LIMIT 1000", offset).toArray();
    const rows: string[] = [];
    let bytes = 0;
    for (const row of candidates) {
      const size = new TextEncoder().encode(row.row_text).byteLength;
      if (rows.length && bytes + size > 500_000) break;
      rows.push(row.row_text); bytes += size;
    }
    return { batchId, offset, rows, nextOffset: offset + rows.length };
  }

  async acknowledge(manifest: Manifest) {
    const text = canonicalJson(manifest);
    const receipt = this.ctx.storage.sql.exec<SqlRow & { manifest: string }>(
      "SELECT manifest FROM publication_receipts WHERE batch_id = ?", manifest.batchId).toArray()[0];
    if (receipt) {
      if (receipt.manifest !== text) throw new Error("Conflicting publication acknowledgement.");
      return { acknowledged: true, version: manifest.version };
    }
    const job = this.job();
    if (!job?.manifest || job.manifest !== text) throw new Error("Acknowledgement does not match prepared publication.");
    const rows = this.ctx.storage.sql.exec<SqlRow & { row_text: string }>("SELECT row_text FROM publication_rows ORDER BY position")
      .toArray().map((row) => JSON.parse(row.row_text));
    const compactHeads = new Map<string, string>();
    for (const row of rows) {
      if (row.kind === "source" && row.payload.source !== "history") {
        const key = scopeKey(row.payload);
        const previous = await this.readSourceHead(key);
        if (!previous || compareVersion(previous,row.payload) < 0) compactHeads.set(key,canonicalJson(await compactSourceHead(row.payload)));
      }
    }
    // Another acknowledgement may complete while compacting; recheck inside the transaction.
    this.ctx.storage.transactionSync(() => {
      if (!this.job() || this.job()!.batch_id !== manifest.batchId) throw new Error("Publication acknowledgement was superseded");
      this.identityExport.enqueue(manifest, rows);
      for (const row of rows) {
        const value = row.payload;
        if (row.kind === "identity") {
          this.ctx.storage.sql.exec("INSERT OR REPLACE INTO identity_state VALUES (?, ?, ?, ?, ?)",
            value.state_kind, value.state_key, value.lookup_key, value.is_deleted, value.payload_json);
          continue;
        }
        if (value.source === "history") continue;
        const key = scopeKey(value);
        const head = this.ctx.storage.sql.exec<SqlRow & { payload: string }>("SELECT payload FROM source_heads WHERE scope_key = ?", key).toArray()[0];
        if (compactHeads.has(key) && (!head || compareVersion(JSON.parse(head.payload), value) < 0)) {
          this.ctx.storage.sql.exec("INSERT OR REPLACE INTO source_heads VALUES (?, ?)", key, compactHeads.get(key)!);
        }
      }
      this.ctx.storage.sql.exec("INSERT INTO publication_receipts VALUES (?, ?)", job.batch_id, text);
      this.ctx.storage.sql.exec("UPDATE publication_meta SET version = ?", job.version);
      this.ctx.storage.sql.exec("UPDATE source_inbox SET acknowledged = 1, round_member = 0, payload = '{}' WHERE acknowledged = 0 AND round_member = 1 AND sequence <= ?", job.cutoff);
      this.ctx.storage.sql.exec("DELETE FROM publication_rows");
      this.ctx.storage.sql.exec("DELETE FROM publication_job");
      const remaining = this.ctx.storage.sql.exec<{ n: number }>(
        "SELECT COUNT(*) n FROM source_inbox WHERE acknowledged = 0 AND round_member = 1").one().n;
      if (!remaining) this.ctx.storage.sql.exec("DELETE FROM publication_round");
    });
    return { acknowledged: true, version: manifest.version };
  }

  private round(): { baseline_id: string } | undefined {
    return this.ctx.storage.sql.exec<{ baseline_id: string }>("SELECT baseline_id FROM publication_round").toArray()[0];
  }

  // Bound each checkpoint to 100 source updates. Historical evidence joins the same
  // round, while later source arrivals wait safely for the next checkpoint.
  private beginRound(baselineId: string): { baseline_id: string } {
    const existing = this.round();
    if (existing) return existing;
    if (!this.ctx.storage.sql.exec("SELECT 1 FROM source_inbox WHERE acknowledged = 0 LIMIT 1").toArray().length) {
      return { baseline_id: baselineId };
    }
    return this.ctx.storage.transactionSync(() => {
      const baseline = this.job()?.baseline_id ?? baselineId;
      this.ctx.storage.sql.exec("INSERT INTO publication_round VALUES (1, ?)", baseline);
      this.ctx.storage.sql.exec(`UPDATE source_inbox SET round_member = 1 WHERE sequence IN (
        SELECT sequence FROM source_inbox WHERE acknowledged = 0 ORDER BY sequence LIMIT 100
      )`);
      return { baseline_id: baseline };
    });
  }

  private async changedIdentityRecords(records: ReturnType<typeof identityRecord>[]) {
    const saved = new Map<string, {payload:string;is_deleted:number;lookup_key:string}>();
    for (const kind of new Set(records.map(row => row.state_kind))) {
      const keys = [...new Set(records.filter(row => row.state_kind === kind).map(row => row.lookup_key))];
      if (kind === "redirect") continue;
      for (let offset = 0; offset < keys.length; offset += 500) {
        for (const row of await this.readIdentityRecords(kind,keys.slice(offset,offset+500))) saved.set(`${kind}:${row.state_key}`,row);
      }
    }
    return records.filter(record => {
      const previous = saved.get(`${record.state_kind}:${record.state_key}`);
      return !previous || previous.payload !== record.payload_json || previous.is_deleted !== record.is_deleted || previous.lookup_key !== record.lookup_key;
    });
  }

  private reader(): IdentityStateReader {
    const read = async (kind: string, keys: string[], keepDeleted = false) => {
      const values = [];
      for (let offset = 0; offset < keys.length; offset += 50) {
        const group = keys.slice(offset, offset + 50);
        const rows = await this.readIdentityRecords(kind,group);
        values.push(...rows.filter(row => keepDeleted || !row.is_deleted).map(row => JSON.parse(row.payload)));
      }
      return values;
    };
    return { facts: (keys) => read("fact", keys, true), mappings: (keys) => read("mapping", keys),
      profiles: (keys) => read("profile", keys), evidence: (keys) => read("evidence", keys) };
  }
  private version(): number {
    return this.ctx.storage.sql.exec<SqlRow & { version: number }>("SELECT version FROM publication_meta").one().version;
  }
  private job(): Job | undefined { return this.ctx.storage.sql.exec<Job>("SELECT * FROM publication_job").toArray()[0]; }
  private assertLease(lease: string) {
    if (this.job()?.lease !== lease) throw new Error("Publication lease was replaced.");
  }
}

export class SourcePublisher extends WorkerEntrypoint<PublicationEnv> {
  async publishSourceReplacements(contracts: SourceReplacement[]): Promise<void> {
    await this.env.PUBLICATION.getByName(this.env.PUBLICATION_NAME ?? "boom").enqueue(contracts);
  }
  async publishSourceReplacement(contract: SourceReplacement): Promise<void> {
    await this.publishSourceReplacements([contract]);
  }
}
