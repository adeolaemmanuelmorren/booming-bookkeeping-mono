import type { SourceReplacementContract, SourceRoute } from "./contracts.ts";

interface InboxRecord {
  id: string;
  source: string;
  sourceAccount: string;
  kind: string;
  chargeId?: string;
  contactId?: string;
  receivedAt: string;
  immutablePayload: unknown;
  progress?: unknown;
}

interface OutboxRecord {
  id: string;
  source: string;
  sourceAccount: string;
  contract: SourceReplacementContract;
  inboxIds?: string[];
  stateUpdates?: Array<{ key: string; value: unknown }>;
}

interface InboxRow {
  [key: string]: SqlStorageValue;
  id: string;
  source: string;
  source_account: string;
  kind: string;
  charge_id: string | null;
  contact_id: string | null;
  received_at: string;
  immutable_payload_json: string;
  payload_hash: string;
  progress_json: string | null;
  priority: number;
  status: string;
  outbox_id: string | null;
}

interface OutboxRow {
  [key: string]: SqlStorageValue;
  id: string;
  source: string;
  source_account: string;
  contract_json: string;
  contract_hash: string;
  inbox_ids_json: string;
  state_updates_json: string;
  priority: number;
  status: string;
  published_at: string | null;
}

interface ControlRow {
  [key: string]: SqlStorageValue;
  backfill_id: string;
  stripe_created_gte: number;
  backfill_started_at: string | null;
  last_slice_started_at: string | null;
  last_slice_succeeded_at: string | null;
  last_error_code: string | null;
  last_error_status: number | null;
}

export class SqliteSourceStore {
  constructor(
    private readonly storage: DurableObjectStorage,
    private readonly clock: Pick<typeof Date, "now"> = Date,
  ) {}

  initializeSchema(): void {
    this.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS source_schema_migrations (
        version INTEGER PRIMARY KEY,
        applied_at_ms INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS coordinator_route (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        source TEXT NOT NULL,
        source_account TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS coordinator_control (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        backfill_id TEXT NOT NULL,
        stripe_created_gte INTEGER NOT NULL,
        backfill_started_at TEXT,
        last_slice_started_at TEXT,
        last_slice_succeeded_at TEXT,
        last_error_code TEXT,
        last_error_status INTEGER,
        updated_at_ms INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS coordinator_lease (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        lease_id TEXT,
        expires_at_ms INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS source_cursors (
        key TEXT PRIMARY KEY,
        value_json TEXT NOT NULL,
        updated_at_ms INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS source_inbox (
        id TEXT PRIMARY KEY,
        source TEXT NOT NULL,
        source_account TEXT NOT NULL,
        kind TEXT NOT NULL,
        charge_id TEXT,
        contact_id TEXT,
        received_at TEXT NOT NULL,
        immutable_payload_json TEXT NOT NULL,
        payload_hash TEXT NOT NULL,
        progress_json TEXT,
        priority INTEGER NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('pending', 'outboxed', 'processed')),
        outbox_id TEXT,
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS source_sequences (
        key TEXT PRIMARY KEY,
        value INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS source_outbox (
        id TEXT PRIMARY KEY,
        source TEXT NOT NULL,
        source_account TEXT NOT NULL,
        scope_id TEXT NOT NULL,
        observation_sequence INTEGER NOT NULL,
        contract_json TEXT NOT NULL,
        contract_hash TEXT NOT NULL,
        inbox_ids_json TEXT NOT NULL,
        state_updates_json TEXT NOT NULL,
        priority INTEGER NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('pending', 'published')),
        published_at TEXT,
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS source_state (
        key TEXT PRIMARY KEY,
        value_json TEXT NOT NULL,
        updated_at_ms INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS source_current_scopes (
        scope_id TEXT PRIMARY KEY,
        rows_json TEXT NOT NULL,
        row_count INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL
      );
    `);

    this.ensurePriorityColumn("source_inbox");
    this.ensurePriorityColumn("source_outbox");
    this.ensureTextColumn("source_inbox", "payload_hash");
    this.ensureTextColumn("source_outbox", "contract_hash");
    this.storage.sql.exec(`
      CREATE INDEX IF NOT EXISTS source_inbox_pending_v2
        ON source_inbox (
          source,
          source_account,
          status,
          priority,
          received_at,
          id
        );
      CREATE INDEX IF NOT EXISTS source_outbox_pending_scope
        ON source_outbox(source, source_account, status, scope_id, observation_sequence);
      CREATE INDEX IF NOT EXISTS source_outbox_pending_v2
        ON source_outbox (
          source,
          source_account,
          status,
          priority,
          scope_id,
          observation_sequence,
          created_at_ms
        );
    `);

    const now = this.clock.now();
    this.storage.sql.exec(
      `INSERT OR IGNORE INTO source_schema_migrations (version, applied_at_ms)
       VALUES (2, ?)`,
      now,
    );
    this.storage.sql.exec(
      `INSERT OR IGNORE INTO coordinator_control (
        id,
        backfill_id,
        stripe_created_gte,
        updated_at_ms
      ) VALUES (1, 'v1', 0, ?)`,
      now,
    );
    this.storage.sql.exec(
      `INSERT OR IGNORE INTO coordinator_lease (id, lease_id, expires_at_ms)
       VALUES (1, NULL, 0)`,
    );
  }

  bindRoute(route: SourceRoute): void {
    validateRoute(route);

    this.storage.transactionSync(() => {
      const existing = this.storage.sql.exec<{
        source: string;
        source_account: string;
      }>(
        "SELECT source, source_account FROM coordinator_route WHERE id = 1",
      ).toArray()[0];

      if (!existing) {
        this.storage.sql.exec(
          `INSERT INTO coordinator_route (
            id,
            source,
            source_account,
            created_at_ms
          ) VALUES (1, ?, ?, ?)`,
          route.source,
          route.account,
          this.clock.now(),
        );
        return;
      }

      if (
        existing.source !== route.source ||
        existing.source_account !== route.account
      ) {
        throw new Error("coordinator route does not match its durable identity");
      }
    });
  }

  getRoute(): SourceRoute | null {
    const row = this.storage.sql.exec<{
      source: string;
      source_account: string;
    }>(
      "SELECT source, source_account FROM coordinator_route WHERE id = 1",
    ).toArray()[0];

    if (!row) {
      return null;
    }

    return validateRoute({ source: row.source, account: row.source_account });
  }

  configureBackfill(
    backfillId: string,
    stripeCreatedGte: number,
    startedAt: string,
  ): void {
    this.storage.sql.exec(
      `UPDATE coordinator_control
       SET
         backfill_id = ?,
         stripe_created_gte = ?,
         backfill_started_at = ?,
         updated_at_ms = ?
       WHERE id = 1`,
      backfillId,
      stripeCreatedGte,
      startedAt,
      this.clock.now(),
    );
  }

  getControl(): ControlRow {
    return this.storage.sql.exec<ControlRow>(
      `SELECT
         backfill_id,
         stripe_created_gte,
         backfill_started_at,
         last_slice_started_at,
         last_slice_succeeded_at,
         last_error_code,
         last_error_status
       FROM coordinator_control
       WHERE id = 1`,
    ).one();
  }

  recordSliceStart(startedAt: string): void {
    this.storage.sql.exec(
      `UPDATE coordinator_control
       SET last_slice_started_at = ?, updated_at_ms = ?
       WHERE id = 1`,
      startedAt,
      this.clock.now(),
    );
  }

  recordSliceSuccess(succeededAt: string): void {
    this.storage.sql.exec(
      `UPDATE coordinator_control
       SET
         last_slice_succeeded_at = ?,
         last_error_code = NULL,
         last_error_status = NULL,
         updated_at_ms = ?
       WHERE id = 1`,
      succeededAt,
      this.clock.now(),
    );
  }

  recordSliceFailure(code: string, status: number | null): void {
    this.storage.sql.exec(
      `UPDATE coordinator_control
       SET last_error_code = ?, last_error_status = ?, updated_at_ms = ?
       WHERE id = 1`,
      code,
      status,
      this.clock.now(),
    );
  }

  acquireLease(leaseId: string, now: number, expiresAt: number): boolean {
    return this.storage.transactionSync(() => {
      const lease = this.storage.sql.exec<{
        lease_id: string | null;
        expires_at_ms: number;
      }>(
        "SELECT lease_id, expires_at_ms FROM coordinator_lease WHERE id = 1",
      ).one();

      if (lease.lease_id && lease.expires_at_ms > now) {
        return false;
      }

      this.storage.sql.exec(
        `UPDATE coordinator_lease
         SET lease_id = ?, expires_at_ms = ?
         WHERE id = 1`,
        leaseId,
        expiresAt,
      );
      return true;
    });
  }

  releaseLease(leaseId: string): void {
    this.storage.sql.exec(
      `UPDATE coordinator_lease
       SET lease_id = NULL, expires_at_ms = 0
       WHERE id = 1 AND lease_id = ?`,
      leaseId,
    );
  }

  leaseStatus(now: number): { active: boolean; expiresAtMs: number | null } {
    const row = this.storage.sql.exec<{
      lease_id: string | null;
      expires_at_ms: number;
    }>(
      "SELECT lease_id, expires_at_ms FROM coordinator_lease WHERE id = 1",
    ).one();
    const active = Boolean(row.lease_id) && row.expires_at_ms > now;

    return {
      active,
      expiresAtMs: active ? row.expires_at_ms : null,
    };
  }

  async getCursor(key: string): Promise<unknown | null> {
    const row = this.storage.sql.exec<{ value_json: string }>(
      "SELECT value_json FROM source_cursors WHERE key = ?",
      key,
    ).toArray()[0];

    return row ? parseJson(row.value_json, `cursor ${key}`) : null;
  }

  async setCursor(key: string, value: unknown): Promise<void> {
    this.storage.sql.exec(
      `INSERT INTO source_cursors (key, value_json, updated_at_ms)
       VALUES (?, ?, ?)
       ON CONFLICT(key) DO UPDATE SET
         value_json = excluded.value_json,
         updated_at_ms = excluded.updated_at_ms`,
      key,
      stringifyJson(value, `cursor ${key}`),
      this.clock.now(),
    );
  }

  async putInboxIfAbsent(record: InboxRecord): Promise<boolean> {
    const now = this.clock.now();
    const payloadHash = await hashJson({
      source: record.source,
      sourceAccount: record.sourceAccount,
      kind: record.kind,
      chargeId: record.chargeId ?? null,
      contactId: record.contactId ?? null,
      immutablePayload: record.immutablePayload,
    });
    const result = this.storage.sql.exec(
      `INSERT OR IGNORE INTO source_inbox (
        id,
        source,
        source_account,
        kind,
        charge_id,
        contact_id,
        received_at,
        immutable_payload_json,
        payload_hash,
        progress_json,
        priority,
        status,
        outbox_id,
        created_at_ms,
        updated_at_ms
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, 'pending', NULL, ?, ?)`,
      record.id,
      record.source,
      record.sourceAccount,
      record.kind,
      record.chargeId ?? null,
      record.contactId ?? null,
      requireTimestamp(record.receivedAt, "inbox receivedAt"),
      stringifyJson(record.immutablePayload, "immutable inbox payload"),
      payloadHash,
      inboxPriority(record),
      now,
      now,
    );

    if (result.rowsWritten > 0) {
      return true;
    }

    const existing = this.storage.sql.exec<{ payload_hash: string }>(
      "SELECT payload_hash FROM source_inbox WHERE id = ?",
      record.id,
    ).one();

    if (existing.payload_hash !== payloadHash) {
      throw new Error(`immutable inbox ID conflict: ${record.id}`);
    }

    return false;
  }

  async listPendingInbox(input: {
    source: string;
    sourceAccount: string;
    limit: number;
    recentFirst?: boolean;
  }): Promise<unknown[]> {
    const rows = this.storage.sql.exec<InboxRow>(
      `SELECT
         id,
         source,
         source_account,
         kind,
         charge_id,
         contact_id,
         received_at,
         immutable_payload_json,
         payload_hash,
         progress_json,
         priority,
         status,
         outbox_id
       FROM source_inbox
       WHERE source = ? AND source_account = ? AND status = 'pending'
       ORDER BY priority, ${input.recentFirst ? "(progress_json IS NOT NULL) DESC, received_at DESC" : "received_at"}, id
       LIMIT ?`,
      input.source,
      input.sourceAccount,
      requirePositiveInteger(input.limit, "inbox limit"),
    ).toArray();

    return rows.map(inboxRecordFromRow);
  }

  async saveInboxProgress(id: string, progress: unknown): Promise<void> {
    const result = this.storage.sql.exec(
      `UPDATE source_inbox
       SET progress_json = ?, updated_at_ms = ?
       WHERE id = ? AND status = 'pending'`,
      stringifyJson(progress, "inbox progress"),
      this.clock.now(),
      id,
    );
    requireWrite(result.rowsWritten, `pending inbox ${id}`);
  }

  async markInboxOutboxed(id: string, outboxId: string): Promise<void> {
    const result = this.storage.sql.exec(
      `UPDATE source_inbox
       SET status = 'outboxed', outbox_id = ?, updated_at_ms = ?
       WHERE
         id = ?
         AND (
           status = 'pending'
           OR (status = 'outboxed' AND outbox_id = ?)
         )`,
      outboxId,
      this.clock.now(),
      id,
      outboxId,
    );
    requireWrite(result.rowsWritten, `inbox ${id}`);
  }

  async markInboxProcessed(
    id: string,
    outboxId: string | null,
  ): Promise<void> {
    const result = this.storage.sql.exec(
      `UPDATE source_inbox
       SET status = 'processed', outbox_id = ?, updated_at_ms = ?
       WHERE id = ?`,
      outboxId,
      this.clock.now(),
      id,
    );
    requireWrite(result.rowsWritten, `inbox ${id}`);
  }

  async nextSequence(key: string): Promise<number> {
    return this.storage.transactionSync(() => {
      this.storage.sql.exec(
        "INSERT OR IGNORE INTO source_sequences (key, value) VALUES (?, 0)",
        key,
      );
      this.storage.sql.exec(
        "UPDATE source_sequences SET value = value + 1 WHERE key = ?",
        key,
      );
      return this.storage.sql.exec<{ value: number }>(
        "SELECT value FROM source_sequences WHERE key = ?",
        key,
      ).one().value;
    });
  }

  async putOutboxIfAbsent(record: OutboxRecord): Promise<boolean> {
    const now = this.clock.now();
    const inboxIds = [...new Set(record.inboxIds ?? [])];
    const stateUpdates = record.stateUpdates ?? [];
    const contractHash = await hashJson({
      contract: record.contract,
      inboxIds,
      stateUpdates,
    });
    const result = this.storage.sql.exec(
      `INSERT OR IGNORE INTO source_outbox (
        id,
        source,
        source_account,
        scope_id,
        observation_sequence,
        contract_json,
        contract_hash,
        inbox_ids_json,
        state_updates_json,
        priority,
        status,
        published_at,
        created_at_ms,
        updated_at_ms
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, ?, ?)`,
      record.id,
      record.source,
      record.sourceAccount,
      record.contract.scope_id,
      record.contract.observation_sequence,
      stringifyJson(record.contract, "outbox contract"),
      contractHash,
      stringifyJson(inboxIds, "outbox inbox IDs"),
      stringifyJson(stateUpdates, "outbox state updates"),
      outboxPriority(record.contract),
      now,
      now,
    );

    if (result.rowsWritten > 0) {
      return true;
    }

    const existing = this.storage.sql.exec<{ contract_hash: string }>(
      "SELECT contract_hash FROM source_outbox WHERE id = ?",
      record.id,
    ).one();

    if (existing.contract_hash !== contractHash) {
      throw new Error(`immutable outbox ID conflict: ${record.id}`);
    }

    return false;
  }

  async listPendingOutbox(input: {
    source: string;
    sourceAccount: string;
    limit: number;
    recentFirst?: boolean;
  }): Promise<unknown[]> {
    const rows = this.storage.sql.exec<OutboxRow>(
      `SELECT
         id,
         source,
         source_account,
         contract_json,
         contract_hash,
         inbox_ids_json,
         state_updates_json,
         priority,
         status,
         published_at
       FROM source_outbox pending
       WHERE source = ? AND source_account = ? AND status = 'pending'
       ${input.recentFirst ? `AND NOT EXISTS (
         SELECT 1 FROM source_outbox earlier
         WHERE earlier.source = pending.source AND earlier.source_account = pending.source_account
           AND earlier.status = 'pending' AND earlier.scope_id = pending.scope_id
           AND earlier.observation_sequence < pending.observation_sequence
       )` : ""}
       ORDER BY priority, ${input.recentFirst ? "created_at_ms DESC, scope_id" : "scope_id, observation_sequence, created_at_ms"}
       LIMIT ?`,
      input.source,
      input.sourceAccount,
      requirePositiveInteger(input.limit, "outbox limit"),
    ).toArray();

    return rows.map(outboxRecordFromRow);
  }

  async commitPublishedOutbox(id: string, publishedAt: string): Promise<boolean> {
    return this.storage.transactionSync(() => {
      const row = this.storage.sql.exec<OutboxRow>(
        `SELECT
           id,
           source,
           source_account,
           contract_json,
           contract_hash,
           inbox_ids_json,
           state_updates_json,
           priority,
           status,
           published_at
         FROM source_outbox
         WHERE id = ?`,
        id,
      ).toArray()[0];

      if (!row) {
        throw new Error(`missing outbox ${id}`);
      }

      if (row.status === "published") {
        return false;
      }

      const contract = parseContract(row.contract_json);
      const inboxIds = parseStringArray(row.inbox_ids_json, "outbox inbox IDs");
      const stateUpdates = parseStateUpdates(row.state_updates_json);
      const now = this.clock.now();

      for (const update of stateUpdates) {
        this.upsertState(update.key, update.value, now);
      }

      this.upsertPublishedRows(contract.scope_id, contract.rows, now);

      for (const inboxId of inboxIds) {
        const inbox = this.storage.sql.exec<{
          outbox_id: string | null;
          status: string;
        }>(
          "SELECT outbox_id, status FROM source_inbox WHERE id = ?",
          inboxId,
        ).toArray()[0];

        if (!inbox) {
          throw new Error(`missing inbox ${inboxId}`);
        }

        if (inbox.outbox_id && inbox.outbox_id !== id) {
          throw new Error(`inbox ${inboxId} belongs to another outbox`);
        }

        this.storage.sql.exec(
          `UPDATE source_inbox
           SET
             status = 'processed',
             outbox_id = ?,
             immutable_payload_json = '{}',
             progress_json = NULL,
             updated_at_ms = ?
           WHERE id = ?`,
          id,
          now,
          inboxId,
        );
      }

      const update = this.storage.sql.exec(
        `UPDATE source_outbox
         SET
           status = 'published',
           published_at = ?,
           contract_json = ?,
           inbox_ids_json = '[]',
           state_updates_json = '[]',
           updated_at_ms = ?
         WHERE id = ? AND status = 'pending'`,
        requireTimestamp(publishedAt, "publishedAt"),
        stringifyJson(compactContractReceipt(contract), "outbox receipt"),
        now,
        id,
      );
      requireWrite(update.rowsWritten, `pending outbox ${id}`);

      return true;
    });
  }

  async markOutboxPublished(id: string, publishedAt: string): Promise<void> {
    await this.commitPublishedOutbox(id, publishedAt);
  }

  async getState(key: string): Promise<unknown | null> {
    const row = this.storage.sql.exec<{ value_json: string }>(
      "SELECT value_json FROM source_state WHERE key = ?",
      key,
    ).toArray()[0];

    return row ? parseJson(row.value_json, `state ${key}`) : null;
  }

  async setState(key: string, value: unknown): Promise<void> {
    this.upsertState(key, value, this.clock.now());
  }

  async getPublishedRows(scopeId: string): Promise<unknown[]> {
    const row = this.storage.sql.exec<{ rows_json: string }>(
      "SELECT rows_json FROM source_current_scopes WHERE scope_id = ?",
      scopeId,
    ).toArray()[0];

    if (!row) {
      return [];
    }

    return parseArray(row.rows_json, `published rows ${scopeId}`);
  }

  async setPublishedRows(scopeId: string, rows: unknown[]): Promise<void> {
    this.upsertPublishedRows(scopeId, rows, this.clock.now());
  }

  counts(): Record<string, number> {
    const inbox = this.storage.sql.exec<{
      status: string;
      count: number;
    }>(
      "SELECT status, COUNT(*) AS count FROM source_inbox GROUP BY status",
    ).toArray();
    const outbox = this.storage.sql.exec<{
      status: string;
      count: number;
    }>(
      "SELECT status, COUNT(*) AS count FROM source_outbox GROUP BY status",
    ).toArray();
    const scopes = this.storage.sql.exec<{
      scope_count: number;
      row_count: number;
    }>(
      `SELECT
         COUNT(*) AS scope_count,
         COALESCE(SUM(row_count), 0) AS row_count
       FROM source_current_scopes`,
    ).one();

    return {
      pendingInbox: countStatus(inbox, "pending"),
      outboxedInbox: countStatus(inbox, "outboxed"),
      processedInbox: countStatus(inbox, "processed"),
      pendingOutbox: countStatus(outbox, "pending"),
      publishedOutbox: countStatus(outbox, "published"),
      cursorCount: this.countTable("source_cursors"),
      currentScopeCount: scopes.scope_count,
      currentRowCount: scopes.row_count,
      immutableVersionCount: this.countTable("source_outbox"),
    };
  }

  pendingInboxCount(input: {
    source: string;
    sourceAccount: string;
    maximumPriority?: number;
  }): number {
    if (input.maximumPriority === undefined) {
      return this.storage.sql.exec<{ count: number }>(
        `SELECT COUNT(*) AS count
         FROM source_inbox
         WHERE source = ? AND source_account = ? AND status = 'pending'`,
        input.source,
        input.sourceAccount,
      ).one().count;
    }

    return this.storage.sql.exec<{ count: number }>(
      `SELECT COUNT(*) AS count
       FROM source_inbox
       WHERE
         source = ?
         AND source_account = ?
         AND status = 'pending'
         AND priority <= ?`,
      input.source,
      input.sourceAccount,
      input.maximumPriority,
    ).one().count;
  }

  storageProfile(): { databaseBytes: number; storedJsonBytes: number } {
    const row = this.storage.sql.exec<{ bytes: number }>(`
      SELECT
        COALESCE((SELECT SUM(LENGTH(value_json)) FROM source_cursors), 0) +
        COALESCE((SELECT SUM(
          LENGTH(immutable_payload_json) + COALESCE(LENGTH(progress_json), 0)
        ) FROM source_inbox), 0) +
        COALESCE((SELECT SUM(
          LENGTH(contract_json) + LENGTH(inbox_ids_json) + LENGTH(state_updates_json)
        ) FROM source_outbox), 0) +
        COALESCE((SELECT SUM(LENGTH(value_json)) FROM source_state), 0) +
        COALESCE((SELECT SUM(LENGTH(rows_json)) FROM source_current_scopes), 0)
        AS bytes
    `).one();

    return {
      databaseBytes: this.storage.sql.databaseSize,
      storedJsonBytes: row.bytes,
    };
  }

  private upsertState(key: string, value: unknown, now: number): void {
    this.storage.sql.exec(
      `INSERT INTO source_state (key, value_json, updated_at_ms)
       VALUES (?, ?, ?)
       ON CONFLICT(key) DO UPDATE SET
         value_json = excluded.value_json,
         updated_at_ms = excluded.updated_at_ms`,
      key,
      stringifyJson(value, `state ${key}`),
      now,
    );
  }

  private upsertPublishedRows(
    scopeId: string,
    rows: unknown[],
    now: number,
  ): void {
    if (!Array.isArray(rows)) {
      throw new TypeError("published rows must be an array");
    }

    this.storage.sql.exec(
      `INSERT INTO source_current_scopes (
        scope_id,
        rows_json,
        row_count,
        updated_at_ms
      ) VALUES (?, ?, ?, ?)
      ON CONFLICT(scope_id) DO UPDATE SET
        rows_json = excluded.rows_json,
        row_count = excluded.row_count,
        updated_at_ms = excluded.updated_at_ms`,
      scopeId,
      stringifyJson(rows, `published rows ${scopeId}`),
      rows.length,
      now,
    );
  }

  private countTable(table: "source_cursors" | "source_outbox"): number {
    return this.storage.sql.exec<{ count: number }>(
      `SELECT COUNT(*) AS count FROM ${table}`,
    ).one().count;
  }

  private ensurePriorityColumn(
    table: "source_inbox" | "source_outbox",
  ): void {
    const columns = this.storage.sql.exec<{ name: string }>(
      `PRAGMA table_info(${table})`,
    ).toArray();

    if (columns.some((column) => column.name === "priority")) {
      return;
    }

    this.storage.sql.exec(
      `ALTER TABLE ${table} ADD COLUMN priority INTEGER NOT NULL DEFAULT 10`,
    );
  }

  private ensureTextColumn(
    table: "source_inbox" | "source_outbox",
    column: "payload_hash" | "contract_hash",
  ): void {
    const columns = this.storage.sql.exec<{ name: string }>(
      `PRAGMA table_info(${table})`,
    ).toArray();

    if (columns.some((entry) => entry.name === column)) {
      return;
    }

    this.storage.sql.exec(
      `ALTER TABLE ${table} ADD COLUMN ${column} TEXT NOT NULL DEFAULT ''`,
    );
  }
}

function inboxRecordFromRow(row: InboxRow): unknown {
  return {
    id: row.id,
    source: row.source,
    sourceAccount: row.source_account,
    kind: row.kind,
    chargeId: row.charge_id,
    contactId: row.contact_id,
    receivedAt: row.received_at,
    immutablePayload: parseJson(row.immutable_payload_json, `inbox ${row.id}`),
    payloadHash: row.payload_hash,
    progress: row.progress_json
      ? parseJson(row.progress_json, `inbox progress ${row.id}`)
      : null,
    priority: row.priority,
    status: row.status,
    outboxId: row.outbox_id,
  };
}

function outboxRecordFromRow(row: OutboxRow): unknown {
  return {
    id: row.id,
    source: row.source,
    sourceAccount: row.source_account,
    contract: parseContract(row.contract_json),
    contractHash: row.contract_hash,
    inboxIds: parseStringArray(row.inbox_ids_json, "outbox inbox IDs"),
    stateUpdates: parseStateUpdates(row.state_updates_json),
    priority: row.priority,
    status: row.status,
    publishedAt: row.published_at,
  };
}

function inboxPriority(record: InboxRecord): number {
  if (record.kind === "stripe_event") {
    return 0;
  }

  const triggerKind = readTriggerKind(record.immutablePayload);

  if (triggerKind === "contact_update_poll") {
    return 10;
  }

  if (record.kind === "charge_backfill" || triggerKind === "backfill") {
    return 20;
  }

  if (triggerKind === "rolling_reconciliation") {
    return 30;
  }

  return 10;
}

function outboxPriority(contract: SourceReplacementContract): number {
  const evidence = contract.source_evidence;

  if (evidence?.kind === "activecampaign_tag_snapshot") {
    return 20;
  }

  const inbox = evidence?.inbox;

  if (!inbox || typeof inbox !== "object" || Array.isArray(inbox)) {
    return 10;
  }

  return inboxPriority({
    id: "outbox-priority",
    source: contract.source,
    sourceAccount: contract.source_account,
    kind: readStringProperty(inbox, "kind") ?? "unknown",
    receivedAt: contract.observed_at,
    immutablePayload:
      readObjectProperty(inbox, "immutablePayload") ?? Object.create(null),
  });
}

function readTriggerKind(value: unknown): string | null {
  const payload = readObject(value);
  const trigger = readObject(payload?.trigger);
  return typeof trigger?.kind === "string" ? trigger.kind : null;
}

function readObjectProperty(
  value: object,
  key: string,
): Record<string, unknown> | null {
  return readObject((value as Record<string, unknown>)[key]);
}

function readStringProperty(value: object, key: string): string | null {
  const result = (value as Record<string, unknown>)[key];
  return typeof result === "string" ? result : null;
}

function readObject(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  return value as Record<string, unknown>;
}

function compactContractReceipt(
  contract: SourceReplacementContract,
): Record<string, unknown> {
  return {
    source: contract.source,
    source_account: contract.source_account,
    scope_id: contract.scope_id,
    replacement_id: contract.replacement_id,
    observed_at: contract.observed_at,
    observation_sequence: contract.observation_sequence,
    row_count: contract.rows.length,
    evidence_inbox_ids: contract.evidence_inbox_ids,
  };
}

async function hashJson(value: unknown): Promise<string> {
  const bytes = new TextEncoder().encode(canonicalJson(value));
  const digest = await crypto.subtle.digest("SHA-256", bytes);

  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function canonicalJson(value: unknown): string {
  const encoded = JSON.stringify(sortJson(value));

  if (encoded === undefined) {
    throw new TypeError("hash input cannot be undefined");
  }

  return encoded;
}

function sortJson(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sortJson);
  }

  if (!value || typeof value !== "object") {
    return value;
  }

  const input = value as Record<string, unknown>;
  const sorted: Record<string, unknown> = {};

  for (const key of Object.keys(input).sort()) {
    sorted[key] = sortJson(input[key]);
  }

  return sorted;
}

function parseContract(value: string): SourceReplacementContract {
  const contract = parseJson(value, "outbox contract") as SourceReplacementContract;

  if (!contract || typeof contract !== "object" || !Array.isArray(contract.rows)) {
    throw new TypeError("outbox contract is invalid");
  }

  return contract;
}

function parseStateUpdates(
  value: string,
): Array<{ key: string; value: unknown }> {
  const updates = parseArray(value, "outbox state updates");

  return updates.map((entry) => {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      throw new TypeError("outbox state update must be an object");
    }

    const update = entry as { key?: unknown; value?: unknown };
    if (typeof update.key !== "string" || !update.key) {
      throw new TypeError("outbox state update key is invalid");
    }

    return { key: update.key, value: update.value };
  });
}

function parseStringArray(value: string, fieldName: string): string[] {
  return parseArray(value, fieldName).map((entry) => {
    if (typeof entry !== "string" || !entry) {
      throw new TypeError(`${fieldName} must contain strings`);
    }

    return entry;
  });
}

function parseArray(value: string, fieldName: string): unknown[] {
  const parsed = parseJson(value, fieldName);

  if (!Array.isArray(parsed)) {
    throw new TypeError(`${fieldName} must be an array`);
  }

  return parsed;
}

function parseJson(value: string, fieldName: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    throw new TypeError(`${fieldName} contains invalid JSON`);
  }
}

function stringifyJson(value: unknown, fieldName: string): string {
  const encoded = JSON.stringify(value);

  if (encoded === undefined) {
    throw new TypeError(`${fieldName} cannot be undefined`);
  }

  return encoded;
}

function countStatus(
  rows: Array<{ status: string; count: number }>,
  status: string,
): number {
  return rows.find((row) => row.status === status)?.count ?? 0;
}

function requirePositiveInteger(value: number, fieldName: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${fieldName} must be a positive integer`);
  }

  return value;
}

function requireTimestamp(value: string, fieldName: string): string {
  if (Number.isNaN(Date.parse(value))) {
    throw new TypeError(`${fieldName} must be a timestamp`);
  }

  return value;
}

function requireWrite(rowsWritten: number, description: string): void {
  if (rowsWritten <= 0) {
    throw new Error(`missing ${description}`);
  }
}

function validateRoute(value: {
  source: string;
  account: string;
}): SourceRoute {
  if (
    value.source === "stripe" &&
    (value.account === "main" || value.account === "kajabi")
  ) {
    return { source: "stripe", account: value.account };
  }

  if (value.source === "activecampaign" && value.account === "default") {
    return { source: "activecampaign", account: "default" };
  }

  throw new TypeError("invalid source coordinator route");
}
