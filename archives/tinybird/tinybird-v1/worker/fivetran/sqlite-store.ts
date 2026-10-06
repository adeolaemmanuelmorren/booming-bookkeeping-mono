import {
  PIPELINE_TABLES,
  type BulkBootstrapCheckpoint,
  type BulkBootstrapCheckpointStore,
  type BulkBootstrapStateSeeder,
  type ChangeWindow,
  type ConversionSourceTable,
  type FactCoordinatorStore,
  type JsonRecord,
  type PipelineId,
  type PipelineState,
  type PreparedScope,
  type ScopeState,
} from "./contracts.ts";
import { canonicalJson, sha256 } from "./json.ts";
import { compareTimestamps, parseTimestamp } from "./timestamp.ts";

interface SqlCursor<Row> {
  toArray(): Row[];
}

interface SqlExecutor {
  exec<Row extends Record<string, unknown>>(
    query: string,
    ...bindings: unknown[]
  ): SqlCursor<Row>;
}

export interface TransactionalSqlStorage {
  sql: SqlExecutor;
  transactionSync<Result>(callback: () => Result): Result;
}

interface PipelineRow extends Record<string, unknown> {
  pipeline: PipelineId;
  snapshot_at: string;
  bootstrap_complete: number;
  completed_observation_at: string | null;
  active_window_id: string | null;
}

interface WindowRow extends Record<string, unknown> {
  id: string;
  pipeline: PipelineId;
  kind: "bootstrap" | "incremental";
  after_exclusive: string;
  through_inclusive: string;
  status: "active" | "complete";
}

interface DirtyRow extends Record<string, unknown> {
  window_id: string;
  scope_id: string;
  status: "pending" | "reserved" | "prepared" | "published";
  input_hash: string | null;
  observation_sequence: number | null;
  replacement_json: string | null;
  compact_state_json: string | null;
}

export class SqliteFactCoordinatorStore implements
  FactCoordinatorStore,
  BulkBootstrapCheckpointStore,
  BulkBootstrapStateSeeder
{
  private readonly storage: TransactionalSqlStorage;
  private readonly clock: Pick<typeof Date, "now">;

  constructor(
    storage: TransactionalSqlStorage,
    clock: Pick<typeof Date, "now"> = Date,
  ) {
    this.storage = storage;
    this.clock = clock;
  }

  initializeSchema(): void {
    this.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS fivetran_fact_pipelines (
        pipeline TEXT PRIMARY KEY,
        snapshot_at TEXT NOT NULL,
        bootstrap_complete INTEGER NOT NULL DEFAULT 0,
        completed_observation_at TEXT,
        active_window_id TEXT,
        updated_at_ms INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS fivetran_fact_windows (
        id TEXT PRIMARY KEY,
        pipeline TEXT NOT NULL,
        kind TEXT NOT NULL CHECK (kind IN ('bootstrap', 'incremental')),
        after_exclusive TEXT NOT NULL,
        through_inclusive TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('active', 'complete')),
        scope_count INTEGER,
        receipt_hash TEXT,
        created_at_ms INTEGER NOT NULL,
        completed_at_ms INTEGER
      );

      CREATE TABLE IF NOT EXISTS fivetran_fact_discovery (
        window_id TEXT NOT NULL,
        source_table TEXT NOT NULL,
        cursor TEXT NOT NULL,
        eof INTEGER NOT NULL DEFAULT 0,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (window_id, source_table)
      );

      CREATE TABLE IF NOT EXISTS fivetran_fact_dirty_scopes (
        window_id TEXT NOT NULL,
        scope_id TEXT NOT NULL,
        status TEXT NOT NULL CHECK (
          status IN ('pending', 'reserved', 'prepared', 'published')
        ),
        input_hash TEXT,
        observation_sequence INTEGER,
        replacement_json TEXT,
        compact_state_json TEXT,
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (window_id, scope_id)
      );

      CREATE INDEX IF NOT EXISTS fivetran_fact_dirty_status
        ON fivetran_fact_dirty_scopes (window_id, status, scope_id);

      CREATE TABLE IF NOT EXISTS fivetran_fact_sequences (
        pipeline TEXT NOT NULL,
        scope_id TEXT NOT NULL,
        value INTEGER NOT NULL,
        PRIMARY KEY (pipeline, scope_id)
      );

      CREATE TABLE IF NOT EXISTS fivetran_fact_scope_state (
        pipeline TEXT NOT NULL,
        scope_id TEXT NOT NULL,
        observation_sequence INTEGER NOT NULL,
        rows_json TEXT NOT NULL,
        compact_state_json TEXT NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (pipeline, scope_id)
      );

      CREATE TABLE IF NOT EXISTS fivetran_fact_bootstrap_progress (
        snapshot_id TEXT NOT NULL,
        pipeline TEXT NOT NULL,
        snapshot_at TEXT NOT NULL,
        after_cursor TEXT NOT NULL,
        complete INTEGER NOT NULL,
        published_scopes INTEGER NOT NULL,
        identity_fact_count INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (snapshot_id, pipeline)
      );
    `);
  }

  async initializePipeline(input: {
    pipeline: PipelineId;
    snapshotAt: string;
  }): Promise<void> {
    const snapshotAt = parseTimestamp(input.snapshotAt, "snapshotAt").iso;

    this.storage.transactionSync(() => {
      const existing = this.pipelineRow(input.pipeline);

      if (existing) {
        if (existing.snapshot_at !== snapshotAt) {
          throw new Error(`${input.pipeline} is bound to another snapshot`);
        }
        return;
      }

      this.storage.sql.exec(
        `INSERT INTO fivetran_fact_pipelines (
          pipeline, snapshot_at, bootstrap_complete, updated_at_ms
        ) VALUES (?, ?, 0, ?)`,
        input.pipeline,
        snapshotAt,
        this.clock.now(),
      );
    });
  }

  async getPipeline(pipeline: PipelineId): Promise<PipelineState> {
    const row = this.pipelineRow(pipeline);
    if (!row) throw new Error(`${pipeline} has not been initialized`);

    let activeWindow: ChangeWindow | null = null;

    if (row.active_window_id) {
      activeWindow = toChangeWindow(this.windowRow(row.active_window_id));
    }

    return {
      pipeline,
      snapshotAt: row.snapshot_at,
      bootstrapComplete: row.bootstrap_complete === 1,
      completedObservationAt: row.completed_observation_at,
      activeWindow,
    };
  }

  async beginBootstrap(input: {
    pipeline: PipelineId;
    snapshotAt: string;
  }): Promise<ChangeWindow> {
    const snapshotAt = parseTimestamp(input.snapshotAt, "snapshotAt").iso;

    return this.storage.transactionSync(() => {
      const pipeline = requiredPipeline(this.pipelineRow(input.pipeline), input.pipeline);

      if (pipeline.snapshot_at !== snapshotAt) {
        throw new Error(`${input.pipeline} bootstrap snapshot changed`);
      }

      if (pipeline.bootstrap_complete === 1) {
        throw new Error(`${input.pipeline} bootstrap is already complete`);
      }

      if (pipeline.active_window_id) {
        return toChangeWindow(this.windowRow(pipeline.active_window_id));
      }

      const window = newWindow({
        pipeline: input.pipeline,
        kind: "bootstrap",
        afterExclusive: snapshotAt,
        throughInclusive: snapshotAt,
      });
      this.insertWindow(window, "bootstrap", ["bootstrap_scopes"]);
      return window;
    });
  }

  async beginIncrementalWindow(input: {
    pipeline: PipelineId;
    afterExclusive: string;
    throughInclusive: string;
  }): Promise<ChangeWindow> {
    const afterExclusive = parseTimestamp(input.afterExclusive, "afterExclusive").iso;
    const throughInclusive = parseTimestamp(input.throughInclusive, "throughInclusive").iso;

    if (compareTimestamps(throughInclusive, afterExclusive) <= 0) {
      throw new Error("incremental window must move forward");
    }

    return this.storage.transactionSync(() => {
      const pipeline = requiredPipeline(this.pipelineRow(input.pipeline), input.pipeline);

      if (pipeline.bootstrap_complete !== 1) {
        throw new Error(`${input.pipeline} bootstrap is not complete`);
      }

      if (pipeline.completed_observation_at !== afterExclusive) {
        throw new Error(`${input.pipeline} incremental cursor changed`);
      }

      if (pipeline.active_window_id) {
        const active = toChangeWindow(this.windowRow(pipeline.active_window_id));

        if (
          active.afterExclusive !== afterExclusive ||
          active.throughInclusive !== throughInclusive
        ) {
          throw new Error(`${input.pipeline} already has another active window`);
        }

        return active;
      }

      const window = newWindow({
        pipeline: input.pipeline,
        kind: "incremental",
        afterExclusive,
        throughInclusive,
      });
      this.insertWindow(window, "incremental", PIPELINE_TABLES[input.pipeline]);
      return window;
    });
  }

  async getDiscovery(input: {
    windowId: string;
    table: ConversionSourceTable | "bootstrap_scopes";
  }): Promise<{ cursor: string; eof: boolean }> {
    const row = this.storage.sql.exec<{
      cursor: string;
      eof: number;
    }>(
      `SELECT cursor, eof
       FROM fivetran_fact_discovery
       WHERE window_id = ? AND source_table = ?`,
      input.windowId,
      input.table,
    ).toArray()[0];

    if (!row) throw new Error(`Missing discovery cursor for ${input.table}`);
    return { cursor: row.cursor, eof: row.eof === 1 };
  }

  async recordDiscoveryPage(input: {
    window: ChangeWindow;
    table: ConversionSourceTable | "bootstrap_scopes";
    expectedCursor: string;
    page: { scopeIds: string[]; nextCursor: string; eof: boolean };
  }): Promise<void> {
    validatePage(input.expectedCursor, input.page);

    this.storage.transactionSync(() => {
      const discovery = this.storage.sql.exec<{
        cursor: string;
        eof: number;
      }>(
        `SELECT cursor, eof
         FROM fivetran_fact_discovery
         WHERE window_id = ? AND source_table = ?`,
        input.window.id,
        input.table,
      ).toArray()[0];

      if (!discovery) throw new Error(`Missing discovery cursor for ${input.table}`);
      if (discovery.cursor !== input.expectedCursor) {
        throw new Error(`Discovery cursor changed for ${input.table}`);
      }
      if (discovery.eof === 1) {
        if (input.page.scopeIds.length || !input.page.eof) {
          throw new Error(`Discovery already completed for ${input.table}`);
        }
        return;
      }

      const now = this.clock.now();

      for (const scopeId of [...new Set(input.page.scopeIds)].sort()) {
        assertScope(input.window.pipeline, scopeId);
        this.storage.sql.exec(
          `INSERT OR IGNORE INTO fivetran_fact_dirty_scopes (
            window_id, scope_id, status, created_at_ms, updated_at_ms
          ) VALUES (?, ?, 'pending', ?, ?)`,
          input.window.id,
          scopeId,
          now,
          now,
        );
      }

      this.storage.sql.exec(
        `UPDATE fivetran_fact_discovery
         SET cursor = ?, eof = ?, updated_at_ms = ?
         WHERE window_id = ? AND source_table = ?`,
        input.page.nextCursor,
        input.page.eof ? 1 : 0,
        now,
        input.window.id,
        input.table,
      );
    });
  }

  async listUnpreparedScopes(windowId: string, limit: number): Promise<string[]> {
    assertLimit(limit);
    return this.storage.sql.exec<{ scope_id: string }>(
      `SELECT scope_id
       FROM fivetran_fact_dirty_scopes
       WHERE window_id = ? AND status IN ('pending', 'reserved')
       ORDER BY scope_id
       LIMIT ?`,
      windowId,
      limit,
    ).toArray().map((row) => row.scope_id);
  }

  async getScopeState(input: {
    pipeline: PipelineId;
    scopeId: string;
  }): Promise<ScopeState | null> {
    const row = this.storage.sql.exec<{
      rows_json: string;
      compact_state_json: string;
      observation_sequence: number;
    }>(
      `SELECT rows_json, compact_state_json, observation_sequence
       FROM fivetran_fact_scope_state
       WHERE pipeline = ? AND scope_id = ?`,
      input.pipeline,
      input.scopeId,
    ).toArray()[0];

    if (!row) return null;
    return {
      rows: parseJson(row.rows_json, `scope rows ${input.scopeId}`) as JsonRecord[],
      compactState: parseJson(
        row.compact_state_json,
        `scope state ${input.scopeId}`,
      ) as JsonRecord,
      observationSequence: row.observation_sequence,
    };
  }

  async reserveScope(input: {
    windowId: string;
    scopeId: string;
  }): Promise<number> {
    return this.storage.transactionSync(() => {
      const dirty = requiredDirty(this.dirtyRow(input.windowId, input.scopeId));

      if (dirty.observation_sequence !== null) {
        return dirty.observation_sequence;
      }

      const window = this.windowRow(input.windowId);
      const current = this.storage.sql.exec<{ value: number }>(
        `SELECT value FROM fivetran_fact_sequences
         WHERE pipeline = ? AND scope_id = ?`,
        window.pipeline,
        input.scopeId,
      ).toArray()[0];
      const sequence = (current?.value ?? 0) + 1;

      this.storage.sql.exec(
        `INSERT INTO fivetran_fact_sequences (pipeline, scope_id, value)
         VALUES (?, ?, ?)
         ON CONFLICT(pipeline, scope_id) DO UPDATE SET value = excluded.value`,
        window.pipeline,
        input.scopeId,
        sequence,
      );
      this.storage.sql.exec(
        `UPDATE fivetran_fact_dirty_scopes
         SET status = 'reserved', observation_sequence = ?, updated_at_ms = ?
         WHERE window_id = ? AND scope_id = ?`,
        sequence,
        this.clock.now(),
        input.windowId,
        input.scopeId,
      );
      return sequence;
    });
  }

  async savePreparedScope(prepared: PreparedScope): Promise<void> {
    this.storage.transactionSync(() => {
      const dirty = requiredDirty(this.dirtyRow(prepared.windowId, prepared.scopeId));
      const sequence = dirty.observation_sequence;

      if (sequence === null) throw new Error(`${prepared.scopeId} is not reserved`);
      if (prepared.replacement.observation_sequence !== sequence) {
        throw new Error(`${prepared.scopeId} replacement has the wrong sequence`);
      }

      const replacementJson = canonicalJson(prepared.replacement);
      const compactStateJson = canonicalJson(prepared.compactState);

      if (dirty.status === "prepared" || dirty.status === "published") {
        if (
          dirty.input_hash !== prepared.inputHash ||
          dirty.replacement_json !== replacementJson ||
          dirty.compact_state_json !== compactStateJson
        ) {
          throw new Error(`${prepared.scopeId} changed after preparation`);
        }
        return;
      }

      this.storage.sql.exec(
        `UPDATE fivetran_fact_dirty_scopes
         SET
           status = 'prepared',
           input_hash = ?,
           replacement_json = ?,
           compact_state_json = ?,
           updated_at_ms = ?
         WHERE window_id = ? AND scope_id = ?`,
        prepared.inputHash,
        replacementJson,
        compactStateJson,
        this.clock.now(),
        prepared.windowId,
        prepared.scopeId,
      );
    });
  }

  async listPreparedScopes(windowId: string, limit: number): Promise<PreparedScope[]> {
    assertLimit(limit);
    return this.storage.sql.exec<DirtyRow>(
      `SELECT
         window_id,
         scope_id,
         status,
         input_hash,
         observation_sequence,
         replacement_json,
         compact_state_json
       FROM fivetran_fact_dirty_scopes
       WHERE window_id = ? AND status = 'prepared'
       ORDER BY scope_id
       LIMIT ?`,
      windowId,
      limit,
    ).toArray().map(toPreparedScope);
  }

  async markPublished(preparedScopes: PreparedScope[]): Promise<void> {
    if (!preparedScopes.length) return;

    this.storage.transactionSync(() => {
      const now = this.clock.now();

      for (const prepared of preparedScopes) {
        const dirty = requiredDirty(this.dirtyRow(prepared.windowId, prepared.scopeId));

        if (dirty.status === "published") continue;
        if (dirty.status !== "prepared") {
          throw new Error(`${prepared.scopeId} is not prepared`);
        }

        assertPreparedMatches(dirty, prepared);
        const pipeline = this.windowRow(prepared.windowId).pipeline;
        const compactRows = prepared.replacement.rows.map(compactPriorFact);

        this.storage.sql.exec(
          `INSERT INTO fivetran_fact_scope_state (
            pipeline,
            scope_id,
            observation_sequence,
            rows_json,
            compact_state_json,
            updated_at_ms
          ) VALUES (?, ?, ?, ?, ?, ?)
          ON CONFLICT(pipeline, scope_id) DO UPDATE SET
            observation_sequence = excluded.observation_sequence,
            rows_json = excluded.rows_json,
            compact_state_json = excluded.compact_state_json,
            updated_at_ms = excluded.updated_at_ms`,
          pipeline,
          prepared.scopeId,
          prepared.replacement.observation_sequence,
          canonicalJson(compactRows),
          canonicalJson(prepared.compactState),
          now,
        );
        this.storage.sql.exec(
          `UPDATE fivetran_fact_dirty_scopes
           SET
             status = 'published',
             replacement_json = NULL,
             compact_state_json = NULL,
             updated_at_ms = ?
           WHERE window_id = ? AND scope_id = ?`,
          now,
          prepared.windowId,
          prepared.scopeId,
        );
      }
    });
  }

  async finishWindow(window: ChangeWindow): Promise<void> {
    this.storage.transactionSync(() => {
      const stored = this.windowRow(window.id);
      if (stored.status === "complete") return;

      const incompleteDiscovery = this.storage.sql.exec<{ count: number }>(
        `SELECT count(*) AS count
         FROM fivetran_fact_discovery
         WHERE window_id = ? AND eof = 0`,
        window.id,
      ).toArray()[0]?.count ?? 0;
      if (incompleteDiscovery > 0) throw new Error("window discovery is incomplete");

      const unpublished = this.storage.sql.exec<{ count: number }>(
        `SELECT count(*) AS count
         FROM fivetran_fact_dirty_scopes
         WHERE window_id = ? AND status != 'published'`,
        window.id,
      ).toArray()[0]?.count ?? 0;
      if (unpublished > 0) throw new Error("window still has unpublished scopes");

      const receipts = this.storage.sql.exec<{
        scope_id: string;
        input_hash: string;
        observation_sequence: number;
      }>(
        `SELECT scope_id, input_hash, observation_sequence
         FROM fivetran_fact_dirty_scopes
         WHERE window_id = ?
         ORDER BY scope_id`,
        window.id,
      ).toArray();
      const receiptHash = sha256(receipts);
      const now = this.clock.now();

      this.storage.sql.exec(
        `UPDATE fivetran_fact_windows
         SET
           status = 'complete',
           scope_count = ?,
           receipt_hash = ?,
           completed_at_ms = ?
         WHERE id = ?`,
        receipts.length,
        receiptHash,
        now,
        window.id,
      );

      if (stored.kind === "bootstrap") {
        this.storage.sql.exec(
          `UPDATE fivetran_fact_pipelines
           SET
             bootstrap_complete = 1,
             completed_observation_at = snapshot_at,
             active_window_id = NULL,
             updated_at_ms = ?
           WHERE pipeline = ? AND active_window_id = ?`,
          now,
          window.pipeline,
          window.id,
        );
      } else {
        this.storage.sql.exec(
          `UPDATE fivetran_fact_pipelines
           SET
             completed_observation_at = ?,
             active_window_id = NULL,
             updated_at_ms = ?
           WHERE pipeline = ? AND active_window_id = ?`,
          window.throughInclusive,
          now,
          window.pipeline,
          window.id,
        );
      }

      this.storage.sql.exec(
        "DELETE FROM fivetran_fact_discovery WHERE window_id = ?",
        window.id,
      );
      this.storage.sql.exec(
        "DELETE FROM fivetran_fact_dirty_scopes WHERE window_id = ?",
        window.id,
      );
    });
  }

  async loadBulkBootstrapCheckpoint(input: {
    snapshotId: string;
    pipeline: PipelineId;
  }): Promise<BulkBootstrapCheckpoint | null> {
    const row = this.storage.sql.exec<{
      snapshot_id: string;
      pipeline: PipelineId;
      snapshot_at: string;
      after_cursor: string;
      complete: number;
      published_scopes: number;
      identity_fact_count: number;
    }>(
      `SELECT
         snapshot_id,
         pipeline,
         snapshot_at,
         after_cursor,
         complete,
         published_scopes,
         identity_fact_count
       FROM fivetran_fact_bootstrap_progress
       WHERE snapshot_id = ? AND pipeline = ?`,
      input.snapshotId,
      input.pipeline,
    ).toArray()[0];

    if (!row) return null;
    return {
      snapshotId: row.snapshot_id,
      snapshotAt: row.snapshot_at,
      pipeline: row.pipeline,
      afterCursor: row.after_cursor,
      complete: row.complete === 1,
      publishedScopes: row.published_scopes,
      identityFactCount: row.identity_fact_count,
    };
  }

  async saveBulkBootstrapCheckpoint(
    checkpoint: BulkBootstrapCheckpoint,
  ): Promise<void> {
    this.storage.transactionSync(() => {
      const prior = this.storage.sql.exec<{
        snapshot_at: string;
        after_cursor: string;
        complete: number;
        published_scopes: number;
        identity_fact_count: number;
      }>(
        `SELECT
           snapshot_at,
           after_cursor,
           complete,
           published_scopes,
           identity_fact_count
         FROM fivetran_fact_bootstrap_progress
         WHERE snapshot_id = ? AND pipeline = ?`,
        checkpoint.snapshotId,
        checkpoint.pipeline,
      ).toArray()[0];

      if (prior) {
        if (prior.snapshot_at !== checkpoint.snapshotAt) {
          throw new Error("Bulk bootstrap checkpoint snapshot changed");
        }
        if (prior.complete === 1 && !checkpoint.complete) {
          throw new Error("Bulk bootstrap checkpoint cannot reopen");
        }
        if (
          checkpoint.publishedScopes < prior.published_scopes ||
          checkpoint.identityFactCount < prior.identity_fact_count
        ) {
          throw new Error("Bulk bootstrap checkpoint cannot move backwards");
        }
      }

      this.storage.sql.exec(
        `INSERT INTO fivetran_fact_bootstrap_progress (
          snapshot_id,
          pipeline,
          snapshot_at,
          after_cursor,
          complete,
          published_scopes,
          identity_fact_count,
          updated_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(snapshot_id, pipeline) DO UPDATE SET
          after_cursor = excluded.after_cursor,
          complete = excluded.complete,
          published_scopes = excluded.published_scopes,
          identity_fact_count = excluded.identity_fact_count,
          updated_at_ms = excluded.updated_at_ms`,
        checkpoint.snapshotId,
        checkpoint.pipeline,
        checkpoint.snapshotAt,
        checkpoint.afterCursor,
        checkpoint.complete ? 1 : 0,
        checkpoint.publishedScopes,
        checkpoint.identityFactCount,
        this.clock.now(),
      );
    });
  }

  async seedBulkBootstrapScopes(input: {
    pipeline: PipelineId;
    snapshotAt: string;
    prepared: PreparedScope[];
  }): Promise<void> {
    const snapshotAt = parseTimestamp(input.snapshotAt, "snapshotAt").iso;

    this.storage.transactionSync(() => {
      const pipeline = requiredPipeline(this.pipelineRow(input.pipeline), input.pipeline);
      if (pipeline.snapshot_at !== snapshotAt) {
        throw new Error("Bulk bootstrap seed belongs to another snapshot");
      }
      if (pipeline.active_window_id) {
        throw new Error("Bulk bootstrap cannot seed an active incremental coordinator");
      }

      const now = this.clock.now();

      for (const prepared of input.prepared) {
        assertScope(input.pipeline, prepared.scopeId);
        assertBootstrapPrepared(input.pipeline, snapshotAt, prepared);
        if (prepared.replacement.observation_sequence !== 1) {
          throw new Error("Bulk bootstrap observation sequence must be one");
        }

        const rowsJson = canonicalJson(
          prepared.replacement.rows.map(compactPriorFact),
        );
        const stateJson = canonicalJson(prepared.compactState);
        const existing = this.storage.sql.exec<{
          observation_sequence: number;
          rows_json: string;
          compact_state_json: string;
        }>(
          `SELECT observation_sequence, rows_json, compact_state_json
           FROM fivetran_fact_scope_state
           WHERE pipeline = ? AND scope_id = ?`,
          input.pipeline,
          prepared.scopeId,
        ).toArray()[0];

        if (existing) {
          if (
            existing.observation_sequence !== 1 ||
            existing.rows_json !== rowsJson ||
            existing.compact_state_json !== stateJson
          ) {
            throw new Error(`${prepared.scopeId} bootstrap seed changed`);
          }
          continue;
        }

        this.storage.sql.exec(
          `INSERT INTO fivetran_fact_scope_state (
            pipeline,
            scope_id,
            observation_sequence,
            rows_json,
            compact_state_json,
            updated_at_ms
          ) VALUES (?, ?, 1, ?, ?, ?)`,
          input.pipeline,
          prepared.scopeId,
          rowsJson,
          stateJson,
          now,
        );
        this.storage.sql.exec(
          `INSERT INTO fivetran_fact_sequences (pipeline, scope_id, value)
           VALUES (?, ?, 1)
           ON CONFLICT(pipeline, scope_id) DO UPDATE SET value = max(value, 1)`,
          input.pipeline,
          prepared.scopeId,
        );
      }
    });
  }

  async finalizeBulkBootstrap(input: {
    pipeline: PipelineId;
    snapshotAt: string;
  }): Promise<void> {
    const snapshotAt = parseTimestamp(input.snapshotAt, "snapshotAt").iso;

    this.storage.transactionSync(() => {
      const pipeline = requiredPipeline(this.pipelineRow(input.pipeline), input.pipeline);
      if (pipeline.snapshot_at !== snapshotAt) {
        throw new Error("Bulk bootstrap finalization snapshot changed");
      }
      if (pipeline.active_window_id) {
        throw new Error("Bulk bootstrap cannot finalize with an active window");
      }
      if (pipeline.bootstrap_complete === 1) {
        if (pipeline.completed_observation_at !== snapshotAt) {
          throw new Error("Bulk bootstrap was finalized at another cursor");
        }
        return;
      }

      this.storage.sql.exec(
        `UPDATE fivetran_fact_pipelines
         SET
           bootstrap_complete = 1,
           completed_observation_at = ?,
           updated_at_ms = ?
         WHERE pipeline = ?`,
        snapshotAt,
        this.clock.now(),
        input.pipeline,
      );
    });
  }

  private pipelineRow(pipeline: PipelineId): PipelineRow | null {
    return this.storage.sql.exec<PipelineRow>(
      `SELECT
         pipeline,
         snapshot_at,
         bootstrap_complete,
         completed_observation_at,
         active_window_id
       FROM fivetran_fact_pipelines
       WHERE pipeline = ?`,
      pipeline,
    ).toArray()[0] ?? null;
  }

  private windowRow(windowId: string): WindowRow {
    const row = this.storage.sql.exec<WindowRow>(
      `SELECT id, pipeline, kind, after_exclusive, through_inclusive, status
       FROM fivetran_fact_windows
       WHERE id = ?`,
      windowId,
    ).toArray()[0];

    if (!row) throw new Error(`Unknown fact window ${windowId}`);
    return row;
  }

  private dirtyRow(windowId: string, scopeId: string): DirtyRow | null {
    return this.storage.sql.exec<DirtyRow>(
      `SELECT
         window_id,
         scope_id,
         status,
         input_hash,
         observation_sequence,
         replacement_json,
         compact_state_json
       FROM fivetran_fact_dirty_scopes
       WHERE window_id = ? AND scope_id = ?`,
      windowId,
      scopeId,
    ).toArray()[0] ?? null;
  }

  private insertWindow(
    window: ChangeWindow,
    kind: "bootstrap" | "incremental",
    tables: readonly string[],
  ): void {
    const now = this.clock.now();
    this.storage.sql.exec(
      `INSERT INTO fivetran_fact_windows (
        id,
        pipeline,
        kind,
        after_exclusive,
        through_inclusive,
        status,
        created_at_ms
      ) VALUES (?, ?, ?, ?, ?, 'active', ?)`,
      window.id,
      window.pipeline,
      kind,
      window.afterExclusive,
      window.throughInclusive,
      now,
    );

    for (const table of tables) {
      this.storage.sql.exec(
        `INSERT INTO fivetran_fact_discovery (
          window_id, source_table, cursor, eof, updated_at_ms
        ) VALUES (?, ?, '', 0, ?)`,
        window.id,
        table,
        now,
      );
    }

    this.storage.sql.exec(
      `UPDATE fivetran_fact_pipelines
       SET active_window_id = ?, updated_at_ms = ?
       WHERE pipeline = ?`,
      window.id,
      now,
      window.pipeline,
    );
  }
}

function newWindow(input: {
  pipeline: PipelineId;
  kind: "bootstrap" | "incremental";
  afterExclusive: string;
  throughInclusive: string;
}): ChangeWindow {
  const digest = sha256(input).slice(0, 24);
  return {
    id: `${input.kind}:${input.pipeline}:${digest}`,
    pipeline: input.pipeline,
    afterExclusive: input.afterExclusive,
    throughInclusive: input.throughInclusive,
  };
}

function toChangeWindow(row: WindowRow): ChangeWindow {
  return {
    id: row.id,
    pipeline: row.pipeline,
    afterExclusive: row.after_exclusive,
    throughInclusive: row.through_inclusive,
  };
}

function requiredPipeline(row: PipelineRow | null, pipeline: PipelineId): PipelineRow {
  if (!row) throw new Error(`${pipeline} has not been initialized`);
  return row;
}

function requiredDirty(row: DirtyRow | null): DirtyRow {
  if (!row) throw new Error("Unknown dirty scope");
  return row;
}

function toPreparedScope(row: DirtyRow): PreparedScope {
  if (!row.input_hash || !row.replacement_json || !row.compact_state_json) {
    throw new Error(`${row.scope_id} has an incomplete prepared payload`);
  }

  return {
    windowId: row.window_id,
    scopeId: row.scope_id,
    inputHash: row.input_hash,
    replacement: parseJson(
      row.replacement_json,
      `${row.scope_id} replacement`,
    ) as PreparedScope["replacement"],
    compactState: parseJson(
      row.compact_state_json,
      `${row.scope_id} compact state`,
    ) as JsonRecord,
  };
}

function assertPreparedMatches(row: DirtyRow, prepared: PreparedScope): void {
  if (
    row.input_hash !== prepared.inputHash ||
    row.replacement_json !== canonicalJson(prepared.replacement) ||
    row.compact_state_json !== canonicalJson(prepared.compactState)
  ) {
    throw new Error(`${prepared.scopeId} publication payload changed`);
  }
}

function compactPriorFact(row: JsonRecord): JsonRecord {
  const result = structuredClone(row);
  delete result.raw_evidence;
  delete result.tombstone_evidence;
  return result;
}

function parseJson(value: string, fieldName: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    throw new Error(`${fieldName} contains invalid JSON`);
  }
}

function validatePage(
  expectedCursor: string,
  page: { scopeIds: string[]; nextCursor: string; eof: boolean },
): void {
  if (!Array.isArray(page.scopeIds)) throw new TypeError("scopeIds must be an array");
  if (typeof page.nextCursor !== "string") {
    throw new TypeError("nextCursor must be a string");
  }
  if (!page.eof && page.nextCursor === expectedCursor) {
    throw new Error("Discovery page did not advance its cursor");
  }
}

function assertScope(pipeline: PipelineId, scopeId: string): void {
  const prefix = pipeline === "stripe_main"
    ? "stripe:main:charge:"
    : pipeline === "stripe_kajabi"
      ? "stripe:kajabi:charge:"
      : "activecampaign:contact:";

  if (!scopeId.startsWith(prefix) || scopeId.length === prefix.length) {
    throw new Error(`${scopeId} does not belong to ${pipeline}`);
  }
}

function assertBootstrapPrepared(
  pipeline: PipelineId,
  snapshotAt: string,
  prepared: PreparedScope,
): void {
  if (prepared.replacement.scope_id !== prepared.scopeId) {
    throw new Error("Bulk bootstrap replacement scope does not match its seed");
  }
  if (parseTimestamp(prepared.replacement.observed_at, "observed_at").iso !== snapshotAt) {
    throw new Error("Bulk bootstrap replacement belongs to another snapshot");
  }

  const expected = pipeline === "stripe_main"
    ? { source: "stripe", account: "main" }
    : pipeline === "stripe_kajabi"
      ? { source: "stripe", account: "kajabi" }
      : { source: "activecampaign", account: "default" };

  if (
    prepared.replacement.source !== expected.source ||
    prepared.replacement.source_account !== expected.account
  ) {
    throw new Error("Bulk bootstrap replacement belongs to another pipeline");
  }
}

function assertLimit(limit: number): void {
  if (!Number.isSafeInteger(limit) || limit < 1) {
    throw new TypeError("limit must be a positive integer");
  }
}
