import assert from "node:assert/strict";

import {
  PIPELINE_TABLES,
  type ChangeWindow,
  type ConversionSourceTable,
  type FactCoordinatorStore,
  type PipelineId,
  type PipelineState,
  type PreparedScope,
  type ScopeState,
} from "../../worker/fivetran/contracts.ts";

interface DirtyState {
  status: "pending" | "reserved" | "prepared" | "published";
  sequence: number | null;
  prepared: PreparedScope | null;
}

export class MemoryStore implements FactCoordinatorStore {
  readonly pipelines = new Map<PipelineId, PipelineState>();
  readonly discoveries = new Map<string, { cursor: string; eof: boolean }>();
  readonly dirty = new Map<string, DirtyState>();
  readonly current = new Map<string, ScopeState>();
  readonly sequences = new Map<string, number>();

  async initializePipeline(input: {
    pipeline: PipelineId;
    snapshotAt: string;
  }): Promise<void> {
    const prior = this.pipelines.get(input.pipeline);

    if (prior) {
      if (prior.snapshotAt !== input.snapshotAt) throw new Error("snapshot changed");
      return;
    }

    this.pipelines.set(input.pipeline, {
      pipeline: input.pipeline,
      snapshotAt: input.snapshotAt,
      bootstrapComplete: false,
      completedObservationAt: null,
      activeWindow: null,
    });
  }

  async getPipeline(pipeline: PipelineId): Promise<PipelineState> {
    const state = this.pipelines.get(pipeline);
    if (!state) throw new Error("pipeline is not initialized");
    return structuredClone(state);
  }

  async beginBootstrap(input: {
    pipeline: PipelineId;
    snapshotAt: string;
  }): Promise<ChangeWindow> {
    const state = this.requiredPipeline(input.pipeline);
    if (state.activeWindow) return structuredClone(state.activeWindow);

    const window = {
      id: `bootstrap:${input.pipeline}:${input.snapshotAt}`,
      pipeline: input.pipeline,
      afterExclusive: input.snapshotAt,
      throughInclusive: input.snapshotAt,
    };
    state.activeWindow = window;
    this.discoveries.set(key(window.id, "bootstrap_scopes"), {
      cursor: "",
      eof: false,
    });
    return structuredClone(window);
  }

  async beginIncrementalWindow(input: {
    pipeline: PipelineId;
    afterExclusive: string;
    throughInclusive: string;
  }): Promise<ChangeWindow> {
    const state = this.requiredPipeline(input.pipeline);
    if (state.activeWindow) return structuredClone(state.activeWindow);

    const window = {
      id: `incremental:${input.pipeline}:${input.afterExclusive}:${input.throughInclusive}`,
      pipeline: input.pipeline,
      afterExclusive: input.afterExclusive,
      throughInclusive: input.throughInclusive,
    };
    state.activeWindow = window;

    for (const table of PIPELINE_TABLES[input.pipeline]) {
      this.discoveries.set(key(window.id, table), { cursor: "", eof: false });
    }

    return structuredClone(window);
  }

  async getDiscovery(input: {
    windowId: string;
    table: ConversionSourceTable | "bootstrap_scopes";
  }): Promise<{ cursor: string; eof: boolean }> {
    const value = this.discoveries.get(key(input.windowId, input.table));
    if (!value) throw new Error("missing discovery");
    return structuredClone(value);
  }

  async recordDiscoveryPage(input: {
    window: ChangeWindow;
    table: ConversionSourceTable | "bootstrap_scopes";
    expectedCursor: string;
    page: { scopeIds: string[]; nextCursor: string; eof: boolean };
  }): Promise<void> {
    const discoveryKey = key(input.window.id, input.table);
    const state = this.discoveries.get(discoveryKey);
    if (!state || state.cursor !== input.expectedCursor) throw new Error("cursor changed");

    for (const scopeId of new Set(input.page.scopeIds)) {
      const dirtyKey = key(input.window.id, scopeId);
      if (!this.dirty.has(dirtyKey)) {
        this.dirty.set(dirtyKey, {
          status: "pending",
          sequence: null,
          prepared: null,
        });
      }
    }

    this.discoveries.set(discoveryKey, {
      cursor: input.page.nextCursor,
      eof: input.page.eof,
    });
  }

  async listUnpreparedScopes(windowId: string, limit: number): Promise<string[]> {
    return [...this.dirty.entries()]
      .filter(([dirtyKey, state]) =>
        dirtyKey.startsWith(`${windowId}\u001f`) &&
        (state.status === "pending" || state.status === "reserved"),
      )
      .map(([dirtyKey]) => dirtyKey.slice(windowId.length + 1))
      .sort()
      .slice(0, limit);
  }

  async getScopeState(input: {
    pipeline: PipelineId;
    scopeId: string;
  }): Promise<ScopeState | null> {
    const value = this.current.get(key(input.pipeline, input.scopeId));
    return value ? structuredClone(value) : null;
  }

  async reserveScope(input: {
    windowId: string;
    scopeId: string;
  }): Promise<number> {
    const state = this.requiredDirty(input.windowId, input.scopeId);
    if (state.sequence !== null) return state.sequence;

    const window = this.requiredWindow(input.windowId);
    const sequenceKey = key(window.pipeline, input.scopeId);
    const sequence = (this.sequences.get(sequenceKey) ?? 0) + 1;
    this.sequences.set(sequenceKey, sequence);
    state.sequence = sequence;
    state.status = "reserved";
    return sequence;
  }

  async savePreparedScope(prepared: PreparedScope): Promise<void> {
    const state = this.requiredDirty(prepared.windowId, prepared.scopeId);

    if (state.prepared) {
      assert.deepEqual(state.prepared, prepared);
      return;
    }

    state.status = "prepared";
    state.prepared = structuredClone(prepared);
  }

  async listPreparedScopes(windowId: string, limit: number): Promise<PreparedScope[]> {
    return [...this.dirty.entries()]
      .filter(([dirtyKey, state]) =>
        dirtyKey.startsWith(`${windowId}\u001f`) && state.status === "prepared",
      )
      .map(([, state]) => structuredClone(state.prepared!))
      .sort((left, right) => left.scopeId.localeCompare(right.scopeId))
      .slice(0, limit);
  }

  async markPublished(preparedScopes: PreparedScope[]): Promise<void> {
    for (const prepared of preparedScopes) {
      const dirty = this.requiredDirty(prepared.windowId, prepared.scopeId);
      assert.deepEqual(dirty.prepared, prepared);
      dirty.status = "published";
      dirty.prepared = null;
      const window = this.requiredWindow(prepared.windowId);
      this.current.set(key(window.pipeline, prepared.scopeId), {
        rows: structuredClone(prepared.replacement.rows),
        compactState: structuredClone(prepared.compactState),
        observationSequence: prepared.replacement.observation_sequence,
      });
    }
  }

  async finishWindow(window: ChangeWindow): Promise<void> {
    for (const [discoveryKey, state] of this.discoveries) {
      if (discoveryKey.startsWith(`${window.id}\u001f`) && !state.eof) {
        throw new Error("discovery is incomplete");
      }
    }

    for (const [dirtyKey, state] of this.dirty) {
      if (dirtyKey.startsWith(`${window.id}\u001f`) && state.status !== "published") {
        throw new Error("publication is incomplete");
      }
    }

    const pipeline = this.requiredPipeline(window.pipeline);
    pipeline.bootstrapComplete = true;
    pipeline.completedObservationAt = window.throughInclusive;
    pipeline.activeWindow = null;
  }

  private requiredPipeline(pipeline: PipelineId): PipelineState {
    const state = this.pipelines.get(pipeline);
    if (!state) throw new Error("pipeline is not initialized");
    return state;
  }

  private requiredWindow(windowId: string): ChangeWindow {
    for (const pipeline of this.pipelines.values()) {
      if (pipeline.activeWindow?.id === windowId) return pipeline.activeWindow;
    }

    throw new Error("missing window");
  }

  private requiredDirty(windowId: string, scopeId: string): DirtyState {
    const state = this.dirty.get(key(windowId, scopeId));
    if (!state) throw new Error("missing dirty scope");
    return state;
  }
}

function key(left: string, right: string): string {
  return `${left}\u001f${right}`;
}
