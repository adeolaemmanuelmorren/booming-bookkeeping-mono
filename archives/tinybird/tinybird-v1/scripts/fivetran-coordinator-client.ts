import type {
  BulkBootstrapCheckpoint,
  BulkBootstrapCheckpointStore,
  BulkBootstrapStateSeeder,
  PipelineId,
  PreparedScope,
} from '../worker/fivetran/contracts.ts';

const WORKER_URL = 'https://boom-tinybird-facts-v1.bill-3e3.workers.dev';
const MAX_SEED_BYTES = 900_000;

/** The live coordinator retains bootstrap state across one-time job restarts. */
export class FivetranCoordinatorClient implements BulkBootstrapCheckpointStore, BulkBootstrapStateSeeder {
  private readonly token: string;
  private readonly snapshotId: string;

  constructor(token: string, snapshotId: string) {
    if (!token || !snapshotId) throw new Error('Coordinator credentials and snapshot ID are required');
    this.token = token;
    this.snapshotId = snapshotId;
  }

  async initializePipeline(input: { pipeline: PipelineId; snapshotAt: string }): Promise<void> {
    await this.request('initialize', input.pipeline, { ...input, snapshotId: this.snapshotId });
  }

  loadBulkBootstrapCheckpoint(input: { pipeline: PipelineId; snapshotId: string }): Promise<BulkBootstrapCheckpoint | null> {
    if (input.snapshotId !== this.snapshotId) throw new Error('Coordinator checkpoint belongs to another snapshot');
    return this.request('checkpoint-load', input.pipeline, input);
  }

  async saveBulkBootstrapCheckpoint(checkpoint: BulkBootstrapCheckpoint): Promise<void> {
    if (checkpoint.snapshotId !== this.snapshotId) throw new Error('Coordinator checkpoint belongs to another snapshot');
    await this.request('checkpoint-save', checkpoint.pipeline, checkpoint);
  }

  async seedBulkBootstrapScopes(input: { pipeline: PipelineId; snapshotAt: string; prepared: PreparedScope[] }): Promise<void> {
    let batch: PreparedScope[] = [];
    let bytes = 0;
    const flush = async () => {
      if (!batch.length) return;
      await this.request('seed', input.pipeline, { ...input, prepared: batch });
      batch = [];
      bytes = 0;
    };
    for (const prepared of input.prepared) {
      const size = Buffer.byteLength(JSON.stringify(prepared));
      if (size > MAX_SEED_BYTES - 1_000) throw new Error('A conversion scope exceeds the coordinator request limit');
      if (batch.length && (batch.length >= 100 || bytes + size > MAX_SEED_BYTES - 1_000)) await flush();
      batch.push(prepared);
      bytes += size + 1;
    }
    await flush();
  }

  async finalizeBulkBootstrap(input: { pipeline: PipelineId; snapshotAt: string }): Promise<void> {
    await this.request('finalize', input.pipeline, input);
  }

  private async request<Result>(method: string, pipeline: PipelineId, input: unknown): Promise<Result> {
    const body = JSON.stringify(input);
    if (Buffer.byteLength(body) > MAX_SEED_BYTES) throw new Error('Coordinator request exceeds its byte limit');
    const response = await fetch(`${WORKER_URL}/admin/fivetran/${pipeline}/${method}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${this.token}`, 'Content-Type': 'application/json' },
      body,
      redirect: 'error',
      signal: AbortSignal.timeout(30_000),
    });
    if (!response.ok) {
      await response.body?.cancel();
      throw new Error(`Fivetran coordinator request failed with HTTP ${response.status}`);
    }
    return await response.json() as Result;
  }
}
