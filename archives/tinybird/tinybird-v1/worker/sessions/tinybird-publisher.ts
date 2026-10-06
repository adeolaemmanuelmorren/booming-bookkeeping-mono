import { WorkerEntrypoint } from 'cloudflare:workers';
import { Tinybird, type TinybirdEnv } from '../storage/tinybird.ts';
import { TinybirdSessionPublisher } from './tinybird-storage.ts';
import type { SessionPublisher, SessionSnapshot, SnapshotManifest, SnapshotRecord } from './session-publication.ts';

export class SessionSnapshotPublisher extends WorkerEntrypoint<TinybirdEnv> implements SessionPublisher {
  publishSnapshots(snapshots: SessionSnapshot[]): Promise<void> {
    return new TinybirdSessionPublisher(new Tinybird(this.env)).publishSnapshots(snapshots);
  }

  async appendRecords(records: SnapshotRecord[]): Promise<void> {
    return new TinybirdSessionPublisher(new Tinybird(this.env)).appendRecords(records);
  }

  async readRecords(manifest: SnapshotManifest): Promise<SnapshotRecord[]> {
    return new TinybirdSessionPublisher(new Tinybird(this.env)).readRecords(manifest);
  }

  async appendCommit(manifest: SnapshotManifest): Promise<void> {
    return new TinybirdSessionPublisher(new Tinybird(this.env)).appendCommit(manifest);
  }

  async readCommit(manifest: SnapshotManifest): Promise<SnapshotManifest | null> {
    return new TinybirdSessionPublisher(new Tinybird(this.env)).readCommit(manifest);
  }
}
