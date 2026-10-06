import { cloneEvidence } from "../../worker/conversions/shared.mjs";

export class MemoryStore {
  constructor() {
    this.cursors = new Map();
    this.inbox = new Map();
    this.outbox = new Map();
    this.sequences = new Map();
    this.state = new Map();
    this.publishedRows = new Map();
  }

  async getCursor(key) {
    return cloneOrNull(this.cursors.get(key));
  }

  async setCursor(key, value) {
    this.cursors.set(key, cloneEvidence(value));
  }

  async putInboxIfAbsent(record) {
    if (this.inbox.has(record.id)) {
      return false;
    }

    this.inbox.set(record.id, {
      ...cloneEvidence(record),
      status: "pending",
      progress: null,
    });
    return true;
  }

  async listPendingInbox({ source, sourceAccount, limit }) {
    return [...this.inbox.values()]
      .filter(
        (record) =>
          record.status === "pending" &&
          record.source === source &&
          record.sourceAccount === sourceAccount,
      )
      .slice(0, limit)
      .map(cloneEvidence);
  }

  async saveInboxProgress(id, progress) {
    const record = this.requireInbox(id);
    record.progress = cloneEvidence(progress);
  }

  async markInboxOutboxed(id, outboxId) {
    const record = this.requireInbox(id);
    record.status = "outboxed";
    record.outboxId = outboxId;
  }

  async markInboxProcessed(id, outboxId) {
    const record = this.requireInbox(id);
    record.status = "processed";
    record.outboxId = outboxId;
  }

  async nextSequence(key) {
    const next = (this.sequences.get(key) ?? 0) + 1;
    this.sequences.set(key, next);
    return next;
  }

  async putOutboxIfAbsent(record) {
    if (this.outbox.has(record.id)) {
      return false;
    }

    this.outbox.set(record.id, {
      ...cloneEvidence(record),
      status: "pending",
    });
    return true;
  }

  async listPendingOutbox({ source, sourceAccount, limit }) {
    return [...this.outbox.values()]
      .filter(
        (record) =>
          record.status === "pending" &&
          record.source === source &&
          record.sourceAccount === sourceAccount,
      )
      .slice(0, limit)
      .map(cloneEvidence);
  }

  async markOutboxPublished(id, publishedAt) {
    const record = this.outbox.get(id);

    if (!record) {
      throw new Error(`missing outbox ${id}`);
    }

    record.status = "published";
    record.publishedAt = publishedAt;
  }

  async getState(key) {
    return cloneOrNull(this.state.get(key));
  }

  async setState(key, value) {
    this.state.set(key, cloneEvidence(value));
  }

  async getPublishedRows(scopeId) {
    return cloneEvidence(this.publishedRows.get(scopeId) ?? []);
  }

  async setPublishedRows(scopeId, rows) {
    this.publishedRows.set(scopeId, cloneEvidence(rows));
  }

  requireInbox(id) {
    const record = this.inbox.get(id);

    if (!record) {
      throw new Error(`missing inbox ${id}`);
    }

    return record;
  }
}

export class FakeClock {
  constructor(now = Date.parse("2026-06-01T00:00:00Z")) {
    this.value = now;
  }

  now() {
    return this.value;
  }

  advance(milliseconds) {
    this.value += milliseconds;
  }
}

function cloneOrNull(value) {
  return value === undefined ? null : cloneEvidence(value);
}
