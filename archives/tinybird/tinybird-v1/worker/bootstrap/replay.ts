import { normalizeHistorical, type BrowserKind, type HistoricalBrowserSource, type NormalizedBrowserEvent } from '../browser/normalize.ts';
import { canonicalJson, compareStrings } from '../sessions/session-engine.ts';
import { hash } from './hash.ts';

const KINDS: Record<string, BrowserKind> = {
  pages: 'page_view', identifies: 'identify', form_submitted: 'client_form',
  order_completed: 'client_order', attr: 'attribution',
};

export interface SourcePartition {
  source: HistoricalBrowserSource;
  kind: BrowserKind;
  table: string;
  columns: string[];
  inputManifestHash: string;
}

export interface SourceCursor {
  inputManifestHash: string;
  table: string;
  recordId: string;
  revision: string;
  rowHash: string;
}

export interface SourceReplayPage {
  partition: SourcePartition;
  after: SourceCursor | null;
  next: SourceCursor | null;
  eof: boolean;
  events: NormalizedBrowserEvent[];
  contentHash: string;
}

/** Schema comes from the frozen Parquet/native landing source, not staged page models. */
export function sourcePartition(table: string, columns: string[], inputManifestHash: string): SourcePartition {
  const match = /^raw_(jitsu_data|boom_domains)_(pages|identifies|form_submitted|order_completed|attr)$/.exec(table);
  if (!match || !/^[a-f0-9]{64}$/.test(inputManifestHash)) throw new Error('Invalid historical source partition');
  const raw = columns.filter(column => !column.startsWith('__tb_'));
  if (!raw.length || raw.some(column => !/^[a-z_][a-z0-9_]*$/.test(column))) throw new Error('Invalid raw source columns');
  if (new Set(raw).size !== raw.length) throw new Error('Duplicate raw source column');
  return { table, columns: raw, source: match[1] as HistoricalBrowserSource, kind: KINDS[match[2]], inputManifestHash };
}

/** A finite keyset scan, independent of browser clocks and duplicate export deliveries. */
export function sourcePageQuery(partition: SourcePartition, after: SourceCursor | null, limit = 1000, recordIdColumn?:string): string {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 10000) throw new Error('Invalid source page limit');
  if (after && (after.inputManifestHash !== partition.inputManifestHash || after.table !== partition.table)) {
    throw new Error('Source cursor belongs to another frozen input');
  }
  if (after && (!/^(0|[1-9][0-9]*)$/.test(after.revision) || !/^[A-F0-9]{64}$/.test(after.rowHash))) {
    throw new Error('Invalid source cursor');
  }
  const fields = new Set(partition.columns);
  const idFields = ['message_id', 'id'].filter(name => fields.has(name));
  if (!idFields.length) throw new Error('Source has no original record ID');
  const id = coalesce(idFields);
  if(recordIdColumn&&!/^[a-z_][a-z0-9_]*$/.test(recordIdColumn)) throw new Error('Invalid physical source ID column');
  const selectedId=recordIdColumn??`ifNull(${id}, '')`;
  const candidates = ['loaded_at', 'received_at', 'sent_at', 'timestamp'];
  if (partition.kind === 'attribution') candidates.push('original_timestamp', 'uuid_ts');
  if (partition.kind === 'client_form' || partition.kind === 'client_order') candidates.push('submitted_at');
  const availableTimes = candidates.filter(name => fields.has(name));
  const time = availableTimes.length ? coalesce(availableTimes) : 'NULL';
  const revision = availableTimes.length
    ? `if(isNull(${time}), toUInt64(0), toUInt64(toUnixTimestamp64Micro(${time})) + 1)`
    : 'toUInt64(0)';
  const columns = partition.columns.map(name => `\`${name}\``).join(', ');
  const cursor = after ? `WHERE tuple(__bootstrap_id, __bootstrap_revision, __bootstrap_hash) > tuple(${sqlString(after.recordId)}, toUInt64('${after.revision}'), '${after.rowHash}')` : '';
  return `SELECT DISTINCT ${columns},
    ${selectedId} AS __bootstrap_id,
    ${revision} AS __bootstrap_revision,
    hex(SHA256(toJSONString(tuple(${columns})))) AS __bootstrap_hash
    FROM ${partition.table}
    ${cursor}
    ORDER BY ${recordIdColumn??'__bootstrap_id'}, __bootstrap_revision, __bootstrap_hash
    LIMIT ${limit}`;
}

export async function normalizeSourceRow(
  partition: SourcePartition, row: Record<string, unknown>, tenantId: string, ingestedAt: string,
): Promise<{ event: NormalizedBrowserEvent; cursor: SourceCursor }> {
  const record = Object.fromEntries(partition.columns.map(name => [name, row[name] ?? null]));
  const cursor: SourceCursor = {
    inputManifestHash: partition.inputManifestHash, table: partition.table,
    recordId: String(row.__bootstrap_id), revision: String(row.__bootstrap_revision), rowHash: String(row.__bootstrap_hash),
  };
  if (!/^[A-F0-9]{64}$/.test(cursor.rowHash) || !/^(0|[1-9][0-9]*)$/.test(cursor.revision)) throw new Error('Invalid source row cursor');
  const event = await normalizeHistorical({ tenantId, source: partition.source, kind: partition.kind, record, ingestedAt });
  if (event.source.source_record_id !== cursor.recordId || event.source.source_revision !== cursor.revision) {
    throw new Error('SQL source cursor differs from canonical normalization');
  }
  return { event, cursor };
}

/** Persist only metadata locally. Raw output stays in the authorized source store. */
export async function buildSourceReplayPage(
  partition: SourcePartition, after: SourceCursor | null, rows: Record<string, unknown>[],
  context: { tenantId: string; ingestedAt: string; limit: number },
): Promise<SourceReplayPage> {
  sourcePageQuery(partition, after, context.limit);
  if (rows.length > context.limit) throw new Error('Source query exceeded its page limit');
  let next = after;
  const events: NormalizedBrowserEvent[] = [];
  for (const row of rows) {
    const normalized = await normalizeSourceRow(partition, row, context.tenantId, context.ingestedAt);
    if (next && compareCursor(normalized.cursor, next) <= 0) throw new Error('Source page cursor did not advance');
    next = normalized.cursor;
    events.push(normalized.event);
  }
  const page = { partition, after, next, eof: rows.length < context.limit, events };
  return { ...page, contentHash: await hash(page) };
}

export interface SourceReplayPublisher {
  appendSources(events: NormalizedBrowserEvent[]): Promise<void>;
  readSources(page: SourceReplayPage): AsyncIterable<NormalizedBrowserEvent>;
  /** One executor compares its saved cursor with after before storing the verified receipt. */
  advanceCheckpoint(page: SourceReplayPage): Promise<void>;
}

/** An ambiguous checkpoint retry rereads the same deterministic output before advancing. */
export async function publishSourceReplayPage(page: SourceReplayPage, publisher: SourceReplayPublisher): Promise<void> {
  const { contentHash, ...payload } = page;
  if (await hash(payload) !== contentHash) throw new Error('Saved source replay page changed');
  const wanted = new Map(page.events.map(event => [event.source.delivery_event_id, canonicalJson(event)]));
  if (wanted.size !== page.events.length) throw new Error('Source page contains duplicate logical deliveries');
  const verify = async () => {
    const seen = new Set<string>();
    for await (const event of publisher.readSources(page)) {
      const id = event.source.delivery_event_id;
      if (wanted.get(id) !== canonicalJson(event)) throw new Error('Source replay readback conflicts');
      seen.add(id);
    }
    return seen.size === wanted.size;
  };
  if (!await verify()) {
    await publisher.appendSources(page.events);
    if (!await verify()) throw new Error('Source replay rows are not fully visible');
  }
  await publisher.advanceCheckpoint(page);
}

/** Retain raw normalized events separately before collapsing their per-source heads. */
export async function* selectSourceHeads(events: AsyncIterable<NormalizedBrowserEvent>): AsyncGenerator<NormalizedBrowserEvent> {
  let selected: NormalizedBrowserEvent | null = null;
  let previousKey: string[] | null = null;
  for await (const event of events) {
    const key = [event.source.source_system, event.source.event_kind, event.source.source_record_id];
    const order = previousKey === null ? 1 : compareKey(key, previousKey);
    if (order < 0) throw new Error('Source heads require sorted logical record keys');
    if (selected && order !== 0) {
      yield selected;
      selected = null;
    }
    previousKey = key;
    if (!selected || BigInt(event.source.source_revision) > BigInt(selected.source.source_revision)) {
      selected = event;
      continue;
    }
    if (BigInt(event.source.source_revision) < BigInt(selected.source.source_revision)) throw new Error('Source versions must arrive in ascending order');
    if (event.source.source_revision === selected.source.source_revision && semanticContent(event) !== semanticContent(selected)) {
      throw new Error('Conflicting source content at the same revision');
    }
  }
  if (selected) yield selected;
}

function semanticContent(event: NormalizedBrowserEvent): string {
  return canonicalJson({
    originalPayloadHash: event.source.original_payload_hash,
    deleted: event.source.source_deleted, page: event.pageRevision,
    identity: event.identity ? {
      observedAt: event.identity.observedAt, hash: event.identity.factPayloadHash, evidence: event.identity.evidenceKeys,
    } : null,
  });
}

function coalesce(values: string[]): string { return values.length === 1 ? values[0] : `coalesce(${values.join(', ')})`; }
function compareKey(left: string[], right: string[]): number {
  for (let index = 0; index < left.length; index++) {
    const order = compareStrings(left[index], right[index]);
    if (order) return order;
  }
  return 0;
}
function compareCursor(left: SourceCursor, right: SourceCursor): number {
  const id = compareStrings(left.recordId, right.recordId);
  if (id) return id;
  if (BigInt(left.revision) < BigInt(right.revision)) return -1;
  if (BigInt(left.revision) > BigInt(right.revision)) return 1;
  return compareStrings(left.rowHash, right.rowHash);
}
function sqlString(value: string): string { return `'${value.replaceAll('\\', '\\\\').replaceAll("'", "\\'")}'`; }
