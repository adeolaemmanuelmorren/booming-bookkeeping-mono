import { normalizeHistorical, normalizeJitsu, type BrowserKind, type HistoricalBrowserSource, type JitsuObservationInput } from '../browser/normalize.ts';
import { canonicalJson, sha256 } from '../storage/json.ts';
import { sqlString, sqlStrings, uint64Number, waitForReadback, type JsonRecord } from '../storage/tinybird.ts';
import { matchActiveCampaignRegistrationTag } from '../conversions/activecampaign.mjs';
import { projectStripeIdentity, projectActiveCampaignIdentity } from './fivetran-facts.ts';
import { IdentityBootstrapStore, type FactRow } from './bootstrap-store.ts';

export const SNAPSHOT_AT = '2026-09-05T22:28:00.000000Z';
export const SNAPSHOTS: Readonly<Record<string, number>> = Object.freeze({
  v1_snapshot_activecampaign_contact: 1183824, v1_snapshot_activecampaign_contact_tag: 4582044,
  v1_snapshot_activecampaign_tags: 656, v1_snapshot_stripe_charge: 293512,
  v1_snapshot_stripe_customer: 196116, v1_snapshot_stripe_payment_intent: 263928,
  v1_snapshot_stripe_kajabi_charge: 111215, v1_snapshot_stripe_kajabi_customer: 15637,
  v1_snapshot_stripe_kajabi_payment_intent: 86423,
});
export interface BrowserInput {
  table: string; source: HistoricalBrowserSource; kind: BrowserKind;
  columns: string[]; expectedPhysicalRows: number; inputHash: string;
}
export interface IdentityInputs {
  browser: BrowserInput[];
  live: { table: string; cutoff: string; expectedPhysicalRows: number };
  fivetran: { snapshotAt: string; tables: { table: string; expectedPhysicalRows: number }[] };
}
export interface SourcePage { cursor: string[]; physical: number; rows: FactRow[] }
export interface SourceStream { key: string; expectedPhysical: number; pages(after: string[] | null): AsyncIterable<SourcePage> }

const BROWSER_FIELDS = ['message_id','id','anonymous_id','user_id','email','phone','first_name','name',
  'extra_submitted_fields_checkout_offer_member_email','context_traits_email','context_traits_phone',
  'loaded_at','received_at','sent_at','timestamp','submitted_at','original_timestamp','uuid_ts'];
const LIVE_FIELDS = ['tenant_id','producer_id','producer_sequence','message_id','event_kind','delivery_event_id',
  'observed_at','ingested_at','source_fact_version','source_deleted','fact_payload'];
const CONTACT = ['id','_fivetran_synced','_fivetran_deleted','deleted','email','phone','first_name','last_name'];
const ASSIGNMENT = ['id','contact','tags','c_date','_fivetran_synced','_fivetran_deleted'];
const CHARGE = ['id','customer_id','payment_intent_id','created','paid','status','billing_detail_email',
  'receipt_email','billing_detail_phone','billing_detail_name','metadata','_fivetran_synced'];

/** Frozen snapshots have one row per ID. A malformed snapshot stops before any output. */
export async function verifyIdentityInputs(store: IdentityBootstrapStore, inputs: IdentityInputs): Promise<void> {
  for (const input of [...inputs.browser, inputs.live, ...inputs.fivetran.tables]) {
    safeTable(input.table);
    const [count] = await store.client.query<{ rows: string | number }>(`SELECT count() AS rows FROM ${input.table}`);
    if (!count || uint64Number(count.rows) !== input.expectedPhysicalRows) throw new Error(`Identity input count differs for ${input.table}`);
  }
  for (const input of inputs.fivetran.tables) {
    const [count] = await store.client.query<{ ids: string | number; missing: string | number }>(`SELECT uniqExact(id) AS ids,countIf(isNull(id) OR toString(__tb_sort_1) != toString(id)) AS missing FROM ${input.table}`);
    if (!count || uint64Number(count.ids) !== input.expectedPhysicalRows || uint64Number(count.missing)) throw new Error(`Identity snapshot IDs are not complete and unique for ${input.table}`);
  }
}

export function historicalStream(store: IdentityBootstrapStore, input: BrowserInput, ingestedAt: string, rangeIds: number, maxRows: number): SourceStream {
  const fields = BROWSER_FIELDS.filter(field => input.columns.includes(field));
  if (!fields.includes('message_id') && !fields.includes('id')) throw new Error('Browser source has no original ID');
  return { key: input.table, expectedPhysical: input.expectedPhysicalRows, pages: async function* (after) {
    for await (const range of ranges(store, input.table, '__tb_sort_1', false, fields, after, rangeIds, maxRows)) {
      const rows = new Map<string, FactRow>();
      for (const record of range.records) {
        const { __source_id, ...original } = record;
        const originalId = original.message_id ?? original.id;
        if (String(originalId) !== String(__source_id)) throw new Error('Browser physical key differs from its original ID');
        const event = await normalizeHistorical({ tenantId: store.tenantId, source: input.source, kind: input.kind, record: original, ingestedAt });
        if (!event.identity) throw new Error('Historical browser kind produced no identity fact');
        const origin = await sha256(canonicalJson([input.table, original]));
        rows.set(origin, await store.factRow(event.identity, input.table, origin));
      }
      yield { cursor: range.cursor, physical: range.physical, rows: [...rows.values()] };
    }
  } };
}

export function liveStream(store: IdentityBootstrapStore, input: IdentityInputs['live'], rangeIds: number, maxRows: number): SourceStream {
  const where = `ingested_at <= toDateTime64(${sqlString(input.cutoff)},6)`;
  return { key: input.table, expectedPhysical: input.expectedPhysicalRows, pages: async function* (after) {
    let cursor = after;
    for (;;) {
      const lower = cursor ? ` AND tuple(producer_id,producer_sequence,delivery_event_id,ingested_at)>tuple(${sqlString(cursor[0])},${uint64Number(cursor[1])},${sqlString(cursor[2])},toDateTime64(${sqlString(cursor[3])},6))` : '';
      const ids = await store.client.query<{ producer_id: string; producer_sequence: string | number; delivery_event_id: string; ingested_at: string }>(`SELECT DISTINCT producer_id,producer_sequence,delivery_event_id,ingested_at FROM ${input.table}
        WHERE ${where}${lower} ORDER BY producer_id,producer_sequence,delivery_event_id,ingested_at LIMIT ${rangeIds}`);
      if (!ids.length) return;
      const last = ids.at(-1)!;
      const bounds = `${where}${lower} AND tuple(producer_id,producer_sequence,delivery_event_id,ingested_at)<=tuple(${sqlString(last.producer_id)},${uint64Number(last.producer_sequence)},${sqlString(last.delivery_event_id)},toDateTime64(${sqlString(last.ingested_at)},6))`;
      const { physical, records } = await readRange(store, input.table, LIVE_FIELDS, bounds, 'producer_id,producer_sequence,delivery_event_id,ingested_at', maxRows);
      if (records.length > maxRows) throw new Error('Live identity source range exceeds its configured bound');
      const rows = new Map<string, FactRow>();
      const seen = new Set<string>();
      for (const record of records) {
        if (record.tenant_id !== store.tenantId) throw new Error('Historical live row belongs to another tenant');
        const observation = record as unknown as JitsuObservationInput;
        const event = await normalizeJitsu(observation);
        const origin = await sha256(canonicalJson([input.table, record.producer_id, String(record.producer_sequence), record.delivery_event_id, record.ingested_at]));
        if (seen.has(origin)) throw new Error('Conflicting live identity delivery');
        seen.add(origin);
        if (event.identity) rows.set(origin, await store.factRow(event.identity, input.table, origin));
      }
      cursor = [last.producer_id, String(last.producer_sequence), last.delivery_event_id, last.ingested_at];
      yield { cursor, physical, rows: [...rows.values()] };
    }
  } };
}

/** All current qualifying scope facts are generated in bulk. No conversion publication occurs. */
export async function fivetranStreams(store: IdentityBootstrapStore, inputs: IdentityInputs, rangeIds: number, maxRows: number): Promise<SourceStream[]> {
  const snapshotAt = inputs.fivetran.snapshotAt;
  const results: SourceStream[] = [];
  for (const account of ['main','kajabi'] as const) {
    const prefix = account === 'main' ? 'v1_snapshot_stripe' : 'v1_snapshot_stripe_kajabi';
    const table = `${prefix}_charge`;
    results.push({ key: table, expectedPhysical: expected(inputs, table), pages: async function* (after) {
      for await (const range of ranges(store, table, '__tb_sort_1', false, CHARGE, after, rangeIds, maxRows)) {
        const customerIds = uniqueIds(range.records.map(row => row.customer_id));
        const intentIds = uniqueIds(range.records.map(row => row.payment_intent_id));
        const customers = await relatedRows(store, `${prefix}_customer`, ['id','email','phone','name','_fivetran_synced'], customerIds, snapshotAt, expected(inputs, `${prefix}_customer`));
        const intents = await relatedRows(store, `${prefix}_payment_intent`, ['id','receipt_email','_fivetran_synced'], intentIds, snapshotAt, expected(inputs, `${prefix}_payment_intent`));
        const customersById = new Map(customers.map(row => [String(row.id), row]));
        const intentsById = new Map(intents.map(row => [String(row.id), row]));
        const rows: FactRow[] = [];
        for (const record of range.records) {
          const id = String(record.id);
          const facts = await projectStripeIdentity({ account, chargeId: id, chargeVersions: [observed(record, snapshotAt)],
            customerVersions: one(customersById.get(String(record.customer_id))),
            paymentIntentVersions: one(intentsById.get(String(record.payment_intent_id))), evidenceRecordIds: [] }, snapshotAt);
          for (const fact of facts) rows.push(await store.factRow(fact, table, await sha256(canonicalJson([table,id])), `stripe:${account}:charge:${id}`));
        }
        yield { cursor: range.cursor, physical: range.physical, rows };
      }
    } });
  }
  const tagTable = 'v1_snapshot_activecampaign_tags';
  const tags = await store.client.query<JsonRecord>(`SELECT id,tags,_fivetran_deleted,_fivetran_synced FROM ${tagTable} ORDER BY id LIMIT ${expected(inputs, tagTable) + 1}`);
  if (tags.length !== expected(inputs, tagTable)) throw new Error('ActiveCampaign tag snapshot is incomplete');
  const tagsById = new Map(tags.map(row => [String(row.id), row]));
  const primary = new Set<string>();
  const assignmentTable = 'v1_snapshot_activecampaign_contact_tag';
  let primaryPhysical = 0;
  for await (const range of ranges(store, assignmentTable, '__tb_sort_1', true, ['id','contact','tags','_fivetran_deleted'], null, rangeIds, maxRows)) {
    primaryPhysical += range.physical;
    for (const row of range.records) {
      const tag = tagsById.get(String(row.tags));
      if (deleted(row._fivetran_deleted) || !tag || deleted(tag._fivetran_deleted)) continue;
      const match = matchActiveCampaignRegistrationTag(tag.tags);
      if (match?.matchType === 'primary') primary.add(canonicalJson([String(row.contact), match.registrationType]));
    }
  }
  if (primaryPhysical !== expected(inputs, assignmentTable)) throw new Error('ActiveCampaign primary-tag scan is incomplete');
  results.push({ key: assignmentTable, expectedPhysical: expected(inputs, assignmentTable), pages: async function* (after) {
    for await (const range of ranges(store, assignmentTable, '__tb_sort_1', true, ASSIGNMENT, after, rangeIds, maxRows)) {
      const eligible = range.records.filter(row => {
        const tag = tagsById.get(String(row.tags));
        if (deleted(row._fivetran_deleted) || !tag || deleted(tag._fivetran_deleted)) return false;
        const match = matchActiveCampaignRegistrationTag(tag.tags);
        return match && (match.matchType === 'primary' || !primary.has(canonicalJson([String(row.contact), match.registrationType])));
      });
      const contactIds = uniqueIds(eligible.map(row => row.contact));
      const contacts = await relatedRows(store, 'v1_snapshot_activecampaign_contact', CONTACT, contactIds, snapshotAt, expected(inputs, 'v1_snapshot_activecampaign_contact'), true);
      const assignments = new Map<string, JsonRecord[]>();
      for (const row of eligible) {
        const key = String(row.contact);
        const group = assignments.get(key) ?? [];
        group.push(observed(row, snapshotAt)); assignments.set(key, group);
      }
      const observedTags = tags.map(row => observed(row, snapshotAt));
      const rows: FactRow[] = [];
      for (const contact of contacts) {
        const contactId = String(contact.id);
        const facts = await projectActiveCampaignIdentity({ contactId, contactVersions: [contact],
          assignmentVersions: assignments.get(contactId) ?? [],
          referencedTagVersions: observedTags, evidenceRecordIds: [] }, snapshotAt);
        for (const fact of facts) rows.push(await store.factRow(fact, assignmentTable,
          await sha256(canonicalJson([assignmentTable,fact.factKey])), `activecampaign:contact:${contactId}`));
      }
      yield { cursor: range.cursor, physical: range.physical, rows };
    }
  } });
  return results;
}

async function* ranges(store: IdentityBootstrapStore, table: string, key: string, numeric: boolean, fields: string[], after: string[] | null, rangeIds: number, maxRows: number): AsyncGenerator<{ cursor: string[]; physical: number; records: JsonRecord[] }> {
  let cursor = after?.[0] ?? null;
  const literal = (value: string) => numeric ? `toInt64(${sqlString(value)})` : sqlString(value);
  for (;;) {
    const lower = cursor === null ? '1' : `${key}>${literal(cursor)}`;
    const ids = await store.client.query<{ source_id: string | number }>(`SELECT DISTINCT ${key} AS source_id FROM ${table} WHERE ${lower} ORDER BY ${key} LIMIT ${rangeIds}`);
    if (!ids.length) return;
    const upper = String(ids.at(-1)!.source_id);
    const bounds = `${lower} AND ${key}<=${literal(upper)}`;
    const selected = fields.includes(key) ? fields : [`${key} AS __source_id`, ...fields];
    const { physical, records } = await readRange(store, table, selected, bounds, key, maxRows);
    if (physical < ids.length) throw new Error('Identity raw range is incomplete');
    cursor = upper;
    yield { cursor: [upper], physical, records };
  }
}

/** The bounded count and payload are returned by one query snapshot. No wide global scan. */
async function readRange(store: IdentityBootstrapStore, table: string, fields: string[], bounds: string, order: string, maxRows: number): Promise<{ physical: number; records: JsonRecord[] }> {
  const projection = fields.map(field => field.includes(' AS ') ? field.split(' AS ')[0] : field);
  const names = fields.map(field => field.includes(' AS ') ? field.split(' AS ')[1] : field);
  const [row] = await store.client.query<{ physical: string | number; projected: string | number; values: unknown[][] }>(`
    SELECT (SELECT count() FROM ${table} WHERE ${bounds}) AS physical,
      (SELECT uniqExact(tuple(${projection.join(',')})) FROM ${table} WHERE ${bounds}) AS projected,
      (SELECT groupArray(tuple(${names.join(',')})) FROM
        (SELECT DISTINCT ${fields.join(',')} FROM ${table} WHERE ${bounds} ORDER BY ${order} LIMIT ${maxRows + 1})) AS values`);
  if (!row || uint64Number(row.projected) > maxRows) throw new Error('Identity raw range exceeds its configured bound');
  if (row.values.length !== uint64Number(row.projected)) throw new Error('Identity raw range projection is incomplete');
  return { physical: uint64Number(row.physical), records: row.values.map(value => Object.fromEntries(names.map((key,index) => [key,value[index]]))) };
}

/** Full snapshot count is metadata-only; it authenticates missing related IDs in this same read. */
async function relatedRows(store: IdentityBootstrapStore, table: string, fields: string[], ids: string[], snapshotAt: string, expectedRows: number, numeric = false): Promise<JsonRecord[]> {
  const rows: JsonRecord[] = [];
  for (let offset = 0; offset < ids.length; offset += 1000) {
    const keys = ids.slice(offset, offset + 1000);
    const literals = numeric ? keys.map(value => `toInt64(${sqlString(value)})`).join(',') : sqlStrings(keys);
    const result = await waitForReadback(async () => {
      const [result] = await store.client.query<{ coverage: string | number; values: unknown[][] }>(`
        SELECT (SELECT count() FROM ${table}) AS coverage,
          (SELECT groupArray(tuple(${fields.join(',')})) FROM
            (SELECT ${fields.join(',')} FROM ${table} WHERE __tb_sort_1 IN (${literals}) ORDER BY __tb_sort_1 LIMIT ${keys.length + 1})) AS values`);
      if (!result || uint64Number(result.coverage) > expectedRows) throw new Error('Frozen related snapshot changed');
      return result;
    }, value => uint64Number(value.coverage) === expectedRows);
    rows.push(...result.values.map(value => Object.fromEntries(fields.map((key,index) => [key,value[index]]))));
  }
  if (new Set(rows.map(row => String(row.id))).size !== rows.length) throw new Error('Conflicting related snapshot rows');
  return rows.map(row => observed(row, snapshotAt));
}
function one(row: JsonRecord | undefined): JsonRecord[] { return row ? [row] : []; }

function observed(row: JsonRecord, at: string): JsonRecord { return { ...row, _v1_observed_at: at }; }
function uniqueIds(values: unknown[]): string[] { return [...new Set(values.filter(value => value !== null && value !== undefined).map(String))]; }
function deleted(value: unknown): boolean { return value === true || value === 1; }
function expected(inputs: IdentityInputs, table: string): number {
  const value = inputs.fivetran.tables.find(input => input.table === table)?.expectedPhysicalRows;
  if (value === undefined) throw new Error('Missing snapshot source');
  return value;
}
function safeTable(table: string): void { if (!/^v1_[a-z0-9_]+$/.test(table)) throw new Error('Invalid identity source table'); }
