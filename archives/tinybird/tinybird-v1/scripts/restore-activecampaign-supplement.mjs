import { readFile, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { query, tinybirdRequest } from './tinybird.mjs';

export const SUPPLEMENT_TABLE = 'v1_history_activecampaign_contact_tag';
const SHA256 = 'd9839d42bef032188281015e2d2e4d0e08c0876b4d8cd6c22e12f82a3f72eb3d';
const ROOT = new URL('../', import.meta.url);
const RECEIPT_PATH = new URL('restore/activecampaign-supplement-receipt.json', ROOT);
const APPEND_INTENT_PATH = new URL('restore/activecampaign-supplement-append-intent.json', ROOT);

async function preservedRow() {
  const bytes = await readFile(new URL('backups/activecampaign/out-of-band-assignment.ndjson', ROOT));
  if (createHash('sha256').update(bytes).digest('hex') !== SHA256) throw new Error('Preserved assignment checksum differs');
  const row = JSON.parse(bytes.toString('utf8'));
  const fields = ['id', 'contact', 'tags', '_fivetran_deleted', '_fivetran_synced', 'c_date', 'updated_timestamp', 'created_timestamp'];
  if (Object.keys(row).length !== fields.length || fields.some(key => !(key in row))) throw new Error('Preserved assignment schema differs');
  const conditions = fields.map(key => {
    const value = row[key];
    if (value === null) return `${key} IS NULL`;
    if (['id', 'contact', 'tags'].includes(key)) {
      if (!Number.isSafeInteger(value)) throw new Error('Invalid preserved assignment identifier');
      return `${key} = ${value}`;
    }
    if (key === '_fivetran_deleted') {
      if (typeof value !== 'boolean') throw new Error('Invalid preserved assignment flag');
      return `${key} = ${value ? 1 : 0}`;
    }
    if (typeof value !== 'string' || !/^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d(?:\.\d{1,6})?Z?$/.test(value)) throw new Error('Invalid preserved assignment timestamp');
    return `${key} = parseDateTime64BestEffort('${value}', 6, 'UTC')`;
  });
  return { bytes, predicate: conditions.join(' AND ') };
}

async function counts(predicate) {
  const result = await query(`SELECT count() AS rows, countIf(${predicate}) AS matching FROM ${SUPPLEMENT_TABLE}`);
  return { rows: Number(result.data[0].rows), matching: Number(result.data[0].matching) };
}

/** A separate old Events API row was preserved alongside the Parquet inventory. */
export async function readVerifiedSupplement() {
  let receipt;
  try { receipt = JSON.parse(await readFile(RECEIPT_PATH, 'utf8')); }
  catch (error) { if (error.code === 'ENOENT') return null; throw error; }
  if (!receipt.verified || receipt.sha256 !== SHA256 || receipt.rows !== 1 || receipt.table !== SUPPLEMENT_TABLE) throw new Error('Invalid supplementary restore receipt');
  const preserved = await preservedRow();
  const actual = await counts(preserved.predicate);
  if (actual.matching !== 1 || actual.rows !== receipt.totalRows) throw new Error('Supplementary restored assignment no longer matches');
  return { sha256: SHA256, rows: 1, verified: true };
}

async function restore() {
  const workspace = await (await tinybirdRequest('/v1/workspace')).json();
  if (workspace.id !== '00c04079-d0b4-4d8b-8de6-6fa8072b85af') throw new Error('Unexpected restore workspace');
  const plan = JSON.parse(await readFile(new URL('restore/history-import-plan.json', ROOT), 'utf8'));
  const nativeRows = plan.sources.raw_activecampaign_contact_tag.expected_physical_rows;
  const proof = JSON.parse(await readFile(new URL('restore/native-import-verification.json', ROOT), 'utf8'));
  const table = proof.tables.find(value => value.table === SUPPLEMENT_TABLE);
  if (!table || table.acceptedRows !== nativeRows || table.quarantineRows !== 0
    || table.missingFiles?.length !== 0 || table.problems?.length !== 0) {
    throw new Error('Finish verifying the native assignment import first');
  }
  const preserved = await preservedRow();
  const before = await counts(preserved.predicate);
  if (before.rows === nativeRows && before.matching === 0) {
    // An unacknowledged append can still commit. Never send it twice while reads lag.
    try {
      await writeFile(APPEND_INTENT_PATH, JSON.stringify({ table: SUPPLEMENT_TABLE,
        sha256: SHA256, attemptedAt: new Date().toISOString() }) + '\n', { flag: 'wx' });
    } catch (error) {
      if (error.code !== 'EEXIST') throw error;
      throw new Error('A preserved-assignment append was already attempted; wait for exact readback before retrying');
    }
    const response = await tinybirdRequest(`/v0/events?name=${SUPPLEMENT_TABLE}&wait=true`, {
      method: 'POST', headers: { 'Content-Type': 'application/x-ndjson' }, body: preserved.bytes,
    });
    const result = await response.json();
    if (result.successful_rows !== 1 || result.quarantined_rows !== 0) throw new Error('The preserved assignment was not fully accepted');
  } else if (before.rows !== nativeRows + 1 || before.matching !== 1) {
    throw new Error('Assignment restore state differs; no row was appended');
  }
  let verified = false;
  for (const delay of [0, 250, 500, 1000, 2000, 4000, 8000]) {
    if (delay) await new Promise(resolve => setTimeout(resolve, delay));
    const actual = await counts(preserved.predicate);
    if (actual.rows === nativeRows + 1 && actual.matching === 1) { verified = true; break; }
  }
  if (!verified) throw new Error('Preserved assignment readback is incomplete');
  const receipt = { checked_at: new Date().toISOString(), table: SUPPLEMENT_TABLE, sha256: SHA256,
    rows: 1, nativeRows, totalRows: nativeRows + 1, verified: true };
  await writeFile(RECEIPT_PATH, JSON.stringify(receipt, null, 2) + '\n');
  console.log(JSON.stringify(receipt));
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) await restore();
