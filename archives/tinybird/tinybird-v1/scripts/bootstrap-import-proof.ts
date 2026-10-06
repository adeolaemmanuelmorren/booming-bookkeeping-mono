import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import type { BootstrapConfig } from '../worker/bootstrap/executor.ts';

export const IMPORT_SNAPSHOT_AT = '2026-09-05T22:28:00.000000Z';
const SUPPLEMENTAL_TABLE = 'v1_history_activecampaign_contact_tag';
const SUPPLEMENTAL_SHA256 = 'd9839d42bef032188281015e2d2e4d0e08c0876b4d8cd6c22e12f82a3f72eb3d';

// Generated from the frozen metadata manifests below. No source rows are read to build this map.
export const NATIVE_IMPORT_MANIFEST_SHA256 = Object.freeze({
  "restore/history-import-plan.json": "cb70eba6acc07098e7ae7ab4ed78d1acc803bfa35b575fd769c78546bb68e311",
  "restore/fivetran-snapshot/manifest.json": "778ceab23d1c110f86afb8c607030584c3b82e3b5b06b59345c07515d12d97b2",
  "backups/live-jitsu/manifest.json": "3a9ce4afd0f0216f1f220202aa327d30a180c8b6a72e7bb8a333a3fec47cb8e3",
  "backups/live-jitsu-tail/manifest.json": "7c3b4266a4f2f3ffabb2bdee5aa12fe6da294b41cba167be1aff451d429e30c1"
});

export const EXPECTED_NATIVE_IMPORTS: Readonly<Record<string, Readonly<{ rows: number; files: number }>>> = Object.freeze({
  "v1_history_activecampaign_contact": Object.freeze({ rows: 1214349, files: 119 }),
  "v1_history_activecampaign_contact_tag": Object.freeze({ rows: 4775155, files: 1999 }),
  "v1_history_activecampaign_tags": Object.freeze({ rows: 9176, files: 14 }),
  "v1_history_boom_domains_attr": Object.freeze({ rows: 1381362, files: 80 }),
  "v1_history_boom_domains_form_submitted": Object.freeze({ rows: 510, files: 24 }),
  "v1_history_boom_domains_identifies": Object.freeze({ rows: 954753, files: 80 }),
  "v1_history_boom_domains_order_completed": Object.freeze({ rows: 13, files: 3 }),
  "v1_history_boom_domains_pages": Object.freeze({ rows: 854367, files: 80 }),
  "v1_history_jitsu_data_attr": Object.freeze({ rows: 2944167, files: 1228 }),
  "v1_history_jitsu_data_form_submitted": Object.freeze({ rows: 101845, files: 735 }),
  "v1_history_jitsu_data_identifies": Object.freeze({ rows: 300164, files: 771 }),
  "v1_history_jitsu_data_order_completed": Object.freeze({ rows: 5851, files: 350 }),
  "v1_history_jitsu_data_pages": Object.freeze({ rows: 2311083, files: 1298 }),
  "v1_history_live_jitsu": Object.freeze({ rows: 793170, files: 272 }),
  "v1_history_stripe_auxiliary": Object.freeze({ rows: 40651, files: 401 }),
  "v1_history_stripe_charge": Object.freeze({ rows: 293354, files: 129 }),
  "v1_history_stripe_checkout_session": Object.freeze({ rows: 11383, files: 42 }),
  "v1_history_stripe_checkout_session_line_item": Object.freeze({ rows: 11353, files: 36 }),
  "v1_history_stripe_coupon": Object.freeze({ rows: 1, files: 1 }),
  "v1_history_stripe_customer": Object.freeze({ rows: 195478, files: 117 }),
  "v1_history_stripe_discount": Object.freeze({ rows: 2, files: 1 }),
  "v1_history_stripe_invoice": Object.freeze({ rows: 51453, files: 90 }),
  "v1_history_stripe_invoice_line_item": Object.freeze({ rows: 55304, files: 57 }),
  "v1_history_stripe_kajabi_charge": Object.freeze({ rows: 111256, files: 124 }),
  "v1_history_stripe_kajabi_customer": Object.freeze({ rows: 16910, files: 96 }),
  "v1_history_stripe_kajabi_invoice": Object.freeze({ rows: 86630, files: 98 }),
  "v1_history_stripe_kajabi_invoice_line_item": Object.freeze({ rows: 86642, files: 142 }),
  "v1_history_stripe_kajabi_payment_intent": Object.freeze({ rows: 86567, files: 102 }),
  "v1_history_stripe_payment_intent": Object.freeze({ rows: 263770, files: 117 }),
  "v1_history_stripe_payment_link_line_item": Object.freeze({ rows: 14, files: 1 }),
  "v1_history_stripe_plan": Object.freeze({ rows: 2250, files: 75 }),
  "v1_history_stripe_price": Object.freeze({ rows: 3568, files: 57 }),
  "v1_history_stripe_product": Object.freeze({ rows: 3523, files: 56 }),
  "v1_history_stripe_promotion_code": Object.freeze({ rows: 0, files: 1 }),
  "v1_history_stripe_subscription_item": Object.freeze({ rows: 10668, files: 52 }),
  "v1_snapshot_activecampaign_contact": Object.freeze({ rows: 1183824, files: 8 }),
  "v1_snapshot_activecampaign_contact_tag": Object.freeze({ rows: 4582044, files: 11 }),
  "v1_snapshot_activecampaign_tags": Object.freeze({ rows: 656, files: 3 }),
  "v1_snapshot_stripe_charge": Object.freeze({ rows: 293512, files: 53 }),
  "v1_snapshot_stripe_customer": Object.freeze({ rows: 196116, files: 49 }),
  "v1_snapshot_stripe_kajabi_charge": Object.freeze({ rows: 111215, files: 35 }),
  "v1_snapshot_stripe_kajabi_customer": Object.freeze({ rows: 15637, files: 1 }),
  "v1_snapshot_stripe_kajabi_payment_intent": Object.freeze({ rows: 86423, files: 9 }),
  "v1_snapshot_stripe_payment_intent": Object.freeze({ rows: 263928, files: 27 }),
});

export const EXPECTED_IMPORT_TABLES = Object.freeze(Object.keys(EXPECTED_NATIVE_IMPORTS));

export const EXPECTED_LIVE_JITSU_INPUT = Object.freeze({
  table: 'v1_history_live_jitsu',
  cutoff: '2026-09-05 21:41:23.124153',
  expectedPhysicalRows: EXPECTED_NATIVE_IMPORTS.v1_history_live_jitsu.rows,
});

const BROWSER_KINDS: Readonly<Record<string, string>> = {
  attr: 'attribution', form_submitted: 'client_form', identifies: 'identify',
  order_completed: 'client_order', pages: 'page_view',
};

/** Production must consume every pinned browser source, with its original source priority. */
export function validateProductionBrowserInputs(value: unknown): void {
  const config = object(value);
  if (!Array.isArray(config.inputs) || config.inputs.length !== 10) {
    throw new Error('Production browser bootstrap requires all ten historical inputs');
  }
  const seen = new Set<string>();
  for (const value of config.inputs) {
    const input = object(value);
    const partition = object(input.partition);
    const matched = typeof partition.table === 'string'
      ? /^raw_(boom_domains|jitsu_data)_(attr|form_submitted|identifies|order_completed|pages)$/.exec(partition.table) : null;
    if (!matched || seen.has(String(partition.table))) throw new Error('Production browser source set differs from the frozen inputs');
    seen.add(String(partition.table));
    const landingTable = `v1_history_${matched[1]}_${matched[2]}`;
    if (input.landingTable !== landingTable || input.expectedPhysicalRows !== EXPECTED_NATIVE_IMPORTS[landingTable].rows
      || partition.source !== matched[1] || partition.kind !== BROWSER_KINDS[matched[2]]
      || partition.inputManifestHash !== config.inputManifestHash) {
      throw new Error('Production browser input mapping differs from its frozen source');
    }
  }
  if (!config.live || typeof config.live !== 'object' || Array.isArray(config.live)) {
    throw new Error('Production browser bootstrap requires the frozen live Jitsu input');
  }
  const live = object(config.live);
  if (live.table !== EXPECTED_LIVE_JITSU_INPUT.table || live.cutoff !== EXPECTED_LIVE_JITSU_INPUT.cutoff
    || live.expectedPhysicalRows !== EXPECTED_LIVE_JITSU_INPUT.expectedPhysicalRows) {
    throw new Error('Production live Jitsu input differs from its frozen table, cutoff, or count');
  }
}

export interface NativeImportTableProof {
  table: string;
  verified: true;
  expectedRows: number;
  expectedNativeRows: number;
  supplementalRows: number;
  rows: number;
  acceptedRows: number;
  quarantineRows: number;
  expectedFiles: number;
  successfulFiles: number;
  missingFiles: unknown[];
  problems: unknown[];
  supplementalReceipt?: { sha256: string; rows: 1; verified: true };
}

export interface NativeImportProof {
  checkedAt: string;
  verified: true;
  snapshotAt: string;
  tables: NativeImportTableProof[];
}

export type ImportGatedConfig = BootstrapConfig & { nativeImportProofSha256: string };

interface CountClient {
  query<Row extends Record<string, unknown>>(sql: string): Promise<Row[]>;
}

/** Reject a partial proof even if its top-level verified flag was set by mistake. */
export function validateNativeImportProof(value: unknown): NativeImportProof {
  const proof = object(value);
  keys(proof, ['checkedAt', 'verified', 'snapshotAt', 'tables']);
  if (proof.verified !== true || !sameSnapshot(proof.snapshotAt)
    || typeof proof.checkedAt !== 'string' || !Number.isFinite(Date.parse(proof.checkedAt))
    || Date.parse(proof.checkedAt) < Date.parse(IMPORT_SNAPSHOT_AT)
    || !Array.isArray(proof.tables) || proof.tables.length !== EXPECTED_IMPORT_TABLES.length) {
    throw new Error('Native import proof is incomplete or belongs to another snapshot');
  }
  const seen = new Set<string>();
  for (const value of proof.tables) {
    const table = object(value);
    keys(table, ['table', 'verified', 'expectedRows', 'expectedNativeRows', 'supplementalRows', 'rows', 'acceptedRows',
      'quarantineRows', 'expectedFiles', 'successfulFiles', 'missingFiles', 'problems'], ['supplementalReceipt']);
    if (typeof table.table !== 'string' || !EXPECTED_IMPORT_TABLES.includes(table.table) || seen.has(table.table)) {
      throw new Error('Native import proof must contain each of the 44 expected tables exactly once');
    }
    seen.add(table.table);
    for (const key of ['expectedRows', 'expectedNativeRows', 'supplementalRows', 'rows', 'acceptedRows', 'quarantineRows',
      'expectedFiles', 'successfulFiles']) count(table[key]);
    const frozen = EXPECTED_NATIVE_IMPORTS[table.table];
    if (table.expectedNativeRows !== frozen.rows || table.expectedFiles !== frozen.files) {
      throw new Error('Native import row or file count differs from the frozen source manifests');
    }
    if (table.verified !== true || table.rows !== table.expectedRows || table.acceptedRows !== table.expectedNativeRows
      || table.expectedRows !== Number(table.expectedNativeRows) + Number(table.supplementalRows)
      || table.quarantineRows !== 0 || table.successfulFiles !== table.expectedFiles
      || !Array.isArray(table.missingFiles) || table.missingFiles.length
      || !Array.isArray(table.problems) || table.problems.length) {
      throw new Error('Native import proof contains an incomplete table or import problem');
    }
    if (table.table !== SUPPLEMENTAL_TABLE && table.supplementalRows === 0 && table.supplementalReceipt === undefined) continue;
    if (table.table !== SUPPLEMENTAL_TABLE || table.supplementalRows !== 1) {
      throw new Error('Native import proof contains an unexpected supplemental row');
    }
    const receipt = object(table.supplementalReceipt);
    keys(receipt, ['sha256', 'rows', 'verified']);
    if (receipt.verified !== true || receipt.rows !== 1 || receipt.sha256 !== SUPPLEMENTAL_SHA256) {
      throw new Error('Preserved supplemental row receipt did not verify');
    }
  }
  return value as NativeImportProof;
}

/** Bind packaged bytes to the build manifest before making any provider request. */
export async function loadNativeImportProof(input: {
  configBytes: Uint8Array;
  proofPath: string | URL;
  buildManifestPath: string | URL;
}): Promise<{ config: ImportGatedConfig; proof: NativeImportProof; proofSha256: string }> {
  const [proofBytes, manifestBytes] = await Promise.all([readFile(input.proofPath), readFile(input.buildManifestPath)]);
  const config = object(JSON.parse(Buffer.from(input.configBytes).toString('utf8')));
  const manifest = object(JSON.parse(Buffer.from(manifestBytes).toString('utf8')));
  if (!sameSnapshot(config.startedAt) || !sameSnapshot(manifest.snapshotAt) || !Array.isArray(manifest.files)) {
    throw new Error('Packaged bootstrap metadata belongs to another snapshot');
  }
  const proofSha256 = digest(proofBytes);
  if (config.nativeImportProofSha256 !== proofSha256) throw new Error('Native import proof differs from the packaged configuration hash');
  for (const [path, bytes] of [['config/bootstrap.json', input.configBytes], ['config/native-import-verification.json', proofBytes]] as const) {
    const files = manifest.files.map(object).filter(file => file.path === path);
    if (files.length !== 1 || files[0].sha256 !== digest(bytes) || files[0].bytes !== bytes.byteLength) {
      throw new Error('Packaged bootstrap input differs from the build manifest');
    }
  }
  const proof = validateNativeImportProof(JSON.parse(Buffer.from(proofBytes).toString('utf8')));
  validateProductionBrowserInputs(config);
  const inputTables = Array.isArray(config.inputs) ? config.inputs.map(value => object(value)) : [];
  const live = config.live === undefined ? [] : [object(config.live)];
  for (const input of [...inputTables.map(value => ({ table: value.landingTable, expectedPhysicalRows: value.expectedPhysicalRows })), ...live]) {
    const table = proof.tables.find(table => table.table === input.table);
    if (!table || table.expectedRows !== input.expectedPhysicalRows) throw new Error('Native import proof disagrees with the frozen browser input count');
  }
  return { config: config as unknown as ImportGatedConfig, proof, proofSha256 };
}

/** A single bounded SELECT checks current physical counts for every pinned table. */
export async function verifyCurrentImportCounts(client: CountClient, proof: NativeImportProof): Promise<void> {
  validateNativeImportProof(proof);
  const sql = EXPECTED_IMPORT_TABLES.map(table => `SELECT '${table}' AS table, toString(count()) AS rows FROM ${table}`).join('\nUNION ALL\n');
  const rows = await client.query<{ table: string; rows: string | number }>(sql);
  if (rows.length !== EXPECTED_IMPORT_TABLES.length) throw new Error('Current import count readback omitted an expected table');
  const expected = new Map(proof.tables.map(table => [table.table, table.expectedRows]));
  const seen = new Set<string>();
  for (const row of rows) {
    const current = typeof row.rows === 'string' && /^(0|[1-9][0-9]*)$/.test(row.rows) ? Number(row.rows) : row.rows;
    count(current);
    if (!expected.has(row.table) || seen.has(row.table) || current !== expected.get(row.table)) {
      throw new Error('Current physical import counts differ from the frozen proof');
    }
    seen.add(row.table);
  }
}

function sameSnapshot(value: unknown): boolean {
  return value === IMPORT_SNAPSHOT_AT || value === '2026-09-05T22:28:00Z';
}

function count(value: unknown): void {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) throw new Error('Invalid native import count');
}

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Invalid native import metadata object');
  return value as Record<string, unknown>;
}

function keys(value: Record<string, unknown>, required: string[], optional: string[] = []): void {
  const accepted = new Set([...required, ...optional]);
  if (Object.keys(value).some(key => !accepted.has(key)) || required.some(key => !(key in value))) {
    throw new Error('Native import proof has missing or unexpected metadata fields');
  }
}

function digest(bytes: Uint8Array): string {
  return createHash('sha256').update(bytes).digest('hex');
}
