import { EXPECTED_IMPORT_TABLES, EXPECTED_LIVE_JITSU_INPUT, EXPECTED_NATIVE_IMPORTS, IMPORT_SNAPSHOT_AT, type NativeImportProof } from '../../scripts/bootstrap-import-proof.ts';
import type { BootstrapConfig } from '../../worker/bootstrap/executor.ts';

/** Synthetic receipt metadata only. No provider data or credentials. */
export function nativeImportProof(): NativeImportProof {
  return {
    checkedAt: '2026-09-05T23:30:00.000Z', verified: true, snapshotAt: IMPORT_SNAPSHOT_AT,
    tables: EXPECTED_IMPORT_TABLES.map(table => {
      const supplementalRows = table === 'v1_history_activecampaign_contact_tag' ? 1 : 0;
      const { rows: expectedNativeRows, files: expectedFiles } = EXPECTED_NATIVE_IMPORTS[table];
      return { table, verified: true, expectedRows: expectedNativeRows + supplementalRows,
        expectedNativeRows, supplementalRows, rows: expectedNativeRows + supplementalRows,
        acceptedRows: expectedNativeRows, quarantineRows: 0, expectedFiles, successfulFiles: expectedFiles,
        missingFiles: [], problems: [],
        ...(supplementalRows ? { supplementalReceipt: { sha256: 'd9839d42bef032188281015e2d2e4d0e08c0876b4d8cd6c22e12f82a3f72eb3d', rows: 1 as const, verified: true as const } } : {}),
      };
    }),
  };
}

/** Use the real frozen counts without loading or synthesizing any corresponding source rows. */
export function applyNativeImportFixtureCounts(config: BootstrapConfig): void {
  for (const input of config.inputs) input.expectedPhysicalRows = EXPECTED_NATIVE_IMPORTS[input.landingTable].rows;
  if (config.live) config.live = { ...EXPECTED_LIVE_JITSU_INPUT };
}
