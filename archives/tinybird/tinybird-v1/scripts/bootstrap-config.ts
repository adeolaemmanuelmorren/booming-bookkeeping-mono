import { readFile, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { sourcePartition } from '../worker/bootstrap/replay.ts';
import { hash } from '../worker/bootstrap/hash.ts';
import type { BootstrapConfig } from '../worker/bootstrap/executor.ts';
// Metadata only. Landing tables must already represent the exact chosen object inventory.
interface Settings extends Omit<BootstrapConfig, 'inputs' | 'inputManifestHash' | 'sourceSeal'> {
    importMode: 'all-frozen-objects' | 'one-object-per-content';
    landingTables: Record<string, string>;
    landingRecordIdColumns: Record<string, string>;
}
interface Summary {
    complete: boolean;
    sources: Record<string, {
        schemas: Record<string, Record<string, string>>;
        all_objects_physical_rows: number;
        unique_content_physical_rows: number;
    }>;
}
const [summaryPath, manifestPath, settingsPath, outputPath] = process.argv.slice(2);
if (!summaryPath || !manifestPath || !settingsPath || !outputPath)
    throw new Error('Pass summary.json, manifest.json, settings.json, and output config path');
const summary = JSON.parse(await readFile(summaryPath, 'utf8')) as Summary;
const manifest = await readFile(manifestPath);
const settings = JSON.parse(await readFile(settingsPath, 'utf8')) as Settings;
if (!summary.complete)
    throw new Error('Historical inventory is incomplete');
if (!['all-frozen-objects', 'one-object-per-content'].includes(settings.importMode))
    throw new Error('Choose the exact import inventory mode');
const inputManifestHash = createHash('sha256').update(manifest).digest('hex');
const inputs = Object.entries(summary.sources).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([table, source]) => {
    const schemas = Object.values(source.schemas);
    if (schemas.length !== 1)
        throw new Error('Resolve historical schema differences before bootstrap');
    const landingTable = settings.landingTables[table];
    if (!landingTable || !/^[a-z][a-z0-9_]*$/.test(landingTable))
        throw new Error('Every raw source needs an explicit immutable landing table');
    return {
        partition: sourcePartition(table, Object.keys(schemas[0]), inputManifestHash), landingTable,
        landingRecordIdColumn: settings.landingRecordIdColumns[table],
        expectedPhysicalRows: settings.importMode === 'all-frozen-objects' ? source.all_objects_physical_rows : source.unique_content_physical_rows,
    };
});
const { importMode, landingTables, landingRecordIdColumns, ...execution } = settings;
const sourceSeal = await hash({ baselineId: settings.baselineId, inputManifestHash, importMode, inputs, live: settings.live ?? null,
    ...(settings.conversionIdentity ? { conversionIdentity: settings.conversionIdentity } : {}) });
const config: BootstrapConfig = { ...execution, inputs, inputManifestHash, sourceSeal };
await writeFile(outputPath, JSON.stringify(config, null, 2) + '\n');
process.stdout.write(JSON.stringify({ config: outputPath, inputManifestHash, sourceSeal, partitions: inputs.length }) + '\n');
