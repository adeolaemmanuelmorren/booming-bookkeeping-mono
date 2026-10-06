import { sourcePartition } from '../../worker/bootstrap/replay.ts';
import { hash } from '../../worker/bootstrap/hash.ts';
import type { BootstrapConfig } from '../../worker/bootstrap/executor.ts';
import type { JsonRecord } from '../../worker/storage/tinybird.ts';
export const SMOKE_BRANCH_ID = 'bfaead01-69a3-4215-a53b-02477bda5323';
export const TIME = '2026-09-05T12:00:00.000000Z';
export const RAW_COLUMNS = ['id', 'anonymous_id', 'user_id', 'email', 'phone', 'first_name', 'name', 'timestamp', 'loaded_at', 'url', 'utm_campaign'];
const row = (values: JsonRecord): JsonRecord => Object.fromEntries(RAW_COLUMNS.map(key => [key, values[key] ?? null]));
/** Entirely synthetic. The same inputs drive local restart tests and the branch smoke. */
export async function bootstrapFixture(workspaceName = 'v1_facts_validation', baselineId = 'bootstrap-transport-smoke-v2') {
    const tables: Record<string, JsonRecord[]> = {};
    const originals = ['boom_domains', 'jitsu_data'].flatMap(source => ['pages', 'identifies', 'form_submitted', 'order_completed', 'attr'].map(kind => `raw_${source}_${kind}`));
    const landing = (source: string) => `v1_smoke_${source}`;
    for (const source of originals)
        tables[landing(source)] = [];
    tables[landing('raw_boom_domains_pages')] = [row({ id: 'p1', anonymous_id: 'old-visitor', timestamp: '2117-01-01T00:00:00.000000Z', loaded_at: '2117-01-01T00:00:00.000000Z' })];
    tables[landing('raw_jitsu_data_pages')] = [
        row({ id: 'p1', anonymous_id: 'browser-b', timestamp: TIME, loaded_at: TIME }),
        row({ id: 'p1', anonymous_id: 'browser-b', timestamp: TIME, loaded_at: '2026-09-05T12:01:00.000000Z', utm_campaign: 'raw-campaign' }),
        row({ id: 'p2', anonymous_id: 'browser-b', timestamp: '2026-09-05T12:30:59.999999Z', loaded_at: TIME }),
        row({ id: 'p3', anonymous_id: 'browser-b', timestamp: '2026-09-05T13:02:00.000000Z', loaded_at: TIME }),
        row({ id: 'null-time', anonymous_id: 'browser-null', timestamp: null, loaded_at: null }),
        row({ id: 'future', anonymous_id: 'browser-future', timestamp: '2117-01-01T00:00:00.000001Z', loaded_at: '2117-01-01T00:00:00.000001Z' }),
    ];
    tables[landing('raw_jitsu_data_identifies')] = [row({ id: 'identify', anonymous_id: 'browser-b', user_id: 'person@example.invalid', email: 'PERSON@example.invalid', phone: '+1 (202) 555-0123', timestamp: TIME, loaded_at: TIME })];
    tables[landing('raw_boom_domains_form_submitted')] = [row({ id: 'form', anonymous_id: 'browser-a', email: 'person@example.invalid', first_name: 'Ada', timestamp: null, loaded_at: null })];
    tables[landing('raw_jitsu_data_order_completed')] = [row({ id: 'order', anonymous_id: 'browser-b', email: 'person@example.invalid', name: 'Grace Hopper', timestamp: TIME, loaded_at: TIME })];
    tables[landing('raw_boom_domains_attr')] = [row({ id: 'attr', anonymous_id: 'browser-a', user_id: 'person@example.invalid', timestamp: TIME, loaded_at: TIME })];
    const tenantId = `synthetic-${baselineId}`;
    tables.v1_smoke_live_jitsu = [{
            tenant_id: tenantId, producer_id: 'synthetic-ingress', producer_sequence: 10, delivery_event_id: 'synthetic-live-p1',
            message_id: 'p1', event_kind: 'page_view', observed_at: TIME, ingested_at: TIME,
            source_fact_version: 1788609600000001, source_deleted: 0,
            fact_payload: JSON.stringify({ messageId: 'p1', anonymousId: 'browser-a', timestamp: TIME, receivedAt: TIME, type: 'page', properties: { url: 'https://example.invalid/a' } }),
        }];
    const inputManifestHash = await hash(tables);
    const config: BootstrapConfig = {
        baselineId, tenantId, inputManifestHash, sourceSeal: await hash([baselineId, inputManifestHash]),
        algorithmVersion: 'synthetic-transport-v1', workspaceId: SMOKE_BRANCH_ID, workspaceName,
        startedAt: TIME, region: 'us-east4',
        inputs: originals.map(source => ({ partition: sourcePartition(source, RAW_COLUMNS, inputManifestHash), landingTable: landing(source), expectedPhysicalRows: tables[landing(source)].length })),
        live: { table: 'v1_smoke_live_jitsu', cutoff: '2026-09-05 12:00:00.000000', expectedPhysicalRows: 1 },
        pageSize: 2, batchSize: 2, maxVisitorsPerChunk: 2, maxSessionRecordsPerChunk: 2,
        maxIdentityFactsPerBatch: 3, maxIdentityFactsPerComponent: 100, maxIdentifiers: 100,
    };
    return { config, tables };
}
export function smokeSchemas(): Record<string, string> {
    const result: Record<string, string> = {};
    for (const source of ['boom_domains', 'jitsu_data'])
        for (const kind of ['pages', 'identifies', 'form_submitted', 'order_completed', 'attr']) {
            const fields = RAW_COLUMNS.map(key => `    \`${key}\` Nullable(${['timestamp', 'loaded_at'].includes(key) ? "DateTime64(6, 'UTC')" : 'String'}) \`json:$.${key}\``);
            result[`v1_smoke_raw_${source}_${kind}`] = `DESCRIPTION >\n    Synthetic bootstrap verification only.\n\nSCHEMA >\n${fields.join(',\n')}\n\nENGINE MergeTree\nENGINE_SORTING_KEY tuple()\n`;
        }
    const live = { tenant_id: 'String', producer_id: 'String', producer_sequence: 'UInt64', delivery_event_id: 'String', message_id: 'String', event_kind: 'String', observed_at: "DateTime64(6, 'UTC')", ingested_at: "DateTime64(6, 'UTC')", source_fact_version: 'UInt64', source_deleted: 'UInt8', fact_payload: 'String' };
    result.v1_smoke_live_jitsu = `DESCRIPTION >\n    Synthetic live backup verification only.\n\nSCHEMA >\n${Object.entries(live).map(([key, type]) => `    \`${key}\` ${type} \`json:$.${key}\``).join(',\n')}\n\nENGINE MergeTree\nENGINE_SORTING_KEY tenant_id, producer_id, producer_sequence\n`;
    return result;
}
