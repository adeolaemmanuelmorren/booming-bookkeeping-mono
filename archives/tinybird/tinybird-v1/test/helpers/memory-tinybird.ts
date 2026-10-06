import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { Tinybird, type JsonRecord } from '../../worker/storage/tinybird.ts';
import { canonicalJson, timestampMicros } from '../../worker/sessions/session-engine.ts';
import { normalizeHistorical } from '../../worker/browser/normalize.ts';
import { hash } from '../../worker/bootstrap/hash.ts';
import type { BootstrapConfig } from '../../worker/bootstrap/executor.ts';
/** Test transport only. Evaluates the finite query forms used by this bootstrap. */
export class MemoryTinybird {
    readonly rows = new Map<string, JsonRecord[]>();
    readonly queries: string[] = [];
    readonly writes: {
        table: string;
        rows: JsonRecord[];
    }[] = [];
    readonly client: Tinybird;
    failAfterWrite: string | null = null;
    hideOnce: {
        table: string;
        match: RegExp;
        predicate: (row: JsonRecord) => boolean;
    } | null = null;
    private config: BootstrapConfig;
    constructor(config: BootstrapConfig, landing: Record<string, JsonRecord[]> = {}) {
        this.config = config;
        for (const [table, rows] of Object.entries(landing))
            this.rows.set(table, structuredClone(rows));
        this.client = new Tinybird({ TINYBIRD_URL: 'https://api.us-east.tinybird.co', TINYBIRD_TOKEN: 'synthetic' }, async (input, init) => {
            const url = new URL(String(input));
            assert.equal(url.origin, 'https://api.us-east.tinybird.co');
            if (url.pathname === '/v0/sql') {
                const sql = new URLSearchParams(String(init?.body)).get('q')!;
                this.queries.push(sql);
                return Response.json({ data: await this.query(sql) });
            }
            assert.equal(url.pathname, '/v0/events');
            assert.equal(url.searchParams.get('wait'), 'true');
            const table = url.searchParams.get('name')!;
            const rows = String(init?.body).trim().split('\n').map(line => JSON.parse(line) as JsonRecord);
            this.writes.push({ table, rows });
            this.rows.set(table, [...this.rows.get(table) ?? [], ...rows]);
            if (this.failAfterWrite === table) {
                this.failAfterWrite = null;
                throw new Error('Synthetic lost acknowledgement');
            }
            return Response.json({ successful_rows: rows.length, quarantined_rows: 0 });
        });
    }
    async query(sql: string): Promise<JsonRecord[]> {
        const clean = compactSql(sql.replace(/\s+FORMAT JSON\s*$/, ''));
        const covered = /^SELECT \(([\s\S]*?)\) AS __coverage, \(SELECT groupArray\(toJSONString\(tuple\((.*?)\)\)\) FROM \(([\s\S]*)\)\) AS __rows$/.exec(clean);
        if (covered) {
            const count = await this.query(covered[1]);
            const rows = await this.query(covered[3]);
            const columns = split(covered[2]);
            return [{ __coverage: Object.values(count[0])[0], __rows: rows.map(row => JSON.stringify(columns.map(column => {
                        const item = row[column];
                        return ['sequence', 'source_revision', 'producer_sequence', 'bucket'].includes(column) && typeof item === 'string' ? Number(item) : item;
                    }))) }];
        }
        const match = /^SELECT\s+(DISTINCT\s+)?([\s\S]*?)\s+FROM\s+(\w+)\s*([\s\S]*)$/i.exec(clean);
        if (!match)
            throw new Error(`Unrecognized test query: ${clean}`);
        const [, distinct, fields, table, tail] = match;
        let rows = structuredClone(this.rows.get(table) ?? []);
        if (this.hideOnce?.table === table && this.hideOnce.match.test(clean)) {
            rows = rows.filter(row => !this.hideOnce!.predicate(row));
            this.hideOnce = null;
        }
        const raw = fields.includes(' AS __bootstrap_hash');
        if (raw) {
            const input = this.config.inputs.find(input => input.landingTable === table)!;
            rows = await Promise.all(rows.map(async (record) => {
                const event = await normalizeHistorical({ tenantId: this.config.tenantId, source: input.partition.source,
                    kind: input.partition.kind, record, ingestedAt: this.config.startedAt });
                return { ...record, __bootstrap_id: event.source.source_record_id, __bootstrap_revision: Number(event.source.source_revision),
                    __bootstrap_hash: (await hash(input.partition.columns.map(key => record[key]))).toUpperCase() };
            }));
        }
        const where = /\bWHERE\s+([\s\S]*?)(?=\s+ORDER BY\s|\s+LIMIT\s|$)/i.exec(tail)?.[1];
        if (where)
            rows = rows.filter(row => conditions(where, row));
        const aggregate = /^(count\(\)|uniqExact\()/i.test(fields.trim());
        if (aggregate)
            return [Object.fromEntries(split(fields).map(field => {
                    const [expression, alias] = aliasOf(field);
                    if (expression === 'count()')
                        return [alias, rows.length];
                    const inside = /^uniqExact\(([\s\S]*)\)$/.exec(expression)?.[1];
                    if (inside === undefined)
                        throw new Error('Unrecognized test aggregate');
                    return [alias, new Set(rows.map(row => canonicalJson(value(inside, row)))).size];
                }))];
        if (!raw)
            rows = rows.map(row => Object.fromEntries(split(fields).map(field => {
                const [expression, alias] = aliasOf(field);
                return [alias, value(expression, row)];
            })));
        if (distinct)
            rows = [...new Map(rows.map(row => [canonicalJson(row), row])).values()];
        const order = /\bORDER BY\s+([\s\S]*?)(?=\s+LIMIT\s|$)/i.exec(tail)?.[1];
        if (order) {
            const keys = split(order).map(key => {
                const expression = key.replace(/\s+DESC$/i, '').trim();
                const alias = split(fields).map(aliasOf).find(([field]) => field === expression)?.[1];
                return { key: alias ?? expression, descending: /\s+DESC$/i.test(key) };
            });
            rows.sort((a, b) => {
                for (const item of keys) {
                    const result = compare(a[item.key], b[item.key]);
                    if (result)
                        return item.descending ? -result : result;
                }
                return 0;
            });
        }
        const limit = Number(/\bLIMIT\s+(\d+)/i.exec(tail)?.[1] ?? rows.length);
        const offset = Number(/\bOFFSET\s+(\d+)/i.exec(tail)?.[1] ?? 0);
        return rows.slice(offset, offset + limit).map(row => Object.fromEntries(Object.entries(row).map(([key, item]) => {
            // The SQL API quotes UInt64 even though the Events API requires JSON numbers.
            const quoted = ['source_revision', 'sequence', 'producer_sequence', 'source_fact_version', 'batch_version', '__bootstrap_revision'].includes(key);
            return [key, quoted && typeof item === 'number' ? String(item) : item];
        })));
    }
}
function compactSql(text: string): string {
    let result = '';
    let quoted = false;
    for (let index = 0; index < text.length; index++) {
        const char = text[index];
        if (quoted && char === '\\') {
            result += char + text[++index];
            continue;
        }
        if (char === "'")
            quoted = !quoted;
        if (!quoted && /\s/.test(char)) {
            if (!result.endsWith(' '))
                result += ' ';
            continue;
        }
        result += char;
    }
    return result.trim();
}
function aliasOf(field: string): [
    string,
    string
] {
    const match = /^([\s\S]*)\s+AS\s+(\w+)$/i.exec(field.trim());
    if (match)
        return [match[1].trim(), match[2]];
    return [field.trim(), field.trim().replaceAll('`', '')];
}
function split(text: string, delimiter = ','): string[] {
    const result: string[] = [];
    let depth = 0;
    let quoted = false;
    let start = 0;
    for (let index = 0; index < text.length; index++) {
        const char = text[index];
        if (quoted && char === '\\') {
            index++;
            continue;
        }
        if (char === "'") {
            quoted = !quoted;
            continue;
        }
        if (quoted)
            continue;
        if (char === '(')
            depth++;
        if (char === ')')
            depth--;
        if (!depth && text.slice(index, index + delimiter.length) === delimiter) {
            result.push(text.slice(start, index).trim());
            index += delimiter.length - 1;
            start = index + 1;
        }
    }
    result.push(text.slice(start).trim());
    return result;
}
function value(expression: string, row: JsonRecord): unknown {
    const text = expression.trim();
    const digest = /^hex\(SHA256\((.*)\)\)$/.exec(text);
    if (digest) return createHash('sha256').update(String(value(digest[1], row))).digest('hex').toUpperCase();
    if (text.startsWith("'"))
        return text.slice(1, -1).replace(/\\(.)/g, '$1');
    if (/^\d+$/.test(text))
        return Number(text);
    const call = /^(tuple|toString|toUInt64|toDateTime64|ifNull|coalesce)\(([\s\S]*)\)$/.exec(text);
    if (call) {
        const args = split(call[2]).map(item => value(item, row));
        if (call[1] === 'tuple')
            return args;
        if (call[1] === 'toString')
            return String(args[0]);
        if (call[1] === 'toUInt64')
            return Number(args[0]);
        if (call[1] === 'toDateTime64')
            return args[0];
        return args.find(item => item !== null && item !== undefined) ?? null;
    }
    const key = text.replaceAll('`', '');
    if (!(key in row))
        throw new Error(`Unknown test expression: ${text}`);
    return row[key];
}
function conditions(text: string, row: JsonRecord): boolean {
    return split(text, ' AND ').every(raw => {
        const expression = raw.trim();
        if (expression === '1')
            return true;
        // Normalize only whitespace outside quoted literals before matching operators.
        const parts = split(expression, ' IN ');
        if (parts.length === 2) {
            const expected = value(parts[0], row);
            return split(parts[1].slice(1, -1)).some(item => compare(expected, value(item, row)) === 0);
        }
        for (const operator of ['>=', '<=', '!=', '=', '>', '<']) {
            const sides = split(expression, ` ${operator} `);
            if (sides.length !== 2)
                continue;
            const order = compare(value(sides[0], row), value(sides[1], row));
            if (operator === '>=')
                return order >= 0;
            if (operator === '<=')
                return order <= 0;
            if (operator === '!=')
                return order !== 0;
            if (operator === '=')
                return order === 0;
            if (operator === '>')
                return order > 0;
            return order < 0;
        }
        throw new Error(`Unrecognized test predicate: ${expression}`);
    });
}
function compare(a: unknown, b: unknown): number {
    if (Array.isArray(a) && Array.isArray(b)) {
        for (let i = 0; i < a.length; i++) {
            const order = compare(a[i], b[i]);
            if (order)
                return order;
        }
        return 0;
    }
    if (typeof a === 'string' && typeof b === 'string' && /^\d{4}-\d\d-\d\d[T ]/.test(a) && /^\d{4}-\d\d-\d\d[T ]/.test(b)) {
        a = timestampMicros(a.replace(' ', 'T').replace(/Z?$/, 'Z'));
        b = timestampMicros(b.replace(' ', 'T').replace(/Z?$/, 'Z'));
    }
    if (a === b)
        return 0;
    return (a as string | number) < (b as string | number) ? -1 : 1;
}
