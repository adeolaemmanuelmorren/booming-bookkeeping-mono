import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
export function fact(key = 'source:a', version = 1, identifiers = ['email:ada@example.com'], deleted = false) {
  const payload = JSON.stringify({ first_name: 'Ada', last_name: 'Lovelace' });
  return {
    eventId: `${key}:${version}:${deleted}`, producerId: 'test-source', observedAt: '2026-09-01T00:00:00.000000Z',
    ingestedAt: '2026-09-05T00:00:00.000000Z', factKind: 'web', factKey: key,
    sourceFactVersion: version, factDeleted: deleted,
    factPayloadHash: createHash('sha256').update(payload + JSON.stringify(identifiers) + deleted).digest('hex'),
    factPayload: payload, evidenceKeys: identifiers,
  };
}

function sqlValues(value) {
  return [...value.matchAll(/'((?:\\.|[^'])*)'/g)].map(match => match[1].replace(/\\(.)/g, '$1'));
}

export function remoteTinybird() {
  return {
    tables: { v1_identity_records: [], v1_identity_commits: [] }, calls: [],
    failAfter: null, partialWrite: false, quarantine: false, corruptRead: false,
    blockWrite: null, writeStarted: null, manifests: new Map(), corruptManifest: false, sealStarted: null, holdSeal: null,
    async fetch(request) {
      const url = new URL(request.url);
      assert.equal(url.origin, 'https://api.us-east.tinybird.co');
      if (url.pathname === '/v0/events') {
        const table = url.searchParams.get('name');
        assert.ok(Object.hasOwn(this.tables, table));
        const rows = (await request.text()).trim().split('\n').filter(Boolean).map(JSON.parse);
        this.calls.push({ operation: 'append', table, rows: structuredClone(rows) });
        if (table === 'v1_identity_records') {
          this.writeStarted?.();
          if (this.blockWrite) await this.blockWrite;
        }
        const partial = table === 'v1_identity_records' && (this.partialWrite || this.quarantine);
        const written = partial ? rows.slice(0, Math.max(0, rows.length - 1)) : rows;
        this.tables[table].push(...structuredClone(written));
        if (this.failAfter === table) {
          this.failAfter = null;
          return new Response('Lost response after remote write', { status: 503 });
        }
        return Response.json({ successful_rows: this.quarantine ? written.length : rows.length, quarantined_rows: this.quarantine ? rows.length - written.length : 0 });
      }
      assert.equal(url.pathname, '/v0/sql');
      const sql = new URLSearchParams(await request.text()).get('q');
      this.calls.push({ operation: 'query', sql });
      if (sql.includes('FROM v1_bootstrap_manifests')) {
        const tenant = sqlValues(/tenant_id = ('(?:\\.|[^'])*')/.exec(sql)[1])[0];
        const baseline = sqlValues(/baseline_id = ('(?:\\.|[^'])*')/.exec(sql)[1])[0];
        const kind = sqlValues(/manifest_kind = ('(?:\\.|[^'])*')/.exec(sql)[1])[0];
        const exact = /manifest_key = ('(?:\\.|[^'])*')/.exec(sql);
        const keys = exact ? sqlValues(exact[1]) : sqlValues(/manifest_key IN \(([^)]*)\)/.exec(sql)[1]);
        if (kind === 'seal') { this.sealStarted?.(); if (this.holdSeal) await this.holdSeal; }
        const rows = keys.map(key => this.manifests.get(JSON.stringify([tenant,baseline,kind,key]))).filter(Boolean).map(row => structuredClone(row));
        if (this.corruptManifest && rows.length) rows[0].payload_hash = 'f'.repeat(64);
        return Response.json({ data: rows });
      }
      const batch = /AND batch_id = ('(?:\\.|[^'])*')/.exec(sql);
      if (batch) {
        const batchId = sqlValues(batch[1])[0];
        const table = sql.includes('FROM v1_identity_records') ? 'v1_identity_records' : 'v1_identity_commits';
        const rows = structuredClone(this.tables[table].filter(row => row.batch_id === batchId))
          .map(row => ({ ...row, batch_version: String(row.batch_version) }));
        if (table === 'v1_identity_records' && this.corruptRead && rows.length) rows[0].payload_json = '{"corrupt":true}';
        return Response.json({ data: rows });
      }
      const kind = sqlValues(/state_kind = ('(?:\\.|[^'])*')/.exec(sql)[1])[0];
      const keys = new Set(sqlValues(/lookup_key IN \(([^)]*)\)/.exec(sql)[1]));
      const version = Number(/batch_version <= (\d+)/.exec(sql)[1]);
      const committed = new Set(this.tables.v1_identity_commits.filter(row => Number(row.batch_version) <= version).map(row => row.batch_id));
      const latest = new Map();
      for (const record of this.tables.v1_identity_records) {
        if (record.state_kind !== kind || !keys.has(record.lookup_key) || Number(record.batch_version) > version || !committed.has(record.batch_id)) continue;
        const previous = latest.get(record.state_key);
        if (!previous || Number(previous.batch_version) < Number(record.batch_version)) latest.set(record.state_key, record);
      }
      return Response.json({ data: [...latest.values()].map(row => ({ payload_json: row.payload_json, is_deleted: row.is_deleted })) });
    },
    commits() { return [...new Map(this.tables.v1_identity_commits.map(row => [row.batch_id, row])).values()]; },
  };
}

