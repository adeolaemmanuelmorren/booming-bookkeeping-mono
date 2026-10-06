import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { tinybirdRequest } from "../../scripts/tinybird.mjs";
import { canonicalJson } from "../../worker/storage/json.ts";
const chunkTable = "proof_attribution_chunks_v2",
  commitTable = "proof_attribution_commits_v2";
export const hash = (value) =>
  createHash("sha256").update(canonicalJson(value)).digest("hex");
const quote = (value) => `'${value.replaceAll("'", "''")}'`;

export async function connect(scenario) {
  const { environments } = await (
    await tinybirdRequest("/v1/environments")
  ).json();
  const branch = environments.find((e) => e.name === "v1_attribution_proof");
  if (branch?.main !== "00c04079-d0b4-4d8b-8de6-6fa8072b85af")
    throw new Error("Unexpected validation branch");
  const request = async (path, options = {}) => {
    const response = await fetch(
      new URL(path, "https://api.us-east.tinybird.co"),
      {
        ...options,
        headers: {
          ...options.headers,
          Authorization: `Bearer ${branch.token}`,
        },
        signal: AbortSignal.timeout(30000),
      },
    );
    if (!response.ok)
      throw new Error(
        `Validation HTTP ${response.status}: ${(await response.text()).slice(0, 1500)}`,
      );
    return response.json();
  };
  const query = (sql) =>
    request("/v0/sql", {
      method: "POST",
      body: new URLSearchParams({ q: `${sql}\nFORMAT JSON` }),
    });
  const existing = new Set(
    (await request("/v0/datasources")).datasources.map((row) => row.name),
  );
  if (![chunkTable, commitTable].every((name) => existing.has(name)))
    throw new Error("Build the dedicated proof schemas before testing");
  const append = async (table, rows) => {
    const result = await request(`/v0/events?name=${table}&wait=true`, {
      method: "POST",
      headers: { "Content-Type": "application/x-ndjson" },
      body: rows.map((row) => JSON.stringify(row)).join("\n"),
    });
    if (
      result.quarantined_rows ||
      Number(result.successful_rows) !== rows.length
    )
      throw new Error(`Append failed: ${JSON.stringify(result)}`);
    return result;
  };
  const scope = `scenario=${quote(scenario)}`;
  const chunkRows = (
    ids,
  ) => `SELECT DISTINCT chunk_id,groups,metrics,sessions,session_ids FROM ${chunkTable} WHERE ${scope}
    AND chunk_id IN (${ids.map(quote).join(",")})`;
  async function verify(chunks) {
    for (let offset = 0; offset < chunks.length; offset += 100)
      await verifyBatch(chunks.slice(offset, offset + 100));
  }
  async function verifyBatch(chunks) {
    if (!chunks.length) return;
    for (let attempt = 0; attempt < 40; attempt++) {
      const result = await query(chunkRows(chunks.map((row) => row.chunk_id)));
      const expected = new Map(chunks.map((row) => [row.chunk_id, row]));
      for (const row of result.data) {
        assert.equal(
          hash({
            groups: row.groups,
            metrics: row.metrics,
            sessions: row.sessions,
            session_ids: row.session_ids,
          }),
          row.chunk_id,
          "Conflicting chunk content",
        );
        assert.deepEqual(row, {
          chunk_id: row.chunk_id,
          groups: expected.get(row.chunk_id)?.groups,
          metrics: expected.get(row.chunk_id)?.metrics,
          sessions: expected.get(row.chunk_id)?.sessions,
          session_ids: expected.get(row.chunk_id)?.session_ids,
        });
      }
      if (result.data.length === chunks.length) return;
      await new Promise((resolve) =>
        setTimeout(resolve, Math.min(2000, 250 * (attempt + 1))),
      );
    }
    throw new Error(
      `Publication chunks are not all visible: ${JSON.stringify({ expected: chunks.map((row) => row.chunk_id), actual: (await query(chunkRows(chunks.map((row) => row.chunk_id)))).data })}`,
    );
  }
  function prepare(version, profiles) {
    if (!Number.isSafeInteger(version) || version < 1 || !profiles.size)
      throw new Error("Invalid publication");
    const chunks = [],
      profileIds = [],
      chunkIds = [];
    for (const [profile, rows] of [...profiles].sort(([a], [b]) =>
      a.localeCompare(b),
    )) {
      const sorted = rows.slice().sort((a, b) => a.key.localeCompare(b.key));
      const ids = [];
      for (let offset = 0; offset < sorted.length; offset += 100) {
        const batch = sorted.slice(offset, offset + 100);
        const payload = {
          groups: batch.map((row) => row.key),
          metrics: batch.map((row) => row.values.map(String)),
          sessions: batch.map((row) => row.sessions),
          session_ids: batch.map((row) => row.sessionIds),
        };
        const chunk_id = hash(payload);
        ids.push(chunk_id);
        chunks.push({ scenario, chunk_id, ...payload });
      }
      profileIds.push(profile);
      chunkIds.push(ids);
    }
    const unique = [
      ...new Map(chunks.map((row) => [row.chunk_id, row])).values(),
    ];
    const commit = {
      scenario,
      version,
      profiles: profileIds,
      chunk_ids: chunkIds,
    };
    return {
      chunks: unique,
      commit: { ...commit, transaction_id: hash(commit) },
    };
  }
  async function stage(prepared) {
    let batch = [],
      bytes = 0;
    for (const chunk of prepared.chunks) {
      const size = Buffer.byteLength(JSON.stringify(chunk)) + 1;
      if (size > 1000000)
        throw new Error("Chunk exceeds publication byte budget");
      if (bytes + size > 1000000 || batch.length === 25) {
        await append(chunkTable, batch);
        batch = [];
        bytes = 0;
      }
      batch.push(chunk);
      bytes += size;
    }
    if (batch.length) await append(chunkTable, batch);
    await verify(prepared.chunks);
  }
  async function commit(prepared) {
    await verify(prepared.chunks);
    const prior = (
      await query(
        `SELECT DISTINCT transaction_id FROM ${commitTable} WHERE ${scope} AND version=${prepared.commit.version}`,
      )
    ).data;
    if (
      prior.some((row) => row.transaction_id !== prepared.commit.transaction_id)
    )
      throw new Error("Conflicting commit version");
    await append(commitTable, [prepared.commit]);
  }
  // One commit scan feeds both data and integrity rows. A separate UNION can
  // observe different publication versions across its independent subqueries.
  const commits = `SELECT DISTINCT version,transaction_id,profiles,chunk_ids FROM ${commitTable} WHERE ${scope}`;
  const checkedCommits = `SELECT *,count() OVER (PARTITION BY version)>1 AS conflicts FROM (${commits})`;
  const expanded = `SELECT version,conflicts,
    arrayJoin(arrayZip(profiles,chunk_ids)) AS item FROM (${checkedCommits})`;
  const latest = `SELECT item.1 AS profile_id,argMax(item.2,version) AS ids,
    max(max(version)) OVER () AS snapshot_version,max(max(conflicts)) OVER () AS conflicts
    FROM (${expanded}) GROUP BY profile_id`;
  const expected = `SELECT profile_id,snapshot_version,conflicts,arrayJoin(arrayPushBack(ids,'')) AS chunk_id FROM (${latest})`;
  const chunks = `SELECT *,toUInt8(1) AS found,count() OVER (PARTITION BY chunk_id) AS copies
    FROM (SELECT DISTINCT chunk_id,groups,metrics,sessions,session_ids FROM ${chunkTable} WHERE ${scope})`;
  const selected = `SELECT e.snapshot_version,e.conflicts,e.chunk_id!='' AND (c.found=0 OR c.copies!=1) AS invalid,
    c.groups,c.metrics,c.session_ids FROM (${expected}) e LEFT JOIN (${chunks}) c ON e.chunk_id=c.chunk_id`;
  function reportSql(start = "0000-00-00", end = "9999-99-99") {
    // Every selected chunk contributes one metadata sentinel, even if missing
    // or outside the date window. Retired profiles also retain a sentinel.
    return `SELECT if(item.1='','integrity','data') AS kind,item.1 AS group_key,
      sumForEach(arrayMap(value -> toFloat64(value),item.2)) AS metrics,
      length(groupUniqArrayArray(item.3)) AS sessions,sum(invalid) AS invalid,
      max(snapshot_version) AS version,max(conflicts) AS conflicts FROM
      (SELECT snapshot_version,conflicts,invalid,
        arrayJoin(arrayPushBack(arrayZip(groups,metrics,session_ids),tuple('',[],[]))) AS item FROM (${selected}))
      WHERE item.1='' OR (JSONExtractString(item.1,1)>=${quote(start)} AND JSONExtractString(item.1,1)<${quote(end)})
      GROUP BY group_key
      SETTINGS max_execution_time=20,max_memory_usage=536870912`;
  }
  async function report(start, end, minVersion = 0) {
    const result = await query(reportSql(start, end));
    const integrity = result.data.filter((row) => row.kind === "integrity");
    if (!result.data.length && minVersion === 0)
      return { version: 0, rows: [], statistics: result.statistics };
    if (integrity.length !== 1)
      throw new Error("Requested report version is not visible");
    if (Number(integrity[0].conflicts) > 0)
      throw new Error("Conflicting committed versions");
    if (Number(integrity[0].invalid) > 0)
      throw new Error("Incomplete committed report");
    if (
      result.data.some(
        (row) =>
          row.kind === "data" &&
          Number(row.version) !== Number(integrity[0].version),
      )
    )
      throw new Error("Requested report version is not consistently visible");
    if (Number(integrity[0].version) < minVersion)
      throw new Error("Requested report version is not visible");
    return {
      version: Number(integrity[0].version),
      rows: result.data
        .filter((row) => row.kind === "data")
        .map((row) => ({
          key: row.group_key,
          values: row.metrics,
          sessions: Number(row.sessions),
        })),
      statistics: result.statistics,
    };
  }
  return {
    branchId: branch.id,
    prepare,
    stage,
    commit,
    report,
    reportSql,
    // Fault injection is available only in this isolated proof client.
    appendChunks: (rows) => append(chunkTable, rows),
    appendCommit: (row) => append(commitTable, [row]),
    readCommits: () =>
      query(
        `SELECT DISTINCT version,transaction_id FROM ${commitTable} WHERE ${scope}`,
      ),
  };
}
