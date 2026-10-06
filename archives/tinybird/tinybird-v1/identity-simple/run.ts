import { createHash } from "node:crypto";
import { readFile, writeFile, mkdir, appendFile } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import { IdentityGraph } from "./graph.mjs";
import { appendOutput } from "./output.mjs";
import { Tinybird, sqlString } from "../worker/storage/tinybird.ts";
import { IdentityBootstrapStore } from "../worker/identity/bootstrap-store.ts";
import { changedIdentityFacts } from "../worker/identity/engine.ts";
import { validateProductionIdentityPlan } from "../worker/identity/bootstrap-plan.ts";

const plan = JSON.parse(
  await readFile(
    process.env.IDENTITY_PLAN ?? "/app/identity-plan.json",
    "utf8",
  ),
);
await validateProductionIdentityPlan(plan);
const host = process.env.TINYBIRD_URL;
const token = process.env.TINYBIRD_TOKEN;
if (host !== "https://api.us-east.tinybird.co" || !token)
  throw new Error("Missing approved Tinybird connection");
const workspace = await fetch(host + "/v1/workspace", {
  headers: { Authorization: `Bearer ${token}` },
}).then((r) => r.json());
if (workspace.id !== "00c04079-d0b4-4d8b-8de6-6fa8072b85af")
  throw new Error("Wrong identity destination");
const snapshot = process.env.SNAPSHOT_ID;
if (!snapshot || !/^identity-simple-[a-z0-9-]+$/.test(snapshot))
  throw new Error("Explicit snapshot ID required");
const client = new Tinybird({ TINYBIRD_URL: host, TINYBIRD_TOKEN: token });
const store = new IdentityBootstrapStore(
  client,
  plan.tenantId,
  plan.baselineId,
);
const receipts = await client.query<any>(
  `SELECT DISTINCT manifest_key,payload_json,payload_hash FROM v1_identity_bootstrap_manifests WHERE ${store.scope()} AND manifest_kind='source' AND sequence=0`,
);
const expectedInputs = [
  ...plan.inputs.browser,
  plan.inputs.live,
  ...plan.inputs.fivetran.tables.filter((t: any) =>
    /_(charge|contact_tag)$/.test(t.table),
  ),
];
if (receipts.length !== expectedInputs.length)
  throw new Error("Incomplete source preparation");
let expected = 0;
for (const input of expectedInputs) {
  const rows = receipts.filter((r: any) => r.manifest_key === input.table);
  if (rows.length !== 1)
    throw new Error("Missing or conflicting source receipt");
  if (
    createHash("sha256").update(rows[0].payload_json).digest("hex") !==
    rows[0].payload_hash
  )
    throw new Error("Source receipt hash mismatch");
  const receipt = JSON.parse(rows[0].payload_json);
  if (receipt.physical !== input.expectedPhysicalRows)
    throw new Error("Source receipt count differs");
  expected += receipt.candidates;
}
const log = (stage: string, data: object) =>
  console.log(
    JSON.stringify({
      at: new Date().toISOString(),
      stage,
      snapshot,
      ...data,
      rssMiB: Math.round(process.memoryUsage().rss / 1048576),
    }),
  );
log("reading", { expectedCandidates: expected });
const graph = new IdentityGraph(plan.maxIdentifiers);
let winner: any = null,
  candidates = 0,
  facts = 0;
const finish = () => {
  if (winner) {
    graph.addFact(winner);
    facts++;
  }
};
for await (const row of store.facts("candidate")) {
  if (
    createHash("sha256").update(row.fact_json).digest("hex") !== row.fact_hash
  )
    throw new Error("Prepared fact hash mismatch");
  const fact = JSON.parse(row.fact_json);
  if (
    fact.factKind !== row.fact_kind ||
    fact.factKey !== row.fact_key ||
    fact.sourceFactVersion !== Number(row.source_version) ||
    (fact.sourcePriority ?? 0) !== Number(row.source_priority)
  )
    throw new Error("Prepared fact metadata mismatch");
  if (
    !winner ||
    fact.factKind !== winner.factKind ||
    fact.factKey !== winner.factKey
  ) {
    finish();
    winner = fact;
  } else {
    const prior = {
      ...winner,
      factObservedAt: winner.observedAt,
      isDeleted: winner.factDeleted,
    };
    if (changedIdentityFacts([fact], [prior]).length) winner = fact;
  }
  candidates++;
  if (process.env.VALIDATE_ONLY === "true" && candidates >= 1000) break;
  if (candidates % 100000 === 0)
    log("reading", {
      candidates,
      expectedCandidates: expected,
      selectedFacts: facts,
      identifiers: graph.keys.size,
    });
}
finish();
if (process.env.VALIDATE_ONLY === "true") {
  log("preflight", {
    sampleCandidates: candidates,
    profiles: graph.resolve().size,
    passed: true,
  });
  process.exit(0);
}
if (candidates !== expected)
  throw new Error(`Candidate coverage differs: ${candidates} vs ${expected}`);
const profiles = graph.resolve();
log("resolved", {
  candidates,
  facts,
  identifiers: graph.keys.size,
  profiles: profiles.size,
});
const folder = "/tmp/identity-simple-output";
await mkdir(folder, { recursive: true });
const exportedAt = process.env.EXPORTED_AT;
if (!exportedAt || !Number.isFinite(Date.parse(exportedAt)))
  throw new Error("Fixed export timestamp required for resumable writes");
const expectedBuckets = new Map<string, string[]>();
const buffers = new Map<string, string[]>();
const bucket = (key: string) =>
  parseInt(createHash("sha256").update(key).digest("hex").slice(0, 2), 16);
async function flush(key: string) {
  const rows = buffers.get(key);
  if (!rows?.length) return;
  await appendFile(`${folder}/${key}.ndjson`, rows.join("\n") + "\n", {
    mode: 0o600,
  });
  rows.length = 0;
}
async function emit(table: string, identity: string, fields: object) {
  const b = bucket(identity),
    key = `${table}-${b}`;
  const row = {
    tenant_id: plan.tenantId,
    snapshot_id: snapshot,
    bucket: b,
    ...fields,
    exported_at: exportedAt,
  };
  const row_hash = createHash("sha256")
    .update(JSON.stringify(row))
    .digest("hex");
  if (!expectedBuckets.has(key)) {
    expectedBuckets.set(key, []);
    buffers.set(key, []);
  }
  expectedBuckets.get(key)!.push(row_hash);
  buffers.get(key)!.push(JSON.stringify({ ...row, row_hash }));
  if (buffers.get(key)!.length >= 500) await flush(key);
}
for (const [key, id] of graph.keys) {
  const node = graph.nodes[id],
    profile = profiles.get(graph.find(id));
  await emit("identifiers", key, {
    profile_id: profile.id,
    id_type: node.type,
    id_value_norm: node.value,
    valid: null,
    validation_meta: null,
    source: node.source,
    action: "added",
    changed_at: node.last,
    first_seen_at: node.first,
    last_seen_at: node.last,
    source_stub: null,
  });
}
for (const p of profiles.values())
  await emit("profiles", p.id, {
    profile_id: p.id,
    winner_identifier: p.winner.key,
    identifier_count: p.count,
    first_seen_at: p.first,
    last_seen_at: p.last,
  });
for (const key of buffers.keys()) await flush(key);
log("prepared", {
  identifiers: graph.keys.size,
  profiles: profiles.size,
  buckets: expectedBuckets.size,
});
// No earlier baseline was published, so there are no merge redirects to invent.
const old = await client.query<any>(
  "SELECT count() AS n FROM v1_identity_commits WHERE tenant_id='boom'",
);
if (Number(old[0].n) !== 0)
  throw new Error(
    "Existing identity publication requires redirect reconciliation",
  );
async function publishBucket([key, hashes]: [string, string[]]) {
  const split = key.lastIndexOf("-"),
    table = key.slice(0, split),
    b = Number(key.slice(split + 1));
  let batch: any[] = [],
    bytes = 0;
  const upload = async () => {
    if (!batch.length) return;
    await appendOutput({ host, token }, table, batch);
    batch = [];
    bytes = 0;
  };
  for await (const line of createInterface({
    input: createReadStream(`${folder}/${key}.ndjson`),
    crlfDelay: Infinity,
  })) {
    if (bytes + Buffer.byteLength(line) > 1000000) await upload();
    batch.push(JSON.parse(line));
    bytes += Buffer.byteLength(line);
  }
  await upload();
  const wanted = [...new Set(hashes)].sort();
  if (wanted.length !== hashes.length) throw new Error("Duplicate output row");
  let verified = false;
  for (let attempt = 0; attempt < 40; attempt++) {
    const actual = await client.query<any>(
      `SELECT DISTINCT row_hash FROM ${table} WHERE tenant_id='boom' AND snapshot_id=${sqlString(snapshot)} AND bucket=${b} ORDER BY row_hash LIMIT ${wanted.length + 1}`,
    );
    if (
      actual.length === wanted.length &&
      actual.every((r: any, i: number) => r.row_hash === wanted[i])
    ) {
      verified = true;
      break;
    }
    if (actual.length > wanted.length)
      throw new Error("Conflicting output bucket");
    await new Promise((resolve) => setTimeout(resolve, 1500));
  }
  if (!verified) throw new Error("Output verification incomplete");
  log("published", { table, bucket: b, rows: hashes.length });
}
const buckets = [...expectedBuckets.entries()];
for (let offset = 0; offset < buckets.length; offset += 4) {
  const settled = await Promise.allSettled(
    buckets.slice(offset, offset + 4).map(publishBucket),
  );
  const failed = settled.find((r) => r.status === "rejected");
  if (failed?.status === "rejected") throw failed.reason;
}
const result = {
  complete: true,
  snapshot,
  sourceBaseline: plan.baselineId,
  sourceSeal: plan.sourceSeal,
  candidates,
  facts,
  identifiers: graph.keys.size,
  profiles: profiles.size,
  redirects: 0,
  exportedAt,
  continuousIdentityEnabled: false,
};
await writeFile(`${folder}/complete.json`, JSON.stringify(result));
log("complete", result);
