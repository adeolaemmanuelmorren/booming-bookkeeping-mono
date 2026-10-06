import assert from "node:assert/strict";
import { IdentityGraph } from "./graph.mjs";
import { runIdentityEngine } from "../worker/identity/engine.ts";
for (let seed = 0; seed < 100; seed++) {
  const facts = Array.from({ length: 40 }, (_, i) => ({
    eventId: `e${i}`,
    producerId: "test",
    observedAt:
      i % 9 === 0
        ? null
        : `2026-09-${String((i % 7) + 1).padStart(2, "0")}T00:00:00.000000Z`,
    ingestedAt: "2026-09-07T00:00:00Z",
    factKind: "test",
    factKey: `${i}`,
    sourceFactVersion: 1,
    factDeleted: i % 17 === 0,
    factPayloadHash: `hash${i}`,
    factPayload: "{}",
    evidenceKeys: [
      `anonymous_id:${(i * 7 + seed) % 31}`,
      `email:${(i + seed) % 13}@example.com`,
      ...(i % 3 === 0 ? [`user_id:${i % 11}`] : []),
    ],
  }));
  const graph = new IdentityGraph();
  facts.forEach((f) => graph.addFact(f));
  const profiles = graph.resolve();
  const result = await runIdentityEngine({
    tenantId: "test",
    batchVersion: 1,
    batchId: "b",
    committedAt: "2026-09-07T00:00:00Z",
    pendingFacts: facts,
    currentFacts: [],
    currentMappings: [],
    currentProfiles: [],
    checkpointIngestedAt: "",
    checkpointEventId: "",
  });
  const mappings = result.rows.filter(
    (row) => row.state_kind === "mapping" && !row.is_deleted,
  );
  assert.equal(mappings.length, graph.keys.size);
  for (const row of mappings)
    assert.equal(
      profiles.get(graph.find(graph.keys.get(row.identifier_key))).id,
      row.profile_id,
    );
}
console.log(
  "100 graph fixtures match existing identity engine profile assignments",
);
const { appendOutput } = await import("./output.mjs");
const connection = { host: "https://api.us-east.tinybird.co", token: "test" };
await appendOutput(
  connection,
  "identifiers",
  [{ profile_id: "p" }],
  async (url, options) => {
    assert.ok(url.endsWith("name=identifiers&wait=true"));
    assert.equal(options.body, '{"profile_id":"p"}\n');
    return Response.json({ successful_rows: 1, quarantined_rows: 0 });
  },
);
await assert.rejects(
  appendOutput(connection, "profiles", [{}], async () =>
    Response.json({ successful_rows: 0, quarantined_rows: 1 }),
  ),
  /fully accepted/,
);
await assert.rejects(appendOutput(connection, "other", [{}]), /Unexpected/);
console.log(
  "Output table allowlist, serialization and partial-ingestion rejection pass",
);
