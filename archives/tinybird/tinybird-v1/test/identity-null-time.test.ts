import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";
import {
  runIdentityEngine, changedIdentityFacts,
  type PendingIdentityFact, type CurrentIdentityFact, type IdentityJournalRow,
} from "../worker/identity/engine.ts";

const early = "2020-01-01T00:00:00.000000Z";
const late = "2026-01-01T00:00:00.000000Z";
const arrival = "2026-09-05T00:00:00.000000Z";
const md5 = (value: string) => createHash("md5").update(value).digest("hex");

function fact(key: string, keys: string[], observedAt: string | null, overrides: Partial<PendingIdentityFact> = {}): PendingIdentityFact {
  const payload = JSON.stringify({ first_name: null, last_name: null, keys });
  return {
    eventId: key, producerId: "test", factKind: "browser", factKey: key,
    observedAt, ingestedAt: arrival, sourceFactVersion: 1, factDeleted: false,
    factPayload: payload, factPayloadHash: createHash("sha256").update(payload).digest("hex"),
    evidenceKeys: keys, ...overrides,
  };
}

function namedFact(key: string, keys: string[], observedAt: string | null, firstName: string | null, lastName: string | null) {
  const payload = JSON.stringify({ first_name: firstName, last_name: lastName, keys });
  return fact(key, keys, observedAt, { factPayload: payload, factPayloadHash: createHash("sha256").update(payload).digest("hex") });
}

function current(input: PendingIdentityFact): CurrentIdentityFact {
  const names = JSON.parse(input.factPayload);
  return {
    producerId: input.producerId, factKind: input.factKind, factKey: input.factKey,
    sourcePriority: input.sourcePriority, sourceFactVersion: input.sourceFactVersion,
    factDeleted: input.factDeleted, factObservedAt: input.observedAt,
    factPayloadHash: input.factPayloadHash, evidenceKeys: input.evidenceKeys,
    firstName: names.first_name ?? "", lastName: names.last_name ?? "", isDeleted: input.factDeleted,
  };
}

function run(pendingFacts: PendingIdentityFact[], currentFacts: CurrentIdentityFact[] = []) {
  return runIdentityEngine({ tenantId: "test", batchId: "one", batchVersion: 1, committedAt: arrival,
    pendingFacts, currentFacts, currentMappings: [], currentProfiles: [],
    checkpointIngestedAt: arrival, checkpointEventId: "last" });
}

function oneProfile(rows: IdentityJournalRow[]) {
  const profiles = rows.filter((row) => row.state_kind === "profile" && !row.is_deleted);
  assert.equal(profiles.length, 1);
  return profiles[0];
}

test("a null-time email wins even when it also has a later dated observation", async () => {
  const result = await run([
    fact("null-z", ["anonymous_id:v", "email:z@example.com"], null),
    fact("dated-z", ["email:z@example.com"], late),
    fact("dated-a", ["anonymous_id:v", "email:a@example.com"], early),
  ]);
  const profile = oneProfile(result.rows);
  assert.equal(profile.profile_id, md5("email:z@example.com"));
  assert.equal(profile.winner_identifier_key, "email:z@example.com");
  assert.equal(profile.first_seen_at, early);
  assert.equal(profile.last_seen_at, late);
  const z = result.rows.find((row) => row.state_kind === "mapping" && row.identifier_key === "email:z@example.com")!;
  assert.equal(z.first_seen_at, late);
  assert.equal(z.last_seen_at, late);
});

test("two null-time candidates tie on identifier value, regardless of their known times", async () => {
  const result = await run([
    fact("null-b", ["anonymous_id:v", "email:b@example.com"], null),
    fact("old-b", ["email:b@example.com"], early),
    fact("null-a", ["anonymous_id:v", "email:a@example.com"], null),
    fact("new-a", ["email:a@example.com"], late),
  ]);
  assert.equal(oneProfile(result.rows).profile_id, md5("email:a@example.com"));
});

test("identifier priority still precedes null timestamps", async () => {
  const result = await run([
    fact("null-user", ["anonymous_id:v", "user_id:first"], null),
    fact("known-email", ["anonymous_id:v", "email:last@example.com"], late),
  ]);
  assert.equal(oneProfile(result.rows).profile_id, md5("email:last@example.com"));
});

test("an all-null component exists and its fact, evidence, mapping, and profile times stay null", async () => {
  const result = await run([fact("one", ["anonymous_id:v", "email:a@example.com"], null)]);
  assert.equal(oneProfile(result.rows).profile_id, md5("email:a@example.com"));
  for (const row of result.rows) {
    assert.equal(row.fact_observed_at, null, row.state_kind);
    assert.equal(row.first_seen_at, null, row.state_kind);
    assert.equal(row.last_seen_at, null, row.state_kind);
  }
});

test("dated names precede null-time names; a null-time name remains an independent field fallback", async () => {
  const result = await run([
    namedFact("fallback", ["anonymous_id:v", "email:a@example.com"], null, "Fallback", "Surname"),
    namedFact("dated", ["email:a@example.com"], early, "Dated", null),
    namedFact("latest-without-names", ["anonymous_id:v"], late, null, null),
  ]);
  const profile = oneProfile(result.rows);
  assert.equal(profile.first_name, "Dated");
  assert.equal(profile.last_name, "Surname");
});

test("all-null named facts remain eligible, including names on different members", async () => {
  const result = await run([
    namedFact("first", ["anonymous_id:v", "email:a@example.com"], null, "Ada", null),
    namedFact("last", ["anonymous_id:v"], null, null, "Lovelace"),
  ]);
  const profile = oneProfile(result.rows);
  assert.equal(profile.first_name, "Ada");
  assert.equal(profile.last_name, "Lovelace");
});

test("removing null-time evidence preserves the old null in its tombstone", async () => {
  const stable = fact("one", ["email:old@example.com"], null);
  const replacement = fact("one", ["email:new@example.com"], early, { sourceFactVersion: 2 });
  const result = await run([replacement], [current(stable)]);
  const tombstone = result.rows.find((row) => row.state_kind === "evidence" && row.is_deleted)!;
  assert.equal(tombstone.identifier_key, "email:old@example.com");
  assert.equal(tombstone.fact_observed_at, null);
  assert.equal(tombstone.first_seen_at, null);
  assert.equal(tombstone.last_seen_at, null);
  assert.equal(oneProfile(result.rows).first_seen_at, early);
});

test("a nullable correction changes the profile winner without inventing first-seen time", async () => {
  const z = fact("z", ["anonymous_id:v", "email:z@example.com"], late);
  const a = fact("a", ["anonymous_id:v", "email:a@example.com"], early);
  const corrected = fact("z", ["anonymous_id:v", "email:z@example.com"], null, { sourceFactVersion: 2 });
  const result = await run([corrected], [current(z), current(a)]);
  const profile = oneProfile(result.rows);
  assert.equal(profile.profile_id, md5("email:z@example.com"));
  assert.equal(profile.first_seen_at, early);
  assert.equal(profile.last_seen_at, early);
});

test("higher-priority source replaces a newer numeric source version and persists priority", async () => {
  const old = fact("one", ["email:historical@example.com"], early, { sourcePriority: 2, sourceFactVersion: 100 });
  const fresh = fact("one", ["email:live@example.com"], late, { sourcePriority: 3, sourceFactVersion: 1 });
  const result = await run([fresh], [current(old)]);
  assert.equal(result.changedFacts.length, 1);
  assert.equal(oneProfile(result.rows).profile_id, md5("email:live@example.com"));
  assert.equal(result.rows.find((row) => row.state_kind === "fact")!.source_priority, 3);
  assert.deepEqual(changedIdentityFacts([old], [current(fresh)]), []);
});

test("pending winner compares source priority before version and does not confuse cross-source versions", async () => {
  const historical = fact("one", ["email:historical@example.com"], early, { sourcePriority: 2, sourceFactVersion: 10 });
  const fresh = fact("one", ["email:live@example.com"], early, { sourcePriority: 3, sourceFactVersion: 10 });
  const result = await run([historical, fresh]);
  assert.deepEqual(result.changedFacts, [fresh]);
  assert.equal(oneProfile(result.rows).profile_id, md5("email:live@example.com"));
});

test("same-priority, same-version conflicts still fail, including time and evidence changes", () => {
  const stable = fact("one", ["email:a@example.com"], early, { sourcePriority: 3 });
  const changed = fact("one", ["email:b@example.com"], early, { sourcePriority: 3 });
  assert.throws(() => changedIdentityFacts([changed], [current(stable)]), /Conflicting identity/);
  assert.throws(() => changedIdentityFacts([{ ...stable, observedAt: null }], [current(stable)]), /Conflicting identity/);
  assert.throws(() => changedIdentityFacts([{ ...stable, evidenceKeys: ["email:other@example.com"] }], [current(stable)]), /Conflicting identity/);
  assert.throws(() => changedIdentityFacts([stable, changed], []), /Conflicting identity/);
});

test("an omitted priority retains provider priority zero and invalid priorities fail", () => {
  const legacy = fact("one", ["email:a@example.com"], early);
  assert.deepEqual(changedIdentityFacts([{ ...legacy, sourcePriority: 0 }], [current(legacy)]), []);
  assert.throws(() => changedIdentityFacts([{ ...legacy, sourcePriority: -1 }], []), /priority/);
  assert.throws(() => changedIdentityFacts([{ ...legacy, sourcePriority: 1.5 }], []), /priority/);
});

test("null winner ties use Unicode code point ordering rather than locale ordering", async () => {
  const result = await run([
    fact("one", ["email:é@example.com", "email:z@example.com"], null),
  ]);
  assert.equal(oneProfile(result.rows).profile_id, md5("email:z@example.com"));
});
