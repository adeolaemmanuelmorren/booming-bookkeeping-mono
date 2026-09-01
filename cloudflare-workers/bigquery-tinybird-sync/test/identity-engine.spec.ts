import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  changedIdentityFacts,
  runIdentityEngine,
  type CurrentIdentityFact,
  type CurrentIdentityMapping,
  type CurrentIdentityProfile,
  type PendingIdentityFact,
} from "../src/identity-engine";
import { md5Hex } from "../src/md5";

const zeroHash = "0".repeat(64);

describe("identity Worker engine", () => {
  it("implements MD5 for the deterministic BigQuery profile ID", () => {
    for (const value of ["", "email:a@example.com", "anonymous_id:visitor-💥"]) {
      const expected = createHash("md5").update(value).digest("hex");
      expect(md5Hex(value)).toBe(expected);
    }
  });

  it("applies the same fact winner rules as the compaction contract", () => {
    const stable = currentFact("fact-1", ["email:a@example.com"], 8);

    expect(changedIdentityFacts([
      pendingFact("fact-1", ["email:b@example.com"], 7),
    ], [stable])).toEqual([]);
    expect(changedIdentityFacts([
      pendingFact("fact-1", ["email:b@example.com"], 8, "changed"),
    ], [stable])).toHaveLength(1);
    expect(changedIdentityFacts([
      pendingFact("fact-1", [], 9, "deleted", true),
    ], [stable])).toHaveLength(1);

    const deleted = currentFact("fact-1", [], 9);
    deleted.factDeleted = true;
    deleted.isDeleted = true;
    deleted.factPayloadHash = sha256("deleted");
    expect(changedIdentityFacts([
      pendingFact("fact-1", [], 9, "deleted", true),
    ], [deleted])).toEqual([]);
  });

  it("merges affected profiles, flattens history, and emits a deterministic journal", async () => {
    const emailA = "email:a@example.com";
    const anonymous = "anonymous_id:visitor-1";
    const emailB = "email:b@example.com";
    const profileA = md5Hex(emailA);
    const profileB = md5Hex(emailB);
    const pending = pendingFact("bridge", [anonymous, emailB], 1, "bridge");
    const currentFacts = [
      currentFact("left", [emailA, anonymous], 1, "2026-08-25 01:00:00.000000"),
      currentFact("right", [emailB], 1, "2026-08-25 02:00:00.000000"),
    ];
    const mappings: CurrentIdentityMapping[] = [
      mapping(emailA, profileA, "2026-08-25 01:00:00.000000"),
      mapping(anonymous, profileA, "2026-08-25 01:00:00.000000"),
      mapping(emailB, profileB, "2026-08-25 02:00:00.000000"),
    ];
    const profiles: CurrentIdentityProfile[] = [
      profile(profileA, emailA, [emailA, anonymous], ["historical-a", profileA]),
      profile(profileB, emailB, [emailB], [profileB]),
    ];

    const result = await runIdentityEngine({
      tenantId: "boom",
      batchVersion: 20,
      batchId: "batch_20",
      committedAt: "2026-08-28T03:00:00.000Z",
      pendingFacts: [pending],
      currentFacts,
      currentMappings: mappings,
      currentProfiles: profiles,
      checkpointIngestedAt: pending.ingestedAt,
      checkpointEventId: pending.eventId,
    });

    const merged = result.rows.find((row) => (
      row.state_kind === "profile" && row.is_deleted === 0
    ));
    expect(merged).toMatchObject({
      profile_id: profileA,
      winner_identifier_key: emailA,
      member_identifier_keys: [anonymous, emailA, emailB].sort(),
      historical_profile_ids: ["historical-a", profileA, profileB].sort(),
    });
    expect(result.rows).toContainEqual(expect.objectContaining({
      state_kind: "profile",
      profile_id: profileB,
      is_deleted: 1,
    }));
    expect(result.rows.filter((row) => row.state_kind === "mapping")).toHaveLength(3);
    expect(result.rows.filter((row) => row.state_kind === "evidence")).toHaveLength(2);
    expect(result.manifest.output_row_count).toBe(result.rows.length);
    expect(result.manifest.output_hash).toBe(result.outputHash);

    const retry = await runIdentityEngine({
      tenantId: "boom",
      batchVersion: 20,
      batchId: "batch_20",
      committedAt: "2026-08-28T03:05:00.000Z",
      pendingFacts: [pending],
      currentFacts,
      currentMappings: mappings,
      currentProfiles: profiles,
      checkpointIngestedAt: pending.ingestedAt,
      checkpointEventId: pending.eventId,
    });
    expect(retry.outputHash).toBe(result.outputHash);
    expect(retry.manifest.row_hash).toBe(result.manifest.row_hash);
  });

  it("uses each name's latest non-empty fact timestamp", async () => {
    const email = "email:a@example.com";
    const older = currentFact("older", [email], 1, "2026-08-25 01:00:00.000000");
    older.firstName = "Ada";
    older.lastName = "Lovelace";
    const correction = pendingFact("newer", [email], 2, "correction");
    correction.observedAt = "2026-08-25 02:00:00.000000";
    correction.factPayload = JSON.stringify({ first_name: "Grace", last_name: "" });

    const result = await runIdentityEngine({
      tenantId: "boom",
      batchVersion: 21,
      batchId: "batch_21",
      committedAt: "2026-08-28T03:05:00.000Z",
      pendingFacts: [correction],
      currentFacts: [older],
      currentMappings: [],
      currentProfiles: [],
      checkpointIngestedAt: correction.ingestedAt,
      checkpointEventId: correction.eventId,
    });
    const profileRow = result.rows.find((row) => (
      row.state_kind === "profile" && row.is_deleted === 0
    ));

    expect(profileRow).toMatchObject({ first_name: "Grace", last_name: "Lovelace" });
  });
});

function pendingFact(
  factKey: string,
  evidenceKeys: string[],
  sourceFactVersion: number,
  hash = "pending",
  deleted = false,
): PendingIdentityFact {
  return {
    eventId: sha256(`event:${factKey}:${sourceFactVersion}:${hash}`),
    producerId: "fixture",
    observedAt: "2026-08-27 03:00:00.000000",
    ingestedAt: "2026-08-27 03:01:00.000000",
    factKind: "fixture",
    factKey,
    sourceFactVersion,
    factDeleted: deleted,
    factPayloadHash: sha256(hash),
    factPayload: JSON.stringify({ first_name: "Ada", last_name: "Lovelace" }),
    evidenceKeys,
  };
}

function currentFact(
  factKey: string,
  evidenceKeys: string[],
  sourceFactVersion: number,
  observedAt = "2026-08-25 01:00:00.000000",
): CurrentIdentityFact {
  return {
    producerId: "fixture",
    factKind: "fixture",
    factKey,
    sourceFactVersion,
    factDeleted: false,
    factObservedAt: observedAt,
    factPayloadHash: sha256("stable"),
    evidenceKeys,
    firstName: "Grace",
    lastName: "Hopper",
    isDeleted: false,
  };
}

function mapping(
  identifierKey: string,
  profileId: string,
  firstSeenAt: string,
): CurrentIdentityMapping {
  const separator = identifierKey.indexOf(":");
  return {
    identifierType: identifierKey.slice(0, separator),
    identifierValue: identifierKey.slice(separator + 1),
    identifierKey,
    profileId,
    firstSeenAt,
    lastSeenAt: firstSeenAt,
  };
}

function profile(
  profileId: string,
  winnerIdentifierKey: string,
  memberIdentifierKeys: string[],
  historicalProfileIds: string[],
): CurrentIdentityProfile {
  return {
    profileId,
    profileKey: winnerIdentifierKey.split(":").slice(1).join(":"),
    winnerIdentifierKey,
    memberIdentifierKeys,
    historicalProfileIds,
    firstName: "",
    lastName: "",
    firstSeenAt: "2026-08-25 01:00:00.000000",
    lastSeenAt: "2026-08-25 02:00:00.000000",
  };
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
