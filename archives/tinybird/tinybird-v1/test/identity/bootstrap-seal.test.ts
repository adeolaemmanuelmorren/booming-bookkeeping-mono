import assert from "node:assert/strict";
import test from "node:test";
import { canonicalJson, sha256 } from "../../worker/storage/json.ts";
import { readIdentityBootstrapSeal } from "../../worker/identity/bootstrap-seal.ts";

const hash = "a".repeat(64);
const tables = [
  "v1_snapshot_activecampaign_contact", "v1_snapshot_activecampaign_contact_tag", "v1_snapshot_activecampaign_tags",
  "v1_snapshot_stripe_charge", "v1_snapshot_stripe_customer", "v1_snapshot_stripe_payment_intent",
  "v1_snapshot_stripe_kajabi_charge", "v1_snapshot_stripe_kajabi_customer", "v1_snapshot_stripe_kajabi_payment_intent",
];
const seal = {
  format: "identity-only-v1" as const,
  baselineId: "identity-1",
  tenantId: "boom",
  identityVersion: 1 as const,
  sourceSeal: hash,
  publicationHash: "b".repeat(64),
  nativeImportProofSha256: "c".repeat(64),
  inputs: { fivetran: { snapshotAt: "2026-09-05T22:28:00.000Z", tables: tables.map((table, expectedPhysicalRows) => ({ table, expectedPhysicalRows })) } },
  counts: { facts: 1, identifiers: 1, components: 1, recordLookups: 1 },
  membership: { version: 1 as const, buckets: 65_536 as const, pageSize: 256 as const, pageHashes: Array(256).fill(hash) },
};

test("accepts an exact identity-only version 1 seal", async () => {
  const payloadHash = await sha256(canonicalJson(seal));
  const result = await readIdentityBootstrapSeal({
    tenantId: "boom", baselineId: "identity-1", expectedSealHash: payloadHash,
    reader: { read: async () => ({ payload: seal, payloadHash }) },
  });
  assert.equal(result.receipt.identityVersion, 1);
  assert.equal(result.receipt.snapshotAt, "2026-09-05T22:28:00.000Z");
});

test("fails closed for a missing or altered membership proof", async () => {
  await assert.rejects(() => readIdentityBootstrapSeal({
    tenantId: "boom", baselineId: "identity-1", expectedSealHash: hash,
    reader: { read: async () => null },
  }), /missing/);
  const bad = structuredClone(seal);
  bad.membership.pageHashes.pop();
  const badHash = await sha256(canonicalJson(bad));
  await assert.rejects(() => readIdentityBootstrapSeal({
    tenantId: "boom", baselineId: "identity-1", expectedSealHash: badHash,
    reader: { read: async () => ({ payload: bad, payloadHash: await sha256(canonicalJson(bad)) }) },
  }), /membership proof/);
});

test("full seal pin rejects changed publication or native proof with the same source seal", async () => {
  const expected = await sha256(canonicalJson(seal));
  for (const field of ["publicationHash", "nativeImportProofSha256"] as const) {
    const changed = { ...seal, [field]: "d".repeat(64) };
    await assert.rejects(() => readIdentityBootstrapSeal({
      tenantId: "boom", baselineId: "identity-1", expectedSealHash: expected,
      reader: { read: async () => ({ payload: changed, payloadHash: await sha256(canonicalJson(changed)) }) },
    }), /seal hash mismatch/);
  }
});
