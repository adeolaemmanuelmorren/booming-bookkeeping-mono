import { canonicalJson, sha256 } from "../storage/json.ts";
import { PIPELINE_TABLES } from "../fivetran/contracts.ts";

export interface IdentityBootstrapSeal {
  format: "identity-only-v1";
  baselineId: string;
  tenantId: string;
  identityVersion: 1;
  sourceSeal: string;
  publicationHash: string;
  nativeImportProofSha256: string;
  membership: { version: 1; buckets: 65_536; pageSize: 256; pageHashes: string[] };
  inputs: {
    fivetran: { snapshotAt: string; tables: Array<{ table: string; expectedPhysicalRows: number }> };
  };
  counts: { facts: number; identifiers: number; components: number; recordLookups: number };
}

export interface IdentityBaselineReceipt {
  tenantId: string;
  baselineId: string;
  sourceSeal: string;
  identityVersion: 1;
  snapshotAt: string;
  sealHash: string;
  publicationHash: string;
}

export interface IdentitySealReader {
  read(tenantId: string, baselineId: string): Promise<{ payload: unknown; payloadHash: string } | null>;
}

export async function readIdentityBootstrapSeal(input: {
  tenantId: string;
  baselineId: string;
  expectedSealHash: string;
  reader: IdentitySealReader;
}): Promise<{ seal: IdentityBootstrapSeal; receipt: IdentityBaselineReceipt }> {
  const saved = await input.reader.read(input.tenantId, input.baselineId);
  if (!saved) throw new Error("Identity bootstrap seal is missing");
  const seal = validateSeal(saved.payload, input);
  const sealHash = await sha256(canonicalJson(seal));
  if (saved.payloadHash !== sealHash || sealHash !== input.expectedSealHash) throw new Error("Identity bootstrap seal hash mismatch");
  return {
    seal,
    receipt: {
      tenantId: seal.tenantId,
      baselineId: seal.baselineId,
      sourceSeal: seal.sourceSeal,
      identityVersion: seal.identityVersion,
      snapshotAt: seal.inputs.fivetran.snapshotAt,
      sealHash,
      publicationHash: seal.publicationHash,
    },
  };
}

function validateSeal(value: unknown, expected: { tenantId: string; baselineId: string }): IdentityBootstrapSeal {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Identity bootstrap seal is invalid");
  const seal = value as IdentityBootstrapSeal;
  if (seal.format !== "identity-only-v1" || seal.identityVersion !== 1) throw new Error("Identity bootstrap seal version is invalid");
  if (seal.tenantId !== expected.tenantId || seal.baselineId !== expected.baselineId) {
    throw new Error("Identity bootstrap seal scope changed");
  }
  if (!validHash(seal.sourceSeal) || !validHash(seal.publicationHash) || !validHash(seal.nativeImportProofSha256)) throw new Error("Identity bootstrap seal hashes are invalid");
  if (!seal.inputs?.fivetran || !Number.isFinite(Date.parse(seal.inputs.fivetran.snapshotAt))) throw new Error("Identity Fivetran snapshot is invalid");
  const expectedTables = [...new Set(Object.values(PIPELINE_TABLES).flat())]
    .map(table => table.replace(/^raw_/, "v1_snapshot_"))
    .sort();
  const tables = seal.inputs.fivetran.tables.map(row => row.table).sort();
  if (canonicalJson(tables) !== canonicalJson(expectedTables)) {
    throw new Error("Identity bootstrap seal must pin nine Fivetran tables");
  }
  for (const row of seal.inputs.fivetran.tables) {
    if (!Number.isSafeInteger(row.expectedPhysicalRows) || row.expectedPhysicalRows < 0) throw new Error("Identity Fivetran row count is invalid");
  }
  for (const key of ["facts", "identifiers", "components", "recordLookups"] as const) {
    if (!Number.isSafeInteger(seal.counts?.[key]) || seal.counts[key] < 0) throw new Error("Identity bootstrap counts are invalid");
  }
  if (seal.membership?.version !== 1 || seal.membership.buckets !== 65_536 || seal.membership.pageSize !== 256 || seal.membership.pageHashes.length !== 256) {
    throw new Error("Identity membership proof is incomplete");
  }
  if (!seal.membership.pageHashes.every(validHash)) throw new Error("Identity membership page hash is invalid");
  return seal;
}

function validHash(value: unknown): value is string {
  return typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
}
