import type { Fetcher } from "./bigquery";
import {
  appendJourneyCommitRows,
  appendJourneyVersionRows,
  readProfileJourneyRows,
  type TinybirdApiConfig,
} from "./tinybird-api";

export const JOURNEY_PROFILE_BATCH_LIMIT = 500;

export interface ProfileJourneyPage {
  profileIds: string[];
  identifierKeys: string[];
  identifierProfileIds: string[];
}

// A profile's member identifier keys must never be split across build calls:
// the window pipe only sees the keys it is given, so a partial key set yields
// journeys with missing touchpoints and wrong payment numbering.
export function takeProfilePage(
  profileIds: string[],
  keysByProfile: Map<string, string[]>,
  keyLimit: number = JOURNEY_PROFILE_BATCH_LIMIT,
): ProfileJourneyPage {
  const takenProfileIds: string[] = [];
  const profileByIdentifierKey = new Map<string, string>();

  for (const profileId of profileIds) {
    const memberKeys = uniqueStrings(keysByProfile.get(profileId) ?? []);
    if (memberKeys.length > keyLimit) {
      throw new Error(
        `Journey profile ${profileId} has ${memberKeys.length} identifier keys, above the ${keyLimit}-key page limit.`,
      );
    }
    if (
      takenProfileIds.length > 0
      && profileByIdentifierKey.size + memberKeys.length > keyLimit
    ) {
      break;
    }
    takenProfileIds.push(profileId);
    for (const identifierKey of memberKeys) {
      const existingProfileId = profileByIdentifierKey.get(identifierKey);
      if (existingProfileId && existingProfileId !== profileId) {
        throw new Error(`Journey identifier ${identifierKey} belongs to multiple profiles.`);
      }
      profileByIdentifierKey.set(identifierKey, profileId);
    }
  }

  const identifierKeys = [...profileByIdentifierKey.keys()].sort();

  return {
    profileIds: takenProfileIds,
    identifierKeys,
    identifierProfileIds: identifierKeys.map((key) => profileByIdentifierKey.get(key) ?? ""),
  };
}

export interface JourneyProfileBatchInput {
  tenantId: string;
  identifierKeys: string[];
  identifierProfileIds?: string[];
  conversionIds?: string[];
  batchVersion: number;
  batchId: string;
}

export interface JourneyProfileBatchResult {
  identifierCount: number;
  conversionCount: number;
  journeyRowCount: number;
}

export async function processJourneyProfileBatch(
  input: JourneyProfileBatchInput,
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<JourneyProfileBatchResult> {
  const identifierMapping = normalizedIdentifierMapping(
    input.identifierKeys,
    input.identifierProfileIds,
  );
  const identifierKeys = identifierMapping.identifierKeys;
  const conversionIds = uniqueStrings(input.conversionIds ?? []);
  assertValidInput(input, identifierKeys, conversionIds);

  if (identifierKeys.length === 0 && conversionIds.length === 0) {
    return { identifierCount: 0, conversionCount: 0, journeyRowCount: 0 };
  }

  const sourceRows = await readProfileJourneyRows(
    identifierKeys,
    conversionIds,
    identifierMapping.identifierProfileIds,
    config,
    fetcher,
  );
  const committedAt = new Date().toISOString();
  const versionRows = sourceRows.map((row) => journeyVersionRow(
    row,
    input,
    committedAt,
  ));
  const commitRows = journeyCommitRows(
    versionRows,
    conversionIds,
    input,
    committedAt,
  );

  await appendJourneyVersionRows(versionRows, config, fetcher);
  await appendJourneyCommitRows(commitRows, config, fetcher);

  return {
    identifierCount: identifierKeys.length,
    conversionCount: commitRows.length,
    journeyRowCount: versionRows.length,
  };
}

function normalizedIdentifierMapping(
  identifierKeys: string[],
  identifierProfileIds: string[] | undefined,
): { identifierKeys: string[]; identifierProfileIds?: string[] } {
  if (identifierProfileIds === undefined) {
    return { identifierKeys: uniqueStrings(identifierKeys) };
  }
  if (identifierKeys.length !== identifierProfileIds.length) {
    throw new Error("Journey identifier keys and profile IDs must have equal lengths.");
  }

  const profileByKey = new Map<string, string>();
  for (const [index, identifierKey] of identifierKeys.entries()) {
    const profileId = identifierProfileIds[index];
    if (!identifierKey || !profileId) {
      throw new Error("Journey literal identity mappings must be non-empty.");
    }
    const existingProfileId = profileByKey.get(identifierKey);
    if (existingProfileId && existingProfileId !== profileId) {
      throw new Error(`Journey identifier ${identifierKey} belongs to multiple profiles.`);
    }
    profileByKey.set(identifierKey, profileId);
  }

  const sortedKeys = [...profileByKey.keys()].sort();
  return {
    identifierKeys: sortedKeys,
    identifierProfileIds: sortedKeys.map((key) => profileByKey.get(key) ?? ""),
  };
}

function journeyVersionRow(
  row: Record<string, unknown>,
  input: JourneyProfileBatchInput,
  committedAt: string,
): Record<string, unknown> {
  const conversionId = requiredString(row.conversion_id, "conversion_id");
  const touchpointId = nullableString(row.touchpoint_id, "touchpoint_id");

  return {
    ...row,
    tenant_id: input.tenantId,
    batch_version: input.batchVersion,
    batch_id: input.batchId,
    committed_at: committedAt,
    journey_row_key: touchpointId ?? `offline:${conversionId}`,
  };
}

function journeyCommitRows(
  rows: Record<string, unknown>[],
  requestedConversionIds: string[],
  input: JourneyProfileBatchInput,
  committedAt: string,
): Record<string, unknown>[] {
  const conversions = new Map<string, {
    profileId: string | null;
    rowCount: number;
  }>();

  for (const row of rows) {
    const conversionId = requiredString(row.conversion_id, "conversion_id");
    const profileId = nullableString(row.profile_id, "profile_id");
    const current = conversions.get(conversionId);

    if (!current) {
      conversions.set(conversionId, { profileId, rowCount: 1 });
      continue;
    }
    if (current.profileId !== profileId) {
      throw new Error(`Journey conversion ${conversionId} returned multiple profiles.`);
    }
    current.rowCount += 1;
  }

  for (const conversionId of requestedConversionIds) {
    if (conversions.has(conversionId)) continue;
    conversions.set(conversionId, { profileId: null, rowCount: 0 });
  }

  return [...conversions.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([conversionId, value]) => ({
      tenant_id: input.tenantId,
      conversion_id: conversionId,
      profile_id: value.profileId,
      batch_version: input.batchVersion,
      batch_id: input.batchId,
      committed_at: committedAt,
      journey_row_count: value.rowCount,
      is_deleted: Number(value.rowCount === 0),
    }));
}

function assertValidInput(
  input: JourneyProfileBatchInput,
  identifierKeys: string[],
  conversionIds: string[],
): void {
  if (!input.tenantId) throw new Error("Journey tenant ID is required.");
  if (!input.batchId) throw new Error("Journey batch ID is required.");
  if (!Number.isSafeInteger(input.batchVersion) || input.batchVersion < 1) {
    throw new Error("Journey batch version must be a positive safe integer.");
  }
  if (identifierKeys.length > JOURNEY_PROFILE_BATCH_LIMIT) {
    throw new Error(
      `Journey identifier batch exceeds ${JOURNEY_PROFILE_BATCH_LIMIT} keys.`,
    );
  }
  if (conversionIds.length > JOURNEY_PROFILE_BATCH_LIMIT) {
    throw new Error(
      `Journey conversion batch exceeds ${JOURNEY_PROFILE_BATCH_LIMIT} conversions.`,
    );
  }
}

function requiredString(value: unknown, name: string): string {
  if (typeof value === "string" && value.length > 0) return value;
  throw new Error(`Journey row ${name} is missing.`);
}

function nullableString(value: unknown, name: string): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") return value;
  throw new Error(`Journey row ${name} is invalid.`);
}

function uniqueStrings(values: string[]): string[] {
  return [...new Set(values.filter(Boolean))].sort();
}
