import type { JsonRecord } from "./contracts.ts";
import { canonicalJson, jsonRecord } from "./json.ts";
import { compareTimestamps, parseTimestamp } from "./timestamp.ts";

export interface CollapsedVersions {
  versions: JsonRecord[];
  currentById: Map<string, JsonRecord>;
}

/**
 * Deduplicate append-only landing rows and reconstruct the current row for each
 * logical ID. Transport observation is the state order; source sync time only
 * breaks ties inside one observation. A physical-delete observation removes
 * the current row until a later observation recreates it.
 */
export function collapseFivetranVersions(input: {
  rows: readonly JsonRecord[];
  idField?: string;
  throughInclusive?: string;
}): CollapsedVersions {
  const idField = input.idField ?? "id";
  const versionsByObservation = new Map<string, JsonRecord>();
  const latestDeliveryByState = new Map<string, JsonRecord>();

  for (const value of input.rows) {
    const row = jsonRecord(value, "Fivetran row");
    const id = identifier(row[idField], `Fivetran row ${idField}`);
    const synced = sourceVersion(row);
    const observed = observationVersion(row);

    if (
      input.throughInclusive &&
      compareTimestamps(observed, input.throughInclusive) > 0
    ) {
      continue;
    }

    const observationKey = `${id}\u001f${synced}\u001f${observed}`;
    const prior = versionsByObservation.get(observationKey);

    if (!prior) {
      versionsByObservation.set(observationKey, structuredClone(row));
    } else if (canonicalSourcePayload(prior) !== canonicalSourcePayload(row)) {
      throw new Error(
        `Conflicting Fivetran rows for ${id} at ${synced}, observed ${observed}`,
      );
    }

    const stateKey = `${id}\u001f${synced}\u001f${canonicalSourcePayload(row)}`;
    const earlierDelivery = latestDeliveryByState.get(stateKey);

    if (
      !earlierDelivery ||
      compareTimestamps(observed, observationVersion(earlierDelivery)) > 0
    ) {
      latestDeliveryByState.set(stateKey, structuredClone(row));
    }
  }

  const versions = [...latestDeliveryByState.values()].sort((left, right) => {
    const idOrder = compareIds(String(left[idField]), String(right[idField]));

    if (idOrder !== 0) return idOrder;
    const observationOrder = compareTimestamps(
      observationVersion(left),
      observationVersion(right),
    );

    if (observationOrder !== 0) return observationOrder;
    return compareTimestamps(sourceVersion(left), sourceVersion(right));
  });
  const currentById = new Map<string, JsonRecord>();

  for (const row of versions) {
    const id = identifier(row[idField], idField);
    if (isPhysicalDelete(row)) {
      currentById.delete(id);
      continue;
    }
    const prior = currentById.get(id) ?? {};
    currentById.set(id, mergeDefined(prior, row));
  }

  return { versions, currentById };
}

export function sourceVersion(row: JsonRecord): string {
  return parseTimestamp(row._fivetran_synced, "_fivetran_synced").iso;
}

/** Transport observation time controls discovery; it never replaces source time. */
export function observationVersion(row: JsonRecord): string {
  const value = row._v1_observed_at ?? row._fivetran_synced;
  return parseTimestamp(value, "_v1_observed_at").iso;
}

export function latestSourceVersion(rows: readonly JsonRecord[]): string | null {
  let latest: string | null = null;

  for (const row of rows) {
    const version = sourceVersion(row);

    if (!latest || compareTimestamps(version, latest) > 0) {
      latest = version;
    }
  }

  return latest;
}

export function identifier(value: unknown, fieldName: string): string {
  if (typeof value === "number" && Number.isSafeInteger(value)) {
    return String(value);
  }

  if (typeof value !== "string" || !value.trim()) {
    throw new TypeError(`${fieldName} must be a non-empty identifier`);
  }

  return value.trim();
}

export function isDeleted(value: unknown): boolean {
  return value === true || value === 1 || value === "1" || value === "true";
}

/** Missing means false for immutable snapshot rows that predate this metadata. */
export function isPhysicalDelete(row: JsonRecord): boolean {
  const value = row._v1_deleted;
  if (value === undefined) return false;
  if (value === true || value === 1) return true;
  if (value === false || value === 0) return false;
  throw new TypeError("_v1_deleted must be a boolean or 0/1");
}

export function optionalString(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text || null;
}

export function requiredInteger(value: unknown, fieldName: string): number {
  const integer = typeof value === "string" && value.trim()
    ? Number(value)
    : value;

  if (!Number.isSafeInteger(integer)) {
    throw new TypeError(`${fieldName} must be a safe integer`);
  }

  return integer as number;
}

export function booleanValue(value: unknown): boolean {
  return value === true || value === 1 || value === "1" || value === "true";
}

export function optionalTimestamp(value: unknown, fieldName: string): string | null {
  if (value === null || value === undefined || value === "") return null;
  return parseTimestamp(value, fieldName).iso;
}

function mergeDefined(current: JsonRecord, update: JsonRecord): JsonRecord {
  const result = { ...current };

  for (const [key, value] of Object.entries(update)) {
    if (value !== undefined) result[key] = value;
  }

  return result;
}

function canonicalSourcePayload(row: JsonRecord): string {
  const payload = Object.fromEntries(
    Object.entries(row).filter(([key]) =>
      key !== "_v1_observed_at" &&
      key !== "_v1_observation_kind" &&
      key !== "_v1_deleted" &&
      !key.startsWith("__tb_"),
    ),
  );
  return canonicalJson({ ...payload, _v1_deleted: isPhysicalDelete(row) });
}

function compareIds(left: string, right: string): number {
  if (/^[0-9]+$/.test(left) && /^[0-9]+$/.test(right)) {
    const lengthOrder = left.length - right.length;
    if (lengthOrder !== 0) return lengthOrder;
  }

  return left.localeCompare(right);
}
