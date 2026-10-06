export function requireObject(value, fieldName) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${fieldName} must be an object`);
  }

  return value;
}

export function requireString(value, fieldName) {
  const normalized = cleanString(value);

  if (!normalized) {
    throw new TypeError(`${fieldName} must be a non-empty string`);
  }

  return normalized;
}

export function cleanString(value) {
  if (value === null || value === undefined) {
    return null;
  }

  const normalized = String(value).trim();
  return normalized || null;
}

export function lowerString(value) {
  return cleanString(value)?.toLowerCase() ?? null;
}

export function extractId(value) {
  if (typeof value === "string") {
    return cleanString(value);
  }

  if (value && typeof value === "object") {
    return cleanString(value.id);
  }

  return null;
}

export function requireNonNegativeInteger(value, fieldName) {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${fieldName} must be a non-negative safe integer`);
  }

  return value;
}

export function toIsoTimestamp(value, fieldName) {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    throw new TypeError(`${fieldName} must be a valid timestamp`);
  }

  return date.toISOString();
}

export function unixSecondsToIso(value, fieldName) {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${fieldName} must be Unix seconds`);
  }

  return new Date(value * 1000).toISOString();
}

export function cloneEvidence(value) {
  return structuredClone(value);
}

export function canonicalJson(value) {
  return JSON.stringify(sortJson(value));
}

function sortJson(value) {
  if (Array.isArray(value)) {
    return value.map(sortJson);
  }

  if (!value || typeof value !== "object") {
    return value;
  }

  const sorted = {};

  for (const key of Object.keys(value).sort()) {
    sorted[key] = sortJson(value[key]);
  }

  return sorted;
}
