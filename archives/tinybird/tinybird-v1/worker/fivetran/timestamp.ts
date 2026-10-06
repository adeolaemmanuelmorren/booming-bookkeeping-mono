export interface ParsedTimestamp {
  iso: string;
  microseconds: bigint;
}

/** Parse DateTime64 values without discarding their microseconds. */
export function parseTimestamp(value: unknown, fieldName: string): ParsedTimestamp {
  if (value instanceof Date) {
    return fromMicroseconds(BigInt(value.valueOf()) * 1_000n);
  }

  if (typeof value !== "string" || !value.trim()) {
    throw new TypeError(`${fieldName} must be a timestamp`);
  }

  const match = value.trim().match(
    /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:?\d{2})?$/,
  );

  if (!match) {
    throw new TypeError(`${fieldName} must be an ISO timestamp`);
  }

  const zone = match[4] ?? "Z";
  const milliseconds = Date.parse(`${match[1]}T${match[2]}${zone}`);

  if (!Number.isFinite(milliseconds)) {
    throw new TypeError(`${fieldName} must be an ISO timestamp`);
  }

  const fraction = (match[3] ?? "").padEnd(6, "0").slice(0, 6);
  return fromMicroseconds(BigInt(milliseconds) * 1_000n + BigInt(fraction || "0"));
}

export function compareTimestamps(left: string, right: string): number {
  const leftValue = parseTimestamp(left, "left timestamp").microseconds;
  const rightValue = parseTimestamp(right, "right timestamp").microseconds;

  if (leftValue < rightValue) return -1;
  if (leftValue > rightValue) return 1;
  return 0;
}

export function minimumTimestamp(values: readonly string[]): string {
  if (!values.length) {
    throw new TypeError("at least one timestamp is required");
  }

  return values.reduce((minimum, value) =>
    compareTimestamps(value, minimum) < 0 ? value : minimum,
  );
}

function fromMicroseconds(microseconds: bigint): ParsedTimestamp {
  const milliseconds = Number(microseconds / 1_000n);
  const wholeSecond = new Date(milliseconds).toISOString().slice(0, 19);
  const fraction = (microseconds % 1_000_000n).toString().padStart(6, "0");

  return {
    iso: `${wholeSecond}.${fraction}Z`,
    microseconds,
  };
}
