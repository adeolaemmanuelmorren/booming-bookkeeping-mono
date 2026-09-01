const bigQueryScalarKinds = new Set([
  "BIGNUMERIC",
  "BOOL",
  "BYTES",
  "DATE",
  "DATETIME",
  "FLOAT64",
  "GEOGRAPHY",
  "INT64",
  "JSON",
  "NUMERIC",
  "STRING",
  "TIME",
  "TIMESTAMP",
]);

export function parseBigQueryType(source) {
  const tokens = source.match(/[A-Za-z_][A-Za-z0-9_]*|\d+|[<>,()]/g) ?? [];
  let position = 0;

  function take(expected) {
    const value = tokens[position];

    if (expected && value !== expected) {
      throw new Error(`Expected ${expected} in BigQuery type ${source}`);
    }

    if (value === undefined) {
      throw new Error(`Unexpected end of BigQuery type ${source}`);
    }

    position += 1;
    return value;
  }

  function parseType() {
    const name = take().toUpperCase();

    if (name === "ARRAY") {
      take("<");
      const element = parseType();
      take(">");
      return { kind: "array", element, source: `ARRAY<${element.source}>` };
    }

    if (name === "STRUCT") {
      take("<");
      const fields = [];

      while (tokens[position] !== ">") {
        const fieldName = take();
        const type = parseType();
        fields.push({ name: fieldName, type });

        if (tokens[position] !== ",") {
          break;
        }

        take(",");
      }

      take(">");
      return { kind: "struct", fields, source };
    }

    if (!bigQueryScalarKinds.has(name)) {
      throw new Error(`Unsupported BigQuery type ${name}`);
    }

    const parameters = [];
    if (tokens[position] === "(") {
      take("(");

      while (tokens[position] !== ")") {
        parameters.push(Number(take()));

        if (tokens[position] !== ",") {
          break;
        }

        take(",");
      }

      take(")");
    }

    return { kind: "scalar", name, parameters, source: name };
  }

  const parsed = parseType();
  if (position !== tokens.length) {
    throw new Error(`Could not parse complete BigQuery type ${source}`);
  }

  return parsed;
}

export function parseTinybirdType(source) {
  return parseTinybirdTypeInner(source.trim(), false);
}

export function areTypesCompatible(bigQueryType, tinybirdType) {
  const expected = typeof bigQueryType === "string"
    ? parseBigQueryType(bigQueryType)
    : bigQueryType;
  const actual = typeof tinybirdType === "string"
    ? parseTinybirdType(tinybirdType)
    : tinybirdType;

  return compareTypeNodes(expected, actual);
}

function parseTinybirdTypeInner(source, nullable) {
  const call = parseOuterCall(source);

  if (call && ["Nullable", "LowCardinality"].includes(call.name)) {
    const parsed = parseTinybirdTypeInner(
      call.body,
      nullable || call.name === "Nullable",
    );
    return { ...parsed, nullable: parsed.nullable || nullable || call.name === "Nullable" };
  }

  if (call?.name === "Array") {
    return {
      kind: "array",
      element: parseTinybirdTypeInner(call.body, false),
      nullable,
      source,
    };
  }

  if (call?.name === "Tuple") {
    const fields = splitTopLevel(call.body).map((fieldSource) => {
      const namedField = splitNamedTupleField(fieldSource);
      return {
        name: namedField.name,
        type: parseTinybirdTypeInner(namedField.type, false),
      };
    });

    return { kind: "struct", fields, nullable, source };
  }

  const name = call?.name ?? source;
  const parameters = call ? splitTopLevel(call.body) : [];
  return tinybirdScalar(name, parameters, nullable, source);
}

function tinybirdScalar(name, parameters, nullable, source) {
  if (["String", "FixedString", "UUID", "IPv4", "IPv6"].includes(name)) {
    return { kind: "scalar", logical: "string", name, nullable, source };
  }

  if (/^(?:U?Int)(?:8|16|32|64|128|256)$/.test(name)) {
    return { kind: "scalar", logical: "integer", name, nullable, source };
  }

  if (name === "Bool") {
    return { kind: "scalar", logical: "boolean", name, nullable, source };
  }

  if (name === "Float64" || name === "Float32") {
    return {
      kind: "scalar",
      logical: name.toLowerCase(),
      name,
      nullable,
      source,
    };
  }

  if (name === "Decimal" || /^Decimal(?:32|64|128|256)$/.test(name)) {
    const scaleIndex = name === "Decimal" ? 1 : 0;
    const scale = Number(parameters[scaleIndex]);
    return { kind: "scalar", logical: "decimal", name, scale, nullable, source };
  }

  if (name === "Date" || name === "Date32") {
    return { kind: "scalar", logical: "date", name, nullable, source };
  }

  if (name === "DateTime64") {
    return {
      kind: "scalar",
      logical: "timestamp",
      name,
      precision: Number(parameters[0] ?? 0),
      nullable,
      source,
    };
  }

  if (name === "DateTime") {
    return {
      kind: "scalar",
      logical: "timestamp",
      name,
      precision: 0,
      nullable,
      source,
    };
  }

  if (["JSON", "Object"].includes(name)) {
    return { kind: "scalar", logical: "json", name, nullable, source };
  }

  return { kind: "scalar", logical: "unknown", name, nullable, source };
}

function compareTypeNodes(expected, actual) {
  if (expected.kind !== actual.kind) {
    return { compatible: false, reason: `${expected.kind} is not ${actual.kind}` };
  }

  if (expected.kind === "array") {
    return compareTypeNodes(expected.element, actual.element);
  }

  if (expected.kind === "struct") {
    if (expected.fields.length !== actual.fields.length) {
      return { compatible: false, reason: "struct field counts differ" };
    }

    for (let index = 0; index < expected.fields.length; index += 1) {
      const expectedField = expected.fields[index];
      const actualField = actual.fields[index];

      if (actualField.name && expectedField.name !== actualField.name) {
        return {
          compatible: false,
          reason: `struct field ${expectedField.name} is ${actualField.name}`,
        };
      }

      const fieldResult = compareTypeNodes(expectedField.type, actualField.type);
      if (!fieldResult.compatible) {
        return {
          compatible: false,
          reason: `${expectedField.name}: ${fieldResult.reason}`,
        };
      }
    }

    return { compatible: true, reason: null };
  }

  return compareScalarTypes(expected, actual);
}

function compareScalarTypes(expected, actual) {
  const expectedName = expected.name;

  if (expectedName === "STRING") {
    return result(actual.logical === "string", "expected a string");
  }

  if (expectedName === "INT64") {
    return result(actual.logical === "integer", "expected an integer");
  }

  if (expectedName === "FLOAT64") {
    return result(actual.logical === "float64", "expected Float64 precision");
  }

  if (["NUMERIC", "BIGNUMERIC"].includes(expectedName)) {
    const requiredScale = expectedName === "NUMERIC" ? 9 : 38;
    const compatible = actual.logical === "decimal" && actual.scale >= requiredScale;
    return result(compatible, `expected a decimal with scale ${requiredScale} or greater`);
  }

  if (expectedName === "BOOL") {
    const clickHouseBoolean = actual.logical === "boolean"
      || (actual.logical === "integer" && actual.name === "UInt8");
    return result(clickHouseBoolean, "expected Bool or UInt8");
  }

  if (expectedName === "TIMESTAMP") {
    const compatible = actual.logical === "timestamp" && actual.precision >= 6;
    return result(compatible, "expected microsecond timestamp precision");
  }

  if (expectedName === "DATE") {
    return result(actual.logical === "date", "expected a date");
  }

  if (expectedName === "JSON") {
    return result(actual.logical === "json", "expected JSON");
  }

  return result(false, `unsupported BigQuery scalar ${expectedName}`);
}

function result(compatible, reason) {
  return { compatible, reason: compatible ? null : reason };
}

function parseOuterCall(source) {
  const openIndex = source.indexOf("(");
  if (openIndex === -1 || !source.endsWith(")")) {
    return null;
  }

  let depth = 0;
  for (let index = openIndex; index < source.length; index += 1) {
    const character = source[index];
    if (character === "(") depth += 1;
    if (character === ")") depth -= 1;

    if (depth === 0 && index !== source.length - 1) {
      return null;
    }
  }

  return {
    name: source.slice(0, openIndex).trim(),
    body: source.slice(openIndex + 1, -1).trim(),
  };
}

function splitTopLevel(source) {
  if (!source) {
    return [];
  }

  const parts = [];
  let start = 0;
  let depth = 0;
  let quote = null;

  for (let index = 0; index < source.length; index += 1) {
    const character = source[index];

    if (quote) {
      if (character === quote && source[index - 1] !== "\\") quote = null;
      continue;
    }

    if (["'", '"', "`"].includes(character)) {
      quote = character;
      continue;
    }

    if (character === "(") depth += 1;
    if (character === ")") depth -= 1;

    if (character === "," && depth === 0) {
      parts.push(source.slice(start, index).trim());
      start = index + 1;
    }
  }

  parts.push(source.slice(start).trim());
  return parts;
}

function splitNamedTupleField(source) {
  let depth = 0;
  let quote = null;

  for (let index = 0; index < source.length; index += 1) {
    const character = source[index];

    if (quote) {
      if (character === quote && source[index - 1] !== "\\") quote = null;
      continue;
    }

    if (["'", '"', "`"].includes(character)) {
      quote = character;
      continue;
    }

    if (character === "(") depth += 1;
    if (character === ")") depth -= 1;

    if (/\s/.test(character) && depth === 0) {
      const name = source.slice(0, index).replace(/^[`"]|[`"]$/g, "");
      const type = source.slice(index).trim();
      return { name, type };
    }
  }

  return { name: null, type: source };
}
