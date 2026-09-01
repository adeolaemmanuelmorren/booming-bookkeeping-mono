import { parseBigQueryType } from "./parity-types.mjs";

const unsignedInt64Range = "18446744073709551616";

export function buildBigQuerySchemaSql(reference) {
  const table = parseBigQueryTableReference(reference);
  const columnsTable = quoteBigQueryReference(
    `${table.project}.${table.dataset}.INFORMATION_SCHEMA.COLUMNS`,
  );
  const tablesTable = quoteBigQueryReference(
    `${table.project}.${table.dataset}.INFORMATION_SCHEMA.TABLES`,
  );

  return `
    SELECT
      columns.column_name,
      columns.data_type,
      columns.is_nullable,
      columns.ordinal_position,
      tables.table_type
    FROM ${columnsTable} AS columns
    INNER JOIN ${tablesTable} AS tables USING (table_name)
    WHERE columns.table_name = @table_name
    ORDER BY columns.ordinal_position
  `;
}

export function buildBigQueryDigestSql(options) {
  if (options.key?.length > 0) {
    return buildBigQueryKeyedDigestSql(options);
  }

  const prepared = prepareColumns(options.columns);
  const encodedFields = prepared.map((column) =>
    bigQueryEncodedValue(
      column.type,
      quoteBigQueryIdentifier(column.name),
      column.name,
      options,
    )
  );
  const metricFields = prepared.flatMap((column, index) =>
    bigQueryMetricFields(column, index, options.floatScale)
  );
  const metricAliases = metricAliasDefinitions(prepared);
  const sourceClause = buildBigQuerySourceClause(options.source);
  const shardFilter = bigQueryShardFilter(prepared, options);

  return `
    WITH canonical_rows AS (
      SELECT
        FARM_FINGERPRINT(CONCAT(
          ${encodedFields.join(",\n          ")}
        )) AS signed_fingerprint${commaLines(metricFields, 8)}
      FROM ${sourceClause}${shardFilter}
    ),
    fingerprints AS (
      SELECT
        signed_fingerprint,
        IF(
          signed_fingerprint < 0,
          CAST(signed_fingerprint AS BIGNUMERIC)
            + CAST('${unsignedInt64Range}' AS BIGNUMERIC),
          CAST(signed_fingerprint AS BIGNUMERIC)
        ) AS fingerprint${commaLines(metricAliases, 8)}
      FROM canonical_rows
    )
    SELECT
      CAST(MOD(fingerprint, ${options.bucketCount}) AS STRING) AS bucket,
      CAST(COUNT(*) AS STRING) AS row_count,
      CAST(SUM(fingerprint) AS STRING) AS fingerprint_sum,
      CAST(
        IF(
          BIT_XOR(signed_fingerprint) < 0,
          CAST(BIT_XOR(signed_fingerprint) AS BIGNUMERIC)
            + CAST('${unsignedInt64Range}' AS BIGNUMERIC),
          CAST(BIT_XOR(signed_fingerprint) AS BIGNUMERIC)
        ) AS STRING
      ) AS fingerprint_xor${commaLines(bigQueryMetricAggregates(prepared), 6)}
    FROM fingerprints
    GROUP BY bucket
    ORDER BY CAST(bucket AS INT64)
  `;
}

export function buildTinybirdDigestSql(options) {
  if (options.key?.length > 0) {
    return buildTinybirdKeyedDigestSql(options);
  }

  const prepared = prepareColumns(options.columns);
  const encodedFields = prepared.map((column) =>
    tinybirdEncodedValue(
      column.type,
      quoteTinybirdIdentifier(column.name),
      column.name,
      options,
    )
  );
  const metricFields = prepared.flatMap((column, index) =>
    tinybirdMetricFields(column, index, options.floatScale)
  );
  const shardFilter = tinybirdShardFilter(prepared, options);

  return `
    SELECT
      toString(fingerprint % ${options.bucketCount}) AS bucket,
      toString(count()) AS row_count,
      toString(sum(toUInt128(fingerprint))) AS fingerprint_sum,
      toString(groupBitXor(fingerprint)) AS fingerprint_xor${commaLines(tinybirdMetricAggregates(prepared), 6)}
    FROM (
      SELECT
        farmFingerprint64(concat(
          ${encodedFields.join(",\n          ")}
        )) AS fingerprint${commaLines(metricFields, 8)}
      FROM ${buildTinybirdSourceClause(options.resource)}${shardFilter}
    )
    GROUP BY bucket
    ORDER BY toUInt16(bucket)
  `;
}

function buildBigQueryKeyedDigestSql(options) {
  const prepared = prepareColumns(options.columns);
  const canonicalRow = bigQueryCanonicalRow(prepared, options);
  const canonicalKey = bigQueryCanonicalKey(prepared, options.key, options);
  const metricFields = prepared.flatMap((column, index) =>
    bigQueryMetricFields(column, index, options.floatScale)
  );
  const metricAliases = metricAliasDefinitions(prepared);
  const sourceClause = buildBigQuerySourceClause(options.source);

  return `
    WITH canonical_rows AS (
      SELECT
        ${canonicalRow} AS canonical_row,
        ${canonicalKey} AS canonical_key${commaLines(metricFields, 8)}
      FROM ${sourceClause}
    ),
    signed_fingerprints AS (
      SELECT
        canonical_key,
        FARM_FINGERPRINT(canonical_row) AS signed_fingerprint,
        FARM_FINGERPRINT(canonical_key) AS signed_key_fingerprint${commaLines(metricAliases, 8)}
      FROM canonical_rows
    ),
    fingerprints AS (
      SELECT
        canonical_key,
        signed_fingerprint,
        IF(
          signed_fingerprint < 0,
          CAST(signed_fingerprint AS BIGNUMERIC)
            + CAST('${unsignedInt64Range}' AS BIGNUMERIC),
          CAST(signed_fingerprint AS BIGNUMERIC)
        ) AS fingerprint,
        IF(
          signed_key_fingerprint < 0,
          CAST(signed_key_fingerprint AS BIGNUMERIC)
            + CAST('${unsignedInt64Range}' AS BIGNUMERIC),
          CAST(signed_key_fingerprint AS BIGNUMERIC)
        ) AS key_fingerprint${commaLines(metricAliases, 8)}
      FROM signed_fingerprints
    ),
    key_rollups AS (
      SELECT
        CAST(MOD(key_fingerprint, ${options.bucketCount}) AS STRING) AS bucket,
        canonical_key,
        COUNT(*) AS key_row_count,
        SUM(fingerprint) AS fingerprint_sum,
        BIT_XOR(signed_fingerprint) AS signed_fingerprint_xor${commaLines(
          bigQueryMetricRollups(prepared),
          8,
        )}
      FROM fingerprints
      GROUP BY bucket, canonical_key
    )
    SELECT
      bucket,
      CAST(SUM(key_row_count) AS STRING) AS row_count,
      CAST(COUNT(*) AS STRING) AS key_count,
      CAST(COUNTIF(key_row_count > 1) AS STRING) AS duplicate_key_count,
      CAST(SUM(GREATEST(key_row_count - 1, 0)) AS STRING) AS duplicate_rows,
      CAST(SUM(fingerprint_sum) AS STRING) AS fingerprint_sum,
      CAST(
        IF(
          BIT_XOR(signed_fingerprint_xor) < 0,
          CAST(BIT_XOR(signed_fingerprint_xor) AS BIGNUMERIC)
            + CAST('${unsignedInt64Range}' AS BIGNUMERIC),
          CAST(BIT_XOR(signed_fingerprint_xor) AS BIGNUMERIC)
        ) AS STRING
      ) AS fingerprint_xor${commaLines(bigQueryMetricAggregates(prepared), 6)}
    FROM key_rollups
    GROUP BY bucket
    ORDER BY CAST(bucket AS INT64)
  `;
}

function buildTinybirdKeyedDigestSql(options) {
  const prepared = prepareColumns(options.columns);
  const canonicalRow = tinybirdCanonicalRow(prepared, options);
  const canonicalKey = tinybirdCanonicalKey(prepared, options.key, options);
  const metricFields = prepared.flatMap((column, index) =>
    tinybirdMetricFields(column, index, options.floatScale)
  );
  const metricAliases = metricAliasDefinitions(prepared);

  return `
    SELECT
      bucket,
      toString(sum(key_row_count)) AS row_count,
      toString(count()) AS key_count,
      toString(countIf(key_row_count > 1)) AS duplicate_key_count,
      toString(sum(greatest(key_row_count - 1, 0))) AS duplicate_rows,
      toString(sum(fingerprint_sum)) AS fingerprint_sum,
      toString(groupBitXor(fingerprint_xor)) AS fingerprint_xor${commaLines(
        tinybirdMetricAggregates(prepared),
        6,
      )}
    FROM (
      SELECT
        toString(key_fingerprint % ${options.bucketCount}) AS bucket,
        canonical_key,
        count() AS key_row_count,
        sum(toUInt128(fingerprint)) AS fingerprint_sum,
        groupBitXor(fingerprint) AS fingerprint_xor${commaLines(
          tinybirdMetricRollups(prepared),
          8,
        )}
      FROM (
        SELECT
          farmFingerprint64(canonical_row) AS fingerprint,
          farmFingerprint64(canonical_key) AS key_fingerprint,
          canonical_key${commaLines(metricAliases, 10)}
        FROM (
          SELECT
            ${canonicalRow} AS canonical_row,
            ${canonicalKey} AS canonical_key${commaLines(metricFields, 12)}
          FROM ${buildTinybirdSourceClause(options.resource)}
        )
      )
      GROUP BY bucket, canonical_key
    )
    GROUP BY bucket
    ORDER BY toUInt16(bucket)
  `;
}

export function buildBigQueryUniquenessSql(options) {
  const keys = options.key.map(quoteBigQueryIdentifier).join(", ");

  return `
    WITH key_counts AS (
      SELECT COUNT(*) AS key_count
      FROM ${buildBigQuerySourceClause(options.source)}
      GROUP BY ${keys}
    )
    SELECT
      CAST(COALESCE(SUM(key_count), 0) AS STRING) AS row_count,
      CAST(COUNTIF(key_count > 1) AS STRING) AS duplicate_key_count,
      CAST(COALESCE(SUM(GREATEST(key_count - 1, 0)), 0) AS STRING) AS duplicate_rows
    FROM key_counts
  `;
}

export function buildTinybirdUniquenessSql(options) {
  const keys = options.key.map(quoteTinybirdIdentifier).join(", ");

  return `
    SELECT
      toString(coalesce(sum(key_count), 0)) AS row_count,
      toString(countIf(key_count > 1)) AS duplicate_key_count,
      toString(coalesce(sum(greatest(key_count - 1, 0)), 0)) AS duplicate_rows
    FROM (
      SELECT count() AS key_count
      FROM ${buildTinybirdSourceClause(options.resource)}
      GROUP BY ${keys}
    )
  `;
}

export function buildBigQueryDiagnosticSql(options) {
  const prepared = prepareColumns(options.columns);
  const canonicalRow = bigQueryCanonicalRow(prepared, options);
  const canonicalKey = bigQueryCanonicalKey(prepared, options.diagnosticKey, options);
  const buckets = validatedBucketList(options.buckets, options.bucketCount);

  return `
    WITH canonical_rows AS (
      SELECT
        ${canonicalRow} AS canonical_row,
        ${canonicalKey} AS canonical_key
      FROM ${buildBigQuerySourceClause(options.source)}
    ),
    fingerprinted_rows AS (
      SELECT
        IF(
          FARM_FINGERPRINT(canonical_row) < 0,
          CAST(FARM_FINGERPRINT(canonical_row) AS BIGNUMERIC)
            + CAST('${unsignedInt64Range}' AS BIGNUMERIC),
          CAST(FARM_FINGERPRINT(canonical_row) AS BIGNUMERIC)
        ) AS fingerprint,
        LOWER(TO_HEX(SHA256(canonical_key))) AS key_hash,
        LOWER(TO_HEX(SHA256(canonical_row))) AS row_hash
      FROM canonical_rows
    )
    SELECT
      CAST(MOD(fingerprint, ${options.bucketCount}) AS STRING) AS bucket,
      key_hash,
      row_hash,
      CAST(COUNT(*) AS STRING) AS occurrence_count
    FROM fingerprinted_rows
    WHERE MOD(fingerprint, ${options.bucketCount}) IN (${buckets.join(", ")})
    GROUP BY bucket, key_hash, row_hash
    ORDER BY bucket, key_hash, row_hash
    LIMIT ${validatedLimit(options.limit)}
  `;
}

export function buildTinybirdDiagnosticSql(options) {
  const prepared = prepareColumns(options.columns);
  const canonicalRow = tinybirdCanonicalRow(prepared, options);
  const canonicalKey = tinybirdCanonicalKey(prepared, options.diagnosticKey, options);
  const buckets = validatedBucketList(options.buckets, options.bucketCount);

  return `
    SELECT
      toString(fingerprint % ${options.bucketCount}) AS bucket,
      lower(hex(SHA256(canonical_key))) AS key_hash,
      lower(hex(SHA256(canonical_row))) AS row_hash,
      toString(count()) AS occurrence_count
    FROM (
      SELECT
        farmFingerprint64(canonical_row) AS fingerprint,
        canonical_key,
        canonical_row
      FROM (
        SELECT
          ${canonicalRow} AS canonical_row,
          ${canonicalKey} AS canonical_key
        FROM ${buildTinybirdSourceClause(options.resource)}
      )
    )
    WHERE fingerprint % ${options.bucketCount} IN (${buckets.join(", ")})
    GROUP BY bucket, key_hash, row_hash
    ORDER BY bucket, key_hash, row_hash
    LIMIT ${validatedLimit(options.limit)}
  `;
}

export function metricDefinitions(columns) {
  return prepareColumns(columns).map((column, index) => ({
    name: column.name,
    null_alias: `n_${index}`,
    numeric_alias: isNumeric(column.type) ? `s_${index}` : null,
  }));
}

export function parseBigQueryTableReference(reference) {
  const parts = reference.split(".");
  if (parts.length !== 3 || parts.some((part) => !/^[A-Za-z0-9_-]+$/.test(part))) {
    throw new Error(`Invalid BigQuery table reference ${reference}`);
  }

  return { project: parts[0], dataset: parts[1], table: parts[2] };
}

function prepareColumns(columns) {
  return columns.map((column) => ({
    ...column,
    type: typeof column.data_type === "string"
      ? parseBigQueryType(column.data_type)
      : column.type,
  }));
}

function bigQueryShardFilter(columns, options) {
  if (!options.shardCount || options.shardCount === 1) return "";

  const column = requiredShardColumn(columns, options);
  if (options.shardStrategy === "hex_range") {
    return hexRangeShardFilter(quoteBigQueryIdentifier(column.name), options);
  }

  const value = bigQueryEncodedValue(
    column.type,
    quoteBigQueryIdentifier(column.name),
    column.name,
    options,
  );
  const signedHash = `FARM_FINGERPRINT(${value})`;
  const unsignedHash = `IF(
          ${signedHash} < 0,
          CAST(${signedHash} AS BIGNUMERIC)
            + CAST('${unsignedInt64Range}' AS BIGNUMERIC),
          CAST(${signedHash} AS BIGNUMERIC)
        )`;

  return `
      WHERE MOD(${unsignedHash}, ${options.shardCount}) = ${options.shardNumber}`;
}

function tinybirdShardFilter(columns, options) {
  if (!options.shardCount || options.shardCount === 1) return "";

  const column = requiredShardColumn(columns, options);
  if (options.shardStrategy === "hex_range") {
    return hexRangeShardFilter(quoteTinybirdIdentifier(column.name), options);
  }

  const value = tinybirdEncodedValue(
    column.type,
    quoteTinybirdIdentifier(column.name),
    column.name,
    options,
  );

  return `
      WHERE farmFingerprint64(${value}) % ${options.shardCount} = ${options.shardNumber}`;
}

function hexRangeShardFilter(identifier, options) {
  const shardWidth = 16 / options.shardCount;
  const lowerBound = (options.shardNumber * shardWidth).toString(16);
  const upperValue = (options.shardNumber + 1) * shardWidth;
  const upperBound = upperValue < 16 ? upperValue.toString(16) : null;

  if (!upperBound) return `
      WHERE ${identifier} >= '${lowerBound}'`;

  return `
      WHERE ${identifier} >= '${lowerBound}' AND ${identifier} < '${upperBound}'`;
}

function requiredShardColumn(columns, options) {
  if (!Number.isInteger(options.shardCount) || options.shardCount < 1) {
    throw new Error("Digest shard count must be a positive integer");
  }

  if (!Number.isInteger(options.shardNumber)
    || options.shardNumber < 0
    || options.shardNumber >= options.shardCount) {
    throw new Error("Digest shard number is outside its shard count");
  }

  const column = columns.find((candidate) => candidate.name === options.shardKey);
  if (!column) throw new Error(`Digest shard key ${options.shardKey} is missing`);
  return column;
}

function buildBigQuerySourceClause(source) {
  if (source.query) {
    return `(${validatedReadOnlyQuery(source.query, "BigQuery")})`;
  }

  const reference = quoteBigQueryReference(source.reference);
  if (!source.timeTravel) {
    return reference;
  }

  return `${reference} FOR SYSTEM_TIME AS OF TIMESTAMP(@snapshot_at)`;
}

function buildTinybirdSourceClause(resource) {
  if (typeof resource === "string") {
    return quoteTinybirdIdentifier(resource);
  }

  return `(${validatedReadOnlyQuery(resource.query, "Tinybird")})`;
}

function validatedReadOnlyQuery(query, dialect) {
  const normalized = typeof query === "string" ? query.trim() : "";

  if (!/^(?:SELECT|WITH)\b/i.test(normalized)) {
    throw new Error(`${dialect} projection must start with SELECT or WITH`);
  }

  if (normalized.includes(";")) {
    throw new Error(`${dialect} projection cannot contain a statement separator`);
  }

  return normalized;
}

function bigQueryCanonicalRow(columns, options) {
  return `CONCAT(\n          ${columns.map((column) =>
    bigQueryEncodedValue(
      column.type,
      quoteBigQueryIdentifier(column.name),
      column.name,
      options,
    )
  ).join(",\n          ")}\n        )`;
}

function tinybirdCanonicalRow(columns, options) {
  return `concat(\n          ${columns.map((column) =>
    tinybirdEncodedValue(
      column.type,
      quoteTinybirdIdentifier(column.name),
      column.name,
      options,
    )
  ).join(",\n          ")}\n        )`;
}

function bigQueryCanonicalKey(columns, keys, options) {
  const columnsByName = new Map(columns.map((column) => [column.name, column]));
  return `CONCAT(${keys.map((key) => {
    const column = requiredColumn(columnsByName, key);
    return bigQueryEncodedValue(
      column.type,
      quoteBigQueryIdentifier(column.name),
      column.name,
      options,
    );
  }).join(", ")})`;
}

function tinybirdCanonicalKey(columns, keys, options) {
  const columnsByName = new Map(columns.map((column) => [column.name, column]));
  return `concat(${keys.map((key) => {
    const column = requiredColumn(columnsByName, key);
    return tinybirdEncodedValue(
      column.type,
      quoteTinybirdIdentifier(column.name),
      column.name,
      options,
    );
  }).join(", ")})`;
}

function bigQueryEncodedValue(type, expression, fieldPath, options) {
  const canonical = bigQueryCanonicalValue(type, expression, fieldPath, options);
  return `IF(
            ${expression} IS NULL,
            'N;',
            CONCAT('V', CAST(BYTE_LENGTH(${canonical}) AS STRING), ':', ${canonical})
          )`;
}

function bigQueryCanonicalValue(type, expression, fieldPath, options) {
  if (type.kind === "array") {
    const itemName = `item_${safeAlias(fieldPath)}`;
    const offsetName = `offset_${safeAlias(fieldPath)}`;
    const encodedItem = bigQueryEncodedValue(
      type.element,
      itemName,
      fieldPath,
      options,
    );
    const orderBy = arrayPolicy(options, fieldPath) === "set"
      ? "encoded_item"
      : offsetName;

    return `ARRAY_TO_STRING(
              ARRAY(
                SELECT encoded_item
                FROM (
                  SELECT
                    ${encodedItem} AS encoded_item,
                    ${offsetName}
                  FROM UNNEST(${expression}) AS ${itemName}
                    WITH OFFSET AS ${offsetName}
                )
                ORDER BY ${orderBy}
              ),
              ''
            )`;
  }

  if (type.kind === "struct") {
    const fields = type.fields.map((field) => {
      const childPath = `${fieldPath}.${field.name}`;
      const childExpression = `${expression}.${quoteBigQueryIdentifier(field.name)}`;
      return bigQueryEncodedValue(field.type, childExpression, childPath, options);
    });
    return `CONCAT(${fields.join(", ")})`;
  }

  return bigQueryScalarValue(type, expression, options.floatScale);
}

function tinybirdEncodedValue(type, expression, fieldPath, options) {
  const canonical = tinybirdCanonicalValue(type, expression, fieldPath, options);
  return `if(
            ${expression} IS NULL,
            'N;',
            concat('V', toString(length(${canonical})), ':', ${canonical})
          )`;
}

function tinybirdCanonicalValue(type, expression, fieldPath, options) {
  if (type.kind === "array") {
    const itemName = `item_${safeAlias(fieldPath)}`;
    const encodedItem = tinybirdEncodedValue(
      type.element,
      itemName,
      fieldPath,
      options,
    );
    const encodedArray = `arrayMap(${itemName} -> ${encodedItem}, assumeNotNull(${expression}))`;
    const orderedArray = arrayPolicy(options, fieldPath) === "set"
      ? `arraySort(${encodedArray})`
      : encodedArray;
    return `arrayStringConcat(${orderedArray}, '')`;
  }

  if (type.kind === "struct") {
    const fields = type.fields.map((field) => {
      const childPath = `${fieldPath}.${field.name}`;
      const childExpression = `tupleElement(assumeNotNull(${expression}), '${escapeSqlString(field.name)}')`;
      return tinybirdEncodedValue(field.type, childExpression, childPath, options);
    });
    return `concat(${fields.join(", ")})`;
  }

  return tinybirdScalarValue(type, expression, options.floatScale);
}

function bigQueryScalarValue(type, expression, floatScale) {
  if (type.name === "STRING") return expression;
  if (type.name === "INT64") return `CAST(${expression} AS STRING)`;
  if (type.name === "FLOAT64") {
    return `FORMAT('%.0f', ROUND(${expression} * POW(10, ${floatScale})))`;
  }
  if (["NUMERIC", "BIGNUMERIC"].includes(type.name)) {
    return `FORMAT('%.0f', ${expression} * BIGNUMERIC '${powerOfTen(floatScale)}')`;
  }
  if (type.name === "BOOL") return `IF(${expression}, '1', '0')`;
  if (type.name === "TIMESTAMP") {
    return `FORMAT_TIMESTAMP('%FT%H:%M:%E6SZ', ${expression}, 'UTC')`;
  }
  if (type.name === "DATE") return `FORMAT_DATE('%F', ${expression})`;

  throw new Error(`Cannot canonicalize BigQuery type ${type.name}`);
}

function tinybirdScalarValue(type, expression, floatScale) {
  const value = `assumeNotNull(${expression})`;
  if (type.name === "STRING") return `toString(${value})`;
  if (type.name === "INT64") return `toString(${value})`;
  if (["FLOAT64", "NUMERIC", "BIGNUMERIC"].includes(type.name)) {
    return `toString(toInt256(round(${value} * ${powerOfTen(floatScale)})))`;
  }
  if (type.name === "BOOL") return `if(${value}, '1', '0')`;
  if (type.name === "TIMESTAMP") {
    return `formatDateTime(toTimeZone(${value}, 'UTC'), '%Y-%m-%dT%H:%i:%S.%fZ')`;
  }
  if (type.name === "DATE") return `toString(${value})`;

  throw new Error(`Cannot canonicalize Tinybird value for ${type.name}`);
}

function bigQueryMetricFields(column, index, floatScale) {
  const expression = quoteBigQueryIdentifier(column.name);
  const fields = [`IF(${expression} IS NULL, 1, 0) AS n_${index}`];
  const scaled = bigQueryScaledNumeric(column.type, expression, floatScale);
  if (scaled) fields.push(`${scaled} AS s_${index}`);
  return fields;
}

function tinybirdMetricFields(column, index, floatScale) {
  const expression = quoteTinybirdIdentifier(column.name);
  const fields = [`toUInt64(${expression} IS NULL) AS n_${index}`];
  const scaled = tinybirdScaledNumeric(column.type, expression, floatScale);
  if (scaled) fields.push(`${scaled} AS s_${index}`);
  return fields;
}

function bigQueryScaledNumeric(type, expression, floatScale) {
  if (type.kind !== "scalar") return null;
  if (type.name === "INT64") {
    return `CAST(COALESCE(${expression}, 0) AS BIGNUMERIC)`;
  }
  if (["FLOAT64", "NUMERIC", "BIGNUMERIC"].includes(type.name)) {
    return `CAST(ROUND(COALESCE(${expression}, 0) * ${powerOfTen(floatScale)}) AS BIGNUMERIC)`;
  }
  return null;
}

function tinybirdScaledNumeric(type, expression, floatScale) {
  if (type.kind !== "scalar") return null;
  if (type.name === "INT64") {
    return `toInt256(coalesce(${expression}, 0))`;
  }
  if (["FLOAT64", "NUMERIC", "BIGNUMERIC"].includes(type.name)) {
    return `toInt256(round(coalesce(${expression}, 0) * ${powerOfTen(floatScale)}))`;
  }
  return null;
}

function metricAliasDefinitions(columns) {
  return columns.flatMap((column, index) => {
    const aliases = [`n_${index}`];
    if (isNumeric(column.type)) aliases.push(`s_${index}`);
    return aliases;
  });
}

function bigQueryMetricAggregates(columns) {
  return columns.flatMap((column, index) => {
    const aggregates = [`CAST(SUM(n_${index}) AS STRING) AS n_${index}`];
    if (isNumeric(column.type)) {
      aggregates.push(`CAST(SUM(s_${index}) AS STRING) AS s_${index}`);
    }
    return aggregates;
  });
}

function bigQueryMetricRollups(columns) {
  return columns.flatMap((column, index) => {
    const aggregates = [`SUM(n_${index}) AS n_${index}`];
    if (isNumeric(column.type)) {
      aggregates.push(`SUM(s_${index}) AS s_${index}`);
    }
    return aggregates;
  });
}

function tinybirdMetricAggregates(columns) {
  return columns.flatMap((column, index) => {
    const aggregates = [`toString(sum(n_${index})) AS n_${index}`];
    if (isNumeric(column.type)) {
      aggregates.push(`toString(sum(s_${index})) AS s_${index}`);
    }
    return aggregates;
  });
}

function tinybirdMetricRollups(columns) {
  return columns.flatMap((column, index) => {
    const aggregates = [`sum(n_${index}) AS n_${index}`];
    if (isNumeric(column.type)) {
      aggregates.push(`sum(s_${index}) AS s_${index}`);
    }
    return aggregates;
  });
}

function isNumeric(type) {
  return type.kind === "scalar"
    && ["INT64", "FLOAT64", "NUMERIC", "BIGNUMERIC"].includes(type.name);
}

function arrayPolicy(options, fieldPath) {
  return options.arrayPolicies?.[fieldPath] ?? "ordered";
}

function quoteBigQueryIdentifier(identifier) {
  return `\`${identifier.replaceAll("`", "``")}\``;
}

function quoteBigQueryReference(reference) {
  return `\`${reference.replaceAll("`", "``")}\``;
}

function quoteTinybirdIdentifier(identifier) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(identifier)) {
    throw new Error(`Invalid Tinybird identifier ${identifier}`);
  }
  return `\`${identifier}\``;
}

function powerOfTen(scale) {
  if (!Number.isInteger(scale) || scale < 0 || scale > 18) {
    throw new Error(`Invalid comparison scale ${scale}`);
  }
  return `1${"0".repeat(scale)}`;
}

function commaLines(values, indentation) {
  if (values.length === 0) return "";
  const spaces = " ".repeat(indentation);
  return `,\n${spaces}${values.join(`,\n${spaces}`)}`;
}

function safeAlias(fieldPath) {
  return fieldPath.replace(/[^A-Za-z0-9_]/g, "_");
}

function escapeSqlString(value) {
  return value.replaceAll("'", "''");
}

function requiredColumn(columnsByName, name) {
  const column = columnsByName.get(name);
  if (!column) throw new Error(`Diagnostic key column ${name} does not exist`);
  return column;
}

function validatedBucketList(buckets, bucketCount) {
  if (!Array.isArray(buckets) || buckets.length === 0) {
    throw new Error("At least one mismatched bucket is required");
  }

  for (const bucket of buckets) {
    if (!Number.isInteger(bucket) || bucket < 0 || bucket >= bucketCount) {
      throw new Error(`Invalid digest bucket ${bucket}`);
    }
  }

  return [...new Set(buckets)].sort((left, right) => left - right);
}

function validatedLimit(limit) {
  if (!Number.isInteger(limit) || limit < 1 || limit > 10000) {
    throw new Error("Diagnostic row limit must be from 1 through 10000");
  }
  return limit;
}
