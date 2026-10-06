import type {
  BootstrapIdentityManifest,
  BootstrapIdentitySnapshotSealer,
  BootstrapSourceReplacementPublisher,
  JsonRecord,
  SourceReplacement,
} from "./contracts.ts";
import { canonicalJson, sha256Text } from "./json.ts";
import { compareTimestamps, parseTimestamp } from "./timestamp.ts";

const SOURCE_RECORDS_TABLE = "v1_source_records";
const SOURCE_COMMITS_TABLE = "v1_source_commits";
const IDENTITY_FACTS_TABLE = "v1_bootstrap_conversion_identity_facts";
const IDENTITY_MANIFESTS_TABLE = "v1_bootstrap_conversion_identity_manifests";
const MAX_WAVE_SCOPES = 20_000;
const MAX_QUERY_KEYS = 2_000;
const DEFAULT_APPEND_BYTES = 8 * 1024 * 1024;
const DEFAULT_MAX_PREPARED_BYTES = 512 * 1024 * 1024;
const MANIFEST_SCAN_ROWS = 10_000;
const RECORD_SEPARATOR = "\u001f";
const READBACK_DELAYS_MS = [0, 250, 500, 1_000, 2_000, 4_000, 8_000, 8_000];

export interface PendingIdentityFact {
  eventId: string;
  producerId: string;
  observedAt: string | null;
  ingestedAt: string;
  factKind: "stripe" | "stripe_kajabi" | "activecampaign";
  factKey: string;
  sourcePriority: number;
  sourceFactVersion: number;
  factDeleted: boolean;
  factPayloadHash: string;
  factPayload: string;
  evidenceKeys: string[];
}

export interface BulkSourceRecord extends JsonRecord {
  tenant_id: string;
  source: string;
  source_account: string;
  scope_id: string;
  replacement_id: string;
  observation_sequence: number;
  record_kind: string;
  record_id: string;
  payload_json: string;
  payload_hash: string;
}

export interface BulkTinybirdClient {
  query<Row extends JsonRecord>(sql: string): Promise<Row[]>;
  append(table: string, rows: readonly JsonRecord[]): Promise<void>;
  appendBulk?(
    table: string,
    rows: readonly JsonRecord[],
    maximumBatchBytes: number,
  ): Promise<void>;
}

export interface BootstrapTransforms {
  sourceRecords(
    tenantId: string,
    replacement: SourceReplacement,
  ): Promise<BulkSourceRecord[]>;
  identityFacts(replacement: SourceReplacement): Promise<PendingIdentityFact[]>;
}

interface SourceCommit extends JsonRecord {
  tenant_id: string;
  source: string;
  source_account: string;
  scope_id: string;
  replacement_id: string;
  observation_sequence: number;
  observed_at: string;
  row_count: number;
  content_hash: string;
}

interface IdentityFactRow extends JsonRecord {
  tenant_id: string;
  snapshot_id: string;
  fact_kind: string;
  fact_key: string;
  event_id: string;
  producer_id: string;
  observed_at: string | null;
  ingested_at: string;
  source_priority: number;
  source_fact_version: number;
  fact_deleted: boolean;
  fact_payload_hash: string;
  fact_payload: string;
  evidence_keys: string[];
  canonical_fact_json: string;
  row_hash: string;
}

interface PreparedScope {
  replacement: SourceReplacement;
  records: BulkSourceRecord[];
  commit: SourceCommit;
  recordVerificationHash: string;
  identityRows: IdentityFactRow[];
}

interface RecordVerificationRow extends JsonRecord {
  replacement_id: string;
  scope_id: string;
  observation_sequence: number | string;
  record_count: number | string;
  conflicts: number | string;
  corrupt_payloads: number | string;
  verification_hash: string;
}

interface CommitVerificationRow extends JsonRecord {
  replacement_id: string;
  scope_id: string;
  observation_sequence: number | string;
  row_count: number | string;
  content_hash: string;
  variants: number | string;
}

/** Bulk-only baseline writer. It never calls the live identity queue. */
export class TinybirdBulkBootstrapPublisher
  implements BootstrapSourceReplacementPublisher, BootstrapIdentitySnapshotSealer {
  private readonly tenantId: string;
  private readonly snapshotId: string;
  private readonly snapshotAt: string;
  private readonly client: BulkTinybirdClient;
  private readonly transforms: BootstrapTransforms;
  private readonly maximumAppendBytes: number;
  private readonly maximumPreparedBytes: number;
  private readonly sleep: (milliseconds: number) => Promise<void>;

  constructor(input: {
    tenantId: string;
    snapshotId: string;
    snapshotAt: string;
    client: BulkTinybirdClient;
    transforms: BootstrapTransforms;
    maximumAppendBytes?: number;
    maximumPreparedBytes?: number;
    sleep?: (milliseconds: number) => Promise<void>;
  }) {
    this.tenantId = requiredText(input.tenantId, "tenantId");
    this.snapshotId = requiredText(input.snapshotId, "snapshotId");
    this.snapshotAt = parseTimestamp(input.snapshotAt, "snapshotAt").iso;
    this.client = input.client;
    this.transforms = input.transforms;
    this.maximumAppendBytes = positiveInteger(
      input.maximumAppendBytes ?? DEFAULT_APPEND_BYTES,
      "maximumAppendBytes",
    );
    this.maximumPreparedBytes = positiveInteger(
      input.maximumPreparedBytes ?? DEFAULT_MAX_PREPARED_BYTES,
      "maximumPreparedBytes",
    );
    this.sleep = input.sleep ?? delay;
  }

  async publishBootstrapSourceReplacements(input: {
    bootstrapId: string;
    replacements: SourceReplacement[];
  }): Promise<void> {
    if (input.bootstrapId !== this.snapshotId) {
      throw new Error("Bootstrap publisher received another snapshot");
    }
    validateWave(input.replacements, this.snapshotId, this.snapshotAt);

    const prepared = await Promise.all(input.replacements.map((replacement) =>
      prepareScope({
        tenantId: this.tenantId,
        snapshotId: this.snapshotId,
        replacement,
        transforms: this.transforms,
      })
    ));
    const preparedBytes = byteSize(prepared.flatMap((scope) => [
      ...scope.records,
      ...scope.identityRows,
      scope.commit,
    ]));

    if (preparedBytes > this.maximumPreparedBytes) {
      throw new Error("Bootstrap wave exceeds its memory budget");
    }

    await this.publishRecords(prepared);
    await this.publishIdentityFacts(prepared.flatMap((scope) => scope.identityRows));
    await this.publishCommits(prepared);
  }

  async sealBootstrapIdentitySnapshot(input: {
    snapshotId: string;
    snapshotAt: string;
    expectedDistinctFactCount: number;
    sealedAt: string;
  }): Promise<BootstrapIdentityManifest> {
    assertSnapshot(input, this.snapshotId, this.snapshotAt);
    const expectedCount = nonNegativeInteger(
      input.expectedDistinctFactCount,
      "expectedDistinctFactCount",
    );
    const sealedAt = parseTimestamp(input.sealedAt, "sealedAt").iso;
    const content = await this.scanIdentitySnapshot(expectedCount);
    const row = {
      tenant_id: this.tenantId,
      snapshot_id: this.snapshotId,
      snapshot_at: this.snapshotAt,
      expected_distinct_fact_count: expectedCount,
      canonical_hash: content.hash,
      sealed_at: sealedAt,
    };

    const read = () => this.readManifestContent();
    const existing = await read();
    if (!manifestMatches(existing, row)) {
      await appendRows(
        this.client,
        IDENTITY_MANIFESTS_TABLE,
        [row],
        this.maximumAppendBytes,
      );
      await waitForReadback({
        read,
        complete: (rows) => manifestMatches(rows, row),
        sleep: this.sleep,
        description: "conversion identity manifest",
      });
    }

    return {
      snapshotId: this.snapshotId,
      snapshotAt: this.snapshotAt,
      expectedDistinctFactCount: expectedCount,
      canonicalHash: content.hash,
      sealedAt,
    };
  }

  private async publishRecords(prepared: PreparedScope[]): Promise<void> {
    const expected = new Map(
      prepared.map((scope) => [scope.replacement.replacement_id, scope]),
    );
    const read = () => readRecordVerification(
      this.client,
      this.tenantId,
      [...expected.keys()],
    );
    const existing = await read();
    const missing = incompleteRecordScopes(existing, expected);
    const rows = prepared
      .filter((scope) => missing.has(scope.replacement.replacement_id))
      .flatMap((scope) => scope.records);

    if (rows.length) {
      await appendRows(this.client, SOURCE_RECORDS_TABLE, rows, this.maximumAppendBytes);
    }
    await waitForReadback({
      read,
      complete: (value) => incompleteRecordScopes(value, expected).size === 0,
      sleep: this.sleep,
      description: "source records",
    });
  }

  private async publishIdentityFacts(rows: IdentityFactRow[]): Promise<void> {
    if (!rows.length) return;
    const expected = uniqueIdentityRows(rows);
    const read = () => readIdentityRows(
      this.client,
      this.tenantId,
      this.snapshotId,
      [...expected.values()],
    );
    const existing = await read();
    const missing = missingIdentityRows(existing, expected);

    if (missing.length) {
      await appendRows(
        this.client,
        IDENTITY_FACTS_TABLE,
        missing,
        this.maximumAppendBytes,
      );
    }
    await waitForReadback({
      read,
      complete: (value) => missingIdentityRows(value, expected).length === 0,
      sleep: this.sleep,
      description: "conversion identity facts",
    });
  }

  private async publishCommits(prepared: PreparedScope[]): Promise<void> {
    const expected = new Map(
      prepared.map((scope) => [scope.replacement.replacement_id, scope]),
    );
    const read = () => readCommitVerification(
      this.client,
      this.tenantId,
      [...expected.keys()],
    );
    const existing = await read();
    const missing = incompleteCommitScopes(existing, expected);
    const rows = prepared
      .filter((scope) => missing.has(scope.replacement.replacement_id))
      .map((scope) => scope.commit);

    if (rows.length) {
      await appendRows(this.client, SOURCE_COMMITS_TABLE, rows, this.maximumAppendBytes);
    }
    await waitForReadback({
      read,
      complete: (value) => incompleteCommitScopes(value, expected).size === 0,
      sleep: this.sleep,
      description: "source commits",
    });
  }

  private async scanIdentitySnapshot(
    expectedCount: number,
  ): Promise<{ count: number; hash: string }> {
    const digest = createHash("sha256");
    let afterKind = "";
    let afterKey = "";
    let count = 0;

    while (true) {
      const after = count
        ? `AND tuple(fact_kind, fact_key) > tuple(${sqlString(afterKind)}, ${sqlString(afterKey)})`
        : "";
      const rows = await this.client.query<IdentityFactRow>(`
        SELECT DISTINCT ${identityColumns().join(", ")}
        FROM ${IDENTITY_FACTS_TABLE}
        WHERE tenant_id = ${sqlString(this.tenantId)}
          AND snapshot_id = ${sqlString(this.snapshotId)}
          ${after}
        ORDER BY fact_kind, fact_key
        LIMIT ${MANIFEST_SCAN_ROWS + 1}
      `);
      rejectDuplicateLogicalKeys(rows);
      const page = rows.slice(0, MANIFEST_SCAN_ROWS);

      for (const row of page) {
        validateIdentityRow(row, this.tenantId, this.snapshotId);
        if (count) digest.update("\n");
        digest.update(row.canonical_fact_json);
        count += 1;
        if (count > expectedCount) {
          throw new Error("Conversion identity snapshot has extra facts");
        }
      }
      if (rows.length <= MANIFEST_SCAN_ROWS) break;
      const last = page.at(-1)!;
      afterKind = last.fact_kind;
      afterKey = last.fact_key;
    }

    if (count !== expectedCount) {
      throw new Error("Conversion identity snapshot fact count is incomplete");
    }
    return { count, hash: digest.digest("hex") };
  }

  private readManifestContent(): Promise<JsonRecord[]> {
    return this.client.query(`
      SELECT DISTINCT
        toString(snapshot_at) AS snapshot_at,
        expected_distinct_fact_count,
        canonical_hash
      FROM ${IDENTITY_MANIFESTS_TABLE}
      WHERE tenant_id = ${sqlString(this.tenantId)}
        AND snapshot_id = ${sqlString(this.snapshotId)}
      LIMIT 2
    `);
  }
}

async function prepareScope(input: {
  tenantId: string;
  snapshotId: string;
  replacement: SourceReplacement;
  transforms: BootstrapTransforms;
}): Promise<PreparedScope> {
  const records = await input.transforms.sourceRecords(
    input.tenantId,
    input.replacement,
  );
  validateSourceRecords(records, input.tenantId, input.replacement);
  const recordVerificationHash = verificationHash(records);
  const contentHash = sha256Text(
    records.map((record) => canonicalJson(record)).sort().join("\n"),
  );
  const commit: SourceCommit = {
    tenant_id: input.tenantId,
    source: input.replacement.source,
    source_account: input.replacement.source_account,
    scope_id: input.replacement.scope_id,
    replacement_id: input.replacement.replacement_id,
    observation_sequence: input.replacement.observation_sequence,
    observed_at: input.replacement.observed_at,
    row_count: records.length,
    content_hash: contentHash,
  };
  const facts = await input.transforms.identityFacts(input.replacement);
  const identityRows = facts.map((fact) => identityRow(
    input.tenantId,
    input.snapshotId,
    fact,
  ));

  if (identityRows.length !== input.replacement.rows.length) {
    throw new Error("Every conversion row must produce one identity fact");
  }

  return {
    replacement: input.replacement,
    records,
    commit,
    recordVerificationHash,
    identityRows,
  };
}

function identityRow(
  tenantId: string,
  snapshotId: string,
  fact: PendingIdentityFact,
): IdentityFactRow {
  validateIdentityFact(fact);
  const canonicalFact = canonicalJson(fact);
  return {
    tenant_id: tenantId,
    snapshot_id: snapshotId,
    fact_kind: fact.factKind,
    fact_key: fact.factKey,
    event_id: fact.eventId,
    producer_id: fact.producerId,
    observed_at: fact.observedAt === null
      ? null
      : parseTimestamp(fact.observedAt, "identity observedAt").iso,
    ingested_at: parseTimestamp(fact.ingestedAt, "identity ingestedAt").iso,
    source_priority: fact.sourcePriority,
    source_fact_version: fact.sourceFactVersion,
    fact_deleted: fact.factDeleted,
    fact_payload_hash: fact.factPayloadHash,
    fact_payload: fact.factPayload,
    evidence_keys: fact.evidenceKeys,
    canonical_fact_json: canonicalFact,
    row_hash: sha256Text(canonicalFact),
  };
}

function validateIdentityFact(fact: PendingIdentityFact): void {
  const kinds = new Set(["stripe", "stripe_kajabi", "activecampaign"]);
  if (!kinds.has(fact.factKind)) throw new Error("Unexpected identity fact kind");
  if (!fact.factKey.startsWith(`${fact.factKind}:`)) {
    throw new Error("Identity fact key has the wrong namespace");
  }
  if (fact.producerId !== `source:${fact.factKind}` || !fact.eventId) {
    throw new Error("Identity fact producer is invalid");
  }
  if (fact.sourcePriority !== 1) throw new Error("Identity source priority must be 1");
  nonNegativeInteger(fact.sourceFactVersion, "sourceFactVersion");
  if (typeof fact.factDeleted !== "boolean") {
    throw new Error("Identity deletion marker must be boolean");
  }
  if (fact.observedAt !== null) parseTimestamp(fact.observedAt, "identity observedAt");
  parseTimestamp(fact.ingestedAt, "identity ingestedAt");
  if (sha256Text(fact.factPayload) !== fact.factPayloadHash) {
    throw new Error("Identity payload hash is invalid");
  }
  const sortedEvidence = [...new Set(fact.evidenceKeys)].sort(compareText);
  if (canonicalJson(sortedEvidence) !== canonicalJson(fact.evidenceKeys)) {
    throw new Error("Identity evidence keys must be sorted and unique");
  }
}

function validateIdentityRow(
  row: IdentityFactRow,
  tenantId: string,
  snapshotId: string,
): void {
  const normalized = normalizeIdentityRow(row);
  if (normalized.tenant_id !== tenantId || normalized.snapshot_id !== snapshotId) {
    throw new Error("Conversion identity row belongs to another snapshot");
  }
  if (sha256Text(normalized.canonical_fact_json) !== normalized.row_hash) {
    throw new Error("Conversion identity row hash is invalid");
  }
  const fact = JSON.parse(normalized.canonical_fact_json) as PendingIdentityFact;
  validateIdentityFact(fact);
  if (
    canonicalJson(identityRow(tenantId, snapshotId, fact)) !==
      canonicalJson(normalized)
  ) {
    throw new Error("Conversion identity columns disagree with the canonical fact");
  }
}

function uniqueIdentityRows(rows: IdentityFactRow[]): Map<string, IdentityFactRow> {
  const result = new Map<string, IdentityFactRow>();
  for (const row of rows) {
    const key = identityKey(row);
    if (result.has(key)) throw new Error(`Duplicate bootstrap identity fact ${key}`);
    result.set(key, row);
  }
  return result;
}

function missingIdentityRows(
  received: IdentityFactRow[],
  expected: Map<string, IdentityFactRow>,
): IdentityFactRow[] {
  const found = new Set<string>();
  for (const raw of received) {
    const row = normalizeIdentityRow(raw);
    const key = identityKey(row);
    const wanted = expected.get(key);
    if (!wanted || canonicalJson(row) !== canonicalJson(wanted)) {
      throw new Error(`Conflicting bootstrap identity fact ${key}`);
    }
    found.add(key);
  }
  return [...expected].filter(([key]) => !found.has(key)).map(([, row]) => row);
}

function normalizeIdentityRow(row: IdentityFactRow): IdentityFactRow {
  return {
    ...row,
    observed_at: row.observed_at === null
      ? null
      : parseTimestamp(row.observed_at, "identity observed_at").iso,
    ingested_at: parseTimestamp(row.ingested_at, "identity ingested_at").iso,
    source_priority: integer(row.source_priority, "identity source_priority"),
    source_fact_version: integer(
      row.source_fact_version,
      "identity source_fact_version",
    ),
    fact_deleted: row.fact_deleted === true || (row.fact_deleted as unknown) === 1,
  };
}

async function readIdentityRows(
  client: BulkTinybirdClient,
  tenantId: string,
  snapshotId: string,
  expected: IdentityFactRow[],
): Promise<IdentityFactRow[]> {
  const pages = await Promise.all(chunk(expected, MAX_QUERY_KEYS).map((rows) => {
    const keys = rows.map((row) =>
      `tuple(${sqlString(row.fact_kind)}, ${sqlString(row.fact_key)})`
    ).join(", ");
    return client.query<IdentityFactRow>(`
      SELECT DISTINCT ${identityColumns().join(", ")}
      FROM ${IDENTITY_FACTS_TABLE}
      WHERE tenant_id = ${sqlString(tenantId)}
        AND snapshot_id = ${sqlString(snapshotId)}
        AND tuple(fact_kind, fact_key) IN (${keys})
    `);
  }));
  return pages.flat();
}

function identityColumns(): string[] {
  return [
    "tenant_id",
    "snapshot_id",
    "fact_kind",
    "fact_key",
    "event_id",
    "producer_id",
    "toString(observed_at) AS observed_at",
    "toString(ingested_at) AS ingested_at",
    "source_priority",
    "source_fact_version",
    "fact_deleted",
    "fact_payload_hash",
    "fact_payload",
    "evidence_keys",
    "canonical_fact_json",
    "row_hash",
  ];
}

function rejectDuplicateLogicalKeys(rows: IdentityFactRow[]): void {
  for (let index = 1; index < rows.length; index += 1) {
    if (identityKey(rows[index - 1]) === identityKey(rows[index])) {
      throw new Error(`Conflicting bootstrap identity fact ${identityKey(rows[index])}`);
    }
  }
}

function identityKey(row: Pick<IdentityFactRow, "fact_kind" | "fact_key">): string {
  return `${row.fact_kind}${RECORD_SEPARATOR}${row.fact_key}`;
}

function verificationHash(records: BulkSourceRecord[]): string {
  const hashes = records.map((record) => {
    assertDigestField(record.record_kind, "record_kind");
    assertDigestField(record.record_id, "record_id");
    return sha256Text(
      `${record.record_kind}${RECORD_SEPARATOR}${record.record_id}` +
        `${RECORD_SEPARATOR}${record.payload_hash}`,
    );
  });
  return sha256Text(hashes.sort().join(""));
}

function validateSourceRecords(
  records: BulkSourceRecord[],
  tenantId: string,
  replacement: SourceReplacement,
): void {
  if (!records.length) throw new Error("Source replacement produced no evidence");
  const keys = new Set<string>();
  for (const record of records) {
    if (
      record.tenant_id !== tenantId ||
      record.source !== replacement.source ||
      record.source_account !== replacement.source_account ||
      record.scope_id !== replacement.scope_id ||
      record.replacement_id !== replacement.replacement_id ||
      integer(record.observation_sequence, "record observation sequence") !==
        replacement.observation_sequence
    ) {
      throw new Error("Source record does not match its replacement");
    }
    if (sha256Text(record.payload_json) !== record.payload_hash) {
      throw new Error("Source record payload hash is invalid");
    }
    const key = `${record.record_kind}${RECORD_SEPARATOR}${record.record_id}`;
    if (keys.has(key)) throw new Error("Source replacement has duplicate record keys");
    keys.add(key);
  }
}

async function readRecordVerification(
  client: BulkTinybirdClient,
  tenantId: string,
  replacementIds: string[],
): Promise<RecordVerificationRow[]> {
  const pages = await Promise.all(chunk(replacementIds, MAX_QUERY_KEYS).map((ids) =>
    client.query<RecordVerificationRow>(`
      SELECT
        replacement_id,
        any(verified_scope_id) AS scope_id,
        any(verified_observation_sequence) AS observation_sequence,
        count() AS record_count,
        sum(variants != 1) AS conflicts,
        sum(corrupt_payloads) AS corrupt_payloads,
        lower(hex(SHA256(arrayStringConcat(arraySort(groupArray(record_digest)), '')))) AS verification_hash
      FROM (
        SELECT
          replacement_id,
          record_kind,
          record_id,
          any(scope_id) AS verified_scope_id,
          any(observation_sequence) AS verified_observation_sequence,
          lower(hex(SHA256(concat(record_kind, char(31), record_id, char(31), any(payload_hash))))) AS record_digest,
          uniqExact(tuple(scope_id, observation_sequence, payload_hash)) AS variants,
          countIf(lower(hex(SHA256(payload_json))) != payload_hash) AS corrupt_payloads
        FROM ${SOURCE_RECORDS_TABLE}
        WHERE tenant_id = ${sqlString(tenantId)}
          AND replacement_id IN (${ids.map(sqlString).join(", ")})
        GROUP BY replacement_id, record_kind, record_id
      )
      GROUP BY replacement_id
    `)
  ));
  return pages.flat();
}

function incompleteRecordScopes(
  rows: RecordVerificationRow[],
  expected: Map<string, PreparedScope>,
): Set<string> {
  const received = uniqueByReplacement(rows, "record verification");
  const missing = new Set<string>();
  for (const [replacementId, scope] of expected) {
    const row = received.get(replacementId);
    if (!row) {
      missing.add(replacementId);
      continue;
    }
    if (
      integer(row.conflicts, "record conflicts") !== 0 ||
      integer(row.corrupt_payloads, "corrupt payloads") !== 0 ||
      row.scope_id !== scope.replacement.scope_id ||
      integer(row.observation_sequence, "record observation sequence") !==
        scope.replacement.observation_sequence
    ) {
      throw new Error(`Conflicting source records for ${replacementId}`);
    }
    const count = integer(row.record_count, "record count");
    if (count > scope.records.length) {
      throw new Error(`Unexpected source records for ${replacementId}`);
    }
    if (count < scope.records.length) {
      missing.add(replacementId);
      continue;
    }
    if (row.verification_hash !== scope.recordVerificationHash) {
      throw new Error(`Source record hash conflict for ${replacementId}`);
    }
  }
  rejectUnknown(received, expected, "source record");
  return missing;
}

async function readCommitVerification(
  client: BulkTinybirdClient,
  tenantId: string,
  replacementIds: string[],
): Promise<CommitVerificationRow[]> {
  const pages = await Promise.all(chunk(replacementIds, MAX_QUERY_KEYS).map((ids) =>
    client.query<CommitVerificationRow>(`
      SELECT
        replacement_id,
        any(commits.scope_id) AS scope_id,
        any(commits.observation_sequence) AS observation_sequence,
        any(commits.row_count) AS row_count,
        any(commits.content_hash) AS content_hash,
        uniqExact(tuple(commits.scope_id, commits.observation_sequence, commits.row_count, commits.content_hash)) AS variants
      FROM ${SOURCE_COMMITS_TABLE} AS commits
      WHERE tenant_id = ${sqlString(tenantId)}
        AND replacement_id IN (${ids.map(sqlString).join(", ")})
      GROUP BY replacement_id
    `)
  ));
  return pages.flat();
}

function incompleteCommitScopes(
  rows: CommitVerificationRow[],
  expected: Map<string, PreparedScope>,
): Set<string> {
  const received = uniqueByReplacement(rows, "commit verification");
  const missing = new Set<string>();
  for (const [replacementId, scope] of expected) {
    const row = received.get(replacementId);
    if (!row) {
      missing.add(replacementId);
      continue;
    }
    if (
      integer(row.variants, "commit variants") !== 1 ||
      row.scope_id !== scope.replacement.scope_id ||
      integer(row.observation_sequence, "commit observation sequence") !==
        scope.replacement.observation_sequence ||
      integer(row.row_count, "commit row count") !== scope.records.length ||
      row.content_hash !== scope.commit.content_hash
    ) {
      throw new Error(`Source commit conflict for ${replacementId}`);
    }
  }
  rejectUnknown(received, expected, "source commit");
  return missing;
}

function validateWave(
  replacements: SourceReplacement[],
  snapshotId: string,
  snapshotAt: string,
): void {
  if (!replacements.length || replacements.length > MAX_WAVE_SCOPES) {
    throw new Error(`Bootstrap wave must contain 1 to ${MAX_WAVE_SCOPES} scopes`);
  }
  const replacementIds = new Set<string>();
  const scopeIds = new Set<string>();
  const source = replacements[0].source;
  const account = replacements[0].source_account;
  for (const replacement of replacements) {
    if (replacement.source !== source || replacement.source_account !== account) {
      throw new Error("Bootstrap wave mixes source namespaces");
    }
    if (!replacement.replacement_id.endsWith(`:bootstrap:${snapshotId}`)) {
      throw new Error("Bootstrap replacement ID does not match its snapshot");
    }
    if (compareTimestamps(replacement.observed_at, snapshotAt) !== 0) {
      throw new Error("Bootstrap replacement has the wrong observation time");
    }
    if (replacement.observation_sequence !== 1) {
      throw new Error("Bootstrap replacement sequence must be 1");
    }
    if (replacementIds.has(replacement.replacement_id) || scopeIds.has(replacement.scope_id)) {
      throw new Error("Bootstrap wave has duplicate scopes");
    }
    replacementIds.add(replacement.replacement_id);
    scopeIds.add(replacement.scope_id);
  }
}

function manifestMatches(received: JsonRecord[], expected: JsonRecord): boolean {
  const wanted = canonicalJson({
    snapshot_at: expected.snapshot_at,
    expected_distinct_fact_count: expected.expected_distinct_fact_count,
    canonical_hash: expected.canonical_hash,
  });
  const variants = new Set(received.map((row) => canonicalJson({
    snapshot_at: parseTimestamp(row.snapshot_at, "manifest snapshot_at").iso,
    expected_distinct_fact_count: integer(
      row.expected_distinct_fact_count,
      "manifest expected count",
    ),
    canonical_hash: row.canonical_hash,
  })));
  if (!variants.size) return false;
  if (variants.size !== 1 || !variants.has(wanted)) {
    throw new Error("Conflicting conversion identity manifest");
  }
  return true;
}

function assertSnapshot(
  input: { snapshotId: string; snapshotAt: string },
  snapshotId: string,
  snapshotAt: string,
): void {
  if (
    input.snapshotId !== snapshotId ||
    compareTimestamps(input.snapshotAt, snapshotAt) !== 0
  ) {
    throw new Error("Identity manifest belongs to another snapshot");
  }
}

async function appendRows(
  client: BulkTinybirdClient,
  table: string,
  rows: readonly JsonRecord[],
  maximumBatchBytes: number,
): Promise<void> {
  if (!rows.length) return;
  if (client.appendBulk) {
    await client.appendBulk(table, rows, maximumBatchBytes);
    return;
  }
  await client.append(table, rows);
}

async function waitForReadback<Value>(input: {
  read: () => Promise<Value>;
  complete: (value: Value) => boolean;
  sleep: (milliseconds: number) => Promise<void>;
  description: string;
}): Promise<Value> {
  for (const milliseconds of READBACK_DELAYS_MS) {
    if (milliseconds) await input.sleep(milliseconds);
    const value = await input.read();
    if (input.complete(value)) return value;
  }
  throw new Error(`Bootstrap ${input.description} are not visible`);
}

function uniqueByReplacement<Row extends { replacement_id: string }>(
  rows: Row[],
  description: string,
): Map<string, Row> {
  const result = new Map<string, Row>();
  for (const row of rows) {
    if (result.has(row.replacement_id)) {
      throw new Error(`Duplicate ${description} for ${row.replacement_id}`);
    }
    result.set(row.replacement_id, row);
  }
  return result;
}

function rejectUnknown<Expected, Row>(
  received: Map<string, Row>,
  expected: Map<string, Expected>,
  description: string,
): void {
  for (const id of received.keys()) {
    if (!expected.has(id)) throw new Error(`Unexpected ${description} ${id}`);
  }
}

function integer(value: unknown, fieldName: string): number {
  const number = typeof value === "string" ? Number(value) : value;
  if (!Number.isSafeInteger(number) || Number(number) < 0) {
    throw new TypeError(`${fieldName} must be a non-negative safe integer`);
  }
  return number as number;
}

function nonNegativeInteger(value: unknown, fieldName: string): number {
  return integer(value, fieldName);
}

function positiveInteger(value: unknown, fieldName: string): number {
  const number = integer(value, fieldName);
  if (number < 1) throw new TypeError(`${fieldName} must be positive`);
  return number;
}

function requiredText(value: unknown, fieldName: string): string {
  if (typeof value !== "string" || !value.trim()) {
    throw new TypeError(`${fieldName} is required`);
  }
  return value.trim();
}

function assertDigestField(value: string, fieldName: string): void {
  if (!value || value.includes(RECORD_SEPARATOR)) {
    throw new Error(`${fieldName} is not safe to hash`);
  }
}

function byteSize(rows: JsonRecord[]): number {
  return new TextEncoder().encode(rows.map((row) => JSON.stringify(row)).join("\n"))
    .byteLength;
}

function sqlString(value: string): string {
  return `'${value.replaceAll("\\", "\\\\").replaceAll("'", "\\'")}'`;
}

function chunk<Value>(values: readonly Value[], size: number): Value[][] {
  const chunks: Value[][] = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
}

function compareText(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
import { createHash } from "node:crypto";
