import {
  PIPELINE_TABLES,
  type ActiveCampaignRawScope,
  type BootstrapScopePage,
  type ConversionSourceTable,
  type DiscoveryPage,
  type FivetranFactReader,
  type JsonRecord,
  type PipelineId,
  type StripeAccount,
  type StripeRawScope,
} from "./contracts.ts";
import { sha256 } from "./json.ts";
import {
  collapseFivetranVersions,
  identifier,
  optionalString,
  sourceVersion,
} from "./raw-collapse.ts";
import { parseTimestamp } from "./timestamp.ts";

const ALL_SOURCE_TABLES = Object.freeze(
  [...new Set(Object.values(PIPELINE_TABLES).flat())].sort(),
);

const SNAPSHOT_TABLES: Readonly<Record<ConversionSourceTable, string>> = {
  raw_activecampaign_contact: "v1_snapshot_activecampaign_contact",
  raw_activecampaign_contact_tag: "v1_snapshot_activecampaign_contact_tag",
  raw_activecampaign_tags: "v1_snapshot_activecampaign_tags",
  raw_stripe_charge: "v1_snapshot_stripe_charge",
  raw_stripe_customer: "v1_snapshot_stripe_customer",
  raw_stripe_payment_intent: "v1_snapshot_stripe_payment_intent",
  raw_stripe_kajabi_charge: "v1_snapshot_stripe_kajabi_charge",
  raw_stripe_kajabi_customer: "v1_snapshot_stripe_kajabi_customer",
  raw_stripe_kajabi_payment_intent: "v1_snapshot_stripe_kajabi_payment_intent",
};

const LIVE_TABLES: Readonly<Record<ConversionSourceTable, string>> = {
  raw_activecampaign_contact: "v1_fivetran_activecampaign_contact",
  raw_activecampaign_contact_tag: "v1_fivetran_activecampaign_contact_tag",
  raw_activecampaign_tags: "v1_fivetran_activecampaign_tags",
  raw_stripe_charge: "v1_fivetran_stripe_charge",
  raw_stripe_customer: "v1_fivetran_stripe_customer",
  raw_stripe_payment_intent: "v1_fivetran_stripe_payment_intent",
  raw_stripe_kajabi_charge: "v1_fivetran_stripe_kajabi_charge",
  raw_stripe_kajabi_customer: "v1_fivetran_stripe_kajabi_customer",
  raw_stripe_kajabi_payment_intent: "v1_fivetran_stripe_kajabi_payment_intent",
};

export interface TinybirdQueryClient {
  query<Row extends JsonRecord>(sql: string): Promise<Row[]>;
}

export interface ActiveCampaignReconciliationStatus {
  cycleCompletedAt: string;
  oldestShardObservedAt: string;
  newestShardObservedAt: string;
}

export class TinybirdFivetranFactReader implements FivetranFactReader {
  private readonly client: TinybirdQueryClient;
  private readonly tenantId: string;
  private readonly snapshotAt: string;

  constructor(
    client: TinybirdQueryClient,
    tenantId: string,
    snapshotAt: string,
  ) {
    parseTimestamp(snapshotAt, "snapshotAt");
    if (!tenantId.trim()) throw new TypeError("tenantId is required");
    this.client = client;
    this.tenantId = tenantId;
    this.snapshotAt = snapshotAt;
  }

  async verifiedObservationThrough(
    requiredTables: readonly ConversionSourceTable[],
  ): Promise<string> {
    const rows = await this.client.query<{
      complete_through_exclusive: string;
      source_count: number;
      manifest_json: string;
    }>(`
      SELECT
        toString(complete_through_exclusive) AS complete_through_exclusive,
        source_count,
        manifest_json
      FROM v1_fivetran_source_receipts
      WHERE tenant_id = ${sqlString(this.tenantId)}
      ORDER BY complete_through_exclusive DESC, verified_at DESC
      LIMIT 1
    `);
    const receipt = rows[0];
    if (!receipt) throw new Error("No verified Fivetran source barrier exists");

    const sources = receiptSources(receipt.manifest_json);
    if (Number(receipt.source_count) !== ALL_SOURCE_TABLES.length) {
      throw new Error("Fivetran source barrier has the wrong source count");
    }

    for (const table of ALL_SOURCE_TABLES) {
      if (!sources.has(table)) {
        throw new Error(`Fivetran source barrier is missing ${table}`);
      }
    }

    for (const table of requiredTables) {
      if (!sources.has(table)) {
        throw new Error(`Fivetran source barrier does not cover ${table}`);
      }
    }

    return parseTimestamp(
      receipt.complete_through_exclusive,
      "complete_through_exclusive",
    ).iso;
  }

  /** Latest proof that every one of the 3×6 AC deletion shards completed. */
  async activeCampaignReconciliationStatus(): Promise<
    ActiveCampaignReconciliationStatus | null
  > {
    const rows = await this.client.query<{
      complete_through_exclusive: string;
      source_count: number;
      shard_count: number;
      manifest_json: string;
    }>(`
      SELECT
        toString(complete_through_exclusive) AS complete_through_exclusive,
        source_count,
        shard_count,
        manifest_json
      FROM v1_fivetran_reconciliation_receipts
      WHERE tenant_id = ${sqlString(this.tenantId)}
      ORDER BY complete_through_exclusive DESC, verified_at DESC
      LIMIT 1
    `);
    const receipt = rows[0];
    if (!receipt) return null;
    if (Number(receipt.source_count) !== 3 || Number(receipt.shard_count) !== 18) {
      throw new Error("ActiveCampaign reconciliation receipt has wrong counts");
    }

    const observed = reconciliationObservations(receipt.manifest_json);
    return {
      cycleCompletedAt: parseTimestamp(
        receipt.complete_through_exclusive,
        "reconciliation complete_through_exclusive",
      ).iso,
      oldestShardObservedAt: observed[0],
      newestShardObservedAt: observed.at(-1)!,
    };
  }

  async readChangedScopePage(input: {
    pipeline: PipelineId;
    table: ConversionSourceTable;
    afterExclusive: string;
    throughInclusive: string;
    afterCursor: string;
    limit: number;
  }): Promise<DiscoveryPage> {
    assertPipelineTable(input.pipeline, input.table);
    const rows = await this.client.query<{ scope_id: string }>(
      buildChangedScopeSql({
        ...input,
        snapshotAt: this.snapshotAt,
        limit: input.limit + 1,
      }),
    );
    return scopePage(rows, input.afterCursor, input.limit);
  }

  async readBootstrapScopePage(input: {
    pipeline: PipelineId;
    snapshotAt: string;
    afterCursor: string;
    limit: number;
  }): Promise<BootstrapScopePage> {
    const snapshotAt = parseTimestamp(input.snapshotAt, "snapshotAt").iso;
    const configured = parseTimestamp(this.snapshotAt, "snapshotAt").iso;
    if (snapshotAt !== configured) throw new Error("Bootstrap snapshot changed");

    const rows = await this.client.query<{ scope_id: string }>(
      buildBootstrapScopeSql({ ...input, limit: input.limit + 1 }),
    );
    return scopePage(rows, input.afterCursor, input.limit);
  }

  async readStripeScopes(input: {
    account: StripeAccount;
    chargeIds: string[];
    throughInclusive: string;
    snapshotOnly: boolean;
  }): Promise<StripeRawScope[]> {
    const ids = sortedIdentifiers(input.chargeIds, "chargeId");
    if (!ids.length) return [];

    const tables = stripeTables(input.account);
    const chargeVersions = await this.readRows({
      table: tables.charge,
      field: "id",
      ids,
      throughInclusive: input.throughInclusive,
      snapshotOnly: input.snapshotOnly,
    });
    const currentCharges = collapseFivetranVersions({
      rows: chargeVersions,
      throughInclusive: input.snapshotOnly ? undefined : input.throughInclusive,
    }).currentById;
    const customerIds = sortedIdentifiers(
      [...currentCharges.values()].map((row) => row.customer_id).filter(hasValue),
      "customerId",
    );
    const paymentIntentIds = sortedIdentifiers(
      [...currentCharges.values()]
        .map((row) => row.payment_intent_id)
        .filter(hasValue),
      "paymentIntentId",
    );
    const [customerVersions, paymentIntentVersions] = await Promise.all([
      this.readRows({
        table: tables.customer,
        field: "id",
        ids: customerIds,
        throughInclusive: input.throughInclusive,
        snapshotOnly: input.snapshotOnly,
      }),
      this.readRows({
        table: tables.paymentIntent,
        field: "id",
        ids: paymentIntentIds,
        throughInclusive: input.throughInclusive,
        snapshotOnly: input.snapshotOnly,
      }),
    ]);
    const chargesById = rowsBy(chargeVersions, "id");
    const customersById = rowsBy(customerVersions, "id");
    const paymentIntentsById = rowsBy(paymentIntentVersions, "id");

    return ids.map((chargeId) => {
      const chargeRows = chargesById.get(chargeId) ?? [];
      const currentCharge = currentCharges.get(chargeId);
      const customerRows = relatedRows(currentCharge?.customer_id, customersById);
      const paymentIntentRows = relatedRows(
        currentCharge?.payment_intent_id,
        paymentIntentsById,
      );

      return {
        account: input.account,
        chargeId,
        chargeVersions: chargeRows,
        customerVersions: customerRows,
        paymentIntentVersions: paymentIntentRows,
        evidenceRecordIds: evidenceIds([
          [tables.charge, chargeRows],
          [tables.customer, customerRows],
          [tables.paymentIntent, paymentIntentRows],
        ]),
      };
    });
  }

  async readActiveCampaignScopes(input: {
    contactIds: string[];
    throughInclusive: string;
    snapshotOnly: boolean;
  }): Promise<ActiveCampaignRawScope[]> {
    const contactIds = sortedIdentifiers(input.contactIds, "contactId");
    if (!contactIds.length) return [];

    const [contactVersions, assignmentVersions] = await Promise.all([
      this.readRows({
        table: "raw_activecampaign_contact",
        field: "id",
        ids: contactIds,
        throughInclusive: input.throughInclusive,
        snapshotOnly: input.snapshotOnly,
      }),
      this.readRows({
        table: "raw_activecampaign_contact_tag",
        field: "contact",
        ids: contactIds,
        throughInclusive: input.throughInclusive,
        snapshotOnly: input.snapshotOnly,
      }),
    ]);
    const collapsedAssignments = collapseFivetranVersions({
      rows: assignmentVersions,
      throughInclusive: input.snapshotOnly ? undefined : input.throughInclusive,
    });
    const tagIds = sortedIdentifiers(
      [...collapsedAssignments.currentById.values()]
        .map((row) => row.tags ?? row.tag)
        .filter(hasValue),
      "tagId",
    );
    const tagVersions = await this.readRows({
      table: "raw_activecampaign_tags",
      field: "id",
      ids: tagIds,
      throughInclusive: input.throughInclusive,
      snapshotOnly: input.snapshotOnly,
    });
    const contactsById = rowsBy(contactVersions, "id");
    const assignmentsByContact = rowsBy(assignmentVersions, "contact");
    const tagsById = rowsBy(tagVersions, "id");

    return contactIds.map((contactId) => {
      const contactRows = contactsById.get(contactId) ?? [];
      const assignmentRows = assignmentsByContact.get(contactId) ?? [];
      const referencedTagIds = new Set(
        assignmentRows
          .map((row) => optionalString(row.tags ?? row.tag))
          .filter((tagId): tagId is string => tagId !== null),
      );
      const referencedTagRows = [...referencedTagIds]
        .flatMap((tagId) => tagsById.get(tagId) ?? []);

      return {
        contactId,
        contactVersions: contactRows,
        assignmentVersions: assignmentRows,
        referencedTagVersions: referencedTagRows,
        evidenceRecordIds: evidenceIds([
          ["raw_activecampaign_contact", contactRows],
          ["raw_activecampaign_contact_tag", assignmentRows],
          ["raw_activecampaign_tags", referencedTagRows],
        ]),
      };
    });
  }

  private async readRows(input: {
    table: ConversionSourceTable;
    field: "id" | "contact";
    ids: string[];
    throughInclusive: string;
    snapshotOnly: boolean;
  }): Promise<JsonRecord[]> {
    if (!input.ids.length) return [];
    const chunks = chunk(input.ids, 1_000);
    const snapshotPages = await Promise.all(chunks.map((ids) =>
      this.client.query(buildSnapshotRowsSql({ ...input, ids })),
    ));
    const snapshotRows = snapshotPages.flat().map((row) => ({
      ...row,
      _v1_observed_at: row._v1_observed_at ?? this.snapshotAt,
      _v1_observation_kind: row._v1_observation_kind ?? "fixed_snapshot",
    }));

    if (input.snapshotOnly) return snapshotRows;

    const livePages = await Promise.all(chunks.map((ids) =>
      this.client.query(buildLiveRowsSql({ ...input, ids })),
    ));
    const liveRows = livePages.flat();

    for (const row of liveRows) {
      parseTimestamp(row._v1_observed_at, "_v1_observed_at");
    }

    return [...snapshotRows, ...liveRows];
  }
}

export function buildBootstrapScopeSql(input: {
  pipeline: PipelineId;
  afterCursor: string;
  limit: number;
}): string {
  assertLimit(input.limit);
  const after = sqlString(input.afterCursor);

  if (input.pipeline === "stripe_main" || input.pipeline === "stripe_kajabi") {
    const account = input.pipeline === "stripe_main" ? "main" : "kajabi";
    const table = SNAPSHOT_TABLES[stripeTables(account).charge];
    const prefix = `stripe:${account}:charge:`;

    return `
      SELECT concat(${sqlString(prefix)}, toString(id)) AS scope_id
      FROM ${table}
      WHERE id IS NOT NULL
        AND paid = 1
        AND status = 'succeeded'
      GROUP BY scope_id
      HAVING scope_id > ${after}
      ORDER BY scope_id
      LIMIT ${input.limit}
    `;
  }

  return `
    SELECT concat('activecampaign:contact:', toString(assignment.contact)) AS scope_id
    FROM ${SNAPSHOT_TABLES.raw_activecampaign_contact_tag} AS assignment
    INNER JOIN ${SNAPSHOT_TABLES.raw_activecampaign_tags} AS tag
      ON toString(tag.id) = toString(assignment.tags)
    INNER JOIN ${SNAPSHOT_TABLES.raw_activecampaign_contact} AS contact
      ON toString(contact.id) = toString(assignment.contact)
    WHERE assignment.contact IS NOT NULL
      AND ifNull(assignment._fivetran_deleted, 0) = 0
      AND ifNull(tag._fivetran_deleted, 0) = 0
      AND ifNull(contact._fivetran_deleted, 0) = 0
      AND ifNull(contact.deleted, 0) = 0
      AND ${activeCampaignRegistrationPredicate("tag.tags")}
    GROUP BY scope_id
    HAVING scope_id > ${after}
    ORDER BY scope_id
    LIMIT ${input.limit}
  `;
}

export function buildChangedScopeSql(input: {
  pipeline: PipelineId;
  table: ConversionSourceTable;
  snapshotAt: string;
  afterExclusive: string;
  throughInclusive: string;
  afterCursor: string;
  limit: number;
}): string {
  assertPipelineTable(input.pipeline, input.table);
  assertLimit(input.limit);
  const afterExclusive = sqlTimestamp(input.afterExclusive);
  const throughInclusive = sqlTimestamp(input.throughInclusive);
  const after = sqlString(input.afterCursor);
  const live = LIVE_TABLES[input.table];
  const snapshotAt = sqlTimestamp(input.snapshotAt);

  if (input.table.endsWith("_charge")) {
    const account = input.pipeline === "stripe_main" ? "main" : "kajabi";
    return directScopeSql({
      live,
      idExpression: "id",
      prefix: `stripe:${account}:charge:`,
      afterExclusive,
      throughInclusive,
      after,
      limit: input.limit,
    });
  }

  if (input.table === "raw_activecampaign_contact") {
    return directScopeSql({
      live,
      idExpression: "id",
      prefix: "activecampaign:contact:",
      afterExclusive,
      throughInclusive,
      after,
      limit: input.limit,
    });
  }

  if (input.table === "raw_activecampaign_contact_tag") {
    return directScopeSql({
      live,
      idExpression: "contact",
      prefix: "activecampaign:contact:",
      afterExclusive,
      throughInclusive,
      after,
      limit: input.limit,
    });
  }

  if (input.table === "raw_activecampaign_tags") {
    return activeCampaignTagChangeSql({
      afterExclusive,
      throughInclusive,
      after,
      limit: input.limit,
      snapshotAt,
    });
  }

  const account = input.pipeline === "stripe_main" ? "main" : "kajabi";
  const relation = input.table.endsWith("_customer")
    ? "customer_id"
    : "payment_intent_id";
  return stripeRelationChangeSql({
    account,
    changedTable: live,
    relation,
    afterExclusive,
    throughInclusive,
    after,
    limit: input.limit,
    snapshotAt,
  });
}

function stripeRelationChangeSql(input: {
  account: StripeAccount;
  changedTable: string;
  relation: "customer_id" | "payment_intent_id";
  afterExclusive: string;
  throughInclusive: string;
  after: string;
  limit: number;
  snapshotAt: string;
}): string {
  const chargeTables = stripeTables(input.account);
  const snapshot = SNAPSHOT_TABLES[chargeTables.charge];
  const live = LIVE_TABLES[chargeTables.charge];
  const prefix = `stripe:${input.account}:charge:`;

  return `
    WITH changed_ids AS (
      SELECT DISTINCT toString(id) AS relation_id
      FROM ${input.changedTable}
      WHERE id IS NOT NULL
        AND _v1_observed_at > ${input.afterExclusive}
        AND _v1_observed_at <= ${input.throughInclusive}
    ),
    current_charges AS (
      SELECT
        toString(id) AS charge_id,
        argMax(
          toString(${input.relation}),
          tuple(_v1_observed_at, _fivetran_synced)
        ) AS relation_id,
        argMax(
          ifNull(_v1_deleted, 0),
          tuple(_v1_observed_at, _fivetran_synced)
        ) AS is_deleted
      FROM (
        SELECT
          id,
          ${input.relation},
          _fivetran_synced,
          ${input.snapshotAt} AS _v1_observed_at,
          toUInt8(0) AS _v1_deleted
        FROM ${snapshot}
        UNION ALL
        SELECT
          id,
          ${input.relation},
          _fivetran_synced,
          _v1_observed_at,
          _v1_deleted
        FROM ${live}
        WHERE _v1_observed_at <= ${input.throughInclusive}
      )
      WHERE id IS NOT NULL
      GROUP BY charge_id
    )
    SELECT concat(${sqlString(prefix)}, charge.charge_id) AS scope_id
    FROM current_charges AS charge
    INNER JOIN changed_ids AS changed USING (relation_id)
    WHERE charge.is_deleted = 0
    GROUP BY scope_id
    HAVING scope_id > ${input.after}
    ORDER BY scope_id
    LIMIT ${input.limit}
  `;
}

function activeCampaignTagChangeSql(input: {
  afterExclusive: string;
  throughInclusive: string;
  after: string;
  limit: number;
  snapshotAt: string;
}): string {
  const snapshot = SNAPSHOT_TABLES.raw_activecampaign_contact_tag;
  const live = LIVE_TABLES.raw_activecampaign_contact_tag;

  return `
    WITH changed_tags AS (
      SELECT DISTINCT toString(id) AS tag_id
      FROM ${LIVE_TABLES.raw_activecampaign_tags}
      WHERE id IS NOT NULL
        AND _v1_observed_at > ${input.afterExclusive}
        AND _v1_observed_at <= ${input.throughInclusive}
    ),
    current_assignments AS (
      SELECT
        toString(id) AS assignment_id,
        argMax(
          toString(contact),
          tuple(_v1_observed_at, _fivetran_synced)
        ) AS contact_id,
        argMax(
          toString(tags),
          tuple(_v1_observed_at, _fivetran_synced)
        ) AS tag_id,
        argMax(
          greatest(ifNull(_fivetran_deleted, 0), ifNull(_v1_deleted, 0)),
          tuple(_v1_observed_at, _fivetran_synced)
        ) AS is_deleted
      FROM (
        SELECT
          id,
          contact,
          tags,
          _fivetran_deleted,
          _fivetran_synced,
          ${input.snapshotAt} AS _v1_observed_at,
          toUInt8(0) AS _v1_deleted
        FROM ${snapshot}
        UNION ALL
        SELECT
          id,
          contact,
          tags,
          _fivetran_deleted,
          _fivetran_synced,
          _v1_observed_at,
          _v1_deleted
        FROM ${live}
        WHERE _v1_observed_at <= ${input.throughInclusive}
      )
      WHERE id IS NOT NULL
      GROUP BY assignment_id
    )
    SELECT concat('activecampaign:contact:', assignment.contact_id) AS scope_id
    FROM current_assignments AS assignment
    INNER JOIN changed_tags AS changed USING (tag_id)
    WHERE assignment.is_deleted = 0
    GROUP BY scope_id
    HAVING scope_id > ${input.after}
    ORDER BY scope_id
    LIMIT ${input.limit}
  `;
}

function directScopeSql(input: {
  live: string;
  idExpression: string;
  prefix: string;
  afterExclusive: string;
  throughInclusive: string;
  after: string;
  limit: number;
}): string {
  return `
    SELECT concat(${sqlString(input.prefix)}, toString(${input.idExpression})) AS scope_id
    FROM ${input.live}
    WHERE ${input.idExpression} IS NOT NULL
      AND _v1_observed_at > ${input.afterExclusive}
      AND _v1_observed_at <= ${input.throughInclusive}
    GROUP BY scope_id
    HAVING scope_id > ${input.after}
    ORDER BY scope_id
    LIMIT ${input.limit}
  `;
}

function buildSnapshotRowsSql(input: {
  table: ConversionSourceTable;
  field: "id" | "contact";
  ids: string[];
}): string {
  const ids = input.ids.map(sqlString).join(", ");
  const predicate = `toString(${input.field}) IN (${ids})`;
  return `SELECT * FROM ${SNAPSHOT_TABLES[input.table]} WHERE ${predicate}`;
}

function buildLiveRowsSql(input: {
  table: ConversionSourceTable;
  field: "id" | "contact";
  ids: string[];
  throughInclusive: string;
}): string {
  const ids = input.ids.map(sqlString).join(", ");
  const predicate = `toString(${input.field}) IN (${ids})`;
  return `
    SELECT *
    FROM ${LIVE_TABLES[input.table]}
    WHERE ${predicate}
      AND _v1_observed_at <= ${sqlTimestamp(input.throughInclusive)}
  `;
}

function activeCampaignRegistrationPredicate(field: string): string {
  return `(
    ${field} = '[CW] Registered for Webinar'
    OR ${field} = '[KRC] Registered for Challenge'
    OR startsWith(${field}, '[KRC] Registered for Challenge -')
    OR startsWith(${field}, '[KRC] Registered -')
  )`;
}

function stripeTables(account: StripeAccount): {
  charge: ConversionSourceTable;
  customer: ConversionSourceTable;
  paymentIntent: ConversionSourceTable;
} {
  if (account === "main") {
    return {
      charge: "raw_stripe_charge",
      customer: "raw_stripe_customer",
      paymentIntent: "raw_stripe_payment_intent",
    };
  }

  return {
    charge: "raw_stripe_kajabi_charge",
    customer: "raw_stripe_kajabi_customer",
    paymentIntent: "raw_stripe_kajabi_payment_intent",
  };
}

function scopePage(
  rows: Array<{ scope_id: string }>,
  priorCursor: string,
  limit: number,
): DiscoveryPage {
  assertLimit(limit);
  const ids = rows.map((row) => identifier(row.scope_id, "scope_id"));
  const unique = [...new Set(ids)];

  if (unique.length !== ids.length) {
    throw new Error("Tinybird returned duplicate replacement scopes");
  }
  if (unique.some((scopeId) => scopeId <= priorCursor)) {
    throw new Error("Tinybird scope page moved backwards");
  }

  const values = unique.slice(0, limit);
  return {
    scopeIds: values,
    nextCursor: values.at(-1) ?? priorCursor,
    eof: unique.length <= limit,
  };
}

function rowsBy(rows: JsonRecord[], field: string): Map<string, JsonRecord[]> {
  const result = new Map<string, JsonRecord[]>();

  for (const row of rows) {
    const id = identifier(row[field], field);
    const values = result.get(id) ?? [];
    values.push(row);
    result.set(id, values);
  }

  return result;
}

function relatedRows(
  value: unknown,
  rows: Map<string, JsonRecord[]>,
): JsonRecord[] {
  const id = optionalString(value);
  return id ? rows.get(id) ?? [] : [];
}

function evidenceIds(
  groups: Array<[ConversionSourceTable, JsonRecord[]]>,
): string[] {
  const values: string[] = [];

  for (const [table, rows] of groups) {
    for (const row of rows) {
      const id = identifier(row.id, `${table}.id`);
      values.push(`${table}:${id}:${sourceVersion(row)}:${sha256(row)}`);
    }
  }

  return [...new Set(values)].sort();
}

function receiptSources(value: string): Set<string> {
  let parsed: unknown;

  try {
    parsed = JSON.parse(value);
  } catch {
    throw new Error("Fivetran source receipt manifest is invalid JSON");
  }

  if (!Array.isArray(parsed)) {
    throw new Error("Fivetran source receipt manifest must be an array");
  }

  const sources = parsed.map((entry) => {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      throw new Error("Fivetran source receipt contains an invalid source");
    }

    return identifier(
      (entry as JsonRecord).source_name,
      "receipt source_name",
    );
  });
  if (sources.length !== ALL_SOURCE_TABLES.length) {
    throw new Error("Fivetran source receipt has the wrong manifest size");
  }
  if (new Set(sources).size !== sources.length) {
    throw new Error("Fivetran source receipt has duplicate sources");
  }
  return new Set(sources);
}

function reconciliationObservations(value: string): string[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    throw new Error("ActiveCampaign reconciliation manifest is invalid JSON");
  }
  if (!Array.isArray(parsed) || parsed.length !== 18) {
    throw new Error("ActiveCampaign reconciliation manifest must have 18 shards");
  }

  const expectedSources = new Set(PIPELINE_TABLES.activecampaign);
  const pairs = new Set<string>();
  const observed: string[] = [];
  for (const entry of parsed) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      throw new Error("ActiveCampaign reconciliation manifest entry is invalid");
    }
    const row = entry as JsonRecord;
    const source = identifier(row.source_name, "reconciliation source_name");
    const shard = Number(row.shard);
    if (!expectedSources.has(source as never) || !Number.isInteger(shard) || shard < 0 || shard > 5) {
      throw new Error("ActiveCampaign reconciliation source or shard is invalid");
    }
    const pair = `${source}:${shard}`;
    if (pairs.has(pair)) throw new Error("ActiveCampaign reconciliation shard is duplicated");
    pairs.add(pair);
    observed.push(parseTimestamp(row.observed_at, "reconciliation observed_at").iso);
  }
  if (pairs.size !== 18) {
    throw new Error("ActiveCampaign reconciliation manifest is incomplete");
  }
  return observed.sort((left, right) => compareTimestampText(left, right));
}

function compareTimestampText(left: string, right: string): number {
  const leftValue = parseTimestamp(left, "left timestamp").microseconds;
  const rightValue = parseTimestamp(right, "right timestamp").microseconds;
  return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
}

function assertPipelineTable(
  pipeline: PipelineId,
  table: ConversionSourceTable,
): void {
  if (!PIPELINE_TABLES[pipeline].includes(table as never)) {
    throw new Error(`${table} does not belong to ${pipeline}`);
  }
}

function sortedIdentifiers(values: unknown[], fieldName: string): string[] {
  return [...new Set(values.map((value) => identifier(value, fieldName)))].sort();
}

function hasValue(value: unknown): value is string | number {
  return value !== null && value !== undefined && value !== "";
}

function sqlTimestamp(value: string): string {
  const timestamp = parseTimestamp(value, "timestamp").iso;
  return `toDateTime64(${sqlString(timestamp)}, 6, 'UTC')`;
}

function sqlString(value: string): string {
  return `'${value.replaceAll("\\", "\\\\").replaceAll("'", "\\'")}'`;
}

function assertLimit(limit: number): void {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100_001) {
    throw new TypeError("limit must be an integer from 1 to 100001");
  }
}

function chunk<Value>(values: Value[], size: number): Value[][] {
  const result: Value[][] = [];

  for (let index = 0; index < values.length; index += size) {
    result.push(values.slice(index, index + size));
  }

  return result;
}
