import type {
  ActiveCampaignRawScope,
  ActiveCampaignReplacementNormalizer,
  JsonRecord,
  PreparedScope,
  ScopeState,
  SourceReplacement,
} from "./contracts.ts";
import { sha256 } from "./json.ts";
import {
  collapseFivetranVersions,
  identifier,
  isDeleted,
  optionalString,
  optionalTimestamp,
  sourceVersion,
} from "./raw-collapse.ts";

export interface ActiveCampaignBuildInput {
  raw: ActiveCampaignRawScope;
  throughInclusive: string;
  windowId: string;
  observationSequence: number;
  previous: ScopeState | null;
  normalizeContactReplacement: ActiveCampaignReplacementNormalizer;
  snapshotOnly?: boolean;
}

export function buildActiveCampaignFivetranScope(
  input: ActiveCampaignBuildInput,
): PreparedScope {
  const scopeId = activeCampaignScopeId(input.raw.contactId);
  const replacementId = `${scopeId}:fivetran:${input.windowId}`;
  const throughInclusive = input.snapshotOnly
    ? undefined
    : input.throughInclusive;
  const contacts = collapseFivetranVersions({
    rows: input.raw.contactVersions,
    throughInclusive,
  });
  const assignments = collapseFivetranVersions({
    rows: input.raw.assignmentVersions,
    throughInclusive,
  });
  const tags = collapseFivetranVersions({
    rows: input.raw.referencedTagVersions,
    throughInclusive,
  });
  const normalizedContact = normalizeContact(
    input.raw.contactId,
    contacts.currentById.get(input.raw.contactId) ?? null,
  );
  const normalizedAssignments = [...assignments.currentById.values()]
    .map(normalizeAssignment)
    .sort((left, right) => String(left.assignmentId).localeCompare(
      String(right.assignmentId),
    ));
  const normalizedTags = [...tags.currentById.values()]
    .map(normalizeTag)
    .sort((left, right) => String(left.tagId).localeCompare(String(right.tagId)));
  const built = input.normalizeContactReplacement({
    contact: normalizedContact,
    assignments: normalizedAssignments,
    tags: normalizedTags,
    previousFacts: input.previous?.rows ?? [],
    replacementVersion: input.throughInclusive,
    replacementId,
  });

  if (built.replacement_scope_id !== scopeId) {
    throw new Error(`Normalizer returned the wrong scope for ${scopeId}`);
  }

  const rows = built.rows.map((row) => ({
    ...row,
    source_observation_sequence: input.observationSequence,
  }));
  const sourceEvidence = {
    transport: {
      kind: "fivetran_raw_landing",
      pipeline: "activecampaign",
      observed_through_inclusive: input.throughInclusive,
      window_id: input.windowId,
    },
    raw_contact_versions: contacts.versions,
    raw_contact_tag_versions: assignments.versions,
    raw_referenced_tag_versions: tags.versions,
  };
  const replacement: SourceReplacement = {
    source: "activecampaign",
    source_account: "default",
    scope_id: scopeId,
    replacement_id: replacementId,
    observed_at: input.throughInclusive,
    observation_sequence: input.observationSequence,
    rows,
    evidence_inbox_ids: sortedUnique(input.raw.evidenceRecordIds),
    source_evidence: sourceEvidence,
  };
  const currentAssignments = normalizedAssignments
    .filter((assignment) => assignment.isDeleted !== true);
  const neededTagIds = new Set(
    currentAssignments.map((assignment) => String(assignment.tagId)),
  );
  const currentTags = normalizedTags
    .filter((tag) => neededTagIds.has(String(tag.tagId)));
  const compactState = {
    contact: normalizedContact,
    assignments: currentAssignments,
    tags: currentTags,
  };

  return {
    windowId: input.windowId,
    scopeId,
    inputHash: sha256({ sourceEvidence, previousRows: input.previous?.rows ?? [] }),
    replacement,
    compactState,
  };
}

export function activeCampaignScopeId(contactId: string): string {
  return `activecampaign:contact:${identifier(contactId, "contactId")}`;
}

function normalizeContact(contactId: string, raw: JsonRecord | null): JsonRecord {
  if (!raw) {
    return {
      contactId: identifier(contactId, "contactId"),
      isDeleted: true,
    };
  }

  return withoutUndefined({
    contactId: identifier(raw.id, "contact.id"),
    email: optionalString(raw.email),
    phone: optionalString(raw.phone),
    firstName: optionalString(raw.first_name ?? raw.firstName),
    lastName: optionalString(raw.last_name ?? raw.lastName),
    fullName: optionalString(raw.full_name ?? raw.fullName),
    sourceVersion: sourceVersion(raw),
    isDeleted: isDeleted(raw._fivetran_deleted) || isDeleted(raw.deleted),
  });
}

function normalizeAssignment(raw: JsonRecord): JsonRecord {
  return withoutUndefined({
    assignmentId: identifier(raw.id, "contact_tag.id"),
    contactId: identifier(raw.contact, "contact_tag.contact"),
    tagId: identifier(raw.tags ?? raw.tag, "contact_tag.tags"),
    assignedAt: optionalTimestamp(
      raw.c_date ?? raw.cdate ?? raw.assignedAt,
      "contact_tag.c_date",
    ) ?? undefined,
    sourceVersion: sourceVersion(raw),
    isDeleted: isDeleted(raw._fivetran_deleted),
  });
}

function normalizeTag(raw: JsonRecord): JsonRecord {
  return withoutUndefined({
    tagId: identifier(raw.id, "tag.id"),
    name: optionalString(raw.tags ?? raw.tag ?? raw.name),
    sourceVersion: sourceVersion(raw),
    isDeleted: isDeleted(raw._fivetran_deleted),
  });
}

function withoutUndefined(value: JsonRecord): JsonRecord {
  return Object.fromEntries(
    Object.entries(value).filter(([, field]) => field !== undefined),
  );
}

function sortedUnique(values: readonly string[]): string[] {
  return [...new Set(values)].sort();
}
