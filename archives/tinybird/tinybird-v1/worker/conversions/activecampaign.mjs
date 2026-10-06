import { createHash } from "node:crypto";

import {
  cleanString,
  cloneEvidence,
  lowerString,
  requireObject,
  requireString,
  toIsoTimestamp,
} from "./shared.mjs";

// Copied exactly from dataform/includes/business_rules.js. Array order is policy order.
export const ACTIVE_CAMPAIGN_REGISTRATION_FORMS = Object.freeze([
  Object.freeze({
    registrationType: "krc",
    contentName: "Keyboard Rich Challenge Registration",
    activeCampaignFormIds: Object.freeze(["20"]),
    primaryTagPrefixes: Object.freeze([
      "[KRC] Registered for Challenge -",
      "[KRC] Registered -",
    ]),
    primaryExactTags: Object.freeze([]),
    fallbackExactTag: "[KRC] Registered for Challenge",
  }),
  Object.freeze({
    registrationType: "webinar",
    contentName: "Booming Bookkeeping Webinar Registration",
    activeCampaignFormIds: Object.freeze(["15"]),
    primaryTagPrefixes: Object.freeze([]),
    primaryExactTags: Object.freeze(["[CW] Registered for Webinar"]),
    fallbackExactTag: null,
  }),
]);

const PACIFIC_DATE_FORMAT = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/Los_Angeles",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});

export function matchActiveCampaignRegistrationTag(tagName) {
  const normalizedTagName = cleanString(tagName);

  if (!normalizedTagName) {
    return null;
  }

  for (const form of ACTIVE_CAMPAIGN_REGISTRATION_FORMS) {
    if (form.primaryExactTags.includes(normalizedTagName)) {
      return tagMatch(form, "primary");
    }

    if (form.primaryTagPrefixes.some((prefix) => normalizedTagName.startsWith(prefix))) {
      return tagMatch(form, "primary");
    }

    if (form.fallbackExactTag === normalizedTagName) {
      return tagMatch(form, "fallback");
    }
  }

  return null;
}

export function buildActiveCampaignContactReplacement(input) {
  requireObject(input, "input");

  const contact = normalizeContact(input.contact);
  const replacementVersionAt = toIsoTimestamp(
    input.replacementVersion,
    "replacementVersion",
  );
  const replacementId = cleanString(input.replacementId) ?? replacementVersionAt;
  const replacementScopeId = `activecampaign:contact:${contact.contactId}`;
  const replacementVersionId = `${replacementScopeId}:replacement:${replacementId}`;
  const assignments = collapseAssignmentVersions(input.assignments ?? []);
  const tags = collapseTagVersions(input.tags ?? []);

  assertContactScope(contact.contactId, assignments, input.previousFacts ?? []);

  const matchedAssignments = getMatchedAssignments({ contact, assignments, tags });
  const liveFacts = buildLiveFacts({
    contact,
    matchedAssignments,
    replacementScopeId,
    replacementVersionAt,
    replacementVersionId,
  });
  const liveFactIds = new Set(liveFacts.map((fact) => fact.source_fact_id));
  const previousFacts = collapsePreviousFacts(input.previousFacts ?? []);
  const tombstones = [];

  for (const previousFact of previousFacts.values()) {
    if (previousFact.is_deleted === true || liveFactIds.has(previousFact.source_fact_id)) {
      continue;
    }

    tombstones.push(
      buildTombstone({
        previousFact,
        contact,
        assignments,
        tags,
        liveFacts,
        replacementScopeId,
        replacementVersionAt,
        replacementVersionId,
      }),
    );
  }

  const rows = [...liveFacts, ...tombstones].sort((left, right) =>
    left.source_fact_id.localeCompare(right.source_fact_id),
  );

  return {
    replacement_scope_id: replacementScopeId,
    replacement_version_at: replacementVersionAt,
    replacement_version_id: replacementVersionId,
    rows,
  };
}

export function createActiveCampaignCanonicalEventId({
  email,
  registrationType,
  submittedAt,
}) {
  const normalizedEmail = lowerString(email);

  if (!normalizedEmail) {
    return null;
  }

  const normalizedRegistrationType = requireString(
    registrationType,
    "registrationType",
  );
  const pacificDate = getPacificDate(submittedAt);
  const digest = createHash("sha256")
    .update(`${normalizedEmail}|${normalizedRegistrationType}|${pacificDate}`)
    .digest("hex");

  return `form_submission_${digest}`;
}

function tagMatch(form, matchType) {
  return {
    registrationType: form.registrationType,
    contentName: form.contentName,
    matchType,
  };
}

function normalizeContact(value) {
  const contact = requireObject(value, "contact");
  const firstName = cleanString(contact.firstName);
  const lastName = cleanString(contact.lastName);
  const computedFullName = cleanString([firstName, lastName].filter(Boolean).join(" "));

  return {
    ...cloneEvidence(contact),
    contactId: requireString(contact.contactId, "contact.contactId"),
    email: lowerString(contact.email),
    phone: cleanString(contact.phone),
    firstName,
    lastName,
    fullName: cleanString(contact.fullName) ?? computedFullName,
    sourceVersion: contact.sourceVersion
      ? toIsoTimestamp(contact.sourceVersion, "contact.sourceVersion")
      : undefined,
    isDeleted: contact.isDeleted === true,
  };
}

function collapseAssignmentVersions(values) {
  if (!Array.isArray(values)) {
    throw new TypeError("assignments must be an array");
  }

  return collapseVersions(values, "assignmentId", normalizeAssignment);
}

function normalizeAssignment(value) {
  const assignment = requireObject(value, "assignment");

  return {
    ...cloneEvidence(assignment),
    assignmentId: requireString(
      assignment.assignmentId,
      "assignment.assignmentId",
    ),
    contactId: requireString(assignment.contactId, "assignment.contactId"),
    tagId: requireString(assignment.tagId, "assignment.tagId"),
    assignedAt: assignment.assignedAt
      ? toIsoTimestamp(assignment.assignedAt, "assignment.assignedAt")
      : undefined,
    sourceVersion: assignment.sourceVersion
      ? toIsoTimestamp(assignment.sourceVersion, "assignment.sourceVersion")
      : undefined,
    isDeleted: assignment.isDeleted === true,
  };
}

function collapseTagVersions(values) {
  if (!Array.isArray(values)) {
    throw new TypeError("tags must be an array");
  }

  return collapseVersions(values, "tagId", normalizeTag);
}

function normalizeTag(value) {
  const tag = requireObject(value, "tag");

  return {
    ...cloneEvidence(tag),
    tagId: requireString(tag.tagId, "tag.tagId"),
    name: cleanString(tag.name),
    sourceVersion: tag.sourceVersion
      ? toIsoTimestamp(tag.sourceVersion, "tag.sourceVersion")
      : undefined,
    isDeleted: tag.isDeleted === true,
  };
}

function collapseVersions(values, idField, normalize) {
  const grouped = new Map();

  for (const value of values) {
    const row = normalize(value);
    const rows = grouped.get(row[idField]) ?? [];
    rows.push(row);
    grouped.set(row[idField], rows);
  }

  const current = new Map();

  for (const [id, rows] of grouped) {
    rows.sort(compareSourceVersions);

    let merged = {};
    for (const row of rows) {
      merged = mergeDefined(merged, row);
    }

    current.set(id, merged);
  }

  return current;
}

function compareSourceVersions(left, right) {
  const leftTime = left.sourceVersion ? Date.parse(left.sourceVersion) : 0;
  const rightTime = right.sourceVersion ? Date.parse(right.sourceVersion) : 0;
  return leftTime - rightTime;
}

function mergeDefined(current, update) {
  const merged = { ...current };

  for (const [key, value] of Object.entries(update)) {
    if (value !== undefined) {
      merged[key] = value;
    }
  }

  return merged;
}

function assertContactScope(contactId, assignments, previousFacts) {
  for (const assignment of assignments.values()) {
    if (assignment.contactId !== contactId) {
      throw new TypeError(
        `assignment ${assignment.assignmentId} is outside contact ${contactId}`,
      );
    }
  }

  if (!Array.isArray(previousFacts)) {
    throw new TypeError("previousFacts must be an array");
  }

  for (const fact of previousFacts) {
    requireObject(fact, "previousFact");

    if (fact.contact_id !== contactId) {
      throw new TypeError("previousFacts must belong to the replacement contact");
    }
  }
}

function getMatchedAssignments({ contact, assignments, tags }) {
  if (contact.isDeleted) {
    return [];
  }

  const matched = [];

  for (const assignment of assignments.values()) {
    if (assignment.isDeleted) {
      continue;
    }

    const tag = tags.get(assignment.tagId);

    if (!tag || tag.isDeleted) {
      continue;
    }

    const policy = matchActiveCampaignRegistrationTag(tag.name);

    if (!policy) {
      continue;
    }

    if (!assignment.assignedAt) {
      throw new TypeError(
        `live assignment ${assignment.assignmentId} is missing assignedAt`,
      );
    }

    matched.push({ assignment, tag, policy });
  }

  const primaryTypes = new Set(
    matched
      .filter(({ policy }) => policy.matchType === "primary")
      .map(({ policy }) => policy.registrationType),
  );

  return matched.filter(({ policy }) => {
    if (policy.matchType === "primary") {
      return true;
    }

    return !primaryTypes.has(policy.registrationType);
  });
}

function buildLiveFacts({
  contact,
  matchedAssignments,
  replacementScopeId,
  replacementVersionAt,
  replacementVersionId,
}) {
  return matchedAssignments.map(({ assignment, tag, policy }) => {
    const sourceFactId = getSourceFactId(contact.contactId, assignment.assignmentId);
    const providerSourceVersionAt = latestTimestamp([
      contact.sourceVersion,
      assignment.sourceVersion,
      tag.sourceVersion,
    ]);

    return {
      source: "activecampaign",
      source_account: "default",
      source_fact_id: sourceFactId,
      source_version_at: replacementVersionAt,
      source_version_id: `${sourceFactId}:${replacementVersionId}`,
      replacement_scope_id: replacementScopeId,
      replacement_version_id: replacementVersionId,
      provider_source_version_at: providerSourceVersionAt,
      occurred_at: assignment.assignedAt,
      is_deleted: false,
      tombstone_reason: null,

      form_submission_id: assignment.assignmentId,
      canonical_event_id: createActiveCampaignCanonicalEventId({
        email: contact.email,
        registrationType: policy.registrationType,
        submittedAt: assignment.assignedAt,
      }),
      contact_id: contact.contactId,
      tag_id: tag.tagId,
      tag_name: tag.name,
      registration_type: policy.registrationType,
      registration_tag_type: policy.matchType,
      content_name: policy.contentName,
      form_type: "activecampaign_tag",
      event_source: "activecampaign",

      email: contact.email,
      phone: contact.phone,
      first_name: contact.firstName,
      last_name: contact.lastName,
      full_name: contact.fullName,
      raw_evidence: {
        contact: cloneEvidence(contact),
        assignment: cloneEvidence(assignment),
        tag: cloneEvidence(tag),
      },
    };
  });
}

function collapsePreviousFacts(previousFacts) {
  const current = new Map();

  for (const fact of previousFacts) {
    const sourceFactId = requireString(fact.source_fact_id, "previousFact.source_fact_id");
    const existing = current.get(sourceFactId);

    if (!existing || compareFactVersions(existing, fact) <= 0) {
      current.set(sourceFactId, cloneEvidence(fact));
    }
  }

  return current;
}

function compareFactVersions(left, right) {
  const leftTime = Date.parse(left.source_version_at ?? 0);
  const rightTime = Date.parse(right.source_version_at ?? 0);

  if (leftTime !== rightTime) {
    return leftTime - rightTime;
  }

  return String(left.source_version_id ?? "").localeCompare(
    String(right.source_version_id ?? ""),
  );
}

function buildTombstone({
  previousFact,
  contact,
  assignments,
  tags,
  liveFacts,
  replacementScopeId,
  replacementVersionAt,
  replacementVersionId,
}) {
  const assignment = assignments.get(previousFact.form_submission_id);
  const tag = assignment ? tags.get(assignment.tagId) : null;
  const sourceFactId = previousFact.source_fact_id;

  return {
    ...cloneEvidence(previousFact),
    source_version_at: replacementVersionAt,
    source_version_id: `${sourceFactId}:${replacementVersionId}`,
    replacement_scope_id: replacementScopeId,
    replacement_version_id: replacementVersionId,
    is_deleted: true,
    tombstone_reason: getTombstoneReason({
      previousFact,
      contact,
      assignment,
      tag,
      liveFacts,
    }),
    tombstone_evidence: {
      contact: cloneEvidence(contact),
      assignment: cloneEvidence(assignment ?? null),
      tag: cloneEvidence(tag ?? null),
    },
  };
}

function getTombstoneReason({ previousFact, contact, assignment, tag, liveFacts }) {
  if (contact.isDeleted) {
    return "contact_deleted";
  }

  if (!assignment) {
    return "assignment_missing_from_replacement";
  }

  if (assignment.isDeleted) {
    return "assignment_deleted";
  }

  if (!tag || tag.isDeleted) {
    return "tag_deleted_or_missing";
  }

  if (!matchActiveCampaignRegistrationTag(tag.name)) {
    return "tag_no_longer_matches_registration_policy";
  }

  const primaryExists = liveFacts.some(
    (fact) =>
      fact.registration_type === previousFact.registration_type &&
      fact.registration_tag_type === "primary",
  );

  if (previousFact.registration_tag_type === "fallback" && primaryExists) {
    return "fallback_suppressed_by_primary";
  }

  return "no_longer_eligible";
}

function getSourceFactId(contactId, assignmentId) {
  return `activecampaign:contact:${contactId}:assignment:${assignmentId}`;
}

function latestTimestamp(values) {
  const timestamps = values.filter(Boolean);

  if (timestamps.length === 0) {
    return null;
  }

  return timestamps.sort((left, right) => Date.parse(right) - Date.parse(left))[0];
}

function getPacificDate(value) {
  const timestamp = toIsoTimestamp(value, "submittedAt");
  const parts = Object.fromEntries(
    PACIFIC_DATE_FORMAT.formatToParts(new Date(timestamp))
      .filter(({ type }) => type !== "literal")
      .map(({ type, value: partValue }) => [type, partValue]),
  );

  return `${parts.year}-${parts.month}-${parts.day}`;
}
