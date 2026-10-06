import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import {
  buildActiveCampaignContactReplacement,
  createActiveCampaignCanonicalEventId,
  matchActiveCampaignRegistrationTag,
} from "../../worker/conversions/activecampaign.mjs";

const contact = {
  contactId: "contact-7",
  email: "  PERSON@Example.COM ",
  phone: " +15555550100 ",
  firstName: "Ada",
  lastName: "Person",
  sourceVersion: "2026-01-01T00:00:00Z",
  isDeleted: false,
};

const fallbackTag = {
  tagId: "tag-fallback",
  name: "[KRC] Registered for Challenge",
  sourceVersion: "2026-01-01T00:00:00Z",
  isDeleted: false,
};

const primaryTag = {
  tagId: "tag-primary",
  name: "[KRC] Registered for Challenge - January 2026",
  sourceVersion: "2026-01-01T00:00:00Z",
  isDeleted: false,
};

const fallbackAssignment = {
  assignmentId: "assignment-fallback",
  contactId: "contact-7",
  tagId: "tag-fallback",
  assignedAt: "2026-01-01T07:30:00Z",
  sourceVersion: "2026-01-01T07:31:00Z",
  isDeleted: false,
};

const primaryAssignment = {
  assignmentId: "assignment-primary",
  contactId: "contact-7",
  tagId: "tag-primary",
  assignedAt: "2026-02-02T20:00:00Z",
  sourceVersion: "2026-02-02T20:01:00Z",
  isDeleted: false,
};

test("matches the exact Dataform primary and fallback tag rules", () => {
  assert.deepEqual(
    matchActiveCampaignRegistrationTag("[KRC] Registered - May 2026"),
    {
      registrationType: "krc",
      contentName: "Keyboard Rich Challenge Registration",
      matchType: "primary",
    },
  );
  assert.equal(
    matchActiveCampaignRegistrationTag("[KRC] Registered for Challenge").matchType,
    "fallback",
  );
  assert.equal(
    matchActiveCampaignRegistrationTag("[CW] Registered for Webinar").registrationType,
    "webinar",
  );
  assert.equal(
    matchActiveCampaignRegistrationTag("[cw] Registered for Webinar"),
    null,
  );
});

test("uses the assignment's original Pacific date in the canonical event ID", () => {
  const expectedDigest = createHash("sha256")
    .update("person@example.com|krc|2025-12-31")
    .digest("hex");

  assert.equal(
    createActiveCampaignCanonicalEventId({
      email: contact.email,
      registrationType: "krc",
      submittedAt: fallbackAssignment.assignedAt,
    }),
    `form_submission_${expectedDigest}`,
  );
});

test("primary addition suppresses fallback, and removal resurrects its original time", () => {
  const first = buildActiveCampaignContactReplacement({
    contact,
    assignments: [fallbackAssignment],
    tags: [fallbackTag, primaryTag],
    previousFacts: [],
    replacementVersion: "2026-01-01T08:00:00Z",
    replacementId: "contact_tag_added:fallback",
  });

  assert.equal(first.rows.length, 1);
  assert.equal(first.rows[0].registration_tag_type, "fallback");
  assert.equal(first.rows[0].occurred_at, "2026-01-01T07:30:00.000Z");

  const second = buildActiveCampaignContactReplacement({
    contact,
    assignments: [fallbackAssignment, primaryAssignment],
    tags: [fallbackTag, primaryTag],
    previousFacts: first.rows,
    replacementVersion: "2026-02-02T20:02:00Z",
    replacementId: "contact_tag_added:primary",
  });
  const suppressedFallback = second.rows.find(
    (row) => row.form_submission_id === "assignment-fallback",
  );
  const livePrimary = second.rows.find(
    (row) => row.form_submission_id === "assignment-primary",
  );

  assert.equal(suppressedFallback.is_deleted, true);
  assert.equal(suppressedFallback.tombstone_reason, "fallback_suppressed_by_primary");
  assert.equal(livePrimary.is_deleted, false);

  const primaryRemoval = {
    assignmentId: "assignment-primary",
    contactId: "contact-7",
    tagId: "tag-primary",
    sourceVersion: "2026-03-03T12:00:00Z",
    isDeleted: true,
  };
  const third = buildActiveCampaignContactReplacement({
    contact,
    assignments: [
      fallbackAssignment,
      primaryAssignment,
      primaryRemoval,
      primaryRemoval,
    ],
    tags: [fallbackTag, primaryTag],
    previousFacts: [...first.rows, ...second.rows],
    replacementVersion: "2026-03-03T12:01:00Z",
    replacementId: "contact_tag_removed:primary",
  });
  const resurrectedFallback = third.rows.find(
    (row) => row.form_submission_id === "assignment-fallback",
  );
  const removedPrimary = third.rows.find(
    (row) => row.form_submission_id === "assignment-primary",
  );

  assert.equal(resurrectedFallback.is_deleted, false);
  assert.equal(resurrectedFallback.registration_tag_type, "fallback");
  assert.equal(resurrectedFallback.occurred_at, "2026-01-01T07:30:00.000Z");
  assert.equal(resurrectedFallback.canonical_event_id, first.rows[0].canonical_event_id);
  assert.equal(removedPrimary.is_deleted, true);
  assert.equal(removedPrimary.tombstone_reason, "assignment_deleted");
});

test("a deleted contact produces tombstones for its contact-scoped replacement", () => {
  const initial = buildActiveCampaignContactReplacement({
    contact,
    assignments: [fallbackAssignment],
    tags: [fallbackTag],
    previousFacts: [],
    replacementVersion: "2026-01-01T08:00:00Z",
  });
  const deleted = buildActiveCampaignContactReplacement({
    contact: {
      ...contact,
      sourceVersion: "2026-04-01T00:00:00Z",
      isDeleted: true,
    },
    assignments: [fallbackAssignment],
    tags: [fallbackTag],
    previousFacts: initial.rows,
    replacementVersion: "2026-04-01T00:01:00Z",
  });

  assert.equal(deleted.rows.length, 1);
  assert.equal(deleted.rows[0].is_deleted, true);
  assert.equal(deleted.rows[0].tombstone_reason, "contact_deleted");
  assert.equal(
    deleted.rows[0].replacement_scope_id,
    "activecampaign:contact:contact-7",
  );
});

test("tag deletion removes a live registration with an explicit tombstone", () => {
  const initial = buildActiveCampaignContactReplacement({
    contact,
    assignments: [fallbackAssignment],
    tags: [fallbackTag],
    previousFacts: [],
    replacementVersion: "2026-01-01T08:00:00Z",
  });
  const removed = buildActiveCampaignContactReplacement({
    contact,
    assignments: [fallbackAssignment],
    tags: [
      fallbackTag,
      {
        tagId: "tag-fallback",
        sourceVersion: "2026-05-01T00:00:00Z",
        isDeleted: true,
      },
    ],
    previousFacts: initial.rows,
    replacementVersion: "2026-05-01T00:01:00Z",
  });

  assert.equal(removed.rows.length, 1);
  assert.equal(removed.rows[0].is_deleted, true);
  assert.equal(removed.rows[0].tombstone_reason, "tag_deleted_or_missing");
});
