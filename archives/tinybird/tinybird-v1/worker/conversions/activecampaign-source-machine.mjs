import {
  cleanString,
  cloneEvidence,
  requireObject,
  requireString,
  toIsoTimestamp,
} from "./shared.mjs";
import { buildActiveCampaignContactReplacement } from "./activecampaign.mjs";
import {
  createSliceBudget,
  drainPublicationOutbox,
  enqueuePublication,
  replacementContract,
} from "./source-machine.mjs";

export async function runActiveCampaignSourceSlice(dependencies, options = {}) {
  const budget = createSliceBudget({
    clock: dependencies.clock,
    maxDurationMs: options.maxDurationMs,
  });
  const result = {
    published: 0,
    contactReadCalls: 0,
    updatePages: 0,
    backfillPages: 0,
    reconciliationPages: 0,
  };

  result.published = await drainPublicationOutbox({
    source: "activecampaign",
    sourceAccount: "default",
    store: dependencies.store,
    publishSourceReplacement: dependencies.publishSourceReplacement,
    publishSourceReplacements: dependencies.publishSourceReplacements,
    budget,
    limit: options.publicationLimit ?? 10,
  });

  if (budget.canStartCall()) {
    result.contactReadCalls = await processActiveCampaignInbox({
      ...dependencies,
      budget,
      inboxLimit: options.inboxLimit ?? 5,
      providerCallLimit: options.contactReadCallLimit ?? 20,
    });
  }

  if (budget.canStartCall()) {
    result.updatePages = await pollUpdatedActiveCampaignContacts({
      ...dependencies,
      budget,
      initialLookbackMs: options.updateInitialLookbackMs,
      overlapMs: options.updateOverlapMs,
      settleMs: options.updateSettleMs,
      maxPages: options.updatePageLimit ?? 1,
    });
  }

  if (budget.canStartCall()) {
    result.backfillPages = await backfillActiveCampaign({
      ...dependencies,
      budget,
      backfillId: options.backfillId ?? "v1",
      maxPages: options.backfillPageLimit ?? 1,
    });
  }

  const backfillCursor = await dependencies.store.getCursor(
    `activecampaign:backfill:${options.backfillId ?? "v1"}`,
  );
  const backfillComplete = backfillCursor?.phase === "complete";

  if (
    budget.canStartCall() &&
    backfillComplete &&
    result.backfillPages === 0 &&
    options.runRollingReconciliation !== false
  ) {
    result.reconciliationPages = await rollActiveCampaignContactReconciliation({
      ...dependencies,
      budget,
      maxPages: options.reconciliationPageLimit ?? 1,
    });
  }

  return result;
}

export async function pollUpdatedActiveCampaignContacts({
  provider,
  store,
  budget,
  initialLookbackMs = 3_600_000,
  overlapMs = 600_000,
  settleMs = 30_000,
  maxPages = 1,
}) {
  const cursorKey = "activecampaign:updated-contacts-cursor";
  let cursor = await store.getCursor(cursorKey);

  if (!cursor) {
    cursor = {
      completedThrough: new Date(budget.clock.now() - initialLookbackMs).toISOString(),
      activeWindow: null,
    };
  }

  if (!cursor.activeWindow) {
    const windowEndMs = budget.clock.now() - settleMs;
    const completedThroughMs = Date.parse(cursor.completedThrough);

    if (windowEndMs <= completedThroughMs) {
      await store.setCursor(cursorKey, cursor);
      return 0;
    }

    cursor.activeWindow = {
      updatedAfter: new Date(Math.max(0, completedThroughMs - overlapMs)).toISOString(),
      updatedBefore: new Date(windowEndMs).toISOString(),
      idGreater: "0",
    };
    await store.setCursor(cursorKey, cursor);
  }

  let pages = 0;

  while (pages < maxPages && budget.canStartCall()) {
    const page = validateActiveCampaignPage(
      await provider.read.listContacts({
        updatedAfter: cursor.activeWindow.updatedAfter,
        updatedBefore: cursor.activeWindow.updatedBefore,
        idGreater: cursor.activeWindow.idGreater,
        orderById: "ASC",
        limit: 100,
        deadlineAtMs: budget.deadlineAtMs,
      }),
      "listContacts",
    );

    for (const contact of page.items) {
      const contactId = requireString(contact.id, "contact.id");
      const updatedAt = getContactUpdatedAt(contact) ?? cursor.activeWindow.updatedBefore;
      await enqueueDirtyActiveCampaignContact({
        store,
        inboxId: `activecampaign:contact-update:${contactId}:${updatedAt}`,
        contactId,
        receivedAt: budget.nowIso(),
        trigger: { kind: "contact_update_poll", contact: cloneEvidence(contact) },
      });
    }

    pages += 1;

    if (page.hasMore) {
      cursor.activeWindow.idGreater = getLastNumericId(page.items, "contacts");
      await store.setCursor(cursorKey, cursor);
      continue;
    }

    cursor.completedThrough = cursor.activeWindow.updatedBefore;
    cursor.activeWindow = null;
    await store.setCursor(cursorKey, cursor);
    break;
  }

  return pages;
}

export async function backfillActiveCampaign({
  provider,
  store,
  budget,
  backfillId = "v1",
  maxPages = 1,
}) {
  const cursorKey = `activecampaign:backfill:${backfillId}`;
  let cursor = await store.getCursor(cursorKey);

  if (cursor?.phase === "complete") {
    return 0;
  }

  if (!cursor) {
    cursor = { phase: "tags", tagOffset: 0, contactIdGreater: "0" };
    await store.setCursor(cursorKey, cursor);
  }

  let pages = 0;

  while (pages < maxPages && budget.canStartCall()) {
    if (cursor.phase === "tags") {
      const page = validateActiveCampaignPage(
        await provider.read.listTags({
          offset: cursor.tagOffset,
          limit: 100,
          deadlineAtMs: budget.deadlineAtMs,
        }),
        "listTags",
      );
      const observedAt = budget.nowIso();

      for (const tag of page.items) {
        const tagId = requireString(tag.id, "tag.id");
        const scopeId = `activecampaign:tag:${tagId}`;
        const sequence = await store.nextSequence(
          "activecampaign:tag-observations",
        );
        const contract = replacementContract({
          source: "activecampaign",
          sourceAccount: "default",
          scopeId,
          replacementId: `${scopeId}:backfill:${backfillId}`,
          observedAt,
          observationSequence: sequence,
          rows: [],
          evidenceInboxIds: [],
          sourceEvidence: {
            kind: "activecampaign_tag_snapshot",
            rawTag: cloneEvidence(tag),
          },
        });

        await enqueuePublication({
          store,
          contract,
        });
      }

      pages += 1;

      if (page.hasMore) {
        assertNonEmptyPage(page, "tags");
        cursor.tagOffset += page.items.length;
      } else {
        cursor.phase = "contacts";
      }

      await store.setCursor(cursorKey, cursor);
      continue;
    }

    const page = validateActiveCampaignPage(
      await provider.read.listContacts({
        idGreater: cursor.contactIdGreater,
        orderById: "ASC",
        limit: 100,
        deadlineAtMs: budget.deadlineAtMs,
      }),
      "listContacts",
    );

    for (const contact of page.items) {
      const contactId = requireString(contact.id, "contact.id");
      await enqueueDirtyActiveCampaignContact({
        store,
        inboxId: `activecampaign:backfill:${backfillId}:contact:${contactId}`,
        contactId,
        receivedAt: budget.nowIso(),
        trigger: { kind: "backfill", contact: cloneEvidence(contact) },
      });
    }

    pages += 1;

    if (page.hasMore) {
      cursor.contactIdGreater = getLastNumericId(page.items, "contacts");
    } else {
      cursor.phase = "complete";
      cursor.completedAt = budget.nowIso();
    }

    await store.setCursor(cursorKey, cursor);
  }

  return pages;
}

export async function rollActiveCampaignContactReconciliation({
  provider,
  store,
  budget,
  maxPages = 1,
}) {
  const cursorKey = "activecampaign:rolling-contact-reconciliation";
  let cursor =
    (await store.getCursor(cursorKey)) ?? {
      cycle: 1,
      contactIdGreater: "0",
    };
  let pages = 0;

  while (pages < maxPages && budget.canStartCall()) {
    const page = validateActiveCampaignPage(
      await provider.read.listContacts({
        idGreater: cursor.contactIdGreater,
        orderById: "ASC",
        limit: 100,
        deadlineAtMs: budget.deadlineAtMs,
      }),
      "listContacts",
    );

    for (const contact of page.items) {
      const contactId = requireString(contact.id, "contact.id");
      await enqueueDirtyActiveCampaignContact({
        store,
        inboxId: `activecampaign:reconcile:${cursor.cycle}:contact:${contactId}`,
        contactId,
        receivedAt: budget.nowIso(),
        trigger: { kind: "rolling_reconciliation" },
      });
    }

    pages += 1;

    if (page.hasMore) {
      cursor.contactIdGreater = getLastNumericId(page.items, "contacts");
    } else {
      cursor.lastCompletedAt = budget.nowIso();
      cursor.cycle += 1;
      cursor.contactIdGreater = "0";
    }

    await store.setCursor(cursorKey, cursor);

    if (!page.hasMore) {
      break;
    }
  }

  return pages;
}

export async function processActiveCampaignInbox({
  provider,
  store,
  budget,
  inboxLimit = 5,
  providerCallLimit = 20,
}) {
  const pending = await store.listPendingInbox({
    source: "activecampaign",
    sourceAccount: "default",
    limit: inboxLimit,
  });
  let calls = 0;
  const contactsStarted = new Set();

  for (const inbox of pending) {
    if (contactsStarted.has(inbox.contactId)) {
      continue;
    }

    contactsStarted.add(inbox.contactId);
    let progress = inbox.progress ?? initialContactProgress(budget.nowIso());

    while (calls < providerCallLimit && budget.canStartCall()) {
      if (progress.stage === "contact") {
        progress.contact = await provider.read.getContact({
          contactId: inbox.contactId,
          deadlineAtMs: budget.deadlineAtMs,
        });
        progress.stage = progress.contact ? "assignments" : "finalize";
        await store.saveInboxProgress(inbox.id, progress);
        calls += 1;
        continue;
      }

      if (progress.stage === "assignments") {
        const page = validateActiveCampaignPage(
          await provider.read.listContactTags({
            contactId: inbox.contactId,
            offset: progress.assignmentOffset,
            limit: 100,
            deadlineAtMs: budget.deadlineAtMs,
          }),
          "listContactTags",
        );
        progress.currentAssignments.push(...cloneEvidence(page.items));
        calls += 1;

        if (page.hasMore) {
          assertNonEmptyPage(page, "contact tags");
          progress.assignmentOffset += page.items.length;
        } else {
          const previousState = await store.getState(
            `activecampaign:contact-state:${inbox.contactId}`,
          );
          progress.tagIds = uniqueTagIds([
            ...progress.currentAssignments,
            ...(previousState?.assignments ?? []).map((assignment) => ({
              tag: assignment.tagId,
            })),
          ]);
          progress.stage = "tags";
        }

        await store.saveInboxProgress(inbox.id, progress);
        continue;
      }

      if (progress.stage === "tags") {
        const tagId = progress.tagIds[progress.tagIndex];

        if (!tagId) {
          progress.stage = "finalize";
          await store.saveInboxProgress(inbox.id, progress);
          continue;
        }

        const tag = await provider.read.getTag({
          tagId,
          deadlineAtMs: budget.deadlineAtMs,
        });
        progress.currentTags.push({ tagId, tag: cloneEvidence(tag) });
        progress.tagIndex += 1;
        await store.saveInboxProgress(inbox.id, progress);
        calls += 1;
        continue;
      }

      if (progress.stage === "finalize") {
        await finalizeActiveCampaignContact({ inbox, progress, store });
        break;
      }

      throw new TypeError(`unknown ActiveCampaign inbox stage: ${progress.stage}`);
    }
  }

  return calls;
}

async function finalizeActiveCampaignContact({ inbox, progress, store }) {
  const stateKey = `activecampaign:contact-state:${inbox.contactId}`;
  const previousState =
    (await store.getState(stateKey)) ?? emptyContactState(inbox.contactId);
  const contact = normalizeReconciledContact({
    contactId: inbox.contactId,
    rawContact: progress.contact,
    previousContact: previousState.contact,
    observedAt: progress.observedAt,
  });
  const assignments = reconcileAssignments({
    contactId: inbox.contactId,
    rawAssignments: progress.currentAssignments,
    previousAssignments: previousState.assignments,
    observedAt: progress.observedAt,
  });
  const tags = reconcileTags({
    tagReads: progress.currentTags,
    previousTags: previousState.tags,
    assignments,
    observedAt: progress.observedAt,
  });
  const scopeId = `activecampaign:contact:${inbox.contactId}`;
  const previousFacts = await store.getPublishedRows(scopeId);
  const sequence =
    progress.observationSequence ??
    (await store.nextSequence("activecampaign:contact-observations"));
  progress.observationSequence = sequence;
  await store.saveInboxProgress(inbox.id, progress);
  const replacement = buildActiveCampaignContactReplacement({
    contact,
    assignments,
    tags,
    previousFacts,
    replacementVersion: progress.observedAt,
    replacementId: `observation:${sequence}`,
  });
  const contract = replacementContract({
    source: "activecampaign",
    sourceAccount: "default",
    scopeId,
    replacementId: `${scopeId}:observation:${sequence}`,
    observedAt: progress.observedAt,
    observationSequence: sequence,
    rows: replacement.rows,
    evidenceInboxIds: [inbox.id],
    sourceEvidence: {
      kind: "activecampaign_contact_snapshot",
      inbox: {
        id: inbox.id,
        kind: inbox.kind,
        receivedAt: inbox.receivedAt,
        immutablePayload: cloneEvidence(inbox.immutablePayload),
      },
      rawContact: cloneEvidence(progress.contact),
      rawContactTags: cloneEvidence(progress.currentAssignments),
      rawTags: cloneEvidence(progress.currentTags),
    },
  });
  const outbox = await enqueuePublication({
    store,
    contract,
    inboxIds: [inbox.id],
    stateUpdates: [
      {
        key: stateKey,
        value: compactActiveCampaignState({ contact, assignments, tags }),
      },
    ],
  });

  await store.markInboxOutboxed(inbox.id, outbox.id);
}

function compactActiveCampaignState({ contact, assignments, tags }) {
  return {
    contact: {
      contactId: contact.contactId,
      email: contact.email,
      phone: contact.phone,
      firstName: contact.firstName,
      lastName: contact.lastName,
      fullName: contact.fullName,
      sourceVersion: contact.sourceVersion,
      isDeleted: contact.isDeleted,
    },
    assignments: assignments.map((assignment) => ({
      assignmentId: assignment.assignmentId,
      contactId: assignment.contactId,
      tagId: assignment.tagId,
      assignedAt: assignment.assignedAt,
      sourceVersion: assignment.sourceVersion,
      isDeleted: assignment.isDeleted,
    })),
    tags: tags.map((tag) => ({
      tagId: tag.tagId,
      name: tag.name,
      sourceVersion: tag.sourceVersion,
      isDeleted: tag.isDeleted,
    })),
  };
}

function enqueueDirtyActiveCampaignContact({
  store,
  inboxId,
  contactId,
  receivedAt,
  trigger,
}) {
  return store.putInboxIfAbsent({
    id: inboxId,
    source: "activecampaign",
    sourceAccount: "default",
    kind: "dirty_contact",
    contactId,
    receivedAt,
    immutablePayload: { trigger: cloneEvidence(trigger) },
  });
}

function initialContactProgress(observedAt) {
  return {
    stage: "contact",
    observedAt,
    contact: null,
    currentAssignments: [],
    assignmentOffset: 0,
    tagIds: [],
    tagIndex: 0,
    currentTags: [],
  };
}

function getContactUpdatedAt(contact) {
  const value = contact.udate ?? contact.updatedAt ?? contact.updated_at;

  if (!value) {
    return null;
  }

  if (String(value).startsWith("0000-00-00")) {
    return null;
  }

  return toIsoTimestamp(value, "contact updated time");
}

function validateActiveCampaignPage(value, methodName) {
  const page = requireObject(value, methodName);

  if (!Array.isArray(page.items)) {
    throw new TypeError(`${methodName}.items must be an array`);
  }

  if (typeof page.hasMore !== "boolean") {
    throw new TypeError(`${methodName}.hasMore must be a boolean`);
  }

  return page;
}

function assertNonEmptyPage(page, collection) {
  if (page.items.length === 0) {
    throw new TypeError(`${collection} page cannot be empty when hasMore is true`);
  }
}

function getLastNumericId(items, collection) {
  assertNonEmptyPage({ items }, collection);

  const id = requireString(items.at(-1).id, `${collection} last ID`);

  if (!/^\d+$/.test(id)) {
    throw new TypeError(`${collection} IDs must be numeric for id_greater pagination`);
  }

  return id;
}

function uniqueTagIds(assignments) {
  return [
    ...new Set(
      assignments
        .map((assignment) => cleanString(assignment.tag))
        .filter(Boolean),
    ),
  ].sort(compareNumericStrings);
}

function compareNumericStrings(left, right) {
  return Number(left) - Number(right);
}

function emptyContactState(contactId) {
  return {
    contact: { contactId, isDeleted: true },
    assignments: [],
    tags: [],
  };
}

function normalizeReconciledContact({
  contactId,
  rawContact,
  previousContact,
  observedAt,
}) {
  if (!rawContact) {
    return {
      ...cloneEvidence(previousContact),
      contactId,
      sourceVersion: observedAt,
      isDeleted: true,
    };
  }

  return {
    contactId,
    email: rawContact.email,
    phone: rawContact.phone,
    firstName: rawContact.firstName ?? rawContact.first_name,
    lastName: rawContact.lastName ?? rawContact.last_name,
    fullName: rawContact.fullName ?? rawContact.full_name,
    sourceVersion: getContactUpdatedAt(rawContact) ?? observedAt,
    isDeleted: rawContact.deleted === "1" || rawContact.isDeleted === true,
    rawEvidence: cloneEvidence(rawContact),
  };
}

function reconcileAssignments({
  contactId,
  rawAssignments,
  previousAssignments,
  observedAt,
}) {
  const previousById = new Map(
    previousAssignments.map((assignment) => [assignment.assignmentId, assignment]),
  );
  const currentById = new Map();

  for (const raw of rawAssignments) {
    const assignmentId = requireString(raw.id, "contactTag.id");
    const previous = previousById.get(assignmentId);
    const assignedAt = raw.cdate ?? raw.assignedAt ?? previous?.assignedAt;

    currentById.set(assignmentId, {
      assignmentId,
      contactId,
      tagId: requireString(raw.tag, "contactTag.tag"),
      // Unrelated contact labels can have no date. Registration validation
      // below still requires the original timestamp before publishing a fact.
      assignedAt: assignedAt ? toIsoTimestamp(assignedAt, "contactTag.cdate") : null,
      sourceVersion: observedAt,
      isDeleted: false,
      rawEvidence: cloneEvidence(raw),
    });
  }

  for (const previous of previousAssignments) {
    if (currentById.has(previous.assignmentId)) {
      continue;
    }

    currentById.set(previous.assignmentId, {
      ...cloneEvidence(previous),
      sourceVersion: observedAt,
      isDeleted: true,
    });
  }

  return [...currentById.values()].sort((left, right) =>
    left.assignmentId.localeCompare(right.assignmentId),
  );
}

function reconcileTags({ tagReads, previousTags, assignments, observedAt }) {
  const previousById = new Map(previousTags.map((tag) => [tag.tagId, tag]));
  const readById = new Map(tagReads.map((read) => [read.tagId, read.tag]));
  const neededTagIds = new Set(assignments.map((assignment) => assignment.tagId));
  const tags = [];

  for (const tagId of neededTagIds) {
    const raw = readById.get(tagId);
    const previous = previousById.get(tagId);

    if (!raw) {
      tags.push({
        ...(previous ? cloneEvidence(previous) : { tagId }),
        sourceVersion: observedAt,
        isDeleted: true,
      });
      continue;
    }

    tags.push({
      tagId,
      name: raw.tag ?? raw.name,
      sourceVersion: observedAt,
      isDeleted: false,
      rawEvidence: cloneEvidence(raw),
    });
  }

  return tags.sort((left, right) => left.tagId.localeCompare(right.tagId));
}
