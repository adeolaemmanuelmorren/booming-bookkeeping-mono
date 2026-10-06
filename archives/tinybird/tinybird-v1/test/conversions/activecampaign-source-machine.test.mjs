import assert from "node:assert/strict";
import test from "node:test";

import {
  backfillActiveCampaign,
  pollUpdatedActiveCampaignContacts,
  processActiveCampaignInbox,
  rollActiveCampaignContactReconciliation,
} from "../../worker/conversions/activecampaign-source-machine.mjs";
import {
  createSliceBudget,
  drainPublicationOutbox,
} from "../../worker/conversions/source-machine.mjs";
import { FakeClock, MemoryStore } from "../support/memory-store.mjs";

const rawContact = {
  id: "7",
  email: "person@example.com",
  firstName: "Ada",
  lastName: "Person",
  udate: "2026-05-31T23:30:00Z",
  deleted: "0",
};

const rawTags = {
  "10": { id: "10", tag: "[KRC] Registered for Challenge" },
  "11": { id: "11", tag: "[KRC] Registered for Challenge - June 2026" },
};

const fallbackAssignment = {
  id: "100",
  contact: "7",
  tag: "10",
  cdate: "2026-05-01T12:00:00Z",
};

const primaryAssignment = {
  id: "101",
  contact: "7",
  tag: "11",
  cdate: "2026-05-20T12:00:00Z",
};

test("ActiveCampaign backfill publishes raw tag evidence, keyset-pages contacts, and queues contactTags work", async () => {
  const store = new MemoryStore();
  const clock = new FakeClock();
  const requests = [];
  const provider = {
    read: {
      async listTags(request) {
        requests.push({ method: "listTags", request });
        return { items: Object.values(rawTags), hasMore: false };
      },
      async listContacts(request) {
        requests.push({ method: "listContacts", request });
        return { items: [rawContact], hasMore: false };
      },
    },
  };

  await backfillActiveCampaign({
    provider,
    store,
    budget: createSliceBudget({ clock }),
    maxPages: 1,
  });
  await backfillActiveCampaign({
    provider,
    store,
    budget: createSliceBudget({ clock }),
    maxPages: 1,
  });

  const tagPublication = [...store.outbox.values()].find(
    (record) => record.contract.scope_id === "activecampaign:tag:10",
  );
  assert.deepEqual(tagPublication.contract.rows, []);
  assert.deepEqual(tagPublication.contract.source_evidence, {
    kind: "activecampaign_tag_snapshot",
    rawTag: rawTags["10"],
  });
  assert.equal(requests[1].request.idGreater, "0");
  assert.equal(requests[1].request.orderById, "ASC");
  assert.equal(
    store.inbox.get("activecampaign:backfill:v1:contact:7").status,
    "pending",
  );
  assert.equal((await store.getCursor("activecampaign:backfill:v1")).phase, "complete");
});

test("contact reconciliation pages contactTags and publishes primary/fallback replacement", async () => {
  const store = new MemoryStore();
  const clock = new FakeClock();
  await store.putInboxIfAbsent({
    id: "activecampaign:backfill:v1:contact:7",
    source: "activecampaign",
    sourceAccount: "default",
    kind: "dirty_contact",
    contactId: "7",
    receivedAt: "2026-06-01T00:00:00.000Z",
    immutablePayload: { trigger: { kind: "backfill" } },
  });

  let assignmentPage = 0;
  const provider = {
    read: {
      async getContact() {
        return rawContact;
      },
      async listContactTags({ offset }) {
        assignmentPage += 1;

        if (offset === 0) {
          return { items: [fallbackAssignment], hasMore: true };
        }

        return { items: [primaryAssignment], hasMore: false };
      },
      async getTag({ tagId }) {
        return rawTags[tagId];
      },
    },
  };

  const calls = await processActiveCampaignInbox({
    provider,
    store,
    budget: createSliceBudget({ clock }),
    providerCallLimit: 10,
  });

  assert.equal(calls, 5);
  assert.equal(assignmentPage, 2);
  const contract = [...store.outbox.values()][0].contract;
  assert.equal(contract.scope_id, "activecampaign:contact:7");
  assert.equal(contract.rows.length, 1);
  assert.equal(contract.rows[0].form_submission_id, "101");
  assert.equal(contract.rows[0].registration_tag_type, "primary");

  await drainPublicationOutbox({
    source: "activecampaign",
    sourceAccount: "default",
    store,
    publishSourceReplacement: async () => {},
    budget: createSliceBudget({ clock }),
  });

  assert.equal(store.inbox.get("activecampaign:backfill:v1:contact:7").status, "processed");
  assert.equal(
    (await store.getState("activecampaign:contact-state:7")).assignments.length,
    2,
  );
});

test("a rolling contact slice repairs a missed tag removal without changing fallback time", async () => {
  const store = new MemoryStore();
  const clock = new FakeClock();
  const scopeId = "activecampaign:contact:7";
  const priorState = {
    contact: {
      contactId: "7",
      email: rawContact.email,
      firstName: "Ada",
      lastName: "Person",
      sourceVersion: rawContact.udate,
      isDeleted: false,
    },
    assignments: [
      {
        assignmentId: "100",
        contactId: "7",
        tagId: "10",
        assignedAt: fallbackAssignment.cdate,
        sourceVersion: "2026-05-20T12:00:00Z",
        isDeleted: false,
      },
      {
        assignmentId: "101",
        contactId: "7",
        tagId: "11",
        assignedAt: primaryAssignment.cdate,
        sourceVersion: "2026-05-20T12:00:00Z",
        isDeleted: false,
      },
    ],
    tags: [
      { tagId: "10", name: rawTags["10"].tag, isDeleted: false },
      { tagId: "11", name: rawTags["11"].tag, isDeleted: false },
    ],
  };
  const previousPrimaryFact = {
    source: "activecampaign",
    source_account: "default",
    source_fact_id: "activecampaign:contact:7:assignment:101",
    source_version_at: "2026-05-20T12:01:00.000Z",
    source_version_id: "prior-primary",
    contact_id: "7",
    form_submission_id: "101",
    occurred_at: "2026-05-20T12:00:00.000Z",
    registration_type: "krc",
    registration_tag_type: "primary",
    is_deleted: false,
  };
  await store.setState("activecampaign:contact-state:7", priorState);
  await store.setPublishedRows(scopeId, [previousPrimaryFact]);

  const provider = {
    read: {
      async listContacts() {
        return { items: [rawContact], hasMore: false };
      },
      async getContact() {
        return rawContact;
      },
      async listContactTags() {
        return { items: [fallbackAssignment], hasMore: false };
      },
      async getTag({ tagId }) {
        return rawTags[tagId];
      },
    },
  };

  await rollActiveCampaignContactReconciliation({
    provider,
    store,
    budget: createSliceBudget({ clock }),
    maxPages: 1,
  });
  await processActiveCampaignInbox({
    provider,
    store,
    budget: createSliceBudget({ clock }),
    providerCallLimit: 10,
  });

  const contract = [...store.outbox.values()][0].contract;
  const fallback = contract.rows.find((row) => row.form_submission_id === "100");
  const removedPrimary = contract.rows.find((row) => row.form_submission_id === "101");

  assert.equal(fallback.is_deleted, false);
  assert.equal(fallback.occurred_at, "2026-05-01T12:00:00.000Z");
  assert.equal(removedPrimary.is_deleted, true);
  assert.equal(removedPrimary.tombstone_reason, "assignment_deleted");
});

test("updated-contact polling uses a fixed filter window but does not infer tag removals", async () => {
  const store = new MemoryStore();
  const clock = new FakeClock();
  const requests = [];
  const provider = {
    read: {
      async listContacts(request) {
        requests.push(request);
        return { items: [rawContact], hasMore: false };
      },
    },
  };

  await pollUpdatedActiveCampaignContacts({
    provider,
    store,
    budget: createSliceBudget({ clock }),
    maxPages: 1,
  });

  assert.ok(requests[0].updatedAfter);
  assert.ok(requests[0].updatedBefore);
  assert.equal(requests[0].orderById, "ASC");
  assert.equal(requests[0].idGreater, "0");
  assert.equal(
    store.inbox.get(
      "activecampaign:contact-update:7:2026-05-31T23:30:00.000Z",
    ).status,
    "pending",
  );
});

test("ActiveCampaign retry reuses its durable observation sequence after an outbox failure", async () => {
  const store = new MemoryStore();
  const clock = new FakeClock();
  await store.putInboxIfAbsent({
    id: "activecampaign:contact-update:7:retry",
    source: "activecampaign",
    sourceAccount: "default",
    kind: "dirty_contact",
    contactId: "7",
    receivedAt: "2026-06-01T00:00:00.000Z",
    immutablePayload: { trigger: { kind: "contact_update_poll" } },
  });

  const provider = {
    read: {
      async getContact() {
        return rawContact;
      },
      async listContactTags() {
        return { items: [fallbackAssignment], hasMore: false };
      },
      async getTag({ tagId }) {
        return rawTags[tagId];
      },
    },
  };
  const realPutOutbox = store.putOutboxIfAbsent.bind(store);
  let failOnce = true;
  store.putOutboxIfAbsent = async (record) => {
    if (failOnce) {
      failOnce = false;
      throw new Error("simulated durable write failure");
    }

    return realPutOutbox(record);
  };

  await assert.rejects(
    processActiveCampaignInbox({
      provider,
      store,
      budget: createSliceBudget({ clock }),
      providerCallLimit: 10,
    }),
    /simulated durable write failure/,
  );
  await processActiveCampaignInbox({
    provider,
    store,
    budget: createSliceBudget({ clock }),
    providerCallLimit: 10,
  });

  assert.equal(store.sequences.get("activecampaign:contact-observations"), 1);
  assert.equal([...store.outbox.values()][0].contract.observation_sequence, 1);
});

test("an undated unrelated tag does not block dated registrations or the next contact", async () => {
  const store = new MemoryStore();
  const clock = new FakeClock();
  for (const contactId of ["7", "8"]) {
    await store.putInboxIfAbsent({
      id: `undated:${contactId}`, source: "activecampaign", sourceAccount: "default",
      kind: "dirty_contact", contactId, receivedAt: new Date(clock.now()).toISOString(), immutablePayload: {},
    });
  }
  const provider = { read: {
    async getContact({ contactId }) { return { ...rawContact, id: contactId }; },
    async listContactTags({ contactId }) {
      return { items: [
        { ...primaryAssignment, contact: contactId },
        { id: "1405610", contact: contactId, tag: "102", cdate: null, created_timestamp: "2022-11-15 14:42:21" },
      ], hasMore: false };
    },
    async getTag({ tagId }) {
      return tagId === "102" ? { id: "102", tag: "Unsubscribed via EMT - 11/15/22" } : rawTags[tagId];
    },
  } };
  await processActiveCampaignInbox({ provider, store, budget: createSliceBudget({ clock }), inboxLimit: 2 });
  const contracts = [...store.outbox.values()].map(row => row.contract);
  assert.equal(contracts.length, 2);
  for (const contract of contracts) {
    assert.equal(contract.rows.length, 1);
    assert.equal(contract.rows[0].form_submission_id, primaryAssignment.id);
    assert.equal(contract.rows[0].occurred_at, primaryAssignment.cdate.replace('Z', '.000Z'));
    assert.equal(contract.source_evidence.rawContactTags[1].cdate, null);
  }
});

test("an undated registration tag still cannot publish a guessed registration date", async () => {
  const store = new MemoryStore();
  const clock = new FakeClock();
  await store.putInboxIfAbsent({
    id: "undated-registration", source: "activecampaign", sourceAccount: "default",
    kind: "dirty_contact", contactId: "7", receivedAt: new Date(clock.now()).toISOString(), immutablePayload: {},
  });
  const provider = { read: {
    async getContact() { return rawContact; },
    async listContactTags() { return { items: [{ ...primaryAssignment, cdate: null }], hasMore: false }; },
    async getTag({ tagId }) { return rawTags[tagId]; },
  } };
  await assert.rejects(
    processActiveCampaignInbox({ provider, store, budget: createSliceBudget({ clock }) }),
    /missing assignedAt/,
  );
  assert.equal(store.outbox.size, 0);
  assert.equal(store.inbox.get("undated-registration").status, "pending");
});
