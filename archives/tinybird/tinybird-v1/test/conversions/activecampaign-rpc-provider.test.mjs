import assert from "node:assert/strict";
import test from "node:test";

import { createActiveCampaignRpcProvider } from "../../worker/conversions/activecampaign-rpc-provider.mjs";
import { ProviderReadError } from "../../worker/conversions/provider-rpc.mjs";

const clock = { now: () => Date.parse("2026-06-01T00:00:00Z") };
const deadlineAtMs = clock.now() + 30_000;

test("ActiveCampaign contacts use documented filters and ID keyset pagination", async () => {
  const calls = [];
  const provider = createActiveCampaignRpcProvider({
    clock,
    rpcRead: async (path, parameters) => {
      calls.push({ path, parameters });
      return ok({ contacts: [{ id: "8" }, { id: "9" }], meta: { total: "50" } });
    },
  });

  const page = await provider.read.listContacts({
    updatedAfter: "2026-05-01T01:02:03-07:00",
    updatedBefore: "2026-06-01T00:00:00Z",
    idGreater: "7",
    orderById: "ASC",
    limit: 2,
    deadlineAtMs,
  });

  assert.deepEqual(calls, [
    {
      path: "/contacts",
      parameters: {
        "filters[updated_after]": "2026-05-01T08:02:03.000Z",
        "filters[updated_before]": "2026-06-01T00:00:00.000Z",
        id_greater: "7",
        "orders[id]": "ASC",
        limit: "2",
      },
    },
  ]);
  assert.deepEqual(page.items.map((item) => item.id), ["8", "9"]);
  assert.equal(page.hasMore, true);
});

test("ActiveCampaign offset reads use meta.total and 404 alone means missing", async () => {
  const calls = [];
  const provider = createActiveCampaignRpcProvider({
    clock,
    rpcRead: async (path, parameters) => {
      calls.push({ path, parameters });

      if (path === "/contacts/7/contactTags") {
        return ok({ contactTags: [{ id: "100" }], meta: { total: "3" } });
      }

      if (path === "/tags") {
        return ok({ tags: [{ id: "10" }], meta: { total: "1" } });
      }

      if (path === "/contacts/404" || path === "/tags/404") {
        return { status: 404, retryAfter: null, body: { message: "missing" } };
      }

      if (path === "/contacts/7") {
        return ok({ contact: { id: "7", email: "person@example.com" } });
      }

      if (path === "/tags/10") {
        return ok({ tag: { id: "10", tag: "Registered" } });
      }

      throw new Error(`unexpected ActiveCampaign test path ${path}`);
    },
  });

  const assignments = await provider.read.listContactTags({
    contactId: "7",
    offset: 1,
    limit: 1,
    deadlineAtMs,
  });
  const tags = await provider.read.listTags({
    offset: 0,
    limit: 100,
    deadlineAtMs,
  });
  const contact = await provider.read.getContact({
    contactId: "7",
    deadlineAtMs,
  });
  const tag = await provider.read.getTag({ tagId: "10", deadlineAtMs });
  const missingContact = await provider.read.getContact({
    contactId: "404",
    deadlineAtMs,
  });
  const missingTag = await provider.read.getTag({
    tagId: "404",
    deadlineAtMs,
  });

  assert.equal(assignments.hasMore, true);
  assert.equal(tags.hasMore, false);
  assert.equal(contact.email, "person@example.com");
  assert.equal(tag.tag, "Registered");
  assert.equal(missingContact, null);
  assert.equal(missingTag, null);
  assert.deepEqual(calls[0], {
    path: "/contacts/7/contactTags",
    parameters: { offset: "1", limit: "1" },
  });
});

test("ActiveCampaign preserves retry hints and does not call RPC after deadline", async () => {
  let calls = 0;
  const provider = createActiveCampaignRpcProvider({
    clock,
    rpcRead: async () => {
      calls += 1;
      return { status: 503, retryAfter: "12", body: { message: "private" } };
    },
  });

  await assert.rejects(
    provider.read.getTag({ tagId: "10", deadlineAtMs }),
    (error) => {
      assert.ok(error instanceof ProviderReadError);
      assert.equal(error.status, 503);
      assert.equal(error.retryable, true);
      assert.equal(error.retryAfter, "12");
      assert.equal(error.message.includes("private"), false);
      return true;
    },
  );
  await assert.rejects(
    provider.read.getTag({ tagId: "10", deadlineAtMs: clock.now() }),
    (error) => error.code === "deadline_exceeded",
  );
  assert.equal(calls, 1);
});

test("ActiveCampaign treats a malformed success as an error, not a deletion", async () => {
  const provider = createActiveCampaignRpcProvider({
    clock,
    rpcRead: async () => ok({}),
  });

  await assert.rejects(
    provider.read.getContact({ contactId: "7", deadlineAtMs }),
    /response\.contact must be an object/,
  );
});

function ok(body) {
  return { status: 200, retryAfter: null, body };
}
