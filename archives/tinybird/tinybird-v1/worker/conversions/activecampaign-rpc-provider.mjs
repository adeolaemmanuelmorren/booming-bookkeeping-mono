import {
  providerBodyOrThrow,
  readProviderRpc,
  requireArray,
  requireLimit,
  requireRecord,
  setParameter,
} from "./provider-rpc.mjs";

export function createActiveCampaignRpcProvider({
  rpcRead,
  clock = Date,
  maxCallMs = 20_000,
}) {
  if (typeof rpcRead !== "function") {
    throw new TypeError("rpcRead must be a function");
  }

  async function read(path, parameters, deadlineAtMs, options = {}) {
    const response = await readProviderRpc({
      provider: "ActiveCampaign",
      deadlineAtMs,
      clock,
      maxCallMs,
      invoke: () => rpcRead(path, parameters),
    });

    return providerBodyOrThrow(response, "ActiveCampaign", options);
  }

  return {
    read: {
      async listContacts(request) {
        requireRecord(request, "listContacts request");
        const parameters = {};

        setParameter(
          parameters,
          "filters[updated_after]",
          optionalTimestamp(request.updatedAfter, "updatedAfter"),
        );
        setParameter(
          parameters,
          "filters[updated_before]",
          optionalTimestamp(request.updatedBefore, "updatedBefore"),
        );
        setParameter(parameters, "id_greater", optionalNumericId(request.idGreater));
        setParameter(parameters, "orders[id]", requireOrder(request.orderById));
        setParameter(parameters, "limit", requireLimit(request.limit));

        const body = await read(
          "/contacts",
          parameters,
          request.deadlineAtMs,
        );
        const items = collection(body, "contacts");

        return {
          items,
          hasMore: items.length === request.limit,
        };
      },

      async getContact(request) {
        requireRecord(request, "getContact request");
        const contactId = requireNumericId(request.contactId, "contactId");
        const body = await read(
          `/contacts/${contactId}`,
          {},
          request.deadlineAtMs,
          { allowNotFound: true },
        );

        if (!body) {
          return null;
        }

        const response = requireRecord(body, "ActiveCampaign contact response");
        return requireRecord(response.contact, "ActiveCampaign contact response.contact");
      },

      async listContactTags(request) {
        requireRecord(request, "listContactTags request");
        const contactId = requireNumericId(request.contactId, "contactId");
        const offset = requireOffset(request.offset);
        const limit = requireLimit(request.limit);
        const body = await read(
          `/contacts/${contactId}/contactTags`,
          { offset: String(offset), limit: String(limit) },
          request.deadlineAtMs,
        );
        const items = collection(body, "contactTags");

        return offsetPage(body, items, offset, limit);
      },

      async getTag(request) {
        requireRecord(request, "getTag request");
        const tagId = requireNumericId(request.tagId, "tagId");
        const body = await read(
          `/tags/${tagId}`,
          {},
          request.deadlineAtMs,
          { allowNotFound: true },
        );

        if (!body) {
          return null;
        }

        const response = requireRecord(body, "ActiveCampaign tag response");
        return requireRecord(response.tag, "ActiveCampaign tag response.tag");
      },

      async listTags(request) {
        requireRecord(request, "listTags request");
        const offset = requireOffset(request.offset);
        const limit = requireLimit(request.limit);
        const body = await read(
          "/tags",
          { offset: String(offset), limit: String(limit) },
          request.deadlineAtMs,
        );
        const items = collection(body, "tags");

        return offsetPage(body, items, offset, limit);
      },
    },
  };
}

function collection(value, name) {
  const body = requireRecord(value, `ActiveCampaign ${name} response`);
  return requireArray(body[name], `ActiveCampaign ${name} response.${name}`);
}

function offsetPage(body, items, offset, limit) {
  const total = parseTotal(body.meta?.total);
  const hasMore = total === null
    ? items.length === limit
    : offset + items.length < total;

  return { items, hasMore };
}

function parseTotal(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const total = Number(value);

  if (!Number.isSafeInteger(total) || total < 0) {
    throw new TypeError("ActiveCampaign meta.total must be a non-negative integer");
  }

  return total;
}

function optionalTimestamp(value, fieldName) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const timestamp = new Date(value);

  if (Number.isNaN(timestamp.getTime())) {
    throw new TypeError(`${fieldName} must be a valid timestamp`);
  }

  return timestamp.toISOString();
}

function optionalNumericId(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  return requireNumericId(value, "idGreater");
}

function requireNumericId(value, fieldName) {
  const id = String(value ?? "").trim();

  if (!/^\d+$/.test(id)) {
    throw new TypeError(`${fieldName} must be a numeric ActiveCampaign ID`);
  }

  return id;
}

function requireOffset(value) {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError("offset must be a non-negative integer");
  }

  return value;
}

function requireOrder(value) {
  if (value === "ASC" || value === "asc") {
    return "ASC";
  }

  if (value === "DESC" || value === "desc") {
    return "desc";
  }

  throw new TypeError("orderById must be ASC or desc");
}
