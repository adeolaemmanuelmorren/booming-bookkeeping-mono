const ORIGIN = "https://boomingbookkeeping.api-us1.com";

export async function read(env, path, parameters = {}) {
  if (!["/contacts", "/tags"].includes(path)) throw new Error("Unsupported AC source path");
  if (!env.ACTIVE_CAMPAIGN_TOKEN) throw new Error("AC source token is missing");
  const url = new URL(`/api/3${path}`, ORIGIN);
  url.search = new URLSearchParams(parameters).toString();
  const response = await fetch(url, {
    method: "GET",
    headers: { "Api-Token": env.ACTIVE_CAMPAIGN_TOKEN, Accept: "application/json" },
    redirect: "manual",
    signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) throw new Error(`AC ${path} returned ${response.status}`);
  return response.json();
}

export function numericId(value) {
  const id = String(value ?? "");
  if (!/^\d+$/.test(id) || !Number.isSafeInteger(Number(id))) throw new Error("Invalid AC ID");
  return id;
}

export function contactRecords(page, previousId, bounds) {
  if (!Array.isArray(page.contacts)) throw new Error("AC returned an invalid contacts page");
  if (!page.contacts.length) return [];
  if (!Array.isArray(page.contactTags)) {
    throw new Error("AC did not return contacts with their complete tag collection");
  }
  const tags = new Map(page.contactTags.map(tag => [numericId(tag.id), tag]));
  let lastId = Number(previousId || 0);
  return page.contacts.map(contact => {
    const id = numericId(contact.id);
    if (Number(id) <= lastId) throw new Error("AC contact pagination did not advance in ID order");
    lastId = Number(id);
    const updatedAt = Date.parse(contact.udate) / 1000;
    if (!Number.isFinite(updatedAt) || updatedAt < bounds.start || updatedAt > bounds.end) {
      throw new Error("AC returned a contact outside the requested update window");
    }
    if (!Array.isArray(contact.contactTags)) throw new Error("AC contact is missing tag references");
    const contactTags = [...new Set(contact.contactTags.map(numericId))].map(tagId => {
      const tag = tags.get(tagId);
      if (!tag || String(tag.contact) !== id) throw new Error("AC included tag collection is incomplete");
      return { id: tagId, contact: id, tag: numericId(tag.tag), cdate: tag.cdate };
    });
    return {
      stream: "contacts", id,
      payload: {
        id, email: contact.email, phone: contact.phone,
        firstName: contact.firstName, lastName: contact.lastName,
        created_utc_timestamp: contact.created_utc_timestamp,
        udate: contact.udate, deleted: contact.deleted,
        contactTags,
      },
    };
  });
}

export function tagRecords(page) {
  if (!Array.isArray(page.tags)) throw new Error("AC returned an invalid tag catalog page");
  return page.tags.map(tag => ({
    stream: "tags", id: numericId(tag.id),
    payload: { id: numericId(tag.id), tag: tag.tag, cdate: tag.cdate, deleted: tag.deleted },
  }));
}
