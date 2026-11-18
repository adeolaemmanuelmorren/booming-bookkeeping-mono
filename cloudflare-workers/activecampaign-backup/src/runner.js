import { completedWindow, publish, complete } from "./warehouse.js";
import { read, contactRecords, tagRecords } from "./activecampaign.js";

const ACCOUNT = "activecampaign";
const RETRIES = { retries: { limit: 5, delay: "10 seconds", backoff: "exponential" }, timeout: "10 minutes" };

export function windowBounds(startAt, completedUntil, now) {
  const initial = Math.floor(Date.parse(startAt) / 1000);
  const cursor = completedUntil ? Math.floor(Date.parse(completedUntil) / 1000) : initial;
  if (!Number.isFinite(initial) || !Number.isFinite(cursor)) throw new Error("Invalid AC cursor timestamp");
  return { start: Math.max(initial, cursor - 600), end: Math.min(now - 60, cursor + 6 * 3600) };
}

export async function runBackup(env, event, step) {
  const bounds = await step.do("read completed window", RETRIES, async () => {
    const cursor = await completedWindow(env, ACCOUNT);
    return windowBounds(env.START_AT, cursor, Math.floor(Date.now() / 1000));
  });
  if (bounds.end <= bounds.start) return { status: "caught_up" };

  let contactCount = 0;
  let cursor = "0";
  for (let pageNumber = 0; ; pageNumber++) {
    const page = await step.do(`contacts ${pageNumber}`, RETRIES, async () => {
      const observedAt = new Date().toISOString();
      const result = await read(env, "/contacts", {
        limit: "100", include: "contactTags", "orders[id]": "ASC", id_greater: cursor,
        "filters[updated_after]": new Date(bounds.start * 1000).toISOString(),
        "filters[updated_before]": new Date(bounds.end * 1000).toISOString(),
      });
      const records = contactRecords(result, cursor, bounds);
      await publish(env, ACCOUNT, event.instanceId, `${event.instanceId}:contacts:${pageNumber}`, observedAt, records);
      return { count: records.length, next: records.at(-1)?.id };
    });
    contactCount += page.count;
    if (page.count < 100) break;
    cursor = page.next;
    if (pageNumber >= 999) throw new Error("AC update window exceeds page limit; cursor has not advanced");
  }

  const catalogObservedAt = await step.do("start tag catalog", () => new Date().toISOString());
  const tagIds = new Set();
  let expectedTags;
  for (let pageNumber = 0; ; pageNumber++) {
    const page = await step.do(`tags ${pageNumber}`, RETRIES, async () => {
      const observedAt = new Date().toISOString();
      const result = await read(env, "/tags", { limit: "100", offset: String(pageNumber * 100) });
      const records = tagRecords(result);
      const total = Number(result.meta?.total);
      if (!Number.isSafeInteger(total) || total < 0) throw new Error("AC tag catalog total is missing");
      await publish(env, ACCOUNT, event.instanceId, `${event.instanceId}:tags:${pageNumber}`, observedAt, records);
      return { ids: records.map(record => record.id), total };
    });
    expectedTags ??= page.total;
    if (expectedTags !== page.total) throw new Error("AC tag catalog changed during pagination; retry in a new workflow");
    for (const id of page.ids) {
      if (tagIds.has(id)) throw new Error("AC tag catalog pagination repeated an ID");
      tagIds.add(id);
    }
    if (page.ids.length < 100) break;
    if (pageNumber >= 99) throw new Error("AC tag catalog exceeds page limit");
  }
  if (tagIds.size !== expectedTags) throw new Error("AC tag catalog is incomplete");

  // Dataform sees this workflow's records only after this final acknowledgement.
  await step.do("complete window", RETRIES, () => complete(
    env, ACCOUNT, bounds, event.instanceId, catalogObservedAt, tagIds.size, contactCount,
  ));
  return { ...bounds, contactCount, tagCount: tagIds.size };
}
