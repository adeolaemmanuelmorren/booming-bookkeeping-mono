import { completedWindow, publish, complete } from "./warehouse.js";
import { read, hydrate, eventReference } from "./stripe.js";
import { windowBounds } from "./window.js";

const ACCOUNTS = ["stripe", "stripe_kajabi"];
const RETRIES = { retries: { limit: 5, delay: "10 seconds", backoff: "exponential" }, timeout: "10 minutes" };

export async function runBackup(env, event, step) {
  const { account } = event.payload;
  if (!ACCOUNTS.includes(account)) throw new Error("Unknown Stripe account");
  const bounds = await step.do("read completed window", RETRIES, async () => {
    const windowEnd = await completedWindow(env, account);
    return windowBounds(env.START_AT, windowEnd, Math.floor(Date.now() / 1000));
  });
  if (bounds.end <= bounds.start) return { account, status: "caught_up" };
  let batches = 0;
  let recordCount = 0;
  // Rebuilt from persisted discovery pages when a Workflow resumes. A charge
  // already reread in this window also covers its earlier change events.
  const hydratedIds = new Set();
  for (const kind of ["charges", "events"]) {
    let cursor;
    for (let pageNumber = 0; ; pageNumber++) {
      const page = await step.do(`${kind} page ${pageNumber}`, RETRIES, async () => {
        const value = await read(env.STRIPE_SOURCE, account, `/${kind}`, {
          limit: "100", "created[gte]": String(bounds.start), "created[lt]": String(bounds.end),
          ...(kind === "events" ? {
            "types[0]": "charge.succeeded", "types[1]": "charge.updated",
            "types[2]": "charge.captured", "types[3]": "charge.refunded",
          } : {}),
          ...(cursor ? { starting_after: cursor } : {}),
        });
        if (!Array.isArray(value.data)) throw new Error("Stripe returned an invalid page");
        const references = kind === "charges"
          ? value.data.map(charge => ({ stream: "charges", id: charge.id }))
          : value.data.map(eventReference).filter(Boolean);
        const next = value.has_more ? value.data.at(-1)?.id : null;
        if (value.has_more && (!next || next === cursor)) throw new Error("Stripe pagination made no progress");
        return { references, next };
      });
      const references = [...new Map(page.references.map(ref => [ref.id, ref])).values()]
        .filter(ref => !hydratedIds.has(ref.id));
      for (let offset = 0; offset < references.length; offset += 5) {
        const batch = references.slice(offset, offset + 5);
        const count = await step.do(`${kind} ${pageNumber} hydrate ${offset}`, RETRIES, async () => {
          const observedAt = new Date().toISOString();
          const records = await hydrate(env.STRIPE_SOURCE, account, batch);
          await publish(env, account, `${event.instanceId}:${kind}:${pageNumber}:${offset}`, observedAt, records);
          return records.length;
        });
        batches++;
        recordCount += count;
        for (const ref of batch) hydratedIds.add(ref.id);
      }
      if (!page.next) break;
      cursor = page.next;
      if (pageNumber >= 100) throw new Error("Source window exceeds page limit; cursor has not advanced");
    }
  }
  await step.do("complete window", RETRIES, () => complete(env, account, bounds, event.instanceId));
  return { account, ...bounds, batches, recordCount };
}
