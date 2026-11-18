export function objectId(value) {
  return typeof value === "string" ? value : value?.id ?? null;
}

export async function read(source, account, path, parameters = {}) {
  const result = await source.read(account, path, parameters);
  if (result.status !== 200) throw new Error(`Stripe ${account} ${path.split("/")[1]} returned ${result.status}`);
  return result.body;
}

export async function list(source, account, path, parameters = {}) {
  const rows = [];
  let cursor;
  do {
    const page = await read(source, account, path, { ...parameters, limit: "100", ...(cursor ? { starting_after: cursor } : {}) });
    if (!Array.isArray(page.data)) throw new Error("Stripe returned an invalid list");
    rows.push(...page.data);
    if (!page.has_more) return rows;
    const next = page.data.at(-1)?.id;
    if (!next || next === cursor) throw new Error("Stripe pagination made no progress");
    cursor = next;
    if (rows.length > 10_000) throw new Error("Stripe child collection exceeds the batch limit");
  } while (true);
}

// Event payloads have their original API version. Use only references, then read
// current objects through the gateway's pinned API version.
export function eventReference(event) {
  if (!["charge.succeeded", "charge.updated", "charge.captured", "charge.refunded"].includes(event.type)) return null;
  const object = event.data?.object;
  if (!object?.id) return null;
  if (object.object === "charge") return { stream: "charges", id: object.id };

  return null;
}

export async function hydrate(source, account, references) {
  const records = new Map();
  const loaded = new Map();
  const pendingCharges = new Set();
  function save(stream, object, extra = {}) {
    if (!object?.id) throw new Error(`Stripe ${stream} record has no ID`);
    const { client_secret, ...payload } = object;
    records.set(`${stream}:${object.id}`, { stream, id: object.id, payload: { ...payload, ...extra } });
  }
  async function get(stream, id, parameters = {}) {
    if (!id) return null;
    const key = `${stream}:${id}`;
    if (loaded.has(key)) return loaded.get(key);
    const path = stream === "checkout_sessions" ? "/checkout/sessions" : `/${stream}`;
    const object = await read(source, account, `${path}/${id}`, parameters);
    loaded.set(key, object);
    save(stream, object);
    return object;
  }
  function discount(object) {
    if (object?.coupon?.id) save("coupons", object.coupon);
    if (object?.promotion_code?.id) save("promotion_codes", object.promotion_code);
  }
  async function product(value) {
    const id = objectId(value);
    if (!id) return;
    await get("products", id);
  }
  async function lineObjects(line) {
    if (line.plan?.id) {
      save("plans", line.plan);
      await product(line.plan.product);
    }
    if (line.price?.id) {
      save("prices", line.price);
      await product(line.price.product);
    }
  }
  async function subscription(id) {
    if (!id || loaded.has(`subscriptions:${id}`)) return;
    const value = await get("subscriptions", id);
    if (value.items?.has_more) throw new Error("Subscription items are truncated; refusing incomplete publication");
    for (const item of value.items?.data ?? []) await lineObjects(item);
    discount(value.discount);
  }
  async function customer(id) {
    const value = await get("customers", id);
    discount(value?.discount);
  }
  async function invoice(id) {
    if (!id || loaded.has(`invoices:${id}`)) return loaded.get(`invoices:${id}`);
    const value = await get("invoices", id);
    const lines = await list(source, account, `/invoices/${id}/lines`);
    for (const line of lines) {
      save("invoice_line_items", line, { invoice_id: id, invoice_created: value.created });
      await lineObjects(line);
    }
    await customer(objectId(value.customer));
    await subscription(objectId(value.subscription));
    discount(value.discount);
    return value;
  }
  async function checkout(value) {
    if (records.has(`checkout_sessions:${value.id}`)) return;
    save("checkout_sessions", value);
    const lines = await list(source, account, `/checkout/sessions/${value.id}/line_items`);
    for (const line of lines) {
      save("checkout_sessions_line_items", line, { checkout_session_id: value.id });
      await lineObjects(line);
    }
    await customer(objectId(value.customer));
    await subscription(objectId(value.subscription));
  }
  for (const ref of references) {
    if (ref.stream !== "charges") throw new Error("Expected a charge reference");
    pendingCharges.add(ref.id);
  }
  for (const id of pendingCharges) {
    const charge = await get("charges", id);
    if (!charge.paid || charge.status !== "succeeded") continue;
    await customer(objectId(charge.customer));
    await get("payment_intents", objectId(charge.payment_intent));
    await invoice(objectId(charge.invoice));
    if (!charge.invoice && objectId(charge.payment_intent)) {
      const sessions = await list(source, account, "/checkout/sessions", { payment_intent: objectId(charge.payment_intent) });
      for (const session of sessions) await checkout(session);
    }
  }
  return [...records.values()];
}
