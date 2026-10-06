import { extractId } from "./shared.mjs";
import {
  providerBodyOrThrow,
  readProviderRpc,
  requireArray,
  requireLimit,
  requireRecord,
  setParameter,
} from "./provider-rpc.mjs";

const GATEWAY_ACCOUNTS = Object.freeze({
  main: "stripe",
  kajabi: "stripe_kajabi",
});

const BUNDLE_CURSOR_VERSION = 1;

export function createStripeRpcProvider({
  rpcRead,
  clock = Date,
  maxCallMs = 20_000,
  extendedObjectPaths = false,
}) {
  if (typeof rpcRead !== "function") {
    throw new TypeError("rpcRead must be a function");
  }

  async function read(account, path, parameters, deadlineAtMs, options = {}) {
    const gatewayAccount = requireAccount(account);
    const response = await readProviderRpc({
      provider: "Stripe",
      deadlineAtMs,
      clock,
      maxCallMs,
      invoke: () => rpcRead(gatewayAccount, path, parameters),
    });

    return providerBodyOrThrow(response, "Stripe", options);
  }

  return {
    read: {
      async listEvents(request) {
        const parameters = stripeWindowParameters(request);
        const types = requireArray(request?.types, "types");

        if (types.length > 20) {
          throw new TypeError("Stripe accepts at most 20 event types");
        }

        types.forEach((type, index) => {
          parameters[`types[${index}]`] = requireString(type, `types[${index}]`);
        });

        const body = await read(
          request.account,
          "/events",
          parameters,
          request.deadlineAtMs,
        );

        return stripeListPage(body, "events");
      },

      async listCharges(request) {
        const parameters = stripeWindowParameters(request);
        const body = await read(
          request.account,
          "/charges",
          parameters,
          request.deadlineAtMs,
        );

        return stripeListPage(body, "charges");
      },

      async readChargeBundlePage(request) {
        requireRecord(request, "readChargeBundlePage request");
        const chargeId = requireStripeId(request.chargeId, "chargeId");

        if (!request.cursor) {
          return readInitialChargeBundlePage({
            account: request.account,
            chargeId,
            deadlineAtMs: request.deadlineAtMs,
            extendedObjectPaths,
            clock,
            read,
          });
        }

        return readNextChargeBundlePage({
          account: request.account,
          cursor: request.cursor,
          deadlineAtMs: request.deadlineAtMs,
          extendedObjectPaths,
          clock,
          read,
        });
      },
    },
  };
}

async function readInitialChargeBundlePage({
  account,
  chargeId,
  deadlineAtMs,
  extendedObjectPaths,
  clock,
  read,
}) {
  const parameters = chargeExpandParameters();
  const body = await read(
    account,
    `/charges/${chargeId}`,
    parameters,
    deadlineAtMs,
  );
  const charge = requireRecord(body, "Stripe charge");
  const relatedRecords = emptyRelatedRecords();
  const state = newBundleState(chargeId);

  addTask(state, {
    kind: "listRefunds",
    chargeId,
    startingAfter: null,
  });
  collectEmbeddedRefunds(charge, relatedRecords);
  collectChargeReferences({
    charge,
    relatedRecords,
    state,
    extendedObjectPaths,
  });

  return bundlePage({
    canonicalCharge: charge,
    relatedRecords,
    state,
    observedAt: nowIso(clock),
  });
}

async function readNextChargeBundlePage({
  account,
  cursor,
  deadlineAtMs,
  extendedObjectPaths,
  clock,
  read,
}) {
  const state = parseBundleState(cursor);
  const task = state.pendingTasks.shift();

  if (!task) {
    throw new TypeError("Stripe bundle cursor has no pending task");
  }

  const result = await executeBundleTask({
    account,
    task,
    deadlineAtMs,
    extendedObjectPaths,
    read,
  });

  if (result.nextStartingAfter) {
    state.pendingTasks.unshift({
      ...task,
      startingAfter: result.nextStartingAfter,
    });
  }

  for (const discoveredTask of result.discoveredTasks) {
    addTask(state, discoveredTask);
  }

  if (
    !result.nextStartingAfter &&
    state.pendingTasks.length === 0 &&
    task.kind !== "refreshCharge"
  ) {
    queueFinalChargeRefresh(state);
  }

  return bundlePage({
    canonicalCharge: result.canonicalCharge,
    relatedRecords: result.relatedRecords,
    state,
    observedAt: nowIso(clock),
  });
}

async function executeBundleTask({
  account,
  task,
  deadlineAtMs,
  extendedObjectPaths,
  read,
}) {
  if (task.kind === "refreshCharge") {
    const chargeId = requireStripeId(task.chargeId, "chargeId");
    const body = await read(
      account,
      `/charges/${chargeId}`,
      chargeExpandParameters(),
      deadlineAtMs,
    );
    const charge = requireRecord(body, "Stripe charge refresh");
    const relatedRecords = emptyRelatedRecords();
    const discoveredState = newBundleState(chargeId);

    collectEmbeddedRefunds(charge, relatedRecords);
    collectChargeReferences({
      charge,
      relatedRecords,
      state: discoveredState,
      extendedObjectPaths,
    });

    return {
      ...taskResult(relatedRecords, discoveredState.pendingTasks),
      canonicalCharge: charge,
    };
  }

  if (task.kind === "listRefunds") {
    return readListTask({
      account,
      path: "/refunds",
      parameters: listParameters({
        startingAfter: task.startingAfter,
        charge: task.chargeId,
      }),
      collection: "refunds",
      deadlineAtMs,
      read,
      discover: (refunds, records, tasks) => {
        for (const refund of refunds) {
          collectExtendedReference({
            value: refund.balance_transaction,
            type: "balance_transaction",
            collection: "balanceTransactions",
            taskKind: "getBalanceTransaction",
            records,
            tasks,
            extendedObjectPaths,
          });
        }
      },
    });
  }

  if (task.kind === "getCustomer") {
    return readObjectTask({
      account,
      path: `/customers/${requireStripeId(task.id, "customer ID")}`,
      collection: "customers",
      referenceType: "customer",
      referenceId: task.id,
      deadlineAtMs,
      read,
    });
  }

  if (task.kind === "getPaymentIntent") {
    return readObjectTask({
      account,
      path: `/payment_intents/${requireStripeId(task.id, "PaymentIntent ID")}`,
      collection: "paymentIntents",
      referenceType: "payment_intent",
      referenceId: task.id,
      deadlineAtMs,
      read,
      discover: (paymentIntent, records, tasks) => {
        collectPaymentIntentReferences({
          paymentIntent,
          records,
          tasks,
          extendedObjectPaths,
        });
      },
    });
  }

  if (task.kind === "getInvoice") {
    return readObjectTask({
      account,
      path: `/invoices/${requireStripeId(task.id, "invoice ID")}`,
      parameters: { "expand[0]": "discounts.promotion_code" },
      collection: "invoices",
      referenceType: "invoice",
      referenceId: task.id,
      deadlineAtMs,
      read,
      discover: (invoice, records, tasks) => {
        collectInvoiceReferences({ invoice, records, tasks });
      },
    });
  }

  if (task.kind === "listInvoiceLines") {
    const invoiceId = requireStripeId(task.invoiceId, "invoice ID");
    return readLineItemTask({
      account,
      path: `/invoices/${invoiceId}/lines`,
      collection: "invoiceLines",
      parent: { invoice_id: invoiceId },
      startingAfter: task.startingAfter,
      deadlineAtMs,
      read,
    });
  }

  if (task.kind === "getSubscription") {
    return readObjectTask({
      account,
      path: `/subscriptions/${requireStripeId(task.id, "subscription ID")}`,
      parameters: { "expand[0]": "items.data.price.product", "expand[1]": "discounts.promotion_code" },
      collection: "subscriptions",
      referenceType: "subscription",
      referenceId: task.id,
      deadlineAtMs,
      read,
      discover: (subscription, records, tasks) => {
        collectSubscriptionItems(subscription, records, tasks);
      },
    });
  }

  if (task.kind === "listCheckoutSessions") {
    const filterName = requireCheckoutFilter(task.filterName);
    const parameters = listParameters({ startingAfter: task.startingAfter });
    parameters[filterName] = requireStripeId(task.filterId, "checkout filter ID");

    return readListTask({
      account,
      path: "/checkout/sessions",
      parameters,
      collection: "checkoutSessions",
      deadlineAtMs,
      read,
      discover: (sessions, records, tasks) => {
        collectCheckoutSessionReferences(sessions, records, tasks);
      },
    });
  }

  if (task.kind === "listCheckoutSessionLineItems") {
    const sessionId = requireStripeId(task.sessionId, "Checkout Session ID");
    return readLineItemTask({
      account,
      path: `/checkout/sessions/${sessionId}/line_items`,
      collection: "checkoutSessionLineItems",
      parent: { checkout_session_id: sessionId },
      startingAfter: task.startingAfter,
      deadlineAtMs,
      read,
    });
  }

  if (task.kind === "getCheckoutDiscounts") {
    return readObjectTask({ account, path: `/checkout/sessions/${requireStripeId(task.id, "Checkout Session ID")}`,
      parameters: { "expand[0]": "total_details.breakdown", "expand[1]": "discounts.promotion_code" },
      collection: "checkoutSessions", referenceType: "checkout_session", referenceId: task.id, deadlineAtMs, read,
      discover: (session, records) => collectDiscounts(session, records, { checkout_session_id: session.id }) });
  }

  if (task.kind === "getPaymentLink") {
    return readObjectTask({
      account,
      path: `/payment_links/${requireStripeId(task.id, "Payment Link ID")}`,
      collection: "paymentLinks",
      referenceType: "payment_link",
      referenceId: task.id,
      deadlineAtMs,
      read,
      discover: (paymentLink, records, tasks) => {
        addTaskToList(tasks, {
          kind: "listPaymentLinkLineItems",
          paymentLinkId: paymentLink.id,
          startingAfter: null,
        });
      },
    });
  }

  if (task.kind === "listPaymentLinkLineItems") {
    const paymentLinkId = requireStripeId(task.paymentLinkId, "Payment Link ID");
    return readLineItemTask({
      account,
      path: `/payment_links/${paymentLinkId}/line_items`,
      collection: "paymentLinkLineItems",
      parent: { payment_link_id: paymentLinkId },
      startingAfter: task.startingAfter,
      deadlineAtMs,
      read,
    });
  }

  if (task.kind === "getPrice") {
    return readObjectTask({
      account,
      path: `/prices/${requireStripeId(task.id, "Price ID")}`,
      parameters: { "expand[0]": "product" },
      collection: "prices",
      referenceType: "price",
      referenceId: task.id,
      deadlineAtMs,
      read,
      discover: (price, records, tasks) => {
        collectProductReference(price.product, records, tasks);
      },
    });
  }

  if (task.kind === "getProduct") {
    return readObjectTask({
      account,
      path: `/products/${requireStripeId(task.id, "Product ID")}`,
      collection: "products",
      referenceType: "product",
      referenceId: task.id,
      deadlineAtMs,
      read,
    });
  }

  if (task.kind === "getBalanceTransaction") {
    requireExtendedPaths(extendedObjectPaths, task.kind);
    return readObjectTask({
      account,
      path: `/balance_transactions/${requireStripeId(task.id, "BalanceTransaction ID")}`,
      collection: "balanceTransactions",
      referenceType: "balance_transaction",
      referenceId: task.id,
      deadlineAtMs,
      read,
    });
  }

  if (task.kind === "getPaymentMethod") {
    requireExtendedPaths(extendedObjectPaths, task.kind);
    return readObjectTask({
      account,
      path: `/payment_methods/${requireStripeId(task.id, "PaymentMethod ID")}`,
      collection: "paymentMethods",
      referenceType: "payment_method",
      referenceId: task.id,
      deadlineAtMs,
      read,
    });
  }

  throw new TypeError(`unknown Stripe bundle task: ${String(task.kind)}`);
}

async function readObjectTask({
  account,
  path,
  parameters = {},
  collection,
  referenceType,
  referenceId,
  deadlineAtMs,
  read,
  discover = () => {},
}) {
  const body = await read(account, path, parameters, deadlineAtMs, {
    allowNotFound: true,
  });
  const relatedRecords = emptyRelatedRecords();
  const discoveredTasks = [];

  if (!body) {
    addUnresolvedReference(
      relatedRecords,
      referenceType,
      requireStripeId(referenceId, `${referenceType} ID`),
      "provider_returned_404",
    );
    return taskResult(relatedRecords, discoveredTasks);
  }

  const record = requireRecord(body, `Stripe ${referenceType}`);
  addRecord(relatedRecords, collection, record);
  discover(record, relatedRecords, discoveredTasks);

  return taskResult(relatedRecords, discoveredTasks);
}

async function readListTask({
  account,
  path,
  parameters,
  collection,
  deadlineAtMs,
  read,
  discover = () => {},
}) {
  const body = await read(account, path, parameters, deadlineAtMs);
  const page = stripeListPage(body, collection);
  const relatedRecords = emptyRelatedRecords();
  const discoveredTasks = [];

  addRecords(relatedRecords, collection, page.items);
  discover(page.items, relatedRecords, discoveredTasks);

  return taskResult(
    relatedRecords,
    discoveredTasks,
    page.hasMore ? page.nextCursor : null,
  );
}

async function readLineItemTask(options) {
  const result = await readListTask({
    ...options,
    parameters: {
      ...listParameters({ startingAfter: options.startingAfter }),
      "expand[0]": "data.price.product",
    },
    discover: (lineItems, records, tasks) => {
      collectLineItemReferences(lineItems, records, tasks);
    },
  });
  if (options.parent) {
    result.relatedRecords[options.collection] = (result.relatedRecords[options.collection] ?? [])
      .map(record => ({ ...record, ...options.parent }));
  }
  return result;
}

function collectChargeReferences({
  charge,
  relatedRecords,
  state,
  extendedObjectPaths,
}) {
  const tasks = [];

  collectReference({
    value: charge.customer,
    collection: "customers",
    taskKind: "getCustomer",
    records: relatedRecords,
    tasks,
  });
  collectReference({
    value: charge.payment_intent,
    collection: "paymentIntents",
    taskKind: "getPaymentIntent",
    records: relatedRecords,
    tasks,
    onObject: (paymentIntent) => {
      collectPaymentIntentReferences({
        paymentIntent,
        records: relatedRecords,
        tasks,
        extendedObjectPaths,
      });
    },
  });
  collectReference({
    value: charge.invoice,
    collection: "invoices",
    taskKind: "getInvoice",
    records: relatedRecords,
    tasks,
    onObject: (invoice) => {
      collectInvoiceReferences({ invoice, records: relatedRecords, tasks });
    },
  });
  collectExtendedReference({
    value: charge.balance_transaction,
    type: "balance_transaction",
    collection: "balanceTransactions",
    taskKind: "getBalanceTransaction",
    records: relatedRecords,
    tasks,
    extendedObjectPaths,
  });
  collectExtendedReference({
    value: charge.payment_method,
    type: "payment_method",
    collection: "paymentMethods",
    taskKind: "getPaymentMethod",
    records: relatedRecords,
    tasks,
    extendedObjectPaths,
  });

  const paymentIntentId = extractId(charge.payment_intent);
  if (paymentIntentId) {
    addTaskToList(tasks, {
      kind: "listCheckoutSessions",
      filterName: "payment_intent",
      filterId: paymentIntentId,
      startingAfter: null,
    });
  }

  for (const task of tasks) {
    addTask(state, task);
  }
}

function collectPaymentIntentReferences({
  paymentIntent,
  records,
  tasks,
  extendedObjectPaths,
}) {
  collectReference({
    value: paymentIntent.customer,
    collection: "customers",
    taskKind: "getCustomer",
    records,
    tasks,
  });
  collectExtendedReference({
    value: paymentIntent.payment_method,
    type: "payment_method",
    collection: "paymentMethods",
    taskKind: "getPaymentMethod",
    records,
    tasks,
    extendedObjectPaths,
  });
}

function collectInvoiceReferences({ invoice, records, tasks }) {
  const invoiceId = requireStripeId(invoice.id, "invoice.id");
  collectDiscounts(invoice, records, { invoice_id: invoiceId });

  addTaskToList(tasks, {
    kind: "listInvoiceLines",
    invoiceId,
    startingAfter: null,
  });
  collectReference({
    value: invoice.subscription,
    collection: "subscriptions",
    taskKind: "getSubscription",
    records,
    tasks,
    onObject: (subscription) => {
      collectSubscriptionItems(subscription, records, tasks);
    },
  });

  const subscriptionId = extractId(invoice.subscription);
  if (subscriptionId) {
    addTaskToList(tasks, {
      kind: "listCheckoutSessions",
      filterName: "subscription",
      filterId: subscriptionId,
      startingAfter: null,
    });
  }
}

function collectCheckoutSessionReferences(sessions, records, tasks) {
  for (const session of sessions) {
    const sessionId = requireStripeId(session.id, "Checkout Session ID");
    if (session.total_details?.amount_discount > 0) {
      addTaskToList(tasks, { kind: "getCheckoutDiscounts", id: sessionId });
    }
    addTaskToList(tasks, {
      kind: "listCheckoutSessionLineItems",
      sessionId,
      startingAfter: null,
    });
    collectReference({
      value: session.payment_link,
      collection: "paymentLinks",
      taskKind: "getPaymentLink",
      records,
      tasks,
      onObject: (paymentLink) => {
        addTaskToList(tasks, {
          kind: "listPaymentLinkLineItems",
          paymentLinkId: paymentLink.id,
          startingAfter: null,
        });
      },
    });
  }
}

function collectSubscriptionItems(subscription, records, tasks) {
  collectDiscounts(subscription, records, { subscription_id: subscription.id });
  const items = subscription.items?.data;

  if (!Array.isArray(items)) {
    return;
  }

  collectLineItemReferences(items, records, tasks);
}

function collectLineItemReferences(lineItems, records, tasks) {
  for (const lineItem of lineItems) {
    if (lineItem.plan && typeof lineItem.plan === "object") {
      addRecord(records, "plans", lineItem.plan);
      collectProductReference(lineItem.plan.product, records, tasks);
    }

    collectPriceReference(lineItem.price, records, tasks);

    const priceDetails = lineItem.pricing?.price_details;
    if (priceDetails) {
      collectPriceReference(priceDetails.price, records, tasks);
      collectProductReference(priceDetails.product, records, tasks);
    }
  }
}

function collectPriceReference(value, records, tasks) {
  collectReference({
    value,
    collection: "prices",
    taskKind: "getPrice",
    records,
    tasks,
    onObject: (price) => collectProductReference(price.product, records, tasks),
  });
}

function collectProductReference(value, records, tasks) {
  collectReference({
    value,
    collection: "products",
    taskKind: "getProduct",
    records,
    tasks,
  });
}

function collectExtendedReference({
  value,
  type,
  collection,
  taskKind,
  records,
  tasks,
  extendedObjectPaths,
}) {
  if (!value) {
    return;
  }

  if (typeof value === "object") {
    addRecord(records, collection, value);
    return;
  }

  const id = requireStripeId(value, `${type} ID`);

  if (extendedObjectPaths) {
    addTaskToList(tasks, { kind: taskKind, id });
    return;
  }

  addUnresolvedReference(records, type, id, "gateway_path_not_allowlisted");
}

function collectReference({
  value,
  collection,
  taskKind,
  records,
  tasks,
  onObject = () => {},
}) {
  if (!value) {
    return;
  }

  if (typeof value === "object") {
    const record = requireRecord(value, collection);
    addRecord(records, collection, record);
    onObject(record);
    return;
  }

  addTaskToList(tasks, {
    kind: taskKind,
    id: requireStripeId(value, `${collection} ID`),
  });
}

function collectEmbeddedRefunds(charge, records) {
  if (!Array.isArray(charge.refunds?.data)) {
    return;
  }

  addRecords(records, "refunds", charge.refunds.data);
}

function stripeWindowParameters(request) {
  requireRecord(request, "Stripe list request");
  const parameters = {};

  setParameter(parameters, "created[gte]", requireUnixSeconds(request.createdGte));
  setParameter(parameters, "created[lte]", requireUnixSeconds(request.createdLte));
  setParameter(parameters, "starting_after", request.startingAfter);
  setParameter(parameters, "limit", requireLimit(request.limit));

  return parameters;
}

function listParameters({ startingAfter, ...filters }) {
  const parameters = { limit: "100" };
  setParameter(parameters, "starting_after", startingAfter);

  for (const [name, value] of Object.entries(filters)) {
    setParameter(parameters, name, value);
  }

  return parameters;
}

function chargeExpandParameters() {
  return {
    "expand[0]": "customer",
    "expand[1]": "payment_intent",
    "expand[2]": "invoice",
    "expand[3]": "balance_transaction",
    // Charge.payment_method is an ID-only field. Expand through PaymentIntent.
    "expand[4]": "payment_intent.payment_method",
    "expand[5]": "invoice.discounts.promotion_code",
  };
}

function collectDiscounts(parent, records, parentFields) {
  const details = parent.total_details?.breakdown?.discounts ?? [];
  const amounts = parent.total_discount_amounts ?? [];
  const discounts = [...(parent.discounts ?? []), ...(parent.discount ? [parent.discount] : []),
    ...details.map(item => item.discount)];
  for (const discount of discounts) {
    if (!discount || typeof discount !== "object") continue;
    const promotion = discount.promotion_code;
    if (promotion && typeof promotion === "object") addRecord(records, "promotionCodes", promotion);
    const coupon = discount.coupon ?? promotion?.coupon;
    if (coupon && typeof coupon === "object") addRecord(records, "coupons", coupon);
    if (!discount.id) continue;
    const amount = details.find(item => extractId(item.discount) === discount.id)?.amount
      ?? amounts.find(item => extractId(item.discount) === discount.id)?.amount ?? 0;
    addRecord(records, "discounts", { ...discount, ...parentFields, amount,
      coupon_id: extractId(coupon), promotion_code: extractId(promotion) });
  }
}

function stripeListPage(value, collection) {
  const body = requireRecord(value, `Stripe ${collection} response`);
  const items = requireArray(body.data, `Stripe ${collection} response.data`);
  const hasMore = body.has_more === true;
  const nextCursor = hasMore ? extractId(items.at(-1)) : null;

  if (hasMore && !nextCursor) {
    throw new TypeError(`Stripe ${collection} response needs a last ID when has_more is true`);
  }

  return { items, hasMore, nextCursor };
}

function newBundleState(chargeId) {
  return {
    version: BUNDLE_CURSOR_VERSION,
    chargeId: requireStripeId(chargeId, "chargeId"),
    nextRefreshPass: 1,
    pendingTasks: [],
    knownTaskKeys: [],
  };
}

function parseBundleState(value) {
  const cursor = structuredClone(requireRecord(value, "Stripe bundle cursor"));

  if (cursor.version !== BUNDLE_CURSOR_VERSION) {
    throw new TypeError("unsupported Stripe bundle cursor version");
  }

  requireArray(cursor.pendingTasks, "Stripe bundle cursor.pendingTasks");
  requireArray(cursor.knownTaskKeys, "Stripe bundle cursor.knownTaskKeys");
  requireStripeId(cursor.chargeId, "Stripe bundle cursor.chargeId");

  if (!Number.isSafeInteger(cursor.nextRefreshPass) || cursor.nextRefreshPass < 1) {
    throw new TypeError("Stripe bundle cursor.nextRefreshPass must be positive");
  }

  for (const task of cursor.pendingTasks) {
    requireRecord(task, "Stripe bundle task");
    requireString(task.kind, "Stripe bundle task.kind");
  }

  return cursor;
}

function queueFinalChargeRefresh(state) {
  const pass = state.nextRefreshPass;
  state.nextRefreshPass += 1;
  addTask(state, {
    kind: "refreshCharge",
    chargeId: state.chargeId,
    pass,
  });
}

function addTask(state, task) {
  const key = taskKey(task);

  if (state.knownTaskKeys.includes(key)) {
    return;
  }

  state.knownTaskKeys.push(key);
  state.pendingTasks.push(structuredClone(task));
}

function addTaskToList(tasks, task) {
  tasks.push(structuredClone(task));
}

function taskKey(task) {
  if (task.kind.startsWith("list")) {
    const stableTask = { ...task, startingAfter: null };
    return JSON.stringify(stableTask);
  }

  return JSON.stringify(task);
}

function taskResult(relatedRecords, discoveredTasks, nextStartingAfter = null) {
  return { relatedRecords, discoveredTasks, nextStartingAfter };
}

function bundlePage({
  canonicalCharge,
  relatedRecords,
  state,
  observedAt,
}) {
  const complete = state.pendingTasks.length === 0;

  return {
    ...(canonicalCharge ? { canonicalCharge } : {}),
    relatedRecords: compactRelatedRecords(relatedRecords),
    observedAt,
    complete,
    nextCursor: complete ? null : state,
  };
}

function emptyRelatedRecords() {
  return {};
}

function compactRelatedRecords(records) {
  return Object.fromEntries(
    Object.entries(records).filter(([, values]) => values.length > 0),
  );
}

function addRecord(records, collection, record) {
  requireRecord(record, collection);
  records[collection] ??= [];
  records[collection].push(record);
}

function addRecords(records, collection, values) {
  for (const value of values) {
    addRecord(records, collection, value);
  }
}

function addUnresolvedReference(records, referenceType, id, reason) {
  addRecord(records, "unresolvedReferences", {
    id,
    object: "unresolved_reference",
    reference_type: referenceType,
    reason,
  });
}

function requireAccount(value) {
  const account = requireString(value, "account");
  const gatewayAccount = GATEWAY_ACCOUNTS[account];

  if (!gatewayAccount) {
    throw new TypeError("account must be main or kajabi");
  }

  return gatewayAccount;
}

function requireStripeId(value, fieldName) {
  const id = requireString(value, fieldName);

  if (!/^[A-Za-z0-9_]+$/.test(id)) {
    throw new TypeError(`${fieldName} is not a safe Stripe object ID`);
  }

  return id;
}

function requireString(value, fieldName) {
  if (typeof value !== "string" || !value.trim()) {
    throw new TypeError(`${fieldName} must be a non-empty string`);
  }

  return value.trim();
}

function requireUnixSeconds(value) {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError("Stripe created filters must be non-negative Unix seconds");
  }

  return value;
}

function requireCheckoutFilter(value) {
  if (value !== "payment_intent" && value !== "subscription") {
    throw new TypeError("checkout filter must be payment_intent or subscription");
  }

  return value;
}

function requireExtendedPaths(enabled, taskKind) {
  if (!enabled) {
    throw new TypeError(`${taskKind} requires extendedObjectPaths`);
  }
}

function nowIso(clock) {
  return new Date(clock.now()).toISOString();
}
