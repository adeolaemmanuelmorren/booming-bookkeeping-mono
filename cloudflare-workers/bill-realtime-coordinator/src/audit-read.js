// Admin-only, read-only access through the existing credential gateways.
// This never acquires a source lease, changes cursors, or publishes records.
export async function readAuditSource(env, { source, account, path, parameters = {} }) {
  const stripe = source === "stripe" && ["main", "kajabi"].includes(account);
  const activeCampaign = source === "activecampaign" && account === "default";
  if (!stripe && !activeCampaign) throw new Error("Invalid audit source.");
  const allowedPath = stripe
    ? /^\/(account|charges|events|refunds)(\/[A-Za-z0-9_]+)?$/
    : /^\/(contacts|contactTags|tags)(\/\d+)?(\/contactTags)?$/;
  if (typeof path !== "string" || !allowedPath.test(path)) throw new Error("Invalid audit path.");
  const allowed = stripe
    ? ["limit", "starting_after", "created[gte]", "created[lt]", "charge"]
    : ["limit", "offset", "id_greater", "orders[id]", "filters[updated_after]", "filters[updated_before]", "tagid", "include", "tag", "contact"];
  if (!parameters || typeof parameters !== "object" || Array.isArray(parameters)) throw new Error("Invalid audit parameters.");
  for (const [key, value] of Object.entries(parameters)) {
    if (!allowed.includes(key) || typeof value !== "string" || value.length > 128) throw new Error("Invalid audit parameter.");
  }
  if (parameters.include && parameters.include !== "contactTags") throw new Error("Invalid audit include.");
  if (parameters.limit && (!/^\d+$/.test(parameters.limit) || Number(parameters.limit) < 1 || Number(parameters.limit) > 100)) {
    throw new Error("Invalid audit page size.");
  }
  const result = stripe
    ? await env.STRIPE_SOURCE.read(account === "main" ? "stripe" : "stripe_kajabi", path, parameters)
    : await env.ACTIVECAMPAIGN_SOURCE.read(path, parameters);
  if (result.status >= 400) return { status: result.status, body: null };
  return result;
}
