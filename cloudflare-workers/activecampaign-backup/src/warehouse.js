async function request(env, path, body) {
  const response = await fetch(new URL(path, env.WAREHOUSE_URL), {
    method: "POST",
    headers: { Authorization: `Bearer ${env.WAREHOUSE_TOKEN}`, "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`Backup receiver ${path} returned ${response.status}`);
  return response.json();
}

export async function completedWindow(env, account) {
  const result = await request(env, "/window", { account });
  return result.windowEnd;
}

export async function publish(env, account, workflowId, batchId, observedAt, records) {
  if (!records.length) return;
  return request(env, "/records", { account, workflowId, batchId, observedAt, records });
}

export async function complete(env, account, bounds, workflowId, catalogObservedAt, tagCount, contactCount) {
  return request(env, "/complete", { account, ...bounds, workflowId, catalogObservedAt, tagCount, contactCount });
}
