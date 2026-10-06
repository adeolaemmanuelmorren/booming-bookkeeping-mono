import { readFile } from "node:fs/promises";

// Migration tools use the existing local login until the old project is archived.
// The deployed V1 Worker receives its own scoped secret instead.
export async function tinybirdConfig() {
  const location = process.env.TINYBIRD_CONFIG_PATH
    ?? new URL("../../tinybird-production/.tinyb", import.meta.url);
  const config = JSON.parse(await readFile(location, "utf8"));
  if (config.host !== "https://api.us-east.tinybird.co" || config.name !== "booming_bookkeeping") {
    throw new Error("The configured Tinybird workspace is not the approved Boom workspace");
  }
  return config;
}

export async function tinybirdRequest(path, options = {}) {
  const config = await tinybirdConfig();
  const response = await fetch(new URL(path, config.host), {
    ...options,
    headers: { ...options.headers, Authorization: `Bearer ${config.token}` },
    signal: AbortSignal.timeout(options.timeoutMs ?? 60_000),
  });
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error(`Tinybird request failed with HTTP ${response.status}`);
  }
  return response;
}

export async function query(sql) {
  const body = new URLSearchParams({ q: `${sql}\nFORMAT JSON` });
  const response = await tinybirdRequest("/v0/sql", { method: "POST", body });
  return response.json();
}
