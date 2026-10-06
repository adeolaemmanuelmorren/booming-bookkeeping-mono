import { readFile, writeFile, mkdir } from "node:fs/promises";

const config = await readFile(new URL("./.dev.vars", import.meta.url), "utf8");
const token = config.match(/^ADMIN_TOKEN=([a-f0-9]{64})$/m)?.[1];
if (!token) throw new Error("Missing local admin token.");
const response = await fetch("https://bill-realtime-coordinator.bill-3e3.workers.dev/admin/check-sources", {
  method: "POST", headers: { Authorization: `Bearer ${token}` },
  signal: AbortSignal.timeout(60_000),
});
if (!response.headers.get("content-type")?.includes("application/json")) {
  throw new Error(`Source check returned HTTP ${response.status}.`);
}
const result = { checkedAt: new Date().toISOString(), status: response.status, ...await response.json() };
await mkdir(new URL("./evidence", import.meta.url), { recursive: true });
await writeFile(new URL("./evidence/source-access.json", import.meta.url), JSON.stringify(result, null, 2) + "\n");
console.log(JSON.stringify(result));
