import { readFile } from "node:fs/promises";

const [operation = "status", source = "stripe", account = "main", startDate] = process.argv.slice(2);
if (!["status", "start", "wake", "pause"].includes(operation)) throw new Error("Invalid operation.");
const createdGte = startDate ? Math.floor(Date.parse(startDate) / 1000) : undefined;
if (operation === "start" && (!Number.isSafeInteger(createdGte) || createdGte < 0)) {
  throw new Error("Start requires an explicit ISO date as the fourth argument.");
}
const config = await readFile(new URL("./.dev.vars", import.meta.url), "utf8");
const token = config.match(/^ADMIN_TOKEN=([a-f0-9]{64})$/m)?.[1];
if (!token) throw new Error("Missing local admin token.");
const response = await fetch(`https://bill-realtime-coordinator.bill-3e3.workers.dev/admin/sources/${operation}`, {
  method: "POST",
  headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
  body: JSON.stringify({ route: { source, account }, createdGte }),
  signal: AbortSignal.timeout(60_000),
});
if (!response.headers.get("content-type")?.includes("application/json")) {
  throw new Error(`Operation returned HTTP ${response.status}.`);
}
console.log(JSON.stringify({ operation, source, account, status: response.status, result: await response.json() }));
if (!response.ok) process.exitCode = 1;
