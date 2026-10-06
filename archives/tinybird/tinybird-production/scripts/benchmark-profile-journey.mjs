import { readFile } from "node:fs/promises";

const profilePagePath = process.argv[2];
if (!profilePagePath) {
  throw new Error("Pass a Tinybird profile-page JSON file.");
}

const [{ token, host }, page] = await Promise.all([
  readJson(".tinyb"),
  readJson(profilePagePath),
]);
const profileIds = page.data?.map((row) => row.profile_id).filter(Boolean) ?? [];
if (profileIds.length === 0) throw new Error("The profile page is empty.");

const url = new URL("/v0/pipes/reporting_profile_journey_window_build.json", host);
url.searchParams.set("p_profile_ids", profileIds.join(","));

const startedAt = performance.now();
const response = await fetch(url, {
  headers: { Authorization: `Bearer ${token}` },
});
const body = await response.json();

console.log(JSON.stringify({
  httpStatus: response.status,
  requestedProfiles: profileIds.length,
  elapsedSeconds: (performance.now() - startedAt) / 1_000,
  rows: body.rows ?? null,
  statistics: body.statistics ?? null,
  error: body.error ?? null,
}, null, 2));

async function readJson(path) {
  return JSON.parse(await readFile(path, "utf8"));
}
