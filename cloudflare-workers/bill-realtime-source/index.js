import { handleAdsRequest } from "./ads.js";
import { normalizeJitsu } from "./browser-normalize.js";

export default {
  async fetch(request, env, ctx) {
    if (
      !env.ADMIN_TOKEN ||
      request.headers.get("Authorization") !== `Bearer ${env.ADMIN_TOKEN}`
    )
      return new Response("Unauthorized", { status: 401 });
    const pathname = new URL(request.url).pathname;
    if (pathname.startsWith("/v1/google-ads/") || pathname.startsWith("/v1/meta-ads/")) {
      return handleAdsRequest(request, env, ctx);
    }
    if (
      request.method !== "POST" ||
      !["/read", "/browser-page"].includes(pathname)
    )
      return new Response("Not found", { status: 404 });
    try {
      if (pathname === "/browser-page")
        return await browserPage(await request.json(), env);
      const { source, account, path, parameters = {} } = await request.json();
      if (
        typeof path !== "string" ||
        !parameters ||
        Array.isArray(parameters) ||
        Object.values(parameters).some((value) => typeof value !== "string")
      )
        return new Response("Invalid request", { status: 400 });
      // These private entrypoints validate provider paths and issue GETs only.
      // This gateway has no source cursors, publication, or ingestion bindings.
      if (source === "stripe" && ["main", "kajabi"].includes(account)) {
        const result = await env.STRIPE_SOURCE.read(
          account === "main" ? "stripe" : "stripe_kajabi",
          path,
          parameters,
        );
        return Response.json(result, {
          headers: { "Cache-Control": "no-store" },
        });
      }
      if (source === "activecampaign" && account === "default") {
        const result = await env.ACTIVECAMPAIGN_SOURCE.read(path, parameters);
        return Response.json(result, {
          headers: { "Cache-Control": "no-store" },
        });
      }
      return new Response("Invalid source", { status: 400 });
    } catch {
      return new Response("Source read failed", { status: 502 });
    }
  },
};

async function browserPage({ cursor, since, until, keys }, env) {
  if (
    !Number.isFinite(Date.parse(since)) ||
    !Number.isFinite(Date.parse(until))
  )
    return new Response("Invalid browser bounds", { status: 400 });
  if (!keys) {
    const page = await env.BROWSER_BUFFER.list({
      prefix: "jitsu/envelopes/",
      limit: 1000,
      ...(cursor ? { cursor } : {}),
    });
    return Response.json(
      {
        keys: page.objects
          .filter(
            (object) =>
              object.uploaded.getTime() >= Date.parse(since) &&
              object.uploaded.getTime() < Date.parse(until),
          )
          .map((object) => object.key),
        scanned: page.objects.length,
        cursor: page.truncated ? page.cursor : null,
      },
      { headers: { "Cache-Control": "no-store" } },
    );
  }
  if (
    !Array.isArray(keys) ||
    keys.length > 50 ||
    keys.some((key) => !/^jitsu\/envelopes\/[a-f0-9]{64}\.json$/.test(key))
  )
    return new Response("Invalid browser keys", { status: 400 });
  const replacements = [];
  for (const key of keys) {
    const object = await env.BROWSER_BUFFER.get(key);
    if (!object || object.size > 2_000_000)
      throw new Error("Browser object unavailable.");
    const body = await object.text();
    const digest = Array.from(
      new Uint8Array(
        await crypto.subtle.digest("SHA-256", new TextEncoder().encode(body)),
      ),
      (byte) => byte.toString(16).padStart(2, "0"),
    ).join("");
    if (key !== `jitsu/envelopes/${digest}.json`)
      throw new Error("Browser object hash mismatch.");
    const envelope = JSON.parse(body);
    if (
      envelope.schema_version !== "jitsu_events_api_v1" ||
      !Array.isArray(envelope.events)
    )
      throw new Error("Invalid browser envelope.");
    for (const observation of envelope.events) {
      if (observation.tenant_id !== "boom")
        throw new Error("Browser tenant mismatch.");
      const browser = await normalizeJitsu(observation);
      replacements.push({
        source: "browser",
        source_account: "default",
        scope_id: `${browser.source.event_kind}:${browser.source.source_record_id}`,
        observation_sequence: Number(browser.source.source_revision),
        observed_at: observation.ingested_at,
        rows: [],
        source_evidence: {},
        browser,
      });
    }
  }
  return Response.json(
    {
      replacements,
      scanned: keys.length,
      cursor: null,
    },
    { headers: { "Cache-Control": "no-store" } },
  );
}
