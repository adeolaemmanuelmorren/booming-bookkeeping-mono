import { createGoogleAdsClient } from "../../bill-ad-reports-site/server/reporting/src/sources/google/google-ads-rest-client.js";
import { readConfig as googleConfig } from "../../bill-ad-reports-site/server/reporting/src/sources/google/config.js";
import { createPerformanceLoader as googlePerformance } from "../../bill-ad-reports-site/server/reporting/src/sources/google/performance.js";
import { createHourlyLoader } from "../../bill-ad-reports-site/server/reporting/src/sources/google/hourly.js";
import { createRequestHandler as googleHandler } from "../../bill-ad-reports-site/server/reporting/src/sources/google/app.js";
import { readConfig as metaConfig } from "../../bill-ad-reports-site/server/reporting/src/sources/meta/config.js";
import { createMetaClient } from "../../bill-ad-reports-site/server/reporting/src/sources/meta/meta-client.js";
import { createPerformanceLoader as metaPerformance } from "../../bill-ad-reports-site/server/reporting/src/sources/meta/performance.js";
import { createRequestHandler as metaHandler } from "../../bill-ad-reports-site/server/reporting/src/sources/meta/app.js";

const metaHandlers = new WeakMap();

// Authentication is checked by index.js before either the cache or provider.
export async function handleAdsRequest(request, env, ctx, {
  fetchImpl = fetch, cache = globalThis.caches?.default,
} = {}) {
  if (request.method !== "GET") return new Response("Not found", { status: 404 });
  const url = new URL(request.url);
  const google = url.pathname.startsWith("/v1/google-ads/");
  const token = request.headers.get("X-Google-Ads-Access-Token");
  if (google && !token) return new Response("Google access token required", { status: 401 });

  // Never put credentials into cache keys or cached responses.
  const cacheUrl = new URL(url.pathname, "https://ads-cache.internal");
  for (const name of ["start_date", "end_date"]) {
    cacheUrl.searchParams.set(name, url.searchParams.get(name) ?? "");
  }
  const key = new Request(cacheUrl);
  const cached = cache ? await cache.match(key) : null;
  if (cached) {
    const headers = new Headers(cached.headers);
    headers.set("Cache-Control", "private, max-age=300");
    return new Response(cached.body, { status: cached.status, headers });
  }

  try {
    let handle;
    if (google) {
      const config = googleConfig(env);
      const auth = { getClient: async () => ({
        getRequestHeaders: async () => new Headers({ Authorization: `Bearer ${token}` }),
      }) };
      const adsClient = createGoogleAdsClient({ ...config, auth, fetchImpl });
      handle = googleHandler({
        loadPerformance: googlePerformance({ adsClient, cacheSeconds: 300 }),
        loadHourly: createHourlyLoader({ adsClient, cacheSeconds: 300 }),
        maxRangeDays: config.maxRangeDays, cacheSeconds: 300,
      });
    } else {
      let cachedHandler = metaHandlers.get(env);
      if (!cachedHandler || cachedHandler.fetchImpl !== fetchImpl) {
        const config = metaConfig(env);
        const loaders = metaPerformance({
          metaClient: createMetaClient({ ...config, fetchImpl }), cacheSeconds: 300,
        });
        cachedHandler = { fetchImpl,
          handle: metaHandler({ loaders, maxRangeDays: config.maxRangeDays, cacheSeconds: 300 }) };
        metaHandlers.set(env, cachedHandler);
      }
      // Preserve pending Meta Insights IDs across retries in this isolate.
      handle = cachedHandler.handle;
    }
    const result = await handle(request);
    const response = new Response(result.body, { status: result.status, headers: result.headers });
    if (response.ok && cache) {
      const stored = new Response(result.body, { headers: {
        ...result.headers, "Cache-Control": "public, max-age=300",
      } });
      const write = cache.put(key, stored).catch(() => {});
      if (ctx?.waitUntil) ctx.waitUntil(write);
      else await write;
    }
    return response;
  } catch {
    return Response.json({ error: "Ad source request failed." }, { status: 502 });
  }
}
