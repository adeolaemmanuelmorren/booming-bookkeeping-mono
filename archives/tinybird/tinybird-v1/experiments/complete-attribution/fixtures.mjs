import { metricCount } from "./engine.mjs";
export const epoch = Date.parse("2026-09-01T07:00:00Z");
export function touch(id, time, options = {}) {
  return {
    id: String(id),
    time: time === null ? null : epoch + time * 3600000,
    sessionTime: time === null ? null : epoch + time * 3600000,
    sessionId: `session:${id}`,
    identityKey: "browser:1",
    source: "google",
    medium: "cpc",
    account: "account:1",
    campaign: "campaign:1",
    adset: "adset:1",
    ad: "ad:1",
    ...options,
  };
}
export function conversion(id, time, options = {}) {
  return {
    id: String(id),
    time: time === null ? null : epoch + time * 3600000,
    identityKey: "customer:1",
    currency: "USD",
    metrics: Array.from({ length: metricCount }, (_, i) =>
      i % 3 === 0 ? 1 : (i + 1) * 100,
    ),
    ...options,
  };
}
export function scenario(seed, touchCount = 25, conversionCount = 30) {
  let state = seed + 1;
  const random = (max) => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state % max;
  };
  const touches = Array.from({ length: touchCount }, (_, i) => {
    const time = random(80) - 10;
    const kind = random(4);
    return touch(`t${i}`, random(13) === 0 ? null : time, {
      sessionTime:
        random(5) === 0 ? null : epoch + (time - random(20)) * 3600000,
      sessionId: random(7) === 0 ? "" : `session:${Math.floor(i / 2)}`,
      source: ["direct", "google", "meta", "organic"][kind],
      medium: ["none", "cpc", "paid_social", "organic"][kind],
      ad: random(8) === 0 ? "" : `ad:${random(3)}`,
      campaign: `campaign:${random(2)}`,
    });
  });
  const conversions = Array.from({ length: conversionCount }, (_, i) =>
    conversion(`c${i}`, random(19) === 0 ? null : random(110) - 15, {
      currency: random(5) === 0 ? "EUR" : "USD",
      metrics: Array.from({ length: metricCount }, () =>
        random(6) === 0 ? 0 : random(50000) - 5000,
      ),
    }),
  );
  return { touches, conversions };
}
