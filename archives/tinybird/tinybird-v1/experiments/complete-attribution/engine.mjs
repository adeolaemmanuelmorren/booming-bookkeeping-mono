import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
export const {
  conversionMetrics,
} = require("../../../dataform/includes/conversion_metrics.js");
export const metricCount = conversionMetrics.length;
export const modelNames = ["ft", "lt", "mt", "paid_ft", "paid_lt", "paid_mt"];
const dateFormatter = new Intl.DateTimeFormat("en-CA", {
  timeZone: "America/Los_Angeles",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});
const dateCache = new Map();
export function reportDate(time) {
  if (dateCache.has(time)) return dateCache.get(time);
  const date = dateFormatter.format(new Date(time));
  if (dateCache.size > 100000) dateCache.clear();
  dateCache.set(time, date);
  return date;
}
export const dimensions = [
  "date",
  "source",
  "medium",
  "account",
  "campaign",
  "adset",
  "ad",
  "campaignName",
  "adsetName",
  "adName",
  "utmContent",
  "utmTerm",
  "landingHost",
  "landingPath",
  "currency",
];
export function groupKey(touch, conversionTime, currency) {
  const values = {
    date: reportDate(touch?.sessionTime ?? conversionTime),
    source: touch?.source ?? "offline",
    medium: touch?.medium ?? "offline",
    account: touch?.account ?? "",
    campaign: touch?.campaign ?? null,
    adset: touch?.adset ?? null,
    ad: touch?.ad ?? null,
    campaignName: touch?.campaignName ?? "unknown",
    adsetName: touch?.adsetName ?? "unknown",
    adName: touch?.adName ?? "unknown",
    utmContent: touch?.utmContent ?? "unknown",
    utmTerm: touch?.utmTerm ?? "unknown",
    landingHost: touch?.landingHost ?? "offline",
    landingPath: touch?.landingPath ?? "offline",
    currency,
  };
  return JSON.stringify(dimensions.map((name) => values[name]));
}
export function isDirect(touch) {
  return (
    touch.source.toLowerCase() === "direct" &&
    touch.medium.toLowerCase() === "none"
  );
}
export function isPaid(touch) {
  return (
    ["google", "meta"].includes(touch.source.toLowerCase()) &&
    ["cpc", "paid", "paid_social"].includes(touch.medium.toLowerCase()) &&
    touch.campaign != null &&
    touch.ad != null
  );
}
function compareTouch(a, b, time) {
  const difference = (a.sessionTime ?? time) - (b.sessionTime ?? time);
  return difference || Buffer.compare(Buffer.from(a.id), Buffer.from(b.id));
}
function choose(a, b, time, last = false) {
  if (!a) return b;
  if (!b) return a;
  return compareTouch(a, b, time) > 0 === last ? a : b;
}
function add(vector, input, factor = 1, offset = 0) {
  for (let i = 0; i < metricCount; i++) vector[offset + i] += input[i] * factor;
}
function rowFor(rows, touch, time, currency) {
  const key = groupKey(touch, time, currency);
  if (!rows.has(key))
    rows.set(key, {
      key,
      values: new Float64Array(metricCount * 6),
      sessions: new Set(),
    });
  const row = rows.get(key);
  if (touch && touch.sessionId != null) row.sessions.add(touch.sessionId);
  return row;
}
function credit(
  rows,
  touch,
  conversion,
  model,
  factor,
  metrics = conversion.metrics,
) {
  if (!factor) return;
  const row = rowFor(rows, touch, conversion.time, conversion.currency);
  add(row.values, metrics, factor, model * metricCount);
  if (touch && isPaid(touch))
    add(row.values, metrics, factor, (model + 3) * metricCount);
}

// One profile's current history. The graph is supplied by the caller.
// A uniform middle-credit accumulator replaces the T*C intermediate table.
export function attributeProfile(touches, conversions) {
  const ordered = touches
    .filter((t) => t.time !== null)
    .slice()
    .sort(
      (a, b) =>
        a.time - b.time || Buffer.compare(Buffer.from(a.id), Buffer.from(b.id)),
    );
  const events = conversions
    .filter((c) => c.time !== null)
    .slice()
    .sort((a, b) => a.time - b.time);
  const rows = new Map();
  const bases = new Map();
  const currencyDays = new Map();
  const activated = [];
  const hasUndatedNonDirect = ordered.some(
    (t) => t.sessionTime === null && !isDirect(t),
  );
  let cursor = 0,
    nonDirectCount = 0;
  let firstDated = null,
    firstUndated = null;
  let firstNonDirectDated = null,
    lastNonDirectDated = null,
    firstNonDirectUndated = null,
    lastNonDirectUndated = null;
  for (const conversion of events) {
    while (cursor < ordered.length && ordered[cursor].time <= conversion.time) {
      const touch = ordered[cursor++];
      const initialBases = new Map(
        [...bases].map(([currency, state]) => [currency, state.total.slice()]),
      );
      activated.push({ touch, initialBases });
      if (touch.sessionTime === null)
        firstUndated = choose(firstUndated, touch, conversion.time);
      else firstDated = choose(firstDated, touch, conversion.time);
      if (isDirect(touch)) continue;
      nonDirectCount++;
      if (touch.sessionTime === null) {
        firstNonDirectUndated = choose(
          firstNonDirectUndated,
          touch,
          conversion.time,
        );
        lastNonDirectUndated = choose(
          lastNonDirectUndated,
          touch,
          conversion.time,
          true,
        );
      } else {
        firstNonDirectDated = choose(
          firstNonDirectDated,
          touch,
          conversion.time,
        );
        lastNonDirectDated = choose(
          lastNonDirectDated,
          touch,
          conversion.time,
          true,
        );
      }
    }
    const date = reportDate(conversion.time);
    if (!currencyDays.has(conversion.currency))
      currencyDays.set(conversion.currency, new Map());
    currencyDays.get(conversion.currency).set(date, conversion.time);
    const first = choose(firstDated, firstUndated, conversion.time);
    if (!first) {
      for (let model = 0; model < 3; model++)
        credit(rows, null, conversion, model, 1);
      continue;
    }
    const firstDirect = isDirect(first);
    const count = nonDirectCount + Number(firstDirect);
    const last =
      choose(lastNonDirectDated, lastNonDirectUndated, conversion.time, true) ??
      first;
    credit(rows, first, conversion, 0, 1);
    credit(rows, last, conversion, 1, 1);
    if (count === 1) {
      credit(rows, first, conversion, 2, 1);
      continue;
    }
    if (count === 2) {
      credit(rows, first, conversion, 2, 0.5);
      credit(rows, last, conversion, 2, 0.5);
      continue;
    }
    const base = 0.2 / (count - 2);
    if (!bases.has(conversion.currency))
      bases.set(conversion.currency, {
        total: new Float64Array(metricCount),
        days: new Map(),
      });
    const state = bases.get(conversion.currency);
    add(state.total, conversion.metrics, base);
    // Retain per-day prefixes only when nullable session starts need date fallback.
    if (hasUndatedNonDirect) {
      if (!state.days.has(date))
        state.days.set(date, {
          total: new Float64Array(metricCount),
          points: [],
        });
      const day = state.days.get(date);
      add(day.total, conversion.metrics, base);
      day.points.push({ time: conversion.time, total: day.total.slice() });
    }
    credit(rows, first, conversion, 2, firstDirect ? 0.4 : 0.4 - base);
    credit(rows, last, conversion, 2, 0.4 - base);
  }
  for (const { touch, initialBases } of activated) {
    for (const [currency, days] of currencyDays) {
      const state = bases.get(currency);
      if (touch.sessionTime !== null) {
        const latest = Math.max(...days.values());
        if (latest < touch.time) continue;
        rowFor(rows, touch, latest, currency); // Preserve zero-credit direct rows and session counts.
        if (isDirect(touch) || !state) continue;
        const before =
          initialBases.get(currency) ?? new Float64Array(metricCount);
        const values = Float64Array.from(
          state.total,
          (value, i) => value - before[i],
        );
        credit(rows, touch, { time: latest, currency, metrics: values }, 2, 1);
        continue;
      }
      for (const [date, latest] of days) {
        if (latest < touch.time) continue;
        rowFor(rows, touch, latest, currency);
        const day = state?.days.get(date);
        if (isDirect(touch) || !day) continue;
        let lower = 0,
          upper = day.points.length;
        while (lower < upper) {
          const middle = Math.floor((lower + upper) / 2);
          if (day.points[middle].time < touch.time) lower = middle + 1;
          else upper = middle;
        }
        const before = lower
          ? day.points[lower - 1].total
          : new Float64Array(metricCount);
        const values = Float64Array.from(
          day.total,
          (value, i) => value - before[i],
        );
        credit(rows, touch, { time: latest, currency, metrics: values }, 2, 1);
      }
    }
  }
  return [...rows.values()].map((row) => ({
    key: row.key,
    values: Array.from(row.values),
    sessions: row.sessions.size,
    sessionIds: [...row.sessions].sort(),
  }));
}
