import {
  conversionMetrics,
  modelNames,
  metricCount,
  dimensions,
} from "./engine.mjs";
const suffixes = ["mt", "ft", "lt", "paid_mt", "paid_ft", "paid_lt"];
export function namedMetrics(values, paidOnly = false) {
  if (values.length !== metricCount * modelNames.length)
    throw new Error("Expected all 144 attribution values");
  const result = {};
  for (let i = 0; i < metricCount; i++) {
    for (const suffix of paidOnly ? ["mt", "ft", "lt"] : suffixes) {
      const index =
        modelNames.indexOf(paidOnly ? `paid_${suffix}` : suffix) * metricCount +
        i;
      result[`${conversionMetrics[i]}_${suffix}`] = values[index];
    }
  }
  return result;
}
export function detailReport(rows) {
  return rows.map((row) => ({
    ...Object.fromEntries(
      dimensions.map((name, i) => [name, JSON.parse(row.key)[i]]),
    ),
    sessions: row.sessions,
    ...namedMetrics(row.values),
  }));
}
