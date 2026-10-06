import { metricCount, groupKey } from "./engine.mjs";

// Deliberately quadratic, simple Dataform-style reference. Small test cases only.
export function referenceProfile(touches, conversions) {
  const groups = new Map();
  for (const conversion of conversions) {
    if (conversion.time === null) continue;
    const earlier = touches.filter(
      (t) => t.time !== null && t.time <= conversion.time,
    );
    earlier.sort(
      (a, b) =>
        (a.sessionTime ?? conversion.time) -
          (b.sessionTime ?? conversion.time) ||
        Buffer.compare(Buffer.from(a.id), Buffer.from(b.id)),
    );
    if (!earlier.length) earlier.push(null);
    const included = earlier.filter(
      (t, i) =>
        !t ||
        i === 0 ||
        t.source.toLowerCase() !== "direct" ||
        t.medium.toLowerCase() !== "none",
    );
    for (const touch of earlier) {
      const key = groupKey(touch, conversion.time, conversion.currency);
      if (!groups.has(key))
        groups.set(key, {
          key,
          values: Array(metricCount * 6).fill(0),
          sessions: new Set(),
        });
      const row = groups.get(key);
      if (touch && touch.sessionId != null) row.sessions.add(touch.sessionId);
      const position = included.indexOf(touch);
      if (position < 0) continue;
      let multi;
      if (included.length === 1) multi = 1;
      else if (included.length === 2) multi = 0.5;
      else if (position === 0 || position === included.length - 1) multi = 0.4;
      else multi = 0.2 / (included.length - 2);
      const weights = [
        Number(position === 0),
        Number(position === included.length - 1),
        multi,
      ];
      const paid =
        touch &&
        ["google", "meta"].includes(touch.source.toLowerCase()) &&
        ["cpc", "paid", "paid_social"].includes(touch.medium.toLowerCase()) &&
        touch.campaign != null &&
        touch.ad != null;
      for (let model = 0; model < 3; model++)
        for (let metric = 0; metric < metricCount; metric++) {
          row.values[model * metricCount + metric] +=
            conversion.metrics[metric] * weights[model];
          if (paid)
            row.values[(model + 3) * metricCount + metric] +=
              conversion.metrics[metric] * weights[model];
        }
    }
  }
  return [...groups.values()].map((row) => ({
    ...row,
    sessions: row.sessions.size,
    sessionIds: [...row.sessions].sort(),
  }));
}
