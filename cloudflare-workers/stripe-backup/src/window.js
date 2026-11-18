export function windowBounds(startAt, completedUntil, now) {
  const initial = Math.floor(Date.parse(startAt) / 1000);
  if (!Number.isFinite(initial)) throw new Error("Invalid initial timestamp");
  const cursor = completedUntil ? Math.floor(Date.parse(completedUntil) / 1000) : initial;
  if (!Number.isFinite(cursor)) throw new Error("Invalid completed timestamp");
  const end = Math.min(now - 60, cursor + 6 * 3600);
  if (cursor < now - 29 * 86400) throw new Error("Backup cursor exceeds Stripe event retention; reconcile before resuming");
  return { start: Math.max(initial, cursor - 600), end };
}
