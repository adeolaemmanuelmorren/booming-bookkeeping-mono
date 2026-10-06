import { detailReport } from "./named-metrics.mjs";
import { paidAdReport } from "./report.mjs";

// Provider adapters must finish pagination and normalize to the reporting time zone.
// This contract is tested with fixtures. Live Google/Meta adapters are not implemented.
export async function requestReport({
  store,
  providers,
  start,
  end,
  minVersion = 0,
}) {
  if (
    !/^\d{4}-\d{2}-\d{2}$/.test(start) ||
    !/^\d{4}-\d{2}-\d{2}$/.test(end) ||
    start >= end
  ) {
    throw new Error("Invalid exclusive-end report window");
  }
  const [attribution, ...responses] = await Promise.all([
    store.report(start, end, minVersion),
    ...providers.map((provider) =>
      provider.fetch({ start, end, timeZone: "America/Los_Angeles" }),
    ),
  ]);
  for (const response of responses) {
    if (
      !response.complete ||
      response.start !== start ||
      response.end !== end ||
      response.timeZone !== "America/Los_Angeles" ||
      !response.asOf
    ) {
      throw new Error(
        "Ad performance is incomplete or uses a different reporting window",
      );
    }
    if (response.rows.some((row) => row.date < start || row.date >= end))
      throw new Error("Ad row outside report window");
  }
  return {
    attributionVersion: attribution.version,
    adAsOf: responses.map((response) => response.asOf),
    attribution: detailReport(attribution.rows),
    paidAds: paidAdReport(
      attribution.rows,
      responses.flatMap((response) => response.rows),
    ),
  };
}
