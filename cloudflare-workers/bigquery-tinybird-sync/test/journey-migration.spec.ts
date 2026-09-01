import { readFile } from "node:fs/promises";
import path from "node:path";
import { describe, expect, it } from "vitest";

describe("journey coordinator migration", () => {
  it("names the legacy columns when inserting into an expanded state table", async () => {
    const source = await readFile(
      path.resolve(process.cwd(), "src/journey-coordinator.ts"),
      "utf8",
    );

    expect(source).toContain("INSERT OR IGNORE INTO journey_state (");
    expect(source).not.toContain("INSERT OR IGNORE INTO journey_state VALUES");
  });

  it("uses a short healthy cadence while preserving backpressure delay", async () => {
    const source = await readFile(
      path.resolve(process.cwd(), "src/journey-coordinator.ts"),
      "utf8",
    );

    expect(source).toContain("const BUILD_CALL_INTERVAL_MS = 1_000;");
    expect(source).toContain("const DEFAULT_BACKPRESSURE_MS = 60_000;");
    expect(source).toContain("error.retryAfterMs ?? DEFAULT_BACKPRESSURE_MS");
  });
});
