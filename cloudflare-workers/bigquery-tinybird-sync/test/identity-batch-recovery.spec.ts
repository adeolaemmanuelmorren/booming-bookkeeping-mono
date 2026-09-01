import { describe, expect, it } from "vitest";
import { recoveredIdentityBatchId } from "../src/identity-batch-recovery";

describe("identity batch recovery", () => {
  it("gives a partial failed attempt a fresh, stable-size batch ID", () => {
    const currentBatchId = "generation_identity_123_engine1_limit5000";

    expect(recoveredIdentityBatchId(currentBatchId, 1_788_184_980_038)).toBe(
      "generation_identity_123_engine1_limit5000_retry1788184980038",
    );
  });

  it("keeps the recovered ID inside Tinybird's 200-character contract", () => {
    const recovered = recoveredIdentityBatchId("a".repeat(200), 1_788_184_980_038);

    expect(recovered).toHaveLength(200);
    expect(recovered.endsWith("_retry1788184980038")).toBe(true);
  });
});
