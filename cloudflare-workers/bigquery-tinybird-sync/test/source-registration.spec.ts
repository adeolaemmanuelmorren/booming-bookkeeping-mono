import { describe, expect, it } from "vitest";
import { validateSourceRegistration } from "../scripts/source-registration.mjs";

const registeredSources = {
  sources: [
    { id: "payments", inputs: ["stripe"] },
    { id: "leads", inputs: ["activecampaign.contact"] },
  ],
};

describe("BigQuery source registration", () => {
  it("accepts dataset-wide and table-specific registrations", () => {
    expect(() => validateSourceRegistration(
      rawContract("stripe", "charge"),
      derivedContract("activecampaign", "contact"),
      registeredSources,
    )).not.toThrow();
  });

  it("rejects an unregistered physical source", () => {
    expect(() => validateSourceRegistration(
      rawContract("new_provider", "events"),
      { exports: [] },
      registeredSources,
    )).toThrow(
      "raw_new_provider_events reads new_provider.events, but no source registry entry owns that input.",
    );
  });

  it("rejects a physical source claimed by two registry entries", () => {
    const duplicateRegistry = {
      sources: [
        { id: "provider", inputs: ["stripe"] },
        { id: "charge-feed", inputs: ["stripe.charge"] },
      ],
    };

    expect(() => validateSourceRegistration(
      rawContract("stripe", "charge"),
      { exports: [] },
      duplicateRegistry,
    )).toThrow(
      "raw_stripe_charge reads stripe.charge, which is owned by multiple source registry entries: provider, charge-feed.",
    );
  });
});

function rawContract(dataset: string, table: string) {
  return {
    tables: [{
      resourceName: `raw_${dataset}_${table}`,
      source: { project: "test", dataset, table },
    }],
  };
}

function derivedContract(dataset: string, table: string) {
  return {
    exports: [{
      resourceName: "raw_derived",
      unionSources: [{ source: { project: "test", dataset, table } }],
    }],
  };
}
