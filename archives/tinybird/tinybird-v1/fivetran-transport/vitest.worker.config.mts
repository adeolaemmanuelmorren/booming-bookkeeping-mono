import { defineWorkersConfig } from "@cloudflare/vitest-pool-workers/config";
export default defineWorkersConfig({
  test: {
    include: ["test/**/*.worker.spec.ts"],
    poolOptions: {
      workers: {
        miniflare: {
          bindings: {
            COMPLETE_THROUGH: "2026-09-05T22:00:00Z",
            GCP_SERVICE_ACCOUNT_JSON: "{}",
            TINYBIRD_ADMIN_TOKEN: "test",
            INGESTION_ENABLED: "false",
            CHANGE_HISTORY_ENABLED_AT: "2026-09-05T21:00:00.000Z",
            SOURCE_CREATED_AT_JSON: JSON.stringify({
              raw_activecampaign_contact: "2026-07-15T09:40:22.635Z",
              raw_activecampaign_contact_tag: "2026-07-15T09:40:41.170Z",
              raw_activecampaign_tags: "2026-07-16T04:07:17.322Z",
              raw_stripe_charge: "2026-06-23T17:56:17.085Z",
              raw_stripe_customer: "2026-06-23T17:56:17.834Z",
              raw_stripe_kajabi_charge: "2026-07-22T05:24:50.089Z",
              raw_stripe_kajabi_customer: "2026-07-22T05:24:55.757Z",
              raw_stripe_kajabi_payment_intent: "2026-07-22T05:24:50.928Z",
              raw_stripe_payment_intent: "2026-06-23T17:56:16.745Z",
            }),
          },
        },
        wrangler: { configPath: "./wrangler.jsonc" },
      },
    },
  },
});
