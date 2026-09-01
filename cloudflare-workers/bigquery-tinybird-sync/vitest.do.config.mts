import { defineWorkersConfig } from "@cloudflare/vitest-pool-workers/config";

export default defineWorkersConfig({
  test: {
    include: ["test/**/*.worker.spec.ts"],
    poolOptions: {
      workers: {
        miniflare: {
          bindings: {
            GCP_SERVICE_ACCOUNT_JSON: "test-service-account-json",
            SYNC_ADMIN_TOKEN: "test-admin-token",
            TINYBIRD_FETCH_TIMEOUT_MS: "50",
            TINYBIRD_GATE_TIMEOUT_MS: "500",
            TINYBIRD_ADMIN_TOKEN: "test-tinybird-admin-token",
          },
        },
        wrangler: { configPath: "./wrangler.jsonc" },
      },
    },
  },
});
