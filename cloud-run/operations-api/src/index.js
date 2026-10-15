import { createServer } from "node:http";
import { BigQuery } from "@google-cloud/bigquery";
import { GoogleAuth } from "google-auth-library";
import { createHandler } from "./app.js";
import { createGoogleProvider } from "./google.js";
import { createJitsuProvider } from "./jitsu.js";
import { createMetaProvider } from "./meta.js";

const projectId = process.env.GOOGLE_CLOUD_PROJECT ?? "able-folio-499722";
const datasets = (process.env.BIGQUERY_DATASETS ?? [
  "booming_data_analytics", "booming_data_analytics_assertions", "boom_domains", "jitsu_data",
  "facebook_ads_airbyte", "google_ads_airbyte", "activecampaign_airbyte",
  "stripe_booming_airbyte", "stripe_kajabi_airbyte", "manual_attribution", "bill_reports_realtime",
].join(",")).split(",").map(value => value.trim()).filter(Boolean);
const auth = new GoogleAuth({ scopes: ["https://www.googleapis.com/auth/cloud-platform"] });
const google = createGoogleProvider({ auth, bigquery: new BigQuery({ projectId }), projectId, datasets,
  repository: process.env.DATAFORM_REPOSITORY ?? `projects/${projectId}/locations/us-central1/repositories/dataform` });
const jitsu = createJitsuProvider({ apiKey: process.env.JITSU_API_KEY, workspaceId: process.env.JITSU_WORKSPACE_ID });
const meta = createMetaProvider({ accessToken: process.env.META_ADS_ACCESS_TOKEN });
const handler = createHandler({ apiKey: process.env.OPERATIONS_API_KEY, google, jitsu, meta,
  secrets: [process.env.JITSU_API_KEY, process.env.META_ADS_ACCESS_TOKEN] });

const server = createServer(handler);
server.requestTimeout = 40_000;
server.headersTimeout = 10_000;
server.listen(Number(process.env.PORT ?? 8080), "0.0.0.0", () => console.log("Boom operations API listening."));
