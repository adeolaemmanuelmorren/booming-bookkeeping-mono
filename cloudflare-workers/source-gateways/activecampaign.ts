import { WorkerEntrypoint } from "cloudflare:workers";
import { activeCampaignReadRequest, readProvider, type ProviderParameters } from "./requests";

import { testContactTagIncremental } from "./ac-incremental-test";

interface ActiveCampaignSourceEnv {
  ACTIVE_CAMPAIGN_API_URL: string;
  ACTIVE_CAMPAIGN_API_TOKEN: string;
}

export class ActiveCampaignSource extends WorkerEntrypoint<ActiveCampaignSourceEnv> {
  async testTagIncremental() {
    return testContactTagIncremental(this.env.ACTIVE_CAMPAIGN_API_URL, this.env.ACTIVE_CAMPAIGN_API_TOKEN);
  }

  async read(path: string, parameters: ProviderParameters = {}) {
    return readProvider(activeCampaignReadRequest(
      this.env.ACTIVE_CAMPAIGN_API_URL,
      this.env.ACTIVE_CAMPAIGN_API_TOKEN,
      path,
      parameters,
    ));
  }
}
