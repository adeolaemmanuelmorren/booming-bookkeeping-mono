import { WorkflowEntrypoint } from "cloudflare:workers";
import { runBackup } from "./runner.js";

export class ActiveCampaignBackup extends WorkflowEntrypoint {
  async run(event, step) {
    return runBackup(this.env, event, step);
  }
}

export default {
  async scheduled(controller, env) {
    if (env.ENABLED !== "true") return;
    const slot = Math.floor(controller.scheduledTime / 300000);
    await env.ACTIVE_CAMPAIGN_BACKUP.create({ id: `activecampaign-${slot}` });
  },
};
