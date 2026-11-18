import { WorkflowEntrypoint } from "cloudflare:workers";
import { runBackup } from "./runner.js";

const ACCOUNTS = ["stripe", "stripe_kajabi"];

export class StripeBackup extends WorkflowEntrypoint {
  async run(event, step) {
    return runBackup(this.env, event, step);
  }
}

export default {
  async scheduled(controller, env) {
    if (env.ENABLED !== "true") return;
    const slot = Math.floor(controller.scheduledTime / 300000);
    for (const account of ACCOUNTS) {
      await env.STRIPE_BACKUP.create({ id: `${account}-${slot}`, params: { account } });
    }
  },
};
