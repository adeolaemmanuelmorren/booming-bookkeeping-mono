import { WorkerEntrypoint } from "cloudflare:workers";
import { readProvider, stripeReadRequest, type ProviderParameters } from "./requests";

interface StripeSourceEnv {
  STRIPE_SECRET_KEY: string;
  STRIPE_KAJABI_SECRET_KEY: string;
}

// Only a private service binding can call this entrypoint. No public route is added.
export class StripeSource extends WorkerEntrypoint<StripeSourceEnv> {
  async read(account: "stripe" | "stripe_kajabi", path: string, parameters: ProviderParameters = {}) {
    if (account !== "stripe" && account !== "stripe_kajabi") throw new Error("Unknown Stripe account");
    const secret = account === "stripe" ? this.env.STRIPE_SECRET_KEY : this.env.STRIPE_KAJABI_SECRET_KEY;
    return readProvider(stripeReadRequest(secret, path, parameters));
  }
}
