import { WorkerEntrypoint } from "cloudflare:workers";

import type {
  CoordinatorStatus,
  Env,
  SourceRoute,
  StartBackfillOptions,
} from "./contracts.ts";
import { sourceCoordinator } from "./routing.ts";

export class SourceCoordinatorService extends WorkerEntrypoint<Env> {
  startBackfill(
    route: SourceRoute,
    options: StartBackfillOptions = {},
  ): Promise<CoordinatorStatus> {
    return sourceCoordinator(this.env, route).startBackfill(route, options);
  }

  wake(route: SourceRoute): Promise<CoordinatorStatus> {
    return sourceCoordinator(this.env, route).wake(route);
  }

  status(route: SourceRoute): Promise<CoordinatorStatus> {
    return sourceCoordinator(this.env, route).status(route);
  }
}
