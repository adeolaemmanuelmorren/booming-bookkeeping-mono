import type { Env, SourceRoute } from "./contracts.ts";
import type { SourceCoordinator } from "./source-coordinator.ts";

export function sourceCoordinatorName(route: SourceRoute): string {
  if (route.source === "stripe") {
    return `stripe:${route.account}`;
  }

  return "activecampaign:default";
}

export function sourceCoordinator(
  env: Pick<Env, "SOURCE_COORDINATOR">,
  route: SourceRoute,
): DurableObjectStub<SourceCoordinator> {
  return env.SOURCE_COORDINATOR.getByName(sourceCoordinatorName(route));
}
