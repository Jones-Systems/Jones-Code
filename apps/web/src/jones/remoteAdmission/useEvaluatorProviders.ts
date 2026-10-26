import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId } from "@t3tools/contracts";
import { serverEnvironment } from "../../state/server";

export function useEvaluatorProviders(environmentId: EnvironmentId) {
  const projection = useAtomValue(serverEnvironment.configProjection({ environmentId, input: {} }));
  return projection._tag === "Success" && projection.value.source === "live"
    ? projection.value.config.providers
    : null;
}
