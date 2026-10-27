import { EnvironmentId } from "@t3tools/contracts";
import type { ContinuityRequirement } from "@t3tools/provider-core/server/jones/GoalContinuityContext";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Environment from "../../../environment/ServerEnvironment.ts";
import * as GoalContinuity from "./GoalContinuity.ts";

/** Legacy startup unit fixtures have no continuity state or delivery producer.
 * Actual continuity behavior uses the real service in its own integration tests. */
export const layerWithRequirement = (requirement: ContinuityRequirement = {
  outcome: "absent", gate: { outcome: "clear", fenceRevision: "0" },
}) => Layer.mergeAll(
  Layer.succeed(Environment.ServerEnvironmentIdentity, {
    getEnvironmentId: Effect.succeed(EnvironmentId.make("environment:mcp-test")),
  }),
  Layer.mock(GoalContinuity.GoalContinuity)({
    requirementFor: () => Effect.succeed(requirement),
    context: (capability) => ({
      capability,
      recordRecoveryTransition: () => Effect.succeed({
        outcome: "absent" as const, gate: { outcome: "clear" as const, fenceRevision: "0" },
      }),
      prepareRecovery: () => Effect.succeed({ outcome: "absent" as const }),
      beginDelivery: () => Effect.succeed({ outcome: "refused" as const, reason: "unavailable" as const }),
      markDispatching: () => Effect.succeed({ outcome: "refused" as const, reason: "unavailable" as const }),
      settleDelivery: () => Effect.succeed({ outcome: "refused" as const, reason: "unavailable" as const }),
      checkContinuation: () => Effect.succeed({
        requirement: { outcome: "absent" as const, gate: { outcome: "clear" as const, fenceRevision: "0" } },
        currentOwner: null,
      }),
    }),
  }),
);

export const layerAbsent = layerWithRequirement();
