import {
  OrchestratorMcpFailure,
  type OrchestratorMcpDelegateTaskInput,
  type ProviderInteractionMode,
  type RuntimeMode,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { runtimeModeRank } from "../../../mcp/delegatedTaskModes.ts";

export const refuseUnsolicitedSupervision = Effect.fn(
  "DelegatedTaskSupervision.refuseUnsolicitedSupervision",
)(function* (
  parent: { readonly runtimeMode: RuntimeMode; readonly interactionMode: ProviderInteractionMode },
  request: Pick<
    OrchestratorMcpDelegateTaskInput,
    "runtimeMode" | "interactionMode" | "supervisionRequestedByUser"
  >,
): Effect.fn.Return<void, OrchestratorMcpFailure> {
  if (request.supervisionRequestedByUser === true) return;
  const narrowerRuntime =
    request.runtimeMode !== undefined &&
    request.runtimeMode !== "inherit" &&
    runtimeModeRank(request.runtimeMode) < runtimeModeRank(parent.runtimeMode);
  if (
    narrowerRuntime ||
    (request.interactionMode === "plan" && parent.interactionMode === "default")
  ) {
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: `Omit runtimeMode and interactionMode so the child inherits your modes (${parent.runtimeMode}, ${parent.interactionMode}). Narrower modes make it wait for a person at each approval. Use one only when the user asked for a supervised child, with supervisionRequestedByUser: true. A role or read-only task is not a reason.`,
    });
  }
});

export const inheritsLinkedDelegationModes = (input: {
  readonly delegatedFrom: unknown;
  readonly linkOrigin: unknown;
  readonly narrowedTo: unknown;
}): boolean =>
  input.delegatedFrom !== undefined &&
  input.linkOrigin !== undefined &&
  input.narrowedTo !== undefined;
