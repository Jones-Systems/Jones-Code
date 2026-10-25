import { OrchestratorMcpFailure, type EnvironmentId } from "@t3tools/contracts";
import {
  ThreadLinkToolInput,
  ThreadLinkToolResult,
  type LinkedThreadLink,
  type LinkedThreadReverseState,
} from "@t3tools/contracts/jones/linkedThreads";
import * as Effect from "effect/Effect";
import { Tool } from "effect/ai";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import * as PeerForwarding from "../../peer/PeerForwarding.ts";

// A schema-only descriptor avoids a registration/handler cycle while reusing the forwarding path.
const MirrorThreadLinkTool = Tool.make("t3_thread_link", {
  parameters: ThreadLinkToolInput,
  success: ThreadLinkToolResult,
  failure: OrchestratorMcpFailure,
});
export const mirrorFailureState = (error: OrchestratorMcpFailure): "unavailable" | "unknown" =>
  [
    "capability_denied",
    "invalid_request",
    "thread_not_found",
    "target_required",
    "provider_not_found",
  ].includes(error.code) ||
  /unknown tool|not found|not linked|version mismatch|unsupported|forbidden/i.test(error.message)
    ? "unavailable"
    : "unknown";
export const mirrorLink = Effect.fn("LinkedThreads.mirrorLink")(function* (
  scope: McpInvocationScope,
  link: LinkedThreadLink,
  input: ThreadLinkToolInput,
  forwarding: PeerForwarding.PeerForwarding["Service"],
) {
  const environmentId: EnvironmentId | null =
    link.from.environmentId !== scope.environmentId
      ? link.from.environmentId
      : link.to.environmentId !== scope.environmentId
        ? link.to.environmentId
        : null;
  if (environmentId === null) return { state: "local" as LinkedThreadReverseState, environmentId };
  // A mirrored call must not bounce back or imply that a linked session can use a reverse grant.
  if (scope.client?.linked === true)
    return { state: "recorded" as LinkedThreadReverseState, environmentId };
  if (scope.thread === undefined)
    return {
      state: "unavailable" as LinkedThreadReverseState,
      environmentId,
      detail: "Only a thread caller can assert an origin to the other environment.",
    };
  return yield* forwarding.call(scope, MirrorThreadLinkTool, environmentId, input).pipe(
    Effect.map(() => ({ state: "recorded" as LinkedThreadReverseState, environmentId })),
    Effect.catch((error) =>
      Effect.succeed({
        state: mirrorFailureState(error),
        environmentId,
        detail: "The local copy is recorded; the other environment did not confirm its copy.",
      }),
    ),
  );
});
