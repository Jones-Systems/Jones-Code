import type { CommandId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import {
  OrchestratorDispatchError,
  type OrchestratorV2Error,
} from "../../orchestration-v2/Orchestrator.ts";
import type { SelfSettlementIntent } from "../../orchestration-v2/SelfSettlement.ts";

export interface CapturedCaller {
  readonly mcpCredentialId: string;
  readonly providerInstanceId: ProviderInstanceId;
}
export interface Request extends CapturedCaller {
  readonly threadId: ThreadId;
  readonly commandId: CommandId;
  readonly projectRemaining?: ReadonlyArray<string>;
}
export interface Ports {
  readonly requestSelfSettlement?: (
    input: Request,
  ) => Effect.Effect<SelfSettlementIntent, OrchestratorV2Error>;
}
/** providerSessionId in the authenticated MCP scope names the credential.
 * Core derives the distinct native session from the exact active provider row
 * and asks the manager for attachment proof; this caller never guesses it.
 */
export const capture = (
  scope: McpInvocationScope,
  threadId: ThreadId,
): CapturedCaller | undefined =>
  scope.client === undefined && scope.thread?.threadId === threadId
    ? {
        mcpCredentialId: scope.thread.providerSessionId,
        providerInstanceId: scope.thread.providerInstanceId,
      }
    : undefined;

export const request = (owner: Ports, input: Request) =>
  owner.requestSelfSettlement === undefined
    ? Effect.fail(
        new OrchestratorDispatchError({
          commandId: input.commandId,
          commandType: "thread.metadata.update",
          cause: "Durable authenticated self-settlement is unavailable; no request was accepted.",
        }),
      )
    : owner.requestSelfSettlement(input);
