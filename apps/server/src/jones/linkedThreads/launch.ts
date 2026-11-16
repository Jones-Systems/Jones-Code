import type { ThreadId } from "@t3tools/contracts";
import type { LinkedThreadOriginLink } from "@t3tools/contracts/jones/linkedThreads";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type { Caller } from "../../mcp/threadAccess.ts";
import { OrchestratorMcpFailure } from "@t3tools/contracts";
import { actorFromCaller, originFromScope } from "./authorization.ts";
import { linkedThreadIdentity } from "./identity.ts";
import * as Links from "./LinkedThreads.ts";

export const claimForCaller = Effect.fn("LinkedThreads.claimForCaller")(function* (
  caller: Caller,
  toThreadId: ThreadId,
  commandId: string,
  delegated = false,
): Effect.fn.Return<LinkedThreadOriginLink | undefined, OrchestratorMcpFailure> {
  const from = originFromScope(caller.scope);
  if (from === undefined || delegated) return undefined;
  // Optional service keeps legacy toolkit embeddings lineage-only; the application supplies the layer.
  const service = yield* Effect.serviceOption(Links.LinkedThreads);
  if (Option.isNone(service)) return undefined;
  const claimed = service.value
    .claimLaunch({
      from,
      toThreadId,
      launchCommandId: commandId,
      actor: actorFromCaller(caller),
      ...(caller.scope.client?.linked === true
        ? {
            remote: {
              environmentLabel: caller.scope.client.label,
              title: caller.scope.client.remoteOrigin?.thread?.title ?? null,
            },
          }
        : {}),
    })
    .pipe(
      Effect.mapError(
        (error) =>
          new OrchestratorMcpFailure({ code: "orchestration_error", message: error.message }),
      ),
    );
  const result = yield* claimed.pipe(Effect.result);
  if (result._tag === "Failure") {
    if (caller.scope.client?.linked !== true) return yield* result.failure;
    yield* Effect.logWarning("Incoming launch navigation copy was not recorded");
    return {
      linkId: linkedThreadIdentity("launch", from, {
        environmentId: caller.scope.environmentId,
        threadId: toThreadId,
      }).linkId,
      state: "not_recorded",
    };
  }
  const linkId = result.success;
  return {
    linkId,
    state: "recorded",
    ...(from.environmentId === caller.scope.environmentId ? { reverse: "local" as const } : {}),
  };
});
