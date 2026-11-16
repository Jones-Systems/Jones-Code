import { EnvironmentId, ThreadId, type OrchestratorMcpFailure } from "@t3tools/contracts";
import { LinkedThreadsError, type ThreadEndpointRef } from "@t3tools/contracts/jones/linkedThreads";
import * as Effect from "effect/Effect";
import type { Caller } from "../../mcp/threadAccess.ts";
import { assertSameLink } from "../../mcp/linkOrigin.ts";
import { assertTargetWithinLimits } from "../../mcp/threadAccess.ts";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import { sameEndpoint } from "./identity.ts";

export type LinkActor =
  | { readonly type: "user" }
  | { readonly type: "thread"; readonly threadId: ThreadId; readonly caller?: Caller }
  | { readonly type: "client"; readonly sessionId: string; readonly caller: Caller }
  | {
      readonly type: "linked";
      readonly sessionId: string;
      readonly label: string;
      readonly origin: ThreadEndpointRef | null;
      readonly fenced: boolean;
      readonly caller?: Caller;
    };
export const actorFromCaller = (caller: Caller): LinkActor => {
  const scope = caller.scope;
  if (scope.client?.linked === true) {
    const bound = scope.client.remoteOrigin;
    return {
      type: "linked",
      sessionId: scope.client.sessionId,
      label: scope.client.label,
      origin:
        bound?.linkSessionId === scope.client.sessionId && bound.thread !== undefined
          ? {
              environmentId: EnvironmentId.make(bound.environmentId),
              threadId: ThreadId.make(bound.thread.threadId),
            }
          : null,
      fenced: scope.client.access !== "full-access",
      caller,
    };
  }
  if (scope.thread !== undefined)
    return { type: "thread", threadId: scope.thread.threadId, caller };
  return { type: "client", sessionId: scope.client?.sessionId ?? "unavailable", caller };
};
export const assertedBy = (actor: LinkActor) =>
  actor.type === "user"
    ? "user"
    : actor.type === "thread"
      ? `thread:${actor.threadId}`
      : actor.type === "linked"
        ? `link:${actor.sessionId}`
        : `client:${actor.sessionId}`;
export const originFromScope = (scope: McpInvocationScope): ThreadEndpointRef | undefined => {
  if (scope.thread !== undefined)
    return { environmentId: scope.environmentId, threadId: scope.thread.threadId };
  const bound = scope.client?.remoteOrigin;
  return scope.client?.linked === true &&
    bound?.linkSessionId === scope.client.sessionId &&
    bound.thread !== undefined
    ? {
        environmentId: EnvironmentId.make(bound.environmentId),
        threadId: ThreadId.make(bound.thread.threadId),
      }
    : undefined;
};
const forbidden = () =>
  new LinkedThreadsError({
    reason: "forbidden",
    message: "This caller cannot control the linked thread endpoints.",
  });
const checkSameLink = Effect.fn("LinkedThreads.assertSameLink")(function* (
  caller: Caller,
  target: Parameters<typeof assertSameLink>[1],
): Effect.fn.Return<void, OrchestratorMcpFailure> {
  yield* assertSameLink(caller, target);
}, Effect.mapError(forbidden));
export const authorize = Effect.fn("LinkedThreads.authorize")(function* (
  actor: LinkActor,
  here: EnvironmentId,
  endpoints: readonly ThreadEndpointRef[],
  options: { readonly claimTo?: ThreadId; readonly read?: boolean } = {},
): Effect.fn.Return<
  void,
  LinkedThreadsError,
  Threads.ThreadManagementService | ProjectionStore.ProjectionStoreV2
> {
  const local = endpoints.filter((endpoint) => endpoint.environmentId === here);
  if (local.length === 0)
    return yield* new LinkedThreadsError({
      reason: "endpoint_not_local",
      message: "At least one endpoint must belong to this environment.",
    });
  if (actor.type === "linked") {
    if (
      actor.origin === null ||
      (!options.read &&
        !endpoints.some(
          (endpoint) => endpoint.environmentId !== here && sameEndpoint(endpoint, actor.origin!),
        )) ||
      endpoints.some(
        (endpoint) => endpoint.environmentId !== here && !sameEndpoint(endpoint, actor.origin!),
      )
    )
      return yield* forbidden();
  }
  if (options.read) {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    for (const endpoint of local) {
      const thread = yield* projections
        .getThread(endpoint.threadId)
        .pipe(
          Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.succeed(null) }),
          Effect.mapError(forbidden),
        );
      if (thread === null || thread.deletedAt !== null)
        return yield* new LinkedThreadsError({
          reason: "endpoint_not_found",
          message: "A local thread endpoint was not found.",
        });
      if (
        actor.type === "linked" &&
        actor.fenced &&
        thread.linkOrigin?.sessionId !== actor.sessionId
      )
        return yield* forbidden();
    }
    return;
  }
  const threads = yield* Threads.ThreadManagementService;
  const isEndpoint =
    actor.type === "thread" &&
    endpoints.some(
      (endpoint) => endpoint.environmentId === here && endpoint.threadId === actor.threadId,
    );
  for (const endpoint of local) {
    const shell = yield* threads.getThreadShell(endpoint.threadId).pipe(Effect.mapError(forbidden));
    if (shell === null || shell.deletedAt !== null) {
      if (options.claimTo === endpoint.threadId) continue;
      return yield* new LinkedThreadsError({
        reason: "endpoint_not_found",
        message: "A local thread endpoint was not found.",
      });
    }
    if (actor.type === "linked" && actor.fenced && shell.linkOrigin?.sessionId !== actor.sessionId)
      return yield* forbidden();
    if (actor.type !== "user" && actor.caller !== undefined && !options.read)
      yield* checkSameLink(actor.caller, shell);
    if (actor.type === "user" || isEndpoint || options.read) continue;
    if (actor.caller !== undefined) {
      yield* assertTargetWithinLimits(actor.caller.limits, shell).pipe(Effect.mapError(forbidden));
      yield* checkSameLink(actor.caller, shell);
    } else if (actor.type === "thread") {
      const source = yield* threads.getThreadShell(actor.threadId).pipe(Effect.mapError(forbidden));
      if (source === null) return yield* forbidden();
      yield* assertTargetWithinLimits(source, shell).pipe(Effect.mapError(forbidden));
    }
  }
});
