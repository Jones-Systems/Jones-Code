import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type { OrchestrationV2DomainEvent } from "@t3tools/contracts";
import type { IntentCommand } from "../ordinaryCheckout/CheckoutIntent.ts";
import { digest } from "../ordinaryCheckout/CheckoutProtocol.ts";
import { LifecycleConfiguration } from "./NativeSource.ts";
import { LifecycleError } from "./Protocol.ts";
import { WorkspaceLifecycle, type NativeTransition } from "./WorkspaceLifecycle.ts";

export class OriginalNativeTransition extends Context.Service<
  OriginalNativeTransition,
  { readonly id: string; readonly requestDigest: string }
>()("t3/jones/workspaceLifecycle/NativeBoundary/OriginalNativeTransition") {}

export const withOriginalTransition = <A, E, R>(
  id: string,
  request: unknown,
  effect: Effect.Effect<A, E, R>,
) =>
  effect.pipe(
    Effect.provideService(OriginalNativeTransition, { id, requestDigest: digest(request) }),
  );

export const inheritOriginalTransition = Effect.fn("WorkspaceLifecycle.inheritOriginalTransition")(
  function* <A, E, R>(id: string, request: unknown, effect: Effect.Effect<A, E, R>) {
    const current = yield* Effect.serviceOption(OriginalNativeTransition);
    return yield* Option.isSome(current) ? effect : withOriginalTransition(id, request, effect);
  },
);

export const beforeUse = Effect.fn("WorkspaceLifecycle.beforeNativeUse")(function* (input: {
  readonly threadId: string;
  readonly boundary: NativeTransition["boundary"];
  readonly fallback?: { readonly id: string; readonly request: unknown };
}) {
  const service = yield* Effect.serviceOption(WorkspaceLifecycle);
  const configured = yield* Effect.serviceOption(LifecycleConfiguration);
  if (Option.isNone(service)) {
    if (Option.isSome(configured)) return yield* new LifecycleError({ reason: "unavailable" });
    return;
  }
  const inherited = yield* Effect.serviceOption(OriginalNativeTransition);
  const original =
    Option.getOrUndefined(inherited) ??
    (input.fallback === undefined
      ? undefined
      : {
          id: input.fallback.id,
          requestDigest: digest(input.fallback.request),
        });
  if (original === undefined) return yield* new LifecycleError({ reason: "identity" });
  yield* service.value.resume({
    transitionId: `${original.id}:${input.boundary}`,
    requestDigest: original.requestDigest,
    threadId: input.threadId,
    boundary: input.boundary,
  });
});

export const beforeCleanup = Effect.fn("WorkspaceLifecycle.beforeNativeCleanup")(function* (input: {
  readonly threadId: string;
  readonly boundary: "cleanup" | "settle-script";
  readonly id: string;
}) {
  const service = yield* Effect.serviceOption(WorkspaceLifecycle);
  const configured = yield* Effect.serviceOption(LifecycleConfiguration);
  if (Option.isNone(service)) {
    if (Option.isSome(configured)) return yield* new LifecycleError({ reason: "unavailable" });
    return;
  }
  yield* service.value.cleanupVeto({
    transitionId: input.id,
    requestDigest: digest(input),
    threadId: input.threadId,
    boundary: input.boundary,
  });
});

export const beforeCleanupPath = Effect.fn("WorkspaceLifecycle.beforeCleanupPath")(function* (
  worktree: string,
) {
  const service = yield* Effect.serviceOption(WorkspaceLifecycle);
  if (Option.isSome(service)) return yield* service.value.cleanupPathVeto(worktree);
  if (Option.isSome(yield* Effect.serviceOption(LifecycleConfiguration)))
    return yield* new LifecycleError({ reason: "unavailable" });
});

const commandTransitions = (
  command: IntentCommand,
  events: readonly OrchestrationV2DomainEvent[],
): readonly NativeTransition[] =>
  events.flatMap((event) =>
    event.type !== "thread.unsettled"
      ? []
      : [
          {
            transitionId: `command:${command.commandId}:${command.type === "thread.settlement.restore" ? "restore" : "unsettle"}`,
            requestDigest: digest(command),
            threadId: event.threadId,
            boundary:
              command.type === "thread.settlement.restore"
                ? ("restore" as const)
                : ("unsettle" as const),
          },
        ],
  );

export const beforeCommand = Effect.fn("WorkspaceLifecycle.beforeCommand")(function* (
  command: IntentCommand,
  events: readonly OrchestrationV2DomainEvent[],
) {
  const service = yield* Effect.serviceOption(WorkspaceLifecycle);
  if (Option.isNone(service)) {
    if (
      commandTransitions(command, events).length > 0 &&
      Option.isSome(yield* Effect.serviceOption(LifecycleConfiguration))
    )
      return yield* new LifecycleError({ reason: "unavailable" });
    return;
  }
  for (const transition of commandTransitions(command, events))
    yield* service.value.resume(transition);
});

// EventSink calls only this SQL validation inside its transaction; external registry I/O belongs to beforeCommand.
export const validateCommand = Effect.fn("WorkspaceLifecycle.validateCommand")(function* (
  command: IntentCommand | undefined,
  events: readonly OrchestrationV2DomainEvent[],
) {
  if (!events.some((e) => e.type === "thread.unsettled")) return;
  const service = yield* Effect.serviceOption(WorkspaceLifecycle);
  if (Option.isNone(service)) {
    if (Option.isSome(yield* Effect.serviceOption(LifecycleConfiguration)))
      return yield* new LifecycleError({ reason: "unavailable" });
    return;
  }
  if (!command) return yield* new LifecycleError({ reason: "identity" });
  for (const transition of commandTransitions(command, events))
    yield* service.value.validate(transition);
});
