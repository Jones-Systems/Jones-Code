import type {
  OrchestrationV2ThreadLaunchWorkspaceStrategy,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import * as ProviderInstanceRegistry from "../../provider/ProviderInstanceRegistry.ts";
import * as ConversationPolicy from "./ConversationPolicy.ts";

type LaunchStrategy = OrchestrationV2ThreadLaunchWorkspaceStrategy;
type AutoStrategy = Extract<LaunchStrategy, { readonly type: "auto" }>;

/** How a launch's `auto` strategy resolved. Other strategies pass through unchanged. */
export interface AutoLaunchResolution {
  /** `auto` stays only for a conversation launch; otherwise it is recorded as an eager worktree. */
  readonly strategy: LaunchStrategy;
  /** The driver an eager fallback observed, reported once the thread exists. */
  readonly fallbackDriver: string | undefined;
}

/** Conversation launches bind after claim. Other auto launches prepare a worktree. */
export const make = Effect.gen(function* () {
  const conversations = yield* Effect.serviceOption(ConversationPolicy.ConversationPolicy);
  const providers = yield* Effect.serviceOption(ProviderInstanceRegistry.ProviderInstanceRegistry);

  const resolve = Effect.fn("AutoLaunchStrategy.resolve")(function* (input: {
    readonly strategy: LaunchStrategy;
    readonly eagerAllocation: boolean;
    readonly instanceId: ProviderInstanceId;
  }) {
    const strategy = input.strategy;
    if (strategy.type !== "auto") {
      const unchanged: AutoLaunchResolution = { strategy, fallbackDriver: undefined };
      return unchanged;
    }
    const instance = Option.isSome(providers)
      ? yield* providers.value.getInstance(input.instanceId)
      : undefined;
    const driver = instance?.driverKind ?? "unavailable";
    const conversation =
      !input.eagerAllocation &&
      Option.isSome(conversations) &&
      (driver === "codex" || driver === "claudeAgent");
    const resolution: AutoLaunchResolution = conversation
      ? { strategy, fallbackDriver: undefined }
      : { strategy: { ...strategy, type: "worktree" }, fallbackDriver: driver };
    return resolution;
  });

  /** Bounded diagnostic for an `auto` launch that fell back to an eager worktree. */
  const observeFallback = (resolution: AutoLaunchResolution, threadId: ThreadId) =>
    Option.isNone(conversations) || resolution.fallbackDriver === undefined
      ? Effect.void
      : ConversationPolicy.observeEagerFallback({
          threadId,
          driver: resolution.fallbackDriver,
          reason: "eager: setting or adapter proof unavailable",
        });

  const allocationInput = (
    strategy: AutoStrategy,
    target: Pick<
      ConversationPolicy.ConversationAllocationInput,
      "threadId" | "projectId" | "projectRoot"
    >,
  ): ConversationPolicy.ConversationAllocationInput => ({
    ...target,
    ...(strategy.baseRef === undefined ? {} : { baseRef: strategy.baseRef }),
  });

  const lookup = (threadId: ThreadId) =>
    Option.isNone(conversations) ? Effect.succeed(null) : conversations.value.lookup(threadId);
  const preflight = (input: ConversationPolicy.ConversationAllocationInput) =>
    Option.isNone(conversations) ? Effect.void : conversations.value.preflight(input);
  const allocate = (
    input: ConversationPolicy.ConversationAllocationInput,
    onUnboundFailure?: Effect.Effect<void>,
  ) =>
    Option.isNone(conversations)
      ? Effect.void
      : conversations.value.allocate(input, onUnboundFailure).pipe(Effect.asVoid);

  return {
    resolve,
    observeFallback,
    available: Option.isSome(conversations),
    allocationInput,
    lookup,
    preflight,
    allocate,
  };
});
