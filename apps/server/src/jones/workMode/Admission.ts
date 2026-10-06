import type { OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as ServerSettings from "../../serverSettings.ts";
import {
  threadShellFromProjection,
  type ProjectionThreadProviderContext,
} from "../../orchestration-v2/ProjectionStore.ts";
import {
  workModeCandidate,
  workModeCommand,
  workModeContext,
  type WorkModeAdmissionResult,
  type WorkModeCandidate,
} from "./Policy.ts";

// The orchestrator owns the thread lock and supplies its unlocked receipt dispatcher.
export function admitWorkMode<E>(
  candidate: WorkModeCandidate,
  dependencies: {
    readonly hasReceipt: Effect.Effect<boolean, E>;
    readonly getProviderContext: Effect.Effect<ProjectionThreadProviderContext, E>;
    readonly getProjection: Effect.Effect<OrchestrationV2ThreadProjection, E>;
    readonly hasLiveSession: (
      owner: NonNullable<ReturnType<typeof workModeContext>>,
    ) => Effect.Effect<boolean, E>;
    readonly dispatch: (command: ReturnType<typeof workModeCommand>) => Effect.Effect<unknown, E>;
  },
): Effect.Effect<
  WorkModeAdmissionResult,
  E | import("@t3tools/contracts").ServerSettingsError,
  ServerSettings.ServerSettingsService
> {
  return Effect.gen(function* () {
    if (yield* dependencies.hasReceipt) {
      yield* dependencies.dispatch(workModeCommand(candidate));
      return "acknowledged" as const;
    }
    const settings = yield* ServerSettings.ServerSettingsService;
    return yield* settings.withSettingsSnapshot((preferences) =>
      Effect.gen(function* () {
        if (!preferences.workModeEnabled) return "skipped" as const;
        // Idle eviction leaves old shells due; reject those before reading their histories.
        const context = yield* dependencies.getProviderContext;
        const currentOwner = context.providerThreads.find(
          (thread) => thread.id === context.thread.activeProviderThreadId,
        );
        if (
          currentOwner?.providerSessionId == null ||
          currentOwner.status !== "idle" ||
          currentOwner.nativeThreadRef == null ||
          !(yield* dependencies.hasLiveSession(currentOwner))
        )
          return "skipped" as const;
        const projection = yield* dependencies.getProjection;
        const nowMs = DateTime.toEpochMillis(yield* DateTime.now);
        const fresh = workModeCandidate(threadShellFromProjection(projection), nowMs);
        if (fresh === null || fresh.generation !== candidate.generation) return "skipped" as const;
        const owner = workModeContext(projection, nowMs);
        if (owner === null || !(yield* dependencies.hasLiveSession(owner)))
          return "skipped" as const;
        yield* dependencies.dispatch(workModeCommand(candidate, projection.thread.modelSelection));
        return "dispatched" as const;
      }),
    );
  });
}
