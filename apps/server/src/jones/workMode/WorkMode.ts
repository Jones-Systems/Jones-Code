import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as Scheduler from "../../scheduling/Scheduler.ts";
import * as ServerSettings from "../../serverSettings.ts";
import { workModeCandidate, workModeTiming, type WorkModeTiming } from "./Policy.ts";

export class WorkMode extends Context.Service<
  WorkMode,
  {
    readonly sweep: Effect.Effect<void>;
    readonly start: Effect.Effect<void, never, Scope.Scope>;
  }
>()("t3/jones/workMode/WorkMode") {}

const make = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const settings = yield* ServerSettings.ServerSettingsService;
  const scheduler = yield* Scheduler.Scheduler;
  const timings = new Map<
    string,
    { readonly generation: string; readonly timing: WorkModeTiming | null }
  >();
  const sweep = Effect.gen(function* () {
    if (!(yield* settings.getSettings).workModeEnabled) {
      timings.clear();
      return;
    }
    const nowMs = DateTime.toEpochMillis(yield* DateTime.now);
    const snapshot = yield* projections.getShellSnapshot({
      location: "active",
      unsettledOnly: true,
    });
    const candidates = new Set<string>();
    for (const thread of snapshot.threads) {
      const candidate = workModeCandidate(thread, nowMs);
      if (candidate === null) continue;
      candidates.add(thread.id);
      let cached = timings.get(thread.id);
      if (cached?.generation !== candidate.generation) {
        const records = yield* projections.getThreadRecords(
          thread.id,
          ["providerTurns", "runs", "messages"],
          { messageRoles: ["user"] },
        );
        cached = {
          generation: candidate.generation,
          timing: workModeTiming(records, thread.activeProviderThreadId!),
        };
        timings.set(thread.id, cached);
      }
      const timing = cached.timing;
      if (
        timing === null ||
        nowMs < timing.lastActivityMs ||
        nowMs < timing.dueAtMs ||
        (timing.expiresAtMs !== null && nowMs > timing.expiresAtMs)
      )
        continue;
      const result = yield* orchestrator.requestWorkMode(candidate).pipe(
        Effect.provideService(ServerSettings.ServerSettingsService, settings),
        Effect.catchCause((cause) =>
          Effect.logWarning("Work Mode admission failed", { threadId: thread.id, cause }).pipe(
            Effect.as("skipped" as const),
          ),
        ),
      );
      if (result === "skipped") timings.delete(thread.id);
    }
    for (const threadId of timings.keys()) {
      if (!candidates.has(threadId)) timings.delete(threadId);
    }
  }).pipe(Effect.catchCause((cause) => Effect.logWarning("Work Mode sweep failed", { cause })));
  return WorkMode.of({ sweep, start: scheduler.register("jones-work-mode", sweep) });
});

export const layer = Layer.effect(WorkMode, make);
