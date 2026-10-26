import type { OrchestrationV2ThreadShellSnapshot } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
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
  const events = yield* EventStore.EventStoreV2;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const settings = yield* ServerSettings.ServerSettingsService;
  const scheduler = yield* Scheduler.Scheduler;
  let snapshot: OrchestrationV2ThreadShellSnapshot | undefined;
  let refreshPending = true;
  const admitted = new Map<string, string>();
  const timings = new Map<
    string,
    { readonly generation: string; readonly timing: WorkModeTiming | null }
  >();
  const sweep = Effect.gen(function* () {
    if (!(yield* settings.getSettings).workModeEnabled) {
      timings.clear();
      admitted.clear();
      snapshot = undefined;
      refreshPending = true;
      return;
    }
    const sequence = yield* events.latestSequence().pipe(NodeSqliteClient.readOnly);
    if (snapshot === undefined || refreshPending || snapshot.snapshotSequence !== sequence) {
      refreshPending = true;
      const refreshed = yield* projections
        .getShellSnapshot({ location: "active", unsettledOnly: true })
        .pipe(NodeSqliteClient.readOnly);
      // Rows and their returned watermark are one publication, after decode succeeds.
      snapshot = refreshed;
      refreshPending = false;
      const present = new Set<string>(refreshed.threads.map((thread) => thread.id));
      for (const threadId of admitted.keys()) {
        if (!present.has(threadId)) admitted.delete(threadId);
      }
    }
    const nowMs = DateTime.toEpochMillis(yield* DateTime.now);
    const candidates = new Set<string>();
    for (const thread of snapshot.threads) {
      const candidate = workModeCandidate(thread, nowMs);
      if (candidate === null) continue;
      candidates.add(thread.id);
      if (admitted.get(thread.id) === candidate.generation) continue;
      let cached = timings.get(thread.id);
      if (cached?.generation !== candidate.generation) {
        const records = yield* projections
          .getThreadRecords(thread.id, ["providerTurns", "runs", "messages"], {
            messageRoles: ["user"],
          })
          .pipe(NodeSqliteClient.readOnly);
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
      else admitted.set(thread.id, candidate.generation);
    }
    for (const threadId of timings.keys()) {
      if (!candidates.has(threadId)) timings.delete(threadId);
    }
  }).pipe(
    Effect.onExit((exit) =>
      Effect.sync(() => {
        if (Exit.isFailure(exit)) refreshPending = true;
      }),
    ),
    Effect.catchCause((cause) => Effect.logWarning("Work Mode sweep failed", { cause })),
  );
  return WorkMode.of({ sweep, start: scheduler.register("jones-work-mode", sweep) });
});

export const layer = Layer.effect(WorkMode, make);
