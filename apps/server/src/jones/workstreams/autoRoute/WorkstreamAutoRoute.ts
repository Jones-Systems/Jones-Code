import { isListedThreadLineage, type ThreadId } from "@t3tools/contracts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ServerSettings from "../../../serverSettings.ts";
import * as Threads from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Agent from "../personal/agentService.ts";
import * as Personal from "../personal/service.ts";
import * as Store from "./AutoRouteStore.ts";
import * as Model from "./WorkstreamClassifierModel.ts";
import {
  AUTO_ROUTE_BOUNDS,
  buildClassifierPrompt,
  decide,
  gate,
  selectCandidates,
  type Decision,
} from "./policy.ts";

/** One accepted queue dispatch. The prompt text stays in memory and is never stored. */
export interface Observation {
  readonly threadId: ThreadId;
  readonly commandId: string;
  readonly messageId: string;
  readonly text: string;
}

/** The recorded outcome with the model's grades, when it was asked. */
export type RecordedOutcome = Decision & { readonly candidateCount?: number };

export type AutoRouteResult =
  | { readonly type: "disabled" }
  | { readonly type: "duplicate" }
  | { readonly type: "unrecorded"; readonly reason: "thread_missing" | "storage_unavailable" }
  | { readonly type: "decided"; readonly outcome: RecordedOutcome };

const AUTO_ROUTE_ORIGIN = "queue_http";

export class WorkstreamAutoRoute extends Context.Service<
  WorkstreamAutoRoute,
  {
    /** Enqueues only: never blocks or fails the dispatch that produced it. */
    readonly observe: (observation: Observation) => Effect.Effect<void>;
    readonly process: (observation: Observation) => Effect.Effect<AutoRouteResult>;
    readonly drain: Effect.Effect<void>;
  }
>()("t3/jones/workstreams/autoRoute/WorkstreamAutoRoute") {}

const make = Effect.gen(function* () {
  const store = yield* Store.make;
  const settingsService = yield* ServerSettings.ServerSettingsService;
  const threads = yield* Threads.ThreadManagementService;
  const personal = yield* Personal.PersonalWorkstreams;
  const agent = yield* Agent.AgentWorkstreams;
  const model = yield* Model.WorkstreamClassifierModel;
  const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));

  const classify = Effect.fn("WorkstreamAutoRoute.classify")(function* (
    observation: Observation,
    key: Store.DecisionKey,
  ): Effect.fn.Return<RecordedOutcome, unknown> {
    const shell = yield* threads.getThreadShell(observation.threadId);
    if (shell === null || shell.archivedAt !== null || shell.deletedAt != null)
      return { state: "skipped", code: "thread_unavailable" } as const;
    const read = yield* personal.readThread(observation.threadId).pipe(
      Effect.catchIf(
        (error) => error.code === "thread_not_found",
        () => Effect.succeed(null),
      ),
    );
    const blocked = gate({
      exists: read !== null,
      archivedOrDeleted: false,
      mainConversation: isListedThreadLineage(shell.lineage) && shell.delegatedFrom === undefined,
      assignment: read?.assignment ?? { workstreamId: null, revision: 0 },
      modelAttempts: yield* store.modelAttempts(key),
    });
    if (blocked !== null) return blocked;

    const settings = yield* settingsService.getSettings;
    const stats = yield* store.candidateStats({
      threadId: observation.threadId,
      projectId: shell.projectId,
    });
    const candidates = selectCandidates(
      (yield* personal.snapshot()).workstreams,
      settings.workstreamAutoRouteExcludedIds,
      stats,
    );
    if (candidates.length === 0) return { state: "abstained", code: "no_candidates" } as const;

    const prompt = buildClassifierPrompt({
      promptText: observation.text,
      threadTitle: shell.title,
      projectName: yield* store.projectName(shell.projectId),
      candidates,
      corrections: yield* store.corrections(observation.threadId, AUTO_ROUTE_BOUNDS.corrections),
    });
    const reply = yield* model
      .run({
        prompt,
        projectId: shell.projectId,
        onRequested: (requested) =>
          store
            .markModelRequested(key, { ...requested, candidateCount: candidates.length })
            .pipe(Effect.orDie),
      })
      .pipe(Effect.result);
    if (reply._tag === "Failure")
      return {
        state: "failed",
        code: reply.failure.category === "timeout" ? "model_timeout" : "model_unavailable",
        candidateCount: candidates.length,
      } as const;
    const decision: Decision = decide(reply.success.text, candidates);
    if (decision.state !== "assigned") return { ...decision, candidateCount: candidates.length };

    // Write-ahead intent, then a compare-and-swap that a concurrent human or agent move wins.
    yield* store.markAssigning(key, {
      workstreamId: decision.workstreamId,
      revision: 1,
      confidence: decision.confidence,
      basis: decision.basis,
    });
    const assigned = yield* agent
      .set({
        threadId: observation.threadId,
        workstreamId: decision.workstreamId,
        expectedRevision: 0,
      })
      .pipe(Effect.result);
    if (assigned._tag === "Success")
      return assigned.success.outcome === "applied"
        ? decision
        : ({ state: "skipped", code: "already_assigned" } as const);
    switch (assigned.failure.code) {
      case "precondition_failed":
        return { state: "skipped", code: "assignment_changed" } as const;
      case "workstream_not_found":
      case "workstream_deleted":
        return { state: "failed", code: "workstream_unavailable" } as const;
      case "thread_not_found":
        return { state: "skipped", code: "thread_unavailable" } as const;
      default: {
        // Unknown effect: the assignment's exact post-condition settles it.
        const current = yield* personal.readThread(observation.threadId).pipe(Effect.option);
        return current._tag === "Some" &&
          current.value.assignment.workstreamId === decision.workstreamId &&
          current.value.assignment.revision === 1
          ? decision
          : ({ state: "failed", code: "storage_unavailable" } as const);
      }
    }
  });

  const process = Effect.fn("WorkstreamAutoRoute.process")(function* (
    observation: Observation,
  ): Effect.fn.Return<AutoRouteResult> {
    const settings = yield* settingsService.getSettings.pipe(Effect.option);
    // Read per item, so a change applies to the next dispatch. Unreadable settings count as off.
    if (settings._tag === "None" || !settings.value.workstreamAutoRouteEnabled)
      return { type: "disabled" };
    const key = { threadId: observation.threadId, commandId: observation.commandId };
    const begun = yield* Effect.gen(function* () {
      if (!(yield* store.available)) return "storage_unavailable" as const;
      return yield* store.begin({
        key,
        messageId: observation.messageId,
        origin: AUTO_ROUTE_ORIGIN,
        now: yield* now,
      });
    }).pipe(Effect.orElseSucceed(() => "storage_unavailable" as const));
    if (begun === "duplicate") return { type: "duplicate" };
    if (begun !== "inserted") return { type: "unrecorded", reason: begun };
    const outcome: RecordedOutcome = yield* classify(observation, key).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("Workstream auto-route classification failed", cause).pipe(
          Effect.as<RecordedOutcome>({ state: "failed", code: "storage_unavailable" }),
        ),
      ),
    );
    yield* store
      .finalize(key, outcome, yield* now)
      .pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Workstream auto-route decision was not recorded", cause),
        ),
      );
    return { type: "decided", outcome };
  });

  const recovered = yield* now.pipe(
    Effect.flatMap(store.recoverInterrupted),
    Effect.catchCause((cause) =>
      Effect.logWarning("Workstream auto-route recovery failed", cause).pipe(
        Effect.as({ interrupted: 0, reconciled: 0 }),
      ),
    ),
  );
  if (recovered.interrupted + recovered.reconciled > 0)
    yield* Effect.logInfo("Workstream auto-route recovered interrupted decisions").pipe(
      Effect.annotateLogs(recovered),
    );
  const worker = yield* makeDrainableWorker((observation: Observation) =>
    Effect.asVoid(process(observation)),
  );
  return WorkstreamAutoRoute.of({
    observe: (observation) => Effect.asVoid(worker.enqueue(observation)),
    process,
    drain: worker.drain,
  });
});

/** Needs the SQL client, settings, threads, Workstreams and a classifier model. */
export const layerWorker = Layer.effect(WorkstreamAutoRoute, make).pipe(Layer.provide(Agent.layer));

/** Production composition with the text-generation classifier. */
export const layer = layerWorker.pipe(Layer.provide(Model.layer));
