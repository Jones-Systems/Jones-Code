import { expect, it } from "@effect/vitest";
import { MessageId, RunId, type OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import { WORK_MODE_INTERVAL_MS } from "@t3tools/shared/jones/workMode";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessions from "../../orchestration-v2/ProviderSessionManager.ts";
import type { ProviderAdapterV2SessionRuntime } from "../../orchestration-v2/ProviderAdapter.ts";
import * as ThreadCommands from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as ServerSettings from "../../serverSettings.ts";
import { workModeFixture } from "../workMode/Fixtures.testkit.ts";
import { admitWorkMode } from "../workMode/Admission.ts";
import { workModeCandidate, workModeCommand } from "../workMode/Policy.ts";
import * as Planned from "./PlannedUpdateContinuity.ts";
import { PlannedContinuityError } from "./PlannedUpdateContinuityService.ts";
import type { NativeOperationBinding } from "./launcherOperation.ts";

const operationId = "12345678-1234-4234-8234-123456789abc";
const binding: NativeOperationBinding = {
  baseDir: "/synthetic",
  dbPath: "/synthetic/userdata/statev2.sqlite",
  environmentId: "synthetic-environment",
  currentVersion: "0.0.0-preview.20261010.1.1",
  targetVersion: "0.0.0-preview.20261010.2.1",
  expectedInstalledSource: "a".repeat(40),
  targetSource: "b".repeat(40),
  stagedHandle: "synthetic-staged-handle",
};
const proof = (outcome: "committed" | "rolled-back" = "committed"): Planned.PlannedUpdateProof => ({
  operationId,
  binding,
  outcome,
  current: {
    baseDir: binding.baseDir,
    dbPath: binding.dbPath,
    environmentId: binding.environmentId,
    activeVersion: outcome === "committed" ? binding.targetVersion : binding.currentVersion,
    activeSourceSha:
      outcome === "committed" ? binding.targetSource : binding.expectedInstalledSource,
  },
});
const unexpected = () => Effect.die("Unexpected provider effect in continuity metadata test");

function harness(initial = workModeFixture(), options: { enabled?: boolean; live?: boolean } = {}) {
  let projection = initial;
  let live = options.live ?? true;
  const session = initial.providerSessions[0]!;
  const runtime: ProviderAdapterV2SessionRuntime = {
    instanceId: session.providerInstanceId,
    driver: session.driver,
    providerSessionId: session.id,
    providerSession: session,
    events: Stream.empty,
    ensureThread: unexpected,
    resumeThread: unexpected,
    startTurn: unexpected,
    steerTurn: unexpected,
    interruptTurn: unexpected,
    respondToRuntimeRequest: unexpected,
    readThreadSnapshot: unexpected,
    rollbackThread: unexpected,
    forkThread: unexpected,
  };
  return {
    get projection() {
      return projection;
    },
    set projection(value: OrchestrationV2ThreadProjection) {
      projection = value;
    },
    set live(value: boolean) {
      live = value;
    },
    layer: Layer.mergeAll(
      Sqlite.layerMemory,
      ThreadCommands.layer,
      ServerSettings.layerTest({
        workModeEnabled: true,
        continueThreadsAfterServerUpdate: options.enabled ?? true,
      }).pipe(Layer.orDie),
      Layer.mock(ProjectionStore.ProjectionStoreV2)({
        getRecoveryThreadIds: () =>
          Effect.sync(() =>
            projection.runs.some((run) => run.status === "queued") ? [projection.thread.id] : [],
          ),
        getShellSnapshot: () =>
          Effect.sync(() => ({
            schemaVersion: 2,
            snapshotSequence: 0,
            threads: [ProjectionStore.threadShellFromProjection(projection)],
            archivedThreads: [],
          })),
        getThreadProviderContext: () => Effect.sync(() => projection),
        getThreadProjection: () => Effect.sync(() => projection),
      }),
      Layer.mock(ProviderSessions.ProviderSessionManagerV2)({
        get: () => Effect.sync(() => (live ? Option.some(runtime) : Option.none())),
      }),
    ),
  };
}
const receipt = (commandId: string, commandType: string, threadId: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_command_receipts(command_id,aggregate_kind,aggregate_id,command_type,accepted_at,result_sequence,status,error)
    VALUES(${commandId},'thread',${threadId},${commandType},'1970-01-01T00:00:00.000Z',0,'accepted',NULL)`;
  });

it.effect(
  "admits captured cold Work Mode at its original due time once and honors a later Stop",
  () => {
    const state = harness();
    return Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const threadId = state.projection.thread.id;
      yield* receipt("capture-anchor", "thread.visit", threadId);
      const continuity = yield* Planned.make;
      yield* continuity.capture({ operationId, binding, continueRunningThreads: false });
      state.live = false;
      state.projection = {
        ...state.projection,
        providerSessions: state.projection.providerSessions.map((session) => ({
          ...session,
          status: "stopped" as const,
        })),
      };
      yield* continuity.activate(proof());
      const candidate = workModeCandidate(
        ProjectionStore.threadShellFromProjection(state.projection),
        0,
        { ignoreInterval: true },
      )!;
      let dispatched = 0;
      const admission = () =>
        admitWorkMode(candidate, {
          hasReceipt: Effect.succeed(false),
          getProviderContext: Effect.sync(() => state.projection),
          getProjection: Effect.sync(() => state.projection),
          hasLiveSession: () => Effect.succeed(false),
          hasPlannedContext: continuity.hasWork(threadId),
          admitPlannedContext: (projection, current, now) =>
            continuity.admitWork(projection, current, now),
          finishPlannedContext: continuity.finishWork(threadId),
          dispatch: (command) =>
            receipt(command.commandId, command.type, threadId).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
              Effect.mapError((cause) => new PlannedContinuityError({ cause })),
              Effect.tap(() =>
                Effect.sync(() => {
                  dispatched += 1;
                }),
              ),
            ),
        });
      yield* TestClock.adjust(WORK_MODE_INTERVAL_MS - 1);
      expect(yield* admission()).toBe("skipped");
      expect(dispatched).toBe(0);
      yield* TestClock.adjust(1);
      expect(yield* admission()).toBe("dispatched");
      expect(dispatched).toBe(1);
      expect(yield* admission()).toBe("skipped");
      expect(yield* continuity.allowWorkStart(threadId, workModeCommand(candidate).messageId)).toBe(
        true,
      );
      yield* receipt("later-noop-stop", "thread.runtime.stop", threadId);
      expect(yield* continuity.allowWorkStart(threadId, workModeCommand(candidate).messageId)).toBe(
        false,
      );
      const restarted = yield* Planned.make;
      yield* restarted.activate(proof());
      expect(yield* restarted.hasWork(threadId)).toBe(false);
    }).pipe(Effect.provide(state.layer));
  },
);

it.effect(
  "holds an exact captured queue after later user control and refuses an unverified rollback source",
  () => {
    const initial = workModeFixture();
    const queued = {
      ...initial.runs[0]!,
      id: RunId.make("queued"),
      ordinal: 2,
      queuePosition: 1,
      userMessageId: MessageId.make("queued-message"),
      status: "queued" as const,
      startedAt: null,
      completedAt: null,
    };
    const state = harness({
      ...initial,
      runs: [...initial.runs, queued],
      messages: [
        ...initial.messages,
        {
          ...initial.messages[0]!,
          id: queued.userMessageId,
          runId: queued.id,
          text: "Queued work",
        },
      ],
    });
    return Effect.gen(function* () {
      const threadId = state.projection.thread.id;
      yield* receipt("capture-anchor", "thread.visit", threadId);
      const continuity = yield* Planned.make;
      yield* continuity.capture({ operationId, binding, continueRunningThreads: false });
      const wrong = proof("rolled-back");
      expect(
        (yield* Effect.result(
          continuity.activate({
            ...wrong,
            current: { ...wrong.current, activeSourceSha: "f".repeat(40) },
          }),
        ))._tag,
      ).toBe("Failure");
      expect(yield* continuity.queueThreadIds).toEqual([]);
      yield* continuity.activate(proof("rolled-back"));
      state.projection = {
        ...state.projection,
        runs: state.projection.runs.map((run) =>
          run.status === "queued" ? { ...run, queueHeld: true } : run,
        ),
      };
      expect(yield* continuity.queueCommand(state.projection)).toBe(
        `command:planned-update-queue:${operationId}:${threadId}`,
      );
      yield* receipt("later-noop-hold", "queue.hold", threadId);
      expect(yield* continuity.queueCommand(state.projection)).toBeUndefined();
    }).pipe(Effect.provide(state.layer));
  },
);

it.effect("does not capture disabled continuation or evicted Work Mode sessions", () => {
  const state = harness(undefined, { enabled: false, live: false });
  return Effect.gen(function* () {
    const continuity = yield* Planned.make;
    yield* continuity.capture({ operationId, binding, continueRunningThreads: false });
    yield* continuity.activate(proof());
    expect(yield* continuity.queueThreadIds).toEqual([]);
    expect(yield* continuity.hasWork(state.projection.thread.id)).toBe(false);
  }).pipe(Effect.provide(state.layer));
});

it.effect("does not recapture a completed live conversation after a preexisting Stop", () => {
  const state = harness();
  return Effect.gen(function* () {
    yield* receipt("previous-stop", "provider-session.detach", state.projection.thread.id);
    const continuity = yield* Planned.make;
    yield* continuity.capture({ operationId, binding, continueRunningThreads: true });
    yield* continuity.activate(proof());
    expect(yield* continuity.hasWork(state.projection.thread.id)).toBe(false);
  }).pipe(Effect.provide(state.layer));
});
