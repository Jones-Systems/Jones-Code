import { assert, it } from "@effect/vitest";
import {
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ServerSettings from "../serverSettings.ts";
import {
  restartContinuationRun,
  continueRestartedRun,
  capturedRestartContinuationIds,
  type RestartContinuationInput,
} from "./RestartContinuation.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ProviderRuntimeRecovery from "./ProviderRuntimeRecoveryService.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";

const threadId = ThreadId.make("thread:restart");
const runId = RunId.make("run:restart");
const instanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const providerThreadId = ProviderThreadId.make("provider-thread:restart");
const sessionId = ProviderSessionId.make("session:restart");
const attemptId = RunAttemptId.make("attempt:restart");
const sourceAttempt: OrchestrationV2RunAttempt = {
  id: attemptId,
  runId,
  attemptOrdinal: 1,
  rootNodeId: NodeId.make("node:restart"),
  providerInstanceId: instanceId,
  providerThreadId,
  providerTurnId: ProviderTurnId.make("turn:restart"),
  reason: "initial",
  status: "running",
  startedAt: null,
  completedAt: null,
};
// An uncaptured continuation never reads markers or held effects; any access dies.
const uncapturedContinuationServices = Layer.merge(
  Layer.mock(EventSink.EventSinkV2)({}),
  Layer.mock(EffectOutbox.EffectOutboxV2)({}),
);

it("keeps captured restart message identity stable for one source across markers and effects", () => {
  const marker: EventSink.RestartContinuationMarkerV2 = {
    markerId: "marker:restart-first",
    threadId,
    projectId: ProjectId.make("restart-project"),
    sourceRunId: runId,
    sourceRunAttemptId: attemptId,
    evidenceRevision: 1,
    createdAt: "2026-10-03T12:00:00.000Z",
    binding: {
      threadId,
      providerThreadId,
      providerSessionId: sessionId,
      instanceId,
      driver,
      nativeThreadId: "native-thread",
      runtimeGeneration: "restart-native-incarnation",
    },
  };
  const first = capturedRestartContinuationIds({ marker, effectId: "effect:restart-first" });
  const second = capturedRestartContinuationIds({
    marker: { ...marker, markerId: "marker:restart-second", createdAt: "2026-10-03T12:01:00.000Z" },
    effectId: "effect:restart-second",
  });
  assert.notEqual(first.commandId, second.commandId);
  assert.equal(first.messageId, second.messageId);
  assert.equal(first.messageId, `message:restart-continuation:${runId}`);
  assert.notEqual(
    first.messageId,
    capturedRestartContinuationIds({
      marker: { ...marker, sourceRunId: RunId.make("run:restart-later") },
      effectId: "effect:restart-later",
    }).messageId,
  );
});

function makeProjection() {
  return {
    thread: {
      id: threadId,
      projectId: ProjectId.make("restart-project"),
      providerInstanceId: instanceId,
      archivedAt: null,
      deletedAt: null,
    },
    runs: [
      {
        id: runId,
        ordinal: 1,
        providerInstanceId: instanceId,
        modelSelection: { instanceId, model: "gpt-6" },
        providerThreadId,
        activeAttemptId: attemptId,
        status: "running",
      },
    ],
    providerThreads: [
      {
        id: providerThreadId,
        appThreadId: threadId,
        ownerNodeId: null,
        driver,
        providerInstanceId: instanceId,
        providerSessionId: sessionId,
        nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
        status: "active",
      },
    ],
    providerSessions: [
      { id: sessionId, driver, providerInstanceId: instanceId, status: "running" },
    ],
    providerTurns: [
      {
        id: ProviderTurnId.make("turn:restart"),
        providerThreadId,
        runAttemptId: attemptId,
        status: "running",
      },
    ],
    runtimeRequests: [],
    attempts: [],
    nodes: [],
    subagents: [],
    messages: [],
    turnItems: [],
  } as unknown as OrchestrationV2ThreadProjection;
}

it("requires matching saved native state for an unfinished root run", () => {
  const projection = makeProjection();
  assert.equal(restartContinuationRun(projection)?.id, runId);
  for (const invalid of [
    { ...projection, thread: { ...projection.thread, archivedAt: {} } },
    { ...projection, thread: { ...projection.thread, deletedAt: {} } },
    {
      ...projection,
      thread: { ...projection.thread, providerInstanceId: ProviderInstanceId.make("other") },
    },
    {
      ...projection,
      providerThreads: [{ ...projection.providerThreads[0]!, nativeThreadRef: null }],
    },
    {
      ...projection,
      providerSessions: [
        {
          ...projection.providerSessions[0]!,
          providerInstanceId: ProviderInstanceId.make("other"),
        },
      ],
    },
    { ...projection, providerTurns: [] },
    ...[
      "queued",
      "preparing",
      "starting",
      "waiting",
      "completed",
      "cancelled",
      "failed",
      "interrupted",
    ].map((status) => ({ ...projection, runs: [{ ...projection.runs[0]!, status }] })),
  ])
    assert.isUndefined(restartContinuationRun(invalid as OrchestrationV2ThreadProjection));
});

it("continues a live turn whose session the adapter never marked running", () => {
  const projection = makeProjection();
  // Codex, Claude, Cursor and ACP sessions stay "ready" for their whole life.
  const withSessionStatus = (status: string) =>
    ({
      ...projection,
      providerSessions: [{ ...projection.providerSessions[0]!, status }],
    }) as OrchestrationV2ThreadProjection;
  for (const status of ["starting", "ready", "running", "waiting"])
    assert.equal(restartContinuationRun(withSessionStatus(status))?.id, runId, status);
  for (const status of ["stopped", "error"])
    assert.isUndefined(restartContinuationRun(withSessionStatus(status)), status);
});

it("recovers an admitted continuation after another crash before provider start", () => {
  const projection = makeProjection();
  const starting = {
    ...projection,
    runs: [
      {
        ...projection.runs[0]!,
        status: "starting" as const,
        restartContinuationOfRunId: RunId.make("run:previous-crash"),
      },
    ],
    providerThreads: [{ ...projection.providerThreads[0]!, status: "idle" as const }],
    providerSessions: [{ ...projection.providerSessions[0]!, status: "stopped" as const }],
    providerTurns: [],
  };
  assert.equal(restartContinuationRun(starting)?.id, runId);
});

it("continues a settled root run only when the restart cancelled its background work", () => {
  const projection = makeProjection();
  const settled = {
    ...projection,
    runs: [{ ...projection.runs[0]!, status: "completed" as const }],
    providerThreads: [{ ...projection.providerThreads[0]!, status: "idle" as const }],
    providerSessions: [{ ...projection.providerSessions[0]!, status: "stopped" as const }],
    providerTurns: [],
  };
  const lostWork = new Set([providerThreadId]);
  assert.isUndefined(restartContinuationRun(settled));
  assert.equal(restartContinuationRun(settled, lostWork)?.id, runId);
  // Work an older provider thread launched (before a provider switch) is not
  // this run's: its provider was never told about it and cannot continue it.
  assert.isUndefined(
    restartContinuationRun(settled, new Set([ProviderThreadId.make("provider-thread:claude")])),
  );
  for (const invalid of [
    { ...settled, thread: { ...settled.thread, archivedAt: {} } },
    { ...settled, thread: { ...settled.thread, deletedAt: {} } },
    { ...settled, runs: [{ ...settled.runs[0]!, status: "failed" as const }] },
    {
      ...settled,
      providerThreads: [{ ...settled.providerThreads[0]!, nativeThreadRef: null }],
    },
  ])
    assert.isUndefined(
      restartContinuationRun(invalid as OrchestrationV2ThreadProjection, lostWork),
    );
});

it.effect("prompts a settled thread's continuation with the note of its lost work", () =>
  Effect.gen(function* () {
    const base = makeProjection();
    const work = [{ kind: "shell" as const, label: "sleep 25 && echo DONE" }];
    const projection = {
      ...base,
      runs: [{ ...base.runs[0]!, status: "completed", restartCancelledBackgroundWork: work }],
      providerTurns: [{ ...base.providerTurns[0]!, status: "completed" }],
    } as unknown as OrchestrationV2ThreadProjection;
    const commands: Parameters<
      ThreadManagementService.ThreadManagementService["Service"]["dispatch"]
    >[0][] = [];
    yield* continueRestartedRun({ threadId, sourceRunId: runId }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.mock(ThreadManagementService.ThreadManagementService)({
            getThreadRecords: () => Effect.succeed(projection),
            dispatch: (command) => {
              commands.push(command);
              return Effect.succeed({} as never);
            },
          }),
          ServerSettings.layerTest({ continueThreadsAfterServerUpdate: true }),
          uncapturedContinuationServices,
        ),
      ),
    );
    assert.lengthOf(commands, 1);
    const command = commands[0]!;
    assert.equal(
      command.type === "message.dispatch" ? command.restartContinuationOfRunId : null,
      runId,
    );
    assert.include(
      command.type === "message.dispatch" ? command.text : "",
      "sleep 25 && echo DONE",
    );
    assert.notInclude(command.type === "message.dispatch" ? command.text : "", "Continue where");
  }),
);

it.effect("does not continue a failed run that lost background work", () =>
  Effect.gen(function* () {
    const base = makeProjection();
    const projection = {
      ...base,
      runs: [
        {
          ...base.runs[0]!,
          status: "failed",
          restartCancelledBackgroundWork: [{ kind: "shell" as const, label: "sleep 25" }],
        },
      ],
      providerTurns: [{ ...base.providerTurns[0]!, status: "failed" }],
    } as unknown as OrchestrationV2ThreadProjection;
    const commands: Parameters<
      ThreadManagementService.ThreadManagementService["Service"]["dispatch"]
    >[0][] = [];
    yield* continueRestartedRun({ threadId, sourceRunId: runId }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.mock(ThreadManagementService.ThreadManagementService)({
            getThreadRecords: () => Effect.succeed(projection),
            dispatch: (command) => {
              commands.push(command);
              return Effect.succeed({} as never);
            },
          }),
          ServerSettings.layerTest({ continueThreadsAfterServerUpdate: true }),
          uncapturedContinuationServices,
        ),
      ),
    );
    assert.lengthOf(commands, 0);
  }),
);

for (const [enabled, projectOverride] of [
  [false, undefined],
  [true, undefined],
  [false, true],
  [true, false],
] as const) {
  it.effect(
    `atomically records restart intent with cancellation when opt-in is ${enabled} and project override is ${projectOverride}`,
    () =>
      Effect.gen(function* () {
        let committed: Parameters<EventSink.EventSinkV2["Service"]["commitCommand"]>[0] | undefined;
        const recovery = yield* ProviderRuntimeRecovery.make.pipe(
          Effect.provide(
            Layer.mergeAll(
              ServerSettings.layerTest({
                continueThreadsAfterServerUpdate: enabled,
                projectSettingsOverrides:
                  projectOverride === undefined
                    ? {}
                    : {
                        [ProjectId.make("restart-project")]: {
                          continueThreadsAfterServerUpdate: projectOverride,
                        },
                      },
              }),
              Layer.mock(ProjectionStore.ProjectionStoreV2)({
                getRecoveryThreadIds: () => Effect.succeed([threadId]),
                getRuntimeRecoveryProjection: () => Effect.succeed(makeProjection()),
              }),
              Layer.mock(EventSink.EventSinkV2)({
                readDormantRestartContinuations: Effect.succeed([]),
                commitCommand: (input) => {
                  committed = input;
                  return Effect.succeed({ committed: true, cancelledEffectCount: 1 } as never);
                },
              }),
              IdAllocator.layer,
              Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({
                runRecoveryOnce: Effect.succeed(false),
              }),
              Layer.mock(EffectOutbox.EffectOutboxV2)({
                listHeldByThreadId: () => Effect.succeed([]),
                reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
              }),
              Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
            ),
          ),
        );
        yield* recovery.reconcile("startup");
        assert.isDefined(committed);
        assert.isTrue(
          committed!.events.some(
            (event) => event.type === "run.updated" && event.payload.status === "cancelled",
          ),
        );
        assert.lengthOf(committed!.effects, (projectOverride ?? enabled) ? 1 : 0);
        if (projectOverride ?? enabled)
          assert.deepEqual(committed!.effects[0]?.request, {
            type: "provider-runtime.continue",
            sourceRunId: runId,
          });
      }),
  );
}

it.effect("delivers a released captured continuation when latest project preferences are off", () =>
  Effect.gen(function* () {
    const projection = makeProjection();
    const binding: EventSink.ProviderBindingExpectationV2 = {
      threadId,
      providerThreadId,
      providerSessionId: sessionId,
      instanceId,
      driver,
      nativeThreadId: "native-thread",
      runtimeGeneration: "captured-actual-generation",
    };
    const marker: EventSink.RestartContinuationMarkerV2 = {
      markerId: "captured-preference-marker",
      threadId,
      projectId: projection.thread.projectId,
      sourceRunId: runId,
      sourceRunAttemptId: attemptId,
      binding,
      evidenceRevision: 2,
      createdAt: "2026-10-03T12:00:00Z",
    };
    const effectId = "captured-restart-effect";
    let markerReads = 0;
    let dispatched = 0;
    const current = {
      ...projection,
      thread: {
        ...projection.thread,
        activeProviderThreadId: providerThreadId,
        modelSelection: projection.runs[0]!.modelSelection,
      },
      runs: [{ ...projection.runs[0]!, status: "cancelled" }],
      attempts: [sourceAttempt],
    } as OrchestrationV2ThreadProjection;
    yield* continueRestartedRun({
      threadId,
      sourceRunId: runId,
      capturedContinuation: {
        effectId,
        marker,
        workerId: "worker:captured-restart",
        expectedAttempt: 2,
      },
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          ServerSettings.layerTest({ continueThreadsAfterServerUpdate: false }),
          Layer.mock(ThreadManagementService.ThreadManagementService)({
            getThreadRecords: () => Effect.succeed(current),
            dispatch: () => Effect.die("Captured continuation must use its private dispatch port."),
            dispatchRestartContinuation: (command, context) =>
              Effect.sync(() => {
                assert.deepEqual(context, {
                  effectId,
                  marker,
                  workerId: "worker:captured-restart",
                  expectedAttempt: 2,
                });
                assert.equal(
                  command.commandId,
                  EventSink.capturedRestartContinuationIdsV1({ effectId, marker }).commandId,
                );
                assert.equal(command.type, "message.dispatch");
                if (command.type === "message.dispatch")
                  assert.equal(command.restartContinuationOfRunId, runId);
                dispatched++;
                return {} as never;
              }),
          }),
          Layer.mock(EventSink.EventSinkV2)({
            readReleasedRestartContinuation: (input) =>
              Effect.sync(() => {
                assert.deepEqual(input, { effectId, threadId, sourceRunId: runId });
                markerReads++;
                return marker;
              }),
            readProviderRuntimeEvidence: () =>
              Effect.succeed({
                binding,
                evidenceRevision: 2,
                observation: null,
                registeredAt: marker.createdAt,
              }),
          }),
          Layer.mock(EffectOutbox.EffectOutboxV2)({ listHeldByThreadId: () => Effect.succeed([]) }),
        ),
      ),
    );
    assert.equal(dispatched, 1);
    assert.isAtLeast(markerReads, 1);
  }),
);

it.effect("delivers a released native-only settled continuation without saved task labels", () =>
  Effect.gen(function* () {
    for (const status of ["completed", "waiting"] as const) {
      const projection = makeProjection();
      const binding: EventSink.ProviderBindingExpectationV2 = {
        threadId,
        providerThreadId,
        providerSessionId: sessionId,
        instanceId,
        driver,
        nativeThreadId: "native-thread",
        runtimeGeneration: "captured-native-only-generation",
      };
      const marker: EventSink.RestartContinuationMarkerV2 = {
        markerId: `native-only-settled-${status}`,
        threadId,
        projectId: projection.thread.projectId,
        sourceRunId: runId,
        sourceRunAttemptId: attemptId,
        binding,
        evidenceRevision: 2,
        createdAt: "2026-10-03T12:00:00Z",
      };
      const effectId = `released-native-only-${status}`;
      let dispatched = 0;
      let markerReads = 0;
      const current = {
        ...projection,
        thread: {
          ...projection.thread,
          activeProviderThreadId: providerThreadId,
          modelSelection: projection.runs[0]!.modelSelection,
        },
        runs: [{ ...projection.runs[0]!, status }],
        providerTurns: [{ ...projection.providerTurns[0]!, status: "completed" }],
        attempts: [sourceAttempt],
      } as OrchestrationV2ThreadProjection;
      yield* continueRestartedRun({
        threadId,
        sourceRunId: runId,
        capturedContinuation: {
          effectId,
          marker,
          workerId: "worker:captured-restart",
          expectedAttempt: 2,
        },
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            ServerSettings.layerTest({ continueThreadsAfterServerUpdate: false }),
            Layer.mock(ThreadManagementService.ThreadManagementService)({
              getThreadRecords: () => Effect.succeed(current),
              dispatch: () =>
                Effect.die("Captured continuation must use its private dispatch port."),
              dispatchRestartContinuation: (command, context) =>
                Effect.sync(() => {
                  assert.deepEqual(context, {
                    effectId,
                    marker,
                    workerId: "worker:captured-restart",
                    expectedAttempt: 2,
                  });
                  assert.equal(
                    command.commandId,
                    EventSink.capturedRestartContinuationIdsV1({ effectId, marker }).commandId,
                  );
                  assert.equal(command.type, "message.dispatch");
                  if (command.type === "message.dispatch") {
                    assert.equal(command.text, "Continue where you left off.");
                    assert.equal(command.restartContinuationOfRunId, runId);
                  }
                  dispatched++;
                  return {} as never;
                }),
            }),
            Layer.mock(EventSink.EventSinkV2)({
              readReleasedRestartContinuation: (input) =>
                Effect.sync(() => {
                  assert.deepEqual(input, { effectId, threadId, sourceRunId: runId });
                  markerReads++;
                  return marker;
                }),
              readProviderRuntimeEvidence: () =>
                Effect.succeed({
                  binding,
                  evidenceRevision: 2,
                  observation: null,
                  registeredAt: marker.createdAt,
                }),
            }),
            Layer.mock(EffectOutbox.EffectOutboxV2)({
              listHeldByThreadId: () => Effect.succeed([]),
            }),
          ),
        ),
      );
      assert.equal(dispatched, 1);
      assert.isAtLeast(markerReads, 1);
    }
  }),
);

function capturedDispatchFixture(status: "completed" | "cancelled" = "completed") {
  const base = makeProjection();
  const binding: EventSink.ProviderBindingExpectationV2 = {
    threadId,
    providerThreadId,
    providerSessionId: sessionId,
    instanceId,
    driver,
    nativeThreadId: "native-thread",
    runtimeGeneration: "released-captured-generation",
  };
  const marker: EventSink.RestartContinuationMarkerV2 = {
    markerId: "released-captured-marker",
    threadId,
    projectId: base.thread.projectId,
    sourceRunId: runId,
    sourceRunAttemptId: attemptId,
    binding,
    evidenceRevision: 2,
    createdAt: "2026-10-03T12:00:00Z",
  };
  const captured: NonNullable<RestartContinuationInput["capturedContinuation"]> = {
    effectId: "released-captured-effect",
    marker,
    workerId: "worker:captured-claim",
    expectedAttempt: 2,
  };
  let released = captured;
  let current = {
    ...base,
    thread: {
      ...base.thread,
      activeProviderThreadId: providerThreadId,
      modelSelection: base.runs[0]!.modelSelection,
    },
    runs: [{ ...base.runs[0]!, status }],
    providerTurns: [{ ...base.providerTurns[0]!, status: "completed" }],
    attempts: [sourceAttempt],
  } as OrchestrationV2ThreadProjection;
  let held = false;
  const ordinary: Parameters<
    ThreadManagementService.ThreadManagementService["Service"]["dispatch"]
  >[0][] = [];
  const privateCommands: Parameters<
    ThreadManagementService.ThreadManagementService["Service"]["dispatchRestartContinuation"]
  >[0][] = [];
  const receiptIds = new Set<string>();
  // Model the full released-marker/current-claim premise at this helper boundary.
  // STORE release and private acceptance are verified by their owning checks.
  const services = Layer.mergeAll(
    Layer.mock(ThreadManagementService.ThreadManagementService)({
      getThreadRecords: () => Effect.succeed(current),
      dispatch: (command) =>
        Effect.sync(() => {
          ordinary.push(command);
          receiptIds.add(String(command.commandId));
          if (command.type === "message.dispatch")
            current = { ...current, messages: [{ id: command.messageId } as never] };
          return {} as never;
        }),
      dispatchRestartContinuation: (command, context) =>
        Effect.sync(() => {
          assert.deepEqual(context, released);
          assert.equal(
            command.commandId,
            EventSink.capturedRestartContinuationIdsV1(context).commandId,
          );
          assert.equal(command.messageId, `message:restart-continuation:${runId}`);
          // A receipt for an ordinary no-op has no message and must not consume this command.
          if (receiptIds.has(String(command.commandId))) return {} as never;
          receiptIds.add(String(command.commandId));
          privateCommands.push(command);
          current = { ...current, messages: [{ id: command.messageId } as never] };
          return {} as never;
        }),
    }),
    Layer.mock(EventSink.EventSinkV2)({
      readReleasedRestartContinuation: () => Effect.succeed(released.marker),
      readProviderRuntimeEvidence: () =>
        Effect.succeed({
          binding,
          evidenceRevision: 2,
          observation: null,
          registeredAt: marker.createdAt,
        }),
    }),
    Layer.mock(EffectOutbox.EffectOutboxV2)({
      listHeldByThreadId: () => Effect.succeed(held ? [{} as never] : []),
    }),
  );
  return {
    captured,
    ordinary,
    privateCommands,
    receiptIds,
    release: (context: NonNullable<RestartContinuationInput["capturedContinuation"]>) => {
      released = context;
    },
    hold: () => {
      held = true;
    },
    replace: (projection: OrchestrationV2ThreadProjection) => {
      current = projection;
    },
    projection: () => current,
    run: (continuation: RestartContinuationInput["capturedContinuation"], enabled = false) =>
      continueRestartedRun({
        threadId,
        sourceRunId: runId,
        ...(continuation === undefined ? {} : { capturedContinuation: continuation }),
      }).pipe(
        Effect.provide(
          Layer.merge(
            services,
            ServerSettings.layerTest({ continueThreadsAfterServerUpdate: enabled }),
          ),
        ),
      ),
  };
}

it.effect("a captured command bypasses an old ordinary no-op receipt without relabeling it", () =>
  Effect.gen(function* () {
    const fixture = capturedDispatchFixture();
    const oldCommandId = `command:restart-continuation:${runId}`;
    fixture.receiptIds.add(oldCommandId);
    yield* fixture.run(undefined, true);
    assert.lengthOf(fixture.ordinary, 0);
    assert.lengthOf(fixture.projection().messages, 0);

    yield* fixture.run(fixture.captured);
    assert.lengthOf(fixture.privateCommands, 1);
    assert.notEqual(fixture.privateCommands[0]!.commandId, oldCommandId);
    assert.isTrue(fixture.receiptIds.has(oldCommandId));
    assert.lengthOf(fixture.projection().messages, 1);
  }),
);

it.effect(
  "keeps one source message across captured marker/effect changes and an ordinary delivery",
  () =>
    Effect.gen(function* () {
      const captured = capturedDispatchFixture();
      yield* captured.run(captured.captured);
      const second = {
        ...captured.captured,
        effectId: "released-second-effect",
        marker: {
          ...captured.captured.marker,
          markerId: "released-second-marker",
          createdAt: "2026-10-03T12:01:00Z",
        },
      };
      captured.release(second);
      yield* captured.run(second);
      assert.lengthOf(captured.privateCommands, 1);
      assert.lengthOf(captured.projection().messages, 1);

      const ordinary = capturedDispatchFixture("cancelled");
      yield* ordinary.run(undefined, true);
      yield* ordinary.run(ordinary.captured);
      assert.lengthOf(ordinary.ordinary, 1);
      assert.lengthOf(ordinary.privateCommands, 0);
      assert.lengthOf(ordinary.projection().messages, 1);
    }),
);

it.effect(
  "rejects a stripped marker and unknown or changed current ownership before private delivery",
  () =>
    Effect.gen(function* () {
      const stripped = capturedDispatchFixture();
      stripped.receiptIds.add(`command:restart-continuation:${runId}`);
      yield* stripped.run({
        ...stripped.captured,
        marker: {
          markerId: stripped.captured.marker.markerId,
        } as EventSink.RestartContinuationMarkerV2,
      });
      assert.lengthOf(stripped.ordinary, 0);
      assert.lengthOf(stripped.privateCommands, 0);

      const held = capturedDispatchFixture();
      held.hold();
      yield* held.run(held.captured);
      assert.lengthOf(held.privateCommands, 0);

      const changed = capturedDispatchFixture();
      const projection = changed.projection();
      changed.replace({
        ...projection,
        thread: {
          ...projection.thread,
          activeProviderThreadId: ProviderThreadId.make("provider-thread:new-owner"),
        },
      });
      yield* changed.run(changed.captured);
      assert.lengthOf(changed.privateCommands, 0);
    }),
);

it.effect("does not duplicate delivery and yields to newer user work or opt-out", () =>
  Effect.gen(function* () {
    let projection = makeProjection();
    projection = { ...projection, runs: [{ ...projection.runs[0]!, status: "cancelled" }] };
    const commands: Parameters<
      ThreadManagementService.ThreadManagementService["Service"]["dispatch"]
    >[0][] = [];
    const threads = Layer.mock(ThreadManagementService.ThreadManagementService)({
      getThreadRecords: () => Effect.succeed(projection),
      dispatch: (command) => {
        commands.push(command);
        if (command.type === "message.dispatch")
          projection = { ...projection, messages: [{ id: command.messageId } as never] };
        return Effect.succeed({} as never);
      },
    });
    const enabled = Layer.mergeAll(
      threads,
      ServerSettings.layerTest({ continueThreadsAfterServerUpdate: true }),
      uncapturedContinuationServices,
    );
    yield* continueRestartedRun({ threadId, sourceRunId: runId }).pipe(Effect.provide(enabled));
    yield* continueRestartedRun({ threadId, sourceRunId: runId }).pipe(Effect.provide(enabled));
    assert.lengthOf(commands, 1);
    assert.match(String(commands[0]!.commandId), /run:restart$/);
    if (commands[0]!.type === "message.dispatch")
      assert.equal(commands[0]!.restartContinuationOfRunId, runId);
    projection = {
      ...projection,
      messages: [],
      runs: [
        ...projection.runs,
        {
          ...projection.runs[0]!,
          id: RunId.make("run:user-newer"),
          ordinal: 2,
          status: "completed",
        },
      ],
    };
    yield* continueRestartedRun({ threadId, sourceRunId: runId }).pipe(Effect.provide(enabled));
    assert.lengthOf(commands, 1);
    projection = { ...projection, runs: [projection.runs[0]!] };
    yield* continueRestartedRun({ threadId, sourceRunId: runId }).pipe(
      Effect.provide(
        Layer.mergeAll(
          threads,
          ServerSettings.layerTest({ continueThreadsAfterServerUpdate: false }),
          uncapturedContinuationServices,
        ),
      ),
    );
    assert.lengthOf(commands, 1);
  }),
);

it.effect("prepares no continuation for background work another provider thread launched", () =>
  Effect.gen(function* () {
    const base = makeProjection();
    const claudeThreadId = ProviderThreadId.make("provider-thread:claude");
    // Claude's background work does not make the current Codex attachment live.
    const projection = {
      ...base,
      runs: [{ ...base.runs[0]!, status: "completed" }],
      providerTurns: [{ ...base.providerTurns[0]!, status: "completed" }],
      turnItems: [
        {
          id: "turn-item:claude-subagent",
          runId: RunId.make("run:claude"),
          providerThreadId: claudeThreadId,
          type: "subagent",
          status: "running",
        },
      ],
    } as unknown as OrchestrationV2ThreadProjection;
    const binding: EventSink.ProviderBindingExpectationV2 = {
      threadId,
      providerThreadId,
      providerSessionId: sessionId,
      instanceId,
      driver,
      nativeThreadId: "native-thread",
      runtimeGeneration: "shutdown-current-generation",
    };
    const markers: EventSink.RestartContinuationMarkerV2[] = [];
    const sink = Layer.mock(EventSink.EventSinkV2)({
      readDormantRestartContinuations: Effect.succeed([]),
      readProviderRuntimeEvidence: () =>
        Effect.succeed({
          binding,
          evidenceRevision: 2,
          observation: null,
          registeredAt: "2026-10-03T12:00:00Z",
        }),
      findDormantRestartContinuation: () => Effect.succeed(null),
      prepareRestartContinuation: (input) =>
        Effect.gen(function* () {
          const markerId =
            typeof input.markerId === "string" ? input.markerId : yield* input.markerId;
          const marker: EventSink.RestartContinuationMarkerV2 = {
            markerId,
            threadId: input.threadId,
            projectId: input.projectId,
            sourceRunId: input.sourceRunId,
            sourceRunAttemptId: input.sourceRunAttemptId,
            binding: input.expectedBinding,
            evidenceRevision: input.expectedEvidenceRevision,
            createdAt: "2026-10-03T12:00:00Z",
          };
          markers.push(marker);
          return marker;
        }),
      writeWithEffects: () => Effect.die("Shutdown preparation must not enqueue runnable effects."),
    });
    const recovery = yield* ProviderRuntimeRecovery.make.pipe(
      Effect.provide(
        Layer.mergeAll(
          ServerSettings.layerTest({ continueThreadsAfterServerUpdate: true }),
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getRecoveryThreadIds: () => Effect.succeed([threadId]),
            getRuntimeRecoveryProjection: () => Effect.succeed(projection),
          }),
          sink,
          IdAllocator.layer,
          Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({}),
          Layer.mock(EffectOutbox.EffectOutboxV2)({}),
          Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
            observeCurrentThreadRuntime: () =>
              Effect.succeed({
                status: "idle",
                binding,
                observedAt: "2026-10-03T12:00:00Z",
                complete: true,
              } as never),
          }),
        ),
      ),
    );
    yield* recovery.prepareForShutdown;
    assert.lengthOf(markers, 0);
    // Current native work on Codex's own attachment creates a dormant marker.
    const ownWork = {
      ...projection,
      turnItems: [{ ...projection.turnItems[0]!, providerThreadId }],
    } as OrchestrationV2ThreadProjection;
    const ownRecovery = yield* ProviderRuntimeRecovery.make.pipe(
      Effect.provide(
        Layer.mergeAll(
          ServerSettings.layerTest({ continueThreadsAfterServerUpdate: true }),
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getRecoveryThreadIds: () => Effect.succeed([threadId]),
            getRuntimeRecoveryProjection: () => Effect.succeed(ownWork),
          }),
          sink,
          IdAllocator.layer,
          Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({}),
          Layer.mock(EffectOutbox.EffectOutboxV2)({}),
          Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
            observeCurrentThreadRuntime: () =>
              Effect.succeed({
                status: "working",
                binding,
                observedAt: "2026-10-03T12:00:00Z",
              } as never),
          }),
        ),
      ),
    );
    yield* ownRecovery.prepareForShutdown;
    assert.lengthOf(markers, 1);
    assert.equal(markers[0]!.sourceRunId, runId);
    assert.deepEqual(markers[0]!.binding, binding);
  }),
);

it.effect("does not cancel or resume a run that completes while shutdown intent commits", () =>
  Effect.gen(function* () {
    let projection = makeProjection();
    const binding: EventSink.ProviderBindingExpectationV2 = {
      threadId,
      providerThreadId,
      providerSessionId: sessionId,
      instanceId,
      driver,
      nativeThreadId: "native-thread",
      runtimeGeneration: "shutdown-completing-generation",
    };
    const markers: EventSink.RestartContinuationMarkerV2[] = [];
    const commits: Parameters<EventSink.EventSinkV2["Service"]["commitCommand"]>[0][] = [];
    const recovery = yield* ProviderRuntimeRecovery.make.pipe(
      Effect.provide(
        Layer.mergeAll(
          ServerSettings.layerTest({ continueThreadsAfterServerUpdate: true }),
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getRecoveryThreadIds: () => Effect.succeed([threadId]),
            getRuntimeRecoveryProjection: () => Effect.sync(() => projection),
          }),
          Layer.mock(EventSink.EventSinkV2)({
            readDormantRestartContinuations: Effect.succeed([]),
            readProviderRuntimeEvidence: () =>
              Effect.succeed({
                binding,
                evidenceRevision: 2,
                observation: null,
                registeredAt: "2026-10-03T12:00:00Z",
              }),
            findDormantRestartContinuation: () => Effect.succeed(null),
            prepareRestartContinuation: (input) =>
              Effect.gen(function* () {
                const markerId =
                  typeof input.markerId === "string" ? input.markerId : yield* input.markerId;
                projection = {
                  ...projection,
                  runs: [{ ...projection.runs[0]!, status: "completed" }],
                };
                const marker: EventSink.RestartContinuationMarkerV2 = {
                  markerId,
                  threadId: input.threadId,
                  projectId: input.projectId,
                  sourceRunId: input.sourceRunId,
                  sourceRunAttemptId: input.sourceRunAttemptId,
                  binding: input.expectedBinding,
                  evidenceRevision: input.expectedEvidenceRevision,
                  createdAt: "2026-10-03T12:00:00Z",
                };
                markers.push(marker);
                return marker;
              }),
            writeWithEffects: () =>
              Effect.die("Shutdown preparation must not enqueue runnable effects."),
            commitCommand: (input) =>
              Effect.sync(() => {
                commits.push(input);
                return { committed: true, cancelledEffectCount: 0 } as never;
              }),
          }),
          IdAllocator.layer,
          Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({
            runRecoveryOnce: Effect.succeed(false),
          }),
          Layer.mock(EffectOutbox.EffectOutboxV2)({
            listHeldByThreadId: () => Effect.succeed([]),
            reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
          }),
          Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
            observeCurrentThreadRuntime: () =>
              Effect.succeed({
                status: "working",
                binding,
                observedAt: "2026-10-03T12:00:00Z",
              } as never),
          }),
        ),
      ),
    );
    yield* recovery.prepareForShutdown;
    assert.lengthOf(markers, 1);
    yield* recovery.reconcile("shutdown");
    assert.isFalse(
      commits.some((commit) => commit.events.some((event) => event.type === "run.updated")),
    );
    let dispatched = false;
    yield* continueRestartedRun({ threadId, sourceRunId: runId }).pipe(
      Effect.provide(
        Layer.mergeAll(
          ServerSettings.layerTest({ continueThreadsAfterServerUpdate: true }),
          Layer.mock(ThreadManagementService.ThreadManagementService)({
            getThreadRecords: () => Effect.succeed(projection),
            dispatch: () =>
              Effect.sync(() => {
                dispatched = true;
                return {} as never;
              }),
          }),
          uncapturedContinuationServices,
        ),
      ),
    );
    assert.isFalse(dispatched);
  }),
);
