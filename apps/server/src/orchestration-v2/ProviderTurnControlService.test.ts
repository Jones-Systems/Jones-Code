import { assert, it } from "@effect/vitest";
import {
  type ModelSelection,
  MessageId,
  NodeId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ThreadProjection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";

import * as EventSink from "./EventSink.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckoutOwnership.ts";
import { OrchestrationEffectRequestV2 } from "./EffectOutbox.ts";
import { nativeCreationCanonicalJson, nativeCreationSha256 } from "./NativeCreationPreparation.ts";

import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import type { ProviderAdapterV2SessionRuntime } from "./ProviderAdapter.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderTurnControlService from "./ProviderTurnControlService.ts";

const driver = ProviderDriverKind.make("codex");
const providerInstanceId = ProviderInstanceId.make("codex");
const modelSelection = {
  instanceId: providerInstanceId,
  model: "gpt-5.4",
} satisfies ModelSelection;

function makeProjection(input: {
  readonly now: DateTime.Utc;
  readonly threadId: ThreadId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly providerTurnId: ProviderTurnId;
  readonly attemptId: RunAttemptId;
}): OrchestrationV2ThreadProjection {
  const runId = RunId.make("run:restart-session");
  const nodeId = NodeId.make("node:restart-session");
  return {
    thread: {
      createdBy: "user",
      creationSource: "web",
      id: input.threadId,
      projectId: ProjectId.make("project:restart-session"),
      title: "Restart session",
      providerInstanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: "/workspace",
      activeProviderThreadId: input.providerThread.id,
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: input.threadId,
      },
      forkedFrom: null,
      createdAt: input.now,
      updatedAt: input.now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    },
    runs: [],
    attempts: [
      {
        id: input.attemptId,
        runId,
        attemptOrdinal: 1,
        rootNodeId: nodeId,
        providerInstanceId,
        providerThreadId: input.providerThread.id,
        providerTurnId: input.providerTurnId,
        reason: "initial",
        status: "superseded",
        startedAt: input.now,
        completedAt: input.now,
      },
    ],
    nodes: [],
    subagents: [],
    providerSessions: [],
    providerThreads: [input.providerThread],
    providerTurns: [
      {
        id: input.providerTurnId,
        providerThreadId: input.providerThread.id,
        nodeId,
        runAttemptId: input.attemptId,
        nativeTurnRef: {
          driver,
          nativeId: "native-turn:restart-session",
          strength: "strong",
        },
        ordinal: 1,
        status: "running",
        startedAt: input.now,
        completedAt: null,
      },
    ],
    runtimeRequests: [],
    messages: [],
    plans: [],
    turnItems: [],
    checkpointScopes: [],
    checkpoints: [],
    contextHandoffs: [],
    contextTransfers: [],
    visibleTurnItems: [],
    updatedAt: input.now,
  };
}

it.effect(
  "interrupts the historical session only for the exact committed restart replacement",
  () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread:restart-session");
      const oldSessionId = ProviderSessionId.make("provider-session:restart-session:old");
      const replacementSessionId = ProviderSessionId.make(
        "provider-session:restart-session:replacement",
      );
      const unrelatedSessionId = ProviderSessionId.make(
        "provider-session:restart-session:unrelated",
      );
      const providerThreadId = ProviderThreadId.make("provider-thread:restart-session");
      const providerTurnId = ProviderTurnId.make("provider-turn:restart-session");
      const attemptId = RunAttemptId.make("run-attempt:restart-session");
      const providerThread: OrchestrationV2ProviderThread = {
        id: providerThreadId,
        driver,
        providerInstanceId,
        // The restart command has already projected this replacement binding
        // before the process-bound restart effect executes.
        providerSessionId: replacementSessionId,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: {
          driver,
          nativeId: "native-thread:restart-session",
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "not_loaded",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      const projection = yield* Ref.make(
        makeProjection({ now, threadId, providerThread, providerTurnId, attemptId }),
      );
      const interruptedThread = yield* Ref.make<OrchestrationV2ProviderThread | null>(null);
      const providerSession = {
        id: oldSessionId,
        driver,
        providerInstanceId,
        status: "running" as const,
        cwd: "/workspace",
        model: modelSelection.model,
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const runtime: ProviderAdapterV2SessionRuntime = {
        instanceId: providerInstanceId,
        driver,
        providerSessionId: oldSessionId,
        providerSession,
        events: Stream.empty,
        ensureThread: () => Effect.die("unused ensureThread"),
        resumeThread: () => Effect.die("unused resumeThread"),
        startTurn: () => Effect.die("unused startTurn"),
        steerTurn: () => Effect.die("unused steerTurn"),
        interruptTurn: ({ providerThread: target }) =>
          Effect.all(
            [
              Ref.set(interruptedThread, target),
              Ref.update(projection, (current) => ({
                ...current,
                providerTurns: current.providerTurns.map((turn) =>
                  turn.id === providerTurnId
                    ? { ...turn, status: "interrupted" as const, completedAt: now }
                    : turn,
                ),
              })),
            ],
            { discard: true },
          ),
        respondToRuntimeRequest: () => Effect.die("unused respondToRuntimeRequest"),
        readThreadSnapshot: () => Effect.die("unused readThreadSnapshot"),
        rollbackThread: () => Effect.die("unused rollbackThread"),
        forkThread: () => Effect.die("unused forkThread"),
      };
      const projectionLayer = Layer.succeed(
        ProjectionStore.ProjectionStoreV2,
        ProjectionStore.ProjectionStoreV2.of({
          apply: () => Effect.void,
          getLimitRecoveryCandidates: () => Effect.die("unused getLimitRecoveryCandidates"),
          getOperatingCountsCandidates: () => Effect.die("unused getOperatingCountsCandidates"),
          getThreadRetainedAttachmentPaths: () =>
            Effect.die("unused getThreadRetainedAttachmentPaths"),
          getShellSnapshot: () => Effect.die("unused getShellSnapshot"),
          getThreadShell: () => Effect.die("unused getThreadShell"),
          getThread: () => Ref.get(projection).pipe(Effect.map((state) => state.thread)),
          getSettlementCandidates: () => Effect.die("unused getSettlementCandidates"),
          getThreadsWithPullRequests: () => Effect.die("unused getThreadsWithPullRequests"),
          getThreadProjection: () => Effect.die("control effects must not load transcript"),
          getTurnStartContext: () => Effect.die("unused"),
          getTurnStartHistory: () => Effect.die("unused"),
          getRuntimeRecoveryProjection: () => Effect.die("unused getRuntimeRecoveryProjection"),
          getPlan: () => Effect.die("unused"),
          hasUnpairedRunInterruptRequest: () => Effect.die("unused interrupt read"),
          getThreadAttachmentIds: () => Effect.die("Unused attachment lookup"),
          getTimelinePage: () => Effect.die("Unused timeline read"),
          getMessageCount: () => Effect.die("unused message count"),
          getNextTurnItemOrdinal: () => Effect.die("unused ordinal read"),
          getThreadRecords: () => Effect.die("unused record read"),
          getRuntimeRequest: () => Effect.die("unused getRuntimeRequest"),
          getRunningTurnContext: () => Effect.die("unused getRunningTurnContext"),
          getThreadProviderContext: () => Effect.die("unused getThreadProviderContext"),
          getRuntimeResponseContext: () => Effect.die("unused getRuntimeResponseContext"),
          getPendingNativeUserInputs: () => Effect.die("unused getPendingNativeUserInputs"),
          getProviderControlContext: (_threadId, target) =>
            Ref.get(projection).pipe(
              Effect.map((current) => ({
                providerThread: current.providerThreads.find(
                  (thread) => thread.id === target.providerThreadId,
                ),
                providerTurn: current.providerTurns.find(
                  (turn) => turn.id === target.providerTurnId,
                ),
                attempt: current.attempts.find((attempt) => attempt.id === target.attemptId),
                message: undefined,
                run: undefined,
              })),
            ),
          getCheckpointContext: () => Effect.die("not used"),
          getCheckpointCaptureContext: () => Effect.die("not used"),
          getRunMessage: () => Effect.die("not used"),
          canStartQueuedRun: () => Effect.die("not used"),
          getRecoveryThreadIds: () => Effect.die("unused getRecoveryThreadIds"),
          getUnreadableThreadIds: () => Effect.die("unused getUnreadableThreadIds"),
          getThreadSnapshot: () => Effect.die("unused getThreadSnapshot"),
          getThreadSnapshotWindow: () => Effect.die("unused getThreadSnapshotWindow"),
        }),
      );
      const sessionManagerLayer = Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
        shutdown: Effect.void,
        open: () => Effect.die("unused open"),
        get: (providerSessionId) =>
          Effect.succeed(providerSessionId === oldSessionId ? Option.some(runtime) : Option.none()),
        close: () => Effect.void,
        closeInstance: () => Effect.void,
        release: () => Effect.void,
        detach: () => Effect.void,
      });
      const controlLayer = ProviderTurnControlService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            projectionLayer,
            sessionManagerLayer,
            Layer.mock(EventSink.EventSinkV2)({
              readOrdinaryCheckoutAdmissionForRun: () => Effect.succeed(null),
              revalidateOrdinaryCheckoutExecution: () =>
                Effect.die("Standalone Stop/restart has no ordinary executor."),
            }),
          ),
        ),
      );

      const [ordinaryInterrupt, unrelatedRestart] = yield* Effect.gen(function* () {
        const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
        const ordinary = yield* Effect.exit(
          control.interrupt({
            threadId,
            providerSessionId: oldSessionId,
            providerThreadId,
            providerTurnId,
          }),
        );
        const unrelated = yield* Effect.exit(
          control.interruptAndAwaitTerminal({
            threadId,
            providerSessionId: oldSessionId,
            replacementProviderSessionId: unrelatedSessionId,
            providerThreadId,
            providerTurnId,
            interruptedAttemptId: attemptId,
          }),
        );
        return [ordinary, unrelated] as const;
      }).pipe(Effect.provide(controlLayer));

      assert.isTrue(Exit.isFailure(ordinaryInterrupt));
      assert.isTrue(Exit.isFailure(unrelatedRestart));
      assert.isNull(yield* Ref.get(interruptedThread));

      yield* Effect.gen(function* () {
        const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
        yield* control.interruptAndAwaitTerminal({
          threadId,
          providerSessionId: oldSessionId,
          replacementProviderSessionId: replacementSessionId,
          providerThreadId,
          providerTurnId,
          interruptedAttemptId: attemptId,
        });
      }).pipe(Effect.provide(controlLayer));

      const interrupted = yield* Ref.get(interruptedThread);
      assert.isNotNull(interrupted);
      assert.equal(interrupted?.providerSessionId, oldSessionId);
      assert.equal(interrupted?.id, providerThreadId);
      assert.equal(interrupted?.nativeThreadRef?.nativeId, "native-thread:restart-session");
    }),
);

function ordinaryClaimFixture(
  threadId: ThreadId,
  runId: RunId,
  request: OrchestrationEffectRequestV2,
) {
  const timestamp = "2026-10-03T00:00:00.000Z";
  const command = { type: "thread.turn.start", commandId: "command:original-start", threadId };
  const birth = {
    kind: "application_v2_thread_birth",
    threadId,
    eventId: "event:original-birth",
    sequence: 1,
  };
  const lease = {
    resourcePath: "/fixture/checkout",
    leaseId: "lease:original-start",
    ownerThreadId: threadId,
    ownerIncarnation: OrdinaryCheckout.ordinaryApplicationIncarnationV1(
      birth as OrdinaryCheckout.OrdinaryApplicationBirthV1,
    ),
    branch: "fixture-branch",
    acquiredAtMs: 1,
    renewedAtMs: 1,
    expiresAtMs: 300001,
  };
  const capture = Schema.decodeUnknownSync(OrdinaryCheckout.OrdinaryCheckoutCaptureV1)({
    version: 1,
    commandId: command.commandId,
    commandType: command.type,
    canonicalCommand: command,
    commandDigest: OrdinaryCheckout.ordinaryCheckoutCommandDigestV1(command),
    origin: { kind: "command" },
    threadId,
    applicationBirth: birth,
    projectId: "project:original-start",
    canonicalProjectRoot: "/fixture/repo",
    canonicalCheckoutPath: lease.resourcePath,
    branch: lease.branch,
    lease,
  });
  const admission = Schema.decodeUnknownSync(OrdinaryCheckout.OrdinaryCheckoutAdmissionV1)({
    version: 1,
    admissionId: OrdinaryCheckout.ordinaryCheckoutAdmissionIdV1(capture),
    capture: Schema.encodeSync(OrdinaryCheckout.OrdinaryCheckoutCaptureV1)(capture),
    receipt: {
      commandId: command.commandId,
      threadId,
      commandType: command.type,
      acceptedAt: timestamp,
      resultSequence: 2,
      status: "accepted",
      error: null,
    },
    eventBasis: [
      {
        eventId: "event:original-run",
        sequence: 2,
        threadId,
        commandId: command.commandId,
        eventType: "run.created",
      },
    ],
    run: {
      runId,
      runAttemptId: "attempt:original-start",
      nodeId: "node:original-start",
      messageId: "message:original-start",
    },
    recordedAt: timestamp,
  });
  const reference = OrdinaryCheckout.ordinaryCheckoutAdmissionRefV1(admission);
  const originalSource = {
    kind: "outbox",
    link: {
      version: 1,
      effectId: "effect:original-start",
      commandId: command.commandId,
      threadId,
      requestSha256: "c".repeat(64),
      admission: reference,
      recordedAt: timestamp,
    },
    workerId: "worker:original-start",
    expectedAttempt: 1,
    leaseExpiresAt: "2026-10-03T00:05:00.000Z",
  };
  const use = Schema.decodeUnknownSync(OrdinaryCheckout.OrdinaryCheckoutUseV1)({
    version: 1,
    kind: "ordinary_checkout_use",
    operationId: "effect:original-start:ordinary-checkout:attempt:1",
    admission: reference,
    source: originalSource,
    lease,
  });
  const source = Schema.decodeUnknownSync(OrdinaryCheckout.OrdinaryCheckoutOutboxExecutionSourceV1)(
    {
      ...originalSource,
      link: {
        ...originalSource.link,
        effectId: "effect:joined-control",
        commandId: "command:joined-control",
        requestSha256: nativeCreationSha256(
          nativeCreationCanonicalJson(Schema.encodeSync(OrchestrationEffectRequestV2)(request)),
        ),
      },
      workerId: "worker:joined-control",
      expectedAttempt: 2,
    },
  );
  const execution = OrdinaryCheckout.makeOrdinaryCheckoutExecutionRefV1({
    originalUse: use,
    executor: { kind: "actual_outbox_claim", source },
  });
  return { admission, use, execution };
}

function ordinaryControlFixture() {
  const threadId = ThreadId.make("thread:ordinary-control");
  const providerSessionId = ProviderSessionId.make("session:ordinary-control");
  const providerThreadId = ProviderThreadId.make("provider-thread:ordinary-control");
  const providerTurnId = ProviderTurnId.make("provider-turn:ordinary-control");
  const runId = RunId.make("run:ordinary-control");
  const messageId = MessageId.make("message:ordinary-control");
  const attemptId = RunAttemptId.make("attempt:original-start");
  const now = DateTime.makeUnsafe("2026-10-03T00:00:00.000Z");
  const providerThread: OrchestrationV2ProviderThread = {
    id: providerThreadId,
    driver,
    providerInstanceId,
    providerSessionId,
    appThreadId: threadId,
    ownerNodeId: null,
    nativeThreadRef: { driver, nativeId: "native:ordinary-control", strength: "strong" },
    nativeConversationHeadRef: null,
    status: "active",
    firstRunOrdinal: 1,
    lastRunOrdinal: 1,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
  const projection = makeProjection({ now, threadId, providerThread, providerTurnId, attemptId });
  const input = { threadId, providerSessionId, providerThreadId, providerTurnId, messageId };
  const request = {
    type: "provider-turn.steer" as const,
    providerSessionId,
    providerThreadId,
    providerTurnId,
    messageId,
  };
  const fixture = ordinaryClaimFixture(threadId, runId, request);
  const context: ProjectionStore.ProjectionProviderControlContext = {
    providerThread,
    providerTurn: projection.providerTurns[0],
    attempt: { ...projection.attempts[0]!, runId },
    run: {
      id: runId,
      activeAttemptId: attemptId,
    } as ProjectionStore.ProjectionProviderControlContext["run"],
    message: {
      id: messageId,
      runId,
      text: "Keep the accepted plan.",
      threadId,
      nodeId: null,
      role: "user",
      streaming: false,
      createdAt: now,
      updatedAt: now,
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
  };
  return { input, fixture, context, runId, attemptId };
}

it.effect(
  "steers through the exact joined original claim and retains ownership loss before native entry",
  () =>
    Effect.gen(function* () {
      const value = ordinaryControlFixture();
      const calls: string[] = [];
      let loseBeforeCall = false;
      let checks = 0;
      const failure = new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
        reason: "claim_mismatch",
        threadId: value.input.threadId,
        path: value.fixture.use.lease.resourcePath,
        message: "Claim replaced before steer.",
      });
      const testLayer = ProviderTurnControlService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(ProjectionStore.ProjectionStoreV2)({
              getProviderControlContext: () => Effect.succeed(value.context),
            }),
            Layer.mock(EventSink.EventSinkV2)({
              readOrdinaryCheckoutAdmissionForRun: (input) =>
                Effect.sync(() => {
                  assert.deepEqual(input, { threadId: value.input.threadId, runId: value.runId });
                  return value.fixture.admission;
                }),
              revalidateOrdinaryCheckoutExecution: (ref) =>
                Effect.gen(function* () {
                  assert.strictEqual(ref, value.fixture.execution);
                  calls.push("revalidate");
                  checks += 1;
                  if (loseBeforeCall && checks === 2) return yield* failure;
                  return ref;
                }),
            }),
            Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
              get: () =>
                Effect.succeed(
                  Option.some({
                    instanceId: providerInstanceId,
                    steerTurn: (
                      input: Parameters<ProviderAdapterV2SessionRuntime["steerTurn"]>[0],
                    ) =>
                      Effect.sync(() => {
                        calls.push("steer");
                        assert.equal(input.message.text, "Keep the accepted plan.");
                        assert.equal(input.runId, value.runId);
                        assert.equal(input.providerThread.id, value.input.providerThreadId);
                        assert.isUndefined(input.nativeOperation?.runtimeGeneration);
                      }),
                  } as never),
                ),
            }),
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const service = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
        const input = {
          ...value.input,
          ordinaryCheckoutUse: value.fixture.use,
          ordinaryCheckoutExecution: value.fixture.execution,
        };
        yield* service.steer(input);
        assert.deepEqual(calls, ["revalidate", "revalidate", "steer"]);
        calls.length = 0;
        checks = 0;
        loseBeforeCall = true;
        const error = yield* service.steer(input).pipe(Effect.flip);
        assert.strictEqual(error.cause, failure);
        assert.isUndefined(error.turnCompleted);
        assert.deepEqual(calls, ["revalidate", "revalidate"]);
      }).pipe(Effect.provide(testLayer));
    }),
);

it.effect(
  "holds admitted steering messages without their exact original actor and claimed request",
  () =>
    Effect.gen(function* () {
      const value = ordinaryControlFixture();
      const testLayer = ProviderTurnControlService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(ProjectionStore.ProjectionStoreV2)({
              getProviderControlContext: () => Effect.succeed(value.context),
            }),
            Layer.mock(EventSink.EventSinkV2)({
              readOrdinaryCheckoutAdmissionForRun: () => Effect.succeed(value.fixture.admission),
              revalidateOrdinaryCheckoutExecution: () =>
                Effect.die("Wrong steer must fail before SQL execution entry."),
            }),
            Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
              get: () =>
                Effect.succeed(
                  Option.some({
                    instanceId: providerInstanceId,
                    steerTurn: () => Effect.die("Wrong steer must not enter the provider."),
                  } as never),
                ),
            }),
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const service = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
        assert.equal(value.fixture.execution.executor.kind, "actual_outbox_claim");
        if (value.fixture.execution.executor.kind !== "actual_outbox_claim")
          throw new Error("Expected claim fixture");
        const source = value.fixture.execution.executor.source;
        const wrongRequest = OrdinaryCheckout.makeOrdinaryCheckoutExecutionRefV1({
          originalUse: value.fixture.use,
          executor: {
            kind: "actual_outbox_claim",
            source: { ...source, link: { ...source.link, requestSha256: "d".repeat(64) } },
          },
        });
        for (const context of [
          {},
          { ordinaryCheckoutUse: value.fixture.use },
          { ordinaryCheckoutExecution: wrongRequest },
          {
            ordinaryCheckoutExecution: value.fixture.execution,
            ordinaryCheckoutUse: { ...value.fixture.use, operationId: "replacement-use" },
          },
        ]) {
          const error = yield* service.steer({ ...value.input, ...context }).pipe(Effect.flip);
          assert.equal(
            error.cause,
            "The provider control has no matching original checkout claim.",
          );
        }
      }).pipe(Effect.provide(testLayer));
    }),
);

it.effect(
  "restart controls require the complete actual claimed request and revalidate while awaiting terminal",
  () =>
    Effect.gen(function* () {
      const value = ordinaryControlFixture();
      const target = {
        providerSessionId: value.input.providerSessionId,
        providerThreadId: value.input.providerThreadId,
        providerTurnId: value.input.providerTurnId,
      };
      const input = {
        ...target,
        threadId: value.input.threadId,
        interruptedAttemptId: value.attemptId,
      };
      const request = {
        type: "provider-turn.restart" as const,
        ...target,
        interruptedAttemptId: value.attemptId,
        runId: value.runId,
      };
      const fixture = ordinaryClaimFixture(value.input.threadId, value.runId, request);
      let context = value.context;
      const calls: string[] = [];
      const testLayer = ProviderTurnControlService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(ProjectionStore.ProjectionStoreV2)({
              getProviderControlContext: (_threadId, target) =>
                Effect.sync(() => {
                  assert.equal(target.attemptId, value.attemptId);
                  return context;
                }),
            }),
            Layer.mock(EventSink.EventSinkV2)({
              readOrdinaryCheckoutAdmissionForRun: () => Effect.succeed(fixture.admission),
              revalidateOrdinaryCheckoutExecution: (ref) =>
                Effect.sync(() => {
                  assert.strictEqual(ref, fixture.execution);
                  calls.push("revalidate");
                  return ref;
                }),
            }),
            Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
              get: () =>
                Effect.succeed(
                  Option.some({
                    instanceId: providerInstanceId,
                    interruptTurn: () =>
                      Effect.sync(() => {
                        calls.push("interrupt");
                        context = {
                          ...context,
                          providerTurn: { ...context.providerTurn!, status: "interrupted" },
                          attempt: { ...context.attempt!, status: "interrupted" },
                        };
                      }),
                  } as never),
                ),
            }),
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const service = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
        const carrier = {
          ordinaryCheckoutUse: fixture.use,
          ordinaryCheckoutExecution: fixture.execution,
        };
        context = { ...context, attempt: undefined };
        const missingRun = yield* service.interruptAndAwaitTerminal(input).pipe(Effect.flip);
        assert.equal(missingRun.cause, "The restart control has no recorded run.");
        assert.deepEqual(calls, []);
        context = value.context;
        const missing = yield* service
          .interruptAndAwaitTerminal({ ...input, ...carrier })
          .pipe(Effect.flip);
        assert.equal(
          missing.cause,
          "The restart control differs from its complete actual claimed request.",
        );
        assert.deepEqual(calls, []);
        const mismatchedTarget = yield* service
          .interruptAndAwaitTerminal({
            ...input,
            ...carrier,
            ordinaryCheckoutRestartRequest: {
              ...request,
              providerSessionId: ProviderSessionId.make("different-session"),
            },
          })
          .pipe(Effect.flip);
        assert.equal(
          mismatchedTarget.cause,
          "The restart control differs from its complete actual claimed request.",
        );
        assert.deepEqual(calls, []);
        const changedFullBody = yield* service
          .interruptAndAwaitTerminal({
            ...input,
            ...carrier,
            ordinaryCheckoutRestartRequest: { ...request, sessionTransition: { type: "detach" } },
          })
          .pipe(Effect.flip);
        assert.equal(
          changedFullBody.cause,
          "The provider control has no matching original checkout claim.",
        );
        assert.deepEqual(calls, []);
        yield* service.interruptAndAwaitTerminal({
          ...input,
          ...carrier,
          ordinaryCheckoutRestartRequest: request,
        });
        assert.deepEqual(calls, ["revalidate", "revalidate", "interrupt", "revalidate"]);
      }).pipe(Effect.provide(testLayer));
    }),
);
