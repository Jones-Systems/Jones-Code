import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import {
  SteerOutcomeUnknown,
  SteerNotDispatched,
  findSteerFailure,
} from "../jones/orchestration/LateSteer.ts";
import { ControlAdmissionHeld } from "../jones/orchestration/ControlAdmission.ts";
import { GoalNotificationDeliveryUnknown } from "../jones/goals/GoalNotificationDelivery.ts";
import * as RestartPreparation from "../jones/orchestration/RestartPreparation.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as ResourceCleanupService from "./ResourceCleanupService.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as ServerConfig from "../config.ts";
import * as PreviewManager from "../preview/Manager.ts";
import { LegacyOwnedTerminalControl } from "./RecordedTypes.ts";
import { legacyBootstrapCreateCommandId } from "./LegacyBootstrap.ts";
import * as Schema from "effect/Schema";
import { assert, it } from "@effect/vitest";
import {
  CheckpointId,
  CheckpointScopeId,
  CommandId,
  MessageId,
  NodeId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  VcsProcessExitError,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { CurrentEffectClaim, guardClaim } from "../jones/workspaceLifecycle/ActiveEffectClaim.ts";
import { LifecycleConfiguration } from "../jones/workspaceLifecycle/NativeSource.ts";
import { ProviderControlOutcomeUnknown } from "@t3tools/provider-core/server/jones/providerControlOutcome";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as NativeAuthority from "../jones/nativeCreation/NativeCreationAuthority.ts";
import * as NativeRepository from "../jones/nativeCreation/NativeCreationRepository.ts";
import * as NativeExecution from "../jones/nativeCreation/NativeCreationProviderExecution.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";

import * as CheckpointRollbackService from "./CheckpointRollbackService.ts";
import * as CheckpointService from "./CheckpointService.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EffectWorker from "./EffectWorker.ts";
import { OrchestratorProjectionError } from "./Orchestrator.ts";
import * as RunFinalizationService from "./RunFinalizationService.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderTurnControlService from "./ProviderTurnControlService.ts";
import * as ProviderTurnStartService from "./ProviderTurnStartService.ts";
import * as RuntimeRequestService from "./RuntimeRequestService.ts";
import * as ThreadTitleRegenerationService from "./ThreadTitleRegenerationService.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ServerSettings from "../serverSettings.ts";

const threadId = ThreadId.make("thread:effect-worker-restart");
const oldSessionId = ProviderSessionId.make("provider-session:effect-worker-restart:old");
const providerThreadId = ProviderThreadId.make("provider-thread:effect-worker-restart");
const providerTurnId = ProviderTurnId.make("provider-turn:effect-worker-restart");
const attemptId = RunAttemptId.make("run-attempt:effect-worker-restart");
const runId = RunId.make("run:effect-worker-restart");

function restartEffect(
  now: DateTime.Utc,
  sessionTransition: NonNullable<
    Extract<
      EffectOutbox.OrchestrationEffectV2["request"],
      { readonly type: "provider-turn.restart" }
    >["sessionTransition"]
  >,
): EffectOutbox.OrchestrationEffectV2 {
  const timestamp = DateTime.formatIso(now);
  return {
    id: `effect:restart:${sessionTransition.type}`,
    commandId: CommandId.make(`command:restart:${sessionTransition.type}`),
    threadId,
    request: {
      type: "provider-turn.restart",
      providerSessionId: oldSessionId,
      providerThreadId,
      providerTurnId,
      interruptedAttemptId: attemptId,
      runId,
      sessionTransition,
    },
    status: "running",
    attemptCount: 1,
    availableAt: timestamp,
    leaseOwner: "test-worker",
    leaseExpiresAt: timestamp,
    createdAt: timestamp,
    updatedAt: timestamp,
    completedAt: null,
    lastError: null,
  };
}

function layerExecutorFor(input: {
  readonly events: Ref.Ref<ReadonlyArray<string>>;
  readonly failFirstStart?: Ref.Ref<boolean>;
  readonly threads?: Partial<ThreadManagementService.ThreadManagementService["Service"]>;
  readonly continueAfterRestart?: boolean;
  readonly steer?: ProviderTurnControlService.ProviderTurnControlServiceV2Shape["steer"];
  readonly rollback?: CheckpointRollbackService.CheckpointRollbackServiceV2Shape["execute"];
  readonly respond?: RuntimeRequestService.RuntimeRequestServiceV2["Service"]["respond"];
  readonly interrupt?: ProviderTurnControlService.ProviderTurnControlServiceV2Shape["interrupt"];
  readonly detach?: ProviderSessionManager.ProviderSessionManagerV2Shape["detach"];
  readonly getSession?: ProviderSessionManager.ProviderSessionManagerV2Shape["get"];
  readonly captureStartFailure?: ProviderTurnStartService.ProviderTurnStartServiceV2Shape["captureStartFailure"];
  readonly getReceipt?: CommandReceiptStore.CommandReceiptStoreV2Shape["getByCommandId"];
}) {
  const record = (event: string) => Ref.update(input.events, (events) => [...events, event]);
  const layerDependencies = Layer.mergeAll(
    Layer.succeed(
      ProviderTurnControlService.ProviderTurnControlServiceV2,
      ProviderTurnControlService.ProviderTurnControlServiceV2.of({
        interrupt: input.interrupt ?? (() => Effect.void),
        steer: input.steer ?? (() => Effect.void),
        interruptAndAwaitTerminal: (request) =>
          record(
            request.replacementProviderSessionId === undefined
              ? "interrupt"
              : `interrupt:${request.replacementProviderSessionId}`,
          ),
      }),
    ),
    Layer.succeed(
      ProviderSessionManager.ProviderSessionManagerV2,
      ProviderSessionManager.ProviderSessionManagerV2.of({
        isMcpCallerAttached: () => Effect.succeed(false),
        shutdown: Effect.void,
        open: () => Effect.die("unused open"),
        get: input.getSession ?? (() => Effect.succeedNone),
        isLive: () => Effect.succeed(false),
        close: () => Effect.void,
        closeInstance: () => Effect.void,
        release: () => record("release"),
        detach: input.detach ?? (() => record("detach")),
      }),
    ),
    Layer.succeed(
      ProviderTurnStartService.ProviderTurnStartServiceV2,
      ProviderTurnStartService.ProviderTurnStartServiceV2.of({
        ...(input.captureStartFailure === undefined
          ? {}
          : { captureStartFailure: input.captureStartFailure }),
        start: () =>
          Effect.gen(function* () {
            yield* record("start");
            if (
              input.failFirstStart !== undefined &&
              (yield* Ref.getAndSet(input.failFirstStart, false))
            ) {
              return yield* new ProviderTurnStartService.ProviderTurnStartError({
                runId,
                cause: "simulated first start failure",
              });
            }
          }),
      }),
    ),
    Layer.succeed(
      RunFinalizationService.RunFinalizationService,
      RunFinalizationService.RunFinalizationService.of({ finalize: () => Effect.void }),
    ),
    Layer.succeed(
      CheckpointRollbackService.CheckpointRollbackServiceV2,
      CheckpointRollbackService.CheckpointRollbackServiceV2.of({
        execute: input.rollback ?? (() => Effect.void),
      }),
    ),
    Layer.succeed(
      RuntimeRequestService.RuntimeRequestServiceV2,
      RuntimeRequestService.RuntimeRequestServiceV2.of({
        respond: input.respond ?? (() => Effect.void),
      }),
    ),
    Layer.succeed(
      ThreadTitleRegenerationService.ThreadTitleRegenerationService,
      ThreadTitleRegenerationService.ThreadTitleRegenerationService.of({
        execute: () => Effect.void,
      }),
    ),
  );
  return EffectWorker.layerExecutor.pipe(
    Layer.provide(
      Layer.mock(RestartPreparation.RestartPreparation)({
        run: ({ interrupt, detach, start }) =>
          interrupt.pipe(
            Effect.andThen(detach),
            Effect.mapError(
              (cause) =>
                new RestartPreparation.RestartPreparationError({
                  threadId,
                  stage: "interrupt",
                  cause,
                }),
            ),
            Effect.andThen(start),
          ),
        recordSteerFailure: () => Effect.void,
      }),
    ),
    Layer.provide(
      Layer.mergeAll(
        layerDependencies,
        Layer.mock(CommandReceiptStore.CommandReceiptStoreV2)({
          getByCommandId: input.getReceipt ?? (() => Effect.succeedNone),
        }),
        Layer.mock(ThreadManagementService.ThreadManagementService)(input.threads ?? {}),
        ServerSettings.layerTest(
          input.continueAfterRestart === true ? { continueThreadsAfterServerUpdate: true } : {},
        ),
      ),
    ),
  );
}

function ownedCleanupBinding() {
  const createCommandId = legacyBootstrapCreateCommandId(
    threadId,
    CommandId.make("owned-cleanup:C"),
  );
  return Schema.decodeSync(LegacyOwnedTerminalControl)({
    version: 1,
    threadId,
    runId,
    terminalId: "legacy-owned-setup",
    generation: "owned-cleanup:terminal-generation",
    preparationGeneration: "owned-cleanup:birth-generation",
    claimEventId: "owned-cleanup:claim",
    claimSequence: 1,
    claimReceiptSequence: 1,
    birthEventId: "owned-cleanup:birth",
    birthSequence: 2,
    birthReceiptSequence: 2,
    policy: {
      version: 1,
      createCommandId,
      birthCommandId: `${createCommandId}:initial-message`,
      releaseCommandId: "owned-cleanup:C",
      projectId: "owned-cleanup:project",
      threadId,
      messageId: "owned-cleanup:M",
      runId,
      payloadHash: "owned-cleanup:hash",
      ownsNewThread: true,
    },
  });
}
it.effect(
  "routes legacy owned terminal cleanup unchanged through the actual executor and cleanup owner",
  () =>
    Effect.gen(function* () {
      const calls: Array<{
        input: Parameters<TerminalManager.TerminalManager["Service"]["close"]>[0];
        binding: LegacyOwnedTerminalControl | undefined;
      }> = [];
      const binding = ownedCleanupBinding();
      const terminals = Layer.mock(TerminalManager.TerminalManager)({
        close: (
          input,
          options: Parameters<TerminalManager.TerminalManager["Service"]["close"]>[1] = {},
        ) =>
          Effect.sync(() => {
            calls.push({ input, binding: options?.legacyOwnedControl });
          }),
      });
      const unexpectedPreview = () =>
        Effect.die("unexpected preview operation during terminal cleanup");
      const cleanup = ResourceCleanupService.layer.pipe(
        Layer.provideMerge(
          Layer.mergeAll(
            terminals,
            Layer.mock(ScheduledTaskService.ScheduledTaskService)({
              deleteForThread: () => Effect.die("unexpected scheduled task cleanup"),
            }),
            Layer.succeed(PreviewManager.PreviewManager, {
              open: unexpectedPreview,
              navigate: unexpectedPreview,
              reportStatus: unexpectedPreview,
              markHeadless: unexpectedPreview,
              requestReveal: unexpectedPreview,
              resize: unexpectedPreview,
              adjust: unexpectedPreview,
              refresh: unexpectedPreview,
              close: unexpectedPreview,
              list: unexpectedPreview,
              events: Stream.die("unexpected preview events during terminal cleanup"),
              subscribeEvents: unexpectedPreview(),
            }),
            ServerConfig.layerTest(process.cwd(), { prefix: "legacy-owned-cleanup-route-" }),
          ),
        ),
      );
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const layer = layerExecutorFor({ events }).pipe(Layer.provide(cleanup));
      const now = yield* DateTime.now;
      const template = restartEffect(now, { type: "detach" });
      yield* Effect.gen(function* () {
        const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
        yield* executor.execute({
          ...template,
          request: { type: "terminal.cleanup", legacyOwnedControl: binding },
        });
        assert.deepEqual(calls, [{ input: { threadId, deleteHistory: true }, binding }]);
        yield* executor.execute({ ...template, request: { type: "terminal.cleanup" } });
        assert.deepEqual(calls[1], {
          input: { threadId, deleteHistory: true },
          binding: undefined,
        });
        assert.deepEqual(yield* Ref.get(events), []);
      }).pipe(Effect.provide(layer));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
it.effect("bound legacy terminal cleanup refuses a default no-op owner", () =>
  Effect.gen(function* () {
    const events = yield* Ref.make<ReadonlyArray<string>>([]);
    const layer = layerExecutorFor({ events });
    const template = restartEffect(yield* DateTime.now, { type: "detach" });
    yield* Effect.gen(function* () {
      const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
      const bound = yield* executor
        .execute({
          ...template,
          request: { type: "terminal.cleanup", legacyOwnedControl: ownedCleanupBinding() },
        })
        .pipe(Effect.result);
      assert.equal(bound._tag, "Failure");
      const native = yield* executor
        .execute({ ...template, request: { type: "terminal.cleanup" } })
        .pipe(Effect.result);
      assert.equal(native._tag, "Success");
    }).pipe(Effect.provide(layer));
  }),
);

it("does not retry pure interrupt races where the turn is already gone", () => {
  assert.isTrue(
    EffectWorker.isNonRetryableProviderTurnControlFailure(
      "provider-turn.interrupt",
      "ProviderAdapterInterruptError: ... ACP provider turn provider-turn:x is not active",
    ),
  );
  assert.isTrue(
    EffectWorker.isNonRetryableProviderTurnControlFailure(
      "provider-turn.interrupt",
      "Provider session provider-session:x is not active.",
    ),
  );
  assert.isFalse(
    EffectWorker.isNonRetryableProviderTurnControlFailure(
      "provider-turn.restart",
      "Provider session provider-session:x is not active.",
    ),
  );
  assert.isFalse(
    EffectWorker.isNonRetryableProviderTurnControlFailure(
      "provider-turn.start",
      "Provider session provider-session:x is not active.",
    ),
  );
  assert.isFalse(
    EffectWorker.isNonRetryableProviderTurnControlFailure(
      "provider-turn.interrupt",
      "ACP hard teardown failed unexpectedly; the session is poisoned",
    ),
  );
});

it.effect.each([
  { title: "settles only the run when the adapter reports the turn idle", probe: "idle" },
  { title: "settles nothing while the adapter still runs the turn", probe: "active" },
  { title: "settles nothing when the adapter cannot report the turn", probe: "unsupported" },
  { title: "settles nothing without a live session", probe: "no-session" },
] as const)("a failed root interrupt $title", ({ probe }) =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const events = yield* Ref.make<ReadonlyArray<string>>([]);
    const probedTurns: Array<string> = [];
    const isTurnActive = (turnId: ProviderTurnId) =>
      Effect.sync(() => {
        probedTurns.push(turnId);
        return probe === "active";
      });
    const probed: Pick<ProviderAdapter.ProviderAdapterV2SessionRuntime, "isTurnActive"> =
      probe === "unsupported" ? {} : { isTurnActive };
    const session = probed as ProviderAdapter.ProviderAdapterV2SessionRuntime;
    const layer = layerExecutorFor({
      events,
      // The idle adapter's query retirement never confirmed, so the interrupt outcome is unknown.
      interrupt: () =>
        new ProviderTurnControlService.ProviderTurnControlError({
          threadId,
          operation: "interrupt",
          providerTurnId,
          cause: new ProviderControlOutcomeUnknown({
            driver: "claudeAgent",
            providerSessionId: oldSessionId,
            nativeThreadId: "captured-native",
            operation: "retire",
          }),
        }),
      getSession: () => (probe === "no-session" ? Effect.succeedNone : Effect.succeedSome(session)),
      threads: {
        dispatch: (command) =>
          Ref.update(events, (current) => [
            ...current,
            command.type === "thread.background-work.settle"
              ? `${command.commandId}:${command.runOnly === true ? "run-only" : "background"}`
              : command.type,
          ]).pipe(Effect.as({ sequence: 1, storedEvents: [] })),
      },
    });
    const effect = {
      ...restartEffect(now, { type: "detach" }),
      request: {
        type: "provider-turn.interrupt" as const,
        providerSessionId: oldSessionId,
        providerThreadId,
        providerTurnId,
      },
    };
    const exit = yield* Effect.gen(function* () {
      const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
      return yield* Effect.exit(executor.execute(effect));
    }).pipe(Effect.provide(layer));
    assert.isTrue(Exit.isFailure(exit), "the unconfirmed interrupt still fails its effect");
    assert.deepEqual(probedTurns, probe === "idle" || probe === "active" ? [providerTurnId] : []);
    // Background work is never settled on an unknown outcome; only the idle turn's run is.
    assert.deepEqual(
      yield* Ref.get(events),
      probe === "idle" ? [`${effect.id}:idle-run-settled:run-only`] : [],
    );
  }),
);

it.effect("settles a stopped run when its adapter has already lost the native turn", () =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const events = yield* Ref.make<ReadonlyArray<string>>([]);
    const layer = layerExecutorFor({
      events,
      interrupt: () =>
        new ProviderTurnControlService.ProviderTurnControlError({
          threadId,
          operation: "interrupt",
          providerTurnId,
          cause: "Provider turn is not active.",
        }),
      threads: {
        dispatch: (command) =>
          Ref.update(events, (current) => [...current, command.type]).pipe(
            Effect.as({ sequence: 1, storedEvents: [] }),
          ),
      },
    });
    yield* Effect.gen(function* () {
      const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
      yield* executor.execute({
        ...restartEffect(now, { type: "detach" }),
        request: {
          type: "provider-turn.interrupt",
          providerSessionId: oldSessionId,
          providerThreadId,
          providerTurnId,
        },
      });
    }).pipe(Effect.provide(layer));
    assert.deepEqual(yield* Ref.get(events), ["thread.background-work.settle"]);
  }),
);

const lockedIndexMessage =
  "Git's index is locked. Wait for other Git operations to finish, then try again.";
const rollbackCheckpointId = CheckpointId.make("checkpoint:effect-worker-rollback");
const rollbackScopeId = CheckpointScopeId.make("checkpoint-scope:effect-worker-rollback");
const restoreFailure = (detail: string) =>
  new CheckpointRollbackService.CheckpointRollbackExecutionError({
    reason: "unexpected-failure",
    threadId,
    providerThreadId,
    checkpointId: rollbackCheckpointId,
    cause: new CheckpointService.CheckpointRestoreError({
      scopeId: rollbackScopeId,
      checkpointId: rollbackCheckpointId,
      cause: new VcsProcessExitError({
        operation: "GitVcsDriverCore.restoreCheckpoint",
        command: "git",
        cwd: "/private/repo",
        exitCode: 128,
        detail,
        failureKind: "command-failed",
      }),
    }),
  });

it.effect.each([
  {
    name: "a held Git index lock",
    failure: restoreFailure(lockedIndexMessage),
    message: lockedIndexMessage,
  },
  {
    name: "raw Git output",
    failure: restoreFailure("error: could not clean /private/repo/secret.txt"),
    message: CheckpointRollbackService.ROLLBACK_FAILED_MESSAGE,
  },
])("reports a rollback blocked by $name only after the last attempt", ({ failure, message }) =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const failures = yield* Ref.make<ReadonlyArray<string>>([]);
    const layer = layerExecutorFor({
      events: yield* Ref.make<ReadonlyArray<string>>([]),
      rollback: () => Effect.fail(failure),
      threads: {
        dispatch: (command) =>
          Ref.update(failures, (current) =>
            command.type === "checkpoint.rollback.fail" ? [...current, command.message] : current,
          ).pipe(Effect.as({ sequence: 1, storedEvents: [] })),
      },
    });
    yield* Effect.gen(function* () {
      const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
      const rollback: EffectOutbox.OrchestrationEffectV2 = {
        ...restartEffect(now, { type: "detach" }),
        request: {
          type: "provider-thread.rollback",
          providerThreadId,
          checkpointId: rollbackCheckpointId,
          scopeId: rollbackScopeId,
          restoreFiles: true,
        },
      };
      yield* Effect.flip(executor.execute(rollback, { willRetry: true }));
      assert.deepEqual(yield* Ref.get(failures), []);
      yield* Effect.flip(executor.execute(rollback, { willRetry: false }));
    }).pipe(Effect.provide(layer));
    assert.deepEqual(yield* Ref.get(failures), [message]);
  }),
);

it.effect("does not report an answer whose last delivery attempt is interrupted", () =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const reported = yield* Ref.make(0);
    const layer = layerExecutorFor({
      events: yield* Ref.make<ReadonlyArray<string>>([]),
      respond: () => Effect.interrupt,
      threads: {
        dispatch: (command) =>
          Ref.update(reported, (count) =>
            command.type === "runtime-request.delivery.fail" ? count + 1 : count,
          ).pipe(Effect.as({ sequence: 1, storedEvents: [] })),
      },
    });
    const exit = yield* Effect.gen(function* () {
      const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
      return yield* Effect.exit(
        executor.execute(
          {
            ...restartEffect(now, { type: "detach" }),
            request: {
              type: "runtime-request.respond",
              providerSessionId: oldSessionId,
              requestId: RuntimeRequestId.make("runtime-request:effect-worker-interrupted"),
              answers: { next: "Ship it" },
            },
          },
          { willRetry: false },
        ),
      );
    }).pipe(Effect.provide(layer));
    assert.isTrue(Exit.hasInterrupts(exit));
    assert.equal(yield* Ref.get(reported), 0);
  }),
);

it.effect("requeues a claim when a pre-execution worker check fails", () =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const effectId = "effect:worker-pre-execution-failure";
    const workerId = "worker-pre-execution-failure";
    const claimedEffect: EffectOutbox.OrchestrationEffectV2 = {
      id: effectId,
      commandId: CommandId.make("command:worker-pre-execution-failure"),
      threadId: ThreadId.make("thread:worker-pre-execution-failure"),
      request: { type: "terminal.cleanup" },
      status: "running",
      attemptCount: 1,
      availableAt: now,
      leaseOwner: workerId,
      leaseExpiresAt: now,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
      lastError: null,
    };
    const retries = yield* Ref.make<
      ReadonlyArray<{
        readonly effectId: string;
        readonly workerId: string;
        readonly error: string;
        readonly delayMs: number;
      }>
    >([]);
    const executionCount = yield* Ref.make(0);
    const layerOutbox = Layer.mock(EffectOutbox.EffectOutboxV2)({
      nextClaimableAt: () => Effect.succeedNone,
      claimNext: () => Effect.succeedSome(claimedEffect),
      get: () =>
        Effect.fail(
          new EffectOutbox.EffectOutboxError({
            operation: "get",
            effectId,
            cause: "simulated cancellation-state read failure",
          }),
        ),
      retry: (input) =>
        Ref.update(retries, (existing) => [...existing, input]).pipe(Effect.as(true)),
    });
    const layerExecutor = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({
        execute: () => Ref.update(executionCount, (count) => count + 1),
      }),
    );
    const layerWorker = EffectWorker.layerWithOptions({ workerId }).pipe(
      Layer.provide(Layer.merge(layerOutbox, layerExecutor)),
    );

    const exit = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
      Effect.flatMap((worker) => worker.runOnce),
      Effect.provide(layerWorker),
      Effect.exit,
    );

    assert.isTrue(Exit.isFailure(exit));
    if (Exit.isFailure(exit)) {
      assert.include(Cause.pretty(exit.cause), "simulated cancellation-state read failure");
    }
    assert.equal(yield* Ref.get(executionCount), 0);
    const retry = (yield* Ref.get(retries))[0];
    assert.isDefined(retry);
    assert.equal(retry.effectId, effectId);
    assert.equal(retry.workerId, workerId);
    assert.equal(retry.delayMs, 0);
    assert.include(retry.error, "simulated cancellation-state read failure");
  }),
);

it.effect("arms cancellation before the durable pre-execution check", () =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const effectId = "effect:worker-cancellation-registration-race";
    const workerId = "worker-cancellation-registration-race";
    const claimedEffect: EffectOutbox.OrchestrationEffectV2 = {
      id: effectId,
      commandId: CommandId.make("command:worker-cancellation-registration-race"),
      threadId: ThreadId.make("thread:worker-cancellation-registration-race"),
      request: { type: "terminal.cleanup" },
      status: "running",
      attemptCount: 1,
      availableAt: now,
      leaseOwner: workerId,
      leaseExpiresAt: now,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
      lastError: null,
    };
    const signal = yield* Deferred.make<void>();
    let cancellationArmed = false;
    const executionCount = yield* Ref.make(0);
    const settlementCount = yield* Ref.make(0);
    const layerOutbox = Layer.mock(EffectOutbox.EffectOutboxV2)({
      nextClaimableAt: () => Effect.succeedNone,
      claimNext: () => Effect.succeedSome(claimedEffect),
      awaitCancellation: () => {
        cancellationArmed = true;
        return Deferred.await(signal);
      },
      get: () =>
        Effect.gen(function* () {
          if (cancellationArmed) {
            yield* Deferred.succeed(signal, undefined);
          }
          return Option.some(claimedEffect);
        }),
      clearCancellation: () => Effect.void,
      succeed: () => Ref.update(settlementCount, (count) => count + 1).pipe(Effect.as(true)),
    });
    const layerExecutor = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({
        execute: () =>
          Effect.yieldNow.pipe(Effect.andThen(Ref.update(executionCount, (count) => count + 1))),
      }),
    );
    const layerWorker = EffectWorker.layerWithOptions({ workerId }).pipe(
      Layer.provide(Layer.merge(layerOutbox, layerExecutor)),
    );

    const exit = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
      Effect.flatMap((worker) => worker.runOnce),
      Effect.provide(layerWorker),
      Effect.exit,
    );

    if (Exit.isFailure(exit)) {
      assert.fail(Cause.pretty(exit.cause));
    }
    assert.equal(yield* Ref.get(executionCount), 0);
    assert.equal(yield* Ref.get(settlementCount), 0);
  }),
);

it.effect("terminalizes a process-bound claim when success settlement fails", () =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const effectId = "effect:worker-process-bound-settlement-failure";
    const workerId = "worker-process-bound-settlement-failure";
    const claimedEffect: EffectOutbox.OrchestrationEffectV2 = {
      id: effectId,
      commandId: CommandId.make("command:worker-process-bound-settlement-failure"),
      threadId: ThreadId.make("thread:worker-process-bound-settlement-failure"),
      request: {
        type: "provider-turn.start",
        runId: RunId.make("run:worker-process-bound-settlement-failure"),
      },
      status: "running",
      attemptCount: 1,
      availableAt: now,
      leaseOwner: workerId,
      leaseExpiresAt: now,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
      lastError: null,
    };
    const retries = yield* Ref.make(0);
    const terminalErrors = yield* Ref.make<ReadonlyArray<string>>([]);
    const executionCount = yield* Ref.make(0);
    const layerOutbox = Layer.mock(EffectOutbox.EffectOutboxV2)({
      nextClaimableAt: () => Effect.succeedNone,
      claimNext: () => Effect.succeedSome(claimedEffect),
      get: () => Effect.succeedSome(claimedEffect),
      awaitCancellation: () => Effect.never,
      clearCancellation: () => Effect.void,
      succeed: () =>
        Effect.fail(
          new EffectOutbox.EffectOutboxError({
            operation: "succeed",
            effectId,
            cause: "simulated success settlement failure",
          }),
        ),
      retry: () => Ref.update(retries, (count) => count + 1).pipe(Effect.as(true)),
      fail: ({ error }) =>
        Ref.update(terminalErrors, (existing) => [...existing, error]).pipe(Effect.as(true)),
    });
    const layerExecutor = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({
        execute: () => Ref.update(executionCount, (count) => count + 1),
      }),
    );
    const layerWorker = EffectWorker.layerWithOptions({ workerId }).pipe(
      Layer.provide(Layer.merge(layerOutbox, layerExecutor)),
    );

    const exit = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
      Effect.flatMap((worker) => worker.runOnce),
      Effect.provide(layerWorker),
      Effect.exit,
    );

    assert.isTrue(Exit.isFailure(exit));
    assert.equal(yield* Ref.get(executionCount), 1);
    assert.equal(yield* Ref.get(retries), 0);
    const terminalError = (yield* Ref.get(terminalErrors))[0];
    assert.isDefined(terminalError);
    assert.include(terminalError, "after execution started");
    assert.include(terminalError, "simulated success settlement failure");
  }),
);

it.effect(
  "renewal refusal follows the executor failure path once and never retries a process-bound start",
  () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const timestamp = DateTime.formatIso(now);
      const workerId = "worker:lost-custody";
      const captured: EffectOutbox.OrchestrationEffectV2 = {
        id: "effect:lost-custody",
        commandId: CommandId.make("command:lost-custody"),
        threadId,
        request: { type: "provider-turn.start", runId: RunId.make("run:lost-custody") },
        status: "running",
        attemptCount: 1,
        availableAt: timestamp,
        leaseOwner: workerId,
        leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { seconds: 30 })),
        createdAt: timestamp,
        updatedAt: timestamp,
        completedAt: null,
        lastError: null,
      };
      const entered = yield* Deferred.make<void>();
      let renewals = 0;
      let retries = 0;
      let failures = 0;
      let runFailures = 0;
      const outbox = Layer.mock(EffectOutbox.EffectOutboxV2)({
        claimNext: () => Effect.succeedSome(captured),
        get: () => Effect.succeedSome(captured),
        awaitCancellation: () => Effect.never,
        clearCancellation: () => Effect.void,
        renew: () => {
          renewals++;
          return Effect.fail(new EffectOutbox.EffectClaimRenewalRefused({ effectId: captured.id }));
        },
        retry: () =>
          Effect.sync(() => {
            retries++;
            return true;
          }),
        fail: (input) =>
          Effect.sync(() => {
            failures++;
            assert.strictEqual(input.claim?.attemptCount, captured.attemptCount);
            assert.strictEqual(input.claim?.leaseExpiresAt, captured.leaseExpiresAt);
            return true;
          }),
      });
      const executor = Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
        execute: () =>
          guardClaim(Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))).pipe(
            Effect.catchTags({
              EffectClaimLost: (cause) =>
                Effect.sync(() => {
                  runFailures++;
                }).pipe(
                  Effect.andThen(
                    Effect.fail(
                      new EffectWorker.OrchestrationEffectExecutionError({
                        effectId: captured.id,
                        effectType: captured.request.type,
                        cause,
                      }),
                    ),
                  ),
                ),
            }),
          ),
      });
      const worker = EffectWorker.layerWithOptions({ workerId }).pipe(
        Layer.provide(Layer.merge(outbox, executor)),
        Layer.provide(
          Layer.succeed(LifecycleConfiguration, {
            environmentId: "synthetic",
            projects: new Map(),
          }),
        ),
      );
      const pending = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
        Effect.flatMap((service) => service.runOnce),
        Effect.provide(worker),
        Effect.forkChild,
      );
      yield* Deferred.await(entered);
      yield* TestClock.adjust("10 seconds");
      assert.isTrue(yield* Fiber.join(pending));
      yield* TestClock.adjust("1 minute");
      assert.strictEqual(renewals, 1);
      assert.strictEqual(runFailures, 1);
      assert.strictEqual(failures, 1);
      assert.strictEqual(retries, 0);
    }),
);

it.effect.each([false, true])(
  "typed control uncertainty fails once despite legacy success text=%s",
  (legacyText) =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const stopId = `effect:typed-unknown:${legacyText}`;
      const nextId = `${stopId}:next`;
      yield* outbox.enqueue([
        {
          id: stopId,
          commandId: CommandId.make(`${stopId}:command`),
          threadId,
          request: {
            type: "provider-turn.interrupt",
            providerSessionId: oldSessionId,
            providerThreadId,
            providerTurnId,
          },
        },
        {
          id: nextId,
          commandId: CommandId.make(`${nextId}:command`),
          threadId,
          request: { type: "delegated-tasks.stop", reason: "test progress" },
        },
      ]);
      let stops = 0;
      const executor = Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
        execute: (effect) =>
          effect.id !== stopId
            ? Effect.void
            : Effect.sync(() => {
                stops++;
              }).pipe(
                Effect.andThen(
                  Effect.fail(
                    new EffectWorker.OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause: new ProviderTurnControlService.ProviderTurnControlError({
                        threadId,
                        operation: "interrupt",
                        providerTurnId,
                        cause: new ProviderControlOutcomeUnknown({
                          driver: "claudeAgent",
                          providerSessionId: oldSessionId,
                          nativeThreadId: "captured-native",
                          operation: "interrupt",
                          ...(legacyText
                            ? {
                                cause: new Error(
                                  "Provider turn is not active; treating as already stopped",
                                ),
                              }
                            : {}),
                        }),
                      }),
                    }),
                  ),
                ),
              ),
      });
      yield* Effect.gen(function* () {
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        assert.isTrue(yield* worker.runOnce);
        assert.isTrue(yield* worker.runOnce);
        assert.isFalse(yield* worker.runOnce);
      }).pipe(Effect.provide(EffectWorker.layer.pipe(Layer.provide(executor))));
      const completionFailure = yield* outbox.awaitCompletion([stopId]).pipe(Effect.flip);
      assert.equal(completionFailure.operation, "await-completion");
      assert.equal(completionFailure.effectId, stopId);
      yield* outbox.awaitCompletion([nextId]);
      const stop = Option.getOrThrow(yield* outbox.get(stopId));
      assert.equal(stop.status, "failed");
      assert.equal(stop.attemptCount, 1);
      assert.isNull(stop.leaseOwner);
      assert.isNull(stop.leaseExpiresAt);
      assert.isNotNull(stop.completedAt);
      assert.equal(Option.getOrThrow(yield* outbox.get(nextId)).status, "succeeded");
      assert.equal(stops, 1);
    }).pipe(Effect.provide(EffectOutbox.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)))),
);

it.effect.each([true, false])(
  "production interrupt executor preserves typed uncertainty=%s through outbox settlement",
  (typedUnknown) =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const stopId = `effect:executor-control:${typedUnknown}`;
      const nextId = `${stopId}:next`;
      const workerId = "worker:executor-control";
      const legacyCause = new Error("Provider turn is not active; treating as already stopped");
      const controlError = new ProviderTurnControlService.ProviderTurnControlError({
        threadId,
        operation: "interrupt",
        providerTurnId,
        cause: typedUnknown
          ? new ProviderControlOutcomeUnknown({
              driver: "claudeAgent",
              providerSessionId: oldSessionId,
              nativeThreadId: "captured-native",
              operation: "interrupt",
              cause: legacyCause,
            })
          : legacyCause,
      });
      assert.isTrue(
        EffectWorker.isNonRetryableProviderTurnControlFailure(
          "provider-turn.interrupt",
          Cause.pretty(Cause.fail(controlError)),
        ),
      );
      yield* outbox.enqueue([
        {
          id: stopId,
          commandId: CommandId.make(`${stopId}:command`),
          threadId,
          request: {
            type: "provider-turn.interrupt",
            providerSessionId: oldSessionId,
            providerThreadId,
            providerTurnId,
          },
        },
        {
          id: nextId,
          commandId: CommandId.make(`${nextId}:command`),
          threadId,
          request: { type: "provider-session.detach", providerSessionId: oldSessionId },
        },
      ]);
      const capturedClaims: Array<EffectOutbox.OrchestrationEffectV2> = [];
      let stops = 0;
      let failures = 0;
      let retries = 0;
      const observedOutbox = Layer.succeed(EffectOutbox.EffectOutboxV2, {
        ...outbox,
        claimNext: (input) =>
          outbox.claimNext(input).pipe(
            Effect.tap((claimed) =>
              Effect.sync(() => {
                if (Option.isSome(claimed) && claimed.value.id === stopId) {
                  capturedClaims.push(claimed.value);
                }
              }),
            ),
          ),
        fail: (input) =>
          Effect.sync(() => {
            failures++;
            assert.equal(input.effectId, stopId);
            assert.equal(input.workerId, workerId);
            assert.isDefined(capturedClaims[0]);
            assert.deepEqual(input.claim, capturedClaims[0]);
          }).pipe(Effect.andThen(outbox.fail(input))),
        retry: (input) =>
          Effect.sync(() => {
            retries++;
          }).pipe(Effect.andThen(outbox.retry(input))),
      });
      const executor = layerExecutorFor({
        events,
        interrupt: () =>
          Effect.gen(function* () {
            stops++;
            yield* Ref.update(events, (current) => [...current, "interrupt"]);
            return yield* controlError;
          }),
        threads: {
          dispatch: (command) =>
            Ref.update(events, (current) => [...current, command.type]).pipe(
              Effect.as({ sequence: 1, storedEvents: [] }),
            ),
        },
      });
      yield* Effect.gen(function* () {
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        assert.isTrue(yield* worker.runOnce);
        const stop = Option.getOrThrow(yield* outbox.get(stopId));
        assert.equal(stop.status, typedUnknown ? "failed" : "succeeded");
        assert.equal(stop.attemptCount, 1);
        assert.isNull(stop.leaseOwner);
        assert.isNull(stop.leaseExpiresAt);
        assert.isNotNull(stop.completedAt);
        assert.isTrue(yield* worker.runOnce);
        assert.isFalse(yield* worker.runOnce);
      }).pipe(
        Effect.provide(
          EffectWorker.layerWithOptions({ workerId, maxAttempts: 3 }).pipe(
            Layer.provide(executor),
            Layer.provide(observedOutbox),
          ),
        ),
      );
      if (typedUnknown) {
        const completionFailure = yield* outbox.awaitCompletion([stopId]).pipe(Effect.flip);
        assert.equal(completionFailure.operation, "await-completion");
        assert.equal(completionFailure.effectId, stopId);
      } else {
        yield* outbox.awaitCompletion([stopId]);
      }
      yield* outbox.awaitCompletion([nextId]);
      assert.lengthOf(capturedClaims, 1);
      assert.equal(capturedClaims[0]?.leaseOwner, workerId);
      assert.equal(capturedClaims[0]?.attemptCount, 1);
      assert.equal(failures, typedUnknown ? 1 : 0);
      assert.equal(retries, 0);
      assert.equal(stops, 1);
      assert.equal(Option.getOrThrow(yield* outbox.get(nextId)).status, "succeeded");
      assert.deepEqual(
        yield* Ref.get(events),
        typedUnknown
          ? ["interrupt", "detach"]
          : ["interrupt", "thread.background-work.settle", "detach"],
      );
    }).pipe(Effect.provide(EffectOutbox.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)))),
);

it.effect("managed retryable startup keeps its original retry and domain failure policy", () =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const events = yield* Ref.make<ReadonlyArray<string>>([]);
    const failFirstStart = yield* Ref.make(true);
    let failures = 0;
    const effect = {
      ...restartEffect(now, { type: "detach" }),
      request: { type: "provider-turn.start" as const, runId },
    };
    yield* Effect.gen(function* () {
      const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(executor.execute(effect, { willRetry: true }))),
      );
      yield* executor.execute(effect, { willRetry: true });
      assert.equal(failures, 0);
      assert.deepEqual(yield* Ref.get(events), ["start", "start"]);
    }).pipe(
      Effect.provide(
        Layer.merge(
          layerExecutorFor({
            events,
            failFirstStart,
            captureStartFailure: () =>
              Effect.succeed(() =>
                Effect.sync(() => {
                  failures++;
                }),
              ),
          }),
          Layer.mock(CurrentEffectClaim)({ nativeCustody: true, hasLost: Effect.succeed(false) }),
        ),
      ),
    );
  }),
);

it.effect.each(["issue", "successor"] as const)(
  "native %s failure records the captured run before outbox settlement",
  (stage) =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const timestamp = DateTime.formatIso(now);
      const captured: EffectOutbox.OrchestrationEffectV2 = {
        id: `effect:native-preissue:${stage}`,
        commandId: CommandId.make(`command:native-preissue:${stage}`),
        threadId,
        request: { type: "provider-turn.start", runId },
        status: "running",
        attemptCount: 1,
        leaseOwner: "worker:preissue",
        leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { seconds: 30 })),
        availableAt: timestamp,
        createdAt: timestamp,
        updatedAt: timestamp,
        completedAt: null,
        lastError: null,
        nativeCreationExecutionReference: {
          version: 2,
          claimId: "claim:preissue",
          effectId: `effect:native-preissue:${stage}`,
          stageCommandId: CommandId.make(`command:native-preissue:${stage}`),
          stage: "native_command",
        },
      };
      let current = captured;
      let issues = 0;
      let holds = 0;
      let runFailures = 0;
      let effectFailures = 0;
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const executor = layerExecutorFor({
        events,
        captureStartFailure: () =>
          Effect.sync(() => {
            if (stage !== "issue") current = { ...captured, attemptCount: 2 };
            return () =>
              Effect.sync(() => {
                runFailures++;
              });
          }),
      });
      const outbox = Layer.mock(EffectOutbox.EffectOutboxV2)({
        claimNext: () => Effect.succeedSome(captured),
        get: () => Effect.sync(() => Option.some(current)),
        awaitCancellation: () => Effect.never,
        clearCancellation: () => Effect.void,
        fail: (input) =>
          Effect.sync(() => {
            assert.equal(runFailures, 1, "domain failure precedes the outbox CAS");
            assert.equal(input.claim?.attemptCount, 1);
            if (current.attemptCount !== input.claim?.attemptCount) return false;
            effectFailures++;
            return true;
          }),
      });
      const native = Layer.mergeAll(
        Layer.mock(NativeAuthority.NativeCreationAuthority)({
          issueExecution: () =>
            Effect.sync(() => {
              issues++;
            }).pipe(
              Effect.andThen(
                Effect.fail(
                  new NativeRepository.NativeCreationRepositoryError({
                    code: "unresolved_claim",
                    message: "Synthetic lost issue reply",
                  }),
                ),
              ),
            ),
        }),
        Layer.mock(NativeRepository.NativeCreationRepository)({
          confirmExecution: () => Effect.die("unconfirmed native operation cannot confirm"),
          holdExecution: () =>
            Effect.sync(() => {
              holds++;
            }),
        }),
        Layer.mock(NativeExecution.NativeCreationProviderExecutor)({
          executeWholeOperation: () => Effect.die("pre-issue failure must not dispatch"),
        }),
      );
      const exit = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
        Effect.flatMap((worker) => worker.runOnce),
        Effect.provide(
          Layer.merge(
            EffectWorker.layerWithOptions({ workerId: "worker:preissue" }).pipe(
              Layer.provide(Layer.merge(outbox, executor)),
            ),
            native,
          ),
        ),
        Effect.exit,
      );
      assert.equal(runFailures, 1);
      assert.equal(issues, stage === "issue" ? 1 : 0);
      assert.equal(holds, stage === "issue" ? 1 : 0);
      assert.equal(effectFailures, stage === "issue" ? 1 : 0);
      assert.equal(current.attemptCount, stage === "issue" ? 1 : 2);
      assert.equal(exit._tag, stage === "issue" ? "Success" : "Failure");
      assert.deepEqual(yield* Ref.get(events), []);
    }),
);

it.effect("settles a delegated child once its restart continuation fails for good", () =>
  Effect.gen(function* () {
    const timestamp = DateTime.formatIso(yield* DateTime.now);
    const events = yield* Ref.make<ReadonlyArray<string>>([]);
    const recovered = yield* Ref.make<ReadonlyArray<ThreadId>>([]);
    const layer = layerExecutorFor({
      events,
      continueAfterRestart: true,
      threads: {
        getThreadRecords: () => Effect.fail(new Error("provider instance removed") as never),
        recoverDelegatedTask: (childThreadId) =>
          Ref.update(recovered, (ids) => [...ids, childThreadId]),
      },
    });
    const effect: EffectOutbox.OrchestrationEffectV2 = {
      id: `effect:restart-continuation:${runId}`,
      commandId: CommandId.make("command:restart-continuation-failure"),
      threadId,
      request: { type: "provider-runtime.continue", sourceRunId: runId },
      status: "running",
      attemptCount: 1,
      availableAt: timestamp,
      leaseOwner: "test-worker",
      leaseExpiresAt: timestamp,
      createdAt: timestamp,
      updatedAt: timestamp,
      completedAt: null,
      lastError: null,
    };
    yield* Effect.gen(function* () {
      const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(executor.execute(effect, { willRetry: true }))),
      );
      assert.deepEqual(yield* Ref.get(recovered), []);
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(executor.execute(effect, { willRetry: false }))),
      );
      assert.deepEqual(yield* Ref.get(recovered), [threadId]);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect.each(["confirmed", "unconfirmed", "failed"] as const)(
  "a native subagent stop only repairs %s provider evidence",
  (outcome) =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const layer = layerExecutorFor({
        events,
        interrupt: (input) => {
          assert.equal(input.subagent?.nativeTaskId, "native-task");
          return Ref.update(events, (current) => [...current, "stop-child"]).pipe(
            Effect.andThen(
              outcome === "failed"
                ? new ProviderTurnControlService.ProviderTurnControlError({
                    threadId,
                    providerTurnId,
                    operation: "interrupt",
                    cause: "SDK stop failed",
                  })
                : Effect.succeed(
                    outcome === "confirmed"
                      ? {
                          status: "cancelled" as const,
                          completedAt: now,
                          result: "Stopped by Claude",
                        }
                      : undefined,
                  ),
            ),
          );
        },
        threads: {
          dispatch: (command) => {
            assert.equal(command.type, "thread.background-work.settle");
            if (command.type === "thread.background-work.settle") {
              assert.equal(command.subagent?.id, "subagent");
              assert.deepEqual(command.subagent?.outcome, {
                status: "cancelled",
                completedAt: now,
                result: "Stopped by Claude",
              });
            }
            return Ref.update(events, (current) => [...current, "settle-child"]).pipe(
              Effect.as({ sequence: 1, storedEvents: [] }),
            );
          },
        },
      });
      const execution = yield* Effect.gen(function* () {
        const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
        yield* executor.execute({
          ...restartEffect(now, { type: "detach" }),
          request: {
            type: "provider-turn.interrupt",
            providerSessionId: oldSessionId,
            providerThreadId,
            providerTurnId,
            subagent: { id: NodeId.make("subagent"), nativeTaskId: "native-task" },
          },
        });
      }).pipe(Effect.provide(layer), Effect.exit);
      assert.equal(Exit.isFailure(execution), outcome === "failed");
      assert.deepEqual(
        yield* Ref.get(events),
        outcome === "confirmed" ? ["stop-child", "settle-child"] : ["stop-child"],
      );
    }),
);

it.effect(
  "requeues a definitively undelivered notification with stable message and command identity",
  () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const messageId = MessageId.make("notification-steer-race");
      const notification = {
        source: { kind: "monitor" as const },
        outcome: "updated",
        summary: "PR ready",
      };
      const commands: unknown[] = [];
      const effect: EffectOutbox.OrchestrationEffectV2 = {
        ...restartEffect(now, { type: "detach" }),
        id: "effect:notification-steer-race",
        request: {
          type: "provider-turn.steer",
          providerSessionId: oldSessionId,
          providerThreadId,
          providerTurnId,
          messageId,
        },
      };
      yield* Effect.gen(function* () {
        const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
        yield* executor.execute(effect);
        yield* executor.execute(effect);
        assert.lengthOf(commands, 2);
        assert.deepEqual(commands[0], commands[1]);
        assert.nestedPropertyVal(commands[0], "dispatchMode.type", "queue_after_active");
        assert.propertyVal(commands[0], "messageId", messageId);
        assert.deepEqual((commands[0] as { notification: unknown }).notification, notification);
      }).pipe(
        Effect.provide(
          layerExecutorFor({
            events,
            steer: () =>
              Effect.fail(
                new ProviderTurnControlService.ProviderTurnControlError({
                  threadId,
                  operation: "steer",
                  providerTurnId,
                  turnCompleted: true,
                }),
              ),
            threads: {
              getThreadRecords: () =>
                Effect.succeed({
                  messages: [
                    {
                      id: messageId,
                      runId,
                      text: "PR ready",
                      attachments: [],
                      createdBy: "agent",
                      creationSource: "server",
                      notification,
                    },
                  ],
                  runs: [
                    {
                      id: runId,
                      status: "completed",
                      modelSelection: { instanceId: "codex", model: "test" },
                    },
                  ],
                } as never),
              dispatch: (command) =>
                Effect.sync(() => {
                  commands.push(command);
                  return {} as never;
                }),
            },
          }),
        ),
      );
    }),
);

it.effect.each(["success", "unknown"] as const)(
  "retains notification delivery evidence when metadata becomes unavailable after %s",
  (outcome) =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const now = yield* DateTime.now;
      const messageId = MessageId.make("notification-post-send-read-failure");
      let sent = false;
      let sends = 0;
      const effect: EffectOutbox.OrchestrationEffectV2 = {
        ...restartEffect(now, { type: "detach" }),
        id: "effect:notification-post-send-read-failure",
        request: {
          type: "provider-turn.steer",
          providerSessionId: oldSessionId,
          providerThreadId,
          providerTurnId,
          messageId,
        },
      };
      yield* Effect.gen(function* () {
        const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
        const result = yield* executor.execute(effect).pipe(Effect.result);
        assert.equal(sends, 1);
        if (outcome === "success") assert.equal(result._tag, "Success");
        else {
          assert.equal(result._tag, "Failure");
          if (result._tag === "Failure")
            assert.nestedPropertyVal(
              result.failure,
              "cause._tag",
              "GoalNotificationDeliveryUnknown",
            );
        }
      }).pipe(
        Effect.provide(
          layerExecutorFor({
            events,
            steer: () =>
              Effect.gen(function* () {
                sent = true;
                sends++;
                if (outcome === "unknown")
                  return yield* new ProviderTurnControlService.ProviderTurnControlError({
                    threadId,
                    operation: "steer",
                    providerTurnId,
                    cause: "Response unavailable after send",
                  });
              }),
            threads: {
              getThreadRecords: () =>
                Effect.suspend(() =>
                  sent
                    ? Effect.fail(
                        new OrchestratorProjectionError({
                          threadId,
                          cause: "Metadata unavailable after send",
                        }),
                      )
                    : Effect.succeed({
                        messages: [
                          {
                            id: messageId,
                            runId,
                            text: "PR ready",
                            attachments: [],
                            createdBy: "agent",
                            creationSource: "server",
                            notification: {
                              source: { kind: "monitor" },
                              outcome: "updated",
                              summary: "PR ready",
                            },
                          },
                        ],
                        runs: [
                          { id: runId, modelSelection: { instanceId: "codex", model: "test" } },
                        ],
                      } as never),
                ),
            },
          }),
        ),
      );
    }),
);

it.effect.each(["single", "batch", "notification"] as const)(
  "typed unknown and held %s steering never becomes a completed-turn follow-up",
  (kind) =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const now = yield* DateTime.now;
      const messageId = MessageId.make("message:uncertain");
      const base = restartEffect(now, {
        type: "replace",
        replacementProviderSessionId: ProviderSessionId.make(
          "provider-session:effect-worker-restart:replacement",
        ),
      });
      const effect = {
        ...base,
        request: {
          providerSessionId: oldSessionId,
          providerThreadId,
          providerTurnId,
          messageId,
          ...(kind === "batch"
            ? {
                type: "provider-turn.steer-batch" as const,
                messageIds: [messageId, MessageId.make("message:second")],
              }
            : { type: "provider-turn.steer" as const }),
        },
      };
      for (const failure of [
        new SteerOutcomeUnknown({
          instanceId: "codex",
          nativeThreadId: "native-thread",
          nativeTurnId: "native-turn",
        }),
        new SteerNotDispatched({ instanceId: "codex" }),
        new ControlAdmissionHeld({ instanceId: "codex" }),
      ]) {
        const result = yield* EffectWorker.OrchestrationEffectExecutorV2.pipe(
          Effect.flatMap((executor) => executor.execute(effect)),
          Effect.exit,
          Effect.provide(
            layerExecutorFor({
              events,
              steer: () =>
                Effect.fail(
                  new ProviderTurnControlService.ProviderTurnControlError({
                    threadId,
                    operation: "steer",
                    providerTurnId,
                    turnCompleted: true,
                    cause:
                      kind === "notification"
                        ? new GoalNotificationDeliveryUnknown({ cause: failure })
                        : failure,
                  }),
                ),
              threads: {
                getThreadRecords: () =>
                  Effect.succeed({
                    messages: [
                      {
                        id: messageId,
                        runId,
                        text: "Steering message",
                        attachments: [],
                        createdBy: "agent",
                        creationSource: "server",
                        ...(kind === "notification"
                          ? {
                              notification: {
                                source: { kind: "monitor" },
                                outcome: "updated",
                                summary: "Steering message",
                              },
                            }
                          : {}),
                      },
                    ],
                    runs: [
                      {
                        id: runId,
                        status: "completed",
                        modelSelection: { instanceId: "codex", model: "test" },
                      },
                    ],
                  } as never),
                dispatch: () =>
                  Ref.update(events, (current) => [...current, "unexpected-follow-up"]).pipe(
                    Effect.as({} as never),
                  ),
              },
            }),
          ),
        );
        assert.isTrue(Exit.isFailure(result));
        if (Exit.isFailure(result)) assert.equal(findSteerFailure(result.cause), failure);
      }
      assert.deepEqual(yield* Ref.get(events), []);
    }),
);

it.effect("restart replacement requests confirmed writer retirement before start", () =>
  Effect.gen(function* () {
    const events = yield* Ref.make<ReadonlyArray<string>>([]);
    const detaches: Array<
      Parameters<ProviderSessionManager.ProviderSessionManagerV2Shape["detach"]>[0]
    > = [];
    yield* Effect.gen(function* () {
      const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
      const effect = restartEffect(yield* DateTime.now, { type: "detach" });
      yield* executor.execute(effect);
      assert.equal(detaches.length, 1);
      assert.isTrue(detaches[0]!.requireWriterRetirement);
      assert.equal(detaches[0]!.providerSessionId, oldSessionId);
      assert.equal(detaches[0]!.providerThreadId, providerThreadId);
      assert.isUndefined(detaches[0]!.replacementProviderSessionId);
      assert.deepEqual(yield* Ref.get(events), ["interrupt", "detach", "start"]);
    }).pipe(
      Effect.provide(
        layerExecutorFor({
          events,
          detach: (input) =>
            Effect.gen(function* () {
              detaches.push(input);
              yield* Ref.update(events, (current) => [...current, "detach"]);
            }),
        }),
      ),
    );
  }),
);

it.effect(
  "restart passes the exact committed replacement and recorded source to writer retirement",
  () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const replacementProviderSessionId = ProviderSessionId.make("restart:committed-replacement");
      const detaches: Array<
        Parameters<ProviderSessionManager.ProviderSessionManagerV2Shape["detach"]>[0]
      > = [];
      yield* Effect.gen(function* () {
        const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
        yield* executor.execute(
          restartEffect(yield* DateTime.now, { type: "replace", replacementProviderSessionId }),
        );
        assert.equal(detaches.length, 1);
        assert.equal(detaches[0]!.providerSessionId, oldSessionId);
        assert.equal(detaches[0]!.providerThreadId, providerThreadId);
        assert.equal(detaches[0]!.replacementProviderSessionId, replacementProviderSessionId);
        assert.isTrue(detaches[0]!.requireWriterRetirement);
        assert.deepEqual(yield* Ref.get(events), [
          `interrupt:${replacementProviderSessionId}`,
          "detach",
          "start",
        ]);
      }).pipe(
        Effect.provide(
          layerExecutorFor({
            events,
            detach: (input) =>
              Effect.gen(function* () {
                detaches.push(input);
                yield* Ref.update(events, (current) => [...current, "detach"]);
              }),
          }),
        ),
      );
    }),
);

it.effect(
  "the executor opts account-selection detaches in only after reading the accepted command receipt",
  () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const effect: EffectOutbox.OrchestrationEffectV2 = {
        ...restartEffect(now, { type: "detach" }),
        request: {
          type: "provider-session.detach",
          providerSessionId: oldSessionId,
          detail: "Provider or model selection changed.",
        },
      };
      const captured: Array<
        Parameters<ProviderSessionManager.ProviderSessionManagerV2Shape["detach"]>[0]
      > = [];
      yield* Effect.gen(function* () {
        const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
        yield* executor.execute(effect);
        yield* executor.execute({
          ...effect,
          request: {
            type: "provider-session.detach",
            providerSessionId: oldSessionId,
            detail: "Provider or model selection changed.",
            revokeMcpCredential: true,
          },
        });
        assert.equal(captured[0]!.retireOnAccountTransfer, true);
        assert.isUndefined(captured[1]!.retireOnAccountTransfer);
        assert.isTrue(captured[1]!.revokeMcpCredential);
      }).pipe(
        Effect.provide(
          layerExecutorFor({
            events,
            detach: (input) =>
              Effect.sync(() => {
                captured.push(input);
              }),
            getReceipt: (commandId) =>
              Effect.succeedSome({
                commandId,
                threadId,
                commandType: "thread.model-selection.set",
                status: "accepted",
                acceptedAt: now,
                resultSequence: 1,
                error: null,
              }),
          }),
        ),
      );
    }),
);
