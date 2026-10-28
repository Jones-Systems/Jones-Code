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
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

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
        get: () => Effect.succeedNone,
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
    const detaches: Array<Parameters<ProviderSessionManager.ProviderSessionManagerV2Shape["detach"]>[0]> = [];
    yield* Effect.gen(function* () {
      const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
      const effect = restartEffect(yield* DateTime.now, { type: "detach" });
      yield* executor.execute(effect);
      assert.equal(detaches.length, 1);
      assert.isTrue(detaches[0]!.requireWriterRetirement);
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
