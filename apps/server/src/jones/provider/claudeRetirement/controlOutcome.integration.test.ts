import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  MessageId,
  ProjectId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { findProviderControlOutcomeUnknown } from "@t3tools/provider-core/server/jones/providerControlOutcome";
import { makeProviderReplayGate } from "@t3tools/provider-testing/replayGate";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import { readProviderReplayTranscript } from "@t3tools/provider-testing/replayTranscript";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as ServerConfig from "../../../config.ts";
import * as Claude from "../../../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import {
  ClaudeOrchestratorReplayHarness,
  makeClaudeRestartReplayHarness,
} from "../../../orchestration-v2/Adapters/ClaudeAdapterV2.testkit.ts";
import * as EffectOutbox from "../../../orchestration-v2/EffectOutbox.ts";
import * as EffectWorker from "../../../orchestration-v2/EffectWorker.ts";
import * as EventSink from "../../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../../orchestration-v2/Orchestrator.ts";
import * as ProviderSessionManager from "../../../orchestration-v2/ProviderSessionManager.ts";
import * as Harness from "../../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import {
  CLAUDE_MODEL_SELECTION,
  MULTI_TURN_FIRST_PROMPT,
} from "../../../orchestration-v2/testkit/fixtures/shared.ts";

it.effect.each(["rejected", "silent"] as const)(
  "Claude %s cancel releases the exact failed claim while native retirement stays fenced",
  (mode) =>
    Effect.gen(function* () {
      const name = `claude-control-outcome-${mode}`;
      const fs = yield* FileSystem.FileSystem;
      const cwd = yield* checkpointWorkspace(name);
      const stateRoot = yield* fs.makeTempDirectoryScoped({ prefix: `${name}-state-` });
      const config = yield* Harness.makeReplayServerConfig(name).pipe(
        Effect.provideService(FileSystem.FileSystem, {
          ...fs,
          makeTempDirectory: (options) =>
            fs.makeTempDirectory({ ...options, directory: stateRoot }),
        }),
      );
      const recorded = yield* readProviderReplayTranscript(
        new URL(
          "../../../orchestration-v2/testkit/fixtures/multi_turn/claude_transcript.ndjson",
          import.meta.url,
        ),
      );
      const terminalIndex = recorded.entries.findIndex(
        (entry) => entry.type === "emit_inbound" && entry.label === "result",
      );
      const terminal = recorded.entries[terminalIndex];
      assert.ok(terminal?.type === "emit_inbound");
      const transcript = yield* ClaudeOrchestratorReplayHarness.decodeTranscript({
        ...recorded,
        entries: [
          ...recorded.entries.slice(0, terminalIndex),
          { type: "expect_outbound", frame: { type: "query.interrupt" } },
          { ...terminal, label: "late-root-result" },
        ],
      });
      const gate = makeProviderReplayGate(["late-root-result"]);
      const cancelled = yield* Deferred.make<void>();
      const releaseCancel = yield* Deferred.make<void>();
      const nativeExit = yield* Deferred.make<void>();
      const closeStarted = yield* Deferred.make<void>();
      let opens = 0;
      let cancels = 0;
      let closes = 0;
      const releaseGates = Effect.gen(function* () {
        gate.releaseAll();
        yield* Deferred.succeed(releaseCancel, undefined);
        yield* Deferred.succeed(nativeExit, undefined);
      });
      yield* Effect.addFinalizer(() => releaseGates);
      const replay = makeClaudeRestartReplayHarness(transcript, {
        replayGate: gate,
        retirementForQuery: () => {
          opens++;
          return {
            interrupt: Effect.sync(() => {
              cancels++;
            }).pipe(
              Effect.andThen(Deferred.succeed(cancelled, undefined)),
              Effect.andThen(
                mode === "rejected"
                  ? Effect.fail(
                      new Claude.ClaudeAgentSdkQueryRunnerError({
                        method: "interrupt",
                        cause: "synthetic cancellation rejection",
                      }),
                    )
                  : Deferred.await(releaseCancel),
              ),
            ),
            close: Effect.sync(() => {
              closes++;
            }).pipe(Effect.andThen(Deferred.succeed(closeStarted, undefined))),
            termination: Deferred.await(nativeExit),
          };
        },
      });
      const layer = Harness.layerWithRegistryAndServices(
        { name, runtimePolicyOverride: { cwd } },
        replay.harness.makeProviderAdapterRegistryLayer(transcript),
        {
          runEffectWorker: false,
          serverConfigLayer: Layer.succeed(ServerConfig.ServerConfig, config),
        },
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const sink = yield* EventSink.EventSinkV2;
        const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const threadId = ThreadId.make(`thread:${name}`);
        const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
          sink.stream({ threadId, afterSequence: 0 }).pipe(
            Stream.filter((stored) => predicate(stored.event)),
            Stream.take(1),
            Stream.runHead,
            Effect.forkScoped,
          );
        const daemon = yield* EffectWorker.runDaemonWithOptions({ concurrency: 1 }).pipe(
          Effect.forkScoped,
        );
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`${name}:create`),
          threadId,
          projectId: ProjectId.make(`project:${name}`),
          title: name,
          modelSelection: CLAUDE_MODEL_SELECTION,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        const running = yield* watch(
          (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
        );
        const startCommand = CommandId.make(`${name}:start`);
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: startCommand,
          threadId,
          messageId: MessageId.make(`${name}:first`),
          text: MULTI_TURN_FIRST_PROMPT,
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* Fiber.join(running);
        const initial = yield* orchestrator.getThreadProjection(threadId);
        const run = initial.runs[0]!;
        const turn = initial.providerTurns.find(
          (candidate) => candidate.runAttemptId === run.activeAttemptId,
        )!;
        const providerThread = initial.providerThreads.find(
          (candidate) => candidate.id === turn.providerThreadId,
        )!;
        assert.ok(providerThread.providerSessionId);
        const providerSessionId = providerThread.providerSessionId;
        const starts = (yield* outbox.listByCommandId(startCommand)).filter(
          (effect) => effect.request.type === "provider-turn.start",
        );
        assert.lengthOf(starts, 1);
        yield* outbox.awaitCompletion(starts.map((effect) => effect.id));
        const stopId = `effect:${name}:stop`;
        const nextId = `effect:${name}:same-thread-progress`;
        yield* outbox.enqueue([
          {
            id: stopId,
            commandId: CommandId.make(`${name}:stop`),
            threadId,
            request: {
              type: "provider-turn.interrupt",
              providerSessionId,
              providerThreadId: providerThread.id,
              providerTurnId: turn.id,
            },
          },
          {
            id: nextId,
            commandId: CommandId.make(`${name}:progress`),
            threadId,
            request: {
              type: "delegated-tasks.stop",
              reason: "synthetic same-thread capacity receipt",
            },
          },
        ]);
        yield* outbox.notifyAvailable(2);
        yield* Deferred.await(cancelled);
        assert.isTrue(yield* Effect.promise(() => gate.waitForReached("late-root-result")));
        const captured = Option.getOrThrow(yield* outbox.get(stopId));
        assert.equal(captured.status, "running");
        assert.equal(captured.attemptCount, 1);
        assert.equal(Option.getOrThrow(yield* outbox.get(nextId)).attemptCount, 0);
        yield* TestClock.adjust("2 seconds");
        yield* Deferred.await(closeStarted);
        assert.equal(closes, 1);
        assert.isFalse(yield* Deferred.isDone(nativeExit));
        yield* TestClock.adjust("8 seconds");
        const completionFailure = yield* outbox.awaitCompletion([stopId]).pipe(Effect.flip);
        assert.equal(completionFailure.operation, "await-completion");
        assert.equal(completionFailure.effectId, stopId);
        yield* outbox.awaitCompletion([nextId]);
        const stopped = Option.getOrThrow(yield* outbox.get(stopId));
        assert.equal(stopped.status, "failed");
        assert.equal(stopped.attemptCount, captured.attemptCount);
        assert.equal(stopped.commandId, captured.commandId);
        assert.isNull(stopped.leaseOwner);
        assert.equal(Option.getOrThrow(yield* outbox.get(nextId)).status, "succeeded");
        assert.isFalse(yield* Deferred.isDone(nativeExit));
        assert.equal(
          (yield* orchestrator.getThreadProjection(threadId)).providerTurns.find(
            (candidate) => candidate.id === turn.id,
          )?.status,
          "running",
        );

        const persistedTerminal = yield* watch(
          (event) =>
            event.type === "provider-turn.updated" &&
            event.payload.id === turn.id &&
            event.payload.status === "completed",
        );
        const persistedAttempt = yield* watch(
          (event) =>
            event.type === "run-attempt.updated" &&
            event.payload.id === run.activeAttemptId &&
            event.payload.providerSettlement?.status === "completed",
        );
        gate.release("late-root-result");
        const terminalReceipt = Option.getOrThrow(yield* Fiber.join(persistedTerminal));
        const attemptReceipt = Option.getOrThrow(yield* Fiber.join(persistedAttempt));
        assert.equal(terminalReceipt.event.runId, run.id);
        assert.equal(attemptReceipt.event.runId, run.id);
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(
          after.providerTurns.find((candidate) => candidate.id === turn.id)?.status,
          "completed",
        );
        assert.equal(
          after.attempts.find((candidate) => candidate.id === run.activeAttemptId)
            ?.providerSettlement?.status,
          "completed",
        );
        assert.isFalse(
          yield* Deferred.isDone(nativeExit),
          "persisted native result and native exit are independent",
        );

        const nextMessageId = MessageId.make(`${name}:second`);
        const failedAdmission = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            event.payload.userMessageId === nextMessageId &&
            event.payload.status === "failed",
        );
        const nextCommand = CommandId.make(`${name}:fenced-start`);
        const admission = yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: nextCommand,
          threadId,
          messageId: nextMessageId,
          text: "This prompt must remain fenced.",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        const createdAdmission = admission.storedEvents.find(
          (stored) =>
            stored.event.type === "run.created" &&
            stored.event.payload.userMessageId === nextMessageId,
        );
        assert.ok(createdAdmission?.event.type === "run.created");
        const admissionRun = createdAdmission.event.payload;
        assert.notEqual(admissionRun.id, run.id);
        assert.ok(admissionRun.activeAttemptId);
        const failedReceipt = Option.getOrThrow(yield* Fiber.join(failedAdmission));
        assert.ok(failedReceipt.event.type === "run.updated");
        assert.equal(failedReceipt.event.runId, admissionRun.id);
        assert.equal(failedReceipt.event.payload.id, admissionRun.id);
        assert.equal(failedReceipt.event.payload.activeAttemptId, admissionRun.activeAttemptId);
        const startEffects = (yield* outbox.listByThreadId(threadId)).filter(
          (effect) => effect.request.type === "provider-turn.start",
        );
        assert.lengthOf(startEffects, 2);
        const admissionEffects = startEffects.filter(
          (effect) =>
            effect.request.type === "provider-turn.start" &&
            effect.request.runId === admissionRun.id,
        );
        assert.lengthOf(admissionEffects, 1);
        assert.equal(
          admissionEffects[0]!.commandId,
          admissionRun.status === "queued"
            ? CommandId.make(`command:system:start-queued:${admissionRun.id}`)
            : nextCommand,
        );
        yield* outbox.awaitCompletion(admissionEffects.map((effect) => effect.id));
        const settledAdmission = Option.getOrThrow(yield* outbox.get(admissionEffects[0]!.id));
        assert.equal(settledAdmission.status, "succeeded");
        assert.equal(settledAdmission.attemptCount, 1);
        assert.isNull(settledAdmission.leaseOwner);
        const admissionAttempt = (yield* orchestrator.getThreadProjection(threadId)).attempts.find(
          (candidate) => candidate.id === admissionRun.activeAttemptId,
        );
        assert.ok(admissionAttempt);
        assert.equal(admissionAttempt.runId, admissionRun.id);
        assert.equal(admissionAttempt.status, "failed");
        assert.isNull(admissionAttempt.providerSettlement);
        assert.isFalse(yield* Deferred.isDone(nativeExit));
        yield* Deferred.succeed(nativeExit, undefined);
        yield* Deferred.succeed(releaseCancel, undefined);
        yield* replay.assertComplete;
        assert.equal(opens, 1);
        assert.equal(cancels, 1);
        assert.equal(closes, 1);
        assert.equal(Option.getOrThrow(yield* outbox.get(stopId)).status, "failed");
        assert.equal(Option.getOrThrow(yield* outbox.get(stopId)).attemptCount, 1);
        const released = yield* sessions
          .release({ providerSessionId, reason: "manual_shutdown" })
          .pipe(Effect.exit);
        assert.isTrue(
          Exit.isFailure(released),
          "the captured unknown outcome cannot become a successful scope-close receipt",
        );
        if (Exit.isFailure(released))
          assert.isDefined(findProviderControlOutcomeUnknown(released.cause));
        yield* Fiber.interrupt(daemon);
      }).pipe(Effect.ensuring(releaseGates), Effect.scoped, Effect.provide(layer));
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
);
