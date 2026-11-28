import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  ContextHandoffId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ThreadId,
} from "@t3tools/contracts";
import {
  historyResponseItems,
  renderHistory,
  selectHistory,
} from "@t3tools/provider-core/server/handoffBudget";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as CodexReplay from "effect-codex-app-server/replay";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";

import * as CodexAdapterV2 from "../../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CodexAdapterV2Testkit from "../../../orchestration-v2/Adapters/CodexAdapterV2.testkit.ts";
import {
  codexReplayPreamble,
  makeCodexReplayTranscript,
  makeCodexReplayTurn,
  producerCompletion,
  withCodexSetup,
  withReplayRequestId,
} from "../../../orchestration-v2/Adapters/CodexAdapterV2.testkit.ts";
import { withInlineContext } from "../../../orchestration-v2/ContextHandoffDelivery.ts";
import * as EffectWorker from "../../../orchestration-v2/EffectWorker.ts";
import * as EventSink from "../../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../../orchestration-v2/Orchestrator.ts";
import * as ProviderReplayHarness from "../../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Sqlite from "../../../persistence/Sqlite.ts";
import { EVENT_LOG_PRUNED_THROUGH_PROJECTION } from "../../persistence/EventLogPruneBoundary.ts";
import { CODEX_START_ACK_TIMEOUT_MS } from "./CodexLateStartRecovery.ts";

const CODEX_TEST_MODEL_SELECTION = {
  instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
  model: "gpt-5.4",
};
const threadTitle = "Late context refusal";
const CODEX_TEST_RUNTIME_POLICY = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: "/workspace",
});

const send = (
  orchestrator: Orchestrator.OrchestratorV2Shape,
  threadId: ThreadId,
  id: string,
  text: string,
) =>
  orchestrator.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make(id),
    threadId,
    messageId: MessageId.make(id),
    text,
    attachments: [],
    createdBy: "user",
    creationSource: "web",
    dispatchMode: { type: "start_immediately" },
  });

describe("Codex inline handoff rejection recovery", () => {
  it.effect("rejoins a stopped conversation without reconstructing its unconfirmed prompt", () =>
    Effect.gen(function* () {
      const scenario = "rejoin-after-stopped-start";
      const cwd = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped({
        prefix: "jones-codex-stopped-start-",
      });
      const threadId = ThreadId.make(scenario);
      const nativeThreadId = "stopped-start-conversation";
      const first = codexReplayPreamble({
        nativeThreadId,
        nativeTurnId: "stopped-start-original",
        prompt: "Original request",
        cwd,
      });
      const next = codexReplayPreamble({
        nativeThreadId,
        nativeTurnId: "stopped-start-next",
        prompt: "New authorized request",
        cwd,
      });
      const transcript = makeCodexReplayTranscript({
        scenario,
        entries: [
          ...first.slice(0, 6),
          {
            type: "emit_inbound",
            label: "original request enqueued",
            frame: {
              method: "thread/status/changed",
              params: { threadId: nativeThreadId, status: { type: "idle" } },
            },
          },
          {
            type: "expect_outbound",
            frame: { id: 4, method: "thread/unsubscribe", params: { threadId: nativeThreadId } },
          },
          { type: "emit_inbound", frame: { id: 4, result: { status: "unsubscribed" } } },
          {
            type: "emit_inbound",
            frame: { method: "thread/closed", params: { threadId: nativeThreadId } },
          },
          first[6]!,
          {
            type: "expect_outbound",
            frame: {
              id: 5,
              method: "thread/resume",
              params: {
                threadId: nativeThreadId,
                excludeTurns: true,
                ...CodexAdapterV2.codexThreadRuntimeParams({
                  threadId,
                  mcpSession: undefined,
                  modelSelection: CODEX_TEST_MODEL_SELECTION,
                  runtimePolicy: { ...CODEX_TEST_RUNTIME_POLICY, cwd },
                }),
              },
            },
          },
          {
            type: "emit_inbound",
            frame: {
              id: 5,
              result: {
                thread: { id: nativeThreadId, updatedAt: 1782622450 },
                model: CODEX_TEST_MODEL_SELECTION.model,
                modelProvider: "openai",
                serviceTier: null,
              },
            },
          },
          withReplayRequestId(next[5]!, 6),
          withReplayRequestId(next[6]!, 6),
          ...next.slice(7),
          producerCompletion(nativeThreadId, "stopped-start-next"),
        ],
      });
      const sent = yield* Deferred.make<void>();
      const sessionOpened = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2SessionRuntime>();
      const effectiveTranscript = withCodexSetup(transcript, threadTitle);
      const driver = yield* CodexReplay.makeReplayDriver(effectiveTranscript, {
        beforeEmitInbound: (entry) =>
          entry.label === "original request enqueued"
            ? Deferred.succeed(sent, undefined).pipe(Effect.asVoid)
            : Effect.void,
      });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const read = () => orchestrator.getThreadProjection(threadId);
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`create-${scenario}`),
          threadId,
          projectId: ProjectId.make(scenario),
          title: threadTitle,
          modelSelection: CODEX_TEST_MODEL_SELECTION,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        yield* send(orchestrator, threadId, "plain-original", "Original request");
        const starting = yield* worker.drain().pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(sent);
        const pending = yield* read();
        const [run] = pending.runs;
        assert.equal(run?.status, "running");
        assert.deepEqual(pending.providerTurns, []);
        yield* orchestrator.dispatch({
          type: "run.interrupt",
          commandId: CommandId.make("plain-stop"),
          threadId,
          runId: run!.id,
          holdQueue: true,
        });
        yield* Fiber.join(starting);
        const stopped = yield* read();
        assert.equal(stopped.runs[0]?.status, "interrupted");
        assert.deepEqual(stopped.contextHandoffs, []);
        const runtime = yield* Deferred.await(sessionOpened);
        yield* runtime.unloadThread!({ providerThread: stopped.providerThreads[0]! });
        yield* TestClock.adjust(0);
        const completed = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.threadId === threadId &&
              event.payload.id !== run!.id &&
              event.payload.status === "completed",
          ),
          Stream.runHead,
          Effect.forkChild({ startImmediately: true }),
        );
        yield* send(orchestrator, threadId, "plain-follow-up", "New authorized request");
        yield* worker.drain();
        const progress = yield* Ref.get(driver.state);
        assert.isNull(progress.failure, String(progress.failure));
        yield* Fiber.join(completed);
        const after = yield* read();
        assert.deepEqual(
          after.runs.map((run) => run.status),
          ["interrupted", "completed"],
        );
        assert.equal(after.providerThreads[0]?.nativeThreadRef?.nativeId, nativeThreadId);
        assert.deepEqual(after.contextHandoffs, []);
        assert.deepEqual(yield* Ref.get(driver.state), {
          failure: null,
          cursor: effectiveTranscript.entries.length,
        });
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistryAndServices(
            { name: scenario, runtimePolicyOverride: { cwd } },
            CodexAdapterV2Testkit.layer({
              transcript,
              driver,
              onSessionOpened: (runtime) =>
                Deferred.doneUnsafe(sessionOpened, Effect.succeed(runtime)),
            }),
            { runEffectWorker: false },
          ),
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect.each([
    { code: -32602, late: true, recovery: "no inline pending marker" },
    { code: -32600, late: false, recovery: "no inline pending marker" },
    { code: -32602, late: true, recovery: "pending without observed contradiction" },
    { code: -32600, late: false, recovery: "pending without observed contradiction" },
    { code: -32602, late: true, recovery: "contradiction while writer is held" },
    { code: -32602, late: true, recovery: "contradiction before outer commit" },
    { code: -32600, late: false, recovery: "contradiction before outer commit" },
    { code: -32602, late: true, recovery: "newer identical marker" },
    { code: -32601, late: false, recovery: "known local stop before prompt" },
    { code: -32600, late: false, recovery: "definitive direct injection rejection" },
    { code: null, late: false, recovery: "on-time ACK and terminal" },
    { code: null, late: false, recovery: "queued ACK without native input" },
    { code: null, late: false, recovery: "queued ACK followed by recorded input" },
    { code: null, late: false, recovery: "queued ACK after native completion" },
    { code: null, late: true, recovery: "late ACK and terminal" },
    { code: null, late: true, recovery: "late terminal before ACK" },
    { code: null, late: true, recovery: "late ACK without a terminal" },
    { code: null, late: true, recovery: "late ACK and interrupted prompt without input" },
    { code: null, late: true, recovery: "late ACK and interrupted prompt with observed input" },
    { code: null, late: false, recovery: "application Stop while awaiting ACK" },
    { code: null, late: false, recovery: "application Stop before queued ACK" },
    { code: null, late: false, recovery: "queued ACK during application Stop commit" },
    { code: null, late: true, recovery: "late ACK with a different terminal" },
    { code: null, late: true, recovery: "late acceptance after newer identical marker" },
    { code: null, late: true, recovery: "late acceptance after binding replacement" },
    { code: null, late: true, recovery: "late acceptance after pruning" },
    { code: null, late: true, recovery: "late acceptance with statement rollback" },
    { code: null, late: true, recovery: "process loss before late acceptance commits" },
    { code: null, late: true, recovery: "late acceptance contradicted before COMMIT" },
    { code: null, late: true, recovery: "readback of restart after COMMIT contradiction" },
    { code: null, late: true, recovery: "process loss after late receipt commits" },
  ] as const)("handles $recovery (reply code: $code, late: $late)", ({ code, late, recovery }) =>
    Effect.gen(function* () {
      const scenario = `inline-handoff-refusal-${code}-${recovery}`;
      const canContinue = recovery === "no inline pending marker";
      const stopBeforePrompt = recovery === "known local stop before prompt";
      const directInjectionRejection = recovery === "definitive direct injection rejection";
      const noPromptRequest = directInjectionRejection || stopBeforePrompt;
      const acceptsPrompt = code === null;
      const queuedCompletedBeforeAck = recovery === "queued ACK after native completion";
      const terminalFirst = recovery === "late terminal before ACK" || queuedCompletedBeforeAck;
      const queuedWithoutInput = recovery === "queued ACK without native input";
      const queuedWithInput = recovery === "queued ACK followed by recorded input";
      const ackOnly = recovery === "late ACK without a terminal" || queuedWithoutInput;
      const wrongTerminal = recovery === "late ACK with a different terminal";
      const interruptedWithoutInput = recovery === "late ACK and interrupted prompt without input";
      const interruptedWithInput =
        recovery === "late ACK and interrupted prompt with observed input";
      const queuedCommitRace = recovery === "queued ACK during application Stop commit";
      const queuedStop = recovery === "application Stop before queued ACK" || queuedCommitRace;
      const queued =
        queuedStop || queuedWithoutInput || queuedWithInput || queuedCompletedBeforeAck;
      const stopWhileAwaitingAck = recovery === "application Stop while awaiting ACK" || queuedStop;
      const nativeTerminalStatus =
        interruptedWithoutInput || interruptedWithInput || stopWhileAwaitingAck
          ? "interrupted"
          : "completed";
      const restartReadback = recovery === "readback of restart after COMMIT contradiction";
      const commitsInline =
        recovery === "on-time ACK and terminal" || queuedWithInput || queuedCompletedBeforeAck;
      const lostAfterCommit = recovery === "process loss after late receipt commits";
      const recordsLateReceipt =
        restartReadback ||
        lostAfterCommit ||
        recovery === "late ACK and terminal" ||
        (terminalFirst && late) ||
        interruptedWithInput;
      const positiveCommitRace =
        restartReadback || recovery === "late acceptance contradicted before COMMIT";
      const atCommit = recovery === "contradiction before outer commit" || positiveCommitRace;
      const lostBeforeCommit = recovery === "process loss before late acceptance commits";
      const reopenDatabase =
        atCommit ||
        commitsInline ||
        (acceptsPrompt && late) ||
        recovery === "pending without observed contradiction";
      const contradicts = recovery.startsWith("contradiction") || positiveCommitRace;
      const fileSystem = yield* FileSystem.FileSystem;
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "jones-codex-history-" });
      const path = yield* Path.Path;
      const database = reopenDatabase
        ? Sqlite.layerFromPath(path.join(cwd, "replay.sqlite")).pipe(
            Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
          )
        : Sqlite.layerMemory;
      const nativeThreadId = "inline-handoff-conversation";
      const threadId = ThreadId.make(scenario);
      const handoffId = ContextHandoffId.make("late-inline-handoff");
      const coverage = "History from a prior conversation.";
      const history = selectHistory({
        messages: [],
        coverage: `Context handoff (full_thread_summary):\n${coverage}`,
        budget: 16_000,
      });
      const preambleFor = (text: string, nativeTurnId: string) =>
        codexReplayPreamble({
          nativeThreadId,
          nativeTurnId,
          prompt: text,
          cwd,
          sentPrompt: canContinue
            ? text
            : withInlineContext(renderHistory(history.messages, history.context), text),
        });
      const originalNativeTurnId = "original-native-turn";
      const first = preambleFor("Original request", originalNativeTurnId);
      const followUp = codexReplayPreamble({
        nativeThreadId,
        nativeTurnId: "follow-up-turn",
        cwd,
        prompt: "New authorized request",
      });
      const originalRunId = IdAllocator.derive.run({ threadId, ordinal: 1 });
      const originalAttemptId = IdAllocator.derive.runAttempt({
        runId: originalRunId,
        attemptOrdinal: 1,
      });
      const codexDriver = ProviderDriverKind.make("codex");
      const originalProviderThreadId = IdAllocator.derive.providerThread({
        driver: codexDriver,
        nativeThreadId: `pending:${originalRunId}`,
      });
      const userItemId = IdAllocator.derive.userTurnItem({
        messageId: MessageId.make("original-inline-prompt"),
      });
      const refusedTurnId = IdAllocator.derive.providerTurn({
        driver: codexDriver,
        nativeTurnId: `failed:${originalAttemptId}`,
      });
      const failureItemId = IdAllocator.derive.turnItemFromProviderItem({
        driver: codexDriver,
        nativeItemId: `terminal-failure:${refusedTurnId}`,
      });
      const historicalFields = {
        threadId,
        runId: originalRunId,
        providerThreadId: originalProviderThreadId,
        runStatus: "failed",
      } as const;
      const recoveredHistory = selectHistory({
        messages: [
          {
            ...historicalFields,
            itemId: userItemId,
            role: "user",
            kind: "user_message",
            status: "completed",
            text: "Original request",
          },
          {
            ...historicalFields,
            itemId: failureItemId,
            role: "assistant",
            kind: "error",
            status: "failed",
            text: "The provider could not start this turn. Retry the turn; if it keeps failing, check the provider setup and server logs.",
          },
        ],
        coverage: [
          "Context handoff (delta_since_target_last_seen):",
          `Provider context handoff. Thread: ${threadId}. Covered app runs: 1-1.`,
          `Source item range: ${userItemId} through ${failureItemId}.`,
          `Recover an omitted item only when the request needs it, with t3_thread_read({threadId:"${threadId}",view:"activity",itemId:"<omitted item ID>",maxCharsPerItem:4000}); continue long text with textOffset=nextTextOffset until null. Items included below need no read. Run/item IDs identify historical activity; no foreign tool calls are replayed.`,
        ].join("\n"),
        budget: 16_000,
      });
      const inject = (id: number, followUp = false): CodexReplay.CodexAppServerReplayEntry[] => [
        {
          type: "expect_outbound",
          frame: {
            id,
            method: "thread/inject_items",
            params: {
              threadId: nativeThreadId,
              items: followUp
                ? historyResponseItems(recoveredHistory.messages, recoveredHistory.context)
                : historyResponseItems(history.messages, history.context),
            },
          },
        },
        {
          type: "emit_inbound",
          ...(noPromptRequest ? { label: "history request rejection" } : {}),
          frame: canContinue
            ? { id, result: {} }
            : {
                id,
                error: {
                  code: directInjectionRejection ? -32600 : -32601,
                  message: directInjectionRejection
                    ? "Rejected native history items"
                    : "Native injection unavailable",
                },
              },
        },
      ];
      const resumeNative = (id: number): CodexReplay.CodexAppServerReplayEntry[] => [
        {
          type: "expect_outbound",
          frame: {
            id,
            method: "thread/resume",
            params: {
              threadId: nativeThreadId,
              excludeTurns: true,
              ...CodexAdapterV2.codexThreadRuntimeParams({
                threadId,
                mcpSession: undefined,
                modelSelection: CODEX_TEST_MODEL_SELECTION,
                runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
              }),
            },
          },
        },
        {
          type: "emit_inbound",
          frame: {
            id,
            result: {
              thread: { id: nativeThreadId, updatedAt: 1782622440 },
              model: CODEX_TEST_MODEL_SELECTION.model,
              modelProvider: "openai",
              serviceTier: null,
            },
          },
        },
      ];
      const terminal = {
        type: "emit_inbound" as const,
        ...(terminalFirst ? { label: "first prompt response" } : {}),
        frame: {
          method: "turn/completed",
          params: {
            threadId: nativeThreadId,
            turn: makeCodexReplayTurn({
              id: wrongTerminal ? "different-native-turn" : originalNativeTurnId,
              status: nativeTerminalStatus,
            }),
          },
        },
      };
      const reply = {
        type: "emit_inbound" as const,
        label: terminalFirst ? "original ACK" : "first prompt response",
        frame: acceptsPrompt
          ? {
              id: 4,
              result: {
                turn: {
                  ...makeCodexReplayTurn({ id: originalNativeTurnId, status: "inProgress" }),
                  ...(queued ? { startedAt: null } : {}),
                },
              },
            }
          : { id: 4, error: { code, message: "Rejected original prompt" } },
      };
      const transcript = makeCodexReplayTranscript({
        scenario,
        entries: [
          ...first.slice(0, 5),
          ...inject(3),
          ...(noPromptRequest
            ? []
            : [
                withReplayRequestId(first[5]!, 4),
                ...(terminalFirst ? [terminal] : []),
                reply,
                ...(queuedWithoutInput
                  ? [
                      {
                        type: "emit_inbound" as const,
                        label: "queued turn starts",
                        frame: {
                          method: "turn/started",
                          params: {
                            threadId: nativeThreadId,
                            turn: makeCodexReplayTurn({
                              id: originalNativeTurnId,
                              status: "inProgress",
                            }),
                          },
                        },
                      },
                    ]
                  : []),
                ...(interruptedWithInput || queuedWithInput
                  ? [
                      {
                        type: "emit_inbound" as const,
                        ...(queuedWithInput ? { label: "queued prompt recorded" } : {}),
                        frame: {
                          method: "item/started",
                          params: {
                            threadId: nativeThreadId,
                            turnId: originalNativeTurnId,
                            startedAtMs: 1782622440000,
                            item: {
                              type: "userMessage",
                              id: "original-native-input",
                              content: [],
                            },
                          },
                        },
                      },
                    ]
                  : []),
                ...(stopWhileAwaitingAck
                  ? [
                      ...(queuedStop
                        ? [
                            {
                              type: "emit_inbound" as const,
                              label: "queued turn starts",
                              frame: {
                                method: "turn/started",
                                params: {
                                  threadId: nativeThreadId,
                                  turn: makeCodexReplayTurn({
                                    id: originalNativeTurnId,
                                    status: "inProgress",
                                  }),
                                },
                              },
                            },
                          ]
                        : []),
                      {
                        type: "expect_outbound" as const,
                        frame: {
                          id: 5,
                          method: "turn/interrupt",
                          params: { threadId: nativeThreadId, turnId: originalNativeTurnId },
                        },
                      },
                      { type: "emit_inbound" as const, frame: { id: 5, result: {} } },
                    ]
                  : []),
                ...(acceptsPrompt && !terminalFirst && !ackOnly ? [terminal] : []),
              ]),
          ...(contradicts
            ? [
                {
                  type: "emit_inbound" as const,
                  label: "contradictory turn activity",
                  frame: {
                    id: "after-rejection",
                    method: "mcpServer/elicitation/request",
                    params: {
                      threadId: nativeThreadId,
                      turnId: "contradictory-native-turn",
                      serverName: "synthetic-mcp",
                      message: "Synthetic elicitation",
                      mode: "url",
                      elicitationId: "fixture",
                      url: "https://example.com/fixture",
                    },
                  },
                },
                {
                  type: "expect_outbound" as const,
                  frame: { id: "after-rejection", result: { action: "cancel" } },
                },
              ]
            : []),
          ...(canContinue
            ? [
                ...resumeNative(5),
                ...inject(6, true),
                withReplayRequestId(followUp[5]!, 7),
                withReplayRequestId(followUp[6]!, 7),
                followUp[7]!,
                producerCompletion(nativeThreadId, "follow-up-turn"),
              ]
            : []),
        ],
      });
      const sent = yield* Deferred.make<void>();
      const allowRejection = yield* Deferred.make<void>();
      const rejectionObserved = yield* Deferred.make<void>();
      const queuedAckObserved = yield* Deferred.make<void>();
      const allowActivity = yield* Deferred.make<void>();
      let interruptsSent = 0;
      let unsubscribeRequests = 0;
      const writerParked = yield* Deferred.make<void>();
      const allowWriter = yield* Deferred.make<void>();
      const possibleRestoreFinished = yield* Deferred.make<void>();
      const activityObserved = yield* Deferred.make<void>();
      const sessionOpened = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2SessionRuntime>();
      const nativeStartReturned = yield* Deferred.make<void>();
      let parkStopCommit = false;
      let commitsUntilPark = 0;
      let commitIntercepted = false;
      const databaseLayer = Layer.effect(
        SqlClient.SqlClient,
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const withTransaction: typeof sql.withTransaction = (body) =>
            Effect.gen(function* () {
              if (
                parkStopCommit &&
                Option.isNone(yield* Effect.serviceOption(sql.transactionService)) &&
                (yield* Effect.currentSpan.pipe(
                  Effect.map((span) => span.name === "orchestrationV2.EventSink.commitThenPublish"),
                  Effect.orElseSucceed(() => false),
                ))
              ) {
                parkStopCommit = false;
                return yield* sql.withTransaction(
                  body.pipe(
                    Effect.tap(() =>
                      Deferred.succeed(writerParked, undefined).pipe(
                        Effect.andThen(Deferred.await(allowWriter)),
                      ),
                    ),
                  ),
                );
              }
              if (
                commitsUntilPark === 0 ||
                Option.isSome(yield* Effect.serviceOption(sql.transactionService))
              )
                return yield* sql.withTransaction(body);
              // A late ACK also reads Stop intent. Count only the receipt's
              // provider batches, so that read cannot move the crash boundary.
              if (
                acceptsPrompt &&
                !(yield* Effect.currentSpan.pipe(
                  Effect.map((span) => span.name === "orchestrationV2.EventSink.providerBatch"),
                  Effect.orElseSucceed(() => false),
                ))
              )
                return yield* sql.withTransaction(body);
              commitsUntilPark -= 1;
              if (commitsUntilPark > 0) return yield* sql.withTransaction(body);
              commitIntercepted = true;
              const park =
                lostBeforeCommit || positiveCommitRace || lostAfterCommit
                  ? Deferred.succeed(writerParked, undefined).pipe(
                      Effect.andThen(Deferred.await(allowWriter)),
                      Effect.interruptible,
                    )
                  : Deferred.await(allowWriter);
              // The interception is outside the batch body and its publication
              // lane acquisition. The post-commit variant stops before publication.
              const transaction = lostAfterCommit
                ? sql.withTransaction(body).pipe(Effect.tap(() => park))
                : sql.withTransaction(body.pipe(Effect.tap(() => park)));
              return yield* transaction.pipe(
                Effect.ensuring(Deferred.succeed(possibleRestoreFinished, undefined)),
              );
            });
          return new Proxy(sql, {
            get(target, key, receiver) {
              return key === "withTransaction"
                ? withTransaction
                : Reflect.get(target, key, receiver);
            },
          });
        }),
      ).pipe(Layer.provide(database));
      const effectiveTranscript = withCodexSetup(transcript, threadTitle);
      const driver = yield* CodexReplay.makeReplayDriver(effectiveTranscript, {
        beforeEmitInbound: (entry) =>
          entry.label === "first prompt response" || entry.label === "history request rejection"
            ? Deferred.succeed(sent, undefined).pipe(Effect.andThen(Deferred.await(allowRejection)))
            : entry.label === "contradictory turn activity" ||
                entry.label === "queued turn starts" ||
                entry.label === "queued prompt recorded"
              ? Deferred.await(allowActivity)
              : Effect.void,
      });
      const retained = yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sql = yield* SqlClient.SqlClient;
        const sink = yield* EventSink.EventSinkV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const getProjection = () => orchestrator.getThreadProjection(threadId);
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`create-${scenario}`),
          threadId,
          projectId: ProjectId.make(scenario),
          title: threadTitle,
          modelSelection: CODEX_TEST_MODEL_SELECTION,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        yield* send(orchestrator, threadId, "original-inline-prompt", "Original request");
        const run = (yield* getProjection()).runs[0];
        if (run?.providerThreadId == null)
          return yield* Effect.die("The start has no provider thread.");
        const now = yield* DateTime.now;
        yield* sink.write({
          events: [
            {
              id: EventId.make("initial-inline-handoff"),
              type: "context-handoff.updated",
              threadId,
              runId: run.id,
              occurredAt: now,
              payload: {
                id: handoffId,
                threadId,
                targetRunId: run.id,
                fromProviderThreadIds: [],
                toProviderThreadId: run.providerThreadId,
                coveredRunOrdinals: { from: 1, to: 1 },
                strategy: "full_thread_summary",
                status: "ready",
                summaryMessageId: null,
                summaryText: "",
                createdByProviderInstanceId: null,
                createdAt: now,
                updatedAt: now,
                history: { messages: [], coverage, omittedItems: 0 },
                ...(contradicts
                  ? {
                      delivery: {
                        nativeThreadId: "previous-conversation",
                        status: "pending",
                        itemIds: [],
                      },
                    }
                  : {}),
              },
            },
          ],
        });
        const starting = yield* worker.drain().pipe(Effect.forkChild({ startImmediately: true }));
        yield* Effect.raceFirst(
          Deferred.await(sent),
          Fiber.join(starting).pipe(
            Effect.andThen(Effect.die("The first start stopped before it sent the prompt.")),
          ),
        );
        const deliveryStatus = canContinue ? "injected" : "pending";
        assert.equal((yield* getProjection()).contextHandoffs[0]?.delivery?.status, deliveryStatus);
        if (directInjectionRejection) {
          yield* Deferred.succeed(allowRejection, undefined);
          yield* Fiber.join(starting);
          const after = yield* getProjection();
          assert.equal(after.runs[0]?.status, "failed");
          assert.isUndefined(after.contextHandoffs[0]?.delivery);
          assert.equal(after.providerThreads[0]?.nativeThreadRef?.nativeId, nativeThreadId);
          assert.deepEqual(yield* Ref.get(driver.state), {
            failure: null,
            cursor: effectiveTranscript.entries.length,
          });
          return;
        }
        if (stopBeforePrompt) {
          const current = (yield* getProjection()).runs[0]!;
          assert.equal(current.status, "running");
          const stoppedAt = yield* DateTime.now;
          yield* sink.write({
            events: [
              {
                id: EventId.make("review-local-stop-before-prompt"),
                type: "run.updated",
                threadId,
                runId: current.id,
                occurredAt: stoppedAt,
                payload: { ...current, status: "interrupted", completedAt: stoppedAt },
              },
            ],
          });
          yield* Deferred.succeed(allowRejection, undefined);
          yield* Fiber.join(starting);
          const after = yield* getProjection();
          assert.equal(after.runs[0]?.status, "interrupted");
          assert.deepEqual(yield* Ref.get(driver.state), {
            failure: null,
            cursor: effectiveTranscript.entries.length,
          });
          assert.isUndefined(
            after.contextHandoffs[0]?.delivery,
            "unsent inline history should be restorable",
          );
          return;
        }
        if (stopWhileAwaitingAck) {
          const beforeStop = yield* getProjection();
          assert.equal(beforeStop.runs[0]?.status, "running");
          assert.deepEqual(beforeStop.providerTurns, []);
          const stop = orchestrator.dispatch({
            type: "run.interrupt",
            commandId: CommandId.make("stop-before-native-ack"),
            threadId,
            runId: run.id,
            holdQueue: true,
          });
          if (queuedCommitRace) {
            parkStopCommit = true;
            const stopping = yield* stop.pipe(Effect.forkChild({ startImmediately: true }));
            yield* Deferred.await(writerParked);
            yield* Deferred.succeed(allowRejection, undefined);
            yield* Deferred.await(nativeStartReturned);
            yield* Deferred.succeed(allowWriter, undefined);
            yield* Fiber.join(stopping);
          } else yield* stop;
          const afterStop = yield* getProjection();
          assert.equal(afterStop.runs[0]?.status, "interrupted");
          yield* Deferred.succeed(allowRejection, undefined);
          if (queuedStop) {
            yield* Deferred.await(queuedAckObserved);
            yield* Fiber.join(starting);
            assert.equal((yield* getProjection()).contextHandoffs[0]?.delivery?.status, "pending");
            yield* TestClock.adjust("2 minutes");
            assert.equal(interruptsSent, 0, "Stop must wait for the queued turn to start");
            assert.equal(unsubscribeRequests, 0, "Idle unload must preserve the pending Stop");
            const runtime = yield* Deferred.await(sessionOpened);
            const providerThread = afterStop.providerThreads.find(
              (candidate) => candidate.nativeThreadRef?.nativeId === nativeThreadId,
            )!;
            assert.isTrue(yield* runtime.hasPendingBackgroundWorkForThread!(providerThread));
            assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
            yield* Deferred.succeed(allowActivity, undefined);
          }
          yield* Deferred.await(rejectionObserved);
          yield* Fiber.join(starting);
          assert.equal(interruptsSent, 1);
          if (queuedStop) yield* TestClock.adjust(0);
          const afterTerminal = yield* getProjection();
          if (queuedStop) {
            const runtime = yield* Deferred.await(sessionOpened);
            assert.isFalse(
              yield* runtime.hasPendingBackgroundWorkForThread!(afterTerminal.providerThreads[0]!),
              "A native terminal releases Stop's idle retention",
            );
            assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
          }
          assert.deepEqual(afterTerminal.runs, afterStop.runs);
          if (queuedCommitRace) {
            assert.equal(afterTerminal.attempts[0]?.status, "interrupted");
            assert.equal(afterTerminal.contextHandoffs[0]?.delivery?.status, "pending");
          } else {
            assert.deepEqual(afterTerminal.attempts, afterStop.attempts);
            assert.deepEqual(afterTerminal.providerTurns, []);
            assert.equal(afterTerminal.contextHandoffs[0]?.delivery?.status, "pending");
          }
          assert.deepEqual(yield* Ref.get(driver.state), {
            failure: null,
            cursor: effectiveTranscript.entries.length,
          });
          return;
        }
        if (late) {
          yield* TestClock.adjust(CODEX_START_ACK_TIMEOUT_MS);
          yield* Fiber.join(starting);
          const timedOut = yield* getProjection();
          assert.equal(timedOut.runs[0]?.status, "failed");
          assert.equal(timedOut.contextHandoffs[0]?.delivery?.status, deliveryStatus);
        }
        const timedOut = yield* getProjection();
        const pending = timedOut.contextHandoffs[0]!;
        const currentMarker = sql<{
          readonly sequence: number;
          readonly id: string;
          readonly payload: string;
        }>`
            SELECT sequence, event_id AS id, payload_json AS payload
            FROM orchestration_events WHERE event_type = 'context-handoff.updated'
            ORDER BY sequence DESC LIMIT 1`.pipe(Effect.map(([marker]) => marker!));
        const marker = yield* currentMarker;
        if (recovery.endsWith("newer identical marker")) {
          const [newer] = yield* sink.write({
            events: [
              {
                id: EventId.make("newer-identical-inline-handoff"),
                type: "context-handoff.updated",
                threadId,
                runId: run.id,
                occurredAt: pending.updatedAt,
                payload: pending,
              },
            ],
          });
          assert.isAbove(newer!.sequence, marker.sequence);
          const rows = yield* sql<{ readonly payload: string }>`
              SELECT payload_json AS payload FROM orchestration_events WHERE sequence = ${newer!.sequence}`;
          assert.equal(rows[0]?.payload, marker.payload);
        }
        if (recovery === "late acceptance after binding replacement") {
          const provider = timedOut.providerThreads[0]!;
          const identity = provider.runtimeIdentity!;
          yield* sink.write({
            runtimeIdentityBoundary: { expectedGeneration: identity.runtimeGeneration! },
            events: [
              {
                id: EventId.make("replacement-binding"),
                type: "provider-thread.updated",
                threadId,
                occurredAt: now,
                payload: {
                  ...provider,
                  runtimeIdentity: { ...identity, runtimeGeneration: "replacement-producer" },
                },
              },
            ],
          });
          assert.equal(
            (yield* getProjection()).providerThreads[0]?.runtimeIdentity?.runtimeGeneration,
            "replacement-producer",
          );
        }
        if (recovery === "late acceptance after pruning") {
          yield* sql`INSERT INTO orchestration_v2_projection_metadata
            (projection_name, schema_version, last_sequence, updated_at)
            VALUES (${EVENT_LOG_PRUNED_THROUGH_PROJECTION}, 1, ${marker.sequence}, ${DateTime.formatIso(now)})`;
        }
        if (recovery === "late acceptance with statement rollback") {
          yield* sql`CREATE TEMP TRIGGER refuse_inline_delivery BEFORE INSERT ON orchestration_events
            WHEN NEW.event_type = 'provider-turn.updated'
              AND json_extract(NEW.payload_json, '$.nativeTurnRef.nativeId') = 'original-native-turn'
            BEGIN SELECT RAISE(ABORT, 'synthetic late delivery rollback'); END`;
        }
        const acceptedDelivery =
          (commitsInline || recordsLateReceipt) && !lostAfterCommit
            ? yield* orchestrator.streamDomainEvents.pipe(
                Stream.filter((event) =>
                  commitsInline
                    ? event.type === "context-handoff.updated" &&
                      event.payload.id === handoffId &&
                      event.payload.delivery?.status === "inline"
                    : event.type === "provider-turn.updated" &&
                      event.payload.runAttemptId === originalAttemptId &&
                      event.payload.nativeTurnRef?.nativeId === originalNativeTurnId &&
                      event.payload.status === nativeTerminalStatus,
                ),
                Stream.runHead,
                Effect.forkChild({ startImmediately: true }),
              )
            : undefined;
        const initialCompletion =
          acceptsPrompt && !late
            ? yield* orchestrator.streamDomainEvents.pipe(
                Stream.filter(
                  (event) =>
                    event.type === "run.updated" &&
                    event.runId === run.id &&
                    event.payload.status === "completed",
                ),
                Stream.runHead,
                Effect.forkChild({ startImmediately: true }),
              )
            : undefined;
        const projectedQueuedTurn =
          queuedWithoutInput || queuedWithInput
            ? yield* orchestrator.streamDomainEvents.pipe(
                Stream.filter(
                  (event) =>
                    event.type === "provider-turn.updated" &&
                    event.runId === run.id &&
                    event.payload.nativeTurnRef?.nativeId === originalNativeTurnId,
                ),
                Stream.runHead,
                Effect.forkChild({ startImmediately: true }),
              )
            : undefined;
        const expectedMarker = yield* currentMarker;
        const heldWriter =
          recovery === "contradiction while writer is held"
            ? yield* sink
                .withProviderBatch(
                  Deferred.succeed(writerParked, undefined).pipe(
                    Effect.andThen(Deferred.await(allowWriter)),
                  ),
                )
                .pipe(Effect.forkChild({ startImmediately: true }))
            : undefined;
        if (heldWriter !== undefined) yield* Deferred.await(writerParked);
        commitsUntilPark =
          lostBeforeCommit || lostAfterCommit || restartReadback ? 2 : atCommit ? 1 : 0;
        yield* Deferred.succeed(allowRejection, undefined);
        if (queuedWithoutInput || queuedWithInput) {
          yield* Fiber.join(starting);
          yield* Fiber.join(projectedQueuedTurn!);
          const afterAck = yield* getProjection();
          assert.equal(afterAck.providerTurns.length, 1);
          assert.equal(afterAck.providerTurns[0]?.nativeTurnRef?.nativeId, originalNativeTurnId);
          assert.equal(afterAck.contextHandoffs[0]?.delivery?.status, "pending");
          if (queuedWithoutInput) {
            assert.deepEqual(yield* Ref.get(driver.state), {
              failure: null,
              cursor: effectiveTranscript.entries.length - 1,
            });
            return;
          }
          yield* Deferred.succeed(allowActivity, undefined);
        }
        yield* Deferred.await(rejectionObserved);
        if (lostBeforeCommit) {
          yield* Deferred.await(writerParked);
          // The producer scope closes while its acceptance transaction is open.
          // Reopen below to verify both writes rolled back.
          return {
            handoffs: timedOut.contextHandoffs,
            runs: timedOut.runs,
            attempts: timedOut.attempts,
            providerTurns: timedOut.providerTurns,
          };
        }
        if (lostAfterCommit) {
          yield* Deferred.await(writerParked);
          const after = yield* getProjection();
          assert.deepEqual(after.contextHandoffs[0], pending, "the pending marker must survive");
          assert.deepEqual(yield* currentMarker, expectedMarker);
          assert.deepEqual(after.runs, timedOut.runs);
          assert.deepEqual(after.attempts, timedOut.attempts);
          assert.equal(after.providerTurns.length, 1);
          assert.equal(after.providerTurns[0]?.nativeTurnRef?.nativeId, originalNativeTurnId);
          assert.equal(after.providerTurns[0]?.runAttemptId, originalAttemptId);
          assert.equal(after.providerTurns[0]?.status, "completed");
          return {
            handoffs: after.contextHandoffs,
            runs: after.runs,
            attempts: after.attempts,
            providerTurns: after.providerTurns,
          };
        }
        if (positiveCommitRace) yield* Deferred.await(writerParked);
        yield* TestClock.adjust(0);
        if (contradicts) {
          yield* Deferred.succeed(allowActivity, undefined);
          yield* Deferred.await(activityObserved);
          yield* Deferred.succeed(allowWriter, undefined);
        }
        if (heldWriter !== undefined) yield* Fiber.join(heldWriter);
        if (commitIntercepted) yield* Deferred.await(possibleRestoreFinished);
        yield* TestClock.adjust(0);
        yield* Fiber.join(starting);
        commitsUntilPark = 0;
        if (commitsInline || recordsLateReceipt) {
          yield* Fiber.join(acceptedDelivery!);
          if (initialCompletion !== undefined) {
            yield* Fiber.join(initialCompletion);
            yield* worker.drain();
          }
          const after = yield* getProjection();
          if (queuedCompletedBeforeAck) {
            const runtime = yield* Deferred.await(sessionOpened);
            assert.isFalse(
              yield* runtime.hasPendingBackgroundWorkForThread!(after.providerThreads[0]!),
            );
          }
          if (late) {
            assert.deepEqual(after.runs, timedOut.runs);
            assert.deepEqual(after.attempts, timedOut.attempts);
          } else assert.equal(after.runs[0]?.status, "completed");
          assert.deepEqual(yield* Ref.get(driver.state), {
            failure: null,
            cursor: effectiveTranscript.entries.length,
          });
          assert.equal(
            after.contextHandoffs[0]?.delivery?.status,
            commitsInline ? "inline" : "pending",
            "a late receipt must preserve the durable hold",
          );
          if (recordsLateReceipt) assert.deepEqual(yield* currentMarker, expectedMarker);
          if (restartReadback) {
            const runtime = yield* Deferred.await(sessionOpened);
            const liveHold = yield* runtime
              .ensureThread({
                threadId: ThreadId.make(`${scenario}-gate-check`),
                modelSelection: CODEX_TEST_MODEL_SELECTION,
                runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
              })
              .pipe(Effect.flip);
            assert.equal(liveHold._tag, "ProviderRuntimeBindingError");
            if (liveHold._tag === "ProviderRuntimeBindingError")
              assert.include(liveHold.detail, "unconfirmed native effect");
          }
          const [accepted] = after.providerTurns;
          assert.equal(after.providerTurns.length, 1);
          assert.equal(accepted?.nativeTurnRef?.nativeId, originalNativeTurnId);
          assert.equal(accepted?.runAttemptId, originalAttemptId);
          assert.equal(accepted?.status, nativeTerminalStatus);
          return {
            handoffs: after.contextHandoffs,
            runs: after.runs,
            attempts: after.attempts,
            providerTurns: after.providerTurns,
          };
        }
        if (!canContinue) {
          const after = yield* getProjection();
          assert.deepEqual(after.contextHandoffs[0], pending, "the pending marker must survive");
          assert.deepEqual(yield* currentMarker, expectedMarker);
          assert.equal(after.runs[0]?.status, "failed");
          if (late) {
            assert.deepEqual(after.runs, timedOut.runs, "the failed run must remain failed");
            assert.deepEqual(after.attempts, timedOut.attempts);
          }
          if (acceptsPrompt) assert.deepEqual(after.providerTurns, timedOut.providerTurns);
          const runtime = yield* Deferred.await(sessionOpened);
          const refusal = yield* runtime
            .ensureThread({
              threadId: ThreadId.make(`${scenario}-sibling`),
              modelSelection: CODEX_TEST_MODEL_SELECTION,
              runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
            })
            .pipe(Effect.flip);
          assert.equal(refusal._tag, "ProviderRuntimeBindingError");
          if (refusal._tag === "ProviderRuntimeBindingError")
            assert.include(refusal.detail, "unconfirmed native effect");
          assert.deepEqual(yield* Ref.get(driver.state), {
            failure: null,
            cursor: effectiveTranscript.entries.length,
          });
          return {
            handoffs: after.contextHandoffs,
            runs: after.runs,
            attempts: after.attempts,
            providerTurns: after.providerTurns,
          };
        }
        assert.equal((yield* getProjection()).contextHandoffs[0]?.delivery?.status, "injected");

        const completed = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.threadId === threadId &&
              event.payload.id !== run.id &&
              event.payload.status === "completed",
          ),
          Stream.runHead,
          Effect.forkChild({ startImmediately: true }),
        );
        yield* send(orchestrator, threadId, "new-inline-prompt", "New authorized request");
        yield* worker.drain();
        const replay = yield* Ref.get(driver.state);
        assert.isNull(replay.failure, String(replay.failure));
        assert.notEqual((yield* getProjection()).runs.at(-1)?.status, "failed");
        yield* Fiber.join(completed);
        yield* worker.drain();
        const after = yield* getProjection();
        assert.deepEqual(
          after.runs.map((candidate) => candidate.status),
          ["failed", "completed"],
        );
        if (late) {
          assert.deepEqual(
            after.attempts.filter((candidate) => candidate.runId === run.id),
            timedOut.attempts,
          );
        }
        assert.equal(after.providerThreads.length, 1);
        assert.equal(after.providerThreads[0]?.nativeThreadRef?.nativeId, nativeThreadId);
        assert.equal(after.contextHandoffs[0]?.delivery?.status, "injected");
        assert.deepEqual(yield* Ref.get(driver.state), {
          failure: null,
          cursor: effectiveTranscript.entries.length,
        });
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistryAndServices(
            { name: scenario, runtimePolicyOverride: { cwd } },
            CodexAdapterV2Testkit.layer({
              transcript,
              driver,
              ...(queuedCommitRace
                ? {
                    onTurnStartReturned: () =>
                      Deferred.doneUnsafe(nativeStartReturned, Effect.void),
                  }
                : {}),
              onSessionOpened: (runtime) =>
                Deferred.doneUnsafe(sessionOpened, Effect.succeed(runtime)),
              observeOutgoingFrame: (frame) => {
                if (Predicate.hasProperty(frame, "method")) {
                  if (frame.method === "turn/interrupt") interruptsSent++;
                  if (frame.method === "thread/unsubscribe") unsubscribeRequests++;
                }
              },
              observeFrame: (frame) => {
                if (queuedStop && Predicate.hasProperty(frame, "id") && frame.id === 4)
                  Deferred.doneUnsafe(queuedAckObserved, Effect.void);
                if (
                  Predicate.hasProperty(frame, "error") &&
                  Predicate.isObject(frame.error) &&
                  frame.error.message === "Rejected original prompt"
                )
                  Deferred.doneUnsafe(rejectionObserved, Effect.void);
                if (
                  acceptsPrompt &&
                  !ackOnly &&
                  !terminalFirst &&
                  Predicate.hasProperty(frame, "method") &&
                  frame.method === "turn/completed"
                )
                  Deferred.doneUnsafe(rejectionObserved, Effect.void);
                if (
                  (ackOnly || terminalFirst) &&
                  Predicate.hasProperty(frame, "result") &&
                  Predicate.isObject(frame.result) &&
                  Predicate.isObject(frame.result.turn) &&
                  frame.result.turn.id === originalNativeTurnId
                )
                  Deferred.doneUnsafe(rejectionObserved, Effect.void);
                if (Predicate.hasProperty(frame, "id") && frame.id === "after-rejection")
                  Deferred.doneUnsafe(activityObserved, Effect.void);
              },
            }),
            { runEffectWorker: false, databaseLayer },
          ),
        ),
        Effect.scoped,
      );
      if (!reopenDatabase) return;
      if (retained === undefined)
        return yield* Effect.die("Expected retained history delivery evidence");

      // Discard the old adapter, session manager and SQLite connection. An accepted
      // original prompt must not reappear in retry history on the fresh producer.
      const restarted = codexReplayPreamble({
        nativeThreadId,
        nativeTurnId: "restart-turn",
        cwd,
        prompt: "Authorized after a restart",
      });
      const restartTranscript = makeCodexReplayTranscript({
        scenario: `${scenario}-restart`,
        entries: commitsInline
          ? [
              ...restarted.slice(0, 3),
              ...resumeNative(2),
              ...restarted.slice(5),
              producerCompletion(nativeThreadId, "restart-turn"),
            ]
          : [],
      });
      const effectiveRestart = withCodexSetup(restartTranscript, threadTitle);
      const restartDriver = yield* CodexReplay.makeReplayDriver(effectiveRestart);
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const read = () => orchestrator.getThreadProjection(threadId);
        const reopened = yield* read();
        assert.deepEqual(reopened.contextHandoffs, retained.handoffs);
        assert.deepEqual(reopened.runs, retained.runs);
        assert.deepEqual(reopened.attempts, retained.attempts);
        assert.deepEqual(reopened.providerTurns, retained.providerTurns);
        const completed = commitsInline
          ? yield* orchestrator.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "run.updated" &&
                  event.threadId === threadId &&
                  event.runId !== originalRunId &&
                  event.payload.status === "completed",
              ),
              Stream.runHead,
              Effect.forkChild({ startImmediately: true }),
            )
          : undefined;
        yield* send(orchestrator, threadId, "after-restart", "Authorized after a restart");
        yield* worker.drain();
        if (completed !== undefined) {
          assert.isNull((yield* Ref.get(restartDriver.state)).failure);
          yield* Fiber.join(completed);
          yield* worker.drain();
        }
        const after = yield* read();
        assert.deepEqual(after.contextHandoffs, retained.handoffs);
        assert.deepEqual(
          after.runs.map((run) => run.status),
          [commitsInline ? "completed" : "failed", commitsInline ? "completed" : "failed"],
        );
        assert.deepEqual(after.runs[0], retained.runs[0]);
        assert.deepEqual(
          after.attempts.filter((attempt) => attempt.runId === originalRunId),
          retained.attempts,
        );
        if (!commitsInline)
          assert.include(
            after.turnItems
              .filter((item) => item.type === "error")
              .find((item) => item.runId === after.runs[1]?.id)?.failure.message ?? "",
            "Conversation history delivery is uncertain",
          );
        assert.deepEqual(yield* Ref.get(restartDriver.state), {
          failure: null,
          cursor: effectiveRestart.entries.length,
        });
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistryAndServices(
            { name: restartTranscript.scenario, runtimePolicyOverride: { cwd } },
            CodexAdapterV2Testkit.layer({ transcript: restartTranscript, driver: restartDriver }),
            { runEffectWorker: false, databaseLayer: database },
          ),
        ),
        Effect.scoped,
      );
    }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );
});
