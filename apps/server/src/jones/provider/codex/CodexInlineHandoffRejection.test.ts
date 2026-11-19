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
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
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
  producerCompletion,
  withConfigReads,
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
const CODEX_TEST_RUNTIME_POLICY = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: "/workspace",
});

describe("Codex inline handoff rejection recovery", () => {
  it.effect.each([
    { code: -32602, late: true, recovery: "restore" },
    { code: -32600, late: false, recovery: "restore" },
    { code: -32602, late: true, recovery: "contradiction" },
    { code: -32602, late: true, recovery: "contradiction after provisional write" },
    { code: -32602, late: true, recovery: "newer identical marker" },
    { code: -32602, late: true, recovery: "pruned marker" },
    { code: -32602, late: true, recovery: "one SQL write failure" },
  ] as const)("handles $recovery after exact $code (late: $late)", ({ code, late, recovery }) =>
    Effect.gen(function* () {
      const scenario = `inline-handoff-refusal-${code}-${recovery}`;
      const fileSystem = yield* FileSystem.FileSystem;
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "jones-codex-history-" });
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
          sentPrompt: withInlineContext(renderHistory(history.messages, history.context), text),
        });
      const first = preambleFor("Original request", "rejected-turn");
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
          `Context handoff (full_thread_summary):\n${coverage}`,
          "Context handoff (delta_since_target_last_seen):",
          `Provider context handoff. Thread: ${threadId}. Covered app runs: 1-1.`,
          `Source item range: ${userItemId} through ${failureItemId}.`,
          `Recover an omitted item only when the request needs it, with t3_thread_read({threadId:"${threadId}",view:"activity",itemId:"<omitted item ID>",maxCharsPerItem:4000}); continue long text with textOffset=nextTextOffset until null. Items included below need no read. Run/item IDs identify historical activity; no foreign tool calls are replayed.`,
        ].join("\n"),
        budget: 16_000,
      });
      const inject = (id: number, accepted = false): CodexReplay.CodexAppServerReplayEntry[] => [
        {
          type: "expect_outbound",
          frame: {
            id,
            method: "thread/inject_items",
            params: {
              threadId: nativeThreadId,
              items: accepted
                ? historyResponseItems(recoveredHistory.messages, recoveredHistory.context)
                : historyResponseItems(history.messages, history.context),
            },
          },
        },
        {
          type: "emit_inbound",
          frame: accepted
            ? { id, result: {} }
            : { id, error: { code: -32601, message: "Native injection unavailable" } },
        },
      ];
      const shouldRestore = recovery === "restore" || recovery === "one SQL write failure";
      const contradicts = recovery.startsWith("contradiction");
      const transcript = makeCodexReplayTranscript({
        scenario,
        entries: [
          ...first.slice(0, 5),
          ...inject(3),
          withReplayRequestId(first[5]!, 4),
          {
            type: "emit_inbound",
            label: "original rejection",
            frame: { id: 4, error: { code, message: "Rejected original prompt" } },
          },
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
          ...(shouldRestore
            ? [
                {
                  type: "expect_outbound" as const,
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
                        runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
                      }),
                    },
                  },
                },
                {
                  type: "emit_inbound" as const,
                  frame: {
                    id: 5,
                    result: {
                      thread: { id: nativeThreadId, updatedAt: 1782622440 },
                      model: CODEX_TEST_MODEL_SELECTION.model,
                      modelProvider: "openai",
                      serviceTier: null,
                    },
                  },
                },
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
      const writerParked = yield* Deferred.make<void>();
      const allowWriter = yield* Deferred.make<void>();
      const restoreQueued = yield* Deferred.make<void>();
      const restoreFinished = yield* Deferred.make<void>();
      const provisionalWrite = yield* Deferred.make<void>();
      const activityObserved = yield* Deferred.make<void>();
      const sessionOpened = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2SessionRuntime>();
      let watchRestore = false;
      let watchRestoreWrite = false;
      let restorationFailed = false;
      const databaseLayer = Layer.effect(
        SqlClient.SqlClient,
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const withTransaction: typeof sql.withTransaction = (body) =>
            Effect.suspend(() => {
              if (watchRestoreWrite) {
                watchRestoreWrite = false;
                // The restore's nested savepoint has succeeded. The outer writer
                // can still roll it back after the reader revokes the proof.
                return sql.withTransaction(body).pipe(
                  Effect.tap(() => Deferred.succeed(provisionalWrite, undefined)),
                  Effect.tap(() => Deferred.await(allowWriter)),
                );
              }
              if (!watchRestore) return sql.withTransaction(body);
              watchRestore = false;
              watchRestoreWrite = recovery === "contradiction after provisional write";
              return Deferred.succeed(restoreQueued, undefined).pipe(
                Effect.andThen(sql.withTransaction(body)),
                Effect.onExit((exit) =>
                  Effect.gen(function* () {
                    restorationFailed = Exit.isFailure(exit);
                    if (recovery === "one SQL write failure")
                      yield* sql`DROP TRIGGER fail_inline_restore`.pipe(Effect.orDie);
                    yield* Deferred.succeed(restoreFinished, undefined);
                  }),
                ),
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
      ).pipe(Layer.provide(Sqlite.layerMemory));
      const effectiveTranscript = withConfigReads(transcript);
      const driver = yield* CodexReplay.makeReplayDriver(effectiveTranscript, {
        beforeEmitInbound: (entry) =>
          entry.label === "original rejection"
            ? Deferred.succeed(sent, undefined).pipe(Effect.andThen(Deferred.await(allowRejection)))
            : entry.label === "contradictory turn activity"
              ? Deferred.await(
                  recovery === "contradiction after provisional write"
                    ? provisionalWrite
                    : restoreQueued,
                )
              : Effect.void,
      });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sql = yield* SqlClient.SqlClient;
        const sink = yield* EventSink.EventSinkV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const getProjection = () => orchestrator.getThreadProjection(threadId);
        const send = (id: string, text: string) =>
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
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`create-${scenario}`),
          threadId,
          projectId: ProjectId.make(scenario),
          title: "Late context refusal",
          modelSelection: CODEX_TEST_MODEL_SELECTION,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        yield* send("original-inline-prompt", "Original request");
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
        assert.equal((yield* getProjection()).contextHandoffs[0]?.delivery?.status, "pending");
        if (late) {
          yield* TestClock.adjust(CODEX_START_ACK_TIMEOUT_MS);
          yield* Fiber.join(starting);
          const timedOut = yield* getProjection();
          assert.equal(timedOut.runs[0]?.status, "failed");
          assert.equal(timedOut.contextHandoffs[0]?.delivery?.status, "pending");
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
        if (recovery === "newer identical marker") {
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
        if (recovery === "pruned marker") {
          yield* sql`
              INSERT INTO orchestration_v2_projection_metadata
                (projection_name, schema_version, last_sequence, updated_at)
              VALUES (${EVENT_LOG_PRUNED_THROUGH_PROJECTION}, 1, ${marker.sequence}, ${DateTime.formatIso(now)})`;
        }
        if (recovery === "one SQL write failure") {
          // A real SQLite append failure. The transaction observer removes the fault
          // only after the first restore has rolled back.
          yield* sql`CREATE TEMP TRIGGER fail_inline_restore BEFORE INSERT ON orchestration_events
              WHEN NEW.event_type = 'context-handoff.updated'
                AND json_extract(NEW.payload_json, '$.delivery') IS NULL
              BEGIN SELECT RAISE(ABORT, 'synthetic restore failure'); END`;
        }
        const expectedMarker = yield* currentMarker;
        const heldWriter =
          recovery === "contradiction"
            ? yield* sink
                .withProviderBatch(
                  Deferred.succeed(writerParked, undefined).pipe(
                    Effect.andThen(Deferred.await(allowWriter)),
                  ),
                )
                .pipe(Effect.forkChild({ startImmediately: true }))
            : undefined;
        if (heldWriter !== undefined) yield* Deferred.await(writerParked);
        watchRestore = late;
        yield* Deferred.succeed(allowRejection, undefined);
        if (contradicts) {
          // Confirm reader activity before releasing the parked writer.
          yield* Deferred.await(activityObserved);
          yield* Deferred.succeed(allowWriter, undefined);
        }
        if (heldWriter !== undefined) yield* Fiber.join(heldWriter);
        if (late) yield* Deferred.await(restoreFinished);
        yield* TestClock.adjust(0);
        yield* Fiber.join(starting);
        if (recovery === "one SQL write failure") assert.isTrue(restorationFailed);
        if (!shouldRestore) {
          const after = yield* getProjection();
          assert.deepEqual(after.contextHandoffs[0], pending, "the pending marker must survive");
          assert.deepEqual(yield* currentMarker, expectedMarker);
          assert.deepEqual(after.runs, timedOut.runs, "the failed run must remain failed");
          assert.deepEqual(after.attempts, timedOut.attempts);
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
          return;
        }
        const restored = (yield* getProjection()).contextHandoffs[0];
        assert.equal(restored?.id, handoffId);
        assert.isUndefined(restored?.delivery);

        const waiting = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.threadId === threadId &&
              event.payload.id !== run.id &&
              event.payload.status === "waiting",
          ),
          Stream.runHead,
          Effect.forkChild({ startImmediately: true }),
        );
        yield* send("new-inline-prompt", "New authorized request");
        yield* worker.drain();
        const replay = yield* Ref.get(driver.state);
        assert.isNull(replay.failure, String(replay.failure));
        assert.notEqual((yield* getProjection()).runs.at(-1)?.status, "failed");
        yield* Fiber.join(waiting);
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
              onSessionOpened: (runtime) =>
                Deferred.doneUnsafe(sessionOpened, Effect.succeed(runtime)),
              observeFrame: (frame) => {
                if (Predicate.hasProperty(frame, "id") && frame.id === "after-rejection")
                  Deferred.doneUnsafe(activityObserved, Effect.void);
              },
            }),
            { runEffectWorker: false, databaseLayer },
          ),
        ),
      );
    }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );
});
