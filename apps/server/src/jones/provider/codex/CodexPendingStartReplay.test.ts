import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CodexSettings,
  MessageId,
  NodeId,
  ProjectId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexReplay from "effect-codex-app-server/replay";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as CodexAdapterV2 from "../../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import {
  makeCodexReplayClientFactory,
  makeReplayServerConfig,
} from "../../../orchestration-v2/Adapters/CodexAdapterV2.testkit.ts";
import * as ControlAdmission from "../../orchestration/ControlAdmission.ts";
import { materializeCodexReplayAppIdentity } from "../../runtimeIdentity/CodexReplayIdentity.ts";

const rootThread = "pre-ack-thread";
const rootTurn = "pre-ack-turn";
const successorTurn = "goal-successor-turn";
const reconciledTurn = "reconciled-turn";
const scenario = "pre-ack-item-reader";
const modelSelection = { instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID, model: "gpt-5.4" };
const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: "/workspace",
});
const turn = (id: string, status: "inProgress" | "completed") => ({
  id,
  items: [],
  itemsView: "notLoaded",
  status,
  error: null,
  startedAt: 1782622440,
  completedAt: status === "inProgress" ? null : 1782622450,
  durationMs: null,
});
const outbound = (frame: unknown): CodexReplay.CodexAppServerReplayEntry => ({
  type: "expect_outbound",
  frame,
});
const inbound = (frame: unknown, label?: string): CodexReplay.CodexAppServerReplayEntry => ({
  type: "emit_inbound",
  frame,
  ...(label === undefined ? {} : { label }),
});

const commandEntries = (turnId: string, itemId: string) => {
  const item = {
    type: "commandExecution",
    id: itemId,
    command: "pwd",
    cwd: "/workspace",
    processId: "42",
    source: "unifiedExecStartup",
    commandActions: [{ type: "unknown", command: "pwd" }],
  };
  return [
    inbound({
      method: "item/started",
      params: {
        threadId: rootThread,
        turnId,
        startedAtMs: 1782622441000,
        item: {
          ...item,
          status: "inProgress",
          aggregatedOutput: null,
          exitCode: null,
          durationMs: null,
        },
      },
    }),
    inbound({
      method: "item/commandExecution/outputDelta",
      params: { threadId: rootThread, turnId, itemId, delta: "/workspace\n" },
    }),
    inbound({
      method: "item/completed",
      params: {
        threadId: rootThread,
        turnId,
        completedAtMs: 1782622441010,
        item: {
          ...item,
          status: "completed",
          aggregatedOutput: "/workspace\n",
          exitCode: 0,
          durationMs: 10,
        },
      },
    }),
  ];
};

const rootCompletion = inbound({
  method: "turn/completed",
  params: { threadId: rootThread, turn: turn(rootTurn, "completed") },
});
const usageBreakdown = (inputTokens: number, cachedInputTokens: number, outputTokens: number) => ({
  inputTokens,
  cachedInputTokens,
  outputTokens,
  reasoningOutputTokens: 0,
  totalTokens: inputTokens + outputTokens,
});
const usageUpdate = (turnId: string, total: ReturnType<typeof usageBreakdown>, last = total) =>
  inbound({
    method: "thread/tokenUsage/updated",
    params: { threadId: rootThread, turnId, tokenUsage: { total, last, modelContextWindow: 10000 } },
  });
type Outcome =
  | "acknowledged"
  | "cancelled"
  | "terminal-after-ack"
  | "overflow-during-replay"
  | "cancelled-during-replay"
  | "goal-successor-during-replay";
const gatedReplay = (outcome: Outcome) => outcome.endsWith("during-replay");
const reconcilesAdmission = (outcome: Outcome) =>
  outcome === "overflow-during-replay" || outcome === "cancelled-during-replay";
const goalUpdate = (status: "active" | "complete") =>
  inbound({
    method: "thread/goal/updated",
    params: {
      threadId: rootThread,
      turnId: null,
      goal: {
        threadId: rootThread,
        objective: "Finish both turns",
        status,
        tokenBudget: null,
        tokensUsed: 10,
        timeUsedSeconds: 3,
        createdAt: 1782622440,
        updatedAt: 1782622450,
      },
    },
  });
const turnStart = (id: number) =>
  outbound({
    id,
    method: "turn/start",
    params: {
      threadId: rootThread,
      input: [{ type: "text", text: "Work" }],
      cwd: "/workspace",
      model: modelSelection.model,
      approvalPolicy: "never",
      approvalsReviewer: "user",
      sandboxPolicy: { type: "dangerFullAccess" },
      summary: "detailed",
    },
  });
const makeTranscript = (outcome: Outcome) =>
  Schema.decodeUnknownSync(CodexReplay.CodexAppServerReplayTranscript)(
    materializeCodexReplayAppIdentity({
      provider: "codex",
      protocol: "codex.app-server",
      version: "0.144.0",
      scenario,
      entries: [
        outbound({
          id: 1,
          method: "initialize",
          params: {
            clientInfo: { name: "T3 Code", title: "T3 Code", version: "replay" },
            capabilities: {
              experimentalApi: true,
              optOutNotificationMethods: ["turn/diff/updated"],
              extensions: {
                "io.modelcontextprotocol/ui": { mimeTypes: ["text/html;profile=mcp-app"] },
              },
            },
          },
        }),
        inbound({
          id: 1,
          result: {
            userAgent: "Codex/replay",
            codexHome: "/synthetic/codex",
            platformFamily: "unix",
            platformOs: "macos",
          },
        }),
        outbound({ method: "initialized" }),
        outbound({
          id: 2,
          method: "thread/start",
          params: { config: CodexAdapterV2.CODEX_THREAD_CONFIG },
        }),
        inbound({
          id: 2,
          result: {
            thread: {
              id: rootThread,
              sessionId: rootThread,
              forkedFromId: null,
              preview: "",
              ephemeral: false,
              modelProvider: "openai",
              createdAt: 1782622440,
              updatedAt: 1782622440,
              status: { type: "idle" },
              path: "/synthetic/root.jsonl",
              cwd: "/workspace",
              cliVersion: "0.144.0",
              source: "vscode",
              threadSource: null,
              agentNickname: null,
              agentRole: null,
              gitInfo: null,
              name: null,
              turns: [],
            },
            model: modelSelection.model,
            modelProvider: "openai",
            serviceTier: null,
            cwd: "/workspace",
            instructionSources: [],
            approvalPolicy: "on-request",
            approvalsReviewer: "user",
            sandbox: { type: "workspaceWrite", writableRoots: [], networkAccess: false },
            reasoningEffort: "medium",
          },
        }),
        turnStart(3),
        inbound({
          method: "turn/started",
          params: { threadId: rootThread, turn: turn(rootTurn, "inProgress") },
        }),
        inbound({
          method: "turn/started",
          params: { threadId: rootThread, turn: turn("unrelated-turn", "inProgress") },
        }),
        ...commandEntries("unrelated-turn", "unrelated-command"),
        ...(outcome === "terminal-after-ack"
          ? [usageUpdate("unrelated-turn", usageBreakdown(1000, 400, 200))]
          : []),
        ...(outcome === "goal-successor-during-replay" ? [goalUpdate("active")] : []),
        ...(gatedReplay(outcome)
          ? [
              inbound({
                method: "turn/plan/updated",
                params: {
                  threadId: rootThread,
                  turnId: rootTurn,
                  explanation: null,
                  plan: [{ step: "Work", status: "inProgress" }],
                },
              }),
            ]
          : []),
        ...commandEntries(rootTurn, "matching-command"),
        // Three unrelated items, one plan and three matching items plus these
        // 505 deltas consume the cumulative 512-event budget before the ACK.
        ...(outcome === "overflow-during-replay"
          ? Array.from({ length: 505 }, () =>
              inbound({
                method: "item/commandExecution/outputDelta",
                params: {
                  threadId: rootThread,
                  turnId: rootTurn,
                  itemId: "matching-command",
                  delta: "",
                },
              }),
            )
          : []),
        ...(outcome === "acknowledged" || outcome === "cancelled" ? [rootCompletion] : []),
        inbound({ id: 3, result: { turn: turn(rootTurn, "inProgress") } }, "original-ack"),
        ...(outcome === "terminal-after-ack"
          ? [
              usageUpdate(rootTurn, usageBreakdown(1030, 410, 206), usageBreakdown(20, 5, 4)),
              rootCompletion,
            ]
          : []),
        ...(gatedReplay(outcome) ? [{ ...rootCompletion, label: "during-replay" }] : []),
        ...(outcome === "goal-successor-during-replay"
          ? [
              inbound({
                method: "turn/started",
                params: { threadId: rootThread, turn: turn(successorTurn, "inProgress") },
              }),
              { ...goalUpdate("complete"), label: "finish-successor" },
              inbound({
                method: "turn/completed",
                params: { threadId: rootThread, turn: turn(successorTurn, "completed") },
              }),
            ]
          : []),
        ...(reconcilesAdmission(outcome)
          ? [
              turnStart(4),
              inbound({ id: 4, result: { turn: turn(reconciledTurn, "inProgress") } }),
              inbound({
                method: "turn/completed",
                params: { threadId: rootThread, turn: turn(reconciledTurn, "completed") },
              }),
            ]
          : []),
        outbound({ id: reconcilesAdmission(outcome) ? 5 : 4, method: "model/list", params: {} }),
        inbound({
          id: reconcilesAdmission(outcome) ? 5 : 4,
          result: { data: [], nextCursor: null },
        }),
      ],
    }),
  );

describe("Codex pre-ACK item reader progress", () => {
  it.effect.each([
    "acknowledged",
    "cancelled",
    "terminal-after-ack",
    "overflow-during-replay",
    "cancelled-during-replay",
    "goal-successor-during-replay",
  ] as const)(
    "retains item output only for the original %s start",
    (outcome) =>
      Effect.gen(function* () {
        const transcript = makeTranscript(outcome);
        const preAckHandled = yield* Deferred.make<void>();
        const releaseAck = yield* Deferred.make<void>();
        const rootTerminal = yield* Deferred.make<void>();
        const replayEntered = yield* Deferred.make<void>();
        const releaseReplay = yield* Deferred.make<void>();
        const duringReplayHandled = yield* Deferred.make<void>();
        const successorStarted = yield* Deferred.make<void>();
        const releaseSuccessor = yield* Deferred.make<void>();
        const reconciledTerminal = yield* Deferred.make<void>();
        const clientReady = yield* Deferred.make<CodexClient.CodexAppServerClient["Service"]>();
        const driver = yield* CodexReplay.makeReplayDriver(transcript, {
          beforeEmitInbound: (entry) =>
            entry.label === "original-ack"
              ? Deferred.await(releaseAck)
              : entry.label === "during-replay"
                ? Deferred.await(replayEntered)
                : entry.label === "finish-successor"
                  ? Deferred.await(releaseSuccessor)
                  : Effect.void,
        });
        const replayFactory = makeCodexReplayClientFactory({ transcript, driver });
        // Only filesystem dependencies are inert. The adapter, RPC correlation,
        // notification dispatch and serialized protocol reader are all real.
        const fileSystem = FileSystem.makeNoop({
          makeTempDirectory: () => Effect.succeed("/synthetic/pre-ack-reader"),
          makeDirectory: () => Effect.void,
          exists: () => Effect.succeed(false),
        });
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const adapter = yield* CodexAdapterV2.makeCodexAdapterV2({
          instanceId: modelSelection.instanceId,
          settings: yield* Schema.decodeEffect(CodexSettings)({}),
          environment: {},
          crypto: yield* Crypto.Crypto,
          fileSystem,
          idAllocator: {
            ...idAllocator,
            allocate: {
              ...idAllocator.allocate,
              // Allocation is reached only inside the replayed plan handler,
              // after the original ACK has registered its native turn.
              plan: (input) =>
                idAllocator.allocate.plan(input).pipe(
                  Effect.tap(() => Deferred.succeed(replayEntered, undefined)),
                  Effect.tap(() => Deferred.await(releaseReplay)),
                ),
            },
          },
          serverConfig: yield* makeReplayServerConfig(scenario).pipe(
            Effect.provideService(FileSystem.FileSystem, fileSystem),
          ),
          clientFactory: {
            open: (input) =>
              replayFactory.open(input).pipe(
                Effect.tap((client) => Deferred.succeed(clientReady, client)),
                Effect.map((client): CodexClient.CodexAppServerClient["Service"] => ({
                  ...client,
                  handleServerNotification: (method, handler) =>
                    client.handleServerNotification(method, (payload) =>
                      handler(payload).pipe(
                        Effect.tap(() =>
                          method === "item/completed" &&
                          typeof payload === "object" &&
                          payload !== null &&
                          "turnId" in payload &&
                          payload.turnId === rootTurn
                            ? Deferred.succeed(preAckHandled, undefined)
                            : Effect.void,
                        ),
                        Effect.tap(() => {
                          if (typeof payload !== "object" || payload === null) return Effect.void;
                          if (
                            "turn" in payload &&
                            typeof payload.turn === "object" &&
                            payload.turn !== null &&
                            "id" in payload.turn &&
                            ((outcome === "goal-successor-during-replay" &&
                              method === "turn/started" &&
                              payload.turn.id === successorTurn) ||
                              (reconcilesAdmission(outcome) &&
                                method === "turn/completed" &&
                                payload.turn.id === rootTurn))
                          )
                            return Deferred.succeed(duringReplayHandled, undefined);
                          return Effect.void;
                        }),
                      ),
                    ),
                })),
              ),
          },
        });
        const threadId = ThreadId.make("pre-ack-reader-app-thread");
        const runtime = yield* adapter.openSession({
          threadId,
          providerSessionId: ProviderSessionId.make("pre-ack-reader-session"),
          modelSelection,
          runtimePolicy,
        });
        const providerThread = yield* runtime.ensureThread({
          threadId,
          modelSelection,
          runtimePolicy,
        });
        const events: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
        const reconciledTurnId = idAllocator.derive.providerTurn({
          driver: CodexAdapterV2.CODEX_DRIVER_KIND,
          nativeTurnId: reconciledTurn,
        });
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            Effect.sync(() => events.push(event)).pipe(
              Effect.andThen(
                event.type === "turn.terminal"
                  ? Deferred.succeed(rootTerminal, undefined)
                  : Effect.void,
              ),
              Effect.andThen(
                event.type === "provider_turn.updated" &&
                  event.providerTurn.nativeTurnRef?.nativeId === successorTurn
                  ? Deferred.succeed(successorStarted, undefined)
                  : Effect.void,
              ),
              Effect.andThen(
                event.type === "turn.terminal" && event.providerTurnId === reconciledTurnId
                  ? Deferred.succeed(reconciledTerminal, undefined)
                  : Effect.void,
              ),
            ),
          ),
          Effect.forkScoped,
        );
        const now = yield* DateTime.now;
        const turnInput: ProviderAdapter.ProviderAdapterV2TurnInput = {
          appThread: {
            createdBy: "user",
            creationSource: "web",
            id: threadId,
            projectId: ProjectId.make("pre-ack-reader-project"),
            title: "Reader progress",
            providerInstanceId: modelSelection.instanceId,
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            activeProviderThreadId: providerThread.id,
            lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
            deletedAt: null,
          },
          threadId,
          runId: RunId.make("pre-ack-reader-run"),
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: RunAttemptId.make("pre-ack-reader-attempt"),
          rootNodeId: NodeId.make("pre-ack-reader-node"),
          providerThread,
          message: {
            createdBy: "user",
            creationSource: "web",
            messageId: MessageId.make("pre-ack-reader-message"),
            text: "Work",
            attachments: [],
          },
          modelSelection,
          runtimePolicy,
        };
        const start = yield* runtime.startTurn(turnInput).pipe(Effect.result, Effect.forkScoped);
        // Wait for real notification handlers to finish, not just for the replay
        // transport to enqueue their frames. The original RPC is still withheld.
        yield* Deferred.await(preAckHandled);
        assert.isUndefined(start.pollUnsafe());
        assert.isFalse(events.some((event) => event.type === "turn_item.updated"));
        if (outcome === "cancelled") yield* Fiber.interrupt(start);
        yield* Deferred.succeed(releaseAck, undefined);
        if (gatedReplay(outcome)) {
          yield* Deferred.await(replayEntered);
          yield* Deferred.await(duringReplayHandled);
          assert.isUndefined(start.pollUnsafe());
          assert.isFalse(events.some((event) => event.type === "plan.updated"));
          if (outcome === "goal-successor-during-replay")
            assert.isFalse(
              events.some(
                (event) =>
                  event.type === "provider_turn.updated" &&
                  event.providerTurn.nativeTurnRef?.nativeId === successorTurn,
              ),
            );
          if (outcome === "cancelled-during-replay") yield* Fiber.interrupt(start);
          yield* Deferred.succeed(releaseReplay, undefined);
        }
        if (reconcilesAdmission(outcome)) {
          if (outcome === "overflow-during-replay")
            assert.equal((yield* Fiber.join(start))._tag, "Failure");
          else assert.equal((yield* Fiber.await(start))._tag, "Failure");
          yield* Deferred.await(rootTerminal);
          // A fresh, explicitly submitted attempt must pass admission after the
          // original ACK and terminal; the failed attempt itself is never resent.
          yield* runtime.startTurn({
            ...turnInput,
            runId: RunId.make("reconciled-run"),
            runOrdinal: 2,
            providerTurnOrdinal: 2,
            attemptId: RunAttemptId.make("reconciled-attempt"),
            rootNodeId: NodeId.make("reconciled-node"),
          });
          yield* Deferred.await(reconciledTerminal);
        } else if (outcome !== "cancelled") {
          assert.equal((yield* Fiber.join(start))._tag, "Success");
          if (outcome === "goal-successor-during-replay") {
            yield* Deferred.await(successorStarted);
            const successor = events.flatMap((event) =>
              event.type === "provider_turn.updated" &&
              event.providerTurn.nativeTurnRef?.nativeId === successorTurn
                ? [event.providerTurn]
                : [],
            );
            assert.lengthOf(successor, 1);
            assert.equal(successor[0]!.ordinal, 2);
            assert.equal(successor[0]!.runAttemptId, turnInput.attemptId);
            // Expire the predecessor's hold timer while the successor is still
            // active. Consuming the hold must prevent premature run settlement.
            yield* TestClock.adjust("5 seconds");
            assert.isFalse(events.some((event) => event.type === "turn.terminal"));
            yield* Deferred.succeed(releaseSuccessor, undefined);
          }
          yield* Deferred.await(rootTerminal);
        } else {
          assert.equal((yield* Fiber.await(start))._tag, "Failure");
        }
        const client = yield* Deferred.await(clientReady);
        assert.deepEqual(yield* client.request("model/list", {}), { data: [], nextCursor: null });
        assert.deepEqual(yield* Ref.get(driver.state), {
          cursor: transcript.entries.length,
          failure: null,
        });
        // ACK/replay progress never depends on advancing a timeout. Only the
        // successor case advances the clock, after replay, to check hold consumption.
        assert.equal(
          DateTime.toEpochMillis(yield* DateTime.now) - DateTime.toEpochMillis(now),
          outcome === "goal-successor-during-replay" ? 5_000 : 0,
        );
        assert.isFalse(
          events.some(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.nativeItemRef?.nativeId === "unrelated-command",
          ),
        );
        assert.isFalse(
          events.some(
            (event) =>
              event.type === "provider_turn.updated" &&
              event.providerTurn.nativeTurnRef?.nativeId === "unrelated-turn",
          ),
        );
        if (outcome === "cancelled") {
          assert.isFalse(
            events.some(
              (event) =>
                event.type === "turn_item.updated" ||
                event.type === "provider_turn.updated" ||
                event.type === "command_output.delta" ||
                event.type === "turn.terminal",
            ),
          );
          return;
        }
        if (reconcilesAdmission(outcome)) {
          const terminals = events.flatMap((event) =>
            event.type === "turn.terminal" ? [event] : [],
          );
          assert.equal(terminals.length, 2);
          assert.equal(terminals[0]!.evidenceKind, "local_failure");
          assert.equal(terminals[1]!.providerTurnId, reconciledTurnId);
          assert.equal(terminals[1]!.status, "completed");
          assert.isFalse(
            events.some(
              (event) =>
                event.type === "command_output.delta" ||
                (event.type === "turn_item.updated" &&
                  event.turnItem.nativeItemRef?.nativeId === "matching-command"),
            ),
          );
          return;
        }
        if (outcome === "goal-successor-during-replay") {
          const terminal = events.find((event) => event.type === "turn.terminal");
          assert.equal(
            terminal?.providerTurnId,
            idAllocator.derive.providerTurn({
              driver: CodexAdapterV2.CODEX_DRIVER_KIND,
              nativeTurnId: successorTurn,
            }),
          );
        }
        if (outcome === "terminal-after-ack") {
          const completedTurn = events.find(
            (event) =>
              event.type === "provider_turn.updated" &&
              event.providerTurn.nativeTurnRef?.nativeId === rootTurn &&
              event.providerTurn.status === "completed",
          );
          assert.deepEqual(
            completedTurn?.type === "provider_turn.updated"
              ? completedTurn.providerTurn.turnTokenUsage
              : undefined,
            {
              usageStatus: "complete",
              usageScope: "main_agent",
              hasSubagents: false,
              inputTokens: 30,
              cachedInputTokens: 10,
              outputTokens: 6,
              reasoningTokens: 0,
            },
          );
        }
        const completed = events.filter(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.status === "completed" &&
            event.turnItem.output === "/workspace\n",
        );
        assert.lengthOf(completed, 1);
        assert.equal(
          events.filter(
            (event) => event.type === "command_output.delta" && event.chunk === "/workspace\n",
          ).length,
          1,
        );
        assert.equal(events.filter((event) => event.type === "turn.terminal").length, 1);
        const terminalIndex = events.findIndex((event) => event.type === "turn.terminal");
        const deltaIndex = events.findIndex((event) => event.type === "command_output.delta");
        assert.isAbove(
          deltaIndex,
          events.findIndex((event) => event.type === "turn_item.updated"),
        );
        assert.isAbove(events.indexOf(completed[0]!), deltaIndex);
        assert.isAbove(terminalIndex, events.indexOf(completed[0]!));
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(
            IdAllocator.layer,
            McpProviderSessions.layer,
            ControlAdmission.layer,
            NodeServices.layer,
          ),
        ),
      ),
  );
});
