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

import * as CodexAdapterV2 from "../../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import {
  makeCodexReplayClientFactory,
  makeReplayServerConfig,
} from "../../../orchestration-v2/Adapters/CodexAdapterV2.testkit.ts";
import * as ControlAdmission from "../../orchestration/ControlAdmission.ts";
import { materializeCodexReplayAppIdentity } from "../../runtimeIdentity/CodexReplayIdentity.ts";

const rootThread = "pre-ack-thread";
const rootTurn = "pre-ack-turn";
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
const makeTranscript = (terminalBeforeAck: boolean) =>
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
        outbound({
          id: 3,
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
        }),
        inbound({
          method: "turn/started",
          params: { threadId: rootThread, turn: turn(rootTurn, "inProgress") },
        }),
        inbound({
          method: "turn/started",
          params: { threadId: rootThread, turn: turn("unrelated-turn", "inProgress") },
        }),
        ...commandEntries("unrelated-turn", "unrelated-command"),
        ...commandEntries(rootTurn, "matching-command"),
        ...(terminalBeforeAck ? [rootCompletion] : []),
        inbound({ id: 3, result: { turn: turn(rootTurn, "inProgress") } }, "original-ack"),
        ...(!terminalBeforeAck ? [rootCompletion] : []),
        outbound({ id: 4, method: "model/list", params: {} }),
        inbound({ id: 4, result: { data: [], nextCursor: null } }),
      ],
    }),
  );

describe("Codex pre-ACK item reader progress", () => {
  it.effect.each(["acknowledged", "cancelled", "terminal-after-ack"] as const)(
    "retains item output only for the original %s start",
    (outcome) =>
      Effect.gen(function* () {
        const transcript = makeTranscript(outcome !== "terminal-after-ack");
        const preAckHandled = yield* Deferred.make<void>();
        const releaseAck = yield* Deferred.make<void>();
        const rootTerminal = yield* Deferred.make<void>();
        const clientReady = yield* Deferred.make<CodexClient.CodexAppServerClient["Service"]>();
        const driver = yield* CodexReplay.makeReplayDriver(transcript, {
          beforeEmitInbound: (entry) =>
            entry.label === "original-ack" ? Deferred.await(releaseAck) : Effect.void,
        });
        const replayFactory = makeCodexReplayClientFactory({ transcript, driver });
        // Only filesystem dependencies are inert. The adapter, RPC correlation,
        // notification dispatch and serialized protocol reader are all real.
        const fileSystem = FileSystem.makeNoop({
          makeTempDirectory: () => Effect.succeed("/synthetic/pre-ack-reader"),
          makeDirectory: () => Effect.void,
          exists: () => Effect.succeed(false),
        });
        const adapter = yield* CodexAdapterV2.makeCodexAdapterV2({
          instanceId: modelSelection.instanceId,
          settings: yield* Schema.decodeEffect(CodexSettings)({}),
          environment: {},
          crypto: yield* Crypto.Crypto,
          fileSystem,
          idAllocator: yield* IdAllocator.IdAllocatorV2,
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
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            Effect.sync(() => events.push(event)).pipe(
              Effect.andThen(
                event.type === "turn.terminal"
                  ? Deferred.succeed(rootTerminal, undefined)
                  : Effect.void,
              ),
            ),
          ),
          Effect.forkScoped,
        );
        const now = yield* DateTime.now;
        const start = yield* runtime.startTurn({
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
        }).pipe(Effect.result, Effect.forkScoped);
        // Wait for real notification handlers to finish, not just for the replay
        // transport to enqueue their frames. The original RPC is still withheld.
        yield* Deferred.await(preAckHandled);
        assert.isUndefined(start.pollUnsafe());
        assert.isFalse(events.some((event) => event.type === "turn_item.updated"));
        if (outcome === "cancelled") yield* Fiber.interrupt(start);
        yield* Deferred.succeed(releaseAck, undefined);
        if (outcome !== "cancelled") {
          assert.equal((yield* Fiber.join(start))._tag, "Success");
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
        // No TestClock.adjust: buffering must release the reader and preserve ACK
        // correlation without advancing a timeout or a coalescer's timer.
        assert.equal(DateTime.toEpochMillis(yield* DateTime.now), DateTime.toEpochMillis(now));
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
        assert.isAbove(deltaIndex, events.findIndex((event) => event.type === "turn_item.updated"));
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
