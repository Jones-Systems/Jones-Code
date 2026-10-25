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

const rootThread = "reader-root";
const rootTurn = "reader-root-turn";
const childThread = "reader-child";
const childTurn = "reader-child-turn";
const lateTurn = "reader-late-child-turn";
const scenario = "late-child-reader-progress";
const modelSelection = { instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID, model: "gpt-5.4" };
const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: "/workspace",
});
const turn = (id: string, status: "inProgress" | "interrupted") => ({
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

const transcript = Schema.decodeUnknownSync(CodexReplay.CodexAppServerReplayTranscript)(
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
      inbound({ id: 3, result: { turn: turn(rootTurn, "inProgress") } }),
      inbound(
        {
          method: "item/completed",
          params: {
            threadId: rootThread,
            turnId: rootTurn,
            completedAtMs: 1782622441000,
            item: {
              type: "subAgentActivity",
              id: "reader-spawn",
              kind: "started",
              agentThreadId: childThread,
              agentPath: "/root/reader-child",
            },
          },
        },
        "child-created",
      ),
      inbound({
        method: "turn/started",
        params: { threadId: childThread, turn: turn(childTurn, "inProgress") },
      }),
      inbound({
        method: "turn/completed",
        params: { threadId: childThread, turn: turn(childTurn, "interrupted") },
      }),
      inbound({
        method: "turn/completed",
        params: { threadId: rootThread, turn: turn(rootTurn, "interrupted") },
      }),
      // This read stays pending while the late notification and interrupt arrive.
      outbound({ id: 4, method: "model/list", params: {} }),
      inbound({
        method: "turn/started",
        params: { threadId: childThread, turn: turn(lateTurn, "inProgress") },
      }),
      outbound({
        id: 5,
        method: "turn/interrupt",
        params: { threadId: childThread, turnId: lateTurn },
      }),
      inbound({ id: 5, result: {} }),
      inbound({ id: 4, result: { data: [], nextCursor: null } }),
    ],
  }),
);

describe("Codex late subagent reader progress", () => {
  it.effect("drains the interrupt ACK and unrelated RPC after ancestor terminalization", () =>
    Effect.gen(function* () {
      const releaseChild = yield* Deferred.make<void>();
      const rootTerminal = yield* Deferred.make<void>();
      const interruptAcknowledged = yield* Deferred.make<void>();
      const clientReady = yield* Deferred.make<CodexClient.CodexAppServerClient["Service"]>();
      const driver = yield* CodexReplay.makeReplayDriver(transcript, {
        beforeEmitInbound: (entry) =>
          entry.label === "child-created" ? Deferred.await(releaseChild) : Effect.void,
      });
      const replayFactory = makeCodexReplayClientFactory({ transcript, driver });
      // Only filesystem dependencies are inert. The adapter, RPC correlation,
      // notification dispatch and serialized protocol reader are all real.
      const fileSystem = FileSystem.makeNoop({
        makeTempDirectory: () => Effect.succeed("/synthetic/late-child-reader"),
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
                request: (method, params) =>
                  client
                    .request(method, params)
                    .pipe(
                      Effect.tap(() =>
                        method === "turn/interrupt"
                          ? Deferred.succeed(interruptAcknowledged, undefined)
                          : Effect.void,
                      ),
                    ),
              })),
            ),
        },
      });
      const threadId = ThreadId.make("reader-progress-app-thread");
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("reader-progress-session"),
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
      yield* runtime.startTurn({
        appThread: {
          createdBy: "user",
          creationSource: "web",
          id: threadId,
          projectId: ProjectId.make("reader-progress-project"),
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
        runId: RunId.make("reader-progress-run"),
        runOrdinal: 1,
        providerTurnOrdinal: 1,
        attemptId: RunAttemptId.make("reader-progress-attempt"),
        rootNodeId: NodeId.make("reader-progress-node"),
        providerThread,
        message: {
          createdBy: "user",
          creationSource: "web",
          messageId: MessageId.make("reader-progress-message"),
          text: "Work",
          attachments: [],
        },
        modelSelection,
        runtimePolicy,
      });
      yield* Deferred.succeed(releaseChild, undefined);
      yield* Deferred.await(rootTerminal);

      const client = yield* Deferred.await(clientReady);
      const unrelated = yield* client.request("model/list", {}).pipe(Effect.forkScoped);
      // No TestClock.adjust: the pre-fix handler cannot consume either reply
      // until its ten-second timeout, so this test must finish at the same time.
      yield* Deferred.await(interruptAcknowledged);
      assert.deepEqual(yield* Fiber.join(unrelated), { data: [], nextCursor: null });
      assert.equal(DateTime.toEpochMillis(yield* DateTime.now), DateTime.toEpochMillis(now));
      assert.deepEqual(yield* Ref.get(driver.state), {
        cursor: transcript.entries.length,
        failure: null,
      });
      const terminalIndex = events.findIndex((event) => event.type === "turn.terminal");
      const childTerminalIndex = events.findIndex(
        (event) =>
          event.type === "provider_turn.updated" &&
          event.providerTurn.nativeTurnRef?.nativeId === childTurn &&
          event.providerTurn.status === "interrupted",
      );
      assert.isAtLeast(childTerminalIndex, 0);
      assert.isAbove(terminalIndex, childTerminalIndex);
      assert.equal(events.filter((event) => event.type === "turn.terminal").length, 1);
      assert.isFalse(
        events.some(
          (event) =>
            event.type === "provider_turn.updated" &&
            event.providerTurn.nativeTurnRef?.nativeId === lateTurn,
        ),
        "a late start must not reopen the terminalized child",
      );
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
