import { historyResponseItems } from "@t3tools/provider-core/server/handoffBudget";
import {
  makeProviderTextDeltaCoalescer,
  type ProviderTextDeltaUpdate,
} from "./ProviderTextDeltaCoalescer.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  CheckpointId,
  CodexSettings,
  EnvironmentId,
  MessageId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  ProjectId,
  type ProviderApprovalDecision,
  ProviderInstanceId,
  ProviderDriverKind,
  ProviderSetupError,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexReplay from "effect-codex-app-server/replay";
import type * as CodexSchema from "effect-codex-app-server/schema";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";
import * as Predicate from "effect/Predicate";
import * as FileSystem from "effect/FileSystem";
import { MCP_APP_OUTPUT_KEY, readMcpAppReference } from "@t3tools/shared/mcpApp";
import { resolveAttachmentPathById } from "../../attachmentStore.ts";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Sink from "effect/Sink";
import { TestClock } from "effect/testing";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import packageJson from "../../../package.json" with { type: "json" };
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as EffectWorker from "../EffectWorker.ts";
import * as Orchestrator from "../Orchestrator.ts";
import * as ProviderReplayHarness from "../testkit/ProviderReplayHarness.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as CodexAdapterV2 from "./CodexAdapterV2.ts";
import { CODEX_START_ACK_TIMEOUT_MS } from "../../jones/provider/codex/CodexLateStartRecovery.ts";
import {
  makeReplayServerConfig,
  makeCodexReplayClientFactory,
  withCodexReplayChildMetadata,
} from "./CodexAdapterV2.testkit.ts";
import * as CodexAdapterV2Testkit from "./CodexAdapterV2.testkit.ts";

const replayTranscriptJson = Schema.fromJsonString(CodexReplay.CodexAppServerReplayTranscript);
const encodeReplayTranscriptJson = Schema.encodeEffect(replayTranscriptJson);
const decodeRawReplayTranscriptJson = Schema.decodeUnknownEffect(replayTranscriptJson);
const decodeReplayTranscriptJson = (input: string) =>
  decodeRawReplayTranscriptJson(input).pipe(
    Effect.flatMap(CodexAdapterV2Testkit.CodexOrchestratorReplayHarness.decodeTranscript),
  );
const encodeStringJson = Schema.encodeEffect(Schema.fromJsonString(Schema.String));

describe("CodexAdapterV2 context usage", () => {
  it("uses the current context rather than cumulative processed tokens", () => {
    const usage = CodexAdapterV2.codexProviderTurnTokenUsage(
      {
        total: {
          totalTokens: 180_000,
          inputTokens: 160_000,
          cachedInputTokens: 20_000,
          outputTokens: 20_000,
          reasoningOutputTokens: 5_000,
        },
        last: {
          totalTokens: 50_000,
          inputTokens: 45_000,
          cachedInputTokens: 10_000,
          outputTokens: 5_000,
          reasoningOutputTokens: 1_000,
        },
        modelContextWindow: 200_000,
      },
      "2026-08-29T00:00:00.000Z",
    );

    assert.deepEqual(usage, {
      usedTokens: 50_000,
      maxTokens: 200_000,
      inputTokens: 45_000,
      cachedInputTokens: 10_000,
      outputTokens: 5_000,
      reasoningOutputTokens: 1_000,
      updatedAt: "2026-08-29T00:00:00.000Z",
    });
  });
});

describe("CodexAdapterV2 assistant message streaming", () => {
  it.effect("flushes buffered text synchronously before item and turn completion", () =>
    Effect.gen(function* () {
      const updates = yield* Ref.make<ReadonlyArray<ProviderTextDeltaUpdate>>([]);
      const coalescer = yield* makeProviderTextDeltaCoalescer({
        flushIntervalMs: 50,
        emit: (update) => Ref.update(updates, (current) => [...current, update]),
      });

      yield* coalescer.append({ turnId: "turn-1", itemId: "message-1", delta: "item final" });
      const completedText = yield* coalescer.complete({
        turnId: "turn-1",
        itemId: "message-1",
      });
      yield* coalescer.append({ turnId: "turn-1", itemId: "message-2", delta: "turn final" });
      yield* coalescer.flushTurn("turn-1");

      assert.equal(completedText, "item final");
      assert.deepEqual(yield* Ref.get(updates), [
        { turnId: "turn-1", itemId: "message-1", text: "item final", completed: true },
        { turnId: "turn-1", itemId: "message-2", text: "turn final", completed: true },
      ]);
      yield* Effect.yieldNow;
      yield* TestClock.adjust("50 millis");
      yield* Effect.yieldNow;
      assert.equal((yield* Ref.get(updates)).length, 2);
    }),
  );
});

describe("CodexAdapterV2 runtime policy", () => {
  it.effect("derives concrete Codex turn policies from every T3 runtime mode", () =>
    Effect.gen(function* () {
      const build = (
        runtimeMode: "approval-required" | "auto-accept-edits" | "auto" | "full-access",
      ) =>
        CodexAdapterV2.buildCodexTurnStartParams({
          nativeThreadId: `native-${runtimeMode}`,
          codexInput: [{ type: "text", text: "test" }],
          runtimePolicy: {
            runtimeMode,
            interactionMode: "default",
            cwd: null,
          },
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5.4",
          },
        });

      const approvalRequired = yield* build("approval-required");
      const autoAcceptEdits = yield* build("auto-accept-edits");
      const auto = yield* build("auto");
      const fullAccess = yield* build("full-access");

      assert.equal(approvalRequired.approvalPolicy, "untrusted");
      assert.equal(approvalRequired.approvalsReviewer, "user");
      assert.equal(approvalRequired.sandboxPolicy?.type, "readOnly");
      assert.equal(autoAcceptEdits.approvalPolicy, "on-request");
      assert.equal(autoAcceptEdits.approvalsReviewer, "user");
      assert.equal(autoAcceptEdits.sandboxPolicy?.type, "workspaceWrite");
      assert.equal(auto.approvalPolicy, "on-request");
      assert.equal(auto.approvalsReviewer, "auto_review");
      assert.equal(auto.sandboxPolicy?.type, "workspaceWrite");
      assert.equal(fullAccess.approvalPolicy, "never");
      assert.equal(fullAccess.approvalsReviewer, "user");
      assert.equal(fullAccess.sandboxPolicy?.type, "dangerFullAccess");
    }),
  );

  it.effect("sends MCP app model context as untrusted Codex context", () =>
    Effect.gen(function* () {
      const policy = { runtimeMode: "full-access", interactionMode: "default", cwd: null } as const;
      const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" };
      const appContext = [{ key: "mcp_app_todos_list_todos_item-1", text: "Filtered to overdue" }];
      const alone = yield* CodexAdapterV2.buildCodexTurnStartParams({
        nativeThreadId: "native-app-context",
        codexInput: [{ type: "text", text: "what's on my list?" }],
        runtimePolicy: policy,
        modelSelection,
        appContext,
      });
      const { jones_code_identity, ...appEntries } = alone.additionalContext!;
      assert.equal(jones_code_identity?.kind, "application");
      assert.include(jones_code_identity?.value ?? "", "product ID: jones-code");
      assert.deepEqual(appEntries, {
        "mcp_app_todos_list_todos_item-1": { kind: "untrusted", value: "Filtered to overdue" },
      });
      const withT3 = yield* CodexAdapterV2.buildCodexTurnStartParams({
        nativeThreadId: "native-app-context",
        codexInput: [{ type: "text", text: "what's on my list?" }],
        runtimePolicy: policy,
        modelSelection,
        hasT3Mcp: true,
        appContext,
      });
      assert.deepEqual(withT3.additionalContext?.["mcp_app_todos_list_todos_item-1"], {
        kind: "untrusted",
        value: "Filtered to overdue",
      });
      assert.isDefined(withT3.additionalContext?.["t3_code_runtime"]);
    }),
  );
});

describe("CodexAdapterV2 native protocol logging", () => {
  it.effect("logs decoded app-server frames once with credentials redacted", () =>
    Effect.gen(function* () {
      const writes: Array<{
        readonly event: unknown;
        readonly threadId: ThreadId | null;
      }> = [];
      const logger: ProviderEventLoggers.EventNdjsonLogger = {
        filePath: "/tmp/events.log",
        write: (event, threadId) =>
          Effect.sync(() => {
            writes.push({ event, threadId });
          }),
        close: () => Effect.void,
      };
      const threadId = ThreadId.make("thread-1");
      const providerSessionId = ProviderSessionId.make("provider-session-1");
      const protocolLogger = CodexAdapterV2.makeCodexAppServerProtocolLogger({
        nativeEventLogger: logger,
        threadId,
        providerSessionId,
      });

      assert.notEqual(protocolLogger, undefined);
      if (protocolLogger === undefined) {
        return;
      }

      yield* protocolLogger({
        direction: "incoming",
        stage: "decoded",
        payload: {
          method: "thread/event",
          params: {
            id: "evt-1",
            http_headers: { Authorization: "Bearer secret-codex-token" },
            usage: { inputTokens: 12, outputTokens: 8, totalTokens: 20 },
          },
        },
      });
      yield* protocolLogger({
        direction: "incoming",
        stage: "raw",
        payload:
          '{"method":"thread/event","params":{"http_headers":{"Authorization":"Bearer secret-codex-token"}}}\n',
      });

      assert.equal(writes.length, 1);
      assert.equal(writes[0]?.threadId, threadId);
      assert.deepEqual(writes[0]?.event, {
        provider: "codex",
        protocol: "codex.app-server",
        kind: "protocol",
        providerSessionId,
        event: {
          direction: "incoming",
          stage: "decoded",
          payload: {
            method: "thread/event",
            params: {
              id: "evt-1",
              http_headers: { Authorization: "[REDACTED]" },
              usage: { inputTokens: 12, outputTokens: 8, totalTokens: 20 },
            },
          },
        },
      });
    }),
  );
});

describe("CodexAdapterV2 rollback mapping", () => {
  it.effect("derives native rollback count from durable provider turns", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const providerThreadId = ProviderThreadId.make("provider-thread-codex-rollback");
      const providerThread: OrchestrationV2ProviderThread = {
        id: providerThreadId,
        driver: CodexAdapterV2.CODEX_DRIVER_KIND,
        providerInstanceId: ProviderInstanceId.make("codex"),
        providerSessionId: ProviderSessionId.make("provider-session-codex-rollback"),
        appThreadId: ThreadId.make("thread-codex-rollback"),
        ownerNodeId: null,
        nativeThreadRef: {
          driver: CodexAdapterV2.CODEX_DRIVER_KIND,
          nativeId: "native-thread-codex-rollback",
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 3,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      const providerTurn = (
        id: string,
        ordinal: number,
        status: OrchestrationV2ProviderTurn["status"],
      ): OrchestrationV2ProviderTurn => ({
        id: ProviderTurnId.make(id),
        providerThreadId,
        nodeId: NodeId.make(`node-${id}`),
        runAttemptId: RunAttemptId.make(`run-attempt-${id}`),
        nativeTurnRef: {
          driver: CodexAdapterV2.CODEX_DRIVER_KIND,
          nativeId: `native-${id}`,
          strength: "strong",
        },
        ordinal,
        status,
        startedAt: now,
        completedAt: status === "running" || status === "pending" ? null : now,
      });
      const firstTurn = providerTurn("provider-turn-first", 1, "completed");
      const secondTurn = providerTurn("provider-turn-second", 2, "completed");
      const runningTurn = providerTurn("provider-turn-running", 3, "running");
      const interruptedTurn = providerTurn("provider-turn-interrupted", 4, "interrupted");
      const goalCommandTurn = {
        ...providerTurn("provider-turn-goal-command", 5, "completed"),
        nativeTurnRef: null,
      };

      const numTurns = yield* CodexAdapterV2.resolveCodexRollbackTurnCount({
        providerThread,
        target: {
          type: "provider_turn",
          checkpointId: CheckpointId.make("checkpoint-first"),
          appRunOrdinal: 1,
          providerTurn: firstTurn,
        },
        providerThreadTurns: [goalCommandTurn, interruptedTurn, runningTurn, secondTurn, firstTurn],
      });

      assert.equal(numTurns, 2);
    }),
  );
});

describe("CodexAdapterV2 fork boundary", () => {
  const providerThreadId = ProviderThreadId.make("provider-thread-codex-fork-boundary");
  const makeProviderThread = (now: DateTime.Utc): OrchestrationV2ProviderThread => ({
    id: providerThreadId,
    driver: CodexAdapterV2.CODEX_DRIVER_KIND,
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerSessionId: ProviderSessionId.make("provider-session-codex-fork-boundary"),
    appThreadId: ThreadId.make("thread-codex-fork-boundary"),
    ownerNodeId: null,
    nativeThreadRef: {
      driver: CodexAdapterV2.CODEX_DRIVER_KIND,
      nativeId: "native-thread-codex-fork-boundary",
      strength: "strong",
    },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: 1,
    lastRunOrdinal: 2,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  });
  const makeProviderTurn = (
    id: string,
    ordinal: number,
    nativeId: string | null,
    now: DateTime.Utc,
  ): OrchestrationV2ProviderTurn => ({
    id: ProviderTurnId.make(id),
    providerThreadId,
    nodeId: NodeId.make(`node-${id}`),
    runAttemptId: RunAttemptId.make(`run-attempt-${id}`),
    nativeTurnRef:
      nativeId === null
        ? { driver: CodexAdapterV2.CODEX_DRIVER_KIND, nativeId: null, strength: "none" }
        : { driver: CodexAdapterV2.CODEX_DRIVER_KIND, nativeId, strength: "strong" },
    ordinal,
    status: "completed",
    startedAt: now,
    completedAt: now,
  });

  it.effect("resolves the selected provider turn to an inclusive native fork boundary", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const firstTurn = makeProviderTurn("provider-turn-first", 1, "native-turn-first", now);
      const secondTurn = makeProviderTurn("provider-turn-second", 2, "native-turn-second", now);

      const boundary = yield* CodexAdapterV2.resolveCodexForkBoundary({
        sourceProviderThread: makeProviderThread(now),
        sourceProviderTurns: [firstTurn, secondTurn],
        providerTurnId: firstTurn.id,
        targetThreadId: ThreadId.make("thread-codex-fork-target"),
      });

      assert.deepEqual(boundary, {
        lastTurnId: "native-turn-first",
        rollbackTurnCount: 0,
      });
    }),
  );

  it.effect("keeps the rollback-count fallback when the boundary turn lacks a native id", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const firstTurn = makeProviderTurn("provider-turn-first", 1, null, now);
      const secondTurn = makeProviderTurn("provider-turn-second", 2, "native-turn-second", now);

      const boundary = yield* CodexAdapterV2.resolveCodexForkBoundary({
        sourceProviderThread: makeProviderThread(now),
        sourceProviderTurns: [firstTurn, secondTurn],
        providerTurnId: firstTurn.id,
        targetThreadId: ThreadId.make("thread-codex-fork-target"),
      });

      assert.deepEqual(boundary, { lastTurnId: undefined, rollbackTurnCount: 1 });
    }),
  );
});

const DEFAULT_CODEX_SETTINGS = Schema.decodeSync(CodexSettings)({});
const CODEX_TEST_MODEL_SELECTION = {
  instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
  model: "gpt-5.4",
} satisfies ModelSelection;
const CODEX_TEST_RUNTIME_POLICY = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: "/workspace",
});

function makeCodexTestAppThread(input: {
  readonly threadId: ThreadId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly now: DateTime.Utc;
}): OrchestrationV2AppThread {
  return {
    createdBy: "user",
    creationSource: "web",
    id: input.threadId,
    projectId: ProjectId.make(`project-${input.threadId}`),
    title: "Codex continuation test",
    providerInstanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
    modelSelection: CODEX_TEST_MODEL_SELECTION,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
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
  };
}

function makeCodexTestTurnInput(input: {
  readonly threadId: ThreadId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly now: DateTime.Utc;
  readonly attemptId: RunAttemptId;
  readonly text: string;
}): ProviderAdapter.ProviderAdapterV2TurnInput {
  return {
    appThread: makeCodexTestAppThread(input),
    threadId: input.threadId,
    runId: RunId.make(`run-${input.attemptId}`),
    runOrdinal: 1,
    providerTurnOrdinal: 1,
    attemptId: input.attemptId,
    rootNodeId: NodeId.make(`node-${input.attemptId}`),
    providerThread: input.providerThread,
    message: {
      createdBy: "user",
      creationSource: "web",
      messageId: MessageId.make(`message-${input.attemptId}`),
      text: input.text,
      attachments: [],
    },
    modelSelection: CODEX_TEST_MODEL_SELECTION,
    runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
  };
}

function makeCodexReplayTurn(input: {
  readonly id: string;
  readonly status: "inProgress" | "completed" | "interrupted" | "failed";
}): Record<string, unknown> {
  const terminal =
    input.status === "completed" || input.status === "interrupted" || input.status === "failed";
  return {
    id: input.id,
    items: [],
    itemsView: "notLoaded",
    status: input.status,
    error: null,
    startedAt: 1782622440,
    completedAt: terminal ? 1782622450 : null,
    durationMs: null,
  };
}

function codexReplayPreamble(input: {
  readonly nativeThreadId: string;
  readonly nativeTurnId: string;
  readonly prompt: string;
  readonly sentPrompt?: string;
}): Array<CodexReplay.CodexAppServerReplayEntry> {
  return [
    {
      type: "expect_outbound",
      label: "initialize",
      frame: {
        id: 1,
        method: "initialize",
        params: {
          clientInfo: { name: "T3 Code", title: "T3 Code", version: packageJson.version },
          capabilities: {
            experimentalApi: true,
            optOutNotificationMethods: ["turn/diff/updated"],
            extensions: {
              "io.modelcontextprotocol/ui": { mimeTypes: ["text/html;profile=mcp-app"] },
            },
          },
        },
      },
    },
    {
      type: "emit_inbound",
      label: "initialize",
      frame: {
        id: 1,
        result: {
          userAgent: "T3 Code/0.156.1",
          codexHome: "/tmp/codex-home",
          platformFamily: "unix",
          platformOs: "macos",
        },
      },
    },
    { type: "expect_outbound", label: "initialized", frame: { method: "initialized" } },
    {
      type: "expect_outbound",
      label: "thread/start",
      frame: {
        id: 2,
        method: "thread/start",
        params: { config: CodexAdapterV2.CODEX_THREAD_CONFIG },
      },
    },
    {
      type: "emit_inbound",
      label: "thread/start",
      frame: {
        id: 2,
        result: {
          thread: {
            id: input.nativeThreadId,
            sessionId: input.nativeThreadId,
            forkedFromId: null,
            preview: "",
            ephemeral: false,
            modelProvider: "openai",
            createdAt: 1782622440,
            updatedAt: 1782622440,
            status: { type: "idle" },
            path: `/tmp/${input.nativeThreadId}.jsonl`,
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
          model: "gpt-5.4",
          modelProvider: "openai",
          serviceTier: null,
          cwd: "/workspace",
          instructionSources: [],
          approvalPolicy: "on-request",
          approvalsReviewer: "user",
          sandbox: { type: "workspaceWrite", writableRoots: [], networkAccess: false },
          reasoningEffort: "medium",
        },
      },
    },
    {
      type: "expect_outbound",
      label: "turn/start",
      frame: {
        id: 3,
        method: "turn/start",
        params: {
          threadId: input.nativeThreadId,
          input: [{ type: "text", text: input.sentPrompt ?? input.prompt }],
          cwd: "/workspace",
          model: "gpt-5.4",
          approvalPolicy: "never",
          approvalsReviewer: "user",
          sandboxPolicy: { type: "dangerFullAccess" },
          summary: "detailed",
        },
      },
    },
    {
      type: "emit_inbound",
      label: "turn/start",
      frame: {
        id: 3,
        result: { turn: makeCodexReplayTurn({ id: input.nativeTurnId, status: "inProgress" }) },
      },
    },
    {
      type: "emit_inbound",
      label: "turn/started",
      frame: {
        method: "turn/started",
        params: {
          threadId: input.nativeThreadId,
          turn: makeCodexReplayTurn({ id: input.nativeTurnId, status: "inProgress" }),
        },
      },
    },
  ];
}

const decodeMaterializedTranscript = Schema.decodeUnknownSync(
  CodexReplay.CodexAppServerReplayTranscript,
);

function makeCodexReplayTranscript(input: {
  readonly scenario: string;
  readonly entries: ReadonlyArray<CodexReplay.CodexAppServerReplayEntry>;
}): CodexReplay.CodexAppServerReplayTranscript {
  return decodeMaterializedTranscript(
    materializeCodexReplayAppIdentity({
      provider: "codex",
      protocol: "codex.app-server",
      version: "0.144.0",
      scenario: input.scenario,
      entries: input.entries,
    }),
  );
}

function withReplayRequestId(
  entry: CodexReplay.CodexAppServerReplayEntry,
  id: number,
): CodexReplay.CodexAppServerReplayEntry {
  return entry.type !== "runtime_exit" && Predicate.isObject(entry.frame)
    ? { ...entry, frame: { ...entry.frame, id } }
    : entry;
}

describe("CodexAdapterV2 session initialize", () => {
  const openReplaySession = (
    transcript: CodexReplay.CodexAppServerReplayTranscript,
    beforeEmitInbound?: CodexReplay.CodexAppServerReplayDriver["beforeEmitInbound"],
  ) =>
    Effect.gen(function* () {
      const driver = yield* CodexReplay.makeReplayDriver(
        transcript,
        beforeEmitInbound === undefined ? {} : { beforeEmitInbound },
      );
      let initializeRequests = 0;
      const adapter = yield* CodexAdapterV2.makeCodexAdapterV2({
        instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
        settings: DEFAULT_CODEX_SETTINGS,
        environment: {},
        clientFactory: {
          open: (openInput) =>
            Layer.build(CodexReplay.layerReplayWithDriver(driver)).pipe(
              Effect.flatMap((context) =>
                Effect.service(CodexClient.CodexAppServerClient).pipe(Effect.provide(context)),
              ),
              Effect.map(
                (client) =>
                  ({
                    ...client,
                    request: (method, params) =>
                      Effect.sync(() => {
                        if (method === "initialize") initializeRequests++;
                      }).pipe(Effect.andThen(client.request(method, params))),
                  }) satisfies CodexClient.CodexAppServerClient["Service"],
              ),
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterOpenSessionError({
                    driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                    providerSessionId: openInput.providerSessionId,
                    cause,
                  }),
              ),
            ),
        },
        crypto: yield* Crypto.Crypto,
        fileSystem: yield* FileSystem.FileSystem,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        serverConfig: yield* makeReplayServerConfig(transcript.scenario).pipe(Effect.orDie),
      });
      const runtime = yield* adapter.openSession({
        threadId: ThreadId.make(`thread-${transcript.scenario}`),
        providerSessionId: ProviderSessionId.make(`provider-session-${transcript.scenario}`),
        modelSelection: CODEX_TEST_MODEL_SELECTION,
        runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
      });
      return {
        ensureThread: (threadId: string) =>
          runtime.ensureThread({
            threadId: ThreadId.make(threadId),
            modelSelection: CODEX_TEST_MODEL_SELECTION,
            runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
          }),
        initializeRequests: () => initializeRequests,
      };
    });

  const replayPreamble = (nativeThreadId: string) =>
    codexReplayPreamble({ nativeThreadId, nativeTurnId: "unused", prompt: "unused" });

  it.effect("sends one initialize when two threads start on a fresh session at once", () =>
    Effect.gen(function* () {
      const initializeAwaitingResponse = yield* Deferred.make<void>();
      const releaseInitialize = yield* Deferred.make<void>();
      // The transcript allows exactly one handshake: a second `initialize`
      // frame fails the replay, as Codex rejects it with "Already initialized".
      const session = yield* openReplaySession(
        makeCodexReplayTranscript({
          scenario: "concurrent-initialize",
          entries: [
            ...replayPreamble("concurrent-first").slice(0, 5),
            ...replayPreamble("concurrent-second")
              .slice(3, 5)
              .map((entry) => withReplayRequestId(entry, 3)),
          ],
        }),
        (entry) =>
          entry.label === "initialize"
            ? Deferred.succeed(initializeAwaitingResponse, undefined).pipe(
                Effect.andThen(Deferred.await(releaseInitialize)),
              )
            : Effect.void,
      );

      const first = yield* session
        .ensureThread("thread-concurrent-first")
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(initializeAwaitingResponse);
      const second = yield* session
        .ensureThread("thread-concurrent-second")
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.succeed(releaseInitialize, undefined);

      const providerThreads = [yield* Fiber.join(first), yield* Fiber.join(second)];
      assert.equal(session.initializeRequests(), 1);
      assert.sameMembers(
        providerThreads.map((providerThread) => providerThread.nativeThreadRef?.nativeId),
        ["concurrent-first", "concurrent-second"],
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  // regression: #16260 one-shot initializers raced; a failed handshake must be retried
  it.effect("retries initialize after a failed handshake", () =>
    Effect.gen(function* () {
      const preamble = replayPreamble("initialize-retry");
      const entries: Array<CodexReplay.CodexAppServerReplayEntry> = [
        ...preamble.slice(0, 1),
        {
          type: "emit_inbound",
          label: "initialize",
          frame: { id: 1, error: { code: -32603, message: "Codex is not ready." } },
        },
        ...preamble.slice(0, 2).map((entry) => withReplayRequestId(entry, 2)),
        ...preamble.slice(2, 3),
        ...preamble.slice(3, 5).map((entry) => withReplayRequestId(entry, 3)),
      ];
      const session = yield* openReplaySession(
        makeCodexReplayTranscript({ scenario: "initialize-retry", entries }),
      );

      const failure = yield* session.ensureThread("thread-initialize-retry").pipe(Effect.flip);
      assert.equal(failure._tag, "ProviderAdapterEnsureThreadError");
      const providerThread = yield* session.ensureThread("thread-initialize-retry");
      assert.equal(providerThread.nativeThreadRef?.nativeId, "initialize-retry");
      assert.equal(session.initializeRequests(), 2);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect("completes the handshake after a caller is interrupted mid-initialize", () =>
    Effect.gen(function* () {
      const initializeAwaitingResponse = yield* Deferred.make<void>();
      const releaseInitialize = yield* Deferred.make<void>();
      const preamble = replayPreamble("initialize-interrupted");
      const session = yield* openReplaySession(
        makeCodexReplayTranscript({
          scenario: "initialize-interrupted",
          entries: [
            ...preamble.slice(0, 2),
            ...preamble.slice(0, 1).map((entry) => withReplayRequestId(entry, 2)),
            {
              type: "emit_inbound",
              label: "initialize-rejected",
              frame: { id: 2, error: { code: -32600, message: "Already initialized" } },
            },
            ...preamble.slice(2, 3),
            ...preamble.slice(3, 5).map((entry) => withReplayRequestId(entry, 3)),
          ],
        }),
        (entry) =>
          entry.label === "initialize"
            ? Deferred.succeed(initializeAwaitingResponse, undefined).pipe(
                Effect.andThen(Deferred.await(releaseInitialize)),
              )
            : Effect.void,
      );

      const interrupted = yield* session
        .ensureThread("thread-initialize-interrupted")
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(initializeAwaitingResponse);
      yield* Fiber.interrupt(interrupted);
      yield* Deferred.succeed(releaseInitialize, undefined);

      const providerThread = yield* session.ensureThread("thread-initialize-interrupted");
      assert.equal(providerThread.nativeThreadRef?.nativeId, "initialize-interrupted");
      assert.equal(session.initializeRequests(), 2);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );
});

describe("CodexAdapterV2 post-settle continuation", () => {
  // The harness notifies after every recorded event or continuation, so a wait
  // re-checks exactly when the state it reads changes. Subscribing before the
  // first check means a change that lands in between is not lost.
  const makeChangeSignal = Effect.gen(function* () {
    const changes = yield* PubSub.unbounded<void>();
    const until = (predicate: () => boolean): Effect.Effect<void> =>
      Effect.scoped(
        Effect.gen(function* () {
          const subscription = yield* PubSub.subscribe(changes);
          while (!predicate()) yield* PubSub.take(subscription);
        }),
      );
    return { notify: PubSub.publish(changes, undefined), until };
  });

  const makeCodexReplayHarness = (
    transcript: CodexReplay.CodexAppServerReplayTranscript,
    onEvent: (event: ProviderAdapter.ProviderAdapterV2Event) => Effect.Effect<unknown> = () =>
      Effect.void,
    onRequest: (method: string, params: unknown) => Effect.Effect<void> = () => Effect.void,
    readChildMetadata?: Parameters<typeof withCodexReplayChildMetadata>[2],
    options: Pick<CodexAdapterV2.CodexAdapterV2Options, "resolveRuntime" | "getModelCatalog"> & {
      readonly replayClientFactory?: CodexAdapterV2.CodexAppServerClientFactoryShape;
      readonly modelSelection?: ModelSelection;
      readonly runtimeLifecycle?: ProviderAdapter.ProviderRuntimeLifecycle;
      readonly onRawRequest?: (method: string, params: unknown) => Effect.Effect<void>;
      readonly replacementTranscripts?: ReadonlyArray<CodexReplay.CodexAppServerReplayTranscript>;
      readonly onOpen?: (
        input: Parameters<CodexAdapterV2.CodexAppServerClientFactoryShape["open"]>[0],
        ordinal: number,
      ) => Effect.Effect<void, ProviderAdapter.ProviderAdapterOpenSessionError>;
      readonly onClose?: (ordinal: number) => Effect.Effect<void>;
      readonly onNotification?: (
        method: string,
        handler: (payload: unknown) => Effect.Effect<unknown>,
        ordinal: number,
      ) => void;
    } = {},
    goalRequest: CodexClient.CodexAppServerClient["Service"]["raw"]["request"] = () =>
      Effect.succeed({ goal: null }),
  ) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const serverConfig = yield* makeReplayServerConfig(transcript.scenario).pipe(Effect.orDie);
      const changes = yield* makeChangeSignal;
      const continuationRequests: Array<ProviderContinuationRequests.ProviderContinuationRequest> =
        [];
      let openCount = 0;
      const clientFactory: CodexAdapterV2.CodexAppServerClientFactoryShape =
        options.replayClientFactory ?? {
          open: (openInput) =>
            Effect.suspend(() => {
              const ordinal = openCount++;
              const actualTranscript =
                ordinal === 0 ? transcript : options.replacementTranscripts?.[ordinal - 1];
              if (actualTranscript === undefined)
                return Effect.die("Unexpected synthetic Codex client replacement");
              return (options.onOpen?.(openInput, ordinal) ?? Effect.void).pipe(
                Effect.andThen(
                  Effect.addFinalizer(() => options.onClose?.(ordinal) ?? Effect.void),
                ),
                Effect.andThen(
                  Layer.build(
                    CodexReplay.layerReplay(
                      actualTranscript,
                      openInput.onTermination === undefined
                        ? {}
                        : { onTermination: openInput.onTermination },
                    ),
                  ),
                ),
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterOpenSessionError({
                      driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                      providerSessionId: openInput.providerSessionId,
                      cause,
                    }),
                ),
                Effect.flatMap((context) =>
                  Effect.service(CodexClient.CodexAppServerClient).pipe(
                    Effect.map((client) =>
                      withCodexReplayChildMetadata(client, actualTranscript, readChildMetadata),
                    ),
                    Effect.map(
                      (client) =>
                        ({
                          ...client,
                          handleServerNotification: (method, handler) => {
                            options.onNotification?.(
                              method,
                              (payload) => Reflect.apply(handler, undefined, [payload]),
                              ordinal,
                            );
                            return client.handleServerNotification(method, handler);
                          },
                          raw: {
                            ...client.raw,
                            request: (method, params) =>
                              method === "thread/goal/get" || method === "thread/goal/set"
                                ? goalRequest(method, params)
                                : (options.onRawRequest?.(method, params) ?? Effect.void).pipe(
                                    Effect.andThen(client.raw.request(method, params)),
                                  ),
                          },
                          request: (method, params) =>
                            onRequest(method, params).pipe(
                              Effect.andThen(client.request(method, params)),
                            ),
                        }) satisfies CodexClient.CodexAppServerClient["Service"],
                    ),
                    Effect.provide(context),
                  ),
                ),
              );
            }),
        };
      const adapter = yield* CodexAdapterV2.makeCodexAdapterV2({
        instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
        settings: DEFAULT_CODEX_SETTINGS,
        environment: {},
        clientFactory,
        crypto: yield* Crypto.Crypto,
        fileSystem,
        idAllocator,
        serverConfig,
        ...(options.resolveRuntime === undefined ? {} : { resolveRuntime: options.resolveRuntime }),
        ...(options.getModelCatalog === undefined
          ? {}
          : { getModelCatalog: options.getModelCatalog }),
        continuationRequests: {
          offer: (request) =>
            Effect.sync(() => {
              continuationRequests.push(request);
            }).pipe(Effect.andThen(changes.notify)),
        },
      });
      const threadId = ThreadId.make(`thread-${transcript.scenario}`);
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make(`provider-session-${transcript.scenario}`),
        modelSelection: options.modelSelection ?? CODEX_TEST_MODEL_SELECTION,
        runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
        ...(options.runtimeLifecycle === undefined
          ? {}
          : { runtimeLifecycle: options.runtimeLifecycle }),
      });
      const providerThread = yield* runtime.ensureThread({
        threadId,
        modelSelection: options.modelSelection ?? CODEX_TEST_MODEL_SELECTION,
        runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
      });
      const events: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
      const firstTerminal = yield* Deferred.make<void>();
      const eventsEnded = yield* Deferred.make<void>();
      yield* runtime.events.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            events.push(event);
          }).pipe(
            Effect.andThen(
              event.type === "turn.terminal"
                ? Deferred.succeed(firstTerminal, undefined)
                : Effect.void,
            ),
            Effect.andThen(changes.notify),
            Effect.andThen(onEvent(event)),
          ),
        ),
        Effect.andThen(Deferred.succeed(eventsEnded, undefined)),
        Effect.forkScoped,
      );
      if (runtime.hasPendingBackgroundWork === undefined) {
        return yield* Effect.die("Codex adapter runtime must expose hasPendingBackgroundWork.");
      }
      const hasPendingBackgroundWork = runtime.hasPendingBackgroundWork;
      const terminalEvents = () =>
        events.filter(
          (
            event,
          ): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }> =>
            event.type === "turn.terminal",
        );
      const subagentUpdates = () =>
        events.filter(
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "subagent.updated" }
          > => event.type === "subagent.updated",
        );
      return {
        runtime,
        providerThread,
        threadId,
        serverConfig,
        events,
        continuationRequests,
        terminalEvents,
        subagentUpdates,
        hasPendingBackgroundWork,
        firstTerminal: Deferred.await(firstTerminal),
        until: changes.until,
        awaitTerminal: (count: number) => changes.until(() => terminalEvents().length >= count),
      };
    });

  it.effect("fails active turns exactly once on producer termination without replay", () =>
    Effect.gen(function* () {
      const nativeThreadId = "dead-producer-thread";
      let opened:
        | Parameters<CodexAdapterV2.CodexAppServerClientFactoryShape["open"]>[0]
        | undefined;
      const requests: Array<string> = [];
      const harness = yield* makeCodexReplayHarness(
        makeCodexReplayTranscript({
          scenario: "dead-producer",
          entries: codexReplayPreamble({
            nativeThreadId,
            nativeTurnId: "dead-turn",
            prompt: "Work",
          }),
        }),
        undefined,
        (method) =>
          Effect.sync(() => {
            requests.push(method);
          }),
        undefined,
        {
          onOpen: (input) =>
            Effect.sync(() => {
              opened = input;
            }),
        },
      );
      yield* harness.runtime.startTurn(
        makeCodexTestTurnInput({
          threadId: harness.threadId,
          providerThread: harness.providerThread,
          now: yield* DateTime.now,
          attemptId: RunAttemptId.make("dead-producer-attempt"),
          text: "Work",
        }),
      );
      const terminate = () =>
        opened?.onTermination?.(new CodexError.CodexAppServerProcessExitedError({ code: 7 })) ??
        Effect.void;
      yield* terminate();
      for (let i = 0; i < 40; i++) yield* Effect.yieldNow;
      assert.equal(harness.terminalEvents().length, 1);
      assert.equal(harness.terminalEvents()[0]?.status, "failed");
      yield* terminate();
      for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
      assert.equal(harness.terminalEvents().length, 1);
      assert.equal(yield* harness.hasPendingBackgroundWork, false);
      const observation = yield* harness.runtime.readThreadActivity!(harness.providerThread);
      assert.equal(observation.status, "unknown");
      assert.equal(requests.filter((method) => method === "turn/start").length, 1);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect("publishes approval artifacts before a consumer can answer the pending request", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "native-origin-approval-order";
        const nativeTurnId = "native-origin-approval-turn";
        const prompt = "Request approval.";
        const transcript = makeCodexReplayTranscript({
          scenario: "origin-approval-order",
          entries: [
            ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt }),
            {
              type: "emit_inbound",
              frame: {
                id: 0,
                method: "item/commandExecution/requestApproval",
                params: {
                  kind: "command",
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  itemId: "command-origin-approval",
                  startedAtMs: 1782622440000,
                  environmentId: "local",
                  command: "printf approved",
                  cwd: "/workspace",
                  commandActions: [{ type: "unknown", command: "printf approved" }],
                  availableDecisions: ["accept", "cancel"],
                },
              },
            },
            {
              type: "expect_outbound",
              frame: { id: 0, result: { decision: "accept" } },
            },
            {
              type: "emit_inbound",
              frame: {
                method: "turn/completed",
                params: {
                  threadId: nativeThreadId,
                  turn: makeCodexReplayTurn({ id: nativeTurnId, status: "completed" }),
                },
              },
            },
            { type: "runtime_exit", status: "success" },
          ],
        });
        const consumed: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
        const requestPublished = yield* Deferred.make<{
          readonly event: Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "runtime_request.updated" }
          >;
          readonly preceding: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>;
        }>();
        const harness = yield* makeCodexReplayHarness(transcript, (event) =>
          Effect.sync(() => {
            consumed.push(event);
            return [...consumed];
          }).pipe(
            Effect.flatMap((preceding) =>
              event.type === "runtime_request.updated"
                ? Deferred.succeed(requestPublished, { event, preceding })
                : Effect.void,
            ),
          ),
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("origin-approval-order"),
            text: prompt,
          }),
        );
        const published = yield* Deferred.await(requestPublished);
        const pending = published.event.runtimeRequest;
        const nodeEvent = published.preceding.find(
          (
            event,
          ): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "node.updated" }> =>
            event.type === "node.updated" && event.node.runtimeRequestId === pending.id,
        );
        const itemEvent = published.preceding.find(
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "turn_item.updated" }
          > =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "approval_request" &&
            event.turnItem.requestId === pending.id,
        );
        assert.equal(pending.status, "pending");
        assert.equal(nodeEvent?.node.status, "waiting");
        assert.equal(itemEvent?.turnItem.status, "waiting");
        yield* harness.runtime.respondToRuntimeRequest({
          requestId: pending.id,
          decision: "accept",
        });
        yield* harness.firstTerminal;
        assert.equal(harness.terminalEvents()[0]?.status, "completed");
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect("reads a bound native goal without starting turns or recovering sessions", () =>
    Effect.gen(function* () {
      const requests: Array<{ method: string; params: unknown }> = [];
      const nativeThreadId = "native-read-only-goal";
      const transcript = makeCodexReplayTranscript({
        scenario: "goal-read-only",
        entries: codexReplayPreamble({
          nativeThreadId,
          nativeTurnId: "unused",
          prompt: "unused",
        }).slice(0, 5),
      });
      const sessionScope = yield* Effect.acquireRelease(Scope.make(), (scope) =>
        Scope.close(scope, Exit.void),
      );
      const harness = yield* makeCodexReplayHarness(
        transcript,
        () => Effect.void,
        () => Effect.void,
        undefined,
        {},
        (method, params) =>
          Effect.sync(() => {
            requests.push({ method, params });
            return { goal: null };
          }),
      ).pipe(Effect.provideService(Scope.Scope, sessionScope));
      const readGoal = harness.runtime.readGoalState;
      assert.isDefined(readGoal);
      assert.deepEqual(yield* readGoal!(harness.providerThread), {
        nativeThreadId,
        state: "inactive",
        reasonCode: "goal_null",
      });
      assert.deepEqual(requests, [
        { method: "thread/goal/get", params: { threadId: nativeThreadId } },
      ]);
      assert.lengthOf(
        harness.events.filter(
          (e) => e.type === "turn.terminal" || e.type === "provider_turn.updated",
        ),
        0,
      );
      assert.lengthOf(harness.continuationRequests, 0);
      const mismatch = yield* readGoal!({
        ...harness.providerThread,
        providerSessionId: ProviderSessionId.make("session:other"),
      });
      assert.equal(mismatch.reasonCode, "instance_mismatch");
      assert.lengthOf(requests, 1);
      yield* Scope.close(sessionScope, Exit.void);
      assert.equal((yield* readGoal!(harness.providerThread)).reasonCode, "session_stopped");
      assert.lengthOf(requests, 1);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect.each([
    {
      label: "catalog",
      configured: undefined,
      explicit: undefined,
      expected: "high",
      reads: 1,
      foreign: false,
    },
    {
      label: "foreign",
      configured: undefined,
      explicit: undefined,
      expected: undefined,
      reads: 0,
      foreign: true,
    },
    { label: "configured", configured: "xhigh", explicit: undefined, expected: "xhigh", reads: 0 },
    { label: "explicit", configured: "xhigh", explicit: "medium", expected: "medium", reads: 0 },
  ])("uses $label effort only at native dispatch without changing requested options", (row) =>
    Effect.gen(function* () {
      let reads = 0;
      const nativeThreadId = `effort-${row.label}`;
      const nativeTurnId = `turn-${row.label}`;
      const entries = codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Continue" });
      const turn = entries.find(
        (entry) => entry.type === "expect_outbound" && entry.label === "turn/start",
      );
      assert.isDefined(turn);
      if (turn?.type === "expect_outbound") {
        const frame = turn.frame as { params: Record<string, unknown> };
        if (row.expected !== undefined) frame.params.effort = row.expected;
      }
      const harness = yield* makeCodexReplayHarness(
        makeCodexReplayTranscript({ scenario: nativeThreadId, entries }),
        undefined,
        undefined,
        undefined,
        {
          getModelCatalog: Effect.sync(() => {
            reads += 1;
            return [
              {
                slug: "gpt-5.4",
                name: "GPT",
                isCustom: false,
                capabilities: {
                  optionDescriptors: [
                    {
                      id: "reasoningEffort",
                      label: "Reasoning",
                      type: "select" as const,
                      currentValue: "high",
                      options: ["medium", "high", "xhigh"].map((id) => ({ id, label: id })),
                    },
                  ],
                },
              },
            ];
          }),
        },
      );
      const input = {
        ...makeCodexTestTurnInput({
          threadId: harness.threadId,
          providerThread: harness.providerThread,
          now: yield* DateTime.now,
          attemptId: RunAttemptId.make(`attempt-${row.label}`),
          text: "Continue",
        }),
        ...(row.configured === undefined ? {} : { configuredReasoningEffort: row.configured }),
      };
      if (row.explicit !== undefined)
        input.modelSelection = {
          ...input.modelSelection,
          options: [{ id: "reasoningEffort", value: row.explicit }],
        };
      if (row.foreign)
        input.modelSelection = {
          ...input.modelSelection,
          instanceId: ProviderInstanceId.make("codex-foreign"),
        };
      const originalSelection = input.modelSelection;
      yield* harness.runtime.startTurn(input);
      assert.equal(reads, row.reads);
      assert.strictEqual(input.modelSelection, originalSelection);
      assert.deepEqual(
        input.modelSelection.options,
        row.explicit === undefined ? undefined : [{ id: "reasoningEffort", value: row.explicit }],
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect("reads changed catalog defaults on each logical turn, including a model switch", () =>
    Effect.gen(function* () {
      const nativeThreadId = "current-effort-per-turn";
      const entries = codexReplayPreamble({
        nativeThreadId,
        nativeTurnId: "first",
        prompt: "First",
      });
      const first = entries.find(
        (entry) => entry.type === "expect_outbound" && entry.label === "turn/start",
      );
      if (first?.type === "expect_outbound")
        (first.frame as { params: Record<string, unknown> }).params.effort = "high";
      entries.push(
        {
          type: "emit_inbound",
          frame: {
            method: "turn/completed",
            params: {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "first", status: "completed" }),
            },
          },
        },
        {
          type: "expect_outbound",
          frame: {
            id: 4,
            method: "turn/start",
            params: {
              threadId: nativeThreadId,
              input: [{ type: "text", text: "Second" }],
              cwd: "/workspace",
              model: "gpt-6",
              effort: "xhigh",
              approvalPolicy: "never",
              approvalsReviewer: "user",
              sandboxPolicy: { type: "dangerFullAccess" },
              summary: "detailed",
            },
          },
        },
        {
          type: "emit_inbound",
          frame: {
            id: 4,
            result: { turn: makeCodexReplayTurn({ id: "second", status: "inProgress" }) },
          },
        },
        {
          type: "emit_inbound",
          frame: {
            method: "turn/started",
            params: {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "second", status: "inProgress" }),
            },
          },
        },
      );
      let effort = "high";
      let reads = 0;
      const harness = yield* makeCodexReplayHarness(
        makeCodexReplayTranscript({ scenario: nativeThreadId, entries }),
        undefined,
        undefined,
        undefined,
        {
          getModelCatalog: Effect.sync(() => {
            reads += 1;
            return ["gpt-5.4", "gpt-6"].map((slug) => ({
              slug,
              name: slug,
              isCustom: false,
              capabilities: {
                optionDescriptors: [
                  {
                    id: "reasoningEffort",
                    label: "Reasoning",
                    type: "select" as const,
                    currentValue: effort,
                    options: ["high", "xhigh"].map((id) => ({ id, label: id })),
                  },
                ],
              },
            }));
          }),
        },
      );
      const firstInput = makeCodexTestTurnInput({
        threadId: harness.threadId,
        providerThread: harness.providerThread,
        now: yield* DateTime.now,
        attemptId: RunAttemptId.make("first-effort"),
        text: "First",
      });
      yield* harness.runtime.startTurn(firstInput);
      yield* harness.firstTerminal;
      effort = "xhigh";
      const secondInput: ProviderAdapter.ProviderAdapterV2TurnInput = {
        ...makeCodexTestTurnInput({
          threadId: harness.threadId,
          providerThread: harness.providerThread,
          now: yield* DateTime.now,
          attemptId: RunAttemptId.make("second-effort"),
          text: "Second",
        }),
        modelSelection: { ...CODEX_TEST_MODEL_SELECTION, model: "gpt-6" },
      };
      yield* harness.runtime.startTurn(secondInput);
      assert.equal(reads, 2);
      assert.isUndefined(firstInput.modelSelection.options);
      assert.isUndefined(secondInput.modelSelection.options);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect.each(["legacy", "recorded"] as const)(
    "shared replay factory preserves interruption and terminal delivery with %s goal capability",
    (capability) =>
      Effect.gen(function* () {
        const nativeThreadId = `shared-goal-${capability}`;
        const nativeTurnId = `shared-turn-${capability}`;
        const goal = {
          createdAt: 1,
          objective: "Keep working",
          status: "active",
          threadId: nativeThreadId,
          timeUsedSeconds: 0,
          tokenBudget: null,
          tokensUsed: 0,
          updatedAt: 1,
        };
        const interruptId = capability === "recorded" ? 6 : 4;
        const transcript = makeCodexReplayTranscript({
          scenario: nativeThreadId,
          entries: [
            ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Keep working" }),
            ...(capability === "recorded"
              ? [
                  {
                    type: "expect_outbound" as const,
                    frame: {
                      id: 4,
                      method: "thread/goal/get",
                      params: { threadId: nativeThreadId },
                    },
                  },
                  { type: "emit_inbound" as const, frame: { id: 4, result: { goal } } },
                  {
                    type: "expect_outbound" as const,
                    frame: {
                      id: 5,
                      method: "thread/goal/set",
                      params: { threadId: nativeThreadId, status: "paused" },
                    },
                  },
                  {
                    type: "emit_inbound" as const,
                    frame: { id: 5, result: { goal: { ...goal, status: "paused" } } },
                  },
                ]
              : []),
            {
              type: "expect_outbound",
              frame: {
                id: interruptId,
                method: "turn/interrupt",
                params: { threadId: nativeThreadId, turnId: nativeTurnId },
              },
            },
            { type: "emit_inbound", frame: { id: interruptId, result: {} } },
            {
              type: "emit_inbound",
              frame: {
                method: "turn/completed",
                params: {
                  threadId: nativeThreadId,
                  turn: makeCodexReplayTurn({ id: nativeTurnId, status: "interrupted" }),
                },
              },
            },
          ],
        });
        const driver = yield* CodexReplay.makeReplayDriver(transcript);
        const harness = yield* makeCodexReplayHarness(transcript, undefined, undefined, undefined, {
          replayClientFactory: makeCodexReplayClientFactory({ transcript, driver }),
        });
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make(`shared-goal-${capability}`),
            text: "Keep working",
          }),
        );
        const providerTurnId = (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
          driver: CodexAdapterV2.CODEX_DRIVER_KIND,
          nativeTurnId,
        });
        yield* harness.runtime.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId,
        });
        yield* harness.firstTerminal;
        assert.equal(harness.terminalEvents()[0]?.status, "interrupted");
        assert.equal(harness.terminalEvents()[0]?.providerTurnId, providerTurnId);
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
        assert.isFalse(
          harness.events.some(
            (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
          ),
        );
        assert.deepEqual(yield* Ref.get(driver.state), {
          cursor: transcript.entries.length,
          failure: null,
        });
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect("shared legacy replay factory rejects an unrecorded goal mutation", () =>
    Effect.gen(function* () {
      const transcript = makeCodexReplayTranscript({
        scenario: "shared-unrecorded-goal-write",
        entries: codexReplayPreamble({
          nativeThreadId: "shared-unrecorded-goal-write",
          nativeTurnId: "shared-unrecorded-goal-turn",
          prompt: "Keep working",
        }),
      });
      const driver = yield* CodexReplay.makeReplayDriver(transcript);
      const factory = makeCodexReplayClientFactory({ transcript, driver });
      const clientReady = yield* Deferred.make<CodexClient.CodexAppServerClient["Service"]>();
      yield* makeCodexReplayHarness(transcript, undefined, undefined, undefined, {
        replayClientFactory: {
          open: (input) =>
            factory.open(input).pipe(Effect.tap((client) => Deferred.succeed(clientReady, client))),
        },
      });
      const client = yield* Deferred.await(clientReady);
      const result = yield* client.raw
        .request("thread/goal/set", {
          threadId: "shared-unrecorded-goal-write",
          status: "paused",
        })
        .pipe(Effect.exit);
      assert.equal(result._tag, "Failure");
      const state = yield* Ref.get(driver.state);
      assert.equal(state.failure?._tag, "CodexAppServerReplayFrameMismatchError");
      assert.equal(
        state.cursor,
        transcript.entries.findIndex(
          (entry) =>
            entry.type === "expect_outbound" &&
            Predicate.isObject(entry.frame) &&
            entry.frame.method === "turn/start",
        ),
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  const goalControlRequest = Schema.Struct({
    method: Schema.String,
    params: Schema.Struct({
      threadId: Schema.String,
      turnId: Schema.optionalKey(Schema.String),
      status: Schema.optionalKey(Schema.String),
    }),
  });
  const decodeGoalControlRequest = Schema.decodeUnknownSync(
    Schema.fromJsonString(goalControlRequest),
  );
  const decodeGoalStateJson = Schema.decodeEffect(
    Schema.fromJsonString(Schema.Struct({ status: Schema.String })),
  );
  const makeGoalProtocolHarness = (scenario: string) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const serverConfig = yield* Effect.acquireRelease(
        makeReplayServerConfig(scenario).pipe(Effect.orDie),
        (config) => fileSystem.remove(config.baseDir, { recursive: true }).pipe(Effect.orDie),
      );
      const threadId = ThreadId.make(`goal-protocol-${scenario}`);
      const goalStatePath = path.join(serverConfig.baseDir, "goal-state.json");
      const open = (
        name: string,
        script: Record<string, unknown>,
        savedThread?: OrchestrationV2ProviderThread,
      ) =>
        Effect.gen(function* () {
          const scriptPath = path.join(serverConfig.baseDir, `${name}.json`);
          yield* fileSystem.writeFileString(
            scriptPath,
            encodeUnknownJson({
              rootThreadId: codexCollabWire.rootThreadId,
              notifications: [],
              holdTurnOpen: true,
              recordControlRequests: true,
              goalStatePath,
              completeTurnOnInterrupt: true,
              ...script,
            }),
          );
          const sessionScope = yield* Effect.acquireRelease(Scope.make(), (scope) =>
            Scope.close(scope, Exit.void),
          );
          const clientReady = yield* Deferred.make<CodexClient.CodexAppServerClient["Service"]>();
          const rootInterruptRequested = yield* Deferred.make<void>();
          const goalGetRequested = yield* Deferred.make<void>();
          const goalSetRequested = yield* Deferred.make<void>();
          const requests: Array<{ method: string; params: unknown }> = [];
          const observe = (method: string, params: unknown) =>
            Effect.gen(function* () {
              requests.push({ method, params });
              if (method === "thread/goal/get")
                yield* Deferred.succeed(goalGetRequested, undefined);
              if (method === "thread/goal/set")
                yield* Deferred.succeed(goalSetRequested, undefined);
              if (
                method === "turn/interrupt" &&
                Predicate.isObject(params) &&
                params.threadId === codexCollabWire.rootThreadId
              ) {
                yield* Deferred.succeed(rootInterruptRequested, undefined);
              }
            });
          const clientFactory: CodexAdapterV2.CodexAppServerClientFactoryShape = {
            open: (openInput) =>
              Effect.gen(function* () {
                const handle = yield* spawner.spawn(
                  ChildProcess.make(
                    process.execPath,
                    [
                      NodeURL.fileURLToPath(
                        new URL(
                          "../../provider/testFixtures/codexCollabMockPeer.mjs",
                          import.meta.url,
                        ),
                      ),
                    ],
                    { env: { T3_CODEX_COLLAB_SCRIPT: scriptPath }, extendEnv: true },
                  ),
                );
                const context = yield* Layer.build(
                  CodexClient.layerChildProcess(
                    handle,
                    openInput.onTermination === undefined
                      ? {}
                      : { onTermination: openInput.onTermination },
                  ),
                );
                const client = yield* Effect.service(CodexClient.CodexAppServerClient).pipe(
                  Effect.provide(context),
                );
                yield* Deferred.succeed(clientReady, client);
                return {
                  ...client,
                  raw: {
                    ...client.raw,
                    request: (method, params) =>
                      observe(method, params).pipe(
                        Effect.andThen(client.raw.request(method, params)),
                      ),
                  },
                  request: (method, params) =>
                    observe(method, params).pipe(Effect.andThen(client.request(method, params))),
                } satisfies CodexClient.CodexAppServerClient["Service"];
              }).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterOpenSessionError({
                      driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                      providerSessionId: openInput.providerSessionId,
                      cause,
                    }),
                ),
              ),
          };
          const adapter = yield* CodexAdapterV2.makeCodexAdapterV2({
            instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
            settings: DEFAULT_CODEX_SETTINGS,
            environment: {},
            clientFactory,
            crypto: yield* Crypto.Crypto,
            fileSystem,
            idAllocator,
            serverConfig,
          });
          const runtime = yield* adapter
            .openSession({
              threadId,
              providerSessionId: ProviderSessionId.make(`goal-protocol-${scenario}-${name}`),
              modelSelection: CODEX_TEST_MODEL_SELECTION,
              runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
            })
            .pipe(Effect.provideService(Scope.Scope, sessionScope));
          const providerThread =
            savedThread === undefined
              ? yield* runtime.ensureThread({
                  threadId,
                  modelSelection: CODEX_TEST_MODEL_SELECTION,
                  runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
                })
              : yield* runtime.resumeThread({
                  threadId,
                  providerThread: savedThread,
                  modelSelection: CODEX_TEST_MODEL_SELECTION,
                  runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
                });
          const events: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
          yield* runtime.events.pipe(
            Stream.runForEach((event) =>
              Effect.sync(() => {
                events.push(event);
              }),
            ),
            Effect.forkScoped,
            Effect.provideService(Scope.Scope, sessionScope),
          );
          const start = (name: string) =>
            Effect.gen(function* () {
              yield* runtime.startTurn(
                makeCodexTestTurnInput({
                  threadId,
                  providerThread,
                  now: yield* DateTime.now,
                  attemptId: RunAttemptId.make(`goal-${scenario}-${name}`),
                  text: "Keep working",
                }),
              );
              yield* awaitUntil(
                () =>
                  events.some(
                    (event) =>
                      event.type === "provider_turn.updated" &&
                      event.threadId === threadId &&
                      event.providerTurn.status === "running",
                  ),
                "native root turn",
              );
              const event = events.find(
                (
                  event,
                ): event is Extract<
                  ProviderAdapter.ProviderAdapterV2Event,
                  { type: "provider_turn.updated" }
                > =>
                  event.type === "provider_turn.updated" &&
                  event.threadId === threadId &&
                  event.providerTurn.status === "running",
              );
              assert.isDefined(event);
              return event.providerTurn.id;
            });
          const controls = () =>
            fileSystem.readFileString(`${scriptPath}.control`).pipe(
              Effect.map((content) =>
                content
                  .trim()
                  .split("\n")
                  .filter(Boolean)
                  .map((line) => decodeGoalControlRequest(line)),
              ),
            );
          return {
            runtime,
            providerThread,
            events,
            requests,
            start,
            controls,
            client: yield* Deferred.await(clientReady),
            rootInterruptRequested: Deferred.await(rootInterruptRequested),
            goalGetRequested: Deferred.await(goalGetRequested),
            goalSetRequested: Deferred.await(goalSetRequested),
            close: Scope.close(sessionScope, Exit.void),
          };
        });
      return { open, fileSystem, goalStatePath, threadId };
    });

  it.effect("pauses the active goal before a user-facing interrupt", () =>
    Effect.gen(function* () {
      const harness = yield* makeGoalProtocolHarness("pause-before-stop");
      const session = yield* harness.open("first", { goalStatus: "active" });
      const providerTurnId = yield* session.start("first");
      yield* session.runtime.interruptTurn({
        providerThread: session.providerThread,
        providerTurnId,
      });
      assert.deepEqual(
        (yield* session.controls()).map((request) => request.method),
        ["thread/goal/get", "thread/goal/set", "turn/interrupt"],
      );
      assert.equal(
        (yield* harness.fileSystem
          .readFileString(harness.goalStatePath)
          .pipe(Effect.flatMap(decodeGoalStateJson))).status,
        "paused",
      );
      assert.isFalse(
        session.events.some(
          (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
        ),
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect("pauses an active goal and keeps it paused across a runtime restart", () =>
    Effect.gen(function* () {
      const harness = yield* makeGoalProtocolHarness("pause-persisted");
      const first = yield* harness.open("first", { goalStatus: "active", turnIds: ["goal-first"] });
      const firstTurn = yield* first.start("first");
      yield* first.runtime.interruptTurn({
        providerThread: first.providerThread,
        providerTurnId: firstTurn,
      });
      // A direct protocol interrupt is goal-neutral; it does not invoke Stop's goal control.
      yield* first.client.request("turn/interrupt", {
        threadId: codexCollabWire.rootThreadId,
        turnId: "restart-interrupt",
      });
      yield* first.close;
      const second = yield* harness.open(
        "second",
        { turnIds: ["goal-second"] },
        first.providerThread,
      );
      const secondTurn = yield* second.start("second");
      yield* second.runtime.interruptTurn({
        providerThread: second.providerThread,
        providerTurnId: secondTurn,
      });
      assert.deepEqual(
        [...(yield* first.controls()), ...(yield* second.controls())].map(
          (request) => request.method,
        ),
        [
          "thread/goal/get",
          "thread/goal/set",
          "turn/interrupt",
          "turn/interrupt",
          "thread/goal/get",
          "turn/interrupt",
        ],
      );
      assert.equal(
        (yield* harness.fileSystem
          .readFileString(harness.goalStatePath)
          .pipe(Effect.flatMap(decodeGoalStateJson))).status,
        "paused",
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect.each(["absent", "paused", "get-error", "get-hang", "set-error", "set-hang"] as const)(
    "keeps Stop interruptible when goal control errors or hangs (%s)",
    (mode) =>
      Effect.gen(function* () {
        const harness = yield* makeGoalProtocolHarness(`goal-control-${mode}`);
        const session = yield* harness.open("first", {
          ...(mode === "absent" ? {} : { goalStatus: mode === "paused" ? "paused" : "active" }),
          failGoalGet: mode === "get-error",
          hangGoalGet: mode === "get-hang",
          failGoalSet: mode === "set-error",
          hangGoalSet: mode === "set-hang",
        });
        const providerTurnId = yield* session.start("first");
        const stopping = yield* session.runtime
          .interruptTurn({ providerThread: session.providerThread, providerTurnId })
          .pipe(Effect.forkScoped);
        if (mode.endsWith("hang")) {
          yield* mode === "get-hang" ? session.goalGetRequested : session.goalSetRequested;
          yield* TestClock.adjust("1 second");
        }
        yield* Fiber.join(stopping);
        assert.deepEqual(
          (yield* session.controls()).map((request) => request.method),
          mode.startsWith("set-")
            ? ["thread/goal/get", "thread/goal/set", "turn/interrupt"]
            : ["thread/goal/get", "turn/interrupt"],
        );
        const warnings = session.events.filter(
          (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
        );
        assert.equal(warnings.length, mode === "absent" || mode === "paused" ? 0 : 1);
        assert.lengthOf(
          session.events.filter((event) => event.type === "turn.terminal"),
          1,
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  const assertBoundedRootStop = (childCount: number) =>
    Effect.gen(function* () {
      const harness = yield* makeGoalProtocolHarness(`root-ack-${childCount}`);
      const childIds = Array.from({ length: childCount }, (_, index) => `goal-stop-child-${index}`);
      const notifications = childIds.flatMap((childId, index) => [
        {
          method: "item/completed",
          params: {
            item: {
              type: "subAgentActivity",
              id: `goal-child-call-${index}`,
              kind: "started",
              agentThreadId: childId,
              agentPath: `/root/child-${index}`,
            },
            threadId: codexCollabWire.rootThreadId,
            turnId: "goal-stop-root",
            completedAtMs: 1782622441000,
          },
        },
        {
          method: "turn/started",
          params: {
            threadId: childId,
            turn: makeCodexReplayTurn({ id: `goal-child-turn-${index}`, status: "inProgress" }),
          },
        },
      ]);
      const session = yield* harness.open("first", {
        turnIds: ["goal-stop-root"],
        notifications,
        hangInterruptsFor: [codexCollabWire.rootThreadId, ...childIds],
        completeTurnOnInterrupt: false,
      });
      const providerTurnId = yield* session.start("first");
      yield* awaitUntil(
        () =>
          new Set(
            session.events.flatMap((event) =>
              event.type === "provider_turn.updated" &&
              event.threadId !== harness.threadId &&
              event.providerTurn.status === "running"
                ? [event.providerTurn.id]
                : [],
            ),
          ).size === childCount,
        "all live children",
      );
      const stopping = yield* session.runtime
        .interruptTurn({ providerThread: session.providerThread, providerTurnId })
        .pipe(Effect.flip, Effect.forkScoped);
      if (childCount > 0) {
        yield* awaitUntil(
          () =>
            session.requests.filter((request) => request.method === "turn/interrupt").length === 8,
          "first child batch",
        );
        for (let batch = 2; batch <= 4; batch++) {
          yield* TestClock.adjust("3 seconds");
          yield* awaitUntil(
            () =>
              session.requests.filter((request) => request.method === "turn/interrupt").length ===
              batch * 8,
            "next bounded child batch",
          );
        }
        assert.isFalse(
          session.requests.some(
            (request) =>
              request.method === "turn/interrupt" &&
              Predicate.isObject(request.params) &&
              request.params.threadId === codexCollabWire.rootThreadId,
          ),
        );
        yield* TestClock.adjust("1 second");
      }
      yield* session.rootInterruptRequested;
      yield* TestClock.adjust("3 seconds");
      const error = yield* Fiber.join(stopping);
      assert.equal(error._tag, "ProviderAdapterInterruptError");
      assert.nestedPropertyVal(error, "cause._tag", "CodexInterruptAcknowledgementTimeout");
      assert.match(error.cause instanceof Error ? error.cause.message : "", /may still be running/);
      // A read after the timeout proves the peer consumed every preceding write.
      yield* session.client.raw.request("thread/goal/get", {
        threadId: codexCollabWire.rootThreadId,
      });
      const interrupts = (yield* session.controls()).filter(
        (request) => request.method === "turn/interrupt",
      );
      assert.equal(interrupts.length, Math.min(childCount, 32) + 1);
      assert.sameMembers(
        interrupts.slice(0, -1).map((request) => request.params.threadId),
        childIds.slice(0, 32),
      );
      assert.deepEqual(interrupts.at(-1)?.params, {
        threadId: codexCollabWire.rootThreadId,
        turnId: "goal-stop-root",
      });
      assert.isFalse(interrupts.some((request) => request.params.threadId === childIds[32]));
      assert.lengthOf(
        session.events.filter((event) => event.type === "turn.terminal"),
        0,
      );
      const rootUpdates = session.events.filter(
        (
          event,
        ): event is Extract<
          ProviderAdapter.ProviderAdapterV2Event,
          { type: "provider_turn.updated" }
        > => event.type === "provider_turn.updated" && event.threadId === harness.threadId,
      );
      assert.equal(rootUpdates.at(-1)?.providerTurn.status, "running");
      yield* awaitUntil(
        () =>
          session.events.some(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.title === "Turn may still be running",
          ),
        "visible unknown acknowledgement",
      );
    });
  it.effect("reports a typed root timeout as failed Stop instead of success", () =>
    assertBoundedRootStop(0).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );
  it.effect.each([0, 33] as const)(
    "fails a hanging root interrupt after a bounded request with %i hanging children",
    (childCount) =>
      assertBoundedRootStop(childCount).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect("interrupts live children without interrupting the root", () =>
    Effect.gen(function* () {
      const harness = yield* makeGoalProtocolHarness("neutral-children");
      const session = yield* harness.open("first", { goalStatus: "active" });
      yield* session.start("first");
      yield* session.client.request("turn/interrupt", {
        threadId: "neutral-child",
        turnId: "neutral-child-turn",
      });
      assert.deepEqual(
        (yield* session.controls()).map((request) => ({
          method: request.method,
          ...request.params,
        })),
        [{ method: "turn/interrupt", threadId: "neutral-child", turnId: "neutral-child-turn" }],
      );
      assert.lengthOf(
        session.events.filter((event) => event.type === "turn.terminal"),
        0,
      );
      const goal = yield* session.client.raw.request("thread/goal/get", {
        threadId: codexCollabWire.rootThreadId,
      });
      assert.nestedPropertyVal(goal, "goal.status", "active");
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect("resolves an omitted root turn id after child interruption yields", () =>
    Effect.gen(function* () {
      const harness = yield* makeGoalProtocolHarness("neutral-root-selection");
      const session = yield* harness.open("first", {
        goalStatus: "active",
        turnIds: ["prior-root"],
        turnStartedOnInterrupt: { threadId: "yielding-child", turnId: "new-root" },
        expectedActiveTurnId: "new-root",
      });
      let nativeTurnId = "prior-root";
      yield* session.client.handleServerNotification("turn/started", (payload) =>
        Effect.sync(() => {
          if (payload.threadId === codexCollabWire.rootThreadId) nativeTurnId = payload.turn.id;
        }),
      );
      yield* session.start("first");
      // V2 user Stop requires an explicit owner. The old omitted-ID race remains
      // a goal-neutral protocol case, with the cursor read after child yielding.
      yield* session.client.request("turn/interrupt", {
        threadId: "yielding-child",
        turnId: "child-turn",
      });
      yield* awaitUntil(() => nativeTurnId === "new-root", "new native root after child yield");
      yield* session.client.request("turn/interrupt", {
        threadId: codexCollabWire.rootThreadId,
        turnId: nativeTurnId,
      });
      assert.deepEqual(
        (yield* session.controls()).map((request) => ({
          method: request.method,
          ...request.params,
        })),
        [
          { method: "turn/interrupt", threadId: "yielding-child", turnId: "child-turn" },
          { method: "turn/interrupt", threadId: codexCollabWire.rootThreadId, turnId: "new-root" },
        ],
      );
      assert.nestedPropertyVal(
        yield* session.client.raw.request("thread/goal/get", {
          threadId: codexCollabWire.rootThreadId,
        }),
        "goal.status",
        "active",
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect.each([
    "unsupported",
    "missing",
    "foreign-read",
    "foreign-ack",
    "active-ack",
    "malformed",
  ] as const)("warns visibly and never replays uncertain goal control (%s)", (mode) =>
    Effect.gen(function* () {
      const nativeThreadId = `goal-validation-${mode}`;
      const nativeTurnId = `goal-turn-${mode}`;
      const transcript = makeCodexReplayTranscript({
        scenario: nativeThreadId,
        entries: [
          ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Keep working" }),
          {
            type: "expect_outbound",
            frame: {
              id: 4,
              method: "turn/interrupt",
              params: { threadId: nativeThreadId, turnId: nativeTurnId },
            },
          },
          { type: "emit_inbound", frame: { id: 4, result: {} } },
          {
            type: "emit_inbound",
            frame: {
              method: "turn/completed",
              params: {
                threadId: nativeThreadId,
                turn: makeCodexReplayTurn({ id: nativeTurnId, status: "interrupted" }),
              },
            },
          },
        ],
      });
      const methods: Array<string> = [];
      const goal = {
        createdAt: 1,
        objective: "Keep working",
        status: "active",
        threadId: nativeThreadId,
        timeUsedSeconds: 0,
        tokenBudget: null,
        tokensUsed: 0,
        updatedAt: 1,
      };
      const harness = yield* makeCodexReplayHarness(
        transcript,
        () => Effect.void,
        (method) =>
          Effect.sync(() => {
            if (method === "turn/interrupt") methods.push(method);
          }),
        undefined,
        {},
        (method) =>
          Effect.suspend(() => {
            methods.push(method);
            if (mode === "unsupported")
              return Effect.fail(
                new CodexErrors.CodexAppServerRequestError({
                  code: -32601,
                  errorMessage: "Method not found",
                }),
              );
            if (mode === "missing") return Effect.succeed({});
            if (mode === "malformed") return Effect.succeed({ goal: { status: "active" } });
            if (method === "thread/goal/get")
              return Effect.succeed({
                goal: {
                  ...goal,
                  threadId: mode === "foreign-read" ? "other-native-thread" : nativeThreadId,
                },
              });
            return Effect.succeed({
              goal: {
                ...goal,
                status: mode === "active-ack" ? "active" : "paused",
                threadId: mode === "foreign-ack" ? "other-native-thread" : nativeThreadId,
              },
            });
          }),
      );
      yield* harness.runtime.startTurn(
        makeCodexTestTurnInput({
          threadId: harness.threadId,
          providerThread: harness.providerThread,
          now: yield* DateTime.now,
          attemptId: RunAttemptId.make(`goal-validation-${mode}`),
          text: "Keep working",
        }),
      );
      const providerTurnId = (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
        driver: CodexAdapterV2.CODEX_DRIVER_KIND,
        nativeTurnId,
      });
      yield* harness.runtime.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId,
      });
      yield* harness.firstTerminal;
      assert.deepEqual(
        methods,
        mode.endsWith("ack")
          ? ["thread/goal/get", "thread/goal/set", "turn/interrupt"]
          : ["thread/goal/get", "turn/interrupt"],
      );
      const warnings = harness.events.filter(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      assert.lengthOf(warnings, 1);
      assert.equal(
        warnings[0]?.type === "turn_item.updated" ? warnings[0].turnItem.title : undefined,
        "Goal may still be active",
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect("shares one second across goal read and pause without replaying an unknown write", () =>
    Effect.gen(function* () {
      const nativeThreadId = "combined-goal-deadline";
      const nativeTurnId = "combined-goal-turn";
      const goal = {
        createdAt: 1,
        objective: "Keep working",
        status: "active",
        threadId: nativeThreadId,
        timeUsedSeconds: 0,
        tokenBudget: null,
        tokensUsed: 0,
        updatedAt: 1,
      };
      const transcript = makeCodexReplayTranscript({
        scenario: nativeThreadId,
        entries: [
          ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Keep working" }),
          {
            type: "expect_outbound",
            frame: {
              id: 4,
              method: "turn/interrupt",
              params: { threadId: nativeThreadId, turnId: nativeTurnId },
            },
          },
          { type: "emit_inbound", frame: { id: 4, result: {} } },
          {
            type: "emit_inbound",
            frame: {
              method: "turn/completed",
              params: {
                threadId: nativeThreadId,
                turn: makeCodexReplayTurn({ id: nativeTurnId, status: "interrupted" }),
              },
            },
          },
        ],
      });
      const methods: Array<string> = [];
      const readStarted = yield* Deferred.make<void>();
      const writeStarted = yield* Deferred.make<void>();
      const harness = yield* makeCodexReplayHarness(
        transcript,
        () => Effect.void,
        (method) =>
          Effect.sync(() => {
            if (method === "turn/interrupt") methods.push(method);
          }),
        undefined,
        {},
        (method) =>
          Effect.gen(function* () {
            methods.push(method);
            if (method === "thread/goal/get") {
              yield* Deferred.succeed(readStarted, undefined);
              yield* Effect.sleep("600 millis");
              return { goal };
            }
            yield* Deferred.succeed(writeStarted, undefined);
            return yield* Effect.never;
          }),
      );
      yield* harness.runtime.startTurn(
        makeCodexTestTurnInput({
          threadId: harness.threadId,
          providerThread: harness.providerThread,
          now: yield* DateTime.now,
          attemptId: RunAttemptId.make("combined-goal-deadline"),
          text: "Keep working",
        }),
      );
      const providerTurnId = (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
        driver: CodexAdapterV2.CODEX_DRIVER_KIND,
        nativeTurnId,
      });
      const stopping = yield* harness.runtime
        .interruptTurn({ providerThread: harness.providerThread, providerTurnId })
        .pipe(Effect.forkScoped);
      yield* Deferred.await(readStarted);
      yield* TestClock.adjust("600 millis");
      yield* Deferred.await(writeStarted);
      yield* TestClock.adjust("399 millis");
      assert.deepEqual(methods, ["thread/goal/get", "thread/goal/set"]);
      yield* TestClock.adjust("1 milli");
      yield* Fiber.join(stopping);
      yield* harness.firstTerminal;
      assert.deepEqual(methods, ["thread/goal/get", "thread/goal/set", "turn/interrupt"]);
      assert.isTrue(
        harness.events.some(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.title === "Goal may still be active",
        ),
      );
      yield* TestClock.adjust("1 minute");
      assert.deepEqual(methods, ["thread/goal/get", "thread/goal/set", "turn/interrupt"]);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect("keeps an unknown root running until an authoritative late native completion", () =>
    Effect.gen(function* () {
      const nativeThreadId = "late-stop-ack-thread";
      const nativeTurnId = "late-stop-ack-turn";
      const transcript = makeCodexReplayTranscript({
        scenario: nativeThreadId,
        entries: [
          ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Keep working" }),
          {
            type: "expect_outbound",
            frame: {
              id: 4,
              method: "turn/interrupt",
              params: { threadId: nativeThreadId, turnId: nativeTurnId },
            },
          },
          { type: "emit_inbound", afterMs: 4000, frame: { id: 4, result: {} } },
          {
            type: "emit_inbound",
            frame: {
              method: "turn/completed",
              params: {
                threadId: nativeThreadId,
                turn: makeCodexReplayTurn({ id: nativeTurnId, status: "interrupted" }),
              },
            },
          },
        ],
      });
      const sent = yield* Deferred.make<void>();
      const harness = yield* makeCodexReplayHarness(
        transcript,
        () => Effect.void,
        (method) =>
          method === "turn/interrupt"
            ? Deferred.succeed(sent, undefined).pipe(Effect.asVoid)
            : Effect.void,
      );
      yield* harness.runtime.startTurn(
        makeCodexTestTurnInput({
          threadId: harness.threadId,
          providerThread: harness.providerThread,
          now: yield* DateTime.now,
          attemptId: RunAttemptId.make("late-stop-ack"),
          text: "Keep working",
        }),
      );
      const providerTurnId = (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
        driver: CodexAdapterV2.CODEX_DRIVER_KIND,
        nativeTurnId,
      });
      const stopping = yield* harness.runtime
        .interruptTurn({ providerThread: harness.providerThread, providerTurnId })
        .pipe(Effect.flip, Effect.forkScoped);
      yield* Deferred.await(sent);
      yield* TestClock.adjust("3 seconds");
      const error = yield* Fiber.join(stopping);
      assert.nestedPropertyVal(error, "cause._tag", "CodexInterruptAcknowledgementTimeout");
      assert.lengthOf(harness.terminalEvents(), 0);
      yield* TestClock.adjust("1 second");
      yield* harness.firstTerminal;
      assert.equal(harness.terminalEvents()[0]?.status, "interrupted");
      assert.lengthOf(harness.terminalEvents(), 1);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect.each(["session", "instance"] as const)(
    "rejects a stale Stop binding before goal or interrupt effects (%s)",
    (mode) =>
      Effect.gen(function* () {
        const nativeThreadId = `stale-stop-${mode}`;
        const nativeTurnId = `stale-stop-turn-${mode}`;
        const transcript = makeCodexReplayTranscript({
          scenario: nativeThreadId,
          entries: codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Keep working" }),
        });
        const controls: Array<string> = [];
        const harness = yield* makeCodexReplayHarness(
          transcript,
          () => Effect.void,
          (method) =>
            Effect.sync(() => {
              if (method === "turn/interrupt") controls.push(method);
            }),
          undefined,
          {},
          (method) =>
            Effect.sync(() => {
              controls.push(method);
              return { goal: null };
            }),
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make(`stale-stop-${mode}`),
            text: "Keep working",
          }),
        );
        const providerTurnId = (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
          driver: CodexAdapterV2.CODEX_DRIVER_KIND,
          nativeTurnId,
        });
        const providerThread = {
          ...harness.providerThread,
          ...(mode === "session"
            ? { providerSessionId: ProviderSessionId.make("other-session") }
            : { providerInstanceId: ProviderInstanceId.make("other-instance") }),
        };
        const error = yield* harness.runtime
          .interruptTurn({ providerThread, providerTurnId })
          .pipe(Effect.flip);
        assert.equal(error._tag, "ProviderAdapterInterruptError");
        assert.deepEqual(controls, []);
        assert.lengthOf(harness.terminalEvents(), 0);
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect.each(["inactive", "unsupported"] as const)(
    "keeps a missing recovery context from swallowing goal-pause uncertainty (%s)",
    (mode) =>
      Effect.gen(function* () {
        const nativeThreadId = `goal-no-context-${mode}`;
        const transcript = makeCodexReplayTranscript({
          scenario: nativeThreadId,
          entries: codexReplayPreamble({
            nativeThreadId,
            nativeTurnId: "unused",
            prompt: "unused",
          }).slice(0, 5),
        });
        const harness = yield* makeCodexReplayHarness(
          transcript,
          () => Effect.void,
          () => Effect.void,
          undefined,
          {},
          () =>
            mode === "inactive"
              ? Effect.succeed({ goal: null })
              : Effect.fail(
                  new CodexErrors.CodexAppServerRequestError({
                    code: -32601,
                    errorMessage: "Method not found",
                  }),
                ),
        );
        const stopping = harness.runtime.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId: ProviderTurnId.make("absent-provider-turn"),
          requestRuntimeRestart: true,
        });
        if (mode === "inactive") yield* stopping;
        else {
          const error = yield* stopping.pipe(Effect.flip);
          assert.nestedPropertyVal(error, "cause._tag", "ProviderAdapterProtocolError");
          assert.match(
            Predicate.isObject(error.cause) && typeof error.cause.detail === "string"
              ? error.cause.detail
              : "",
            /goal may remain active/,
          );
        }
        assert.lengthOf(harness.terminalEvents(), 0);
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  const capacityErrorEntry = (
    threadId: string,
    turnId: string,
    code = "serverOverloaded",
  ): CodexReplay.CodexAppServerReplayEntry => ({
    type: "emit_inbound",
    frame: {
      method: "error",
      params: {
        threadId,
        turnId,
        willRetry: false,
        error: {
          message: "Synthetic native capacity failure",
          codexErrorInfo: code,
          additionalDetails: null,
        },
      },
    },
  });
  const capacityCompletionEntry = (
    threadId: string,
    turnId: string,
    status: "completed" | "failed" = "failed",
  ): CodexReplay.CodexAppServerReplayEntry => ({
    type: "emit_inbound",
    frame: {
      method: "turn/completed",
      params: {
        threadId,
        turn: {
          ...makeCodexReplayTurn({ id: turnId, status }),
          error:
            status === "failed"
              ? {
                  message: "Synthetic native capacity failure",
                  codexErrorInfo: "serverOverloaded",
                  additionalDetails: null,
                }
              : null,
        },
      },
    },
  });
  const capacityPreamble = (threadId: string, turnId: string, prompt = "Work") =>
    codexReplayPreamble({ nativeThreadId: threadId, nativeTurnId: turnId, prompt });
  it.effect.each(["capacity", "completed", "silent"] as const)(
    "settles producer termination safely after %s without another start",
    (state) =>
      Effect.gen(function* () {
        const nativeThreadId = `termination-${state}`;
        let opened:
          | Parameters<CodexAdapterV2.CodexAppServerClientFactoryShape["open"]>[0]
          | undefined;
        const requests: Array<string> = [];
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: nativeThreadId,
            entries: [
              ...capacityPreamble(nativeThreadId, "termination-turn"),
              ...(state === "capacity"
                ? [
                    capacityErrorEntry(nativeThreadId, "termination-turn"),
                    capacityCompletionEntry(nativeThreadId, "termination-turn"),
                  ]
                : []),
              ...(state === "completed"
                ? [capacityCompletionEntry(nativeThreadId, "termination-turn", "completed")]
                : []),
            ],
          }),
          undefined,
          (method) =>
            Effect.sync(() => {
              requests.push(method);
            }),
          undefined,
          {
            onOpen: (input) =>
              Effect.sync(() => {
                opened = input;
              }),
          },
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("termination-state-attempt"),
            text: "Work",
          }),
        );
        if (state === "capacity") yield* awaitCapacityDelay(harness);
        if (state === "completed") yield* harness.firstTerminal;
        if (state === "silent") {
          yield* TestClock.adjust("1 hour");
          assert.lengthOf(harness.terminalEvents(), 0);
        }
        assert.isDefined(opened?.onTermination);
        yield* opened!.onTermination!(new CodexError.CodexAppServerProcessExitedError({ code: 7 }));
        yield* harness.firstTerminal;
        yield* harness.eventsEnded;
        yield* TestClock.adjust("1 minute");
        assert.lengthOf(harness.terminalEvents(), 1);
        assert.equal(
          harness.terminalEvents()[0]?.status,
          state === "completed" ? "completed" : "failed",
        );
        assert.equal(yield* harness.hasPendingBackgroundWork, false);
        assert.equal(requests.filter((method) => method === "turn/start").length, 1);
        assert.lengthOf(harness.continuationRequests, 0);
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect("fails both shared roots on producer termination and preserves another session", () =>
    Effect.gen(function* () {
      let opened:
        | Parameters<CodexAdapterV2.CodexAppServerClientFactoryShape["open"]>[0]
        | undefined;
      const secondPreamble = capacityPreamble("shared-death-second", "second-turn");
      const harness = yield* makeCodexReplayHarness(
        makeCodexReplayTranscript({
          scenario: "shared-producer-death",
          entries: [
            ...capacityPreamble("shared-death-first", "first-turn"),
            ...secondPreamble.slice(3, 5).map((entry) => withReplayRequestId(entry, 4)),
            ...secondPreamble.slice(5).map((entry) => withReplayRequestId(entry, 5)),
          ],
        }),
        undefined,
        undefined,
        undefined,
        {
          onOpen: (input) =>
            Effect.sync(() => {
              opened = input;
            }),
        },
      );
      const other = yield* makeCodexReplayHarness(
        makeCodexReplayTranscript({
          scenario: "unrelated-live-producer",
          entries: capacityPreamble("unrelated", "unrelated-turn"),
        }),
      );
      yield* harness.runtime.startTurn(
        makeCodexTestTurnInput({
          threadId: harness.threadId,
          providerThread: harness.providerThread,
          now: yield* DateTime.now,
          attemptId: RunAttemptId.make("shared-first-attempt"),
          text: "Work",
        }),
      );
      const secondThreadId = ThreadId.make("shared-second-app-thread");
      const second = yield* harness.runtime.ensureThread({
        threadId: secondThreadId,
        modelSelection: CODEX_TEST_MODEL_SELECTION,
        runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
      });
      yield* harness.runtime.startTurn(
        makeCodexTestTurnInput({
          threadId: secondThreadId,
          providerThread: second,
          now: yield* DateTime.now,
          text: "Work",
          attemptId: RunAttemptId.make("second-attempt"),
        }),
      );
      yield* other.runtime.startTurn(
        makeCodexTestTurnInput({
          threadId: other.threadId,
          providerThread: other.providerThread,
          now: yield* DateTime.now,
          attemptId: RunAttemptId.make("unrelated-live-attempt"),
          text: "Work",
        }),
      );
      yield* opened!.onTermination!(new CodexError.CodexAppServerProcessExitedError({ code: 9 }));
      yield* awaitUntil(() => harness.terminalEvents().length === 2, "shared failed terminals");
      assert.deepEqual(
        harness.terminalEvents().map((event) => event.status),
        ["failed", "failed"],
      );
      assert.lengthOf(new Set(harness.terminalEvents().map((event) => event.providerTurnId)), 2);
      assert.lengthOf(other.terminalEvents(), 0);
      assert.equal(yield* harness.hasPendingBackgroundWork, false);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect(
    "binds a 149-second start acknowledgement and admits its queued sibling without replay",
    () =>
      Effect.gen(function* () {
        const nativeThreadId = "delayed-start";
        const nativeTurnId = "delayed-original-turn";
        const replyScheduled = yield* Deferred.make<void>();
        const entries = capacityPreamble(nativeThreadId, nativeTurnId).map((entry) =>
          entry.type === "emit_inbound" && entry.label === "turn/start"
            ? { ...entry, afterMs: 149_000 }
            : entry,
        );
        entries.push(
          capacityCompletionEntry(nativeThreadId, nativeTurnId, "completed"),
          ...capacityPreamble("queued-sibling", "sibling-turn")
            .slice(3, 5)
            .map((entry) => withReplayRequestId(entry, 4)),
          ...capacityPreamble("queued-sibling", "sibling-turn")
            .slice(5)
            .map((entry) => withReplayRequestId(entry, 5)),
          capacityCompletionEntry("queued-sibling", "sibling-turn", "completed"),
        );
        const transcript = makeCodexReplayTranscript({ scenario: nativeThreadId, entries });
        let sends = 0;
        const driver = yield* CodexReplay.makeReplayDriver(transcript, {
          beforeEmitInbound: (entry) =>
            entry.label === "turn/start"
              ? Deferred.succeed(replyScheduled, undefined).pipe(Effect.asVoid)
              : Effect.void,
        });
        const factory = makeCodexReplayClientFactory({ transcript, driver });
        const harness = yield* makeCodexReplayHarness(transcript, undefined, undefined, undefined, {
          replayClientFactory: {
            open: (input) =>
              factory.open(input).pipe(
                Effect.map((client) => ({
                  ...client,
                  request: (method, params) =>
                    Effect.sync(() => {
                      if (method === "turn/start") sends++;
                    }).pipe(Effect.andThen(client.request(method, params))),
                })),
              ),
          },
        });
        const attemptId = RunAttemptId.make("delayed-original-attempt");
        const start = yield* harness.runtime
          .startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId,
              text: "Work",
            }),
          )
          .pipe(Effect.result, Effect.forkScoped);
        yield* Deferred.await(replyScheduled);
        const sibling = yield* harness.runtime
          .ensureThread({
            threadId: ThreadId.make("queued-sibling"),
            modelSelection: CODEX_TEST_MODEL_SELECTION,
            runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
          })
          .pipe(Effect.result, Effect.forkScoped);
        yield* TestClock.adjust("148 seconds");
        assert.isUndefined(
          start.pollUnsafe(),
          "the original attempt must still await its acknowledgement",
        );
        assert.isUndefined(
          sibling.pollUnsafe(),
          "the sibling must queue behind the original start",
        );
        assert.equal(sends, 1);
        assert.lengthOf(harness.terminalEvents(), 0);
        yield* TestClock.adjust("1 second");
        assert.equal((yield* Fiber.join(start))._tag, "Success");
        const siblingResult = yield* Fiber.join(sibling);
        assert.equal(siblingResult._tag, "Success");
        if (siblingResult._tag !== "Success") return;
        yield* harness.firstTerminal;
        const providerTurnId = (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
          driver: CodexAdapterV2.CODEX_DRIVER_KIND,
          nativeTurnId,
        });
        assert.equal(harness.terminalEvents()[0]?.providerTurnId, providerTurnId);
        assert.equal(harness.terminalEvents()[0]?.status, "completed");
        assert.isTrue(
          harness.events.some(
            (event) =>
              event.type === "provider_turn.updated" &&
              event.providerTurn.id === providerTurnId &&
              event.providerTurn.runAttemptId === attemptId,
          ),
        );
        assert.equal(sends, 1, "the original prompt is sent exactly once");
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: ThreadId.make("queued-sibling"),
            providerThread: siblingResult.success,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("queued-sibling-attempt"),
            text: "Work",
          }),
        );
        yield* awaitUntil(() => harness.terminalEvents().length === 2, "queued sibling completion");
        assert.deepEqual(
          harness.terminalEvents().map((event) => event.status),
          ["completed", "completed"],
        );
        assert.equal(sends, 2, "one native start per prompt");
        assert.deepEqual(yield* Ref.get(driver.state), {
          cursor: transcript.entries.length,
          failure: null,
        });
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect.each(["caller", "stop"] as const)(
    "ends a pending start on %s cancellation and reconciles the original late reply without replay",
    (cancellation) =>
      Effect.gen(function* () {
        const nativeThreadId = `cancel-pending-${cancellation}`;
        const nativeTurnId = "cancelled-native-turn";
        const sent = yield* Deferred.make<void>();
        let sends = 0;
        let complete: ((payload: unknown) => Effect.Effect<unknown>) | undefined;
        const entries = capacityPreamble(nativeThreadId, nativeTurnId).map((entry) =>
          entry.type === "emit_inbound" && entry.label === "turn/start"
            ? { ...entry, afterMs: 149_000 }
            : entry,
        );
        entries.push(
          ...capacityPreamble("after-cancel-sibling", "unused")
            .slice(3, 5)
            .map((entry) => withReplayRequestId(entry, 4)),
        );
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({ scenario: nativeThreadId, entries }),
          undefined,
          (method) =>
            method === "turn/start"
              ? Effect.sync(() => {
                  sends++;
                }).pipe(Effect.andThen(Deferred.succeed(sent, undefined)))
              : Effect.void,
          undefined,
          {
            onNotification: (method, handler) => {
              if (method === "turn/completed") complete = handler;
            },
          },
        );
        const start = yield* harness.runtime
          .startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make(`cancel-${cancellation}`),
              text: "Work",
            }),
          )
          .pipe(Effect.result, Effect.forkScoped);
        yield* Deferred.await(sent);
        yield* TestClock.adjust("148 seconds");
        assert.isUndefined(start.pollUnsafe());
        if (cancellation === "caller") yield* Fiber.interrupt(start);
        else
          yield* harness.runtime.interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId: ProviderTurnId.make("not-yet-acknowledged"),
            requestRuntimeRestart: true,
          });
        yield* TestClock.adjust(0);
        assert.isDefined(
          start.pollUnsafe(),
          "cancellation must end the caller before the start ceiling",
        );
        if (cancellation === "caller") assert.equal((yield* Fiber.await(start))._tag, "Failure");
        else assert.equal((yield* Fiber.join(start))._tag, "Failure");
        const sibling = () =>
          harness.runtime
            .ensureThread({
              threadId: ThreadId.make("after-cancel-sibling"),
              modelSelection: CODEX_TEST_MODEL_SELECTION,
              runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
            })
            .pipe(Effect.result);
        assert.equal(
          (yield* sibling())._tag,
          "Failure",
          "unconfirmed native work retains the hold",
        );
        yield* TestClock.adjust("1 second");
        assert.equal(
          (yield* sibling())._tag,
          "Failure",
          "acknowledgement alone cannot release the hold",
        );
        yield* complete!({
          threadId: nativeThreadId,
          turn: makeCodexReplayTurn({ id: nativeTurnId, status: "completed" }),
        });
        assert.equal((yield* sibling())._tag, "Success");
        assert.equal(sends, 1);
        assert.lengthOf(
          harness.terminalEvents(),
          0,
          "late completion must not revive the cancelled attempt",
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect("fails an unacknowledged start promptly when its producer terminates", () =>
    Effect.gen(function* () {
      let sends = 0;
      const harness = yield* makeCodexReplayHarness(
        makeCodexReplayTranscript({
          scenario: "pending-start-producer-exit",
          entries: [
            ...capacityPreamble("pending-start-producer-exit", "unused").slice(0, 6),
            { type: "runtime_exit", status: "success" },
          ],
        }),
        undefined,
        (method) =>
          Effect.sync(() => {
            if (method === "turn/start") sends++;
          }),
      );
      const start = yield* harness.runtime
        .startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("producer-exit-pending"),
            text: "Work",
          }),
        )
        .pipe(Effect.result, Effect.forkScoped);
      yield* harness.eventsEnded;
      yield* TestClock.adjust(0);
      assert.isDefined(
        start.pollUnsafe(),
        "producer exit must fail the pending RPC without waiting for the ceiling",
      );
      assert.equal((yield* Fiber.join(start))._tag, "Failure");
      assert.equal(sends, 1);
      assert.lengthOf(harness.terminalEvents(), 0, "no native turn was acknowledged");
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect.each(["ack-first", "completion-first"] as const)(
    "recovers sibling admission after a late start acknowledgement and completion (%s)",
    (order) =>
      Effect.gen(function* () {
        const nativeThreadId = `late-start-${order}`;
        const nativeTurnId = "late-native-turn";
        const sent = yield* Deferred.make<void>();
        let sends = 0;
        let complete: ((payload: unknown) => Effect.Effect<unknown>) | undefined;
        const entries = capacityPreamble(nativeThreadId, nativeTurnId).map((entry) =>
          entry.type === "emit_inbound" && entry.label === "turn/start"
            ? { ...entry, afterMs: CODEX_START_ACK_TIMEOUT_MS + 1_000 }
            : entry,
        );
        entries.push(
          ...capacityPreamble("new-sibling", "unused")
            .slice(3, 5)
            .map((entry) => withReplayRequestId(entry, 4)),
          {
            type: "expect_outbound",
            frame: {
              id: 5,
              method: "thread/resume",
              params: {
                threadId: "saved-sibling",
                excludeTurns: true,
                config: CodexAdapterV2.CODEX_THREAD_CONFIG,
              },
            },
          },
          {
            type: "emit_inbound",
            frame: {
              id: 5,
              result: { thread: { id: "saved-sibling", updatedAt: 1782622440 }, model: "gpt-5.4" },
            },
          },
        );
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({ scenario: nativeThreadId, entries }),
          undefined,
          (method) =>
            method === "turn/start"
              ? Effect.sync(() => {
                  sends++;
                }).pipe(Effect.andThen(Deferred.succeed(sent, undefined)))
              : Effect.void,
          undefined,
          {
            onNotification: (method, handler) => {
              if (method === "turn/completed") complete = handler;
            },
          },
        );
        const newSibling = () =>
          harness.runtime.ensureThread({
            threadId: ThreadId.make("new-sibling"),
            modelSelection: CODEX_TEST_MODEL_SELECTION,
            runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
          });
        const savedSibling = {
          ...harness.providerThread,
          id: ProviderThreadId.make("saved-sibling"),
          appThreadId: ThreadId.make("saved-sibling"),
          nativeThreadRef: {
            driver: CodexAdapterV2.CODEX_DRIVER_KIND,
            nativeId: "saved-sibling",
            strength: "strong" as const,
          },
        };
        const resumeSibling = () => harness.runtime.resumeThread({ providerThread: savedSibling });
        const start = yield* harness.runtime
          .startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make("timed-out-attempt"),
              text: "Work",
            }),
          )
          .pipe(Effect.result, Effect.forkScoped);
        yield* Deferred.await(sent);
        yield* TestClock.adjust(CODEX_START_ACK_TIMEOUT_MS);
        assert.equal((yield* Fiber.join(start))._tag, "Failure");
        assert.equal((yield* newSibling().pipe(Effect.result))._tag, "Failure");
        assert.equal((yield* resumeSibling().pipe(Effect.result))._tag, "Failure");
        const notifyComplete = (threadId: string, turnId: string) =>
          complete!({
            threadId,
            turn: makeCodexReplayTurn({ id: turnId, status: "completed" }),
          });
        yield* notifyComplete("wrong-conversation", nativeTurnId);
        yield* notifyComplete(nativeThreadId, "wrong-turn");
        if (order === "completion-first") yield* notifyComplete(nativeThreadId, nativeTurnId);
        assert.equal((yield* newSibling().pipe(Effect.result))._tag, "Failure");
        yield* TestClock.adjust("1 second");
        if (order === "ack-first") {
          assert.equal((yield* newSibling().pipe(Effect.result))._tag, "Failure");
          yield* notifyComplete(nativeThreadId, nativeTurnId);
        }
        const opened = yield* newSibling().pipe(Effect.result);
        assert.equal(
          opened._tag,
          "Success",
          "matching late evidence must restore new-conversation admission",
        );
        const resumed = yield* resumeSibling().pipe(Effect.result);
        assert.equal(
          resumed._tag,
          "Success",
          "matching late evidence must restore saved-conversation admission",
        );
        assert.equal(sends, 1, "the timed-out prompt is never replayed");
        assert.lengthOf(
          harness.terminalEvents(),
          0,
          "the failed start is not resurrected as a provider result",
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  const usageBreakdown = (input: number, cached: number, output: number) => ({
    totalTokens: input + output,
    inputTokens: input,
    cachedInputTokens: cached,
    outputTokens: output,
    reasoningOutputTokens: 0,
  });
  const usageUpdateEntry = (
    threadId: string,
    turnId: string,
    total: ReturnType<typeof usageBreakdown>,
    last = total,
  ): CodexReplay.CodexAppServerReplayEntry => ({
    type: "emit_inbound",
    label: "usage",
    frame: {
      method: "thread/tokenUsage/updated",
      params: { threadId, turnId, tokenUsage: { total, last, modelContextWindow: 200_000 } },
    },
  });
  const completedTurnEntry = (
    threadId: string,
    turnId: string,
  ): CodexReplay.CodexAppServerReplayEntry => ({
    type: "emit_inbound",
    frame: {
      method: "turn/completed",
      params: { threadId, turn: makeCodexReplayTurn({ id: turnId, status: "completed" }) },
    },
  });
  const contextUsageReports = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) =>
    events.flatMap((event) =>
      event.type === "provider_turn.updated" && event.providerTurn.tokenUsage !== undefined
        ? [event.providerTurn]
        : [],
    );
  const causeTags = (error: unknown) => {
    const tags: Array<string> = [];
    let current = error;
    while (Predicate.hasProperty(current, "cause") && tags.length < 8) {
      current = current.cause;
      tags.push(Predicate.hasProperty(current, "_tag") ? String(current._tag) : typeof current);
    }
    return tags;
  };

  // A resumed Codex conversation can replay usage for a turn that never becomes active again.
  // One stdout reader serves every thread of the session, so that notification must not hold
  // back the reply a pending start is waiting for.
  it.effect.each(["sibling", "same-thread"] as const)(
    "keeps a pending start readable behind replayed usage for a resumed thread (%s)",
    (target) =>
      Effect.gen(function* () {
        const activeThreadId = `replayed-usage-${target}`;
        const savedThreadId = `replayed-usage-saved-${target}`;
        const startThreadId = target === "sibling" ? activeThreadId : savedThreadId;
        const nativeTurnId = "fresh-native-turn";
        const freshTurn = codexReplayPreamble({
          nativeThreadId: startThreadId,
          nativeTurnId,
          prompt: "Work",
        });
        const transcript = makeCodexReplayTranscript({
          scenario: activeThreadId,
          entries: [
            ...codexReplayPreamble({
              nativeThreadId: activeThreadId,
              nativeTurnId,
              prompt: "Work",
            }).slice(0, 5),
            {
              type: "expect_outbound",
              frame: {
                id: 3,
                method: "thread/resume",
                params: {
                  threadId: savedThreadId,
                  excludeTurns: true,
                  config: CodexAdapterV2.CODEX_THREAD_CONFIG,
                },
              },
            },
            {
              type: "emit_inbound",
              frame: {
                id: 3,
                result: { thread: { id: savedThreadId, updatedAt: 1782622440 }, model: "gpt-5.4" },
              },
            },
            ...freshTurn.slice(5, 6).map((entry) => withReplayRequestId(entry, 4)),
            usageUpdateEntry(
              savedThreadId,
              "historical-native-turn",
              usageBreakdown(1000, 400, 200),
            ),
            // Delivering the reply on a clock wake lets the test clock settle everything it
            // causes before the assertion deadline, provided the reader is free.
            ...freshTurn
              .slice(6, 7)
              .map((entry) => withReplayRequestId(entry, 4))
              .map((entry) => (entry.type === "emit_inbound" ? { ...entry, afterMs: 1 } : entry)),
            ...freshTurn.slice(7),
            target === "sibling"
              ? usageUpdateEntry(startThreadId, nativeTurnId, usageBreakdown(30, 10, 6))
              : // The thread-wide total includes the replayed history; only the difference is billed.
                usageUpdateEntry(
                  startThreadId,
                  nativeTurnId,
                  usageBreakdown(1030, 410, 206),
                  usageBreakdown(20, 5, 4),
                ),
            completedTurnEntry(startThreadId, nativeTurnId),
          ],
        });
        const replyScheduled = yield* Deferred.make<void>();
        const driver = yield* CodexReplay.makeReplayDriver(transcript, {
          beforeEmitInbound: (entry) =>
            entry.label === "turn/start"
              ? Deferred.succeed(replyScheduled, undefined).pipe(Effect.asVoid)
              : Effect.void,
        });
        const harness = yield* makeCodexReplayHarness(transcript, undefined, undefined, undefined, {
          replayClientFactory: makeCodexReplayClientFactory({ transcript, driver }),
        });
        const savedThread = {
          ...harness.providerThread,
          id: ProviderThreadId.make(savedThreadId),
          appThreadId: ThreadId.make(savedThreadId),
          nativeThreadRef: {
            driver: CodexAdapterV2.CODEX_DRIVER_KIND,
            nativeId: savedThreadId,
            strength: "strong" as const,
          },
        };
        yield* harness.runtime.resumeThread({ providerThread: savedThread });
        const startThread = target === "sibling" ? harness.providerThread : savedThread;
        const attemptId = RunAttemptId.make(`replayed-usage-${target}-attempt`);
        const start = yield* harness.runtime
          .startTurn(
            makeCodexTestTurnInput({
              threadId: target === "sibling" ? harness.threadId : savedThread.appThreadId,
              providerThread: startThread,
              now: yield* DateTime.now,
              attemptId,
              text: "Work",
            }),
          )
          .pipe(Effect.result, Effect.forkScoped);
        yield* Deferred.await(replyScheduled);
        yield* TestClock.adjust("30 seconds");
        const started = yield* Fiber.join(start);
        assert.equal(
          started._tag,
          "Success",
          started._tag === "Failure"
            ? `the queued start reply was not read before the timeout: ${started.failure.message} (causes ${causeTags(started.failure).join(" > ")})`
            : undefined,
        );
        yield* harness.firstTerminal;
        assert.deepEqual(yield* Ref.get(driver.state), {
          cursor: transcript.entries.length,
          failure: null,
        });
        const providerTurnId = (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
          driver: CodexAdapterV2.CODEX_DRIVER_KIND,
          nativeTurnId,
        });
        assert.deepEqual(
          contextUsageReports(harness.events).map((turn) => ({
            id: turn.id,
            usedTokens: turn.tokenUsage?.usedTokens,
          })),
          [{ id: providerTurnId, usedTokens: target === "sibling" ? 36 : 24 }],
          "only the fresh turn reports context usage",
        );
        const completed = harness.events.find(
          (event) =>
            event.type === "provider_turn.updated" &&
            event.providerTurn.id === providerTurnId &&
            event.providerTurn.status === "completed",
        );
        assert.deepEqual(
          completed?.type === "provider_turn.updated"
            ? completed.providerTurn.turnTokenUsage
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
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect("reports context usage that directly follows a start acknowledgement", () =>
    Effect.gen(function* () {
      const nativeThreadId = "early-context-usage";
      const nativeTurnId = "early-context-usage-turn";
      // No item notification separates this usage from the start reply, so nothing else
      // waits for the new turn before the usage is handled.
      const transcript = makeCodexReplayTranscript({
        scenario: nativeThreadId,
        entries: [
          ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Work" }),
          usageUpdateEntry(nativeThreadId, nativeTurnId, usageBreakdown(30, 10, 6)),
          completedTurnEntry(nativeThreadId, nativeTurnId),
        ],
      });
      const harness = yield* makeCodexReplayHarness(transcript);
      const attemptId = RunAttemptId.make("early-context-usage-attempt");
      yield* harness.runtime.startTurn(
        makeCodexTestTurnInput({
          threadId: harness.threadId,
          providerThread: harness.providerThread,
          now: yield* DateTime.now,
          attemptId,
          text: "Work",
        }),
      );
      yield* harness.firstTerminal;
      const providerTurnId = (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
        driver: CodexAdapterV2.CODEX_DRIVER_KIND,
        nativeTurnId,
      });
      assert.deepEqual(
        contextUsageReports(harness.events).map((turn) => ({
          id: turn.id,
          runAttemptId: turn.runAttemptId,
          status: turn.status,
          usedTokens: turn.tokenUsage?.usedTokens,
          maxTokens: turn.tokenUsage?.maxTokens,
        })),
        [
          {
            id: providerTurnId,
            runAttemptId: attemptId,
            status: "running",
            usedTokens: 36,
            maxTokens: 200_000,
          },
        ],
      );
      const turnUpdateIndex = (predicate: (turn: OrchestrationV2ProviderTurn) => boolean) =>
        harness.events.findIndex(
          (event) =>
            event.type === "provider_turn.updated" &&
            event.providerTurn.id === providerTurnId &&
            predicate(event.providerTurn),
        );
      assert.isBelow(
        turnUpdateIndex((turn) => turn.tokenUsage !== undefined),
        turnUpdateIndex((turn) => turn.status === "completed"),
        "a running usage report must not follow the completion",
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  const awaitCapacityDelay = (
    harness: { readonly events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event> },
    ordinal = 1,
  ) =>
    awaitUntil(
      () =>
        harness.events.some(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "error" &&
            event.turnItem.status === "running" &&
            event.turnItem.retry?.attempt === ordinal,
        ),
      `capacity retry ${ordinal}`,
    );

  it.effect(
    "keeps twelve promptless capacity retries frozen and completes the original logical turn once",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const threadId = "native-capacity-exhausted";
          const entries: Array<CodexReplay.CodexAppServerReplayEntry> = capacityPreamble(
            threadId,
            "capacity-0",
          ).map((entry) =>
            entry.type === "expect_outbound" &&
            Predicate.isObject(entry.frame) &&
            entry.frame.method === "turn/start"
              ? {
                  ...entry,
                  frame: {
                    ...entry.frame,
                    params: {
                      ...(entry.frame.params as Record<string, unknown>),
                      effort: "low",
                      serviceTier: "priority",
                    },
                  },
                }
              : entry,
          );
          entries.push(
            capacityErrorEntry(threadId, "capacity-0"),
            capacityCompletionEntry(threadId, "capacity-0"),
          );
          for (let retry = 1; retry <= 12; retry++) {
            entries.push(
              ...capacityPreamble(threadId, `capacity-${retry}`)
                .slice(5, 7)
                .map((entry) =>
                  (entry.type === "expect_outbound" || entry.type === "emit_inbound") &&
                  Predicate.isObject(entry.frame)
                    ? {
                        ...entry,
                        frame: {
                          ...entry.frame,
                          id: retry + 3,
                          ...(entry.frame.method === "turn/start"
                            ? {
                                params: {
                                  ...(entry.frame.params as Record<string, unknown>),
                                  input: [],
                                  effort: "low",
                                  serviceTier: "priority",
                                },
                              }
                            : {}),
                        },
                      }
                    : entry,
                ),
            );
            entries.push(
              capacityErrorEntry(threadId, `capacity-${retry}`),
              capacityCompletionEntry(threadId, `capacity-${retry}`),
            );
          }
          let sends = 0;
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({ scenario: "capacity-twelve-frozen", entries }),
            undefined,
            (method) =>
              Effect.sync(() => {
                if (method === "turn/start") sends++;
              }),
          );
          const modelSelection = {
            ...CODEX_TEST_MODEL_SELECTION,
            options: [
              { id: "reasoningEffort", value: "low" },
              { id: "serviceTier", value: "priority" },
            ],
          };
          yield* harness.runtime.startTurn({
            ...makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make("capacity-frozen"),
              text: "Work",
            }),
            modelSelection,
          });
          for (let retry = 1; retry <= 12; retry++) {
            yield* awaitUntil(
              () =>
                harness.terminalEvents().length > 0 ||
                harness.events.some(
                  (event) =>
                    event.type === "turn_item.updated" &&
                    event.turnItem.type === "error" &&
                    event.turnItem.status === "running" &&
                    event.turnItem.retry?.attempt === retry,
                ),
              `capacity retry ${retry} or logical terminal`,
            );
            assert.lengthOf(
              harness.terminalEvents(),
              0,
              "A confirmed overload must keep its logical turn open for the scheduled retry",
            );
            modelSelection.options = [
              { id: "reasoningEffort", value: "high" },
              { id: "serviceTier", value: "flex" },
            ];
            yield* TestClock.adjust("4 seconds");
            assert.equal(sends, retry);
            yield* TestClock.adjust("1 second");
            yield* awaitUntil(() => sends === retry + 1, "promptless capacity send");
          }
          yield* harness.firstTerminal;
          assert.lengthOf(harness.terminalEvents(), 1);
          assert.equal(harness.terminalEvents()[0]!.status, "failed");
          assert.equal(
            harness.terminalEvents()[0]!.providerTurnId,
            harness.events.find(
              (
                event,
              ): event is Extract<
                ProviderAdapter.ProviderAdapterV2Event,
                { type: "provider_turn.updated" }
              > => event.type === "provider_turn.updated",
            )!.providerTurn.id,
          );
          const nativeAttempts = new Set(
            harness.events.flatMap((event) =>
              event.type === "provider_turn.updated"
                ? [event.providerTurn.nativeTurnRef?.nativeId]
                : [],
            ),
          );
          assert.deepEqual(
            [...nativeAttempts],
            Array.from({ length: 13 }, (_, index) => `capacity-${index}`),
          );
          yield* TestClock.adjust("1 minute");
          assert.equal(sends, 13);
        }),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect.each(["early-success", "unknown-start", "stop", "unload"] as const)(
    "fences capacity recovery for %s",
    (outcome) =>
      Effect.scoped(
        Effect.gen(function* () {
          const threadId = `native-capacity-${outcome}`;
          const entries: Array<CodexReplay.CodexAppServerReplayEntry> = [
            ...capacityPreamble(threadId, "original-capacity-turn"),
            capacityErrorEntry(threadId, "original-capacity-turn"),
            capacityCompletionEntry(threadId, "original-capacity-turn"),
          ];
          if (outcome === "early-success" || outcome === "unknown-start") {
            entries.push({
              type: "expect_outbound",
              frame: {
                id: 4,
                method: "turn/start",
                params: {
                  threadId,
                  input: [],
                  cwd: "/workspace",
                  model: "gpt-5.4",
                  approvalPolicy: "never",
                  approvalsReviewer: "user",
                  sandboxPolicy: { type: "dangerFullAccess" },
                  summary: "detailed",
                },
              },
            });
            if (outcome === "early-success")
              entries.push(
                capacityCompletionEntry(threadId, "unrelated-early-turn", "completed"),
                capacityCompletionEntry(threadId, "matching-early-turn", "completed"),
                {
                  type: "emit_inbound",
                  frame: {
                    id: 4,
                    result: {
                      turn: makeCodexReplayTurn({
                        id: "matching-early-turn",
                        status: "inProgress",
                      }),
                    },
                  },
                },
              );
            else
              entries.push({
                type: "runtime_exit",
                status: "error",
                error: "Synthetic lost retry reply",
              });
          } else if (outcome === "unload")
            entries.push(
              {
                type: "expect_outbound",
                frame: { id: 4, method: "thread/unsubscribe", params: { threadId } },
              },
              { type: "emit_inbound", frame: { id: 4, result: { status: "unsubscribed" } } },
            );
          let sends = 0;
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({ scenario: `capacity-${outcome}`, entries }),
            undefined,
            (method) =>
              Effect.sync(() => {
                if (method === "turn/start") sends++;
              }),
          );
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make(`capacity-${outcome}`),
              text: "Work",
            }),
          );
          yield* awaitCapacityDelay(harness);
          const original = harness.events.find(
            (
              event,
            ): event is Extract<
              ProviderAdapter.ProviderAdapterV2Event,
              { type: "provider_turn.updated" }
            > => event.type === "provider_turn.updated",
          )!.providerTurn.id;
          if (outcome === "stop")
            yield* harness.runtime.interruptTurn({
              providerThread: harness.providerThread,
              providerTurnId: original,
              requestRuntimeRestart: true,
            });
          if (outcome === "unload")
            yield* harness.runtime.unloadThread!({ providerThread: harness.providerThread });
          if (outcome !== "stop") yield* TestClock.adjust("10 seconds");
          yield* harness.firstTerminal;
          assert.lengthOf(harness.terminalEvents(), 1);
          const terminal = harness.terminalEvents()[0]!;
          assert.equal(terminal.providerTurnId, original);
          assert.equal(
            terminal.status,
            outcome === "early-success"
              ? "completed"
              : outcome === "unknown-start"
                ? "failed"
                : "interrupted",
          );
          if (outcome === "unknown-start") {
            if (terminal.status !== "failed")
              return assert.fail("Expected an unknown logical failure");
            assert.equal(terminal.failure.class, "unknown");
            assert.equal(terminal.threadDisposition, "broken");
            assert.equal(
              (yield* harness.runtime
                .startTurn(
                  makeCodexTestTurnInput({
                    threadId: harness.threadId,
                    providerThread: harness.providerThread,
                    now: yield* DateTime.now,
                    attemptId: RunAttemptId.make("after-unknown"),
                    text: "Do not duplicate",
                  }),
                )
                .pipe(Effect.result))._tag,
              "Failure",
            );
          }
          assert.isFalse(
            harness.events.some(
              (event) =>
                event.type === "provider_turn.updated" &&
                event.providerTurn.nativeTurnRef?.nativeId === "unrelated-early-turn",
            ),
          );
          yield* TestClock.adjust("1 minute");
          assert.equal(sends, outcome === "early-success" || outcome === "unknown-start" ? 2 : 1);
        }),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect("preserves queued native follow-ups while superseding capacity recovery", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const threadId = "native-capacity-queued";
        const followup = capacityPreamble(threadId, "queued-follow-up", "New input")
          .slice(5, 7)
          .map((entry) =>
            (entry.type === "expect_outbound" || entry.type === "emit_inbound") &&
            Predicate.isObject(entry.frame)
              ? { ...entry, frame: { ...entry.frame, id: 4 } }
              : entry,
          );
        let sends = 0;
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "capacity-queued-follow-up",
            entries: [
              ...capacityPreamble(threadId, "queued-original"),
              ...followup,
              capacityErrorEntry(threadId, "queued-original"),
              capacityCompletionEntry(threadId, "queued-original"),
              capacityCompletionEntry(threadId, "queued-follow-up", "completed"),
            ],
          }),
          undefined,
          (method) =>
            Effect.sync(() => {
              if (method === "turn/start") sends++;
            }),
        );
        for (const [attempt, text] of [
          ["first", "Work"],
          ["second", "New input"],
        ]) {
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make(`queued-${attempt}`),
              text: text!,
            }),
          );
        }
        yield* awaitUntil(
          () => harness.terminalEvents().length === 2,
          "queued logical completions",
        );
        assert.deepEqual(
          harness.terminalEvents().map((event) => event.status),
          ["failed", "completed"],
        );
        yield* TestClock.adjust("1 minute");
        assert.equal(sends, 2);
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  const capacityTurnExchange = (
    threadId: string,
    nativeTurnId: string,
    id: number,
    params: Record<string, unknown> = {},
    prompt = "Work",
  ) =>
    capacityPreamble(threadId, nativeTurnId, prompt)
      .slice(5, 7)
      .map((entry) =>
        (entry.type === "expect_outbound" || entry.type === "emit_inbound") &&
        Predicate.isObject(entry.frame)
          ? {
              ...entry,
              frame: {
                ...entry.frame,
                id,
                ...(entry.frame.method === "turn/start"
                  ? { params: { ...(entry.frame.params as Record<string, unknown>), ...params } }
                  : {}),
              },
            }
          : entry,
      );

  it.effect("retries a queued capacity follow-up with its own settings and logical turn", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const threadId = "native-capacity-follow-up-settings";
        let sends = 0;
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "capacity-follow-up-settings",
            entries: [
              ...capacityPreamble(threadId, "follow-up-first"),
              ...capacityTurnExchange(
                threadId,
                "follow-up-second",
                4,
                { effort: "high", serviceTier: "priority" },
                "Queued work",
              ),
              capacityCompletionEntry(threadId, "follow-up-first", "completed"),
              capacityErrorEntry(threadId, "follow-up-second"),
              capacityCompletionEntry(threadId, "follow-up-second"),
              ...capacityTurnExchange(threadId, "follow-up-retry", 5, {
                input: [],
                effort: "high",
                serviceTier: "priority",
              }),
              capacityCompletionEntry(threadId, "follow-up-retry", "completed"),
            ],
          }),
          undefined,
          (method) =>
            Effect.sync(() => {
              if (method === "turn/start") sends++;
            }),
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("capacity-follow-first"),
            text: "Work",
          }),
        );
        yield* harness.runtime.startTurn({
          ...makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("capacity-follow-second"),
            text: "Queued work",
          }),
          modelSelection: {
            ...CODEX_TEST_MODEL_SELECTION,
            options: [
              { id: "reasoningEffort", value: "high" },
              { id: "serviceTier", value: "priority" },
            ],
          },
        });
        yield* awaitCapacityDelay(harness);
        const second = harness.events.find(
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "provider_turn.updated" }
          > =>
            event.type === "provider_turn.updated" &&
            event.providerTurn.nativeTurnRef?.nativeId === "follow-up-second",
        )!.providerTurn.id;
        yield* TestClock.adjust("10 seconds");
        yield* awaitUntil(
          () => harness.terminalEvents().length === 2,
          "queued capacity logical terminals",
        );
        assert.equal(harness.terminalEvents()[1]!.providerTurnId, second);
        assert.deepEqual(
          harness.terminalEvents().map((event) => event.status),
          ["completed", "completed"],
        );
        yield* TestClock.adjust("1 minute");
        assert.equal(sends, 3);
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect("finishes delayed capacity recovery when newer owner input supersedes it", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const threadId = "native-capacity-new-input";
        let sends = 0;
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "capacity-new-input",
            entries: [
              ...capacityPreamble(threadId, "superseded-original"),
              capacityErrorEntry(threadId, "superseded-original"),
              capacityCompletionEntry(threadId, "superseded-original"),
              ...capacityTurnExchange(threadId, "new-owner-turn", 4, {}, "New input"),
              capacityCompletionEntry(threadId, "new-owner-turn", "completed"),
            ],
          }),
          undefined,
          (method) =>
            Effect.sync(() => {
              if (method === "turn/start") sends++;
            }),
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("capacity-superseded"),
            text: "Work",
          }),
        );
        yield* awaitCapacityDelay(harness);
        const original = harness.events.find(
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "provider_turn.updated" }
          > => event.type === "provider_turn.updated",
        )!.providerTurn.id;
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("capacity-new-owner"),
            text: "New input",
          }),
        );
        yield* awaitUntil(
          () => harness.terminalEvents().length === 2,
          "superseded capacity terminal",
        );
        assert.equal(harness.terminalEvents()[0]!.providerTurnId, original);
        assert.deepEqual(
          harness.terminalEvents().map((event) => event.status),
          ["interrupted", "completed"],
        );
        yield* TestClock.adjust("1 minute");
        assert.equal(sends, 2);
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect.each(["initial", "retry"] as const)(
    "Stop fences a pending capacity %s start without waiting for its reply",
    (pending) =>
      Effect.scoped(
        Effect.gen(function* () {
          const threadId = `native-capacity-pending-${pending}`;
          let sends = 0;
          const preamble = capacityPreamble(threadId, "pending-original");
          const entries: Array<CodexReplay.CodexAppServerReplayEntry> =
            pending === "initial"
              ? preamble.slice(0, 6)
              : [
                  ...preamble,
                  capacityErrorEntry(threadId, "pending-original"),
                  capacityCompletionEntry(threadId, "pending-original"),
                  ...capacityTurnExchange(threadId, "never-acknowledged-retry", 4, {
                    input: [],
                  }).slice(0, 1),
                ];
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({ scenario: `capacity-pending-${pending}`, entries }),
            undefined,
            (method) =>
              Effect.sync(() => {
                if (method === "turn/start") sends++;
              }),
          );
          const firstStart = yield* harness.runtime
            .startTurn(
              makeCodexTestTurnInput({
                threadId: harness.threadId,
                providerThread: harness.providerThread,
                now: yield* DateTime.now,
                attemptId: RunAttemptId.make(`capacity-pending-${pending}`),
                text: "Work",
              }),
            )
            .pipe(Effect.exit, Effect.forkScoped);
          let original = ProviderTurnId.make("not-yet-acknowledged");
          if (pending === "retry") {
            assert.equal((yield* Fiber.join(firstStart))._tag, "Success");
            yield* awaitCapacityDelay(harness);
            original = harness.events.find(
              (
                event,
              ): event is Extract<
                ProviderAdapter.ProviderAdapterV2Event,
                { type: "provider_turn.updated" }
              > => event.type === "provider_turn.updated",
            )!.providerTurn.id;
            yield* TestClock.adjust("10 seconds");
          }
          yield* awaitUntil(
            () => sends === (pending === "initial" ? 1 : 2),
            "unacknowledged capacity RPC",
          );
          yield* harness.runtime.interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId: original,
            requestRuntimeRestart: true,
          });
          const held = yield* harness.runtime
            .startTurn(
              makeCodexTestTurnInput({
                threadId: harness.threadId,
                providerThread: harness.providerThread,
                now: yield* DateTime.now,
                attemptId: RunAttemptId.make("capacity-after-pending-stop"),
                text: "Never replay",
              }),
            )
            .pipe(Effect.result);
          assert.equal(held._tag, "Failure");
          if (pending === "retry") {
            yield* harness.firstTerminal;
            const terminal = harness.terminalEvents()[0]!;
            assert.equal(terminal.providerTurnId, original);
            assert.equal(terminal.status, "failed");
            if (terminal.status !== "failed") return assert.fail("Expected unknown native start");
            assert.equal(terminal.evidenceKind, "local_failure");
            assert.isUndefined(terminal.providerTurn);
            assert.equal(terminal.failure.class, "unknown");
            assert.equal(terminal.threadDisposition, "broken");
          }
          yield* TestClock.adjust("1 minute");
          assert.equal(sends, pending === "initial" ? 1 : 2);
          assert.lengthOf(harness.terminalEvents(), pending === "initial" ? 0 : 1);
          if (pending === "initial") assert.equal((yield* Fiber.join(firstStart))._tag, "Failure");
        }),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect.each(["same", "changed", "failed", "defect", "timeout"] as const)(
    "validates the managed capacity revision before retrying: %s",
    (outcome) =>
      Effect.scoped(
        Effect.gen(function* () {
          const threadId = `native-capacity-managed-${outcome}`;
          let checks = 0;
          let sends = 0;
          const entries: Array<CodexReplay.CodexAppServerReplayEntry> = [
            ...capacityPreamble(threadId, "managed-original"),
            capacityErrorEntry(threadId, "managed-original"),
            capacityCompletionEntry(threadId, "managed-original"),
          ];
          if (outcome === "same")
            entries.push(
              ...capacityTurnExchange(threadId, "managed-retry", 4, { input: [] }),
              capacityCompletionEntry(threadId, "managed-retry", "completed"),
            );
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({ scenario: `capacity-managed-${outcome}`, entries }),
            undefined,
            (method) =>
              Effect.sync(() => {
                if (method === "turn/start") sends++;
              }),
            undefined,
            {
              resolveRuntime: Effect.suspend(() => {
                checks++;
                if (checks > 2 && outcome === "failed")
                  return Effect.fail(
                    new ProviderSetupError({
                      instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
                      operation: "runtime",
                      detail: "Synthetic revision check failed",
                    }),
                  );
                if (checks > 2 && outcome === "defect")
                  return Effect.die("Synthetic managed revision defect");
                if (checks > 2 && outcome === "timeout") return Effect.never;
                return Effect.succeed({
                  config: DEFAULT_CODEX_SETTINGS,
                  environment: {},
                  revision: checks > 2 && outcome === "changed" ? "changed" : "original",
                });
              }),
            },
          );
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make(`capacity-managed-${outcome}`),
              text: "Work",
            }),
          );
          yield* awaitCapacityDelay(harness);
          assert.equal(checks, 2);
          assert.isTrue(yield* harness.hasPendingBackgroundWork);
          assert.isTrue(
            yield* harness.runtime.hasPendingBackgroundWorkForThread!(harness.providerThread),
          );
          yield* TestClock.adjust("4 seconds");
          assert.equal(checks, 2);
          assert.equal(sends, 1);
          yield* TestClock.adjust("1 second");
          yield* awaitUntil(() => checks === 3, "bounded managed revision check");
          if (outcome === "timeout") {
            yield* TestClock.adjust("29 seconds");
            assert.lengthOf(harness.terminalEvents(), 0);
            assert.equal(sends, 1);
            yield* TestClock.adjust("1 second");
          }
          yield* harness.firstTerminal;
          assert.lengthOf(harness.terminalEvents(), 1);
          assert.equal(
            harness.terminalEvents()[0]!.status,
            outcome === "same" ? "completed" : "failed",
          );
          const nativeIds = new Set(
            harness.events.flatMap((event) =>
              event.type === "provider_turn.updated"
                ? [event.providerTurn.nativeTurnRef?.nativeId]
                : [],
            ),
          );
          assert.deepEqual(
            [...nativeIds],
            outcome === "same" ? ["managed-original", "managed-retry"] : ["managed-original"],
          );
          yield* TestClock.adjust("1 minute");
          assert.equal(checks, 3);
          assert.equal(sends, outcome === "same" ? 2 : 1);
          assert.isFalse(yield* harness.hasPendingBackgroundWork);
        }),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect("holds a capacity retry after its actual attempt binding is lost", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const threadId = "native-capacity-binding-lost";
        let sends = 0;
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "capacity-binding-lost",
            entries: [
              ...capacityPreamble(threadId, "binding-original"),
              capacityErrorEntry(threadId, "binding-original"),
              capacityCompletionEntry(threadId, "binding-original"),
            ],
          }),
          undefined,
          (method) =>
            Effect.sync(() => {
              if (method === "turn/start") sends++;
            }),
        );
        const turn = {
          ...makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("capacity-bound-attempt"),
            text: "Work",
          }),
        };
        yield* harness.runtime.startTurn(turn);
        yield* awaitCapacityDelay(harness);
        turn.attemptId = RunAttemptId.make("foreign-attempt");
        yield* TestClock.adjust("10 seconds");
        yield* harness.firstTerminal;
        assert.equal(harness.terminalEvents()[0]!.status, "failed");
        yield* TestClock.adjust("1 minute");
        assert.equal(sends, 1);
        assert.lengthOf(harness.terminalEvents(), 1);
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect("does not send a delayed capacity retry after its session scope closes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const threadId = "native-capacity-scope-close";
        let sends = 0;
        let opened:
          | Parameters<CodexAdapterV2.CodexAppServerClientFactoryShape["open"]>[0]
          | undefined;
        const sessionScope = yield* Scope.make();
        yield* Effect.addFinalizer(() => Scope.close(sessionScope, Exit.void));
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "capacity-scope-close",
            entries: [
              ...capacityPreamble(threadId, "closed-original"),
              capacityErrorEntry(threadId, "closed-original"),
              capacityCompletionEntry(threadId, "closed-original"),
            ],
          }),
          undefined,
          (method) =>
            Effect.sync(() => {
              if (method === "turn/start") sends++;
            }),
          undefined,
          {
            onOpen: (input) =>
              Effect.sync(() => {
                opened = input;
              }),
          },
        ).pipe(Effect.provideService(Scope.Scope, sessionScope));
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("capacity-closed-attempt"),
            text: "Work",
          }),
        );
        yield* awaitCapacityDelay(harness);
        yield* Scope.close(sessionScope, Exit.void);
        yield* (
          opened?.onTermination?.(new CodexError.CodexAppServerProcessExitedError({ code: 0 })) ??
            Effect.void
        );
        yield* TestClock.adjust("1 minute");
        assert.equal(sends, 1);
        assert.lengthOf(
          harness.terminalEvents(),
          0,
          "intentional shutdown does not synthesize a turn failure",
        );
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect.each([
    "notification",
    "completion",
    "early-completion",
    "overload-completion",
  ] as const)(
    "recovers capacity reported by %s with a five-second promptless continuation",
    (surface) =>
      Effect.scoped(
        Effect.gen(function* () {
          const threadId = `native-model-capacity-${surface}`;
          const failure = capacityCompletionEntry(threadId, "model-capacity-original");
          const notification = capacityErrorEntry(threadId, "model-capacity-original", "other");
          const withModelCapacity = (entry: CodexReplay.CodexAppServerReplayEntry) => {
            if (entry.type !== "emit_inbound" || !Predicate.isObject(entry.frame)) return entry;
            const params = entry.frame.params as Record<string, unknown>;
            const error = {
              message: "Selected model is at capacity. Please try a different model.",
              codexErrorInfo: "other",
              additionalDetails: null,
            };
            return {
              ...entry,
              frame: {
                ...entry.frame,
                params: {
                  ...params,
                  ...(entry.frame.method === "error"
                    ? { error }
                    : {
                        turn: {
                          ...(params.turn as Record<string, unknown>),
                          error,
                        },
                      }),
                },
              },
            };
          };
          const completion =
            surface === "overload-completion" ? failure : withModelCapacity(failure);
          const entries = [...capacityPreamble(threadId, "model-capacity-original")];
          if (surface === "early-completion") entries.splice(entries.length - 1, 0, completion);
          else {
            if (surface === "notification") entries.push(withModelCapacity(notification));
            entries.push(completion);
          }
          entries.push(
            ...capacityTurnExchange(threadId, "model-capacity-success", 4, { input: [] }),
            capacityCompletionEntry(threadId, "model-capacity-success", "completed"),
          );
          let sends = 0;
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({ scenario: `model-capacity-${surface}`, entries }),
            undefined,
            (method) =>
              Effect.sync(() => {
                if (method === "turn/start") sends++;
              }),
          );
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make(`capacity-${surface}`),
              text: "Work",
            }),
          );
          yield* awaitCapacityDelay(harness);
          const retry = harness.events.find(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "error" &&
              event.turnItem.status === "running",
          );
          assert.equal(
            retry?.type === "turn_item.updated" &&
              retry.turnItem.type === "error" &&
              retry.turnItem.retry?.retryDelayMs,
            5000,
          );
          assert.equal(
            retry?.type === "turn_item.updated" &&
              retry.turnItem.type === "error" &&
              retry.turnItem.retry?.maxAttempts,
            12,
          );
          yield* TestClock.adjust("4 seconds");
          assert.equal(sends, 1);
          assert.lengthOf(harness.terminalEvents(), 0);
          yield* TestClock.adjust("1 second");
          yield* harness.firstTerminal;
          assert.equal(sends, 2);
          assert.lengthOf(harness.terminalEvents(), 1);
          assert.equal(harness.terminalEvents()[0]!.status, "completed");
        }),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect.each(["native-retry", "noncapacity"] as const)(
    "leaves unconfirmed or native-owned capacity recovery alone: %s",
    (outcome) =>
      Effect.scoped(
        Effect.gen(function* () {
          const threadId = `native-capacity-owned-${outcome}`;
          const error = capacityErrorEntry(
            threadId,
            "native-owned",
            outcome === "noncapacity" ? "internalServerError" : "serverOverloaded",
          );
          let sends = 0;
          const entries = [...capacityPreamble(threadId, "native-owned")];
          entries.push(
            outcome === "native-retry" &&
              error.type === "emit_inbound" &&
              Predicate.isObject(error.frame)
              ? {
                  ...error,
                  frame: {
                    ...error.frame,
                    params: {
                      ...(error.frame.params as Record<string, unknown>),
                      willRetry: true,
                    },
                  },
                }
              : error,
          );
          entries.push(capacityCompletionEntry(threadId, "native-owned"));
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({ scenario: `capacity-owned-${outcome}`, entries }),
            undefined,
            (method) =>
              Effect.sync(() => {
                if (method === "turn/start") sends++;
              }),
          );
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make(`capacity-owned-${outcome}`),
              text: "Work",
            }),
          );
          yield* harness.firstTerminal;
          assert.equal(harness.terminalEvents()[0]!.status, "failed");
          yield* TestClock.adjust("1 minute");
          assert.equal(sends, 1);
          assert.lengthOf(harness.terminalEvents(), 1);
        }),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect(
    "rejects a duplicate capacity attempt without resending or cancelling its owned retry",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const threadId = "native-capacity-duplicate-attempt";
          let sends = 0;
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({
              scenario: "capacity-duplicate-attempt",
              entries: [
                ...capacityPreamble(threadId, "duplicate-original"),
                capacityErrorEntry(threadId, "duplicate-original"),
                capacityCompletionEntry(threadId, "duplicate-original"),
                ...capacityTurnExchange(threadId, "duplicate-retry", 4, { input: [] }),
                capacityCompletionEntry(threadId, "duplicate-retry", "completed"),
              ],
            }),
            undefined,
            (method) =>
              Effect.sync(() => {
                if (method === "turn/start") sends++;
              }),
          );
          const turn = makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("capacity-duplicate-attempt"),
            text: "Work",
          });
          yield* harness.runtime.startTurn(turn);
          yield* awaitCapacityDelay(harness);
          assert.equal(
            (yield* harness.runtime.startTurn(turn).pipe(Effect.result))._tag,
            "Failure",
          );
          assert.equal(sends, 1);
          assert.lengthOf(harness.terminalEvents(), 0);
          yield* TestClock.adjust("10 seconds");
          yield* harness.firstTerminal;
          assert.equal(harness.terminalEvents()[0]!.status, "completed");
          yield* TestClock.adjust("1 minute");
          assert.equal(sends, 2);
          assert.lengthOf(harness.terminalEvents(), 1);
        }),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect.each(["supported", "unsupported", "invalid"] as const)(
    "delivers native history with %s app-server protocol",
    (response) =>
      Effect.gen(function* () {
        const nativeThreadId = `inject-${response}`;
        const prompt = "Only the current request";
        const history: ProviderAdapter.ProviderAdapterV2HistoricalContext = {
          context: "Historical conversation",
          messages: (["user", "assistant"] as const).map((role) => ({
            role,
            text:
              role === "user"
                ? "Original request\n" + "界".repeat(300)
                : "Partial interrupted work",
            threadId: ThreadId.make("source"),
            runId: RunId.make("source-run"),
            itemId: TurnItemId.make(`source-${role}`),
            providerThreadId: null,
            kind: `${role}_message`,
            status: "interrupted",
          })),
        };
        const items = historyResponseItems(history.messages, history.context);
        const preamble = codexReplayPreamble({
          nativeThreadId,
          nativeTurnId: "current-turn",
          prompt,
        });
        const transcript = makeCodexReplayTranscript({
          scenario: `inject-${response}`,
          entries: [
            ...preamble.slice(0, -3),
            {
              type: "expect_outbound",
              label: "inject",
              frame: {
                id: 3,
                method: "thread/inject_items",
                params: { threadId: nativeThreadId, items },
              },
            },
            {
              type: "emit_inbound",
              label: "inject-result",
              frame:
                response === "supported"
                  ? { id: 3, result: {} }
                  : {
                      id: 3,
                      error: {
                        code: response === "unsupported" ? -32601 : -32602,
                        message: "Injection rejected",
                      },
                    },
            },
            ...(response === "invalid"
              ? []
              : preamble
                  .slice(-3)
                  .map((entry) =>
                    "frame" in entry && Predicate.isObject(entry.frame) && "id" in entry.frame
                      ? { ...entry, frame: { ...entry.frame, id: 4 } }
                      : entry,
                  )),
          ],
        });
        const requests: string[] = [];
        const harness = yield* makeCodexReplayHarness(
          transcript,
          () => Effect.void,
          (method) =>
            Effect.sync(() => {
              requests.push(method);
            }),
        );
        const injection = yield* harness.runtime.injectHistory!({
          providerThread: harness.providerThread,
          ...history,
        }).pipe(Effect.result);
        if (response === "invalid") {
          assert.equal(injection._tag, "Failure");
          if (injection._tag === "Failure") {
            assert.equal(injection.failure._tag, "ProviderAdapterProtocolError");
            assert.propertyVal(injection.failure.cause, "code", -32602);
            assert.notProperty(injection.failure, "payload");
          }
          assert.notInclude(requests, "turn/start");
          return;
        }
        assert.equal(injection._tag, "Success");
        if (injection._tag === "Success") assert.equal(injection.success, response === "supported");
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("inject-attempt"),
            text: prompt,
          }),
        );
        assert.equal(requests.filter((method) => method === "turn/start").length, 1);
        assert.isBelow(requests.indexOf("thread/inject_items"), requests.indexOf("turn/start"));
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect("adds the session MCP server without replacing launch-argument servers", () =>
    Effect.gen(function* () {
      const scenario = "launch-mcp-servers";
      yield* (yield* McpProviderSessions.McpProviderSessions).set({
        environmentId: EnvironmentId.make("environment-codex-mcp"),
        threadId: ThreadId.make(`thread-${scenario}`),
        providerSessionId: `provider-session-${scenario}`,
        providerInstanceId: ProviderInstanceId.make("codex"),
        endpoint: "http://127.0.0.1:43123/mcp",
        authorizationHeader: "Bearer secret-codex-token",
        browserToolsAvailable: true,
      });
      const requests: Array<unknown> = [];
      yield* makeCodexReplayHarness(
        makeCodexReplayTranscript({
          scenario,
          entries: codexReplayPreamble({
            nativeThreadId: "launch-mcp-thread",
            nativeTurnId: "unused",
            prompt: "unused",
          }).slice(0, 5),
        }),
        () => Effect.void,
        (method, params) =>
          Effect.sync(() => {
            if (method === "thread/start") requests.push(params);
          }),
      );
      assert.deepEqual(requests, [
        {
          cwd: "/workspace",
          model: "gpt-5.4",
          config: {
            "tools.update_plan.enabled": true,
            mcp_optional_startup_grace_ms: 10_000,
            "mcp_servers.t3-code": {
              url: "http://127.0.0.1:43123/mcp",
              tool_timeout_sec: 3_900,
              http_headers: { Authorization: "Bearer secret-codex-token" },
            },
          },
        },
      ]);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  // regression: #15839 launch-arg MCP servers survive the thread's t3-code server
  it.effect("replays historical and dotted MCP config while enforcing other thread config", () =>
    Effect.gen(function* () {
      const recordedServer = {
        url: "http://127.0.0.1:41000/mcp",
        http_headers: { Authorization: "Bearer recorded-token" },
      };
      const currentConfig = {
        "tools.update_plan.enabled": true,
        "mcp_servers.t3-code": {
          url: "http://127.0.0.1:42000/mcp",
          http_headers: { Authorization: "Bearer current-token" },
        },
        "mcp_servers.extra.command": "cat",
      };
      for (const recordedMcp of [
        { mcp_servers: { "t3-code": recordedServer } },
        { "mcp_servers.t3-code": recordedServer, "mcp_servers.extra.command": "echo" },
      ]) {
        const entries = codexReplayPreamble({
          nativeThreadId: "replayed-mcp-thread",
          nativeTurnId: "unused",
          prompt: "unused",
        })
          .slice(3, 5)
          .map((entry) => withReplayRequestId(entry, 1));
        entries[0] = {
          type: "expect_outbound",
          frame: {
            id: 1,
            method: "thread/start",
            params: { config: { "tools.update_plan.enabled": true, ...recordedMcp } },
          },
        };
        entries.push({
          ...entries[0],
          frame: {
            id: 2,
            method: "thread/start",
            params: { config: { "tools.update_plan.enabled": true, ...recordedMcp } },
          },
        });
        const driver = yield* CodexReplay.makeReplayDriver(
          makeCodexReplayTranscript({
            scenario: "mcp-config-normalization",
            entries,
          }),
        );
        yield* Effect.gen(function* () {
          const client = yield* CodexClient.CodexAppServerClient;
          const response = yield* client.request("thread/start", { config: currentConfig });
          assert.equal(response.thread.id, "replayed-mcp-thread");
          assert.isNull((yield* Ref.get(driver.state)).failure);
          yield* client
            .request("thread/start", {
              config: { ...currentConfig, "tools.update_plan.enabled": false },
            })
            .pipe(Effect.flip);
          assert.instanceOf(
            (yield* Ref.get(driver.state)).failure,
            CodexReplay.CodexAppServerReplayFrameMismatchError,
          );
        }).pipe(Effect.provide(CodexReplay.layerReplayWithDriver(driver)));
      }
    }).pipe(Effect.scoped),
  );

  it.effect("waits for native start before interrupting an acknowledged queued turn", () =>
    Effect.gen(function* () {
      const nativeThreadId = "early-stop-thread";
      const nativeTurnId = "early-stop-turn";
      const prompt = "Run a command.";
      const preamble = codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt });
      const transcript = makeCodexReplayTranscript({
        scenario: "early-stop-await-native-start",
        entries: [
          ...preamble.slice(0, -2),
          {
            type: "emit_inbound",
            label: "turn/start/queued",
            frame: {
              id: 3,
              result: {
                turn: {
                  ...makeCodexReplayTurn({ id: nativeTurnId, status: "inProgress" }),
                  startedAt: null,
                },
              },
            },
          },
          {
            type: "emit_inbound",
            label: "turn/started",
            afterMs: 1000,
            frame: {
              method: "turn/started",
              params: {
                threadId: nativeThreadId,
                turn: makeCodexReplayTurn({ id: nativeTurnId, status: "inProgress" }),
              },
            },
          },
          {
            type: "expect_outbound",
            label: "turn/interrupt",
            frame: {
              id: 4,
              method: "turn/interrupt",
              params: { threadId: nativeThreadId, turnId: nativeTurnId },
            },
          },
          { type: "emit_inbound", label: "turn/interrupt", frame: { id: 4, result: {} } },
          {
            type: "emit_inbound",
            label: "turn/completed",
            frame: {
              method: "turn/completed",
              params: {
                threadId: nativeThreadId,
                turn: makeCodexReplayTurn({ id: nativeTurnId, status: "interrupted" }),
              },
            },
          },
        ],
      });
      let interruptSent = false;
      const harness = yield* makeCodexReplayHarness(
        transcript,
        () => Effect.void,
        (method) =>
          Effect.sync(() => {
            if (method === "turn/interrupt") interruptSent = true;
          }),
      );
      yield* harness.runtime.startTurn(
        makeCodexTestTurnInput({
          threadId: harness.threadId,
          providerThread: harness.providerThread,
          now: yield* DateTime.now,
          attemptId: RunAttemptId.make("early-stop-attempt"),
          text: prompt,
        }),
      );
      const providerTurnId = (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
        driver: CodexAdapterV2.CODEX_DRIVER_KIND,
        nativeTurnId,
      });
      const interrupt = yield* harness.runtime
        .interruptTurn({ providerThread: harness.providerThread, providerTurnId })
        .pipe(Effect.forkScoped);
      yield* TestClock.adjust("500 millis");
      assert.isFalse(interruptSent, "Stop must not reach Codex before native turn/started");
      yield* TestClock.adjust("500 millis");
      yield* Fiber.join(interrupt);
      yield* harness.firstTerminal;
      assert.equal(harness.terminalEvents()[0]?.status, "interrupted");
      assert.lengthOf(harness.terminalEvents(), 1);
      assert.isFalse(yield* harness.hasPendingBackgroundWork);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  const assistantMessages = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) =>
    events.filter(
      (
        event,
      ): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "message.updated" }> =>
        event.type === "message.updated" && event.message.role === "assistant",
    );

  it.effect("keeps an asynchronous Codex question actionable after the turn completes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "async-question-thread";
        const nativeTurnId = "async-question-turn";
        const usage = {
          totalTokens: 15,
          inputTokens: 10,
          cachedInputTokens: 2,
          outputTokens: 5,
          reasoningOutputTokens: 1,
        };
        const transcript = makeCodexReplayTranscript({
          scenario: "async-question-and-billed-usage",
          entries: [
            ...codexReplayPreamble({
              nativeThreadId,
              nativeTurnId,
              prompt: "Continue while I decide.",
            }),
            {
              type: "emit_inbound",
              label: "question",
              frame: {
                method: "item/completed",
                params: {
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  item: {
                    type: "agentMessage",
                    id: "async-question-item",
                    text: "Which branch?",
                    delivery: "async",
                    questions: [{ title: "Which branch?", options: ["main", "dev"] }],
                  },
                },
              },
            },
            {
              type: "emit_inbound",
              label: "usage",
              frame: {
                method: "thread/tokenUsage/updated",
                params: {
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  tokenUsage: { total: usage, last: usage, modelContextWindow: 200_000 },
                },
              },
            },
            {
              type: "emit_inbound",
              label: "complete",
              frame: {
                method: "turn/completed",
                params: {
                  threadId: nativeThreadId,
                  turn: makeCodexReplayTurn({ id: nativeTurnId, status: "completed" }),
                },
              },
            },
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("async-question-attempt"),
            text: "Continue while I decide.",
          }),
        );
        yield* harness.firstTerminal;
        const requests = harness.events.flatMap((event) =>
          event.type === "runtime_request.updated" ? [event.runtimeRequest] : [],
        );
        assert.lengthOf(requests, 1);
        assert.equal(requests[0]?.status, "pending");
        assert.deepEqual(requests[0]?.responseCapability, { type: "message" });
        const questionItem = harness.events.find(
          (event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "user_input_request",
        );
        assert.equal(questionItem?.type, "turn_item.updated");
        if (
          questionItem?.type === "turn_item.updated" &&
          questionItem.turnItem.type === "user_input_request"
        ) {
          assert.deepEqual(
            questionItem.turnItem.questions[0]?.options.map((option) => option.label),
            ["main", "dev"],
          );
          assert.equal(questionItem.turnItem.responseMode, "message");
        }
        const questionNode = harness.events.find(
          (event) => event.type === "node.updated" && event.node.id === requests[0]?.nodeId,
        );
        assert.equal(
          questionNode?.type === "node.updated" && questionNode.node.countsForRun,
          false,
        );
        const contextReport = harness.events.find(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.tokenUsage !== undefined,
        );
        assert.equal(contextReport?.type, "provider_turn.updated");
        if (contextReport?.type === "provider_turn.updated") {
          assert.equal(
            contextReport.providerTurn.runAttemptId,
            RunAttemptId.make("async-question-attempt"),
          );
          assert.equal(contextReport.providerTurn.providerThreadId, harness.providerThread.id);
          assert.equal(contextReport.providerTurn.tokenUsage?.usedTokens, 15);
          assert.equal(contextReport.providerTurn.tokenUsage?.maxTokens, 200_000);
        }
        const completed = harness.events.find(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status === "completed",
        );
        assert.equal(completed?.type, "provider_turn.updated");
        if (completed?.type === "provider_turn.updated") {
          assert.deepEqual(completed.providerTurn.turnTokenUsage, {
            usageStatus: "complete",
            usageScope: "main_agent",
            hasSubagents: false,
            inputTokens: 10,
            cachedInputTokens: 2,
            outputTokens: 5,
            reasoningTokens: 1,
          });
        }
        assert.isEmpty(assistantMessages(harness.events));
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect("stops asking for commands the user allowed for the session", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "session-approval-thread";
        const turnIds = [
          "session-approval-turn-1",
          "session-approval-turn-2",
          "session-approval-turn-3",
        ];
        const turnPolicies = {
          "approval-required": {
            approvalPolicy: "untrusted",
            approvalsReviewer: "user",
            sandboxPolicy: { type: "readOnly" },
          },
          "full-access": {
            approvalPolicy: "never",
            approvalsReviewer: "user",
            sandboxPolicy: { type: "dangerFullAccess" },
          },
        } as const;
        const turnModes = ["approval-required", "full-access", "approval-required"] as const;
        const turnText = (index: number) => `Review round ${index + 1}.`;
        const startTurn = (index: number): ReadonlyArray<CodexReplay.CodexAppServerReplayEntry> => [
          {
            type: "expect_outbound",
            label: `turn ${index + 1} turn/start`,
            frame: {
              id: 3 + index,
              method: "turn/start",
              params: {
                threadId: nativeThreadId,
                input: [{ type: "text", text: turnText(index) }],
                cwd: "/workspace",
                model: "gpt-5.4",
                ...turnPolicies[turnModes[index]!],
                summary: "detailed",
              },
            },
          },
          {
            type: "emit_inbound",
            label: `turn ${index + 1} turn/start`,
            frame: {
              id: 3 + index,
              result: { turn: makeCodexReplayTurn({ id: turnIds[index]!, status: "inProgress" }) },
            },
          },
          turnEvent("turn/started", turnIds[index]!),
        ];
        const approval = (input: {
          readonly id: number;
          readonly turnId: string;
          readonly kind: "commandExecution" | "fileChange";
          readonly itemId: string;
          readonly decision: string;
        }): ReadonlyArray<CodexReplay.CodexAppServerReplayEntry> => [
          {
            type: "emit_inbound",
            label: `${input.itemId} approval`,
            frame: {
              id: input.id,
              method: `item/${input.kind}/requestApproval`,
              params: {
                threadId: nativeThreadId,
                turnId: input.turnId,
                itemId: input.itemId,
                startedAtMs: 1782622441000,
                ...(input.kind === "commandExecution" ? { command: `cat ${input.itemId}` } : {}),
              },
            },
          },
          {
            type: "expect_outbound",
            label: `${input.itemId} decision`,
            frame: { id: input.id, result: { decision: input.decision } },
          },
        ];
        const turnEvent = (
          method: "turn/started" | "turn/completed",
          turnId: string,
        ): CodexReplay.CodexAppServerReplayEntry => ({
          type: "emit_inbound",
          label: `${turnId} ${method}`,
          frame: {
            method,
            params: {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({
                id: turnId,
                status: method === "turn/started" ? "inProgress" : "completed",
              }),
            },
          },
        });
        const [patchRequest, patchDecision] = approval({
          id: 102,
          turnId: turnIds[0]!,
          kind: "fileChange",
          itemId: "first-patch",
          decision: "acceptForSession",
        });
        const transcript = makeCodexReplayTranscript({
          scenario: "codex-session-approvals",
          entries: [
            ...codexReplayPreamble({
              nativeThreadId,
              nativeTurnId: turnIds[0]!,
              prompt: turnText(0),
            }).slice(0, -3),
            ...startTurn(0),
            ...approval({
              id: 100,
              turnId: turnIds[0]!,
              kind: "commandExecution",
              itemId: "first-command",
              decision: "acceptForSession",
            }),
            patchRequest!,
            ...approval({
              id: 101,
              turnId: turnIds[0]!,
              kind: "commandExecution",
              itemId: "second-command",
              decision: "acceptForSession",
            }),
            turnEvent("turn/completed", turnIds[0]!),
            ...startTurn(1),
            patchDecision!,
            turnEvent("turn/completed", turnIds[1]!),
            ...startTurn(2),
            ...approval({
              id: 103,
              turnId: turnIds[2]!,
              kind: "commandExecution",
              itemId: "third-command",
              decision: "accept",
            }),
            ...approval({
              id: 104,
              turnId: turnIds[2]!,
              kind: "fileChange",
              itemId: "second-patch",
              decision: "accept",
            }),
            turnEvent("turn/completed", turnIds[2]!),
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        const requests = () => [
          ...new Map(
            harness.events.flatMap((event) =>
              event.type === "runtime_request.updated"
                ? [[event.runtimeRequest.id, event.runtimeRequest] as const]
                : [],
            ),
          ).values(),
        ];
        const answer = (
          count: number,
          kind: "command" | "file-change",
          decision: ProviderApprovalDecision,
        ) =>
          Effect.gen(function* () {
            yield* harness.until(() => requests().length === count);
            const request = requests()[count - 1]!;
            assert.equal(request.kind, kind);
            yield* harness.runtime.respondToRuntimeRequest({ requestId: request.id, decision });
          });

        const runTurn = (index: number) =>
          Effect.gen(function* () {
            yield* harness.runtime.startTurn({
              ...makeCodexTestTurnInput({
                threadId: harness.threadId,
                providerThread: harness.providerThread,
                now: yield* DateTime.now,
                attemptId: RunAttemptId.make(`session-approvals-${index + 1}`),
                text: turnText(index),
              }),
              runOrdinal: index + 1,
              providerTurnOrdinal: index + 1,
              runtimePolicy: { ...CODEX_TEST_RUNTIME_POLICY, runtimeMode: turnModes[index]! },
            });
          });

        yield* runTurn(0);
        yield* answer(1, "command", "acceptForSession");
        yield* harness.until(() => requests().length === 2);
        yield* harness.awaitTerminal(1);
        yield* runTurn(1);
        yield* answer(2, "file-change", "acceptForSession");
        yield* harness.awaitTerminal(2);
        yield* runTurn(2);
        yield* answer(3, "command", "accept");
        yield* answer(4, "file-change", "accept");
        yield* harness.awaitTerminal(3);
        assert.lengthOf(requests(), 4);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect("preserves T3 context on the wire and restores it after compaction", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "context-thread";
        const nativeTurnId = "context-turn";
        const params = yield* CodexAdapterV2.buildCodexTurnStartParams({
          nativeThreadId,
          codexInput: [{ type: "text", text: "work" }],
          runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
          modelSelection: CODEX_TEST_MODEL_SELECTION,
          hasT3Mcp: true,
        });
        assert.include(
          params.additionalContext?.t3_code_orchestration_01?.value ?? "",
          "delegate_task",
        );
        const entries = codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "work" });
        const transcript = makeCodexReplayTranscript({
          scenario: "restore-context",
          entries: [
            ...entries.slice(0, 5),
            {
              type: "expect_outbound",
              label: "context turn",
              frame: { id: 3, method: "turn/start", params },
            },
            ...entries.slice(6),
            {
              type: "emit_inbound",
              label: "compacted",
              frame: {
                method: "item/completed",
                params: {
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  item: { type: "contextCompaction", id: "compact-context" },
                },
              },
            },
            {
              type: "expect_outbound",
              label: "restore context",
              frame: {
                id: 4,
                method: "thread/inject_items",
                params: {
                  threadId: nativeThreadId,
                  items: Object.entries(params.additionalContext ?? {}).map(([key, entry]) => ({
                    type: "message",
                    role: "developer",
                    content: [{ type: "input_text", text: `<${key}>${entry.value}</${key}>` }],
                  })),
                },
              },
            },
            { type: "emit_inbound", label: "restored", frame: { id: 4, result: {} } },
            {
              type: "emit_inbound",
              label: "done",
              frame: {
                method: "turn/completed",
                params: {
                  threadId: nativeThreadId,
                  turn: makeCodexReplayTurn({ id: nativeTurnId, status: "completed" }),
                },
              },
            },
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        yield* (yield* McpProviderSessions.McpProviderSessions).set({
          environmentId: EnvironmentId.make("test"),
          threadId: harness.threadId,
          providerSessionId: "context-session",
          providerInstanceId: ProviderInstanceId.make("codex"),
          endpoint: "http://127.0.0.1:43123/mcp",
          authorizationHeader: "Bearer test",
          browserToolsAvailable: true,
        });
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("context-attempt"),
            text: "work",
          }),
        );
        yield* harness.firstTerminal;
        assert.equal(harness.terminalEvents()[0]?.status, "completed");
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect("compacts Codex with the native RPC and completes the compaction turn", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "compact-thread";
        const nativeTurnId = "compact-turn";
        const item = { type: "contextCompaction", id: "compact-item" };
        const transcript = makeCodexReplayTranscript({
          scenario: "native-compaction",
          entries: [
            ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "unused" }).slice(0, 5),
            {
              type: "expect_outbound",
              label: "compact",
              frame: {
                id: 3,
                method: "thread/compact/start",
                params: { threadId: nativeThreadId },
              },
            },
            { type: "emit_inbound", label: "compact", frame: { id: 3, result: {} } },
            {
              type: "emit_inbound",
              label: "start",
              frame: {
                method: "turn/started",
                params: {
                  threadId: nativeThreadId,
                  turn: makeCodexReplayTurn({ id: nativeTurnId, status: "inProgress" }),
                },
              },
            },
            ...(["item/started", "item/completed"] as const).map((method) => ({
              type: "emit_inbound" as const,
              label: method,
              frame: { method, params: { threadId: nativeThreadId, turnId: nativeTurnId, item } },
            })),
            {
              type: "emit_inbound",
              label: "complete",
              frame: {
                method: "turn/completed",
                params: {
                  threadId: nativeThreadId,
                  turn: makeCodexReplayTurn({ id: nativeTurnId, status: "completed" }),
                },
              },
            },
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        assert.isDefined(harness.runtime.compactThread);
        yield* harness.runtime.compactThread!(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("compact-attempt"),
            text: "/compact",
          }),
        );
        yield* harness.firstTerminal;
        const items = harness.events.flatMap((event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "compaction"
            ? [event.turnItem]
            : [],
        );
        assert.deepEqual(
          items.map((entry) => entry.status),
          ["running", "completed"],
        );
        assert.equal(items[0]?.id, items[1]?.id);
        assert.equal(harness.terminalEvents()[0]?.status, "completed");
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect.each(["failed", "interrupted", "Stop"] as const)(
    "settles unfinished compaction on %s without changing completed compaction",
    (outcome) =>
      Effect.scoped(
        Effect.gen(function* () {
          const nativeThreadId = "compact-thread";
          const nativeTurnId = "compact-turn";
          const error = {
            message: "Remote compaction stream disconnected",
            codexErrorInfo: "other",
            additionalDetails: null,
          };
          const transcript = makeCodexReplayTranscript({
            scenario: `automatic-compaction-${outcome}`,
            entries: [
              ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Continue." }),
              ...(
                [
                  { method: "item/started", id: "completed-item", startedAtMs: 1782622441000 },
                  { method: "item/completed", id: "completed-item", completedAtMs: 1782622442000 },
                  { method: "item/started", id: "unfinished-item", startedAtMs: 1782622443000 },
                ] as const
              ).map(({ method, id, ...timestamps }) => ({
                type: "emit_inbound" as const,
                label: `${method}/${id}`,
                frame: {
                  method,
                  params: {
                    threadId: nativeThreadId,
                    turnId: nativeTurnId,
                    item: { type: "contextCompaction", id },
                    ...timestamps,
                  },
                },
              })),
              ...(outcome === "Stop"
                ? [
                    {
                      type: "expect_outbound" as const,
                      label: "turn/interrupt",
                      frame: {
                        id: 4,
                        method: "turn/interrupt",
                        params: { threadId: nativeThreadId, turnId: nativeTurnId },
                      },
                    },
                    {
                      type: "emit_inbound" as const,
                      label: "turn/interrupt/failed",
                      frame: { id: 4, error: { code: -32_000, message: "Interrupt failed" } },
                    },
                  ]
                : [
                    {
                      type: "emit_inbound" as const,
                      label: "turn/completed",
                      frame: {
                        method: "turn/completed",
                        params: {
                          threadId: nativeThreadId,
                          turn: {
                            ...makeCodexReplayTurn({ id: nativeTurnId, status: outcome }),
                            error: outcome === "failed" ? error : null,
                          },
                        },
                      },
                    },
                  ]),
            ],
          });
          const compactionStarted = yield* Deferred.make<ProviderTurnId>();
          const harness = yield* makeCodexReplayHarness(transcript, (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.nativeItemRef?.nativeId === "unfinished-item" &&
            event.turnItem.status === "running" &&
            event.turnItem.providerTurnId !== null
              ? Deferred.succeed(compactionStarted, event.turnItem.providerTurnId)
              : Effect.void,
          );
          yield* TestClock.setTime(1782622445000);
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make(`compact-${outcome}-attempt`),
              text: "Continue.",
            }),
          );
          if (outcome === "Stop") {
            const providerTurnId = yield* Deferred.await(compactionStarted);
            const interrupt = yield* harness.runtime
              .interruptTurn({ providerThread: harness.providerThread, providerTurnId })
              .pipe(Effect.exit);
            assert.equal(interrupt._tag, "Failure");
          }
          yield* harness.firstTerminal;
          const items = harness.events.flatMap((event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "compaction"
              ? [event.turnItem]
              : [],
          );
          const completed = items.filter(
            (item) => item.nativeItemRef?.nativeId === "completed-item",
          );
          const unfinished = items.filter(
            (item) => item.nativeItemRef?.nativeId === "unfinished-item",
          );
          const status = outcome === "failed" ? "failed" : "interrupted";
          assert.deepEqual(
            unfinished.map((item) => item.status),
            ["running", status],
          );
          assert.equal(unfinished[0]?.id, unfinished[1]?.id);
          assert.deepEqual(unfinished[1]?.startedAt, DateTime.makeUnsafe(1782622443000));
          assert.deepEqual(
            unfinished[1]?.completedAt,
            DateTime.makeUnsafe(outcome === "Stop" ? 1782622445000 : 1782622450000),
          );
          assert.deepEqual(
            completed.map((item) => item.status),
            ["running", "completed"],
          );
          assert.deepEqual(completed[1]?.startedAt, DateTime.makeUnsafe(1782622441000));
          assert.deepEqual(completed[1]?.completedAt, DateTime.makeUnsafe(1782622442000));
          assert.equal(harness.terminalEvents()[0]?.status, status);
          if (outcome === "failed") {
            assert.equal(harness.terminalEvents()[0]?.failure?.message, error.message);
          }
        }).pipe(
          Effect.provide(
            Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
          ),
        ),
      ),
  );

  it.effect("resumes a provider thread without requesting or decoding its history", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const scenario = "codex-resume-metadata";
        const nativeThreadId = `native-${scenario}-thread`;
        const preamble = codexReplayPreamble({
          nativeThreadId,
          nativeTurnId: "unused-turn",
          prompt: "unused-prompt",
        }).slice(0, 5);
        const transcript = makeCodexReplayTranscript({
          scenario,
          entries: [
            ...preamble,
            {
              type: "expect_outbound",
              label: "thread/resume",
              frame: {
                id: 3,
                method: "thread/resume",
                params: {
                  threadId: nativeThreadId,
                  excludeTurns: true,
                  config: CodexAdapterV2.CODEX_THREAD_CONFIG,
                },
              },
            },
            {
              type: "emit_inbound",
              label: "thread/resume",
              frame: { id: 3, result: { thread: { id: nativeThreadId, updatedAt: 1782622450 } } },
            },
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        const resumed = yield* harness.runtime.resumeThread({
          providerThread: harness.providerThread,
          modelSelection: CODEX_TEST_MODEL_SELECTION,
          runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
        });

        assert.equal(resumed.nativeThreadRef?.nativeId, nativeThreadId);
        assert.equal(resumed.status, "idle");
        assert.equal(DateTime.toEpochMillis(resumed.updatedAt), 1782622450000);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  // regression: #15389 archived native Codex sessions resume via thread/unarchive
  it.effect.each(
    (
      [
        ["recovers archived-session errors", "session saved-thread is archived", 0, ""],
        ["preserves missing-thread errors", "thread not found", 3, "thread not found"],
        [
          "propagates unarchive missing-thread errors",
          "session saved-thread is archived",
          4,
          "thread not found",
        ],
        [
          "does not retry archived errors twice",
          "session saved-thread is archived",
          5,
          "session saved-thread is archived",
        ],
      ] as const
    ).map(([name, resumeError, failAt, finalError]) => ({ name, resumeError, failAt, finalError })),
  )("$name", ({ name, resumeError, failAt, finalError }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "saved-thread";
        const params = {
          threadId: nativeThreadId,
          excludeTurns: true,
          cwd: CODEX_TEST_RUNTIME_POLICY.cwd,
          model: CODEX_TEST_MODEL_SELECTION.model,
          config: CodexAdapterV2.CODEX_THREAD_CONFIG,
        };
        const entries: Array<CodexReplay.CodexAppServerReplayEntry> = [
          ...codexReplayPreamble({
            nativeThreadId,
            nativeTurnId: "unused-turn",
            prompt: "unused-prompt",
          }).slice(0, 5),
          {
            type: "expect_outbound",
            label: "resume archived thread",
            frame: { id: 3, method: "thread/resume", params },
          },
          {
            type: "emit_inbound",
            label: "resume error",
            frame: { id: 3, error: { code: -32600, message: resumeError } },
          },
        ];
        if (failAt !== 3) {
          entries.push(
            {
              type: "expect_outbound",
              label: "unarchive same thread",
              frame: { id: 4, method: "thread/unarchive", params: { threadId: nativeThreadId } },
            },
            {
              type: "emit_inbound",
              label: "unarchive result",
              frame:
                failAt === 4
                  ? { id: 4, error: { code: -32600, message: finalError } }
                  : { id: 4, result: { thread: { turns: [{ type: "unknown-history-item" }] } } },
            },
          );
        }
        if (failAt === 0 || failAt === 5) {
          entries.push(
            {
              type: "expect_outbound",
              label: "retry identical resume",
              frame: { id: 5, method: "thread/resume", params },
            },
            {
              type: "emit_inbound",
              label: "retry result",
              frame:
                failAt === 5
                  ? { id: 5, error: { code: -32600, message: finalError } }
                  : {
                      id: 5,
                      result: {
                        thread: {
                          id: nativeThreadId,
                          updatedAt: 1782622450,
                          turns: [{ type: "unknown-history-item" }],
                        },
                      },
                    },
            },
          );
        }
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({ scenario: name, entries }),
        );
        const resume = harness.runtime.resumeThread({
          providerThread: harness.providerThread,
          modelSelection: CODEX_TEST_MODEL_SELECTION,
          runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
        });
        if (failAt !== 0) {
          const error = yield* Effect.flip(resume);
          assert.equal(error._tag, "ProviderAdapterResumeThreadError");
          assert.nestedPropertyVal(error, "cause.errorMessage", finalError);
          assert.nestedPropertyVal(
            error,
            "cause.method",
            failAt === 4 ? "thread/unarchive" : "thread/resume",
          );
          assert.nestedPropertyVal(error, "cause.requestId", String(failAt));
          return;
        }
        const resumed = yield* resume;
        assert.equal(resumed.id, harness.providerThread.id);
        assert.equal(resumed.nativeThreadRef?.nativeId, nativeThreadId);
        assert.deepEqual(
          resumed.nativeConversationHeadRef,
          harness.providerThread.nativeConversationHeadRef,
        );
        assert.equal(resumed.status, "idle");
        assert.equal(DateTime.toEpochMillis(resumed.updatedAt), 1782622450000);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect("resolves retryable app-server errors on resumed provider activity", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const scenario = "codex-provider-api-retry";
        const nativeThreadId = `native-${scenario}-thread`;
        const nativeTurnId = `native-${scenario}-turn`;
        const transcript = makeCodexReplayTranscript({
          scenario,
          entries: [
            ...codexReplayPreamble({
              nativeThreadId,
              nativeTurnId,
              prompt: "Open github.com.",
            }),
            {
              type: "emit_inbound",
              label: "error/retry",
              frame: {
                method: "error",
                params: {
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  willRetry: true,
                  error: {
                    message: "Reconnecting... 2/5",
                    additionalDetails: "The response stream disconnected.",
                    codexErrorInfo: {
                      responseStreamDisconnected: { httpStatusCode: 529 },
                    },
                  },
                },
              },
            },
            {
              type: "emit_inbound",
              label: "item/started/after-retry",
              frame: {
                method: "item/started",
                params: {
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  startedAtMs: 1782622445000,
                  item: {
                    type: "commandExecution",
                    id: "command-after-provider-retry",
                    command: "pwd",
                    cwd: "/workspace",
                    processId: "42",
                    source: "unifiedExecStartup",
                    status: "inProgress",
                    commandActions: [{ type: "unknown", command: "pwd" }],
                    aggregatedOutput: null,
                    exitCode: null,
                    durationMs: null,
                  },
                },
              },
            },
            {
              type: "emit_inbound",
              label: "item/completed/after-retry",
              frame: {
                method: "item/completed",
                params: {
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  completedAtMs: 1782622445010,
                  item: {
                    type: "commandExecution",
                    id: "command-after-provider-retry",
                    command: "pwd",
                    cwd: "/workspace",
                    processId: "42",
                    source: "unifiedExecStartup",
                    status: "completed",
                    commandActions: [{ type: "unknown", command: "pwd" }],
                    aggregatedOutput: "/workspace\n",
                    exitCode: 0,
                    durationMs: 10,
                  },
                },
              },
            },
            {
              type: "emit_inbound",
              label: "turn/completed",
              frame: {
                method: "turn/completed",
                params: {
                  threadId: nativeThreadId,
                  turn: makeCodexReplayTurn({ id: nativeTurnId, status: "completed" }),
                },
              },
            },
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        const now = yield* DateTime.now;
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-provider-api-retry"),
            text: "Open github.com.",
          }),
        );
        yield* harness.awaitTerminal(1);

        const retryItems = harness.events.flatMap((event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "error" &&
          event.turnItem.retry !== undefined
            ? [event.turnItem]
            : [],
        );
        assert.lengthOf(retryItems, 2);
        assert.equal(retryItems[0]?.status, "running");
        assert.equal(retryItems[0]?.failure.code, "responseStreamDisconnected");
        assert.deepEqual(retryItems[0]?.retry, {
          attempt: 2,
          maxAttempts: 5,
          retryDelayMs: null,
        });
        assert.equal(retryItems[1]?.id, retryItems[0]?.id);
        assert.equal(retryItems[1]?.status, "completed");
        assert.equal(retryItems[1]?.title, "Provider recovered");

        const recoveredIndex = harness.events.findIndex(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "error" &&
            event.turnItem.status === "completed",
        );
        const resumedCommandIndex = harness.events.findIndex(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.input === "pwd",
        );
        const terminalIndex = harness.events.findIndex((event) => event.type === "turn.terminal");
        assert.isAtLeast(recoveredIndex, 0);
        assert.isAbove(resumedCommandIndex, recoveredIndex);
        assert.isAbove(terminalIndex, resumedCommandIndex);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect.each(["completed", "interrupted"] as const)(
    "retains Codex reasoning parts when the turn is %s",
    (terminalStatus) =>
      Effect.scoped(
        Effect.gen(function* () {
          const scenario = `codex-reasoning-${terminalStatus}`;
          const nativeThreadId = `native-${scenario}-thread`;
          const nativeTurnId = `native-${scenario}-turn`;
          const prompt = "Explain the check.";
          const transcript = makeCodexReplayTranscript({
            scenario,
            entries: [
              ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt }),
              ...[
                {
                  method: "item/reasoning/summaryTextDelta",
                  params: { itemId: "thought", summaryIndex: 0, delta: "Summary " },
                },
                {
                  method: "item/reasoning/summaryTextDelta",
                  params: { itemId: "thought", summaryIndex: 0, delta: "one" },
                },
                {
                  method: "item/reasoning/summaryTextDelta",
                  params: { itemId: "thought", summaryIndex: 1, delta: "Summary two" },
                },
                {
                  method: "item/reasoning/textDelta",
                  params: { itemId: "thought", contentIndex: 0, delta: "Raw trace" },
                },
                {
                  method: "item/completed",
                  params: {
                    item: {
                      type: "commandExecution",
                      id: "after-thought",
                      command: "pwd",
                      cwd: "/workspace",
                      processId: "42",
                      source: "unifiedExecStartup",
                      status: "completed",
                      commandActions: [{ type: "unknown", command: "pwd" }],
                      aggregatedOutput: "/workspace",
                      exitCode: 0,
                      durationMs: 1,
                    },
                  },
                },
                ...(terminalStatus === "completed"
                  ? [
                      {
                        method: "item/completed",
                        params: {
                          item: {
                            type: "reasoning",
                            id: "thought",
                            summary: ["Final summary one", "Summary two"],
                            content: ["Raw trace"],
                          },
                        },
                      },
                      {
                        method: "item/completed",
                        params: {
                          item: {
                            type: "reasoning",
                            id: "completion-only",
                            summary: ["Completion without deltas"],
                            content: [],
                          },
                        },
                      },
                      {
                        method: "item/reasoning/textDelta",
                        params: {
                          itemId: "delta-only",
                          contentIndex: 0,
                          delta: "Retained when completion omits content",
                        },
                      },
                      {
                        method: "item/completed",
                        params: {
                          item: { type: "reasoning", id: "delta-only", summary: [], content: [] },
                        },
                      },
                    ]
                  : []),
              ].map((event, index) => ({
                type: "emit_inbound" as const,
                label: `reasoning-${index}`,
                frame: {
                  method: event.method,
                  params: { threadId: nativeThreadId, turnId: nativeTurnId, ...event.params },
                },
              })),
              {
                type: "emit_inbound",
                label: "turn/completed",
                frame: {
                  method: "turn/completed",
                  params: {
                    threadId: nativeThreadId,
                    turn: makeCodexReplayTurn({ id: nativeTurnId, status: terminalStatus }),
                  },
                },
              },
            ],
          });
          const harness = yield* makeCodexReplayHarness(transcript);
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make(`attempt-${scenario}`),
              text: prompt,
            }),
          );
          yield* harness.firstTerminal;
          const latest = new Map(
            harness.events.flatMap((event) =>
              event.type === "turn_item.updated" && event.turnItem.type === "reasoning"
                ? [[event.turnItem.id, event.turnItem] as const]
                : [],
            ),
          );
          assert.deepEqual(
            [...latest.values()].map((item) => item.text),
            terminalStatus === "completed"
              ? [
                  "Final summary one",
                  "Summary two",
                  "Raw trace",
                  "Completion without deltas",
                  "Retained when completion omits content",
                ]
              : ["Summary one", "Summary two", "Raw trace"],
          );
          assert.isTrue([...latest.values()].every((item) => item.status === terminalStatus));
          assert.isTrue([...latest.values()].every((item) => item.streaming === false));
          assert.isTrue(
            [...latest.values()].every(
              (item) => item.runId !== null && item.providerTurnId !== null,
            ),
          );
          assert.equal(new Set([...latest.values()].map((item) => item.ordinal)).size, latest.size);
          const command = harness.events.find(
            (event) =>
              event.type === "turn_item.updated" && event.turnItem.type === "command_execution",
          );
          assert.isDefined(command);
          if (command?.type === "turn_item.updated") {
            assert.isTrue(
              [...latest.values()]
                .slice(0, 3)
                .every((item) => item.ordinal < command.turnItem.ordinal),
            );
          }
          assert.deepEqual(assistantMessages(harness.events), []);
        }).pipe(
          Effect.provide(
            Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
          ),
        ),
      ),
  );

  const finalAnswerTranscript = (
    scenario: string,
    answers: ReadonlyArray<{
      readonly id: string;
      readonly text: string;
      readonly phase?: "commentary" | "final_answer" | null;
      readonly omitPhase?: boolean;
      readonly streamed?: boolean;
      readonly completionDelayMs?: number;
    }>,
  ) => {
    const nativeThreadId = `native-${scenario}-thread`;
    const nativeTurnId = `native-${scenario}-turn`;
    const prompt = "Reply with the requested recovery marker.";
    return makeCodexReplayTranscript({
      scenario,
      entries: [
        ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt }),
        ...answers.flatMap(
          (answer, index): ReadonlyArray<CodexReplay.CodexAppServerReplayEntry> => {
            const phase = answer.omitPhase
              ? {}
              : { phase: answer.phase === undefined ? ("final_answer" as const) : answer.phase };
            const completed: CodexReplay.CodexAppServerReplayEntry = {
              type: "emit_inbound",
              label: `item/completed/${answer.id}`,
              ...(answer.completionDelayMs === undefined
                ? {}
                : { afterMs: answer.completionDelayMs }),
              frame: {
                method: "item/completed",
                params: {
                  item: {
                    type: "agentMessage",
                    id: answer.id,
                    text: answer.text,
                    ...phase,
                    memoryCitation: null,
                  },
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  completedAtMs: 1782622441000 + index,
                },
              },
            };
            if (!answer.streamed) {
              return [completed];
            }
            return [
              {
                type: "emit_inbound",
                label: `item/started/${answer.id}`,
                frame: {
                  method: "item/started",
                  params: {
                    item: {
                      type: "agentMessage",
                      id: answer.id,
                      text: "",
                      ...phase,
                      memoryCitation: null,
                    },
                    threadId: nativeThreadId,
                    turnId: nativeTurnId,
                    startedAtMs: 1782622440500 + index,
                  },
                },
              },
              {
                type: "emit_inbound",
                label: `item/agentMessage/delta/${answer.id}`,
                frame: {
                  method: "item/agentMessage/delta",
                  params: {
                    threadId: nativeThreadId,
                    turnId: nativeTurnId,
                    itemId: answer.id,
                    delta: answer.text,
                  },
                },
              },
              completed,
            ];
          },
        ),
        {
          type: "emit_inbound",
          label: "turn/completed",
          frame: {
            method: "turn/completed",
            params: {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: nativeTurnId, status: "completed" }),
            },
          },
        },
      ],
    });
  };

  it.effect("suppresses a trailing empty final answer after a non-empty final answer", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const transcript = finalAnswerTranscript("codex-redundant-empty-final", [
          { id: "answer-non-empty", text: "CODEX_RECOVERY_OK" },
          { id: "answer-empty", text: "" },
        ]);
        const harness = yield* makeCodexReplayHarness(transcript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-redundant-empty-final"),
            text: "Reply with the requested recovery marker.",
          }),
        );
        yield* harness.awaitTerminal(1);

        assert.deepEqual(
          assistantMessages(harness.events).map((event) => event.message.text),
          ["CODEX_RECOVERY_OK"],
        );
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  const BG_SCENARIO = "codex-bg-exec-wake";
  const BG_NATIVE_THREAD = "native-codex-bg-thread";
  const BG_NATIVE_TURN = "native-codex-bg-turn";
  const BG_COMMAND_ITEM = "call-codex-bg-command";
  const BG_COMMAND = "sleep 20 && echo CODEX_BG_WAKE_DONE";
  const BG_PROMPT = "Start the sleep in the background and reply STARTED.";

  const backgroundCommandItem = (status: "inProgress" | "completed"): Record<string, unknown> => ({
    type: "commandExecution",
    id: BG_COMMAND_ITEM,
    command: BG_COMMAND,
    cwd: "/workspace",
    processId: "4242",
    source: "unifiedExecStartup",
    status,
    commandActions: [{ type: "unknown", command: BG_COMMAND }],
    aggregatedOutput: status === "completed" ? "CODEX_BG_WAKE_DONE\n" : null,
    exitCode: status === "completed" ? 0 : null,
    durationMs: status === "completed" ? 25_000 : null,
  });

  const backgroundExecTranscript = makeCodexReplayTranscript({
    scenario: BG_SCENARIO,
    entries: [
      ...codexReplayPreamble({
        nativeThreadId: BG_NATIVE_THREAD,
        nativeTurnId: BG_NATIVE_TURN,
        prompt: BG_PROMPT,
      }),
      {
        type: "emit_inbound",
        label: "item/started/command",
        frame: {
          method: "item/started",
          params: {
            item: backgroundCommandItem("inProgress"),
            threadId: BG_NATIVE_THREAD,
            turnId: BG_NATIVE_TURN,
            startedAtMs: 1782622440500,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "item/completed/root-answer",
        frame: {
          method: "item/completed",
          params: {
            item: {
              type: "agentMessage",
              id: "root-answer-bg",
              text: "STARTED",
              phase: "final_answer",
              memoryCitation: null,
            },
            threadId: BG_NATIVE_THREAD,
            turnId: BG_NATIVE_TURN,
            completedAtMs: 1782622441000,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/completed",
        frame: {
          method: "turn/completed",
          params: {
            threadId: BG_NATIVE_THREAD,
            turn: makeCodexReplayTurn({ id: BG_NATIVE_TURN, status: "completed" }),
          },
        },
      },
      {
        type: "emit_inbound",
        label: "item/completed/command-late",
        afterMs: 30_000,
        frame: {
          method: "item/completed",
          params: {
            item: backgroundCommandItem("completed"),
            threadId: BG_NATIVE_THREAD,
            turnId: BG_NATIVE_TURN,
            completedAtMs: 1782622465500,
          },
        },
      },
    ],
  });

  it.effect.each([false, true])(
    "keeps commands interrupted after native thread closure (persistent tool: %s)",
    (retainContext) =>
      Effect.gen(function* () {
        const notification = (method: string, params: unknown) => ({
          type: "emit_inbound" as const,
          label: method,
          frame: { method, params },
        });
        const monitor = (status: "inProgress" | "completed") =>
          notification(status === "inProgress" ? "item/started" : "item/completed", {
            threadId: BG_NATIVE_THREAD,
            turnId: BG_NATIVE_TURN,
            item: {
              type: "dynamicToolCall",
              id: "persistent-monitor",
              namespace: "grok",
              tool: "monitor",
              status,
              arguments: { persistent: true, command: "tail -f" },
            },
          });
        // Replies fence notification handling on the same protocol reader.
        const unsubscribe = (id: number, notifications: ReturnType<typeof notification>[] = []) => [
          {
            type: "expect_outbound" as const,
            label: `unsubscribe-${id}`,
            frame: { id, method: "thread/unsubscribe", params: { threadId: BG_NATIVE_THREAD } },
          },
          ...notifications,
          {
            type: "emit_inbound" as const,
            label: `unsubscribe-result-${id}`,
            frame: { id, result: { status: "notSubscribed" } },
          },
        ];
        const closed = notification("thread/closed", { threadId: BG_NATIVE_THREAD });
        const transcript = makeCodexReplayTranscript({
          scenario: `codex-background-thread-closed-${retainContext}`,
          entries: [
            ...backgroundExecTranscript.entries.slice(0, -2),
            ...(retainContext ? [monitor("inProgress")] : []),
            backgroundExecTranscript.entries.at(-2)!,
            ...unsubscribe(4),
            ...unsubscribe(5, [notification("thread/closed", { threadId: "unrelated-thread" })]),
            ...unsubscribe(6, [closed]),
            ...unsubscribe(7, [
              closed,
              notification("item/completed", {
                threadId: BG_NATIVE_THREAD,
                turnId: BG_NATIVE_TURN,
                item: backgroundCommandItem("completed"),
              }),
              ...(retainContext ? [monitor("completed")] : []),
            ]),
          ],
        });
        const interrupted = yield* Deferred.make<void>();
        const monitorCompleted = yield* Deferred.make<void>();
        const harness = yield* makeCodexReplayHarness(transcript, (event) => {
          if (event.type !== "turn_item.updated") return Effect.void;
          if (
            event.turnItem.nativeItemRef?.nativeId === BG_COMMAND_ITEM &&
            event.turnItem.status === "interrupted"
          ) {
            return Deferred.succeed(interrupted, undefined);
          }
          return event.turnItem.nativeItemRef?.nativeId === "persistent-monitor" &&
            event.turnItem.status === "completed"
            ? Deferred.succeed(monitorCompleted, undefined)
            : Effect.void;
        });
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("attempt-codex-thread-closed"),
            text: BG_PROMPT,
          }),
        );
        yield* harness.firstTerminal;
        const unload = harness.runtime.unloadThread!({ providerThread: harness.providerThread });
        yield* unload;
        assert.isTrue(yield* harness.hasPendingBackgroundWork);
        yield* unload;
        assert.isTrue(yield* harness.hasPendingBackgroundWork);
        yield* unload;
        assert.equal(yield* harness.hasPendingBackgroundWork, retainContext);
        yield* Deferred.await(interrupted);
        yield* unload;
        if (retainContext) yield* Deferred.await(monitorCompleted);
        assert.deepEqual(
          {
            items: harness.events.flatMap((event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.nativeItemRef?.nativeId === BG_COMMAND_ITEM
                ? [event.turnItem.status]
                : [],
            ),
            nodes: harness.events.flatMap((event) =>
              event.type === "node.updated" &&
              event.node.nativeItemRef?.nativeId === BG_COMMAND_ITEM
                ? [event.node.status]
                : [],
            ),
            roots: harness.terminalEvents().map((event) => event.status),
            continuations: harness.continuationRequests.length,
          },
          {
            items: ["running", "interrupted"],
            nodes: ["running", "interrupted"],
            roots: ["completed"],
            continuations: 0,
          },
        );
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
        assert.isFalse(
          yield* harness.runtime.hasPendingBackgroundWorkForThread!(harness.providerThread),
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  const backgroundCompletionCases = [
    {
      label: "expected server expiry",
      command:
        "/bin/bash -lc 'timeout 600 python3 -m http.server 18763 --bind 127.0.0.1 --directory .t3/pr-media/pr-visual-evidence'",
      exitCode: 124,
      wake: false,
    },
    {
      label: "unexpected server failure",
      command: "timeout 600 python3 -m http.server 18763",
      exitCode: 1,
      wake: true,
    },
    { label: "timed-out tests", command: "timeout 600 pytest", exitCode: 124, wake: true },
  ];
  it.effect.each(backgroundCompletionCases)(
    "records $label and only wakes for actionable results",
    (completion) =>
      Effect.scoped(
        Effect.gen(function* () {
          const command = completion.command;
          const output =
            '127.0.0.1 - - [20/Oct/2026 19:36:02] "GET /behavior.mp4 HTTP/1.1" 200 -\n';
          let completeItem: ((payload: unknown) => Effect.Effect<unknown>) | undefined;
          const transcript = makeCodexReplayTranscript({
            scenario: `codex-background-${completion.label.replaceAll(" ", "-")}`,
            entries: backgroundExecTranscript.entries.slice(0, -1).map((entry) => {
              if (entry.type !== "emit_inbound") return entry;
              const frame = entry.frame;
              if (
                !Predicate.isObject(frame) ||
                frame.method !== "item/started" ||
                !Predicate.isObject(frame.params) ||
                !Predicate.isObject(frame.params.item) ||
                frame.params.item.type !== "commandExecution"
              )
                return entry;
              return {
                ...entry,
                frame: {
                  ...frame,
                  params: {
                    ...frame.params,
                    item: {
                      ...frame.params.item,
                      command,
                      commandActions: [{ type: "unknown", command }],
                    },
                  },
                },
              };
            }),
          });
          const harness = yield* makeCodexReplayHarness(
            transcript,
            undefined,
            undefined,
            undefined,
            {
              onNotification: (method, handler) => {
                if (method === "item/completed") completeItem = handler;
              },
            },
          );
          const now = yield* DateTime.now;
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now,
              attemptId: RunAttemptId.make("attempt-temporary-http-server-expiry"),
              text: BG_PROMPT,
            }),
          );
          yield* awaitUntil(() => harness.terminalEvents().length === 1, "root turn terminal");
          assert.isTrue(yield* harness.hasPendingBackgroundWork);
          if (completeItem === undefined)
            return yield* Effect.die("Missing item completion handler");
          yield* completeItem({
            item: {
              ...backgroundCommandItem("completed"),
              command,
              commandActions: [{ type: "unknown", command }],
              aggregatedOutput: output,
              status: "failed",
              exitCode: completion.exitCode,
              durationMs: 600_000,
            },
            threadId: BG_NATIVE_THREAD,
            turnId: BG_NATIVE_TURN,
            completedAtMs: 1782622465500,
          });
          yield* awaitUntil(
            () =>
              harness.events.some(
                (event) =>
                  event.type === "turn_item.updated" &&
                  event.turnItem.type === "command_execution" &&
                  event.turnItem.exitCode === completion.exitCode &&
                  event.turnItem.status === "failed" &&
                  event.turnItem.output === output,
              ),
            "raw server expiry recorded",
          );
          assert.isFalse(yield* harness.hasPendingBackgroundWork);
          assert.lengthOf(harness.continuationRequests, completion.wake ? 1 : 0);
          if (completion.wake) {
            assert.equal(harness.continuationRequests[0]?.notification?.outcome, "failed");
          }
          assert.lengthOf(harness.terminalEvents(), 1);
        }).pipe(
          Effect.provide(
            Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
          ),
        ),
      ),
  );

  const backgroundStopCases = [true, false, "still_running", "thread_unloaded"] as const;
  const makeBackgroundStopTranscript = (terminated: (typeof backgroundStopCases)[number]) => {
    const stillRunning = terminated === "still_running";
    return makeCodexReplayTranscript({
      scenario: `codex-bg-stop-${terminated}`,
      entries: [
        ...backgroundExecTranscript.entries.slice(0, -1),
        {
          type: "expect_outbound",
          label: "terminate-background-command",
          frame: {
            id: 4,
            method: "thread/backgroundTerminals/terminate",
            params: { threadId: BG_NATIVE_THREAD, processId: "4242" },
          },
        },
        {
          type: "emit_inbound",
          label: "terminate-background-command",
          frame:
            terminated === "thread_unloaded"
              ? {
                  id: 4,
                  error: { code: -32600, message: `thread not found: ${BG_NATIVE_THREAD}` },
                }
              : { id: 4, result: { terminated: terminated === true } },
        },
        ...(terminated === false || stillRunning
          ? [
              {
                type: "expect_outbound" as const,
                frame: {
                  id: 5,
                  method: "thread/backgroundTerminals/list",
                  params: { threadId: BG_NATIVE_THREAD },
                },
              },
              {
                type: "emit_inbound" as const,
                frame: {
                  id: 5,
                  result: { data: stillRunning ? [{ processId: "4242" }] : [], nextCursor: null },
                },
              },
            ]
          : []),
        ...(stillRunning
          ? [
              {
                type: "expect_outbound" as const,
                frame: {
                  id: 6,
                  method: "thread/backgroundTerminals/terminate",
                  params: { threadId: BG_NATIVE_THREAD, processId: "4242" },
                },
              },
              {
                type: "emit_inbound" as const,
                frame: { id: 6, result: { terminated: false } },
              },
              {
                type: "expect_outbound" as const,
                frame: {
                  id: 7,
                  method: "thread/backgroundTerminals/list",
                  params: { threadId: BG_NATIVE_THREAD },
                },
              },
              {
                type: "emit_inbound" as const,
                frame: { id: 7, result: { data: [{ processId: "4242" }], nextCursor: null } },
              },
              {
                type: "expect_outbound" as const,
                frame: {
                  id: 8,
                  method: "thread/backgroundTerminals/terminate",
                  params: { threadId: BG_NATIVE_THREAD, processId: "4242" },
                },
              },
              {
                type: "emit_inbound" as const,
                frame: { id: 8, result: { terminated: true } },
              },
            ]
          : []),
        backgroundExecTranscript.entries.at(-1)!,
      ],
    });
  };

  it.effect.each(backgroundStopCases)(
    "stops a command after root completion when termination returns %s",
    (terminated) => {
      const stillRunning = terminated === "still_running";
      const transcript = makeBackgroundStopTranscript(terminated);
      return Effect.scoped(
        Effect.gen(function* () {
          const stopped = yield* Deferred.make<void>();
          const harness = yield* makeCodexReplayHarness(transcript, (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.status === "interrupted"
              ? Deferred.succeed(stopped, undefined)
              : Effect.void,
          );
          const now = yield* DateTime.now;
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now,
              attemptId: RunAttemptId.make("attempt-codex-bg-stop"),
              text: BG_PROMPT,
            }),
          );
          yield* harness.firstTerminal;
          const terminal = harness.terminalEvents()[0]!;
          assert.equal(terminal.status, "completed");
          assert.equal(terminal.evidenceKind, "provider_result");
          assert.equal(
            terminal.providerTurn?.runAttemptId,
            RunAttemptId.make("attempt-codex-bg-stop"),
          );
          assert.equal(terminal.providerTurn?.id, terminal.providerTurnId);
          assert.equal(terminal.providerTurn?.status, "completed");
          assert.isNotNull(terminal.providerTurn?.completedAt);
          assert.isTrue(yield* harness.hasPendingBackgroundWork);
          assert.isFalse(
            yield* harness.runtime.hasPendingBackgroundWorkForThread!({
              ...harness.providerThread,
              id: ProviderThreadId.make("unrelated-provider-thread"),
            }),
          );
          if (stillRunning) {
            const failed = yield* harness.runtime
              .interruptTurn({
                providerThread: harness.providerThread,
                providerTurnId: terminal.providerTurnId,
                requestRuntimeRestart: true,
              })
              .pipe(Effect.exit);
            assert.equal(failed._tag, "Failure");
            assert.isTrue(yield* harness.hasPendingBackgroundWork);
          }
          yield* harness.runtime.interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId: stillRunning
              ? ProviderTurnId.make("later-completed-turn")
              : terminal.providerTurnId,
            requestRuntimeRestart: true,
          });
          yield* Deferred.await(stopped);
          assert.isFalse(yield* harness.hasPendingBackgroundWork);
          assert.lengthOf(harness.terminalEvents(), 1);
          assert.equal(harness.terminalEvents()[0]?.status, "completed");
          assert.lengthOf(harness.continuationRequests, 0);
        }).pipe(
          Effect.provide(
            Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
          ),
        ),
      );
    },
  );
  it.effect("stops a completed run's background command without a client-supplied run", () => {
    const transcript = makeBackgroundStopTranscript(true);
    return Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-bg-stop-workspace-" });
        const localTranscript = yield* decodeReplayTranscriptJson(
          (yield* encodeReplayTranscriptJson(transcript)).replaceAll(
            yield* encodeStringJson("/workspace"),
            yield* encodeStringJson(cwd),
          ),
        );
        const replayDriver = yield* CodexReplay.makeReplayDriver(localTranscript);
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        assert.equal(
          Number(yield* spawner.exitCode(ChildProcess.make("git", ["init", "--quiet"], { cwd }))),
          0,
        );
        assert.equal(
          Number(
            yield* spawner.exitCode(
              ChildProcess.make(
                "git",
                [
                  "-c",
                  "user.name=Test",
                  "-c",
                  "user.email=test@example.com",
                  "commit",
                  "--allow-empty",
                  "--quiet",
                  "-m",
                  "Initial commit",
                ],
                { cwd },
              ),
            ),
          ),
          0,
        );
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const threadId = ThreadId.make("thread:background-stop");
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-background-stop"),
            threadId,
            projectId: ProjectId.make("project:background-stop"),
            title: "Background stop",
            modelSelection: CODEX_TEST_MODEL_SELECTION,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          const waiting = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) => event.type === "run.updated" && event.payload.status === "waiting",
            ),
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("start-background-stop"),
            threadId,
            messageId: MessageId.make("message:background-stop"),
            text: BG_PROMPT,
            attachments: [],
            createdBy: "user",
            creationSource: "web",
            dispatchMode: { type: "start_immediately" },
          });
          yield* worker.drain();
          assert.isNull((yield* Ref.get(replayDriver.state)).failure);
          yield* Fiber.join(waiting);
          yield* worker.drain();
          const projection = yield* orchestrator.getThreadProjection(threadId);
          const run = projection.runs.at(-1)!;
          assert.equal(run.status, "completed");
          assert.equal(
            (yield* orchestrator.getThreadShell(threadId))?.pendingBackgroundTasks?.length,
            1,
          );
          const stopped = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "turn-item.updated" &&
                event.payload.type === "command_execution" &&
                event.payload.status === "interrupted",
            ),
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );
          yield* orchestrator.dispatch({
            type: "thread.stop",
            commandId: CommandId.make("stop-background-command"),
            threadId,
          });
          yield* worker.drain();
          assert.isNull((yield* Ref.get(replayDriver.state)).failure);
          yield* Fiber.join(stopped);
          assert.equal(
            (yield* orchestrator.getThreadProjection(threadId)).runs.at(-1)?.status,
            "completed",
          );
          assert.deepEqual(
            (yield* orchestrator.getThreadShell(threadId))?.pendingBackgroundTasks,
            [],
          );
        }).pipe(
          Effect.provide(
            ProviderReplayHarness.layerWithRegistry(
              { name: "codex-background-stop", runtimePolicyOverride: { cwd } },
              CodexAdapterV2Testkit.layer({
                transcript: localTranscript,
                driver: replayDriver,
              }),
              { runEffectWorker: false },
            ),
          ),
        );
      }).pipe(Effect.provide(NodeServices.layer)),
    );
  });

  it.effect(
    "persists one failed run after process exit mid-tool without starting another turn",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-process-exit-workspace-" });
          const end = backgroundExecTranscript.entries.findIndex(
            (entry) =>
              entry.type === "emit_inbound" && entry.label === "item/completed/root-answer",
          );
          const transcript = makeCodexReplayTranscript({
            scenario: "codex-mid-tool-process-exit",
            entries: [
              ...backgroundExecTranscript.entries.slice(0, end),
              { type: "runtime_exit", status: "success" },
            ],
          });
          const localTranscript = yield* decodeReplayTranscriptJson(
            (yield* encodeReplayTranscriptJson(transcript)).replaceAll(
              yield* encodeStringJson("/workspace"),
              yield* encodeStringJson(cwd),
            ),
          );
          const driver = yield* CodexReplay.makeReplayDriver(localTranscript);
          yield* Effect.gen(function* () {
            const orchestrator = yield* Orchestrator.OrchestratorV2;
            const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
            const threadId = ThreadId.make("thread:mid-tool-process-exit");
            yield* orchestrator.dispatch({
              type: "thread.create",
              commandId: CommandId.make("create-mid-tool-process-exit"),
              threadId,
              projectId: ProjectId.make("project:mid-tool-process-exit"),
              title: "Process exit",
              modelSelection: CODEX_TEST_MODEL_SELECTION,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: cwd,
              createdBy: "user",
              creationSource: "web",
            });
            const failed = yield* Deferred.make<void>();
            const released = yield* Deferred.make<void>();
            let failureEvents = 0;
            yield* orchestrator.streamDomainEvents.pipe(
              Stream.runForEach((event) => {
                if (event.type === "provider-session.updated" && event.payload.status === "error")
                  return Deferred.succeed(released, undefined);
                if (event.type !== "run.updated" || event.payload.status !== "failed")
                  return Effect.void;
                failureEvents++;
                return Deferred.succeed(failed, undefined);
              }),
              Effect.forkScoped,
            );
            yield* orchestrator.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make("start-mid-tool-process-exit"),
              threadId,
              messageId: MessageId.make("message:mid-tool-process-exit"),
              text: BG_PROMPT,
              attachments: [],
              createdBy: "user",
              creationSource: "web",
              dispatchMode: { type: "start_immediately" },
            });
            yield* worker.drain();
            yield* Deferred.await(failed);
            yield* Deferred.await(released);
            yield* worker.drain();
            const projection = yield* orchestrator.getThreadProjection(threadId);
            assert.equal(failureEvents, 1);
            assert.equal(projection.runs.length, 1);
            assert.equal(projection.runs[0]?.status, "failed");
            assert.isNotTrue(projection.runs[0]?.queueHeld);
            assert.deepEqual(
              projection.turnItems.flatMap((item) =>
                item.type === "error" ? [item.failure.code] : [],
              ),
              ["codexProcessExited"],
            );
            assert.deepEqual(
              projection.turnItems.flatMap((item) =>
                item.type === "command_execution" ? [item.status] : [],
              ),
              ["failed"],
            );
            assert.equal(projection.providerThreads[0]?.status, "idle");
            assert.equal(projection.providerSessions[0]?.status, "error");
            assert.deepEqual(
              yield* Ref.get(driver.state),
              { cursor: localTranscript.entries.length, failure: null },
              "strict replay accepts only the single recorded turn/start and no recovery traffic",
            );
          }).pipe(
            Effect.provide(
              ProviderReplayHarness.layerWithRegistry(
                { name: "codex-mid-tool-process-exit", runtimePolicyOverride: { cwd } },
                CodexAdapterV2Testkit.layer({ transcript: localTranscript, driver }),
                { runEffectWorker: false },
              ),
            ),
          );
        }).pipe(Effect.provide(NodeServices.layer)),
      ),
  );

  it.effect("Stop ends a background command no Codex process tracks any more", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-bg-stale-workspace-" });
        const staleTranscript = makeCodexReplayTranscript({
          scenario: "codex-bg-stop-untracked",
          entries: [
            ...backgroundExecTranscript.entries.slice(0, -1),
            { type: "runtime_exit", status: "success" },
          ],
        });
        const localTranscript = yield* decodeReplayTranscriptJson(
          (yield* encodeReplayTranscriptJson(staleTranscript)).replaceAll(
            yield* encodeStringJson("/workspace"),
            yield* encodeStringJson(cwd),
          ),
        );
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        for (const args of [
          ["init", "--quiet"],
          [
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "--allow-empty",
            "--quiet",
            "-m",
            "Initial commit",
          ],
        ]) {
          assert.equal(Number(yield* spawner.exitCode(ChildProcess.make("git", args, { cwd }))), 0);
        }
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const threadId = ThreadId.make("thread:background-stop-untracked");
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-background-stop-untracked"),
            threadId,
            projectId: ProjectId.make("project:background-stop-untracked"),
            title: "Background stop untracked",
            modelSelection: CODEX_TEST_MODEL_SELECTION,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          const settled = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                (event.payload.status === "waiting" || event.payload.status === "completed"),
            ),
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("start-background-stop-untracked"),
            threadId,
            messageId: MessageId.make("message:background-stop-untracked"),
            text: BG_PROMPT,
            attachments: [],
            createdBy: "user",
            creationSource: "web",
            dispatchMode: { type: "start_immediately" },
          });
          yield* worker.drain();
          yield* Fiber.join(settled);
          yield* worker.drain();
          const before = yield* orchestrator.getThreadShell(threadId);
          assert.deepEqual(
            before?.pendingBackgroundTasks?.map((task) => task.kind),
            ["command"],
            "the thread still shows the command the gone process never finished",
          );
          const run = (yield* orchestrator.getThreadProjection(threadId)).runs.at(-1)!;
          yield* orchestrator.dispatch({
            type: "run.interrupt",
            commandId: CommandId.make("stop-background-untracked"),
            threadId,
            runId: run.id,
            holdQueue: true,
          });
          yield* worker.drain();
          const projection = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(
            projection.turnItems.flatMap((item) =>
              item.type === "command_execution" ? [item.status] : [],
            ),
            ["interrupted"],
          );
          assert.equal(projection.runs.at(-1)?.status, "completed");
          assert.deepEqual(
            (yield* orchestrator.getThreadShell(threadId))?.pendingBackgroundTasks,
            [],
          );
        }).pipe(
          Effect.provide(
            ProviderReplayHarness.layerWithRegistry(
              { name: "codex-background-stop-untracked", runtimePolicyOverride: { cwd } },
              CodexAdapterV2Testkit.layer({ transcript: localTranscript }),
              { runEffectWorker: false },
            ),
          ),
        );
      }).pipe(Effect.provide(NodeServices.layer)),
    ),
  );

  const INTERRUPT_SCENARIO = "codex-interrupt-mid-command";
  const INTERRUPT_NATIVE_THREAD = "native-codex-interrupt-thread";
  const INTERRUPT_NATIVE_TURN = "native-codex-interrupt-turn";
  const INTERRUPT_COMMAND_ITEM = "exec-codex-interrupt-command";
  const INTERRUPT_COMMAND_ITEM_TWO = "exec-codex-interrupt-command-two";
  const INTERRUPT_CHILD_COMMAND_ITEM = "exec-codex-interrupt-child-command";
  const INTERRUPT_CHILD_NATIVE_THREAD = "native-codex-interrupt-child-thread";
  const INTERRUPT_CHILD_NATIVE_TURN = "native-codex-interrupt-child-turn";
  const INTERRUPT_LATE_CHILD_NATIVE_TURN = "native-codex-interrupt-late-child-turn";
  const INTERRUPT_COMMAND = "bash -c 'sleep 30; echo SHOULD_NOT_FINISH_CMD_INTERRUPT_FIXTURE'";
  const INTERRUPT_COMMAND_TWO = "bash -c 'sleep 20; echo SECOND_COMMAND'";
  const INTERRUPT_PROMPT = "Run a long foreground command and wait until interrupted.";

  const interruptCommandItem = (status: "inProgress" | "completed"): Record<string, unknown> => ({
    type: "commandExecution",
    id: INTERRUPT_COMMAND_ITEM,
    command: INTERRUPT_COMMAND,
    cwd: "/workspace",
    processId: "57680",
    source: "unifiedExecStartup",
    status,
    commandActions: [{ type: "unknown", command: INTERRUPT_COMMAND }],
    aggregatedOutput: status === "completed" ? "SHOULD_NOT_FINISH_CMD_INTERRUPT_FIXTURE\n" : null,
    exitCode: status === "completed" ? 0 : null,
    durationMs: status === "completed" ? 30_000 : null,
  });

  const interruptMidCommandTranscript = makeCodexReplayTranscript({
    scenario: INTERRUPT_SCENARIO,
    entries: [
      ...codexReplayPreamble({
        nativeThreadId: INTERRUPT_NATIVE_THREAD,
        nativeTurnId: INTERRUPT_NATIVE_TURN,
        prompt: INTERRUPT_PROMPT,
      }),
      {
        type: "emit_inbound",
        label: "item/started/command",
        frame: {
          method: "item/started",
          params: {
            item: interruptCommandItem("inProgress"),
            threadId: INTERRUPT_NATIVE_THREAD,
            turnId: INTERRUPT_NATIVE_TURN,
            startedAtMs: 1782622440500,
          },
        },
      },
      {
        type: "expect_outbound",
        label: "turn/interrupt",
        frame: {
          id: 4,
          method: "turn/interrupt",
          params: {
            threadId: INTERRUPT_NATIVE_THREAD,
            turnId: INTERRUPT_NATIVE_TURN,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/interrupt",
        frame: { id: 4, result: {} },
      },
      {
        type: "emit_inbound",
        label: "item/started/command-two-after-interrupt-response",
        frame: {
          method: "item/started",
          params: {
            item: {
              ...interruptCommandItem("inProgress"),
              id: INTERRUPT_COMMAND_ITEM_TWO,
              command: INTERRUPT_COMMAND_TWO,
              processId: "57681",
              commandActions: [{ type: "unknown", command: INTERRUPT_COMMAND_TWO }],
            },
            threadId: INTERRUPT_NATIVE_THREAD,
            turnId: INTERRUPT_NATIVE_TURN,
            startedAtMs: 1782622440600,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/completed",
        frame: {
          method: "turn/completed",
          params: {
            threadId: INTERRUPT_NATIVE_THREAD,
            turn: makeCodexReplayTurn({
              id: INTERRUPT_NATIVE_TURN,
              status: "interrupted",
            }),
          },
        },
      },
      {
        type: "expect_outbound",
        label: "thread/backgroundTerminals/terminate/one",
        frame: {
          id: 5,
          method: "thread/backgroundTerminals/terminate",
          params: { threadId: INTERRUPT_NATIVE_THREAD, processId: "57680" },
        },
      },
      {
        type: "emit_inbound",
        label: "thread/backgroundTerminals/terminate/one",
        frame: { id: 5, result: { terminated: false } },
      },
      {
        type: "expect_outbound",
        label: "thread/backgroundTerminals/list/after-false",
        frame: {
          id: 6,
          method: "thread/backgroundTerminals/list",
          params: { threadId: INTERRUPT_NATIVE_THREAD },
        },
      },
      {
        type: "emit_inbound",
        label: "thread/backgroundTerminals/list/after-false",
        frame: { id: 6, result: { data: [], nextCursor: null } },
      },
      {
        type: "expect_outbound",
        label: "thread/backgroundTerminals/terminate/two",
        frame: {
          id: 7,
          method: "thread/backgroundTerminals/terminate",
          params: { threadId: INTERRUPT_NATIVE_THREAD, processId: "57681" },
        },
      },
      {
        type: "emit_inbound",
        label: "thread/backgroundTerminals/terminate/two",
        frame: { id: 7, result: { terminated: true } },
      },
      {
        type: "emit_inbound",
        label: "item/completed/command-late",
        afterMs: 30_000,
        frame: {
          method: "item/completed",
          params: {
            item: interruptCommandItem("completed"),
            threadId: INTERRUPT_NATIVE_THREAD,
            turnId: INTERRUPT_NATIVE_TURN,
            completedAtMs: 1782622465500,
          },
        },
      },
    ],
  });

  it.effect("contains commands that start before and after the interrupt response", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeCodexReplayHarness(interruptMidCommandTranscript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-interrupt-mid-command"),
            text: INTERRUPT_PROMPT,
          }),
        );

        yield* harness.until(() =>
          harness.events.some(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "command_execution" &&
              event.turnItem.status === "running",
          ),
        );

        const providerTurnId = harness.events.find(
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "provider_turn.updated" }
          > => event.type === "provider_turn.updated",
        )?.providerTurn.id;
        assert.isDefined(providerTurnId);

        yield* harness.runtime.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId,
        });

        yield* harness.awaitTerminal(1);
        assert.equal(harness.terminalEvents()[0]?.status, "interrupted");

        const terminalIndex = harness.events.findIndex((event) => event.type === "turn.terminal");
        assert.isAtLeast(terminalIndex, 0);

        let lastCommandBeforeTerminal:
          | Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn_item.updated" }>
          | undefined;
        for (let index = 0; index < terminalIndex; index++) {
          const event = harness.events[index];
          if (event?.type === "turn_item.updated" && event.turnItem.type === "command_execution") {
            lastCommandBeforeTerminal = event;
          }
        }
        assert.isDefined(lastCommandBeforeTerminal);
        assert.equal(lastCommandBeforeTerminal.turnItem.status, "interrupted");
        assert.isNotNull(lastCommandBeforeTerminal.turnItem.completedAt);

        const interruptedCommandsBeforeTerminal = harness.events
          .slice(0, terminalIndex)
          .flatMap((event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.status === "interrupted"
              ? [event.turnItem.input]
              : [],
          )
          .sort();
        assert.deepEqual(
          interruptedCommandsBeforeTerminal,
          [INTERRUPT_COMMAND, INTERRUPT_COMMAND_TWO].sort(),
        );

        const interruptedCommandIndex = harness.events.findIndex(
          (event, index) =>
            index < terminalIndex &&
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.status === "interrupted",
        );
        assert.isAbove(
          terminalIndex,
          interruptedCommandIndex,
          "command terminalization must precede turn.terminal",
        );

        assert.isFalse(yield* harness.hasPendingBackgroundWork);
        assert.lengthOf(harness.continuationRequests, 0);

        yield* TestClock.adjust("30 seconds");
        for (let attempt = 0; attempt < 100; attempt++) {
          yield* Effect.yieldNow;
        }
        assert.lengthOf(harness.continuationRequests, 0);
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
        assert.lengthOf(harness.terminalEvents(), 1);

        const postTerminalCommandUpdates = harness.events.filter(
          (event, index) =>
            index > terminalIndex &&
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution",
        );
        assert.lengthOf(
          postTerminalCommandUpdates,
          0,
          "late item/completed after interrupt must not project",
        );

        const commandUpdates = harness.events.filter(
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "turn_item.updated" }
          > => event.type === "turn_item.updated" && event.turnItem.type === "command_execution",
        );
        assert.isAtLeast(commandUpdates.length, 2, "start + interrupt terminalization");
        assert.equal(commandUpdates[commandUpdates.length - 1]?.turnItem.status, "interrupted");
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  const interruptSubagentCommandTranscript = makeCodexReplayTranscript({
    scenario: "codex-interrupt-subagent-command",
    entries: [
      ...codexReplayPreamble({
        nativeThreadId: INTERRUPT_NATIVE_THREAD,
        nativeTurnId: INTERRUPT_NATIVE_TURN,
        prompt: INTERRUPT_PROMPT,
      }),
      {
        type: "emit_inbound",
        label: "item/completed/subAgentActivity-started",
        frame: {
          method: "item/completed",
          params: {
            item: {
              type: "subAgentActivity",
              id: "call-codex-interrupt-subagent",
              kind: "started",
              agentThreadId: INTERRUPT_CHILD_NATIVE_THREAD,
              agentPath: "/root/stop_hold",
            },
            threadId: INTERRUPT_NATIVE_THREAD,
            turnId: INTERRUPT_NATIVE_TURN,
            completedAtMs: 1782622441000,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/started/child",
        frame: {
          method: "turn/started",
          params: {
            threadId: INTERRUPT_CHILD_NATIVE_THREAD,
            turn: makeCodexReplayTurn({
              id: INTERRUPT_CHILD_NATIVE_TURN,
              status: "inProgress",
            }),
          },
        },
      },
      {
        type: "emit_inbound",
        label: "item/started/child-command",
        frame: {
          method: "item/started",
          params: {
            item: {
              ...interruptCommandItem("inProgress"),
              id: INTERRUPT_CHILD_COMMAND_ITEM,
              processId: "57682",
            },
            threadId: INTERRUPT_CHILD_NATIVE_THREAD,
            turnId: INTERRUPT_CHILD_NATIVE_TURN,
            startedAtMs: 1782622441500,
          },
        },
      },
      {
        type: "expect_outbound",
        label: "turn/interrupt/child",
        frame: {
          id: 4,
          method: "turn/interrupt",
          params: {
            threadId: INTERRUPT_CHILD_NATIVE_THREAD,
            turnId: INTERRUPT_CHILD_NATIVE_TURN,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/interrupt/child",
        frame: { id: 4, result: {} },
      },
      {
        type: "expect_outbound",
        label: "turn/interrupt/root",
        frame: {
          id: 5,
          method: "turn/interrupt",
          params: {
            threadId: INTERRUPT_NATIVE_THREAD,
            turnId: INTERRUPT_NATIVE_TURN,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/interrupt/root",
        frame: { id: 5, result: {} },
      },
      {
        type: "expect_outbound",
        label: "thread/backgroundTerminals/terminate/child",
        frame: {
          id: 6,
          method: "thread/backgroundTerminals/terminate",
          params: { threadId: INTERRUPT_CHILD_NATIVE_THREAD, processId: "57682" },
        },
      },
      {
        type: "emit_inbound",
        label: "thread/backgroundTerminals/terminate/child",
        frame: { id: 6, result: { terminated: true } },
      },
      {
        type: "emit_inbound",
        label: "turn/completed/root",
        frame: {
          method: "turn/completed",
          params: {
            threadId: INTERRUPT_NATIVE_THREAD,
            turn: makeCodexReplayTurn({ id: INTERRUPT_NATIVE_TURN, status: "interrupted" }),
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/completed/child-completed-race",
        frame: {
          method: "turn/completed",
          params: {
            threadId: INTERRUPT_CHILD_NATIVE_THREAD,
            turn: makeCodexReplayTurn({
              id: INTERRUPT_CHILD_NATIVE_TURN,
              status: "completed",
            }),
          },
        },
      },
      { type: "runtime_exit", status: "success" },
    ],
  });

  const assertChildProviderTerminalBeforeRoot = (
    events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>,
    rootThreadId: ThreadId,
    status: "interrupted" | "failed" = "interrupted",
  ) => {
    const terminalIndex = events.findIndex((event) => event.type === "turn.terminal");
    const childProviderTurnIndex = events.findIndex(
      (event) =>
        event.type === "provider_turn.updated" &&
        event.threadId !== rootThreadId &&
        event.providerTurn.status === status,
    );
    const childProviderThreadIndex = events.findIndex(
      (event) =>
        event.type === "provider_thread.updated" &&
        event.providerThread.appThreadId !== rootThreadId &&
        event.providerThread.status === "idle",
    );
    assert.isAtLeast(childProviderTurnIndex, 0, "child provider turn must terminalize");
    assert.isAtLeast(childProviderThreadIndex, 0, "child provider thread must become idle");
    assert.isAbove(
      terminalIndex,
      childProviderTurnIndex,
      "child provider turn must terminalize before the root run",
    );
    assert.isAbove(
      terminalIndex,
      childProviderThreadIndex,
      "child provider thread must become idle before the root run",
    );
  };

  it.effect("contains descendant commands and keeps Stop authoritative", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeCodexReplayHarness(interruptSubagentCommandTranscript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-interrupt-subagent-command"),
            text: INTERRUPT_PROMPT,
          }),
        );
        yield* harness.until(() =>
          harness.events.some(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "command_execution" &&
              event.turnItem.nativeItemRef?.nativeId === INTERRUPT_CHILD_COMMAND_ITEM &&
              event.turnItem.status === "running",
          ),
        );
        const providerTurnId = harness.events.find(
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "provider_turn.updated" }
          > => event.type === "provider_turn.updated" && event.threadId === harness.threadId,
        )?.providerTurn.id;
        assert.isDefined(providerTurnId);

        yield* harness.runtime.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId,
        });

        yield* harness.awaitTerminal(1);
        assert.equal(harness.terminalEvents()[0]?.status, "interrupted");
        const childCommandUpdates = harness.events.filter(
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "turn_item.updated" }
          > =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.nativeItemRef?.nativeId === INTERRUPT_CHILD_COMMAND_ITEM,
        );
        assert.equal(childCommandUpdates.at(-1)?.turnItem.status, "interrupted");
        assert.equal(harness.subagentUpdates().at(-1)?.subagent.status, "interrupted");
        assertChildProviderTerminalBeforeRoot(harness.events, harness.threadId);
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  const rootCompletionIndex = interruptSubagentCommandTranscript.entries.findIndex(
    (entry) => entry.type === "emit_inbound" && entry.label === "turn/completed/root",
  );
  const interruptLateSubagentTurnTranscript = makeCodexReplayTranscript({
    scenario: "codex-interrupt-late-subagent-turn",
    entries: [
      ...interruptSubagentCommandTranscript.entries.slice(0, rootCompletionIndex),
      {
        type: "emit_inbound",
        label: "turn/started/late-child",
        frame: {
          method: "turn/started",
          params: {
            threadId: INTERRUPT_CHILD_NATIVE_THREAD,
            turn: makeCodexReplayTurn({
              id: INTERRUPT_LATE_CHILD_NATIVE_TURN,
              status: "inProgress",
            }),
          },
        },
      },
      ...interruptSubagentCommandTranscript.entries.slice(rootCompletionIndex, -1),
      {
        type: "expect_outbound",
        label: "turn/interrupt/late-child",
        frame: {
          id: 7,
          method: "turn/interrupt",
          params: {
            threadId: INTERRUPT_CHILD_NATIVE_THREAD,
            turnId: INTERRUPT_LATE_CHILD_NATIVE_TURN,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/interrupt/late-child",
        frame: { id: 7, result: {} },
      },
      { type: "runtime_exit", status: "success" },
    ],
  });

  it.effect("interrupts descendants that start after the initial Stop snapshot", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeCodexReplayHarness(interruptLateSubagentTurnTranscript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-interrupt-late-subagent-turn"),
            text: INTERRUPT_PROMPT,
          }),
        );
        yield* harness.until(() =>
          harness.events.some(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "command_execution" &&
              event.turnItem.nativeItemRef?.nativeId === INTERRUPT_CHILD_COMMAND_ITEM &&
              event.turnItem.status === "running",
          ),
        );
        const providerTurnId = harness.events.find(
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "provider_turn.updated" }
          > => event.type === "provider_turn.updated" && event.threadId === harness.threadId,
        )?.providerTurn.id;
        assert.isDefined(providerTurnId);

        const interruptFiber = yield* harness.runtime
          .interruptTurn({ providerThread: harness.providerThread, providerTurnId })
          .pipe(Effect.forkScoped);
        yield* harness.until(() =>
          harness.events.some(
            (event) =>
              event.type === "provider_turn.updated" &&
              event.providerTurn.nativeTurnRef?.nativeId === INTERRUPT_LATE_CHILD_NATIVE_TURN,
          ),
        );
        yield* Fiber.join(interruptFiber);

        yield* harness.awaitTerminal(1);
        const lateChildUpdates = harness.events.filter(
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "provider_turn.updated" }
          > =>
            event.type === "provider_turn.updated" &&
            event.providerTurn.nativeTurnRef?.nativeId === INTERRUPT_LATE_CHILD_NATIVE_TURN,
        );
        assert.equal(lateChildUpdates.at(-1)?.providerTurn.status, "interrupted");
        assertChildProviderTerminalBeforeRoot(harness.events, harness.threadId);
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect.each([
    {
      name: "usage",
      code: "usageLimitExceeded",
      notification: false,
      expectedClass: "usage_limit",
    },
    {
      name: "replacement",
      code: "usageLimitExceeded",
      notification: true,
      expectedClass: "provider_error",
    },
    {
      name: "late-reset",
      code: "usageLimitExceeded",
      notification: false,
      expectedClass: "usage_limit",
    },
    {
      name: "matching-details",
      code: "usageLimitExceeded",
      notification: true,
      expectedClass: "usage_limit",
    },
    {
      name: "deferred-reset",
      code: "usageLimitExceeded",
      notification: false,
      expectedClass: "usage_limit",
    },
    { name: "retry", code: "usageLimitExceeded", notification: true, expectedClass: "usage_limit" },
    {
      name: "exhausted-429",
      code: "responseTooManyFailedAttempts",
      errorInfo: { responseTooManyFailedAttempts: { httpStatusCode: 429 } },
      notification: false,
      expectedClass: "usage_limit",
    },
    {
      name: "exhausted-500",
      code: "responseTooManyFailedAttempts",
      errorInfo: { responseTooManyFailedAttempts: { httpStatusCode: 500 } },
      notification: false,
      expectedClass: "provider_error",
    },
  ] as const)("classifies Codex terminal failures from $name evidence", (scenario) =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = `native-limit-${scenario.name}`;
        const nativeTurnId = `turn-limit-${scenario.name}`;
        const message = "Provider stopped this request.";
        const errorInfo = "errorInfo" in scenario ? scenario.errorInfo : scenario.code;
        const resetAt = "2033-05-19T07:20:00.000Z";
        const snapshot = {
          type: "emit_inbound" as const,
          label: "account/rateLimits/updated",
          frame: {
            method: "account/rateLimits/updated",
            params: {
              rateLimits: {
                limitId: "codex",
                primary: { usedPercent: 100, resetsAt: 2000100000, windowDurationMins: 300 },
              },
            },
          },
        };
        const transcript = makeCodexReplayTranscript({
          scenario: `codex-limit-${scenario.name}`,
          entries: [
            ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Continue." }),
            ...(scenario.name === "deferred-reset" ? [snapshot] : []),
            ...(scenario.name === "deferred-reset"
              ? [
                  {
                    type: "emit_inbound" as const,
                    label: "item/completed/subAgentActivity-started",
                    frame: {
                      method: "item/completed",
                      params: {
                        threadId: nativeThreadId,
                        turnId: nativeTurnId,
                        item: {
                          type: "subAgentActivity",
                          id: "limit-child-spawn",
                          kind: "started",
                          agentThreadId: "native-limit-child",
                          agentPath: "/root/limit_child",
                        },
                      },
                    },
                  },
                  {
                    type: "emit_inbound" as const,
                    label: "turn/started/child",
                    frame: {
                      method: "turn/started",
                      params: {
                        threadId: "native-limit-child",
                        turn: makeCodexReplayTurn({
                          id: "limit-child-turn",
                          status: "inProgress",
                        }),
                      },
                    },
                  },
                ]
              : []),
            ...(scenario.notification
              ? [
                  {
                    type: "emit_inbound" as const,
                    label: "error",
                    frame: {
                      method: "error",
                      params: {
                        threadId: nativeThreadId,
                        turnId: nativeTurnId,
                        willRetry: scenario.name === "retry",
                        error: {
                          message,
                          codexErrorInfo: errorInfo,
                          additionalDetails:
                            scenario.name === "matching-details"
                              ? "Detailed provider allowance explanation."
                              : null,
                        },
                      },
                    },
                  },
                ]
              : []),
            {
              type: "emit_inbound",
              label: "turn/completed",
              frame: {
                method: "turn/completed",
                params: {
                  threadId: nativeThreadId,
                  turn: {
                    ...makeCodexReplayTurn({ id: nativeTurnId, status: "failed" }),
                    error: {
                      message: scenario.name === "replacement" ? "A different failure." : message,
                      ...(scenario.notification && scenario.name !== "matching-details"
                        ? {}
                        : { codexErrorInfo: errorInfo }),
                    },
                  },
                },
              },
            },
            ...(scenario.name === "late-reset" ? [snapshot] : []),
            ...(scenario.name === "deferred-reset"
              ? [
                  {
                    ...snapshot,
                    frame: {
                      method: "account/rateLimits/updated",
                      params: {
                        rateLimits: {
                          limitId: "codex",
                          primary: {
                            usedPercent: 100,
                            resetsAt: 2000200000,
                            windowDurationMins: 300,
                          },
                        },
                      },
                    },
                  },
                  {
                    type: "emit_inbound" as const,
                    label: "turn/completed/child",
                    frame: {
                      method: "turn/completed",
                      params: {
                        threadId: "native-limit-child",
                        turn: makeCodexReplayTurn({
                          id: "limit-child-turn",
                          status: "completed",
                        }),
                      },
                    },
                  },
                ]
              : []),
          ],
        });
        const resetReceipt = yield* Deferred.make<void>();
        const harness = yield* makeCodexReplayHarness(transcript, (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "error" &&
          event.turnItem.failure.resetAt === resetAt
            ? Deferred.succeed(resetReceipt, undefined)
            : Effect.void,
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            text: "Continue.",
            attemptId: RunAttemptId.make(`attempt-limit-${scenario.name}`),
          }),
        );
        yield* harness.firstTerminal;
        const terminal = harness.terminalEvents()[0];
        assert.equal(terminal?.status, "failed");
        if (terminal?.status !== "failed") return;
        assert.equal(terminal.failure.class, scenario.expectedClass);
        assert.equal(terminal.threadDisposition, "reusable");
        if (scenario.name === "deferred-reset") assert.equal(terminal.failure.resetAt, resetAt);
        if (scenario.name === "matching-details")
          assert.equal(terminal.failure.message, "Detailed provider allowance explanation.");
        if (scenario.name === "late-reset") {
          yield* Deferred.await(resetReceipt);
          const item = harness.events.find(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "error" &&
              event.turnItem.failure.resetAt === resetAt,
          );
          assert.isDefined(item);
        }
        if (scenario.name === "retry") assert.equal(terminal.retry?.attempt, 1);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  // regression: #17501 Codex 429 retry exhaustion classifies as a usage limit
  it.effect.each([
    {
      notificationStatus: 500,
      completionStatus: 429,
      willRetry: false,
      expectedClass: "usage_limit",
    },
    {
      notificationStatus: 429,
      completionStatus: 500,
      willRetry: true,
      expectedClass: "provider_error",
    },
    {
      notificationStatus: 429,
      completionStatus: null,
      willRetry: false,
      expectedClass: "usage_limit",
    },
    {
      notificationStatus: 429,
      completionStatus: null,
      willRetry: true,
      expectedClass: "usage_limit",
    },
  ] as const)(
    "honors completion HTTP status from $notificationStatus to $completionStatus (retry: $willRetry)",
    ({ notificationStatus, completionStatus, expectedClass, willRetry }) =>
      Effect.scoped(
        Effect.gen(function* () {
          const name = `${notificationStatus}-${completionStatus}-${willRetry}`;
          const nativeThreadId = `native-exhaustion-status-${name}`;
          const nativeTurnId = `turn-exhaustion-status-${name}`;
          const message = "Provider stopped this request.";
          const additionalDetails = "Detailed notification evidence.";
          const transcript = makeCodexReplayTranscript({
            scenario: `codex-exhaustion-status-${name}`,
            entries: [
              ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Continue." }),
              {
                type: "emit_inbound",
                label: "error",
                frame: {
                  method: "error",
                  params: {
                    threadId: nativeThreadId,
                    turnId: nativeTurnId,
                    willRetry,
                    error: {
                      message,
                      codexErrorInfo: {
                        responseTooManyFailedAttempts: { httpStatusCode: notificationStatus },
                      },
                      additionalDetails,
                    },
                  },
                },
              },
              {
                type: "emit_inbound",
                label: "turn/completed",
                frame: {
                  method: "turn/completed",
                  params: {
                    threadId: nativeThreadId,
                    turn: {
                      ...makeCodexReplayTurn({ id: nativeTurnId, status: "failed" }),
                      error: {
                        message,
                        codexErrorInfo: {
                          responseTooManyFailedAttempts: { httpStatusCode: completionStatus },
                        },
                      },
                    },
                  },
                },
              },
            ],
          });
          const harness = yield* makeCodexReplayHarness(transcript);
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              text: "Continue.",
              attemptId: RunAttemptId.make(`attempt-exhaustion-status-${name}`),
            }),
          );
          yield* harness.firstTerminal;
          const terminal = harness.terminalEvents()[0];
          assert.equal(terminal?.status, "failed");
          if (terminal?.status !== "failed") return;
          assert.equal(terminal.failure.class, expectedClass);
          assert.equal(terminal.failure.code, "responseTooManyFailedAttempts");
          assert.equal(terminal.threadDisposition, "reusable");
          const notificationClass = notificationStatus === 429 ? "usage_limit" : "provider_error";
          assert.equal(
            terminal.failure.message,
            expectedClass === notificationClass ? additionalDetails : message,
          );
          if (willRetry) assert.equal(terminal.retry?.attempt, 1);
        }).pipe(
          Effect.provide(
            Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
          ),
        ),
      ),
  );

  const FAILED_SCENARIO = "codex-failed-mid-command";
  const FAILED_NATIVE_THREAD = "native-codex-failed-thread";
  const FAILED_NATIVE_TURN = "native-codex-failed-turn";
  const FAILED_COMMAND_ITEM = "exec-codex-failed-command";
  const FAILED_COMMAND = "sleep 30";
  const FAILED_PROMPT = "Run a command that will be abandoned when the turn fails.";

  const failedMidCommandTranscript = makeCodexReplayTranscript({
    scenario: FAILED_SCENARIO,
    entries: [
      ...codexReplayPreamble({
        nativeThreadId: FAILED_NATIVE_THREAD,
        nativeTurnId: FAILED_NATIVE_TURN,
        prompt: FAILED_PROMPT,
      }),
      {
        type: "emit_inbound",
        label: "item/started/command",
        frame: {
          method: "item/started",
          params: {
            item: {
              type: "commandExecution",
              id: FAILED_COMMAND_ITEM,
              command: FAILED_COMMAND,
              cwd: "/workspace",
              processId: "99",
              source: "unifiedExecStartup",
              status: "inProgress",
              commandActions: [{ type: "unknown", command: FAILED_COMMAND }],
              aggregatedOutput: null,
              exitCode: null,
              durationMs: null,
            },
            threadId: FAILED_NATIVE_THREAD,
            turnId: FAILED_NATIVE_TURN,
            startedAtMs: 1782622440500,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/completed",
        frame: {
          method: "turn/completed",
          params: {
            threadId: FAILED_NATIVE_THREAD,
            turn: {
              ...makeCodexReplayTurn({
                id: FAILED_NATIVE_TURN,
                status: "failed",
              }),
              error: { message: "provider failed mid-command" },
            },
          },
        },
      },
    ],
  });

  it.effect("terminalizes running command items before turn.terminal on failed turns", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeCodexReplayHarness(failedMidCommandTranscript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-failed-mid-command"),
            text: FAILED_PROMPT,
          }),
        );
        yield* harness.awaitTerminal(1);
        assert.equal(harness.terminalEvents()[0]?.status, "failed");

        const terminalIndex = harness.events.findIndex((event) => event.type === "turn.terminal");
        const failedCommandIndex = harness.events.findIndex(
          (event, index) =>
            index < terminalIndex &&
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.status === "failed",
        );
        assert.isAtLeast(failedCommandIndex, 0);
        assert.isAbove(
          terminalIndex,
          failedCommandIndex,
          "failed-turn command terminalization must precede turn.terminal",
        );
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
        assert.lengthOf(harness.continuationRequests, 0);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  const MCP_APP_SCENARIO = "codex-mcp-app-capture";
  const MCP_APP_NATIVE_THREAD = "native-codex-mcp-app-thread";
  const MCP_APP_NATIVE_TURN = "native-codex-mcp-app-turn";
  const MCP_APP_ITEM = "mcp-weather-call";
  const MCP_APP_PROMPT = "Show the weather in Oslo.";
  const MCP_APP_RESOURCE = "ui://weather/dashboard";
  const MCP_APP_HTML = "<!doctype html><html><body><p>Weather</p><script>1</script></body></html>";
  const mcpAppToolItem = (status: "inProgress" | "completed") => ({
    type: "mcpToolCall",
    id: MCP_APP_ITEM,
    server: "weather",
    tool: "get_weather",
    status,
    arguments: { city: "Oslo" },
    mcpAppResourceUri: MCP_APP_RESOURCE,
    ...(status === "completed"
      ? { result: { content: [{ type: "text", text: "Sunny" }], structuredContent: { temp: 21 } } }
      : {}),
  });

  const mcpAppTranscript = makeCodexReplayTranscript({
    scenario: MCP_APP_SCENARIO,
    entries: [
      ...codexReplayPreamble({
        nativeThreadId: MCP_APP_NATIVE_THREAD,
        nativeTurnId: MCP_APP_NATIVE_TURN,
        prompt: MCP_APP_PROMPT,
      }),
      {
        type: "emit_inbound",
        label: "item/started/app-tool",
        frame: {
          method: "item/started",
          params: {
            item: mcpAppToolItem("inProgress"),
            threadId: MCP_APP_NATIVE_THREAD,
            turnId: MCP_APP_NATIVE_TURN,
            startedAtMs: 1782622440500,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "item/completed/app-tool",
        frame: {
          method: "item/completed",
          params: {
            item: mcpAppToolItem("completed"),
            threadId: MCP_APP_NATIVE_THREAD,
            turnId: MCP_APP_NATIVE_TURN,
            completedAtMs: 1782622441500,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/completed",
        frame: {
          method: "turn/completed",
          params: {
            threadId: MCP_APP_NATIVE_THREAD,
            turn: makeCodexReplayTurn({ id: MCP_APP_NATIVE_TURN, status: "completed" }),
          },
        },
      },
      {
        type: "expect_outbound",
        label: "mcpServer/resource/read",
        frame: {
          id: 4,
          method: "mcpServer/resource/read",
          params: { threadId: MCP_APP_NATIVE_THREAD, server: "weather", uri: MCP_APP_RESOURCE },
        },
      },
      {
        type: "emit_inbound",
        label: "mcpServer/resource/read",
        frame: {
          id: 4,
          result: {
            contents: [
              {
                uri: MCP_APP_RESOURCE,
                mimeType: "text/html;profile=mcp-app",
                text: MCP_APP_HTML,
                _meta: { ui: { csp: { connectDomains: ["https://api.weather.test"] } } },
              },
            ],
          },
        },
      },
    ],
  });

  it.effect("captures an MCP app's resource and holds the turn open until it lands", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const appCaptured = yield* Deferred.make<OrchestrationV2TurnItem>();
        const harness = yield* makeCodexReplayHarness(mcpAppTranscript, (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "dynamic_tool" &&
          event.turnItem.status === "completed"
            ? Deferred.succeed(appCaptured, event.turnItem)
            : Effect.void,
        );
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-mcp-app"),
            text: MCP_APP_PROMPT,
          }),
        );
        const item = yield* Deferred.await(appCaptured);
        assert.equal(harness.terminalEvents()[0]?.status, "completed");

        const completedRows = harness.events.filter(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "dynamic_tool" &&
            event.turnItem.status === "completed",
        );
        assert.lengthOf(completedRows, 1);

        assert.equal(item.type, "dynamic_tool");
        const output = item.type === "dynamic_tool" ? (item.output as Record<string, unknown>) : {};
        const reference = readMcpAppReference(output[MCP_APP_OUTPUT_KEY]);
        assert.deepEqual(
          { ...reference, attachmentId: undefined },
          {
            attachmentId: undefined,
            server: "weather",
            tool: "get_weather",
            resourceUri: MCP_APP_RESOURCE,
            csp: { connectDomains: ["https://api.weather.test"] },
          },
        );
        assert.deepEqual(output.result, {
          content: [{ type: "text", text: "Sunny" }],
          structuredContent: { temp: 21 },
        });

        const fileSystem = yield* FileSystem.FileSystem;
        const stored = yield* fileSystem.readFileString(
          resolveAttachmentPathById({
            attachmentsDir: harness.serverConfig.attachmentsDir,
            attachmentId: reference!.attachmentId,
          })!,
        );
        assert.include(stored, "connect-src https://api.weather.test");
        assert.isBelow(stored.indexOf("Content-Security-Policy"), stored.indexOf("<script>"));
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  const ORPHAN_WAIT_SCENARIO = "codex-orphaned-dynamic-tool";
  const ORPHAN_WAIT_NATIVE_THREAD = "native-codex-orphan-wait-thread";
  const ORPHAN_WAIT_NATIVE_TURN = "native-codex-orphan-wait-turn";
  const ORPHAN_WAIT_ITEM = "exec-4669f3bb-78c9-4af1-b44e-daa340d2c538";
  const PERSISTENT_MONITOR_ITEM = "exec-persistent-monitor";
  const COMPLETED_WAIT_ITEM = "exec-completed-wait";
  const ORPHAN_WAIT_PROMPT = "Wait on two nested tasks, then finish.";

  const orphanedDynamicToolTranscript = makeCodexReplayTranscript({
    scenario: ORPHAN_WAIT_SCENARIO,
    entries: [
      ...codexReplayPreamble({
        nativeThreadId: ORPHAN_WAIT_NATIVE_THREAD,
        nativeTurnId: ORPHAN_WAIT_NATIVE_TURN,
        prompt: ORPHAN_WAIT_PROMPT,
      }),
      {
        type: "emit_inbound",
        label: "item/started/completed-wait",
        frame: {
          method: "item/started",
          params: {
            item: {
              type: "mcpToolCall",
              id: COMPLETED_WAIT_ITEM,
              server: "t3-code",
              tool: "t3_thread_wait",
              status: "inProgress",
              arguments: { threadId: "thread:completed-wait", timeoutMs: 30000 },
            },
            threadId: ORPHAN_WAIT_NATIVE_THREAD,
            turnId: ORPHAN_WAIT_NATIVE_TURN,
            startedAtMs: 1782622440500,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "item/completed/completed-wait",
        frame: {
          method: "item/completed",
          params: {
            item: {
              type: "mcpToolCall",
              id: COMPLETED_WAIT_ITEM,
              server: "t3-code",
              tool: "t3_thread_wait",
              status: "completed",
              arguments: { threadId: "thread:completed-wait", timeoutMs: 30000 },
              result: { content: [{ type: "text", text: "idle" }] },
            },
            threadId: ORPHAN_WAIT_NATIVE_THREAD,
            turnId: ORPHAN_WAIT_NATIVE_TURN,
            completedAtMs: 1782622441500,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "item/started/orphan-wait",
        frame: {
          method: "item/started",
          params: {
            item: {
              type: "mcpToolCall",
              id: ORPHAN_WAIT_ITEM,
              server: "t3-code",
              tool: "t3_thread_wait",
              status: "inProgress",
              arguments: {
                threadId:
                  "thread:delegated-task:command%3Amcp%3Aaafffab1-e811-458a-ae83-558e542c61ff%3Adelegate-task%3Areview-mobile-reconnect-opus-20260815",
                timeoutMs: 30000,
              },
            },
            threadId: ORPHAN_WAIT_NATIVE_THREAD,
            turnId: ORPHAN_WAIT_NATIVE_TURN,
            startedAtMs: 1782622442500,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "item/started/persistent-monitor",
        frame: {
          method: "item/started",
          params: {
            item: {
              type: "dynamicToolCall",
              id: PERSISTENT_MONITOR_ITEM,
              namespace: "grok",
              tool: "monitor",
              status: "inProgress",
              arguments: { persistent: true, command: "tail -f" },
            },
            threadId: ORPHAN_WAIT_NATIVE_THREAD,
            turnId: ORPHAN_WAIT_NATIVE_TURN,
            startedAtMs: 1782622443500,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/completed",
        frame: {
          method: "turn/completed",
          params: {
            threadId: ORPHAN_WAIT_NATIVE_THREAD,
            turn: makeCodexReplayTurn({
              id: ORPHAN_WAIT_NATIVE_TURN,
              status: "completed",
            }),
          },
        },
      },
      {
        type: "emit_inbound",
        label: "item/completed/persistent-monitor",
        afterMs: 30_000,
        frame: {
          method: "item/completed",
          params: {
            item: {
              type: "dynamicToolCall",
              id: PERSISTENT_MONITOR_ITEM,
              namespace: "grok",
              tool: "monitor",
              status: "completed",
              arguments: { persistent: true, command: "tail -f" },
              result: { content: [{ type: "text", text: "stopped" }] },
            },
            threadId: ORPHAN_WAIT_NATIVE_THREAD,
            turnId: ORPHAN_WAIT_NATIVE_TURN,
            completedAtMs: 1782622473500,
          },
        },
      },
    ],
  });

  it.effect(
    "terminalizes leftover nonpersistent dynamic tools when a completed turn never closes them",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const monitorCompleted = yield* Deferred.make<void>();
          const harness = yield* makeCodexReplayHarness(orphanedDynamicToolTranscript, (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "dynamic_tool" &&
            event.turnItem.nativeItemRef?.nativeId === PERSISTENT_MONITOR_ITEM &&
            event.turnItem.status === "completed"
              ? Deferred.succeed(monitorCompleted, undefined)
              : Effect.void,
          );
          const now = yield* DateTime.now;

          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now,
              attemptId: RunAttemptId.make("attempt-codex-orphan-wait"),
              text: ORPHAN_WAIT_PROMPT,
            }),
          );
          yield* harness.firstTerminal;
          assert.equal(harness.terminalEvents()[0]?.status, "completed");

          const terminalIndex = harness.events.findIndex((event) => event.type === "turn.terminal");
          const cancelledWait = harness.events.find(
            (event, index) =>
              index < terminalIndex &&
              event.type === "turn_item.updated" &&
              event.turnItem.type === "dynamic_tool" &&
              event.turnItem.nativeItemRef?.nativeId === ORPHAN_WAIT_ITEM &&
              event.turnItem.status === "cancelled",
          );
          assert.isDefined(cancelledWait);
          assert.isAbove(
            terminalIndex,
            harness.events.indexOf(cancelledWait!),
            "orphaned wait terminalization must precede turn.terminal",
          );

          const completedWaitStatuses = new Set(
            harness.events.flatMap((event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "dynamic_tool" &&
              event.turnItem.nativeItemRef?.nativeId === COMPLETED_WAIT_ITEM
                ? [event.turnItem.status]
                : [],
            ),
          );
          assert.isTrue(
            completedWaitStatuses.has("completed"),
            "the wait that received item/completed must stay completed",
          );
          assert.isFalse(
            completedWaitStatuses.has("cancelled"),
            "a completed wait must not be rewritten as cancelled",
          );

          const persistentMonitorStatuses = new Set(
            harness.events.flatMap((event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "dynamic_tool" &&
              event.turnItem.nativeItemRef?.nativeId === PERSISTENT_MONITOR_ITEM
                ? [event.turnItem.status]
                : [],
            ),
          );
          assert.isTrue(persistentMonitorStatuses.has("running"));
          assert.isFalse(
            persistentMonitorStatuses.has("cancelled"),
            "persistent monitors must remain running after the root turn completes",
          );
          assert.isTrue(
            yield* harness.hasPendingBackgroundWork,
            "persistent dynamic tools must keep the session residency pin until they complete",
          );

          if (harness.runtime.readThreadActivity === undefined)
            return yield* Effect.die("Missing Codex activity port");
          assert.equal(
            (yield* harness.runtime.readThreadActivity(harness.providerThread)).status,
            "monitoring",
          );
          assert.equal(
            (yield* harness.runtime.readThreadActivity({
              ...harness.providerThread,
              runtimeIdentity: undefined,
            })).status,
            "unknown",
          );
          yield* TestClock.adjust("30 seconds");
          yield* Deferred.await(monitorCompleted);
          const lateMonitorUpdateIndex = harness.events.findIndex(
            (event, index) =>
              index > terminalIndex &&
              event.type === "turn_item.updated" &&
              event.turnItem.type === "dynamic_tool" &&
              event.turnItem.nativeItemRef?.nativeId === PERSISTENT_MONITOR_ITEM &&
              event.turnItem.status === "completed",
          );
          assert.isAbove(
            lateMonitorUpdateIndex,
            terminalIndex,
            "persistent tool completion must follow turn.terminal",
          );
          assert.lengthOf(harness.terminalEvents(), 1);
          assert.isFalse(yield* harness.hasPendingBackgroundWork);
        }).pipe(
          Effect.provide(
            Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
          ),
        ),
      ),
  );

  const RESUME_SCENARIO = "codex-resume-subagent";
  const RESUME_NATIVE_THREAD = "native-codex-resume-thread";
  const RESUME_NATIVE_TURN = "native-codex-resume-root-turn";
  const RESUME_CHILD_THREAD = "native-codex-resume-child-thread";
  const RESUME_CHILD_TURN_1 = "native-codex-resume-child-turn-1";
  const RESUME_CHILD_TURN_2 = "native-codex-resume-child-turn-2";
  const RESUME_PROMPT = "Spawn a sub-agent, nudge it, and reply NUDGED.";

  const childAgentMessage = (input: {
    readonly id: string;
    readonly text: string;
    readonly turnId: string;
    readonly completedAtMs: number;
    readonly afterMs?: number;
    readonly omitPhase?: boolean;
  }): CodexReplay.CodexAppServerReplayEntry => ({
    type: "emit_inbound",
    label: `item/completed/${input.id}`,
    ...(input.afterMs === undefined ? {} : { afterMs: input.afterMs }),
    frame: {
      method: "item/completed",
      params: {
        item: {
          type: "agentMessage",
          id: input.id,
          text: input.text,
          ...(input.omitPhase ? {} : { phase: "final_answer" as const }),
          memoryCitation: null,
        },
        threadId: RESUME_CHILD_THREAD,
        turnId: input.turnId,
        completedAtMs: input.completedAtMs,
      },
    },
  });

  const childTurnStarted = (
    turnId: string,
    afterMs?: number,
  ): CodexReplay.CodexAppServerReplayEntry => ({
    type: "emit_inbound",
    label: `turn/started/${turnId}`,
    ...(afterMs === undefined ? {} : { afterMs }),
    frame: {
      method: "turn/started",
      params: {
        threadId: RESUME_CHILD_THREAD,
        turn: {
          ...makeCodexReplayTurn({ id: turnId, status: "inProgress" }),
          startedAt: turnId === RESUME_CHILD_TURN_2 ? 1782622470 : 1782622440,
        },
      },
    },
  });

  const childTurnCompleted = (
    turnId: string,
    afterMs?: number,
  ): CodexReplay.CodexAppServerReplayEntry => ({
    type: "emit_inbound",
    label: `turn/completed/${turnId}`,
    ...(afterMs === undefined ? {} : { afterMs }),
    frame: {
      method: "turn/completed",
      params: {
        threadId: RESUME_CHILD_THREAD,
        turn: makeCodexReplayTurn({ id: turnId, status: "completed" }),
      },
    },
  });

  const resumeSubagentTranscript = makeCodexReplayTranscript({
    scenario: RESUME_SCENARIO,
    entries: [
      ...codexReplayPreamble({
        nativeThreadId: RESUME_NATIVE_THREAD,
        nativeTurnId: RESUME_NATIVE_TURN,
        prompt: RESUME_PROMPT,
      }),
      {
        type: "emit_inbound",
        label: "item/completed/subAgentActivity-started",
        frame: {
          method: "item/completed",
          params: {
            item: {
              type: "subAgentActivity",
              id: "call-codex-resume-spawn",
              kind: "started",
              agentThreadId: RESUME_CHILD_THREAD,
              agentPath: "/root/resume_agent",
            },
            threadId: RESUME_NATIVE_THREAD,
            turnId: RESUME_NATIVE_TURN,
            completedAtMs: 1782622441000,
          },
        },
      },
      childTurnStarted(RESUME_CHILD_TURN_1),
      childAgentMessage({
        id: "child-first-answer",
        text: "CODEX_FIRST_DONE",
        turnId: RESUME_CHILD_TURN_1,
        completedAtMs: 1782622442000,
      }),
      childAgentMessage({
        id: "child-first-answer-empty",
        text: "",
        turnId: RESUME_CHILD_TURN_1,
        completedAtMs: 1782622442001,
        omitPhase: true,
      }),
      childAgentMessage({
        id: "child-first-answer-duplicate",
        text: "CODEX_FIRST_DONE",
        turnId: RESUME_CHILD_TURN_1,
        completedAtMs: 1782622442002,
      }),
      childTurnCompleted(RESUME_CHILD_TURN_1, 100),
      {
        type: "emit_inbound",
        label: "item/completed/root-answer",
        frame: {
          method: "item/completed",
          params: {
            item: {
              type: "agentMessage",
              id: "root-answer-resume",
              text: "NUDGED",
              phase: "final_answer",
              memoryCitation: null,
            },
            threadId: RESUME_NATIVE_THREAD,
            turnId: RESUME_NATIVE_TURN,
            completedAtMs: 1782622443000,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/completed/root",
        frame: {
          method: "turn/completed",
          params: {
            threadId: RESUME_NATIVE_THREAD,
            turn: makeCodexReplayTurn({ id: RESUME_NATIVE_TURN, status: "completed" }),
          },
        },
      },
      childTurnStarted(RESUME_CHILD_TURN_2, 30_000),
      childAgentMessage({
        id: "child-resume-answer",
        text: "CODEX_RESUME_DONE",
        turnId: RESUME_CHILD_TURN_2,
        completedAtMs: 1782622480000,
        afterMs: 30_000,
      }),
      childTurnCompleted(RESUME_CHILD_TURN_2),
    ],
  });

  it.effect.each(["model/rerouted"] as const)(
    "keeps %s child metadata when an older lookup finishes later",
    (method) =>
      Effect.scoped(
        Effect.gen(function* () {
          const releaseMetadata = yield* Deferred.make<void>();
          const observed = yield* Deferred.make<void>();
          const model = "gpt-5.6-sol";
          const notification: CodexReplay.CodexAppServerReplayEntry = {
            type: "emit_inbound",
            frame: {
              method,
              params: {
                threadId: RESUME_CHILD_THREAD,
                turnId: RESUME_CHILD_TURN_1,
                fromModel: "gpt-6-astra",
                toModel: model,
                reason: "highRiskCyberActivity",
              },
            },
          };
          const initialSettings: CodexReplay.CodexAppServerReplayEntry = {
            type: "emit_inbound",
            frame: {
              method: "thread/settings/updated",
              params: {
                threadId: RESUME_CHILD_THREAD,
                threadSettings: {
                  model: "gpt-6-astra",
                  effort: "low",
                  serviceTier: "ultrafast",
                  modelProvider: "openai",
                  cwd: "/workspace",
                  approvalPolicy: "never",
                  approvalsReviewer: "auto_review",
                  collaborationMode: { mode: "default", settings: { model: "gpt-6-astra" } },
                  sandboxPolicy: { type: "dangerFullAccess" },
                },
              },
            },
          };
          const harness = yield* makeCodexReplayHarness(
            {
              ...resumeSubagentTranscript,
              entries: resumeSubagentTranscript.entries.flatMap((entry) =>
                entry.type === "emit_inbound" && entry.label === "turn/completed/root"
                  ? [entry, initialSettings, notification]
                  : [entry],
              ),
            },
            (event) =>
              event.type === "subagent.updated" && event.subagent.model === model
                ? Deferred.succeed(observed, undefined)
                : Effect.void,
            undefined,
            (threadId) =>
              Deferred.await(releaseMetadata).pipe(
                Effect.as({ thread: { id: threadId }, model: "gpt-6-astra" }),
              ),
          );
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make("attempt-child-model-update"),
              text: RESUME_PROMPT,
            }),
          );
          yield* TestClock.adjust("100 millis");
          yield* Deferred.await(observed);
          assert.equal(harness.subagentUpdates().at(-1)?.subagent.status, "completed");
          yield* Deferred.succeed(releaseMetadata, undefined);
          yield* TestClock.adjust("30 seconds");
          assert.equal(harness.subagentUpdates().at(-1)?.subagent.model, model);
          assert.deepEqual(harness.subagentUpdates().at(-1)?.subagent.modelSelection?.options, [
            { id: "reasoningEffort", value: "low" },
            { id: "serviceTier", value: "ultrafast" },
          ]);
        }).pipe(
          Effect.provide(
            Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
          ),
        ),
      ),
  );

  it.effect("preserves a subagent result across a trailing empty final and resume", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeCodexReplayHarness(resumeSubagentTranscript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-resume"),
            text: RESUME_PROMPT,
          }),
        );
        yield* harness.until(() =>
          harness.subagentUpdates().some((event) => event.subagent.result === "CODEX_FIRST_DONE"),
        );
        assert.lengthOf(
          harness.subagentUpdates().filter((event) => event.subagent.result === "CODEX_FIRST_DONE"),
          1,
        );
        yield* TestClock.adjust("100 millis");
        yield* harness.awaitTerminal(1);
        assert.equal(harness.terminalEvents()[0]?.status, "completed");
        const settledUpdates = harness.subagentUpdates();
        const firstCompletion = settledUpdates[settledUpdates.length - 1];
        assert.equal(firstCompletion?.subagent.status, "completed");
        assert.equal(firstCompletion?.subagent.result, "CODEX_FIRST_DONE");
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
        const settledUpdateCount = settledUpdates.length;

        yield* TestClock.adjust("30 seconds");
        yield* harness.until(() => harness.subagentUpdates().length > settledUpdateCount);
        const reopened = harness.subagentUpdates()[settledUpdateCount];
        assert.equal(reopened?.subagent.status, "running");
        assert.equal(DateTime.toEpochMillis(reopened!.subagent.startedAt!), 1782622470000);
        assert.isNull(reopened!.subagent.completedAt);
        assert.isTrue(yield* harness.hasPendingBackgroundWork);
        if (harness.runtime.readThreadActivity === undefined)
          return yield* Effect.die("Missing Codex activity port");
        assert.equal(
          (yield* harness.runtime.readThreadActivity(harness.providerThread)).status,
          "working",
        );

        yield* TestClock.adjust("30 seconds");
        yield* harness.until(() => {
          const updates = harness.subagentUpdates();
          const latest = updates[updates.length - 1];
          return (
            latest !== undefined &&
            latest.subagent.status === "completed" &&
            latest.subagent.result === "CODEX_RESUME_DONE"
          );
        });
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
        assert.lengthOf(harness.terminalEvents(), 1);
        assert.lengthOf(harness.continuationRequests, 0);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect.each([
    ["shutdown", "cancelled"],
    ["activity-completed", "completed"],
    ["late-activity-completed", "completed"],
    ["stale-running", "completed"],
    ["duplicate-completed", "completed"],
  ] as const)(
    "normalizes subagent %s without losing its lifecycle",
    ([nativeStatus, expectedStatus]) =>
      Effect.scoped(
        Effect.gen(function* () {
          const marker = yield* Deferred.make<void>();
          const firstCompletion = yield* Deferred.make<void>();
          const stateEntry = (
            status: string,
            id: string,
          ): Extract<CodexReplay.CodexAppServerReplayEntry, { type: "emit_inbound" }> => ({
            type: "emit_inbound",
            label: id,
            frame: {
              method: "item/completed",
              params: {
                threadId: RESUME_NATIVE_THREAD,
                turnId: RESUME_NATIVE_TURN,
                item: {
                  type: "collabAgentToolCall",
                  id,
                  tool: "listAgents",
                  status: "completed",
                  senderThreadId: RESUME_NATIVE_THREAD,
                  receiverThreadIds: [RESUME_CHILD_THREAD],
                  agentsStates: { [RESUME_CHILD_THREAD]: { status, message: null } },
                },
              },
            },
          });
          const entries: Array<CodexReplay.CodexAppServerReplayEntry> = [
            ...codexReplayPreamble({
              nativeThreadId: RESUME_NATIVE_THREAD,
              nativeTurnId: RESUME_NATIVE_TURN,
              prompt: RESUME_PROMPT,
            }),
            resumeSubagentTranscript.entries.find(
              (e) =>
                e.type === "emit_inbound" && e.label === "item/completed/subAgentActivity-started",
            )!,
          ];
          if (nativeStatus === "late-activity-completed") {
            entries.push(
              resumeSubagentTranscript.entries.find(
                (e) => e.type === "emit_inbound" && e.label === "turn/completed/root",
              )!,
            );
          }
          if (nativeStatus === "activity-completed" || nativeStatus === "late-activity-completed") {
            entries.push({
              type: "emit_inbound",
              label: "activity-done",
              frame: {
                method: "item/completed",
                params: {
                  threadId: RESUME_NATIVE_THREAD,
                  turnId: RESUME_NATIVE_TURN,
                  item: {
                    type: "subAgentActivity",
                    id: "activity-done",
                    kind: "completed",
                    agentThreadId: RESUME_CHILD_THREAD,
                    agentPath: "/root/resume_agent",
                  },
                },
              },
            });
          } else if (nativeStatus === "stale-running" || nativeStatus === "duplicate-completed") {
            entries.push(stateEntry("completed", "child-completed"), {
              ...stateEntry(
                nativeStatus === "stale-running" ? "running" : "completed",
                "trailing-snapshot",
              ),
              afterMs: 100,
            });
          } else {
            entries.push(stateEntry(nativeStatus, "status-update"));
          }
          if (nativeStatus === "late-activity-completed") {
            entries.push({
              type: "emit_inbound",
              label: "late-marker",
              frame: {
                method: "turn/started",
                params: {
                  threadId: RESUME_CHILD_THREAD,
                  turn: makeCodexReplayTurn({ id: RESUME_CHILD_TURN_1, status: "inProgress" }),
                },
              },
            });
          } else {
            entries.push({
              type: "emit_inbound",
              label: "marker",
              frame: {
                method: "item/completed",
                params: {
                  threadId: RESUME_NATIVE_THREAD,
                  turnId: RESUME_NATIVE_TURN,
                  item: {
                    type: "agentMessage",
                    id: "marker",
                    text: "LIFECYCLE_MARKER",
                    phase: "final_answer",
                    memoryCitation: null,
                  },
                },
              },
            });
          }
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({ scenario: `subagent-${nativeStatus}`, entries }),
            (event) =>
              (event.type === "message.updated" && event.message.text === "LIFECYCLE_MARKER") ||
              (nativeStatus === "late-activity-completed" &&
                event.type === "provider_turn.updated" &&
                event.providerTurn.nativeTurnRef?.nativeId === RESUME_CHILD_TURN_1)
                ? Deferred.succeed(marker, undefined)
                : event.type === "subagent.updated" && event.subagent.status === "completed"
                  ? Deferred.succeed(firstCompletion, undefined)
                  : Effect.void,
          );
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make(`subagent-${nativeStatus}`),
              text: RESUME_PROMPT,
            }),
          );
          if (nativeStatus === "stale-running" || nativeStatus === "duplicate-completed") {
            yield* Deferred.await(firstCompletion);
            yield* TestClock.adjust("100 millis");
          }
          yield* Deferred.await(marker);
          const latest = harness.subagentUpdates().at(-1)!.subagent;
          assert.equal(latest.status, expectedStatus);
          if (nativeStatus === "duplicate-completed") {
            const first = harness.subagentUpdates().find((e) => e.subagent.status === "completed")!;
            assert.equal(
              DateTime.toEpochMillis(latest.completedAt!),
              DateTime.toEpochMillis(first.subagent.completedAt!),
            );
          }
        }).pipe(
          Effect.provide(
            Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
          ),
        ),
      ),
  );

  const codexReplayThreadResult = (input: {
    readonly nativeThreadId: string;
    readonly forkedFromId: string | null;
  }) => ({
    thread: {
      id: input.nativeThreadId,
      sessionId: input.nativeThreadId,
      forkedFromId: input.forkedFromId,
      preview: "",
      ephemeral: false,
      modelProvider: "openai",
      createdAt: 1782622440,
      updatedAt: 1782622440,
      status: { type: "idle" },
      path: `/tmp/${input.nativeThreadId}.jsonl`,
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
    model: "gpt-5.4",
    modelProvider: "openai",
    serviceTier: null,
    cwd: "/workspace",
    instructionSources: [],
    approvalPolicy: "on-request",
    approvalsReviewer: "user",
    sandbox: { type: "workspaceWrite", writableRoots: [], networkAccess: false },
    reasoningEffort: "medium",
  });

  const errorCauseChainText = (error: unknown): string =>
    error instanceof Error ? `${error.message} ${errorCauseChainText(error.cause)}` : String(error);

  const codexReplaySourceTurn = (input: {
    readonly id: string;
    readonly ordinal: number;
    readonly nativeId: string | null;
    readonly providerThreadId: ProviderThreadId;
    readonly now: DateTime.Utc;
  }): OrchestrationV2ProviderTurn => ({
    id: ProviderTurnId.make(input.id),
    providerThreadId: input.providerThreadId,
    nodeId: NodeId.make(`node-${input.id}`),
    runAttemptId: RunAttemptId.make(`run-attempt-${input.id}`),
    nativeTurnRef:
      input.nativeId === null
        ? { driver: CodexAdapterV2.CODEX_DRIVER_KIND, nativeId: null, strength: "none" }
        : {
            driver: CodexAdapterV2.CODEX_DRIVER_KIND,
            nativeId: input.nativeId,
            strength: "strong",
          },
    ordinal: input.ordinal,
    status: "completed",
    startedAt: input.now,
    completedAt: input.now,
  });

  it.effect("fails honestly when rolling back a legacy Codex thread", () =>
    Effect.gen(function* () {
      const nativeThreadId = "legacy-rollback-thread";
      const preamble = codexReplayPreamble({
        nativeThreadId,
        nativeTurnId: "legacy-rollback-turn",
        prompt: "unused",
      });
      const transcript = makeCodexReplayTranscript({
        scenario: "codex-legacy-rollback",
        entries: [
          ...preamble.slice(0, 5),
          {
            type: "expect_outbound",
            label: "thread/read",
            frame: {
              id: 3,
              method: "thread/read",
              params: { threadId: nativeThreadId, includeTurns: false },
            },
          },
          {
            type: "emit_inbound",
            label: "thread/read",
            frame: {
              id: 3,
              result: { thread: { id: nativeThreadId, historyMode: "legacy" } },
            },
          },
        ],
      });
      const outbound: Array<string> = [];
      const harness = yield* makeCodexReplayHarness(
        transcript,
        () => Effect.void,
        (method) =>
          Effect.sync(() => {
            outbound.push(method);
          }),
      );
      const now = yield* DateTime.now;
      const firstTurn = codexReplaySourceTurn({
        id: "provider-turn-first",
        ordinal: 1,
        nativeId: "native-turn-first",
        providerThreadId: harness.providerThread.id,
        now,
      });
      const secondTurn = codexReplaySourceTurn({
        id: "provider-turn-second",
        ordinal: 2,
        nativeId: "native-turn-second",
        providerThreadId: harness.providerThread.id,
        now,
      });

      const error = yield* Effect.flip(
        harness.runtime.rollbackThread({
          providerThread: harness.providerThread,
          target: {
            type: "provider_turn",
            checkpointId: CheckpointId.make("checkpoint-legacy-rollback"),
            appRunOrdinal: 1,
            providerTurn: firstTurn,
          },
          providerThreadTurns: [firstTurn, secondTurn],
        }),
      );

      assert.instanceOf(error, ProviderAdapter.ProviderAdapterRollbackThreadError);
      assert.include(
        errorCauseChainText(error),
        "legacy",
        "legacy rollback must surface an honest unsupported-history failure",
      );
      assert.notInclude(
        outbound,
        "thread/rollback",
        "thread/rollback must not be sent to a legacy Codex thread",
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect.each([
    { operation: "start", tier: undefined },
    { operation: "start", tier: null },
    { operation: "start", tier: "priority" },
    { operation: "resume", tier: undefined },
    { operation: "resume", tier: null },
    { operation: "resume", tier: "priority" },
  ] as const)(
    "records requested and native identity separately for Codex $operation tier $tier",
    ({ operation, tier }) =>
      Effect.scoped(
        Effect.gen(function* () {
          const nativeThreadId = `identity-${operation}-${String(tier)}`;
          const selection: ModelSelection = {
            ...CODEX_TEST_MODEL_SELECTION,
            model: "requested-model",
            ...(tier == null ? {} : { options: [{ id: "serviceTier", value: tier }] }),
          };
          const observedResult = {
            ...codexReplayThreadResult({ nativeThreadId, forkedFromId: null }),
            model: "native-model",
            ...(tier === undefined ? { serviceTier: null } : { serviceTier: tier }),
          };
          const entries = codexReplayPreamble({
            nativeThreadId,
            nativeTurnId: "unused",
            prompt: "unused",
          })
            .slice(0, 5)
            .map((entry) =>
              entry.type === "emit_inbound" &&
              Predicate.isObject(entry.frame) &&
              entry.frame.id === 2
                ? { ...entry, frame: { ...entry.frame, result: observedResult } }
                : entry,
            ) as Array<CodexReplay.CodexAppServerReplayEntry>;
          if (operation === "resume")
            entries.push(
              {
                type: "expect_outbound",
                frame: {
                  id: 3,
                  method: "thread/resume",
                  params: {
                    threadId: nativeThreadId,
                    excludeTurns: true,
                    model: selection.model,
                    cwd: "/workspace",
                    config: CodexAdapterV2.CODEX_THREAD_CONFIG,
                  },
                },
              },
              { type: "emit_inbound", frame: { id: 3, result: observedResult } },
            );
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({ scenario: nativeThreadId, entries }),
            undefined,
            undefined,
            undefined,
            { modelSelection: selection },
          );
          const row =
            operation === "start"
              ? harness.providerThread
              : yield* harness.runtime.resumeThread({
                  providerThread: harness.providerThread,
                  modelSelection: selection,
                  runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
                });
          const identity = row.runtimeIdentity;
          assert.isString(identity?.runtimeGeneration);
          assert.equal(identity?.requested.model, "requested-model");
          assert.equal(identity?.requested.serviceTier, tier ?? null);
          assert.deepEqual(identity?.observed.model, {
            status: "observed",
            value: "native-model",
            sourceEvent: "codex.thread/open",
          });
          assert.deepEqual(identity?.observed.backend, {
            status: "observed",
            value: "openai",
            sourceEvent: "codex.thread/open",
          });
          assert.equal(identity?.observed.account.status, "unavailable");
          if (tier === "priority")
            assert.deepEqual(identity?.observed.serviceTier, {
              status: "observed",
              value: "priority",
              sourceEvent: "codex.thread/open",
            });
          else assert.equal(identity?.observed.serviceTier.status, "unavailable");
        }),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  const recordingRuntimeLifecycle = (
    timeline: Array<string>,
    reject: (phase: "admit" | "bind", threadId: ThreadId | null) => boolean = () => false,
    beforeBind: (threadId: ThreadId | null) => Effect.Effect<void> = () => Effect.void,
  ): ProviderAdapter.ProviderRuntimeLifecycle => {
    let ordinal = 0;
    const admitted = new Map<ThreadId, string>();
    return {
      reserve: () => Effect.sync(() => `shared-producer-generation-${++ordinal}`),
      admit: ({ threadId, runtimeGeneration }) =>
        Effect.suspend(() => {
          timeline.push(`admit:${threadId}@${runtimeGeneration}`);
          admitted.set(threadId, runtimeGeneration);
          return reject("admit", threadId)
            ? Effect.fail(
                new ProviderAdapter.ProviderRuntimeBindingError({
                  driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                  detail: "Synthetic admission rejection before native effects.",
                }),
              )
            : Effect.void;
        }),
      bind: ({ providerThread, runtimeGeneration, requested, observed }) =>
        beforeBind(providerThread.appThreadId).pipe(
          Effect.andThen(
            Effect.suspend(() => {
              timeline.push(`bind:${providerThread.appThreadId}@${runtimeGeneration}`);
              assert.equal(admitted.get(providerThread.appThreadId!), runtimeGeneration);
              return reject("bind", providerThread.appThreadId)
                ? Effect.fail(
                    new ProviderAdapter.ProviderRuntimeBindingError({
                      driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                      detail: "Synthetic binding rejection after a native effect.",
                    }),
                  )
                : Effect.succeed({
                    ...providerThread,
                    runtimeIdentity: {
                      runtimeGeneration,
                      evidenceRevision: (providerThread.runtimeIdentity?.evidenceRevision ?? 0) + 1,
                      requested,
                      observed,
                    },
                  });
            }),
          ),
        ),
      abandon: () => Effect.void,
      invalidate: () => Effect.void,
    };
  };

  it.effect(
    "hands initialized physical diagnostics to the exact generation without blocking unsupported providers",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let captures = 0;
          const base = recordingRuntimeLifecycle([]);
          yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({
              scenario: "physical-responder-handoff",
              entries: codexReplayPreamble({
                nativeThreadId: "native-physical",
                nativeTurnId: "unused",
                prompt: "unused",
              }).slice(0, 5),
            }),
            undefined,
            undefined,
            undefined,
            {
              runtimeLifecycle: {
                ...base,
                bind: (input) =>
                  Effect.gen(function* () {
                    assert.equal(input.physicalProcess?.runtimeGeneration, input.runtimeGeneration);
                    const observation = yield* input.physicalProcess!.capture;
                    assert.deepEqual(observation, {
                      status: "unknown",
                      runtimeGeneration: input.runtimeGeneration,
                      reason: "unsupported",
                    });
                    captures++;
                    return yield* base.bind(input);
                  }),
              },
            },
          );
          assert.equal(captures, 1);
        }),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect("admits each shared producer target before native resume and creation", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const timeline: Array<string> = [];
        const targetB = ThreadId.make("shared-target-b");
        const targetC = ThreadId.make("shared-target-c");
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "shared-target-admission",
            entries: [
              ...codexReplayPreamble({
                nativeThreadId: "native-target-a",
                nativeTurnId: "unused",
                prompt: "unused",
              }).slice(0, 5),
              {
                type: "expect_outbound",
                frame: {
                  id: 3,
                  method: "thread/resume",
                  params: {
                    threadId: "native-target-b",
                    excludeTurns: true,
                    config: CodexAdapterV2.CODEX_THREAD_CONFIG,
                  },
                },
              },
              {
                type: "emit_inbound",
                frame: {
                  id: 3,
                  result: codexReplayThreadResult({
                    nativeThreadId: "native-target-b",
                    forkedFromId: null,
                  }),
                },
              },
              {
                type: "expect_outbound",
                frame: {
                  id: 4,
                  method: "thread/start",
                  params: { config: CodexAdapterV2.CODEX_THREAD_CONFIG },
                },
              },
              {
                type: "emit_inbound",
                frame: {
                  id: 4,
                  result: codexReplayThreadResult({
                    nativeThreadId: "native-target-c",
                    forkedFromId: null,
                  }),
                },
              },
            ],
          }),
          undefined,
          (method) =>
            Effect.sync(() => {
              timeline.push(method);
            }),
          undefined,
          {
            runtimeLifecycle: recordingRuntimeLifecycle(timeline),
            onRawRequest: (method) =>
              Effect.sync(() => {
                timeline.push(method);
              }),
          },
        );
        const generation = harness.providerThread.runtimeIdentity!.runtimeGeneration;
        timeline.length = 0;
        const resumed = yield* harness.runtime.resumeThread({
          providerThread: {
            ...harness.providerThread,
            id: ProviderThreadId.make("provider-thread-target-b"),
            appThreadId: targetB,
            nativeThreadRef: {
              driver: CodexAdapterV2.CODEX_DRIVER_KIND,
              nativeId: "native-target-b",
              strength: "strong",
            },
          },
        });
        const created = yield* harness.runtime.ensureThread({
          threadId: targetC,
          modelSelection: CODEX_TEST_MODEL_SELECTION,
          runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
        });
        assert.deepEqual(timeline, [
          `admit:${targetB}@${generation}`,
          "thread/resume",
          `bind:${targetB}@${generation}`,
          `admit:${targetC}@${generation}`,
          "thread/start",
          `bind:${targetC}@${generation}`,
        ]);
        assert.equal(resumed.runtimeIdentity?.runtimeGeneration, generation);
        assert.equal(created.runtimeIdentity?.runtimeGeneration, generation);
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect.each(["admit", "bind"] as const)(
    "handles direct resume %s rejection with the corresponding native-effect fence",
    (phase) =>
      Effect.scoped(
        Effect.gen(function* () {
          const timeline: Array<string> = [];
          const target = ThreadId.make(`shared-rejected-${phase}`);
          const bindEntered = yield* Deferred.make<void>();
          const releaseBind = yield* Deferred.make<void>();
          let reject = true;
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({
              scenario: `shared-target-${phase}-failure`,
              entries: [
                ...codexReplayPreamble({
                  nativeThreadId: "native-source",
                  nativeTurnId: "unused",
                  prompt: "unused",
                }).slice(0, 5),
                ...(phase === "bind"
                  ? [
                      {
                        type: "expect_outbound" as const,
                        frame: {
                          id: 3,
                          method: "thread/resume",
                          params: {
                            threadId: "native-rejected",
                            excludeTurns: true,
                            config: CodexAdapterV2.CODEX_THREAD_CONFIG,
                          },
                        },
                      },
                      {
                        type: "emit_inbound" as const,
                        frame: {
                          id: 3,
                          result: codexReplayThreadResult({
                            nativeThreadId: "native-rejected",
                            forkedFromId: null,
                          }),
                        },
                      },
                    ]
                  : [
                      {
                        type: "expect_outbound" as const,
                        frame: {
                          id: 3,
                          method: "thread/start",
                          params: { config: CodexAdapterV2.CODEX_THREAD_CONFIG },
                        },
                      },
                      {
                        type: "emit_inbound" as const,
                        frame: {
                          id: 3,
                          result: codexReplayThreadResult({
                            nativeThreadId: "native-recovered",
                            forkedFromId: null,
                          }),
                        },
                      },
                    ]),
              ],
            }),
            undefined,
            (method) =>
              Effect.sync(() => {
                timeline.push(method);
              }),
            undefined,
            {
              onRawRequest: (method) =>
                Effect.sync(() => {
                  timeline.push(method);
                }),
              runtimeLifecycle: recordingRuntimeLifecycle(
                timeline,
                (stage, threadId) => reject && stage === phase && threadId === target,
                (threadId) =>
                  phase === "bind" && threadId === target
                    ? Deferred.succeed(bindEntered, undefined).pipe(
                        Effect.andThen(Deferred.await(releaseBind)),
                        Effect.asVoid,
                      )
                    : Effect.void,
              ),
            },
          );
          const generation = harness.providerThread.runtimeIdentity!.runtimeGeneration;
          timeline.length = 0;
          const targetRow = {
            ...harness.providerThread,
            id: ProviderThreadId.make(`provider-thread-${phase}-failure`),
            appThreadId: target,
            nativeThreadRef: {
              driver: CodexAdapterV2.CODEX_DRIVER_KIND,
              nativeId: "native-rejected",
              strength: "strong" as const,
            },
          };
          const now = yield* DateTime.now;
          const turnInput = makeCodexTestTurnInput({
            threadId: target,
            providerThread: targetRow,
            now,
            attemptId: RunAttemptId.make("attempt-queued-behind-binding-failure"),
            text: "Must not launch after unknown binding",
          });
          const resumeFiber = yield* harness.runtime
            .resumeThread({ providerThread: targetRow })
            .pipe(Effect.flip, Effect.forkScoped);
          const queuedStart =
            phase === "bind"
              ? yield* Effect.gen(function* () {
                  yield* Deferred.await(bindEntered);
                  const started = yield* harness.runtime
                    .startTurn(turnInput)
                    .pipe(Effect.flip, Effect.forkScoped);
                  yield* Effect.yieldNow;
                  yield* Deferred.succeed(releaseBind, undefined);
                  return started;
                })
              : undefined;
          const error = yield* Fiber.join(resumeFiber);
          assert.equal(error._tag, "ProviderAdapterResumeThreadError");
          assert.nestedPropertyVal(error, "cause._tag", "ProviderRuntimeBindingError");
          assert.deepEqual(
            timeline,
            phase === "admit"
              ? [`admit:${target}@${generation}`]
              : [`admit:${target}@${generation}`, "thread/resume", `bind:${target}@${generation}`],
          );
          reject = false;
          const ensure = harness.runtime.ensureThread({
            threadId: target,
            modelSelection: CODEX_TEST_MODEL_SELECTION,
            runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
          });
          if (phase === "admit") {
            const recovered = yield* ensure;
            assert.equal(recovered.nativeThreadRef?.nativeId, "native-recovered");
            assert.deepEqual(timeline, [
              `admit:${target}@${generation}`,
              `admit:${target}@${generation}`,
              "thread/start",
              `bind:${target}@${generation}`,
            ]);
          } else {
            const before = [...timeline];
            const queuedError = yield* Fiber.join(queuedStart!);
            assert.equal(queuedError._tag, "ProviderAdapterTurnStartError");
            assert.nestedPropertyVal(queuedError, "cause._tag", "ProviderAdapterProtocolError");
            const firstTurn = codexReplaySourceTurn({
              id: "provider-turn-before-failed-binding",
              ordinal: 1,
              nativeId: "native-turn-before-failed-binding",
              providerThreadId: targetRow.id,
              now,
            });
            const rollbackError = yield* harness.runtime
              .rollbackThread({
                providerThread: targetRow,
                target: {
                  type: "provider_turn",
                  checkpointId: CheckpointId.make("checkpoint-after-failed-binding"),
                  appRunOrdinal: 1,
                  providerTurn: firstTurn,
                },
                providerThreadTurns: [firstTurn],
              })
              .pipe(Effect.flip);
            assert.equal(rollbackError._tag, "ProviderAdapterProtocolError");
            assert.instanceOf(
              yield* ensure.pipe(Effect.flip),
              ProviderAdapter.ProviderRuntimeBindingError,
            );
            assert.instanceOf(
              yield* harness.runtime.resumeThread({ providerThread: targetRow }).pipe(Effect.flip),
              ProviderAdapter.ProviderRuntimeBindingError,
            );
            const startError = yield* harness.runtime
              .startTurn(
                makeCodexTestTurnInput({
                  threadId: target,
                  providerThread: targetRow,
                  now,
                  attemptId: RunAttemptId.make("attempt-after-binding-failure"),
                  text: "Must not launch after unknown binding",
                }),
              )
              .pipe(Effect.flip);
            assert.equal(startError._tag, "ProviderAdapterTurnStartError");
            assert.nestedPropertyVal(startError, "cause._tag", "ProviderAdapterProtocolError");
            assert.deepEqual(timeline, before);
          }
        }),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect("conservatively fences an admission rejection reached through startTurn", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const timeline: Array<string> = [];
        const target = ThreadId.make("start-turn-admission-rejected");
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "start-turn-admission-rejection",
            entries: codexReplayPreamble({
              nativeThreadId: "native-start-turn-admission-opener",
              nativeTurnId: "unused",
              prompt: "unused",
            }).slice(0, 5),
          }),
          undefined,
          (method) =>
            Effect.sync(() => {
              timeline.push(method);
            }),
          undefined,
          {
            runtimeLifecycle: recordingRuntimeLifecycle(
              timeline,
              (phase, threadId) => phase === "admit" && threadId === target,
            ),
            onRawRequest: (method) =>
              Effect.sync(() => {
                timeline.push(method);
              }),
          },
        );
        const generation = harness.providerThread.runtimeIdentity!.runtimeGeneration;
        const row: OrchestrationV2ProviderThread = {
          ...harness.providerThread,
          id: ProviderThreadId.make("provider-thread-start-turn-admission-rejected"),
          appThreadId: target,
          nativeThreadRef: {
            driver: CodexAdapterV2.CODEX_DRIVER_KIND,
            nativeId: "native-start-turn-admission-target",
            strength: "strong",
          },
        };
        const now = yield* DateTime.now;
        timeline.length = 0;
        const input = makeCodexTestTurnInput({
          threadId: target,
          providerThread: row,
          now,
          attemptId: RunAttemptId.make("attempt-start-turn-admission-rejection"),
          text: "Must not send a native request",
        });
        const first = yield* harness.runtime.startTurn(input).pipe(Effect.flip);
        assert.equal(first._tag, "ProviderAdapterTurnStartError");
        assert.nestedPropertyVal(first, "cause._tag", "ProviderAdapterResumeThreadError");
        assert.nestedPropertyVal(first, "cause.cause._tag", "ProviderRuntimeBindingError");
        const later = yield* harness.runtime.startTurn(input).pipe(Effect.flip);
        assert.equal(later._tag, "ProviderAdapterTurnStartError");
        assert.nestedPropertyVal(later, "cause._tag", "ProviderAdapterProtocolError");
        assert.instanceOf(
          yield* harness.runtime.resumeThread({ providerThread: row }).pipe(Effect.flip),
          ProviderAdapter.ProviderRuntimeBindingError,
        );
        assert.deepEqual(timeline, [`admit:${target}@${generation}`]);
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect.each([true, false])("admits an unbound rollback target with loaded=%s", (loaded) =>
    Effect.scoped(
      Effect.gen(function* () {
        const timeline: Array<string> = [];
        const target = ThreadId.make(`rollback-target-loaded-${loaded}`);
        const nativeThreadId = `native-rollback-loaded-${loaded}`;
        const entries = codexReplayPreamble({
          nativeThreadId: "native-rollback-opener",
          nativeTurnId: "unused",
          prompt: "unused",
        }).slice(0, 5);
        let requestId = 3;
        const exchange = (method: string, params: unknown, result: unknown) => {
          const id = requestId++;
          entries.push(
            { type: "expect_outbound", frame: { id, method, params } },
            { type: "emit_inbound", frame: { id, result } },
          );
        };
        exchange(
          "thread/read",
          { threadId: nativeThreadId, includeTurns: false },
          {
            thread: {
              id: nativeThreadId,
              historyMode: "paginated",
              status: { type: loaded ? "idle" : "notLoaded" },
            },
          },
        );
        if (!loaded) {
          exchange(
            "thread/resume",
            {
              threadId: nativeThreadId,
              excludeTurns: true,
              config: CodexAdapterV2.CODEX_THREAD_CONFIG,
            },
            codexReplayThreadResult({ nativeThreadId, forkedFromId: null }),
          );
        }
        exchange(
          "thread/turns/list",
          {
            threadId: nativeThreadId,
            cursor: null,
            limit: 1,
            sortDirection: "desc",
            itemsView: "summary",
          },
          {
            data: [
              { id: "native-rollback-second-turn", items: [], status: "completed", error: null },
            ],
            nextCursor: null,
          },
        );
        exchange(
          "thread/revert",
          {
            threadId: nativeThreadId,
            beforeTurnId: "native-rollback-second-turn",
          },
          codexReplayThreadResult({ nativeThreadId, forkedFromId: null }),
        );
        const recordRequest = (method: string) =>
          Effect.sync(() => {
            timeline.push(method);
          });
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({ scenario: `rollback-admission-${loaded}`, entries }),
          undefined,
          recordRequest,
          undefined,
          {
            runtimeLifecycle: recordingRuntimeLifecycle(timeline),
            onRawRequest: recordRequest,
          },
        );
        const generation = harness.providerThread.runtimeIdentity!.runtimeGeneration;
        const row: OrchestrationV2ProviderThread = {
          ...harness.providerThread,
          id: ProviderThreadId.make(`provider-thread-rollback-loaded-${loaded}`),
          appThreadId: target,
          nativeThreadRef: {
            driver: CodexAdapterV2.CODEX_DRIVER_KIND,
            nativeId: nativeThreadId,
            strength: "strong",
          },
        };
        const now = yield* DateTime.now;
        const turns = [1, 2].map((ordinal) =>
          codexReplaySourceTurn({
            id: `provider-turn-rollback-${ordinal}`,
            ordinal,
            nativeId: ordinal === 1 ? "native-rollback-first-turn" : "native-rollback-second-turn",
            providerThreadId: row.id,
            now,
          }),
        );
        timeline.length = 0;
        const rolledBack = yield* harness.runtime.rollbackThread({
          providerThread: row,
          target: {
            type: "provider_turn",
            checkpointId: CheckpointId.make(`checkpoint-rollback-loaded-${loaded}`),
            appRunOrdinal: 1,
            providerTurn: turns[0]!,
          },
          providerThreadTurns: turns,
        });
        assert.deepEqual(timeline, [
          "thread/read",
          `admit:${target}@${generation}`,
          ...(!loaded ? ["thread/resume"] : []),
          `bind:${target}@${generation}`,
          "thread/turns/list",
          "thread/revert",
        ]);
        assert.equal(rolledBack.providerThread.runtimeIdentity?.runtimeGeneration, generation);
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect("buffers a native reroute until its thread-open generation is bound", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "identity-buffered-reroute";
        const preamble = codexReplayPreamble({
          nativeThreadId,
          nativeTurnId: "unused",
          prompt: "unused",
        }).slice(0, 5);
        const entries = [
          ...preamble.slice(0, 4),
          {
            type: "emit_inbound" as const,
            frame: {
              method: "model/rerouted",
              params: {
                threadId: nativeThreadId,
                fromModel: "gpt-5.4",
                toModel: "rerouted-before-open",
                turnId: "pending-native-turn",
                reason: "highRiskCyberActivity",
              },
            },
          },
          ...preamble.slice(4),
        ];
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({ scenario: nativeThreadId, entries }),
        );
        yield* awaitUntil(
          () =>
            harness.events.filter((event) => event.type === "runtime_identity.observed").length ===
            2,
          "bound buffered reroute",
        );
        const observations = harness.events.flatMap((event) =>
          event.type === "runtime_identity.observed" ? [event] : [],
        );
        assert.deepEqual(
          observations.map((event) => event.observed.model),
          [
            { status: "observed", value: "gpt-5.4", sourceEvent: "codex.thread/open" },
            { status: "observed", value: "rerouted-before-open", sourceEvent: "model/rerouted" },
          ],
        );
        assert.equal(
          observations[0]?.binding.runtimeGeneration,
          observations[1]?.binding.runtimeGeneration,
        );
        assert.equal(observations[0]?.binding.nativeThreadId, nativeThreadId);
        assert.equal(observations[1]?.binding.providerThreadId, harness.providerThread.id);
        assert.equal(observations[1]?.requested.model, CODEX_TEST_MODEL_SELECTION.model);
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect(
    "replaces a managed Codex process before the next prompt and resumes its native cursor with the current request",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const nativeThreadId = "managed-rotation-cursor";
          let revision = "first-runtime";
          const lifecycle: Array<string> = [];
          const opens: Array<
            Parameters<CodexAdapterV2.CodexAppServerClientFactoryShape["open"]>[0]
          > = [];
          const sends: Array<unknown> = [];
          const oldReroutes: Array<(payload: unknown) => Effect.Effect<unknown>> = [];
          const first = makeCodexReplayTranscript({
            scenario: "identity-managed-original",
            entries: [
              ...codexReplayPreamble({
                nativeThreadId,
                nativeTurnId: "first-turn",
                prompt: "Original prompt",
              }),
              capacityCompletionEntry(nativeThreadId, "first-turn", "completed"),
            ],
          });
          const replacementTurn = codexReplayPreamble({
            nativeThreadId,
            nativeTurnId: "second-turn",
            prompt: "Next prompt",
          })
            .slice(5)
            .map((entry) => {
              if (
                entry.type !== "expect_outbound" ||
                !Predicate.isObject(entry.frame) ||
                entry.frame.method !== "turn/start"
              )
                return entry;
              return {
                ...entry,
                frame: {
                  ...entry.frame,
                  params: {
                    ...(entry.frame.params as Record<string, unknown>),
                    model: "current-request",
                  },
                },
              };
            }) as Array<CodexReplay.CodexAppServerReplayEntry>;
          const replacement = makeCodexReplayTranscript({
            scenario: "identity-managed-replacement",
            entries: [
              ...codexReplayPreamble({
                nativeThreadId,
                nativeTurnId: "unused",
                prompt: "unused",
              }).slice(0, 3),
              {
                type: "expect_outbound",
                frame: {
                  id: 2,
                  method: "thread/resume",
                  params: {
                    threadId: nativeThreadId,
                    excludeTurns: true,
                    model: "current-request",
                    cwd: "/workspace",
                    config: CodexAdapterV2.CODEX_THREAD_CONFIG,
                  },
                },
              },
              {
                type: "emit_inbound",
                frame: {
                  id: 2,
                  result: {
                    ...codexReplayThreadResult({ nativeThreadId, forkedFromId: null }),
                    model: "replacement-native",
                    modelProvider: "replacement-backend",
                    serviceTier: "priority",
                  },
                },
              },
              ...replacementTurn,
              capacityCompletionEntry(nativeThreadId, "second-turn", "completed"),
            ],
          });
          const harness = yield* makeCodexReplayHarness(
            first,
            undefined,
            (method, params) =>
              Effect.sync(() => {
                if (method === "turn/start") sends.push(params);
              }),
            undefined,
            {
              replacementTranscripts: [replacement],
              resolveRuntime: Effect.sync(() => ({
                config: DEFAULT_CODEX_SETTINGS,
                environment: { SYNTHETIC_RUNTIME_VERSION: revision },
                revision,
              })),
              onOpen: (input, ordinal) =>
                Effect.sync(() => {
                  opens.push(input);
                  lifecycle.push(`open:${ordinal}`);
                }),
              onClose: (ordinal) =>
                Effect.sync(() => {
                  lifecycle.push(`close:${ordinal}`);
                }),
              onNotification: (method, handler, ordinal) => {
                if (method === "model/rerouted" && ordinal === 0) oldReroutes.push(handler);
              },
            },
          );
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make("managed-original"),
              text: "Original prompt",
            }),
          );
          yield* harness.firstTerminal;
          revision = "replacement-runtime";
          yield* harness.runtime.startTurn({
            ...makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make("managed-next"),
              text: "Next prompt",
            }),
            modelSelection: {
              ...CODEX_TEST_MODEL_SELECTION,
              model: "current-request",
              options: [{ id: "serviceTier", value: "priority" }],
            },
          });
          yield* awaitUntil(
            () => harness.terminalEvents().length === 2,
            "replacement native turn completion",
          );
          assert.deepEqual(lifecycle.slice(0, 3), ["open:0", "close:0", "open:1"]);
          yield* opens[0]!.onTermination!(
            new CodexError.CodexAppServerProcessExitedError({ code: 7 }),
          );
          yield* Effect.yieldNow;
          assert.lengthOf(harness.terminalEvents(), 2);
          assert.equal(opens.length, 2);
          assert.equal(opens[1]?.environment.SYNTHETIC_RUNTIME_VERSION, "replacement-runtime");
          const beforeStale = harness.events.filter(
            (event) => event.type === "runtime_identity.observed",
          ).length;
          assert.lengthOf(oldReroutes, 1);
          yield* oldReroutes[0]!({
            threadId: nativeThreadId,
            fromModel: "native-old",
            toModel: "stale-native-model",
            turnId: "retired-native-turn",
            reason: "highRiskCyberActivity",
          });
          yield* Effect.yieldNow;
          assert.equal(
            harness.events.filter((event) => event.type === "runtime_identity.observed").length,
            beforeStale,
            "a retired producer cannot borrow the replacement's generation",
          );
          const observations = harness.events.filter(
            (event) => event.type === "runtime_identity.observed",
          );
          const last = observations.at(-1);
          assert.isDefined(last);
          if (last?.type !== "runtime_identity.observed") return;
          assert.notEqual(
            last.binding.runtimeGeneration,
            harness.providerThread.runtimeIdentity?.runtimeGeneration,
          );
          assert.equal(last.binding.nativeThreadId, nativeThreadId);
          assert.equal(last.requested.model, "current-request");
          assert.deepEqual(last.observed.model, {
            status: "observed",
            value: "replacement-native",
            sourceEvent: "codex.thread/open",
          });
          assert.equal(sends.length, 2);
          assert.deepEqual(
            sends.map((value) => (Predicate.isObject(value) ? value.input : undefined)),
            [[{ type: "text", text: "Original prompt" }], [{ type: "text", text: "Next prompt" }]],
            "the original prompt is never resent",
          );
        }),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect.each(["failure", "interruption"] as const)(
    "never reuses a closed Codex producer after managed replacement %s",
    (outcome) =>
      Effect.scoped(
        Effect.gen(function* () {
          const nativeThreadId = `identity-rotation-${outcome}`;
          let revision = "original";
          let opens = 0;
          let sends = 0;
          const closed: Array<number> = [];
          const candidateStarted = yield* Deferred.make<void>();
          const original = makeCodexReplayTranscript({
            scenario: nativeThreadId,
            entries: [
              ...codexReplayPreamble({
                nativeThreadId,
                nativeTurnId: "completed-original",
                prompt: "Original only",
              }),
              capacityCompletionEntry(nativeThreadId, "completed-original", "completed"),
            ],
          });
          const harness = yield* makeCodexReplayHarness(
            original,
            undefined,
            (method) =>
              Effect.sync(() => {
                if (method === "turn/start") sends++;
              }),
            undefined,
            {
              replacementTranscripts: [original],
              resolveRuntime: Effect.sync(() => ({
                config: DEFAULT_CODEX_SETTINGS,
                environment: {},
                revision,
              })),
              onOpen: (input, ordinal) =>
                Effect.gen(function* () {
                  opens++;
                  if (ordinal === 0) return;
                  yield* Deferred.succeed(candidateStarted, undefined);
                  if (outcome === "interruption") return yield* Effect.never;
                  return yield* new ProviderAdapter.ProviderAdapterOpenSessionError({
                    driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                    providerSessionId: input.providerSessionId,
                    cause: "synthetic replacement launch failure",
                  });
                }),
              onClose: (ordinal) =>
                Effect.sync(() => {
                  closed.push(ordinal);
                }),
            },
          );
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make("identity-rotation-original"),
              text: "Original only",
            }),
          );
          yield* harness.firstTerminal;
          revision = "replacement";
          const input = makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make(`identity-rotation-${outcome}`),
            text: "Never replay original",
          });
          const replacement = yield* harness.runtime
            .startTurn(input)
            .pipe(Effect.exit, Effect.forkScoped);
          yield* Deferred.await(candidateStarted);
          if (outcome === "interruption") {
            yield* Fiber.interrupt(replacement);
          } else {
            const result = yield* Fiber.join(replacement);
            assert.equal(result._tag, "Failure");
          }
          assert.include(closed, 0);
          assert.equal(opens, 2);
          assert.equal(sends, 1);
          const retry = yield* harness.runtime.startTurn(input).pipe(Effect.result);
          assert.equal(retry._tag, "Failure");
          assert.equal(opens, 2);
          assert.equal(sends, 1);
          assert.isFalse(
            harness.events.some(
              (event) =>
                event.type === "runtime_identity.observed" &&
                event.binding.runtimeGeneration !==
                  harness.providerThread.runtimeIdentity?.runtimeGeneration,
            ),
          );
        }),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect.each(["active", "background"] as const)(
    "does not rotate a shared Codex process while sibling %s work remains",
    (work) =>
      Effect.scoped(
        Effect.gen(function* () {
          const nativeThreadId = "managed-shared-busy";
          let revision = "original";
          let opens = 0;
          let sends = 0;
          const entries = codexReplayPreamble({
            nativeThreadId,
            nativeTurnId: "busy-turn",
            prompt: "Keep working",
          });
          if (work === "background")
            entries.push(
              {
                type: "emit_inbound",
                frame: {
                  method: "item/started",
                  params: {
                    threadId: nativeThreadId,
                    turnId: "busy-turn",
                    startedAtMs: 1782622445000,
                    item: backgroundCommandItem("inProgress"),
                  },
                },
              },
              capacityCompletionEntry(nativeThreadId, "busy-turn", "completed"),
            );
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({ scenario: nativeThreadId, entries }),
            undefined,
            (method) =>
              Effect.sync(() => {
                if (method === "turn/start") sends++;
              }),
            undefined,
            {
              resolveRuntime: Effect.sync(() => ({
                config: DEFAULT_CODEX_SETTINGS,
                environment: {},
                revision,
              })),
              onOpen: () =>
                Effect.sync(() => {
                  opens++;
                }),
            },
          );
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make("managed-busy"),
              text: "Keep working",
            }),
          );
          if (work === "background") {
            yield* harness.firstTerminal;
            assert.isTrue(yield* harness.hasPendingBackgroundWork);
          }
          revision = "replacement";
          const sibling = {
            ...harness.providerThread,
            id: ProviderThreadId.make("sibling-provider-row"),
            appThreadId: ThreadId.make("sibling-app"),
            nativeThreadRef: {
              driver: CodexAdapterV2.CODEX_DRIVER_KIND,
              nativeId: "sibling-native",
              strength: "strong" as const,
            },
          };
          const rejected = yield* harness.runtime
            .startTurn(
              makeCodexTestTurnInput({
                threadId: sibling.appThreadId,
                providerThread: sibling,
                now: yield* DateTime.now,
                attemptId: RunAttemptId.make("managed-sibling"),
                text: "Do not replace busy process",
              }),
            )
            .pipe(Effect.result);
          assert.equal(rejected._tag, "Failure");
          assert.equal(opens, 1);
          assert.equal(sends, 1);
          assert.lengthOf(harness.terminalEvents(), work === "background" ? 1 : 0);
        }),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  // A fork's first turn runs on its run's own provider-thread row, which the
  // fork call never sees; the turn must bind the fork's native thread to it.
  const forkFirstTurnTranscript = (scenario: string) => {
    const nativeThreadId = `${scenario}-source`;
    const forkThreadId = `${scenario}-fork`;
    const source = codexReplayPreamble({
      nativeThreadId,
      nativeTurnId: "unused",
      prompt: "unused",
    });
    const forkTurn = codexReplayPreamble({
      nativeThreadId: forkThreadId,
      nativeTurnId: `${scenario}-fork-turn`,
      prompt: "Continue in the fork",
    })
      .slice(5)
      .map((entry) => {
        const frame = "frame" in entry ? (entry.frame as Record<string, unknown>) : undefined;
        return frame?.id === 3 ? { ...entry, frame: { ...frame, id: 4 } } : entry;
      }) as Array<CodexReplay.CodexAppServerReplayEntry>;
    return {
      forkThreadId,
      transcript: makeCodexReplayTranscript({
        scenario,
        entries: [
          ...source.slice(0, 5),
          {
            type: "expect_outbound",
            label: "thread/fork",
            frame: {
              id: 3,
              method: "thread/fork",
              params: { threadId: nativeThreadId, config: CodexAdapterV2.CODEX_THREAD_CONFIG },
            },
          },
          {
            type: "emit_inbound",
            label: "thread/fork",
            frame: {
              id: 3,
              result: codexReplayThreadResult({
                nativeThreadId: forkThreadId,
                forkedFromId: nativeThreadId,
              }),
            },
          },
          ...forkTurn,
        ],
      }),
    };
  };

  it.effect("admits the fork target before creating its native conversation", () =>
    Effect.gen(function* () {
      const { transcript } = forkFirstTurnTranscript("codex-fork-admission");
      const timeline: Array<string> = [];
      const harness = yield* makeCodexReplayHarness(
        transcript,
        undefined,
        (method) =>
          Effect.sync(() => {
            timeline.push(method);
          }),
        undefined,
        { runtimeLifecycle: recordingRuntimeLifecycle(timeline) },
      );
      const generation = harness.providerThread.runtimeIdentity!.runtimeGeneration;
      timeline.length = 0;
      const targetThreadId = ThreadId.make("thread-fork-admission-target");
      yield* harness.runtime.forkThread({
        sourceProviderThread: harness.providerThread,
        targetThreadId,
      });
      assert.equal(timeline[0], `admit:${targetThreadId}@${generation}`);
      assert.equal(timeline[1], "thread/fork");
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect.each([
    {
      foreignFirst: false,
      title:
        "binds a native fork to its target run's provider-thread row so the fork's first turn starts",
    },
    {
      foreignFirst: true,
      title:
        "refuses a native fork's binding to another app thread and keeps it for the fork's own first turn",
    },
  ])("$title", ({ foreignFirst }) =>
    Effect.gen(function* () {
      const { forkThreadId, transcript } = forkFirstTurnTranscript(
        foreignFirst ? "codex-fork-foreign-turn" : "codex-fork-first-turn",
      );
      const requests: Array<string> = [];
      const harness = yield* makeCodexReplayHarness(transcript, undefined, (method) =>
        Effect.sync(() => {
          requests.push(method);
        }),
      );
      const now = yield* DateTime.now;
      const targetThreadId = ThreadId.make(`thread-${transcript.scenario}-target`);
      const forked = yield* harness.runtime.forkThread({
        sourceProviderThread: harness.providerThread,
        targetThreadId,
      });
      assert.equal(forked.nativeThreadRef?.nativeId, forkThreadId);
      // The orchestrator keeps the run's prepared row id for the fork.
      const row: OrchestrationV2ProviderThread = {
        ...forked,
        id: ProviderThreadId.make(`provider-thread:${transcript.scenario}:run-row`),
        appThreadId: targetThreadId,
      };
      if (foreignFirst) {
        const foreignThreadId = ThreadId.make(`thread-${transcript.scenario}-foreign`);
        const foreign = yield* harness.runtime
          .startTurn(
            makeCodexTestTurnInput({
              threadId: foreignThreadId,
              providerThread: { ...row, appThreadId: foreignThreadId },
              now,
              attemptId: RunAttemptId.make(`attempt-${transcript.scenario}-foreign`),
              text: "Continue in the fork",
            }),
          )
          .pipe(Effect.result);
        assert.equal(foreign._tag, "Failure");
        assert.notInclude(requests, "turn/start");
      }
      yield* harness.runtime.startTurn(
        makeCodexTestTurnInput({
          threadId: targetThreadId,
          providerThread: row,
          now,
          attemptId: RunAttemptId.make(`attempt-${transcript.scenario}`),
          text: "Continue in the fork",
        }),
      );
      assert.deepEqual(requests, ["initialize", "thread/start", "thread/fork", "turn/start"]);
      const bound = harness.events.filter(
        (event) =>
          event.type === "runtime_identity.observed" && event.binding.threadId === targetThreadId,
      );
      assert.equal(bound.length, 1);
      if (bound[0]?.type !== "runtime_identity.observed") return;
      assert.equal(bound[0].binding.providerThreadId, row.id);
      assert.equal(bound[0].binding.nativeThreadId, forkThreadId);
      // The source thread keeps its own binding.
      assert.isTrue(
        harness.events.some(
          (event) =>
            event.type === "runtime_identity.observed" &&
            event.binding.providerThreadId === harness.providerThread.id,
        ),
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect("rejects a replaced native fork issuer before its first turn without reforking", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = forkFirstTurnTranscript("identity-fork-replaced-issuer");
        let revision = "original";
        const requests: Array<string> = [];
        const sourceNativeId = "identity-fork-replaced-issuer-source";
        const replacement = makeCodexReplayTranscript({
          scenario: "identity-fork-replacement",
          entries: [
            ...codexReplayPreamble({
              nativeThreadId: sourceNativeId,
              nativeTurnId: "unused",
              prompt: "unused",
            }).slice(0, 3),
            {
              type: "expect_outbound",
              frame: {
                id: 2,
                method: "thread/resume",
                params: {
                  threadId: sourceNativeId,
                  excludeTurns: true,
                  model: CODEX_TEST_MODEL_SELECTION.model,
                  cwd: "/workspace",
                  config: CodexAdapterV2.CODEX_THREAD_CONFIG,
                },
              },
            },
            {
              type: "emit_inbound",
              frame: {
                id: 2,
                result: codexReplayThreadResult({
                  nativeThreadId: sourceNativeId,
                  forkedFromId: null,
                }),
              },
            },
            ...codexReplayPreamble({
              nativeThreadId: sourceNativeId,
              nativeTurnId: "replacement-source-turn",
              prompt: "After replacement",
            }).slice(5),
            capacityCompletionEntry(sourceNativeId, "replacement-source-turn", "completed"),
          ],
        });
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: fixture.transcript.scenario,
            entries: fixture.transcript.entries.slice(0, 7),
          }),
          undefined,
          (method) =>
            Effect.sync(() => {
              requests.push(method);
            }),
          undefined,
          {
            replacementTranscripts: [replacement],
            resolveRuntime: Effect.sync(() => ({
              config: DEFAULT_CODEX_SETTINGS,
              environment: {},
              revision,
            })),
          },
        );
        const targetThreadId = ThreadId.make("identity-fork-target");
        const fork = yield* harness.runtime.forkThread({
          sourceProviderThread: harness.providerThread,
          targetThreadId,
        });
        revision = "replaced";
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("identity-fork-replacement-source"),
            text: "After replacement",
          }),
        );
        yield* harness.firstTerminal;
        const beforeForkTurn = requests.filter((method) => method === "turn/start").length;
        const failed = yield* harness.runtime
          .startTurn(
            makeCodexTestTurnInput({
              threadId: targetThreadId,
              providerThread: { ...fork, id: ProviderThreadId.make("identity-fork-prepared-row") },
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make("identity-fork-replaced"),
              text: "Continue in the fork",
            }),
          )
          .pipe(Effect.result);
        assert.equal(failed._tag, "Failure");
        assert.equal(requests.filter((method) => method === "turn/start").length, beforeForkTurn);
        assert.equal(requests.filter((method) => method === "thread/fork").length, 1);
        assert.isFalse(
          harness.events.some(
            (event) =>
              event.type === "runtime_identity.observed" &&
              event.binding.threadId === targetThreadId,
          ),
        );
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect.each(["same-source", "blank"] as const)(
    "refuses a native fork response without a new conversation: %s",
    (variant) =>
      Effect.scoped(
        Effect.gen(function* () {
          const nativeThreadId = `identity-fork-invalid-${variant}`;
          let forks = 0;
          const transcript = makeCodexReplayTranscript({
            scenario: nativeThreadId,
            entries: [
              ...codexReplayPreamble({
                nativeThreadId,
                nativeTurnId: "unused",
                prompt: "unused",
              }).slice(0, 5),
              {
                type: "expect_outbound",
                frame: {
                  id: 3,
                  method: "thread/fork",
                  params: { threadId: nativeThreadId, config: CodexAdapterV2.CODEX_THREAD_CONFIG },
                },
              },
              {
                type: "emit_inbound",
                frame: {
                  id: 3,
                  result: codexReplayThreadResult({
                    nativeThreadId: variant === "blank" ? " " : nativeThreadId,
                    forkedFromId: nativeThreadId,
                  }),
                },
              },
            ],
          });
          const harness = yield* makeCodexReplayHarness(transcript, undefined, (method) =>
            Effect.sync(() => {
              if (method === "thread/fork") forks++;
            }),
          );
          const input = {
            sourceProviderThread: harness.providerThread,
            targetThreadId: ThreadId.make("invalid-fork-target"),
          };
          const failed = yield* harness.runtime.forkThread(input).pipe(Effect.result);
          assert.equal(failed._tag, "Failure");
          const repeated = yield* harness.runtime.forkThread(input).pipe(Effect.result);
          assert.equal(repeated._tag, "Failure");
          assert.equal(forks, 1, "an unconfirmed native effect must not be blindly reforked");
          assert.isFalse(
            harness.events.some(
              (event) =>
                event.type === "runtime_identity.observed" &&
                event.binding.threadId === input.targetThreadId,
            ),
          );
        }),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect(
    "falls back to fork-local thread/revert on paginated history when the source turn lacks a native reference",
    () =>
      Effect.gen(function* () {
        const nativeThreadId = "fallback-source-thread";
        const forkThreadId = "fallback-fork-thread";
        const preamble = codexReplayPreamble({
          nativeThreadId,
          nativeTurnId: "fallback-source-turn",
          prompt: "unused",
        });
        const transcript = makeCodexReplayTranscript({
          scenario: "codex-fork-paginated-fallback",
          entries: [
            ...preamble.slice(0, 5),
            {
              type: "expect_outbound",
              label: "thread/fork",
              frame: {
                id: 3,
                method: "thread/fork",
                params: { threadId: nativeThreadId, config: CodexAdapterV2.CODEX_THREAD_CONFIG },
              },
            },
            {
              type: "emit_inbound",
              label: "thread/fork",
              frame: {
                id: 3,
                result: codexReplayThreadResult({
                  nativeThreadId: forkThreadId,
                  forkedFromId: nativeThreadId,
                }),
              },
            },
            {
              type: "expect_outbound",
              label: "thread/read",
              frame: {
                id: 4,
                method: "thread/read",
                params: { threadId: forkThreadId, includeTurns: false },
              },
            },
            {
              type: "emit_inbound",
              label: "thread/read",
              frame: {
                id: 4,
                result: { thread: { id: forkThreadId, historyMode: "paginated" } },
              },
            },
            {
              type: "expect_outbound",
              label: "thread/turns/list",
              frame: {
                id: 5,
                method: "thread/turns/list",
                params: {
                  threadId: forkThreadId,
                  cursor: null,
                  limit: 1,
                  sortDirection: "desc",
                  itemsView: "summary",
                },
              },
            },
            {
              type: "emit_inbound",
              label: "thread/turns/list",
              frame: {
                id: 5,
                result: {
                  data: [{ id: "native-turn-second", items: [], status: "completed", error: null }],
                  nextCursor: null,
                },
              },
            },
            {
              type: "expect_outbound",
              label: "thread/revert",
              frame: {
                id: 6,
                method: "thread/revert",
                params: { threadId: forkThreadId, beforeTurnId: "native-turn-second" },
              },
            },
            {
              type: "emit_inbound",
              label: "thread/revert",
              frame: {
                id: 6,
                result: codexReplayThreadResult({
                  nativeThreadId: forkThreadId,
                  forkedFromId: null,
                }),
              },
            },
          ],
        });
        const outbound: Array<string> = [];
        const harness = yield* makeCodexReplayHarness(
          transcript,
          () => Effect.void,
          (method) =>
            Effect.sync(() => {
              outbound.push(method);
            }),
        );
        const now = yield* DateTime.now;
        const firstTurn = codexReplaySourceTurn({
          id: "provider-turn-first",
          ordinal: 1,
          nativeId: null,
          providerThreadId: harness.providerThread.id,
          now,
        });
        const secondTurn = codexReplaySourceTurn({
          id: "provider-turn-second",
          ordinal: 2,
          nativeId: "native-turn-second",
          providerThreadId: harness.providerThread.id,
          now,
        });

        const forkedProviderThread = yield* harness.runtime.forkThread({
          sourceProviderThread: harness.providerThread,
          sourceProviderTurns: [firstTurn, secondTurn],
          providerTurnId: firstTurn.id,
          targetThreadId: ThreadId.make("thread-fork-paginated-fallback-target"),
        });

        assert.equal(forkedProviderThread.nativeThreadRef?.nativeId, forkThreadId);
        assert.notEqual(forkedProviderThread.id, harness.providerThread.id);
        assert.equal(forkedProviderThread.forkedFrom?.providerTurnId, firstTurn.id);
        assert.deepEqual(outbound.slice(-2), ["thread/turns/list", "thread/revert"]);
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  describe("native goals", () => {
    const nativeThreadId = "goal-thread";
    const objective = "Ship the feature";
    const codexGoal = (status: string, tokensUsed: number) => ({
      threadId: nativeThreadId,
      objective,
      status,
      tokenBudget: null,
      tokensUsed,
      timeUsedSeconds: 3,
      createdAt: 1782622440,
      updatedAt: 1782622450,
    });
    const notification = (
      label: string,
      method: string,
      params: unknown,
    ): CodexReplay.CodexAppServerReplayEntry => ({
      type: "emit_inbound",
      label,
      frame: { method, params },
    });
    const request = (id: number, method: string, params: unknown, result: unknown) =>
      [
        { type: "expect_outbound", label: method, frame: { id, method, params } },
        { type: "emit_inbound", label: method, frame: { id, result } },
      ] satisfies Array<CodexReplay.CodexAppServerReplayEntry>;
    const withReplayRequestId = (
      entries: ReadonlyArray<CodexReplay.CodexAppServerReplayEntry>,
      id: number,
    ) =>
      entries.map((entry) =>
        "frame" in entry && Predicate.isObject(entry.frame) && "id" in entry.frame
          ? { ...entry, frame: { ...entry.frame, id } }
          : entry,
      );
    const sessionStart = codexReplayPreamble({
      nativeThreadId,
      nativeTurnId: "unused",
      prompt: "unused",
    }).slice(0, 5);
    const goalTurnInput = (
      harness: {
        readonly threadId: ThreadId;
        readonly providerThread: OrchestrationV2ProviderThread;
      },
      text: string,
    ) =>
      DateTime.now.pipe(
        Effect.map((now) =>
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("goal-attempt"),
            text,
          }),
        ),
      );
    const providerGoals = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) =>
      events.flatMap((event) =>
        event.type === "provider_thread.updated" ? [event.providerThread.goal?.status ?? null] : [],
      );

    it.effect("keeps a /goal run open across the turns Codex continues on its own", () =>
      Effect.gen(function* () {
        const transcript = makeCodexReplayTranscript({
          scenario: "goal-continuation",
          entries: [
            ...sessionStart,
            ...request(3, "thread/goal/get", { threadId: nativeThreadId }, { goal: null }),
            ...request(
              4,
              "thread/goal/set",
              { threadId: nativeThreadId, objective, status: "paused" },
              { goal: codexGoal("paused", 0) },
            ),
            ...withReplayRequestId(
              codexReplayPreamble({
                nativeThreadId,
                nativeTurnId: "goal-turn-1",
                prompt: objective,
              }).slice(5),
              5,
            ),
            ...request(
              6,
              "thread/goal/set",
              { threadId: nativeThreadId, status: "active" },
              { goal: codexGoal("active", 0) },
            ),
            notification("goal active", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: "goal-turn-1",
              goal: codexGoal("active", 0),
            }),
            notification("first turn done", "turn/completed", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn-1", status: "completed" }),
            }),
            notification("continuation", "turn/started", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn-2", status: "inProgress" }),
            }),
            notification("goal complete", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: "goal-turn-2",
              goal: codexGoal("complete", 1200),
            }),
            notification("continuation done", "turn/completed", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn-2", status: "completed" }),
            }),
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        yield* harness.runtime.startTurn(yield* goalTurnInput(harness, `/goal ${objective}`));
        yield* harness.firstTerminal;

        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const providerTurnId = (nativeTurnId: string) =>
          idAllocator.derive.providerTurn({
            driver: CodexAdapterV2.CODEX_DRIVER_KIND,
            nativeTurnId,
          });
        assert.deepEqual(
          harness.terminalEvents().map((event) => [event.providerTurnId, event.status]),
          [[providerTurnId("goal-turn-2"), "completed"]],
        );
        assert.deepEqual(
          harness.events.flatMap((event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status === "running"
              ? [
                  [
                    event.providerTurn.id,
                    event.providerTurn.runAttemptId,
                    event.providerTurn.ordinal,
                  ],
                ]
              : [],
          ),
          [
            [providerTurnId("goal-turn-1"), RunAttemptId.make("goal-attempt"), 1],
            [providerTurnId("goal-turn-2"), RunAttemptId.make("goal-attempt"), 2],
          ],
        );
        assert.equal(providerGoals(harness.events).at(-1), "complete");
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    );

    it.effect("settles a run held for the next goal turn when Stop arrives in between", () =>
      Effect.gen(function* () {
        const prompt = "Keep going";
        const transcript = makeCodexReplayTranscript({
          scenario: "goal-stop-between-turns",
          entries: [
            ...codexReplayPreamble({ nativeThreadId, nativeTurnId: "goal-turn", prompt }),
            notification("goal active", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: "goal-turn",
              goal: codexGoal("active", 10),
            }),
            notification("turn done", "turn/completed", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn", status: "completed" }),
            }),
            notification("goal accounted", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: null,
              goal: codexGoal("active", 11),
            }),
            ...request(
              4,
              "thread/goal/set",
              { threadId: nativeThreadId, status: "paused" },
              { goal: codexGoal("paused", 10) },
            ),
            notification("goal paused", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: null,
              goal: codexGoal("paused", 10),
            }),
          ],
        });
        const requests: Array<string> = [];
        const harness = yield* makeCodexReplayHarness(
          transcript,
          () => Effect.void,
          (method) =>
            Effect.sync(() => {
              requests.push(method);
            }),
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("goal-hold-attempt"),
            text: prompt,
          }),
        );
        const providerTurnId = (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
          driver: CodexAdapterV2.CODEX_DRIVER_KIND,
          nativeTurnId: "goal-turn",
        });
        const turnStatuses = () =>
          harness.events.flatMap((event) =>
            event.type === "provider_turn.updated" && event.providerTurn.id === providerTurnId
              ? [event.providerTurn.status]
              : [],
          );
        yield* harness.until(() =>
          harness.events.some(
            (event) =>
              event.type === "provider_thread.updated" &&
              event.providerThread.goal?.tokensUsed === 11,
          ),
        );
        assert.deepEqual(turnStatuses(), ["running"]);
        assert.lengthOf(harness.terminalEvents(), 0);

        yield* harness.runtime.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId,
        });
        yield* harness.firstTerminal;

        assert.notInclude(requests, "turn/interrupt");
        assert.deepEqual(turnStatuses(), ["running", "completed"]);
        assert.deepEqual(
          harness.terminalEvents().map((event) => event.status),
          ["interrupted"],
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    );

    it.effect("releases a held goal run when its captured runtime is stopped", () =>
      Effect.gen(function* () {
        const prompt = "Keep going";
        const transcript = makeCodexReplayTranscript({
          scenario: "goal-captured-runtime-stop",
          entries: [
            ...codexReplayPreamble({ nativeThreadId, nativeTurnId: "goal-turn", prompt }),
            notification("goal active", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: "goal-turn",
              goal: codexGoal("active", 10),
            }),
            notification("turn done", "turn/completed", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn", status: "completed" }),
            }),
            // Notifications run in order, so seeing this one proves the turn settled.
            notification("goal accounted", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: null,
              goal: codexGoal("active", 11),
            }),
          ],
        });
        const requests: Array<string> = [];
        const harness = yield* makeCodexReplayHarness(
          transcript,
          () => Effect.void,
          (method) =>
            Effect.sync(() => {
              requests.push(method);
            }),
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("goal-hold-attempt"),
            text: prompt,
          }),
        );
        const providerTurnId = (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
          driver: CodexAdapterV2.CODEX_DRIVER_KIND,
          nativeTurnId: "goal-turn",
        });
        const turnStatuses = () =>
          harness.events.flatMap((event) =>
            event.type === "provider_turn.updated" && event.providerTurn.id === providerTurnId
              ? [event.providerTurn.status]
              : [],
          );
        yield* awaitUntil(
          () =>
            harness.events.some(
              (event) =>
                event.type === "provider_thread.updated" &&
                event.providerThread.goal?.tokensUsed === 11,
            ),
          "the completed turn to be held",
        );
        // The held turn keeps reading as running, so Stop still reaches the adapter.
        assert.deepEqual(turnStatuses(), ["running"]);
        assert.lengthOf(harness.terminalEvents(), 0);

        const captured = yield* harness.runtime.captureRuntimeStop!(harness.providerThread);
        assert.isNotNull(captured);
        yield* captured!.stop;
        yield* harness.firstTerminal;

        assert.notInclude(requests, "turn/interrupt");
        assert.deepEqual(turnStatuses(), ["running", "completed"]);
        assert.deepEqual(
          harness.terminalEvents().map((event) => event.status),
          ["completed"],
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    );

    it.effect("moves a Stop that names an earlier goal turn to the turn Codex continued with", () =>
      Effect.gen(function* () {
        const prompt = "Keep going";
        const transcript = makeCodexReplayTranscript({
          scenario: "goal-stop-after-continuation",
          entries: [
            ...codexReplayPreamble({ nativeThreadId, nativeTurnId: "goal-turn-a", prompt }),
            notification("goal active", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: "goal-turn-a",
              goal: codexGoal("active", 10),
            }),
            notification("first turn done", "turn/completed", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn-a", status: "completed" }),
            }),
            notification("continuation", "turn/started", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn-b", status: "inProgress" }),
            }),
            notification("goal accounted", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: "goal-turn-b",
              goal: codexGoal("active", 11),
            }),
            ...request(
              4,
              "thread/goal/set",
              { threadId: nativeThreadId, status: "paused" },
              { goal: codexGoal("paused", 11) },
            ),
            ...request(
              5,
              "turn/interrupt",
              { threadId: nativeThreadId, turnId: "goal-turn-b" },
              {},
            ),
            notification("continuation interrupted", "turn/completed", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn-b", status: "interrupted" }),
            }),
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("goal-successor-attempt"),
            text: prompt,
          }),
        );
        yield* harness.until(() =>
          harness.events.some(
            (event) =>
              event.type === "provider_thread.updated" &&
              event.providerThread.goal?.tokensUsed === 11,
          ),
        );
        yield* harness.runtime.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId: (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
            driver: CodexAdapterV2.CODEX_DRIVER_KIND,
            nativeTurnId: "goal-turn-a",
          }),
          requestRuntimeRestart: true,
        });
        yield* harness.firstTerminal;
        assert.deepEqual(
          harness.terminalEvents().map((event) => event.status),
          ["interrupted"],
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    );

    it.effect("pauses an active goal before Stop interrupts its turn", () =>
      Effect.gen(function* () {
        const prompt = "Keep going";
        const transcript = makeCodexReplayTranscript({
          scenario: "goal-stop",
          entries: [
            ...codexReplayPreamble({ nativeThreadId, nativeTurnId: "goal-turn", prompt }),
            notification("goal active", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: "goal-turn",
              goal: codexGoal("active", 10),
            }),
            ...request(
              4,
              "thread/goal/set",
              { threadId: nativeThreadId, status: "paused" },
              { goal: codexGoal("paused", 10) },
            ),
            ...request(5, "turn/interrupt", { threadId: nativeThreadId, turnId: "goal-turn" }, {}),
            notification("interrupted", "turn/completed", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn", status: "interrupted" }),
            }),
          ],
        });
        const requests: Array<string> = [];
        const harness = yield* makeCodexReplayHarness(
          transcript,
          () => Effect.void,
          (method) =>
            Effect.sync(() => {
              requests.push(method);
            }),
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("goal-stop-attempt"),
            text: prompt,
          }),
        );
        yield* harness.until(() => providerGoals(harness.events).includes("active"));
        yield* harness.runtime.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId: (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
            driver: CodexAdapterV2.CODEX_DRIVER_KIND,
            nativeTurnId: "goal-turn",
          }),
        });
        yield* harness.firstTerminal;

        assert.deepEqual(requests.slice(-2), ["thread/goal/set", "turn/interrupt"]);
        assert.deepEqual(
          harness.terminalEvents().map((event) => event.status),
          ["interrupted"],
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    );
  });

  describe("Codex file-change approval paths", () => {
    const nativeThreadId = "file-approval-thread";
    const nativeTurnId = "file-approval-turn";
    const prompt = "Edit the files.";
    type FileUpdateChange = CodexSchema.V2ItemStartedNotification__FileUpdateChange;
    const addA: FileUpdateChange = { path: "/w/a.ts", kind: { type: "add" }, diff: "export {};" };
    const updateB: FileUpdateChange = {
      path: "/w/b.ts",
      kind: { type: "update", move_path: null },
      diff: "@@",
    };
    const renameC: FileUpdateChange = {
      path: "/w/c.ts",
      kind: { type: "update", move_path: "/w/d.ts" },
      diff: "@@",
    };

    const deleteE: FileUpdateChange = { path: "/w/e.ts", kind: { type: "delete" }, diff: "gone" };

    const fileChange = (
      phase: "started" | "completed",
      turnId: string,
      itemId: string,
      changes: ReadonlyArray<FileUpdateChange>,
    ): CodexReplay.CodexAppServerReplayEntry => ({
      type: "emit_inbound",
      label: `${phase} ${itemId}`,
      frame: {
        method: `item/${phase}`,
        params: {
          threadId: nativeThreadId,
          turnId,
          ...(phase === "started" ? { startedAtMs: 1 } : { completedAtMs: 3 }),
          item: {
            type: "fileChange",
            id: itemId,
            status: phase === "started" ? "inProgress" : "completed",
            changes,
          },
        },
      },
    });

    const approval = (input: {
      readonly id: number;
      readonly turnId: string;
      readonly itemId: string;
    }): Array<CodexReplay.CodexAppServerReplayEntry> => [
      {
        type: "emit_inbound",
        label: `approval ${input.itemId}`,
        frame: {
          id: input.id,
          method: "item/fileChange/requestApproval",
          params: {
            threadId: nativeThreadId,
            turnId: input.turnId,
            itemId: input.itemId,
            startedAtMs: 2,
            reason: null,
            grantRoot: null,
          },
        },
      },
      {
        type: "expect_outbound",
        label: `approval ${input.itemId}`,
        frame: { id: input.id, result: { decision: "accept" } },
      },
    ];

    const turnCompleted = (turnId: string): CodexReplay.CodexAppServerReplayEntry => ({
      type: "emit_inbound",
      label: `complete ${turnId}`,
      frame: {
        method: "turn/completed",
        params: {
          threadId: nativeThreadId,
          turn: makeCodexReplayTurn({ id: turnId, status: "completed" }),
        },
      },
    });

    const startApprovalTurn = (
      scenario: string,
      entries: ReadonlyArray<CodexReplay.CodexAppServerReplayEntry>,
    ) =>
      Effect.gen(function* () {
        const pending = yield* Queue.unbounded<OrchestrationV2RuntimeRequest>();
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario,
            entries: [...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt }), ...entries],
          }),
          (event) =>
            event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending"
              ? Queue.offer(pending, event.runtimeRequest)
              : Effect.void,
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make(`${scenario}-attempt`),
            text: prompt,
          }),
        );
        return {
          harness,
          answerNext: Queue.take(pending).pipe(
            Effect.tap((request) =>
              harness.runtime.respondToRuntimeRequest({
                requestId: request.id,
                decision: "accept",
              }),
            ),
          ),
        };
      });

    const approvalItem = (
      events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>,
      request: OrchestrationV2RuntimeRequest,
    ) =>
      events.flatMap((event) =>
        event.type === "turn_item.updated" &&
        event.turnItem.type === "approval_request" &&
        event.turnItem.requestId === request.id
          ? [event.turnItem]
          : [],
      )[0];

    it.effect("answers without waiting for late paths or borrowing another item's paths", () =>
      Effect.gen(function* () {
        const { harness, answerNext } = yield* startApprovalTurn("file-approval-after", [
          fileChange("started", nativeTurnId, "other-patch", [updateB]),
          // Late details cannot arrive until the approval response has been sent.
          ...approval({ id: 0, turnId: nativeTurnId, itemId: "patch-a" }),
          fileChange("started", nativeTurnId, "patch-a", [addA]),
          fileChange("completed", nativeTurnId, "patch-a", [addA]),
          fileChange("completed", nativeTurnId, "other-patch", [updateB]),
          turnCompleted(nativeTurnId),
        ]);
        const request = yield* answerNext;
        yield* harness.firstTerminal;

        const items = harness.events.flatMap((event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "approval_request" &&
          event.turnItem.requestId === request.id
            ? [event.turnItem]
            : [],
        );
        assert.equal(request.nativeRequestRef?.nativeId, "patch-a");
        assert.isNotEmpty(items);
        for (const item of items) {
          assert.equal(item.requestKind, "file-change");
          assert.isUndefined(item.prompt);
        }
        assert.deepEqual(
          harness.terminalEvents().map((event) => event.status),
          ["completed"],
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    );

    it.effect("matches file paths to items approved out of order", () =>
      Effect.gen(function* () {
        const { harness, answerNext } = yield* startApprovalTurn("file-approval-concurrent", [
          fileChange("started", nativeTurnId, "patch-a", [renameC, deleteE, addA]),
          fileChange("started", nativeTurnId, "patch-b", [updateB]),
          ...approval({ id: 0, turnId: nativeTurnId, itemId: "patch-b" }),
          fileChange("completed", nativeTurnId, "patch-b", [updateB]),
          ...approval({ id: 1, turnId: nativeTurnId, itemId: "patch-a" }),
          fileChange("completed", nativeTurnId, "patch-a", [renameC, deleteE, addA]),
          turnCompleted(nativeTurnId),
        ]);
        const first = yield* answerNext;
        const second = yield* answerNext;
        yield* harness.firstTerminal;

        assert.deepEqual(
          [first, second].map((request) => [
            request.nativeRequestRef?.nativeId,
            approvalItem(harness.events, request)?.prompt,
          ]),
          [
            ["patch-b", "update /w/b.ts"],
            ["patch-a", "add /w/a.ts\nupdate /w/c.ts -> /w/d.ts\ndelete /w/e.ts"],
          ],
        );
        assert.deepEqual(
          harness.terminalEvents().map((event) => event.status),
          ["completed"],
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    );
  });
});
