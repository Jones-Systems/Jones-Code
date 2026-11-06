import * as Digest from "effect/crypto/Digest";
// @effect-diagnostics nodeBuiltinImport:off -- Synthetic provider transport, no provider process or user configuration.
import {
  ClaudeSettings,
  CodexSettings,
  MessageId,
  NodeId,
  ProjectId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  type ModelSelection,
  type OrchestrationV2ProviderThread,
  type ThreadId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpSessions from "@t3tools/provider-core/server/McpProviderSessions";
import type { McpProviderSessionConfig } from "@t3tools/provider-core/server/mcpSession";
import * as CodexClient from "effect-codex-app-server/client";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ServerConfig from "../../config.ts";
import * as Codex from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Claude from "../../orchestration-v2/Adapters/ClaudeAdapterV2.ts";

const codexSettings = Schema.decodeSync(CodexSettings)({});
const claudeSettings = Schema.decodeSync(ClaudeSettings)({});

/** Exercises the shipped adapter runtime. The synthetic transport proves only
 * request routing/configuration and bounded fixture behavior, not live providers. */
export const makeAdapterRuntimeProbe = Effect.fn("C9.makeAdapterRuntimeProbe")(function* (
  driver: "codex" | "claudeAgent",
  threadId: ThreadId,
  session: McpProviderSessionConfig,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const config = yield* ServerConfig.ServerConfig;
  const random = yield* SecureRandom.SecureRandom;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const paths = yield* Path.Path;
  const sessions = yield* McpSessions.McpProviderSessions;
  yield* sessions.set(session);
  const modelSelection: ModelSelection = {
    instanceId: session.providerInstanceId,
    model: driver === "codex" ? "gpt-5.4" : "claude-sonnet-4-6",
  };
  const observed: Array<{ method: string; cwd: string | null; mcp: boolean; resume: boolean }> = [];
  const nativeThread = (cwd: string | null) => ({
    id: "native-c9",
    sessionId: "native-c9",
    cwd,
    createdAt: 1,
    updatedAt: 1,
    turns: [],
    status: { type: "idle" },
    modelProvider: "openai",
    path: paths.join(config.stateDir, "synthetic.jsonl"),
  });
  const respond = (method: string, params: unknown): Effect.Effect<unknown> =>
    Effect.sync(() => {
      const input = params as { cwd?: string; config?: Record<string, unknown>; threadId?: string };
      if (method === "initialize")
        return {
          userAgent: "synthetic",
          codexHome: config.stateDir,
          platformFamily: "unix",
          platformOs: "macos",
        };
      if (method === "thread/start" || method === "thread/resume") {
        observed.push({
          method,
          cwd: input.cwd ?? null,
          mcp: input.config?.["mcp_servers.t3-code"] !== undefined,
          resume: method === "thread/resume",
        });
        return {
          thread: nativeThread(input.cwd ?? null),
          model: modelSelection.model,
          modelProvider: "openai",
          serviceTier: null,
        };
      }
      if (method === "turn/start") {
        observed.push({ method, cwd: input.cwd ?? null, mcp: true, resume: false });
        return {
          turn: {
            id: "native-turn-c9",
            items: [],
            status: "inProgress",
            error: null,
            startedAt: 1,
            completedAt: null,
            durationMs: null,
          },
        };
      }
      if (method === "thread/goal/get") return { goal: null };
      if (method === "thread/unsubscribe") return { status: "notSubscribed" };
      throw new Error(`Unmodelled C9 transport call: ${method}`);
    });
  const unused = () => Effect.die("Unused synthetic transport operation");
  const codexClient: CodexClient.CodexAppServerClient["Service"] = {
    awaitTermination: Effect.never,
    request: respond as CodexClient.CodexAppServerClient["Service"]["request"],
    notify: () => Effect.void,
    handleServerRequest: () => Effect.void,
    handleServerNotification: () => Effect.void,
    handleUnknownServerRequest: () => Effect.void,
    handleUnknownServerNotification: () => Effect.void,
    raw: {
      request: respond as CodexClient.CodexAppServerClient["Service"]["raw"]["request"],
      notify: () => Effect.void,
      respond: unused,
      respondError: unused,
      requests: Stream.empty,
      notifications: Stream.empty,
    },
  };
  const adapter =
    driver === "codex"
      ? yield* Codex.makeCodexAdapterV2({
          instanceId: modelSelection.instanceId,
          settings: { ...codexSettings, homePath: config.stateDir },
          environment: {},
          digest: yield* Digest.Digest,
          fileSystem,
          idAllocator,
          serverConfig: config,
          clientFactory: { open: () => Effect.succeed(codexClient) },
        })
      : yield* Claude.makeClaudeAdapterV2({
          instanceId: modelSelection.instanceId,
          settings: { ...claudeSettings, homePath: config.stateDir },
          environment: { CLAUDE_CONFIG_DIR: config.stateDir },
          attachmentsDir: config.attachmentsDir,
          random,
          fileSystem,
          path: paths,
          idAllocator,
          queryRunner: {
            allocateSessionId: Effect.succeed("native-c9"),
            open: (input) =>
              Effect.sync(() => {
                observed.push({
                  method: "query/open",
                  cwd: input.options.cwd ?? null,
                  mcp: input.options.mcpServers?.["t3-code"] !== undefined,
                  resume: Boolean(input.options.resume),
                });
                const closed = Deferred.makeUnsafe<void>();
                return {
                  messages: Stream.fromEffect(Deferred.await(closed)).pipe(Stream.drain),
                  close: Deferred.succeed(closed, undefined).pipe(Effect.asVoid),
                  offer: (_message, onQueued) =>
                    Effect.sync(() => {
                      onQueued?.();
                    }),
                  setModel: () => Effect.void,
                  applyFlagSettings: () => Effect.void,
                  setPermissionMode: () => Effect.void,
                  backgroundTasks: () => Effect.succeed(false),
                  stopTask: () => Effect.void,
                  termination: Effect.void,
                  interrupt: Effect.void,
                };
              }),
            forkSession: unused,
            subagentLaunchToolUseId: () => Effect.succeed(null),
            assertComplete: Effect.void,
          },
        });
  const run = Effect.fn("C9.adapterRun")(function* (
    cwd: string,
    previous?: OrchestrationV2ProviderThread,
  ) {
    const runtimePolicy = { cwd, runtimeMode: "full-access", interactionMode: "default" } as const;
    const runtime = yield* adapter.openSession({
      threadId,
      providerSessionId: ProviderSessionId.make(
        `provider-session:c9:${previous === undefined ? "first" : "continued"}`,
      ),
      modelSelection,
      runtimePolicy,
    });
    const providerThread =
      previous === undefined
        ? yield* runtime.ensureThread({ threadId, modelSelection, runtimePolicy })
        : yield* runtime.resumeThread({
            threadId,
            providerThread: previous,
            modelSelection,
            runtimePolicy,
          });
    const now = yield* DateTime.now;
    const ordinal = previous === undefined ? 1 : 2;
    yield* runtime.startTurn({
      threadId,
      providerThread,
      modelSelection,
      runtimePolicy,
      runId: RunId.make(`run:c9:${ordinal}`),
      runOrdinal: ordinal,
      providerTurnOrdinal: ordinal,
      nativeThreadHasTurns: previous !== undefined,
      attemptId: RunAttemptId.make(`attempt:c9:${ordinal}`),
      rootNodeId: NodeId.make(`node:c9:${ordinal}`),
      appThread: {
        id: threadId,
        projectId: ProjectId.make("project:c9"),
        title: "C9 proof",
        createdBy: "user",
        creationSource: "web",
        providerInstanceId: modelSelection.instanceId,
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: previous === undefined ? null : cwd,
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
      message: {
        messageId: MessageId.make(`message:c9:${ordinal}`),
        text:
          previous === undefined
            ? "Read source through t3_source_read."
            : "Continue in the attached checkout.",
        attachments: [],
        createdBy: "user",
        creationSource: "web",
      },
    });
    return providerThread;
  });
  return { run, observed };
});
