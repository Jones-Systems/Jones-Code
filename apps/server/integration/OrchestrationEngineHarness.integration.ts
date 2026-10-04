// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { makeSqlitePersistenceLive } from "../src/persistence/Layers/Sqlite.ts";
import { ClaudeProviderCapabilitiesV2 } from "../src/orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import { CodexProviderCapabilitiesV2 } from "../src/orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EffectWorker from "../src/orchestration-v2/EffectWorker.ts";
import * as EffectOutbox from "../src/orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../src/orchestration-v2/EventSink.ts";
import * as Orchestrator from "../src/orchestration-v2/Orchestrator.ts";
import * as ProjectStore from "../src/orchestration-v2/ProjectStore.ts";
import * as ProviderAdapter from "../src/orchestration-v2/ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "../src/orchestration-v2/ProviderAdapterRegistry.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "../src/orchestration-v2/testkit/ProviderReplayHarness.ts";

export interface LocalTurn {
  readonly text: string;
  readonly contents?: string;
  readonly approval?: boolean;
  readonly fail?: boolean;
}

function git(cwd: string, args: readonly string[]): string {
  const environment = Object.fromEntries(
    Object.entries(process.env).filter(([name]) => !name.startsWith("GIT_")),
  );
  return NodeChildProcess.execFileSync("git", [...args], {
    cwd,
    encoding: "utf8",
    timeout: 10_000,
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...environment, GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null" },
  }).trimEnd();
}

export function gitRefExists(cwd: string, ref: string): boolean {
  try {
    git(cwd, ["show-ref", "--verify", "--quiet", ref]);
    return true;
  } catch (error) {
    if ((error as { readonly status?: number }).status === 1) return false;
    throw error;
  }
}

export function gitShowFileAtRef(cwd: string, ref: string, file: string): string {
  return git(cwd, ["show", `${ref}:${file}`]) + "\n";
}

export const makeOrchestrationIntegrationHarness = Effect.fn("makeOrchestrationIntegrationHarness")(
  function* (driverName: "codex" | "claudeAgent") {
    const fs = yield* FileSystem.FileSystem;
    // The focused runner routes Node's scoped temporary directories to its
    // unique disk-backed TMPDIR and removes the invocation root after exit.
    const scratch = process.env.TMPDIR;
    if (
      scratch === undefined ||
      !NodePath.isAbsolute(scratch) ||
      scratch === "/tmp" ||
      scratch.startsWith("/tmp/")
    ) {
      return yield* Effect.die("Run this fixture with a unique disk-backed TMPDIR.");
    }
    // Register the captured root's cleanup before any Git or database acquisition.
    const root = yield* fs.makeTempDirectoryScoped({ directory: scratch, prefix: "t3-engine-v2-" });
    const repositoryDir = NodePath.join(root, "repository");
    const workspaceDir = NodePath.join(root, "linked-worktree");
    yield* fs.makeDirectory(repositoryDir);
    yield* Effect.sync(() => {
      git(repositoryDir, ["init", "--initial-branch=main"]);
      git(repositoryDir, ["config", "user.name", "T3 Integration"]);
      git(repositoryDir, ["config", "user.email", "integration@example.invalid"]);
    });
    yield* fs.writeFileString(NodePath.join(repositoryDir, "README.md"), "v1\n");
    yield* Effect.sync(() => {
      git(repositoryDir, ["add", "README.md"]);
      git(repositoryDir, ["commit", "-m", "fixture baseline"]);
      git(repositoryDir, ["worktree", "add", "-b", "fixture-linked", workspaceDir]);
    });

    const driver = ProviderDriverKind.make(driverName);
    const instanceId = ProviderInstanceId.make(driverName);
    const capabilities =
      driverName === "claudeAgent" ? ClaudeProviderCapabilitiesV2 : CodexProviderCapabilitiesV2;
    const modelSelection = { instanceId, model: "local-fixture" };
    const threadId = ThreadId.make("thread:linked-worktree");
    const projectId = ProjectId.make("project:engine-fixture");
    const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    yield* Effect.addFinalizer(() => Queue.shutdown(events));
    const scripts: LocalTurn[] = [];
    const started: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
    const approvalResponses: ProviderAdapter.ProviderAdapterV2RuntimeRequestResponseInput[] = [];
    const rollbackCalls: ProviderAdapter.ProviderAdapterV2RollbackThreadInput[] = [];
    const stages: string[] = [];
    let openedRuntimeCount = 0;
    let messages: OrchestrationV2ConversationMessage[] = [];
    let providerTurns: OrchestrationV2ProviderTurn[] = [];
    const pending = new Map<
      RuntimeRequestId,
      Effect.Effect<void, ProviderAdapter.ProviderAdapterV2Error>
    >();

    const adapter: ProviderAdapter.ProviderAdapterV2Shape = {
      instanceId,
      driver,
      getCapabilities: () => Effect.succeed(capabilities),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
      openSession: (input) =>
        Effect.gen(function* () {
          stages.push("session.open");
          // Correlate this actual local runtime closure; this is no process or
          // qualification attestation. It stays immutable for the open's lifetime.
          const runtimeGeneration = `local-runtime:${driverName}:${++openedRuntimeCount}`;
          const now = yield* DateTime.now;
          return {
            instanceId,
            driver,
            providerSessionId: input.providerSessionId,
            runtimeGeneration,
            providerSession: {
              id: input.providerSessionId,
              driver,
              providerInstanceId: instanceId,
              status: "ready",
              cwd: workspaceDir,
              model: modelSelection.model,
              capabilities,
              createdAt: now,
              updatedAt: now,
              lastError: null,
            },
            events: Stream.fromQueue(events),
            ensureThread: (selection) =>
              Effect.sync(() => {
                stages.push("thread.ensure");
                const localThread: OrchestrationV2ProviderThread =
                  selection.existingProviderThread ?? {
                    id: ProviderThreadId.make(`provider:${selection.threadId}`),
                    driver,
                    providerInstanceId: instanceId,
                    providerSessionId: input.providerSessionId,
                    appThreadId: selection.threadId,
                    ownerNodeId: null,
                    nativeThreadRef: null,
                    nativeConversationHeadRef: null,
                    status: "idle",
                    firstRunOrdinal: null,
                    lastRunOrdinal: null,
                    handoffIds: [],
                    forkedFrom: null,
                    createdAt: now,
                    updatedAt: now,
                  };
                // Adopt the persisted placeholder's row rather than returning it
                // uninitialized or allocating a second application provider row.
                return {
                  ...localThread,
                  appThreadId: selection.threadId,
                  providerSessionId: input.providerSessionId,
                  nativeThreadRef: localThread.nativeThreadRef ?? {
                    driver,
                    nativeId: `local:${selection.threadId}`,
                    strength: "strong",
                  },
                } satisfies OrchestrationV2ProviderThread;
              }),
            resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
            startTurn: (turn) =>
              Effect.gen(function* () {
                stages.push("turn.start");
                const script = scripts.shift();
                if (script === undefined || turn.runtimePolicy.cwd !== workspaceDir) {
                  return yield* new ProviderAdapter.ProviderAdapterProtocolError({
                    driver,
                    detail: "The local tool received an unexpected script or workspace.",
                  });
                }
                started.push(turn);
                const startedAt = yield* DateTime.now;
                messages.push({
                  id: turn.message.messageId,
                  threadId: turn.threadId,
                  runId: turn.runId,
                  nodeId: turn.rootNodeId,
                  role: "user",
                  text: turn.message.text,
                  attachments: turn.message.attachments,
                  streaming: false,
                  createdAt: startedAt,
                  updatedAt: startedAt,
                  createdBy: turn.message.createdBy,
                  creationSource: turn.message.creationSource,
                });
                const providerTurn: OrchestrationV2ProviderTurn = {
                  id: ProviderTurnId.make(`turn:${turn.attemptId}`),
                  providerThreadId: turn.providerThread.id,
                  nodeId: turn.rootNodeId,
                  runAttemptId: turn.attemptId,
                  ordinal: turn.providerTurnOrdinal,
                  nativeTurnRef: {
                    driver,
                    nativeId: `local-turn:${turn.attemptId}`,
                    strength: "strong",
                  },
                  status: "running",
                  startedAt,
                  completedAt: null,
                };
                yield* Queue.offer(events, { type: "provider_turn.updated", driver, providerTurn });
                const finish = Effect.gen(function* () {
                  const completedAt = yield* DateTime.now;
                  if (script.contents !== undefined) {
                    const file = NodePath.join(workspaceDir, "README.md");
                    const oldStr = yield* fs.readFileString(file);
                    yield* fs.writeFileString(file, script.contents);
                    yield* Queue.offer(events, {
                      type: "turn_item.updated",
                      driver,
                      turnItem: {
                        id: TurnItemId.make(`edit:${turn.attemptId}`),
                        threadId: turn.threadId,
                        runId: turn.runId,
                        nodeId: turn.rootNodeId,
                        providerThreadId: turn.providerThread.id,
                        providerTurnId: providerTurn.id,
                        nativeItemRef: null,
                        parentItemId: null,
                        ordinal: turn.runOrdinal * 100 + 1,
                        status: "completed",
                        title: "Edit README",
                        startedAt,
                        completedAt,
                        updatedAt: completedAt,
                        type: "file_change",
                        fileName: "README.md",
                        oldStr,
                        newStr: script.contents,
                      },
                    });
                  }
                  const message: OrchestrationV2ConversationMessage = {
                    id: MessageId.make(`assistant:${turn.attemptId}`),
                    threadId: turn.threadId,
                    runId: turn.runId,
                    nodeId: turn.rootNodeId,
                    role: "assistant",
                    text: script.text,
                    attachments: [],
                    streaming: false,
                    createdAt: startedAt,
                    updatedAt: completedAt,
                    createdBy: "agent",
                    creationSource: "provider",
                  };
                  messages.push(message);
                  yield* Queue.offer(events, { type: "message.updated", driver, message });
                  // V2 orders conversation messages by their companion turn items.
                  // Emit the same assistant item shape used by both real adapters.
                  yield* Queue.offer(events, {
                    type: "turn_item.updated",
                    driver,
                    turnItem: {
                      id: TurnItemId.make(`assistant-item:${turn.attemptId}`),
                      threadId: turn.threadId,
                      runId: turn.runId,
                      nodeId: turn.rootNodeId,
                      providerThreadId: turn.providerThread.id,
                      providerTurnId: providerTurn.id,
                      nativeItemRef: null,
                      parentItemId: null,
                      ordinal: turn.runOrdinal * 100 + 2,
                      status: "completed",
                      title: null,
                      startedAt,
                      completedAt,
                      updatedAt: completedAt,
                      type: "assistant_message",
                      messageId: message.id,
                      text: message.text,
                      streaming: false,
                    },
                  });
                  const terminalTurn = {
                    ...providerTurn,
                    status: script.fail ? ("failed" as const) : ("completed" as const),
                    completedAt,
                  };
                  providerTurns.push(terminalTurn);
                  stages.push(`turn.terminal.enqueued:${terminalTurn.status}`);
                  yield* Queue.offer(events, {
                    type: "provider_turn.updated",
                    driver,
                    providerTurn: terminalTurn,
                  });
                  yield* Queue.offer(
                    events,
                    script.fail
                      ? {
                          type: "turn.terminal",
                          driver,
                          providerThreadId: turn.providerThread.id,
                          providerTurnId: providerTurn.id,
                          runOrdinal: turn.runOrdinal,
                          failureItemOrdinal: turn.runOrdinal * 100 + 3,
                          status: "failed",
                          failure: {
                            class: "provider_error",
                            message: "Local fixture failure",
                            code: "fixture",
                            retryable: false,
                          },
                          threadDisposition: "reusable",
                        }
                      : {
                          type: "turn.terminal",
                          driver,
                          providerThreadId: turn.providerThread.id,
                          providerTurnId: providerTurn.id,
                          runOrdinal: turn.runOrdinal,
                          status: "completed",
                          failure: null,
                          threadDisposition: "reusable",
                        },
                  );
                }).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderAdapter.ProviderAdapterProtocolError({
                        driver,
                        detail: "The bounded local tool failed.",
                        cause,
                      }),
                  ),
                );
                if (!script.approval) return yield* finish;
                const requestId = RuntimeRequestId.make(`approval:${turn.attemptId}`);
                pending.set(requestId, finish);
                stages.push("approval.enqueued");
                yield* Queue.offer(events, {
                  type: "runtime_request.updated",
                  driver,
                  threadId: turn.threadId,
                  runtimeRequest: {
                    id: requestId,
                    nodeId: turn.rootNodeId,
                    providerTurnId: providerTurn.id,
                    nativeRequestRef: { driver, nativeId: requestId, strength: "strong" },
                    kind: "file-change",
                    status: "pending",
                    responseCapability: {
                      type: "live",
                      providerSessionId: input.providerSessionId,
                    },
                    createdAt: startedAt,
                    resolvedAt: null,
                  },
                });
              }),
            steerTurn: () => Effect.die("This fixture does not steer turns."),
            interruptTurn: () => Effect.die("This fixture does not interrupt turns."),
            respondToRuntimeRequest: (response) =>
              Effect.gen(function* () {
                approvalResponses.push(response);
                const finish = pending.get(response.requestId);
                if (finish === undefined || response.decision !== "accept") {
                  return yield* new ProviderAdapter.ProviderAdapterProtocolError({
                    driver,
                    detail: "Unexpected approval response.",
                  });
                }
                pending.delete(response.requestId);
                yield* finish;
              }),
            readThreadSnapshot: ({ providerThread }) =>
              Effect.succeed({
                providerThread,
                providerTurns,
                messages,
                runtimeRequests: [],
              }),
            rollbackThread: (rollback) =>
              Effect.sync(() => {
                rollbackCalls.push(rollback);
                const retained = new Set(
                  started
                    .filter((turn) => turn.runOrdinal <= rollback.target.appRunOrdinal)
                    .map((turn) => turn.runId),
                );
                messages = messages.filter(
                  (message) => message.runId !== null && retained.has(message.runId),
                );
                providerTurns = providerTurns.filter(
                  (turn) => turn.ordinal <= rollback.target.appRunOrdinal,
                );
                return {
                  providerThread: {
                    ...rollback.providerThread,
                    status: "idle" as const,
                    lastRunOrdinal:
                      rollback.target.appRunOrdinal === 0 ? null : rollback.target.appRunOrdinal,
                  },
                  providerTurns,
                  messages,
                  runtimeRequests: [],
                };
              }),
            forkThread: () => Effect.die("This fixture does not fork provider conversations."),
          } satisfies ProviderAdapter.ProviderAdapterV2SessionRuntime;
        }),
    };

    const databaseLayer = makeSqlitePersistenceLive(NodePath.join(root, "state.sqlite")).pipe(
      Layer.provide(NodeServices.layer),
    );
    const replayLayer = makeOrchestratorV2ReplayLayerWithRegistry(
      { name: `linked-engine-${driverName}` },
      ProviderAdapterRegistry.makeSingleLayer(adapter),
      { databaseLayer, runEffectWorker: false },
    );
    const context = yield* Layer.build(
      Layer.mergeAll(
        replayLayer,
        databaseLayer,
        ProjectStore.layer.pipe(Layer.provide(databaseLayer)),
      ),
    );
    const orchestrator = Context.get(context, Orchestrator.OrchestratorV2);
    const worker = Context.get(context, EffectWorker.OrchestrationEffectWorkerV2);
    const sink = Context.get(context, EventSink.EventSinkV2);
    const outbox = Context.get(context, EffectOutbox.EffectOutboxV2);
    const sql = Context.get(context, SqlClient.SqlClient);
    const projects = Context.get(context, ProjectStore.ProjectStoreV2);
    const timestamp = DateTime.formatIso(yield* DateTime.now);
    yield* projects.apply({
      sequence: 1,
      eventId: EventId.make("project:fixture"),
      aggregateKind: "project",
      aggregateId: projectId,
      occurredAt: timestamp,
      commandId: null,
      causationEventId: null,
      correlationId: null,
      metadata: {},
      type: "project.created",
      payload: {
        projectId,
        title: "Engine fixture",
        workspaceRoot: repositoryDir,
        defaultModelSelection: modelSelection,
        scripts: [],
        createdAt: timestamp,
        updatedAt: timestamp,
      },
    });
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("thread:create"),
      threadId,
      projectId,
      title: "Linked worktree",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: "fixture-linked",
      worktreePath: workspaceDir,
      createdBy: "user",
      creationSource: "web",
    });
    const primaryThreadId = ThreadId.make("thread:primary-checkout");
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("thread:primary:create"),
      threadId: primaryThreadId,
      projectId,
      title: "Unrelated primary checkout",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: "main",
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });

    const diagnostics = Effect.gen(function* () {
      const projection = yield* orchestrator.getThreadProjection(threadId);
      const effects = yield* sql<{
        readonly effect_type: string;
        readonly status: string;
        readonly available_at: string;
        readonly last_error: string | null;
      }>`
        SELECT effect_type, status, available_at, last_error FROM orchestration_v2_effect_outbox
        WHERE thread_id = ${threadId} ORDER BY created_at, effect_id LIMIT 32
      `;
      const holds = yield* outbox.listHeldByThreadId(threadId);
      return {
        stages: stages.slice(-32),
        scriptCount: scripts.length,
        startedCount: started.length,
        runs: projection.runs.map((run) => ({
          ordinal: run.ordinal,
          status: run.status,
          activeAttemptId: run.activeAttemptId,
          checkpointId: run.checkpointId,
        })),
        providerTurns: projection.providerTurns.map((turn) => ({
          ordinal: turn.ordinal,
          status: turn.status,
        })),
        requests: projection.runtimeRequests.map((request) => ({
          id: request.id,
          status: request.status,
        })),
        effects,
        holds,
      };
    });
    const waitForProjection = (
      predicate: (projection: OrchestrationV2ThreadProjection) => boolean,
    ) =>
      sink.stream({ threadId, afterSequence: 0 }).pipe(
        Stream.mapEffect(() => orchestrator.getThreadProjection(threadId)),
        Stream.filter(predicate),
        Stream.runHead,
        Effect.map(Option.getOrThrow),
        Effect.timeout("15 seconds"),
        Effect.catchCause((cause) =>
          diagnostics.pipe(
            Effect.flatMap((state) => Effect.logError("V2 engine fixture wait failed", state)),
            Effect.catchCause(() =>
              Effect.logError("V2 engine fixture diagnostics unavailable", {
                stages: stages.slice(-32),
                startedCount: started.length,
              }),
            ),
            Effect.andThen(Effect.failCause(cause)),
          ),
        ),
      );
    const settle = (ordinal: number) =>
      Effect.gen(function* () {
        yield* waitForProjection((projection) =>
          projection.runs.some(
            (run) =>
              run.ordinal === ordinal &&
              (run.status === "waiting" || run.status === "completed" || run.status === "failed"),
          ),
        );
        // The real terminal transaction enqueues capture before publishing waiting.
        // Drain the same worker through finalization before the next edit or rollback.
        yield* worker.drain(32);
        return yield* orchestrator.getThreadProjection(threadId);
      });
    return {
      root,
      repositoryDir,
      workspaceDir,
      threadId,
      primaryThreadId,
      modelSelection,
      orchestrator,
      worker,
      sink,
      outbox,
      sql,
      fs,
      started,
      approvalResponses,
      rollbackCalls,
      waitForProjection,
      settle,
      conversation: () => messages.map((message) => [message.role, message.text]),
      dispatchTurn: (script: LocalTurn) =>
        Effect.gen(function* () {
          scripts.push(script);
          const ordinal = started.length + 1;
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`dispatch:${ordinal}`),
            threadId,
            messageId: MessageId.make(`user:${ordinal}`),
            text: `Request ${ordinal}`,
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          const drained = yield* worker.drain(32);
          stages.push(`dispatch.drain:${drained}`);
          return ordinal;
        }),
    };
  },
  Effect.provide(NodeServices.layer),
);
