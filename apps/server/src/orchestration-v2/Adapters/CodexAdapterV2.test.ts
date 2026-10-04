import * as NodeOS from "node:os";

import { historyResponseItems } from "../ContextHandoffBudget.ts";
import type { ProviderAdapterV2HistoricalContext } from "../ProviderAdapter.ts";
import {
  makeProviderTextDeltaCoalescer,
  type ProviderTextDeltaUpdate,
} from "./ProviderTextDeltaCoalescer.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  CheckpointId,
  CheckpointScopeId,
  CodexSettings,
  EnvironmentId,
  EventId,
  MessageId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import { HostProcessEnvironment, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { SpawnExecutableResolution } from "@t3tools/shared/shell";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexErrors from "effect-codex-app-server/errors";
import * as CodexReplay from "effect-codex-app-server/replay";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Predicate from "effect/Predicate";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import packageJson from "../../../package.json" with { type: "json" };
import * as ServerConfig from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import type { McpCapability } from "../../mcp/McpInvocationContext.ts";
import type { EventNdjsonLogger } from "../../provider/Layers/EventNdjsonLogger.ts";
import * as ProviderEventLoggers from "../../provider/Layers/ProviderEventLoggers.ts";
import * as ProcessAttribution from "../../resourceTelemetry/ProcessAttribution.ts";
import * as IdAllocator from "../IdAllocator.ts";
import * as EffectWorker from "../EffectWorker.ts";
import * as Orchestrator from "../Orchestrator.ts";
import * as EventSink from "../EventSink.ts";
import { planProjectCommand } from "../ProjectCommands.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "../testkit/ProviderReplayHarness.ts";
import {
  ProviderAdapterEnsureThreadError,
  ProviderAdapterForkThreadError,
  ProviderAdapterOpenSessionError,
  ProviderAdapterProtocolError,
  ProviderAdapterResumeThreadError,
  ProviderAdapterTurnStartError,
  ProviderAdapterRollbackThreadError,
  ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2TurnInput,
  type ProviderPendingStartStopInput,
} from "../ProviderAdapter.ts";
import type { ProviderContinuationRequest } from "../ProviderContinuationRequests.ts";
import { readProviderEventOrigin } from "../ProviderEventOrigin.ts";
import {
  prepareProviderManagedActorRun,
  withProviderManagedActorExecution,
  type ProviderManagedActorAdmissionV1,
} from "../ProviderManagedActorCompletion.ts";
import {
  OrdinaryCheckoutAdmissionV1,
  OrdinaryCheckoutCaptureV1,
  OrdinaryCheckoutExecutionExecutorV1,
  OrdinaryCheckoutUseV1,
  makeOrdinaryCheckoutExecutionRefV1,
  ordinaryApplicationIncarnationV1,
  ordinaryCheckoutAdmissionIdV1,
  ordinaryCheckoutAdmissionRefV1,
  ordinaryCheckoutCommandDigestV1,
} from "../OrdinaryCheckoutOwnership.ts";
import { NativeCreationAuthorityError } from "../NativeCreationAuthority.ts";
import * as CodexAdapterV2 from "./CodexAdapterV2.ts";
import {
  makeCodexNativeCreationFixture,
  makeReplayServerConfig,
  makeCodexProviderAdapterRegistryReplayLayer,
  withCodexReplayChildMetadata,
} from "./CodexAdapterV2.testkit.ts";

const encodeUnknownJson = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const replayTranscriptJson = Schema.fromJsonString(CodexReplay.CodexAppServerReplayTranscript);
const encodeReplayTranscriptJson = Schema.encodeEffect(replayTranscriptJson);
const decodeReplayTranscriptJson = Schema.decodeUnknownEffect(replayTranscriptJson);
const encodeStringJson = Schema.encodeEffect(Schema.fromJsonString(Schema.String));

const seedCodexReplayProject = (projectId: ProjectId, threadId: ThreadId, workspaceRoot: string) =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const commandId = CommandId.make(`create-codex-project:${projectId}`);
    const now = yield* DateTime.now;
    const planned = planProjectCommand({
      command: {
        type: "project.create",
        commandId,
        projectId,
        title: "Codex background Stop",
        workspaceRoot,
      },
      state: { project: undefined, workspaceOwner: undefined },
      eventId: EventId.make(`codex-project-created:${projectId}`),
      now,
    });
    if (planned._tag === "Failure") return yield* Effect.fail(planned.failure);
    const committed = yield* sink.commitProjectCommand({
      commandId,
      projectId,
      commandType: "project.create",
      acceptedAt: now,
      event: planned.success,
    });
    assert.isTrue(committed.committed);
    assert.equal(committed.receipt.status, "accepted");
    assert.isAbove(committed.receipt.resultSequence, 0);
    const identity = yield* sink.readCommandReceiptIdentity(commandId);
    assert.isNotNull(identity.projectReceipt);
    assert.equal(identity.projectReceipt?.commandId, commandId);
    assert.equal(identity.projectReceipt?.projectId, projectId);
    assert.equal(identity.projectReceipt?.commandType, "project.create");
    assert.equal(identity.projectReceipt?.status, "accepted");
    assert.equal(identity.projectReceipt?.resultSequence, committed.receipt.resultSequence);
    const facts = yield* sink.readNativeCommandFacts({
      threadId,
      commandId,
      authority: { projectId },
    });
    assert.deepEqual(
      facts.commitSnapshot.records.project?.map((row) => ({
        projectId: row.project_id,
        workspaceRoot: row.workspace_root,
      })),
      [{ projectId, workspaceRoot }],
    );
    assert.isNull(facts.projection);
  });

describe("Codex replay fixture cleanup", () => {
  it.effect("removes its exact scratch root when the fixture scope succeeds", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const config = yield* makeReplayServerConfig("cleanup-success").pipe(Effect.scoped);
      assert.isFalse(yield* fileSystem.exists(config.baseDir));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("removes its scratch root while preserving a fixture failure", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      let baseDir: string | undefined;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          const config = yield* makeReplayServerConfig("cleanup-failure");
          baseDir = config.baseDir;
          return yield* Effect.fail("deliberate-fixture-failure");
        }),
      ).pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") assert.equal(result.failure, "deliberate-fixture-failure");
      assert.isDefined(baseDir);
      assert.isFalse(yield* fileSystem.exists(baseDir!));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("removes its scratch root when a running fixture is interrupted", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const ready = yield* Deferred.make<string>();
      const fiber = yield* Effect.scoped(
        Effect.gen(function* () {
          const config = yield* makeReplayServerConfig("cleanup-interrupted");
          yield* Deferred.succeed(ready, config.baseDir);
          return yield* Effect.never;
        }),
      ).pipe(Effect.forkChild);
      const baseDir = yield* Deferred.await(ready);
      yield* Fiber.interrupt(fiber);
      assert.isFalse(yield* fileSystem.exists(baseDir));
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});

describe("Codex context usage compatibility", () => {
  const previous: ModelSelection = {
    instanceId: ProviderInstanceId.make("codex"),
    model: "gpt-6-astra",
  };
  it("retains measured usage for reasoning-only changes in either direction", () => {
    const low: ModelSelection = { ...previous, options: [{ id: "reasoningEffort", value: "low" }] };
    assert.isTrue(CodexAdapterV2.canReuseCodexContextUsage(previous, low));
    assert.isTrue(CodexAdapterV2.canReuseCodexContextUsage(low, previous));
    assert.isTrue(
      CodexAdapterV2.canReuseCodexContextUsage(low, {
        ...low,
        options: [{ id: "reasoningEffort", value: "high" }],
      }),
    );
  });
  it("invalidates usage for model, instance, context-window and unknown option changes", () => {
    for (const next of [
      { ...previous, model: "other-model" },
      { ...previous, instanceId: ProviderInstanceId.make("other-codex") },
      { ...previous, options: [{ id: "contextWindow", value: "32k" }] },
      { ...previous, options: [{ id: "customOption", value: "value" }] },
    ])
      assert.isFalse(CodexAdapterV2.canReuseCodexContextUsage(previous, next));
  });
});

describe("CodexAdapterV2 file change approvals", () => {
  it("uses nonblank reasons before sorted file operations and renamed paths", () => {
    const fileChanges = {
      "/tmp/removed.md": { type: "delete" as const, content: "gone" },
      "/tmp/added.ts": { type: "add" as const, content: "export {};" },
      "/tmp/moved.ts": {
        type: "update" as const,
        unified_diff: "@@",
        move_path: "/tmp/renamed.ts",
      },
    };
    assert.equal(
      CodexAdapterV2.codexFileChangeApprovalPrompt({
        reason: "  Update configuration. ",
        fileChanges,
      }),
      "Update configuration.",
    );
    assert.equal(
      CodexAdapterV2.codexFileChangeApprovalPrompt({ reason: " ", fileChanges }),
      "add /tmp/added.ts\nupdate /tmp/moved.ts -> /tmp/renamed.ts\ndelete /tmp/removed.md",
    );
  });

  it("falls back to a nonblank grant root and omits empty details", () => {
    assert.equal(
      CodexAdapterV2.codexFileChangeApprovalPrompt({ reason: " ", grantRoot: " /workspace " }),
      "/workspace",
    );
    assert.equal(
      CodexAdapterV2.codexFileChangeApprovalPrompt({ fileChanges: {}, grantRoot: "/workspace" }),
      "/workspace",
    );
    assert.isUndefined(
      CodexAdapterV2.codexFileChangeApprovalPrompt({
        reason: " ",
        grantRoot: " ",
        fileChanges: {},
      }),
    );
  });

  it("bounds large patch descriptions without losing the remaining count", () => {
    const fileChanges = Object.fromEntries(
      Array.from({ length: 25 }, (_, index) => [
        `/tmp/file-${String(index).padStart(2, "0")}.ts`,
        { type: "add" as const, content: "" },
      ]),
    );
    const detail = CodexAdapterV2.codexFileChangeApprovalPrompt({ fileChanges });
    assert.equal(detail?.split("\n").length, 21);
    assert.isTrue(detail?.startsWith("add /tmp/file-00.ts") ?? false);
    assert.isTrue(detail?.endsWith("+5 more") ?? false);
    assert.notInclude(detail, "file-20.ts");
  });
});

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
  it.effect("makes accumulated assistant text visible after the bounded flush interval", () =>
    Effect.gen(function* () {
      const updates = yield* Ref.make<
        ReadonlyArray<{
          readonly turnId: string;
          readonly itemId: string;
          readonly text: string;
          readonly completed: boolean;
        }>
      >([]);
      const coalescer = yield* makeProviderTextDeltaCoalescer({
        flushIntervalMs: 50,
        emit: (update) => Ref.update(updates, (current) => [...current, update]),
      });

      yield* coalescer.append({ turnId: "turn-1", itemId: "message-1", delta: "partial" });
      assert.deepEqual(yield* Ref.get(updates), []);
      yield* Effect.yieldNow;
      yield* TestClock.adjust("50 millis");
      yield* Effect.yieldNow;

      assert.deepEqual(yield* Ref.get(updates), [
        {
          turnId: "turn-1",
          itemId: "message-1",
          text: "partial",
          completed: false,
        },
      ]);
    }),
  );

  it.effect("coalesces multiple token deltas into one assistant update per interval", () =>
    Effect.gen(function* () {
      const updates = yield* Ref.make<ReadonlyArray<ProviderTextDeltaUpdate>>([]);
      const coalescer = yield* makeProviderTextDeltaCoalescer({
        flushIntervalMs: 50,
        emit: (update) => Ref.update(updates, (current) => [...current, update]),
      });

      yield* coalescer.append({ turnId: "turn-1", itemId: "message-1", delta: "one" });
      yield* coalescer.append({ turnId: "turn-1", itemId: "message-1", delta: " two" });
      yield* coalescer.append({ turnId: "turn-1", itemId: "message-1", delta: " three" });
      yield* Effect.yieldNow;
      yield* TestClock.adjust("50 millis");
      yield* Effect.yieldNow;

      assert.deepEqual(yield* Ref.get(updates), [
        {
          turnId: "turn-1",
          itemId: "message-1",
          text: "one two three",
          completed: false,
        },
      ]);
    }),
  );

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

  it.effect("retains buffered text until completion updates are emitted", () =>
    Effect.gen(function* () {
      const updates = yield* Ref.make<ReadonlyArray<ProviderTextDeltaUpdate>>([]);
      const failNext = yield* Ref.make(true);
      const coalescer = yield* makeProviderTextDeltaCoalescer({
        flushIntervalMs: 50,
        emit: (update) =>
          Ref.getAndSet(failNext, false).pipe(
            Effect.flatMap((shouldFail) =>
              shouldFail
                ? Effect.die("projection unavailable")
                : Ref.update(updates, (current) => [...current, update]),
            ),
          ),
      });

      yield* coalescer.append({ turnId: "turn-1", itemId: "message-1", delta: "turn final" });
      const failedFlush = yield* coalescer.flushTurn("turn-1").pipe(Effect.exit);
      assert.equal(failedFlush._tag, "Failure");
      yield* coalescer.flushTurn("turn-1");

      yield* coalescer.append({ turnId: "turn-1", itemId: "message-2", delta: "item final" });
      yield* Ref.set(failNext, true);
      const failedComplete = yield* coalescer
        .complete({ turnId: "turn-1", itemId: "message-2" })
        .pipe(Effect.exit);
      assert.equal(failedComplete._tag, "Failure");
      const completedText = yield* coalescer.complete({
        turnId: "turn-1",
        itemId: "message-2",
      });

      assert.equal(completedText, "item final");
      assert.deepEqual(yield* Ref.get(updates), [
        { turnId: "turn-1", itemId: "message-1", text: "turn final", completed: true },
        { turnId: "turn-1", itemId: "message-2", text: "item final", completed: true },
      ]);
    }),
  );

  it.effect("can discard an empty completion without emitting an assistant update", () =>
    Effect.gen(function* () {
      const updates = yield* Ref.make<ReadonlyArray<ProviderTextDeltaUpdate>>([]);
      const coalescer = yield* makeProviderTextDeltaCoalescer({
        flushIntervalMs: 50,
        emit: (update) => Ref.update(updates, (current) => [...current, update]),
      });

      yield* coalescer.append({ turnId: "turn-1", itemId: "message-1", delta: "" });
      yield* Effect.yieldNow;
      yield* TestClock.adjust("50 millis");
      yield* Effect.yieldNow;
      const completedText = yield* coalescer.complete({
        turnId: "turn-1",
        itemId: "message-1",
        finalText: "",
        emitEmpty: false,
      });

      assert.equal(completedText, "");
      assert.deepEqual(yield* Ref.get(updates), []);

      yield* coalescer.append({ turnId: "turn-1", itemId: "message-2", delta: "buffered" });
      assert.equal(
        yield* coalescer.complete({
          turnId: "turn-1",
          itemId: "message-2",
          emitEmpty: false,
        }),
        "buffered",
      );
      assert.deepEqual(yield* Ref.get(updates), [
        {
          turnId: "turn-1",
          itemId: "message-2",
          text: "buffered",
          completed: true,
        },
      ]);
    }),
  );

  it.effect("treats explicit empty final text as authoritative over buffered deltas", () =>
    Effect.gen(function* () {
      const updates = yield* Ref.make<ReadonlyArray<ProviderTextDeltaUpdate>>([]);
      const coalescer = yield* makeProviderTextDeltaCoalescer({
        flushIntervalMs: 50,
        emit: (update) => Ref.update(updates, (current) => [...current, update]),
      });

      yield* coalescer.append({
        turnId: "turn-1",
        itemId: "message-1",
        delta: "stale buffered text",
      });
      const completedText = yield* coalescer.complete({
        turnId: "turn-1",
        itemId: "message-1",
        finalText: "",
      });

      assert.equal(completedText, "");
      assert.deepEqual(yield* Ref.get(updates), [
        {
          turnId: "turn-1",
          itemId: "message-1",
          text: "",
          completed: true,
        },
      ]);
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

  it.effect("preserves explicit Codex turn policy overrides", () =>
    Effect.gen(function* () {
      const params = yield* CodexAdapterV2.buildCodexTurnStartParams({
        nativeThreadId: "native-override",
        codexInput: [{ type: "text", text: "test" }],
        runtimePolicy: {
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: null,
          approvalPolicy: "on-request",
          sandboxPolicy: {
            type: "readOnly",
          },
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.4",
        },
      });

      assert.equal(params.approvalPolicy, "on-request");
      assert.equal(params.sandboxPolicy?.type, "readOnly");
    }),
  );

  it.effect("adds default-mode developer instructions when the T3 MCP server is attached", () =>
    Effect.gen(function* () {
      const params = yield* CodexAdapterV2.buildCodexTurnStartParams({
        nativeThreadId: "native-orchestration-instructions",
        codexInput: [{ type: "text", text: "delegate this task" }],
        runtimePolicy: {
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: null,
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.4",
        },
        hasT3Mcp: true,
      });

      assert.equal(params.collaborationMode?.mode, "default");
      assert.include(
        params.additionalContext?.t3_code_orchestration?.value ?? "",
        "Use `delegate_task`",
      );
      assert.include(
        params.additionalContext?.t3_code_orchestration?.value ?? "",
        "structured object, never as JSON text",
      );
    }),
  );

  it.effect("omits default-mode collaboration settings without the T3 MCP server", () =>
    Effect.gen(function* () {
      const params = yield* CodexAdapterV2.buildCodexTurnStartParams({
        nativeThreadId: "native-default-without-t3-mcp",
        codexInput: [{ type: "text", text: "implement this task" }],
        runtimePolicy: {
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: null,
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.4",
        },
        hasT3Mcp: false,
      });

      assert.isUndefined(params.collaborationMode);
    }),
  );

  it.effect("adds T3 plan-mode developer instructions when the T3 MCP server is attached", () =>
    Effect.gen(function* () {
      const params = yield* CodexAdapterV2.buildCodexTurnStartParams({
        nativeThreadId: "native-plan-with-t3-mcp",
        codexInput: [{ type: "text", text: "plan this task" }],
        runtimePolicy: {
          runtimeMode: "full-access",
          interactionMode: "plan",
          cwd: null,
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.4",
        },
        hasT3Mcp: true,
      });

      assert.equal(params.collaborationMode?.mode, "plan");
      assert.include(
        params.collaborationMode?.settings.developer_instructions ?? "",
        "request_user_input",
      );
      assert.include(params.additionalContext?.t3_code_tools?.value ?? "", "preview_status");
    }),
  );

  it.effect("keeps Codex in plan mode without referencing unavailable T3 MCP tools", () =>
    Effect.gen(function* () {
      const params = yield* CodexAdapterV2.buildCodexTurnStartParams({
        nativeThreadId: "native-plan-without-t3-mcp",
        codexInput: [{ type: "text", text: "plan this task" }],
        runtimePolicy: {
          runtimeMode: "full-access",
          interactionMode: "plan",
          cwd: null,
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.4",
        },
        hasT3Mcp: false,
      });

      assert.equal(params.collaborationMode?.mode, "plan");
      assert.notProperty(params.collaborationMode?.settings, "developer_instructions");
    }),
  );

  it.effect("compiles per-turn Codex model options and cwd from their owning inputs", () =>
    Effect.gen(function* () {
      const params = yield* CodexAdapterV2.buildCodexTurnStartParams({
        nativeThreadId: "native-model-options",
        codexInput: [{ type: "text", text: "test" }],
        runtimePolicy: {
          runtimeMode: "full-access",
          interactionMode: "plan",
          cwd: "/workspace/model-options",
          reasoningEffort: "low",
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.4",
          options: [
            { id: "reasoningEffort", value: "xhigh" },
            { id: "serviceTier", value: "priority" },
          ],
        },
      });

      assert.equal(params.model, "gpt-5.4");
      assert.equal(params.effort, "xhigh");
      assert.equal(params.serviceTier, "priority");
      assert.equal(params.cwd, "/workspace/model-options");
      assert.equal(params.collaborationMode?.settings.model, "gpt-5.4");
      assert.equal(params.collaborationMode?.settings.reasoning_effort, "xhigh");

      // ChatGPT token sharing rejects service tiers, so managed sessions drop a stale pick.
      const managed = yield* CodexAdapterV2.buildCodexTurnStartParams({
        nativeThreadId: "native-model-options",
        codexInput: [{ type: "text", text: "test" }],
        runtimePolicy: {
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: "/workspace/model-options",
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.4",
          options: [{ id: "serviceTier", value: "priority" }],
        },
        omitServiceTier: true,
      });
      assert.equal(managed.serviceTier, undefined);
    }),
  );
});

describe("CodexAdapterV2 process spawning", () => {
  for (const state of ["current", "revoked", "resource-mismatch", "missing-directory"] as const) {
    it.effect(
      `checks ${state} issued native creation authority immediately before the actual spawn seam`,
      () =>
        Effect.gen(function* () {
          const fixture = yield* makeCodexNativeCreationFixture(`spawn-${state}`);
          assert.equal(fixture.startCount, 1);
          if (state === "revoked") yield* fixture.revoke;
          const execution =
            state === "resource-mismatch"
              ? {
                  ...fixture.execution,
                  resources: {
                    ...fixture.execution.resources,
                    worktreePath: "/synthetic/other-worktree",
                  },
                }
              : fixture.execution;
          let spawns = 0;
          const spawner = ChildProcessSpawner.make((command) =>
            Effect.sync(() => {
              spawns++;
              assert.equal(command._tag, "StandardCommand");
              if (command._tag === "StandardCommand")
                assert.equal(command.options.cwd, fixture.execution.resources.worktreePath);
            }).pipe(
              Effect.andThen(
                Effect.fail(
                  PlatformError.systemError({
                    _tag: "NotFound",
                    module: "ChildProcess",
                    method: "spawn",
                  }),
                ),
              ),
            ),
          );
          const factory = yield* CodexAdapterV2.CodexAppServerClientFactory.pipe(
            Effect.provide(CodexAdapterV2.codexAppServerClientFactoryFromSettingsLayer),
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
            Effect.provideService(
              ProviderEventLoggers.ProviderEventLoggers,
              ProviderEventLoggers.NoOpProviderEventLoggers,
            ),
          );
          const result = yield* factory
            .open({
              instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
              threadId: ThreadId.make(`native-spawn-${state}`),
              providerSessionId: ProviderSessionId.make(`native-spawn-session-${state}`),
              runtimePolicy: {
                ...CODEX_TEST_RUNTIME_POLICY,
                cwd:
                  state === "missing-directory" ? null : fixture.execution.resources.worktreePath,
              },
              settings: DEFAULT_CODEX_SETTINGS,
              environment: {},
              nativeCreationExecution: execution,
            })
            .pipe(Effect.scoped, Effect.result);
          if (
            result._tag !== "Failure" ||
            !Schema.is(ProviderAdapterOpenSessionError)(result.failure)
          )
            return assert.fail("Expected the synthetic factory to reject or fail its fake spawn");
          assert.equal(spawns, state === "current" ? 1 : 0);
          assert.equal(fixture.startCount, 1);
          if (state !== "current") {
            if (!Schema.is(ProviderAdapterProtocolError)(result.failure.cause))
              return assert.fail(
                "Expected actual launch directory or authority validation to reject spawn",
              );
            if (state === "revoked") {
              assert.isTrue(Schema.is(NativeCreationAuthorityError)(result.failure.cause.cause));
              if (Schema.is(NativeCreationAuthorityError)(result.failure.cause.cause))
                assert.equal(result.failure.cause.cause.code, "stale_grant");
            }
          }
        }).pipe(Effect.provideService(HostProcessPlatform, "linux")),
    );
  }

  it.effect("attributes the spawned process only for its open runtime scope", () =>
    Effect.gen(function* () {
      const attribution = yield* ProcessAttribution.make();
      const pid = ChildProcessSpawner.ProcessId(4_242);
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.sync(() => {
          assert.equal(command._tag, "StandardCommand");
          if (command._tag === "StandardCommand") assert.isUndefined(command.options.cwd);
        }).pipe(
          Effect.andThen(
            Effect.succeed(
              ChildProcessSpawner.makeHandle({
                pid,
                exitCode: Effect.never,
                isRunning: Effect.succeed(true),
                kill: () => Effect.void,
                stdin: Sink.drain,
                stdout: Stream.never,
                stderr: Stream.never,
                all: Stream.never,
                getInputFd: () => Sink.drain,
                getOutputFd: () => Stream.empty,
                unref: Effect.succeed(Effect.void),
              }),
            ),
          ),
        ),
      );
      const factory = yield* CodexAdapterV2.CodexAppServerClientFactory.pipe(
        Effect.provide(CodexAdapterV2.codexAppServerClientFactoryFromSettingsLayer),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      );
      const openInput = {
        instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
        threadId: ThreadId.make("thread-process-attribution"),
        providerSessionId: ProviderSessionId.make("session-process-attribution"),
        runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
        settings: DEFAULT_CODEX_SETTINGS,
        environment: {},
        processAttribution: attribution,
      };

      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* factory.open(openInput);
          assert.deepEqual((yield* attribution.snapshot).get(pid)?.owner, {
            kind: "provider",
            threadId: openInput.threadId,
            provider: "codex",
          });
        }),
      );

      assert.isFalse((yield* attribution.snapshot).has(pid));
    }).pipe(Effect.provideService(HostProcessPlatform, "linux")),
  );

  it.effect(
    "retains distinct actual spawned handles for leader exit readback after scope closure",
    () =>
      Effect.gen(function* () {
        const exits = [
          yield* Deferred.make<ChildProcessSpawner.ExitCode>(),
          yield* Deferred.make<ChildProcessSpawner.ExitCode>(),
        ];
        let spawns = 0;
        const handles: ChildProcessSpawner.ChildProcessHandle[] = [];
        const spawner = ChildProcessSpawner.make(() =>
          Effect.sync(() => {
            const exit = exits[spawns++]!;
            const handle = ChildProcessSpawner.makeHandle({
              pid: ChildProcessSpawner.ProcessId(4_242),
              exitCode: Deferred.await(exit),
              isRunning: Deferred.isDone(exit).pipe(Effect.map((done) => !done)),
              kill: () => Effect.die("Exit observation must not signal a process"),
              stdin: Sink.drain,
              stdout: Stream.never,
              stderr: Stream.never,
              all: Stream.never,
              getInputFd: () => Sink.drain,
              getOutputFd: () => Stream.empty,
              unref: Effect.succeed(Effect.void),
            });
            handles.push(handle);
            return handle;
          }),
        );
        const factory = yield* CodexAdapterV2.CodexAppServerClientFactory.pipe(
          Effect.provide(CodexAdapterV2.codexAppServerClientFactoryFromSettingsLayer),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.provideService(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        );
        const clients = [];
        for (const runtimeGeneration of ["owned-generation-one", "owned-generation-two"]) {
          const client = yield* factory
            .open({
              instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
              threadId: ThreadId.make("owned-process-thread"),
              providerSessionId: ProviderSessionId.make("owned-process-session"),
              runtimeGeneration,
              runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
              settings: DEFAULT_CODEX_SETTINGS,
              environment: {},
            })
            .pipe(Effect.scoped);
          assert.isDefined(client.ownedProcess);
          assert.isTrue(Object.isFrozen(client.ownedProcess));
          clients.push(client);
        }
        const first = clients[0]!.ownedProcess!;
        const second = clients[1]!.ownedProcess!;
        assert.equal(first.handle, handles[0]);
        assert.equal(second.handle, handles[1]);
        assert.equal(first.pid, second.pid);
        assert.notEqual(first.handleToken, second.handleToken);
        assert.equal(first.runtimeGeneration, "owned-generation-one");
        assert.equal(second.runtimeGeneration, "owned-generation-two");
        assert.equal(first.providerSessionId, "owned-process-session");
        assert.isTrue(yield* first.handle.isRunning);
        yield* Deferred.succeed(exits[0]!, ChildProcessSpawner.ExitCode(7));
        assert.isFalse(yield* first.handle.isRunning);
        assert.equal(Number(yield* first.handle.exitCode), 7);
        assert.isTrue(yield* second.handle.isRunning);
      }).pipe(Effect.provideService(HostProcessPlatform, "linux")),
  );

  it("injects cwd, model, and MCP authorization into thread-scoped params", () => {
    const threadId = ThreadId.make("thread-codex-mcp");
    McpProviderSession.setMcpProviderSession({
      environmentId: EnvironmentId.make("environment-codex-mcp"),
      threadId,
      providerSessionId: "mcp-session-codex",
      providerInstanceId: ProviderInstanceId.make("codex"),
      endpoint: "http://127.0.0.1:43123/mcp",
      authorizationHeader: "Bearer secret-codex-token",
      capabilities: new Set<McpCapability>(["preview", "orchestration"]),
      browserToolsAvailable: true,
    });

    try {
      assert.deepEqual(
        CodexAdapterV2.codexThreadRuntimeParams({
          threadId,
          modelSelection: { model: "gpt-5.4" },
          runtimePolicy: {
            runtimeMode: "full-access",
            interactionMode: "default",
            cwd: "/workspace/thread-codex-mcp",
          },
        }),
        {
          cwd: "/workspace/thread-codex-mcp",
          model: "gpt-5.4",
          config: {
            "tools.update_plan.enabled": true,
            mcp_servers: {
              "t3-code": {
                url: "http://127.0.0.1:43123/mcp",
                http_headers: {
                  Authorization: "Bearer secret-codex-token",
                },
              },
            },
          },
        },
      );
    } finally {
      McpProviderSession.clearMcpProviderSession(threadId);
    }
  });

  it.effect("resolves Windows command shims through the shared spawn policy", () =>
    Effect.gen(function* () {
      const command = yield* CodexAdapterV2.makeCodexAppServerSpawnCommand({
        command: "codex",
        args: ["app-server", "argument with spaces"],
        cwd: "C:\\workspace",
        env: { CUSTOM: "1" },
        extendEnv: true,
      });

      assert.isTrue(ChildProcess.isStandardCommand(command));
      if (!ChildProcess.isStandardCommand(command)) {
        return;
      }
      assert.equal(command.command, '^"C:\\npm\\codex.cmd^"');
      assert.deepEqual(command.args, ['^"app-server^"', '^"argument^ with^ spaces^"']);
      assert.equal(command.options.shell, true);
      assert.equal(command.options.cwd, "C:\\workspace");
      assert.deepEqual(command.options.env, { CUSTOM: "1" });
      assert.equal(command.options.extendEnv, true);
    }).pipe(
      Effect.provideService(HostProcessPlatform, "win32"),
      Effect.provideService(HostProcessEnvironment, {
        PATH: "C:\\Windows\\System32",
        HOST_ONLY: "1",
      }),
      Effect.provideService(SpawnExecutableResolution, (_command, _platform, environment) => {
        assert.equal(environment.HOST_ONLY, "1");
        assert.equal(environment.CUSTOM, "1");
        return "C:\\npm\\codex.cmd";
      }),
    ),
  );

  it.effect("uses direct execution for native executables", () =>
    Effect.gen(function* () {
      const command = yield* CodexAdapterV2.makeCodexAppServerSpawnCommand({
        command: "codex.exe",
        args: ["app-server"],
      });

      assert.isTrue(ChildProcess.isStandardCommand(command));
      if (!ChildProcess.isStandardCommand(command)) {
        return;
      }
      assert.equal(command.command, "C:\\bin\\codex.exe");
      assert.deepEqual(command.args, ["app-server"]);
      assert.equal(command.options.shell, false);
    }).pipe(
      Effect.provideService(HostProcessPlatform, "win32"),
      Effect.provideService(SpawnExecutableResolution, () => "C:\\bin\\codex.exe"),
    ),
  );

  it.effect("launches the app-server with the configured launch arguments", () =>
    Effect.gen(function* () {
      const spawnedArgs: Array<ReadonlyArray<string>> = [];
      const spawner = ChildProcessSpawner.make((command) => {
        if (ChildProcess.isStandardCommand(command)) spawnedArgs.push(command.args);
        return Effect.fail(
          PlatformError.systemError({ _tag: "NotFound", module: "ChildProcess", method: "spawn" }),
        );
      });
      const factory = yield* CodexAdapterV2.CodexAppServerClientFactory.pipe(
        Effect.provide(CodexAdapterV2.codexAppServerClientFactoryFromSettingsLayer),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      );
      const open = (environment: NodeJS.ProcessEnv) =>
        factory
          .open({
            instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
            threadId: ThreadId.make("thread-launch-args"),
            providerSessionId: ProviderSessionId.make("provider-session-launch-args"),
            runtimePolicy: ProviderAdapterV2RuntimePolicy.make({
              runtimeMode: "full-access",
              interactionMode: "default",
              cwd: "/workspace",
            }),
            settings: {
              ...DEFAULT_CODEX_SETTINGS,
              launchArgs: " --strict-config -c model_reasoning_summary=detailed ",
            },
            environment,
          })
          .pipe(Effect.scoped, Effect.exit);

      yield* open({});
      yield* open({ T3CODE_CODEX_LAUNCH_ARGS: " --enable env-feature " });

      assert.deepEqual(spawnedArgs, [
        ["app-server", "--strict-config", "-c", "model_reasoning_summary=detailed"],
        ["app-server", "--enable", "env-feature"],
      ]);
    }).pipe(Effect.provideService(HostProcessPlatform, "linux")),
  );

  it.effect("expands ~ in the configured binary path before spawning", () =>
    Effect.gen(function* () {
      const spawnedCommands: Array<string> = [];
      const spawner = ChildProcessSpawner.make((command) => {
        if (ChildProcess.isStandardCommand(command)) spawnedCommands.push(command.command);
        return Effect.fail(
          PlatformError.systemError({ _tag: "NotFound", module: "ChildProcess", method: "spawn" }),
        );
      });
      const path = yield* Path.Path;
      const adapter = yield* CodexAdapterV2.createCodexAdapterV2({
        instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
        displayName: undefined,
        environment: [],
        enabled: true,
        config: { ...DEFAULT_CODEX_SETTINGS, binaryPath: "~/bin/codex" },
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            CodexAdapterV2.codexAppServerClientFactoryFromSettingsLayer,
            ServerConfig.layerTest(process.cwd(), { prefix: "t3-codex-binary-home-" }),
          ),
        ),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      );

      yield* adapter
        .openSession({
          threadId: ThreadId.make("thread-binary-home"),
          providerSessionId: ProviderSessionId.make("provider-session-binary-home"),
          modelSelection: CODEX_TEST_MODEL_SELECTION,
          runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
        })
        .pipe(Effect.scoped, Effect.exit);

      assert.deepEqual(spawnedCommands, [path.join(NodeOS.homedir(), "bin", "codex")]);
    }).pipe(
      Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer)),
      Effect.provideService(HostProcessPlatform, "linux"),
    ),
  );
});

describe("CodexAdapterV2 dynamic tool projection", () => {
  it("uses the CUA call title while leaving other MCP titles as tool arguments", () => {
    const call = {
      type: "mcpToolCall" as const,
      id: "inspect",
      server: "cua_repl",
      tool: "js",
      status: "completed" as const,
      arguments: {
        code: "await game.getAXStateAndScreenshot();",
        title: "Inspect Saga music screen",
      },
      result: { content: [] },
    };
    assert.equal(
      CodexAdapterV2.projectCodexDynamicToolItem(call).title,
      "Inspect Saga music screen",
    );
    assert.equal(
      CodexAdapterV2.projectCodexDynamicToolItem({ ...call, arguments: { title: "  " } }).title,
      undefined,
    );
    assert.equal(
      CodexAdapterV2.projectCodexDynamicToolItem({ ...call, server: "github" }).title,
      undefined,
    );
  });

  it("preserves native browser and app icons alongside MCP tool output", () => {
    const browser = CodexAdapterV2.projectCodexDynamicToolItem({
      type: "mcpToolCall",
      id: "browser",
      server: "browser",
      tool: "open",
      status: "completed",
      arguments: {},
      result: {
        content: [],
        _meta: {
          "codex/toolSurface": {
            kind: "browserUse",
            browserFamily: "Chrome",
            screenshot: {
              pageUrl: "https://example.com/docs",
              faviconUrl: "https://example.com/icon.png",
            },
          },
        },
      },
    });
    assert.equal(browser.toolSurface, "browser");
    assert.deepEqual(browser.toolIcon, {
      _tag: "website",
      pageUrl: "https://example.com/docs",
      faviconUrl: "https://example.com/icon.png",
    });
    assert.equal(browser.toolSource?.name, "Chrome");
    const app = CodexAdapterV2.projectCodexDynamicToolItem({
      type: "mcpToolCall",
      id: "app",
      server: "computer",
      tool: "click",
      status: "completed",
      arguments: {},
      result: {
        content: [],
        _meta: {
          "codex/toolSurface": {
            kind: "computerUse",
            app: { kind: "appId", appId: "com.apple.finder" },
          },
        },
      },
    });
    assert.deepEqual(app.toolIcon, {
      _tag: "native-app",
      app: { _tag: "app-id", appId: "com.apple.finder" },
    });
    assert.equal(app.toolSource?.name, "Finder");
  });

  it("preserves MCP arguments and prefers structured output", () => {
    const projection = CodexAdapterV2.projectCodexDynamicToolItem({
      type: "mcpToolCall",
      id: "call-create-threads",
      server: "t3-code",
      tool: "create_threads",
      status: "completed",
      arguments: {
        threads: [{ title: "Fixture child", prompt: "fixture child prompt" }],
      },
      result: {
        content: [{ type: "text", text: '{"threads":[{"threadId":"thread:mcp:fixture:0"}]}' }],
        structuredContent: {
          threads: [{ threadId: "thread:mcp:fixture:0" }],
        },
      },
    });

    assert.deepEqual(projection, {
      toolName: "t3-code.create_threads",
      input: {
        threads: [{ title: "Fixture child", prompt: "fixture child prompt" }],
      },
      output: {
        threads: [{ threadId: "thread:mcp:fixture:0" }],
      },
      status: "completed",
    });
  });

  it("preserves namespaced dynamic tool output", () => {
    const projection = CodexAdapterV2.projectCodexDynamicToolItem({
      type: "dynamicToolCall",
      id: "call-dynamic",
      namespace: "workspace",
      tool: "inspect",
      status: "failed",
      arguments: { path: "package.json" },
      contentItems: [{ type: "inputText", text: "inspection failed" }],
      success: false,
    });

    assert.deepEqual(projection, {
      toolName: "workspace.inspect",
      input: { path: "package.json" },
      output: [{ type: "inputText", text: "inspection failed" }],
      status: "failed",
    });
  });
});

describe("CodexAdapterV2 native protocol logging", () => {
  it.effect("logs decoded app-server frames once with credentials redacted", () =>
    Effect.gen(function* () {
      const writes: Array<{
        readonly event: unknown;
        readonly threadId: ThreadId | null;
      }> = [];
      const logger: EventNdjsonLogger = {
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

  it.effect("filters streaming frames before redaction without losing decode failures", () =>
    Effect.gen(function* () {
      const writes: Array<unknown> = [];
      const protocolLogger = CodexAdapterV2.makeCodexAppServerProtocolLogger({
        nativeEventLogger: {
          filePath: "/tmp/events.log",
          write: (event) =>
            Effect.sync(() => {
              writes.push(event);
            }),
          close: () => Effect.void,
        },
        threadId: ThreadId.make("thread-1"),
        providerSessionId: ProviderSessionId.make("provider-session-1"),
      });
      assert.exists(protocolLogger);
      if (protocolLogger === undefined) return;

      yield* protocolLogger({
        direction: "incoming",
        stage: "decoded",
        payload: {
          method: "item/agentMessage/delta",
          get params() {
            throw new Error("delta must not be copied");
          },
        },
      });
      yield* protocolLogger({
        direction: "incoming",
        stage: "raw",
        get payload() {
          throw new Error("raw frame must not be parsed");
        },
      });
      yield* protocolLogger({
        direction: "incoming",
        stage: "decode_failed",
        payload: { operation: "decode", method: "turn/completed", issueCount: 1 },
      });

      assert.equal(writes.length, 1);
      assert.nestedPropertyVal(writes[0], "event.stage", "decode_failed");
      assert.nestedPropertyVal(writes[0], "event.payload.method", "turn/completed");
    }),
  );

  it.effect("retains redacted failures when large payloads are summarized", () =>
    Effect.gen(function* () {
      const writes: Array<unknown> = [];
      const protocolLogger = CodexAdapterV2.makeCodexAppServerProtocolLogger({
        nativeEventLogger: {
          filePath: "/tmp/events.log",
          write: (event) =>
            Effect.sync(() => {
              writes.push(event);
            }),
          close: () => Effect.void,
        },
        threadId: ThreadId.make("thread-1"),
        providerSessionId: ProviderSessionId.make("provider-session-1"),
      });
      assert.exists(protocolLogger);
      if (protocolLogger === undefined) return;

      yield* protocolLogger({
        direction: "incoming",
        stage: "decoded",
        payload: {
          method: "error",
          params: {
            threadId: "native-thread",
            turnId: "native-turn",
            error: {
              code: "unauthorized",
              message: '{"message":"Unauthorized","Authorization":"Bearer secret-token"}',
            },
            history: "x".repeat(128 * 1_024),
          },
        },
      });

      const serialized = encodeUnknownJson(writes);
      assert.equal(writes.length, 1);
      assert.isBelow(serialized.length, 2_048);
      assert.notInclude(serialized, "secret-token");
      assert.include(serialized, "[REDACTED]");
      assert.nestedPropertyVal(writes[0], "event.payload.params.error.code", "unauthorized");
      assert.nestedPropertyVal(writes[0], "event.payload.params.turnId", "native-turn");
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

      const numTurns = yield* CodexAdapterV2.resolveCodexRollbackTurnCount({
        providerThread,
        target: {
          type: "provider_turn",
          checkpointId: CheckpointId.make("checkpoint-first"),
          appRunOrdinal: 1,
          providerTurn: firstTurn,
        },
        providerThreadTurns: [interruptedTurn, runningTurn, secondTurn, firstTurn],
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

  it.effect("resolves the latest source turn to a native fork boundary without rollback", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const firstTurn = makeProviderTurn("provider-turn-first", 1, "native-turn-first", now);
      const secondTurn = makeProviderTurn("provider-turn-second", 2, "native-turn-second", now);

      const boundary = yield* CodexAdapterV2.resolveCodexForkBoundary({
        sourceProviderThread: makeProviderThread(now),
        sourceProviderTurns: [firstTurn, secondTurn],
        providerTurnId: secondTurn.id,
        targetThreadId: ThreadId.make("thread-codex-fork-target"),
      });

      assert.deepEqual(boundary, {
        lastTurnId: "native-turn-second",
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

  it.effect("keeps the rollback-count fallback when the boundary turn has no native ref", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const firstTurn: OrchestrationV2ProviderTurn = {
        ...makeProviderTurn("provider-turn-first", 1, "native-turn-first", now),
        nativeTurnRef: null,
      };
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

  it.effect("forks at head without a boundary when no provider turn is selected", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;

      const boundary = yield* CodexAdapterV2.resolveCodexForkBoundary({
        sourceProviderThread: makeProviderThread(now),
        targetThreadId: ThreadId.make("thread-codex-fork-target"),
      });

      assert.deepEqual(boundary, { lastTurnId: undefined, rollbackTurnCount: 0 });
    }),
  );

  it.effect("fails with a typed error when the selected source turn is missing", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const firstTurn = makeProviderTurn("provider-turn-first", 1, "native-turn-first", now);

      const error = yield* Effect.flip(
        CodexAdapterV2.resolveCodexForkBoundary({
          sourceProviderThread: makeProviderThread(now),
          sourceProviderTurns: [firstTurn],
          providerTurnId: ProviderTurnId.make("provider-turn-missing"),
          targetThreadId: ThreadId.make("thread-codex-fork-target"),
        }),
      );

      assert.instanceOf(error, ProviderAdapterForkThreadError);
      assert.include(String(error.cause), "provider-turn-missing");
    }),
  );
});

describe("CodexAdapterV2 skill mentions", () => {
  it("sends currency-sigil skill mentions as the $ mention Codex parses", () => {
    const cases: ReadonlyArray<readonly [string, string]> = [
      ["€review do it", "$review do it"],
      ["£ship", "$ship"],
      ["please ¥review this diff", "please $review this diff"],
      ["first line\n₹ship it", "first line\n$ship it"],
      ["𑿝review then €2spec", "$review then $2spec"],
      ["$review", "$review"],
      ["costs €20", "costs €20"],
      ["€5k", "€5k"],
      ["budget €100M or €1e6", "budget €100M or €1e6"],
      ["5€review", "5€review"],
    ];
    for (const [text, expected] of cases) {
      assert.equal(CodexAdapterV2.codexSkillMentionText(text), expected, text);
    }
  });
});

describe("CodexAdapterV2 background command detail", () => {
  it("summarizes command, exit code, and output tail", () => {
    assert.equal(
      CodexAdapterV2.codexBackgroundCommandDetail({
        command: "sleep 20 && echo CODEX_BG_WAKE_DONE",
        exitCode: 0,
        aggregatedOutput: "CODEX_BG_WAKE_DONE\n",
      }),
      "Background command completed (exit 0): sleep 20 && echo CODEX_BG_WAKE_DONE\n\n" +
        "Output tail:\nCODEX_BG_WAKE_DONE",
    );
  });

  it("omits the output section and exit code when absent", () => {
    assert.equal(
      CodexAdapterV2.codexBackgroundCommandDetail({
        command: "sleep 20",
        exitCode: null,
        aggregatedOutput: null,
      }),
      "Background command completed: sleep 20",
    );
  });

  it("truncates long commands and keeps only the output tail", () => {
    const detail = CodexAdapterV2.codexBackgroundCommandDetail({
      command: "x".repeat(300),
      exitCode: 1,
      aggregatedOutput: `${"y".repeat(2000)}TAIL`,
    });
    assert.include(detail, `(exit 1): ${"x".repeat(200)}...`);
    assert.include(detail, "Output tail:\n...");
    assert.include(detail, "TAIL");
    assert.notInclude(detail, "y".repeat(1001));
  });
});

const DEFAULT_CODEX_SETTINGS = Schema.decodeSync(CodexSettings)({});
const CODEX_TEST_MODEL_SELECTION = {
  instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
  model: "gpt-5.4",
} satisfies ModelSelection;
const CODEX_TEST_RUNTIME_POLICY = ProviderAdapterV2RuntimePolicy.make({
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
}): ProviderAdapterV2TurnInput {
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
  readonly codexHome?: string;
  readonly cwd?: string;
  /** Text the adapter should send, when it differs from what the user typed. */
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
          codexHome: input.codexHome ?? "/tmp/codex-home",
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
            projectId: null,
            ephemeral: false,
            modelProvider: "openai",
            createdAt: 1782622440,
            updatedAt: 1782622440,
            status: { type: "idle" },
            path: `/tmp/${input.nativeThreadId}.jsonl`,
            cwd: input.cwd ?? "/workspace",
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
          cwd: input.cwd ?? "/workspace",
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
          cwd: input.cwd ?? "/workspace",
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

function makeCodexReplayTranscript(input: {
  readonly scenario: string;
  readonly entries: ReadonlyArray<CodexReplay.CodexAppServerReplayEntry>;
}): CodexReplay.CodexAppServerReplayTranscript {
  return {
    provider: "codex",
    protocol: "codex.app-server",
    version: "0.144.0",
    scenario: input.scenario,
    entries: input.entries,
  };
}

describe("CodexAdapterV2 post-settle continuation", () => {
  const awaitUntil = (predicate: () => boolean, label: string): Effect.Effect<void> =>
    Effect.gen(function* () {
      for (let attempt = 0; attempt < 5000; attempt++) {
        if (predicate()) {
          return;
        }
        yield* Effect.yieldNow;
      }
      return yield* Effect.die(`Timed out waiting for ${label}.`);
    });

  const makeCodexReplayHarness = (
    transcript: CodexReplay.CodexAppServerReplayTranscript,
    onEvent: (event: ProviderAdapterV2Event) => Effect.Effect<unknown> = () => Effect.void,
    onRequest: (method: string, params: unknown) => Effect.Effect<void> = () => Effect.void,
    readChildMetadata?: (threadId: string) => Effect.Effect<unknown>,
    options: {
      readonly initialProviderThread?: OrchestrationV2ProviderThread;
      readonly existingProviderThread?: OrchestrationV2ProviderThread;
      readonly rawRequest?: CodexClient.CodexAppServerClient["Service"]["raw"]["request"];
      readonly goalResponses?: ReadonlyMap<
        string,
        import("effect-codex-app-server/schema").V2ThreadGoalGetResponse
      >;
      readonly beforeEmitInbound?: CodexReplay.CodexAppServerReplayDriver["beforeEmitInbound"];
      readonly runtimeGeneration?: string;
      readonly afterNotification?: (method: string) => Effect.Effect<void>;
      readonly continuationHomeLayout?: CodexAdapterV2.CodexAdapterV2Options["continuationHomeLayout"];
      readonly settings?: CodexAdapterV2.CodexAdapterV2Options["settings"];
      readonly getModelCatalog?: CodexAdapterV2.CodexAdapterV2Options["getModelCatalog"];
      readonly environment?: NodeJS.ProcessEnv;
      readonly resolveRuntime?: CodexAdapterV2.CodexAdapterV2Options["resolveRuntime"];
      readonly resolveRuntimeForSend?: CodexAdapterV2.CodexAdapterV2Options["resolveRuntimeForSend"];
      readonly readRuntimeRevisionForSend?: CodexAdapterV2.CodexAdapterV2Options["readRuntimeRevisionForSend"];
      readonly transcriptForOpen?: (ordinal: number) => CodexReplay.CodexAppServerReplayTranscript;
      readonly transformClient?: (
        client: CodexClient.CodexAppServerClient["Service"],
        ordinal: number,
      ) => CodexClient.CodexAppServerClient["Service"];
      readonly withRuntimeReplacement?: import("../ProviderAdapter.ts").ProviderAdapterV2OpenSessionInput["withRuntimeReplacement"];
      readonly withPendingStartStop?: import("../ProviderAdapter.ts").ProviderAdapterV2OpenSessionInput["withPendingStartStop"];
      readonly beforeRuntimeReplacement?: import("../ProviderAdapter.ts").ProviderAdapterV2OpenSessionInput["beforeRuntimeReplacement"];
      readonly nativeCreationExecution?: import("../ProviderAdapter.ts").ProviderAdapterV2OpenSessionInput["nativeCreationExecution"];
      readonly ownedClientFactory?: CodexAdapterV2.CodexAppServerClientFactoryShape;
      readonly onFactoryOpen?: (
        input: Parameters<CodexAdapterV2.CodexAppServerClientFactoryShape["open"]>[0],
      ) => Effect.Effect<void, ProviderAdapterOpenSessionError, Scope.Scope>;
      readonly onRuntimeOpened?: (
        runtime: import("../ProviderAdapter.ts").ProviderAdapterV2SessionRuntime,
      ) => Effect.Effect<void>;
    } = {},
  ) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const serverConfig = yield* makeReplayServerConfig(transcript.scenario).pipe(Effect.orDie);
      const continuationRequests: Array<ProviderContinuationRequest> = [];
      const replayDriver = yield* CodexReplay.makeReplayDriver(transcript, {
        ...(options.beforeEmitInbound === undefined
          ? {}
          : { beforeEmitInbound: options.beforeEmitInbound }),
      });
      let factoryOpenOrdinal = 0;
      const clientFactory: CodexAdapterV2.CodexAppServerClientFactoryShape = {
        open: (openInput) =>
          Effect.gen(function* () {
            const ordinal = factoryOpenOrdinal++;
            const activeTranscript = options.transcriptForOpen?.(ordinal) ?? transcript;
            const activeDriver =
              ordinal === 0 ? replayDriver : yield* CodexReplay.makeReplayDriver(activeTranscript);
            yield* options.onFactoryOpen?.(openInput) ?? Effect.void;
            const ownedClient =
              options.ownedClientFactory === undefined
                ? undefined
                : yield* options.ownedClientFactory.open(openInput);
            return yield* Layer.build(CodexReplay.layerReplayWithDriver(activeDriver)).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterOpenSessionError({
                    driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                    providerSessionId: openInput.providerSessionId,
                    cause,
                  }),
              ),
              Effect.flatMap((context) =>
                Effect.service(CodexClient.CodexAppServerClient).pipe(
                  Effect.map((client) =>
                    withCodexReplayChildMetadata(client, activeTranscript, readChildMetadata),
                  ),
                  Effect.map((client) => options.transformClient?.(client, ordinal) ?? client),
                  Effect.map(
                    (client) =>
                      ({
                        ...client,
                        ...(ownedClient?.ownedProcess === undefined
                          ? {}
                          : { ownedProcess: ownedClient.ownedProcess }),
                        ...(options.afterNotification === undefined
                          ? {}
                          : {
                              handleServerNotification: ((method, handler) =>
                                client.handleServerNotification(method, (payload) =>
                                  handler(payload).pipe(
                                    Effect.tap(() => options.afterNotification!(method)),
                                  ),
                                )) satisfies CodexClient.CodexAppServerClient["Service"]["handleServerNotification"],
                            }),
                        ...(options.rawRequest === undefined && options.goalResponses === undefined
                          ? {}
                          : {
                              raw: {
                                ...client.raw,
                                request: ((method, params) =>
                                  method === "thread/goal/get" &&
                                  Predicate.isObject(params) &&
                                  typeof params.threadId === "string" &&
                                  options.goalResponses?.has(params.threadId)
                                    ? Effect.succeed(options.goalResponses.get(params.threadId))
                                    : (options.rawRequest ?? client.raw.request)(
                                        method,
                                        params,
                                      )) satisfies CodexClient.CodexAppServerClient["Service"]["raw"]["request"],
                              },
                            }),
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
      const adapter = CodexAdapterV2.makeCodexAdapterV2({
        instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
        settings: options.settings ?? DEFAULT_CODEX_SETTINGS,
        environment: options.environment ?? {},
        ...(options.getModelCatalog === undefined
          ? {}
          : { getModelCatalog: options.getModelCatalog }),
        ...(options.continuationHomeLayout === undefined
          ? {}
          : {
              continuationHomeLayout: options.continuationHomeLayout,
            }),
        ...(options.resolveRuntime === undefined ? {} : { resolveRuntime: options.resolveRuntime }),
        ...(options.resolveRuntimeForSend === undefined
          ? {}
          : { resolveRuntimeForSend: options.resolveRuntimeForSend }),
        ...(options.readRuntimeRevisionForSend === undefined
          ? {}
          : { readRuntimeRevisionForSend: options.readRuntimeRevisionForSend }),
        clientFactory,
        fileSystem,
        idAllocator,
        serverConfig,
        continuationRequests: {
          offer: (request) =>
            Effect.sync(() => {
              continuationRequests.push(request);
            }),
        },
      });
      const threadId = ThreadId.make(`thread-${transcript.scenario}`);
      const runtimePolicy =
        options.nativeCreationExecution === undefined
          ? CODEX_TEST_RUNTIME_POLICY
          : {
              ...CODEX_TEST_RUNTIME_POLICY,
              cwd: options.nativeCreationExecution.resources.worktreePath,
            };
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make(`provider-session-${transcript.scenario}`),
        modelSelection: CODEX_TEST_MODEL_SELECTION,
        runtimePolicy,
        ...(options.beforeRuntimeReplacement === undefined
          ? {}
          : {
              beforeRuntimeReplacement: options.beforeRuntimeReplacement,
            }),
        ...(options.withRuntimeReplacement === undefined
          ? {}
          : { withRuntimeReplacement: options.withRuntimeReplacement }),
        ...(options.withPendingStartStop === undefined
          ? {}
          : { withPendingStartStop: options.withPendingStartStop }),
        ...(options.nativeCreationExecution === undefined
          ? {}
          : {
              nativeCreationExecution: options.nativeCreationExecution,
            }),
        ...(options.runtimeGeneration === undefined
          ? {}
          : {
              nativeOperation: {
                operationId: `open-${transcript.scenario}`,
                operation: "open_session" as const,
                runtimeGeneration: options.runtimeGeneration,
              },
            }),
      });
      if (options.onRuntimeOpened !== undefined) yield* options.onRuntimeOpened(runtime);
      const providerThread =
        options.initialProviderThread ??
        (yield* runtime.ensureThread({
          threadId,
          modelSelection: CODEX_TEST_MODEL_SELECTION,
          runtimePolicy,
          ...(options.existingProviderThread === undefined
            ? {}
            : { existingProviderThread: options.existingProviderThread }),
          ...(options.nativeCreationExecution === undefined
            ? {}
            : {
                nativeCreationExecution: options.nativeCreationExecution,
              }),
        }));
      const events: Array<ProviderAdapterV2Event> = [];
      const firstTerminal = yield* Deferred.make<void>();
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
            Effect.andThen(onEvent(event)),
          ),
        ),
        Effect.forkScoped,
      );
      if (runtime.hasPendingBackgroundWork === undefined) {
        return yield* Effect.die("Codex adapter runtime must expose hasPendingBackgroundWork.");
      }
      const hasPendingBackgroundWork = runtime.hasPendingBackgroundWork;
      const terminalEvents = () =>
        events.filter(
          (event): event is Extract<ProviderAdapterV2Event, { type: "turn.terminal" }> =>
            event.type === "turn.terminal",
        );
      const subagentUpdates = () =>
        events.filter(
          (event): event is Extract<ProviderAdapterV2Event, { type: "subagent.updated" }> =>
            event.type === "subagent.updated",
        );
      return {
        runtime,
        providerThread,
        threadId,
        events,
        continuationRequests,
        terminalEvents,
        subagentUpdates,
        hasPendingBackgroundWork,
        firstTerminal: Deferred.await(firstTerminal),
      };
    });

  const runtimeBinding = (harness: {
    readonly threadId: ThreadId;
    readonly providerThread: OrchestrationV2ProviderThread;
    readonly runtime: import("../ProviderAdapter.ts").ProviderAdapterV2SessionRuntime;
  }) => {
    const nativeThreadId = harness.providerThread.nativeThreadRef?.nativeId;
    if (nativeThreadId == null) throw new Error("Replay fixture has no native thread ID.");
    return {
      threadId: harness.threadId,
      providerThreadId: harness.providerThread.id,
      providerSessionId: harness.runtime.providerSessionId,
      instanceId: harness.runtime.instanceId,
      runtimeGeneration: harness.runtime.runtimeGeneration!,
      nativeThreadId,
    };
  };

  const prepareManagedCodexTurn = (
    runtime: import("../ProviderAdapter.ts").ProviderAdapterV2SessionRuntime,
    turn: ProviderAdapterV2TurnInput,
  ) =>
    Effect.gen(function* () {
      const at = "2026-10-03T00:00:00.000Z";
      const commandId = `managed-command-${turn.attemptId}`;
      const command = { type: "message.send", commandId, threadId: turn.threadId };
      const birth = {
        kind: "application_v2_thread_birth",
        threadId: turn.threadId,
        eventId: `managed-birth-${turn.attemptId}`,
        sequence: 1,
      };
      const capture = yield* Schema.decodeUnknownEffect(OrdinaryCheckoutCaptureV1)({
        version: 1,
        commandId,
        commandType: command.type,
        canonicalCommand: command,
        commandDigest: ordinaryCheckoutCommandDigestV1(command),
        origin: { kind: "command" },
        threadId: turn.threadId,
        applicationBirth: birth,
        projectId: turn.appThread.projectId,
        canonicalProjectRoot: "/workspace",
        canonicalCheckoutPath: "/workspace",
        branch: "fixture-branch",
        lease: {
          resourcePath: "/workspace",
          leaseId: `managed-lease-${turn.attemptId}`,
          ownerThreadId: turn.threadId,
          ownerIncarnation: ordinaryApplicationIncarnationV1(
            yield* Schema.decodeUnknownEffect(OrdinaryCheckoutCaptureV1.fields.applicationBirth)(
              birth,
            ).pipe(Effect.orDie),
          ),
          branch: "fixture-branch",
          acquiredAtMs: 1,
          renewedAtMs: 1,
          expiresAtMs: 300001,
        },
      }).pipe(Effect.orDie);
      const admission = yield* Schema.decodeUnknownEffect(OrdinaryCheckoutAdmissionV1)({
        version: 1,
        admissionId: ordinaryCheckoutAdmissionIdV1(capture),
        capture: yield* Schema.encodeEffect(OrdinaryCheckoutCaptureV1)(capture).pipe(Effect.orDie),
        receipt: {
          commandId,
          threadId: turn.threadId,
          commandType: command.type,
          acceptedAt: at,
          resultSequence: 1,
          status: "accepted",
          error: null,
        },
        eventBasis: [
          {
            eventId: birth.eventId,
            sequence: 1,
            threadId: turn.threadId,
            commandId,
            eventType: "message.accepted",
          },
        ],
        run: {
          runId: turn.runId,
          runAttemptId: turn.attemptId,
          nodeId: turn.rootNodeId,
          messageId: turn.message.messageId,
        },
        recordedAt: at,
      }).pipe(Effect.orDie);
      const source = {
        kind: "outbox",
        link: {
          version: 1,
          effectId: `managed-effect-${turn.attemptId}`,
          commandId,
          threadId: turn.threadId,
          requestSha256: "c".repeat(64),
          admission: ordinaryCheckoutAdmissionRefV1(admission),
          recordedAt: at,
        },
        workerId: "managed-fixture-worker",
        expectedAttempt: 1,
        leaseExpiresAt: "2026-10-03T00:05:00.000Z",
      };
      const originalUse = yield* Schema.decodeUnknownEffect(OrdinaryCheckoutUseV1)({
        version: 1,
        kind: "ordinary_checkout_use",
        operationId: `${source.link.effectId}:ordinary-checkout:attempt:1`,
        admission: source.link.admission,
        source,
        lease: capture.lease,
      }).pipe(Effect.orDie);
      const startExecution = makeOrdinaryCheckoutExecutionRefV1({
        originalUse,
        executor: yield* Schema.decodeUnknownEffect(OrdinaryCheckoutExecutionExecutorV1)({
          kind: "actual_outbox_claim",
          source,
        }).pipe(Effect.orDie),
      });
      const offer: ProviderManagedActorAdmissionV1 = {
        startExecution,
        admission,
        providerThreadId: turn.providerThread.id,
        checkpointScopeId: CheckpointScopeId.make(`managed-scope-${turn.attemptId}`),
      };
      const reader = yield* prepareProviderManagedActorRun(runtime, offer);
      yield* Effect.addFinalizer(() => reader.release);
      const managedExecution = makeOrdinaryCheckoutExecutionRefV1({
        originalUse,
        executor: yield* Schema.decodeUnknownEffect(OrdinaryCheckoutExecutionExecutorV1)({
          kind: "captured_managed_run",
          captureId: `managed-capture-${turn.attemptId}`,
          run: admission.run,
          checkpointScopeId: offer.checkpointScopeId,
          driver: runtime.driver,
          binding: {
            threadId: turn.threadId,
            providerThreadId: turn.providerThread.id,
            providerSessionId: runtime.providerSessionId,
            instanceId: runtime.instanceId,
          },
          ...(runtime.runtimeGeneration === undefined
            ? {}
            : { runtimeGeneration: runtime.runtimeGeneration }),
          nativeThreadId: turn.providerThread.nativeThreadRef!.nativeId,
        }).pipe(Effect.orDie),
      });
      return { reader, startExecution, managedExecution };
    });

  it.effect(
    "binds fresh native state to the prepared Jones row and rejects foreign prepared targets",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const scenario = "codex-prepared-row-binding";
          const now = yield* DateTime.now;
          const prepared: OrchestrationV2ProviderThread = {
            id: ProviderThreadId.make("prepared-jones-provider-thread"),
            driver: CodexAdapterV2.CODEX_DRIVER_KIND,
            providerInstanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
            providerSessionId: null,
            appThreadId: ThreadId.make(`thread-${scenario}`),
            ownerNodeId: null,
            nativeThreadRef: null,
            nativeConversationHeadRef: {
              driver: CodexAdapterV2.CODEX_DRIVER_KIND,
              nativeId: "prior-native-head",
              strength: "strong",
            },
            status: "not_loaded",
            firstRunOrdinal: 7,
            lastRunOrdinal: 9,
            handoffIds: [],
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
          };
          const requests: Array<string> = [];
          const harness = yield* makeCodexReplayHarness(
            finalAnswerTranscript(scenario, [{ id: "prepared-row-answer", text: "DONE" }]),
            undefined,
            (method) =>
              Effect.sync(() => {
                requests.push(method);
              }),
            undefined,
            {
              existingProviderThread: prepared,
              onRuntimeOpened: (runtime) =>
                Effect.gen(function* () {
                  const foreignTargets: ReadonlyArray<OrchestrationV2ProviderThread> = [
                    { ...prepared, driver: ProviderDriverKind.make("pi") },
                    {
                      ...prepared,
                      providerInstanceId: ProviderInstanceId.make("foreign-instance"),
                    },
                    { ...prepared, providerSessionId: ProviderSessionId.make("foreign-session") },
                    { ...prepared, appThreadId: ThreadId.make("foreign-app-thread") },
                  ];
                  for (const [index, existingProviderThread] of foreignTargets.entries()) {
                    const result = yield* runtime
                      .ensureThread({
                        threadId: prepared.appThreadId!,
                        modelSelection: CODEX_TEST_MODEL_SELECTION,
                        runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
                        existingProviderThread,
                        nativeOperation: {
                          operationId: `foreign-prepared-target-${index}`,
                          operation: "ensure_thread",
                          instanceId: runtime.instanceId,
                          threadId: prepared.appThreadId!,
                          providerSessionId: runtime.providerSessionId,
                          providerThreadId: prepared.id,
                          runtimeGeneration: runtime.runtimeGeneration!,
                        },
                      })
                      .pipe(Effect.result);
                    if (
                      result._tag !== "Failure" ||
                      !Schema.is(ProviderAdapterEnsureThreadError)(result.failure)
                    )
                      return assert.fail(
                        "Expected a foreign prepared target to fail before native initialization",
                      );
                    assert.equal(result.failure.nativeEffect?.outcome, "known_no_effect");
                    assert.isTrue(Schema.is(ProviderAdapterProtocolError)(result.failure.cause));
                    assert.deepEqual(requests, []);
                  }
                }),
            },
          );
          assert.equal(harness.providerThread.id, prepared.id);
          assert.equal(harness.providerThread.createdAt, prepared.createdAt);
          assert.equal(harness.providerThread.firstRunOrdinal, 7);
          assert.equal(harness.providerThread.lastRunOrdinal, 9);
          assert.equal(
            harness.providerThread.nativeThreadRef?.nativeId,
            `native-${scenario}-thread`,
          );
          assert.isNull(harness.providerThread.nativeConversationHeadRef);
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now,
              attemptId: RunAttemptId.make("attempt-prepared-row"),
              text: "Reply with the requested recovery marker.",
            }),
          );
          yield* harness.firstTerminal;
          assert.equal(harness.terminalEvents()[0]?.status, "completed");
          const turnUpdates = harness.events.filter(
            (event) => event.type === "provider_turn.updated",
          );
          assert.isNotEmpty(turnUpdates);
          assert.isTrue(
            turnUpdates.every((event) => event.providerTurn.providerThreadId === prepared.id),
          );
          assert.deepEqual(requests, ["initialize", "thread/start", "turn/start"]);
        }),
      ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("pauses the active goal before user Stop and preserves the pause across restart", () =>
    Effect.gen(function* () {
      const nativeThreadId = "native-goal-stop-thread";
      const calls: Array<string> = [];
      let persistedGoal = {
        threadId: nativeThreadId,
        status: "active",
        objective: "Keep working until the requested result is complete",
        createdAt: 1782622440,
        updatedAt: 1782622440,
        timeUsedSeconds: 0,
        tokensUsed: 0,
      };
      for (const restarted of [false, true]) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const nativeTurnId = restarted
              ? "native-goal-stop-restarted"
              : "native-goal-stop-first";
            const transcript = makeCodexReplayTranscript({
              scenario: `goal-stop-restart-${restarted}`,
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
            const harness = yield* makeCodexReplayHarness(
              transcript,
              undefined,
              (method) =>
                Effect.sync(() => {
                  if (method === "turn/interrupt") calls.push(`${method}:${nativeTurnId}`);
                }),
              undefined,
              {
                runtimeGeneration: `goal-stop-generation-${restarted}`,
                rawRequest: (method, params) =>
                  Effect.sync(() => {
                    if (method === "thread/goal/get") {
                      assert.deepEqual(params, { threadId: nativeThreadId });
                      calls.push(method);
                      return { goal: { ...persistedGoal } };
                    }
                    if (method === "thread/goal/set") {
                      assert.deepEqual(params, { threadId: nativeThreadId, status: "paused" });
                      calls.push(method);
                      persistedGoal = { ...persistedGoal, status: "paused", updatedAt: 1782622450 };
                      return { goal: { ...persistedGoal } };
                    }
                    throw new Error(`Unexpected native goal fixture request: ${method}`);
                  }),
              },
            );
            yield* harness.runtime.startTurn(
              makeCodexTestTurnInput({
                threadId: harness.threadId,
                providerThread: harness.providerThread,
                now: yield* DateTime.now,
                attemptId: RunAttemptId.make(`attempt-goal-stop-${restarted}`),
                text: "Keep working",
              }),
            );
            yield* awaitUntil(
              () => harness.events.some((event) => event.type === "provider_turn.updated"),
              "goal Stop provider turn",
            );
            const providerTurnId = harness.events.find(
              (
                event,
              ): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
                event.type === "provider_turn.updated",
            )!.providerTurn.id;
            yield* harness.runtime.interruptTurn({
              providerThread: harness.providerThread,
              providerTurnId,
              ...(restarted ? {} : { requestRuntimeRestart: true }),
            });
            if (restarted) {
              yield* harness.runtime.interruptTurn({
                providerThread: harness.providerThread,
                providerTurnId,
                requestRuntimeRestart: true,
              });
            }
            assert.equal(persistedGoal.status, "paused");
          }),
        );
      }
      assert.deepEqual(calls, [
        "thread/goal/get",
        "thread/goal/set",
        "turn/interrupt:native-goal-stop-first",
        "turn/interrupt:native-goal-stop-restarted",
        "thread/goal/get",
      ]);
    }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  for (const control of [
    "absent",
    "paused",
    "get-error",
    "get-hang",
    "set-error",
    "set-hang",
    "malformed-ack",
    "wrong-thread-ack",
  ] as const) {
    it.effect(`delivers user Stop when goal control is ${control}`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const nativeThreadId = `native-goal-${control}`;
          const nativeTurnId = `native-turn-${control}`;
          const blocked = yield* Deferred.make<void>();
          const calls: Array<string> = [];
          const goal = {
            threadId: nativeThreadId,
            status: control === "paused" ? "paused" : "active",
            objective: "Continue working",
            createdAt: 1782622440,
            updatedAt: 1782622440,
            timeUsedSeconds: 0,
            tokensUsed: 0,
          };
          const transcript = makeCodexReplayTranscript({
            scenario: `goal-control-${control}`,
            entries: [
              ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Work" }),
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
          const harness = yield* makeCodexReplayHarness(
            transcript,
            undefined,
            (method) =>
              Effect.sync(() => {
                if (method === "turn/interrupt") calls.push(method);
              }),
            undefined,
            {
              rawRequest: (method, params) =>
                Effect.gen(function* () {
                  calls.push(method);
                  assert.equal((params as { threadId: string }).threadId, nativeThreadId);
                  const reading = method === "thread/goal/get";
                  assert.equal(method, reading ? "thread/goal/get" : "thread/goal/set");
                  if (control === (reading ? "get-error" : "set-error")) {
                    return yield* Effect.fail(
                      new CodexErrors.CodexAppServerRequestError({
                        code: -32000,
                        errorMessage: "Synthetic goal control error",
                      }),
                    );
                  }
                  if (control === (reading ? "get-hang" : "set-hang")) {
                    yield* Deferred.succeed(blocked, undefined);
                    return yield* Effect.never;
                  }
                  if (reading) return { goal: control === "absent" ? null : goal };
                  if (control === "malformed-ack") return {};
                  return {
                    goal: {
                      ...goal,
                      status: "paused",
                      threadId:
                        control === "wrong-thread-ack" ? "unrelated-native-thread" : nativeThreadId,
                    },
                  };
                }),
            },
          );
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make(`attempt-goal-${control}`),
              text: "Work",
            }),
          );
          yield* awaitUntil(
            () => harness.events.some((event) => event.type === "provider_turn.updated"),
            "goal control provider turn",
          );
          const providerTurnId = harness.events.find(
            (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
              event.type === "provider_turn.updated",
          )!.providerTurn.id;
          const stop = yield* harness.runtime
            .interruptTurn({
              providerThread: harness.providerThread,
              providerTurnId,
              requestRuntimeRestart: true,
            })
            .pipe(Effect.forkChild);
          if (control === "get-hang" || control === "set-hang") {
            yield* Deferred.await(blocked);
            yield* TestClock.adjust("1 second");
          }
          yield* Fiber.join(stop);
          assert.deepEqual(
            calls,
            control === "absent" ||
              control === "paused" ||
              control === "get-error" ||
              control === "get-hang"
              ? ["thread/goal/get", "turn/interrupt"]
              : ["thread/goal/get", "thread/goal/set", "turn/interrupt"],
          );
        }),
      ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    );
  }

  it.effect("rejects a stale user Stop generation before native goal control", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const calls: Array<string> = [];
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "goal-stop-stale-generation",
            entries: codexReplayPreamble({
              nativeThreadId: "native-goal-stale",
              nativeTurnId: "unused",
              prompt: "unused",
            }).slice(0, 5),
          }),
          undefined,
          undefined,
          undefined,
          {
            rawRequest: (method) =>
              Effect.sync(() => {
                calls.push(method);
                return {};
              }),
          },
        );
        const result = yield* harness.runtime
          .interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId: ProviderTurnId.make("settled-goal-turn"),
            requestRuntimeRestart: true,
            nativeOperation: {
              operationId: "stale-user-stop",
              operation: "interrupt_turn",
              runtimeGeneration: "previous-incarnation",
            },
          })
          .pipe(Effect.result);
        assert.equal(result._tag, "Failure");
        assert.deepEqual(calls, []);
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect(
    "resolves live catalog effort on each fresh send without changing the requested selection",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const nativeThreadId = "native-live-default";
          const entries = codexReplayPreamble({
            nativeThreadId,
            nativeTurnId: "native-default-first",
            prompt: "Work",
          }).map((entry) =>
            entry.type === "expect_outbound" &&
            Predicate.isObject(entry.frame) &&
            entry.frame.method === "turn/start"
              ? {
                  ...entry,
                  frame: {
                    ...entry.frame,
                    params: { ...(entry.frame.params as Record<string, unknown>), effort: "low" },
                  },
                }
              : entry,
          );
          entries.push({
            type: "emit_inbound",
            frame: {
              method: "turn/completed",
              params: {
                threadId: nativeThreadId,
                turn: makeCodexReplayTurn({ id: "native-default-first", status: "completed" }),
              },
            },
          });
          entries.push(
            ...codexReplayPreamble({
              nativeThreadId,
              nativeTurnId: "native-default-second",
              prompt: "Work",
            })
              .slice(5)
              .map((entry) => {
                if (
                  (entry.type !== "emit_inbound" && entry.type !== "expect_outbound") ||
                  !Predicate.isObject(entry.frame) ||
                  !("id" in entry.frame)
                )
                  return entry;
                return {
                  ...entry,
                  frame: {
                    ...entry.frame,
                    id: 4,
                    ...(entry.frame.method === "turn/start"
                      ? {
                          params: {
                            ...(entry.frame.params as Record<string, unknown>),
                            effort: "high",
                          },
                        }
                      : {}),
                  },
                };
              }),
          );
          let lookups = 0;
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({ scenario: "live-default-effort", entries }),
            undefined,
            undefined,
            undefined,
            {
              getModelCatalog: () =>
                Effect.sync(() => [
                  {
                    slug: "gpt-5.4",
                    name: "Native model",
                    isCustom: false,
                    capabilities: {
                      optionDescriptors: [
                        {
                          id: "reasoningEffort",
                          label: "Effort",
                          type: "select",
                          options: [],
                          currentValue: ++lookups === 1 ? "low" : "high",
                        },
                      ],
                    },
                  },
                ]),
            },
          );
          const selection = Object.freeze({ ...CODEX_TEST_MODEL_SELECTION });
          for (const attempt of ["first", "second"]) {
            yield* harness.runtime.startTurn({
              ...makeCodexTestTurnInput({
                threadId: harness.threadId,
                providerThread: harness.providerThread,
                now: yield* DateTime.now,
                attemptId: RunAttemptId.make(`default-${attempt}`),
                text: "Work",
              }),
              modelSelection: selection,
            });
            if (attempt === "first") yield* harness.firstTerminal;
          }
          assert.equal(lookups, 2);
          assert.deepEqual(selection, CODEX_TEST_MODEL_SELECTION);
          assert.notProperty(selection, "options");
        }),
      ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("keeps explicit effort independent of the live catalog", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const entries = codexReplayPreamble({
          nativeThreadId: "native-explicit-effort",
          nativeTurnId: "native-explicit-turn",
          prompt: "Work",
        }).map((entry) =>
          entry.type === "expect_outbound" &&
          Predicate.isObject(entry.frame) &&
          entry.frame.method === "turn/start"
            ? {
                ...entry,
                frame: {
                  ...entry.frame,
                  params: { ...(entry.frame.params as Record<string, unknown>), effort: "xhigh" },
                },
              }
            : entry,
        );
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({ scenario: "explicit-effort", entries }),
          undefined,
          undefined,
          undefined,
          {
            getModelCatalog: () => Effect.die("Explicit effort must not query the catalog"),
          },
        );
        yield* harness.runtime.startTurn({
          ...makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("explicit-effort"),
            text: "Work",
          }),
          modelSelection: {
            ...CODEX_TEST_MODEL_SELECTION,
            options: [{ id: "reasoningEffort", value: "xhigh" }],
          },
        });
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("uses the current configured effort only for an omitted user choice", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "native-current-configured-effort";
        const entries = codexReplayPreamble({
          nativeThreadId,
          nativeTurnId: "configured-effort-turn",
          prompt: "Work",
        }).map((entry) =>
          entry.type === "expect_outbound" &&
          Predicate.isObject(entry.frame) &&
          entry.frame.method === "turn/start"
            ? {
                ...entry,
                frame: {
                  ...entry.frame,
                  params: { ...(entry.frame.params as Record<string, unknown>), effort: "high" },
                },
              }
            : entry,
        );
        let reads = 0;
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({ scenario: "configured-effort-dispatch", entries }),
          undefined,
          undefined,
          undefined,
          {
            getModelCatalog: () =>
              Effect.sync(() => {
                reads++;
                return [
                  {
                    slug: "gpt-5.4",
                    name: "Native model",
                    isCustom: false,
                    capabilities: {
                      optionDescriptors: [
                        {
                          id: "reasoningEffort",
                          label: "Effort",
                          type: "select",
                          currentValue: "low",
                          options: [
                            { id: "low", label: "Low" },
                            { id: "high", label: "High" },
                          ],
                        },
                      ],
                    },
                  },
                ];
              }),
          },
        );
        const selection = Object.freeze({ ...CODEX_TEST_MODEL_SELECTION });
        yield* harness.runtime.startTurn({
          ...makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("configured-effort-dispatch"),
            text: "Work",
          }),
          modelSelection: selection,
          configuredDefaultModelSelection: {
            driver: CodexAdapterV2.CODEX_DRIVER_KIND,
            modelSelection: { ...selection, options: [{ id: "reasoningEffort", value: "high" }] },
          },
        });
        assert.equal(reads, 1);
        assert.isFalse(Object.hasOwn(selection, "options"));
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  for (const outcome of [
    "restored",
    "different-home",
    "failed-open",
    "missing-reservation",
    "active",
    "strict-bundle",
  ] as const) {
    it.effect(`fences managed credential replacement for ${outcome}`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const nativeThreadId = "native-managed-refresh";
          const initialGeneration = "managed-original-incarnation";
          const nativeFixture =
            outcome === "strict-bundle"
              ? yield* makeCodexNativeCreationFixture("managed-refresh-strict")
              : undefined;
          const cwd = nativeFixture?.execution.resources.worktreePath ?? "/workspace";
          const initialEntries = codexReplayPreamble({
            nativeThreadId,
            nativeTurnId: "managed-original-turn",
            prompt: "Work",
            cwd,
          });
          if (outcome !== "active")
            initialEntries.push({
              type: "emit_inbound",
              frame: {
                method: "turn/completed",
                params: {
                  threadId: nativeThreadId,
                  turn: makeCodexReplayTurn({ id: "managed-original-turn", status: "completed" }),
                },
              },
            });
          const nextEntries = codexReplayPreamble({
            nativeThreadId,
            nativeTurnId: "managed-new-turn",
            prompt: "Work",
            cwd,
            ...(outcome === "different-home"
              ? { codexHome: "/synthetic/different-managed-home" }
              : {}),
          }).slice(0, 3);
          nextEntries.push(
            {
              type: "expect_outbound",
              frame: {
                id: 2,
                method: "thread/resume",
                params: {
                  threadId: nativeThreadId,
                  excludeTurns: true,
                  cwd,
                  model: "gpt-5.4",
                  config: CodexAdapterV2.CODEX_THREAD_CONFIG,
                },
              },
            },
            {
              type: "emit_inbound",
              frame: {
                id: 2,
                result: {
                  thread: { id: nativeThreadId, updatedAt: 1782622450, cwd },
                  cwd,
                  model: "gpt-5.4",
                  modelProvider: "openai",
                  serviceTier: null,
                },
              },
            },
            ...codexReplayPreamble({
              nativeThreadId,
              nativeTurnId: "managed-new-turn",
              prompt: "Work",
              cwd,
            }).slice(5),
          );
          let revision = "synthetic-original-revision";
          let resolves = 0;
          let opens = 0;
          let resumes = 0;
          const phases: Array<string> = [];
          let runtime: import("../ProviderAdapter.ts").ProviderAdapterV2SessionRuntime | undefined;
          let oldSettings:
            | ((
                payload: import("effect-codex-app-server/schema").V2ThreadSettingsUpdatedNotification,
              ) => Effect.Effect<void, CodexErrors.CodexAppServerError>)
            | undefined;
          let newSettings: typeof oldSettings;
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({
              scenario: `managed-refresh-${outcome}`,
              entries: initialEntries,
            }),
            undefined,
            undefined,
            undefined,
            {
              runtimeGeneration: initialGeneration,
              ...(nativeFixture === undefined
                ? {}
                : { nativeCreationExecution: nativeFixture.execution }),
              resolveRuntime: Effect.sync(() => ({
                config: DEFAULT_CODEX_SETTINGS,
                environment: {},
                revision,
              })),
              readRuntimeRevisionForSend: Effect.sync(() => revision),
              resolveRuntimeForSend: Effect.sync(() => {
                resolves++;
                assert.isUndefined(runtime!.continuationSourceIdentity);
                assert.notEqual(runtime!.runtimeGeneration, initialGeneration);
                phases.push("resolve");
                return { config: DEFAULT_CODEX_SETTINGS, environment: {}, revision };
              }),
              transcriptForOpen: (ordinal) =>
                ordinal === 0
                  ? makeCodexReplayTranscript({
                      scenario: `managed-refresh-${outcome}`,
                      entries: initialEntries,
                    })
                  : makeCodexReplayTranscript({
                      scenario: `managed-replacement-${outcome}`,
                      entries: nextEntries,
                    }),
              onFactoryOpen: (openInput) =>
                Effect.gen(function* () {
                  opens++;
                  phases.push(opens === 1 ? "open-old" : "open-new");
                  yield* Effect.addFinalizer(() =>
                    Effect.sync(() => {
                      phases.push(
                        openInput.runtimeGeneration === initialGeneration
                          ? "close-old"
                          : "close-new",
                      );
                    }),
                  );
                  if (opens > 1 && outcome === "failed-open")
                    return yield* new ProviderAdapterOpenSessionError({
                      driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                      providerSessionId: openInput.providerSessionId,
                      cause: "Synthetic replacement creation failure",
                    });
                }),
              beforeRuntimeReplacement: (generation) =>
                Effect.sync(() => {
                  phases.push(generation === initialGeneration ? "fence-old" : "fence-new");
                  if (generation !== initialGeneration) {
                    assert.equal(runtime!.runtimeGeneration, generation);
                    assert.isUndefined(runtime!.continuationSourceIdentity);
                  }
                }),
              ...(outcome === "missing-reservation"
                ? {}
                : {
                    withRuntimeReplacement: (generation, replace) =>
                      Effect.gen(function* () {
                        assert.equal(runtime!.runtimeGeneration, initialGeneration);
                        phases.push("reserve");
                        yield* replace;
                        assert.equal(
                          runtime!.continuationSourceIdentity?.runtimeGeneration,
                          generation,
                        );
                        phases.push("registered");
                      }).pipe(
                        Effect.ensuring(
                          Effect.sync(() => {
                            phases.push("release");
                          }),
                        ),
                      ),
                  }),
              onRuntimeOpened: (value) =>
                Effect.sync(() => {
                  runtime = value;
                }),
              transformClient: (client, ordinal) => ({
                ...client,
                raw: {
                  ...client.raw,
                  request: ((method, params) =>
                    Effect.sync(() => {
                      if (method === "thread/resume") resumes++;
                    }).pipe(
                      Effect.andThen(client.raw.request(method, params)),
                    )) satisfies CodexClient.CodexAppServerClient["Service"]["raw"]["request"],
                },
                handleServerNotification: ((method, handler) => {
                  if (ordinal === 0 && method === "thread/settings/updated")
                    oldSettings = handler as typeof oldSettings;
                  if (ordinal === 1 && method === "thread/settings/updated")
                    newSettings = handler as typeof newSettings;
                  return client.handleServerNotification(method, handler);
                }) satisfies CodexClient.CodexAppServerClient["Service"]["handleServerNotification"],
              }),
            },
          );
          const turn = (attemptId: string): ProviderAdapterV2TurnInput => ({
            ...makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: harness.providerThread.updatedAt,
              attemptId: RunAttemptId.make(attemptId),
              text: "Work",
            }),
            runtimePolicy: { ...CODEX_TEST_RUNTIME_POLICY, cwd },
            ...(nativeFixture === undefined
              ? {}
              : { nativeCreationExecution: nativeFixture.execution }),
          });
          yield* harness.runtime.startTurn(turn("managed-first"));
          if (outcome !== "active") yield* harness.firstTerminal;
          revision = "synthetic-current-revision";
          const result = yield* harness.runtime
            .startTurn(turn("managed-second"))
            .pipe(Effect.result);
          if (outcome === "restored") {
            assert.equal(result._tag, "Success");
            assert.equal(opens, 2);
            assert.equal(resolves, 1);
            assert.equal(resumes, 1);
            assert.deepEqual(phases, [
              "fence-old",
              "open-old",
              "reserve",
              "fence-new",
              "close-old",
              "resolve",
              "open-new",
              "registered",
              "release",
            ]);
            assert.notEqual(runtime!.runtimeGeneration, initialGeneration);
            assert.equal(
              runtime!.continuationSourceIdentity?.continuationKey,
              "codex:home:/tmp/codex-home",
            );
            const newGeneration = runtime!.runtimeGeneration;
            yield* awaitUntil(
              () =>
                harness.events.some(
                  (event) =>
                    event.type === "runtime_identity.observed" &&
                    event.binding.runtimeGeneration === newGeneration,
                ),
              "the actual restored incarnation binding",
            );
            const restored = harness.events
              .filter(
                (
                  event,
                ): event is Extract<ProviderAdapterV2Event, { type: "provider_thread.updated" }> =>
                  event.type === "provider_thread.updated" &&
                  event.providerThread.id === harness.providerThread.id,
              )
              .at(-1)!.providerThread;
            assert.deepEqual(restored.nativeThreadRef, harness.providerThread.nativeThreadRef);
            assert.deepEqual(
              restored.nativeConversationHeadRef,
              harness.providerThread.nativeConversationHeadRef,
            );
            assert.deepEqual(restored.nativeMetadata, harness.providerThread.nativeMetadata);
            const observations = harness.events.filter(
              (event) => event.type === "runtime_identity.observed",
            ).length;
            yield* oldSettings!({
              threadId: nativeThreadId,
              threadSettings: {
                model: "stale-native-model",
                modelProvider: "openai",
                effort: "high",
                cwd,
                approvalPolicy: "never",
                approvalsReviewer: "user",
                collaborationMode: { mode: "default", settings: { model: "stale-native-model" } },
                sandboxPolicy: { type: "dangerFullAccess" },
              },
            });
            yield* newSettings!({
              threadId: nativeThreadId,
              threadSettings: {
                model: "gpt-5.4",
                modelProvider: "openai",
                effort: "high",
                cwd,
                approvalPolicy: "never",
                approvalsReviewer: "user",
                collaborationMode: { mode: "default", settings: { model: "gpt-5.4" } },
                sandboxPolicy: { type: "dangerFullAccess" },
              },
            });
            yield* awaitUntil(
              () =>
                harness.events.some(
                  (event) =>
                    event.type === "runtime_identity.observed" &&
                    event.attestation.observed.model.status === "observed" &&
                    event.attestation.observed.model.sourceEvent === "thread/settings/updated",
                ),
              "the current native observation barrier",
            );
            assert.equal(
              harness.events.filter((event) => event.type === "runtime_identity.observed").length,
              observations + 1,
            );
            assert.isFalse(
              harness.events.some(
                (event) =>
                  event.type === "runtime_identity.observed" &&
                  event.attestation.observed.model.status === "observed" &&
                  event.attestation.observed.model.value === "stale-native-model",
              ),
            );
          } else {
            assert.equal(result._tag, "Failure");
            if (result._tag !== "Failure") return;
            assert.equal(
              "nativeEffect" in result.failure ? result.failure.nativeEffect?.outcome : undefined,
              "unknown",
            );
            assert.equal(resumes, 0);
            if (outcome === "different-home" || outcome === "failed-open") {
              assert.equal(opens, 2);
              assert.equal(resolves, 1);
              assert.notEqual(runtime!.runtimeGeneration, initialGeneration);
              assert.isUndefined(runtime!.continuationSourceIdentity);
              const held = yield* harness.runtime
                .startTurn(turn("managed-after-unknown"))
                .pipe(Effect.result);
              assert.equal(held._tag, "Failure");
              assert.equal(opens, 2);
            } else {
              assert.equal(opens, 1);
              assert.equal(resolves, 0);
              assert.equal(runtime!.runtimeGeneration, initialGeneration);
            }
          }
        }),
      ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    );
  }

  for (const boundary of ["queued output", "suspended callback"] as const) {
    it.effect(`retains the captured Codex source across replacement for ${boundary}`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const nativeThreadId = `native-event-origin-${boundary.replaceAll(" ", "-")}`;
          const scenario = `event-origin-${boundary.replaceAll(" ", "-")}`;
          const initialGeneration = "event-origin-original-generation";
          const releaseOutput = yield* Deferred.make<void>();
          const queuedOutput = yield* Deferred.make<ProviderAdapterV2Event>();
          const callbackEntered = yield* Deferred.make<void>();
          const releaseCallback = yield* Deferred.make<void>();
          let revision = "synthetic-event-origin-old-revision";
          let oldSettings:
            | ((
                payload: import("effect-codex-app-server/schema").V2ThreadSettingsUpdatedNotification,
              ) => Effect.Effect<void, CodexErrors.CodexAppServerError>)
            | undefined;
          const outputEntries = (
            nativeTurnId: string,
            text: string,
          ): Array<CodexReplay.CodexAppServerReplayEntry> => [
            {
              type: "emit_inbound",
              frame: {
                method: "item/agentMessage/delta",
                params: {
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  itemId: `${nativeTurnId}-message`,
                  delta: text,
                },
              },
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
          ];
          const initialEntries = [
            ...codexReplayPreamble({
              nativeThreadId,
              nativeTurnId: "event-origin-old-turn",
              prompt: "Work",
            }),
            ...outputEntries("event-origin-old-turn", "Old output"),
          ];
          const replacementEntries = codexReplayPreamble({
            nativeThreadId,
            nativeTurnId: "event-origin-new-turn",
            prompt: "Work",
          }).slice(0, 3);
          replacementEntries.push(
            {
              type: "expect_outbound",
              frame: {
                id: 2,
                method: "thread/resume",
                params: {
                  threadId: nativeThreadId,
                  excludeTurns: true,
                  cwd: "/workspace",
                  model: "gpt-5.4",
                  config: CodexAdapterV2.CODEX_THREAD_CONFIG,
                },
              },
            },
            {
              type: "emit_inbound",
              frame: {
                id: 2,
                result: {
                  thread: { id: nativeThreadId, updatedAt: 1782622450, cwd: "/workspace" },
                  cwd: "/workspace",
                  model: "gpt-5.4",
                  modelProvider: "openai",
                  serviceTier: null,
                },
              },
            },
            ...codexReplayPreamble({
              nativeThreadId,
              nativeTurnId: "event-origin-new-turn",
              prompt: "Work",
            }).slice(5),
            ...outputEntries("event-origin-new-turn", "New output"),
          );
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({ scenario, entries: initialEntries }),
            (event) =>
              boundary === "queued output" &&
              event.type === "provider_turn.updated" &&
              event.providerTurn.nativeTurnRef?.nativeId === "event-origin-old-turn" &&
              event.providerTurn.status === "running"
                ? Deferred.succeed(queuedOutput, event).pipe(
                    Effect.andThen(Deferred.await(releaseOutput)),
                  )
                : Effect.void,
            undefined,
            undefined,
            {
              runtimeGeneration: initialGeneration,
              resolveRuntime: Effect.sync(() => ({
                config: DEFAULT_CODEX_SETTINGS,
                environment: {},
                revision,
              })),
              readRuntimeRevisionForSend: Effect.sync(() => revision),
              resolveRuntimeForSend: Effect.sync(() => ({
                config: DEFAULT_CODEX_SETTINGS,
                environment: {},
                revision,
              })),
              beforeRuntimeReplacement: () => Effect.void,
              withRuntimeReplacement: (_generation, replace) => replace,
              transcriptForOpen: (ordinal) =>
                makeCodexReplayTranscript({
                  scenario: `event-origin-open-${ordinal}`,
                  entries: ordinal === 0 ? initialEntries : replacementEntries,
                }),
              transformClient: (client, ordinal) => ({
                ...client,
                handleServerNotification: ((method, handler) => {
                  if (ordinal === 0 && method === "thread/settings/updated") {
                    const captured = handler as NonNullable<typeof oldSettings>;
                    oldSettings = (payload) =>
                      Deferred.succeed(callbackEntered, undefined).pipe(
                        Effect.andThen(Deferred.await(releaseCallback)),
                        Effect.andThen(captured(payload)),
                      );
                  }
                  return client.handleServerNotification(method, handler);
                }) satisfies CodexClient.CodexAppServerClient["Service"]["handleServerNotification"],
              }),
            },
          );
          const originalBinding = runtimeBinding(harness);
          const first = makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("event-origin-first"),
            text: "Work",
          });
          yield* harness.runtime.startTurn(first);
          let oldEvent: ProviderAdapterV2Event;
          if (boundary === "queued output") oldEvent = yield* Deferred.await(queuedOutput);
          else {
            yield* harness.firstTerminal;
            oldEvent = harness.terminalEvents()[0]!;
          }
          const oldOrigin = readProviderEventOrigin(oldEvent)!;
          assert.isDefined(
            oldOrigin,
            "Origin must be captured before the subscriber can suspend or replacement begins",
          );
          assert.equal(oldOrigin.producer.runtimeGeneration, initialGeneration);
          assert.deepEqual(oldOrigin.turn?.binding, originalBinding);
          assert.equal(
            (yield* oldOrigin.producer.revalidateCurrent.pipe(Effect.result))._tag,
            "Success",
          );
          const suspended =
            boundary === "suspended callback"
              ? yield* oldSettings!({
                  threadId: nativeThreadId,
                  threadSettings: {
                    model: "stale-event-origin-model",
                    modelProvider: "openai",
                    effort: "high",
                    cwd: "/workspace",
                    approvalPolicy: "never",
                    approvalsReviewer: "user",
                    sandboxPolicy: { type: "dangerFullAccess" },
                    collaborationMode: {
                      mode: "default",
                      settings: { model: "stale-event-origin-model" },
                    },
                  },
                }).pipe(Effect.forkChild)
              : undefined;
          if (suspended !== undefined) yield* Deferred.await(callbackEntered);
          revision = "synthetic-event-origin-new-revision";
          const second: ProviderAdapterV2TurnInput = {
            ...makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make("event-origin-second"),
              text: "Work",
            }),
            nativeOperation: {
              ...originalBinding,
              operationId: "event-origin-start-before-refresh",
              operation: "start_turn",
              attemptId: RunAttemptId.make("event-origin-second"),
            },
          };
          yield* harness.runtime.startTurn(second);
          const actualGeneration = harness.runtime.runtimeGeneration!;
          assert.notEqual(actualGeneration, initialGeneration);
          assert.equal(second.nativeOperation!.runtimeGeneration, initialGeneration);
          assert.equal(
            readProviderEventOrigin(oldEvent)!.producer.runtimeGeneration,
            initialGeneration,
          );
          assert.equal(
            (yield* oldOrigin.producer.revalidateCurrent.pipe(Effect.result))._tag,
            "Failure",
          );
          yield* Deferred.succeed(releaseOutput, undefined);
          if (suspended !== undefined) {
            yield* Deferred.succeed(releaseCallback, undefined);
            yield* Fiber.join(suspended);
          }
          yield* awaitUntil(
            () => harness.terminalEvents().length === 2,
            "both actual native turn completions",
          );
          const newTurn = harness.events.find(
            (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
              event.type === "provider_turn.updated" &&
              event.providerTurn.nativeTurnRef?.nativeId === "event-origin-new-turn",
          )!;
          const newOrigin = readProviderEventOrigin(newTurn)!;
          assert.isDefined(newOrigin);
          assert.notEqual(newOrigin.producer.token, oldOrigin.producer.token);
          assert.equal(newOrigin.producer.runtimeGeneration, actualGeneration);
          assert.deepEqual(newOrigin.turn, {
            binding: { ...originalBinding, runtimeGeneration: actualGeneration },
            runId: second.runId,
            attemptId: second.attemptId,
            providerTurnId: newTurn.providerTurn.id,
          });
          assert.equal(
            (yield* newOrigin.producer.revalidateCurrent.pipe(Effect.result))._tag,
            "Success",
          );
          assert.isFalse(Object.hasOwn(newTurn, "origin"));
          assert.isFalse(
            harness.events.some(
              (event) =>
                event.type === "runtime_identity.observed" &&
                event.attestation.observed.model.status === "observed" &&
                event.attestation.observed.model.value === "stale-event-origin-model",
            ),
          );
          for (const [text, generation] of [
            ["Old output", initialGeneration],
            ["New output", actualGeneration],
          ] as const) {
            const output = harness.events.find(
              (event): event is Extract<ProviderAdapterV2Event, { type: "message.updated" }> =>
                event.type === "message.updated" &&
                event.message.text === text &&
                !event.message.streaming,
            )!;
            assert.isDefined(output, "Completed coalesced output must survive both incarnations");
            assert.equal(readProviderEventOrigin(output)!.producer.runtimeGeneration, generation);
          }
        }),
      ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    );
  }

  const capacityErrorEntry = (
    threadId: string,
    turnId: string,
    code = "serverOverloaded",
  ): Extract<CodexReplay.CodexAppServerReplayEntry, { type: "emit_inbound" }> => ({
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
  const awaitCapacityDelay = (
    harness: { readonly events: ReadonlyArray<ProviderAdapterV2Event> },
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

  it.effect("does not treat a Codex start ACK as native actor completion", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "managed-start-ack",
            entries: capacityPreamble("native-managed-start-ack", "managed-ack-turn"),
          }),
        );
        const turn = makeCodexTestTurnInput({
          threadId: harness.threadId,
          providerThread: harness.providerThread,
          now: yield* DateTime.now,
          attemptId: RunAttemptId.make("managed-start-ack"),
          text: "Work",
        });
        const managed = yield* prepareManagedCodexTurn(harness.runtime, turn);
        yield* withProviderManagedActorExecution(
          managed.startExecution,
          harness.runtime.startTurn(turn),
        );
        yield* managed.reader.bindManagedExecution(managed.managedExecution);
        assert.equal((yield* managed.reader.readClosure).status, "pending");
        assert.equal(
          (yield* managed.reader.revalidateCompletionBinding.pipe(Effect.result))._tag,
          "Failure",
        );
        assert.lengthOf(harness.terminalEvents(), 0);
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("joins actual Codex completion before a matching start ACK and managed commit", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "native-managed-early-completion";
        const nativeTurnId = "managed-early-turn";
        const releaseAck = yield* Deferred.make<void>();
        const completionSeen = yield* Deferred.make<void>();
        const preamble = capacityPreamble(nativeThreadId, nativeTurnId);
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "managed-early-completion",
            entries: [
              ...preamble.slice(0, 6),
              capacityCompletionEntry(nativeThreadId, nativeTurnId, "completed"),
              preamble[6]!,
            ],
          }),
          undefined,
          undefined,
          undefined,
          {
            beforeEmitInbound: (entry) =>
              Predicate.isObject(entry.frame) && entry.frame.id === 3
                ? Deferred.await(releaseAck)
                : Effect.void,
            afterNotification: (method) =>
              method === "turn/completed"
                ? Deferred.succeed(completionSeen, undefined).pipe(Effect.asVoid)
                : Effect.void,
          },
        );
        const turn = makeCodexTestTurnInput({
          threadId: harness.threadId,
          providerThread: harness.providerThread,
          now: yield* DateTime.now,
          attemptId: RunAttemptId.make("managed-early"),
          text: "Work",
        });
        const managed = yield* prepareManagedCodexTurn(harness.runtime, turn);
        const start = yield* withProviderManagedActorExecution(
          managed.startExecution,
          harness.runtime.startTurn({ ...turn }),
        ).pipe(Effect.forkChild);
        yield* Deferred.await(completionSeen);
        assert.equal((yield* managed.reader.readClosure).status, "pending");
        assert.equal(
          (yield* managed.reader.revalidateCompletionBinding.pipe(Effect.result))._tag,
          "Failure",
        );
        yield* Deferred.succeed(releaseAck, undefined);
        yield* Fiber.join(start);
        yield* harness.firstTerminal;
        yield* managed.reader.revalidateCompletionBinding;
        assert.equal((yield* managed.reader.readClosure).status, "pending");
        yield* managed.reader.bindManagedExecution(managed.managedExecution);
        const closure = yield* managed.reader.awaitNativeClosure;
        assert.equal(closure.status, "closed");
        if (closure.status !== "closed") return;
        const root = closure.observation.descriptor.actors.find(
          (actor) => actor.kind === "foreground",
        )!;
        assert.equal(root.source.nativeTurnId, nativeTurnId);
        assert.equal(root.source.nativeThreadId, nativeThreadId);
        assert.equal(root.source.runtimeGeneration, harness.runtime.runtimeGeneration);
        assert.equal(root.endEvidence.kind, "endpoint_and_task_joins");
        if (root.endEvidence.kind !== "endpoint_and_task_joins") return;
        assert.equal(root.endEvidence.endpoint.kind, "native_endpoint");
        assert.equal(root.endEvidence.endpoint.outcome, "completed");
        assert.deepEqual(
          root.endEvidence.tasks.map((task) => task.outcome),
          ["completed"],
        );
        assert.lengthOf(harness.terminalEvents(), 1);
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("keeps Codex closure unknown when native command work survives its root endpoint", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "native-managed-command";
        const nativeTurnId = "managed-command-turn";
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "managed-command",
            entries: [
              ...capacityPreamble(nativeThreadId, nativeTurnId),
              {
                type: "emit_inbound",
                frame: {
                  method: "item/started",
                  params: {
                    threadId: nativeThreadId,
                    turnId: nativeTurnId,
                    startedAtMs: 1782622440500,
                    item: {
                      type: "commandExecution",
                      id: "managed-command-item",
                      command: "sleep 20",
                      cwd: "/workspace",
                      processId: "managed-native-process",
                      source: "unifiedExecStartup",
                      status: "inProgress",
                      commandActions: [{ type: "unknown", command: "sleep 20" }],
                      aggregatedOutput: null,
                      exitCode: null,
                      durationMs: null,
                    },
                  },
                },
              },
              capacityCompletionEntry(nativeThreadId, nativeTurnId, "completed"),
            ],
          }),
        );
        const turn = makeCodexTestTurnInput({
          threadId: harness.threadId,
          providerThread: harness.providerThread,
          now: yield* DateTime.now,
          attemptId: RunAttemptId.make("managed-command"),
          text: "Work",
        });
        const managed = yield* prepareManagedCodexTurn(harness.runtime, turn);
        yield* withProviderManagedActorExecution(
          managed.startExecution,
          harness.runtime.startTurn(turn),
        );
        yield* managed.reader.bindManagedExecution(managed.managedExecution);
        yield* harness.firstTerminal;
        assert.equal(harness.terminalEvents()[0]!.status, "completed");
        assert.isTrue(yield* harness.hasPendingBackgroundWork);
        assert.equal((yield* managed.reader.readClosure).status, "unknown");
        assert.equal(
          (yield* managed.reader.revalidateCompletionBinding.pipe(Effect.result))._tag,
          "Failure",
        );
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect(
    "preserves a promptless Codex capacity retry while additional closure coverage is unknown",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const nativeThreadId = "native-managed-capacity";
          const entries: Array<CodexReplay.CodexAppServerReplayEntry> = [
            ...capacityPreamble(nativeThreadId, "managed-capacity-first"),
            capacityErrorEntry(nativeThreadId, "managed-capacity-first"),
            capacityCompletionEntry(nativeThreadId, "managed-capacity-first"),
            ...capacityPreamble(nativeThreadId, "managed-capacity-retry")
              .slice(5, 7)
              .map((entry) =>
                (entry.type === "expect_outbound" || entry.type === "emit_inbound") &&
                Predicate.isObject(entry.frame)
                  ? {
                      ...entry,
                      frame: {
                        ...entry.frame,
                        id: 4,
                        ...(entry.frame.method === "turn/start"
                          ? {
                              params: {
                                ...(entry.frame.params as Record<string, unknown>),
                                input: [],
                              },
                            }
                          : {}),
                      },
                    }
                  : entry,
              ),
            capacityCompletionEntry(nativeThreadId, "managed-capacity-retry", "completed"),
          ];
          let sends = 0;
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({ scenario: "managed-capacity", entries }),
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
            attemptId: RunAttemptId.make("managed-capacity"),
            text: "Work",
          });
          const managed = yield* prepareManagedCodexTurn(harness.runtime, turn);
          yield* withProviderManagedActorExecution(
            managed.startExecution,
            harness.runtime.startTurn(turn),
          );
          yield* managed.reader.bindManagedExecution(managed.managedExecution);
          yield* awaitCapacityDelay(harness);
          yield* TestClock.adjust("9 seconds");
          assert.equal(sends, 1);
          assert.lengthOf(harness.terminalEvents(), 0);
          yield* TestClock.adjust("1 second");
          yield* harness.firstTerminal;
          assert.equal(sends, 2);
          assert.lengthOf(harness.terminalEvents(), 1);
          assert.equal(harness.terminalEvents()[0]!.status, "completed");
          assert.equal((yield* managed.reader.readClosure).status, "unknown");
          yield* TestClock.adjust("1 minute");
          assert.equal(sends, 2);
        }),
      ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect(
    "keeps five promptless capacity retries frozen and completes the original logical turn once",
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
          for (let retry = 1; retry <= 5; retry++) {
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
          let catalogEffort = "low";
          let catalogReads = 0;
          let sends = 0;
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({ scenario: "capacity-five-frozen", entries }),
            undefined,
            (method) =>
              Effect.sync(() => {
                if (method === "turn/start") sends++;
              }),
            undefined,
            {
              getModelCatalog: () =>
                Effect.sync(() => {
                  catalogReads++;
                  return [
                    {
                      slug: "gpt-5.4",
                      name: "Native model",
                      isCustom: false,
                      capabilities: {
                        optionDescriptors: [
                          {
                            id: "reasoningEffort",
                            label: "Effort",
                            type: "select",
                            options: [],
                            currentValue: catalogEffort,
                          },
                        ],
                      },
                    },
                  ];
                }),
            },
          );
          yield* harness.runtime.startTurn({
            ...makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make("capacity-frozen"),
              text: "Work",
            }),
            modelSelection: {
              ...CODEX_TEST_MODEL_SELECTION,
              options: [{ id: "serviceTier", value: "priority" }],
            },
          });
          for (let retry = 1; retry <= 5; retry++) {
            yield* awaitCapacityDelay(harness, retry);
            assert.lengthOf(harness.terminalEvents(), 0);
            catalogEffort = "high";
            yield* TestClock.adjust("9 seconds");
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
              ): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
                event.type === "provider_turn.updated",
            )!.providerTurn.id,
          );
          assert.equal(catalogReads, 1);
          const nativeAttempts = new Set(
            harness.events.flatMap((event) =>
              event.type === "provider_turn.updated"
                ? [event.providerTurn.nativeTurnRef?.nativeId]
                : [],
            ),
          );
          assert.deepEqual(
            [...nativeAttempts],
            ["capacity-0", "capacity-1", "capacity-2", "capacity-3", "capacity-4", "capacity-5"],
          );
          for (const event of harness.events) {
            if (event.type !== "provider_turn.updated" && event.type !== "turn.terminal") continue;
            const origin = readProviderEventOrigin(event)!;
            assert.isDefined(origin);
            assert.equal(
              origin.turn?.providerTurnId,
              event.type === "provider_turn.updated" ? event.providerTurn.id : event.providerTurnId,
            );
            assert.equal(origin.turn?.binding.nativeThreadId, threadId);
            assert.equal(origin.turn?.binding.runtimeGeneration, harness.runtime.runtimeGeneration);
            assert.equal(origin.turn?.attemptId, RunAttemptId.make("capacity-frozen"));
          }
          yield* TestClock.adjust("1 minute");
          assert.equal(sends, 6);
        }),
      ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  for (const outcome of ["early-success", "unknown-start", "stop", "unload"] as const) {
    it.effect(`fences capacity recovery for ${outcome}`, () =>
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
            undefined,
            {
              rawRequest: (method, params) =>
                Effect.sync(() => {
                  assert.equal(method, "thread/goal/get");
                  assert.deepEqual(params, { threadId });
                  return { goal: null };
                }),
            },
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
            (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
              event.type === "provider_turn.updated",
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
      ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    );
  }

  for (const phase of ["running", "waiting_retry"] as const) {
    it.effect(
      `reports not_pending for an acknowledged ${phase} start and fences capacity recovery`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const nativeThreadId = `native-not-pending-${phase}`;
            const nativeTurnId = `native-not-pending-turn-${phase}`;
            const releaseCapacity = yield* Deferred.make<void>();
            let closedIncarnations = 0;
            let sends = 0;
            const harness = yield* makeCodexReplayHarness(
              makeCodexReplayTranscript({
                scenario: `not-pending-${phase}`,
                entries: [
                  ...capacityPreamble(nativeThreadId, nativeTurnId),
                  {
                    ...capacityErrorEntry(nativeThreadId, nativeTurnId),
                    label: "capacity-after-observation",
                  },
                  capacityCompletionEntry(nativeThreadId, nativeTurnId),
                ],
              }),
              undefined,
              (method) =>
                Effect.sync(() => {
                  if (method === "turn/start") sends++;
                }),
              undefined,
              {
                runtimeGeneration: `not-pending-generation-${phase}`,
                onFactoryOpen: () =>
                  Effect.addFinalizer(() =>
                    Effect.sync(() => {
                      closedIncarnations++;
                    }),
                  ),
                rawRequest: () =>
                  Effect.die(
                    "Pending-start discrimination must not make a native observer or cleanup request",
                  ),
                withPendingStartStop: () =>
                  Effect.die(
                    "An acknowledged request must not close its incarnation as a pending start",
                  ),
                beforeEmitInbound: (entry) =>
                  phase === "running" && entry.label === "capacity-after-observation"
                    ? Deferred.await(releaseCapacity)
                    : Effect.void,
              },
            );
            const binding = runtimeBinding(harness);
            const turnInput = {
              ...makeCodexTestTurnInput({
                threadId: harness.threadId,
                providerThread: harness.providerThread,
                now: yield* DateTime.now,
                attemptId: RunAttemptId.make(`not-pending-attempt-${phase}`),
                text: "Work",
              }),
              nativeOperation: {
                operationId: `not-pending-start-operation-${phase}`,
                operation: "start_turn" as const,
                instanceId: binding.instanceId,
                threadId: binding.threadId,
                providerSessionId: binding.providerSessionId,
                providerThreadId: binding.providerThreadId,
                runtimeGeneration: binding.runtimeGeneration,
                attemptId: RunAttemptId.make(`not-pending-attempt-${phase}`),
              },
            };
            const stopInput: ProviderPendingStartStopInput = {
              binding,
              runId: turnInput.runId,
              attemptId: turnInput.attemptId,
              startOperation: turnInput.nativeOperation,
            };
            assert.equal(
              (yield* harness.runtime.stopPendingStart!(stopInput)).status,
              "unknown",
              "A native binding without an app-issued start record cannot prove not_pending",
            );
            yield* harness.runtime.startTurn(turnInput);
            if (phase === "waiting_retry") yield* awaitCapacityDelay(harness);
            assert.lengthOf(harness.terminalEvents(), 0);
            for (const stale of [
              { ...stopInput, attemptId: RunAttemptId.make("unrelated-not-pending-attempt") },
              {
                ...stopInput,
                binding: { ...binding, runtimeGeneration: "unrelated-not-pending-generation" },
              },
              {
                ...stopInput,
                startOperation: {
                  ...stopInput.startOperation,
                  operationId: "unrelated-not-pending-operation",
                },
              },
            ])
              assert.equal((yield* harness.runtime.stopPendingStart!(stale)).status, "unknown");
            const observed = yield* harness.runtime.stopPendingStart!(stopInput);
            assert.deepEqual(observed, {
              status: "not_pending",
              binding,
              runId: turnInput.runId,
              attemptId: turnInput.attemptId,
              startOperationId: turnInput.nativeOperation.operationId,
            });
            assert.equal(
              closedIncarnations,
              0,
              "not_pending is not a provider-session cleanup receipt",
            );
            if (phase === "running") yield* Deferred.succeed(releaseCapacity, undefined);
            yield* harness.firstTerminal;
            assert.lengthOf(harness.terminalEvents(), 1);
            assert.equal(
              harness.terminalEvents()[0]!.status,
              phase === "running" ? "failed" : "interrupted",
            );
            yield* TestClock.adjust("1 minute");
            assert.equal(
              sends,
              1,
              "Neither a retained delay nor a later capacity failure may dispatch recovery after Stop",
            );
            assert.equal(closedIncarnations, 0);
            assert.lengthOf(harness.terminalEvents(), 1);
          }),
        ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    );
  }

  it.effect(
    "bounds user Stop after a capacity continuation was dispatched without a start reply",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const nativeThreadId = "native-capacity-dispatched-stop";
          const originalNativeTurnId = "native-capacity-dispatched-original";
          const dispatched = yield* Deferred.make<void>();
          let closedIncarnations = 0;
          let reservations = 0;
          let expectedStop: ProviderPendingStartStopInput | undefined;
          const requests: Array<string> = [];
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({
              scenario: "capacity-dispatched-stop",
              entries: [
                ...capacityPreamble(nativeThreadId, originalNativeTurnId),
                capacityErrorEntry(nativeThreadId, originalNativeTurnId),
                capacityCompletionEntry(nativeThreadId, originalNativeTurnId),
                {
                  type: "expect_outbound",
                  label: "turn/start/continuation",
                  frame: {
                    id: 4,
                    method: "turn/start",
                    params: {
                      threadId: nativeThreadId,
                      input: [],
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
                  label: "turn/start/held-continuation-reply",
                  frame: {
                    id: 4,
                    result: {
                      turn: makeCodexReplayTurn({
                        id: "unacknowledged-continuation",
                        status: "inProgress",
                      }),
                    },
                  },
                },
              ],
            }),
            undefined,
            (method) =>
              Effect.sync(() => {
                requests.push(method);
              }),
            undefined,
            {
              runtimeGeneration: "capacity-dispatched-stop-generation",
              goalResponses: new Map([[nativeThreadId, { goal: null }]]),
              onFactoryOpen: () =>
                Effect.addFinalizer(() =>
                  Effect.sync(() => {
                    closedIncarnations++;
                  }),
                ),
              withPendingStartStop: (stopInput, stop) =>
                Effect.gen(function* () {
                  assert.deepEqual(stopInput, expectedStop);
                  reservations++;
                  yield* stop;
                  return {
                    status: "cancelled",
                    binding: stopInput.binding,
                    runId: stopInput.runId,
                    attemptId: stopInput.attemptId,
                    startOperationId: stopInput.startOperation.operationId,
                  };
                }),
              beforeEmitInbound: (entry) =>
                entry.label === "turn/start/held-continuation-reply"
                  ? Deferred.succeed(dispatched, undefined).pipe(Effect.andThen(Effect.never))
                  : Effect.void,
            },
          );
          const binding = runtimeBinding(harness);
          const turnInput = {
            ...makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make("capacity-dispatched-stop-attempt"),
              text: "Work",
            }),
            nativeOperation: {
              operationId: "capacity-dispatched-start-operation",
              operation: "start_turn" as const,
              instanceId: binding.instanceId,
              threadId: binding.threadId,
              providerSessionId: binding.providerSessionId,
              providerThreadId: binding.providerThreadId,
              runtimeGeneration: binding.runtimeGeneration,
              attemptId: RunAttemptId.make("capacity-dispatched-stop-attempt"),
            },
          };
          expectedStop = {
            binding,
            runId: turnInput.runId,
            attemptId: turnInput.attemptId,
            startOperation: turnInput.nativeOperation,
          };
          yield* harness.runtime.startTurn(turnInput);
          yield* awaitCapacityDelay(harness);
          yield* TestClock.adjust("10 seconds");
          // The replay accepted the outbound retry frame; its actual reply never arrives.
          yield* Deferred.await(dispatched);
          assert.equal(requests.filter((method) => method === "turn/start").length, 2);
          assert.isFalse(
            harness.events.some(
              (event) =>
                event.type === "provider_turn.updated" &&
                event.providerTurn.nativeTurnRef?.nativeId === "unacknowledged-continuation",
            ),
          );
          const original = harness.events.find(
            (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
              event.type === "provider_turn.updated" &&
              event.providerTurn.nativeTurnRef?.nativeId === originalNativeTurnId,
          )!.providerTurn.id;
          const stopped = yield* harness.runtime
            .interruptTurn({
              providerThread: harness.providerThread,
              providerTurnId: original,
              requestRuntimeRestart: true,
              nativeOperation: {
                ...runtimeBinding(harness),
                operationId: "capacity-dispatched-stop-operation",
                operation: "interrupt_turn",
                attemptId: turnInput.attemptId,
              },
            })
            .pipe(Effect.result, Effect.timeoutOption("4 seconds"), Effect.forkChild);
          yield* TestClock.adjust("4 seconds");
          const bounded = yield* Fiber.join(stopped);
          assert.equal(
            bounded._tag,
            "Some",
            "Stop must not wait for the dispatched continuation start reply",
          );
          if (bounded._tag !== "Some") return;
          assert.equal(bounded.value._tag, "Success");
          assert.equal(
            closedIncarnations,
            1,
            "Stop must close the captured pending-start incarnation once",
          );
          assert.equal(reservations, 1);
          assert.isFalse(
            requests.includes("turn/interrupt"),
            "there is no acknowledged current native turn to interrupt",
          );
          yield* harness.firstTerminal;
          assert.lengthOf(harness.terminalEvents(), 1);
          assert.equal(harness.terminalEvents()[0]!.providerTurnId, original);
          assert.equal(harness.terminalEvents()[0]!.status, "interrupted");
          yield* TestClock.adjust("1 minute");
          assert.equal(requests.filter((method) => method === "turn/start").length, 2);
        }),
      ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("bounds user Stop after an initial start was dispatched without a native turn ID", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "native-initial-dispatched-stop";
        const dispatched = yield* Deferred.make<void>();
        const requests: Array<string> = [];
        let closedIncarnations = 0;
        let reservations = 0;
        let expectedStop: ProviderPendingStartStopInput | undefined;
        const preamble = capacityPreamble(nativeThreadId, "unacknowledged-initial");
        const initialReply = preamble[6];
        if (initialReply?.type !== "emit_inbound")
          return yield* Effect.die("The initial turn/start reply must be an inbound replay entry.");
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "initial-dispatched-stop",
            entries: [
              ...preamble.slice(0, 6),
              { ...initialReply, label: "turn/start/held-initial-reply" },
            ],
          }),
          undefined,
          (method) =>
            Effect.sync(() => {
              requests.push(method);
            }),
          undefined,
          {
            runtimeGeneration: "initial-dispatched-stop-generation",
            goalResponses: new Map([[nativeThreadId, { goal: null }]]),
            onFactoryOpen: () =>
              Effect.addFinalizer(() =>
                Effect.sync(() => {
                  closedIncarnations++;
                }),
              ),
            withPendingStartStop: (stopInput, stop) =>
              Effect.gen(function* () {
                assert.deepEqual(stopInput, expectedStop);
                reservations++;
                yield* stop;
                return {
                  status: "cancelled",
                  binding: stopInput.binding,
                  runId: stopInput.runId,
                  attemptId: stopInput.attemptId,
                  startOperationId: stopInput.startOperation.operationId,
                };
              }),
            beforeEmitInbound: (entry) =>
              entry.label === "turn/start/held-initial-reply"
                ? Deferred.succeed(dispatched, undefined).pipe(Effect.andThen(Effect.never))
                : Effect.void,
          },
        );
        const binding = runtimeBinding(harness);
        const turnInput = {
          ...makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("initial-dispatched-stop-attempt"),
            text: "Work",
          }),
          nativeOperation: {
            operationId: "initial-dispatched-start-operation",
            operation: "start_turn" as const,
            instanceId: binding.instanceId,
            threadId: binding.threadId,
            providerSessionId: binding.providerSessionId,
            providerThreadId: binding.providerThreadId,
            runtimeGeneration: binding.runtimeGeneration,
            attemptId: RunAttemptId.make("initial-dispatched-stop-attempt"),
          },
        };
        expectedStop = {
          binding,
          runId: turnInput.runId,
          attemptId: turnInput.attemptId,
          startOperation: turnInput.nativeOperation,
        };
        const initial = yield* harness.runtime
          .startTurn(turnInput)
          .pipe(Effect.result, Effect.forkChild);
        // Native dispatch is accepted before Stop; no reply supplies a turn ID.
        yield* Deferred.await(dispatched);
        assert.equal(requests.filter((method) => method === "turn/start").length, 1);
        assert.isFalse(harness.events.some((event) => event.type === "provider_turn.updated"));
        const stopped = yield* (
          harness.runtime.stopPendingStart?.(expectedStop) ??
          Effect.succeed({
            status: "unknown" as const,
            reason: "not_supported",
          })
        ).pipe(Effect.timeoutOption("4 seconds"), Effect.forkChild);
        yield* TestClock.adjust("4 seconds");
        const bounded = yield* Fiber.join(stopped);
        assert.equal(
          bounded._tag,
          "Some",
          "Stop must not wait for the dispatched initial start reply",
        );
        if (bounded._tag !== "Some") return;
        assert.equal(
          bounded.value.status,
          "cancelled",
          "Codex must cancel its pending initial start without inventing a turn ID",
        );
        assert.equal(closedIncarnations, 1);
        assert.equal(reservations, 1);
        assert.isFalse(requests.includes("turn/interrupt"));
        const settled = yield* Fiber.await(initial).pipe(
          Effect.timeoutOption("1 second"),
          Effect.forkChild,
        );
        yield* TestClock.adjust("1 second");
        assert.equal(
          (yield* Fiber.join(settled))._tag,
          "Some",
          "the dispatched start caller must settle after Stop",
        );
        assert.isFalse(harness.events.some((event) => event.type === "provider_turn.updated"));
        assert.lengthOf(harness.terminalEvents(), 0);
        const retry = yield* harness.runtime
          .startTurn({
            ...turnInput,
            attemptId: RunAttemptId.make("after-initial-stop"),
            nativeOperation: {
              ...turnInput.nativeOperation,
              operationId: "after-initial-stop-operation",
              attemptId: RunAttemptId.make("after-initial-stop"),
            },
          })
          .pipe(Effect.result);
        assert.equal(retry._tag, "Failure");
        yield* TestClock.adjust("1 minute");
        assert.equal(requests.filter((method) => method === "turn/start").length, 1);
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
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
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("cancels delayed capacity recovery when the validated managed revision changes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "native-capacity-managed-revision";
        let revision = "synthetic-original-revision";
        let revisionReads = 0;
        let opens = 0;
        let sends = 0;
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "capacity-managed-revision",
            entries: [
              ...capacityPreamble(nativeThreadId, "capacity-managed-original"),
              capacityErrorEntry(nativeThreadId, "capacity-managed-original"),
              capacityCompletionEntry(nativeThreadId, "capacity-managed-original"),
            ],
          }),
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
            readRuntimeRevisionForSend: Effect.sync(() => {
              revisionReads++;
              return revision;
            }),
            resolveRuntimeForSend: Effect.die(
              "A delayed retry must not materialize a replacement runtime",
            ),
            onFactoryOpen: () =>
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
            attemptId: RunAttemptId.make("capacity-managed-revision-attempt"),
            text: "Work",
          }),
        );
        yield* awaitCapacityDelay(harness);
        const originalGeneration = harness.runtime.runtimeGeneration;
        revision = "synthetic-current-revision";
        yield* TestClock.adjust("10 seconds");
        yield* harness.firstTerminal;
        assert.equal(revisionReads, 2);
        assert.equal(opens, 1);
        assert.equal(sends, 1);
        assert.equal(harness.runtime.runtimeGeneration, originalGeneration);
        assert.lengthOf(harness.terminalEvents(), 1);
        assert.equal(harness.terminalEvents()[0]!.status, "interrupted");
        const attempts = harness.events.filter(
          (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
            event.type === "provider_turn.updated",
        );
        assert.equal(attempts.at(-1)?.providerTurn.status, "failed");
        assert.isTrue(
          attempts.every(
            (event) => event.providerTurn.nativeTurnRef?.nativeId === "capacity-managed-original",
          ),
        );
        yield* TestClock.adjust("1 minute");
        assert.equal(sends, 1);
        assert.lengthOf(harness.terminalEvents(), 1);
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  const makeOwnedExitFactory = (
    options: {
      readonly isRunning?: ChildProcessSpawner.ChildProcessHandle["isRunning"];
      readonly exitCode?: ChildProcessSpawner.ChildProcessHandle["exitCode"];
    } = {},
  ) =>
    Effect.gen(function* () {
      const reads = { status: 0, exit: 0 };
      const handle = ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(4_242),
        isRunning: Effect.sync(() => {
          reads.status++;
        }).pipe(Effect.andThen(options.isRunning ?? Effect.succeed(false))),
        exitCode: Effect.sync(() => {
          reads.exit++;
        }).pipe(
          Effect.andThen(options.exitCode ?? Effect.succeed(ChildProcessSpawner.ExitCode(9))),
        ),
        kill: () => Effect.die("Leader observation must not signal or terminate a process"),
        stdin: Sink.drain,
        stdout: Stream.never,
        stderr: Stream.never,
        all: Stream.never,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
        unref: Effect.succeed(Effect.void),
      });
      const factory = yield* CodexAdapterV2.CodexAppServerClientFactory.pipe(
        Effect.provide(CodexAdapterV2.codexAppServerClientFactoryFromSettingsLayer),
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.succeed(handle)),
        ),
        Effect.provideService(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      );
      return { factory, reads };
    });

  const ownedExitTranscript = (scenario: string) =>
    makeCodexReplayTranscript({
      scenario,
      entries: codexReplayPreamble({
        nativeThreadId: `native-${scenario}`,
        nativeTurnId: "unused",
        prompt: "unused",
      }).slice(0, 5),
    });

  it.effect(
    "retains exact leader exit observation after scope closure without treating closure as exit",
    () =>
      Effect.gen(function* () {
        const exit = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
        const fixture = yield* makeOwnedExitFactory({
          isRunning: Deferred.isDone(exit).pipe(Effect.map((done) => !done)),
          exitCode: Deferred.await(exit),
        });
        const runtimeScope = yield* Scope.make();
        yield* Effect.addFinalizer(() => Scope.close(runtimeScope, Exit.void));
        const harness = yield* makeCodexReplayHarness(
          ownedExitTranscript("owned-exit-retained"),
          undefined,
          undefined,
          undefined,
          {
            runtimeGeneration: "actual-owned-generation",
            ownedClientFactory: fixture.factory,
          },
        ).pipe(Effect.provideService(Scope.Scope, runtimeScope));
        const identity = harness.runtime.ownedRuntimeIdentity!;
        assert.isDefined(identity);
        assert.isTrue(Object.isFrozen(identity));
        assert.equal(identity.runtimeGeneration, "actual-owned-generation");
        assert.equal(identity.providerSessionId, harness.runtime.providerSessionId);
        assert.equal(identity.instanceId, harness.runtime.instanceId);
        assert.equal(identity.pid, 4_242);
        yield* awaitUntil(
          () => harness.events.some((event) => event.type === "runtime_identity.observed"),
          "the captured current native source",
        );
        const queuedOrigin = readProviderEventOrigin(
          harness.events.find((event) => event.type === "runtime_identity.observed")!,
        )!;
        assert.isDefined(queuedOrigin);
        yield* Scope.close(runtimeScope, Exit.void);
        assert.equal(
          (yield* queuedOrigin.producer.revalidateCurrent.pipe(Effect.result))._tag,
          "Success",
          "Normal scope closure must not invalidate queued terminal draining from the same native incarnation",
        );
        assert.deepEqual(yield* harness.runtime.observeOwnedRuntimeExit!(identity), {
          status: "unknown",
          reason: "owned_process_running",
          identity,
        });
        assert.equal(fixture.reads.exit, 0);
        yield* Deferred.succeed(exit, ChildProcessSpawner.ExitCode(17));
        const observed = yield* harness.runtime.observeOwnedRuntimeExit!(identity);
        assert.equal(observed.status, "leader_exited");
        if (observed.status !== "leader_exited")
          return assert.fail("Expected the captured handle's actual leader exit");
        assert.deepEqual(observed.identity, identity);
        assert.equal(observed.exitCode, 17);
        assert.isString(observed.observedAt);
        assert.equal(fixture.reads.exit, 1);
      }).pipe(
        Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer)),
        Effect.provideService(HostProcessPlatform, "linux"),
      ),
  );

  for (const [field, changed] of [
    ["instanceId", ProviderInstanceId.make("other-owned-instance")],
    ["providerSessionId", ProviderSessionId.make("other-owned-session")],
    ["runtimeGeneration", "other-owned-generation"],
    ["handleToken", "other-owned-handle"],
    ["pid", 4_243],
  ] as const) {
    it.effect(`rejects mismatched owned ${field} before reading the captured process`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fixture = yield* makeOwnedExitFactory();
          const harness = yield* makeCodexReplayHarness(
            ownedExitTranscript(`owned-mismatch-${field}`),
            undefined,
            undefined,
            undefined,
            {
              ownedClientFactory: fixture.factory,
            },
          );
          const identity = harness.runtime.ownedRuntimeIdentity!;
          const observed = yield* harness.runtime.observeOwnedRuntimeExit!({
            ...identity,
            [field]: changed,
          });
          assert.deepEqual(observed, {
            status: "unknown",
            reason: "owned_runtime_identity_mismatch",
            identity,
          });
          assert.deepEqual(fixture.reads, { status: 0, exit: 0 });
        }),
      ).pipe(
        Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer)),
        Effect.provideService(HostProcessPlatform, "linux"),
      ),
    );
  }

  it.effect("keeps leader exit unknown when the factory supplies no owned process", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeCodexReplayHarness(ownedExitTranscript("owned-process-missing"));
        assert.isUndefined(harness.runtime.ownedRuntimeIdentity);
        const observed = yield* harness.runtime.observeOwnedRuntimeExit!({
          instanceId: harness.runtime.instanceId,
          providerSessionId: harness.runtime.providerSessionId,
          runtimeGeneration: harness.runtime.runtimeGeneration!,
          handleToken: "unowned-expected-handle",
          pid: 4_242,
        });
        assert.deepEqual(observed, { status: "unknown", reason: "owned_process_unavailable" });
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect(
    "rejects a captured handle from a different native incarnation before process readback",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fixture = yield* makeOwnedExitFactory();
          const harness = yield* makeCodexReplayHarness(
            ownedExitTranscript("owned-incarnation-mismatch"),
            undefined,
            undefined,
            undefined,
            {
              runtimeGeneration: "current-native-incarnation",
              ownedClientFactory: {
                open: (input) =>
                  fixture.factory.open({
                    ...input,
                    runtimeGeneration: "previous-native-incarnation",
                  }),
              },
            },
          );
          assert.isUndefined(harness.runtime.ownedRuntimeIdentity);
          const observed = yield* harness.runtime.observeOwnedRuntimeExit!({
            instanceId: harness.runtime.instanceId,
            providerSessionId: harness.runtime.providerSessionId,
            runtimeGeneration: harness.runtime.runtimeGeneration!,
            handleToken: "unmatched-incarnation-handle",
            pid: 4_242,
          });
          assert.deepEqual(observed, { status: "unknown", reason: "owned_process_unavailable" });
          assert.deepEqual(fixture.reads, { status: 0, exit: 0 });
        }),
      ).pipe(
        Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer)),
        Effect.provideService(HostProcessPlatform, "linux"),
      ),
  );

  for (const phase of ["status", "exit"] as const) {
    it.effect(`keeps failed owned ${phase} readback unknown`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const failure = Effect.fail(
            PlatformError.systemError({
              _tag: "Unknown",
              module: "ChildProcess",
              method: phase,
            }),
          );
          const fixture = yield* makeOwnedExitFactory(
            phase === "status" ? { isRunning: failure } : { exitCode: failure },
          );
          const harness = yield* makeCodexReplayHarness(
            ownedExitTranscript(`owned-failed-${phase}`),
            undefined,
            undefined,
            undefined,
            {
              ownedClientFactory: fixture.factory,
            },
          );
          const observed = yield* harness.runtime.observeOwnedRuntimeExit!(
            harness.runtime.ownedRuntimeIdentity!,
          );
          assert.equal(observed.status, "unknown");
          if (observed.status === "unknown")
            assert.equal(observed.reason, `owned_process_${phase}_unavailable`);
        }),
      ).pipe(
        Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer)),
        Effect.provideService(HostProcessPlatform, "linux"),
      ),
    );

    it.effect(`rejects owned identity changes during asynchronous ${phase} readback`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const started = yield* Deferred.make<void>();
          const released = yield* Deferred.make<void>();
          const pending = Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Deferred.await(released)),
          );
          const fixture = yield* makeOwnedExitFactory(
            phase === "status"
              ? {
                  isRunning: pending.pipe(Effect.as(false)),
                }
              : {
                  exitCode: pending.pipe(Effect.as(ChildProcessSpawner.ExitCode(9))),
                },
          );
          const harness = yield* makeCodexReplayHarness(
            ownedExitTranscript(`owned-race-${phase}`),
            undefined,
            undefined,
            undefined,
            {
              ownedClientFactory: fixture.factory,
            },
          );
          const expected = { ...harness.runtime.ownedRuntimeIdentity! };
          const read = yield* harness.runtime.observeOwnedRuntimeExit!(expected).pipe(
            Effect.forkChild,
          );
          yield* Deferred.await(started);
          expected.runtimeGeneration = "replaced-during-read";
          yield* Deferred.succeed(released, undefined);
          const observed = yield* Fiber.join(read);
          assert.equal(observed.status, "unknown");
          if (observed.status === "unknown")
            assert.equal(observed.reason, "owned_runtime_identity_changed");
        }),
      ).pipe(
        Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer)),
        Effect.provideService(HostProcessPlatform, "linux"),
      ),
    );

    it.effect(`bounds unavailable owned ${phase} readback and retains unknown`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const started = yield* Deferred.make<void>();
          const pending = Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never));
          const fixture = yield* makeOwnedExitFactory(
            phase === "status" ? { isRunning: pending } : { exitCode: pending },
          );
          const harness = yield* makeCodexReplayHarness(
            ownedExitTranscript(`owned-timeout-${phase}`),
            undefined,
            undefined,
            undefined,
            {
              ownedClientFactory: fixture.factory,
            },
          );
          const read = yield* harness.runtime.observeOwnedRuntimeExit!(
            harness.runtime.ownedRuntimeIdentity!,
          ).pipe(Effect.forkChild);
          yield* Deferred.await(started);
          yield* TestClock.adjust("3 seconds");
          const observed = yield* Fiber.join(read);
          assert.equal(observed.status, "unknown");
          if (observed.status === "unknown")
            assert.equal(observed.reason, `owned_process_${phase}_unavailable`);
        }),
      ).pipe(
        Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer)),
        Effect.provideService(HostProcessPlatform, "linux"),
      ),
    );
  }

  it.effect("rechecks current authority after managed resolution and before factory open", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* makeCodexNativeCreationFixture("managed-resolution-revocation");
        let opened = false;
        const result = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "managed-resolution-revocation",
            entries: [],
          }),
          undefined,
          undefined,
          undefined,
          {
            nativeCreationExecution: fixture.execution,
            runtimeGeneration: "managed-resolution-generation",
            resolveRuntime: fixture.revoke.pipe(
              Effect.as({
                config: DEFAULT_CODEX_SETTINGS,
                environment: {},
                revision: "synthetic-revision",
              }),
            ),
            onFactoryOpen: () =>
              Effect.sync(() => {
                opened = true;
              }),
          },
        ).pipe(Effect.result);
        if (
          result._tag !== "Failure" ||
          !Schema.is(ProviderAdapterOpenSessionError)(result.failure)
        )
          return assert.fail("Expected current authority to reject factory open");
        assert.isFalse(opened);
        assert.equal(result.failure.nativeEffect?.outcome, "unknown");
        assert.equal(fixture.startCount, 1);
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  for (const revokedAt of ["after-open", "initialize-response"] as const) {
    it.effect(
      `retains issued authority and rejects revocation at ${revokedAt} before the next lazy native effect`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fixture = yield* makeCodexNativeCreationFixture(`lazy-${revokedAt}`);
            const requests: string[] = [];
            let runtime:
              | import("../ProviderAdapter.ts").ProviderAdapterV2SessionRuntime
              | undefined;
            const result = yield* makeCodexReplayHarness(
              makeCodexReplayTranscript({
                scenario: `lazy-authority-${revokedAt}`,
                entries: codexReplayPreamble({
                  nativeThreadId: "native-lazy-authority",
                  nativeTurnId: "unused",
                  prompt: "unused",
                }).slice(0, 5),
              }),
              undefined,
              (method) =>
                Effect.sync(() => {
                  requests.push(method);
                }),
              undefined,
              {
                nativeCreationExecution: fixture.execution,
                onFactoryOpen: (input) =>
                  Effect.sync(() => {
                    assert.strictEqual(input.nativeCreationExecution, fixture.execution);
                  }),
                onRuntimeOpened: (opened) =>
                  Effect.sync(() => {
                    runtime = opened;
                  }).pipe(
                    Effect.andThen(revokedAt === "after-open" ? fixture.revoke : Effect.void),
                  ),
                beforeEmitInbound: (entry) =>
                  revokedAt === "initialize-response" && entry.label === "initialize"
                    ? fixture.revoke
                    : Effect.void,
              },
            ).pipe(Effect.result);
            if (
              result._tag !== "Failure" ||
              !Schema.is(ProviderAdapterEnsureThreadError)(result.failure)
            )
              return assert.fail(
                "Expected rejected lazy initialization to fail the whole ensure operation",
              );
            assert.equal(result.failure.nativeEffect?.outcome, "unknown");
            assert.deepEqual(requests, revokedAt === "after-open" ? [] : ["initialize"]);
            assert.isDefined(runtime);
            assert.isUndefined(runtime!.continuationSourceIdentity);
            assert.equal(fixture.startCount, 1);
          }),
        ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    );
  }

  it.effect("does not apply an old native creation grant to resident goal or liveness reads", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* makeCodexNativeCreationFixture("resident-observation");
        const nativeThreadId = "native-resident-observation";
        const calls: string[] = [];
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "resident-observation",
            entries: codexReplayPreamble({
              nativeThreadId,
              nativeTurnId: "unused",
              prompt: "unused",
              cwd: fixture.execution.resources.worktreePath,
            }).slice(0, 5),
          }),
          undefined,
          undefined,
          undefined,
          {
            nativeCreationExecution: fixture.execution,
            rawRequest: (method) =>
              Effect.sync(() => {
                calls.push(method);
                return method === "thread/goal/get"
                  ? { goal: null }
                  : { thread: { id: nativeThreadId, status: { type: "idle" } } };
              }),
          },
        );
        yield* fixture.revoke;
        const previousReads = fixture.authorizationReads;
        assert.equal((yield* harness.runtime.getGoal!(runtimeBinding(harness))).state, "inactive");
        assert.equal(
          (yield* harness.runtime.observeThreadRuntime!(runtimeBinding(harness))).status,
          "unknown",
        );
        assert.deepEqual(calls, ["thread/goal/get", "thread/read"]);
        assert.equal(fixture.authorizationReads, previousReads);
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  for (const method of ["ensure", "resume", "start", "compact", "inject"] as const) {
    for (const state of ["current", "revoked", "resource-mismatch"] as const) {
      it.effect(
        `checks ${state} current operation authority at the ${method} native mutation seam`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const scenario = `current-authority-${method}-${state}`;
              const fixture = yield* makeCodexNativeCreationFixture(scenario);
              const nativeThreadId = `native-${scenario}`;
              const prompt = "Run the current command";
              const runtimePolicy = {
                ...CODEX_TEST_RUNTIME_POLICY,
                cwd: fixture.execution.resources.worktreePath,
              };
              const preamble = codexReplayPreamble({
                nativeThreadId,
                nativeTurnId: `turn-${scenario}`,
                prompt,
                cwd: runtimePolicy.cwd,
              });
              const history: ProviderAdapterV2HistoricalContext = {
                context: "Synthetic previous context",
                messages: [],
              };
              const nativeMethod = {
                ensure: "thread/start",
                resume: "thread/resume",
                start: "turn/start",
                compact: "thread/compact/start",
                inject: "thread/inject_items",
              }[method];
              const operation = {
                ensure: "ensure_thread",
                resume: "resume_thread",
                start: "start_turn",
                compact: "compact_thread",
                inject: "inject_history",
              } as const;
              const successfulEntries: Array<CodexReplay.CodexAppServerReplayEntry> =
                method === "ensure"
                  ? preamble
                      .slice(3, 5)
                      .map((entry) =>
                        "frame" in entry && Predicate.isObject(entry.frame) && "id" in entry.frame
                          ? { ...entry, frame: { ...entry.frame, id: 3 } }
                          : entry,
                      )
                  : method === "start"
                    ? preamble.slice(5, 7)
                    : method === "resume"
                      ? []
                      : [
                          {
                            type: "expect_outbound",
                            label: nativeMethod,
                            frame: {
                              id: 3,
                              method: nativeMethod,
                              params:
                                method === "inject"
                                  ? {
                                      threadId: nativeThreadId,
                                      items: historyResponseItems(
                                        history.messages,
                                        history.context,
                                      ),
                                    }
                                  : { threadId: nativeThreadId },
                            },
                          },
                          {
                            type: "emit_inbound",
                            label: nativeMethod,
                            frame: { id: 3, result: {} },
                          },
                        ];
              const calls: string[] = [];
              const harness = yield* makeCodexReplayHarness(
                makeCodexReplayTranscript({
                  scenario,
                  entries: [
                    ...preamble.slice(0, 5),
                    ...(state === "current" ? successfulEntries : []),
                  ],
                }),
                undefined,
                (called) =>
                  Effect.sync(() => {
                    calls.push(called);
                  }),
                undefined,
                {
                  rawRequest: (called) =>
                    Effect.sync(() => {
                      calls.push(called);
                      assert.equal(called, "thread/resume");
                      return {
                        cwd: runtimePolicy.cwd,
                        thread: {
                          id: nativeThreadId,
                          updatedAt: 1782622450,
                          cwd: runtimePolicy.cwd,
                        },
                      };
                    }),
                },
              );
              if (state === "revoked") yield* fixture.revoke;
              const nativeCreationExecution =
                state === "resource-mismatch"
                  ? {
                      ...fixture.execution,
                      resources: {
                        ...fixture.execution.resources,
                        worktreePath: "/synthetic/unrelated-worktree",
                      },
                    }
                  : fixture.execution;
              const nativeOperation = {
                ...runtimeBinding(harness),
                operationId: scenario,
                operation: operation[method],
              };
              const turnInput = {
                ...makeCodexTestTurnInput({
                  threadId: harness.threadId,
                  providerThread: harness.providerThread,
                  now: yield* DateTime.now,
                  attemptId: RunAttemptId.make(scenario),
                  text: prompt,
                }),
                nativeCreationExecution,
                nativeOperation,
                runtimePolicy,
              };
              const mutation: Effect.Effect<
                unknown,
                import("../ProviderAdapter.ts").ProviderAdapterV2Error
              > =
                method === "ensure"
                  ? harness.runtime.ensureThread({
                      threadId: harness.threadId,
                      modelSelection: CODEX_TEST_MODEL_SELECTION,
                      runtimePolicy,
                      nativeCreationExecution,
                      nativeOperation,
                    })
                  : method === "resume"
                    ? harness.runtime.resumeThread({
                        threadId: harness.threadId,
                        providerThread: harness.providerThread,
                        nativeCreationExecution,
                        nativeOperation,
                      })
                    : method === "start"
                      ? harness.runtime.startTurn(turnInput)
                      : method === "compact"
                        ? harness.runtime.compactThread!(turnInput)
                        : harness.runtime.injectHistory!({
                            ...history,
                            providerThread: harness.providerThread,
                            nativeCreationExecution,
                            nativeOperation,
                          });
              const result = yield* mutation.pipe(Effect.result);
              assert.equal(fixture.startCount, 1);
              assert.deepEqual(calls, [
                "initialize",
                "thread/start",
                ...(state === "current" ? [nativeMethod] : []),
              ]);
              if (state === "current") {
                assert.equal(result._tag, "Success");
                if (method === "inject" && result._tag === "Success")
                  assert.equal(result.success, true);
                return;
              }
              if (result._tag !== "Failure")
                return assert.fail("Expected current authority to block the native mutation");
              const error = result.failure;
              assert.equal(
                error._tag,
                method === "ensure"
                  ? "ProviderAdapterEnsureThreadError"
                  : method === "resume"
                    ? "ProviderAdapterResumeThreadError"
                    : method === "inject"
                      ? "ProviderAdapterProtocolError"
                      : "ProviderAdapterTurnStartError",
              );
              if (!("nativeEffect" in error) || !("cause" in error))
                return assert.fail(
                  "Expected whole-operation evidence and the native authority cause",
                );
              assert.equal(error.nativeEffect?.outcome, "unknown");
              assert.equal(error.nativeEffect?.operationId, scenario);
              const directoryFailure = Schema.is(CodexErrors.CodexAppServerRequestError)(
                error.cause,
              )
                ? error.cause.cause
                : error.cause;
              if (Schema.is(CodexErrors.CodexAppServerRequestError)(error.cause))
                assert.equal(error.cause.code, -32603);
              if (!Schema.is(ProviderAdapterProtocolError)(directoryFailure))
                return assert.fail(
                  "Expected actual native directory or issued authority validation",
                );
              if (state === "revoked") {
                if (!Schema.is(NativeCreationAuthorityError)(directoryFailure.cause))
                  return assert.fail("Expected the actual issued authority rejection as the cause");
                assert.equal(directoryFailure.cause.code, "stale_grant");
              }
            }),
          ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
      );
    }
  }

  it.effect("rechecks current operation authority after the asynchronous native resume gate", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* makeCodexNativeCreationFixture("resume-gate-revocation");
        const calls: string[] = [];
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "resume-gate-revocation",
            entries: codexReplayPreamble({
              nativeThreadId: "native-authority-gate",
              nativeTurnId: "unused",
              prompt: "unused",
              cwd: fixture.execution.resources.worktreePath,
            }).slice(0, 5),
          }),
          undefined,
          undefined,
          undefined,
          {
            rawRequest: (method) =>
              Effect.sync(() => {
                calls.push(method);
                return {};
              }),
          },
        );
        const result = yield* harness.runtime
          .resumeThread({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            nativeCreationExecution: fixture.execution,
            nativeOperation: {
              ...runtimeBinding(harness),
              operationId: "resume-gate-revocation",
              operation: "resume_thread",
            },
            beforeNativeResume: (actual) =>
              Effect.sync(() => {
                assert.strictEqual(actual, harness.runtime.continuationSourceIdentity);
              }).pipe(Effect.andThen(Effect.yieldNow), Effect.andThen(fixture.revoke)),
          })
          .pipe(Effect.result);
        if (
          result._tag !== "Failure" ||
          !Schema.is(ProviderAdapterResumeThreadError)(result.failure)
        )
          return assert.fail("Expected revocation after the source gate to prevent native resume");
        assert.deepEqual(calls, []);
        assert.equal(result.failure.nativeEffect?.outcome, "unknown");
        assert.equal(fixture.startCount, 1);
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect(
    "does not apply an old native creation grant to later ordinary resident mutations",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fixture = yield* makeCodexNativeCreationFixture("ordinary-resident-start");
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({
              scenario: "ordinary-resident-start",
              entries: codexReplayPreamble({
                nativeThreadId: "native-ordinary-resident",
                nativeTurnId: "ordinary-turn",
                prompt: "Ordinary later command",
                cwd: fixture.execution.resources.worktreePath,
              }).slice(0, 7),
            }),
            undefined,
            undefined,
            undefined,
            { nativeCreationExecution: fixture.execution },
          );
          yield* fixture.revoke;
          const previousReads = fixture.authorizationReads;
          yield* harness.runtime.startTurn({
            ...makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make("ordinary-resident-start"),
              text: "Ordinary later command",
            }),
            runtimePolicy: {
              ...CODEX_TEST_RUNTIME_POLICY,
              cwd: fixture.execution.resources.worktreePath,
            },
          });
          assert.equal(fixture.authorizationReads, previousReads);
          assert.equal(fixture.startCount, 1);
        }),
      ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  for (const reply of [
    "matching",
    "conflicting",
    "missing-thread",
    "missing-top",
    "malformed",
  ] as const) {
    it.effect(
      `requires ${reply} native reply directory evidence before confirming bundled resume or injecting history`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const scenario = `native-directory-${reply}`;
            const fixture = yield* makeCodexNativeCreationFixture(scenario);
            const cwd = fixture.execution.resources.worktreePath;
            const nativeThreadId = `native-${scenario}`;
            const calls: string[] = [];
            const history: ProviderAdapterV2HistoricalContext = {
              messages: [],
              context: "Previous context",
            };
            const harness = yield* makeCodexReplayHarness(
              makeCodexReplayTranscript({
                scenario,
                entries: [
                  ...codexReplayPreamble({
                    nativeThreadId,
                    nativeTurnId: "unused",
                    prompt: "unused",
                    cwd,
                  }).slice(0, 5),
                  ...(reply !== "matching"
                    ? []
                    : [
                        {
                          type: "expect_outbound" as const,
                          label: "inject",
                          frame: {
                            id: 3,
                            method: "thread/inject_items",
                            params: {
                              threadId: nativeThreadId,
                              items: historyResponseItems(history.messages, history.context),
                            },
                          },
                        },
                        {
                          type: "emit_inbound" as const,
                          label: "inject",
                          frame: { id: 3, result: {} },
                        },
                      ]),
                ],
              }),
              undefined,
              (method) =>
                Effect.sync(() => {
                  calls.push(method);
                }),
              undefined,
              {
                rawRequest: (method) =>
                  Effect.sync(() => {
                    calls.push(method);
                    assert.equal(method, "thread/resume");
                    return {
                      ...(reply === "missing-top"
                        ? {}
                        : { cwd: reply === "conflicting" ? "/synthetic/other-directory" : cwd }),
                      thread: {
                        id: nativeThreadId,
                        updatedAt: 1782622450,
                        ...(reply === "missing-thread"
                          ? {}
                          : { cwd: reply === "malformed" ? 42 : cwd }),
                      },
                    };
                  }),
              },
            );
            const resumed = yield* harness.runtime
              .resumeThread({
                threadId: harness.threadId,
                providerThread: harness.providerThread,
                runtimePolicy: { ...CODEX_TEST_RUNTIME_POLICY, cwd },
                nativeCreationExecution: fixture.execution,
                nativeOperation: {
                  ...runtimeBinding(harness),
                  operationId: scenario,
                  operation: "resume_thread",
                },
              })
              .pipe(Effect.result);
            if (reply === "matching") assert.equal(resumed._tag, "Success");
            else {
              if (
                resumed._tag !== "Failure" ||
                !Schema.is(ProviderAdapterResumeThreadError)(resumed.failure)
              )
                return assert.fail(
                  "Expected unproved actual reply directory to fail the complete resume",
                );
              assert.equal(resumed.failure.nativeEffect?.outcome, "unknown");
            }
            const injected = yield* harness.runtime.injectHistory!({
              ...history,
              providerThread: harness.providerThread,
              nativeCreationExecution: fixture.execution,
              nativeOperation: {
                ...runtimeBinding(harness),
                operationId: `${scenario}-inject`,
                operation: "inject_history",
              },
            }).pipe(Effect.result);
            if (reply === "matching") {
              assert.equal(injected._tag, "Success");
              if (injected._tag === "Success") assert.isTrue(injected.success);
            } else {
              if (
                injected._tag !== "Failure" ||
                !Schema.is(ProviderAdapterProtocolError)(injected.failure)
              )
                return assert.fail("Expected missing current target evidence to block injection");
              assert.equal(injected.failure.nativeEffect?.outcome, "unknown");
            }
            assert.deepEqual(calls, [
              "initialize",
              "thread/start",
              "thread/resume",
              ...(reply === "matching" ? ["thread/inject_items"] : []),
            ]);
            assert.equal(fixture.startCount, 1);
          }),
        ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    );
  }

  it.effect("keeps a conflicting completed native thread start directory unknown", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* makeCodexNativeCreationFixture("directory-start-conflict");
        const calls: string[] = [];
        const entries = codexReplayPreamble({
          nativeThreadId: "native-directory-start-conflict",
          nativeTurnId: "unused",
          prompt: "unused",
          cwd: fixture.execution.resources.worktreePath,
        })
          .slice(0, 5)
          .map((entry) => {
            if (
              entry.type !== "emit_inbound" ||
              entry.label !== "thread/start" ||
              !Predicate.isObject(entry.frame) ||
              !Predicate.isObject(entry.frame.result) ||
              !Predicate.isObject(entry.frame.result.thread)
            )
              return entry;
            return {
              ...entry,
              frame: {
                ...entry.frame,
                result: {
                  ...entry.frame.result,
                  thread: {
                    ...entry.frame.result.thread,
                    cwd: "/synthetic/conflicting-native-directory",
                  },
                },
              },
            };
          });
        const result = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "directory-start-conflict",
            entries,
          }),
          undefined,
          (method) =>
            Effect.sync(() => {
              calls.push(method);
            }),
          undefined,
          {
            nativeCreationExecution: fixture.execution,
          },
        ).pipe(Effect.result);
        if (
          result._tag !== "Failure" ||
          !Schema.is(ProviderAdapterEnsureThreadError)(result.failure)
        )
          return assert.fail(
            "Expected completed native response disagreement to fail the whole ensure operation",
          );
        assert.equal(result.failure.nativeEffect?.outcome, "unknown");
        assert.deepEqual(calls, ["initialize", "thread/start"]);
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect(
    "does not reuse prior incarnation directory evidence even with an explicit resume cwd",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fixture = yield* makeCodexNativeCreationFixture("directory-incarnation");
          const cwd = fixture.execution.resources.worktreePath;
          const seed = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({
              scenario: "directory-incarnation-seed",
              entries: codexReplayPreamble({
                nativeThreadId: "native-directory-incarnation",
                nativeTurnId: "unused",
                prompt: "unused",
                cwd,
              }).slice(0, 5),
            }),
          );
          const calls: string[] = [];
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({
              scenario: "directory-new-incarnation",
              entries: codexReplayPreamble({
                nativeThreadId: "native-directory-incarnation",
                nativeTurnId: "unused",
                prompt: "unused",
                cwd,
              }).slice(0, 3),
            }),
            undefined,
            (method) =>
              Effect.sync(() => {
                calls.push(method);
              }),
            undefined,
            {
              initialProviderThread: seed.providerThread,
              nativeCreationExecution: fixture.execution,
              rawRequest: (method) =>
                Effect.sync(() => {
                  calls.push(method);
                  return {};
                }),
            },
          );
          const resumed = yield* harness.runtime
            .resumeThread({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              runtimePolicy: { ...CODEX_TEST_RUNTIME_POLICY, cwd },
              nativeCreationExecution: fixture.execution,
              nativeOperation: {
                ...runtimeBinding(harness),
                operationId: "new-incarnation-resume",
                operation: "resume_thread",
              },
            })
            .pipe(Effect.result);
          if (
            resumed._tag !== "Failure" ||
            !Schema.is(ProviderAdapterResumeThreadError)(resumed.failure)
          )
            return assert.fail(
              "Expected absent current incarnation target evidence to hold native resume",
            );
          assert.equal(resumed.failure.nativeEffect?.outcome, "unknown");
          const injected = yield* harness.runtime.injectHistory!({
            providerThread: harness.providerThread,
            messages: [],
            context: "Prior context",
            nativeCreationExecution: fixture.execution,
          }).pipe(Effect.result);
          assert.equal(injected._tag, "Failure");
          assert.deepEqual(calls, ["initialize"]);
        }),
      ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect(
    "captures current target directory from an existing completed native read without another query",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fixture = yield* makeCodexNativeCreationFixture("directory-read-refresh");
          const cwd = fixture.execution.resources.worktreePath;
          const nativeThreadId = "native-directory-read-refresh";
          const history: ProviderAdapterV2HistoricalContext = {
            messages: [],
            context: "Prior context",
          };
          const calls: string[] = [];
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({
              scenario: "directory-read-refresh",
              entries: [
                ...codexReplayPreamble({
                  nativeThreadId,
                  nativeTurnId: "unused",
                  prompt: "unused",
                  cwd,
                }).slice(0, 5),
                {
                  type: "expect_outbound",
                  label: "inject",
                  frame: {
                    id: 3,
                    method: "thread/inject_items",
                    params: {
                      threadId: nativeThreadId,
                      items: historyResponseItems(history.messages, history.context),
                    },
                  },
                },
                { type: "emit_inbound", label: "inject", frame: { id: 3, result: {} } },
              ],
            }),
            undefined,
            (method) =>
              Effect.sync(() => {
                calls.push(method);
              }),
            undefined,
            {
              rawRequest: (method) =>
                Effect.sync(() => {
                  calls.push(method);
                  return method === "thread/resume"
                    ? { thread: { id: nativeThreadId, updatedAt: 1782622450 } }
                    : { thread: { id: nativeThreadId, cwd, status: { type: "idle" } } };
                }),
            },
          );
          yield* harness.runtime.resumeThread({
            providerThread: harness.providerThread,
            threadId: harness.threadId,
          });
          yield* harness.runtime.observeThreadRuntime!(runtimeBinding(harness));
          assert.isTrue(
            yield* harness.runtime.injectHistory!({
              ...history,
              providerThread: harness.providerThread,
              nativeCreationExecution: fixture.execution,
            }),
          );
          assert.deepEqual(calls, [
            "initialize",
            "thread/start",
            "thread/resume",
            "thread/read",
            "thread/inject_items",
          ]);
        }),
      ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("invalidates actual target directory evidence when the native binding unloads", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* makeCodexNativeCreationFixture("directory-unload");
        const nativeThreadId = "native-directory-unload";
        const calls: string[] = [];
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "directory-unload",
            entries: [
              ...codexReplayPreamble({
                nativeThreadId,
                nativeTurnId: "unused",
                prompt: "unused",
                cwd: fixture.execution.resources.worktreePath,
              }).slice(0, 5),
              {
                type: "expect_outbound",
                label: "unsubscribe",
                frame: {
                  id: 3,
                  method: "thread/unsubscribe",
                  params: { threadId: nativeThreadId },
                },
              },
              {
                type: "emit_inbound",
                label: "unsubscribe",
                frame: { id: 3, result: { status: "unsubscribed" } },
              },
            ],
          }),
          undefined,
          (method) =>
            Effect.sync(() => {
              calls.push(method);
            }),
        );
        yield* harness.runtime.unloadThread!({ providerThread: harness.providerThread });
        const injected = yield* harness.runtime.injectHistory!({
          providerThread: harness.providerThread,
          messages: [],
          context: "Prior context",
          nativeCreationExecution: fixture.execution,
        }).pipe(Effect.result);
        assert.equal(injected._tag, "Failure");
        assert.deepEqual(calls, ["initialize", "thread/start", "thread/unsubscribe"]);
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect(
    "rechecks native target directory after asynchronous authority invalidates the binding",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let beforeGrantRead: () => Effect.Effect<void> = () => Effect.void;
          const fixture = yield* makeCodexNativeCreationFixture("directory-authorization-race", {
            beforeGrantRead: () => beforeGrantRead(),
          });
          const nativeThreadId = "native-directory-authorization-race";
          const calls: string[] = [];
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({
              scenario: "directory-authorization-race",
              entries: [
                ...codexReplayPreamble({
                  nativeThreadId,
                  nativeTurnId: "unused",
                  prompt: "unused",
                  cwd: fixture.execution.resources.worktreePath,
                }).slice(0, 5),
                {
                  type: "expect_outbound",
                  label: "unsubscribe",
                  frame: {
                    id: 3,
                    method: "thread/unsubscribe",
                    params: { threadId: nativeThreadId },
                  },
                },
                {
                  type: "emit_inbound",
                  label: "unsubscribe",
                  frame: { id: 3, result: { status: "unsubscribed" } },
                },
              ],
            }),
            undefined,
            (method) =>
              Effect.sync(() => {
                calls.push(method);
              }),
          );
          beforeGrantRead = () =>
            Effect.sync(() => {
              beforeGrantRead = () => Effect.void;
            }).pipe(
              Effect.andThen(
                harness.runtime.unloadThread!({ providerThread: harness.providerThread }),
              ),
              Effect.orDie,
            );
          const result = yield* harness.runtime.injectHistory!({
            providerThread: harness.providerThread,
            messages: [],
            context: "Prior context",
            nativeCreationExecution: fixture.execution,
            nativeOperation: {
              ...runtimeBinding(harness),
              operationId: "directory-authorization-race",
              operation: "inject_history",
            },
          }).pipe(Effect.result);
          if (result._tag !== "Failure" || !Schema.is(ProviderAdapterProtocolError)(result.failure))
            return assert.fail(
              "Expected invalidated native target evidence to block the history mutation",
            );
          assert.equal(result.failure.nativeEffect?.outcome, "unknown");
          assert.deepEqual(calls, ["initialize", "thread/start", "thread/unsubscribe"]);
          assert.equal(fixture.startCount, 1);
        }),
      ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  for (const fixture of ["direct", "overlay", "mismatched-overlay", "unproved-overlay"] as const) {
    it.effect(
      `captures ${fixture} continuation identity only from the initialized native incarnation`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const nativeThreadId = "native-continuation-source";
            const sharedHomePath = "/synthetic/shared-codex-home";
            const effectiveHomePath = "/synthetic/overlay-codex-home";
            const reportedHome =
              fixture === "overlay"
                ? effectiveHomePath
                : fixture === "direct"
                  ? "/synthetic/ambient-codex-home"
                  : "/synthetic/native-codex-home";
            const layout = {
              mode: fixture === "direct" ? ("direct" as const) : ("authOverlay" as const),
              sharedHomePath,
              effectiveHomePath: fixture === "direct" ? undefined : effectiveHomePath,
              continuationKey: `codex:home:${sharedHomePath}`,
            };
            const transcript = makeCodexReplayTranscript({
              scenario: `continuation-source-${fixture}`,
              entries: codexReplayPreamble({
                nativeThreadId,
                nativeTurnId: "unused",
                prompt: "unused",
                codexHome: reportedHome,
              }).slice(0, 5),
            });
            const harness = yield* makeCodexReplayHarness(
              transcript,
              undefined,
              undefined,
              undefined,
              {
                runtimeGeneration: "initialized-native-generation",
                settings: {
                  ...DEFAULT_CODEX_SETTINGS,
                  homePath: fixture === "direct" ? "" : effectiveHomePath,
                  shadowHomePath: fixture === "direct" ? "" : effectiveHomePath,
                },
                environment: { CODEX_HOME: "/synthetic/ambient-codex-home" },
                ...(fixture === "unproved-overlay" ? {} : { continuationHomeLayout: layout }),
                onRuntimeOpened: (runtime) =>
                  Effect.sync(() => {
                    assert.isUndefined(runtime.continuationSourceIdentity);
                    layout.sharedHomePath = "/synthetic/later-shared-home";
                    layout.continuationKey = "codex:home:/synthetic/later-shared-home";
                  }),
              },
            );
            const source = harness.runtime.continuationSourceIdentity;
            if (fixture === "mismatched-overlay" || fixture === "unproved-overlay") {
              assert.isUndefined(source);
              return;
            }
            assert.deepEqual(source, {
              driverKind: CodexAdapterV2.CODEX_DRIVER_KIND,
              continuationKey: `codex:home:${fixture === "overlay" ? sharedHomePath : reportedHome}`,
              runtimeGeneration: harness.runtime.runtimeGeneration,
            });
            assert.isTrue(Object.isFrozen(source));
          }),
        ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    );
  }

  it.effect("awaits the reserved generation fence before resolving and opening Codex", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const order: string[] = [];
        const nativeThreadId = "native-replacement-fence";
        const transcript = makeCodexReplayTranscript({
          scenario: "replacement-fence",
          entries: codexReplayPreamble({
            nativeThreadId,
            nativeTurnId: "unused",
            prompt: "unused",
          }).slice(0, 5),
        });
        const harness = yield* makeCodexReplayHarness(
          transcript,
          undefined,
          (method) =>
            Effect.sync(() => {
              order.push(method);
            }),
          undefined,
          {
            runtimeGeneration: "reserved-generation",
            beforeRuntimeReplacement: (generation) =>
              Effect.sync(() => {
                assert.equal(generation, "reserved-generation");
                order.push("fence");
              }),
            resolveRuntime: Effect.sync(() => {
              order.push("resolve");
              return {
                config: DEFAULT_CODEX_SETTINGS,
                environment: {},
                revision: "synthetic-revision",
              };
            }),
            onFactoryOpen: () =>
              Effect.sync(() => {
                order.push("factory");
              }),
            onRuntimeOpened: (runtime) =>
              Effect.sync(() => {
                assert.isUndefined(runtime.continuationSourceIdentity);
              }),
          },
        );
        assert.deepEqual(order, ["fence", "resolve", "factory", "initialize", "thread/start"]);
        assert.equal(
          harness.runtime.continuationSourceIdentity?.runtimeGeneration,
          "reserved-generation",
        );
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("does not resolve or open Codex when the reserved generation fence fails", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const order: string[] = [];
        const result = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "replacement-fence-failure",
            entries: [],
          }),
          undefined,
          undefined,
          undefined,
          {
            runtimeGeneration: "rejected-generation",
            beforeRuntimeReplacement: () =>
              Effect.sync(() => {
                order.push("fence");
              }).pipe(
                Effect.andThen(
                  Effect.fail(
                    new ProviderAdapterOpenSessionError({
                      driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                      providerSessionId: ProviderSessionId.make(
                        "provider-session-replacement-fence-failure",
                      ),
                      cause: new Error("Synthetic generation fence rejection"),
                    }),
                  ),
                ),
              ),
            resolveRuntime: Effect.sync(() => {
              order.push("resolve");
              return {
                config: DEFAULT_CODEX_SETTINGS,
                environment: {},
                revision: "synthetic-revision",
              };
            }),
            onFactoryOpen: () =>
              Effect.sync(() => {
                order.push("factory");
              }),
          },
        ).pipe(Effect.result);
        assert.equal(result._tag, "Failure");
        assert.deepEqual(order, ["fence"]);
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("keeps continuation source identity absent after native initialization fails", () =>
    Effect.scoped(
      Effect.gen(function* () {
        let opened: import("../ProviderAdapter.ts").ProviderAdapterV2SessionRuntime | undefined;
        const preamble = codexReplayPreamble({
          nativeThreadId: "unused",
          nativeTurnId: "unused",
          prompt: "unused",
        });
        const result = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "continuation-initialize-failure",
            entries: [
              preamble[0]!,
              {
                type: "emit_inbound",
                label: "initialize-failure",
                frame: {
                  id: 1,
                  error: { code: -32000, message: "Synthetic initialization failure" },
                },
              },
            ],
          }),
          undefined,
          undefined,
          undefined,
          {
            onRuntimeOpened: (runtime) =>
              Effect.sync(() => {
                opened = runtime;
              }),
          },
        ).pipe(Effect.result);
        assert.equal(result._tag, "Failure");
        assert.isDefined(opened);
        assert.isUndefined(opened!.continuationSourceIdentity);
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("binds actual Codex observations to a fresh process generation", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "native-identity-thread";
        const transcript = makeCodexReplayTranscript({
          scenario: "identity-generation",
          entries: codexReplayPreamble({
            nativeThreadId,
            nativeTurnId: "unused",
            prompt: "unused",
          }).slice(0, 5),
        });
        const first = yield* makeCodexReplayHarness(transcript);
        const second = yield* makeCodexReplayHarness(transcript);
        assert.isString(first.runtime.runtimeGeneration);
        assert.notEqual(first.runtime.runtimeGeneration, second.runtime.runtimeGeneration);
        yield* awaitUntil(
          () => first.events.some((event) => event.type === "runtime_identity.observed"),
          "native identity",
        );
        const observed = first.events.find((event) => event.type === "runtime_identity.observed");
        assert.isDefined(observed);
        if (observed?.type !== "runtime_identity.observed") return;
        assert.deepEqual(observed.binding, runtimeBinding(first));
        assert.equal(observed.attestation.runtimeGeneration, first.runtime.runtimeGeneration);
        assert.deepEqual(observed.attestation.observed.model, {
          status: "observed",
          value: "gpt-5.4",
          sourceEvent: "thread/start",
        });
        assert.deepEqual(observed.attestation.observed.backend, {
          status: "observed",
          value: "openai",
          sourceEvent: "thread/start",
        });
        assert.equal(observed.attestation.observed.account.status, "unavailable");
        assert.equal(observed.attestation.observed.serviceTier.status, "unavailable");
        const supplied = yield* makeCodexReplayHarness(
          transcript,
          undefined,
          undefined,
          undefined,
          {
            runtimeGeneration: "manager-launch-generation",
          },
        );
        assert.equal(supplied.runtime.runtimeGeneration, "manager-launch-generation");
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  for (const notificationCount of [1, 33]) {
    it.effect(
      `preserves ${notificationCount} eager Codex reroutes without fabricating requested identity`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const nativeThreadId = "native-eager-identity";
            const handled = yield* Deferred.make<void>();
            let count = 0;
            const preamble = codexReplayPreamble({
              nativeThreadId,
              nativeTurnId: "unused",
              prompt: "unused",
            }).slice(0, 5);
            const transcript = makeCodexReplayTranscript({
              scenario: `eager-reroute-${notificationCount}`,
              entries: [
                ...preamble.slice(0, 4),
                ...Array.from({ length: notificationCount }, (_, index) => ({
                  type: "emit_inbound" as const,
                  label: `eager-reroute-${index}`,
                  frame: {
                    method: "model/rerouted",
                    params: {
                      threadId: nativeThreadId,
                      turnId: "unused",
                      fromModel: "gpt-5.4",
                      toModel: "native-rerouted-model",
                      reason: "highRiskCyberActivity",
                    },
                  },
                })),
                ...preamble.slice(4),
              ],
            });
            const harness = yield* makeCodexReplayHarness(
              transcript,
              undefined,
              undefined,
              undefined,
              {
                beforeEmitInbound: (entry) =>
                  entry.label === "thread/start" ? Deferred.await(handled) : Effect.void,
                afterNotification: (method) =>
                  Effect.gen(function* () {
                    if (method !== "model/rerouted") return;
                    count++;
                    if (count === notificationCount) yield* Deferred.succeed(handled, undefined);
                  }),
              },
            );
            yield* awaitUntil(
              () =>
                harness.events.filter((event) => event.type === "runtime_identity.observed")
                  .length >= (notificationCount === 1 ? 2 : 1),
              "eager identity",
            );
            const identities = harness.events.filter(
              (event) => event.type === "runtime_identity.observed",
            );
            const last = identities.at(-1)!;
            assert.equal(last.attestation.requested.model, "gpt-5.4");
            assert.equal(last.attestation.observed.account.status, "unavailable");
            if (notificationCount === 1) {
              assert.deepEqual(last.attestation.observed.model, {
                status: "observed",
                value: "native-rerouted-model",
                sourceEvent: "model/rerouted",
              });
              assert.deepEqual(last.attestation.observed.backend, {
                status: "observed",
                value: "openai",
                sourceEvent: "thread/start",
              });
            } else {
              assert.equal(last.attestation.observed.model.status, "unknown");
              assert.equal(last.attestation.observed.backend.status, "unknown");
            }
          }),
        ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    );
  }

  for (const fixture of [
    { name: "null", response: { goal: null }, state: "inactive", reasonCode: "goal_null" },
    { name: "omitted", response: {}, state: "unknown", reasonCode: "goal_field_omitted" },
    { name: "malformed", response: { goal: {} }, state: "unknown", reasonCode: "malformed" },
    {
      name: "active",
      response: {
        goal: {
          createdAt: 1782622440,
          objective: "Finish",
          status: "active",
          threadId: "native-goal-thread",
          timeUsedSeconds: 1,
          tokensUsed: 10,
          updatedAt: 1782622441,
        },
      },
      state: "active",
      reasonCode: "goal_present",
    },
    {
      name: "mismatch",
      response: {
        goal: {
          createdAt: 1782622440,
          objective: "Finish",
          status: "active",
          threadId: "other-thread",
          timeUsedSeconds: 1,
          tokensUsed: 10,
          updatedAt: 1782622441,
        },
      },
      state: "unknown",
      reasonCode: "context_changed",
    },
  ] as const) {
    it.effect(`reads ${fixture.name} Codex goals without opening another native thread`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const calls: string[] = [];
          const transcript = makeCodexReplayTranscript({
            scenario: `goal-${fixture.name}`,
            entries: codexReplayPreamble({
              nativeThreadId: "native-goal-thread",
              nativeTurnId: "unused",
              prompt: "unused",
            }).slice(0, 5),
          });
          const harness = yield* makeCodexReplayHarness(
            transcript,
            undefined,
            undefined,
            undefined,
            {
              rawRequest: (method, params) =>
                Effect.sync(() => {
                  calls.push(method);
                  assert.deepEqual(params, { threadId: "native-goal-thread" });
                  return fixture.response;
                }),
            },
          );
          assert.isDefined(harness.runtime.getGoal);
          const result = yield* harness.runtime.getGoal!(runtimeBinding(harness));
          assert.deepEqual(result, {
            nativeThreadId: "native-goal-thread",
            state: fixture.state,
            reasonCode: fixture.reasonCode,
          });
          assert.deepEqual(calls, ["thread/goal/get"]);
          const stale = yield* harness.runtime.getGoal!({
            ...runtimeBinding(harness),
            runtimeGeneration: "old-generation",
          });
          assert.equal(stale.state, "unknown");
          assert.deepEqual(calls, ["thread/goal/get"]);
        }),
      ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    );
  }

  it.effect("keeps unsupported and timed-out Codex goals unknown", () =>
    Effect.scoped(
      Effect.gen(function* () {
        for (const unsupported of [true, false]) {
          const requestStarted = yield* Deferred.make<void>();
          const transcript = makeCodexReplayTranscript({
            scenario: `goal-unavailable-${unsupported}`,
            entries: codexReplayPreamble({
              nativeThreadId: "native-goal-unavailable",
              nativeTurnId: "unused",
              prompt: "unused",
            }).slice(0, 5),
          });
          const harness = yield* makeCodexReplayHarness(
            transcript,
            undefined,
            undefined,
            undefined,
            {
              rawRequest: () =>
                Deferred.succeed(requestStarted, undefined).pipe(
                  Effect.andThen(
                    unsupported
                      ? Effect.fail(
                          new CodexErrors.CodexAppServerRequestError({
                            code: -32601,
                            errorMessage: "Unsupported method",
                          }),
                        )
                      : Effect.never,
                  ),
                ),
            },
          );
          const read = yield* harness.runtime.getGoal!(runtimeBinding(harness)).pipe(
            Effect.forkChild,
          );
          yield* Deferred.await(requestStarted);
          if (!unsupported) yield* TestClock.adjust("3 seconds");
          assert.equal(
            (yield* Fiber.join(read)).reasonCode,
            unsupported ? "unsupported" : "timeout",
          );
        }
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("does not treat an idle root without complete descendant evidence as native idle", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const transcript = makeCodexReplayTranscript({
          scenario: "native-idle-incomplete",
          entries: codexReplayPreamble({
            nativeThreadId: "native-idle",
            nativeTurnId: "unused",
            prompt: "unused",
          }).slice(0, 5),
        });
        const calls: string[] = [];
        const harness = yield* makeCodexReplayHarness(transcript, undefined, undefined, undefined, {
          rawRequest: (method) =>
            Effect.sync(() => {
              calls.push(method);
              return { thread: { id: "native-idle", status: { type: "idle" } } };
            }),
        });
        const observation = yield* harness.runtime.observeThreadRuntime!(runtimeBinding(harness));
        assert.equal(observation.status, "unknown");
        assert.deepEqual(calls, ["thread/read"]);
        const mismatched = yield* harness.runtime.observeThreadRuntime!({
          ...runtimeBinding(harness),
          instanceId: ProviderInstanceId.make("other-account"),
        });
        assert.equal(mismatched.status, "unknown");
        assert.deepEqual(calls, ["thread/read"]);
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("invalidates an in-flight goal read when its runtime scope closes", () =>
    Effect.gen(function* () {
      const runtimeScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(runtimeScope, Exit.void));
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const calls: string[] = [];
      const transcript = makeCodexReplayTranscript({
        scenario: "goal-scope-close",
        entries: codexReplayPreamble({
          nativeThreadId: "native-goal-close",
          nativeTurnId: "unused",
          prompt: "unused",
        }).slice(0, 5),
      });
      const harness = yield* makeCodexReplayHarness(transcript, undefined, undefined, undefined, {
        rawRequest: (method) =>
          Effect.sync(() => calls.push(method)).pipe(
            Effect.andThen(Deferred.succeed(started, undefined)),
            Effect.andThen(Deferred.await(release)),
            Effect.as({ goal: null }),
          ),
      }).pipe(Effect.provideService(Scope.Scope, runtimeScope));
      const read = yield* harness.runtime.getGoal!(runtimeBinding(harness)).pipe(Effect.forkChild);
      yield* Deferred.await(started);
      yield* Scope.close(runtimeScope, Exit.void);
      assert.isUndefined(harness.runtime.continuationSourceIdentity);
      yield* Deferred.succeed(release, undefined);
      assert.equal((yield* Fiber.join(read)).reasonCode, "context_changed");
      assert.deepEqual(calls, ["thread/goal/get"]);
    }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect(
    "lazily resumes a stopped imported Codex thread while preserving its head and item identity",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const scenario = "legacy-native-resume";
          const nativeThreadId = "native-legacy-conversation";
          const threadId = ThreadId.make(`thread-${scenario}`);
          const now = yield* DateTime.now;
          const imported: OrchestrationV2ProviderThread = {
            id: ProviderThreadId.make("imported-provider-thread"),
            driver: CodexAdapterV2.CODEX_DRIVER_KIND,
            providerInstanceId: ProviderInstanceId.make("previous-codex-account"),
            providerSessionId: null,
            appThreadId: threadId,
            ownerNodeId: null,
            nativeThreadRef: {
              driver: CodexAdapterV2.CODEX_DRIVER_KIND,
              nativeId: nativeThreadId,
              strength: "strong",
            },
            nativeConversationHeadRef: {
              driver: CodexAdapterV2.CODEX_DRIVER_KIND,
              nativeId: "native-legacy-head",
              strength: "strong",
            },
            status: "not_loaded",
            firstRunOrdinal: null,
            lastRunOrdinal: null,
            handoffIds: [],
            forkedFrom: null,
            pendingBackgroundTasks: [],
            contextUsage: null,
            nativeMetadata: { itemIdentityVersion: 2 },
            createdAt: now,
            updatedAt: now,
          };
          const calls: string[] = [];
          const transcript = makeCodexReplayTranscript({
            scenario,
            entries: codexReplayPreamble({
              nativeThreadId,
              nativeTurnId: "unused",
              prompt: "unused",
            }).slice(0, 3),
          });
          const harness = yield* makeCodexReplayHarness(
            transcript,
            undefined,
            (method) =>
              Effect.sync(() => {
                calls.push(method);
              }),
            undefined,
            {
              initialProviderThread: imported,
              rawRequest: (method, params) =>
                Effect.sync(() => {
                  calls.push(method);
                  assert.equal(method, "thread/resume");
                  assert.isObject(params);
                  assert.equal((params as { readonly threadId: string }).threadId, nativeThreadId);
                  return { thread: { id: nativeThreadId, updatedAt: 1782622450 } };
                }),
            },
          );
          const resumed = yield* harness.runtime.resumeThread({ providerThread: imported });
          assert.deepEqual(calls, ["initialize", "thread/resume"]);
          assert.equal(resumed.id, imported.id);
          assert.equal(resumed.providerInstanceId, harness.runtime.instanceId);
          assert.deepEqual(resumed.nativeThreadRef, imported.nativeThreadRef);
          assert.deepEqual(resumed.nativeConversationHeadRef, imported.nativeConversationHeadRef);
          assert.deepEqual(resumed.nativeMetadata, imported.nativeMetadata);
          yield* awaitUntil(
            () => harness.events.some((event) => event.type === "runtime_identity.observed"),
            "resume identity",
          );
          const attestation = harness.events.find(
            (event) => event.type === "runtime_identity.observed",
          );
          if (attestation?.type !== "runtime_identity.observed")
            return assert.fail("Missing resume attestation");
          assert.equal(attestation.attestation.observed.model.status, "unknown");
          assert.equal(attestation.attestation.observed.backend.status, "unknown");
          assert.equal(attestation.attestation.observed.account.status, "unavailable");
        }),
      ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("initializes native source identity before the resume gate and native resume RPC", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "native-gated-resume";
        const seed = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "gated-resume-seed",
            entries: codexReplayPreamble({
              nativeThreadId,
              nativeTurnId: "unused",
              prompt: "unused",
            }).slice(0, 5),
          }),
        );
        const order: string[] = [];
        const nativeHome = "/synthetic/current-native-store";
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "gated-native-resume",
            entries: codexReplayPreamble({
              nativeThreadId,
              nativeTurnId: "unused",
              prompt: "unused",
              codexHome: nativeHome,
            }).slice(0, 3),
          }),
          undefined,
          (method) =>
            Effect.sync(() => {
              order.push(method);
            }),
          undefined,
          {
            initialProviderThread: seed.providerThread,
            runtimeGeneration: "new-native-resume-generation",
            rawRequest: (method, params) =>
              Effect.sync(() => {
                order.push(method);
                assert.equal(method, "thread/resume");
                assert.equal((params as { readonly threadId: string }).threadId, nativeThreadId);
                return { thread: { id: nativeThreadId, updatedAt: 1782622450 } };
              }),
          },
        );
        assert.isUndefined(harness.runtime.continuationSourceIdentity);
        const resumed = yield* harness.runtime.resumeThread({
          threadId: harness.threadId,
          providerThread: harness.providerThread,
          beforeNativeResume: (actual) =>
            Effect.sync(() => {
              order.push("gate");
              assert.deepEqual(actual, {
                driverKind: CodexAdapterV2.CODEX_DRIVER_KIND,
                continuationKey: `codex:home:${nativeHome}`,
                runtimeGeneration: "new-native-resume-generation",
              });
            }),
        });
        assert.deepEqual(order, ["initialize", "gate", "thread/resume"]);
        assert.equal(resumed.id, seed.providerThread.id);
        assert.deepEqual(resumed.nativeThreadRef, seed.providerThread.nativeThreadRef);
        assert.deepEqual(
          resumed.nativeConversationHeadRef,
          seed.providerThread.nativeConversationHeadRef,
        );
        assert.deepEqual(resumed.nativeMetadata, seed.providerThread.nativeMetadata);
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  for (const fixture of ["missing-source", "mismatched-source", "initialized-mismatch"] as const) {
    it.effect(`keeps a rejected ${fixture} resume gate unknown and skips native resume`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const nativeThreadId = "native-rejected-resume-gate";
          const seed = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({
              scenario: `rejected-resume-seed-${fixture}`,
              entries: codexReplayPreamble({
                nativeThreadId,
                nativeTurnId: "unused",
                prompt: "unused",
              }).slice(0, 5),
            }),
          );
          const order: string[] = [];
          const initialized = fixture === "initialized-mismatch";
          const nativeHome = "/synthetic/actual-native-store";
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({
              scenario: `rejected-resume-${fixture}`,
              entries: codexReplayPreamble({
                nativeThreadId,
                nativeTurnId: "unused",
                prompt: "unused",
                codexHome: nativeHome,
              }).slice(0, initialized ? 5 : 3),
            }),
            undefined,
            (method) =>
              Effect.sync(() => {
                order.push(method);
              }),
            undefined,
            {
              ...(initialized ? {} : { initialProviderThread: seed.providerThread }),
              ...(fixture !== "missing-source"
                ? {}
                : {
                    continuationHomeLayout: {
                      mode: "authOverlay",
                      sharedHomePath: "/synthetic/shared-store",
                      effectiveHomePath: "/synthetic/expected-overlay",
                      continuationKey: "codex:home:/synthetic/shared-store",
                    },
                  }),
              rawRequest: (method) =>
                Effect.sync(() => {
                  order.push(method);
                  return { thread: { id: nativeThreadId, updatedAt: 1782622450 } };
                }),
            },
          );
          const result = yield* harness.runtime
            .resumeThread({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              nativeOperation: {
                operationId: `resume-gate-${fixture}`,
                operation: "resume_thread",
                instanceId: harness.runtime.instanceId,
                threadId: harness.threadId,
                providerSessionId: harness.runtime.providerSessionId,
                providerThreadId: harness.providerThread.id,
                runtimeGeneration: harness.runtime.runtimeGeneration,
              },
              beforeNativeResume: (actual) =>
                Effect.gen(function* () {
                  order.push("gate");
                  if (fixture === "missing-source") assert.isUndefined(actual);
                  else {
                    assert.isDefined(actual);
                    assert.notEqual(
                      actual!.continuationKey,
                      "codex:home:/synthetic/historical-store",
                    );
                    assert.equal(actual!.runtimeGeneration, harness.runtime.runtimeGeneration);
                  }
                  return yield* Effect.fail(
                    new ProviderAdapterProtocolError({
                      driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                      detail: `Synthetic ${fixture} continuation rejection`,
                    }),
                  );
                }),
            })
            .pipe(Effect.result);
          if (
            result._tag !== "Failure" ||
            !Schema.is(ProviderAdapterResumeThreadError)(result.failure)
          )
            return assert.fail("Expected a rejected native resume gate");
          assert.equal(result.failure.nativeEffect?.outcome, "unknown");
          assert.equal(result.failure.nativeEffect?.operationId, `resume-gate-${fixture}`);
          assert.equal(
            result.failure.nativeEffect?.runtimeGeneration,
            harness.runtime.runtimeGeneration,
          );
          assert.deepEqual(
            order,
            initialized ? ["initialize", "thread/start", "gate"] : ["initialize", "gate"],
          );
        }),
      ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    );
  }

  for (const interrupted of [false, true]) {
    it.effect(
      `discards provisional identity after a binding ${interrupted ? "interruption" : "failure"}`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const nativeThreadId = "native-provisional-cleanup";
            const preamble = codexReplayPreamble({
              nativeThreadId,
              nativeTurnId: "unused",
              prompt: "unused",
            });
            const seed = yield* makeCodexReplayHarness(
              makeCodexReplayTranscript({
                scenario: `provisional-seed-${interrupted}`,
                entries: preamble.slice(0, 5),
              }),
            );
            const requestStarted = yield* Deferred.make<void>();
            const notificationHandled = yield* Deferred.make<void>();
            let resumeCalls = 0;
            const harness = yield* makeCodexReplayHarness(
              makeCodexReplayTranscript({
                scenario: `provisional-cleanup-${interrupted}`,
                entries: [
                  ...preamble.slice(0, 3),
                  {
                    type: "emit_inbound",
                    label: "provisional-reroute",
                    frame: {
                      method: "model/rerouted",
                      params: {
                        threadId: nativeThreadId,
                        turnId: "unused",
                        fromModel: "gpt-5.4",
                        toModel: "discarded-native-model",
                        reason: "highRiskCyberActivity",
                      },
                    },
                  },
                ],
              }),
              undefined,
              undefined,
              undefined,
              {
                initialProviderThread: seed.providerThread,
                beforeEmitInbound: (entry) =>
                  entry.label === "provisional-reroute"
                    ? Deferred.await(requestStarted)
                    : Effect.void,
                afterNotification: (method) =>
                  method === "model/rerouted"
                    ? Deferred.succeed(notificationHandled, undefined).pipe(Effect.asVoid)
                    : Effect.void,
                rawRequest: (method) =>
                  Effect.gen(function* () {
                    assert.equal(method, "thread/resume");
                    resumeCalls++;
                    if (resumeCalls === 1) {
                      yield* Deferred.succeed(requestStarted, undefined);
                      yield* Deferred.await(notificationHandled);
                      if (interrupted) return yield* Effect.never;
                      return yield* Effect.fail(
                        new CodexErrors.CodexAppServerRequestError({
                          code: -32000,
                          errorMessage: "Binding response failed after dispatch",
                        }),
                      );
                    }
                    return {
                      thread: { id: nativeThreadId, updatedAt: 1782622450 },
                      model: "native-resumed-model",
                      modelProvider: "openai",
                    };
                  }),
              },
            );
            const first = yield* harness.runtime
              .resumeThread({
                threadId: harness.threadId,
                providerThread: harness.providerThread,
              })
              .pipe(Effect.forkChild);
            yield* Deferred.await(notificationHandled);
            if (interrupted) yield* Fiber.interrupt(first);
            else assert.equal((yield* Fiber.join(first).pipe(Effect.result))._tag, "Failure");
            yield* harness.runtime.resumeThread({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
            });
            yield* awaitUntil(
              () => harness.events.some((event) => event.type === "runtime_identity.observed"),
              "identity after binding cleanup",
            );
            const identities = harness.events.filter(
              (event) => event.type === "runtime_identity.observed",
            );
            assert.equal(identities.length, 1);
            assert.deepEqual(identities[0]!.attestation.observed.model, {
              status: "observed",
              value: "native-resumed-model",
              sourceEvent: "thread/resume",
            });
            assert.equal(resumeCalls, 2);
          }),
        ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    );
  }

  it.effect("holds mismatched and malformed native resume replies as unknown effects", () =>
    Effect.scoped(
      Effect.gen(function* () {
        for (const response of [
          { thread: { id: "different-native-thread", updatedAt: 1782622450 } },
          {},
        ]) {
          const nativeThreadId = "native-resume-bound";
          const transcript = makeCodexReplayTranscript({
            scenario: "resume-unknown",
            entries: codexReplayPreamble({
              nativeThreadId,
              nativeTurnId: "unused",
              prompt: "unused",
            }).slice(0, 5),
          });
          const harness = yield* makeCodexReplayHarness(
            transcript,
            undefined,
            undefined,
            undefined,
            {
              rawRequest: (method) =>
                Effect.sync(() => {
                  assert.equal(method, "thread/resume");
                  return response;
                }),
            },
          );
          const operation = {
            ...runtimeBinding(harness),
            operationId: "resume-unknown-operation",
            operation: "resume_thread" as const,
          };
          const result = yield* harness.runtime
            .resumeThread({ providerThread: harness.providerThread, nativeOperation: operation })
            .pipe(Effect.result);
          if (
            result._tag !== "Failure" ||
            !Schema.is(ProviderAdapterResumeThreadError)(result.failure)
          )
            return assert.fail("Expected a native resume failure");
          assert.equal(result.failure.nativeEffect?.outcome, "unknown");
          assert.equal(result.failure.nativeEffect?.operationId, operation.operationId);
          assert.equal(
            result.failure.nativeEffect?.runtimeGeneration,
            harness.runtime.runtimeGeneration,
          );
          assert.equal(harness.providerThread.nativeThreadRef?.nativeId, nativeThreadId);
        }
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("does not let a safe resume rejection erase earlier initialization effects", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "native-safe-rejection";
        const preamble = codexReplayPreamble({
          nativeThreadId,
          nativeTurnId: "unused",
          prompt: "unused",
        });
        const reject = () =>
          Effect.fail(
            new CodexErrors.CodexAppServerRequestError({
              code: -32601,
              errorMessage: "Unsupported resume",
            }),
          );
        const initialized = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "safe-rejection-initialized",
            entries: preamble.slice(0, 5),
          }),
          undefined,
          undefined,
          undefined,
          { rawRequest: reject },
        );
        const uninitialized = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "safe-rejection-uninitialized",
            entries: preamble.slice(0, 3),
          }),
          undefined,
          undefined,
          undefined,
          {
            initialProviderThread: initialized.providerThread,
            rawRequest: reject,
          },
        );
        for (const fixture of [
          { harness: initialized, expected: "known_no_effect" },
          { harness: uninitialized, expected: "unknown" },
        ] as const) {
          const result = yield* fixture.harness.runtime
            .resumeThread({
              threadId: fixture.harness.threadId,
              providerThread: fixture.harness.providerThread,
              nativeOperation: {
                operationId: `resume-${fixture.expected}`,
                operation: "resume_thread",
                instanceId: fixture.harness.runtime.instanceId,
                threadId: fixture.harness.threadId,
                providerSessionId: fixture.harness.runtime.providerSessionId,
                providerThreadId: fixture.harness.providerThread.id,
                runtimeGeneration: fixture.harness.runtime.runtimeGeneration,
              },
            })
            .pipe(Effect.result);
          if (
            result._tag !== "Failure" ||
            !Schema.is(ProviderAdapterResumeThreadError)(result.failure)
          )
            return assert.fail("Expected a rejected native resume");
          assert.equal(result.failure.nativeEffect?.outcome, fixture.expected);
        }
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  for (const nativeHead of ["absent", "uncorrelated", "unrelated"] as const) {
    it.effect(
      `keeps a failed start reply with an ${nativeHead} native head unknown without another dispatch`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const nativeThreadId = "native-lost-start-reply";
            const nativeTurnId = "native-lost-start-turn";
            const confirmation = yield* Deferred.make<void>();
            const preamble = codexReplayPreamble({
              nativeThreadId,
              nativeTurnId,
              prompt: "Start once",
            });
            const transcript = makeCodexReplayTranscript({
              scenario: `lost-start-${nativeHead}`,
              entries: [
                ...preamble.slice(0, 6),
                ...(nativeHead === "absent"
                  ? []
                  : [
                      {
                        type: "emit_inbound" as const,
                        label: "uncorrelated-native-head",
                        frame: {
                          method: "turn/started",
                          params: {
                            threadId: nativeThreadId,
                            turn: {
                              id: nativeHead === "unrelated" ? "prior-native-turn" : nativeTurnId,
                              items: [],
                              status: "inProgress",
                              startedAt: 1782622440,
                            },
                          },
                        },
                      },
                    ]),
                {
                  type: "emit_inbound",
                  label: "lost-reply",
                  frame: {
                    id: 3,
                    error: { code: -32000, message: "Reply lost after native dispatch" },
                  },
                },
              ],
            });
            const requests: string[] = [];
            const harness = yield* makeCodexReplayHarness(
              transcript,
              undefined,
              (method) =>
                Effect.sync(() => {
                  requests.push(method);
                }),
              undefined,
              {
                beforeEmitInbound: (entry) =>
                  nativeHead !== "absent" && entry.label === "lost-reply"
                    ? Deferred.await(confirmation)
                    : Effect.void,
                afterNotification: (method) =>
                  method === "turn/started"
                    ? Deferred.succeed(confirmation, undefined).pipe(Effect.asVoid)
                    : Effect.void,
              },
            );
            const turnInput = makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make(`lost-start-attempt-${nativeHead}`),
              text: "Start once",
            });
            const result = yield* harness.runtime
              .startTurn({
                ...turnInput,
                nativeOperation: {
                  ...runtimeBinding(harness),
                  operationId: "lost-start-operation",
                  operation: "start_turn",
                  attemptId: turnInput.attemptId,
                },
              })
              .pipe(Effect.result);
            assert.deepEqual(requests, ["initialize", "thread/start", "turn/start"]);
            if (
              result._tag !== "Failure" ||
              !Schema.is(ProviderAdapterTurnStartError)(result.failure)
            )
              return assert.fail("Expected an unconfirmed native start failure");
            assert.equal(result.failure.nativeEffect?.outcome, "unknown");
            assert.equal(result.failure.nativeEffect?.operationId, "lost-start-operation");
            assert.equal(result.failure.nativeEffect?.attemptId, turnInput.attemptId);
          }),
        ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    );
  }

  for (const response of ["supported", "unsupported", "invalid"] as const) {
    it.effect(`delivers native history with ${response} app-server protocol`, () =>
      Effect.gen(function* () {
        const nativeThreadId = `inject-${response}`;
        const prompt = "Only the current request";
        const history: ProviderAdapterV2HistoricalContext = {
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
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    );
  }

  it.effect("identifies sessions to Codex with the same client info as main", () =>
    Effect.gen(function* () {
      const transcript = makeCodexReplayTranscript({
        scenario: "initialize-client-info",
        entries: codexReplayPreamble({
          nativeThreadId: "client-info-thread",
          nativeTurnId: "unused",
          prompt: "unused",
        }).slice(0, 5),
      });
      const initializeParams: Array<unknown> = [];
      yield* makeCodexReplayHarness(
        transcript,
        () => Effect.void,
        (method, params) =>
          Effect.sync(() => {
            if (method === "initialize") initializeParams.push(params);
          }),
      );
      // Codex uses clientInfo.name as the request originator. Replays ignore the
      // version, so pin the whole value here.
      assert.deepEqual(initializeParams, [
        {
          clientInfo: { name: "T3 Code", title: "T3 Code", version: packageJson.version },
          capabilities: {
            experimentalApi: true,
            optOutNotificationMethods: ["turn/diff/updated"],
          },
        },
      ]);
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("unsubscribes from the native thread when it is unloaded", () =>
    Effect.gen(function* () {
      const nativeThreadId = "unload-thread";
      const preamble = codexReplayPreamble({
        nativeThreadId,
        nativeTurnId: "unused",
        prompt: "unused",
      });
      const transcript = makeCodexReplayTranscript({
        scenario: "unload-thread",
        entries: [
          // initialize + thread/start only; no turn runs.
          ...preamble.slice(0, 5),
          {
            type: "expect_outbound",
            label: "thread/unsubscribe",
            frame: { id: 3, method: "thread/unsubscribe", params: { threadId: nativeThreadId } },
          },
          // Response shape recorded from codex app-server 0.156.1.
          {
            type: "emit_inbound",
            label: "thread/unsubscribe",
            frame: { id: 3, result: { status: "unsubscribed" } },
          },
        ],
      });
      const requests: Array<string> = [];
      const harness = yield* makeCodexReplayHarness(
        transcript,
        () => Effect.void,
        (method) => Effect.sync(() => requests.push(method)),
      );
      assert.isDefined(harness.runtime.unloadThread);
      yield* harness.runtime.unloadThread!({ providerThread: harness.providerThread });
      assert.deepEqual(requests, ["initialize", "thread/start", "thread/unsubscribe"]);
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("keeps the app-server failure as the cause when an unload is rejected", () =>
    Effect.gen(function* () {
      const nativeThreadId = "unload-thread-rejected";
      const preamble = codexReplayPreamble({
        nativeThreadId,
        nativeTurnId: "unused",
        prompt: "unused",
      });
      const transcript = makeCodexReplayTranscript({
        scenario: "unload-thread-rejected",
        entries: [
          ...preamble.slice(0, 5),
          {
            type: "expect_outbound",
            label: "thread/unsubscribe",
            frame: { id: 3, method: "thread/unsubscribe", params: { threadId: nativeThreadId } },
          },
          {
            type: "emit_inbound",
            label: "thread/unsubscribe",
            frame: { id: 3, error: { code: -32600, message: "invalid thread id" } },
          },
        ],
      });
      const harness = yield* makeCodexReplayHarness(transcript);
      const error = yield* harness.runtime.unloadThread!({
        providerThread: harness.providerThread,
      }).pipe(Effect.flip);
      assert.equal(error._tag, "ProviderAdapterProtocolError");
      const cause = error._tag === "ProviderAdapterProtocolError" ? error.cause : undefined;
      assert.equal((cause as { _tag?: string } | undefined)?._tag, "CodexAppServerRequestError");
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
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
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("settles Stop when a queued native turn fails before starting", () =>
    Effect.gen(function* () {
      const nativeThreadId = "early-stop-thread";
      const nativeTurnId = "early-stop-turn";
      const prompt = "Run a command.";
      const preamble = codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt });
      const transcript = makeCodexReplayTranscript({
        scenario: "early-stop-failed-before-native-start",
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
            label: "turn/failed",
            afterMs: 1000,
            frame: {
              method: "turn/completed",
              params: {
                threadId: nativeThreadId,
                turn: {
                  ...makeCodexReplayTurn({ id: nativeTurnId, status: "failed" }),
                  startedAt: null,
                  error: {
                    message: "Failed before native start",
                    codexErrorInfo: null,
                    additionalDetails: null,
                  },
                },
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
      assert.equal(harness.terminalEvents()[0]?.status, "failed");
      assert.lengthOf(harness.terminalEvents(), 1);
      assert.isFalse(interruptSent, "A terminal native turn must not receive turn/interrupt");
      assert.isFalse(yield* harness.hasPendingBackgroundWork);
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("bounds Stop when a queued native turn never starts", () =>
    Effect.gen(function* () {
      const nativeThreadId = "early-stop-thread";
      const nativeTurnId = "early-stop-turn";
      const prompt = "Run a command.";
      const preamble = codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt });
      const transcript = makeCodexReplayTranscript({
        scenario: "early-stop-never-starts",
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
        .pipe(Effect.exit, Effect.forkScoped);
      yield* TestClock.adjust("10 seconds");
      assert.equal((yield* Fiber.join(interrupt))._tag, "Failure");
      yield* harness.firstTerminal;
      assert.equal(harness.terminalEvents()[0]?.status, "interrupted");
      assert.lengthOf(harness.terminalEvents(), 1);
      assert.isFalse(interruptSent, "An unstarted native turn must not receive turn/interrupt");
      assert.isFalse(yield* harness.hasPendingBackgroundWork);
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("sends currency-sigil skill mentions to Codex as $ mentions", () =>
    Effect.gen(function* () {
      const nativeThreadId = "skill-sigil-thread";
      const nativeTurnId = "skill-sigil-turn";
      const transcript = makeCodexReplayTranscript({
        scenario: "skill-sigil-canonicalized",
        entries: [
          ...codexReplayPreamble({
            nativeThreadId,
            nativeTurnId,
            prompt: "€review do it",
            sentPrompt: "$review do it",
          }),
          {
            type: "expect_outbound",
            label: "turn/steer",
            frame: {
              id: 4,
              method: "turn/steer",
              params: {
                expectedTurnId: nativeTurnId,
                input: [{ type: "text", text: "then $ship it" }],
                threadId: nativeThreadId,
              },
            },
          },
          {
            type: "emit_inbound",
            label: "turn/steer",
            frame: { id: 4, result: { turnId: nativeTurnId } },
          },
        ],
      });
      const harness = yield* makeCodexReplayHarness(transcript);
      const turnInput = makeCodexTestTurnInput({
        threadId: harness.threadId,
        providerThread: harness.providerThread,
        now: yield* DateTime.now,
        attemptId: RunAttemptId.make("skill-sigil-attempt"),
        text: "€review do it",
      });
      yield* harness.runtime.startTurn(turnInput);
      yield* harness.runtime.steerTurn({
        threadId: harness.threadId,
        runId: turnInput.runId,
        providerThread: harness.providerThread,
        providerTurnId: (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
          driver: CodexAdapterV2.CODEX_DRIVER_KIND,
          nativeTurnId,
        }),
        message: {
          ...turnInput.message,
          messageId: MessageId.make("message-skill-sigil-steer"),
          text: "then £ship it",
        },
      });
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  const assistantMessages = (events: ReadonlyArray<ProviderAdapterV2Event>) =>
    events.filter(
      (event): event is Extract<ProviderAdapterV2Event, { type: "message.updated" }> =>
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
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
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
          params.additionalContext?.t3_code_orchestration?.value ?? "",
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
        McpProviderSession.setMcpProviderSession({
          environmentId: EnvironmentId.make("test"),
          threadId: harness.threadId,
          providerSessionId: "context-session",
          providerInstanceId: ProviderInstanceId.make("codex"),
          endpoint: "http://127.0.0.1:43123/mcp",
          authorizationHeader: "Bearer test",
          capabilities: new Set<McpCapability>(["preview", "orchestration"]),
          browserToolsAvailable: true,
        });
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => McpProviderSession.clearMcpProviderSession(harness.threadId)),
        );
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
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  for (const order of ["reply-first", "notification-first"] as const) {
    it.effect(
      order === "reply-first"
        ? "compacts Codex with the native RPC and completes the compaction turn"
        : "joins an early compaction start to the captured request acknowledgement",
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const nativeThreadId = "compact-thread";
            const nativeTurnId = "compact-turn";
            const item = { type: "contextCompaction", id: "compact-item" };
            const acknowledgement: CodexReplay.CodexAppServerReplayEntry = {
              type: "emit_inbound",
              label: "compact",
              frame: { id: 3, result: {} },
            };
            const nativeStart: CodexReplay.CodexAppServerReplayEntry = {
              type: "emit_inbound",
              label: "start",
              frame: {
                method: "turn/started",
                params: {
                  threadId: nativeThreadId,
                  turn: makeCodexReplayTurn({ id: nativeTurnId, status: "inProgress" }),
                },
              },
            };
            const transcript = makeCodexReplayTranscript({
              scenario: `native-compaction-${order}`,
              entries: [
                ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "unused" }).slice(
                  0,
                  5,
                ),
                {
                  type: "expect_outbound",
                  label: "compact",
                  frame: {
                    id: 3,
                    method: "thread/compact/start",
                    params: { threadId: nativeThreadId },
                  },
                },
                ...(order === "reply-first"
                  ? [acknowledgement, nativeStart]
                  : [nativeStart, acknowledgement]),
                ...(["item/started", "item/completed"] as const).map((method) => ({
                  type: "emit_inbound" as const,
                  label: method,
                  frame: {
                    method,
                    params: { threadId: nativeThreadId, turnId: nativeTurnId, item },
                  },
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
            const terminal = harness.terminalEvents()[0]!;
            const origin = readProviderEventOrigin(terminal)!;
            assert.isDefined(origin);
            assert.deepEqual(origin.turn, {
              binding: runtimeBinding(harness),
              runId: RunId.make("run-compact-attempt"),
              attemptId: RunAttemptId.make("compact-attempt"),
              providerTurnId: terminal.providerTurnId,
            });
            assert.equal(
              (yield* origin.producer.revalidateCurrent.pipe(Effect.result))._tag,
              "Success",
            );
          }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
        ),
    );
  }

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
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  it.effect("continues an interrupted native thread with empty input and reasoning summaries", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const scenario = "codex-restart-promptless";
        const nativeThreadId = "native-restart-promptless";
        const nativeTurnId = "turn-restart-promptless";
        const preamble = codexReplayPreamble({
          nativeThreadId,
          nativeTurnId,
          prompt: "unused",
        }).slice(0, 5);
        const transcript = makeCodexReplayTranscript({
          scenario,
          entries: [
            ...preamble,
            {
              type: "expect_outbound",
              label: "resume",
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
              label: "resume",
              frame: { id: 3, result: { thread: { id: nativeThreadId, updatedAt: 1782622450 } } },
            },
            {
              type: "expect_outbound",
              label: "continue",
              frame: {
                id: 4,
                method: "turn/start",
                params: {
                  threadId: nativeThreadId,
                  input: [],
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
              label: "continue",
              frame: {
                id: 4,
                result: { turn: makeCodexReplayTurn({ id: nativeTurnId, status: "inProgress" }) },
              },
            },
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        const resumed = yield* harness.runtime.resumeThread({
          providerThread: harness.providerThread,
          modelSelection: CODEX_TEST_MODEL_SELECTION,
          runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
        });
        yield* harness.runtime.startTurn({
          ...makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: resumed,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("attempt-restart-promptless"),
            text: "Continue where you left off.",
          }),
          restartContinuationOfRunId: RunId.make("run-before-restart"),
        });
        assert.equal(resumed.nativeThreadRef?.nativeId, nativeThreadId);
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
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
        yield* awaitUntil(() => harness.terminalEvents().length === 1, "Codex retry recovery");

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
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  it.effect("stamps Codex items with their own start time, not the turn's", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const scenario = "codex-item-start-times";
        const nativeThreadId = `native-${scenario}-thread`;
        const nativeTurnId = `native-${scenario}-turn`;
        const commandLifecycle = (id: string, startedAtMs: number) =>
          (["started", "completed"] as const).map((phase) => ({
            type: "emit_inbound" as const,
            label: `item/${phase}/${id}`,
            frame: {
              method: `item/${phase}`,
              params: {
                threadId: nativeThreadId,
                turnId: nativeTurnId,
                ...(phase === "started" ? { startedAtMs } : { completedAtMs: startedAtMs + 10 }),
                item: {
                  type: "commandExecution",
                  id,
                  command: "pwd",
                  cwd: "/workspace",
                  processId: "42",
                  source: "unifiedExecStartup",
                  status: phase === "started" ? "inProgress" : "completed",
                  commandActions: [{ type: "unknown", command: "pwd" }],
                  aggregatedOutput: phase === "started" ? null : "/workspace\n",
                  exitCode: phase === "started" ? null : 0,
                  durationMs: phase === "started" ? null : 10,
                },
              },
            },
          }));
        const transcript = makeCodexReplayTranscript({
          scenario,
          entries: [
            ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Run two commands." }),
            ...commandLifecycle("first-command", 1782622445000),
            ...commandLifecycle("second-command", 1782622505000),
            ...(["started", "completed"] as const).map((phase) => ({
              type: "emit_inbound" as const,
              label: `item/${phase}/compaction`,
              frame: {
                method: `item/${phase}`,
                params: {
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  ...(phase === "started"
                    ? { startedAtMs: 1782622565000 }
                    : { completedAtMs: 1782622575000 }),
                  item: { type: "contextCompaction", id: "compaction" },
                },
              },
            })),
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
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("attempt-codex-item-start-times"),
            text: "Run two commands.",
          }),
        );
        yield* awaitUntil(() => harness.terminalEvents().length === 1, "Codex item start times");

        const startedAtByItem = harness.events.flatMap((event) =>
          event.type === "turn_item.updated" &&
          (event.turnItem.type === "command_execution" || event.turnItem.type === "compaction")
            ? [[event.turnItem.nativeItemRef?.nativeId, event.turnItem.startedAt] as const]
            : [],
        );
        assert.deepEqual(
          startedAtByItem.map(([id, startedAt]) => [id, startedAt?.epochMilliseconds]),
          [
            ["first-command", 1782622445000],
            ["first-command", 1782622445000],
            ["second-command", 1782622505000],
            ["second-command", 1782622505000],
            ["compaction", 1782622565000],
            ["compaction", 1782622565000],
          ],
        );
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  for (const terminalStatus of ["completed", "interrupted"] as const) {
    it.effect(`retains Codex reasoning parts when the turn is ${terminalStatus}`, () =>
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
        }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
      ),
    );
  }

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
        yield* awaitUntil(() => harness.terminalEvents().length === 1, "root turn terminal");

        assert.deepEqual(
          assistantMessages(harness.events).map((event) => event.message.text),
          ["CODEX_RECOVERY_OK"],
        );
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  it.effect("suppresses a later streamed duplicate final answer", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const transcript = finalAnswerTranscript("codex-streamed-duplicate-final", [
          { id: "answer-original", text: "CODEX_RECOVERY_OK" },
          {
            id: "answer-duplicate",
            text: "CODEX_RECOVERY_OK",
            streamed: true,
            completionDelayMs: 100,
          },
        ]);
        const harness = yield* makeCodexReplayHarness(transcript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-streamed-duplicate-final"),
            text: "Reply with the requested recovery marker.",
          }),
        );
        yield* awaitUntil(() => assistantMessages(harness.events).length === 1, "original answer");
        yield* Effect.yieldNow;
        yield* TestClock.adjust("50 millis");
        yield* Effect.yieldNow;

        assert.deepEqual(
          assistantMessages(harness.events).map((event) => event.message.text),
          ["CODEX_RECOVERY_OK"],
        );

        yield* TestClock.adjust("50 millis");
        yield* awaitUntil(() => harness.terminalEvents().length === 1, "root turn terminal");
        assert.deepEqual(
          assistantMessages(harness.events).map((event) => event.message.text),
          ["CODEX_RECOVERY_OK"],
        );
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  it.effect("buffers an overlapping later final stream until duplicate detection", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const scenario = "codex-overlapping-duplicate-final";
        const nativeThreadId = `native-${scenario}-thread`;
        const nativeTurnId = `native-${scenario}-turn`;
        const answerItem = (id: string, text: string) => ({
          type: "agentMessage" as const,
          id,
          text,
          phase: "final_answer" as const,
          memoryCitation: null,
        });
        const transcript = makeCodexReplayTranscript({
          scenario,
          entries: [
            ...codexReplayPreamble({
              nativeThreadId,
              nativeTurnId,
              prompt: "Reply with the requested recovery marker.",
            }),
            ...["answer-overlap-original", "answer-overlap-duplicate"].flatMap(
              (itemId, index): ReadonlyArray<CodexReplay.CodexAppServerReplayEntry> => [
                {
                  type: "emit_inbound",
                  label: `item/started/${itemId}`,
                  frame: {
                    method: "item/started",
                    params: {
                      item: answerItem(itemId, ""),
                      threadId: nativeThreadId,
                      turnId: nativeTurnId,
                      startedAtMs: 1782622440500 + index,
                    },
                  },
                },
                {
                  type: "emit_inbound",
                  label: `item/agentMessage/delta/${itemId}`,
                  frame: {
                    method: "item/agentMessage/delta",
                    params: {
                      threadId: nativeThreadId,
                      turnId: nativeTurnId,
                      itemId,
                      delta: "CODEX_RECOVERY_OK",
                    },
                  },
                },
              ],
            ),
            {
              type: "emit_inbound",
              label: "item/completed/answer-overlap-original",
              afterMs: 100,
              frame: {
                method: "item/completed",
                params: {
                  item: answerItem("answer-overlap-original", "CODEX_RECOVERY_OK"),
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  completedAtMs: 1782622441000,
                },
              },
            },
            {
              type: "emit_inbound",
              label: "item/completed/answer-overlap-duplicate",
              frame: {
                method: "item/completed",
                params: {
                  item: answerItem("answer-overlap-duplicate", "CODEX_RECOVERY_OK"),
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  completedAtMs: 1782622441001,
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
            attemptId: RunAttemptId.make("attempt-codex-overlapping-duplicate-final"),
            text: "Reply with the requested recovery marker.",
          }),
        );
        yield* Effect.yieldNow;
        yield* TestClock.adjust("50 millis");
        yield* Effect.yieldNow;

        assert.equal(
          new Set(assistantMessages(harness.events).map((event) => event.message.id)).size,
          1,
        );

        yield* TestClock.adjust("50 millis");
        yield* awaitUntil(() => harness.terminalEvents().length === 1, "root turn terminal");
        assert.equal(
          new Set(assistantMessages(harness.events).map((event) => event.message.id)).size,
          1,
        );
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  it.effect("preserves a sole empty final answer", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const transcript = finalAnswerTranscript("codex-sole-empty-final", [
          { id: "answer-empty", text: "" },
        ]);
        const harness = yield* makeCodexReplayHarness(transcript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-sole-empty-final"),
            text: "Reply with the requested recovery marker.",
          }),
        );
        yield* awaitUntil(() => harness.terminalEvents().length === 1, "root turn terminal");

        assert.deepEqual(
          assistantMessages(harness.events).map((event) => event.message.text),
          [""],
        );
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  it.effect("suppresses a second empty final answer", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const transcript = finalAnswerTranscript("codex-duplicate-empty-final", [
          { id: "answer-empty-original", text: "" },
          { id: "answer-empty-duplicate", text: "" },
        ]);
        const harness = yield* makeCodexReplayHarness(transcript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-duplicate-empty-final"),
            text: "Reply with the requested recovery marker.",
          }),
        );
        yield* awaitUntil(() => harness.terminalEvents().length === 1, "root turn terminal");

        assert.deepEqual(
          assistantMessages(harness.events).map((event) => event.message.text),
          [""],
        );
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  it.effect("preserves an empty final answer when only commentary preceded it", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const transcript = finalAnswerTranscript("codex-commentary-then-empty-final", [
          { id: "answer-commentary", text: "Working on it.", phase: "commentary" },
          { id: "answer-empty", text: "" },
        ]);
        const harness = yield* makeCodexReplayHarness(transcript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-commentary-then-empty-final"),
            text: "Reply with the requested recovery marker.",
          }),
        );
        yield* awaitUntil(() => harness.terminalEvents().length === 1, "root turn terminal");

        assert.deepEqual(
          assistantMessages(harness.events).map((event) => event.message.text),
          ["Working on it.", ""],
        );
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  it.effect("suppresses an empty final answer after a non-empty unknown-phase answer", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const transcript = finalAnswerTranscript("codex-unknown-non-empty-then-empty-final", [
          { id: "answer-non-empty", text: "CODEX_RECOVERY_OK", phase: null },
          { id: "answer-empty", text: "" },
        ]);
        const harness = yield* makeCodexReplayHarness(transcript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-unknown-non-empty-then-empty-final"),
            text: "Reply with the requested recovery marker.",
          }),
        );
        yield* awaitUntil(() => harness.terminalEvents().length === 1, "root turn terminal");

        assert.deepEqual(
          assistantMessages(harness.events).map((event) => event.message.text),
          ["CODEX_RECOVERY_OK"],
        );
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  it.effect("suppresses a trailing empty answer with an omitted phase", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const transcript = finalAnswerTranscript("codex-final-then-empty-unknown", [
          { id: "answer-non-empty", text: "CODEX_RECOVERY_OK" },
          { id: "answer-empty", text: "", omitPhase: true },
        ]);
        const harness = yield* makeCodexReplayHarness(transcript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-final-then-empty-unknown"),
            text: "Reply with the requested recovery marker.",
          }),
        );
        yield* awaitUntil(() => harness.terminalEvents().length === 1, "root turn terminal");

        assert.deepEqual(
          assistantMessages(harness.events).map((event) => event.message.text),
          ["CODEX_RECOVERY_OK"],
        );
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  it.effect("keeps a later non-empty final answer after an initial empty final answer", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const transcript = finalAnswerTranscript("codex-empty-then-non-empty-final", [
          { id: "answer-empty", text: "" },
          { id: "answer-non-empty", text: "CODEX_RECOVERY_OK" },
        ]);
        const harness = yield* makeCodexReplayHarness(transcript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-empty-then-non-empty-final"),
            text: "Reply with the requested recovery marker.",
          }),
        );
        yield* awaitUntil(() => harness.terminalEvents().length === 1, "root turn terminal");

        assert.deepEqual(
          assistantMessages(harness.events).map((event) => event.message.text),
          ["", "CODEX_RECOVERY_OK"],
        );
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
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

  it.effect(
    "projects a post-settle background command completion and requests a continuation",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeCodexReplayHarness(backgroundExecTranscript);
          const now = yield* DateTime.now;

          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now,
              attemptId: RunAttemptId.make("attempt-codex-bg-wake"),
              text: BG_PROMPT,
            }),
          );
          yield* awaitUntil(() => harness.terminalEvents().length === 1, "root turn terminal");
          assert.equal(harness.terminalEvents()[0]?.status, "completed");
          assert.isTrue(yield* harness.hasPendingBackgroundWork);
          assert.isTrue(
            yield* harness.runtime.hasPendingBackgroundWorkForThread!(harness.providerThread),
          );
          assert.lengthOf(harness.continuationRequests, 0);
          const terminalIndex = harness.events.findIndex((event) => event.type === "turn.terminal");

          yield* TestClock.adjust("30 seconds");
          yield* awaitUntil(
            () => harness.continuationRequests.length === 1,
            "continuation request",
          );
          const request = harness.continuationRequests[0];
          assert.equal(request?.threadId, harness.threadId);
          assert.equal(request?.providerThreadId, harness.providerThread.id);
          assert.equal(request?.driver, CodexAdapterV2.CODEX_DRIVER_KIND);
          assert.deepEqual(request?.notification, {
            source: { kind: "command" },
            outcome: "completed",
            summary: `Command "${BG_COMMAND}" finished (exit 0)`,
            detail: BG_COMMAND,
          });
          assert.equal(
            request?.detail,
            `Background command completed (exit 0): ${BG_COMMAND}\n\n` +
              "Output tail:\nCODEX_BG_WAKE_DONE",
          );

          const lateCommandUpdateIndex = () =>
            harness.events.findIndex(
              (event, index) =>
                index > terminalIndex &&
                event.type === "turn_item.updated" &&
                event.turnItem.type === "command_execution" &&
                event.turnItem.status === "completed" &&
                event.turnItem.output === "CODEX_BG_WAKE_DONE\n" &&
                event.turnItem.exitCode === 0,
            );
          yield* awaitUntil(
            () => lateCommandUpdateIndex() > terminalIndex,
            "post-settle command projection",
          );
          assert.lengthOf(harness.terminalEvents(), 1);
          assert.isFalse(yield* harness.hasPendingBackgroundWork);
        }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
      ),
  );

  for (const terminated of [true, false, "still_running"] as const) {
    const stillRunning = terminated === "still_running";
    const transcript = makeCodexReplayTranscript({
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
          frame: { id: 4, result: { terminated: terminated === true } },
        },
        ...(terminated !== true
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

    it.effect(`stops a command after root completion when termination returns ${terminated}`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const stopped = yield* Deferred.make<void>();
          const harness = yield* makeCodexReplayHarness(
            transcript,
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "command_execution" &&
              event.turnItem.status === "interrupted"
                ? Deferred.succeed(stopped, undefined)
                : Effect.void,
            undefined,
            undefined,
            {
              goalResponses: new Map([[BG_NATIVE_THREAD, { goal: null }]]),
            },
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
        }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
      ),
    );
    if (terminated === true) {
      it.effect("interrupts a completed run's background command through orchestration", () =>
        Effect.scoped(
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
              Number(
                yield* spawner.exitCode(ChildProcess.make("git", ["init", "--quiet"], { cwd })),
              ),
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
              yield* seedCodexReplayProject(
                ProjectId.make("project:background-stop"),
                threadId,
                cwd,
              );
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
              const firstDrain = yield* worker
                .drain()
                .pipe(Effect.forkChild({ startImmediately: true }));
              yield* Fiber.join(firstDrain);
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
                type: "run.interrupt",
                commandId: CommandId.make("stop-background-command"),
                threadId,
                runId: run.id,
              });
              yield* worker.drain();
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
                makeOrchestratorV2ReplayLayerWithRegistry(
                  { name: "codex-background-stop", runtimePolicyOverride: { cwd } },
                  makeCodexProviderAdapterRegistryReplayLayer({
                    transcript: localTranscript,
                    driver: replayDriver,
                    goalResponses: new Map([[BG_NATIVE_THREAD, { goal: null }]]),
                  }),
                  { runEffectWorker: false },
                ),
              ),
            );
          }).pipe(Effect.provide(NodeServices.layer)),
        ),
      );
    }
  }

  // The app-server exits after the root turn, before the command's own
  // item/completed (Codex always sends one, so only a lost notification or a
  // gone process leaves it running). Nothing tracks the command any more, yet
  // the thread still shows it, and Stop is the only way to clear it.
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
          yield* seedCodexReplayProject(
            ProjectId.make("project:background-stop-untracked"),
            threadId,
            cwd,
          );
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
            makeOrchestratorV2ReplayLayerWithRegistry(
              { name: "codex-background-stop-untracked", runtimePolicyOverride: { cwd } },
              makeCodexProviderAdapterRegistryReplayLayer({ transcript: localTranscript }),
              { runEffectWorker: false },
            ),
          ),
        );
      }).pipe(Effect.provide(NodeServices.layer)),
    ),
  );

  const PRE_SETTLE_SCENARIO = "codex-bg-exec-pre-settle";
  const PRE_SETTLE_NATIVE_THREAD = "native-codex-pre-settle-thread";
  const PRE_SETTLE_NATIVE_TURN = "native-codex-pre-settle-turn";

  const preSettleTranscript = makeCodexReplayTranscript({
    scenario: PRE_SETTLE_SCENARIO,
    entries: [
      ...codexReplayPreamble({
        nativeThreadId: PRE_SETTLE_NATIVE_THREAD,
        nativeTurnId: PRE_SETTLE_NATIVE_TURN,
        prompt: BG_PROMPT,
      }),
      {
        type: "emit_inbound",
        label: "item/started/command",
        frame: {
          method: "item/started",
          params: {
            item: backgroundCommandItem("inProgress"),
            threadId: PRE_SETTLE_NATIVE_THREAD,
            turnId: PRE_SETTLE_NATIVE_TURN,
            startedAtMs: 1782622440500,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "item/completed/command-pre-settle",
        frame: {
          method: "item/completed",
          params: {
            item: backgroundCommandItem("completed"),
            threadId: PRE_SETTLE_NATIVE_THREAD,
            turnId: PRE_SETTLE_NATIVE_TURN,
            completedAtMs: 1782622441000,
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
              id: "root-answer-pre-settle",
              text: "DONE",
              phase: "final_answer",
              memoryCitation: null,
            },
            threadId: PRE_SETTLE_NATIVE_THREAD,
            turnId: PRE_SETTLE_NATIVE_TURN,
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
            threadId: PRE_SETTLE_NATIVE_THREAD,
            turn: makeCodexReplayTurn({ id: PRE_SETTLE_NATIVE_TURN, status: "completed" }),
          },
        },
      },
    ],
  });

  it.effect("does not request a continuation for a command that completes before settle", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeCodexReplayHarness(preSettleTranscript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-bg-pre-settle"),
            text: BG_PROMPT,
          }),
        );
        yield* awaitUntil(() => harness.terminalEvents().length === 1, "root turn terminal");
        assert.equal(harness.terminalEvents()[0]?.status, "completed");
        yield* awaitUntil(
          () =>
            harness.events.some(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.type === "command_execution" &&
                event.turnItem.status === "completed" &&
                event.turnItem.exitCode === 0,
            ),
          "pre-settle command projection",
        );

        yield* TestClock.adjust("30 seconds");
        for (let attempt = 0; attempt < 100; attempt++) {
          yield* Effect.yieldNow;
        }
        assert.lengthOf(harness.continuationRequests, 0);
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
        assert.lengthOf(harness.terminalEvents(), 1);
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  const INTERRUPT_SCENARIO = "codex-interrupt-mid-command";
  const INTERRUPT_NATIVE_THREAD = "native-codex-interrupt-thread";
  const INTERRUPT_NATIVE_TURN = "native-codex-interrupt-turn";
  const INTERRUPT_COMMAND_ITEM = "exec-codex-interrupt-command";
  const INTERRUPT_COMMAND_ITEM_TWO = "exec-codex-interrupt-command-two";
  const INTERRUPT_CHILD_COMMAND_ITEM = "exec-codex-interrupt-child-command";
  const INTERRUPT_CHILD_TIMEOUT_BOUNDARY_ITEM = "exec-codex-interrupt-child-timeout-boundary";
  const INTERRUPT_CHILD_NATIVE_THREAD = "native-codex-interrupt-child-thread";
  const INTERRUPT_CHILD_NATIVE_TURN = "native-codex-interrupt-child-turn";
  const INTERRUPT_LATE_CHILD_NATIVE_TURN = "native-codex-interrupt-late-child-turn";
  const INTERRUPT_LATE_CHILD_2_NATIVE_TURN = "native-codex-interrupt-late-child-2-turn";
  const INTERRUPT_TIMEOUT_BOUNDARY_ITEM = "exec-codex-interrupt-timeout-boundary";
  const INTERRUPT_TIMEOUT_LATE_ITEM = "exec-codex-interrupt-timeout-late";
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

        yield* awaitUntil(
          () =>
            harness.events.some(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.type === "command_execution" &&
                event.turnItem.status === "running",
            ),
          "running command item",
        );

        const providerTurnId = harness.events.find(
          (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
            event.type === "provider_turn.updated",
        )?.providerTurn.id;
        assert.isDefined(providerTurnId);

        yield* harness.runtime.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId,
        });

        yield* awaitUntil(() => harness.terminalEvents().length === 1, "interrupted terminal");
        assert.equal(harness.terminalEvents()[0]?.status, "interrupted");

        const terminalIndex = harness.events.findIndex((event) => event.type === "turn.terminal");
        assert.isAtLeast(terminalIndex, 0);

        let lastCommandBeforeTerminal:
          | Extract<ProviderAdapterV2Event, { type: "turn_item.updated" }>
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

        // Late provider item/completed after interrupt must not revive the card
        // or request a background-command wake continuation.
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
          (event): event is Extract<ProviderAdapterV2Event, { type: "turn_item.updated" }> =>
            event.type === "turn_item.updated" && event.turnItem.type === "command_execution",
        );
        assert.isAtLeast(commandUpdates.length, 2, "start + interrupt terminalization");
        assert.equal(commandUpdates[commandUpdates.length - 1]?.turnItem.status, "interrupted");
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
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
    events: ReadonlyArray<ProviderAdapterV2Event>,
    rootThreadId: ThreadId,
  ) => {
    const terminalIndex = events.findIndex((event) => event.type === "turn.terminal");
    const childProviderTurnIndex = events.findIndex(
      (event) =>
        event.type === "provider_turn.updated" &&
        event.threadId !== rootThreadId &&
        event.providerTurn.status === "interrupted",
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
        yield* awaitUntil(
          () =>
            harness.events.some(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.type === "command_execution" &&
                event.turnItem.nativeItemRef?.nativeId === INTERRUPT_CHILD_COMMAND_ITEM &&
                event.turnItem.status === "running",
            ),
          "running child command item",
        );
        const providerTurnId = harness.events.find(
          (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
            event.type === "provider_turn.updated" && event.threadId === harness.threadId,
        )?.providerTurn.id;
        assert.isDefined(providerTurnId);

        yield* harness.runtime.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId,
        });

        yield* awaitUntil(() => harness.terminalEvents().length === 1, "interrupted root terminal");
        assert.equal(harness.terminalEvents()[0]?.status, "interrupted");
        const childCommandUpdates = harness.events.filter(
          (event): event is Extract<ProviderAdapterV2Event, { type: "turn_item.updated" }> =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.nativeItemRef?.nativeId === INTERRUPT_CHILD_COMMAND_ITEM,
        );
        assert.equal(childCommandUpdates.at(-1)?.turnItem.status, "interrupted");
        assert.equal(harness.subagentUpdates().at(-1)?.subagent.status, "interrupted");
        assertChildProviderTerminalBeforeRoot(harness.events, harness.threadId);
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  const childInterruptResponseIndex = interruptSubagentCommandTranscript.entries.findIndex(
    (entry) => entry.type === "emit_inbound" && entry.label === "turn/interrupt/child",
  );
  const interruptSubagentRequestFailureTranscript = makeCodexReplayTranscript({
    scenario: "codex-interrupt-subagent-request-failure",
    entries: [
      ...interruptSubagentCommandTranscript.entries.slice(0, childInterruptResponseIndex),
      {
        type: "emit_inbound",
        label: "turn/completed/root-before-child-interrupt-failure",
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
        label: "turn/interrupt/child",
        frame: {
          id: 4,
          error: { code: -32_000, message: "child interrupt request failed" },
        },
      },
      { type: "runtime_exit", status: "success" },
    ],
  });

  it.effect("terminalizes descendants before the root when an interrupt request fails", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeCodexReplayHarness(interruptSubagentRequestFailureTranscript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-interrupt-subagent-request-failure"),
            text: INTERRUPT_PROMPT,
          }),
        );
        yield* awaitUntil(
          () =>
            harness.events.some(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.type === "command_execution" &&
                event.turnItem.nativeItemRef?.nativeId === INTERRUPT_CHILD_COMMAND_ITEM &&
                event.turnItem.status === "running",
            ),
          "running child command item",
        );
        const providerTurnId = harness.events.find(
          (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
            event.type === "provider_turn.updated" && event.threadId === harness.threadId,
        )?.providerTurn.id;
        assert.isDefined(providerTurnId);

        const interruptExit = yield* harness.runtime
          .interruptTurn({ providerThread: harness.providerThread, providerTurnId })
          .pipe(Effect.exit);

        assert.equal(interruptExit._tag, "Failure");
        yield* awaitUntil(() => harness.terminalEvents().length === 1, "interrupted root terminal");
        assertChildProviderTerminalBeforeRoot(harness.events, harness.threadId);
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  const childTerminationResponseIndex = interruptSubagentCommandTranscript.entries.findIndex(
    (entry) =>
      entry.type === "emit_inbound" && entry.label === "thread/backgroundTerminals/terminate/child",
  );
  const interruptSubagentTimeoutTranscript = makeCodexReplayTranscript({
    scenario: "codex-interrupt-subagent-timeout",
    entries: [
      ...interruptSubagentCommandTranscript.entries.slice(0, childTerminationResponseIndex + 1),
      {
        type: "emit_inbound",
        label: "item/started/child-timeout-boundary",
        frame: {
          method: "item/started",
          params: {
            item: {
              ...interruptCommandItem("inProgress"),
              id: INTERRUPT_CHILD_TIMEOUT_BOUNDARY_ITEM,
              processId: null,
            },
            threadId: INTERRUPT_CHILD_NATIVE_THREAD,
            turnId: INTERRUPT_CHILD_NATIVE_TURN,
            startedAtMs: 1782622441600,
          },
        },
      },
    ],
  });

  it.effect("terminalizes timed-out descendants before the root", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeCodexReplayHarness(interruptSubagentTimeoutTranscript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-interrupt-subagent-timeout"),
            text: INTERRUPT_PROMPT,
          }),
        );
        yield* awaitUntil(
          () =>
            harness.events.some(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.type === "command_execution" &&
                event.turnItem.nativeItemRef?.nativeId === INTERRUPT_CHILD_COMMAND_ITEM &&
                event.turnItem.status === "running",
            ),
          "running child command item",
        );
        const providerTurnId = harness.events.find(
          (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
            event.type === "provider_turn.updated" && event.threadId === harness.threadId,
        )?.providerTurn.id;
        assert.isDefined(providerTurnId);

        const interruptFiber = yield* harness.runtime
          .interruptTurn({ providerThread: harness.providerThread, providerTurnId })
          .pipe(Effect.forkScoped);
        yield* awaitUntil(
          () =>
            harness.events.some(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.type === "command_execution" &&
                event.turnItem.nativeItemRef?.nativeId === INTERRUPT_CHILD_TIMEOUT_BOUNDARY_ITEM &&
                event.turnItem.status === "running",
            ),
          "child timeout boundary item",
        );
        yield* TestClock.adjust("10 seconds");
        yield* Fiber.join(interruptFiber);

        yield* awaitUntil(() => harness.terminalEvents().length === 1, "interrupted root terminal");
        assertChildProviderTerminalBeforeRoot(harness.events, harness.threadId);
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
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
        yield* awaitUntil(
          () =>
            harness.events.some(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.type === "command_execution" &&
                event.turnItem.nativeItemRef?.nativeId === INTERRUPT_CHILD_COMMAND_ITEM &&
                event.turnItem.status === "running",
            ),
          "running child command item",
        );
        const providerTurnId = harness.events.find(
          (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
            event.type === "provider_turn.updated" && event.threadId === harness.threadId,
        )?.providerTurn.id;
        assert.isDefined(providerTurnId);

        const interruptFiber = yield* harness.runtime
          .interruptTurn({ providerThread: harness.providerThread, providerTurnId })
          .pipe(Effect.forkScoped);
        yield* awaitUntil(
          () =>
            harness.events.some(
              (event) =>
                event.type === "provider_turn.updated" &&
                event.providerTurn.nativeTurnRef?.nativeId === INTERRUPT_LATE_CHILD_NATIVE_TURN,
            ),
          "late child provider turn",
        );
        yield* Fiber.join(interruptFiber);

        yield* awaitUntil(() => harness.terminalEvents().length === 1, "interrupted root terminal");
        const lateChildUpdates = harness.events.filter(
          (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
            event.type === "provider_turn.updated" &&
            event.providerTurn.nativeTurnRef?.nativeId === INTERRUPT_LATE_CHILD_NATIVE_TURN,
        );
        assert.equal(lateChildUpdates.at(-1)?.providerTurn.status, "interrupted");
        assertChildProviderTerminalBeforeRoot(harness.events, harness.threadId);
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  const interruptRescanLateSubagentTurnTranscript = makeCodexReplayTranscript({
    scenario: "codex-interrupt-rescan-late-subagent-turn",
    entries: [
      ...interruptSubagentCommandTranscript.entries.slice(0, childTerminationResponseIndex + 1),
      {
        type: "emit_inbound",
        label: "turn/started/late-child-1",
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
      {
        type: "expect_outbound",
        label: "turn/interrupt/late-child-1",
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
        label: "turn/started/late-child-2",
        frame: {
          method: "turn/started",
          params: {
            threadId: INTERRUPT_CHILD_NATIVE_THREAD,
            turn: makeCodexReplayTurn({
              id: INTERRUPT_LATE_CHILD_2_NATIVE_TURN,
              status: "inProgress",
            }),
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/interrupt/late-child-1",
        frame: { id: 7, result: {} },
      },
      {
        type: "expect_outbound",
        label: "turn/interrupt/late-child-2",
        frame: {
          id: 8,
          method: "turn/interrupt",
          params: {
            threadId: INTERRUPT_CHILD_NATIVE_THREAD,
            turnId: INTERRUPT_LATE_CHILD_2_NATIVE_TURN,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/interrupt/late-child-2",
        frame: { id: 8, result: {} },
      },
      { type: "runtime_exit", status: "success" },
    ],
  });

  it.effect("interrupts descendants discovered only by the final interrupt rescan", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeCodexReplayHarness(interruptRescanLateSubagentTurnTranscript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-interrupt-rescan-late-subagent-turn"),
            text: INTERRUPT_PROMPT,
          }),
        );
        yield* awaitUntil(
          () =>
            harness.events.some(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.type === "command_execution" &&
                event.turnItem.nativeItemRef?.nativeId === INTERRUPT_CHILD_COMMAND_ITEM &&
                event.turnItem.status === "running",
            ),
          "running child command item",
        );
        const providerTurnId = harness.events.find(
          (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
            event.type === "provider_turn.updated" && event.threadId === harness.threadId,
        )?.providerTurn.id;
        assert.isDefined(providerTurnId);

        const interruptFiber = yield* harness.runtime
          .interruptTurn({ providerThread: harness.providerThread, providerTurnId })
          .pipe(Effect.forkScoped);
        yield* awaitUntil(
          () =>
            harness.events.some(
              (event) =>
                event.type === "provider_turn.updated" &&
                event.providerTurn.nativeTurnRef?.nativeId === INTERRUPT_LATE_CHILD_NATIVE_TURN,
            ),
          "late child 1 provider turn",
        );
        yield* TestClock.adjust("10 seconds");
        yield* Fiber.join(interruptFiber);

        yield* awaitUntil(() => harness.terminalEvents().length === 1, "interrupted root terminal");
        assert.equal(harness.terminalEvents()[0]?.status, "interrupted");

        const rootTerminalIndex = harness.events.findIndex(
          (event) => event.type === "turn.terminal",
        );
        const lateChild1InterruptedIndex = harness.events.findIndex(
          (event) =>
            event.type === "provider_turn.updated" &&
            event.providerTurn.nativeTurnRef?.nativeId === INTERRUPT_LATE_CHILD_NATIVE_TURN &&
            event.providerTurn.status === "interrupted",
        );
        const lateChild2InterruptedIndex = harness.events.findIndex(
          (event) =>
            event.type === "provider_turn.updated" &&
            event.providerTurn.nativeTurnRef?.nativeId === INTERRUPT_LATE_CHILD_2_NATIVE_TURN &&
            event.providerTurn.status === "interrupted",
        );
        assert.isAtLeast(
          lateChild1InterruptedIndex,
          0,
          "late child 1 must terminalize interrupted",
        );
        assert.isAtLeast(
          lateChild2InterruptedIndex,
          0,
          "late child 2 must terminalize interrupted",
        );
        assert.isAbove(
          rootTerminalIndex,
          lateChild1InterruptedIndex,
          "late child 1 must terminalize before the root run",
        );
        assert.isAbove(
          rootTerminalIndex,
          lateChild2InterruptedIndex,
          "late child 2 must terminalize before the root run",
        );
        assertChildProviderTerminalBeforeRoot(harness.events, harness.threadId);
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  const interruptTimeoutTranscript = makeCodexReplayTranscript({
    scenario: "codex-interrupt-timeout",
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
        frame: { id: 5, result: { terminated: true } },
      },
      {
        type: "emit_inbound",
        label: "item/started/command-at-timeout-boundary",
        afterMs: 9_999,
        frame: {
          method: "item/started",
          params: {
            item: {
              ...interruptCommandItem("inProgress"),
              id: INTERRUPT_TIMEOUT_BOUNDARY_ITEM,
              command: "echo TIMEOUT_BOUNDARY",
              processId: null,
              commandActions: [{ type: "unknown", command: "echo TIMEOUT_BOUNDARY" }],
            },
            threadId: INTERRUPT_NATIVE_THREAD,
            turnId: INTERRUPT_NATIVE_TURN,
            startedAtMs: 1782622450500,
          },
        },
      },
      {
        type: "expect_outbound",
        label: "thread/backgroundTerminals/terminate/two",
        frame: {
          id: 6,
          method: "thread/backgroundTerminals/terminate",
          params: { threadId: INTERRUPT_NATIVE_THREAD, processId: "57681" },
        },
      },
      {
        type: "emit_inbound",
        label: "thread/backgroundTerminals/terminate/two",
        frame: { id: 6, result: { terminated: true } },
      },
      {
        type: "emit_inbound",
        label: "turn/completed/late",
        afterMs: 20_000,
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
        type: "emit_inbound",
        label: "item/started/command-after-timeout",
        frame: {
          method: "item/started",
          params: {
            item: {
              ...interruptCommandItem("inProgress"),
              id: INTERRUPT_TIMEOUT_LATE_ITEM,
              command: "echo LATE_AFTER_TIMEOUT",
              processId: null,
              commandActions: [{ type: "unknown", command: "echo LATE_AFTER_TIMEOUT" }],
            },
            threadId: INTERRUPT_NATIVE_THREAD,
            turnId: INTERRUPT_NATIVE_TURN,
            startedAtMs: 1782622470500,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "item/completed/command-late",
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
      { type: "runtime_exit", status: "success" },
    ],
  });

  it.effect("bounds a hanging native root interrupt without terminalizing the active turn", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "native-root-interrupt-hang";
        const nativeTurnId = "native-root-interrupt-hang-turn";
        const dispatched = yield* Deferred.make<void>();
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "root-interrupt-hang",
            entries: codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Work" }),
          }),
          undefined,
          (method, params) =>
            method === "turn/interrupt"
              ? Effect.sync(() =>
                  assert.deepEqual(params, { threadId: nativeThreadId, turnId: nativeTurnId }),
                ).pipe(
                  Effect.andThen(Deferred.succeed(dispatched, undefined)),
                  Effect.andThen(Effect.never),
                )
              : Effect.void,
          undefined,
          { runtimeGeneration: "root-interrupt-incarnation" },
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("root-interrupt-hang-attempt"),
            text: "Work",
          }),
        );
        yield* awaitUntil(
          () =>
            harness.events.some(
              (event) =>
                event.type === "provider_turn.updated" &&
                event.providerTurn.nativeTurnRef?.nativeId === nativeTurnId,
            ),
          "the acknowledged native root turn",
        );
        const providerTurnId = harness.events.find(
          (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
            event.type === "provider_turn.updated",
        )!.providerTurn.id;
        const result = yield* harness.runtime
          .interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId,
            nativeOperation: {
              operationId: "root-interrupt-hang-operation",
              operation: "interrupt_turn",
              instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
              providerSessionId: harness.runtime.providerSessionId,
              providerThreadId: harness.providerThread.id,
              runtimeGeneration: "root-interrupt-incarnation",
            },
          })
          .pipe(Effect.result, Effect.timeoutOption("4 seconds"), Effect.forkChild);
        yield* Deferred.await(dispatched);
        yield* TestClock.adjust("4 seconds");
        const bounded = yield* Fiber.join(result);
        assert.equal(
          bounded._tag,
          "Some",
          "the native root RPC must return a typed timeout within three seconds",
        );
        if (bounded._tag !== "Some") return;
        assert.equal(bounded.value._tag, "Failure");
        if (bounded.value._tag !== "Failure") return;
        const error = bounded.value.failure;
        assert.equal(error._tag, "ProviderAdapterInterruptError");
        assert.equal((error.cause as { _tag: string })._tag, "CodexNativeInterruptTimeoutError");
        assert.equal((error.cause as { nativeThreadId: string }).nativeThreadId, nativeThreadId);
        assert.equal((error.cause as { nativeTurnId: string }).nativeTurnId, nativeTurnId);
        assert.equal("nativeEffect" in error ? error.nativeEffect?.outcome : undefined, "unknown");
        assert.lengthOf(harness.terminalEvents(), 0);
        assert.isFalse(
          harness.events.some(
            (event) =>
              event.type === "provider_turn.updated" && event.providerTurn.status === "interrupted",
          ),
        );
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("retains native background-command state after an unacknowledged root interrupt", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "native-root-interrupt-command";
        const nativeTurnId = "native-root-interrupt-command-turn";
        const dispatched = yield* Deferred.make<void>();
        const entries = codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Work" });
        entries.push({
          type: "emit_inbound",
          frame: {
            method: "item/started",
            params: {
              threadId: nativeThreadId,
              turnId: nativeTurnId,
              startedAtMs: 1782622441000,
              item: { ...interruptCommandItem("inProgress"), id: "root-unacknowledged-command" },
            },
          },
        });
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({ scenario: "root-interrupt-command", entries }),
          undefined,
          (method) =>
            method === "turn/interrupt"
              ? Deferred.succeed(dispatched, undefined).pipe(Effect.andThen(Effect.never))
              : Effect.void,
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("root-interrupt-command-attempt"),
            text: "Work",
          }),
        );
        yield* awaitUntil(
          () =>
            harness.events.some(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.nativeItemRef?.nativeId === "root-unacknowledged-command" &&
                event.turnItem.status === "running",
            ),
          "the running native command",
        );
        const providerTurnId = harness.events.find(
          (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
            event.type === "provider_turn.updated",
        )!.providerTurn.id;
        const stopped = yield* harness.runtime
          .interruptTurn({ providerThread: harness.providerThread, providerTurnId })
          .pipe(Effect.result, Effect.forkChild);
        yield* Deferred.await(dispatched);
        yield* TestClock.adjust("3 seconds");
        const result = yield* Fiber.join(stopped);
        assert.equal(result._tag, "Failure");
        assert.isTrue(yield* harness.hasPendingBackgroundWork);
        assert.lengthOf(harness.terminalEvents(), 0);
        const commands = harness.events.filter(
          (event): event is Extract<ProviderAdapterV2Event, { type: "turn_item.updated" }> =>
            event.type === "turn_item.updated" &&
            event.turnItem.nativeItemRef?.nativeId === "root-unacknowledged-command",
        );
        assert.equal(commands.at(-1)?.turnItem.status, "running");
      }),
    ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect(
    "reaches a bounded root interrupt after 32 hanging children and leaves the 33rd untouched",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const nativeThreadId = "native-root-interrupt-fleet";
          const nativeTurnId = "native-root-interrupt-fleet-turn";
          const calls: Array<string> = [];
          const rootDispatched = yield* Deferred.make<void>();
          const entries = codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Work" });
          for (let index = 0; index < 33; index++) {
            entries.push(
              {
                type: "emit_inbound",
                frame: {
                  method: "item/completed",
                  params: {
                    threadId: nativeThreadId,
                    turnId: nativeTurnId,
                    completedAtMs: 1782622441000,
                    item: {
                      type: "subAgentActivity",
                      id: `fleet-child-item-${index}`,
                      kind: "started",
                      agentThreadId: `fleet-child-${index}`,
                      agentPath: `/root/fleet_${index}`,
                    },
                  },
                },
              },
              {
                type: "emit_inbound",
                frame: {
                  method: "turn/started",
                  params: {
                    threadId: `fleet-child-${index}`,
                    turn: makeCodexReplayTurn({
                      id: `fleet-child-turn-${index}`,
                      status: "inProgress",
                    }),
                  },
                },
              },
            );
          }
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({ scenario: "root-interrupt-fleet", entries }),
            undefined,
            (method, params) =>
              method === "turn/interrupt"
                ? Effect.gen(function* () {
                    const target = params as { threadId: string; turnId: string };
                    calls.push(target.threadId);
                    if (target.threadId === nativeThreadId) {
                      assert.equal(target.turnId, nativeTurnId);
                      yield* Deferred.succeed(rootDispatched, undefined);
                    } else
                      assert.equal(
                        target.turnId,
                        `fleet-child-turn-${target.threadId.slice("fleet-child-".length)}`,
                      );
                    return yield* Effect.never;
                  })
                : Effect.void,
          );
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make("root-interrupt-fleet-attempt"),
              text: "Work",
            }),
          );
          yield* awaitUntil(
            () =>
              harness.events.filter(
                (event) =>
                  event.type === "provider_turn.updated" && event.providerTurn.status === "running",
              ).length === 34,
            "all actual native child turns",
          );
          const providerTurnId = harness.events.find(
            (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
              event.type === "provider_turn.updated" &&
              event.providerTurn.nativeTurnRef?.nativeId === nativeTurnId,
          )!.providerTurn.id;
          const result = yield* harness.runtime
            .interruptTurn({
              providerThread: harness.providerThread,
              providerTurnId,
              nativeOperation: {
                operationId: "fleet-interrupt-operation",
                operation: "interrupt_turn",
                runtimeGeneration: harness.runtime.runtimeGeneration!,
              },
            })
            .pipe(Effect.result, Effect.forkChild);
          yield* awaitUntil(() => calls.length === 8, "the first eight-child batch");
          for (const count of [16, 24, 32]) {
            yield* TestClock.adjust("3 seconds");
            yield* awaitUntil(() => calls.length === count, `the ${count}-child dispatch boundary`);
          }
          yield* TestClock.adjust("1 second");
          yield* Deferred.await(rootDispatched);
          assert.deepEqual(calls, [
            ...Array.from({ length: 32 }, (_, index) => `fleet-child-${index}`),
            nativeThreadId,
          ]);
          yield* TestClock.adjust("3 seconds");
          const failure = yield* Fiber.join(result);
          assert.equal(failure._tag, "Failure");
          if (failure._tag !== "Failure") return;
          assert.equal(
            (failure.failure.cause as { _tag: string })._tag,
            "CodexNativeInterruptTimeoutError",
          );
          assert.equal(
            "nativeEffect" in failure.failure ? failure.failure.nativeEffect?.outcome : undefined,
            "unknown",
          );
          assert.lengthOf(harness.terminalEvents(), 0);
          assert.isFalse(
            harness.events.some(
              (event) =>
                event.type === "provider_turn.updated" &&
                event.providerTurn.status === "interrupted",
            ),
          );
        }),
      ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("bounds interrupt settlement and drops late completion events", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeCodexReplayHarness(interruptTimeoutTranscript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-interrupt-timeout"),
            text: INTERRUPT_PROMPT,
          }),
        );
        yield* awaitUntil(
          () =>
            harness.events.filter(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.type === "command_execution" &&
                event.turnItem.status === "running",
            ).length === 1,
          "running command item",
        );
        const providerTurnId = harness.events.find(
          (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
            event.type === "provider_turn.updated",
        )?.providerTurn.id;
        assert.isDefined(providerTurnId);

        const interruptFiber = yield* harness.runtime
          .interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId,
          })
          .pipe(Effect.forkScoped);
        yield* awaitUntil(
          () =>
            harness.events.filter(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.type === "command_execution" &&
                event.turnItem.status === "running",
            ).length === 2,
          "post-interrupt running command item",
        );

        yield* TestClock.adjust("10 seconds");
        yield* Fiber.join(interruptFiber);
        yield* awaitUntil(() => harness.terminalEvents().length === 1, "timeout terminal");
        assert.equal(harness.terminalEvents()[0]?.status, "interrupted");
        const terminalProviderTurnsBeforeLateEvents = harness.events.filter(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status === "interrupted",
        );
        assert.lengthOf(terminalProviderTurnsBeforeLateEvents, 1);

        const commandUpdatesBeforeLateEvents = harness.events.filter(
          (event): event is Extract<ProviderAdapterV2Event, { type: "turn_item.updated" }> =>
            event.type === "turn_item.updated" && event.turnItem.type === "command_execution",
        );
        const terminalCommands = commandUpdatesBeforeLateEvents.filter(
          (event) =>
            event.turnItem.status === "interrupted" &&
            event.turnItem.nativeItemRef?.nativeId !== INTERRUPT_TIMEOUT_BOUNDARY_ITEM,
        );
        assert.lengthOf(terminalCommands, 2);
        const boundaryUpdates = commandUpdatesBeforeLateEvents.filter(
          (event) => event.turnItem.nativeItemRef?.nativeId === INTERRUPT_TIMEOUT_BOUNDARY_ITEM,
        );
        const lastBoundaryUpdate = boundaryUpdates.at(-1);
        assert.isDefined(lastBoundaryUpdate);
        assert.equal(lastBoundaryUpdate.turnItem.status, "interrupted");
        assert.isFalse(yield* harness.hasPendingBackgroundWork);

        yield* TestClock.adjust("20 seconds");
        for (let attempt = 0; attempt < 100; attempt++) {
          yield* Effect.yieldNow;
        }
        assert.lengthOf(harness.terminalEvents(), 1);
        assert.lengthOf(
          harness.events.filter(
            (event) =>
              event.type === "provider_turn.updated" && event.providerTurn.status === "interrupted",
          ),
          terminalProviderTurnsBeforeLateEvents.length,
          "late completion must not duplicate provider-turn finalization",
        );
        assert.lengthOf(
          harness.events.filter(
            (event) =>
              event.type === "turn_item.updated" && event.turnItem.type === "command_execution",
          ),
          commandUpdatesBeforeLateEvents.length,
          "late starts and completions must not project after timeout",
        );
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  const interruptTerminationFailureTranscript = makeCodexReplayTranscript({
    scenario: "codex-interrupt-termination-failure",
    entries: [
      ...interruptMidCommandTranscript.entries
        .filter(
          (entry) => entry.type === "runtime_exit" || entry.label !== "item/completed/command-late",
        )
        .map((entry) =>
          entry.type === "emit_inbound" &&
          entry.label === "thread/backgroundTerminals/list/after-false"
            ? {
                ...entry,
                frame: {
                  id: 6,
                  result: {
                    data: [{ processId: "57680" }],
                    nextCursor: null,
                  },
                },
              }
            : entry,
        ),
      {
        type: "expect_outbound",
        label: "thread/backgroundTerminals/terminate/one-retry",
        frame: {
          id: 8,
          method: "thread/backgroundTerminals/terminate",
          params: { threadId: INTERRUPT_NATIVE_THREAD, processId: "57680" },
        },
      },
      {
        type: "emit_inbound",
        label: "thread/backgroundTerminals/terminate/one-retry",
        frame: { id: 8, result: { terminated: false } },
      },
      {
        type: "expect_outbound",
        label: "thread/backgroundTerminals/list/after-false-retry",
        frame: {
          id: 9,
          method: "thread/backgroundTerminals/list",
          params: { threadId: INTERRUPT_NATIVE_THREAD },
        },
      },
      {
        type: "emit_inbound",
        label: "thread/backgroundTerminals/list/after-false-retry",
        frame: {
          id: 9,
          result: { data: [{ processId: "57680" }], nextCursor: null },
        },
      },
      { type: "runtime_exit", status: "success" },
    ],
  });

  it.effect("attempts every terminal and cleans up tracking when termination fails", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeCodexReplayHarness(interruptTerminationFailureTranscript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-interrupt-termination-failure"),
            text: INTERRUPT_PROMPT,
          }),
        );
        yield* awaitUntil(
          () =>
            harness.events.some(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.type === "command_execution" &&
                event.turnItem.status === "running",
            ),
          "running command item",
        );
        const providerTurnId = harness.events.find(
          (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
            event.type === "provider_turn.updated",
        )?.providerTurn.id;
        assert.isDefined(providerTurnId);

        const interruptExit = yield* harness.runtime
          .interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId,
          })
          .pipe(Effect.exit);

        assert.equal(interruptExit._tag, "Failure");
        yield* awaitUntil(() => harness.terminalEvents().length === 1, "interrupted terminal");
        assert.equal(harness.terminalEvents()[0]?.status, "interrupted");
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  for (const scenario of [
    {
      name: "usage",
      code: "usageLimitExceeded",
      notification: false,
      expectedClass: "usage_limit",
    },
    { name: "rate", code: "rateLimitExceeded", notification: false, expectedClass: "usage_limit" },
    {
      name: "ordinary",
      code: "contextWindowExceeded",
      notification: false,
      expectedClass: "provider_error",
    },
    {
      name: "notification",
      code: "usageLimitExceeded",
      notification: true,
      expectedClass: "usage_limit",
    },
    {
      name: "replacement",
      code: "usageLimitExceeded",
      notification: true,
      expectedClass: "provider_error",
    },
    {
      name: "known-reset",
      code: "usageLimitExceeded",
      notification: false,
      expectedClass: "usage_limit",
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
  ] as const) {
    it.effect(`classifies Codex terminal failures from ${scenario.name} evidence`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const nativeThreadId = `native-limit-${scenario.name}`;
          const nativeTurnId = `turn-limit-${scenario.name}`;
          const message = "Provider stopped this request.";
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
              ...(scenario.name === "known-reset" || scenario.name === "deferred-reset"
                ? [snapshot]
                : []),
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
                            codexErrorInfo: scenario.code,
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
                          : { codexErrorInfo: scenario.code }),
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
          if (scenario.name === "known-reset" || scenario.name === "deferred-reset")
            assert.equal(terminal.failure.resetAt, resetAt);
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
        }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
      ),
    );
  }

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
        yield* awaitUntil(() => harness.terminalEvents().length === 1, "failed terminal");
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
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
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
        }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
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

  it.effect.each([
    { name: "Sol", model: "gpt-5.6-sol" },
    { name: "Fable", model: "gpt-5.6-fable" },
    { name: "Astra", model: "gpt-6-astra" },
    { name: "missing", model: null },
    { name: "invalid", model: null },
    { name: "wrong child", model: null },
  ])("reads $name child metadata without using the parent model", ({ name, model }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const metadataRead = yield* Deferred.make<void>();
        const modelReported = yield* Deferred.make<void>();
        const harness = yield* makeCodexReplayHarness(
          resumeSubagentTranscript,
          (event) =>
            event.type === "subagent.updated" && event.subagent.model === model
              ? Deferred.succeed(modelReported, undefined)
              : Effect.void,
          undefined,
          (threadId) => {
            assert.equal(threadId, RESUME_CHILD_THREAD);
            return Deferred.succeed(metadataRead, undefined).pipe(
              Effect.as(
                name === "invalid"
                  ? {}
                  : {
                      thread: { id: name === "wrong child" ? "other-child" : threadId },
                      model: name === "wrong child" ? "gpt-5.6-sol" : model,
                    },
              ),
            );
          },
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("attempt-child-model"),
            text: RESUME_PROMPT,
          }),
        );
        yield* Deferred.await(metadataRead);
        yield* Deferred.await(modelReported);
        yield* TestClock.adjust("100 millis");
        yield* harness.firstTerminal;
        assert.equal(harness.subagentUpdates().at(-1)?.subagent.model, model);
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  for (const wrongChild of [false, true]) {
    it.effect(
      `queries missing native child effort when its model is known and ${wrongChild ? "rejects another child's reply" : "reports the matching reply"}`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const metadataRead = yield* Deferred.make<void>();
            const observed = yield* Deferred.make<void>();
            const childModel = "gpt-5.6-sol";
            const transcript = {
              ...resumeSubagentTranscript,
              entries: resumeSubagentTranscript.entries.map((entry) =>
                entry.type === "emit_inbound" &&
                entry.label === "item/completed/subAgentActivity-started" &&
                Predicate.isObject(entry.frame)
                  ? {
                      ...entry,
                      frame: {
                        ...entry.frame,
                        params: {
                          threadId: RESUME_NATIVE_THREAD,
                          turnId: RESUME_NATIVE_TURN,
                          item: {
                            type: "collabAgentToolCall",
                            id: "spawn-known-model",
                            tool: "spawnAgent",
                            status: "completed",
                            senderThreadId: RESUME_NATIVE_THREAD,
                            receiverThreadIds: [RESUME_CHILD_THREAD],
                            agentsStates: {
                              [RESUME_CHILD_THREAD]: { status: "running", message: null },
                            },
                            model: childModel,
                            prompt: "Work independently",
                          },
                        },
                      },
                    }
                  : entry.type === "expect_outbound" &&
                      Predicate.isObject(entry.frame) &&
                      entry.frame.method === "turn/start"
                    ? {
                        ...entry,
                        frame: {
                          ...entry.frame,
                          params: {
                            ...(entry.frame.params as Record<string, unknown>),
                            effort: "low",
                          },
                        },
                      }
                    : entry,
              ),
            };
            const harness = yield* makeCodexReplayHarness(
              transcript,
              (event) =>
                event.type === "subagent.updated" && event.subagent.reasoningEffort === "xhigh"
                  ? Deferred.succeed(observed, undefined)
                  : Effect.void,
              undefined,
              (nativeId) =>
                Deferred.succeed(metadataRead, undefined).pipe(
                  Effect.as({
                    thread: { id: wrongChild ? "unrelated-native-child" : nativeId },
                    model: childModel,
                    reasoningEffort: "xhigh",
                  }),
                ),
            );
            yield* harness.runtime.startTurn({
              ...makeCodexTestTurnInput({
                threadId: harness.threadId,
                providerThread: harness.providerThread,
                now: yield* DateTime.now,
                attemptId: RunAttemptId.make(`child-effort-${wrongChild}`),
                text: RESUME_PROMPT,
              }),
              modelSelection: {
                ...CODEX_TEST_MODEL_SELECTION,
                options: [{ id: "reasoningEffort", value: "low" }],
              },
            });
            yield* Deferred.await(metadataRead);
            if (!wrongChild) yield* Deferred.await(observed);
            yield* TestClock.adjust("100 millis");
            yield* harness.firstTerminal;
            const child = harness.subagentUpdates().at(-1)!.subagent;
            assert.equal(child.model, childModel);
            assert.equal(child.reasoningEffort, wrongChild ? undefined : "xhigh");
          }),
        ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    );
  }

  it.effect(
    "keeps native child effort from a newer settings report when older metadata completes",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const releaseMetadata = yield* Deferred.make<void>();
          const observed = yield* Deferred.make<void>();
          const model = "gpt-5.6-sol";
          const notification: CodexReplay.CodexAppServerReplayEntry = {
            type: "emit_inbound",
            frame: {
              method: "thread/settings/updated",
              params: {
                threadId: RESUME_CHILD_THREAD,
                threadSettings: {
                  model,
                  effort: "high",
                  modelProvider: "openai",
                  cwd: "/workspace",
                  approvalPolicy: "never",
                  approvalsReviewer: "auto_review",
                  collaborationMode: {
                    mode: "default",
                    settings: { model, reasoning_effort: "high" },
                  },
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
                  ? [entry, notification]
                  : [entry],
              ),
            },
            (event) =>
              event.type === "subagent.updated" && event.subagent.reasoningEffort === "high"
                ? Deferred.succeed(observed, undefined)
                : Effect.void,
            undefined,
            (nativeId) =>
              Deferred.await(releaseMetadata).pipe(
                Effect.as({ thread: { id: nativeId }, model, reasoningEffort: "low" }),
              ),
          );
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make("newer-child-effort"),
              text: RESUME_PROMPT,
            }),
          );
          yield* TestClock.adjust("100 millis");
          yield* Deferred.await(observed);
          yield* Deferred.succeed(releaseMetadata, undefined);
          yield* TestClock.adjust("30 seconds");
          assert.equal(harness.subagentUpdates().at(-1)?.subagent.reasoningEffort, "high");
        }),
      ).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect.each(["thread/settings/updated", "model/rerouted"] as const)(
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
              params:
                method === "model/rerouted"
                  ? {
                      threadId: RESUME_CHILD_THREAD,
                      turnId: RESUME_CHILD_TURN_1,
                      fromModel: "gpt-6-astra",
                      toModel: model,
                      reason: "highRiskCyberActivity",
                    }
                  : {
                      threadId: RESUME_CHILD_THREAD,
                      threadSettings: {
                        model,
                        modelProvider: "openai",
                        cwd: "/workspace",
                        approvalPolicy: "never",
                        approvalsReviewer: "auto_review",
                        collaborationMode: { mode: "default", settings: { model } },
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
                  ? [entry, notification]
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
        }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
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
        yield* awaitUntil(
          () =>
            harness.subagentUpdates().some((event) => event.subagent.result === "CODEX_FIRST_DONE"),
          "first subagent result",
        );
        assert.lengthOf(
          harness.subagentUpdates().filter((event) => event.subagent.result === "CODEX_FIRST_DONE"),
          1,
        );
        yield* TestClock.adjust("100 millis");
        yield* awaitUntil(() => harness.terminalEvents().length === 1, "root turn terminal");
        assert.equal(harness.terminalEvents()[0]?.status, "completed");
        const settledUpdates = harness.subagentUpdates();
        const firstCompletion = settledUpdates[settledUpdates.length - 1];
        assert.equal(firstCompletion?.subagent.status, "completed");
        assert.equal(firstCompletion?.subagent.result, "CODEX_FIRST_DONE");
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
        const settledUpdateCount = settledUpdates.length;

        yield* TestClock.adjust("30 seconds");
        yield* awaitUntil(
          () => harness.subagentUpdates().length > settledUpdateCount,
          "subagent re-open",
        );
        const reopened = harness.subagentUpdates()[settledUpdateCount];
        assert.equal(reopened?.subagent.status, "running");
        assert.equal(DateTime.toEpochMillis(reopened!.subagent.startedAt!), 1782622470000);
        assert.isNull(reopened!.subagent.completedAt);
        assert.isTrue(yield* harness.hasPendingBackgroundWork);

        yield* TestClock.adjust("30 seconds");
        yield* awaitUntil(() => {
          const updates = harness.subagentUpdates();
          const latest = updates[updates.length - 1];
          return (
            latest !== undefined &&
            latest.subagent.status === "completed" &&
            latest.subagent.result === "CODEX_RESUME_DONE"
          );
        }, "resumed subagent completion");
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
        assert.lengthOf(harness.terminalEvents(), 1);
        assert.lengthOf(harness.continuationRequests, 0);
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  it.effect("rejects duplicate child starts across parent runs", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const firstDone = yield* Deferred.make<void>();
        const resumed = yield* Deferred.make<void>();
        const secondTurn = "native-parent-resume-turn";
        const secondPrompt = "Resume the child.";
        const entries = [...resumeSubagentTranscript.entries];
        const resumeIndex = entries.findIndex(
          (e) => e.type === "emit_inbound" && e.label === `turn/started/${RESUME_CHILD_TURN_2}`,
        );
        const suffix = entries.splice(resumeIndex);
        for (const entry of codexReplayPreamble({
          nativeThreadId: RESUME_NATIVE_THREAD,
          nativeTurnId: secondTurn,
          prompt: secondPrompt,
        }).slice(-3)) {
          entries.push(
            entry.type === "expect_outbound" || entry.type === "emit_inbound"
              ? {
                  ...entry,
                  frame:
                    Predicate.isObject(entry.frame) && "id" in entry.frame
                      ? { ...entry.frame, id: 4 }
                      : entry.frame,
                }
              : entry,
          );
        }
        entries.push(childTurnStarted(RESUME_CHILD_TURN_1));
        entries.push(...suffix);
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "codex-cross-run-resume",
            entries,
          }),
          (event) =>
            event.type === "turn.terminal"
              ? Deferred.succeed(firstDone, undefined)
              : event.type === "subagent.updated" && event.subagent.runId === "run-cross-run-second"
                ? Deferred.succeed(resumed, undefined)
                : Effect.void,
        );
        const now = yield* DateTime.now;
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("cross-run-first"),
            text: RESUME_PROMPT,
          }),
        );
        yield* TestClock.adjust("100 millis");
        yield* Deferred.await(firstDone);
        yield* harness.runtime.startTurn({
          ...makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("cross-run-second"),
            text: secondPrompt,
          }),
          runOrdinal: 2,
          providerTurnOrdinal: 2,
        });
        yield* TestClock.adjust("30 seconds");
        yield* Deferred.await(resumed);
        const row = harness
          .subagentUpdates()
          .find((e) => e.subagent.runId === "run-cross-run-second")?.subagent;
        assert.equal(row?.status, "running");
        assert.equal(row?.parentNodeId, "node-cross-run-second");
        assert.isNull(row?.completedAt);
        assert.isNotNull(row?.startedAt);
        assert.equal(DateTime.toEpochMillis(row!.startedAt!), 1782622470000);
      }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
    ),
  );

  for (const [nativeStatus, expectedStatus] of [
    ["pendingInit", "pending"],
    ["running", "running"],
    ["interrupted", "interrupted"],
    ["shutdown", "cancelled"],
    ["notFound", "failed"],
    ["errored", "failed"],
    ["completed", "completed"],
    ["activity-completed", "completed"],
    ["late-activity-completed", "completed"],
    ["stale-running", "completed"],
    ["duplicate-completed", "completed"],
  ] as const) {
    it.effect(`normalizes subagent ${nativeStatus} without losing its lifecycle`, () =>
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
          // A known child's turn provides a receipt even after the parent context is released.
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
        }).pipe(Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
      ),
    );
  }

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

      assert.instanceOf(error, ProviderAdapterRollbackThreadError);
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
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
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
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect(
    "fails honestly when a legacy fork cannot honor a source turn without a native reference",
    () =>
      Effect.gen(function* () {
        const nativeThreadId = "legacy-fallback-source-thread";
        const forkThreadId = "legacy-fallback-fork-thread";
        const preamble = codexReplayPreamble({
          nativeThreadId,
          nativeTurnId: "legacy-fallback-source-turn",
          prompt: "unused",
        });
        const transcript = makeCodexReplayTranscript({
          scenario: "codex-fork-legacy-fallback",
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
                result: { thread: { id: forkThreadId, historyMode: "legacy" } },
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

        const error = yield* Effect.flip(
          harness.runtime.forkThread({
            sourceProviderThread: harness.providerThread,
            sourceProviderTurns: [firstTurn, secondTurn],
            providerTurnId: firstTurn.id,
            targetThreadId: ThreadId.make("thread-fork-legacy-fallback-target"),
          }),
        );

        assert.instanceOf(error, ProviderAdapterForkThreadError);
        assert.include(
          errorCauseChainText(error),
          "legacy",
          "the missing-native-reference fallback must name the legacy limitation",
        );
        assert.notInclude(
          outbound,
          "thread/rollback",
          "thread/rollback must not be sent to a legacy Codex fork",
        );
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );

  it.effect("propagates native thread/fork failures as typed fork errors", () =>
    Effect.gen(function* () {
      const nativeThreadId = "fork-failure-source-thread";
      const preamble = codexReplayPreamble({
        nativeThreadId,
        nativeTurnId: "fork-failure-source-turn",
        prompt: "unused",
      });
      const transcript = makeCodexReplayTranscript({
        scenario: "codex-fork-request-failure",
        entries: [
          ...preamble.slice(0, 5),
          {
            type: "expect_outbound",
            label: "thread/fork",
            frame: {
              id: 3,
              method: "thread/fork",
              params: {
                threadId: nativeThreadId,
                lastTurnId: "native-turn-first",
                config: CodexAdapterV2.CODEX_THREAD_CONFIG,
              },
            },
          },
          {
            type: "emit_inbound",
            label: "thread/fork",
            frame: { id: 3, error: { code: -32000, message: "fork exploded" } },
          },
        ],
      });
      const harness = yield* makeCodexReplayHarness(transcript);
      const now = yield* DateTime.now;
      const firstTurn = codexReplaySourceTurn({
        id: "provider-turn-first",
        ordinal: 1,
        nativeId: "native-turn-first",
        providerThreadId: harness.providerThread.id,
        now,
      });

      const error = yield* Effect.flip(
        harness.runtime.forkThread({
          sourceProviderThread: harness.providerThread,
          sourceProviderTurns: [firstTurn],
          providerTurnId: firstTurn.id,
          targetThreadId: ThreadId.make("thread-fork-failure-target"),
        }),
      );

      assert.instanceOf(error, ProviderAdapterForkThreadError);
      assert.include(errorCauseChainText(error), "fork exploded");
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer))),
  );
});
