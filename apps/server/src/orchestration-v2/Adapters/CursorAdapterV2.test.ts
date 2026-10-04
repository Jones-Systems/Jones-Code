import { ProviderDriverKind } from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import type { InteractionUpdate } from "@cursor/sdk";
import {
  CursorSettings,
  EnvironmentId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import * as IdAllocator from "../IdAllocator.ts";
import { readProviderEventOrigin } from "../ProviderEventOrigin.ts";
import {
  ProviderAdapterProtocolError,
  ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2SessionRuntime,
} from "../ProviderAdapter.ts";
import {
  cursorMcpServers,
  cursorRuntimeAgentPolicy,
  cursorSdkModelSelection,
  makeCursorAgentOptions,
  makeCursorAdapterV2,
  nestedToolCallFromEnvelope,
} from "./CursorAdapterV2.ts";
import { isCursorCancellationError, loggedCursorAgentOptions } from "./CursorAgentSdk.ts";

const decodeCursorSettings = Schema.decodeEffect(CursorSettings);

describe("CursorAdapterV2", () => {
  it.effect("uses current creation authority at the factory and the cached native directory", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspace = yield* fileSystem.makeTempDirectoryScoped({ prefix: "cursor-native-authority-" });
      const instanceId = ProviderInstanceId.make("cursor");
      const threadId = ThreadId.make("cursor-native-authority-thread");
      const modelSelection = { instanceId, model: "composer-2.5" };
      const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access", interactionMode: "default", cwd: workspace,
      });
      let opens = 0;
      let sends = 0;
      const adapter = makeCursorAdapterV2({
        instanceId, settings: yield* decodeCursorSettings({}),
        environment: { HOME: workspace }, fileSystem, path,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        serverConfig: yield* ServerConfig.ServerConfig.pipe(
          Effect.provide(ServerConfig.layerTest(workspace, { prefix: "cursor-native-authority-config-" })),
        ),
        runner: {
          assertComplete: Effect.void,
          open: (input) => Effect.sync(() => {
            opens++;
            return {
              agentId: input.agentId ?? "native-cursor-authority",
              listMessages: Effect.succeed([]), close: Effect.void,
              send: () => { sends++; return Effect.die("Unauthorized turn reached native send."); },
            };
          }),
        },
      });
      const runtime = yield* adapter.openSession({
        threadId, providerSessionId: ProviderSessionId.make("cursor-native-authority-session"),
        modelSelection, runtimePolicy,
      });
      const creation = {
        context: {} as never,
        resources: { projectCwd: workspace, branch: "feature/native", worktreePath: workspace },
      };
      const rejected = yield* runtime.ensureThread({
        threadId, modelSelection, runtimePolicy, nativeCreationExecution: creation,
      }).pipe(Effect.match({ onFailure: (error) => error, onSuccess: () => undefined }));
      assert.isDefined(rejected);
      assert.equal(opens, 0);
      const providerThread = yield* runtime.ensureThread({ threadId, modelSelection, runtimePolicy });
      assert.equal(opens, 1);
      const targetDirectory = path.join(workspace, "different-worktree");
      const nativeOperation = {
        operationId: "cursor-cached-directory", operation: "start_turn" as const,
        instanceId, threadId, providerThreadId: providerThread.id,
        providerSessionId: runtime.providerSessionId, runtimeGeneration: runtime.runtimeGeneration!,
      };
      const held = yield* runtime.startTurn({
        nativeOperation,
        nativeCreationExecution: { ...creation, resources: { ...creation.resources, worktreePath: targetDirectory } },
        appThread: {} as never, threadId, runId: RunId.make("cursor-authority-run"),
        runOrdinal: 1, providerTurnOrdinal: 1, attemptId: RunAttemptId.make("cursor-authority-attempt"),
        rootNodeId: NodeId.make("cursor-authority-root"), providerThread,
        message: {
          messageId: MessageId.make("cursor-authority-message"), text: "hi", attachments: [],
          createdBy: "user", creationSource: "web",
        },
        modelSelection, runtimePolicy: { ...runtimePolicy, cwd: targetDirectory },
      }).pipe(Effect.match({ onFailure: (error) => error, onSuccess: () => undefined }));
      assert.isDefined(held);
      if (held !== undefined) {
        assert.equal(held._tag, "ProviderAdapterTurnStartError");
        assert.include(String(held.cause), "actual runtime directory");
        assert.deepEqual("nativeEffect" in held ? held.nativeEffect : undefined,
          { ...nativeOperation, outcome: "unknown" });
      }
      assert.equal(opens, 1);
      assert.equal(sends, 0);
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, IdAllocator.layer))),
  );

  it.effect("holds strict resume when native initialization and attachment cannot be separated", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspace = yield* fileSystem.makeTempDirectoryScoped({ prefix: "cursor-strict-resume-" });
      const instanceId = ProviderInstanceId.make("cursor");
      const threadId = ThreadId.make("cursor-strict-resume-thread");
      const modelSelection = { instanceId, model: "composer-2.5" };
      const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access", interactionMode: "default", cwd: workspace,
      });
      let opens = 0;
      let closes = 0;
      let gateCalls = 0;
      const adapter = makeCursorAdapterV2({
        instanceId,
        settings: yield* decodeCursorSettings({}),
        environment: { HOME: workspace },
        fileSystem, path,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        serverConfig: yield* ServerConfig.ServerConfig.pipe(
          Effect.provide(ServerConfig.layerTest(workspace, { prefix: "cursor-strict-resume-config-" })),
        ),
        runner: {
          assertComplete: Effect.void,
          open: (input) => Effect.sync(() => {
            opens++;
            return {
              agentId: input.agentId ?? "native-cursor-first",
              listMessages: Effect.succeed([]),
              close: Effect.sync(() => { closes++; }),
              send: () => Effect.die("This fixture must not send a turn."),
            };
          }),
        },
      });
      const runtime = yield* adapter.openSession({
        threadId, providerSessionId: ProviderSessionId.make("cursor-strict-resume-session"),
        modelSelection, runtimePolicy,
      });
      const first = yield* runtime.ensureThread({ threadId, modelSelection, runtimePolicy });
      const providerThread = {
        ...first,
        nativeThreadRef: { ...first.nativeThreadRef!, nativeId: "native-cursor-target" },
      };
      const nativeOperation = {
        operationId: "cursor-strict-source-resume", operation: "resume_thread" as const,
        instanceId, threadId, providerThreadId: providerThread.id,
        providerSessionId: runtime.providerSessionId, runtimeGeneration: runtime.runtimeGeneration!,
      };
      const result = yield* runtime.resumeThread({
        providerThread, nativeOperation,
        beforeNativeResume: () => Effect.sync(() => { gateCalls++; }),
      }).pipe(Effect.match({ onFailure: (error) => error, onSuccess: () => undefined }));
      assert.isDefined(result);
      if (result !== undefined) {
        assert.equal(result._tag, "ProviderAdapterResumeThreadError");
        assert.include(String(result.cause), "source proof before native attachment");
        assert.deepEqual("nativeEffect" in result ? result.nativeEffect : undefined,
          { ...nativeOperation, outcome: "unknown" });
      }
      assert.equal(gateCalls, 0);
      assert.equal(opens, 1);
      assert.equal(closes, 0);
      assert.equal(runtime.runtimeGeneration, nativeOperation.runtimeGeneration);
      const ungated = yield* runtime.resumeThread({ providerThread });
      assert.equal(ungated.nativeThreadRef?.nativeId, "native-cursor-target");
      assert.equal(opens, 2);
      assert.equal(closes, 1);
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, IdAllocator.layer))),
  );

  it.effect("changes runtime generation only when the native agent is replaced", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspace = yield* fileSystem.makeTempDirectoryScoped({ prefix: "cursor-generation-" });
      const instanceId = ProviderInstanceId.make("cursor");
      const threadId = ThreadId.make("cursor-generation-thread");
      const modelSelection = { instanceId, model: "composer-2.5" };
      const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd: workspace,
      });
      let opens = 0;
      const adapter = makeCursorAdapterV2({
        instanceId,
        settings: yield* decodeCursorSettings({}),
        environment: { HOME: workspace },
        fileSystem,
        path,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        serverConfig: yield* ServerConfig.ServerConfig.pipe(
          Effect.provide(ServerConfig.layerTest(workspace, { prefix: "cursor-generation-config-" })),
        ),
        runner: {
          assertComplete: Effect.void,
          open: (input) => {
            opens++;
            return Effect.succeed({
              agentId: input.agentId ?? "native-cursor-first",
              listMessages: Effect.succeed([]),
              close: Effect.void,
              send: () => Effect.die("This observation test must not submit a turn."),
            });
          },
        },
      });
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("cursor-generation-session"),
        modelSelection,
        runtimePolicy,
      });
      const before = runtime.runtimeGeneration;
      const providerThread = yield* runtime.ensureThread({ threadId, modelSelection, runtimePolicy });
      assert.isString(runtime.runtimeGeneration);
      assert.notEqual(runtime.runtimeGeneration, before);
      const first = runtime.runtimeGeneration;
      yield* runtime.resumeThread({ providerThread });
      assert.equal(runtime.runtimeGeneration, first);
      assert.equal(opens, 1);
      const binding = {
        threadId,
        providerThreadId: providerThread.id,
        providerSessionId: runtime.providerSessionId,
        instanceId,
        runtimeGeneration: first!,
        nativeThreadId: providerThread.nativeThreadRef!.nativeId ?? undefined,
      };
      const observed = yield* runtime.observeThreadRuntime!(binding);
      assert.equal(observed.status, "unknown");
      assert.equal(opens, 1);
      yield* runtime.resumeThread({
        providerThread: {
          ...providerThread,
          nativeThreadRef: { ...providerThread.nativeThreadRef!, nativeId: "native-cursor-second" },
        },
      });
      assert.notEqual(runtime.runtimeGeneration, first);
      const stale = yield* runtime.observeThreadRuntime!(binding);
      assert.equal(stale.status, "unknown");
      assert.equal(opens, 2);
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, IdAllocator.layer))),
  );

  it.effect("retains the pre-ACK agent owner through replacement and accepts fresh replacement output", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspace = yield* fileSystem.makeTempDirectoryScoped({ prefix: "cursor-origin-ack-" });
      const instanceId = ProviderInstanceId.make("cursor");
      const threadId = ThreadId.make("cursor-origin-ack-thread");
      const modelSelection = { instanceId, model: "composer-2.5" };
      const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access", interactionMode: "default", cwd: workspace,
      });
      const received = yield* Deferred.make<void>();
      const acknowledged = yield* Deferred.make<void>();
      let sends = 0;
      const adapter = makeCursorAdapterV2({
        instanceId, settings: yield* decodeCursorSettings({}), environment: { HOME: workspace },
        fileSystem, path, idAllocator: yield* IdAllocator.IdAllocatorV2,
        serverConfig: yield* ServerConfig.ServerConfig.pipe(
          Effect.provide(ServerConfig.layerTest(workspace, { prefix: "cursor-origin-ack-config-" })),
        ),
        runner: {
          assertComplete: Effect.void,
          open: (opened) => Effect.succeed({
            agentId: opened.agentId ?? "native-cursor-origin-first",
            listMessages: Effect.succeed([]), close: Effect.void,
            send: (sent) => Effect.gen(function* () {
              const ordinal = ++sends;
              yield* (sent.onDelta?.({ type: "text-delta", text: ordinal === 1 ? "Old owner output" : "Fresh owner output" }) ?? Effect.void);
              if (ordinal === 1) {
                yield* Deferred.succeed(received, undefined);
                yield* Deferred.await(acknowledged);
              }
              return { agentId: opened.agentId ?? "native-cursor-origin-first", runId: `cursor-origin-run-${ordinal}`, cancel: Effect.void,
                wait: Effect.succeed({ status: "finished" as const }) };
            }),
          }),
        },
      });
      const runtime = yield* adapter.openSession({
        threadId, providerSessionId: ProviderSessionId.make("cursor-origin-ack-session"), modelSelection, runtimePolicy,
      });
      const providerThread = yield* runtime.ensureThread({ threadId, modelSelection, runtimePolicy });
      const oldGeneration = runtime.runtimeGeneration;
      const turnInput = {
        appThread: {} as never, threadId, providerThread, modelSelection, runtimePolicy,
        runId: RunId.make("cursor-origin-old-run"), runOrdinal: 1, providerTurnOrdinal: 1,
        attemptId: RunAttemptId.make("cursor-origin-old-attempt"), rootNodeId: NodeId.make("cursor-origin-old-root"),
        message: { messageId: MessageId.make("cursor-origin-message"), text: "hi", attachments: [],
          createdBy: "user" as const, creationSource: "web" as const },
      };
      const sending = yield* runtime.startTurn(turnInput).pipe(Effect.forkScoped);
      yield* Deferred.await(received);
      const replacement = yield* runtime.resumeThread({ providerThread: {
        ...providerThread, nativeThreadRef: { ...providerThread.nativeThreadRef!, nativeId: "native-cursor-origin-next" },
      } });
      const newGeneration = runtime.runtimeGeneration;
      assert.notEqual(newGeneration, oldGeneration);
      yield* Deferred.succeed(acknowledged, undefined);
      yield* Fiber.join(sending);
      const oldEvents = yield* runtime.events.pipe(
        Stream.takeUntil((event) => event.type === "turn.terminal"), Stream.runCollect,
      );
      assert.isTrue(oldEvents.some((event) => event.type === "message.updated" && event.message.text === "Old owner output"));
      for (const event of oldEvents) {
        const origin = readProviderEventOrigin(event)!;
        assert.isDefined(origin);
        assert.equal(origin.producer.runtimeGeneration, oldGeneration);
        assert.equal(origin.turn?.runId, turnInput.runId);
        assert.equal(origin.turn?.binding.nativeThreadId, "native-cursor-origin-first");
        assert.equal((yield* Effect.exit(origin.producer.revalidateCurrent))._tag, "Failure");
      }
      yield* runtime.startTurn({ ...turnInput, providerThread: replacement,
        runId: RunId.make("cursor-origin-fresh-run"), attemptId: RunAttemptId.make("cursor-origin-fresh-attempt"),
        providerTurnOrdinal: 2 });
      const freshEvents = yield* runtime.events.pipe(
        Stream.takeUntil((event) => event.type === "turn.terminal"), Stream.runCollect,
      );
      assert.isTrue(freshEvents.some((event) => event.type === "message.updated" && event.message.text === "Fresh owner output"));
      for (const event of freshEvents) {
        const origin = readProviderEventOrigin(event)!;
        assert.isDefined(origin);
        assert.equal(origin.producer.runtimeGeneration, newGeneration);
        assert.equal(origin.turn?.runId, RunId.make("cursor-origin-fresh-run"));
        yield* origin.producer.revalidateCurrent;
      }
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, IdAllocator.layer))),
  );

  it.effect("awaits registration before native replacement and blocks replacement when registration fails", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspace = yield* fileSystem.makeTempDirectoryScoped({ prefix: "cursor-replacement-fence-" });
      const instanceId = ProviderInstanceId.make("cursor");
      const threadId = ThreadId.make("cursor-replacement-fence-thread");
      const modelSelection = { instanceId, model: "composer-2.5" };
      const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access", interactionMode: "default", cwd: workspace,
      });
      const order: string[] = [];
      let opens = 0;
      let closes = 0;
      let rejectRegistration = false;
      let runtime: ProviderAdapterV2SessionRuntime | undefined;
      const adapter = makeCursorAdapterV2({
        instanceId,
        settings: yield* decodeCursorSettings({}),
        environment: { HOME: workspace },
        fileSystem,
        path,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        serverConfig: yield* ServerConfig.ServerConfig.pipe(
          Effect.provide(ServerConfig.layerTest(workspace, { prefix: "cursor-replacement-fence-config-" })),
        ),
        runner: {
          assertComplete: Effect.void,
          open: (input) => Effect.sync(() => {
            opens++;
            order.push("native_open");
            return {
              agentId: input.agentId ?? "native-cursor-registered",
              listMessages: Effect.succeed([]),
              close: Effect.sync(() => { closes++; }),
              send: () => Effect.die("This replacement test must not submit a turn."),
            };
          }),
        },
      });
      runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("cursor-replacement-fence-session"),
        modelSelection,
        runtimePolicy,
        beforeRuntimeReplacement: (nextGeneration) => Effect.gen(function* () {
          order.push("registration_started");
          assert.equal(runtime?.runtimeGeneration, nextGeneration);
          if (rejectRegistration) {
            return yield* new ProviderAdapterProtocolError({
              driver: ProviderDriverKind.make("cursor"), detail: "replacement registration failed",
            });
          }
          yield* Effect.yieldNow;
          order.push("registration_finished");
        }),
      });
      const providerThread = yield* runtime.ensureThread({ threadId, modelSelection, runtimePolicy });
      assert.deepEqual(order, ["registration_started", "registration_finished", "native_open"]);
      assert.equal(opens, 1);
      yield* runtime.resumeThread({ providerThread });
      assert.lengthOf(order, 3);
      const oldGeneration = runtime.runtimeGeneration;
      const nativeOperation = {
        operationId: "cursor-replacement-registration-failure",
        operation: "resume_thread" as const,
        instanceId,
        threadId,
        providerSessionId: runtime.providerSessionId,
        providerThreadId: providerThread.id,
        runtimeGeneration: oldGeneration,
      };
      rejectRegistration = true;
      const error = yield* runtime.resumeThread({
        providerThread: {
          ...providerThread,
          nativeThreadRef: { ...providerThread.nativeThreadRef!, nativeId: "native-cursor-replacement" },
        },
        nativeOperation,
      }).pipe(Effect.flip);
      assert.equal(error._tag, "ProviderAdapterResumeThreadError");
      assert.deepEqual("nativeEffect" in error ? error.nativeEffect : undefined,
        { ...nativeOperation, outcome: "unknown" });
      assert.notEqual(runtime.runtimeGeneration, oldGeneration);
      assert.equal(opens, 1);
      assert.equal(closes, 0);
      assert.deepEqual(order, ["registration_started", "registration_finished", "native_open", "registration_started"]);
      const observation = yield* runtime.observeThreadRuntime!({
        instanceId,
        threadId,
        providerSessionId: runtime.providerSessionId,
        providerThreadId: providerThread.id,
        runtimeGeneration: runtime.runtimeGeneration!,
        nativeThreadId: providerThread.nativeThreadRef!.nativeId ?? undefined,
      });
      assert.equal(observation.status, "unknown");
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, IdAllocator.layer))),
  );

  for (const { status, model } of [
    { status: "finished", model: undefined },
    { status: "cancelled", model: "claude-opus-4-6" },
    { status: "error", model: "custom-fable" },
  ] as const) {
    it.effect(`settles missing task completions when the Cursor run is ${status}`, () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const workspace = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "cursor-v2-lifecycle-",
        });
        const instanceId = ProviderInstanceId.make("cursor");
        const threadId = ThreadId.make("cursor-lifecycle-thread");
        const modelSelection = { instanceId, model: "composer-2.5" };
        const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: workspace,
        });
        const adapter = makeCursorAdapterV2({
          instanceId,
          settings: yield* decodeCursorSettings({}),
          environment: { HOME: workspace },
          fileSystem,
          path,
          idAllocator: yield* IdAllocator.IdAllocatorV2,
          serverConfig: yield* ServerConfig.ServerConfig.pipe(
            Effect.provide(
              ServerConfig.layerTest(workspace, { prefix: "cursor-v2-lifecycle-config-" }),
            ),
          ),
          runner: {
            assertComplete: Effect.void,
            open: () =>
              Effect.succeed({
                agentId: "native-cursor-lifecycle",
                listMessages: Effect.succeed([]),
                close: Effect.void,
                send: (input) =>
                  Effect.gen(function* () {
                    // Recorded Cursor task calls (see fixtures/subagent) stream a
                    // partial-tool-call before tool-call-started with the same args.
                    // The run then ends without tool-call-completed, which a live
                    // run cannot produce on demand.
                    const taskToolCall = {
                      type: "task" as const,
                      args: {
                        description: "Review",
                        prompt: "Review the code.",
                        subagentType: { kind: "unspecified" },
                        ...(model === undefined ? {} : { model }),
                        agentId: "cursor-task-agent",
                        mode: "unspecified" as const,
                      },
                    };
                    for (const type of ["partial-tool-call", "tool-call-started"] as const) {
                      yield* input.onDelta!({
                        type,
                        modelCallId: "model-call",
                        callId: "task-call",
                        toolCall: taskToolCall,
                      }).pipe(Effect.orDie);
                    }
                    return {
                      agentId: "native-cursor-lifecycle",
                      runId: "native-cursor-run",
                      wait: Effect.succeed({
                        id: "native-cursor-run",
                        requestId: "native-request",
                        status,
                        model: { id: "composer-2.5" },
                        durationMs: 1,
                      }),
                      cancel: Effect.void,
                    };
                  }),
              }),
          },
        });
        const runtime = yield* adapter.openSession({
          threadId,
          providerSessionId: ProviderSessionId.make("cursor-lifecycle-session"),
          modelSelection,
          runtimePolicy,
        });
        const providerThread = yield* runtime.ensureThread({
          threadId,
          modelSelection,
          runtimePolicy,
        });
        const now = yield* DateTime.now;
        yield* runtime.startTurn({
          threadId,
          providerThread,
          modelSelection,
          runtimePolicy,
          runId: RunId.make("cursor-lifecycle-run"),
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: RunAttemptId.make("cursor-lifecycle-attempt"),
          rootNodeId: NodeId.make("cursor-lifecycle-root"),
          appThread: {
            id: threadId,
            projectId: ProjectId.make("cursor-lifecycle-project"),
            createdBy: "user",
            creationSource: "web",
            title: "Cursor lifecycle",
            providerInstanceId: instanceId,
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
          message: {
            messageId: MessageId.make("cursor-lifecycle-message"),
            createdBy: "user",
            creationSource: "web",
            text: "Review the code.",
            attachments: [],
          },
        });
        const events = yield* runtime.events.pipe(
          Stream.takeUntil((event) => event.type === "turn.terminal"),
          Stream.runCollect,
        );
        const capturedGeneration = runtime.runtimeGeneration;
        const origins = events.map((event) => readProviderEventOrigin(event));
        assert.isAbove(origins.length, 0);
        for (const origin of origins) {
          assert.isDefined(origin);
          assert.equal(origin?.producer.driver, "cursor");
          assert.equal(origin?.producer.runtimeGeneration, capturedGeneration);
          assert.equal(origin?.turn?.runId, RunId.make("cursor-lifecycle-run"));
          assert.equal(origin?.turn?.attemptId, RunAttemptId.make("cursor-lifecycle-attempt"));
          assert.equal(origin?.turn?.binding.nativeThreadId, "native-cursor-lifecycle");
          assert.equal(origin?.producer.token, origins[0]?.producer.token);
          yield* origin!.producer.revalidateCurrent;
        }
        yield* runtime.resumeThread({ providerThread: {
          ...providerThread, nativeThreadRef: { ...providerThread.nativeThreadRef!, nativeId: "native-cursor-replacement" },
        } });
        assert.notEqual(runtime.runtimeGeneration, capturedGeneration);
        for (const origin of origins) {
          assert.equal((yield* Effect.exit(origin!.producer.revalidateCurrent))._tag, "Failure");
          assert.equal(origin?.producer.runtimeGeneration, capturedGeneration);
        }
        const rows = events.filter((event) => event.type === "subagent.updated");
        assert.equal(rows[0]?.subagent.status, "running");
        assert.equal(rows[0]?.subagent.model, model ?? null);
        assert.equal(
          rows.at(-1)?.subagent.status,
          status === "finished" ? "idle" : status === "cancelled" ? "cancelled" : "failed",
        );
        assert.isNotNull(rows.at(-1)?.subagent.completedAt);
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, IdAllocator.layer))),
    );
  }

  it.effect("fails standalone SDK transport diagnostics and sends compaction as /compress", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspace = yield* fileSystem.makeTempDirectoryScoped({ prefix: "cursor-v2-errors-" });
      const sentMessages: Array<string> = [];
      let sendCalls = 0;
      const instanceId = ProviderInstanceId.make("cursor");
      const threadId = ThreadId.make("cursor-transport-error-thread");
      const modelSelection = { instanceId, model: "composer-2.5" };
      const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd: workspace,
      });
      const adapter = makeCursorAdapterV2({
        instanceId,
        settings: yield* decodeCursorSettings({}),
        environment: { HOME: workspace },
        fileSystem,
        path,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        serverConfig: yield* ServerConfig.ServerConfig.pipe(
          Effect.provide(ServerConfig.layerTest(workspace, { prefix: "cursor-v2-error-config-" })),
        ),
        runner: {
          assertComplete: Effect.void,
          open: () =>
            Effect.succeed({
              agentId: "native-cursor-error",
              listMessages: Effect.succeed([]),
              close: Effect.void,
              send: (input) =>
                Effect.sync(() => {
                  sendCalls += 1;
                  sentMessages.push(
                    typeof input.message === "string" ? input.message : input.message.text,
                  );
                  const runId = `native-cursor-run-${sendCalls}`;
                  return {
                    agentId: "native-cursor-error",
                    runId,
                    wait: Effect.succeed({
                      id: runId,
                      requestId: `native-request-${sendCalls}`,
                      status: "finished" as const,
                      model: { id: "composer-2.5" },
                      durationMs: 1,
                      ...(sendCalls === 1
                        ? { result: "Error: RetriableError: WritableIterable is closed" }
                        : {}),
                    }),
                    cancel: Effect.void,
                  };
                }),
            }),
        },
      });
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("cursor-error-session"),
        modelSelection,
        runtimePolicy,
      });
      const providerThread = yield* runtime.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy,
      });
      const now = yield* DateTime.now;
      const appThread = {
        id: threadId,
        projectId: ProjectId.make("cursor-error-project"),
        createdBy: "user" as const,
        creationSource: "web" as const,
        title: "Cursor transport failure",
        providerInstanceId: instanceId,
        modelSelection,
        runtimeMode: "full-access" as const,
        interactionMode: "default" as const,
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
      };
      yield* runtime.startTurn({
        threadId,
        providerThread,
        modelSelection,
        runtimePolicy,
        runId: RunId.make("cursor-error-run"),
        runOrdinal: 1,
        providerTurnOrdinal: 1,
        attemptId: RunAttemptId.make("cursor-error-attempt"),
        rootNodeId: NodeId.make("cursor-error-root"),
        appThread,
        message: {
          messageId: MessageId.make("cursor-error-message"),
          createdBy: "user",
          creationSource: "web",
          text: "continue",
          attachments: [],
        },
      });
      const first = yield* runtime.events.pipe(
        Stream.filter((event) => event.type === "turn.terminal"),
        Stream.runHead,
      );
      assert.isTrue(Option.isSome(first));
      if (Option.isSome(first)) {
        assert.equal(first.value.status, "failed");
        assert.equal(first.value.failure?.class, "transport_error");
      }

      assert.isDefined(runtime.compactThread);
      yield* runtime.compactThread!({
        threadId,
        providerThread,
        modelSelection,
        runtimePolicy,
        runId: RunId.make("cursor-compact-run"),
        runOrdinal: 2,
        providerTurnOrdinal: 2,
        attemptId: RunAttemptId.make("cursor-compact-attempt"),
        rootNodeId: NodeId.make("cursor-compact-root"),
        appThread,
        message: {
          messageId: MessageId.make("cursor-compact-message"),
          createdBy: "user",
          creationSource: "web",
          text: "ignored by compactThread",
          attachments: [],
        },
      });
      yield* runtime.events.pipe(
        Stream.filter((event) => event.type === "turn.terminal"),
        Stream.runHead,
      );
      assert.equal(sentMessages[1], "/compress");
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, IdAllocator.layer))),
  );

  it.effect("projects Cursor directory trees and lint diagnostics as file search results", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspace = yield* fileSystem.makeTempDirectoryScoped({ prefix: "cursor-v2-search-" });
      const instanceId = ProviderInstanceId.make("cursor");
      const threadId = ThreadId.make("cursor-search-thread");
      const modelSelection = { instanceId, model: "composer-2.5" };
      const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd: workspace,
      });
      const emptyLsNode = {
        childrenDirs: [],
        childrenFiles: [],
        childrenWereProcessed: true,
        fullSubtreeExtensionCounts: {},
        numFiles: 0,
      };
      const updates: ReadonlyArray<InteractionUpdate> = [
        {
          type: "tool-call-completed",
          modelCallId: "native-model-call",
          callId: "read-file",
          toolCall: {
            type: "read",
            args: { path: "src/env.ts" },
            result: {
              status: "success",
              value: { fileSize: 12, content: "---\nfile body", totalLines: 2 },
            },
          },
        },
        {
          type: "tool-call-completed",
          modelCallId: "native-model-call",
          callId: "ls-nested",
          toolCall: {
            type: "ls",
            args: { path: workspace },
            result: {
              status: "success",
              value: {
                directoryTreeRoot: {
                  ...emptyLsNode,
                  absPath: workspace,
                  childrenFiles: [{ name: "README.md" }],
                  childrenDirs: [
                    {
                      ...emptyLsNode,
                      absPath: path.join(workspace, "src"),
                      childrenFiles: [{ name: "index.ts" }],
                      childrenDirs: [
                        {
                          ...emptyLsNode,
                          absPath: path.join(workspace, "src", "nested"),
                          childrenFiles: [{ name: "util.ts" }],
                        },
                      ],
                    },
                  ],
                },
              },
            },
          },
        },
        {
          type: "tool-call-completed",
          modelCallId: "native-model-call",
          callId: "ls-empty",
          toolCall: {
            type: "ls",
            args: { path: path.join(workspace, "empty") },
            result: {
              status: "success",
              value: {
                directoryTreeRoot: { ...emptyLsNode, absPath: path.join(workspace, "empty") },
              },
            },
          },
        },
        {
          type: "tool-call-completed",
          modelCallId: "native-model-call",
          callId: "ls-failed",
          toolCall: {
            type: "ls",
            args: { path: path.join(workspace, "missing") },
            result: { status: "error", error: "ENOENT" },
          },
        },
        {
          type: "tool-call-completed",
          modelCallId: "native-model-call",
          callId: "lints",
          toolCall: {
            type: "readLints",
            args: { paths: ["src/a.ts", "src/b.ts"] },
            result: {
              status: "success",
              value: {
                totalFiles: 2,
                totalDiagnostics: 3,
                fileDiagnostics: [
                  {
                    path: "src/a.ts",
                    diagnosticsCount: 2,
                    diagnostics: [
                      {
                        message: "Unused variable",
                        code: "TS6133",
                        source: "ts",
                        severity: "warning",
                        range: { start: { line: 0, character: 4 } },
                      },
                      {
                        message: "Cannot find name",
                        code: "TS2304",
                        source: "ts",
                        severity: "error",
                        range: { start: { line: 11 } },
                      },
                    ],
                  },
                  {
                    path: "src/b.ts",
                    diagnosticsCount: 1,
                    diagnostics: [
                      {
                        message: "File-level diagnostic",
                        code: "TS0",
                        source: "ts",
                        severity: "information",
                      },
                    ],
                  },
                ],
              },
            },
          },
        },
        {
          type: "tool-call-completed",
          modelCallId: "native-model-call",
          callId: "lints-empty",
          toolCall: {
            type: "readLints",
            args: { paths: ["src/clean.ts"] },
            result: {
              status: "success",
              value: {
                totalFiles: 1,
                totalDiagnostics: 0,
                fileDiagnostics: [{ path: "src/clean.ts", diagnosticsCount: 0, diagnostics: [] }],
              },
            },
          },
        },
        {
          type: "tool-call-completed",
          modelCallId: "native-model-call",
          callId: "lints-failed",
          toolCall: {
            type: "readLints",
            args: { paths: ["src/a.ts"] },
            result: { status: "error", error: "lint failed" },
          },
        },
      ];
      const adapter = makeCursorAdapterV2({
        instanceId,
        settings: yield* decodeCursorSettings({}),
        environment: { HOME: workspace },
        fileSystem,
        path,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        serverConfig: yield* ServerConfig.ServerConfig.pipe(
          Effect.provide(ServerConfig.layerTest(workspace, { prefix: "cursor-v2-search-config-" })),
        ),
        runner: {
          assertComplete: Effect.void,
          open: () =>
            Effect.succeed({
              agentId: "native-cursor-search",
              listMessages: Effect.succeed([]),
              close: Effect.void,
              send: (input) =>
                Effect.gen(function* () {
                  for (const update of updates) {
                    if (input.onDelta !== undefined) {
                      yield* input.onDelta(update).pipe(Effect.orDie);
                    }
                  }
                  return {
                    agentId: "native-cursor-search",
                    runId: "native-cursor-run",
                    wait: Effect.succeed({
                      id: "native-cursor-run",
                      requestId: "native-request",
                      status: "finished" as const,
                      model: { id: "composer-2.5" },
                      durationMs: 1,
                    }),
                    cancel: Effect.void,
                  };
                }),
            }),
        },
      });
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("cursor-search-session"),
        modelSelection,
        runtimePolicy,
      });
      const providerThread = yield* runtime.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy,
      });
      const now = yield* DateTime.now;
      yield* runtime.startTurn({
        threadId,
        providerThread,
        modelSelection,
        runtimePolicy,
        runId: RunId.make("cursor-search-run"),
        runOrdinal: 1,
        providerTurnOrdinal: 1,
        attemptId: RunAttemptId.make("cursor-search-attempt"),
        rootNodeId: NodeId.make("cursor-search-root"),
        appThread: {
          id: threadId,
          projectId: ProjectId.make("cursor-search-project"),
          createdBy: "user",
          creationSource: "web",
          title: "Cursor search results",
          providerInstanceId: instanceId,
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
        message: {
          messageId: MessageId.make("cursor-search-message"),
          createdBy: "user",
          creationSource: "web",
          text: "inspect the workspace",
          attachments: [],
        },
      });
      const events = yield* runtime.events.pipe(
        Stream.takeUntil((event) => event.type === "turn.terminal"),
        Stream.runCollect,
      );
      const fileSearchItems = events.flatMap((event) =>
        event.type === "turn_item.updated" &&
        event.turnItem.type === "file_search" &&
        event.turnItem.status !== "running"
          ? [event.turnItem]
          : [],
      );
      const readItems = events.flatMap((event) =>
        event.type === "turn_item.updated" &&
        event.turnItem.type === "dynamic_tool" &&
        event.turnItem.toolName === "Read" &&
        event.turnItem.status === "completed"
          ? [event.turnItem]
          : [],
      );
      assert.deepEqual(
        readItems.map((item) => ({ title: item.title, input: item.input })),
        [{ title: "Read src/env.ts", input: { path: "src/env.ts" } }],
      );
      assert.deepEqual(
        fileSearchItems.map((item) => ({
          pattern: item.pattern,
          status: item.status,
          results: item.results,
        })),
        [
          {
            pattern: workspace,
            status: "completed",
            results: [
              { fileName: path.join(workspace, "README.md") },
              { fileName: path.join(workspace, "src", "index.ts") },
              { fileName: path.join(workspace, "src", "nested", "util.ts") },
            ],
          },
          {
            pattern: path.join(workspace, "empty"),
            status: "completed",
            results: undefined,
          },
          {
            pattern: path.join(workspace, "missing"),
            status: "failed",
            results: undefined,
          },
          {
            pattern: "src/a.ts, src/b.ts",
            status: "completed",
            results: [
              { fileName: "src/a.ts", line: 1, column: 5, preview: "Unused variable" },
              { fileName: "src/a.ts", line: 12, preview: "Cannot find name" },
              { fileName: "src/b.ts", preview: "File-level diagnostic" },
            ],
          },
          {
            pattern: "src/clean.ts",
            status: "completed",
            results: undefined,
          },
          {
            pattern: "src/a.ts",
            status: "failed",
            results: undefined,
          },
        ],
      );
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, IdAllocator.layer))),
  );

  it("maps Cursor auto and model parameters to SDK selections", () => {
    assert.deepEqual(
      cursorSdkModelSelection({
        instanceId: ProviderInstanceId.make("cursor"),
        model: "auto",
        options: [
          { id: "thinking", value: "high" },
          { id: "contextWindow", value: "1m" },
          { id: "fastMode", value: true },
        ],
      }),
      {
        id: "default",
        params: [
          { id: "thinking", value: "high" },
          { id: "context", value: "1m" },
          { id: "fast", value: "true" },
        ],
      },
    );
  });

  it("maps runtime modes to the SDK sandbox and auto-review controls", () => {
    const base = {
      interactionMode: "default" as const,
      cwd: "/tmp/cursor-adapter",
    };
    assert.deepEqual(
      cursorRuntimeAgentPolicy({
        ...base,
        runtimeMode: "full-access",
      }),
      {
        autoReview: false,
        sandboxEnabled: false,
      },
    );
    assert.deepEqual(
      cursorRuntimeAgentPolicy({
        ...base,
        runtimeMode: "auto-accept-edits",
      }),
      {
        autoReview: false,
        sandboxEnabled: true,
      },
    );
    assert.deepEqual(
      cursorRuntimeAgentPolicy({
        ...base,
        runtimeMode: "approval-required",
      }),
      {
        autoReview: true,
        sandboxEnabled: true,
      },
    );
    assert.deepEqual(
      cursorRuntimeAgentPolicy({
        ...base,
        runtimeMode: "full-access",
        approvalPolicy: "never",
        sandboxPolicy: { type: "readOnly" },
      }),
      {
        autoReview: false,
        sandboxEnabled: true,
      },
    );
    assert.deepEqual(
      cursorRuntimeAgentPolicy({
        ...base,
        runtimeMode: "approval-required",
        approvalPolicy: "never",
        sandboxPolicy: { type: "dangerFullAccess" },
      }),
      {
        autoReview: false,
        sandboxEnabled: false,
      },
    );
  });

  it("loads the user's Cursor settings layers in every runtime mode", () => {
    // The SDK loads no rules, skills, hooks, or MCP config from disk unless
    // settingSources names them, so an omitted list silently drops AGENTS.md.
    for (const runtimeMode of ["full-access", "auto-accept-edits", "approval-required"] as const) {
      const options = makeCursorAgentOptions({
        modelSelection: {
          instanceId: ProviderInstanceId.make("cursor"),
          model: "composer-2.5",
        },
        runtimePolicy: { runtimeMode, interactionMode: "default", cwd: "/workspace" },
        threadId: ThreadId.make("thread-cursor-setting-sources"),
      });
      assert.deepEqual(options.local?.settingSources, [
        "project",
        "user",
        "team",
        "mdm",
        "plugins",
      ]);
    }
  });

  it("injects thread-scoped MCP credentials without logging them", () => {
    const threadId = ThreadId.make("thread-cursor-mcp");
    McpProviderSession.setMcpProviderSession({
      environmentId: EnvironmentId.make("environment-cursor-mcp"),
      threadId,
      providerSessionId: "mcp-session-cursor",
      providerInstanceId: ProviderInstanceId.make("cursor"),
      endpoint: "http://127.0.0.1:43123/mcp",
      authorizationHeader: "Bearer secret-cursor-mcp-token",
      capabilities: new Set<string>(["preview"]),
      browserToolsAvailable: true,
    });

    try {
      assert.deepEqual(cursorMcpServers(threadId), {
        "t3-code": {
          type: "http",
          url: "http://127.0.0.1:43123/mcp",
          headers: {
            Authorization: "Bearer secret-cursor-mcp-token",
          },
        },
      });

      const options = makeCursorAgentOptions({
        apiKey: "secret-cursor-api-key",
        modelSelection: {
          instanceId: ProviderInstanceId.make("cursor"),
          model: "composer-2.5",
        },
        runtimePolicy: {
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: "/workspace",
        },
        threadId,
      });
      assert.deepEqual(options.mcpServers, cursorMcpServers(threadId));

      const logged = JSON.stringify(loggedCursorAgentOptions(options));
      assert.notInclude(logged, "secret-cursor-api-key");
      assert.notInclude(logged, "secret-cursor-mcp-token");
    } finally {
      McpProviderSession.clearMcpProviderSession(threadId);
    }
  });

  it("recognizes direct and SDK-wrapped abort failures as cancellation", () => {
    assert.isTrue(isCursorCancellationError({ name: "AbortError" }));
    assert.isTrue(
      isCursorCancellationError({
        name: "ConnectError",
        cause: {
          name: "ConnectError",
          cause: { name: "AbortError" },
        },
      }),
    );
    assert.isFalse(isCursorCancellationError(new Error("request failed")));
    assert.isFalse(isCursorCancellationError(null));
  });

  it("preserves failed nested read calls when Cursor omits their path", () => {
    assert.deepEqual(
      nestedToolCallFromEnvelope({
        toolCallId: "tool:failed-read",
        readToolCall: {
          args: {},
          result: { error: "File path was not provided." },
        },
      }),
      {
        callId: "tool:failed-read",
        toolCall: {
          type: "read",
          args: { path: "<unknown path>" },
          result: {
            status: "error",
            error: "File path was not provided.",
          },
        },
      },
    );
  });
});
