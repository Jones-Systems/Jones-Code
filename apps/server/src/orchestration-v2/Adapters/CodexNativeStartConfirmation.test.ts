import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CodexSettings,
  CommandId,
  MessageId,
  NodeId,
  ProjectId,
  RunAttemptId,
  RunId,
  ThreadId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderInstanceId,
  ProviderDriverKind,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexReplay from "effect-codex-app-server/replay";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";

import packageJson from "../../../package.json" with { type: "json" };
import * as IdAllocator from "../IdAllocator.ts";
import {
  ProviderAdapterTurnStartError,
  ProviderAdapterOpenSessionError,
  ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2TurnInput,
  type ProviderNativeStartAcknowledgmentV1,
  type ProviderNativeStartProducerCaptureV1,
} from "../ProviderAdapter.ts";
import * as Adapter from "./CodexAdapterV2.ts";
import { makeReplayServerConfig } from "./CodexAdapterV2.testkit.ts";

const modelSelection = { instanceId: Adapter.CODEX_DEFAULT_INSTANCE_ID, model: "gpt-5.4" };
const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: "/workspace",
});
const nativeId = "native-original-start";
const nativeTurnId = "native-original-turn";
const prompt = "Exercise the actual original start";
const nativeTurn = {
  id: nativeTurnId,
  items: [],
  itemsView: "notLoaded",
  status: "inProgress",
  error: null,
  startedAt: null,
  completedAt: null,
  durationMs: null,
};
const transcript: CodexReplay.CodexAppServerReplayTranscript = {
  provider: "codex",
  protocol: "codex.app-server",
  version: "0.144.0",
  scenario: "native-original-confirmation",
  entries: [
    {
      type: "expect_outbound",
      frame: {
        id: 1,
        method: "initialize",
        params: {
          clientInfo: { name: "T3 Code", title: "T3 Code", version: packageJson.version },
          capabilities: { experimentalApi: true, optOutNotificationMethods: ["turn/diff/updated"] },
        },
      },
    },
    {
      type: "emit_inbound",
      frame: {
        id: 1,
        result: {
          userAgent: "synthetic Codex",
          codexHome: "/synthetic/codex",
          platformFamily: "unix",
          platformOs: "linux",
        },
      },
    },
    { type: "expect_outbound", frame: { method: "initialized" } },
    {
      type: "expect_outbound",
      frame: { id: 2, method: "thread/start", params: { config: Adapter.CODEX_THREAD_CONFIG } },
    },
    {
      type: "emit_inbound",
      frame: {
        id: 2,
        result: {
          thread: {
            id: nativeId,
            sessionId: nativeId,
            forkedFromId: null,
            preview: "",
            ephemeral: false,
            modelProvider: "openai",
            createdAt: 1782622440,
            updatedAt: 1782622440,
            status: { type: "idle" },
            path: "/synthetic/original.jsonl",
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
      frame: {
        id: 3,
        method: "turn/start",
        params: {
          threadId: nativeId,
          input: [{ type: "text", text: prompt }],
          cwd: "/workspace",
          model: "gpt-5.4",
          approvalPolicy: "never",
          approvalsReviewer: "user",
          sandboxPolicy: { type: "dangerFullAccess" },
          summary: "detailed",
        },
      },
    },
    { type: "emit_inbound", frame: { id: 3, result: { turn: nativeTurn } } },
  ],
};

// The real typed client decodes the reply. This barrier withholds it from the actual producer;
// it never fabricates an ACK, a SQL confirmation, or managed ownership.
const fixture = Effect.fnUntraced(function* (compact?: {
  readonly nativeTurnIds?: ReadonlyArray<string>;
  readonly notificationThreadId?: string;
  readonly notificationAfterResponse?: boolean;
}) {
  const fs = yield* FileSystem.FileSystem;
  const config = yield* Effect.acquireRelease(
    makeReplayServerConfig("native-original-confirmation"),
    (value) => fs.remove(value.baseDir, { recursive: true }).pipe(Effect.orDie),
  );
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const compactTranscript: CodexReplay.CodexAppServerReplayTranscript = {
    ...transcript,
    scenario: "native-original-compact-confirmation",
    entries: [
      ...transcript.entries.slice(0, 5),
      {
        type: "expect_outbound",
        frame: { id: 3, method: "thread/compact/start", params: { threadId: nativeId } },
      },
      ...(compact?.notificationAfterResponse
        ? [{ type: "emit_inbound" as const, frame: { id: 3, result: {} } }]
        : []),
      ...(compact?.nativeTurnIds ?? [nativeTurnId]).map((id) => ({
        type: "emit_inbound" as const,
        frame: {
          method: "turn/started",
          params: {
            threadId: compact?.notificationThreadId ?? nativeId,
            turn: { ...nativeTurn, id },
          },
        },
      })),
      ...(!compact?.notificationAfterResponse
        ? [{ type: "emit_inbound" as const, frame: { id: 3, result: {} } }]
        : []),
    ],
  };
  const activeTranscript = compact === undefined ? transcript : compactTranscript;
  const driver = yield* CodexReplay.makeReplayDriver(activeTranscript);
  const responseReceived = yield* Deferred.make<void>();
  const releaseResponse = yield* Deferred.make<void>();
  const releaseNotification = yield* Deferred.make<void>();
  const notificationsHandled = yield* Deferred.make<void>();
  let handledNotifications = 0;
  let turnRequests = 0;
  const clientFactory: Adapter.CodexAppServerClientFactoryShape = {
    open: (openInput) =>
      Effect.gen(function* () {
        const context = yield* Layer.build(CodexReplay.layerReplayWithDriver(driver));
        const client = yield* Effect.service(CodexClient.CodexAppServerClient).pipe(
          Effect.provide(context),
        );
        const request: typeof client.request = (method, params) =>
          Effect.sync(() => {
            if (method === "turn/start" || method === "thread/compact/start") turnRequests++;
          }).pipe(
            Effect.andThen(client.request(method, params)),
            Effect.tap(() =>
              method === "turn/start" || method === "thread/compact/start"
                ? Deferred.succeed(responseReceived, undefined)
                : Effect.void,
            ),
            Effect.tap(() =>
              method === "turn/start" || method === "thread/compact/start"
                ? Deferred.await(releaseResponse)
                : Effect.void,
            ),
          );
        const handleServerNotification: typeof client.handleServerNotification = (
          method,
          handler,
        ) =>
          client.handleServerNotification(method, (payload) =>
            compact !== undefined && method === "turn/started"
              ? Deferred.await(releaseNotification).pipe(
                  Effect.andThen(handler(payload)),
                  Effect.tap(() => {
                    handledNotifications++;
                    return handledNotifications === (compact.nativeTurnIds ?? [nativeTurnId]).length
                      ? Deferred.succeed(notificationsHandled, undefined)
                      : Effect.void;
                  }),
                )
              : handler(payload),
          );
        return { ...client, request, handleServerNotification };
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterOpenSessionError({
              driver: Adapter.CODEX_DRIVER_KIND,
              providerSessionId: openInput.providerSessionId,
              cause,
            }),
        ),
      ),
  };
  const adapter = Adapter.makeCodexAdapterV2({
    instanceId: Adapter.CODEX_DEFAULT_INSTANCE_ID,
    settings: yield* Schema.decodeUnknownEffect(CodexSettings)({}),
    environment: {},
    fileSystem: fs,
    idAllocator,
    serverConfig: config,
    clientFactory,
  });
  const threadId = ThreadId.make("thread-native-original");
  const runtime = yield* adapter.openSession({
    threadId,
    providerSessionId: ProviderSessionId.make("session-native-original"),
    modelSelection,
    runtimePolicy,
  });
  const providerThread = yield* runtime.ensureThread({ threadId, modelSelection, runtimePolicy });
  const now = yield* DateTime.now;
  const appThread: OrchestrationV2AppThread = {
    id: threadId,
    projectId: ProjectId.make("project-native-original"),
    title: "Original native start",
    providerInstanceId: Adapter.CODEX_DEFAULT_INSTANCE_ID,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: providerThread.id,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  const input: ProviderAdapterV2TurnInput = {
    appThread,
    threadId,
    runId: RunId.make("run-native-original"),
    runOrdinal: 1,
    providerTurnOrdinal: 1,
    attemptId: RunAttemptId.make("attempt-native-original"),
    rootNodeId: NodeId.make("node-native-original"),
    providerThread,
    message: {
      messageId: MessageId.make("message-native-original"),
      text: prompt,
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection,
    runtimePolicy,
  };
  const nativeOperation = {
    operationId: `effect:${CommandId.make("command-native-original")}:provider-turn.start:${input.runId}`,
    operation: compact === undefined ? ("start_turn" as const) : ("compact_thread" as const),
    instanceId: runtime.instanceId,
    threadId,
    providerSessionId: runtime.providerSessionId,
    providerThreadId: providerThread.id,
    attemptId: input.attemptId,
  };
  return {
    runtime,
    input,
    nativeOperation,
    driver,
    responseReceived,
    releaseResponse,
    requests: () => turnRequests,
    releaseNotification,
    notificationsHandled,
    activeTranscript,
  };
});
const dependencies = Layer.merge(NodeServices.layer, IdAllocator.layer);

describe("Actual Codex native start confirmation", () => {
  it.effect("issues one original current producer and ACK only after the actual queued reply", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const captures: ProviderNativeStartProducerCaptureV1[] = [];
      const acknowledgments: ProviderNativeStartAcknowledgmentV1[] = [];
      const started = yield* f.runtime
        .startTurn({
          ...f.input,
          nativeOperation: f.nativeOperation,
          nativeStartConfirmation: {
            beforeDispatch: (capture) =>
              Effect.sync(() => {
                captures.push(capture);
                return { evidenceRevision: 1, revalidate: Effect.void };
              }),
            acknowledged: (packet) =>
              Effect.sync(() => {
                acknowledgments.push(packet);
              }),
          },
        })
        .pipe(Effect.forkScoped);
      yield* Deferred.await(f.responseReceived);
      assert.strictEqual(captures.length, 1);
      assert.strictEqual(acknowledgments.length, 0);
      assert.strictEqual(f.requests(), 1);
      assert.strictEqual(captures[0]!.binding.runtimeGeneration, f.runtime.runtimeGeneration);
      assert.strictEqual(captures[0]!.binding.nativeThreadId, nativeId);
      assert.isNotNull(Adapter.readIssuedCodexNativeStartCapture(captures[0]));
      assert.isNull(Adapter.readIssuedCodexNativeStartCapture({ ...captures[0] }));
      yield* Deferred.succeed(f.releaseResponse, undefined);
      assert.isUndefined(yield* Fiber.join(started));
      assert.strictEqual(acknowledgments.length, 1);
      const packet = acknowledgments[0]!;
      assert.strictEqual(packet.capture, captures[0]);
      assert.strictEqual(packet.method, "turn/start");
      assert.strictEqual(packet.nativeTurnId, nativeTurnId);
      assert.isNotNull(Adapter.readIssuedCodexNativeStartAcknowledgment(packet));
      assert.isNull(Adapter.readIssuedCodexNativeStartAcknowledgment({ ...packet }));
      assert.deepEqual(yield* Ref.get(f.driver.state), {
        cursor: transcript.entries.length,
        failure: null,
      });
    }).pipe(Effect.scoped, Effect.provide(dependencies)),
  );

  it.effect("a lost original dispatch fence refuses the delayed reply and any repeated RPC", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      let current = true;
      let acknowledgments = 0;
      const revalidate = Effect.suspend(() =>
        current
          ? Effect.void
          : Effect.fail(
              new ProviderAdapterTurnStartError({
                driver: Adapter.CODEX_DRIVER_KIND,
                threadId: f.input.threadId,
                providerThreadId: f.input.providerThread.id,
                runId: f.input.runId,
                cause: "Original dispatch fence lost",
              }),
            ),
      );
      const input = {
        ...f.input,
        nativeOperation: f.nativeOperation,
        nativeStartConfirmation: {
          beforeDispatch: () => Effect.succeed({ evidenceRevision: 1, revalidate }),
          acknowledged: () =>
            Effect.sync(() => {
              acknowledgments++;
            }),
        },
      };
      const started = yield* f.runtime.startTurn(input).pipe(Effect.forkScoped);
      yield* Deferred.await(f.responseReceived);
      current = false;
      yield* Deferred.succeed(f.releaseResponse, undefined);
      assert.isTrue(Exit.isFailure(yield* Fiber.await(started)));
      assert.strictEqual(acknowledgments, 0);
      assert.strictEqual(f.requests(), 1);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(f.runtime.startTurn(input))));
      assert.strictEqual(f.requests(), 1);
    }).pipe(Effect.scoped, Effect.provide(dependencies)),
  );
});

const compactDependencies = Layer.merge(NodeServices.layer, IdAllocator.layer);
const boundedWait = (value: Deferred.Deferred<void>) =>
  Deferred.await(value).pipe(Effect.timeout("2 seconds"));

describe("Actual captured Codex native compaction confirmation", () => {
  it.live("joins the actual compact reply with its captured early notification exactly once", () =>
    Effect.gen(function* () {
      const f = yield* fixture({});
      const captures: ProviderNativeStartProducerCaptureV1[] = [];
      const packets: ProviderNativeStartAcknowledgmentV1[] = [];
      const input = {
        ...f.input,
        nativeOperation: f.nativeOperation,
        nativeStartConfirmation: {
          beforeDispatch: (capture: ProviderNativeStartProducerCaptureV1) =>
            Effect.sync(() => {
              captures.push(capture);
              return { evidenceRevision: 7, revalidate: Effect.void };
            }),
          acknowledged: (packet: ProviderNativeStartAcknowledgmentV1) =>
            Effect.sync(() => {
              packets.push(packet);
            }),
        },
      };
      const pending = yield* f.runtime.compactThread!(input).pipe(Effect.forkScoped);
      yield* Deferred.succeed(f.releaseNotification, undefined);
      yield* boundedWait(f.notificationsHandled);
      yield* boundedWait(f.responseReceived);
      assert.strictEqual(captures.length, 1);
      assert.strictEqual(packets.length, 0);
      assert.strictEqual(f.requests(), 1);
      assert.strictEqual(captures[0]!.nativeOperation.operation, "compact_thread");
      assert.strictEqual(captures[0]!.binding.runtimeGeneration, f.runtime.runtimeGeneration);
      assert.strictEqual(captures[0]!.binding.nativeThreadId, nativeId);
      assert.isNotNull(Adapter.readIssuedCodexNativeStartCapture(captures[0]));
      assert.isNull(Adapter.readIssuedCodexNativeStartCapture({ ...captures[0] }));
      yield* Deferred.succeed(f.releaseResponse, undefined);
      assert.isUndefined(yield* Fiber.join(pending).pipe(Effect.timeout("2 seconds")));
      assert.strictEqual(packets.length, 1);
      const packet = packets[0]!;
      assert.strictEqual(packet.capture, captures[0]);
      assert.strictEqual(packet.method, "thread/compact/start");
      assert.strictEqual(packet.nativeTurnId, nativeTurnId);
      assert.strictEqual(packet.evidenceRevision, 7);
      assert.isNotNull(Adapter.readIssuedCodexNativeStartAcknowledgment(packet));
      assert.isNull(Adapter.readIssuedCodexNativeStartAcknowledgment({ ...packet }));
      yield* Adapter.readIssuedCodexNativeStartAcknowledgment(packet)!.revalidate;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(f.runtime.compactThread!(input))));
      assert.strictEqual(f.requests(), 1);
      assert.deepEqual(yield* Ref.get(f.driver.state), {
        cursor: f.activeTranscript.entries.length,
        failure: null,
      });
    }).pipe(Effect.scoped, Effect.provide(compactDependencies)),
  );

  it.live("the actual reply alone produces no ACK while its native notification is withheld", () =>
    Effect.gen(function* () {
      const f = yield* fixture({ notificationAfterResponse: true });
      let acknowledgments = 0;
      const pending = yield* f.runtime.compactThread!({
        ...f.input,
        nativeOperation: f.nativeOperation,
        nativeStartConfirmation: {
          beforeDispatch: () => Effect.succeed({ evidenceRevision: 1, revalidate: Effect.void }),
          acknowledged: () =>
            Effect.sync(() => {
              acknowledgments++;
            }),
        },
      }).pipe(Effect.forkScoped);
      yield* boundedWait(f.responseReceived);
      yield* Deferred.succeed(f.releaseResponse, undefined);
      yield* Effect.yieldNow;
      assert.strictEqual(acknowledgments, 0);
      assert.strictEqual(f.requests(), 1);
      yield* Deferred.succeed(f.releaseNotification, undefined);
      yield* boundedWait(f.notificationsHandled);
      assert.isUndefined(yield* Fiber.join(pending).pipe(Effect.timeout("2 seconds")));
      assert.strictEqual(acknowledgments, 1);
      assert.strictEqual(f.requests(), 1);
    }).pipe(Effect.scoped, Effect.provide(compactDependencies)),
  );

  it.live("a lost original compact fence rejects the delayed reply and a repeated RPC", () =>
    Effect.gen(function* () {
      const f = yield* fixture({});
      let current = true;
      let acknowledgments = 0;
      const revalidate = Effect.suspend(() =>
        current
          ? Effect.void
          : Effect.fail(
              new ProviderAdapterTurnStartError({
                driver: Adapter.CODEX_DRIVER_KIND,
                threadId: f.input.threadId,
                providerThreadId: f.input.providerThread.id,
                runId: f.input.runId,
                cause: "Original compact owner lost",
              }),
            ),
      );
      const input = {
        ...f.input,
        nativeOperation: f.nativeOperation,
        nativeStartConfirmation: {
          beforeDispatch: () => Effect.succeed({ evidenceRevision: 1, revalidate }),
          acknowledged: () =>
            Effect.sync(() => {
              acknowledgments++;
            }),
        },
      };
      const pending = yield* f.runtime.compactThread!(input).pipe(Effect.forkScoped);
      yield* Deferred.succeed(f.releaseNotification, undefined);
      yield* boundedWait(f.notificationsHandled);
      yield* boundedWait(f.responseReceived);
      current = false;
      yield* Deferred.succeed(f.releaseResponse, undefined);
      assert.isTrue(Exit.isFailure(yield* Fiber.await(pending).pipe(Effect.timeout("2 seconds"))));
      assert.strictEqual(acknowledgments, 0);
      assert.strictEqual(f.requests(), 1);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(f.runtime.compactThread!(input))));
      assert.strictEqual(f.requests(), 1);
    }).pipe(Effect.scoped, Effect.provide(compactDependencies)),
  );

  it.live("conflicting genuine native start notifications cannot qualify the compact ACK", () =>
    Effect.gen(function* () {
      const f = yield* fixture({
        nativeTurnIds: [nativeTurnId, "conflicting-native-compact-turn"],
      });
      let acknowledgments = 0;
      const input = {
        ...f.input,
        nativeOperation: f.nativeOperation,
        nativeStartConfirmation: {
          beforeDispatch: () => Effect.succeed({ evidenceRevision: 1, revalidate: Effect.void }),
          acknowledged: () =>
            Effect.sync(() => {
              acknowledgments++;
            }),
        },
      };
      const pending = yield* f.runtime.compactThread!(input).pipe(Effect.forkScoped);
      yield* Deferred.succeed(f.releaseNotification, undefined);
      yield* boundedWait(f.notificationsHandled);
      yield* boundedWait(f.responseReceived);
      yield* Deferred.succeed(f.releaseResponse, undefined);
      assert.isTrue(Exit.isFailure(yield* Fiber.await(pending).pipe(Effect.timeout("2 seconds"))));
      assert.strictEqual(acknowledgments, 0);
      assert.strictEqual(f.requests(), 1);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(f.runtime.compactThread!(input))));
      assert.strictEqual(f.requests(), 1);
    }).pipe(Effect.scoped, Effect.provide(compactDependencies)),
  );

  it.live(
    "an unrelated notification retains unknown compact custody through owned cancellation",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture({ notificationThreadId: "unrelated-native-thread" });
        let acknowledgments = 0;
        const input = {
          ...f.input,
          nativeOperation: f.nativeOperation,
          nativeStartConfirmation: {
            beforeDispatch: () => Effect.succeed({ evidenceRevision: 1, revalidate: Effect.void }),
            acknowledged: () =>
              Effect.sync(() => {
                acknowledgments++;
              }),
          },
        };
        const pending = yield* f.runtime.compactThread!(input).pipe(Effect.forkScoped);
        yield* Deferred.succeed(f.releaseNotification, undefined);
        yield* boundedWait(f.notificationsHandled);
        yield* boundedWait(f.responseReceived);
        yield* Deferred.succeed(f.releaseResponse, undefined);
        yield* Effect.yieldNow;
        assert.strictEqual(acknowledgments, 0);
        assert.strictEqual(f.requests(), 1);
        yield* Fiber.interrupt(pending);
        assert.isTrue(
          Exit.isFailure(yield* Fiber.await(pending).pipe(Effect.timeout("2 seconds"))),
        );
        assert.isTrue(Exit.isFailure(yield* Effect.exit(f.runtime.compactThread!(input))));
        assert.strictEqual(f.requests(), 1);
      }).pipe(Effect.scoped, Effect.provide(compactDependencies)),
  );

  it.live(
    "complete compact subjects, driver and generation mismatch refuse before native dispatch",
    () =>
      Effect.gen(function* () {
        const mutations = [
          { operation: "start_turn" as const },
          { runtimeGeneration: "unregistered-generation" },
          { providerThreadId: ProviderThreadId.make("foreign-provider-thread") },
          { providerSessionId: ProviderSessionId.make("foreign-session") },
          { instanceId: ProviderInstanceId.make("foreign-instance") },
          { attemptId: RunAttemptId.make("foreign-attempt") },
        ];
        for (const mutation of mutations) {
          yield* Effect.gen(function* () {
            const f = yield* fixture({});
            let registrations = 0;
            let acknowledgments = 0;
            assert.isTrue(
              Exit.isFailure(
                yield* Effect.exit(
                  f.runtime.compactThread!({
                    ...f.input,
                    nativeOperation: { ...f.nativeOperation, ...mutation },
                    nativeStartConfirmation: {
                      beforeDispatch: () =>
                        Effect.sync(() => {
                          registrations++;
                          return { evidenceRevision: 1, revalidate: Effect.void };
                        }),
                      acknowledged: () =>
                        Effect.sync(() => {
                          acknowledgments++;
                        }),
                    },
                  }),
                ),
              ),
            );
            assert.strictEqual(registrations, 0);
            assert.strictEqual(acknowledgments, 0);
            assert.strictEqual(f.requests(), 0);
          }).pipe(Effect.scoped);
        }
        const f = yield* fixture({});
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              f.runtime.compactThread!({
                ...f.input,
                providerThread: {
                  ...f.input.providerThread,
                  driver: ProviderDriverKind.make("foreign-driver"),
                },
                nativeOperation: f.nativeOperation,
                nativeStartConfirmation: {
                  beforeDispatch: () =>
                    Effect.succeed({ evidenceRevision: 1, revalidate: Effect.void }),
                  acknowledged: () => Effect.void,
                },
              }),
            ),
          ),
        );
        assert.strictEqual(f.requests(), 0);
      }).pipe(Effect.scoped, Effect.provide(compactDependencies)),
  );

  it.live(
    "rotated complete subjects and nullable identities invalidate an already captured compact reply",
    () =>
      Effect.gen(function* () {
        for (const mutation of [
          "session",
          "instance",
          "driver",
          "native-id",
          "generation",
        ] as const) {
          yield* Effect.gen(function* () {
            const f = yield* fixture({});
            let acknowledgments = 0;
            const operation = { ...f.nativeOperation };
            const input = {
              ...f.input,
              nativeOperation: operation,
              nativeStartConfirmation: {
                beforeDispatch: () =>
                  Effect.succeed({ evidenceRevision: 1, revalidate: Effect.void }),
                acknowledged: () =>
                  Effect.sync(() => {
                    acknowledgments++;
                  }),
              },
            };
            const pending = yield* f.runtime.compactThread!(input).pipe(Effect.forkScoped);
            yield* Deferred.succeed(f.releaseNotification, undefined);
            yield* boundedWait(f.notificationsHandled);
            yield* boundedWait(f.responseReceived);
            if (mutation === "session")
              Object.assign(input.providerThread, {
                providerSessionId: ProviderSessionId.make("rotated-session"),
              });
            else if (mutation === "instance")
              Object.assign(input.providerThread, {
                providerInstanceId: ProviderInstanceId.make("rotated-instance"),
              });
            else if (mutation === "driver")
              Object.assign(input.providerThread, {
                driver: ProviderDriverKind.make("foreign-driver"),
              });
            else if (mutation === "native-id")
              Object.assign(input.providerThread, {
                nativeThreadRef: { driver: Adapter.CODEX_DRIVER_KIND, nativeId: null },
              });
            else Object.assign(operation, { runtimeGeneration: null });
            yield* Deferred.succeed(f.releaseResponse, undefined);
            assert.isTrue(
              Exit.isFailure(yield* Fiber.await(pending).pipe(Effect.timeout("2 seconds"))),
            );
            assert.strictEqual(acknowledgments, 0);
            assert.strictEqual(f.requests(), 1);
            assert.isTrue(Exit.isFailure(yield* Effect.exit(f.runtime.compactThread!(input))));
            assert.strictEqual(f.requests(), 1);
          }).pipe(Effect.scoped);
        }
      }).pipe(Effect.scoped, Effect.provide(compactDependencies)),
  );

  it.live("closing the actual captured producer scope rejects its delayed compact result", () =>
    Effect.gen(function* () {
      const producerScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(producerScope, Exit.void));
      const f = yield* fixture({}).pipe(Effect.provideService(Scope.Scope, producerScope));
      let acknowledgments = 0;
      const input = {
        ...f.input,
        nativeOperation: f.nativeOperation,
        nativeStartConfirmation: {
          beforeDispatch: () => Effect.succeed({ evidenceRevision: 1, revalidate: Effect.void }),
          acknowledged: () =>
            Effect.sync(() => {
              acknowledgments++;
            }),
        },
      };
      const pending = yield* f.runtime.compactThread!(input).pipe(Effect.forkScoped);
      yield* Deferred.succeed(f.releaseNotification, undefined);
      yield* boundedWait(f.notificationsHandled);
      yield* boundedWait(f.responseReceived);
      yield* Scope.close(producerScope, Exit.void);
      yield* Deferred.succeed(f.releaseResponse, undefined);
      assert.isTrue(Exit.isFailure(yield* Fiber.await(pending).pipe(Effect.timeout("2 seconds"))));
      assert.strictEqual(acknowledgments, 0);
      assert.strictEqual(f.requests(), 1);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(f.runtime.compactThread!(input))));
      assert.strictEqual(f.requests(), 1);
    }).pipe(Effect.scoped, Effect.provide(compactDependencies)),
  );

  it.live("failed compact ACK publication invalidates the issued capsule without replay", () =>
    Effect.gen(function* () {
      const f = yield* fixture({});
      const packets: ProviderNativeStartAcknowledgmentV1[] = [];
      const input = {
        ...f.input,
        nativeOperation: f.nativeOperation,
        nativeStartConfirmation: {
          beforeDispatch: () => Effect.succeed({ evidenceRevision: 1, revalidate: Effect.void }),
          acknowledged: (packet: ProviderNativeStartAcknowledgmentV1) =>
            Effect.sync(() => {
              packets.push(packet);
            }).pipe(
              Effect.andThen(
                Effect.fail(
                  new ProviderAdapterTurnStartError({
                    driver: Adapter.CODEX_DRIVER_KIND,
                    threadId: f.input.threadId,
                    providerThreadId: f.input.providerThread.id,
                    runId: f.input.runId,
                    cause: "ACK owner commit failed",
                  }),
                ),
              ),
            ),
        },
      };
      const pending = yield* f.runtime.compactThread!(input).pipe(Effect.forkScoped);
      yield* Deferred.succeed(f.releaseNotification, undefined);
      yield* boundedWait(f.notificationsHandled);
      yield* boundedWait(f.responseReceived);
      yield* Deferred.succeed(f.releaseResponse, undefined);
      assert.isTrue(Exit.isFailure(yield* Fiber.await(pending).pipe(Effect.timeout("2 seconds"))));
      assert.strictEqual(packets.length, 1);
      assert.strictEqual(f.requests(), 1);
      const issued = Adapter.readIssuedCodexNativeStartAcknowledgment(packets[0]);
      assert.isNotNull(issued);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(issued!.revalidate)));
      assert.isTrue(Exit.isFailure(yield* Effect.exit(f.runtime.compactThread!(input))));
      assert.strictEqual(f.requests(), 1);
    }).pipe(Effect.scoped, Effect.provide(compactDependencies)),
  );
});
