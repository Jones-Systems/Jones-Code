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
const fixture = Effect.fnUntraced(function* () {
  const fs = yield* FileSystem.FileSystem;
  const config = yield* Effect.acquireRelease(
    makeReplayServerConfig("native-original-confirmation"),
    (value) => fs.remove(value.baseDir, { recursive: true }).pipe(Effect.orDie),
  );
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const driver = yield* CodexReplay.makeReplayDriver(transcript);
  const responseReceived = yield* Deferred.make<void>();
  const releaseResponse = yield* Deferred.make<void>();
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
            if (method === "turn/start") turnRequests++;
          }).pipe(
            Effect.andThen(client.request(method, params)),
            Effect.tap(() =>
              method === "turn/start" ? Deferred.succeed(responseReceived, undefined) : Effect.void,
            ),
            Effect.tap(() =>
              method === "turn/start" ? Deferred.await(releaseResponse) : Effect.void,
            ),
          );
        return { ...client, request };
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
    operation: "start_turn" as const,
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
