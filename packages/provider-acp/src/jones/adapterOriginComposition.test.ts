import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, describe, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import type * as AcpSchema from "effect-acp/compat";
import * as AcpErrors from "effect-acp/errors";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as Origin from "@t3tools/provider-core/server/jones/ProviderEventOrigin";
import * as AcpSessionRuntime from "../server/AcpSessionRuntime.ts";
import { AcpProviderCapabilitiesV2, makeAcpAdapterV2 } from "../server/adapter.ts";

const cwd = "/synthetic/acp-origin";
const host = ProviderHost.ProviderHost.of({
  appIdentity: {
    productId: "jones-code",
    productName: "Jones Code",
    version: "synthetic-acp-origin",
    source: null,
  },
  paths: { cwd, baseDir: cwd, stateDir: cwd, providerStatusCacheDir: cwd, attachmentsDir: cwd },
  settings: {
    get: Effect.succeed(DEFAULT_SERVER_SETTINGS),
    withSnapshot: (use) => use(DEFAULT_SERVER_SETTINGS),
    changes: Stream.empty,
    subscribe: Effect.succeed(Stream.empty),
  },
  shouldRunBackgroundWork: () => Effect.succeed(false),
  resolveAttachmentPath: () => null,
  credentials: () => Effect.die("origin harness does not access credentials"),
});
const layer = Layer.mergeAll(
  NodeServices.layer,
  NodeCrypto.layer,
  IdAllocator.layer,
  McpProviderSessions.layer,
  Layer.succeed(ProviderHost.ProviderHost, host),
);

describe("ACP adapter origin composition", () => {
  it.effect.each([false, true])(
    "keeps captured generations through replacement (failed candidate: %s)",
    (failCandidate) =>
      Effect.gen(function* () {
        const instanceId = ProviderInstanceId.make("acp-origin");
        const driver = ProviderDriverKind.make("acp-origin");
        const threadId = ThreadId.make("acp-origin-thread");
        const modelSelection = { instanceId, model: "default" };
        const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd,
        });
        const callbacks: Parameters<
          AcpSessionRuntime.AcpSessionRuntime["Service"]["handleSessionUpdate"]
        >[0][] = [];
        const prompts: AcpSchema.PromptRequest["prompt"][] = [];
        let opens = 0;
        const adapter = yield* makeAcpAdapterV2({
          instanceId,
          selfInvocation: { command: "/synthetic/node", entrypoint: undefined },
          flavor: {
            driver,
            capabilities: AcpProviderCapabilitiesV2,
            makeRuntime: () =>
              Effect.sync(() => {
                const ordinal = ++opens;
                const sessionId = `native-origin-${ordinal}`;
                const startResult: AcpSessionRuntime.AcpSessionRuntimeStartResult = {
                  sessionId,
                  initializeResult: { protocolVersion: 1, agentCapabilities: {}, authMethods: [] },
                  sessionSetupResult: { sessionId },
                  modelConfigId: undefined,
                };
                let notify: Parameters<
                  AcpSessionRuntime.AcpSessionRuntime["Service"]["handleSessionUpdate"]
                >[0] = () => Effect.void;
                const unused = () => Effect.die("unexpected native operation in origin harness");
                return AcpSessionRuntime.AcpSessionRuntime.of({
                  handleRequestPermission: () => Effect.void,
                  handleElicitation: () => Effect.void,
                  handleMcpConnect: () => Effect.void,
                  handleMcpMessage: () => Effect.void,
                  handleMcpDisconnect: () => Effect.void,
                  handleMcpNotification: () => Effect.void,
                  handleReadTextFile: () => Effect.void,
                  handleWriteTextFile: () => Effect.void,
                  handleCreateTerminal: () => Effect.void,
                  handleTerminalOutput: () => Effect.void,
                  handleTerminalWaitForExit: () => Effect.void,
                  handleTerminalKill: () => Effect.void,
                  handleTerminalRelease: () => Effect.void,
                  handleElicitationComplete: () => Effect.void,
                  handleUnknownExtRequest: () => Effect.void,
                  handleUnknownExtNotification: () => Effect.void,
                  handleExtRequest: () => Effect.void,
                  handleExtNotification: () => Effect.void,
                  handleSessionUpdate: (handler) =>
                    Effect.sync(() => {
                      notify = handler;
                      callbacks[ordinal - 1] = handler;
                    }),
                  initialize: () => Effect.succeed(startResult.initializeResult),
                  start: () =>
                    ordinal === 2 && failCandidate
                      ? Effect.fail(
                          new AcpErrors.AcpTransportError({
                            detail: "synthetic candidate refusal",
                            cause: "synthetic",
                          }),
                        )
                      : Effect.succeed(startResult),
                  getEvents: () => Stream.never,
                  drainEvents: Effect.void,
                  getModeState: Effect.undefined,
                  getConfigOptions: Effect.succeed([]),
                  configOptionChanges: Stream.empty,
                  loadSession: unused,
                  resumeSession: unused,
                  forkSession: unused,
                  listSessions: unused,
                  closeSession: () => Effect.succeed({}),
                  deleteSession: unused,
                  listProviders: Effect.die("unused provider listing"),
                  setProvider: unused,
                  disableProvider: unused,
                  logout: Effect.die("unused logout"),
                  prompt: (payload, options) =>
                    Effect.gen(function* () {
                      prompts.push(payload.prompt);
                      if (options?.dispatched !== undefined)
                        yield* Deferred.succeed(options.dispatched, undefined);
                      yield* notify({
                        sessionId,
                        update: {
                          sessionUpdate: "agent_message_chunk",
                          content: { type: "text", text: `generation-${ordinal}` },
                        },
                      });
                      return { stopReason: "end_turn" as const };
                    }),
                  cancel: Effect.void,
                  processContainment: "none",
                  setMode: unused,
                  setSessionModel: unused,
                  setConfigOption: unused,
                  setModel: () => Effect.void,
                  request: unused,
                  notify: () => Effect.void,
                });
              }),
          },
        });
        const runtime = yield* adapter.openSession({
          threadId,
          providerSessionId: ProviderSessionId.make("acp-origin-session"),
          modelSelection,
          runtimePolicy,
        });
        assert.equal(runtime.eventOriginMode, "captured");
        const providerThread = yield* runtime.ensureThread({
          threadId,
          modelSelection,
          runtimePolicy,
        });
        const now = yield* DateTime.now;
        const turn: ProviderAdapter.ProviderAdapterV2TurnInput = {
          threadId,
          providerThread,
          modelSelection,
          runtimePolicy,
          runId: RunId.make("acp-origin-run"),
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: RunAttemptId.make("acp-origin-attempt"),
          rootNodeId: NodeId.make("acp-origin-root"),
          appThread: {
            id: threadId,
            projectId: ProjectId.make("acp-origin-project"),
            createdBy: "user",
            creationSource: "web",
            title: "Synthetic origin",
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
            messageId: MessageId.make("acp-origin-message"),
            text: "Synthetic prompt",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
          },
        };
        yield* runtime.startTurn(turn);
        const events = yield* runtime.events.pipe(
          Stream.takeUntil((event) => event.type === "turn.terminal"),
          Stream.runCollect,
        );
        const first = events.find(
          (event) => event.type === "message.updated" && event.message.role === "assistant",
        );
        assert.isDefined(first);
        const origin = Origin.readProviderEventOrigin(first!);
        assert.isDefined(origin);
        assert.equal(origin!.producer.instanceId, instanceId);
        yield* origin!.producer.revalidateCurrent;
        assert.include(JSON.stringify(prompts[0]), "Jones Code");
        assert.include(JSON.stringify(prompts[0]), "synthetic-acp-origin");
        const replacement = yield* Effect.exit(
          runtime.ensureThread({
            threadId,
            modelSelection,
            runtimePolicy,
            forceNewNativeThread: true,
          }),
        );
        if (failCandidate) {
          assert.isTrue(Exit.isFailure(replacement));
          yield* origin!.producer.revalidateCurrent;
        } else {
          assert.isTrue(Exit.isSuccess(replacement));
          assert.isTrue(Exit.isFailure(yield* Effect.exit(origin!.producer.revalidateCurrent)));
        }
        yield* callbacks[0]!({
          sessionId: "native-origin-1",
          update: {
            sessionUpdate: "agent_message_chunk",
            content: { type: "text", text: "stale callback" },
          },
        });
        if (Exit.isSuccess(replacement)) {
          yield* runtime.startTurn({
            ...turn,
            providerThread: replacement.value,
            runId: RunId.make("acp-origin-successor"),
            attemptId: RunAttemptId.make("acp-origin-successor"),
            runOrdinal: 2,
            providerTurnOrdinal: 2,
          });
          const successor = yield* runtime.events.pipe(
            Stream.takeUntil((event) => event.type === "turn.terminal"),
            Stream.runCollect,
          );
          const next = successor.find(
            (event) => event.type === "message.updated" && event.message.role === "assistant",
          );
          assert.isDefined(next);
          const nextOrigin = Origin.readProviderEventOrigin(next!);
          assert.isDefined(nextOrigin);
          assert.notStrictEqual(nextOrigin!.producer.token, origin!.producer.token);
          yield* nextOrigin!.producer.revalidateCurrent;
          assert.notInclude(JSON.stringify(successor), "stale callback");
        }
      }).pipe(Effect.scoped, Effect.provide(layer)),
  );
});
