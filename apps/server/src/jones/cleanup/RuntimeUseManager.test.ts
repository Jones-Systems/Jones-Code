// @effect-diagnostics nodeBuiltinImport:off -- Task-isolated native owner, directory and shared-database fixtures; no provider turns.
import * as NodePath from "node:path";
import { assert, describe, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EventId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  RunId,
  RuntimeRequestId,
  NodeId,
  ProviderThreadId,
  CheckpointId,
  type OrchestrationV2ProviderThread,
  ThreadId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as Adapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { AgentScope } from "@t3tools/shared/AgentScope";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";
import * as McpSessionRegistry from "../../mcp/McpSessionRegistry.ts";
import * as CommandOutputHub from "../../orchestration-v2/CommandOutputHub.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderEventIngestor from "../../orchestration-v2/ProviderEventIngestor.ts";
import * as Manager from "../../orchestration-v2/ProviderSessionManager.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { runMigrations } from "../../persistence/Migrations.ts";
import * as ProcessRunner from "../../processRunner.ts";
import * as Ownership from "../../serverOwnership.ts";
import { WorkspaceLifecycle } from "../workspaceLifecycle/WorkspaceLifecycle.ts";
import {
  HeldRuntimeOwner,
  invalidateCheckedServerOwnership,
  bindHeldDatabaseOwner,
} from "./CheckedRuntimeOwner.ts";
import { captureRuntimeUseSession } from "./RuntimeUseSession.ts";
import { runtimeUseError, type RuntimeUseError } from "./RuntimeUseProtocol.ts";
import { RuntimeUseLifetime } from "./RuntimeUseLifetime.ts";
import { makeRuntimeUseStore } from "./RuntimeUseStore.ts";
import { withRuntimeUseClaim } from "./RuntimeUseClaim.ts";

const platform = ProcessRunner.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);
const instanceId = ProviderInstanceId.make("codex");
const threadId = ThreadId.make("runtime-use-thread");
const sessionId = ProviderSessionId.make("runtime-use-original-session");
const requestId = RuntimeRequestId.make("runtime-use-original-request");
const projectId = ProjectId.make("runtime-use-project");
const modelSelection = { instanceId, model: "synthetic-local-worker" };
const refused = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    assert.isTrue(Exit.isFailure(yield* Effect.exit(effect)));
  });

const fixture = <A, E>(
  test: (input: {
    readonly open: ReturnType<Manager.ProviderSessionManagerV2["Service"]["open"]>;
    readonly reopenAt: (
      cwd: string,
    ) => ReturnType<Manager.ProviderSessionManagerV2["Service"]["open"]>;
    readonly attachmentPublicationRace: (
      drift: "target" | "claim" | "none" | "owner" | "directory",
    ) => Effect.Effect<
      { readonly refused: boolean; readonly clears: number; readonly bindings: number },
      RuntimeUseError
    >;
    readonly calls: () => number;
    readonly nativeCalls: () => number;
    readonly nativeTargets: () => readonly (string | null)[];
    readonly responseCalls: () => number;
    readonly optionalCalls: () => readonly string[];
    readonly publishProviderThread: Effect.Effect<
      OrchestrationV2ProviderThread,
      EventSink.EventSinkV2Error
    >;
    readonly setBeforeProducer: (hook: Effect.Effect<void, RuntimeUseError>) => void;
    readonly updateWorkspace: (path: string) => Effect.Effect<void, RuntimeUseError>;
    readonly dbPath: string;
    readonly workspace: string;
    readonly loseOwner: Effect.Effect<void>;
    readonly captureSessionUse: ReturnType<typeof captureRuntimeUseSession>;
    readonly publishSession: (
      status: "ready" | "stopped",
    ) => Effect.Effect<void, EventSink.EventSinkV2Error>;
    readonly publishRequest: (
      status: "pending" | "resolved" | "expired",
      responseSession?: ProviderSessionId,
    ) => Effect.Effect<void, EventSink.EventSinkV2Error>;
  }) => Effect.Effect<A, E, SqlClient.SqlClient | EffectOutbox.EffectOutboxV2>,
  openSucceeds = false,
  rootWorkspace = false,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.realPath(
        yield* fs.makeTempDirectoryScoped({ prefix: "runtime-use-manager-" }),
      );
      const stateDir = NodePath.join(root, "userdata");
      const workspace = NodePath.join(root, "workspace");
      yield* fs.makeDirectory(stateDir);
      yield* fs.makeDirectory(workspace);
      const dbPath = NodePath.join(stateDir, "statev2.sqlite");
      return yield* Effect.gen(function* () {
        const owner = yield* Ownership.acquireServerOwnership(
          NodePath.join(stateDir, "server-runtime.json"),
        );
        const persistence = Layer.effectDiscard(runMigrations()).pipe(
          Layer.provideMerge(NodeSqliteClient.layer({ filename: dbPath })),
        );
        const stores = Layer.mergeAll(
          EventStore.layer,
          ProjectionStore.layer,
          ProjectStore.layer,
        ).pipe(Layer.provideMerge(persistence));
        const sink = EventSink.layer.pipe(Layer.provideMerge(stores));
        let calls = 0;
        let nativeCalls = 0;
        const nativeTargets: (string | null)[] = [];
        let responseCalls = 0;
        const optionalCalls: string[] = [];
        let producerGeneration = 0;
        let beforeProducer: Effect.Effect<void, RuntimeUseError> = Effect.void;
        let beforeClear: Effect.Effect<void> = Effect.void;
        const makeAdapter = (sql: SqlClient.SqlClient) =>
          Adapter.ProviderAdapterV2.of({
            instanceId,
            driver: ProviderDriverKind.make("codex"),
            getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
            planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
            openSession: (input) =>
              Effect.gen(function* () {
                assert.strictEqual(input.nativeCreationGuard, undefined);
                assert.isTrue((yield* Effect.serviceOption(WorkspaceLifecycle))._tag === "None");
                const pending = yield* (yield* makeRuntimeUseStore).pending().pipe(
                  Effect.mapError(
                    (cause) =>
                      new Adapter.ProviderAdapterOpenSessionError({
                        driver: ProviderDriverKind.make("codex"),
                        providerSessionId: input.providerSessionId,
                        cause,
                      }),
                  ),
                );
                assert.strictEqual(
                  pending.length,
                  1,
                  "durable pending precedes the actual adapter boundary",
                );
                assert.strictEqual(pending[0]?.threadId, threadId);
                assert.strictEqual(pending[0]?.canonicalPath, workspace);
                calls++;
                if (openSucceeds) {
                  const now = yield* DateTime.now;
                  const lifetime = yield* Effect.serviceOption(RuntimeUseLifetime);
                  const unused = Effect.die("Unused synthetic runtime operation was invoked");
                  const optionalBoundary = Effect.fn("RuntimeUseManager.testOptionalBoundary")(
                    (operation: string) =>
                      Effect.sync(() => {
                        optionalCalls.push(operation);
                      }).pipe(
                        Effect.andThen(
                          Effect.fail(
                            new Adapter.ProviderAdapterProtocolError({
                              driver: ProviderDriverKind.make("codex"),
                              detail: "Synthetic optional boundary entered without native dispatch",
                            }),
                          ),
                        ),
                      ),
                  );
                  const runtime: Adapter.ProviderAdapterV2SessionRuntime = {
                    instanceId,
                    driver: ProviderDriverKind.make("codex"),
                    providerSessionId: sessionId,
                    providerSession: {
                      id: sessionId,
                      driver: ProviderDriverKind.make("codex"),
                      providerInstanceId: instanceId,
                      status: "ready",
                      cwd: workspace,
                      model: modelSelection.model,
                      capabilities: CodexProviderCapabilitiesV2,
                      createdAt: now,
                      updatedAt: now,
                      lastError: null,
                    },
                    events: Stream.never,
                    ensureThread: (request) =>
                      Effect.gen(function* () {
                        yield* beforeProducer;
                        if (Option.isSome(lifetime)) {
                          const producer = yield* lifetime.value.beginProducer({
                            driver: "codex",
                            instanceId,
                            providerSessionId: sessionId,
                            threadId: request.threadId,
                            generation: `synthetic-producer-${++producerGeneration}`,
                          });
                          yield* producer.beforeSpawn;
                        }
                        nativeCalls++;
                        nativeTargets.push(request.runtimePolicy.cwd);
                        return yield* new Adapter.ProviderAdapterEnsureThreadError({
                          driver: ProviderDriverKind.make("codex"),
                          threadId: request.threadId,
                          cause:
                            "synthetic adapter boundary entered; no provider request dispatched",
                        });
                      }).pipe(
                        Effect.mapError((cause) =>
                          cause._tag === "RuntimeUseError"
                            ? new Adapter.ProviderAdapterEnsureThreadError({
                                driver: ProviderDriverKind.make("codex"),
                                threadId: request.threadId,
                                cause,
                              })
                            : cause,
                        ),
                      ),
                    resumeThread: () => unused,
                    injectHistory: () => optionalBoundary("injectHistory"),
                    compactThread: () => optionalBoundary("compactThread"),
                    startTurn: () => unused,
                    steerTurn: () => unused,
                    interruptTurn: () => unused,
                    respondToRuntimeRequest: (request) =>
                      Effect.sync(() => {
                        responseCalls++;
                      }).pipe(
                        Effect.andThen(
                          Effect.fail(
                            new Adapter.ProviderAdapterRuntimeRequestResponseError({
                              driver: ProviderDriverKind.make("codex"),
                              requestId: request.requestId,
                              cause:
                                "synthetic response boundary entered; no provider request dispatched",
                            }),
                          ),
                        ),
                      ),
                    readThreadSnapshot: () => unused,
                    rollbackThread: () => optionalBoundary("rollbackThread"),
                    forkThread: () => unused,
                    mcpApps: {
                      listTools: () => unused,
                      readResource: () => unused,
                      callTool: () => optionalBoundary("mcpApps.callTool"),
                    },
                    controlGoal: (request) =>
                      Effect.sync(() => {
                        optionalCalls.push(`controlGoal.${request.action}`);
                        return {
                          outcome: "unsupported" as const,
                          goal: null,
                          reason: "synthetic-no-native-goal-control",
                        };
                      }),
                  };
                  return runtime;
                }
                // The external call has been entered. Failure before attachment must
                // never erase its possible effect, even if session scope close succeeds.
                return yield* new Adapter.ProviderAdapterOpenSessionError({
                  driver: ProviderDriverKind.make("codex"),
                  providerSessionId: input.providerSessionId,
                  cause: "synthetic entered open failed before attachment",
                });
              }).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
          });
        const registry = Layer.unwrap(
          Effect.map(SqlClient.SqlClient, (sql) =>
            ProviderAdapterRegistry.layerSingle(makeAdapter(sql)),
          ),
        ).pipe(Layer.provideMerge(persistence));
        const dependencies = Layer.mergeAll(
          sink,
          EffectOutbox.layer.pipe(Layer.provideMerge(persistence)),
          IdAllocator.layer.pipe(Layer.provideMerge(persistence)),
          CommandOutputHub.layer.pipe(Layer.provideMerge(sink)),
          registry,
          McpProviderSessions.layer,
          // These unused capabilities die on invocation. No fake ingestion or MCP
          // result substitutes for the actual journal/manager/EventSink/SQL path.
          Layer.mock(ProviderEventIngestor.ProviderEventIngestorV2)({}),
          Layer.mock(McpSessionRegistry.McpSessionRegistry)({}),
        );
        const manager = Manager.layerWithOptions({ configureMcp: false }).pipe(
          Layer.provideMerge(dependencies),
          Layer.provideMerge(persistence),
          Layer.provide(Layer.succeed(HeldRuntimeOwner, { token: owner.heldRuntimeOwner })),
          Layer.provide(
            Layer.succeed(AgentScope, {
              wrap: ({ command, args }) => Effect.succeed({ command, args }),
              oomKilled: () => Effect.succeed(false),
              stop: () => Effect.void,
              kill: () => Effect.succeed(false),
              clear: () => Effect.suspend(() => beforeClear),
            }),
          ),
        );
        return yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const projects = yield* ProjectStore.ProjectStoreV2;
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const eventSink = yield* EventSink.EventSinkV2;
          const sessions = yield* Manager.ProviderSessionManagerV2;
          const attachmentOutbox = yield* EffectOutbox.EffectOutboxV2;
          const now = yield* DateTime.now;
          const iso = DateTime.formatIso(now);
          let sessionEvent = 0;
          yield* sql.withTransaction(
            projects.apply({
              sequence: 1,
              eventId: EventId.make("runtime-use-project-event"),
              aggregateKind: "project",
              aggregateId: projectId,
              occurredAt: iso,
              commandId: null,
              causationEventId: null,
              correlationId: null,
              metadata: {},
              type: "project.created",
              payload: {
                projectId,
                title: "Synthetic runtime use",
                workspaceRoot: workspace,
                defaultModelSelection: null,
                scripts: [],
                createdAt: iso,
                updatedAt: iso,
              },
            }),
          );
          yield* eventSink.write({
            events: [
              {
                id: EventId.make("runtime-use-thread-event"),
                type: "thread.created",
                threadId,
                occurredAt: now,
                payload: {
                  id: threadId,
                  projectId,
                  title: "Synthetic runtime use",
                  createdBy: "user",
                  creationSource: "web",
                  providerInstanceId: instanceId,
                  modelSelection,
                  runtimeMode: "full-access",
                  interactionMode: "default",
                  worktreePath: rootWorkspace ? null : workspace,
                  branch: null,
                  activeProviderThreadId: null,
                  lineage: {
                    parentThreadId: null,
                    relationshipToParent: null,
                    rootThreadId: threadId,
                  },
                  forkedFrom: null,
                  createdAt: now,
                  updatedAt: now,
                  archivedAt: null,
                  settledOverride: null,
                  settledAt: null,
                  lastVisitedAt: null,
                  deletedAt: null,
                },
              },
            ],
          });
          return yield* test({
            attachmentPublicationRace: Effect.fn("RuntimeUseManager.testAttachmentPublication")(
              function* (drift: "target" | "claim" | "none" | "owner" | "directory") {
                const childId = ThreadId.make("runtime-use-attachment-race-child");
                const parent = yield* projections.getThread(threadId);
                const target = NodePath.join(root, "attachment-raced-target");
                yield* fs.makeDirectory(target);
                const child = {
                  ...parent,
                  id: childId,
                  title: "Synthetic attachment",
                  lineage: {
                    parentThreadId: null,
                    relationshipToParent: null,
                    rootThreadId: childId,
                  },
                };
                yield* eventSink.write({
                  events: [
                    {
                      id: EventId.make(`attachment-birth-${++sessionEvent}`),
                      type: "thread.created",
                      threadId: childId,
                      occurredAt: now,
                      payload: child,
                    },
                  ],
                });
                let operation = sessions.open({
                  threadId: childId,
                  providerSessionId: sessionId,
                  modelSelection,
                  runtimePolicy: {
                    cwd: workspace,
                    runtimeMode: "full-access",
                    interactionMode: "default",
                  },
                });
                let clears = 0;
                const changed: Effect.Effect<void, RuntimeUseError> =
                  drift === "target"
                    ? eventSink
                        .write({
                          events: [
                            {
                              id: EventId.make(`attachment-drift-${++sessionEvent}`),
                              type: "thread.metadata-updated",
                              threadId: childId,
                              occurredAt: now,
                              payload: { ...child, worktreePath: target, updatedAt: now },
                            },
                          ],
                        })
                        .pipe(Effect.asVoid, Effect.mapError(runtimeUseError))
                    : drift === "claim"
                      ? sql`UPDATE orchestration_v2_effect_outbox SET lease_owner='replacement-attachment-worker' WHERE effect_id='original-attachment-effect'`.pipe(
                          Effect.asVoid,
                          Effect.mapError(runtimeUseError),
                        )
                      : drift === "owner"
                        ? Effect.sync(() =>
                            invalidateCheckedServerOwnership(owner.heldRuntimeOwner),
                          )
                        : drift === "directory"
                          ? fs
                              .rename(workspace, NodePath.join(root, "workspace-before-attachment"))
                              .pipe(
                                Effect.andThen(fs.makeDirectory(workspace)),
                                Effect.mapError(runtimeUseError),
                              )
                          : Effect.void;
                if (drift === "claim") {
                  yield* attachmentOutbox.enqueue([
                    {
                      id: "original-attachment-effect",
                      commandId: CommandId.make("original-attachment-command"),
                      threadId: childId,
                      request: {
                        type: "provider-turn.start",
                        runId: RunId.make("original-attachment-run"),
                      },
                    },
                  ]);
                  const claimed = yield* attachmentOutbox.claimNext({
                    workerId: "original-attachment-worker",
                  });
                  assert.isTrue(Option.isSome(claimed));
                  if (Option.isNone(claimed))
                    return yield* Effect.die("Expected actual attachment claim");
                  operation = withRuntimeUseClaim(
                    claimed.value,
                    "original-attachment-worker",
                    operation,
                  );
                }
                beforeClear = changed.pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      clears++;
                    }),
                  ),
                  Effect.asVoid,
                  Effect.orDie,
                );
                const result = yield* Effect.exit(operation).pipe(
                  Effect.ensuring(
                    Effect.sync(() => {
                      beforeClear = Effect.void;
                    }),
                  ),
                );
                const bindings =
                  yield* sql`SELECT thread_id FROM orchestration_v2_projection_provider_session_bindings WHERE provider_session_id=${sessionId} AND thread_id=${childId}`;
                return { refused: Exit.isFailure(result), clears, bindings: bindings.length };
              },
              Effect.mapError(runtimeUseError),
            ),
            reopenAt: (cwd) =>
              sessions.open({
                threadId,
                providerSessionId: sessionId,
                modelSelection,
                runtimePolicy: { cwd, runtimeMode: "full-access", interactionMode: "default" },
              }),
            open: sessions.open({
              threadId,
              providerSessionId: sessionId,
              modelSelection,
              runtimePolicy: {
                cwd: workspace,
                runtimeMode: "full-access",
                interactionMode: "default",
              },
            }),
            calls: () => calls,
            nativeCalls: () => nativeCalls,
            nativeTargets: () => [...nativeTargets],
            responseCalls: () => responseCalls,
            optionalCalls: () => [...optionalCalls],
            publishProviderThread: Effect.gen(function* () {
              const providerThread: OrchestrationV2ProviderThread = {
                id: ProviderThreadId.make("runtime-use-bound-provider-thread"),
                driver: ProviderDriverKind.make("codex"),
                providerInstanceId: instanceId,
                providerSessionId: sessionId,
                appThreadId: threadId,
                ownerNodeId: null,
                nativeThreadRef: {
                  driver: ProviderDriverKind.make("codex"),
                  nativeId: "synthetic-native-thread",
                  strength: "strong",
                },
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                createdAt: now,
                updatedAt: now,
                runtimeIdentity: {
                  runtimeGeneration: "synthetic-bound-generation",
                  requested: Adapter.requestedRuntimeIdentity(
                    modelSelection,
                    ProviderDriverKind.make("codex"),
                  ),
                  observed: Adapter.unobservedRuntimeIdentity(),
                },
              };
              yield* eventSink.write({
                events: [
                  {
                    id: EventId.make(`runtime-use-bound-provider-${++sessionEvent}`),
                    type: "provider-thread.updated",
                    threadId,
                    occurredAt: now,
                    payload: providerThread,
                  },
                ],
              });
              return providerThread;
            }),
            setBeforeProducer: (hook) => {
              beforeProducer = hook;
            },
            updateWorkspace: Effect.fn("RuntimeUseManager.testUpdateWorkspace")(function* (
              path: string,
            ) {
              assert.strictEqual(NodePath.dirname(path), root);
              yield* fs.makeDirectory(path);
              const current = yield* projections.getThread(threadId);
              if (current === undefined) return yield* Effect.die("Expected original app thread");
              yield* eventSink.write({
                events: [
                  {
                    id: EventId.make(`runtime-use-workspace-${++sessionEvent}`),
                    type: "thread.metadata-updated",
                    threadId,
                    occurredAt: now,
                    payload: { ...current, worktreePath: path, updatedAt: now },
                  },
                ],
              });
            }, Effect.mapError(runtimeUseError)),
            dbPath,
            workspace,
            loseOwner: Effect.sync(() => invalidateCheckedServerOwnership(owner.heldRuntimeOwner)),
            captureSessionUse: bindHeldDatabaseOwner(owner.heldRuntimeOwner).pipe(
              Effect.mapError(runtimeUseError),
              Effect.flatMap((token) =>
                captureRuntimeUseSession({
                  owner: token,
                  threadId,
                  providerSessionId: sessionId,
                  instanceId,
                  driver: "codex",
                }),
              ),
            ),
            publishSession: (status) =>
              eventSink
                .write({
                  events: [
                    {
                      id: EventId.make(`runtime-use-session-${++sessionEvent}`),
                      type: "provider-session.attached",
                      threadId,
                      occurredAt: now,
                      payload: {
                        id: sessionId,
                        driver: ProviderDriverKind.make("codex"),
                        providerInstanceId: instanceId,
                        status,
                        cwd: workspace,
                        model: modelSelection.model,
                        capabilities: CodexProviderCapabilitiesV2,
                        createdAt: now,
                        updatedAt: now,
                        lastError: null,
                      },
                    },
                  ],
                })
                .pipe(Effect.asVoid),
            publishRequest: (status, responseSession = sessionId) =>
              eventSink
                .write({
                  events: [
                    {
                      id: EventId.make(`runtime-use-request-${++sessionEvent}`),
                      type: "runtime-request.updated",
                      threadId,
                      occurredAt: now,
                      payload: {
                        id: requestId,
                        nodeId: NodeId.make("runtime-use-request-node"),
                        providerTurnId: null,
                        nativeRequestRef: null,
                        kind: "user_input",
                        status,
                        responseCapability: { type: "live", providerSessionId: responseSession },
                        createdAt: now,
                        resolvedAt: status === "resolved" ? now : null,
                        decision: "decline",
                      },
                    },
                  ],
                })
                .pipe(Effect.asVoid),
          });
        }).pipe(Effect.provide(manager));
      }).pipe(
        Effect.provideService(HostProcess.HomeDirectory, root),
        Effect.provideService(HostProcess.Environment, {}),
      );
    }).pipe(Effect.provide(platform)),
  );

describe("actual raw manager durable pre-effect open", () => {
  it.effect(
    "optional native mutations preserve valid dispatch and reject foreign or stale bound conversations",
    () =>
      fixture(
        (input) =>
          Effect.gen(function* () {
            const runtime = yield* input.open;
            const providerThread = yield* input.publishProviderThread;
            if (
              runtime.injectHistory === undefined ||
              runtime.mcpApps === undefined ||
              runtime.controlGoal === undefined
            )
              return yield* Effect.die("Expected explicit synthetic optional capabilities");
            yield* refused(
              runtime.injectHistory({ providerThread, messages: [], context: "Synthetic history" }),
            );
            yield* refused(
              runtime.rollbackThread({
                providerThread,
                target: {
                  type: "thread_start",
                  checkpointId: CheckpointId.make("synthetic-start-checkpoint"),
                  appRunOrdinal: 0,
                },
                providerThreadTurns: [],
              }),
            );
            yield* refused(
              runtime.mcpApps.callTool({
                providerThread,
                server: "synthetic-server",
                tool: "synthetic-tool",
                arguments: {},
              }),
            );
            assert.deepStrictEqual(input.optionalCalls(), [
              "injectHistory",
              "rollbackThread",
              "mcpApps.callTool",
            ]);
            const foreign = {
              ...providerThread,
              providerSessionId: ProviderSessionId.make("foreign-native-session"),
            };
            yield* refused(
              runtime.injectHistory({
                providerThread: foreign,
                messages: [],
                context: "Refused history",
              }),
            );
            if (providerThread.runtimeIdentity === undefined)
              return yield* Effect.die("Expected explicit bound generation");
            const stale = {
              ...providerThread,
              runtimeIdentity: {
                ...providerThread.runtimeIdentity,
                runtimeGeneration: "retired-generation",
              },
            };
            yield* refused(
              runtime.mcpApps.callTool({
                providerThread: stale,
                server: "synthetic-server",
                tool: "synthetic-tool",
                arguments: {},
              }),
            );
            assert.strictEqual(input.optionalCalls().length, 3);
            const sql = yield* SqlClient.SqlClient;
            yield* sql`UPDATE orchestration_v2_projection_provider_sessions SET status='stopped',payload_json=json_set(payload_json,'$.status','stopped')
      WHERE provider_session_id=${sessionId}`;
            const replaced = yield* runtime.controlGoal({
              providerThread,
              action: "replace",
              objective: "Synthetic replacement",
              revalidate: Effect.succeed(true),
            });
            assert.strictEqual(replaced.outcome, "unavailable");
            assert.strictEqual(input.optionalCalls().length, 3);
            const paused = yield* runtime.controlGoal({
              providerThread,
              action: "pause",
              revalidate: Effect.succeed(true),
            });
            assert.strictEqual(paused.outcome, "unsupported");
            assert.deepStrictEqual(input.optionalCalls(), [
              "injectHistory",
              "rollbackThread",
              "mcpApps.callTool",
              "controlGoal.pause",
            ]);
          }),
        true,
      ),
  );
  it.effect(
    "a new workspace attachment reserves separate coverage before the same live session can use it",
    () =>
      fixture(
        (input) =>
          Effect.gen(function* () {
            const runtime = yield* input.open;
            const additional = input.workspace + "-additional";
            yield* input.updateWorkspace(additional);
            yield* refused(
              runtime.ensureThread({
                threadId,
                modelSelection,
                runtimePolicy: {
                  cwd: additional,
                  runtimeMode: "full-access",
                  interactionMode: "default",
                },
              }),
            );
            assert.strictEqual(input.nativeCalls(), 1);
            assert.deepStrictEqual(input.nativeTargets(), [additional]);
            const store = yield* makeRuntimeUseStore;
            const pending = yield* store.pending();
            assert.strictEqual(pending.length, 2);
            assert.deepStrictEqual(
              pending.map((item) => item.canonicalPath).sort(),
              [input.workspace, additional].sort(),
            );
            for (const binding of pending) {
              const record = yield* store.read(binding.useId);
              assert.strictEqual(record?.producers.length, 1);
              assert.strictEqual(record?.producers[0]?.generation, "synthetic-producer-1");
            }
          }),
        true,
      ),
  );
  it.effect(
    "root-project sessions validate their actual project checkout without requiring a worktree path",
    () =>
      fixture(
        (input) =>
          Effect.gen(function* () {
            const runtime = yield* input.open;
            yield* refused(
              runtime.ensureThread({
                threadId,
                modelSelection,
                runtimePolicy: {
                  cwd: input.workspace,
                  runtimeMode: "full-access",
                  interactionMode: "default",
                },
              }),
            );
            assert.strictEqual(input.calls(), 1);
            assert.strictEqual(input.nativeCalls(), 1);
            assert.strictEqual(
              (yield* (yield* makeRuntimeUseStore).pending())[0]?.canonicalPath,
              input.workspace,
            );
          }),
        true,
        true,
      ),
  );
  it.effect(
    "an invocation cannot substitute an unrelated or unproved default cwd for its bound server workspace",
    () =>
      fixture(
        (input) =>
          Effect.gen(function* () {
            const runtime = yield* input.open;
            for (const cwd of [input.workspace + "-unrelated", null]) {
              yield* refused(
                runtime.ensureThread({
                  threadId,
                  modelSelection,
                  runtimePolicy: { cwd, runtimeMode: "full-access", interactionMode: "default" },
                }),
              );
            }
            assert.strictEqual(input.nativeCalls(), 0);
            assert.strictEqual((yield* (yield* makeRuntimeUseStore).pending()).length, 1);
          }),
        true,
      ),
  );
  it.effect(
    "a later independent producer freezes its own caller after the original opening effect completes",
    () =>
      fixture(
        (input) =>
          Effect.gen(function* () {
            const outbox = yield* EffectOutbox.EffectOutboxV2;
            const sql = yield* SqlClient.SqlClient;
            yield* outbox.enqueue([
              {
                id: "original-opening-effect",
                commandId: CommandId.make("original-opening-command"),
                threadId,
                request: { type: "provider-turn.start", runId: RunId.make("original-opening-run") },
              },
            ]);
            const opening = yield* outbox.claimNext({ workerId: "original-opening-worker" });
            if (Option.isNone(opening)) return yield* Effect.die("Expected original opening claim");
            const runtime = yield* withRuntimeUseClaim(
              opening.value,
              "original-opening-worker",
              input.open,
            );
            yield* sql`UPDATE orchestration_v2_effect_outbox SET status='succeeded' WHERE effect_id='original-opening-effect'`;
            yield* outbox.enqueue([
              {
                id: "later-producer-effect",
                commandId: CommandId.make("later-producer-command"),
                threadId,
                request: { type: "provider-turn.start", runId: RunId.make("later-producer-run") },
              },
            ]);
            const later = yield* outbox.claimNext({ workerId: "later-producer-worker" });
            if (Option.isNone(later)) return yield* Effect.die("Expected later producer claim");
            yield* refused(
              withRuntimeUseClaim(
                later.value,
                "later-producer-worker",
                runtime.ensureThread({
                  threadId,
                  modelSelection,
                  runtimePolicy: {
                    cwd: input.workspace,
                    runtimeMode: "full-access",
                    interactionMode: "default",
                  },
                }),
              ),
            );
            assert.strictEqual(input.nativeCalls(), 1);
            const pending = (yield* (yield* makeRuntimeUseStore).pending())[0];
            assert.strictEqual(pending?.original.claim?.workerId, "original-opening-worker");
            const record = yield* (yield* makeRuntimeUseStore).read(pending!.useId);
            assert.strictEqual(record?.producers.length, 1);
            assert.strictEqual(record?.producers[0]?.threadId, threadId);
          }),
        true,
      ),
  );
  it.effect(
    "caller drift after manager preparation refuses the producer observation and spawn boundary",
    () =>
      fixture(
        (input) =>
          Effect.gen(function* () {
            const runtime = yield* input.open;
            const outbox = yield* EffectOutbox.EffectOutboxV2;
            const sql = yield* SqlClient.SqlClient;
            yield* outbox.enqueue([
              {
                id: "drifting-producer-effect",
                commandId: CommandId.make("drifting-producer-command"),
                threadId,
                request: {
                  type: "provider-turn.start",
                  runId: RunId.make("drifting-producer-run"),
                },
              },
            ]);
            const claimed = yield* outbox.claimNext({ workerId: "captured-producer-worker" });
            if (Option.isNone(claimed))
              return yield* Effect.die("Expected captured producer claim");
            input.setBeforeProducer(
              sql`UPDATE orchestration_v2_effect_outbox SET lease_owner='replacement-producer-worker'
      WHERE effect_id='drifting-producer-effect'`.pipe(
                Effect.asVoid,
                Effect.mapError(runtimeUseError),
              ),
            );
            yield* refused(
              withRuntimeUseClaim(
                claimed.value,
                "captured-producer-worker",
                runtime.ensureThread({
                  threadId,
                  modelSelection,
                  runtimePolicy: {
                    cwd: input.workspace,
                    runtimeMode: "full-access",
                    interactionMode: "default",
                  },
                }),
              ),
            );
            assert.strictEqual(input.nativeCalls(), 0);
            const pending = (yield* (yield* makeRuntimeUseStore).pending())[0];
            assert.strictEqual(
              (yield* (yield* makeRuntimeUseStore).read(pending!.useId))?.producers.length,
              0,
            );
          }),
        true,
      ),
  );
  it.effect(
    "runtime response requires the exact resolved request, recorded answer and original live session",
    () =>
      fixture(
        (input) =>
          Effect.gen(function* () {
            const runtime = yield* input.open;
            yield* refused(runtime.respondToRuntimeRequest({ requestId, decision: "decline" }));
            assert.strictEqual(input.responseCalls(), 0);
            yield* input.publishRequest("pending");
            yield* refused(runtime.respondToRuntimeRequest({ requestId, decision: "decline" }));
            yield* input.publishRequest("resolved", ProviderSessionId.make("foreign-session"));
            yield* refused(runtime.respondToRuntimeRequest({ requestId, decision: "decline" }));
            yield* input.publishRequest("resolved");
            yield* refused(runtime.respondToRuntimeRequest({ requestId, decision: "accept" }));
            assert.strictEqual(input.responseCalls(), 0);
            yield* refused(runtime.respondToRuntimeRequest({ requestId, decision: "decline" }));
            assert.strictEqual(input.responseCalls(), 1);
            yield* input.publishRequest("expired");
            yield* refused(runtime.respondToRuntimeRequest({ requestId, decision: "decline" }));
            assert.strictEqual(input.responseCalls(), 1);
          }),
        true,
      ),
  );
  it.effect(
    "runtime response validates the captured worker's exact effect intent before dispatch",
    () =>
      fixture(
        (input) =>
          Effect.gen(function* () {
            const runtime = yield* input.open;
            yield* input.publishRequest("resolved");
            const outbox = yield* EffectOutbox.EffectOutboxV2;
            yield* outbox.enqueue([
              {
                id: "original-response-effect",
                commandId: CommandId.make("original-response-command"),
                threadId,
                request: {
                  type: "runtime-request.respond",
                  providerSessionId: sessionId,
                  requestId,
                  decision: "decline",
                },
              },
            ]);
            const claimed = yield* outbox.claimNext({ workerId: "original-response-worker" });
            if (Option.isNone(claimed))
              return yield* Effect.die("Expected original response claim");
            yield* refused(
              withRuntimeUseClaim(
                claimed.value,
                "original-response-worker",
                runtime.respondToRuntimeRequest({ requestId, decision: "decline" }),
              ),
            );
            assert.strictEqual(input.responseCalls(), 1);
            const sql = yield* SqlClient.SqlClient;
            yield* sql`UPDATE orchestration_v2_effect_outbox SET lease_owner='replacement-response-worker' WHERE effect_id='original-response-effect'`;
            yield* refused(
              withRuntimeUseClaim(
                claimed.value,
                "original-response-worker",
                runtime.respondToRuntimeRequest({ requestId, decision: "decline" }),
              ),
            );
            assert.strictEqual(input.responseCalls(), 1);
          }),
        true,
      ),
  );
  it.effect(
    "raw invocation with managed lifecycle absent reaches only its actual same-session attachment",
    () =>
      fixture(
        (input) =>
          Effect.gen(function* () {
            const runtime = yield* input.open;
            yield* refused(
              runtime.ensureThread({
                threadId,
                modelSelection,
                runtimePolicy: {
                  cwd: input.workspace,
                  runtimeMode: "full-access",
                  interactionMode: "default",
                },
              }),
            );
            assert.strictEqual(input.calls(), 1);
            assert.strictEqual(input.nativeCalls(), 1);
            const sql = yield* SqlClient.SqlClient;
            // A supported separate writer has detached the persisted association while
            // the old in-memory runtime still exists. It cannot authorize another call.
            yield* sql`DELETE FROM orchestration_v2_projection_provider_session_bindings WHERE provider_session_id=${sessionId} AND thread_id=${threadId}`;
            yield* sql`UPDATE orchestration_v2_projection_provider_sessions SET status='stopped',payload_json=json_set(payload_json,'$.status','stopped') WHERE provider_session_id=${sessionId}`;
            yield* refused(
              runtime.ensureThread({
                threadId,
                modelSelection,
                runtimePolicy: {
                  cwd: input.workspace,
                  runtimeMode: "full-access",
                  interactionMode: "default",
                },
              }),
            );
            assert.strictEqual(input.nativeCalls(), 1);
            assert.strictEqual((yield* (yield* makeRuntimeUseStore).pending()).length, 1);
          }),
        true,
      ),
  );
  it.effect(
    "raw invocation refuses a replaced session birth and lost actual owner before the adapter boundary",
    () =>
      fixture(
        (input) =>
          Effect.gen(function* () {
            const runtime = yield* input.open;
            const sql = yield* SqlClient.SqlClient;
            yield* sql`UPDATE orchestration_v2_projection_provider_sessions SET payload_json=json_set(payload_json,'$.createdAt','2000-01-01T00:00:00.000Z') WHERE provider_session_id=${sessionId}`;
            yield* refused(
              runtime.ensureThread({
                threadId,
                modelSelection,
                runtimePolicy: {
                  cwd: input.workspace,
                  runtimeMode: "full-access",
                  interactionMode: "default",
                },
              }),
            );
            assert.strictEqual(input.nativeCalls(), 0);
            yield* input.loseOwner;
            yield* refused(
              runtime.ensureThread({
                threadId,
                modelSelection,
                runtimePolicy: {
                  cwd: input.workspace,
                  runtimeMode: "full-access",
                  interactionMode: "default",
                },
              }),
            );
            assert.strictEqual(input.nativeCalls(), 0);
          }),
        true,
      ),
  );
  it.effect(
    "invocation capture validates the actual persisted session and exact attachment in its owning database",
    () =>
      fixture((input) =>
        Effect.gen(function* () {
          yield* refused(input.captureSessionUse);
          yield* input.publishSession("ready");
          const use = yield* input.captureSessionUse;
          const sql = yield* SqlClient.SqlClient;
          yield* sql.withTransaction(use.validateAttachment);
          assert.strictEqual(use.workspace.canonicalPath, input.workspace);
          assert.strictEqual(use.snapshot.session.id, sessionId);
          yield* input.publishSession("stopped");
          yield* refused(sql.withTransaction(use.validateAttachment));
          yield* refused(input.captureSessionUse);
          assert.strictEqual(input.calls(), 0);
        }),
      ),
  );
  it.effect(
    "same-session identity drift and a missing exact thread attachment refuse an invocation before a provider call",
    () =>
      fixture((input) =>
        Effect.gen(function* () {
          yield* input.publishSession("ready");
          const use = yield* input.captureSessionUse;
          const sql = yield* SqlClient.SqlClient;
          yield* sql`DELETE FROM orchestration_v2_projection_provider_session_bindings WHERE provider_session_id=${sessionId} AND thread_id=${threadId}`;
          yield* refused(sql.withTransaction(use.validateAttachment));
          yield* input.publishSession("ready");
          yield* sql`UPDATE orchestration_v2_projection_provider_sessions SET payload_json=json_set(payload_json,'$.createdAt','2000-01-01T00:00:00.000Z') WHERE provider_session_id=${sessionId}`;
          yield* refused(sql.withTransaction(use.validateAttachment));
          assert.strictEqual(input.calls(), 0);
        }),
      ),
  );
  it.effect(
    "a second connection's unresolved deletion refuses the real raw adapter before entry",
    () =>
      fixture((input) =>
        Effect.gen(function* () {
          yield* Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            // Storage-only reservation fixture, not a fabricated deletion permit.
            yield* sql`INSERT INTO jones_deletion_worktree_admissions VALUES('synthetic-original',${input.workspace},'{}','{}','started')`;
          }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: input.dbPath })));
          yield* refused(input.open);
          assert.strictEqual(input.calls(), 0);
          assert.deepStrictEqual(yield* (yield* makeRuntimeUseStore).pending(), []);
        }),
      ),
  );
  it.effect(
    "a pending raw open and a second-connection deletion race admit exactly one durable side",
    () =>
      fixture((input) =>
        Effect.gen(function* () {
          const deletion = Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            yield* sql`INSERT INTO jones_deletion_worktree_admissions VALUES('synthetic-race',${input.workspace},'{}','{}','started')`;
          }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: input.dbPath })));
          yield* Effect.all([Effect.exit(input.open), Effect.exit(deletion)], { concurrency: 2 });
          const sql = yield* SqlClient.SqlClient;
          const pending = yield* (yield* makeRuntimeUseStore).pending();
          const deletions = yield* sql`SELECT effect_id FROM jones_deletion_worktree_admissions`;
          assert.strictEqual(pending.length + deletions.length, 1);
          assert.strictEqual(input.calls(), pending.length);
        }),
      ),
  );
  it.effect("a failed entered open durably blocks a later second-connection deletion", () =>
    fixture((input) =>
      Effect.gen(function* () {
        yield* refused(input.open);
        assert.strictEqual(input.calls(), 1);
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* refused(
            sql`INSERT INTO jones_deletion_worktree_admissions VALUES('synthetic-late',${input.workspace},'{}','{}','started')`,
          );
          assert.deepStrictEqual(
            yield* sql`SELECT effect_id FROM jones_deletion_worktree_admissions`,
            [],
          );
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: input.dbPath })));
      }),
    ),
  );
  it.effect(
    "records the actual worker generation and original request before the raw adapter boundary",
    () =>
      fixture((input) =>
        Effect.gen(function* () {
          const outbox = yield* EffectOutbox.EffectOutboxV2;
          yield* outbox.enqueue([
            {
              id: "actual-owned-start",
              commandId: CommandId.make("actual-owned-command"),
              threadId,
              request: { type: "provider-turn.start", runId: RunId.make("actual-owned-run") },
            },
          ]);
          const claimed = yield* outbox.claimNext({ workerId: "actual-owned-worker" });
          if (Option.isNone(claimed)) return yield* Effect.die("Expected actual worker claim");
          yield* refused(withRuntimeUseClaim(claimed.value, "actual-owned-worker", input.open));
          assert.strictEqual(input.calls(), 1);
          const [pending] = yield* (yield* makeRuntimeUseStore).pending();
          assert.strictEqual(pending?.original.commandId, "actual-owned-command");
          assert.strictEqual(pending?.original.runId, "actual-owned-run");
          assert.strictEqual(pending?.original.claim?.workerId, "actual-owned-worker");
          assert.strictEqual(pending?.original.claim?.attemptCount, 1);
          assert.strictEqual(pending?.original.claim?.claimedAt, claimed.value.updatedAt);
          yield* refused(withRuntimeUseClaim(claimed.value, "actual-owned-worker", input.open));
          assert.strictEqual(input.calls(), 1);
        }),
      ),
  );
  it.effect("a stale captured caller cannot borrow a different current worker's claim", () =>
    fixture((input) =>
      Effect.gen(function* () {
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        yield* outbox.enqueue([
          {
            id: "stale-owned-start",
            commandId: CommandId.make("stale-owned-command"),
            threadId,
            request: { type: "provider-turn.start", runId: RunId.make("stale-owned-run") },
          },
        ]);
        const claimed = yield* outbox.claimNext({ workerId: "original-worker" });
        if (Option.isNone(claimed)) return yield* Effect.die("Expected actual worker claim");
        const sql = yield* SqlClient.SqlClient;
        yield* sql`UPDATE orchestration_v2_effect_outbox SET lease_owner='replacement-worker' WHERE effect_id='stale-owned-start'`;
        yield* refused(withRuntimeUseClaim(claimed.value, "original-worker", input.open));
        assert.strictEqual(input.calls(), 0);
        assert.deepStrictEqual(yield* (yield* makeRuntimeUseStore).pending(), []);
      }),
    ),
  );
  it.effect(
    "journals with managed lifecycle and native guard absent; failed open cannot silently retry",
    () =>
      fixture((input) =>
        Effect.gen(function* () {
          yield* refused(input.open);
          assert.strictEqual(input.calls(), 1);
          const store = yield* makeRuntimeUseStore;
          const original = yield* store.pending();
          assert.strictEqual(original.length, 1);
          assert.strictEqual(original[0]?.actor.kind, "provider_open");
          yield* refused(input.open);
          assert.strictEqual(input.calls(), 1);
          assert.deepStrictEqual(yield* store.pending(), original);
          const sql = yield* SqlClient.SqlClient;
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM orchestration_v2_projection_provider_sessions`,
            [],
          );
        }),
      ),
  );
  it.effect(
    "an actual reservation transaction refusal produces zero adapter calls and no partial journal",
    () =>
      fixture((input) =>
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`CREATE TRIGGER synthetic_refuse_runtime_reservation BEFORE INSERT ON jones_workspace_runtime_uses
        BEGIN SELECT RAISE(ABORT,'synthetic reservation refusal'); END`;
          yield* refused(input.open);
          assert.strictEqual(input.calls(), 0);
          assert.deepStrictEqual(yield* (yield* makeRuntimeUseStore).pending(), []);
          assert.deepStrictEqual(yield* sql`SELECT * FROM jones_workspace_runtime_use_events`, []);
        }),
      ),
  );
  it.effect("lost actual owner refuses before adapter invocation", () =>
    fixture((input) =>
      Effect.gen(function* () {
        yield* input.loseOwner;
        yield* refused(input.open);
        assert.strictEqual(input.calls(), 0);
        assert.deepStrictEqual(yield* (yield* makeRuntimeUseStore).pending(), []);
      }),
    ),
  );
  it.effect(
    "another actual SQL connection observes the committed pending use after failed attachment",
    () =>
      fixture((input) =>
        Effect.gen(function* () {
          yield* refused(input.open);
          assert.strictEqual(input.calls(), 1);
          const original = yield* (yield* makeRuntimeUseStore).pending();
          yield* Effect.gen(function* () {
            const recovered = yield* makeRuntimeUseStore;
            assert.deepStrictEqual(yield* recovered.pending(), original);
            assert.isFalse((yield* recovered.reserve(original[0]!)).created);
          }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: input.dbPath })));
          assert.strictEqual(input.calls(), 1);
        }),
      ),
  );
  it.effect(
    "workspace-changing open of an existing raw session journals attachment before publication without another adapter open",
    () =>
      fixture(
        (input) =>
          Effect.gen(function* () {
            yield* input.open;
            yield* input.publishSession("ready");
            const target = NodePath.join(NodePath.dirname(input.workspace), "attached-workspace");
            yield* input.updateWorkspace(target);
            yield* input.reopenAt(target);
            assert.strictEqual(input.calls(), 1);
            assert.strictEqual(input.nativeCalls(), 0);
            const store = yield* makeRuntimeUseStore;
            const uses = yield* store.pending();
            assert.strictEqual(uses.length, 2);
            assert.isTrue(
              uses.some(
                (use) =>
                  use.canonicalPath === target && use.useId.startsWith("provider-attach:v1:"),
              ),
            );
            yield* input.reopenAt(target);
            assert.deepStrictEqual(yield* store.pending(), uses);
            assert.strictEqual(input.calls(), 1);
          }),
        true,
      ),
  );
  it.effect(
    "stopped persisted session refuses workspace-changing attachment before reservation or publication",
    () =>
      fixture(
        (input) =>
          Effect.gen(function* () {
            yield* input.open;
            yield* input.publishSession("stopped");
            const target = NodePath.join(NodePath.dirname(input.workspace), "refused-attachment");
            yield* input.updateWorkspace(target);
            yield* refused(input.reopenAt(target));
            assert.strictEqual(input.calls(), 1);
            const uses = yield* (yield* makeRuntimeUseStore).pending();
            assert.strictEqual(uses.length, 1);
            assert.strictEqual(uses[0]?.canonicalPath, input.workspace);
          }),
        true,
      ),
  );
  it.effect(
    "actual attachment publication revalidates target drift after preparation in its owning transaction",
    () =>
      fixture(
        (input) =>
          Effect.gen(function* () {
            yield* input.open;
            yield* input.publishSession("ready");
            const result = yield* input.attachmentPublicationRace("target");
            assert.strictEqual(
              result.clears,
              1,
              "real pre-publication boundary was reached after reservation",
            );
            assert.isTrue(result.refused);
            assert.strictEqual(result.bindings, 0);
            assert.strictEqual(input.calls(), 1);
            assert.strictEqual((yield* (yield* makeRuntimeUseStore).pending()).length, 2);
          }),
        true,
      ),
  );
  it.effect(
    "actual attachment transaction refuses a changed expected worker after preparation",
    () =>
      fixture(
        (input) =>
          Effect.gen(function* () {
            yield* input.open;
            yield* input.publishSession("ready");
            const result = yield* input.attachmentPublicationRace("claim");
            assert.strictEqual(result.clears, 1);
            assert.isTrue(result.refused);
            assert.strictEqual(result.bindings, 0);
            assert.strictEqual(input.calls(), 1);
            const uses = yield* (yield* makeRuntimeUseStore).pending();
            assert.strictEqual(uses.length, 2);
            assert.isTrue(
              uses.some((use) => use.original.claim?.workerId === "original-attachment-worker"),
            );
          }),
        true,
      ),
  );
  it.effect(
    "unchanged actual attachment publishes the exact additional binding without another native open",
    () =>
      fixture(
        (input) =>
          Effect.gen(function* () {
            yield* input.open;
            yield* input.publishSession("ready");
            const result = yield* input.attachmentPublicationRace("none");
            assert.strictEqual(result.clears, 1);
            assert.isFalse(result.refused);
            assert.strictEqual(result.bindings, 1);
            assert.strictEqual(input.calls(), 1);
            assert.strictEqual(input.nativeCalls(), 0);
            assert.strictEqual((yield* (yield* makeRuntimeUseStore).pending()).length, 2);
          }),
        true,
      ),
  );
  it.effect.each(["owner", "directory"] as const)(
    "actual attachment refuses %s drift at real publication boundary",
    (drift) =>
      fixture(
        (input) =>
          Effect.gen(function* () {
            yield* input.open;
            yield* input.publishSession("ready");
            const result = yield* input.attachmentPublicationRace(drift);
            assert.strictEqual(result.clears, 1);
            assert.isTrue(result.refused);
            assert.strictEqual(result.bindings, 0);
            assert.strictEqual(input.calls(), 1);
            assert.strictEqual(input.nativeCalls(), 0);
            assert.strictEqual((yield* (yield* makeRuntimeUseStore).pending()).length, 2);
          }),
        true,
      ),
  );
});
