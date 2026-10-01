import {
  AuthSessionId,
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentOrchestrationHttpApi,
  ClientOrchestrationCommand,
  CommandId,
  MessageId,
  OrchestrationCommand,
  OrchestrationCommandObservation,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type ThreadTurnDispatchGuard,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServer from "effect/unstable/http/HttpServer";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { expect } from "vite-plus/test";

import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import { orchestrationHttpApiLayer } from "./http.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import { ServerConfig } from "../config.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../persistence/Layers/OrchestrationEventStore.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import { makeCommandObservationQuery } from "./CommandObservation.ts";
import { OrchestrationEngineLive } from "./Layers/OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./Layers/ProjectionSnapshotQuery.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import * as ThreadBackgroundLiveness from "./ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "./ThreadPlanProgress.ts";

const testLayer = OrchestrationEngineLive.pipe(
  Layer.provideMerge(OrchestrationProjectionSnapshotQueryLive),
  Layer.provide(OrchestrationProjectionPipelineLive),
  Layer.provideMerge(ThreadBackgroundLiveness.layer),
  Layer.provide(ThreadPlanProgress.layer),
  Layer.provide(OrchestrationEventStoreLive),
  Layer.provide(OrchestrationCommandReceiptRepositoryLive),
  Layer.provide(RepositoryIdentityResolver.layer),
  Layer.provideMerge(SqlitePersistenceMemory),
  Layer.provideMerge(WorkspacePaths.layer),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-dispatch-guard-" })),
  Layer.provideMerge(NodeServices.layer),
);
const createdAt = "2026-09-30T12:00:00.000Z";
const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "test-model" };
const fixture = Effect.fn("DispatchGuard.fixture")(function* (name: string) {
  const engine = yield* OrchestrationEngineService;
  const query = yield* makeCommandObservationQuery();
  const sql = yield* SqlClient.SqlClient;
  const threadId = ThreadId.make(`thread-${name}`);
  const projectId = ProjectId.make(`project-${name}`);
  yield* engine.dispatch({
    type: "project.create",
    commandId: CommandId.make(`project-${name}`),
    projectId,
    title: "Guard fixture",
    workspaceRoot: `/test/dispatch-guard/${name}`,
    createdAt,
  });
  yield* engine.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`thread-${name}`),
    threadId,
    projectId,
    title: "Guard fixture",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdAt,
  });
  const observe = (
    commandId = CommandId.make(`start-${name}`),
    messageId = MessageId.make(`message-${name}`),
  ) => query.observe({ threadId, commandId, messageId });
  const initial = yield* observe();
  const guard: ThreadTurnDispatchGuard = {
    observedSnapshotSequence: initial.snapshotSequence,
    expectedModelSelection: modelSelection,
    expectedSessionStatus: null,
    expectedActiveTurnId: null,
    expectedLatestTurnId: null,
    requireIdle: true,
  };
  const command = {
    type: "thread.turn.start" as const,
    commandId: initial.commandId,
    threadId,
    message: {
      messageId: initial.messageId,
      role: "user" as const,
      text: "Bounded task",
      attachments: [],
    },
    runtimeMode: "full-access" as const,
    interactionMode: "default" as const,
    dispatchGuard: guard,
    createdAt,
  };
  return { engine, query, sql, projectId, threadId, initial, observe, command };
});

it.layer(testLayer)("conditional native dispatch and command observation", (it) => {
  it.effect("accepts once and replays the same receipt before rechecking a now-busy guard", () =>
    Effect.gen(function* () {
      const { engine, initial, observe, command } = yield* fixture("replay");
      expect(initial.commandStatus).toBe("not_found");
      expect(initial.target?.idle).toBe(true);
      const client = yield* Schema.decodeUnknownEffect(ClientOrchestrationCommand)(command);
      const decoded = yield* Schema.decodeUnknownEffect(OrchestrationCommand)(client);
      const disabled = yield* Schema.decodeUnknownEffect(ClientOrchestrationCommand)({
        ...command,
        dispatchGuard: { ...command.dispatchGuard, requireIdle: false },
      }).pipe(Effect.result);
      expect(disabled._tag).toBe("Failure");
      const accepted = yield* engine.dispatch(decoded);
      const pending = yield* observe();
      expect(pending.commandStatus).toBe("accepted");
      expect(pending.acceptedSequence).toBe(accepted.sequence);
      expect(pending.correlation).toBe("pending");
      expect(pending.turn?.state).toBe("pending");
      expect(pending.target?.blockers).toContain("pending_turn");
      expect(yield* engine.dispatch(command)).toEqual(accepted);
      expect(yield* engine.latestSequence).toBe(accepted.sequence);
      const pendingRejected = yield* engine
        .dispatch({
          ...command,
          commandId: CommandId.make("replay-pending"),
          dispatchGuard: {
            ...command.dispatchGuard,
            observedSnapshotSequence: pending.snapshotSequence,
          },
        })
        .pipe(Effect.flip);
      expect(pendingRejected.message).toContain("pending_turn");
      expect(yield* engine.latestSequence).toBe(accepted.sequence);
      yield* Schema.decodeUnknownEffect(OrchestrationCommandObservation)(pending);
    }),
  );

  it.effect("rejects target changes without appending events and preserves rejected replay", () =>
    Effect.gen(function* () {
      const { engine, threadId, observe, command } = yield* fixture("stale");
      yield* engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("stale-update"),
        threadId,
        title: "Changed",
      });
      const before = yield* engine.latestSequence;
      expect((yield* engine.dispatch(command).pipe(Effect.flip)).message).toContain(
        "target changed",
      );
      expect(yield* engine.latestSequence).toBe(before);
      expect((yield* observe()).commandStatus).toBe("rejected");
      expect((yield* engine.dispatch(command).pipe(Effect.flip))._tag).toBe(
        "OrchestrationCommandPreviouslyRejectedError",
      );
    }),
  );

  it.effect("allows unrelated aggregate changes but rejects a model-options mismatch", () =>
    Effect.gen(function* () {
      const { engine, command } = yield* fixture("unrelated");
      yield* fixture("other-aggregate");
      yield* engine.dispatch(command);
      const mismatch = yield* fixture("model-mismatch");
      const before = yield* engine.latestSequence;
      const rejected = yield* engine
        .dispatch({
          ...mismatch.command,
          dispatchGuard: {
            ...mismatch.command.dispatchGuard,
            expectedModelSelection: {
              ...modelSelection,
              options: [{ id: "reasoningEffort", value: "high" }],
            },
          },
        })
        .pipe(Effect.flip);
      expect(rejected.message).toContain("binding changed");
      expect(yield* engine.latestSequence).toBe(before);
    }),
  );

  it.effect(
    "holds persisted settlement until an explicit unsettle and leaves idle threads eligible",
    () =>
      Effect.gen(function* () {
        const { engine, threadId, observe, command } = yield* fixture("settled");
        expect((yield* observe()).target?.idle).toBe(true);
        yield* engine.dispatch({
          type: "thread.settle",
          commandId: CommandId.make("settled-owner-close"),
          threadId,
        });
        const settled = yield* observe();
        expect(settled.target?.blockers).toContain("settled");
        expect(settled.target?.idle).toBe(false);
        const before = yield* engine.latestSequence;
        const rejected = yield* engine
          .dispatch({
            ...command,
            dispatchGuard: {
              ...command.dispatchGuard,
              observedSnapshotSequence: settled.snapshotSequence,
            },
          })
          .pipe(Effect.flip);
        expect(rejected.message).toContain("settled");
        expect(yield* engine.latestSequence).toBe(before);
        expect((yield* observe()).target?.blockers).toContain("settled");
        yield* engine.dispatch({
          type: "thread.unsettle",
          commandId: CommandId.make("settled-owner-reopen"),
          threadId,
          reason: "user",
        });
        const reopened = yield* observe();
        expect(reopened.target?.idle).toBe(true);
        yield* engine.dispatch({
          ...command,
          commandId: CommandId.make("settled-after-reopen"),
          dispatchGuard: {
            ...command.dispatchGuard,
            observedSnapshotSequence: reopened.snapshotSequence,
          },
        });
      }),
  );

  it.effect(
    "allows a new model on the observed provider instance and rejects a foreign instance",
    () =>
      Effect.gen(function* () {
        const { engine, command, observe } = yield* fixture("requested-model");
        const desired = {
          ...modelSelection,
          model: "gpt-6.1-sol",
          options: [{ id: "reasoningEffort", value: "medium" }],
        };
        const before = yield* engine.latestSequence;
        const foreign = yield* engine
          .dispatch({
            ...command,
            commandId: CommandId.make("requested-model-foreign"),
            modelSelection: { ...desired, instanceId: ProviderInstanceId.make("another-provider") },
          })
          .pipe(Effect.flip);
        expect(foreign.message).toContain("binding changed");
        expect(yield* engine.latestSequence).toBe(before);
        yield* engine.dispatch({ ...command, modelSelection: desired });
        expect((yield* observe()).commandStatus).toBe("accepted");
        const events = yield* Stream.runCollect(engine.readEvents(before));
        const start = events.find((event) => event.type === "thread.turn-start-requested");
        expect(start?.payload).toMatchObject({ modelSelection: desired });
      }),
  );

  it.effect("blocks durable attention flags and live background work", () =>
    Effect.gen(function* () {
      const { engine, sql, threadId, observe, command } = yield* fixture("attention");
      yield* sql`UPDATE projection_threads SET pending_approval_count = 1, pending_user_input_count = 1,
        has_actionable_proposed_plan = 1 WHERE thread_id = ${threadId}`;
      const liveness = yield* ThreadBackgroundLiveness.ThreadBackgroundLivenessService;
      liveness.recordTaskLiveness({
        threadId,
        taskId: "live-task",
        taskType: "agent",
        status: "running",
        kind: "started",
      });
      expect((yield* observe()).target?.blockers).toEqual([
        "pending_approval",
        "pending_user_input",
        "actionable_plan",
        "background_work",
      ]);
      const before = yield* engine.latestSequence;
      expect((yield* engine.dispatch(command).pipe(Effect.flip)).message).toContain(
        "pending_approval",
      );
      expect(yield* engine.latestSequence).toBe(before);
    }),
  );

  it.effect("correlates an old turn exactly after a newer turn exists and detects mismatches", () =>
    Effect.gen(function* () {
      const { engine, sql, threadId, observe, command, query } = yield* fixture("historical");
      yield* engine.dispatch(command);
      const turnId = TurnId.make("historical-turn");
      yield* sql`UPDATE projection_turns SET turn_id = ${turnId}, state = 'completed', started_at = ${createdAt},
        completed_at = ${createdAt}, assistant_message_id = 'historical-assistant'
        WHERE thread_id = ${threadId} AND pending_message_id = ${command.message.messageId}`;
      yield* sql`INSERT INTO projection_turns (thread_id, turn_id, pending_message_id, state, requested_at, checkpoint_files_json)
        VALUES (${threadId}, 'newer-turn', 'newer-message', 'running', '2026-09-30T13:00:00.000Z', '[]')`;
      yield* sql`UPDATE projection_threads SET latest_turn_id = 'newer-turn' WHERE thread_id = ${threadId}`;
      const observed = yield* observe();
      expect(observed.correlation).toBe("exact");
      expect(observed.turn?.turnId).toBe(turnId);
      expect(observed.turn?.state).toBe("completed");
      expect(observed.turn?.assistantMessageId).toBe("historical-assistant");
      expect(observed.target?.latestTurnId).toBe("newer-turn");
      expect((yield* observe(command.commandId, MessageId.make("wrong-message"))).correlation).toBe(
        "mismatched",
      );
      expect(
        (yield* query.observe({
          threadId: ThreadId.make("wrong-thread"),
          commandId: command.commandId,
          messageId: command.message.messageId,
        })).correlation,
      ).toBe("mismatched");
      yield* sql`INSERT INTO projection_turns (thread_id, turn_id, pending_message_id, state, requested_at, checkpoint_files_json)
        VALUES (${threadId}, 'duplicate-turn', ${command.message.messageId}, 'completed', ${createdAt}, '[]')`;
      expect((yield* observe()).correlation).toBe("ambiguous");
      expect((yield* observe()).turn).toBeNull();
    }),
  );

  it.effect(
    "requires the command event binding even when a matching message projection exists",
    () =>
      Effect.gen(function* () {
        const { engine, observe, command } = yield* fixture("missing-event");
        yield* engine.dispatch(command);
        const observed = yield* observe(CommandId.make("never-dispatched"));
        expect(observed.commandStatus).toBe("not_found");
        expect(observed.correlation).toBe("missing");
        expect(observed.turn).toBeNull();
      }),
  );

  it.effect("rejects bootstrap and future snapshots without events", () =>
    Effect.gen(function* () {
      const { engine, command } = yield* fixture("bootstrap");
      const before = yield* engine.latestSequence;
      expect(
        (yield* engine
          .dispatch({ ...command, bootstrap: { runSetupScript: true } })
          .pipe(Effect.flip)).message,
      ).toContain("bootstrap is unsupported");
      expect(yield* engine.latestSequence).toBe(before);
      const future = yield* fixture("future");
      expect(
        (yield* engine
          .dispatch({
            ...future.command,
            dispatchGuard: { ...future.command.dispatchGuard, observedSnapshotSequence: 999999 },
          })
          .pipe(Effect.flip)).message,
      ).toContain("target changed");
    }),
  );

  it.effect("blocks a freshly observed starting or active session", () =>
    Effect.gen(function* () {
      for (const status of ["starting", "running", "ready"] as const) {
        const { engine, observe, threadId, command } = yield* fixture(`session-${status}`);
        const activeTurnId = status === "starting" ? null : TurnId.make(`active-${status}`);
        yield* engine.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make(`session-${status}`),
          threadId,
          createdAt,
          session: {
            threadId,
            status,
            activeTurnId,
            providerName: "codex",
            runtimeMode: "full-access",
            lastError: null,
            updatedAt: createdAt,
          },
        });
        const current = yield* observe();
        const target = current.target!;
        const before = yield* engine.latestSequence;
        const rejected = yield* engine
          .dispatch({
            ...command,
            dispatchGuard: {
              ...command.dispatchGuard,
              observedSnapshotSequence: current.snapshotSequence,
              expectedSessionStatus: target.sessionStatus,
              expectedActiveTurnId: target.activeTurnId,
              expectedLatestTurnId: target.latestTurnId,
            },
          })
          .pipe(Effect.flip);
        expect(rejected.message).toContain(
          status === "starting" ? "session_starting" : "active_turn",
        );
        expect(yield* engine.latestSequence).toBe(before);
      }
    }),
  );

  it.effect("allows only one of two commands with the same observed idle state", () =>
    Effect.gen(function* () {
      const { engine, command } = yield* fixture("race");
      const results = yield* Effect.all(
        [
          engine.dispatch(command).pipe(Effect.result),
          engine
            .dispatch({
              ...command,
              commandId: CommandId.make("race-second"),
              message: { ...command.message, messageId: MessageId.make("race-second-message") },
            })
            .pipe(Effect.result),
        ],
        { concurrency: 2 },
      );
      expect(results.filter((result) => result._tag === "Success")).toHaveLength(1);
      expect(results.filter((result) => result._tag === "Failure")).toHaveLength(1);
    }),
  );
  it.effect("keeps accepted commands without a matching turn unresolved", () =>
    Effect.gen(function* () {
      const { engine, sql, threadId, command, observe } = yield* fixture("missing-turn");
      const accepted = yield* engine.dispatch(command);
      yield* sql`DELETE FROM projection_turns WHERE thread_id = ${threadId} AND pending_message_id = ${command.message.messageId}`;
      const observation = yield* observe();
      expect(observation.commandStatus).toBe("accepted");
      expect(observation.acceptedSequence).toBe(accepted.sequence);
      expect(observation.correlation).toBe("pending");
      expect(observation.turn).toBeNull();
    }),
  );

  it.effect("preserves HTTP clone rejection and cleanup alongside guarded observation", () =>
    Effect.gen(function* () {
      const { engine, projectId, threadId, command } = yield* fixture("http-current");
      const context = yield* Effect.context<
        | OrchestrationEngineService
        | ProjectionSnapshotQuery
        | SqlClient.SqlClient
        | ServerConfig
        | FileSystem.FileSystem
        | Path.Path
        | WorkspacePaths.WorkspacePaths
      >();
      let cloning = false;
      const discarded: ProjectId[] = [];
      const clones = Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({
        get: () =>
          Effect.succeed(
            cloning
              ? {
                  projectId,
                  remoteUrl: "https://example.invalid/test.git",
                  destinationPath: "/test/clone",
                  repository: null,
                  phase: "running" as const,
                  stage: "receiving" as const,
                  percent: null,
                  detail: null,
                  error: null,
                  startedAt: createdAt,
                  endedAt: null,
                  sequence: 1,
                }
              : null,
          ),
        discard: (id) =>
          Effect.sync(() => {
            discarded.push(id);
          }),
      });
      const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          return yield* effect.pipe(
            Effect.provideService(EnvironmentAuthenticatedPrincipal, {
              sessionId: AuthSessionId.make("test-current"),
              subject: "test",
              method: "bearer-access-token",
              scopes: new Set(
                request.headers["x-test-no-scope"] === "true"
                  ? []
                  : [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
              ),
            }),
          );
        }),
      );
      const routeLayer = HttpApiBuilder.layer(
        HttpApi.make("environment").add(EnvironmentOrchestrationHttpApi),
      ).pipe(
        Layer.provide(orchestrationHttpApiLayer),
        Layer.provide(clones),
        Layer.provide(auth),
        Layer.provide(HttpServer.layerServices),
      );
      const routes = routeLayer.pipe(Layer.provide(Layer.succeedContext(context)));
      yield* Effect.acquireUseRelease(
        Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
        ({ handler }) =>
          Effect.gen(function* () {
            const request = (input: Request) => handler(input, context);
            const post = Effect.fn("DispatchGuard.currentHttpPost")(function* (body: unknown) {
              const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(
                body,
              );
              return yield* Effect.promise(() =>
                request(
                  new Request("http://test/api/orchestration/dispatch", {
                    method: "POST",
                    headers: { "content-type": "application/json" },
                    body: encoded,
                  }),
                ),
              );
            });
            const url = `http://test/api/orchestration/threads/${threadId}/commands/${command.commandId}?messageId=${command.message.messageId}`;
            const initial = yield* Effect.promise(() => request(new Request(url)));
            expect(initial.status).toBe(200);
            expect(yield* Effect.promise(() => initial.json())).toMatchObject({
              commandStatus: "not_found",
              target: { idle: true },
            });
            const forbidden = yield* Effect.promise(() =>
              request(new Request(url, { headers: { "x-test-no-scope": "true" } })),
            );
            expect(forbidden.status).toBe(403);
            const missingMessage = yield* Effect.promise(() =>
              request(new Request(url.split("?")[0]!)),
            );
            expect(missingMessage.status).toBe(400);
            cloning = true;
            const beforeClone = yield* engine.latestSequence;
            const cloneRejected = yield* post({
              type: "thread.create",
              commandId: "clone-rejected",
              projectId,
              threadId: "during-clone",
              title: "Clone rejection",
              modelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: null,
              createdAt,
            });
            expect(cloneRejected.status).toBe(500);
            expect(yield* engine.latestSequence).toBe(beforeClone);
            cloning = false;
            const bootstrapRejected = yield* post({
              ...command,
              bootstrap: {
                prepareWorktree: {
                  projectCwd: "/test",
                  baseBranch: "main",
                  requireWorktree: true,
                },
              },
            });
            expect(bootstrapRejected.status).toBe(400);
            expect(yield* Effect.promise(() => bootstrapRejected.json())).toMatchObject({
              reason: "dispatch_guard_bootstrap_unsupported",
            });
            expect((yield* post(command)).status).toBe(200);
            const rejected = yield* post({ ...command, commandId: "current-http-stale" });
            expect(rejected.status).toBe(400);
            expect(yield* Effect.promise(() => rejected.json())).toMatchObject({
              reason: "dispatch_guard_rejected",
            });
            const deleted = yield* post({
              type: "project.delete",
              commandId: "delete-cloned-project",
              projectId,
              force: true,
            });
            expect(deleted.status, yield* Effect.promise(() => deleted.text())).toBe(200);
            expect(discarded).toEqual([projectId]);
          }),
        ({ dispose }) => Effect.promise(dispose),
      );
      const legacyEngine = { ...engine };
      delete legacyEngine.observeCommand;
      const legacyContext = Context.add(context, OrchestrationEngineService, legacyEngine);
      const legacyRoutes = routeLayer.pipe(Layer.provide(Layer.succeedContext(legacyContext)));
      yield* Effect.acquireUseRelease(
        Effect.sync(() => HttpRouter.toWebHandler(legacyRoutes, { disableLogger: true })),
        ({ handler }) =>
          Effect.gen(function* () {
            const url = `http://test/api/orchestration/threads/${threadId}/commands/${command.commandId}?messageId=${command.message.messageId}`;
            const response = yield* Effect.promise(() => handler(new Request(url), legacyContext));
            expect(response.status).toBe(400);
            expect(yield* Effect.promise(() => response.json())).toMatchObject({
              reason: "observation_unsupported",
            });
          }),
        ({ dispose }) => Effect.promise(dispose),
      );
    }),
  );
});
