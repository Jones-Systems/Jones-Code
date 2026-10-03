import * as NativeCreationRepositoryLayer from "../../persistence/Layers/NativeCreationRepository.ts";
import {
  NativePreparationBinding,
  nativePreparationCommand,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  validateNativeCreationPreparation,
  nativeCreationCommandDigest,
} from "../NativeCreationPreparation.ts";
import { NativeCreationHistoricalBinding } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { nativeBootstrapCommandIds } from "../../ws.ts";
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeSqlite from "node:sqlite";

import {
  ApprovalRequestId,
  EventId,
  CheckpointRef,
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  MessageId,
  ProjectId,
  ThreadId,
  TurnId,
  OrchestrationCommand,
  type OrchestrationEvent,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it as effectIt } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import { describe, expect, it, vi } from "vite-plus/test";

import { PersistenceSqlError } from "../../persistence/Errors.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../../persistence/Layers/OrchestrationCommandReceipts.ts";
import * as OrchestrationCommandReceipts from "../../persistence/Services/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../../persistence/Layers/OrchestrationEventStore.ts";
import {
  makeSqlitePersistenceLive,
  SqlitePersistenceMemory,
} from "../../persistence/Layers/Sqlite.ts";
import {
  OrchestrationEventStore,
  type OrchestrationEventStoreShape,
} from "../../persistence/Services/OrchestrationEventStore.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { OrchestrationEngineLive } from "./OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "../ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "../ThreadPlanProgress.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import {
  OrchestrationProjectionPipeline,
  type OrchestrationProjectionPipelineShape,
} from "../Services/ProjectionPipeline.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { ServerConfig } from "../../config.ts";

const asProjectId = (value: string): ProjectId => ProjectId.make(value);
const asMessageId = (value: string): MessageId => MessageId.make(value);
const asTurnId = (value: string): TurnId => TurnId.make(value);
const asCheckpointRef = (value: string): CheckpointRef => CheckpointRef.make(value);

function makeOrchestrationLayer(
  databasePath?: string,
  repositoryIdentityResolver?: RepositoryIdentityResolver.RepositoryIdentityResolver["Service"],
  fileSystemTransform: (fileSystem: FileSystem.FileSystem) => FileSystem.FileSystem = (
    fileSystem,
  ) => fileSystem,
) {
  const persistence = databasePath
    ? makeSqlitePersistenceLive(databasePath)
    : SqlitePersistenceMemory;
  const ServerConfigLayer = ServerConfig.layerTest(process.cwd(), {
    prefix: "t3-orchestration-engine-test-",
  });
  return Layer.mergeAll(
    OrchestrationEngineLive.pipe(
      Layer.provide(OrchestrationProjectionSnapshotQueryLive),
      Layer.provide(OrchestrationProjectionPipelineLive),
    ),
    OrchestrationProjectionSnapshotQueryLive,
  ).pipe(
    Layer.provideMerge(ThreadBackgroundLiveness.layer),
    Layer.provide(ThreadPlanProgress.layer),
    Layer.provide(OrchestrationEventStoreLive),
    Layer.provideMerge(OrchestrationCommandReceiptRepositoryLive),
    Layer.provide(
      repositoryIdentityResolver
        ? Layer.succeed(
            RepositoryIdentityResolver.RepositoryIdentityResolver,
            repositoryIdentityResolver,
          )
        : RepositoryIdentityResolver.layer,
    ),
    Layer.provideMerge(persistence),
    Layer.provideMerge(ServerConfigLayer),
    Layer.updateService(FileSystem.FileSystem, fileSystemTransform),
    Layer.provideMerge(NodeServices.layer),
  );
}

async function createOrchestrationSystem(
  databasePath?: string,
  repositoryIdentityResolver?: RepositoryIdentityResolver.RepositoryIdentityResolver["Service"],
  fileSystemTransform?: (fileSystem: FileSystem.FileSystem) => FileSystem.FileSystem,
) {
  const runtime = ManagedRuntime.make(
    makeOrchestrationLayer(databasePath, repositoryIdentityResolver, fileSystemTransform),
  );
  const engine = await runtime.runPromise(Effect.service(OrchestrationEngineService));
  const snapshotQuery = await runtime.runPromise(Effect.service(ProjectionSnapshotQuery));
  return {
    engine,
    readModel: () => runtime.runPromise(snapshotQuery.getSnapshot()),
    readThread: (threadId: ThreadId) =>
      runtime.runPromise(snapshotQuery.getThreadDetailById(threadId)),
    run: <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient>) => runtime.runPromise(effect),
    dispose: () => runtime.dispose(),
  };
}

function now() {
  return "2026-01-01T00:00:00.000Z";
}

const hasMetricSnapshot = (
  snapshots: ReadonlyArray<Metric.Metric.Snapshot>,
  id: string,
  attributes: Readonly<Record<string, string>>,
) =>
  snapshots.some(
    (snapshot) =>
      snapshot.id === id &&
      Object.entries(attributes).every(([key, value]) => snapshot.attributes?.[key] === value),
  );

describe("OrchestrationEngine", () => {
  it("acquires worktree ownership after a foreign commit during path preparation", async () => {
    const directory = await NodeFSP.mkdtemp(
      NodePath.join(process.cwd(), "node_modules", ".jones-lease-snapshot-"),
    );
    const pathPreparationEntered = Deferred.makeUnsafe<void>();
    const resumePathPreparation = Deferred.makeUnsafe<void>();
    let armed = false;
    let targetRealPathCalls = 0;
    let system: Awaited<ReturnType<typeof createOrchestrationSystem>> | undefined;
    let writer: NodeSqlite.DatabaseSync | undefined;
    let acquisition: Promise<unknown> | undefined;
    try {
      const databasePath = NodePath.join(directory, "state.sqlite");
      system = await createOrchestrationSystem(databasePath, undefined, (fileSystem) => ({
        ...fileSystem,
        realPath: (targetPath) =>
          Effect.gen(function* () {
            if (armed && targetPath === directory) {
              targetRealPathCalls += 1;
              if (targetRealPathCalls === 2) {
                yield* Deferred.succeed(pathPreparationEntered, undefined);
                yield* Deferred.await(resumePathPreparation);
              }
            }
            return yield* fileSystem.realPath(targetPath);
          }),
      }));
      const projectId = ProjectId.make("lease-snapshot-project");
      const threadId = ThreadId.make("lease-snapshot-thread");
      await system.run(
        system.engine.dispatch({
          type: "project.create",
          commandId: CommandId.make("lease-snapshot-project-create"),
          projectId,
          title: "Lease snapshot",
          workspaceRoot: directory,
          createdAt: now(),
        }),
      );
      await system.run(
        system.engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("lease-snapshot-thread-create"),
          threadId,
          projectId,
          title: "Lease snapshot",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdAt: now(),
        }),
      );
      await system.run(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`PRAGMA busy_timeout = 0`;
        }),
      );
      writer = new NodeSqlite.DatabaseSync(databasePath);
      writer.exec("PRAGMA busy_timeout = 0");
      writer.exec("CREATE TABLE lease_snapshot_foreign_commits (id INTEGER PRIMARY KEY)");

      armed = true;
      const ownership = system.run(
        system.engine.acquireWorktreeOwnership(threadId).pipe(Effect.result),
      );
      acquisition = ownership;
      await Promise.race([
        system.run(Deferred.await(pathPreparationEntered)),
        ownership.then(() => {
          throw new Error("ownership acquisition completed before the path barrier");
        }),
      ]);
      writer.exec("INSERT INTO lease_snapshot_foreign_commits (id) VALUES (1)");
      await system.run(Deferred.succeed(resumePathPreparation, undefined));
      const result = await ownership;
      expect(result).toMatchObject({
        _tag: "Success",
        success: { resourcePath: directory, ownerThreadId: threadId },
      });
      expect(await system.run(system.engine.listWorktreeOwnershipLeases)).toHaveLength(1);
      expect(writer.prepare("SELECT id FROM lease_snapshot_foreign_commits").all()).toEqual([
        { id: 1 },
      ]);
    } finally {
      if (system !== undefined) {
        await system.run(Deferred.succeed(resumePathPreparation, undefined));
        await acquisition;
        await system.dispose();
      }
      writer?.close();
      await NodeFSP.rm(directory, { recursive: true, force: true });
      await expect(NodeFSP.access(directory)).rejects.toMatchObject({ code: "ENOENT" });
    }
  });

  it.each(["running", "stopped"] as const)(
    "sends async answers with a %s session and rejects old duplicate replies",
    async (status) => {
      const directory = await NodeFSP.mkdtemp(
        NodePath.join(NodeOS.tmpdir(), "t3-async-questions-"),
      );
      const databasePath = NodePath.join(directory, "state.sqlite");
      let system = await createOrchestrationSystem(databasePath);
      const threadId = ThreadId.make("async-thread");
      const projectId = ProjectId.make("async-project");
      const requestId = ApprovalRequestId.make("codex-async:question-1");
      try {
        await system.run(
          system.engine.dispatch({
            type: "project.create",
            commandId: CommandId.make("async-project"),
            projectId,
            title: "Async questions",
            workspaceRoot: "/tmp/async-questions",
            createdAt: now(),
          }),
        );
        await system.run(
          system.engine.dispatch({
            type: "thread.create",
            commandId: CommandId.make("async-thread"),
            threadId,
            projectId,
            title: "Async questions",
            modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            createdAt: now(),
          }),
        );
        await system.run(
          system.engine.dispatch({
            type: "thread.session.set",
            commandId: CommandId.make("async-session"),
            threadId,
            createdAt: now(),
            session: {
              threadId,
              status,
              providerName: "codex",
              runtimeMode: "full-access",
              activeTurnId: status === "running" ? TurnId.make("turn-1") : null,
              lastError: null,
              updatedAt: now(),
            },
          }),
        );
        await system.run(
          system.engine.dispatch({
            type: "thread.activity.append",
            commandId: CommandId.make("async-question"),
            threadId,
            createdAt: now(),
            activity: {
              id: EventId.make("async-question"),
              kind: "user-input.requested",
              summary: "User input requested",
              tone: "info",
              turnId: TurnId.make("turn-1"),
              createdAt: now(),
              payload: {
                requestId,
                responseMode: "message",
                questions: [
                  {
                    id: "0",
                    header: "Question",
                    question: "Which package manager?",
                    options: [{ label: "pnpm", description: "" }],
                  },
                  {
                    id: "1",
                    header: "Question",
                    question: "What should it be named?",
                    options: [],
                  },
                ],
              },
            },
          }),
        );
        const appendWork = async (prefix: string, createdAt: string) => {
          for (let index = 0; index < 501; index += 1) {
            await system.run(
              system.engine.dispatch({
                type: "thread.activity.append",
                commandId: CommandId.make(`${prefix}-${index}`),
                threadId,
                createdAt,
                activity: {
                  id: EventId.make(`${prefix}-${index}`),
                  kind: "tool.completed",
                  summary: "Work continued",
                  payload: {},
                  tone: "info",
                  turnId: TurnId.make("turn-1"),
                  createdAt,
                },
              }),
            );
          }
        };
        await appendWork("work", "2026-01-01T00:00:01.000Z");
        const before = await system.readModel();
        expect(
          before.threads[0]?.activities.some((activity) => activity.id === "async-question"),
        ).toBe(true);
        if (status === "stopped") {
          await system.dispose();
          system = await createOrchestrationSystem(databasePath);
        }
        const response = {
          type: "thread.user-input.respond" as const,
          commandId: CommandId.make("async-response"),
          threadId,
          requestId,
          answers: { "0": "pnpm", "1": "Example" },
          attachmentsByQuestionId: {
            "1": [
              {
                type: "file" as const,
                id: "thread-1-00000000-0000-4000-8000-0000000000aa-txt",
                name: "spec.txt",
                mimeType: "text/plain",
                sizeBytes: 4,
              },
            ],
          },
          createdAt: "2026-01-01T00:00:02.000Z",
        };
        await expect(
          system.run(
            system.engine.dispatch({
              ...response,
              commandId: CommandId.make("incomplete-answer"),
              answers: { "0": "pnpm" },
            }),
          ),
        ).rejects.toThrow("Answer each question before sending.");
        await system.run(system.engine.dispatch(response));
        const after = await system.readModel();
        const userMessages = after.threads[0]?.messages.filter(
          (message) => message.role === "user",
        );
        expect(userMessages).toHaveLength(1);
        expect(userMessages?.[0]?.attachments).toEqual(response.attachmentsByQuestionId["1"]);
        expect(userMessages?.[0]?.text).toBe(
          "Which package manager?\npnpm\n\nWhat should it be named?\nExample\nAttached file: spec.txt (thread-1-00000000-0000-4000-8000-0000000000aa-txt)",
        );
        expect(
          after.threads[0]?.activities.find((activity) => activity.kind === "user-input.resolved")
            ?.payload,
        ).toMatchObject({ requestId, responseMode: "message", answers: response.answers });
        const events = await system.run(Stream.runCollect(system.engine.readEvents(0)));
        expect(
          Array.from(events)
            .filter((event) => event.commandId === response.commandId)
            .map((event) => event.type),
        ).toEqual([
          "thread.activity-appended",
          "thread.message-sent",
          "thread.turn-start-requested",
        ]);
        await expect(
          system.run(
            system.engine.dispatch({
              ...response,
              commandId: CommandId.make("second-client-reply"),
            }),
          ),
        ).rejects.toThrow("This question has already been answered.");
        await appendWork("later-work", "2026-01-01T00:00:03.000Z");
        const afterEviction = Option.getOrThrow(await system.readThread(threadId));
        expect(
          afterEviction.activities.some((activity) => activity.kind === "user-input.resolved"),
        ).toBe(false);
        if (status === "stopped") {
          await system.dispose();
          system = await createOrchestrationSystem(databasePath);
        }
        await expect(
          system.run(
            system.engine.dispatch({
              ...response,
              commandId: CommandId.make("reply-after-eviction"),
            }),
          ),
        ).rejects.toThrow("This question has already been answered.");
      } finally {
        await system.dispose();
        await NodeFSP.rm(directory, { recursive: true, force: true });
      }
    },
  );

  it("bootstraps command handling from persisted projections without reading the full snapshot", async () => {
    let nextSequence = 8;
    const eventStore: OrchestrationEventStoreShape = {
      readMetadataByCommandId: () => Effect.die("unused command metadata lookup"),
      append: (event) =>
        Effect.sync(() => {
          const savedEvent = {
            ...event,
            sequence: nextSequence,
          } as OrchestrationEvent;
          nextSequence += 1;
          return savedEvent;
        }),
      readFromSequence: () => Stream.empty,
      readAll: () =>
        Stream.fail(
          new PersistenceSqlError({
            operation: "test.readAll",
            detail: "historical replay should not be used during bootstrap",
          }),
        ),
      hasEventAfter: () => Effect.succeed(false),
      readAggregateRange: () => Stream.die("unused aggregate replay"),
      getAggregateReplayStats: () => Effect.die("unused aggregate replay stats"),
    };

    const projectionSnapshot = {
      snapshotSequence: 7,
      updatedAt: "2026-03-03T00:00:04.000Z",
      projects: [
        {
          id: asProjectId("project-bootstrap"),
          title: "Bootstrap Project",
          workspaceRoot: "/tmp/project-bootstrap",
          defaultModelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          scripts: [],
          createdAt: "2026-03-03T00:00:00.000Z",
          updatedAt: "2026-03-03T00:00:01.000Z",
          deletedAt: null,
        },
      ],
      threads: [
        {
          id: ThreadId.make("thread-bootstrap"),
          projectId: asProjectId("project-bootstrap"),
          title: "Bootstrap Thread",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access" as const,
          branch: null,
          worktreePath: null,
          pullRequests: [],
          latestTurn: null,
          createdAt: "2026-03-03T00:00:02.000Z",
          updatedAt: "2026-03-03T00:00:03.000Z",
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          deletedAt: null,
          messages: [],
          proposedPlans: [],
          activities: [],
          checkpoints: [],
          session: null,
        },
      ],
    };
    const commandReadModel = {
      ...projectionSnapshot,
      threads: projectionSnapshot.threads.map((thread) => ({
        ...thread,
        messages: [],
        proposedPlans: [],
        activities: [],
        checkpoints: [],
      })),
    };
    let fullSnapshotReadCount = 0;

    const layer = OrchestrationEngineLive.pipe(
      Layer.provide(
        Layer.succeed(ProjectionSnapshotQuery, {
          getOperatingCounts: () => Effect.die("unused"),
          getUserInputActivity: () => Effect.die("unused"),
          listActivitiesByKind: () => Effect.die("unused"),
          getCommandReadModel: () => Effect.succeed(commandReadModel),
          getSnapshot: () =>
            Effect.sync(() => {
              fullSnapshotReadCount += 1;
              return projectionSnapshot;
            }),
          getShellSnapshot: () =>
            Effect.succeed({
              snapshotSequence: projectionSnapshot.snapshotSequence,
              projects: [],
              threads: [],
              updatedAt: projectionSnapshot.updatedAt,
            }),
          getDeletedWorktreeThreads: () => Effect.die("unused"),
          listThreadsWithPullRequests: () => Effect.die("unused"),
          getArchivedShellSnapshot: () =>
            Effect.succeed({
              snapshotSequence: projectionSnapshot.snapshotSequence,
              projects: [],
              threads: [],
              updatedAt: projectionSnapshot.updatedAt,
            }),
          getSnapshotSequence: () =>
            Effect.succeed({ snapshotSequence: projectionSnapshot.snapshotSequence }),
          getCounts: () => Effect.succeed({ projectCount: 1, threadCount: 1 }),
          getEventReplayStats: () => Effect.die("unused"),
          getActiveProjectByWorkspaceRoot: () => Effect.succeedNone,
          getProjectShellById: () => Effect.succeedNone,
          getProjectShells: () => Effect.succeed([]),
          getFirstActiveThreadIdByProjectId: () => Effect.succeedNone,
          getImportedAgentSessionSources: () => Effect.die("unused"),
          getThreadCheckpointContext: () => Effect.succeedNone,
          getFullThreadDiffContext: () => Effect.succeedNone,
          getThreadRuntimeContext: () => Effect.die("unused"),
          getTurnStartMessage: () => Effect.die("unused"),
          getThreadShellById: () => Effect.succeedNone,
          getThreadDetailById: () => Effect.succeedNone,
          getThreadDetailSnapshot: () => Effect.succeedNone,
          searchThreads: () => Effect.succeed({ matches: [] }),
        }),
      ),
      Layer.provide(
        Layer.succeed(OrchestrationProjectionPipeline, {
          bootstrap: Effect.void,
          projectEvent: () => Effect.void,
          projectEventDeferred: () => Effect.succeed(Effect.void),
        } satisfies OrchestrationProjectionPipelineShape),
      ),
      Layer.provide(Layer.succeed(OrchestrationEventStore, eventStore)),
      Layer.provide(ThreadBackgroundLiveness.layer),
      Layer.provide(OrchestrationCommandReceiptRepositoryLive),
      Layer.provide(SqlitePersistenceMemory),
      Layer.provideMerge(NodeServices.layer),
    );

    const runtime = ManagedRuntime.make(layer);

    const engine = await runtime.runPromise(Effect.service(OrchestrationEngineService));
    expect(await runtime.runPromise(engine.latestSequence)).toBe(7);
    const result = await runtime.runPromise(
      engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-bootstrap-thread-update"),
        threadId: ThreadId.make("thread-bootstrap"),
        title: "Updated Bootstrap Thread",
      }),
    );

    expect(result.sequence).toBe(8);
    expect(await runtime.runPromise(engine.latestSequence)).toBe(8);
    expect(fullSnapshotReadCount).toBe(0);

    await runtime.dispose();
  });

  effectIt.effect("preserves the blocked-settle error and persists its rejected receipt", () =>
    Effect.gen(function* () {
      const engine = yield* OrchestrationEngineService;
      const receipts = yield* OrchestrationCommandReceipts.OrchestrationCommandReceiptRepository;
      const projectId = ProjectId.make("project-blocked-settle");
      const threadId = ThreadId.make("thread-blocked-settle");
      const commandId = CommandId.make("cmd-blocked-settle");
      const createdAt = now();

      yield* engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-blocked-settle-project-create"),
        projectId,
        title: "Project",
        workspaceRoot: "/tmp/project-blocked-settle",
        createdAt,
      });
      yield* engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-blocked-settle-thread-create"),
        threadId,
        projectId,
        title: "Thread",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        branch: null,
        worktreePath: null,
        createdAt,
      });
      yield* engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-blocked-settle-session-set"),
        threadId,
        createdAt,
        session: {
          threadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
      });

      const sequence = yield* engine.latestSequence;
      const error = yield* engine
        .dispatch({ type: "thread.settle", commandId, threadId })
        .pipe(Effect.flip);
      const message =
        "This thread still needs attention. Resolve or interrupt it first, then try again.";
      expect(error).toMatchObject({
        _tag: "OrchestrationThreadSettleBlockedError",
        threadId,
        message,
      });
      expect(Option.getOrNull(yield* receipts.getByCommandId({ commandId }))).toMatchObject({
        commandId,
        aggregateKind: "thread",
        aggregateId: threadId,
        status: "rejected",
        error: message,
        resultSequence: sequence,
      });
      expect(yield* engine.latestSequence).toBe(sequence);
    }).pipe(Effect.provide(makeOrchestrationLayer())),
  );

  effectIt.effect(
    "rejects persisted changes and live background work without blocking unrelated threads",
    () =>
      Effect.gen(function* () {
        const engine = yield* OrchestrationEngineService;
        const snapshots = yield* ProjectionSnapshotQuery;
        const backgroundLiveness = yield* ThreadBackgroundLiveness.ThreadBackgroundLivenessService;
        const projectId = ProjectId.make("project-auto-settle-guard");
        const guardedThreadId = ThreadId.make("thread-auto-settle-guarded");
        const unrelatedThreadId = ThreadId.make("thread-auto-settle-unrelated");
        const liveThreadId = ThreadId.make("thread-auto-settle-live");

        yield* engine.dispatch({
          type: "project.create",
          commandId: CommandId.make("cmd-auto-settle-guard-project"),
          projectId,
          title: "Project",
          workspaceRoot: "/tmp/project-auto-settle-guard",
          createdAt: now(),
        });
        for (const threadId of [guardedThreadId, unrelatedThreadId, liveThreadId]) {
          yield* engine.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`cmd-create-${threadId}`),
            threadId,
            projectId,
            title: "Thread",
            modelSelection: {
              instanceId: ProviderInstanceId.make("codex"),
              model: "gpt-5-codex",
            },
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt: now(),
          });
        }

        const beforeUpdate = yield* snapshots.getSnapshot();
        const snapshotSequence = beforeUpdate.snapshotSequence;
        const originalUpdatedAt = beforeUpdate.threads.find(
          (thread) => thread.id === guardedThreadId,
        )?.updatedAt;
        yield* engine.dispatch({
          type: "thread.meta.update",
          commandId: CommandId.make("cmd-auto-settle-guard-meta"),
          threadId: guardedThreadId,
          branch: "new-branch",
        });
        const afterUpdate = yield* snapshots.getSnapshot();
        expect(afterUpdate.threads.find((thread) => thread.id === guardedThreadId)?.updatedAt).toBe(
          originalUpdatedAt,
        );

        // Automatic settlement stamps the last activity, never the sweep time.
        const lastActivityAt = "2025-12-20T00:00:00.000Z";
        const staleError = yield* engine
          .dispatch({
            type: "thread.auto-settle",
            commandId: CommandId.make("cmd-auto-settle-stale-snapshot"),
            threadId: guardedThreadId,
            snapshotSequence,
            settledAt: lastActivityAt,
          })
          .pipe(Effect.flip);
        expect(staleError._tag).toBe("OrchestrationCommandInvariantError");

        const livenessSnapshotSequence = yield* engine.latestSequence;
        for (const [taskType, expectedLiveness] of [
          ["subagent", "working"],
          ["local_bash", "monitoring"],
        ] as const) {
          backgroundLiveness.recordTaskLiveness({
            threadId: liveThreadId,
            taskId: `task-${expectedLiveness}`,
            taskType,
            status: undefined,
            kind: "started",
          });
          expect(backgroundLiveness.getThreadBackgroundLiveness(liveThreadId)).toBe(
            expectedLiveness,
          );
          expect(yield* engine.latestSequence).toBe(livenessSnapshotSequence);

          const livenessError = yield* engine
            .dispatch({
              type: "thread.auto-settle",
              commandId: CommandId.make(`cmd-auto-settle-${expectedLiveness}`),
              threadId: liveThreadId,
              snapshotSequence: livenessSnapshotSequence,
              settledAt: lastActivityAt,
            })
            .pipe(Effect.flip);
          expect(livenessError._tag).toBe("OrchestrationCommandInvariantError");
          expect(yield* engine.latestSequence).toBe(livenessSnapshotSequence);
          backgroundLiveness.clearThreadLiveness(liveThreadId);
        }

        yield* engine.dispatch({
          type: "thread.auto-settle",
          commandId: CommandId.make("cmd-auto-settle-after-liveness-cleared"),
          threadId: liveThreadId,
          snapshotSequence: livenessSnapshotSequence,
          settledAt: lastActivityAt,
        });

        const freshSnapshotSequence = yield* engine.latestSequence;
        yield* engine.dispatch({
          type: "thread.meta.update",
          commandId: CommandId.make("cmd-auto-settle-unrelated-meta"),
          threadId: unrelatedThreadId,
          title: "Unrelated update",
        });
        yield* engine.dispatch({
          type: "thread.auto-settle",
          commandId: CommandId.make("cmd-auto-settle-after-unrelated-update"),
          threadId: guardedThreadId,
          snapshotSequence: freshSnapshotSequence,
          settledAt: lastActivityAt,
        });

        const settled = yield* snapshots.getSnapshot();
        for (const threadId of [guardedThreadId, liveThreadId]) {
          const thread = settled.threads.find((candidate) => candidate.id === threadId);
          expect(thread?.settledOverride).toBe("settled");
          expect(thread?.settledAt).toBe(lastActivityAt);
          expect(thread?.updatedAt).toBe(now());
        }
      }).pipe(Effect.provide(makeOrchestrationLayer()), (test) =>
        TestClock.setTime(Date.parse(now())).pipe(Effect.andThen(test)),
      ),
  );

  it("persists deterministic read models for repeated snapshot reads", async () => {
    const createdAt = now();
    const system = await createOrchestrationSystem();
    const { engine } = system;

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-1-create"),
        projectId: asProjectId("project-1"),
        title: "Project 1",
        workspaceRoot: "/tmp/project-1",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-1-create"),
        threadId: ThreadId.make("thread-1"),
        projectId: asProjectId("project-1"),
        title: "Thread",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("msg-1"),
          role: "user",
          text: "hello",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt,
      }),
    );

    const readModelA = await system.readModel();
    const readModelB = await system.readModel();
    expect(readModelB).toEqual(readModelA);
    await system.dispose();
  });

  it("archives and unarchives threads through orchestration commands", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-archive-create"),
        projectId: asProjectId("project-archive"),
        title: "Project Archive",
        workspaceRoot: "/tmp/project-archive",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-archive-create"),
        threadId: ThreadId.make("thread-archive"),
        projectId: asProjectId("project-archive"),
        title: "Archive me",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    await system.run(
      engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-archive-title-regeneration"),
        threadId: ThreadId.make("thread-archive"),
        regenerateTitle: true,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.archive",
        commandId: CommandId.make("cmd-thread-archive"),
        threadId: ThreadId.make("thread-archive"),
      }),
    );
    expect(
      (await system.readModel()).threads.find((thread) => thread.id === "thread-archive")
        ?.archivedAt,
    ).not.toBeNull();
    expect(
      (await system.readModel()).threads.find((thread) => thread.id === "thread-archive")
        ?.titleRegeneration,
    ).toBeNull();

    await system.run(
      engine.dispatch({
        type: "thread.unarchive",
        commandId: CommandId.make("cmd-thread-unarchive"),
        threadId: ThreadId.make("thread-archive"),
      }),
    );
    expect(
      (await system.readModel()).threads.find((thread) => thread.id === "thread-archive")
        ?.archivedAt,
    ).toBeNull();
    expect(
      (await system.readModel()).threads.find((thread) => thread.id === "thread-archive")
        ?.titleRegeneration,
    ).toBeNull();
    await system.run(
      engine.dispatch({
        type: "thread.title.regeneration.complete",
        commandId: CommandId.make("cmd-thread-archive-stale-title-completion"),
        threadId: ThreadId.make("thread-archive"),
        requestId: CommandId.make("cmd-thread-archive-title-regeneration"),
        title: "Stale generated title",
      }),
    );
    expect(
      (await system.readModel()).threads.find((thread) => thread.id === "thread-archive")?.title,
    ).toBe("Archive me");

    await system.dispose();
  });

  it("replays append-only events from sequence", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-replay-create"),
        projectId: asProjectId("project-replay"),
        title: "Replay Project",
        workspaceRoot: "/tmp/project-replay",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-replay-create"),
        threadId: ThreadId.make("thread-replay"),
        projectId: asProjectId("project-replay"),
        title: "replay",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.delete",
        commandId: CommandId.make("cmd-thread-replay-delete"),
        threadId: ThreadId.make("thread-replay"),
      }),
    );

    const events = await system.run(
      Stream.runCollect(engine.readEvents(0)).pipe(
        Effect.map((chunk): OrchestrationEvent[] => Array.from(chunk)),
      ),
    );
    expect(events.map((event) => event.type)).toEqual([
      "project.created",
      "thread.created",
      "thread.deleted",
    ]);
    await system.dispose();
  });

  it("streams persisted domain events in order", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-stream-create"),
        projectId: asProjectId("project-stream"),
        title: "Stream Project",
        workspaceRoot: "/tmp/project-stream",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );

    const eventTypes: string[] = [];
    await system.run(
      Effect.gen(function* () {
        const eventQueue = yield* Queue.unbounded<OrchestrationEvent>();
        yield* Effect.forkScoped(
          Stream.take(engine.streamDomainEvents, 2).pipe(
            Stream.runForEach((event) => Queue.offer(eventQueue, event).pipe(Effect.asVoid)),
          ),
        );
        yield* Effect.sleep("10 millis");
        yield* engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("cmd-stream-thread-create"),
          threadId: ThreadId.make("thread-stream"),
          projectId: asProjectId("project-stream"),
          title: "domain-stream",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt,
        });
        yield* engine.dispatch({
          type: "thread.meta.update",
          commandId: CommandId.make("cmd-stream-thread-update"),
          threadId: ThreadId.make("thread-stream"),
          title: "domain-stream-updated",
        });
        eventTypes.push((yield* Queue.take(eventQueue)).type);
        eventTypes.push((yield* Queue.take(eventQueue)).type);
      }).pipe(Effect.scoped),
    );

    expect(eventTypes).toEqual(["thread.created", "thread.meta-updated"]);
    await system.dispose();
  });

  it("does not regress a generated branch to a stale temporary worktree branch", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-branch-race-project-create"),
        projectId: asProjectId("project-branch-race"),
        title: "Branch Race Project",
        workspaceRoot: "/tmp/project-branch-race",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-branch-race-thread-create"),
        threadId: ThreadId.make("thread-branch-race"),
        projectId: asProjectId("project-branch-race"),
        title: "Branch Race Thread",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: "t3code/generated-branch-name",
        worktreePath: "/tmp/project-branch-race-worktree",
        createdAt,
      }),
    );

    await system.run(
      engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-stale-temporary-branch-sync"),
        threadId: ThreadId.make("thread-branch-race"),
        branch: "t3code/1234abcd",
        expectedBranch: "t3code/1234abcd",
      }),
    );

    const snapshot = await system.readModel();
    expect(snapshot.threads[0]?.branch).toBe("t3code/generated-branch-name");
    await system.dispose();
  });

  it.each(["unlink", "relink", "branch", "worktree", "project", "delete"] as const)(
    "rejects PR discovery completed after a newer %s command",
    async (change) => {
      const system = await createOrchestrationSystem(undefined, {
        resolve: (workspaceRoot) =>
          Effect.succeed({
            canonicalKey: "example.test/owner/repository",
            provider: "github",
            displayName: "owner/repository",
            rootPath: workspaceRoot,
            locator: {
              source: "git-remote",
              remoteName: "origin",
              remoteUrl: "https://example.test/owner/repository.git",
            },
          }),
      });
      // Same-tick links must replace the old PR, not rely on timestamp ordering.
      const clock = vi.spyOn(Date, "now").mockReturnValue(Date.parse(now()));
      try {
        const projectId = ProjectId.make("pr-race-project");
        const threadId = ThreadId.make("pr-race-thread");
        const previous = {
          projectId,
          repository: "owner/repository",
          number: 1,
          url: "https://example.test/owner/repository/pull/1",
        };
        const replacement = {
          ...previous,
          number: 2,
          url: "https://example.test/owner/repository/pull/2",
        };
        await system.run(
          system.engine.dispatch({
            type: "project.create",
            commandId: CommandId.make("pr-race-project-create"),
            projectId,
            title: "PR race project",
            workspaceRoot: "/tmp/pr-race-project",
            defaultModelSelection: null,
            createdAt: now(),
          }),
        );
        await system.run(
          system.engine.dispatch({
            type: "thread.create",
            commandId: CommandId.make("pr-race-thread-create"),
            threadId,
            projectId,
            title: "PR race thread",
            modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: "feature",
            worktreePath: null,
            createdAt: now(),
          }),
        );
        const observed = await system.run(
          system.engine.dispatch({
            type: "thread.meta.update",
            commandId: CommandId.make("pr-race-link"),
            threadId,
            linkedPullRequest: previous,
          }),
        );
        expect((await system.readModel()).threads[0]?.linkedPullRequest).toEqual(previous);
        const metadataChanges = {
          unlink: { linkedPullRequest: null },
          relink: {
            linkedPullRequest: {
              ...previous,
              number: 3,
              url: "https://example.test/owner/repository/pull/3",
            },
          },
          branch: { branch: "another-feature" },
          worktree: { worktreePath: "/tmp/another-worktree" },
          project: {},
        };
        await system.run(
          system.engine.dispatch(
            change === "project"
              ? {
                  type: "project.meta.update",
                  commandId: CommandId.make("pr-race-project-move"),
                  projectId,
                  workspaceRoot: "/tmp/another-project-root",
                }
              : change === "delete"
                ? { type: "thread.delete", commandId: CommandId.make("pr-race-delete"), threadId }
                : {
                    type: "thread.meta.update",
                    commandId: CommandId.make(`pr-race-${change}`),
                    threadId,
                    ...metadataChanges[change],
                  },
          ),
        );
        const command = {
          type: "thread.pull-request.sync",
          commandId: CommandId.make("pr-race-stale-sync"),
          threadId,
          projectId,
          snapshotSequence: observed.sequence,
          expected: {
            workspaceRoot: "/tmp/pr-race-project",
            branch: "feature",
            worktreePath: null,
            linkedPullRequest: previous,
            branchPullRequest: null,
          },
          branchPullRequest: replacement,
          linkedPullRequest: replacement,
        } satisfies OrchestrationCommand;
        const error = await system.run(system.engine.dispatch(command).pipe(Effect.flip));
        expect(error._tag).toBe("OrchestrationCommandInvariantError");
        if (change === "delete") return;
        const current = (await system.readModel()).threads[0];
        expect(current?.branchPullRequest ?? null).toBeNull();
        expect(current?.pullRequests.map((link) => link.number)).toEqual(
          change === "unlink" ? [] : change === "relink" ? [3] : [1],
        );
        expect(current?.linkedPullRequest ?? null).toEqual(
          change === "unlink"
            ? null
            : change === "relink"
              ? metadataChanges.relink.linkedPullRequest
              : previous,
        );
      } finally {
        clock.mockRestore();
        await system.dispose();
      }
    },
  );

  it("saves PR associations through streaming and unrelated metadata edits", async () => {
    const system = await createOrchestrationSystem();
    try {
      const projectId = ProjectId.make("pr-sync-project");
      const threadId = ThreadId.make("pr-sync-thread");
      await system.run(
        system.engine.dispatch({
          type: "project.create",
          commandId: CommandId.make("pr-sync-project-create"),
          projectId,
          title: "PR sync project",
          workspaceRoot: "/tmp/pr-sync-project",
          defaultModelSelection: null,
          createdAt: now(),
        }),
      );
      const created = await system.run(
        system.engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("pr-sync-thread-create"),
          threadId,
          projectId,
          title: "PR sync thread",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: "feature",
          worktreePath: null,
          createdAt: now(),
        }),
      );
      const reference = {
        projectId,
        repository: "owner/repository",
        number: 42,
        url: "https://example.test/owner/repository/pull/42",
      };
      const activityAt = "2026-01-01T01:00:00.000Z";
      await system.run(
        system.engine.dispatch({
          type: "thread.message.assistant.delta",
          commandId: CommandId.make("pr-sync-streaming-message"),
          threadId,
          messageId: MessageId.make("pr-sync-message"),
          delta: "The PR is ready.",
          createdAt: activityAt,
        }),
      );
      await system.run(
        system.engine.dispatch({
          type: "thread.meta.update",
          commandId: CommandId.make("pr-sync-title-and-model"),
          threadId,
          title: "Renamed thread",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
        }),
      );
      await system.run(
        system.engine.dispatch({
          type: "project.meta.update",
          commandId: CommandId.make("pr-sync-project-title"),
          projectId,
          title: "Renamed project",
        }),
      );
      const beforeSync = (await system.readModel()).threads[0];
      await system.run(
        system.engine.dispatch({
          type: "thread.pull-request.sync",
          commandId: CommandId.make("pr-sync-discovery"),
          projectId,
          threadId,
          snapshotSequence: created.sequence,
          expected: {
            workspaceRoot: "/tmp/pr-sync-project",
            branch: "feature",
            worktreePath: null,
            linkedPullRequest: null,
            branchPullRequest: null,
          },
          branchPullRequest: reference,
        }),
      );
      const current = (await system.readModel()).threads[0];
      expect(current?.branchPullRequest).toEqual(reference);
      expect(current?.linkedPullRequest ?? null).toBeNull();
      expect(current?.updatedAt).toBe(beforeSync?.updatedAt);
    } finally {
      await system.dispose();
    }
  });

  it("allows authoritative worktree bootstrap to assign a temporary branch", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-worktree-bootstrap-project-create"),
        projectId: asProjectId("project-worktree-bootstrap"),
        title: "Worktree Bootstrap Project",
        workspaceRoot: "/tmp/project-worktree-bootstrap",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-worktree-bootstrap-thread-create"),
        threadId: ThreadId.make("thread-worktree-bootstrap"),
        projectId: asProjectId("project-worktree-bootstrap"),
        title: "Worktree Bootstrap Thread",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: "main",
        worktreePath: null,
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-authoritative-worktree-bootstrap"),
        threadId: ThreadId.make("thread-worktree-bootstrap"),
        branch: "t3code/1234abcd",
        worktreePath: "/tmp/project-worktree-bootstrap-worktree",
      }),
    );

    const snapshot = await system.readModel();
    expect(snapshot.threads[0]?.branch).toBe("t3code/1234abcd");
    expect(snapshot.threads[0]?.worktreePath).toBe("/tmp/project-worktree-bootstrap-worktree");
    await system.dispose();
  });

  it("records command ack duration using the first committed event type", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-ack-create"),
        projectId: asProjectId("project-ack"),
        title: "Ack Project",
        workspaceRoot: "/tmp/project-ack",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );

    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-ack-create"),
        threadId: ThreadId.make("thread-ack"),
        projectId: asProjectId("project-ack"),
        title: "Ack Thread",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    const snapshots = await system.run(Metric.snapshot);
    expect(
      hasMetricSnapshot(snapshots, "t3_orchestration_command_ack_duration", {
        commandType: "thread.create",
        aggregateKind: "thread",
        ackEventType: "thread.created",
      }),
    ).toBe(true);

    await system.dispose();
  });

  it("records failed command dispatches as metric failures", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await expect(
      system.run(
        engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("cmd-thread-missing-project"),
          threadId: ThreadId.make("thread-missing-project"),
          projectId: asProjectId("project-missing"),
          title: "Missing Project Thread",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access",
          branch: null,
          worktreePath: null,
          createdAt,
        }),
      ),
    ).rejects.toThrow("does not exist");

    const snapshots = await system.run(Metric.snapshot);
    expect(
      hasMetricSnapshot(snapshots, "t3_orchestration_commands_total", {
        commandType: "thread.create",
        aggregateKind: "thread",
        outcome: "failure",
      }),
    ).toBe(true);

    await system.dispose();
  });

  it("stores completed checkpoint summaries even when no files changed", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-turn-diff-create"),
        projectId: asProjectId("project-turn-diff"),
        title: "Turn Diff Project",
        workspaceRoot: "/tmp/project-turn-diff",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-turn-diff-create"),
        threadId: ThreadId.make("thread-turn-diff"),
        projectId: asProjectId("project-turn-diff"),
        title: "Turn diff thread",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.turn.diff.complete",
        commandId: CommandId.make("cmd-turn-diff-complete"),
        threadId: ThreadId.make("thread-turn-diff"),
        turnId: asTurnId("turn-1"),
        completedAt: createdAt,
        checkpointRef: asCheckpointRef("refs/t3/checkpoints/thread-turn-diff/turn/1"),
        status: "ready",
        files: [],
        checkpointTurnCount: 1,
        createdAt,
      }),
    );

    const thread = (await system.readModel()).threads.find(
      (entry) => entry.id === "thread-turn-diff",
    );
    expect(thread?.checkpoints).toEqual([
      {
        turnId: asTurnId("turn-1"),
        checkpointTurnCount: 1,
        checkpointRef: asCheckpointRef("refs/t3/checkpoints/thread-turn-diff/turn/1"),
        status: "ready",
        files: [],
        assistantMessageId: null,
        completedAt: createdAt,
      },
    ]);
    await system.dispose();
  });

  it("keeps processing queued commands after a storage failure", async () => {
    type StoredEvent =
      ReturnType<OrchestrationEventStoreShape["append"]> extends Effect.Effect<infer A, any, any>
        ? A
        : never;
    const events: StoredEvent[] = [];
    let nextSequence = 1;
    let shouldFailFirstAppend = true;

    const flakyStore: OrchestrationEventStoreShape = {
      readMetadataByCommandId: () => Effect.die("unused command metadata lookup"),
      append(event) {
        if (shouldFailFirstAppend && event.commandId === CommandId.make("cmd-flaky-1")) {
          shouldFailFirstAppend = false;
          return Effect.fail(
            new PersistenceSqlError({
              operation: "test.append",
              detail: "append failed",
            }),
          );
        }
        const savedEvent = {
          ...event,
          sequence: nextSequence,
        } as StoredEvent;
        nextSequence += 1;
        events.push(savedEvent);
        return Effect.succeed(savedEvent);
      },
      readFromSequence(sequenceExclusive) {
        return Stream.fromIterable(events.filter((event) => event.sequence > sequenceExclusive));
      },
      readAll() {
        return Stream.fromIterable(events);
      },
      hasEventAfter: () => Effect.succeed(false),
      readAggregateRange: () => Stream.die("unused aggregate replay"),
      getAggregateReplayStats: () => Effect.die("unused aggregate replay stats"),
    };

    const ServerConfigLayer = ServerConfig.layerTest(process.cwd(), {
      prefix: "t3-orchestration-engine-test-",
    });

    const runtime = ManagedRuntime.make(
      OrchestrationEngineLive.pipe(
        Layer.provide(OrchestrationProjectionSnapshotQueryLive),
        Layer.provide(ThreadBackgroundLiveness.layer),
        Layer.provide(ThreadPlanProgress.layer),
        Layer.provide(OrchestrationProjectionPipelineLive),
        Layer.provide(Layer.succeed(OrchestrationEventStore, flakyStore)),
        Layer.provide(OrchestrationCommandReceiptRepositoryLive),
        Layer.provide(RepositoryIdentityResolver.layer),
        Layer.provide(SqlitePersistenceMemory),
        Layer.provideMerge(ServerConfigLayer),
        Layer.provideMerge(NodeServices.layer),
      ),
    );
    const engine = await runtime.runPromise(Effect.service(OrchestrationEngineService));
    const createdAt = now();

    await runtime.runPromise(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-flaky-create"),
        projectId: asProjectId("project-flaky"),
        title: "Flaky Project",
        workspaceRoot: "/tmp/project-flaky",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );

    await expect(
      runtime.runPromise(
        engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("cmd-flaky-1"),
          threadId: ThreadId.make("thread-flaky-fail"),
          projectId: asProjectId("project-flaky"),
          title: "flaky-fail",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt,
        }),
      ),
    ).rejects.toThrow("append failed");

    const result = await runtime.runPromise(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-flaky-2"),
        threadId: ThreadId.make("thread-flaky-ok"),
        projectId: asProjectId("project-flaky"),
        title: "flaky-ok",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    expect(result.sequence).toBe(2);
    const eventsAfterRetry = await runtime.runPromise(
      Stream.runCollect(engine.readEvents(0)).pipe(
        Effect.map((chunk): OrchestrationEvent[] => Array.from(chunk)),
      ),
    );
    expect(eventsAfterRetry.map((event) => event.type)).toEqual([
      "project.created",
      "thread.created",
    ]);
    await runtime.dispose();
  });

  it("rolls back all events for a multi-event command when projection fails mid-dispatch", async () => {
    let shouldFailRequestedProjection = true;
    const flakyProjectionPipeline: OrchestrationProjectionPipelineShape = {
      bootstrap: Effect.void,
      projectEvent: () => Effect.void,
      projectEventDeferred: (event) => {
        if (
          shouldFailRequestedProjection &&
          event.commandId === CommandId.make("cmd-turn-start-atomic") &&
          event.type === "thread.turn-start-requested"
        ) {
          shouldFailRequestedProjection = false;
          return Effect.fail(
            new PersistenceSqlError({
              operation: "test.projection",
              detail: "projection failed",
            }),
          );
        }
        return Effect.succeed(Effect.void);
      },
    };

    const runtime = ManagedRuntime.make(
      OrchestrationEngineLive.pipe(
        Layer.provide(OrchestrationProjectionSnapshotQueryLive),
        Layer.provide(ThreadBackgroundLiveness.layer),
        Layer.provide(ThreadPlanProgress.layer),
        Layer.provide(Layer.succeed(OrchestrationProjectionPipeline, flakyProjectionPipeline)),
        Layer.provide(OrchestrationEventStoreLive),
        Layer.provide(OrchestrationCommandReceiptRepositoryLive),
        Layer.provide(RepositoryIdentityResolver.layer),
        Layer.provide(SqlitePersistenceMemory),
        Layer.provide(NodeServices.layer),
      ),
    );
    const engine = await runtime.runPromise(Effect.service(OrchestrationEngineService));
    const createdAt = now();

    await runtime.runPromise(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-atomic-create"),
        projectId: asProjectId("project-atomic"),
        title: "Atomic Project",
        workspaceRoot: "/tmp/project-atomic",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await runtime.runPromise(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-atomic-create"),
        threadId: ThreadId.make("thread-atomic"),
        projectId: asProjectId("project-atomic"),
        title: "atomic",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    const turnStartCommand = {
      type: "thread.turn.start" as const,
      commandId: CommandId.make("cmd-turn-start-atomic"),
      threadId: ThreadId.make("thread-atomic"),
      message: {
        messageId: asMessageId("msg-atomic-1"),
        role: "user" as const,
        text: "hello",
        attachments: [],
      },
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      runtimeMode: "approval-required" as const,
      createdAt,
    };

    await expect(runtime.runPromise(engine.dispatch(turnStartCommand))).rejects.toThrow(
      "projection failed",
    );

    const eventsAfterFailure = await runtime.runPromise(
      Stream.runCollect(engine.readEvents(0)).pipe(
        Effect.map((chunk): OrchestrationEvent[] => Array.from(chunk)),
      ),
    );
    expect(eventsAfterFailure.map((event) => event.type)).toEqual([
      "project.created",
      "thread.created",
    ]);

    const retryResult = await runtime.runPromise(engine.dispatch(turnStartCommand));
    expect(retryResult.sequence).toBe(4);

    const eventsAfterRetry = await runtime.runPromise(
      Stream.runCollect(engine.readEvents(0)).pipe(
        Effect.map((chunk): OrchestrationEvent[] => Array.from(chunk)),
      ),
    );
    expect(eventsAfterRetry.map((event) => event.type)).toEqual([
      "project.created",
      "thread.created",
      "thread.message-sent",
      "thread.turn-start-requested",
    ]);
    expect(
      eventsAfterRetry.filter((event) => event.commandId === turnStartCommand.commandId),
    ).toHaveLength(2);

    await runtime.dispose();
  });

  it("reconciles command state when append persists but projection fails", async () => {
    type StoredEvent =
      ReturnType<OrchestrationEventStoreShape["append"]> extends Effect.Effect<infer A, any, any>
        ? A
        : never;
    const events: StoredEvent[] = [];
    let nextSequence = 1;

    const nonTransactionalStore: OrchestrationEventStoreShape = {
      readMetadataByCommandId: () => Effect.die("unused command metadata lookup"),
      append(event) {
        const savedEvent = {
          ...event,
          sequence: nextSequence,
        } as StoredEvent;
        nextSequence += 1;
        events.push(savedEvent);
        return Effect.succeed(savedEvent);
      },
      readFromSequence(sequenceExclusive) {
        return Stream.fromIterable(events.filter((event) => event.sequence > sequenceExclusive));
      },
      readAll() {
        return Stream.fromIterable(events);
      },
      hasEventAfter: () => Effect.succeed(false),
      readAggregateRange: () => Stream.die("unused aggregate replay"),
      getAggregateReplayStats: () => Effect.die("unused aggregate replay stats"),
    };

    let shouldFailProjection = true;
    const flakyProjectionPipeline: OrchestrationProjectionPipelineShape = {
      bootstrap: Effect.void,
      projectEvent: () => Effect.void,
      projectEventDeferred: (event) => {
        if (
          shouldFailProjection &&
          event.commandId === CommandId.make("cmd-thread-archive-sync-fail")
        ) {
          shouldFailProjection = false;
          return Effect.fail(
            new PersistenceSqlError({
              operation: "test.projection",
              detail: "projection failed",
            }),
          );
        }
        return Effect.succeed(Effect.void);
      },
    };

    const runtime = ManagedRuntime.make(
      OrchestrationEngineLive.pipe(
        Layer.provide(OrchestrationProjectionSnapshotQueryLive),
        Layer.provide(ThreadBackgroundLiveness.layer),
        Layer.provide(ThreadPlanProgress.layer),
        Layer.provide(Layer.succeed(OrchestrationProjectionPipeline, flakyProjectionPipeline)),
        Layer.provide(Layer.succeed(OrchestrationEventStore, nonTransactionalStore)),
        Layer.provide(OrchestrationCommandReceiptRepositoryLive),
        Layer.provide(RepositoryIdentityResolver.layer),
        Layer.provide(SqlitePersistenceMemory),
        Layer.provide(NodeServices.layer),
      ),
    );
    const engine = await runtime.runPromise(Effect.service(OrchestrationEngineService));
    const createdAt = now();

    await runtime.runPromise(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-sync-create"),
        projectId: asProjectId("project-sync"),
        title: "Sync Project",
        workspaceRoot: "/tmp/project-sync",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await runtime.runPromise(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-sync-create"),
        threadId: ThreadId.make("thread-sync"),
        projectId: asProjectId("project-sync"),
        title: "sync-before",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    await expect(
      runtime.runPromise(
        engine.dispatch({
          type: "thread.archive",
          commandId: CommandId.make("cmd-thread-archive-sync-fail"),
          threadId: ThreadId.make("thread-sync"),
        }),
      ),
    ).rejects.toThrow("projection failed");

    await expect(
      runtime.runPromise(
        engine.dispatch({
          type: "thread.archive",
          commandId: CommandId.make("cmd-thread-archive-sync-retry"),
          threadId: ThreadId.make("thread-sync"),
        }),
      ),
    ).rejects.toThrow("already archived");

    await runtime.dispose();
  });

  it("does not republish another server's turn when a local dispatch fails", async () => {
    const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-shared-db-"));
    const databasePath = NodePath.join(directory, "state.sqlite");
    const serverA = await createOrchestrationSystem(databasePath);
    const serverB = await createOrchestrationSystem(databasePath);
    const threadId = ThreadId.make("thread-shared");
    const createdAt = now();
    const sentinelCommandId = CommandId.make("cmd-shared-rename");
    try {
      await serverA.run(
        serverA.engine.dispatch({
          type: "project.create",
          commandId: CommandId.make("cmd-shared-project-create"),
          projectId: asProjectId("project-shared"),
          title: "Shared Project",
          workspaceRoot: "/tmp/project-shared",
          createdAt,
        }),
      );
      await serverA.run(
        serverA.engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("cmd-shared-thread-create"),
          threadId,
          projectId: asProjectId("project-shared"),
          title: "shared",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5-codex" },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt,
        }),
      );
      await serverA.run(
        serverA.engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-shared-turn-start"),
          threadId,
          message: {
            messageId: asMessageId("msg-shared"),
            role: "user",
            text: "hello",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt,
        }),
      );

      const published = await serverB.run(
        Effect.gen(function* () {
          const events = yield* serverB.engine.subscribeDomainEvents;
          // B's command model is still empty, so this fails and reconciles.
          yield* serverB.engine
            .dispatch({
              type: "thread.meta.update",
              commandId: CommandId.make("cmd-shared-stale-rename"),
              threadId,
              title: "stale",
            })
            .pipe(Effect.flip);
          yield* serverB.engine.dispatch({
            type: "thread.meta.update",
            commandId: sentinelCommandId,
            threadId,
            title: "renamed on B",
          });
          return yield* Stream.runCollect(
            Stream.takeUntil(events, (event) => event.commandId === sentinelCommandId),
          );
        }).pipe(Effect.scoped),
      );

      expect(Array.from(published).map((event) => event.type)).toEqual(["thread.meta-updated"]);
    } finally {
      await serverA.dispose();
      await serverB.dispose();
      await NodeFSP.rm(directory, { recursive: true, force: true });
    }
  });

  it("fails command dispatch when command invariants are violated", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;

    await expect(
      system.run(
        engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-invariant-missing-thread"),
          threadId: ThreadId.make("thread-missing"),
          message: {
            messageId: asMessageId("msg-missing"),
            role: "user",
            text: "hello",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt: now(),
        }),
      ),
    ).rejects.toThrow("Thread 'thread-missing' does not exist");

    await system.dispose();
  });

  it("rejects duplicate thread creation", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-duplicate-create"),
        projectId: asProjectId("project-duplicate"),
        title: "Duplicate Project",
        workspaceRoot: "/tmp/project-duplicate",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );

    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-duplicate-1"),
        threadId: ThreadId.make("thread-duplicate"),
        projectId: asProjectId("project-duplicate"),
        title: "duplicate",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    await expect(
      system.run(
        engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("cmd-thread-duplicate-2"),
          threadId: ThreadId.make("thread-duplicate"),
          projectId: asProjectId("project-duplicate"),
          title: "duplicate",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt,
        }),
      ),
    ).rejects.toThrow("already exists");

    await system.dispose();
  });

  it("replays the accepted receipt for a genuine retry of the same command", async () => {
    const createdAt = now();
    const system = await createOrchestrationSystem();
    const { engine } = system;

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-retry-project-create"),
        projectId: asProjectId("project-retry"),
        title: "Retry Project",
        workspaceRoot: "/tmp/project-retry",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-retry-thread-create"),
        threadId: ThreadId.make("thread-retry"),
        projectId: asProjectId("project-retry"),
        title: "retry",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    const turnStart = {
      type: "thread.turn.start",
      commandId: CommandId.make("cmd-retry-turn-start"),
      threadId: ThreadId.make("thread-retry"),
      message: {
        messageId: asMessageId("msg-retry"),
        role: "user",
        text: "hello",
        attachments: [],
      },
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      runtimeMode: "approval-required",
      createdAt,
    } as const;

    const first = await system.run(engine.dispatch(turnStart));
    const second = await system.run(engine.dispatch(turnStart));
    expect(second.sequence).toBe(first.sequence);

    const readModel = await system.readModel();
    const thread = readModel.threads.find((candidate) => candidate.id === "thread-retry");
    expect(thread?.messages.filter((message) => message.role === "user")).toHaveLength(1);

    await system.dispose();
  });

  it("rejects reusing an accepted command id for a different aggregate", async () => {
    const createdAt = now();
    const system = await createOrchestrationSystem();
    const { engine } = system;

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-conflict-project-create"),
        projectId: asProjectId("project-conflict"),
        title: "Conflict Project",
        workspaceRoot: "/tmp/project-conflict",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    for (const threadId of ["thread-conflict-a", "thread-conflict-b"]) {
      await system.run(
        engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`cmd-${threadId}-create`),
          threadId: ThreadId.make(threadId),
          projectId: asProjectId("project-conflict"),
          title: threadId,
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt,
        }),
      );
    }

    await system.run(
      engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-conflict-turn-start"),
        threadId: ThreadId.make("thread-conflict-a"),
        message: {
          messageId: asMessageId("msg-conflict-a"),
          role: "user",
          text: "hello",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt,
      }),
    );

    await expect(
      system.run(
        engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-conflict-turn-start"),
          threadId: ThreadId.make("thread-conflict-b"),
          message: {
            messageId: asMessageId("msg-conflict-b"),
            role: "user",
            text: "hello again",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt,
        }),
      ),
    ).rejects.toThrow("already used for thread 'thread-conflict-a'");

    const readModel = await system.readModel();
    const targetThread = readModel.threads.find(
      (candidate) => candidate.id === "thread-conflict-b",
    );
    expect(targetThread?.messages.filter((message) => message.role === "user")).toHaveLength(0);

    await system.dispose();
  });

  it("stamps the dispatching client's origin onto persisted event metadata", async () => {
    const createdAt = now();
    const system = await createOrchestrationSystem();
    const { engine } = system;

    await system.run(
      engine.dispatch(
        {
          type: "project.create",
          commandId: CommandId.make("cmd-origin-project-create"),
          projectId: asProjectId("project-origin"),
          title: "Origin Project",
          workspaceRoot: "/tmp/project-origin",
          defaultModelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          createdAt,
        },
        { origin: { surface: "mobile", appVersion: "1.2.3" } },
      ),
    );
    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-no-origin-project-create"),
        projectId: asProjectId("project-no-origin"),
        title: "No Origin Project",
        workspaceRoot: "/tmp/project-no-origin",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );

    const events = await system.run(
      Stream.runCollect(engine.readEvents(0)).pipe(Effect.map((chunk) => Array.from(chunk))),
    );
    const withOrigin = events.find((event) => event.commandId === "cmd-origin-project-create");
    const withoutOrigin = events.find(
      (event) => event.commandId === "cmd-no-origin-project-create",
    );

    expect(withOrigin?.metadata.origin).toEqual({ surface: "mobile", appVersion: "1.2.3" });
    expect(withoutOrigin?.metadata.origin).toBeUndefined();

    await system.dispose();
  });
});

const prepareNativeEngineFixture = (operationId: string) =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepositoryLayer.make;
    const binding = yield* Schema.decodeUnknownEffect(NativePreparationBinding)({
      backend_instance: "synthetic-backend",
      environment_id: "synthetic-env",
      project_id: "synthetic-project",
      project_cwd: "/synthetic/project",
      account_ref: "synthetic-account",
      runtime_mode: "full-access",
      interaction_mode: "default",
      base_branch: "main",
      start_from_origin: false,
      run_setup_script: false,
      provider_model_selection: { instanceId: "codex", model: "synthetic-model" },
    });
    const raw = nativePreparationCommand(
      operationId,
      binding,
      "Synthetic text",
      "Synthetic thread",
      now(),
    );
    const preparation = yield* validateNativeCreationPreparation(
      new TextEncoder().encode(
        nativeCreationCanonicalJson({
          schema: "voice.t3-bootstrap-preparation/v1",
          operation_id: operationId,
          preparation_id: raw.commandId.replace("voice-command-", "voice-bootstrap-"),
          binding,
          command: raw,
          binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
          prompt_digest: nativeCreationSha256(raw.message.text),
          command_digest: nativeCreationSha256(nativeCreationCanonicalJson(raw)),
        }),
      ),
    );
    const historical = yield* Schema.decodeUnknownEffect(NativeCreationHistoricalBinding)({
      backendInstance: binding.backend_instance,
      environmentId: binding.environment_id,
      projectId: binding.project_id,
      projectCwd: binding.project_cwd,
      accountRef: binding.account_ref,
      accountBindingId: "synthetic-account-binding",
      accountBindingRevision: 1,
      providerModelSelection: binding.provider_model_selection,
      runtimeMode: binding.runtime_mode,
      interactionMode: binding.interaction_mode,
      baseBranch: binding.base_branch,
      startFromOrigin: false,
      runSetupScript: false,
      requestedBranch: raw.bootstrap.prepareWorktree.branch,
    });
    const claimId = `synthetic-${operationId}`;
    yield* repository.claim(
      {
        preparation,
        resources: {
          projectCwd: binding.project_cwd,
          branch: historical.requestedBranch,
          worktreePath: "/synthetic/worktree",
        },
        claimId,
        claimedBootId: "synthetic-boot",
        claimedAt: now(),
        actorSessionId: "synthetic-session",
        grantId: "synthetic-grant",
        grantRevision: 1,
      },
      Effect.succeed(historical),
    );
    yield* repository.reserveCommandIdentities(claimId, nativeBootstrapCommandIds(raw.commandId));
    const command = yield* Schema.decodeUnknownEffect(OrchestrationCommand)({
      type: "thread.create",
      commandId: `${raw.commandId}:bootstrap-thread-create`,
      threadId: raw.threadId,
      ...raw.bootstrap.createThread,
    });
    if (command.type !== "thread.create")
      return yield* Effect.die("Invalid synthetic command fixture");
    yield* repository.reserveCommand(claimId, command);
    const start = yield* repository.startEffect(
      claimId,
      {
        effectId: "synthetic-create-effect",
        kind: "native_command",
        phase: "started",
        timestamp: now(),
        commandId: command.commandId,
        threadId: ThreadId.make(raw.threadId),
        commandType: "thread.create",
        commandDigest: nativeCreationCommandDigest(command),
      },
      Effect.succeed(historical),
    );
    return { repository, claimId, command, start, raw };
  });

describe("native creation engine transaction", () => {
  it("rejects a reserved identity without creation context before command events", async () => {
    const system = await createOrchestrationSystem();
    try {
      const fixture = await system.run(prepareNativeEngineFixture("missing-context"));
      const result = await system.run(system.engine.dispatch(fixture.command).pipe(Effect.result));
      expect(result._tag).toBe("Failure");
      expect(Option.isNone(await system.readThread(fixture.command.threadId))).toBe(true);
      const history = await system.run(fixture.repository.readHistoryByClaim(fixture.claimId));
      expect(history.effects.filter((fact) => fact.phase === "completed")).toHaveLength(0);
    } finally {
      await system.dispose();
    }
  });
  it("commits creation event, projection, receipt and effect completion together", async () => {
    const system = await createOrchestrationSystem();
    try {
      const fixture = await system.run(prepareNativeEngineFixture("atomic-success"));
      await system.run(
        system.engine.dispatch({
          type: "project.create",
          commandId: CommandId.make("synthetic-project-create"),
          projectId: ProjectId.make("synthetic-project"),
          title: "Synthetic",
          workspaceRoot: "/synthetic/project",
          createdAt: now(),
        }),
      );
      const accepted = await system.run(
        system.engine.dispatch(fixture.command, {
          bootstrapEffect: { claimId: fixture.claimId, effectId: fixture.start.effectId },
        }),
      );
      const history = await system.run(fixture.repository.readHistoryByClaim(fixture.claimId));
      const completion = history.effects.find((fact) => fact.phase === "completed");
      expect(completion).toMatchObject({
        kind: "native_command",
        commandType: "thread.create",
        sequence: accepted.sequence,
      });
      expect(Option.isSome(await system.readThread(fixture.command.threadId))).toBe(true);
      const rows = await system.run(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          return yield* sql`SELECT status, result_sequence FROM orchestration_command_receipts WHERE command_id = ${fixture.command.commandId}`;
        }),
      );
      expect(rows).toEqual([{ status: "accepted", result_sequence: accepted.sequence }]);
    } finally {
      await system.dispose();
    }
  });
  it("rolls back events, projection and accepted receipt when fact completion fails", async () => {
    const system = await createOrchestrationSystem();
    try {
      const fixture = await system.run(prepareNativeEngineFixture("atomic-rollback"));
      await system.run(
        system.engine.dispatch({
          type: "project.create",
          commandId: CommandId.make("synthetic-project-create"),
          projectId: ProjectId.make("synthetic-project"),
          title: "Synthetic",
          workspaceRoot: "/synthetic/project",
          createdAt: now(),
        }),
      );
      await system.run(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql.unsafe(
            "CREATE TRIGGER synthetic_completion_failure BEFORE INSERT ON native_creation_effect_facts WHEN NEW.phase = 'completed' BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END",
          );
        }),
      );
      const result = await system.run(
        system.engine
          .dispatch(fixture.command, {
            bootstrapEffect: { claimId: fixture.claimId, effectId: fixture.start.effectId },
          })
          .pipe(Effect.result),
      );
      expect(result._tag).toBe("Failure");
      expect(Option.isNone(await system.readThread(fixture.command.threadId))).toBe(true);
      const history = await system.run(fixture.repository.readHistoryByClaim(fixture.claimId));
      expect(history.effects.filter((fact) => fact.phase === "completed")).toHaveLength(0);
      const rows = await system.run(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          return yield* sql`SELECT COUNT(*) AS count FROM orchestration_events WHERE command_id = ${fixture.command.commandId}`;
        }),
      );
      expect(rows).toEqual([{ count: 0 }]);
    } finally {
      await system.dispose();
    }
  });
});

const prepareNativeCleanupFixture = (
  engine: OrchestrationEngineService["Service"],
  operationId: string,
) =>
  Effect.gen(function* () {
    const fixture = yield* prepareNativeEngineFixture(operationId);
    yield* engine.dispatch({
      type: "project.create",
      commandId: CommandId.make(`${operationId}:project-create`),
      projectId: fixture.command.projectId,
      title: "Synthetic cleanup project",
      workspaceRoot: "/synthetic/project",
      createdAt: now(),
    });
    yield* engine.dispatch(fixture.command, {
      bootstrapEffect: { claimId: fixture.claimId, effectId: fixture.start.effectId },
    });
    const history = yield* fixture.repository.readHistoryByClaim(fixture.claimId);
    const created = history.effects.find(
      (fact) =>
        fact.kind === "native_command" &&
        fact.phase === "completed" &&
        fact.commandType === "thread.create",
    );
    if (created?.kind !== "native_command" || created.phase !== "completed")
      return yield* Effect.die("Missing synthetic creation incarnation");
    yield* fixture.repository.startEffect(
      fixture.claimId,
      {
        kind: "cleanup",
        phase: "started",
        effectId: `${operationId}:cleanup-thread`,
        timestamp: now(),
        resource: {
          kind: "thread",
          threadId: fixture.command.threadId,
          incarnation: { eventId: created.eventId, sequence: created.sequence },
        },
        recoveryScopeId: `${operationId}:cleanup:thread`,
      },
      Effect.succeed(history.intent.binding),
    );
    const cleanupCommand = {
      type: "thread.delete" as const,
      commandId: CommandId.make(`${fixture.raw.commandId}:bootstrap-thread-delete`),
      threadId: fixture.command.threadId,
    };
    yield* fixture.repository.reserveCommand(fixture.claimId, cleanupCommand);
    const cleanupStart = yield* fixture.repository.startEffect(
      fixture.claimId,
      {
        kind: "native_command",
        phase: "started",
        effectId: `${operationId}:cleanup-command`,
        timestamp: now(),
        commandId: cleanupCommand.commandId,
        threadId: cleanupCommand.threadId,
        commandType: cleanupCommand.type,
        commandDigest: nativeCreationCommandDigest(cleanupCommand),
      },
      Effect.succeed(history.intent.binding),
    );
    return { ...fixture, cleanupCommand, cleanupStart };
  });

describe("native cleanup incarnation fence", () => {
  it("rejects historical cleanup after replacement without effective deletion", async () => {
    const system = await createOrchestrationSystem();
    try {
      const fixture = await system.run(
        prepareNativeCleanupFixture(system.engine, "cleanup-replacement"),
      );
      await system.run(
        system.engine.dispatch({
          type: "thread.delete",
          commandId: CommandId.make("ordinary-delete-original"),
          threadId: fixture.command.threadId,
        }),
      );
      await system.run(
        system.engine.dispatch({
          ...fixture.command,
          commandId: CommandId.make("ordinary-create-replacement"),
          title: "Replacement incarnation",
        }),
      );
      const beforeThread = await system.readThread(fixture.command.threadId);
      const beforeModel = await system.readModel();
      const beforeHistory = await system.run(
        fixture.repository.readHistoryByClaim(fixture.claimId),
      );
      expect(Option.isSome(beforeThread)).toBe(true);
      const result = await system.run(
        system.engine
          .dispatch(fixture.cleanupCommand, {
            bootstrapEffect: { claimId: fixture.claimId, effectId: fixture.cleanupStart.effectId },
          })
          .pipe(Effect.result),
      );
      expect(result._tag).toBe("Failure");
      expect(await system.readThread(fixture.command.threadId)).toEqual(beforeThread);
      expect(await system.readModel()).toEqual(beforeModel);
      expect(await system.run(fixture.repository.readHistoryByClaim(fixture.claimId))).toEqual(
        beforeHistory,
      );
      const rows = await system.run(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          return {
            events:
              yield* sql`SELECT COUNT(*) AS count FROM orchestration_events WHERE command_id = ${fixture.cleanupCommand.commandId}`,
            receipts:
              yield* sql`SELECT COUNT(*) AS count FROM orchestration_command_receipts WHERE command_id = ${fixture.cleanupCommand.commandId} AND status = 'accepted'`,
          };
        }),
      );
      expect(rows).toEqual({ events: [{ count: 0 }], receipts: [{ count: 0 }] });
    } finally {
      await system.dispose();
    }
  });

  it("accepts cleanup of the unchanged authorized incarnation", async () => {
    const system = await createOrchestrationSystem();
    try {
      const fixture = await system.run(
        prepareNativeCleanupFixture(system.engine, "cleanup-unchanged"),
      );
      const accepted = await system.run(
        system.engine.dispatch(fixture.cleanupCommand, {
          bootstrapEffect: { claimId: fixture.claimId, effectId: fixture.cleanupStart.effectId },
        }),
      );
      expect(Option.isNone(await system.readThread(fixture.command.threadId))).toBe(true);
      const history = await system.run(fixture.repository.readHistoryByClaim(fixture.claimId));
      expect(
        history.effects.find(
          (fact) => fact.effectId === fixture.cleanupStart.effectId && fact.phase === "completed",
        ),
      ).toMatchObject({
        kind: "native_command",
        commandType: "thread.delete",
        sequence: accepted.sequence,
      });
      const rows = await system.run(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          return yield* sql`SELECT status, result_sequence FROM orchestration_command_receipts WHERE command_id = ${fixture.cleanupCommand.commandId}`;
        }),
      );
      expect(rows).toEqual([{ status: "accepted", result_sequence: accepted.sequence }]);
    } finally {
      await system.dispose();
    }
  });
});
