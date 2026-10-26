import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  EnvironmentId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";

import * as GitManager from "../../git/GitManager.ts";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import * as OrchestratorMcpService from "../../mcp/OrchestratorMcpService.ts";
import * as CommandReceiptStore from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderSessionManager from "../../orchestration-v2/ProviderSessionManager.ts";
import type { RecordedLifecycleEvent } from "../../orchestration-v2/RecordedTypes.ts";
import * as ThreadLaunchService from "../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as ThreadPullRequestService from "../../orchestration-v2/ThreadPullRequestService.ts";
import * as RemoteDelegation from "../../peer/RemoteDelegation.ts";
import * as OrchestrationEventStore from "../../persistence/OrchestrationEventStore.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import * as ProviderRegistry from "../../provider/ProviderRegistry.ts";
import * as PullRequestService from "../../pullRequest/PullRequestService.ts";
import * as ScheduledTaskService from "../../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../../secrets/SecretRequests.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";
import * as ThreadMetadataReads from "./threadMetadataReads.ts";

const now = DateTime.makeUnsafe("2026-10-01T00:00:00.000Z");
const projectId = ProjectId.make("metadata-callers:project");
const instanceId = ProviderInstanceId.make("codex");
const thread = (
  id: string,
  overrides: Partial<OrchestrationV2AppThread> = {},
): OrchestrationV2AppThread => ({
  id: ThreadId.make(id),
  projectId,
  title: id,
  providerInstanceId: instanceId,
  modelSelection: { instanceId, model: "synthetic" },
  createdBy: "user",
  creationSource: "web",
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: "feature/metadata",
  worktreePath: "/synthetic/worktree",
  activeProviderThreadId: null,
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: ThreadId.make(id) },
  forkedFrom: null,
  createdAt: now,
  updatedAt: now,
  archivedAt: null,
  deletedAt: null,
  settledAt: null,
  settledOverride: null,
  lastVisitedAt: null,
  ...overrides,
});
const seed = Effect.fn("threadMetadataCallers.test.seed")(function* (
  rows: ReadonlyArray<OrchestrationV2AppThread>,
) {
  const sql = yield* SqlClient.SqlClient;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const events = yield* OrchestrationEventStore.OrchestrationEventStore;
  for (const row of rows) {
    const event: RecordedLifecycleEvent = {
      id: EventId.make(`created:${row.id}`),
      type: "thread.created",
      threadId: row.id,
      occurredAt: now,
      payload: row,
    };
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* events.appendAgentEvents({ events: [event] });
        yield* projections.apply(event);
      }),
    );
  }
});
const stores = Layer.mergeAll(
  ProjectionStore.layer,
  OrchestrationEventStore.layer,
  ThreadMetadataReads.layer,
);
const memory = stores.pipe(Layer.provideMerge(SqlitePersistence.layerMemory));

it.effect(
  "PR sweeps and single-thread refresh preserve commands and sequence guards using metadata SQL only",
  () =>
    Effect.gen(function* () {
      const active = thread("metadata-callers:active");
      const settled = thread("metadata-callers:settled", { settledAt: now });
      const deleted = thread("metadata-callers:deleted", { deletedAt: now });
      yield* seed([
        active,
        settled,
        deleted,
        thread("metadata-callers:archive", { archivedAt: now }),
      ]);
      const metadata = yield* ThreadMetadataReads.ThreadMetadataReads;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const oracle = yield* projections.getShellSnapshot({ location: "active" });
      const sequence = oracle.snapshotSequence;
      const events = yield* Queue.unbounded<OrchestrationV2DomainEvent>();
      const pulls = yield* Queue.unbounded<void>();
      const listStarted = yield* Queue.unbounded<void>();
      const stages: string[] = [];
      const queries: ThreadMetadataReads.ThreadMetadataQuery[] = [];
      const commands: OrchestrationV2ServerCommand[] = [];
      const statements: string[] = [];
      let legacyReads = 0;
      const capture: Statement.Transformer = (statement) => {
        statements.push(statement.compile()[0]);
        return Effect.succeed(statement);
      };
      const identity = {
        canonicalKey: "github.com/acme/project",
        provider: "github" as const,
        locator: {
          source: "git-remote" as const,
          remoteName: "origin",
          remoteUrl: "git@github.com:acme/project.git",
        },
        owner: "acme",
        name: "project",
      };
      const project = {
        id: projectId,
        title: "Project",
        workspaceRoot: "/synthetic/project",
        defaultModelSelection: null,
        scripts: [],
        repositoryIdentity: identity,
        createdAt: "2026-10-01T00:00:00Z",
        updatedAt: "2026-10-01T00:00:00Z",
      };
      const reference = {
        projectId,
        repository: "acme/project",
        number: 42,
        url: "https://github.com/acme/project/pull/42",
      };
      const dependencies = Layer.mergeAll(
        NodeCrypto.layer,
        FileSystem.layerNoop({}),
        Layer.succeed(ThreadMetadataReads.ThreadMetadataReads, {
          ...metadata,
          list: (query = {}) =>
            Effect.gen(function* () {
              stages.push("metadata");
              queries.push(query);
              yield* Queue.offer(listStarted, undefined);
              return yield* metadata.list(query);
            }),
        }),
        Layer.mock(Orchestrator.OrchestratorV2)({
          streamDomainEvents: Stream.fromEffectRepeat(
            Queue.offer(pulls, undefined).pipe(Effect.andThen(Queue.take(events))),
          ),
          getShellSnapshot: (query) =>
            Effect.suspend(() => {
              legacyReads += 1;
              return projections.getShellSnapshot(query).pipe(
                Effect.tap(() => Queue.offer(listStarted, undefined)),
                Effect.mapError(
                  (cause) =>
                    new Orchestrator.OrchestratorProjectionError({ threadId: active.id, cause }),
                ),
              );
            }),
          getThreadShell: (id) =>
            projections
              .getThreadShell(id)
              .pipe(
                Effect.mapError(
                  (cause) => new Orchestrator.OrchestratorProjectionError({ threadId: id, cause }),
                ),
              ),
          getThreadEventSequence: () =>
            Effect.sync(() => {
              stages.push("sequence");
              return sequence - 1;
            }),
          dispatch: (command) =>
            Effect.sync(() => {
              commands.push(command);
              return { sequence: sequence + 1, storedEvents: [] };
            }),
        }),
        Layer.mock(ProjectStore.ProjectStoreV2)({
          listShells: () => Effect.succeed([project]),
          getShell: () => Effect.succeedSome(project),
        }),
        Layer.mock(GitManager.GitManager)({
          branchPullRequest: () =>
            Effect.succeed({
              ...reference,
              title: "Metadata PR",
              state: "open",
              repositoryKey: identity.canonicalKey,
              baseRef: "main",
              headRef: active.branch!,
              updatedAt: null,
            }),
        }),
        Layer.mock(PullRequestService.PullRequestService)({}),
        Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
          resolve: () => Effect.succeed(identity),
        }),
      );
      yield* Effect.gen(function* () {
        const service = yield* ThreadPullRequestService.make;
        yield* service.start();
        yield* Queue.take(pulls);
        yield* Queue.take(listStarted);
        yield* service.drain;
        const expected = (id: ThreadId, snapshotSequence: number) => ({
          type: "thread.pull-request.sync" as const,
          threadId: id,
          projectId,
          snapshotSequence,
          expected: {
            workspaceRoot: project.workspaceRoot,
            branch: active.branch,
            worktreePath: active.worktreePath,
            linkedPullRequest: null,
            branchPullRequest: null,
          },
          branchPullRequest: reference,
        });
        const withoutId = ({
          commandId: _commandId,
          ...command
        }: Extract<OrchestrationV2ServerCommand, { type: "thread.pull-request.sync" }>) => command;
        const syncCommands = () =>
          commands.filter((command) => command.type === "thread.pull-request.sync").map(withoutId);
        assert.deepEqual(queries, [{ location: "active", unsettledOnly: false }]);
        assert.deepEqual(
          syncCommands(),
          oracle.threads.map((row) => expected(row.id, sequence)),
        );
        commands.length = 0;
        stages.length = 0;
        yield* Queue.offer(events, {
          type: "thread.metadata-updated",
          id: EventId.make("metadata-callers:refresh"),
          threadId: active.id,
          occurredAt: now,
          payload: active,
        });
        yield* Queue.take(pulls);
        yield* service.drain;
        assert.deepEqual(stages, ["sequence", "metadata"]);
        assert.deepEqual(queries.at(-1), { threadIds: [active.id] });
        assert.deepEqual(syncCommands(), [expected(active.id, sequence - 1)]);
        commands.length = 0;
        yield* Queue.offer(events, {
          type: "thread.metadata-updated",
          id: EventId.make("metadata-callers:deleted-refresh"),
          threadId: deleted.id,
          occurredAt: now,
          payload: deleted,
        });
        yield* Queue.take(pulls);
        yield* service.drain;
        assert.deepEqual(commands, []);
        yield* TestClock.adjust("1 minute");
        // Consume the two per-thread signals and then the periodic sweep signal.
        yield* Queue.take(listStarted);
        yield* Queue.take(listStarted);
        yield* Queue.take(listStarted);
        yield* service.drain;
        assert.deepEqual(queries.at(-1), { location: "active", unsettledOnly: true });
        assert.deepEqual(syncCommands(), [expected(active.id, sequence)]);
      }).pipe(
        Effect.provide(dependencies),
        Effect.provideService(Statement.CurrentTransformer, capture),
      );
      assert.equal(legacyReads, 0);
      const selects = statements.filter((query) => /SELECT/i.test(query));
      assert.isTrue(selects.some((query) => query.includes("orchestration_v2_projection_threads")));
      for (const query of selects)
        assert.notMatch(query, /projection_(runs|turn_items|provider_threads)/);
    }).pipe(Effect.provide(memory), Effect.scoped),
);

const fileDatabase = Layer.unwrap(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "jones-metadata-callers-" });
    return stores.pipe(
      Layer.provideMerge(SqlitePersistence.layerFromPath(path.join(directory, "state.sqlite"))),
    );
  }),
);

it.live(
  "t3_thread_list projection reads retain filtering, ordering and pagination while the writer is held",
  () =>
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const first = thread("metadata-list:first", {
        branch: null,
        title: "First",
        updatedAt: DateTime.add(now, { minutes: 1 }),
      });
      const second = thread("metadata-list:second", {
        branch: null,
        title: "Second",
        settledAt: now,
      });
      yield* seed([
        first,
        second,
        thread("metadata-list:archive", { archivedAt: now }),
        thread("metadata-list:deleted", { deletedAt: now }),
      ]);
      const settings = ServerSettings.layerTest();
      const threadServices = ThreadManagement.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            settings,
            Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
            Layer.mock(CommandReceiptStore.CommandReceiptStoreV2)({}),
            Layer.mock(Orchestrator.OrchestratorV2)({
              getShellSnapshot: (query) =>
                projections
                  .getShellSnapshot(query)
                  .pipe(
                    Effect.mapError(
                      (cause) =>
                        new Orchestrator.OrchestratorProjectionError({ threadId: first.id, cause }),
                    ),
                  ),
            }),
          ),
        ),
      );
      const dependencies = Layer.mergeAll(
        NodeCrypto.layer,
        settings,
        // Keep the real listProjectThreads method. Native activity observation is
        // a separate read path outside this marker's scope.
        Layer.unwrap(
          Effect.gen(function* () {
            const threads = yield* ThreadManagement.ThreadManagementService;
            return Layer.mock(ThreadManagement.ThreadManagementService)({
              listProjectThreads: threads.listProjectThreads,
            });
          }),
        ).pipe(Layer.provide(threadServices)),
        Layer.mock(ThreadLaunchService.ThreadLaunchService)({}),
        Layer.mock(GitVcsDriver.GitVcsDriver)({}),
        Layer.mock(ProviderRegistry.ProviderRegistry)({}),
        Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({}),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(SecretRequests.SecretRequests)({}),
        Layer.mock(RemoteDelegation.RemoteDelegation)({}),
      );
      const scope: McpInvocationScope = {
        environmentId: EnvironmentId.make("metadata-list:environment"),
        requestNamespace: "metadata-list",
        thread: undefined,
        client: undefined,
        capabilities: new Set(["orchestration"]),
        issuedAt: 1,
      };
      yield* Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        const all = yield* service.listThreads(scope, { projectId });
        const filtered = yield* service.listThreads(scope, {
          projectId,
          settled: false,
          titleContains: "first",
          limit: 1,
        });
        const page = yield* service.listThreads(scope, { projectId, limit: 1, cursor: 1 });
        assert.deepEqual(
          all.threads.map((row) => row.threadId),
          [first.id, second.id],
        );
        assert.equal(filtered.total, 1);
        assert.equal(filtered.threads[0]?.title, "First");
        assert.equal(page.threads[0]?.threadId, second.id);
        assert.equal(page.nextCursor, null);
        yield* sql`SELECT 1`.pipe(NodeSqliteClient.readOnly);
        const held = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const writer = yield* Effect.forkScoped(
          sql.withTransaction(
            Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release))),
          ),
        );
        yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined));
        yield* Deferred.await(held);
        const reads = yield* Effect.forkChild(
          Effect.all([
            service.listThreads(scope, { projectId }),
            service.listThreads(scope, {
              projectId,
              settled: false,
              titleContains: "first",
              limit: 1,
            }),
            service.listThreads(scope, { projectId, limit: 1, cursor: 1 }),
          ]),
        );
        const result = yield* Fiber.join(reads).pipe(Effect.timeoutOption("5 seconds"));
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(writer);
        if (Option.isNone(result)) {
          yield* Fiber.interrupt(reads);
          assert.fail("t3_thread_list waited behind the writer");
        } else assert.deepEqual(result.value, [all, filtered, page]);
      }).pipe(Effect.provide(OrchestratorMcpService.layer.pipe(Layer.provide(dependencies))));
    }).pipe(Effect.provide(fileDatabase), Effect.scoped, Effect.provide(NodeServices.layer)),
);
