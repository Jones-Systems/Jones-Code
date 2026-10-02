import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  DEFAULT_MODEL,
  DEFAULT_SERVER_SETTINGS,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PlatformError from "effect/PlatformError";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

import * as ServerConfig from "./config.ts";
import * as OrchestrationEngine from "./orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "./orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderSessionDirectory from "./provider/Services/ProviderSessionDirectory.ts";
import * as ServerRuntimeStartup from "./serverRuntimeStartup.ts";
import * as ServerSettings from "./serverSettings.ts";
import * as GitVcsDriver from "./vcs/GitVcsDriver.ts";

it.effect("automatic pull only updates enabled, behind, clean default-branch checkouts", () =>
  Effect.gen(function* () {
    const pulled: string[] = [];
    const git = {
      statusDetails: (cwd: string) =>
        Effect.succeed({
          isRepo: true,
          isDefaultBranch: cwd !== "/feature",
          hasUpstream: true,
          hasWorkingTreeChanges: cwd === "/dirty",
          aheadCount: cwd === "/ahead" ? 1 : 0,
          behindCount: cwd === "/current" ? 0 : 1,
        } as never),
      pullCurrentBranch: (cwd: string) =>
        Effect.sync(() => {
          pulled.push(cwd);
          return {
            status: "pulled" as const,
            refName: "main",
            upstreamRef: "origin/main",
          };
        }),
    } as unknown as GitVcsDriver.GitVcsDriver["Service"];
    const project = (workspaceRoot: string) =>
      ({ id: ProjectId.make(workspaceRoot), workspaceRoot }) as never;
    const overrides = (entries: Record<string, boolean>) => ({
      ...DEFAULT_SERVER_SETTINGS,
      projectSettingsOverrides: Object.fromEntries(
        Object.entries(entries).map(([root, defaultAutoPull]) => [
          ProjectId.make(root),
          { defaultAutoPull },
        ]),
      ),
    });

    yield* ServerRuntimeStartup.autoPullProjects(
      [
        project("/clean"),
        project("/current"),
        project("/dirty"),
        project("/ahead"),
        project("/feature"),
        project("/disabled"),
      ],
      overrides({
        "/clean": true,
        "/current": true,
        "/dirty": true,
        "/ahead": true,
        "/feature": true,
        "/disabled": false,
      }),
    ).pipe(Effect.provideService(GitVcsDriver.GitVcsDriver, git));

    assert.deepStrictEqual(pulled, ["/clean"]);

    pulled.length = 0;
    yield* ServerRuntimeStartup.autoPullProjects(
      [project("/inherited"), project("/opted-out"), project("/dirty")],
      { ...overrides({ "/opted-out": false }), defaultAutoPull: true },
    ).pipe(Effect.provideService(GitVcsDriver.GitVcsDriver, git));
    assert.deepStrictEqual(pulled, ["/inherited"]);
  }),
);

it.effect("enqueueCommand waits for readiness and then drains queued work", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const executionCount = yield* Ref.make(0);
      const commandGate = yield* ServerRuntimeStartup.makeCommandGate;

      const queuedCommandFiber = yield* commandGate
        .enqueueCommand(Ref.updateAndGet(executionCount, (count) => count + 1))
        .pipe(Effect.forkScoped);

      yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(executionCount), 0);

      yield* commandGate.signalCommandReady;

      const result = yield* Fiber.join(queuedCommandFiber);
      assert.equal(result, 1);
      assert.equal(yield* Ref.get(executionCount), 1);
    }),
  ),
);

it.effect("enqueueCommand fails queued work when readiness fails", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const commandGate = yield* ServerRuntimeStartup.makeCommandGate;
      const failure = yield* Deferred.make<void, never>();

      const queuedCommandFiber = yield* commandGate
        .enqueueCommand(Deferred.await(failure).pipe(Effect.as("should-not-run")))
        .pipe(Effect.forkScoped);

      yield* commandGate.failCommandReady(
        new ServerRuntimeStartup.ServerRuntimeStartupError({
          mode: "web",
          host: "127.0.0.1",
          port: 3773,
          cause: new Error("test startup failure"),
        }),
      );

      const error = yield* Effect.flip(Fiber.join(queuedCommandFiber));
      assert.equal(error.message, "Server runtime startup failed before command readiness.");
    }),
  ),
);

it.effect("resolveWelcomeBase derives cwd and project name from server config", () =>
  Effect.gen(function* () {
    const welcome = yield* ServerRuntimeStartup.resolveWelcomeBase.pipe(
      Effect.provideService(ServerConfig.ServerConfig, {
        cwd: "/tmp/startup-project",
      } as never),
    );

    assert.deepStrictEqual(welcome, {
      cwd: "/tmp/startup-project",
      projectName: "startup-project",
    });
  }),
);

it.effect("resolveAutoBootstrapWelcomeTargets returns existing project and thread ids", () => {
  const bootstrapProjectId = ProjectId.make("project-startup-bootstrap");
  const bootstrapThreadId = ThreadId.make("thread-startup-bootstrap");

  return Effect.gen(function* () {
    const dispatchCalls = yield* Ref.make<ReadonlyArray<string>>([]);
    const targets = yield* ServerRuntimeStartup.resolveAutoBootstrapWelcomeTargets.pipe(
      Effect.provide(ServerSettings.layerTest()),
      Effect.provideService(ServerConfig.ServerConfig, {
        cwd: "/tmp/startup-project",
        autoBootstrapProjectFromCwd: true,
      } as never),
      Effect.provideService(ProjectionSnapshotQuery.ProjectionSnapshotQuery, {
        getOperatingCounts: () => Effect.die("unused"),
        getUserInputActivity: () => Effect.die("unused"),
        listActivitiesByKind: () => Effect.succeed([]),
        getCommandReadModel: () => Effect.die("unused"),
        getSnapshot: () => Effect.die("unused"),
        getShellSnapshot: () => Effect.die("unused"),
        getDeletedWorktreeThreads: () => Effect.die("unused"),
        listThreadsWithPullRequests: () => Effect.die("unused"),
        getArchivedShellSnapshot: () => Effect.die("unused"),
        getSnapshotSequence: () => Effect.die("unused"),
        getCounts: () => Effect.die("unused"),
        getEventReplayStats: () => Effect.die("unused"),
        getActiveProjectByWorkspaceRoot: () =>
          Effect.succeedSome({
            id: bootstrapProjectId,
            title: "Startup Project",
            workspaceRoot: "/tmp/startup-project",
            defaultModelSelection: {
              instanceId: ProviderInstanceId.make("codex"),
              model: DEFAULT_MODEL,
            },
            scripts: [],
            createdAt: "2026-01-01T00:00:00.000Z",
            updatedAt: "2026-01-01T00:00:00.000Z",
            deletedAt: null,
          }),
        getProjectShells: () => Effect.die("unused"),
        getProjectShellById: () => Effect.die("unused"),
        getFirstActiveThreadIdByProjectId: () => Effect.succeedSome(bootstrapThreadId),
        getImportedAgentSessionSources: () => Effect.die("unused"),
        getThreadCheckpointContext: () => Effect.succeedNone,
        getFullThreadDiffContext: () => Effect.succeedNone,
        getThreadRuntimeContext: () => Effect.die("unused"),
        getTurnStartMessage: () => Effect.die("unused"),
        getThreadShellById: () => Effect.die("unused"),
        getThreadDetailById: () => Effect.die("unused"),
        getThreadDetailSnapshot: () => Effect.die("unused"),
        searchThreads: () => Effect.succeed({ matches: [] }),
      }),
      Effect.provideService(OrchestrationEngine.OrchestrationEngineService, {
        readEvents: () => Stream.empty,
        readThreadEvents: () => Stream.empty,
        getThreadReplayStats: () => Effect.die("unused thread replay stats"),
        dispatch: (command) =>
          Ref.update(dispatchCalls, (calls) => [...calls, command.type]).pipe(
            Effect.as({ sequence: 1 }),
          ),
        streamDomainEvents: Stream.empty,
        subscribeDomainEvents: Effect.succeed(Stream.empty),
        latestSequence: Effect.succeed(0),
        acquireWorktreeOwnership: () => Effect.die("unused"),
        releaseWorktreeOwnership: () => Effect.die("unused"),
        listWorktreeOwnershipLeases: Effect.succeed([]),
        getThreadOwnershipIncarnation: () => Effect.succeed(Option.none()),
      } satisfies OrchestrationEngine.OrchestrationEngineService["Service"]),
      Effect.provide(NodeServices.layer),
    );

    assert.deepStrictEqual(targets, {
      bootstrapProjectId,
      bootstrapThreadId,
      bootstrapProjectCreated: false,
      bootstrapThreadCreated: false,
    });
    assert.deepStrictEqual(yield* Ref.get(dispatchCalls), []);
  });
});

it.effect.each([
  {
    existing: false,
    machineModel: null,
    projectModel: null,
    machineMode: "full-access",
    projectMode: null,
  },
  {
    existing: false,
    machineModel: "claude-sonnet-4-6",
    projectModel: null,
    machineMode: "approval-required",
    projectMode: null,
  },
  {
    existing: true,
    machineModel: "claude-sonnet-4-6",
    projectModel: null,
    machineMode: "auto",
    projectMode: null,
  },
  {
    existing: true,
    machineModel: "claude-sonnet-4-6",
    projectModel: "gpt-5.4",
    machineMode: "full-access",
    projectMode: "auto-accept-edits",
  },
] as const)("auto-bootstrap model and permissions precedence: %j", (options) =>
  Effect.gen(function* () {
    const { existing, machineModel, projectModel, machineMode, projectMode } = options;
    const machineSelection = machineModel
      ? { instanceId: ProviderInstanceId.make("claude-code"), model: machineModel }
      : null;
    const projectSelection = projectModel
      ? { instanceId: ProviderInstanceId.make("codex"), model: projectModel }
      : null;
    const dispatchCalls = yield* Ref.make<
      ReadonlyArray<{
        readonly type: string;
        readonly defaultModelSelection?: unknown;
        readonly modelSelection?: unknown;
        readonly runtimeMode?: unknown;
      }>
    >([]);
    const targets = yield* ServerRuntimeStartup.resolveAutoBootstrapWelcomeTargets.pipe(
      Effect.provide(
        ServerSettings.layerTest({
          defaultModelSelection: machineSelection,
          defaultRuntimeMode: machineMode,
          projectSettingsOverrides:
            existing && projectSelection
              ? {
                  [ProjectId.make("existing-project")]: {
                    defaultModelSelection: projectSelection,
                    ...(projectMode ? { defaultRuntimeMode: projectMode } : {}),
                  },
                }
              : {},
        }),
      ),
      Effect.provideService(ServerConfig.ServerConfig, {
        cwd: "/tmp/startup-project",
        autoBootstrapProjectFromCwd: true,
      } as never),
      Effect.provideService(ProjectionSnapshotQuery.ProjectionSnapshotQuery, {
        getOperatingCounts: () => Effect.die("unused"),
        getUserInputActivity: () => Effect.die("unused"),
        listActivitiesByKind: () => Effect.succeed([]),
        getCommandReadModel: () => Effect.die("unused"),
        getSnapshot: () => Effect.die("unused"),
        getShellSnapshot: () => Effect.die("unused"),
        getDeletedWorktreeThreads: () => Effect.die("unused"),
        listThreadsWithPullRequests: () => Effect.die("unused"),
        getArchivedShellSnapshot: () => Effect.die("unused"),
        getSnapshotSequence: () => Effect.die("unused"),
        getCounts: () => Effect.die("unused"),
        getEventReplayStats: () => Effect.die("unused"),
        getActiveProjectByWorkspaceRoot: () =>
          Effect.succeed(
            existing
              ? Option.some({
                  id: ProjectId.make("existing-project"),
                  title: "Startup Project",
                  workspaceRoot: "/tmp/startup-project",
                  defaultModelSelection: null,
                  scripts: [],
                  createdAt: "2026-01-01T00:00:00.000Z",
                  updatedAt: "2026-01-01T00:00:00.000Z",
                  deletedAt: null,
                })
              : Option.none(),
          ),
        getProjectShells: () => Effect.die("unused"),
        getProjectShellById: () => Effect.die("unused"),
        getFirstActiveThreadIdByProjectId: () => Effect.succeedNone,
        getImportedAgentSessionSources: () => Effect.die("unused"),
        getThreadCheckpointContext: () => Effect.succeedNone,
        getFullThreadDiffContext: () => Effect.succeedNone,
        getThreadRuntimeContext: () => Effect.die("unused"),
        getTurnStartMessage: () => Effect.die("unused"),
        getThreadShellById: () => Effect.die("unused"),
        getThreadDetailById: () => Effect.die("unused"),
        getThreadDetailSnapshot: () => Effect.die("unused"),
        searchThreads: () => Effect.succeed({ matches: [] }),
      }),
      Effect.provideService(OrchestrationEngine.OrchestrationEngineService, {
        readEvents: () => Stream.empty,
        readThreadEvents: () => Stream.empty,
        getThreadReplayStats: () => Effect.die("unused thread replay stats"),
        dispatch: (command) =>
          Ref.update(dispatchCalls, (calls) => [...calls, command]).pipe(
            Effect.as({ sequence: 1 }),
          ),
        streamDomainEvents: Stream.empty,
        subscribeDomainEvents: Effect.succeed(Stream.empty),
        latestSequence: Effect.succeed(0),
        acquireWorktreeOwnership: () => Effect.die("unused"),
        releaseWorktreeOwnership: () => Effect.die("unused"),
        listWorktreeOwnershipLeases: Effect.succeed([]),
        getThreadOwnershipIncarnation: () => Effect.succeed(Option.none()),
      } satisfies OrchestrationEngine.OrchestrationEngineService["Service"]),
      Effect.provide(NodeServices.layer),
    );

    assert.equal(typeof targets.bootstrapProjectId, "string");
    assert.equal(typeof targets.bootstrapThreadId, "string");
    assert.equal(targets.bootstrapProjectCreated, !existing);
    assert.equal(targets.bootstrapThreadCreated, true);
    const commands = yield* Ref.get(dispatchCalls);
    assert.deepStrictEqual(
      commands.map((command) => command.type),
      existing ? ["thread.create"] : ["project.create", "thread.create"],
    );
    if (!existing) assert.equal("defaultModelSelection" in commands[0]!, false);
    assert.equal(commands.at(-1)?.runtimeMode, projectMode ?? machineMode);
    assert.deepStrictEqual(
      commands.at(-1)?.modelSelection,
      projectSelection ??
        machineSelection ?? {
          instanceId: ProviderInstanceId.make("codex"),
          model: DEFAULT_MODEL,
        },
    );
  }),
);

it.effect(
  "resolveAutoBootstrapWelcomeTargets preserves a project created before thread failure",
  () =>
    Effect.gen(function* () {
      const dispatchCalls = yield* Ref.make<ReadonlyArray<string>>([]);
      const targets = yield* ServerRuntimeStartup.resolveAutoBootstrapWelcomeTargets.pipe(
        Effect.provide(ServerSettings.layerTest()),
        Effect.provideService(ServerConfig.ServerConfig, {
          cwd: "/tmp/startup-project",
          autoBootstrapProjectFromCwd: true,
        } as never),
        Effect.provideService(ProjectionSnapshotQuery.ProjectionSnapshotQuery, {
          getOperatingCounts: () => Effect.die("unused"),
          getUserInputActivity: () => Effect.die("unused"),
          listActivitiesByKind: () => Effect.succeed([]),
          getCommandReadModel: () => Effect.die("unused"),
          getSnapshot: () => Effect.die("unused"),
          getShellSnapshot: () => Effect.die("unused"),
          getDeletedWorktreeThreads: () => Effect.die("unused"),
          listThreadsWithPullRequests: () => Effect.die("unused"),
          getArchivedShellSnapshot: () => Effect.die("unused"),
          getSnapshotSequence: () => Effect.die("unused"),
          getCounts: () => Effect.die("unused"),
          getEventReplayStats: () => Effect.die("unused"),
          getActiveProjectByWorkspaceRoot: () => Effect.succeedNone,
          getProjectShells: () => Effect.die("unused"),
          getProjectShellById: () => Effect.die("unused"),
          getFirstActiveThreadIdByProjectId: () => Effect.die("thread lookup failed"),
          getImportedAgentSessionSources: () => Effect.die("unused"),
          getThreadCheckpointContext: () => Effect.succeedNone,
          getFullThreadDiffContext: () => Effect.succeedNone,
          getThreadRuntimeContext: () => Effect.die("unused"),
          getTurnStartMessage: () => Effect.die("unused"),
          getThreadShellById: () => Effect.die("unused"),
          getThreadDetailById: () => Effect.die("unused"),
          getThreadDetailSnapshot: () => Effect.die("unused"),
          searchThreads: () => Effect.succeed({ matches: [] }),
        }),
        Effect.provideService(OrchestrationEngine.OrchestrationEngineService, {
          readEvents: () => Stream.empty,
          readThreadEvents: () => Stream.empty,
          getThreadReplayStats: () => Effect.die("unused thread replay stats"),
          dispatch: (command) =>
            Ref.update(dispatchCalls, (calls) => [...calls, command.type]).pipe(
              Effect.as({ sequence: 1 }),
            ),
          acquireWorktreeOwnership: () => Effect.die("unused ownership acquisition"),
          releaseWorktreeOwnership: () => Effect.die("unused ownership release"),
          getThreadOwnershipIncarnation: () => Effect.die("unused ownership incarnation"),
          listWorktreeOwnershipLeases: Effect.die("unused ownership list"),
          streamDomainEvents: Stream.empty,
          subscribeDomainEvents: Effect.succeed(Stream.empty),
          latestSequence: Effect.succeed(0),
        } satisfies OrchestrationEngine.OrchestrationEngineService["Service"]),
        Effect.provide(NodeServices.layer),
      );

      assert.equal(typeof targets.bootstrapProjectId, "string");
      assert.equal(targets.bootstrapProjectCreated, true);
      assert.equal(targets.bootstrapThreadId, undefined);
      assert.equal(targets.bootstrapThreadCreated, undefined);
      assert.deepStrictEqual(yield* Ref.get(dispatchCalls), ["project.create"]);
    }),
);

it.effect("resolveAutoBootstrapWelcomeTargets preserves typed UUID generation failures", () =>
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;
    const uuidError = PlatformError.systemError({
      _tag: "Unknown",
      module: "Crypto",
      method: "randomUUIDv4",
      description: "UUID generation unavailable",
    });
    const dispatchCalls = yield* Ref.make<ReadonlyArray<string>>([]);

    const error = yield* ServerRuntimeStartup.resolveAutoBootstrapWelcomeTargets.pipe(
      Effect.provide(ServerSettings.layerTest()),
      Effect.provideService(ServerConfig.ServerConfig, {
        cwd: "/tmp/startup-project",
        autoBootstrapProjectFromCwd: true,
      } as never),
      Effect.provideService(ProjectionSnapshotQuery.ProjectionSnapshotQuery, {
        getOperatingCounts: () => Effect.die("unused"),
        getUserInputActivity: () => Effect.die("unused"),
        listActivitiesByKind: () => Effect.succeed([]),
        getCommandReadModel: () => Effect.die("unused"),
        getSnapshot: () => Effect.die("unused"),
        getShellSnapshot: () => Effect.die("unused"),
        getDeletedWorktreeThreads: () => Effect.die("unused"),
        listThreadsWithPullRequests: () => Effect.die("unused"),
        getArchivedShellSnapshot: () => Effect.die("unused"),
        getSnapshotSequence: () => Effect.die("unused"),
        getCounts: () => Effect.die("unused"),
        getEventReplayStats: () => Effect.die("unused"),
        getActiveProjectByWorkspaceRoot: () => Effect.succeedNone,
        getProjectShells: () => Effect.die("unused"),
        getProjectShellById: () => Effect.die("unused"),
        getFirstActiveThreadIdByProjectId: () => Effect.succeedNone,
        getImportedAgentSessionSources: () => Effect.die("unused"),
        getThreadCheckpointContext: () => Effect.succeedNone,
        getFullThreadDiffContext: () => Effect.succeedNone,
        getThreadRuntimeContext: () => Effect.die("unused"),
        getTurnStartMessage: () => Effect.die("unused"),
        getThreadShellById: () => Effect.die("unused"),
        getThreadDetailById: () => Effect.die("unused"),
        getThreadDetailSnapshot: () => Effect.die("unused"),
        searchThreads: () => Effect.succeed({ matches: [] }),
      }),
      Effect.provideService(OrchestrationEngine.OrchestrationEngineService, {
        readEvents: () => Stream.empty,
        readThreadEvents: () => Stream.empty,
        getThreadReplayStats: () => Effect.die("unused thread replay stats"),
        dispatch: (command) =>
          Ref.update(dispatchCalls, (calls) => [...calls, command.type]).pipe(
            Effect.as({ sequence: 1 }),
          ),
        streamDomainEvents: Stream.empty,
        subscribeDomainEvents: Effect.succeed(Stream.empty),
        latestSequence: Effect.succeed(0),
        acquireWorktreeOwnership: () => Effect.die("unused"),
        releaseWorktreeOwnership: () => Effect.die("unused"),
        listWorktreeOwnershipLeases: Effect.succeed([]),
        getThreadOwnershipIncarnation: () => Effect.succeed(Option.none()),
      } satisfies OrchestrationEngine.OrchestrationEngineService["Service"]),
      Effect.provideService(Crypto.Crypto, {
        ...crypto,
        randomUUIDv4: Effect.fail(uuidError),
      }),
      Effect.flip,
    );

    assert.strictEqual(error, uuidError);
    assert.deepStrictEqual(yield* Ref.get(dispatchCalls), []);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("completeAutoBootstrapWelcome settles failures without bootstrap targets", () =>
  Effect.gen(function* () {
    const completion = yield* ServerRuntimeStartup.completeAutoBootstrapWelcome(
      Effect.fail("bootstrap failed"),
    );

    assert.deepStrictEqual(completion, { bootstrapStatus: "complete" });
  }),
);

it.effect("completeAutoBootstrapWelcome settles unexpected defects", () =>
  Effect.gen(function* () {
    const completion = yield* ServerRuntimeStartup.completeAutoBootstrapWelcome(
      Effect.die("bootstrap defect"),
    );

    assert.deepStrictEqual(completion, { bootstrapStatus: "complete" });
  }),
);

it.effect("completeAutoBootstrapWelcome settles an empty bootstrap result", () =>
  Effect.gen(function* () {
    const completion = yield* ServerRuntimeStartup.completeAutoBootstrapWelcome(Effect.succeed({}));

    assert.deepStrictEqual(completion, { bootstrapStatus: "complete" });
  }),
);

it.effect.each([false, true])(
  "desktop preparation marks only effectively opted-in projects when environment continuation is %s",
  (environmentOptIn) =>
    Effect.gen(function* () {
      const ids = ["inherited", "enabled", "disabled"];
      const bindings = new Map(
        ids.map((id) => [
          ThreadId.make(id),
          {
            threadId: ThreadId.make(id),
            provider: "codex" as const,
            resumeCursor: { threadId: id },
            runtimePayload: { retained: id },
          } as ProviderSessionDirectory.ProviderRuntimeBinding,
        ]),
      );
      const writes: ProviderSessionDirectory.ProviderRuntimeBinding[] = [];
      const marked = yield* ServerRuntimeStartup.markOptedInProviderSessionsForContinuation.pipe(
        Effect.provide(
          Layer.mergeAll(
            ServerSettings.layerTest({
              continueThreadsAfterServerUpdate: environmentOptIn,
              projectSettingsOverrides: {
                [ProjectId.make("enabled")]: { continueThreadsAfterServerUpdate: true },
                [ProjectId.make("disabled")]: { continueThreadsAfterServerUpdate: false },
              },
            }),
            Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
              getCommandReadModel: () =>
                Effect.succeed({
                  threads: ids.map((id) => ({
                    id: ThreadId.make(id),
                    projectId: ProjectId.make(id),
                    archivedAt: null,
                    deletedAt: null,
                    session: { status: "running", activeTurnId: TurnId.make(`turn-${id}`) },
                  })),
                } as never),
            }),
            Layer.mock(ProviderSessionDirectory.ProviderSessionDirectory)({
              getBinding: (id) => Effect.succeed(Option.fromUndefinedOr(bindings.get(id))),
              upsert: (binding) =>
                Effect.sync(() => {
                  writes.push(binding);
                }),
            }),
          ),
        ),
      );
      const expected = environmentOptIn ? ["inherited", "enabled"] : ["enabled"];
      assert.deepStrictEqual(
        marked,
        expected.map((id) => ThreadId.make(id)),
      );
      assert.deepStrictEqual(
        writes.map((binding) => binding.threadId),
        marked,
      );
      for (const binding of writes) {
        assert.deepStrictEqual(binding.runtimePayload, {
          retained: binding.threadId,
          continueAfterServerUpdate: `turn-${binding.threadId}`,
          continueAfterServerUpdatePrepared: null,
        });
      }
    }),
);

it.effect("desktop preparation with default-off continuation writes no resume markers", () =>
  Effect.gen(function* () {
    const result = yield* ServerRuntimeStartup.markOptedInProviderSessionsForContinuation.pipe(
      Effect.provide(
        Layer.mergeAll(
          ServerSettings.layerTest(),
          Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
            getCommandReadModel: () =>
              Effect.succeed({
                threads: [
                  {
                    id: ThreadId.make("running"),
                    projectId: ProjectId.make("project"),
                    archivedAt: null,
                    deletedAt: null,
                    session: { status: "running", activeTurnId: TurnId.make("turn") },
                  },
                ],
              } as never),
          }),
          Layer.mock(ProviderSessionDirectory.ProviderSessionDirectory)({
            getBinding: () => Effect.die("opted-out thread must not be marked"),
            upsert: () => Effect.die("opted-out thread must not be written"),
          }),
        ),
      ),
    );
    assert.deepStrictEqual(result, []);
  }),
);

it.effect(
  "desktop preparation refuses unreadable continuation preferences before touching sessions",
  () =>
    Effect.gen(function* () {
      const result = yield* ServerRuntimeStartup.markOptedInProviderSessionsForContinuation.pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.mock(ServerSettings.ServerSettingsService)({
              getSettings: Effect.fail(new Error("settings unavailable") as never),
            }),
            Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
              getCommandReadModel: () => Effect.die("must read preferences first"),
            }),
            Layer.mock(ProviderSessionDirectory.ProviderSessionDirectory)({
              getBinding: () => Effect.die("must not mark after settings failure"),
              upsert: () => Effect.die("must not write after settings failure"),
            }),
          ),
        ),
        Effect.exit,
      );
      assert.equal(result._tag, "Failure");
    }),
);
