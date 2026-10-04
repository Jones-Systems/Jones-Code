import { assert, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  CommandId,
  DEFAULT_MODEL,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type Project,
  type ModelSelection,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Crypto from "effect/Crypto";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import * as ProjectService from "./project/ProjectService.ts";
import * as ThreadManagement from "./orchestration-v2/ThreadManagementService.ts";
import * as ThreadLaunch from "./orchestration-v2/ThreadLaunchService.ts";
import * as ServerSettings from "./serverSettings.ts";

import * as GitVcsDriver from "./vcs/GitVcsDriver.ts";

import * as ServerConfig from "./config.ts";
import * as ServerRuntimeStartup from "./serverRuntimeStartup.ts";

it("uses the canonical Codex model for auto-bootstrap", () => {
  assert.deepEqual(ServerRuntimeStartup.getAutoBootstrapThreadModelSelection(), {
    instanceId: ProviderInstanceId.make("codex"),
    model: DEFAULT_MODEL,
  });
});

it.effect("starts without scanning or rebuilding projection history", () =>
  Effect.gen(function* () {
    const calls = yield* Ref.make<ReadonlyArray<string>>([]);
    const record = (label: string) => Ref.update(calls, (current) => [...current, label]);

    const result = yield* ServerRuntimeStartup.runOrderedV2StartupPhases({
      importLegacyShells: record("import"),
      recover: record("recover").pipe(Effect.as({ closedRequests: 2 })),
      startEffectWorker: record("worker"),
      autoBootstrap: record("bootstrap").pipe(Effect.as({ projectId: "project-1" })),
    });

    assert.deepEqual(yield* Ref.get(calls), ["import", "recover", "worker", "bootstrap"]);
    assert.deepEqual(result, {
      recovery: { closedRequests: 2 },
      bootstrap: { projectId: "project-1" },
    });
  }),
);

it.effect("interrupts the effect worker when awareness relay startup fails", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const workerInterrupted = yield* Ref.make(false);
      const workerFiberRef = yield* Ref.make<Fiber.Fiber<void, never> | null>(null);

      const exit = yield* ServerRuntimeStartup.startEffectWorkerWithRelay({
        runWorker: Effect.never.pipe(Effect.ensuring(Ref.set(workerInterrupted, true))),
        startRelay: Effect.yieldNow.pipe(
          Effect.andThen(Effect.die("awareness relay startup failed")),
        ),
        workerFiberRef,
      }).pipe(Effect.exit);

      assert.isTrue(Exit.isFailure(exit));
      assert.isTrue(yield* Ref.get(workerInterrupted));
      assert.isNull(yield* Ref.get(workerFiberRef));
    }),
  ),
);

it.effect("queues commands until startup signals readiness", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const gate = yield* ServerRuntimeStartup.makeCommandGate;
      const count = yield* Ref.make(0);
      const queued = yield* gate
        .enqueueCommand(Ref.updateAndGet(count, (value) => value + 1))
        .pipe(Effect.forkScoped);

      yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(count), 0);
      yield* gate.signalCommandReady;
      assert.equal(yield* Fiber.join(queued), 1);
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

const bootstrapProject = {
  id: ProjectId.make("startup-project"),
  title: "Startup project",
  workspaceRoot: "/fixture/startup-project",
  defaultModelSelection: null,
  scripts: [],
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-01T00:00:00.000Z",
  deletedAt: null,
} satisfies Project;
const bootstrapThreadId = ThreadId.make("startup-thread");
const runBootstrap = (input: {
  readonly bootstrap?: ProjectService.ProjectService["Service"]["bootstrap"];
  readonly threads?: ReadonlyArray<OrchestrationV2ThreadShell>;
  readonly launch?: ThreadLaunch.ThreadLaunchService["Service"]["launch"];
  readonly settings?: Partial<ServerSettings.ServerSettingsService["Service"]>;
}) =>
  ServerRuntimeStartup.resolveAutoBootstrapWelcomeTargets.pipe(
    Effect.provideService(ServerConfig.ServerConfig, {
      cwd: bootstrapProject.workspaceRoot,
      autoBootstrapProjectFromCwd: true,
    } as ServerConfig.ServerConfig["Service"]),
    Effect.provide(
      Layer.mock(ProjectService.ProjectService)({
        bootstrap:
          input.bootstrap ?? (() => Effect.succeed({ project: bootstrapProject, created: false })),
      }),
    ),
    Effect.provide(
      Layer.mock(ThreadManagement.ThreadManagementService)({
        getShellSnapshot: () => Effect.succeed({ threads: input.threads ?? [] } as never),
      }),
    ),
    Effect.provide(
      Layer.mock(ThreadLaunch.ThreadLaunchService)({
        launch:
          input.launch ??
          (() =>
            Effect.succeed({
              threadId: bootstrapThreadId,
              projection: {} as never,
              resumed: false,
            })),
      }),
    ),
    Effect.provide(
      input.settings === undefined
        ? ServerSettings.layerTest()
        : Layer.mock(ServerSettings.ServerSettingsService)(input.settings),
    ),
  );

it.layer(NodeServices.layer)("V2 bootstrap targets", (it) => {
  it.effect("resolveAutoBootstrapWelcomeTargets returns existing project and thread ids", () =>
    Effect.gen(function* () {
      let launches = 0;
      const shell = (id: string, projectId: ProjectId, relationshipToParent: string | null) =>
        ({
          id: ThreadId.make(id),
          projectId,
          lineage: { relationshipToParent },
        }) as OrchestrationV2ThreadShell;
      const result = yield* runBootstrap({
        threads: [
          shell("other-project", ProjectId.make("other"), null),
          shell("delegated", bootstrapProject.id, "subagent"),
          shell(bootstrapThreadId, bootstrapProject.id, null),
        ],
        launch: () =>
          Effect.sync(() => {
            launches += 1;
            return { threadId: bootstrapThreadId, projection: {} as never, resumed: false };
          }),
      });
      assert.deepEqual(result, { bootstrapProjectId: bootstrapProject.id, bootstrapThreadId });
      assert.equal(launches, 0);
    }),
  );

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
      const machineSelection: ModelSelection | null =
        options.machineModel === null
          ? null
          : {
              instanceId: ProviderInstanceId.make("claude-code"),
              model: options.machineModel,
            };
      const projectSelection: ModelSelection | null =
        options.projectModel === null
          ? null
          : {
              instanceId: ProviderInstanceId.make("codex"),
              model: options.projectModel,
            };
      const project = { ...bootstrapProject, defaultModelSelection: projectSelection };
      const settings = {
        ...DEFAULT_SERVER_SETTINGS,
        defaultModelSelection: machineSelection,
        defaultRuntimeMode: options.machineMode,
        projectSettingsOverrides: {
          [project.id]: {
            ...(options.projectMode === null ? {} : { defaultRuntimeMode: options.projectMode }),
          },
        },
      };
      let launch: ThreadLaunch.ThreadLaunchInput | undefined;
      const result = yield* runBootstrap({
        bootstrap: () => Effect.succeed({ project, created: !options.existing }),
        settings: { getSettings: Effect.succeed(settings) },
        launch: (input) =>
          Effect.sync(() => {
            launch = input;
            return { threadId: bootstrapThreadId, projection: {} as never, resumed: false };
          }),
      });
      assert.deepEqual(result, { bootstrapProjectId: project.id, bootstrapThreadId });
      assert.deepEqual(
        launch?.modelSelection,
        projectSelection ??
          machineSelection ?? {
            instanceId: ProviderInstanceId.make("codex"),
            model: DEFAULT_MODEL,
          },
      );
      assert.equal(launch?.runtimeMode, options.projectMode ?? options.machineMode);
      assert.equal(launch?.workspaceStrategy.type, "root");
      assert.equal(launch?.createdBy, "system");
      assert.equal(launch?.creationSource, "server");
    }),
  );

  it.effect(
    "resolveAutoBootstrapWelcomeTargets preserves a project created before thread failure",
    () =>
      Effect.gen(function* () {
        let project: Project | undefined;
        const failure = new ThreadLaunch.ThreadLaunchError({
          operation: "resolve-project",
          commandId: CommandId.make("launch-failed"),
          projectId: bootstrapProject.id,
          cause: "Thread launch unavailable",
        });
        const error = yield* runBootstrap({
          bootstrap: () =>
            Effect.sync(() => {
              project = bootstrapProject;
              return { project, created: true };
            }),
          launch: () => Effect.fail(failure),
        }).pipe(Effect.flip);
        assert.strictEqual(error, failure);
        assert.strictEqual(project, bootstrapProject);
      }),
  );

  it.effect("resolveAutoBootstrapWelcomeTargets preserves typed UUID generation failures", () =>
    Effect.gen(function* () {
      const crypto = yield* Crypto.Crypto;
      const failure = PlatformError.systemError({
        _tag: "Unknown",
        module: "Crypto",
        method: "randomUUIDv4",
        description: "UUID generation unavailable",
      });
      let created = 0;
      let launched = 0;
      const error = yield* runBootstrap({
        bootstrap: () =>
          Effect.sync(() => {
            created += 1;
            return { project: bootstrapProject, created: true };
          }),
        launch: () =>
          Effect.sync(() => {
            launched += 1;
            return { threadId: bootstrapThreadId, projection: {} as never, resumed: false };
          }),
      }).pipe(
        Effect.provideService(Crypto.Crypto, { ...crypto, randomUUIDv4: Effect.fail(failure) }),
        Effect.flip,
      );
      assert.strictEqual(error, failure);
      assert.equal(created, 0);
      assert.equal(launched, 0);
    }),
  );
});
