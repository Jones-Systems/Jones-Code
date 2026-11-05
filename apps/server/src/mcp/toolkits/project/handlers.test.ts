import { makeClientLaunchHarness } from "../../../jones/mcp/clientLaunch/ClientLaunch.testkit.ts";
import { ClaudeProviderCapabilitiesV2 } from "../../../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import { CodexProviderCapabilitiesV2 } from "../../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Deferred from "effect/Deferred";
import * as SqlClient from "effect/sql/SqlClient";

import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as CommandReceipts from "../../../orchestration-v2/CommandReceiptStore.ts";
import * as EventSink from "../../../orchestration-v2/EventSink.ts";
import * as ProviderRegistry from "../../../provider/ProviderRegistry.ts";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  type ServerProvider,
  ThreadId,
  type OrchestrationV2ThreadShell,
  type ModelSelection,
  type RuntimeMode,
} from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import * as DocumentText from "../../../provider/DocumentText.ts";
import { OrchestratorRuntimeModeAboveCeilingError } from "../../../orchestration-v2/Orchestrator.ts";
import * as ThreadLaunch from "../../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as ServerConfig from "../../../config.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as Project from "../../../project/ProjectService.ts";
import * as ProjectCloneTracker from "../../../project/ProjectCloneTracker.ts";
import * as ManagedProjectFolders from "../../../project/ManagedProjectFolders.ts";
import * as PeerForwarding from "../../../peer/PeerForwarding.ts";
import * as GitVcsDriver from "../../../vcs/GitVcsDriver.ts";
import * as VcsProcess from "../../../vcs/VcsProcess.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import * as ProjectHandlers from "./handlers.ts";
import { ProjectToolkit } from "./tools.ts";

it.effect("attributes a launched thread's first message to the calling thread", () =>
  Effect.gen(function* () {
    const sourceThreadId = ThreadId.make("source-thread");
    const projectId = ProjectId.make("project");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const modelSelection = { instanceId: providerInstanceId, model: "gpt-5" };
    const caller = {
      id: sourceThreadId,
      projectId,
      providerInstanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      activeRunId: "active-run",
      archivedAt: null,
      deletedAt: null,
    } as OrchestrationV2ThreadShell;
    let launchedSender: ThreadId | undefined;
    const layerDependencies = Layer.mergeAll(
      Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
      NodeSqliteClient.layer({ filename: ":memory:" }),
      Layer.mock(CommandReceipts.CommandReceiptStoreV2)({}),
      Layer.mock(EventSink.EventSinkV2)({}),
      ServerSettings.layerTest(),
      NodeCrypto.layer,
      Layer.succeed(McpInvocationContext.McpInvocationContext, {
        environmentId: EnvironmentId.make("environment"),
        requestNamespace: "session",
        thread: {
          threadId: sourceThreadId,
          providerSessionId: "session",
          providerInstanceId,
        },
        client: undefined,
        issuedAt: 0,
        capabilities: new Set(["orchestration" as const]),
      }),
      Layer.mock(ThreadManagement.ThreadManagementService)({
        getThreadShell: () => Effect.succeed(caller),
      }),
      Layer.mock(ThreadLaunch.ThreadLaunchService)({
        launch: (input) => {
          launchedSender = input.initialMessage?.senderThreadId;
          return Effect.succeed({
            threadId: input.threadId,
            projection: {
              thread: { id: input.threadId, title: input.title, projectId, modelSelection },
              runs: [],
            },
            resumed: false,
          } as unknown as ThreadLaunch.ThreadLaunchResult);
        },
      }),
      Layer.mock(Project.ProjectService)({}),
      Layer.mock(PeerForwarding.PeerForwarding)({}),
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({ namedProjectsRoot: "/projects" }),
      Layer.mock(GitVcsDriver.GitVcsDriver)({}),
      NodeServices.layer,
      DocumentText.layer.pipe(Layer.provide(NodeServices.layer)),
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-source-link-" }).pipe(
        Layer.provide(NodeServices.layer),
      ),
    );
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
          Layer.provide(layerDependencies),
        ),
      ),
    );
    const result = yield* toolkit
      .handle("t3_thread_launch", { title: "Audit", message: "Review the change" })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(layerDependencies));
    expect(result.at(-1)?.result).toMatchObject({ projectId, modelSelection });
    expect(launchedSender).toBe(sourceThreadId);
  }),
);

it.effect("launches a scratch thread into the Scratch project", () =>
  Effect.gen(function* () {
    const sourceThreadId = ThreadId.make("source-thread");
    const scratchProjectId = ProjectId.make("project:scratch");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const modelSelection = { instanceId: providerInstanceId, model: "gpt-5" };
    const caller = {
      id: sourceThreadId,
      projectId: ProjectId.make("project:caller"),
      providerInstanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      activeRunId: "active-run",
      archivedAt: null,
      deletedAt: null,
    } as OrchestrationV2ThreadShell;
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const layerDependencies = Layer.mergeAll(
      Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
      NodeSqliteClient.layer({ filename: ":memory:" }),
      Layer.mock(CommandReceipts.CommandReceiptStoreV2)({}),
      Layer.mock(EventSink.EventSinkV2)({}),
      ServerSettings.layerTest(),
      NodeCrypto.layer,
      Layer.succeed(McpInvocationContext.McpInvocationContext, {
        environmentId: EnvironmentId.make("environment"),
        requestNamespace: "session",
        thread: {
          threadId: sourceThreadId,
          providerSessionId: "session",
          providerInstanceId,
        },
        client: undefined,
        issuedAt: 0,
        capabilities: new Set(["orchestration" as const]),
      }),
      Layer.mock(ThreadManagement.ThreadManagementService)({
        getThreadShell: () => Effect.succeed(caller),
      }),
      Layer.mock(ThreadLaunch.ThreadLaunchService)({
        launch: (input) => {
          launched.push(input);
          return Effect.succeed({
            threadId: input.threadId,
            projection: {
              thread: {
                id: input.threadId,
                title: input.title,
                projectId: input.projectId,
                modelSelection,
              },
              runs: [],
            },
            resumed: false,
          } as unknown as ThreadLaunch.ThreadLaunchResult);
        },
      }),
      Layer.mock(Project.ProjectService)({}),
      Layer.mock(PeerForwarding.PeerForwarding)({}),
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/projects",
        ensureScratchProject: Effect.succeed({ projectId: scratchProjectId }),
      }),
      Layer.mock(GitVcsDriver.GitVcsDriver)({}),
      NodeServices.layer,
      DocumentText.layer.pipe(Layer.provide(NodeServices.layer)),
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-scratch-launch-" }).pipe(
        Layer.provide(NodeServices.layer),
      ),
    );
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
          Layer.provide(layerDependencies),
        ),
      ),
    );
    const handle = (params: Parameters<typeof toolkit.handle<"t3_thread_launch">>[1]) =>
      toolkit
        .handle("t3_thread_launch", params)
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(layerDependencies));

    const result = yield* handle({ title: "Notes", scratch: true, message: "Draft a list" });
    expect(result.at(-1)?.result).toMatchObject({ projectId: scratchProjectId });
    expect(launched.map((input) => [input.projectId, input.workspaceStrategy])).toEqual([
      [scratchProjectId, { type: "root" }],
    ]);

    const rejected = yield* handle({
      title: "Notes",
      scratch: true,
      projectId: caller.projectId,
    });
    expect(rejected.at(-1)?.result).toMatchObject({ code: "invalid_request" });
    expect(launched).toHaveLength(1);
  }),
);

it.effect("starts a project from just a title when workspaceRoot is omitted", () =>
  Effect.gen(function* () {
    const sourceThreadId = ThreadId.make("source-thread");
    const createdProjectId = ProjectId.make("project:named");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const caller = {
      id: sourceThreadId,
      projectId: ProjectId.make("project:caller"),
      providerInstanceId,
      modelSelection: { instanceId: providerInstanceId, model: "gpt-5" },
      runtimeMode: "full-access",
      interactionMode: "default",
      activeRunId: "active-run",
      archivedAt: null,
      deletedAt: null,
    } as OrchestrationV2ThreadShell;
    const named: Array<string> = [];
    const registered: Array<string> = [];
    const createdProject = {
      id: createdProjectId,
      title: "Pinball Stats",
      workspaceRoot: "/projects/pinball-stats",
      defaultModelSelection: null,
      scripts: [],
      createdAt: "2026-10-01T00:00:00.000Z",
      updatedAt: "2026-10-01T00:00:00.000Z",
      deletedAt: null,
    };
    const layerDependencies = Layer.mergeAll(
      Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
      NodeSqliteClient.layer({ filename: ":memory:" }),
      Layer.mock(CommandReceipts.CommandReceiptStoreV2)({}),
      Layer.mock(EventSink.EventSinkV2)({}),
      ServerSettings.layerTest(),
      NodeCrypto.layer,
      Layer.succeed(McpInvocationContext.McpInvocationContext, {
        environmentId: EnvironmentId.make("environment"),
        requestNamespace: "session",
        thread: {
          threadId: sourceThreadId,
          providerSessionId: "session",
          providerInstanceId,
        },
        client: undefined,
        issuedAt: 0,
        capabilities: new Set(["orchestration" as const]),
      }),
      Layer.mock(ThreadManagement.ThreadManagementService)({
        getThreadShell: () => Effect.succeed(caller),
      }),
      Layer.mock(ThreadLaunch.ThreadLaunchService)({}),
      Layer.mock(Project.ProjectService)({
        create: (input) =>
          Effect.sync(() => {
            registered.push(input.workspaceRoot);
            return { ...createdProject, id: input.projectId, workspaceRoot: input.workspaceRoot };
          }),
        getById: (projectId) =>
          Effect.succeed(
            projectId === createdProjectId ? Option.some(createdProject) : Option.none(),
          ),
      }),
      Layer.mock(PeerForwarding.PeerForwarding)({}),
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/projects",
        createNamedProject: (input) =>
          Effect.sync(() => {
            named.push(input.name);
            return {
              projectId: createdProjectId,
              workspaceRoot: createdProject.workspaceRoot,
              commitError: "Git has no name or email on this machine.",
            };
          }),
      }),
      Layer.mock(GitVcsDriver.GitVcsDriver)({}),
      NodeServices.layer,
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-named-project-" }).pipe(
        Layer.provide(NodeServices.layer),
      ),
    );
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
          Layer.provide(layerDependencies),
        ),
      ),
    );
    const handle = (params: Parameters<typeof toolkit.handle<"t3_project_create">>[1]) =>
      toolkit
        .handle("t3_project_create", params)
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(layerDependencies));

    const result = yield* handle({ title: "Pinball Stats" });
    expect(result.at(-1)?.result).toMatchObject({
      id: createdProjectId,
      workspaceRoot: "/projects/pinball-stats",
      commitError: "Git has no name or email on this machine.",
    });
    expect(named).toEqual(["Pinball Stats"]);

    yield* handle({ title: "Existing", workspaceRoot: "/work/existing" });
    expect(registered).toEqual(["/work/existing"]);

    for (const extra of [
      { scripts: [] },
      { defaultModelSelection: { instanceId: providerInstanceId, model: "gpt-5" } },
    ]) {
      const rejected = yield* handle({ title: "Configured", ...extra });
      expect(rejected.at(-1)?.result).toMatchObject({ code: "invalid_request" });
    }
    expect(named).toEqual(["Pinball Stats"]);
  }),
);

const clientLaunchHarness = Effect.fn("test.clientLaunchHarness")(function* (input: {
  readonly runtimeModeCeiling: RuntimeMode;
  readonly defaultRuntimeMode?: RuntimeMode;
  readonly fallbackModel?: ModelSelection;
  readonly providers?: readonly ServerProvider[];
  /** Where Settings saves a project's model; the legacy project field stays null. */
  readonly overrideModel?: ModelSelection;
  readonly launched: Array<ThreadLaunch.ThreadLaunchInput>;
  readonly linked?: boolean;
  readonly narrowedTo?: McpInvocationContext.ClientModes;
  readonly workspaceRoot?: string;
  readonly checkWorktreeBase?: ThreadLaunch.ThreadLaunchService["Service"]["checkWorktreeBase"];
  readonly webOrigin?: string;
  /** The orchestrator refuses the new thread's mode, as under a managed ceiling. */
  readonly refusedByCeiling?: true;
}) {
  const projectId = ProjectId.make("project:client-target");
  const modelSelection = {
    instanceId: ProviderInstanceId.make("claudeAgent"),
    model: "claude-opus",
  };
  const projectDefaults: { model: ModelSelection | null } = {
    model: input.overrideModel === undefined ? modelSelection : null,
  };
  const projectRecord = {
    id: projectId,
    title: "Client target",
    workspaceRoot: input.workspaceRoot ?? "/projects/client-target",
    repositoryIdentity: null,
    faviconPath: null,
    defaultModelSelection: modelSelection,
    defaultThreadEnvMode: null,
    scripts: [],
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    deletedAt: null,
  };
  const readProject: Project.ProjectService["Service"]["getById"] = (id) =>
    Effect.succeed(
      id === projectId
        ? Option.some({ ...projectRecord, defaultModelSelection: projectDefaults.model })
        : Option.none(),
    );
  const settings = {
    fallbackModelSelection: input.fallbackModel ?? null,
    ...(input.defaultRuntimeMode === undefined
      ? {}
      : { defaultRuntimeMode: input.defaultRuntimeMode }),
    ...(input.overrideModel === undefined
      ? {}
      : {
          projectSettingsFolded: true,
          projectSettingsOverrides: { [projectId]: { defaultModelSelection: input.overrideModel } },
        }),
  };
  const setupEntered = yield* Deferred.make<void>();
  const config = yield* ServerConfig.ServerConfig.pipe(
    Effect.provide(
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-client-launch-" }).pipe(
        Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
      ),
    ),
  );
  const configLayer = ServerConfig.layer(config);
  const gitDriver = yield* GitVcsDriver.GitVcsDriver.pipe(
    Effect.provide(
      GitVcsDriver.layer.pipe(
        Layer.provide(VcsProcess.layer),
        Layer.provide(configLayer),
        Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
      ),
    ),
  );
  const h = makeClientLaunchHarness({
    projectRecord,
    readProject,
    gitDriver,
    serverConfigLayer: configLayer,
    serverSettings: settings,
    ...(input.providers === undefined ? {} : { providers: input.providers }),
    adapters: ["claudeAgent", "codex"].map((driver) => ({
      instanceId: ProviderInstanceId.make(driver),
      driver: ProviderDriverKind.make(driver),
      getCapabilities: () =>
        Effect.succeed(
          driver === "claudeAgent" ? ClaudeProviderCapabilitiesV2 : CodexProviderCapabilitiesV2,
        ),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
      openSession: () => Effect.die("Provider execution is disabled in project launch tests"),
    })),
    runSetup: () =>
      Deferred.succeed(setupEntered, undefined).pipe(Effect.as({ status: "no-script" as const })),
    managedFolders: Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
      namedProjectsRoot: "/projects",
      isScratchProject: () => Effect.succeed(false),
      folderForThread: () => Effect.succeedNone,
    }),
  });
  const recordingLaunch = Layer.effect(
    ThreadLaunch.ThreadLaunchService,
    Effect.map(ThreadLaunch.ThreadLaunchService, (launches) => ({
      ...launches,
      checkWorktreeBase: input.checkWorktreeBase ?? (() => Effect.void),
      launch: (launch: ThreadLaunch.ThreadLaunchInput) =>
        Effect.gen(function* () {
          input.launched.push(launch);
          if (input.refusedByCeiling)
            return yield* new ThreadLaunch.ThreadLaunchError({
              operation: "create-thread",
              commandId: launch.commandId,
              projectId,
              cause: new OrchestratorRuntimeModeAboveCeilingError({
                commandId: launch.commandId,
                threadId: launch.threadId!,
                refused: "mode",
                runtimeMode: launch.runtimeMode,
                maxRuntimeMode: "approval-required",
                organization: "Example Corp",
              }),
            });
          return yield* launches.launch(launch);
        }),
    })),
  ).pipe(Layer.provideMerge(h.layer));
  const layerDependencies = Layer.mergeAll(
    recordingLaunch,
    Layer.mock(ProviderRegistry.ProviderRegistry)({
      getProviders: Effect.succeed(input.providers ?? []),
    }),
    Layer.succeed(McpInvocationContext.McpInvocationContext, {
      environmentId: EnvironmentId.make("environment"),
      requestNamespace: "client:session-1",
      thread: undefined,
      client: {
        sessionId: "session-1",
        label: "Claude Code",
        access: input.runtimeModeCeiling,
        webOrigin: input.webOrigin,
        ...(input.linked === undefined ? {} : { linked: input.linked }),
        ...(input.narrowedTo === undefined ? {} : { narrowedTo: input.narrowedTo }),
      },
      issuedAt: 0,
      capabilities: new Set(["orchestration" as const]),
    }),
    Layer.mock(PeerForwarding.PeerForwarding)({}),
    DocumentText.layer.pipe(Layer.provideMerge(NodeServices.layer)),
  ).pipe(
    Layer.provideMerge(GitVcsDriver.layer),
    Layer.provideMerge(VcsProcess.layer),
    Layer.provideMerge(configLayer),
    Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
  );
  // One actual migrated database and graph live through all invocations and
  // assertions of this harness. Invocation authorization still executes afresh.
  const dependencies = Layer.succeedContext(yield* Layer.build(layerDependencies));
  const receipt = (commandId: import("@t3tools/contracts").CommandId) =>
    CommandReceipts.CommandReceiptStoreV2.pipe(
      Effect.flatMap((store) => store.getByCommandId(commandId)),
      Effect.provide(dependencies),
    );
  const projection = (threadId: ThreadId) =>
    ThreadManagement.ThreadManagementService.pipe(
      Effect.flatMap((threads) => threads.getThreadProjection(threadId)),
      Effect.provide(dependencies),
    );
  const creationReceipts = (commandId: import("@t3tools/contracts").CommandId) =>
    SqlClient.SqlClient.pipe(
      Effect.flatMap(
        (sql) =>
          sql`SELECT command_id, aggregate_id, command_type, status FROM orchestration_command_receipts WHERE command_id = ${commandId}`,
      ),
      Effect.provide(dependencies),
    );
  return {
    projectId,
    modelSelection,
    projectDefaults,
    dependencies,
    receipt,
    projection,
    creationReceipts,
    createWorktree: h.createWorktree,
    runSetup: h.runSetup,
    waitForSetup: Deferred.await(setupEntered),
  };
});

it.effect(
  "a client launches with the project's default model and mode, capped at its ceiling",
  () =>
    Effect.gen(function* () {
      const launchWith = Effect.fn("test.launchWith")(function* (
        runtimeModeCeiling: RuntimeMode,
        defaultRuntimeMode: RuntimeMode,
        params: {
          readonly title: string;
          readonly message?: string;
          readonly runtimeMode?: RuntimeMode;
          readonly targeted?: false;
          readonly overrideModel?: ModelSelection;
          readonly webOrigin?: string;
          readonly refusedByCeiling?: true;
        },
      ) {
        const { targeted, overrideModel, webOrigin, refusedByCeiling, ...input } = params;
        const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
        const { projectId, modelSelection, dependencies } = yield* clientLaunchHarness({
          runtimeModeCeiling,
          defaultRuntimeMode,
          launched,
          ...(overrideModel === undefined ? {} : { overrideModel }),
          ...(webOrigin === undefined ? {} : { webOrigin }),
          ...(refusedByCeiling === undefined ? {} : { refusedByCeiling }),
        });
        const toolkit = yield* ProjectToolkit.pipe(
          Effect.provide(
            McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
              Layer.provide(dependencies),
            ),
          ),
        );
        const result = yield* toolkit
          .handle("t3_thread_launch", targeted === false ? input : { ...input, projectId })
          .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
        return { result: result.at(-1)?.result, launched, projectId, modelSelection };
      });

      const belowCeiling = yield* launchWith("full-access", "approval-required", {
        title: "Fix",
        message: "Fix the bug",
      });
      expect(belowCeiling.result).toMatchObject({
        projectId: belowCeiling.projectId,
        modelSelection: belowCeiling.modelSelection,
        link: `<t3-thread://v2/environment/${belowCeiling.launched[0]?.threadId}>`,
      });
      expect(belowCeiling.launched[0]?.runtimeMode).toBe("approval-required");
      expect(belowCeiling.launched[0]?.initialMessage?.senderThreadId).toBeUndefined();

      const aboveCeiling = yield* launchWith("auto-accept-edits", "full-access", {
        title: "Fix",
        webOrigin: "https://box.example.ts.net",
      });
      expect(aboveCeiling.launched[0]?.runtimeMode).toBe("auto-accept-edits");
      expect(aboveCeiling.result).toMatchObject({
        link: `https://box.example.ts.net/environment/${encodeURIComponent(aboveCeiling.launched[0]?.threadId ?? "")}`,
      });

      const requested = yield* launchWith("full-access", "approval-required", {
        title: "Fix",
        runtimeMode: "auto",
      });
      expect(requested.launched[0]?.runtimeMode).toBe("auto");

      const escalated = yield* launchWith("auto-accept-edits", "auto-accept-edits", {
        title: "Fix",
        runtimeMode: "full-access",
      });
      expect(escalated.result).toMatchObject({ code: "runtime_mode_escalation_denied" });
      expect(escalated.launched).toHaveLength(0);

      const untargeted = yield* launchWith("full-access", "full-access", {
        title: "Fix",
        targeted: false,
      });
      expect(untargeted.result).toMatchObject({ code: "target_required" });

      const overrideModel = {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-settings",
      };
      const fromSettings = yield* launchWith("full-access", "full-access", {
        title: "Fix",
        overrideModel,
      });
      expect(fromSettings.launched[0]?.modelSelection).toEqual(overrideModel);

      // An administrator's ceiling the orchestrator enforces is named, not reported as unavailable.
      const managed = yield* launchWith("full-access", "full-access", {
        title: "Fix",
        refusedByCeiling: true,
      });
      expect(managed.result).toMatchObject({
        code: "invalid_request",
        message: "Example Corp limits threads to Supervised.",
      });
    }),
);

it.effect("a launch steps an inherited mode down to the provider's, and refuses one it lacks", () =>
  Effect.gen(function* () {
    // Pi offers no Auto; a mode a provider lacks would run as Supervised.
    const pi = ProviderInstanceId.make("pi");
    const launchPi = Effect.fn("test.launchPi")(function* (runtimeMode?: RuntimeMode) {
      const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
      const { projectId, dependencies } = yield* clientLaunchHarness({
        runtimeModeCeiling: "auto",
        launched,
        providers: [
          {
            instanceId: pi,
            driver: ProviderDriverKind.make("pi"),
            enabled: true,
            installed: true,
            status: "ready",
            version: null,
            auth: { status: "authenticated" },
            checkedAt: "2026-01-01T00:00:00Z",
            models: [],
            slashCommands: [],
            skills: [],
            supportedRuntimeModes: ["approval-required", "auto-accept-edits", "full-access"],
          },
        ],
      });
      const toolkit = yield* ProjectToolkit.pipe(
        Effect.provide(
          McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
            Layer.provide(dependencies),
          ),
        ),
      );
      const result = yield* toolkit
        .handle("t3_thread_launch", {
          projectId,
          title: "Fix",
          modelSelection: { instanceId: pi, model: "pi-model" },
          ...(runtimeMode === undefined ? {} : { runtimeMode }),
        })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
      return { result: result.at(-1)?.result, launched };
    });

    const inherited = yield* launchPi();
    expect(inherited.launched[0]?.runtimeMode).toBe("auto-accept-edits");

    const requested = yield* launchPi("auto");
    expect(requested.result).toMatchObject({ code: "invalid_request" });
    expect(requested.launched).toHaveLength(0);

    const offered = yield* launchPi("approval-required");
    expect(offered.launched[0]?.runtimeMode).toBe("approval-required");
  }),
);

it.effect("a launch retried with the same clientRequestId replays the first one", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const { projectId, dependencies, receipt, projection, creationReceipts } =
      yield* clientLaunchHarness({
        runtimeModeCeiling: "auto",
        launched,
      });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const launch = (clientRequestId: string | undefined) =>
      toolkit
        .handle("t3_thread_launch", {
          title: "Fix",
          projectId,
          message: "Fix the bug",
          ...(clientRequestId === undefined ? {} : { clientRequestId }),
        })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    const first = (yield* launch("fix-1")).at(-1)!;
    expect(first.isFailure).toBe(false);
    expect(first.result).toMatchObject({
      threadId: "mcp-launch:v1:bb28e36939ff8007bb08bada853383628e4e045f10401a4624afe27b092919dc",
    });
    const originalId = launched[0]!.commandId;
    const originalReceipt = yield* receipt(originalId);
    expect(Option.isSome(originalReceipt)).toBe(true);
    const replay = (yield* launch("fix-1")).at(-1)!;
    expect(replay.isFailure).toBe(false);
    expect(replay.result).toMatchObject({
      threadId: "mcp-launch:v1:bb28e36939ff8007bb08bada853383628e4e045f10401a4624afe27b092919dc",
      replayed: true,
    });
    expect(yield* receipt(originalId)).toEqual(originalReceipt);
    expect(yield* creationReceipts(originalId)).toEqual([
      {
        command_id: originalId,
        aggregate_id: originalId,
        command_type: "thread.create",
        status: "accepted",
      },
    ]);
    expect(launched).toHaveLength(1);
    const stored = yield* projection(ThreadId.make(originalId));
    expect(stored.messages).toHaveLength(1);
    expect(stored.runs).toHaveLength(1);
    expect(stored.messages[0]!.id).toBe(originalId);
    expect(stored.runs[0]!.userMessageId).toBe(originalId);
    const distinct = (yield* launch("fix-2")).at(-1)!;
    expect(distinct.isFailure).toBe(false);
    expect(distinct.result).toMatchObject({
      threadId: "mcp-launch:v1:329666c48e9d7c10f3662c4b0c2d1deb27b9668930a024b5e1ad66ae183d0475",
    });
    yield* launch(undefined);
    yield* launch(undefined);
    const ids = launched.map((input) => [input.commandId, input.threadId]);
    expect(ids[0]).toEqual([
      "mcp-launch:v1:bb28e36939ff8007bb08bada853383628e4e045f10401a4624afe27b092919dc",
      "mcp-launch:v1:bb28e36939ff8007bb08bada853383628e4e045f10401a4624afe27b092919dc",
    ]);
    expect(ids[1]).toEqual([
      "mcp-launch:v1:329666c48e9d7c10f3662c4b0c2d1deb27b9668930a024b5e1ad66ae183d0475",
      "mcp-launch:v1:329666c48e9d7c10f3662c4b0c2d1deb27b9668930a024b5e1ad66ae183d0475",
    ]);
    expect(ids[2]).not.toEqual(ids[3]);
    expect(launched).toHaveLength(4);
  }),
);

it.effect("a launch key of any length or content gives a short id and never a defect", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const { projectId, dependencies } = yield* clientLaunchHarness({
      runtimeModeCeiling: "auto",
      launched,
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const launch = (clientRequestId: string) =>
      toolkit
        .handle("t3_thread_launch", { title: "Fix", projectId, clientRequestId })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    // Percent-encoding would grow this six times; scratch folders are named from the id.
    yield* launch("\u00e9".repeat(256));
    expect(launched.map((input) => input.commandId)).toEqual([
      "mcp-launch:v1:fa00cd8989a923f2f37819f440da079e51c68af9acc8df646423602cfa536b09",
    ]);

    const loneSurrogate = yield* launch("key-\ud800").pipe(Effect.exit);
    expect(Exit.isFailure(loneSurrogate) && Cause.hasDies(loneSurrogate.cause)).toBe(false);
    expect(launched).toHaveLength(1);
  }),
);

it.effect("a retried launch replays after the project loses its default model", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const {
      projectId,
      modelSelection,
      projectDefaults,
      dependencies,
      receipt,
      projection,
      creationReceipts,
    } = yield* clientLaunchHarness({
      runtimeModeCeiling: "auto",
      launched,
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const launch = toolkit
      .handle("t3_thread_launch", { title: "Fix", projectId, clientRequestId: "fix-1" })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    const first = (yield* launch).at(-1)!;
    expect(first.isFailure).toBe(false);
    const originalId = launched[0]!.commandId;
    const originalReceipt = yield* receipt(originalId);
    expect(Option.isSome(originalReceipt)).toBe(true);
    projectDefaults.model = null;
    const retried = (yield* launch).at(-1)?.result;
    expect(retried).toMatchObject({ projectId, modelSelection });
    expect(retried).toMatchObject({
      threadId: originalId,
      replayed: true,
      initialMessage: "not_requested",
      runId: null,
    });
    expect(launched).toHaveLength(1);
    expect(yield* receipt(originalId)).toEqual(originalReceipt);
    expect(yield* creationReceipts(originalId)).toEqual([
      {
        command_id: originalId,
        aggregate_id: originalId,
        command_type: "thread.create",
        status: "accepted",
      },
    ]);
    const stored = yield* projection(ThreadId.make(originalId));
    expect(stored.messages).toEqual([]);
    expect(stored.runs).toEqual([]);
    const fresh = (yield* toolkit
      .handle("t3_thread_launch", { title: "Fix", projectId, clientRequestId: "new-key" })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies))).at(-1)!;
    expect(fresh.isFailure).toBe(true);
    expect(fresh.result).toMatchObject({ code: "invalid_request" });
    expect(launched).toHaveLength(1);
  }),
);

it.effect("a launch binds only an existing checkout that is one of the project's worktrees", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-launch-worktree-" });
    const repo = path.join(root, "repo");
    const worktree = path.join(root, "feature");
    const outside = path.join(root, "outside");
    yield* fileSystem.makeDirectory(repo);
    yield* fileSystem.makeDirectory(outside);

    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const { projectId, dependencies } = yield* clientLaunchHarness({
      runtimeModeCeiling: "auto",
      launched,
      workspaceRoot: repo,
    });
    const git = yield* GitVcsDriver.GitVcsDriver.pipe(Effect.provide(dependencies));
    for (const args of [
      ["init", "-b", "main"],
      ["-c", "user.email=t@t", "-c", "user.name=t", "commit", "--allow-empty", "-m", "init"],
      ["worktree", "add", "-b", "feature", worktree],
    ]) {
      yield* git.execute({ operation: "test.setupRepo", cwd: repo, args });
    }
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const launchInto = (worktreePath: string) =>
      toolkit
        .handle("t3_thread_launch", {
          title: "Fix",
          projectId,
          workspaceStrategy: { type: "existing_worktree", worktreePath },
        })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    expect((yield* launchInto(worktree)).at(-1)?.result).toMatchObject({ projectId });
    expect((yield* launchInto(repo)).at(-1)?.result).toMatchObject({ projectId });
    for (const elsewhere of [outside, path.join(worktree, "..", "outside"), "/"]) {
      expect((yield* launchInto(elsewhere)).at(-1)?.result).toMatchObject({
        code: "invalid_request",
      });
    }
    expect(launched.map((launch) => launch.workspaceStrategy)).toEqual([
      { type: "existing_worktree", worktreePath: worktree },
      { type: "existing_worktree", worktreePath: repo },
    ]);

    yield* fileSystem.remove(worktree, { recursive: true });
    yield* fileSystem.makeDirectory(worktree);
    expect((yield* launchInto(worktree)).at(-1)?.result).toMatchObject({
      code: "invalid_request",
    });
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("a launch into a new worktree creates nothing when its base ref has no commit", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const { projectId, dependencies } = yield* clientLaunchHarness({
      runtimeModeCeiling: "auto",
      launched,
      checkWorktreeBase: ({ projectId, workspaceStrategy }) =>
        workspaceStrategy.type === "worktree" && workspaceStrategy.baseRef === "t3/renamed"
          ? Effect.fail(
              new ThreadLaunch.ThreadLaunchBaseRefError({
                projectId,
                projectTitle: "Client target",
                workspaceRoot: "/projects/client-target",
                baseRef: workspaceStrategy.baseRef ?? "HEAD",
              }),
            )
          : Effect.void,
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const launchFrom = (baseRef: string) =>
      toolkit
        .handle("t3_thread_launch", {
          title: "Fix",
          projectId,
          workspaceStrategy: { type: "worktree", baseRef, startFromOrigin: false },
        })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    const refused = (yield* launchFrom("t3/renamed")).at(-1)?.result;
    expect(refused).toMatchObject({ code: "invalid_request" });
    expect((refused as { message: string }).message).toContain(
      'Base ref "t3/renamed" does not resolve to a commit in project "Client target" (/projects/client-target). No thread was created.',
    );
    expect(launched).toHaveLength(0);

    expect((yield* launchFrom("main")).at(-1)?.result).toMatchObject({ projectId });
    expect(launched).toHaveLength(1);
  }),
);

it.effect("a retried worktree launch replays after its base ref is gone", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const base = { exists: true };
    const {
      projectId,
      dependencies,
      receipt,
      projection,
      creationReceipts,
      createWorktree,
      runSetup,
      waitForSetup,
    } = yield* clientLaunchHarness({
      runtimeModeCeiling: "auto",
      launched,
      checkWorktreeBase: ({ projectId, workspaceStrategy }) =>
        base.exists || workspaceStrategy.type !== "worktree"
          ? Effect.void
          : Effect.fail(
              new ThreadLaunch.ThreadLaunchBaseRefError({
                projectId,
                projectTitle: "Client target",
                workspaceRoot: "/projects/client-target",
                baseRef: workspaceStrategy.baseRef ?? "HEAD",
              }),
            ),
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const launch = toolkit
      .handle("t3_thread_launch", {
        title: "Fix",
        message: "Synthetic replay prompt",
        projectId,
        clientRequestId: "fix-worktree",
        workspaceStrategy: { type: "worktree", baseRef: "feature", startFromOrigin: false },
      })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    const first = (yield* launch).at(-1)!;
    expect(first.isFailure).toBe(false);
    yield* waitForSetup;
    const originalId = launched[0]!.commandId;
    const originalReceipt = yield* receipt(originalId);
    expect(Option.isSome(originalReceipt)).toBe(true);
    base.exists = false;
    expect((yield* launch).at(-1)?.result).toMatchObject({ projectId });
    const replay = (yield* launch).at(-1)!;
    expect(replay.isFailure).toBe(false);
    expect(replay.result).toMatchObject({ threadId: originalId, replayed: true });
    expect(yield* receipt(originalId)).toEqual(originalReceipt);
    expect(yield* creationReceipts(originalId)).toEqual([
      {
        command_id: originalId,
        aggregate_id: originalId,
        command_type: "thread.create",
        status: "accepted",
      },
    ]);
    expect(launched).toHaveLength(1);
    const stored = yield* projection(ThreadId.make(originalId));
    expect(stored.messages).toHaveLength(1);
    expect(stored.runs).toHaveLength(1);
    expect(stored.messages[0]!.id).toBe(originalId);
    expect(stored.runs[0]!.userMessageId).toBe(originalId);
    expect(createWorktree.mock.calls).toHaveLength(1);
    expect(runSetup.mock.calls).toHaveLength(1);
    const fresh = (yield* toolkit
      .handle("t3_thread_launch", {
        title: "Fix",
        message: "Synthetic replay prompt",
        projectId,
        clientRequestId: "new-worktree-key",
        workspaceStrategy: { type: "worktree", baseRef: "feature", startFromOrigin: false },
      })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies))).at(-1)!;
    expect(fresh.isFailure).toBe(true);
    expect(fresh.result).toMatchObject({ code: "invalid_request" });
    expect(createWorktree.mock.calls).toHaveLength(1);
    expect(runSetup.mock.calls).toHaveLength(1);
  }),
);

it.effect("a client launches with the fallback for an exhausted default", () =>
  Effect.gen(function* () {
    const launched: ThreadLaunch.ThreadLaunchInput[] = [];
    const fallbackModel = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1-sol" };
    const providers: ServerProvider[] = ["claudeAgent", "codex"].map((driver) => ({
      instanceId: ProviderInstanceId.make(driver),
      driver: ProviderDriverKind.make(driver),
      enabled: true,
      installed: true,
      status: "ready",
      version: null,
      auth: { status: "authenticated" },
      checkedAt: "2026-01-01T00:00:00Z",
      models: [],
      slashCommands: [],
      skills: [],
      ...(driver === "claudeAgent"
        ? {
            usageLimits: {
              checkedAt: "2026-01-01T00:00:00Z",
              windows: [{ id: "session", kind: "session", label: "Session", usedPercent: 100 }],
            },
          }
        : {}),
    }));
    const { projectId, dependencies } = yield* clientLaunchHarness({
      runtimeModeCeiling: "full-access",
      launched,
      fallbackModel,
      providers,
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const result = yield* toolkit
      .handle("t3_thread_launch", {
        projectId,
        title: "Fallback",
      })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
    expect(result.at(-1)?.result).toMatchObject({
      modelSelection: fallbackModel,
    });
    expect(launched[0]?.modelSelection).toEqual(fallbackModel);
  }),
);

it.effect("deleting a project through MCP also discards its clone record", () =>
  Effect.gen(function* () {
    const discarded: Array<ProjectId> = [];
    const harness = yield* clientLaunchHarness({ runtimeModeCeiling: "full-access", launched: [] });
    const dependencies = Layer.merge(
      harness.dependencies,
      Layer.mergeAll(
        Layer.mock(Project.ProjectService)({
          delete: (input) =>
            Effect.succeed({
              id: input.projectId,
              title: "client-target",
              workspaceRoot: "/projects/client-target",
              repositoryIdentity: null,
              faviconPath: null,
              defaultModelSelection: null,
              defaultThreadEnvMode: null,
              autoPull: false,
              projectIcon: null,
              scripts: [],
              createdAt: "2026-01-01T00:00:00.000Z",
              updatedAt: "2026-01-01T00:00:00.000Z",
              deletedAt: "2026-01-01T00:00:00.000Z",
            }),
        }),
        Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({
          discard: (projectId) => Effect.sync(() => void discarded.push(projectId)),
        }),
      ),
    );
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );

    yield* toolkit
      .handle("t3_project_delete", { projectId: harness.projectId })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    expect(discarded).toEqual([harness.projectId]);
  }),
);

it.effect(
  "a linked launch retried with the same clientRequestId retains its origin and modes",
  () =>
    Effect.gen(function* () {
      const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
      const { projectId, dependencies } = yield* clientLaunchHarness({
        runtimeModeCeiling: "auto",
        linked: true,
        narrowedTo: { runtimeMode: "approval-required", interactionMode: "plan" },
        launched,
      });
      const toolkit = yield* ProjectToolkit.pipe(
        Effect.provide(
          McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
            Layer.provide(dependencies),
          ),
        ),
      );
      const launch = (clientRequestId: string | undefined) =>
        toolkit
          .handle("t3_thread_launch", {
            title: "Fix",
            projectId,
            message: "Fix the bug",
            delegatedFrom: {
              environmentId: EnvironmentId.make("source-environment"),
              threadId: ThreadId.make("source-parent"),
              title: "Parent task",
            },
            ...(clientRequestId === undefined ? {} : { clientRequestId }),
          })
          .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

      yield* launch("fix-1");
      yield* launch("fix-1");
      yield* launch("fix-2");
      yield* launch(undefined);
      yield* launch(undefined);
      const ids = launched.map((input) => [input.commandId, input.threadId]);
      // The same key sends the same command and thread ids, which the launch
      // service replays instead of starting a second thread.
      expect(ids[0]).toEqual(ids[1]);
      expect(ids[0]).toEqual([
        "mcp:launch:13323d3d71845a7f2c293a894f39437ca1b337408c1927ee33499e96d5a002a8",
        "mcp:launch:13323d3d71845a7f2c293a894f39437ca1b337408c1927ee33499e96d5a002a8",
      ]);
      expect(ids[2]).toEqual([
        "mcp:launch:cd0220b9ca42aaa97fde52795c9105489747f3786347088272dc8b23e173456d",
        "mcp:launch:cd0220b9ca42aaa97fde52795c9105489747f3786347088272dc8b23e173456d",
      ]);
      expect(ids[2]).not.toEqual(ids[0]);
      // Without a key every launch is new.
      expect(ids[3]).not.toEqual(ids[4]);
      for (const input of launched) {
        expect(input.threadId).toBe(input.commandId);
        expect(input.initialMessage?.messageId).toBe(input.commandId);
        expect(input.linkOrigin).toEqual({
          sessionId: "session-1",
          label: "Claude Code",
          access: "auto",
        });
        expect(input.delegatedFrom).toEqual({
          environmentId: "source-environment",
          threadId: "source-parent",
          title: "Parent task",
        });
        expect(input.runtimeMode).toBe("approval-required");
        expect(input.interactionMode).toBe("plan");
      }
    }),
);

it.effect("a delegated linked launch preserves inherited modes over project defaults", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const { projectId, dependencies } = yield* clientLaunchHarness({
      runtimeModeCeiling: "full-access",
      defaultRuntimeMode: "approval-required",
      linked: true,
      narrowedTo: { runtimeMode: "auto", interactionMode: "default" },
      launched,
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    yield* toolkit
      .handle("t3_thread_launch", {
        title: "Inherited delegated task",
        projectId,
        delegatedFrom: {
          environmentId: EnvironmentId.make("source-environment"),
          threadId: ThreadId.make("source-parent"),
          title: "Parent task",
        },
      })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
    expect(launched).toHaveLength(1);
    expect(launched[0]?.runtimeMode).toBe("auto");
    expect(launched[0]?.interactionMode).toBe("default");
    expect(launched[0]?.linkOrigin?.access).toBe("full-access");
  }),
);

it.effect.each(["without delegation", "without source modes"] as const)(
  "a linked launch %s keeps its project mode default",
  (scenario) =>
    Effect.gen(function* () {
      const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
      const { projectId, dependencies } = yield* clientLaunchHarness({
        runtimeModeCeiling: "full-access",
        defaultRuntimeMode: "approval-required",
        linked: true,
        ...(scenario === "without delegation"
          ? { narrowedTo: { runtimeMode: "auto" as const, interactionMode: "default" as const } }
          : {}),
        launched,
      });
      const toolkit = yield* ProjectToolkit.pipe(
        Effect.provide(
          McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
            Layer.provide(dependencies),
          ),
        ),
      );
      yield* toolkit
        .handle("t3_thread_launch", {
          title: "Ordinary linked launch",
          projectId,
          ...(scenario === "without source modes"
            ? {
                delegatedFrom: {
                  environmentId: EnvironmentId.make("source-environment"),
                  threadId: ThreadId.make("source-parent"),
                  title: "Parent task",
                },
              }
            : {}),
        })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
      expect(launched).toHaveLength(1);
      expect(launched[0]?.runtimeMode).toBe("approval-required");
    }),
);
