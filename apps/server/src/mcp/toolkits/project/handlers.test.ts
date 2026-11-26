import { makeClientLaunchHarness } from "../../../jones/mcp/clientLaunch/ClientLaunch.testkit.ts";
import { clientLaunchIds } from "../../../jones/mcp/clientLaunch/ClientLaunchIdentity.ts";
import {
  encodeLinkedMessageOrigin,
  withLinkedMessageOrigin,
} from "../../../jones/peerTransport/LinkedMessageOrigin.ts";
import { ClaudeProviderCapabilitiesV2 } from "../../../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import { CodexProviderCapabilitiesV2 } from "../../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Deferred from "effect/Deferred";
import * as SqlClient from "effect/sql/SqlClient";

import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as CommandReceipts from "../../../orchestration-v2/CommandReceiptStore.ts";
import * as EventSink from "../../../orchestration-v2/EventSink.ts";
import * as SupersededTerminalTrace from "../../../jones/orchestration/SupersededTerminalTrace.ts";
import * as ProviderRegistry from "../../../provider/ProviderRegistry.ts";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  GitCommandError,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  type ServerProvider,
  ThreadId,
  type OrchestrationV2ThreadShell,
  type ModelSelection,
  type ProjectScript,
  type RuntimeMode,
} from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";
import * as TestClock from "effect/testing/TestClock";

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
import * as SourceControlProviderRegistry from "../../../sourceControl/SourceControlProviderRegistry.ts";
import * as Repositories from "../../../sourceControl/SourceControlRepositoryService.ts";
import * as GitVcsDriver from "../../../vcs/GitVcsDriver.ts";
import * as VcsProcess from "../../../vcs/VcsProcess.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import * as ProjectHandlers from "./handlers.ts";
import { ProjectToolkit } from "./tools.ts";
import * as NodeFSP from "node:fs/promises";

function launchProjectLayer(projectId: ProjectId) {
  return Layer.mock(Project.ProjectService)({
    getById: (id) =>
      id === projectId
        ? Effect.succeedSome({
            id,
            title: id,
            workspaceRoot: "/projects/test",
            defaultModelSelection: null,
            scripts: [],
            createdAt: "2026-01-01T00:00:00.000Z",
            updatedAt: "2026-01-01T00:00:00.000Z",
            deletedAt: null,
          })
        : Effect.succeedNone,
  });
}

const launchedFromThread = (linkOrigin?: OrchestrationV2ThreadShell["linkOrigin"]) =>
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
      ...(linkOrigin === undefined ? {} : { linkOrigin }),
    } as OrchestrationV2ThreadShell;
    let launched: ThreadLaunch.ThreadLaunchInput | undefined;
    const layerDependencies = Layer.mergeAll(
      Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
      NodeSqliteClient.layer({ filename: ":memory:" }),
      Layer.mock(CommandReceipts.CommandReceiptStoreV2)({
        getByCommandId: () => Effect.succeedNone,
      }),
      Layer.mock(EventSink.EventSinkV2)({
        supersededTerminalTrace: SupersededTerminalTrace.make(),
      }),
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
        getThreadShell: (threadId) => Effect.succeed(threadId === sourceThreadId ? caller : null),
      }),
      Layer.mock(ThreadLaunch.ThreadLaunchService)({
        launch: (input) => {
          launched = input;
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
      launchProjectLayer(projectId),
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
      .handle("t3_thread_launch", {
        title: "Audit",
        message: "Review the change",
        ...(linkOrigin === undefined
          ? {}
          : {
              clientRequestId: `workflow:${"ab".repeat(32)}`,
              workflowRun: {
                environmentId: EnvironmentId.make("source-environment"),
                workflow: "audit",
                runId: "workflow-run",
                step: "review",
              },
            }),
      })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(layerDependencies));
    expect(result.at(-1)?.result).toMatchObject({ projectId, modelSelection });
    expect(launched?.initialMessage?.senderThreadId).toBe(sourceThreadId);
    expect(launched?.linkOrigin).toEqual(linkOrigin);
    expect(launched?.workflowRun).toBeUndefined();
  });

it.effect("attributes a launched thread's first message to the calling thread", () =>
  launchedFromThread(),
);

it.effect("preserves the inherited link while ignoring a calling thread's workflow claim", () =>
  launchedFromThread({ sessionId: "peer-session", label: "Linked T3 Code", access: "full-access" }),
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
      Layer.mock(EventSink.EventSinkV2)({
        supersededTerminalTrace: SupersededTerminalTrace.make(),
      }),
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
      launchProjectLayer(scratchProjectId),
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
      Layer.mock(EventSink.EventSinkV2)({
        supersededTerminalTrace: SupersededTerminalTrace.make(),
      }),
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
  readonly newWorktreesStartFromOrigin?: boolean;
  readonly defaultThreadEnvMode?: "local" | "worktree";
  readonly fallbackModel?: ModelSelection;
  readonly providers?: readonly ServerProvider[];
  /** Where Settings saves a project's model; the legacy project field stays null. */
  readonly overrideModel?: ModelSelection;
  readonly overrideScripts?: readonly ProjectScript[];
  readonly launched: Array<ThreadLaunch.ThreadLaunchInput>;
  readonly linked?: boolean;
  /** Command ids whose launch an earlier build already accepted. */
  readonly acceptedCommandIds?: ReadonlySet<string>;
  /** Fails the receipt read, as an unavailable store would. */
  readonly receiptReadFails?: boolean;
  readonly narrowedTo?: McpInvocationContext.ClientModes;
  readonly workspaceRoot?: string;
  readonly hasCommit?: NonNullable<Parameters<typeof makeClientLaunchHarness>[0]>["hasCommit"];
  readonly checkWorktreeBase?: ThreadLaunch.ThreadLaunchService["Service"]["checkWorktreeBase"];
  readonly hostedAppUrl?: string;
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
    ...(input.defaultThreadEnvMode === undefined
      ? {}
      : { defaultThreadEnvMode: input.defaultThreadEnvMode }),
    ...(input.defaultRuntimeMode === undefined
      ? {}
      : { defaultRuntimeMode: input.defaultRuntimeMode }),
    ...(input.newWorktreesStartFromOrigin === undefined
      ? {}
      : { newWorktreesStartFromOrigin: input.newWorktreesStartFromOrigin }),
    ...(input.overrideModel === undefined
      ? {}
      : {
          projectSettingsFolded: true,
          projectSettingsOverrides: { [projectId]: { defaultModelSelection: input.overrideModel } },
        }),
  };
  const setupEntered = yield* Deferred.make<void>();
  const scopedConfig = ServerConfig.layerTest(process.cwd(), { prefix: "t3-client-launch-" }).pipe(
    Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
  );
  // The harness owns the scoped directory through its last launch.
  const config = Context.get(yield* Layer.build(scopedConfig), ServerConfig.ServerConfig);
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
    ...(input.hasCommit === undefined ? {} : { hasCommit: input.hasCommit }),
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
      refreshWorkspaceSnapshot: () => Effect.succeed(input.providers ?? []),
    }),
    Layer.succeed(McpInvocationContext.McpInvocationContext, {
      environmentId: EnvironmentId.make("environment"),
      requestNamespace: "client:session-1",
      thread: undefined,
      client: {
        sessionId: "session-1",
        label: "Claude Code",
        access: input.runtimeModeCeiling,
        hostedAppUrl: input.hostedAppUrl,
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
  const built = yield* Layer.build(layerDependencies);
  const storedReceipts = Context.get(built, CommandReceipts.CommandReceiptStoreV2);
  // Earlier-build acceptances and an unavailable store are simulated on the real store.
  const dependencies = Layer.succeedContext(
    input.acceptedCommandIds === undefined && input.receiptReadFails !== true
      ? built
      : Context.add(built, CommandReceipts.CommandReceiptStoreV2, {
          ...storedReceipts,
          getByCommandId: (commandId) =>
            input.receiptReadFails === true
              ? Effect.fail(
                  new CommandReceipts.CommandReceiptStoreReadError({
                    commandId,
                    cause: "unavailable",
                  }),
                )
              : input.acceptedCommandIds?.has(commandId) === true
                ? Effect.succeedSome({} as CommandReceipts.CommandReceiptV2)
                : storedReceipts.getByCommandId(commandId),
        }),
  );
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
          readonly generateTitle?: boolean;
          readonly message?: string;
          readonly runtimeMode?: RuntimeMode;
          readonly targeted?: false;
          readonly overrideModel?: ModelSelection;
          readonly hostedAppUrl?: string;
          readonly refusedByCeiling?: true;
        },
      ) {
        const { targeted, overrideModel, hostedAppUrl, refusedByCeiling, ...input } = params;
        const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
        const { projectId, modelSelection, dependencies } = yield* clientLaunchHarness({
          runtimeModeCeiling,
          defaultRuntimeMode,
          launched,
          ...(overrideModel === undefined ? {} : { overrideModel }),
          ...(hostedAppUrl === undefined ? {} : { hostedAppUrl }),
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
        link: `[Fix](t3-thread://v2/environment/${belowCeiling.launched[0]?.threadId})`,
      });
      expect(belowCeiling.launched[0]?.runtimeMode).toBe("approval-required");
      expect(belowCeiling.launched[0]?.initialMessage?.senderThreadId).toBeUndefined();
      expect(belowCeiling.launched[0]?.generateTitle).toBeUndefined();
      expect(belowCeiling.launched[0]?.titleRenamed).toBe(true);

      const titled = yield* launchWith("full-access", "full-access", {
        title: "Fix",
        generateTitle: true,
        message: "Fix the bug",
      });
      expect(titled.launched[0]).toMatchObject({ title: "Fix", generateTitle: true });
      expect(titled.launched[0]?.titleRenamed).toBeUndefined();

      for (const message of [undefined, "", " \n "]) {
        const idleTitled = yield* launchWith("full-access", "full-access", {
          title: "Fix",
          generateTitle: true,
          ...(message === undefined ? {} : { message }),
        });
        expect(idleTitled.result).toMatchObject({ code: "invalid_request" });
        expect(idleTitled.launched).toHaveLength(0);
      }

      const aboveCeiling = yield* launchWith("auto-accept-edits", "full-access", {
        title: "Fix",
        hostedAppUrl: "https://t3.example.com",
      });
      expect(aboveCeiling.launched[0]?.runtimeMode).toBe("auto-accept-edits");
      expect(aboveCeiling.result).toMatchObject({
        link: `https://t3.example.com/threads/environment/${encodeURIComponent(aboveCeiling.launched[0]?.threadId ?? "")}`,
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

it.effect("a launch with a model its provider does not advertise creates no thread", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const providers: ServerProvider[] = ["claudeAgent", "codex"].map((driver) => ({
      instanceId: ProviderInstanceId.make(driver),
      driver: ProviderDriverKind.make(driver),
      enabled: true,
      installed: true,
      status: "ready",
      version: null,
      auth: { status: "authenticated" },
      checkedAt: "2026-01-01T00:00:00Z",
      models: [
        driver === "claudeAgent"
          ? {
              slug: "claude-opus",
              name: "Opus",
              aliases: ["opus"],
              isCustom: false,
              capabilities: null,
            }
          : { slug: "gpt-5.4", name: "GPT", isCustom: false, capabilities: null },
      ],
      slashCommands: [],
      skills: [],
      // The project folder's own provider config adds a model the machine catalog lacks.
      workspaceSnapshots:
        driver === "codex"
          ? [
              {
                cwd: "/projects/client-target",
                checkedAt: "2026-01-01T00:00:00Z",
                slashCommands: [],
                skills: [],
                models: [
                  { slug: "gpt-5.4", name: "GPT", isCustom: false, capabilities: null },
                  { slug: "folder-local", name: "Local", isCustom: false, capabilities: null },
                ],
              },
            ]
          : [],
    }));
    const { projectId, dependencies } = yield* clientLaunchHarness({
      runtimeModeCeiling: "auto",
      launched,
      providers,
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const launchWith = (instanceId: string, model: string) =>
      toolkit
        .handle("t3_thread_launch", {
          title: "Fix",
          projectId,
          modelSelection: { instanceId: ProviderInstanceId.make(instanceId), model },
        })
        .pipe(
          Stream.unwrap,
          Stream.runCollect,
          Effect.map((result) => result.at(-1)?.result),
          Effect.provide(dependencies),
        );

    for (const [instanceId, model] of [
      ["claudeAgent", "claude-unadvertised"],
      ["codex", "opus"],
    ] as const) {
      expect(yield* launchWith(instanceId, model)).toMatchObject({
        code: "model_unavailable",
        message: expect.stringContaining(
          `Model ${model} is not advertised by provider ${instanceId}. Advertised models: `,
        ),
      });
    }
    expect(launched).toHaveLength(0);

    expect(yield* launchWith("claudeAgent", "Opus")).toMatchObject({
      modelSelection: { instanceId: "claudeAgent", model: "Opus" },
    });
    expect(yield* launchWith("codex", "folder-local")).toMatchObject({
      modelSelection: { instanceId: "codex", model: "folder-local" },
    });
    expect(launched).toHaveLength(2);
  }),
);

it.effect(
  "a launch rejects options its model does not declare and normalizes legacy ultracode effort",
  () =>
    Effect.gen(function* () {
      const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
      const providers: ServerProvider[] = [
        {
          instanceId: ProviderInstanceId.make("claudeAgent"),
          driver: ProviderDriverKind.make("claudeAgent"),
          enabled: true,
          installed: true,
          status: "ready",
          version: null,
          auth: { status: "authenticated" },
          checkedAt: "2026-01-01T00:00:00Z",
          models: [
            {
              slug: "claude-opus-5-5",
              name: "Claude Opus 5.5",
              isCustom: false,
              capabilities: {
                optionDescriptors: [
                  {
                    id: "effort",
                    role: "reasoning",
                    label: "Effort",
                    type: "select",
                    options: [
                      { id: "high", label: "High" },
                      { id: "xhigh", label: "Extra High" },
                    ],
                  },
                  { id: "ultracode", label: "Ultracode", type: "boolean" },
                ],
              },
            },
          ],
          slashCommands: [],
          skills: [],
        },
      ];
      const { projectId, dependencies } = yield* clientLaunchHarness({
        runtimeModeCeiling: "auto",
        launched,
        providers,
      });
      const toolkit = yield* ProjectToolkit.pipe(
        Effect.provide(
          McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
            Layer.provide(dependencies),
          ),
        ),
      );
      const launchWith = (options: NonNullable<ModelSelection["options"]>) =>
        toolkit
          .handle("t3_thread_launch", {
            title: "Options check",
            projectId,
            modelSelection: {
              instanceId: ProviderInstanceId.make("claudeAgent"),
              model: "claude-opus-5-5",
              options,
            },
          })
          .pipe(
            Stream.unwrap,
            Stream.runCollect,
            Effect.map((result) => result.at(-1)?.result),
            Effect.provide(dependencies),
          );

      expect(yield* launchWith([{ id: "thought_level", value: "high" }])).toMatchObject({
        code: "invalid_request",
        message: expect.stringContaining("Unknown option thought_level"),
      });
      expect(launched).toHaveLength(0);

      yield* launchWith([{ id: "effort", value: "ultracode" }]);
      expect(launched.map((launch) => launch.modelSelection.options)).toEqual([
        [
          { id: "effort", value: "xhigh" },
          { id: "ultracode", value: true },
        ],
      ]);
    }),
);

it.effect("only a full-access client may pick the agent's own mode at launch", () =>
  Effect.gen(function* () {
    const agent = ProviderInstanceId.make("acp-agent");
    const launchAs = (runtimeModeCeiling: "approval-required" | "full-access") =>
      Effect.gen(function* () {
        const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
        const { projectId, dependencies } = yield* clientLaunchHarness({
          runtimeModeCeiling,
          launched,
          providers: [
            {
              instanceId: agent,
              driver: ProviderDriverKind.make("acpRegistry"),
              enabled: true,
              installed: true,
              status: "ready",
              version: null,
              auth: { status: "authenticated" },
              checkedAt: "2026-01-01T00:00:00Z",
              models: [
                {
                  slug: "default",
                  name: "Default",
                  isCustom: false,
                  capabilities: {
                    optionDescriptors: [
                      {
                        id: "_t3/session-mode",
                        label: "Mode",
                        type: "select",
                        options: [
                          { id: "default", label: "Default" },
                          { id: "yolo", label: "YOLO" },
                        ],
                        currentValue: "default",
                      },
                    ],
                  },
                },
              ],
              slashCommands: [],
              skills: [],
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
            message: "Fix the bug",
            modelSelection: {
              instanceId: agent,
              model: "default",
              options: [{ id: "_t3/session-mode", value: "yolo" }],
            },
          })
          .pipe(
            Stream.unwrap,
            Stream.runCollect,
            Effect.map((collected) => collected.at(-1)?.result),
            Effect.provide(dependencies),
          );
        return { result, launched };
      });

    const supervised = yield* launchAs("approval-required");
    expect(supervised.result).toMatchObject({ code: "runtime_mode_escalation_denied" });
    expect(supervised.launched).toHaveLength(0);

    const fullAccess = yield* launchAs("full-access");
    expect(fullAccess.launched.map((launch) => launch.modelSelection.options)).toEqual([
      [{ id: "_t3/session-mode", value: "yolo" }],
    ]);
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
      initialMessage: "accepted",
    });
    const originalId = launched[0]!.commandId;
    const originalReceipt = yield* receipt(originalId);
    expect(Option.isSome(originalReceipt)).toBe(true);
    const replay = (yield* launch("fix-1")).at(-1)!;
    expect(replay.isFailure).toBe(false);
    expect(replay.result).toMatchObject({
      threadId: "mcp-launch:v1:bb28e36939ff8007bb08bada853383628e4e045f10401a4624afe27b092919dc",
      replayed: true,
      initialMessage: "accepted",
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
    const { messageId } = clientLaunchIds("client:session-1", "fix-1");
    expect(stored.messages[0]!.id).toBe(messageId);
    expect(stored.runs[0]!.userMessageId).toBe(messageId);
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
    // Git registers resolved paths, and a launch keeps the registered one.
    const root = yield* fileSystem.realPath(
      yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-launch-worktree-" }),
    );
    const repo = path.join(root, "repo");
    const worktree = path.join(root, "feature");
    const outside = path.join(root, "outside");
    const alias = path.join(root, "alias");
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
    const launchInto = (worktreePath: string, clientRequestId?: string) =>
      toolkit
        .handle("t3_thread_launch", {
          title: "Fix",
          projectId,
          ...(clientRequestId === undefined ? {} : { clientRequestId }),
          workspaceStrategy: { type: "existing_worktree", worktreePath },
        })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    expect((yield* launchInto(worktree, "existing-checkout")).at(-1)?.result).toMatchObject({
      projectId,
    });
    expect((yield* launchInto(outside, "existing-checkout")).at(-1)?.result).toMatchObject({
      code: "request_conflict",
    });
    expect((yield* launchInto(repo)).at(-1)?.result).toMatchObject({ projectId });
    for (const elsewhere of [outside, path.join(worktree, "..", "outside"), "/"]) {
      expect((yield* launchInto(elsewhere)).at(-1)?.result).toMatchObject({
        code: "invalid_request",
      });
    }
    // The thread keeps the registered worktree path when launched through a link to it.
    yield* Effect.promise(() => NodeFSP.symlink(worktree, alias, "junction"));
    expect((yield* launchInto(alias)).at(-1)?.result).toMatchObject({ projectId });
    expect(launched.map((launch) => launch.workspaceStrategy)).toEqual([
      { type: "existing_worktree", worktreePath: worktree },
      { type: "existing_worktree", worktreePath: repo },
      { type: "existing_worktree", worktreePath: worktree },
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
      checkWorktreeBase: ({ projectId, workspaceStrategy }) => {
        if (workspaceStrategy.type !== "worktree") return Effect.void;
        if (workspaceStrategy.baseRef === "t3/renamed") {
          return Effect.fail(
            new ThreadLaunch.ThreadLaunchBaseRefError({
              projectId,
              projectTitle: "Client target",
              workspaceRoot: "/projects/client-target",
              baseRef: workspaceStrategy.baseRef,
            }),
          );
        }
        if (workspaceStrategy.branch === "feature/taken") {
          return Effect.fail(
            new ThreadLaunch.ThreadLaunchBranchExistsError({
              projectId,
              projectTitle: "Client target",
              branch: workspaceStrategy.branch,
            }),
          );
        }
        return Effect.void;
      },
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const launchFrom = (baseRef: string, branch?: string) =>
      toolkit
        .handle("t3_thread_launch", {
          title: "Fix",
          projectId,
          workspaceStrategy: {
            type: "worktree",
            baseRef,
            ...(branch === undefined ? {} : { branch }),
            startFromOrigin: false,
          },
        })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    const refused = (yield* launchFrom("t3/renamed")).at(-1)?.result;
    expect(refused).toMatchObject({ code: "invalid_request" });
    expect((refused as { message: string }).message).toContain(
      'Base ref "t3/renamed" does not resolve to a commit in project "Client target" (/projects/client-target). No thread was created. Pass a branch, tag, or commit that exists there',
    );
    expect((yield* launchFrom("main", "feature/taken")).at(-1)?.result).toMatchObject({
      code: "invalid_request",
      message:
        'Branch "feature/taken" already exists in project "Client target". No thread was created. Pass a branch name that does not exist yet, or omit branch to get a generated name.',
    });
    expect(launched).toHaveLength(0);

    expect((yield* launchFrom("main")).at(-1)?.result).toMatchObject({ projectId });
    expect(launched).toHaveLength(1);
  }),
);

it.effect("a worktree launch that omits startFromOrigin follows the new-worktree setting", () =>
  Effect.gen(function* () {
    const cases = [
      { setting: true, given: undefined, expected: true },
      { setting: false, given: undefined, expected: false },
      { setting: true, given: false, expected: false },
    ];
    for (const { setting, given, expected } of cases) {
      const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
      const checked: Array<ThreadLaunch.ThreadLaunchInput["workspaceStrategy"]> = [];
      const { projectId, dependencies } = yield* clientLaunchHarness({
        runtimeModeCeiling: "auto",
        launched,
        newWorktreesStartFromOrigin: setting,
        checkWorktreeBase: ({ workspaceStrategy }) =>
          Effect.sync(() => {
            checked.push(workspaceStrategy);
          }),
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
          title: "Fix",
          projectId,
          workspaceStrategy: {
            type: "worktree",
            baseRef: "main",
            ...(given === undefined ? {} : { startFromOrigin: given }),
          },
        })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

      const strategy = { type: "worktree", baseRef: "main", startFromOrigin: expected };
      expect(checked).toEqual([strategy]);
      expect(launched.map((launch) => launch.workspaceStrategy)).toEqual([strategy]);
    }
  }),
);

it.effect("a launch that omits its workspace follows the new-threads-in-a-worktree default", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    for (const source of ["setting", "t3.json"] as const) {
      const repo = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-launch-env-mode-" });
      const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
      const { projectId, dependencies } = yield* clientLaunchHarness({
        runtimeModeCeiling: "auto",
        launched,
        workspaceRoot: repo,
        ...(source === "setting" ? { defaultThreadEnvMode: "worktree" as const } : {}),
      });
      const git = yield* GitVcsDriver.GitVcsDriver.pipe(Effect.provide(dependencies));
      for (const args of [
        ["init", "-b", "main"],
        ["-c", "user.email=t@t", "-c", "user.name=t", "commit", "--allow-empty", "-m", "init"],
      ]) {
        yield* git.execute({ operation: "test.setupRepo", cwd: repo, args });
      }
      if (source === "t3.json")
        yield* fileSystem.writeFileString(
          `${repo}/t3.json`,
          JSON.stringify({ defaultThreadEnvMode: "worktree" }),
        );
      const toolkit = yield* ProjectToolkit.pipe(
        Effect.provide(
          McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
            Layer.provide(dependencies),
          ),
        ),
      );
      for (const workspaceStrategy of [undefined, { type: "root" as const }]) {
        yield* toolkit
          .handle("t3_thread_launch", {
            title: "Fix",
            projectId,
            ...(workspaceStrategy === undefined ? {} : { workspaceStrategy }),
          })
          .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
      }

      expect(
        launched.map((launch) => launch.workspaceStrategy.type),
        source,
      ).toEqual(["worktree", "root"]);
    }
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("a defaulted worktree launch stays shared only when Git says HEAD has no commit", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const commitArgs = ["-c", "user.email=t@t", "-c", "user.name=t", "commit", "--allow-empty"];
    for (const [checkout, expected] of [
      ["not a repository", "root"],
      ["no commit yet", "root"],
      ["branch tip object missing", "worktree"],
      ["git refuses the repository", "refused"],
    ] as const) {
      const repo = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-launch-head-" });
      const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
      const { projectId, dependencies } = yield* clientLaunchHarness({
        runtimeModeCeiling: "auto",
        launched,
        workspaceRoot: repo,
        defaultThreadEnvMode: "worktree",
      });
      const git = yield* GitVcsDriver.GitVcsDriver.pipe(Effect.provide(dependencies));
      if (checkout !== "not a repository")
        yield* git.execute({
          operation: "test.setupRepo",
          cwd: repo,
          args: ["init", "-b", "main"],
        });
      if (checkout === "branch tip object missing")
        yield* fileSystem.writeFileString(`${repo}/.git/refs/heads/main`, `${"1".repeat(40)}\n`);
      if (checkout === "git refuses the repository") {
        yield* git.execute({
          operation: "test.setupRepo",
          cwd: repo,
          args: [...commitArgs, "-m", "init"],
        });
        // Git quotes the broken file's path in its refusal; that path must not read as "not a repository".
        yield* fileSystem.writeFileString(`${repo}/not a git repository`, "[core\n");
        yield* fileSystem.writeFileString(
          `${repo}/.git/config`,
          `[include]\n\tpath = ${repo}/not a git repository\n`,
        );
      }
      const toolkit = yield* ProjectToolkit.pipe(
        Effect.provide(
          McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
            Layer.provide(dependencies),
          ),
        ),
      );
      const result = (yield* toolkit
        .handle("t3_thread_launch", { title: "Fix", projectId })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies))).at(-1)?.result;

      if (expected === "refused") {
        expect(result, checkout).toMatchObject({ code: "orchestration_error" });
        expect(launched, checkout).toHaveLength(0);
      } else {
        expect(
          launched.map((launch) => launch.workspaceStrategy.type),
          checkout,
        ).toEqual([expected]);
      }
    }
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("a launch into a new worktree creates nothing when its branch collides", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const { projectId, dependencies } = yield* clientLaunchHarness({
      runtimeModeCeiling: "auto",
      launched,
      checkWorktreeBase: ({ projectId, workspaceStrategy }) =>
        Effect.fail(
          new ThreadLaunch.ThreadLaunchBranchConflictError({
            projectId,
            projectTitle: "Client target",
            branch: workspaceStrategy.branch ?? "",
            conflictingBranch: "werkstatt",
          }),
        ),
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const refused = (yield* toolkit
      .handle("t3_thread_launch", {
        title: "Fix",
        projectId,
        workspaceStrategy: { type: "worktree", baseRef: "main", branch: "werkstatt/baseline" },
      })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies))).at(-1)?.result;

    expect(refused).toMatchObject({
      code: "invalid_request",
      message:
        'Branch "werkstatt/baseline" cannot be created in project "Client target": branch "werkstatt" exists. No thread was created. Use a name that does not nest under or contain an existing branch, such as "werkstatt-baseline".',
    });
    expect(launched).toHaveLength(0);
  }),
);

it.effect("a launch into an unknown project says the project was not found", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const { modelSelection, dependencies } = yield* clientLaunchHarness({
      runtimeModeCeiling: "auto",
      launched,
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    for (const workspaceStrategy of [
      undefined,
      { type: "worktree", baseRef: "main", startFromOrigin: false } as const,
    ]) {
      const result = yield* toolkit
        .handle("t3_thread_launch", {
          title: "Fix",
          message: "Fix the bug",
          projectId: ProjectId.make("current"),
          modelSelection,
          ...(workspaceStrategy === undefined ? {} : { workspaceStrategy }),
        })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
      expect(result.at(-1)?.result).toMatchObject({
        code: "invalid_request",
        message: "The project was not found.",
      });
    }
    expect(launched).toHaveLength(0);
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
      runtimeModeCeiling: "full-access",
      defaultRuntimeMode: "full-access",
      launched,
      hasCommit: ({ refName }) => Effect.succeed(base.exists && refName === "feature"),
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
    expect(replay.result).toMatchObject({
      threadId: originalId,
      replayed: true,
      initialMessage: "accepted",
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
    const { messageId } = clientLaunchIds("client:session-1", "fix-worktree");
    expect(stored.messages[0]!.id).toBe(messageId);
    expect(stored.runs[0]!.userMessageId).toBe(messageId);
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

it.effect("creating a project through MCP refuses a model default instead of dropping it", () =>
  Effect.gen(function* () {
    const harness = yield* clientLaunchHarness({ runtimeModeCeiling: "full-access", launched: [] });
    const dependencies = Layer.merge(
      harness.dependencies,
      Layer.mock(Project.ProjectService)({
        create: () => Effect.die("the project was created without its model default"),
      }),
    );
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );

    const refused = yield* toolkit
      .handle("t3_project_create", {
        title: "client-target",
        workspaceRoot: "/projects/client-target",
        defaultModelSelection: harness.modelSelection,
      })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    expect(refused.at(-1)?.result).toMatchObject({
      code: "invalid_request",
      message: expect.stringContaining("t3_project_update"),
    });
  }),
);

it.effect("an agent's clone is not cut off while git is still transferring", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const parent = yield* fs.makeTempDirectoryScoped({ prefix: "t3-mcp-clone-" });
    const destinationPath = path.join(parent, "repo");
    const started = yield* Deferred.make<void>();
    const transferred = yield* Deferred.make<void>();
    const { dependencies } = yield* clientLaunchHarness({
      runtimeModeCeiling: "full-access",
      launched: [],
    });
    const repositories = Repositories.layer.pipe(
      Layer.provide(
        Layer.mock(SourceControlProviderRegistry.SourceControlProviderRegistry)({
          resolveLink: () => undefined,
        }),
      ),
      Layer.provide(
        Layer.mock(GitVcsDriver.GitVcsDriver)({
          invalidateRefs: () => Effect.void,
          // Like the real driver, the clone fails once it outlives the deadline it was given.
          execute: (input) =>
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Deferred.await(transferred)),
              Effect.as({
                exitCode: ChildProcessSpawner.ExitCode(0),
                stdout: "",
                stderr: "",
                stdoutTruncated: false,
                stderrTruncated: false,
              }),
              Effect.timeoutOrElse({
                duration: input.timeoutMs ?? Infinity,
                orElse: () =>
                  Effect.fail(
                    new GitCommandError({
                      operation: input.operation,
                      command: "git",
                      cwd: input.cwd,
                      detail: "Git command timed out.",
                    }),
                  ),
              }),
            ),
        }),
      ),
      Layer.provideMerge(dependencies),
    );
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(repositories)),
      ),
    );

    const clone = yield* toolkit
      .handle("t3_project_clone", { remoteUrl: "https://example.test/repo.git", destinationPath })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(repositories), Effect.forkChild);
    yield* Deferred.await(started);
    // A large repository over a slow link; agents' MCP calls get a far longer budget.
    yield* TestClock.adjust("10 minutes");
    yield* Deferred.succeed(transferred, undefined);

    expect((yield* Fiber.join(clone)).at(-1)?.result).toMatchObject({ cwd: destinationPath });
  }).pipe(Effect.provide(NodeServices.layer)),
);

const deletedProject = (id: ProjectId) => ({
  id,
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
});

it.effect("deleting a project through MCP also discards its clone record", () =>
  Effect.gen(function* () {
    const discarded: Array<ProjectId> = [];
    const harness = yield* clientLaunchHarness({ runtimeModeCeiling: "full-access", launched: [] });
    const dependencies = Layer.merge(
      harness.dependencies,
      Layer.mergeAll(
        Layer.mock(Project.ProjectService)({
          delete: (input) => Effect.succeed(deletedProject(input.projectId)),
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

it.effect("an agent cannot delete the built-in scratch project, even with force", () =>
  Effect.gen(function* () {
    const deleted: Array<ProjectId> = [];
    const harness = yield* clientLaunchHarness({ runtimeModeCeiling: "full-access", launched: [] });
    const dependencies = Layer.merge(
      harness.dependencies,
      Layer.mergeAll(
        Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
          namedProjectsRoot: "/projects",
          isScratchProject: (projectId) => Effect.succeed(projectId === harness.projectId),
        }),
        Layer.mock(Project.ProjectService)({
          delete: (input) =>
            Effect.sync(() => {
              deleted.push(input.projectId);
              return deletedProject(input.projectId);
            }),
        }),
        Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ discard: () => Effect.void }),
      ),
    );
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );

    const refused = yield* toolkit
      .handle("t3_project_delete", { projectId: harness.projectId, force: true })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    expect(deleted).toEqual([]);
    expect(refused.at(-1)?.result).toMatchObject({
      code: "invalid_request",
      message: expect.stringMatching(/scratch|no project/i),
    });
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

it.effect(
  "derives linked keyed-launch ids as upstream's fixed-length hash, pinned by golden vectors",
  () =>
    Effect.gen(function* () {
      // Vectors computed outside this code: sha256 of the JSON pair [namespace, key].
      const vectors = [
        [
          "client:session-1",
          "fix-1",
          "mcp:launch:13323d3d71845a7f2c293a894f39437ca1b337408c1927ee33499e96d5a002a8",
        ],
        [
          "client:session-1",
          "fix-2",
          "mcp:launch:cd0220b9ca42aaa97fde52795c9105489747f3786347088272dc8b23e173456d",
        ],
        [
          "fwd:0123abcd",
          "key with space/é",
          "mcp:launch:a04b39bdceb432506effe49944eeef206dec806c6af11bcbec5e824666eec89f",
        ],
      ] as const;
      for (const [namespace, key, expected] of vectors) {
        expect(yield* ProjectHandlers.linkedLaunchCommandId(namespace, key)).toBe(expected);
      }
      // The caller stays apart from the key.
      expect(yield* ProjectHandlers.linkedLaunchCommandId("a:b", "c")).not.toBe(
        yield* ProjectHandlers.linkedLaunchCommandId("a", "b:c"),
      );
      // The earlier derivation, kept only to recognise a retry, is the percent-encoded form.
      expect(ProjectHandlers.legacyLinkedLaunchCommandId("client:session-1", "fix-1")).toBe(
        "mcp:launch:client%3Asession-1:fix-1",
      );
    }).pipe(Effect.provide(NodeCrypto.layer)),
);

it.effect("attributes workflow lineage only to a matching direct linked workflow launch", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const { projectId, dependencies, projection } = yield* clientLaunchHarness({
      runtimeModeCeiling: "auto",
      linked: true,
      launched,
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const scope = yield* McpInvocationContext.McpInvocationContext.pipe(
      Effect.provide(dependencies),
    );
    const workflowRun = {
      environmentId: EnvironmentId.make("source-environment"),
      runId: "workflow-run",
      workflow: "fix",
      step: "write",
    };
    const origin = {
      schemaVersion: "t3.linked-message-origin/v1",
      environmentId: workflowRun.environmentId,
    } as const;
    const direct = withLinkedMessageOrigin(scope, encodeLinkedMessageOrigin(origin))!;
    const launch = (
      invocation: McpInvocationContext.McpInvocationScope,
      clientRequestId: string,
      attribution = workflowRun,
    ) =>
      toolkit
        .handle("t3_thread_launch", {
          title: "Fix",
          projectId,
          message: "Fix the bug",
          clientRequestId,
          workflowRun: attribution,
        })
        .pipe(
          Stream.unwrap,
          Stream.runCollect,
          Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
          Effect.provide(dependencies),
        );
    yield* launch(direct, `workflow:${"1".repeat(64)}`);
    yield* launch(scope, `workflow:${"2".repeat(64)}`);
    yield* launch(direct, `workflow:${"3".repeat(64)}`, {
      ...workflowRun,
      environmentId: EnvironmentId.make("different-environment"),
    });
    const forwardedThread = withLinkedMessageOrigin(
      scope,
      encodeLinkedMessageOrigin({ ...origin, thread: { threadId: "source-thread" } }),
    )!;
    yield* launch(forwardedThread, `workflow:${"4".repeat(64)}`);
    yield* launch(direct, `fwd:${"5".repeat(64)}`);
    const localClaim = withLinkedMessageOrigin(
      scope,
      encodeLinkedMessageOrigin({ ...origin, environmentId: scope.environmentId }),
    )!;
    yield* launch(localClaim, `workflow:${"6".repeat(64)}`, {
      ...workflowRun,
      environmentId: scope.environmentId,
    });
    yield* launch(
      {
        ...direct,
        requestNamespace: "client:other-session",
        client: { ...direct.client!, sessionId: "other-session" },
      },
      `workflow:${"7".repeat(64)}`,
    );

    expect(launched).toHaveLength(7);
    expect(launched[0]?.workflowRun).toEqual(workflowRun);
    expect(launched.slice(1).every((input) => input.workflowRun === undefined)).toBe(true);
    for (const input of launched.slice(0, 6)) expect(input.linkOrigin?.sessionId).toBe("session-1");
    expect(launched[6]?.linkOrigin?.sessionId).toBe("other-session");
    const accepted = yield* projection(launched[0]!.threadId!);
    const ordinary = yield* projection(launched[1]!.threadId!);
    expect(accepted.thread.workflowRun).toEqual(workflowRun);
    expect(accepted.thread.lineage.workflowStep).toBe(true);
    expect(ordinary.thread.workflowRun).toBeUndefined();
    expect(ordinary.thread.lineage.workflowStep).toBeUndefined();
  }),
);

it.effect("a linked keyed launch gets a short id with no percent-encoded separators", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const { projectId, dependencies } = yield* clientLaunchHarness({
      runtimeModeCeiling: "auto",
      linked: true,
      launched,
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    yield* toolkit
      .handle("t3_thread_launch", {
        title: "Fix",
        projectId,
        message: "Fix the bug",
        clientRequestId: `fwd:${"ab".repeat(32)}`,
      })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
    expect(launched).toHaveLength(1);
    expect(launched[0]?.commandId).toMatch(/^mcp:launch:[0-9a-f]{64}$/);
    expect(launched[0]?.commandId).not.toContain("%");
    expect(launched[0]?.commandId.length).toBeLessThan(80);
    expect(launched[0]?.threadId).toBe(launched[0]?.commandId);
  }),
);

it.effect(
  "a linked keyed launch accepted under the earlier id is replayed under it, not duplicated",
  () =>
    Effect.gen(function* () {
      const legacyId = ProjectHandlers.legacyLinkedLaunchCommandId("client:session-1", "fix-1");
      const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
      const { projectId, dependencies } = yield* clientLaunchHarness({
        runtimeModeCeiling: "auto",
        linked: true,
        acceptedCommandIds: new Set([legacyId]),
        launched,
      });
      const toolkit = yield* ProjectToolkit.pipe(
        Effect.provide(
          McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
            Layer.provide(dependencies),
          ),
        ),
      );
      const launch = (clientRequestId: string) =>
        toolkit
          .handle("t3_thread_launch", {
            title: "Fix",
            projectId,
            message: "Fix the bug",
            clientRequestId,
          })
          .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
      // The retried key was accepted before the upgrade: the same ids reach the launch service.
      yield* launch("fix-1");
      expect(launched[0]?.commandId).toBe(legacyId);
      expect(launched[0]?.threadId).toBe(legacyId);
      expect(launched[0]?.initialMessage?.messageId).toBe(legacyId);
      // A key with no earlier acceptance takes the hashed id.
      yield* launch("fix-2");
      expect(launched[1]?.commandId).toBe(
        "mcp:launch:cd0220b9ca42aaa97fde52795c9105489747f3786347088272dc8b23e173456d",
      );
    }),
);

it.effect("a linked keyed launch stops when the earlier-id receipt cannot be read", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const { projectId, dependencies } = yield* clientLaunchHarness({
      runtimeModeCeiling: "auto",
      linked: true,
      receiptReadFails: true,
      launched,
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const result = yield* toolkit
      .handle("t3_thread_launch", {
        title: "Fix",
        projectId,
        message: "Fix the bug",
        clientRequestId: "fix-1",
      })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
    // Guessing an id could create a second thread, so nothing launches.
    expect(result.at(-1)?.result).toMatchObject({ code: "orchestration_error" });
    expect(launched).toHaveLength(0);
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

it.effect("an update the project refuses tells the agent why", () =>
  Effect.gen(function* () {
    const harness = yield* clientLaunchHarness({ runtimeModeCeiling: "full-access", launched: [] });
    const detail = "Project monograms must contain at most two characters.";
    const dependencies = Layer.merge(
      harness.dependencies,
      Layer.mock(Project.ProjectService)({
        update: (input) =>
          Effect.fail(new Project.ProjectInvalidError({ projectId: input.projectId, detail })),
      }),
    );
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );

    const refused = yield* toolkit
      .handle("t3_project_update", {
        projectId: harness.projectId,
        projectIcon: { kind: "monogram", text: "ABC", color: "blue" },
      })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    expect(refused.at(-1)?.result).toMatchObject({ code: "invalid_request", message: detail });

    // A project started from its title has no folder yet for tracked folders to be checked against.
    const dropped = yield* toolkit
      .handle("t3_project_create", {
        title: "Wrapper",
        trackedFolders: [{ path: "/repos/api", name: "api" }],
      })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
    expect(dropped.at(-1)?.result).toMatchObject({ code: "invalid_request" });
  }),
);
