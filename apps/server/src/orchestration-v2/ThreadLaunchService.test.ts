import * as Scheduler from "../scheduling/Scheduler.ts";
import * as NodeBuffer from "node:buffer";
import * as WorktreeSetupTracker from "../project/WorktreeSetupTracker.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";
import * as ServerConfig from "../config.ts";
import { DEFAULT_SIGNAL_EXPORT } from "@t3tools/shared/observability";
import * as OtelEnvironment from "@t3tools/shared/otelEnvironment";
import { createPendingAttachmentId, resolveAttachmentPath } from "../attachmentStore.ts";
import * as ThreadMessageIntake from "./ThreadMessageIntake.ts";
import { assert, it, vi } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  ChatAttachmentId,
  AuthSessionId,
  ComposerContextId,
  type ChatAttachment,
  CommandId,
  DEFAULT_SERVER_SETTINGS,
  GitCommandError,
  MessageId,
  EnvironmentAuthenticatedPrincipal,
  EventId,
  NativeCreationHistoricalBinding,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  OrchestrationV2ThreadProjectionJson,
  ScheduledTaskId,
  type ServerProvider,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as GitWorkflow from "../git/GitWorkflowService.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { AuthSessionRepository, make as makeAuthSessions } from "../persistence/AuthSessions.ts";
import { layer as nativeRepositoryLayer } from "../persistence/Layers/NativeCreationRepository.ts";
import { NativeCreationRepository } from "../persistence/Services/NativeCreationRepository.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import { makeProviderRegistryLayer } from "../provider/testUtils/providerRegistryMock.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "./IdAllocator.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ThreadLaunch from "./ThreadLaunchService.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as ThreadTitleRegeneration from "./ThreadTitleRegenerationService.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";
import {
  NativeCreationAuthority,
  NativeCreationAuthorityError,
  NativeCreationAuthorityLive,
  NativeCreationBindingResolver,
  NativeCreationGrantResolver,
  type NativeCreationGrant,
} from "./NativeCreationAuthority.ts";
import {
  NativePreparationBinding,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  nativePreparationCommand,
  validateNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";
import { nativeWorktreePath } from "../vcs/worktreePath.ts";

const projectId = ProjectId.make("project:launch-test");
const otherProjectId = ProjectId.make("project:launch-other");
const encodeThreadProjection = Schema.encodeEffect(OrchestrationV2ThreadProjectionJson);
const encodeUnknownJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.1-codex",
} as const;
const project = {
  id: projectId,
  title: "Project",
  workspaceRoot: "/repo",
  repositoryIdentity: null,
  faviconPath: null,
  defaultModelSelection: modelSelection,
  defaultThreadEnvMode: null,
  scripts: [],
  createdAt: "2026-06-20T00:00:00.000Z",
  updatedAt: "2026-06-20T00:00:00.000Z",
  deletedAt: null,
} as const;

const otherProject = {
  ...project,
  id: otherProjectId,
  title: "Other",
} as const;

const adapter = {
  instanceId: modelSelection.instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("provider execution is disabled in launch tests"),
} as ProviderAdapterV2Shape;

interface HarnessOptions {
  readonly nativeAuthority?: Layer.Layer<NativeCreationAuthority>;
  readonly setupTracker?: Layer.Layer<WorktreeSetupTracker.WorktreeSetupTracker>;
  readonly managedFolders?: Layer.Layer<ManagedProjectFolders.ManagedProjectFolders>;
  readonly createWorktree?: GitWorkflow.GitWorkflowService["Service"]["createWorktree"];
  readonly fetchRemote?: GitWorkflow.GitWorkflowService["Service"]["fetchRemote"];
  readonly hasCommit?: GitWorkflow.GitWorkflowService["Service"]["hasCommit"];
  readonly renameBranch?: GitWorkflow.GitWorkflowService["Service"]["renameBranch"];
  readonly runSetup?: ProjectSetupScriptRunner.ProjectSetupScriptRunner["Service"]["runForThread"];
  readonly captureOwnedTargets?: TerminalManager.TerminalManager["Service"]["captureOwnedTargets"];
  readonly generateTitle?: TextGeneration.TextGeneration["Service"]["generateThreadTitle"];
  readonly generateBranchName?: TextGeneration.TextGeneration["Service"]["generateBranchName"];
  readonly serverSettings?: Parameters<typeof ServerSettings.layerTest>[0];
  readonly providers?: ReadonlyArray<ServerProvider>;
}

const syntheticServerConfig = (worktreesDir: string) =>
  Layer.effect(
    ServerConfig.ServerConfig,
    Effect.gen(function* () {
      const baseDir = "/synthetic/thread-launch";
      return ServerConfig.make({
        ...(yield* ServerConfig.deriveServerPaths(baseDir, undefined)),
        worktreesDir,
        logLevel: "Error",
        traceMinLevel: "Info",
        traceTimingEnabled: true,
        traceBatchWindowMs: 200,
        traceMaxBytes: 10 * 1024 * 1024,
        traceMaxFiles: 10,
        otlpTracesUrl: undefined,
        otlpMetricsUrl: undefined,
        otlpLogsUrl: undefined,
        otlpTracesExport: DEFAULT_SIGNAL_EXPORT,
        otlpMetricsExport: DEFAULT_SIGNAL_EXPORT,
        otlpLogsExport: DEFAULT_SIGNAL_EXPORT,
        otelEnvironment: OtelEnvironment.none,
        cwd: project.workspaceRoot,
        baseDir,
        mode: "web",
        autoBootstrapProjectFromCwd: false,
        logWebSocketEvents: false,
        tailscaleServeEnabled: false,
        tailscaleServePort: 443,
        port: 0,
        host: undefined,
        desktopBootstrapToken: undefined,
        staticDir: undefined,
        devUrl: undefined,
        devAllowedOrigins: [],
        noBrowser: false,
        startupPresentation: "browser",
      });
    }),
  ).pipe(Layer.provide(Path.layer));

function makeHarness(options: HarnessOptions = {}) {
  const database = SqlitePersistenceMemory;
  const projectStore = ProjectStore.layer.pipe(Layer.provide(database));
  const seededProjects = Layer.effectDiscard(
    Effect.gen(function* () {
      const projects = yield* ProjectStore.ProjectStoreV2;
      const now = DateTime.formatIso(yield* DateTime.now);
      for (const fixture of [project, otherProject]) {
        yield* projects.apply({
          sequence: 0,
          eventId: EventId.make(`created:${fixture.id}`),
          aggregateKind: "project",
          aggregateId: fixture.id,
          occurredAt: now,
          commandId: null,
          causationEventId: null,
          correlationId: null,
          metadata: {},
          type: "project.created",
          payload: {
            projectId: fixture.id,
            title: fixture.title,
            workspaceRoot: fixture.workspaceRoot,
            defaultModelSelection: fixture.defaultModelSelection,
            scripts: fixture.scripts,
            createdAt: now,
            updatedAt: now,
          },
        });
      }
    }),
  ).pipe(Layer.provide(projectStore));
  const nativeRepository = nativeRepositoryLayer.pipe(Layer.provide(database));
  const nativeAuthority =
    options.nativeAuthority ??
    Layer.mock(NativeCreationAuthority)({
      authorize: () => Effect.die("Ordinary launch must not invoke native authority"),
      isAutomationEnrolled: () => Effect.die("Ordinary launch must not inspect native enrollment"),
      issueExecution: () =>
        Effect.die("The disabled launch worker must not issue native execution"),
      authorizeExecution: () => Effect.die("Ordinary launch must not authorize native execution"),
    });
  const registry = ProviderAdapterRegistry.makeLayer([adapter]);
  const orchestrator = makeOrchestratorV2ReplayLayerWithRegistry(
    { name: "thread-launch" },
    registry,
    { databaseLayer: database, runEffectWorker: false },
  ).pipe(
    Layer.provide(Layer.mergeAll(database, nativeRepository, nativeAuthority, seededProjects)),
  );
  const threadManagement = ThreadManagement.layer.pipe(Layer.provide(orchestrator));
  const receipts = CommandReceiptStore.layer.pipe(Layer.provide(database));
  const fixtureBranches = new Map<string, string>();
  const createWorktree = vi.fn(
    (..._args: Parameters<GitWorkflow.GitWorkflowService["Service"]["createWorktree"]>): void => {},
  );
  const performCreateWorktree: GitWorkflow.GitWorkflowService["Service"]["createWorktree"] =
    options.createWorktree ??
    ((input) =>
      Effect.succeed({
        worktree: {
          path: input.path ?? "/repo-worktrees/feature",
          refName: input.newRefName ?? input.refName,
          headSha: "abc",
        },
      }));
  const executeCreateWorktree: GitWorkflow.GitWorkflowService["Service"]["createWorktree"] = (
    input,
    executionOptions,
  ) => {
    createWorktree(input, executionOptions);
    return (executionOptions?.revalidateMutation ?? Effect.void).pipe(
      Effect.andThen(performCreateWorktree(input, executionOptions)),
      Effect.tap((result) =>
        Effect.sync(() => {
          fixtureBranches.set(result.worktree.path, result.worktree.refName);
        }),
      ),
    );
  };
  const renameBranch = vi.fn(
    (input: Parameters<GitWorkflow.GitWorkflowService["Service"]["renameBranch"]>[0]) =>
      (options.renameBranch ?? ((request) => Effect.succeed({ branch: request.newBranch })))(
        input,
      ).pipe(
        Effect.tap((result) =>
          Effect.sync(() => {
            fixtureBranches.set(input.cwd, result.branch);
          }),
        ),
      ),
  );
  const runSetup = vi.fn(
    options.runSetup ?? (() => Effect.succeed({ status: "no-script" as const })),
  );
  const generateBranchName = vi.fn(
    options.generateBranchName ?? (() => Effect.succeed({ branch: "generated-branch" })),
  );
  const generateThreadTitle = vi.fn(
    options.generateTitle ?? (() => Effect.succeed({ title: "Generated title" })),
  );
  const externalServices = Layer.mergeAll(
    syntheticServerConfig("/repo-worktrees"),
    options.setupTracker ?? WorktreeSetupTracker.layer,
    Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
    Layer.mock(TerminalManager.TerminalManager)({
      close: () => Effect.void,
      // Fixture setup runners never open a managed terminal, so the readback is empty.
      captureOwnedTargets:
        options.captureOwnedTargets ??
        ((input) =>
          Effect.succeed({
            managerId: "thread-launch-test-terminals",
            threadId: input.threadId,
            ownerBirth: input.ownerBirth,
            status: "captured" as const,
            managedTargetsOnly: true as const,
            targets: [],
          })),
    }),
    Layer.succeed(ProjectService.ProjectService, {
      create: () => Effect.die("unused"),
      bootstrap: () => Effect.die("unused"),
      update: () => Effect.die("unused"),
      delete: () => Effect.die("unused"),
      getById: (id) =>
        Effect.succeed(
          id === projectId
            ? Option.some(project)
            : id === otherProjectId
              ? Option.some(otherProject)
              : Option.none(),
        ),
      getByWorkspaceRoot: () => Effect.succeed(Option.some(project)),
      snapshot: Effect.die("unused"),
      getShell: () => Effect.die("unused"),
      listShells: () => Effect.die("unused"),
    }),
    Layer.mock(GitWorkflow.GitWorkflowService)({
      createWorktree: executeCreateWorktree,
      renameBranch,
      isRepository: () => Effect.succeed(true),
      hasCommit: options.hasCommit ?? (() => Effect.succeed(true)),
      invalidateLocalStatus: () => Effect.void,
      localStatus: ({ cwd }) =>
        Effect.succeed({
          isRepo: fixtureBranches.has(cwd),
          refName: fixtureBranches.get(cwd) ?? null,
        } as never),
      fetchRemote: options.fetchRemote ?? (() => Effect.void),
      remoteExists: () => Effect.succeed(true),
      remoteBranchExists: () => Effect.succeed(true),
      removeWorktree: () => Effect.void,
      resolveRemoteTrackingCommit: () =>
        Effect.succeed({ commitSha: "remote-main-sha", remoteRefName: "origin/main" }),
    }),
    Layer.succeed(ProjectSetupScriptRunner.ProjectSetupScriptRunner, {
      runForThread: runSetup,
    }),
    Layer.mock(TextGeneration.TextGeneration)({
      generateThreadTitle,
      generateBranchName,
    }),
    ServerSettings.layerTest(options.serverSettings),
    makeProviderRegistryLayer(options.providers),
    options.managedFolders ??
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/projects",
        folderForThread: () => Effect.succeed(Option.none()),
      }),
  );
  const launch = ThreadLaunch.layer.pipe(
    Layer.provide(
      Layer.mergeAll(receipts, IdAllocator.layer, orchestrator, threadManagement, externalServices),
    ),
  );
  const titleRegeneration = ThreadTitleRegeneration.layer.pipe(
    Layer.provide(Layer.mergeAll(threadManagement, projectStore, externalServices)),
  );
  return {
    layer: Layer.mergeAll(
      launch,
      titleRegeneration,
      orchestrator,
      threadManagement,
      receipts,
      database,
      nativeRepository,
      nativeAuthority,
      projectStore,
      externalServices,
    ),
    createWorktree,
    renameBranch,
    generateBranchName,
    generateThreadTitle,
    runSetup,
  };
}

function makeNativeLaunchHarness(
  unknownWorktreeOutcome = false,
  runSetup?: ProjectSetupScriptRunner.ProjectSetupScriptRunner["Service"]["runForThread"],
  setupTracker?: Layer.Layer<WorktreeSetupTracker.WorktreeSetupTracker>,
) {
  const worktreesDir = "/synthetic/native-worktrees";
  const createdPaths = new Set<string>();
  const qualified = new Map<
    string,
    {
      readonly grant: NativeCreationGrant;
      readonly binding: typeof NativeCreationHistoricalBinding.Type;
    }
  >();
  const unavailable = () =>
    new NativeCreationAuthorityError({
      code: "unsupported_authority",
      message: "This synthetic launch has no explicit qualified fixture",
    });
  const nativeAuthority = NativeCreationAuthorityLive.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.effect(AuthSessionRepository, makeAuthSessions).pipe(
          Layer.provide(SqlitePersistenceMemory),
        ),
        nativeRepositoryLayer.pipe(Layer.provide(SqlitePersistenceMemory)),
        Layer.succeed(NativeCreationGrantResolver, {
          resolveCurrent: ({ actorSessionId }) => {
            const current = qualified.get(actorSessionId);
            return current === undefined
              ? Effect.fail(unavailable())
              : Effect.succeed({
                  enrolledSessionId: current.grant.actorSessionId,
                  trustedIssuerId: current.grant.issuerId,
                  grant: current.grant,
                });
          },
        }),
        Layer.succeed(NativeCreationBindingResolver, {
          resolveCurrent: (preparation) => {
            const current = [...qualified.values()].find(
              (entry) => entry.grant.preparationId === preparation.preparationId,
            );
            return current === undefined
              ? Effect.fail(unavailable())
              : Effect.succeed(current.binding);
          },
        }),
      ),
    ),
    Layer.orDie,
  );
  const harness = makeHarness({
    nativeAuthority,
    ...(setupTracker === undefined ? {} : { setupTracker }),
    createWorktree: (input) =>
      Effect.gen(function* () {
        if (input.path === null || input.path === undefined || input.newRefName === undefined)
          return yield* Effect.die("Native worktree must bind its exact path and branch");
        createdPaths.add(input.path);
        if (unknownWorktreeOutcome)
          return yield* new GitCommandError({
            operation: "GitVcsDriver.createWorktree",
            command: "git",
            cwd: input.cwd,
            detail: "Synthetic response lost after checkout allocation",
          });
        return { worktree: { path: input.path, refName: input.newRefName } };
      }),
    runSetup:
      runSetup ??
      (() =>
        Effect.succeed({
          status: "started",
          scriptId: "synthetic-setup",
          scriptName: "Synthetic setup",
          scriptCommand: "synthetic-setup-command",
          terminalId: "synthetic-setup-terminal",
          cwd: "/synthetic/native-worktrees",
          async: false,
          completion: Effect.succeed({ exitCode: 0, durationMs: 1 }),
        })),
  });
  return {
    ...harness,
    worktreesDir,
    qualify: (grant: NativeCreationGrant, binding: typeof NativeCreationHistoricalBinding.Type) => {
      qualified.set(grant.actorSessionId, { grant, binding });
    },
    layer: Layer.mergeAll(
      harness.layer,
      syntheticServerConfig(worktreesDir),
      FileSystem.layerNoop({
        exists: (path) => Effect.succeed(createdPaths.has(path)),
        realPath: (path) => Effect.succeed(path),
      }),
    ),
  };
}

const nativeLaunchFixture = Effect.fn("ThreadLaunch.nativeFixture")(function* (
  worktreesDir: string,
  name: string,
  qualify: (
    grant: NativeCreationGrant,
    binding: typeof NativeCreationHistoricalBinding.Type,
  ) => void,
) {
  const launches = yield* ThreadLaunch.ThreadLaunchService;
  const repository = yield* NativeCreationRepository;
  const sql = yield* SqlClient.SqlClient;
  const sink = yield* EventSink.EventSinkV2;
  const sessions = yield* makeAuthSessions;
  const now = yield* DateTime.now;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const timestamp = DateTime.formatIso(now);
  yield* projects.apply({
    sequence: 0,
    eventId: EventId.make(`native-launch-project:${name}`),
    aggregateKind: "project",
    aggregateId: projectId,
    occurredAt: timestamp,
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
    type: "project.created",
    payload: {
      projectId,
      title: project.title,
      workspaceRoot: project.workspaceRoot,
      defaultModelSelection: project.defaultModelSelection,
      scripts: project.scripts,
      createdAt: timestamp,
      updatedAt: timestamp,
    },
  });
  const sessionId = AuthSessionId.make(`native-launch-session:${name}`);
  const principal: EnvironmentAuthenticatedPrincipal["Service"] = {
    sessionId,
    subject: `native-launch:${name}`,
    method: "bearer-access-token",
    scopes: new Set(["orchestration:read", "orchestration:operate"]),
  };
  yield* sessions.create({
    sessionId,
    subject: principal.subject,
    method: principal.method,
    scopes: [...principal.scopes],
    issuedAt: now,
    expiresAt: DateTime.add(now, { days: 1 }),
    client: {
      label: null,
      ipAddress: null,
      userAgent: null,
      deviceType: "bot",
      os: null,
      browser: null,
    },
  });
  yield* sql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at) VALUES (${sessionId}, ${DateTime.formatIso(now)})`;
  const binding = yield* Schema.decodeEffect(NativePreparationBinding)({
    backend_instance: "synthetic-backend",
    environment_id: "synthetic-environment",
    project_id: projectId,
    project_cwd: project.workspaceRoot,
    account_ref: "synthetic-account",
    runtime_mode: "full-access",
    interaction_mode: "default",
    base_branch: "main",
    start_from_origin: false,
    run_setup_script: true,
    provider_model_selection: modelSelection,
  });
  const preparedCommand = nativePreparationCommand(
    `native-launch:${name}`,
    binding,
    "Exact native prompt",
    "Canonical native title",
    DateTime.formatIso(now),
  );
  const command = {
    ...preparedCommand,
    commandId: CommandId.make(preparedCommand.commandId),
    threadId: ThreadId.make(preparedCommand.threadId),
  };
  const canonicalText = nativeCreationCanonicalJson({
    schema: "voice.t3-bootstrap-preparation/v1",
    operation_id: `native-launch:${name}`,
    preparation_id: command.commandId.replace("voice-command-", "voice-bootstrap-"),
    binding,
    command,
    binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
    prompt_digest: nativeCreationSha256(command.message.text),
    command_digest: nativeCreationSha256(nativeCreationCanonicalJson(command)),
  });
  const preparation = yield* validateNativeCreationPreparation(
    new TextEncoder().encode(canonicalText),
  );
  const historical = yield* Schema.decodeEffect(NativeCreationHistoricalBinding)({
    backendInstance: binding.backend_instance,
    environmentId: binding.environment_id,
    projectId: binding.project_id,
    projectCwd: binding.project_cwd,
    accountRef: binding.account_ref,
    accountBindingId: "synthetic-qualified-account",
    accountBindingRevision: 1,
    providerModelSelection: binding.provider_model_selection,
    runtimeMode: binding.runtime_mode,
    interactionMode: binding.interaction_mode,
    baseBranch: binding.base_branch,
    startFromOrigin: false,
    runSetupScript: true,
    requestedBranch: command.bootstrap.prepareWorktree.branch,
  });
  const resources = {
    projectCwd: binding.project_cwd,
    branch: historical.requestedBranch,
    worktreePath: nativeWorktreePath({
      worktreesDir,
      cwd: binding.project_cwd,
      branch: historical.requestedBranch,
    }),
  };
  const guard = {
    schema: "t3.native-creation-guard/v1" as const,
    grantId: `synthetic-grant:${name}`,
    grantRevision: 1,
  };
  const grant: NativeCreationGrant = {
    grantId: guard.grantId,
    revision: 1,
    actorSessionId: sessionId,
    issuerId: "synthetic-issuer",
    expiresAt: DateTime.add(now, { days: 1 }),
    revoked: false,
    operationId: preparation.operationId,
    preparationId: preparation.preparationId,
    preparationSha256: preparation.preparationSha256,
    bindingDigest: preparation.bindingDigest,
    binding: historical,
    resources,
    allowedStages: [
      "claim",
      "normalization",
      "tracker_registration",
      "bootstrap_detachment",
      "worktree_ownership",
      "fetch",
      "worktree",
      "setup",
      "setup_detachment",
      "setup_completion_detachment",
      "native_command",
    ],
    recoveryScopes: [],
  };
  qualify(grant, historical);
  const authority = yield* NativeCreationAuthority;
  const submission = {
    schema: "t3.native-bootstrap-submission/v1" as const,
    preparationBase64: NodeBuffer.Buffer.from(canonicalText).toString("base64"),
    creationGuard: guard,
  };
  const dispatch = (currentAuthority = authority) =>
    launches
      .dispatchNativeBootstrap(submission, { nativeCreationBootId: "synthetic-original-boot" })
      .pipe(
        Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
        Effect.provideService(NativeCreationAuthority, currentAuthority),
      );
  const enableRecovery = Effect.gen(function* () {
    const facts = yield* sink.readNativeCommandFacts({
      threadId: command.threadId,
      commandId: CommandId.make(`${command.commandId}:native:v2:create`),
    });
    assert.equal(facts.creationProvenance, "native_created");
    assert.isNotNull(facts.incarnation);
    qualify(
      {
        ...grant,
        allowedStages: [...grant.allowedStages, "cleanup"],
        recoveryScopes: [
          {
            scopeId: `${preparation.operationId}:cleanup:thread`,
            resource: {
              kind: "thread",
              threadId: command.threadId,
              incarnation: facts.incarnation!,
            },
          },
        ],
      },
      historical,
    );
  });
  return { dispatch, command, preparation, repository, resources, enableRecovery, authority, sink };
});

it.effect(
  "accepts the three native stages after durable preparation and keeps external execution separate",
  () => {
    const harness = makeNativeLaunchHarness();
    return Effect.gen(function* () {
      const fixture = yield* nativeLaunchFixture(harness.worktreesDir, "positive", harness.qualify);
      const sql = yield* SqlClient.SqlClient;
      const result = yield* fixture.dispatch();
      assert.equal(result.commandAcceptance, "accepted");
      assert.equal(result.commandId, fixture.command.commandId);
      assert.equal(result.threadId, fixture.command.threadId);
      assert.equal(result.messageId, fixture.command.message.messageId);
      assert.isNotNull(result.creation);
      const creation = result.creation!;
      assert.deepEqual(
        creation.stageCommands.map((stage) => [stage.commandId, stage.commandType]),
        [
          [`${fixture.command.commandId}:native:v2:create`, "thread.create"],
          [`${fixture.command.commandId}:native:v2:message`, "message.dispatch"],
          [fixture.command.commandId, "prepared-run.release"],
        ],
      );
      assert.isTrue(
        creation.stageCommands.every(
          (stage) =>
            stage.receipt?.status === "accepted" &&
            stage.event?.sequence === stage.receipt.resultSequence,
        ),
      );
      assert.deepEqual(creation.incarnation, creation.stageCommands[0]!.event);
      assert.equal(creation.commandDigest, fixture.preparation.commandDigest);
      assert.notEqual(creation.normalizedCommandDigest, fixture.preparation.commandDigest);
      assert.equal(creation.outcome, "in_progress");
      assert.equal(creation.effectsV2.length, 0);
      assert.isTrue(
        creation.effectsV1.some(
          (fact) =>
            fact.kind === "setup" &&
            fact.phase === "completed" &&
            fact.result === "succeeded" &&
            fact.exitCode === 0,
        ),
      );
      assert.isTrue(
        creation.effectsV1.some(
          (fact) =>
            fact.kind === "lifecycle" &&
            fact.action === "worktree_ownership" &&
            fact.phase === "completed" &&
            fact.result === "succeeded",
        ),
      );
      const history = yield* fixture.repository.readHistoryByClaim(creation.claimId);
      assert.equal(history.intent.canonicalPreparation, fixture.preparation.canonicalText);
      assert.equal(history.intent.claimedBootId, "synthetic-original-boot");
      for (const stage of creation.stageCommands) {
        const reserved = yield* fixture.repository.getReservedCommand(stage.commandId);
        assert.isTrue(Option.isSome(reserved));
        if (Option.isSome(reserved))
          assert.equal(reserved.value.commandDigest, stage.commandDigest);
      }
      const outbox = yield* sql<{
        command_id: string;
        effect_type: string;
        payload_json: string;
      }>`SELECT command_id, effect_type, payload_json FROM orchestration_v2_effect_outbox`;
      assert.equal(outbox.length, 1);
      assert.equal(outbox[0]!.command_id, fixture.command.commandId);
      assert.equal(outbox[0]!.effect_type, "provider-turn.start");
      const payload = yield* Schema.decodeEffect(
        Schema.fromJsonString(EffectOutbox.NativeOrchestrationEffectPayloadV2),
      )(outbox[0]!.payload_json);
      assert.equal(
        payload.nativeCreationExecutionReference.stageCommandId,
        fixture.command.commandId,
      );
      assert.equal(payload.nativeCreationExecutionReference.claimId, creation.claimId);
      assert.deepEqual(yield* fixture.dispatch(), result);
      assert.equal(harness.createWorktree.mock.calls.length, 1);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      assert.equal(harness.generateThreadTitle.mock.calls.length, 0);
      assert.equal(harness.generateBranchName.mock.calls.length, 0);
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect(
  "holds a native retry after an unknown worktree outcome without reallocating or releasing",
  () => {
    const harness = makeNativeLaunchHarness(true);
    return Effect.gen(function* () {
      const fixture = yield* nativeLaunchFixture(harness.worktreesDir, "unknown", harness.qualify);
      const sql = yield* SqlClient.SqlClient;
      const first = yield* fixture.dispatch().pipe(Effect.flip);
      assert.equal(
        first._tag === "NativeCreationAuthorityError" ? first.code : undefined,
        "unresolved_claim",
      );
      const again = yield* fixture.dispatch().pipe(Effect.flip);
      assert.equal(
        again._tag === "NativeCreationAuthorityError" ? again.code : undefined,
        "unresolved_claim",
      );
      assert.equal(harness.createWorktree.mock.calls.length, 1);
      assert.equal(harness.runSetup.mock.calls.length, 0);
      const history = yield* fixture.repository.readHistory(fixture.command.commandId);
      assert.isTrue(Option.isSome(history));
      if (Option.isSome(history))
        assert.isTrue(
          history.value.effects.some(
            (fact) =>
              fact.kind === "worktree" && fact.phase === "completed" && fact.result === "unknown",
          ),
        );
      const threads = yield* sql<{
        count: number;
      }>`SELECT COUNT(*) AS count FROM orchestration_v2_projection_threads WHERE thread_id = ${fixture.command.threadId}`;
      const released = yield* sql<{
        count: number;
      }>`SELECT COUNT(*) AS count FROM orchestration_command_receipts WHERE command_id = ${fixture.command.commandId}`;
      const effects = yield* sql<{
        count: number;
      }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox`;
      assert.equal(threads[0]?.count, 1);
      assert.equal(released[0]?.count, 0);
      assert.equal(effects[0]?.count, 0);
      assert.isNull(
        yield* fixture.repository.readThreadRecoveryCommand(
          `${fixture.command.commandId}:bootstrap-thread-delete`,
        ),
      );
      if (Option.isSome(history))
        assert.isFalse(history.value.effects.some((fact) => fact.kind === "cleanup"));
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect(
  "native known setup failure atomically deletes its exact V2 birth and observes completed recovery on retry",
  () => {
    let enableRecovery = Effect.void;
    const harness = makeNativeLaunchHarness(false, () =>
      Effect.succeed({
        status: "started",
        scriptId: "synthetic-setup",
        scriptName: "Synthetic setup",
        scriptCommand: "synthetic-setup-command",
        terminalId: "synthetic-setup-terminal",
        cwd: "/synthetic/native-worktrees",
        async: false,
        completion: Effect.suspend(() => enableRecovery).pipe(
          Effect.as({ exitCode: 1, durationMs: 1 }),
        ),
      }),
    );
    return Effect.gen(function* () {
      const fixture = yield* nativeLaunchFixture(
        harness.worktreesDir,
        "known-failure",
        harness.qualify,
      );
      enableRecovery = fixture.enableRecovery.pipe(Effect.orDie);
      const failed = yield* fixture.dispatch().pipe(Effect.flip);
      assert.equal(
        failed._tag === "NativeCreationAuthorityError" ? failed.message : undefined,
        "Native setup did not complete successfully.",
      );
      const deleteId = CommandId.make(`${fixture.command.commandId}:bootstrap-thread-delete`);
      const companion = yield* fixture.repository.readThreadRecoveryCommand(deleteId);
      assert.isNotNull(companion);
      const facts = yield* fixture.sink.readNativeCommandFacts({
        threadId: fixture.command.threadId,
        commandId: deleteId,
      });
      assert.equal(facts.receipt?.status, "accepted");
      const deleted = facts.events.filter((stored) => stored.event.type === "thread.deleted");
      assert.lengthOf(deleted, 1);
      assert.equal(deleted[0]!.event.threadId, fixture.command.threadId);
      const history = yield* fixture.repository.readHistoryByClaim(
        `native:v2:${fixture.command.commandId}`,
      );
      const completion = history.effects.find(
        (fact) => fact.effectId === companion?.commandStartEffectId && fact.phase === "completed",
      );
      assert.isDefined(completion);
      if (completion?.kind === "native_command" && completion.phase === "completed") {
        assert.equal(completion.sequence, facts.receipt?.resultSequence);
        assert.isTrue(
          facts.eventMetadata.some(
            (event) =>
              event.eventId === completion.eventId && event.sequence === completion.sequence,
          ),
        );
      }
      assert.isFalse(
        history.effects.some((fact) => fact.kind === "cleanup" && fact.phase === "completed"),
      );
      const retry = yield* fixture.dispatch().pipe(Effect.flip);
      assert.equal(
        retry._tag === "NativeCreationAuthorityError" ? retry.code : undefined,
        "unresolved_claim",
      );
      const after = yield* fixture.sink.readNativeCommandFacts({
        threadId: fixture.command.threadId,
        commandId: deleteId,
      });
      assert.deepEqual(after.receipt, facts.receipt);
      assert.deepEqual(after.events, facts.events);
      assert.equal(harness.createWorktree.mock.calls.length, 1);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      assert.lengthOf(
        (yield* fixture.repository.readBoundedHistoryByThread(fixture.command.threadId))
          ?.stageCommands ?? [],
        3,
      );
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect(
  "native failure preserves its thread when current recovery authority rejects cleanup",
  () => {
    const harness = makeNativeLaunchHarness(false, () =>
      Effect.succeed({
        status: "started",
        scriptId: "synthetic-setup",
        scriptName: "Synthetic setup",
        scriptCommand: "synthetic-setup-command",
        terminalId: "synthetic-setup-terminal",
        cwd: "/synthetic/native-worktrees",
        async: false,
        completion: Effect.succeed({ exitCode: 1, durationMs: 1 }),
      }),
    );
    return Effect.gen(function* () {
      const fixture = yield* nativeLaunchFixture(
        harness.worktreesDir,
        "denied-recovery",
        harness.qualify,
      );
      const failed = yield* fixture.dispatch().pipe(Effect.flip);
      assert.equal(
        failed._tag === "NativeCreationAuthorityError" ? failed.message : undefined,
        "Native setup did not complete successfully.",
      );
      const facts = yield* fixture.sink.readNativeCommandFacts({
        threadId: fixture.command.threadId,
        commandId: CommandId.make(`${fixture.command.commandId}:bootstrap-thread-delete`),
      });
      assert.isNull(facts.receipt);
      assert.isNull(facts.projection?.thread.deletedAt);
      assert.isNull(
        yield* fixture.repository.readThreadRecoveryCommand(
          `${fixture.command.commandId}:bootstrap-thread-delete`,
        ),
      );
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect("native recovery preserves a replacement birth instead of deleting by thread ID", () => {
  let replaceBirth = Effect.void;
  const harness = makeNativeLaunchHarness(false, () =>
    Effect.succeed({
      status: "started",
      scriptId: "synthetic-setup",
      scriptName: "Synthetic setup",
      scriptCommand: "synthetic-setup-command",
      terminalId: "synthetic-setup-terminal",
      cwd: "/synthetic/native-worktrees",
      async: false,
      completion: Effect.suspend(() => replaceBirth).pipe(
        Effect.as({ exitCode: 1, durationMs: 1 }),
      ),
    }),
  );
  return Effect.gen(function* () {
    const fixture = yield* nativeLaunchFixture(
      harness.worktreesDir,
      "replacement-birth",
      harness.qualify,
    );
    replaceBirth = Effect.gen(function* () {
      yield* fixture.enableRecovery;
      const facts = yield* fixture.sink.readNativeCommandFacts({
        threadId: fixture.command.threadId,
        commandId: CommandId.make(`${fixture.command.commandId}:native:v2:create`),
      });
      const create = facts.events.find((stored) => stored.event.type === "thread.created");
      assert.isDefined(create);
      if (create?.event.type !== "thread.created")
        return yield* Effect.die("Missing original V2 birth");
      yield* fixture.sink.write({
        commandId: CommandId.make("replacement-birth-command"),
        events: [
          {
            ...create.event,
            id: EventId.make("replacement-birth-event"),
          },
        ],
      });
    }).pipe(Effect.orDie);
    yield* fixture.dispatch().pipe(Effect.flip);
    const facts = yield* fixture.sink.readNativeCommandFacts({
      threadId: fixture.command.threadId,
      commandId: CommandId.make(`${fixture.command.commandId}:bootstrap-thread-delete`),
    });
    assert.isNull(facts.receipt);
    assert.isNull(facts.projection?.thread.deletedAt);
    assert.isNull(
      yield* fixture.repository.readThreadRecoveryCommand(
        `${fixture.command.commandId}:bootstrap-thread-delete`,
      ),
    );
  }).pipe(Effect.provide(harness.layer));
});

it.effect("native preparation survives socket waiter interruption and releases exactly once", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const complete = yield* Deferred.make<void>();
    const harness = makeNativeLaunchHarness(false, () =>
      Deferred.succeed(entered, undefined).pipe(
        Effect.as({
          status: "started" as const,
          scriptId: "synthetic-setup",
          scriptName: "Synthetic setup",
          scriptCommand: "synthetic-setup-command",
          terminalId: "synthetic-setup-terminal",
          cwd: "/synthetic/native-worktrees",
          async: false,
          completion: Deferred.await(complete).pipe(Effect.as({ exitCode: 0, durationMs: 1 })),
        }),
      ),
    );
    yield* Effect.gen(function* () {
      const fixture = yield* nativeLaunchFixture(
        harness.worktreesDir,
        "waiter-interrupt",
        harness.qualify,
      );
      const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
      const waiter = yield* fixture.dispatch().pipe(Effect.forkScoped);
      yield* Deferred.await(entered);
      yield* Fiber.interrupt(waiter);
      assert.equal((yield* tracker.get(fixture.command.threadId))?.phase, "running");
      const retry = yield* fixture.dispatch().pipe(Effect.flip);
      assert.equal(
        retry._tag === "NativeCreationAuthorityError" ? retry.code : undefined,
        "unresolved_claim",
      );
      yield* Deferred.succeed(complete, undefined);
      yield* tracker.stream(fixture.command.threadId).pipe(
        Stream.filter((snapshot) => snapshot?.phase === "done"),
        Stream.runHead,
      );
      assert.equal((yield* fixture.dispatch()).commandAcceptance, "accepted");
      assert.equal(harness.createWorktree.mock.calls.length, 1);
      assert.equal(harness.runSetup.mock.calls.length, 1);
    }).pipe(Effect.provide(harness.layer));
  }).pipe(Effect.scoped),
);

it.effect(
  "explicit native cancellation before an external stage owns recovery through finalization",
  () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const blocked = yield* Deferred.make<void>();
      const harness = makeNativeLaunchHarness();
      yield* Effect.gen(function* () {
        const fixture = yield* nativeLaunchFixture(
          harness.worktreesDir,
          "safe-cancel",
          harness.qualify,
        );
        const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
        const authority = {
          ...fixture.authority,
          authorize: (input: Parameters<typeof fixture.authority.authorize>[0]) =>
            input.stage === "bootstrap_detachment"
              ? fixture.enableRecovery.pipe(
                  Effect.orDie,
                  Effect.andThen(Deferred.succeed(entered, undefined)),
                  Effect.andThen(Deferred.await(blocked)),
                  Effect.andThen(fixture.authority.authorize(input)),
                )
              : fixture.authority.authorize(input),
        };
        const waiter = yield* fixture.dispatch(authority).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        assert.isTrue(yield* tracker.cancel(fixture.command.threadId));
        assert.equal((yield* tracker.get(fixture.command.threadId))?.phase, "cancelled");
        assert.isTrue(Exit.isFailure(yield* Fiber.await(waiter)));
        const facts = yield* fixture.sink.readNativeCommandFacts({
          threadId: fixture.command.threadId,
          commandId: CommandId.make(`${fixture.command.commandId}:bootstrap-thread-delete`),
        });
        assert.equal(facts.receipt?.status, "accepted");
        assert.equal(harness.createWorktree.mock.calls.length, 0);
        assert.equal(harness.runSetup.mock.calls.length, 0);
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.scoped),
);

it.effect(
  "native known failure after an accepted release never deletes the accepted thread",
  () => {
    let enableRecovery = Effect.void;
    const failure = new Error("Synthetic tracker failure after accepted native release");
    const setupTracker = Layer.effect(
      WorktreeSetupTracker.WorktreeSetupTracker,
      WorktreeSetupTracker.make.pipe(
        Effect.map((tracker): WorktreeSetupTracker.WorktreeSetupTracker["Service"] => ({
          ...tracker,
          finish: (threadId, phase, error) =>
            phase === "done" ? Effect.die(failure) : tracker.finish(threadId, phase, error),
        })),
      ),
    );
    const harness = makeNativeLaunchHarness(
      false,
      () =>
        Effect.succeed({
          status: "started",
          scriptId: "synthetic-setup",
          scriptName: "Synthetic setup",
          scriptCommand: "synthetic-setup-command",
          terminalId: "synthetic-setup-terminal",
          cwd: "/synthetic/native-worktrees",
          async: false,
          completion: Effect.suspend(() => enableRecovery).pipe(
            Effect.as({ exitCode: 0, durationMs: 1 }),
          ),
        }),
      setupTracker,
    );
    return Effect.gen(function* () {
      const fixture = yield* nativeLaunchFixture(
        harness.worktreesDir,
        "released-before-failure",
        harness.qualify,
      );
      enableRecovery = fixture.enableRecovery.pipe(Effect.orDie);
      const failed = yield* fixture.dispatch().pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(failed));
      if (Exit.isFailure(failed)) assert.equal(Cause.squash(failed.cause), failure);
      const facts = yield* fixture.sink.readNativeCommandFacts({
        threadId: fixture.command.threadId,
        commandId: fixture.command.commandId,
      });
      assert.equal(facts.receipt?.status, "accepted");
      assert.isNull(facts.projection?.thread.deletedAt);
      assert.isNull(
        yield* fixture.repository.readThreadRecoveryCommand(
          `${fixture.command.commandId}:bootstrap-thread-delete`,
        ),
      );
    }).pipe(Effect.provide(harness.layer));
  },
);

function launchInput(input: {
  readonly command: string;
  readonly thread: string;
  readonly message?: string;
  readonly workspace?: ThreadLaunch.ThreadLaunchWorkspaceStrategy;
}) {
  return {
    commandId: CommandId.make(input.command),
    threadId: ThreadId.make(input.thread),
    projectId,
    title: "New thread",
    modelSelection,
    runtimeMode: "full-access" as const,
    interactionMode: "default" as const,
    workspaceStrategy: input.workspace ?? { type: "root" as const },
    ...(input.message === undefined
      ? {}
      : {
          initialMessage: {
            messageId: MessageId.make(`${input.message}:id`),
            text: input.message,
            attachments: [],
          },
        }),
    createdBy: "user" as const,
    creationSource: "web" as const,
  };
}

function waitUntil<E, R>(predicate: () => Effect.Effect<boolean, E, R>): Effect.Effect<void, E, R> {
  return Effect.gen(function* () {
    for (let attempt = 0; attempt < 200; attempt += 1) {
      if (yield* predicate()) return;
      yield* Effect.promise(
        () =>
          new Promise<void>((resolve) => {
            setImmediate(resolve);
          }),
      );
    }
    assert.fail("Condition was not reached before timeout.");
  });
}

for (const target of ["new", "existing"] as const) {
  for (const createdBy of ["user", "agent"] as const) {
    it.effect(
      `attributes ${createdBy}-configured automations in ${target} threads without changing their prompt`,
      () => {
        const harness = makeHarness();
        const scheduledTasks = ScheduledTasks.layer.pipe(
          Layer.provide(Layer.mergeAll(harness.layer, NodeCrypto.layer, Scheduler.layer)),
        );
        return Effect.gen(function* () {
          const tasks = yield* ScheduledTasks.ScheduledTaskService;
          const launches = yield* ThreadLaunch.ThreadLaunchService;
          const threads = yield* ThreadManagement.ThreadManagementService;
          const existing =
            target === "existing"
              ? yield* launches.launch(
                  launchInput({ command: "command:existing", thread: "thread:existing" }),
                )
              : null;
          const { task } = yield* tasks.upsert({
            id: ScheduledTaskId.make("scheduled-task:attribution"),
            title: "Daily audit",
            prompt: "Audit performance and crashes.",
            enabled: false,
            schedule: { type: "interval", everyMs: 60_000 },
            projectId,
            threadId: existing?.threadId ?? null,
            workspaceStrategy: { type: "root" },
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            createdBy,
            creationSource: createdBy === "agent" ? "mcp" : "web",
          });
          const result = yield* tasks.runNow({ id: task.id });
          assert.equal(result.task.lastRunStatus, "succeeded");
          const projectThreads = yield* threads.listProjectThreads({
            projectId,
            includeSubagents: false,
          });
          const thread =
            projectThreads.find((candidate) => candidate.id === existing?.threadId) ??
            projectThreads[0];
          assert.isDefined(thread);
          const projection = yield* threads.getThreadProjection(thread!.id);
          // Encoding the persisted projection exercises both message and turn-item wire schemas.
          const wire = yield* encodeThreadProjection(projection);
          assert.equal(wire.messages[0]?.text, task.prompt);
          assert.equal(wire.messages[0]?.scheduledTaskId, task.id);
          assert.equal(wire.messages[0]?.createdBy, createdBy);
          const turnItem = wire.turnItems.find((item) => item.type === "user_message");
          assert.equal(turnItem?.text, task.prompt);
          assert.equal(turnItem?.scheduledTaskId, task.id);
        }).pipe(Effect.provide(Layer.mergeAll(harness.layer, scheduledTasks)));
      },
    );
  }
}

it.effect("retains automation and sender attribution while a message waits in the queue", () => {
  const harness = makeHarness({ runSetup: () => Effect.never });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const launched = yield* launches.launch(
      launchInput({
        command: "command:automation:queue",
        thread: "thread:automation:queue",
        message: "First message",
      }),
    );
    const scheduledTaskId = ScheduledTaskId.make("scheduled-task:queued");
    const senderThreadId = ThreadId.make("thread:agent-sender");
    const queued = yield* threads.sendToThread({
      projectId,
      commandId: CommandId.make("command:automation:queued"),
      threadId: launched.threadId,
      messageId: MessageId.make("message:automation:queued"),
      scheduledTaskId,
      senderThreadId,
      text: "Run the audit",
      attachments: [],
      mode: "queue",
      createdBy: "agent",
      creationSource: "mcp",
    });
    assert.equal(queued.delivery, "queued");
    const projection = yield* threads.getThreadProjection(launched.threadId);
    const message = projection.messages.find((item) => item.id === queued.message.id);
    assert.equal(message?.scheduledTaskId, scheduledTaskId);
    assert.equal(message?.senderThreadId, senderThreadId);
    assert.equal(message?.text, "Run the audit");
  }).pipe(Effect.provide(harness.layer));
});

it.effect("binds the exact isolated target before deferred acceptance and reuses it on retry", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const allowWorktree = yield* Deferred.make<void>();
    const harness = makeHarness({
      createWorktree: (input) =>
        Effect.gen(function* () {
          if (input.path === null || input.path === undefined || input.newRefName === undefined) {
            return yield* Effect.die(
              "Provisioning must receive the accepted target path and branch",
            );
          }
          yield* Deferred.succeed(entered, undefined);
          yield* Deferred.await(allowWorktree);
          return { worktree: { path: input.path, refName: input.newRefName, headSha: "abc" } };
        }),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const input = launchInput({
        command: "launch:preplanned-target",
        thread: "thread:preplanned-target",
        message: "Prepare in isolation",
        workspace: { type: "worktree", baseRef: "main", branch: "feature" },
      });
      const expectedPath = nativeWorktreePath({
        worktreesDir: "/repo-worktrees",
        cwd: project.workspaceRoot,
        branch: "feature",
      });
      const first = yield* launches.launch(input);
      assert.equal(first.projection.thread.branch, "feature");
      assert.equal(first.projection.thread.worktreePath, expectedPath);
      assert.equal(first.projection.runs[0]?.status, "preparing");
      assert.isEmpty(first.projection.checkpointScopes);
      assert.equal(
        first.projection.nodes.find((node) => node.id === first.projection.runs[0]?.rootNodeId)
          ?.checkpointScopeId,
        null,
      );
      yield* Deferred.await(entered);
      const sink = yield* EventSink.EventSinkV2;
      const preparationUse = yield* sink.readOrdinaryCheckoutUse(
        `${input.commandId}:ordinary-preparation`,
      );
      assert.isNotNull(preparationUse);
      assert.equal(preparationUse!.state, "started");
      assert.equal(preparationUse!.subject.source.worktreePath, expectedPath);
      assert.equal(preparationUse!.subject.use.source.kind, "prepared_run");
      const executionHistory = yield* sink.readOrdinaryCheckoutExecutionAssociations(
        preparationUse!.subject.use,
      );
      assert.lengthOf(executionHistory.participants, 1);
      assert.equal(executionHistory.participants[0]!.state, "active");
      assert.equal(executionHistory.participants[0]!.ref.executor.kind, "actual_prepared_producer");
      const executor = executionHistory.participants[0]!.ref.executor;
      if (executor.kind !== "actual_prepared_producer")
        return yield* Effect.die("Missing prepared producer");
      assert.isNull(
        ThreadLaunch.readIssuedOrdinaryPreparedPhysicalResult({
          version: 1,
          kind: "prepared_setup_completed",
          producerId: executor.producerId,
          execution: executionHistory.participants[0]!.ref,
          targetSource: preparationUse!.subject.source,
          checkoutPath: expectedPath,
          observedAt: DateTime.formatIso(yield* DateTime.now),
          branch: "feature",
          worktree: { path: expectedPath, refName: "feature", headSha: "abc" },
          setup: { status: "no-script" },
        }),
      );
      const retry = yield* launches.launch(input);
      assert.isTrue(retry.resumed);
      assert.equal(retry.projection.thread.worktreePath, expectedPath);
      assert.equal(retry.projection.runs[0]?.id, first.projection.runs[0]?.id);
      assert.equal(harness.createWorktree.mock.calls.length, 1);
      assert.equal(harness.createWorktree.mock.calls[0]?.[0]?.path, expectedPath);
      assert.equal(harness.createWorktree.mock.calls[0]?.[0]?.newRefName, "feature");
      assert.equal(harness.runSetup.mock.calls.length, 0);
      yield* Deferred.succeed(allowWorktree, undefined);
      const releaseCommandId = CommandId.make(`${input.commandId}:release`);
      const scopeEvent = yield* threads.streamStoredEventsFrom({ threadId: first.threadId }).pipe(
        Stream.filter(
          (stored) =>
            stored.commandId === releaseCommandId &&
            stored.event.type === "checkpoint-scope.created",
        ),
        Stream.runHead,
        Effect.map(Option.getOrThrow),
      );
      assert.equal(scopeEvent.event.type, "checkpoint-scope.created");
      if (scopeEvent.event.type !== "checkpoint-scope.created")
        return yield* Effect.die("Missing release scope");
      const scope = scopeEvent.event.payload;
      assert.equal(scope.cwd, expectedPath);
      const released = yield* threads.getThreadProjection(first.threadId);
      assert.equal(released.thread.worktreePath, expectedPath);
      assert.equal(released.runs[0]?.status, "starting");
      assert.equal(
        released.checkpointScopes.find((candidate) => candidate.id === scope.id)?.cwd,
        expectedPath,
      );
      assert.equal(
        released.nodes.find((node) => node.id === first.projection.runs[0]?.rootNodeId)
          ?.checkpointScopeId,
        scope.id,
      );
      const scopeRows = yield* sql<{ readonly scope_id: string; readonly cwd: string }>`
        SELECT scope.scope_id, json_extract(scope.payload_json, '$.cwd') AS cwd
        FROM orchestration_v2_projection_nodes node
        JOIN orchestration_v2_projection_checkpoint_scopes scope
          ON scope.scope_id = node.checkpoint_scope_id AND scope.thread_id = node.thread_id
        WHERE node.thread_id = ${first.threadId} AND node.node_id = ${first.projection.runs[0]!.rootNodeId}
      `;
      assert.deepEqual(scopeRows, [{ scope_id: scope.id, cwd: expectedPath }]);
      assert.equal(
        Option.getOrThrow(yield* receipts.getByCommandId(releaseCommandId)).status,
        "accepted",
      );
      const replayAfterRelease = yield* launches.launch(input);
      assert.isTrue(replayAfterRelease.resumed);
      assert.equal(replayAfterRelease.projection.runs[0]?.id, first.projection.runs[0]?.id);
      assert.equal(harness.createWorktree.mock.calls.length, 1);
      assert.equal(harness.runSetup.mock.calls.length, 1);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("returns a visible preparing message while provisioning is still blocked", () =>
  Effect.gen(function* () {
    const worktreeEntered = yield* Deferred.make<void>();
    const allowWorktree = yield* Deferred.make<void>();
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      createWorktree: (input) =>
        Deferred.succeed(worktreeEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowWorktree)),
          Effect.as({
            worktree: { path: input.path, refName: input.newRefName, headSha: "abc" },
          } as never),
        ),
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const input = launchInput({
        command: "command:launch:blocked",
        thread: "thread:launch:blocked",
        message: "Build the feature",
        workspace: { type: "worktree", baseRef: "main" },
      });
      const launched = yield* launches.launch(input);
      assert.equal(launched.projection.messages[0]?.text, "Build the feature");
      assert.equal(launched.projection.runs[0]?.status, "preparing");
      assert.equal(
        launched.projection.turnItems.find((item) => item.type === "command_execution")?.status,
        "running",
      );
      yield* Deferred.await(worktreeEntered);
      let current = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(
        current.turnItems.find((item) => item.type === "command_execution")?.title,
        "Preparing worktree",
      );
      yield* Deferred.succeed(allowWorktree, undefined);
      const entered = yield* Deferred.await(setupEntered).pipe(
        Effect.timeoutOption(Duration.seconds(2)),
      );
      if (Option.isNone(entered)) {
        current = yield* threads.getThreadProjection(launched.threadId);
        assert.fail(
          `Setup was not reached; run=${current.runs[0]?.status ?? "missing"}, worklog=${current.turnItems.find((item) => item.type === "command_execution")?.title ?? "missing"}.`,
        );
      }
      current = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(
        current.turnItems.find((item) => item.type === "command_execution")?.title,
        "Starting setup script",
      );
      const prematureEffects = yield* outbox.listByCommandId(
        CommandId.make("command:launch:blocked:initial-message"),
      );
      assert.isEmpty(prematureEffects);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("provisions independent launches concurrently instead of behind a global semaphore", () =>
  Effect.gen(function* () {
    const setupCount = yield* Ref.make(0);
    const bothEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Ref.updateAndGet(setupCount, (count) => count + 1).pipe(
          Effect.tap((count) =>
            count === 2 ? Deferred.succeed(bothEntered, undefined) : Effect.void,
          ),
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const results = yield* Effect.all(
        [
          launches.launch(
            launchInput({
              command: "command:launch:concurrent-a",
              thread: "thread:launch:concurrent-a",
              message: "First",
              workspace: { type: "worktree", baseRef: "main", branch: "concurrent-a" },
            }),
          ),
          launches.launch(
            launchInput({
              command: "command:launch:concurrent-b",
              thread: "thread:launch:concurrent-b",
              message: "Second",
              workspace: { type: "worktree", baseRef: "main", branch: "concurrent-b" },
            }),
          ),
        ],
        { concurrency: "unbounded" },
      );
      assert.deepEqual(
        results.map((result) => result.projection.runs[0]?.status),
        ["preparing", "preparing"],
      );
      yield* Deferred.await(bothEntered);
      assert.equal(yield* Ref.get(setupCount), 2);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("enqueues provider work only after setup has been initiated", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: (input) =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({
            status: "started" as const,
            async: false,
            scriptId: "setup",
            scriptName: "Setup",
            scriptCommand: "vp install",
            terminalId: "setup",
            cwd: input.worktreePath,
            completion: Effect.succeed({ exitCode: 0, durationMs: 1 }),
          }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const input = launchInput({
        command: "command:launch:release",
        thread: "thread:launch:release",
        message: "Start after setup",
        workspace: { type: "worktree", baseRef: "main" },
      });
      const launched = yield* launches.launch(input);
      yield* Deferred.await(setupEntered);
      assert.isEmpty(
        yield* outbox.listByCommandId(CommandId.make("command:launch:release:release")),
      );
      yield* Deferred.succeed(allowSetup, undefined);
      yield* waitUntil(() =>
        outbox
          .listByCommandId(CommandId.make("command:launch:release:release"))
          .pipe(Effect.map((effects) => effects.length === 1)),
      );
      const projection = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(projection.runs[0]?.status, "starting");
      assert.equal(projection.checkpointScopes[0]?.cwd, launched.projection.thread.worktreePath);
      assert.equal(
        projection.turnItems.find((item) => item.type === "command_execution")?.status,
        "completed",
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect(
  "queues follow-up messages behind preparation and checkpoints them in the final workspace",
  () =>
    Effect.gen(function* () {
      const setupEntered = yield* Deferred.make<void>();
      const failSetup = yield* Deferred.make<void>();
      const harness = makeHarness({
        runSetup: () =>
          Deferred.succeed(setupEntered, undefined).pipe(
            Effect.andThen(Deferred.await(failSetup)),
            Effect.andThen(Effect.fail(new Error("setup failed") as never)),
          ),
      });
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const launched = yield* launches.launch(
          launchInput({
            command: "command:launch:queued-during-preparation",
            thread: "thread:launch:queued-during-preparation",
            message: "Prepare the workspace",
            workspace: { type: "worktree", baseRef: "main" },
          }),
        );
        yield* Deferred.await(setupEntered);

        const followUp = yield* threads.sendToThread({
          projectId,
          commandId: CommandId.make("command:launch:queued-follow-up"),
          threadId: launched.threadId,
          messageId: MessageId.make("message:launch:queued-follow-up"),
          text: "Run after preparation",
          attachments: [],
          mode: "auto",
          createdBy: "user",
          creationSource: "web",
        });
        assert.equal(followUp.delivery, "queued");
        assert.equal(followUp.run.status, "queued");
        assert.equal(
          (yield* threads.getThreadRecords(launched.threadId, ["nodes"])).nodes.find(
            (node) => node.runId === followUp.run.id && node.kind === "root_turn",
          )?.checkpointScopeId,
          null,
        );

        yield* Deferred.succeed(failSetup, undefined);
        yield* waitUntil(() =>
          threads
            .getThreadProjection(launched.threadId)
            .pipe(
              Effect.map(
                (projection) =>
                  projection.runs.find((run) => run.id === followUp.run.id)?.status === "starting",
              ),
            ),
        );

        const projection = yield* threads.getThreadProjection(launched.threadId);
        const rootNode = projection.nodes.find(
          (node) => node.runId === followUp.run.id && node.kind === "root_turn",
        );
        assert.isNotNull(rootNode?.checkpointScopeId);
        assert.equal(
          projection.checkpointScopes.find((scope) => scope.id === rootNode?.checkpointScopeId)
            ?.cwd,
          launched.projection.thread.worktreePath,
        );
      }).pipe(Effect.provide(harness.layer));
    }),
);

for (const nativeCommand of [" /COMPACT ", "/logout"]) {
  it.effect(`uses the first conversation message for a title after ${nativeCommand}`, () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const launched = yield* launches.launch({
          ...launchInput({
            command: "compact-title",
            thread: "compact-title-thread",
            message: nativeCommand,
          }),
          generateTitle: true,
        });
        assert.isUndefined(
          (yield* threads.getThreadProjection(launched.threadId)).thread.titleRegeneration,
        );
        assert.isFalse(
          (yield* outbox.listByCommandId(CommandId.make("compact-title:initial-message"))).some(
            (effect) => effect.request.type === "thread-title.generate",
          ),
        );
        const commandId = CommandId.make("compact-title-conversation");
        const messageId = MessageId.make("compact-title-conversation-message");
        yield* threads.dispatch({
          type: "message.dispatch",
          commandId,
          threadId: launched.threadId,
          messageId,
          createdBy: "user",
          creationSource: "web",
          text: "Fix the failing parser",
          attachments: [],
          dispatchMode: { type: "defer_start" },
        });
        assert.equal(
          (yield* threads.getThreadProjection(launched.threadId)).thread.titleRegeneration
            ?.requestId,
          commandId,
        );
        assert.deepEqual(
          (yield* outbox.listByCommandId(commandId))
            .filter((effect) => effect.request.type === "thread-title.generate")
            .map((effect) => effect.request),
          [{ type: "thread-title.generate", kind: { type: "initial", messageId } }],
        );
      }).pipe(Effect.provide(harness.layer));
    }),
  );
}

it.effect("keeps native maintenance commands out of steering and restart messages", () =>
  Effect.gen(function* () {
    const harness = makeHarness();
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      for (const scenario of [
        {
          name: "compact-steer",
          first: "Fix the parser",
          next: " /COMPACT ",
          mode: "steer_active",
        },
        {
          name: "compact-restart",
          first: "Fix the parser",
          next: "/compact",
          mode: "restart_active",
        },
        {
          name: "logout-steer",
          first: "Fix the parser",
          next: "/logout",
          mode: "steer_active",
        },
        {
          name: "logout-restart",
          first: "Fix the parser",
          next: "/logout",
          mode: "restart_active",
        },
        {
          name: "steer-logout",
          first: "/logout",
          next: "Continue with the parser",
          mode: "steer_active",
        },
        {
          name: "steer-compaction",
          first: "/compact",
          next: "Continue with the parser",
          mode: "steer_active",
        },
      ] as const) {
        const launched = yield* launches.launch(
          launchInput({
            command: `${scenario.name}:launch`,
            thread: scenario.name,
            message: scenario.first,
            workspace: { type: "worktree", baseRef: "main", branch: scenario.name },
          }),
        );
        const before = yield* threads.getThreadProjection(launched.threadId);
        const targetRun = before.runs[0];
        if (targetRun === undefined) return yield* Effect.die("Launch must create a run");
        const commandId = CommandId.make(`${scenario.name}:message`);
        const failure = yield* threads
          .dispatch({
            type: "message.dispatch",
            commandId,
            threadId: launched.threadId,
            messageId: MessageId.make(`${scenario.name}:message`),
            createdBy: "user",
            creationSource: "web",
            text: scenario.next,
            attachments: [],
            dispatchMode: { type: scenario.mode, targetRunId: targetRun.id },
          })
          .pipe(Effect.flip);
        assert.include(
          String(failure.cause).toLowerCase(),
          scenario.name.includes("logout") ? "sign" : "context compaction",
        );
        const after = yield* threads.getThreadProjection(launched.threadId);
        assert.deepEqual(after.messages, before.messages);
        assert.deepEqual(after.thread.titleRegeneration, before.thread.titleRegeneration);
        assert.deepEqual(yield* outbox.listByCommandId(commandId), []);
      }
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("arms durable title generation after accepting the first message", () =>
  Effect.gen(function* () {
    const harness = makeHarness({
      generateTitle: (input) =>
        Effect.succeed({
          title: input.previousTitle === undefined ? "Generated title" : "Regenerated title",
        }),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
      const input = {
        ...launchInput({
          command: "command:launch:title-generation",
          thread: "thread:launch:title-generation",
          message: "Generate my title",
        }),
        title: "Generate my title",
        generateTitle: true,
      };
      const launched = yield* launches.launch(input);
      const generationCommandId = CommandId.make("command:launch:title-generation:initial-message");

      const projection = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(projection.thread.title, "Generate my title");
      assert.equal(projection.thread.titleRegeneration?.requestId, generationCommandId);
      assert.deepEqual(
        (yield* outbox.listByCommandId(generationCommandId)).map((effect) => effect.request),
        [
          {
            type: "thread-title.generate",
            kind: { type: "initial", messageId: MessageId.make("Generate my title:id") },
          },
        ],
      );
      yield* titleRegeneration.execute({
        threadId: launched.threadId,
        requestId: generationCommandId,
        kind: { type: "initial", messageId: MessageId.make("Generate my title:id") },
      });
      const generated = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(generated.thread.title, "Generated title");
      assert.deepEqual(
        harness.generateThreadTitle.mock.calls[0]?.[0]?.modelSelection,
        DEFAULT_SERVER_SETTINGS.textGenerationModelSelection,
      );

      const manualRequestId = CommandId.make("command:title-generation:manual");
      yield* threads.dispatch({
        type: "thread.metadata.update",
        commandId: manualRequestId,
        threadId: launched.threadId,
        regenerateTitle: true,
      });
      assert.deepEqual(
        (yield* outbox.listByCommandId(manualRequestId)).map((effect) => effect.request),
        [{ type: "thread-title.generate", kind: { type: "regenerate" } }],
      );
      yield* titleRegeneration.execute({
        threadId: launched.threadId,
        requestId: manualRequestId,
        kind: { type: "regenerate" },
      });
      const regenerated = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(regenerated.thread.title, "Regenerated title");
      assert.equal(
        harness.generateThreadTitle.mock.calls[1]?.[0]?.previousTitle,
        "Generated title",
      );

      yield* threads.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("command:title-generation:user-rename"),
        threadId: launched.threadId,
        title: "Keep my title",
      });
      const renamed = yield* threads.getThreadProjection(launched.threadId);
      yield* TestClock.adjust(Duration.seconds(1));
      yield* threads.dispatch({
        type: "thread.title.regeneration.complete",
        commandId: CommandId.make("command:title-generation:stale-completion"),
        threadId: launched.threadId,
        requestId: generationCommandId,
        title: "Stale generated title",
      });
      const afterStaleCompletion = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(afterStaleCompletion.thread.title, "Keep my title");
      assert.equal(
        DateTime.toEpochMillis(afterStaleCompletion.thread.updatedAt),
        DateTime.toEpochMillis(renamed.thread.updatedAt),
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("does not update a reused thread title when the initial message is rejected", () =>
  Effect.gen(function* () {
    const harness = makeHarness();
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threadId = ThreadId.make("thread:launch:reused-title-failure");
      yield* threads.dispatch({
        type: "thread.create",
        commandId: CommandId.make("command:launch:reused-title-failure:create"),
        threadId,
        projectId,
        title: "Original title",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });

      const commandId = CommandId.make("command:launch:reused-title-failure");
      const failed = yield* launches
        .launch({
          ...launchInput({
            command: commandId,
            thread: threadId,
            message: "Generate a provisional title",
          }),
          reuseExistingThread: true,
          title: "Generate a provisional title",
          generateTitle: true,
          modelSelection: {
            instanceId: ProviderInstanceId.make("missing-provider"),
            model: "missing-model",
          },
        })
        .pipe(Effect.exit);

      assert.isTrue(Exit.isFailure(failed));
      const projection = yield* threads.getThreadProjection(threadId);
      assert.equal(projection.thread.title, "Original title");
      assert.isUndefined(projection.thread.titleRegeneration);
      assert.isEmpty(projection.messages);
      assert.isEmpty(yield* outbox.listByCommandId(CommandId.make(`${commandId}:initial-message`)));
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("generates an initial title for an attachment-only message", () =>
  Effect.gen(function* () {
    const harness = makeHarness();
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
      const messageId = MessageId.make("message:image-only");
      const input = {
        ...launchInput({
          command: "command:launch:image-only",
          thread: "thread:launch:image-only",
        }),
        title: "Image: screenshot.png",
        generateTitle: true,
        initialMessage: {
          messageId,
          text: "",
          attachments: [
            {
              type: "image" as const,
              id: "attachment-image-only",
              name: "screenshot.png",
              mimeType: "image/png",
              sizeBytes: 128,
            },
          ],
        },
      };

      const launched = yield* launches.launch(input);
      yield* titleRegeneration.execute({
        threadId: launched.threadId,
        requestId: CommandId.make("command:launch:image-only:initial-message"),
        kind: { type: "initial", messageId },
      });

      assert.equal(harness.generateThreadTitle.mock.calls[0]?.[0]?.message, "");
      assert.equal(
        harness.generateThreadTitle.mock.calls[0]?.[0]?.attachments?.[0]?.name,
        "screenshot.png",
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("uses the available source control writer for generated worktree branches", () =>
  Effect.gen(function* () {
    const branchNameStarted = yield* Deferred.make<void>();
    const writerInstanceId = ProviderInstanceId.make("source-control-writer");
    const writerModelSelection = {
      instanceId: writerInstanceId,
      model: "branch-writer-model",
    } as const;
    const harness = makeHarness({
      generateBranchName: () =>
        Deferred.succeed(branchNameStarted, undefined).pipe(
          Effect.as({ branch: "generated-branch" }),
        ),
      serverSettings: {
        providerInstances: {
          [writerInstanceId]: {
            driver: ProviderDriverKind.make("codex"),
            config: {},
          },
        },
        sourceControlWriterModelSelection: writerModelSelection,
      },
      providers: [
        {
          instanceId: writerInstanceId,
          driver: ProviderDriverKind.make("codex"),
          enabled: true,
          installed: true,
          version: null,
          status: "ready",
          auth: { status: "authenticated" },
          checkedAt: "2026-07-28T00:00:00.000Z",
          availability: "available",
          models: [],
          slashCommands: [],
          skills: [],
        },
      ],
    });

    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      yield* launches.launch(
        launchInput({
          command: "command:launch:source-control-writer",
          thread: "thread:launch:source-control-writer",
          message: "Generate a branch with the configured writer",
          workspace: { type: "worktree", baseRef: "main" },
        }),
      );
      yield* Deferred.await(branchNameStarted);
      assert.equal(harness.generateBranchName.mock.calls.length, 1);
      assert.deepEqual(
        harness.generateBranchName.mock.calls[0]?.[0]?.modelSelection,
        writerModelSelection,
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("falls back when the source control writer is unavailable", () =>
  Effect.gen(function* () {
    const branchNameStarted = yield* Deferred.make<void>();
    const writerInstanceId = ProviderInstanceId.make("missing-source-control-writer");
    const harness = makeHarness({
      generateBranchName: () =>
        Deferred.succeed(branchNameStarted, undefined).pipe(
          Effect.as({ branch: "generated-branch" }),
        ),
      serverSettings: {
        providerInstances: {
          [writerInstanceId]: {
            driver: ProviderDriverKind.make("missing-driver"),
            config: {},
          },
        },
        sourceControlWriterModelSelection: {
          instanceId: writerInstanceId,
          model: "missing-branch-writer-model",
        },
      },
      providers: [],
    });

    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      yield* launches.launch(
        launchInput({
          command: "command:launch:source-control-writer-fallback",
          thread: "thread:launch:source-control-writer-fallback",
          message: "Generate a branch with the available writer",
          workspace: { type: "worktree", baseRef: "main" },
        }),
      );
      yield* Deferred.await(branchNameStarted);
      assert.equal(harness.generateBranchName.mock.calls.length, 1);
      assert.deepEqual(
        harness.generateBranchName.mock.calls[0]?.[0]?.modelSelection,
        DEFAULT_SERVER_SETTINGS.textGenerationModelSelection,
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("runs a Scratch thread launched at the root in its own folder", () =>
  Effect.gen(function* () {
    const setupStarted = yield* Deferred.make<void>();
    // Only `projectId` stands in for the Scratch project here.
    const claimed: Array<{ readonly threadId: ThreadId; readonly text: string }> = [];
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupStarted, undefined).pipe(Effect.as({ status: "no-script" as const })),
      managedFolders: Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/projects",
        folderForThread: (input) =>
          Effect.sync(() => {
            if (input.projectId !== projectId) return Option.none();
            claimed.push({ threadId: input.threadId, text: input.text });
            return Option.some(`/scratch/folder-${claimed.length}`);
          }),
      }),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const input = launchInput({
        command: "command:launch:scratch",
        thread: "thread:launch:scratch",
        message: "Convert these PNGs",
      });
      const launched = yield* launches.launch(input);
      assert.deepEqual(claimed, [{ threadId: launched.threadId, text: "Convert these PNGs" }]);
      assert.equal(launched.projection.thread.worktreePath, "/scratch/folder-1");
      yield* Deferred.await(setupStarted);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      assert.equal(harness.runSetup.mock.calls[0]?.[0]?.worktreePath, "/scratch/folder-1");
      assert.equal(harness.createWorktree.mock.calls.length, 0);

      // A retry replays the first attempt and claims no second folder.
      const retried = yield* launches.launch(input);
      assert.isTrue(retried.resumed);
      assert.lengthOf(claimed, 1);
      assert.equal(
        (yield* threads.getThreadProjection(launched.threadId)).thread.worktreePath,
        "/scratch/folder-1",
      );

      const other = yield* launches.launch({
        ...launchInput({
          command: "command:launch:scratch-other",
          thread: "thread:launch:scratch-other",
          message: "Elsewhere",
        }),
        projectId: otherProjectId,
      });
      assert.lengthOf(claimed, 1);
      assert.isNull(other.projection.thread.worktreePath);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("names the worktree itself when the client provides no branch", () =>
  Effect.gen(function* () {
    const harness = makeHarness();
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: "command:launch:server-named-branch",
          thread: "thread:launch:server-named-branch",
          message: "Build the feature",
          workspace: { type: "worktree", baseRef: "main" },
        }),
      );
      yield* waitUntil(() => Effect.sync(() => harness.createWorktree.mock.calls.length === 1));
      assert.match(
        harness.createWorktree.mock.calls[0]?.[0]?.newRefName ?? "",
        /^t3code\/[0-9a-f]{8}$/u,
      );
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.thread.branch === "generated-branch")),
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("renames a temporary t3code/<hash> branch off the provisioning critical path", () =>
  Effect.gen(function* () {
    const branchNameStarted = yield* Deferred.make<void>();
    const allowBranchName = yield* Deferred.make<void>();
    const harness = makeHarness({
      createWorktree: (input) =>
        Effect.succeed({
          worktree: { path: input.path, refName: input.newRefName, headSha: "abc" },
        } as never),
      generateBranchName: () =>
        Deferred.succeed(branchNameStarted, undefined).pipe(
          Effect.andThen(Deferred.await(allowBranchName)),
          Effect.as({ branch: "generated-branch" }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: "command:launch:temp-branch",
          thread: "thread:launch:temp-branch",
          message: "Build the feature",
          workspace: { type: "worktree", baseRef: "main", branch: "t3code/abcd1234" },
        }),
      );
      yield* Deferred.await(branchNameStarted);
      assert.equal(harness.createWorktree.mock.calls[0]?.[0]?.newRefName, "t3code/abcd1234");
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.runs[0]?.status === "starting")),
      );
      assert.equal(
        (yield* threads.getThreadProjection(launched.threadId)).thread.branch,
        "t3code/abcd1234",
      );
      yield* Deferred.succeed(allowBranchName, undefined);
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.thread.branch === "generated-branch")),
      );
      assert.deepEqual(harness.renameBranch.mock.calls[0]?.[0], {
        cwd: nativeWorktreePath({
          worktreesDir: "/repo-worktrees",
          cwd: project.workspaceRoot,
          branch: "t3code/abcd1234",
        }),
        oldBranch: "t3code/abcd1234",
        newBranch: "generated-branch",
      });
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("keeps an explicit branch name instead of generating one", () =>
  Effect.gen(function* () {
    const worktreeStarted = yield* Deferred.make<void>();
    const harness = makeHarness({
      createWorktree: (input) =>
        Deferred.succeed(worktreeStarted, undefined).pipe(
          Effect.as({
            worktree: {
              path: input.path ?? "/repo-worktrees/feature",
              refName: input.newRefName,
              headSha: "abc",
            },
          } as never),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      yield* launches.launch(
        launchInput({
          command: "command:launch:explicit-branch",
          thread: "thread:launch:explicit-branch",
          message: "Build the feature",
          workspace: { type: "worktree", baseRef: "main", branch: "my-feature" },
        }),
      );
      yield* Deferred.await(worktreeStarted);
      assert.equal(harness.createWorktree.mock.calls.length, 1);
      assert.equal(harness.generateBranchName.mock.calls.length, 0);
      assert.equal(harness.createWorktree.mock.calls[0]?.[0]?.newRefName, "my-feature");
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("keeps the temporary branch when branch generation fails", () =>
  Effect.gen(function* () {
    const branchNameStarted = yield* Deferred.make<void>();
    const harness = makeHarness({
      createWorktree: (input) =>
        Effect.succeed({
          worktree: { path: input.path, refName: input.newRefName, headSha: "abc" },
        } as never),
      generateBranchName: () =>
        Deferred.succeed(branchNameStarted, undefined).pipe(
          Effect.andThen(Effect.die("branch generation is down")),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: "command:launch:branch-fallback",
          thread: "thread:launch:branch-fallback",
          message: "Build the feature",
          workspace: { type: "worktree", baseRef: "main", branch: "t3code/abcd1234" },
        }),
      );
      yield* Deferred.await(branchNameStarted);
      assert.equal(harness.generateBranchName.mock.calls.length, 1);
      assert.equal(harness.createWorktree.mock.calls[0]?.[0]?.newRefName, "t3code/abcd1234");
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.runs[0]?.status === "starting")),
      );
      assert.equal(harness.renameBranch.mock.calls.length, 0);
      assert.equal(
        (yield* threads.getThreadProjection(launched.threadId)).thread.branch,
        "t3code/abcd1234",
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("renames a temporary branch on an existing worktree to a generated name", () =>
  Effect.gen(function* () {
    const harness = makeHarness();
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: "command:launch:existing-worktree-rename",
          thread: "thread:launch:existing-worktree-rename",
          message: "Build the feature",
          workspace: {
            type: "existing_worktree",
            worktreePath: "/repo-worktrees/t3code-abcd1234",
            branch: "t3code/abcd1234",
          },
        }),
      );
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.thread.branch === "generated-branch")),
      );
      assert.deepEqual(harness.renameBranch.mock.calls[0]?.[0], {
        cwd: "/repo-worktrees/t3code-abcd1234",
        oldBranch: "t3code/abcd1234",
        newBranch: "generated-branch",
      });
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("shows the fetch diagnosis when preparing a worktree from origin fails", () => {
  const detail =
    "Git could not authenticate with the remote. Check Git credentials or SSH access on the server, then retry.";
  const harness = makeHarness({
    fetchRemote: () =>
      Effect.fail(
        new GitCommandError({
          operation: "GitVcsDriver.fetchRemote",
          command: "git",
          cwd: project.workspaceRoot,
          detail,
          exitCode: 128,
        }),
      ),
  });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const launched = yield* launches.launch(
      launchInput({
        command: "command:launch:fetch-failure",
        thread: "thread:launch:fetch-failure",
        message: "Start from origin",
        workspace: { type: "worktree", baseRef: "main", startFromOrigin: true },
      }),
    );
    yield* threads.streamStoredEventsFrom({ threadId: launched.threadId }).pipe(
      Stream.filter(
        (stored) => stored.event.type === "run.updated" && stored.event.payload.status === "failed",
      ),
      Stream.runHead,
    );
    const projection = yield* threads.getThreadProjection(launched.threadId);
    assert.equal(projection.messages[0]?.text, "Start from origin");
    assert.equal(projection.runs[0]?.status, "failed");
    assert.equal(projection.thread.worktreePath, launched.projection.thread.worktreePath);
    assert.isNotNull(projection.thread.worktreePath);
    assert.equal(
      projection.turnItems.find((item) => item.type === "command_execution")?.status,
      "failed",
    );
    assert.include(
      projection.turnItems.find((item) => item.type === "error")?.failure.message ?? "",
      detail,
    );
    assert.equal(harness.createWorktree.mock.calls.length, 0);
    assert.equal(harness.runSetup.mock.calls.length, 0);
  }).pipe(Effect.provide(harness.layer));
});

for (const failurePoint of ["worktree", "setup"] as const) {
  it.effect(
    `${failurePoint} failure keeps the thread and message visible and emits failure items`,
    () =>
      Effect.gen(function* () {
        const failure = new Error(`${failurePoint} failed`);
        const harness = makeHarness(
          failurePoint === "worktree"
            ? { createWorktree: () => Effect.fail(failure as never) }
            : { runSetup: () => Effect.fail(failure as never) },
        );
        yield* Effect.gen(function* () {
          const launches = yield* ThreadLaunch.ThreadLaunchService;
          const threads = yield* ThreadManagement.ThreadManagementService;
          const input = launchInput({
            command: `command:launch:${failurePoint}-failure`,
            thread: `thread:launch:${failurePoint}-failure`,
            message: `Fail during ${failurePoint}`,
            workspace: { type: "worktree", baseRef: "main" },
          });
          const launched = yield* launches.launch(input);
          yield* waitUntil(() =>
            threads
              .getThreadProjection(launched.threadId)
              .pipe(Effect.map((projection) => projection.runs[0]?.status === "failed")),
          );
          const projection = yield* threads.getThreadProjection(launched.threadId);
          assert.equal(projection.messages[0]?.text, `Fail during ${failurePoint}`);
          assert.equal(projection.runs[0]?.status, "failed");
          assert.equal(
            projection.turnItems.find((item) => item.type === "command_execution")?.status,
            "failed",
          );
          assert.match(
            projection.turnItems.find((item) => item.type === "error")?.failure.message ?? "",
            new RegExp(`${failurePoint} failed`, "u"),
          );
        }).pipe(Effect.provide(harness.layer));
      }),
  );
}

it.effect("replays a server-allocated launch", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const { threadId: _unusedThreadId, ...rest } = launchInput({
        command: "command:launch:allocated-retry",
        thread: "unused",
        message: "Only once",
      });
      const first = yield* launches.launch(rest);
      yield* Deferred.await(setupEntered);
      const retry = yield* launches.launch(rest);
      assert.equal(first.threadId, retry.threadId);
      assert.isFalse(first.resumed);
      assert.isTrue(retry.resumed);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      assert.equal(retry.projection.messages.length, 1);
      assert.equal(retry.projection.runs.length, 1);
      assert.equal(retry.projection.messages[0]?.id, first.projection.messages[0]?.id);
      assert.equal(retry.projection.runs[0]?.id, first.projection.runs[0]?.id);
      yield* Deferred.succeed(allowSetup, undefined);
      yield* threads.streamStoredEventsFrom({ threadId: first.threadId }).pipe(
        Stream.filter(
          (stored) =>
            stored.commandId === CommandId.make(`${rest.commandId}:release`) &&
            stored.event.type === "run.updated",
        ),
        Stream.runHead,
      );
      const settled = yield* launches.launch(rest);
      assert.equal(settled.threadId, first.threadId);
      assert.isTrue(settled.resumed);
      assert.equal(settled.projection.messages[0]?.id, first.projection.messages[0]?.id);
      assert.equal(settled.projection.runs[0]?.id, first.projection.runs[0]?.id);
      assert.equal(harness.runSetup.mock.calls.length, 1);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("rejects a server-allocated launch replay with a mismatching thread id", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const { threadId: _unusedThreadId, ...rest } = launchInput({
      command: "command:launch:allocated-mismatch",
      thread: "unused",
      message: "Mismatch",
    });
    const first = yield* launches.launch(rest);
    const failed = yield* launches
      .launch({
        ...rest,
        threadId: ThreadId.make("thread:launch:allocated-mismatch"),
      })
      .pipe(Effect.flip);
    assert.notEqual(first.threadId, ThreadId.make("thread:launch:allocated-mismatch"));
    assert.equal(failed._tag, "ThreadLaunchError");
    assert.equal(failed.operation, "create-thread");
    assert.include(String(failed.cause), "cannot be replayed");
  }).pipe(Effect.provide(harness.layer));
});

it.effect("rejects a server-allocated launch receipt from another project", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const { threadId: _unusedThreadId, ...rest } = launchInput({
      command: "command:launch:allocated-wrong-project",
      thread: "unused",
      message: "Wrong project",
    });
    const first = yield* launches.launch(rest);
    const failed = yield* launches
      .launch({
        ...rest,
        projectId: otherProjectId,
      })
      .pipe(Effect.flip);
    assert.equal(failed._tag, "ThreadLaunchError");
    assert.equal(failed.operation, "resolve-project");
    assert.equal(failed.threadId, first.threadId);
    assert.equal(failed.cause, "Project identity changed.");
  }).pipe(Effect.provide(harness.layer));
});

it.effect("rejects a server-allocated launch retry after the thread is deleted", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const { threadId: _unusedThreadId, ...rest } = launchInput({
      command: "command:launch:allocated-deleted",
      thread: "unused",
      message: "Deleted before retry",
    });
    const first = yield* launches.launch(rest);
    yield* threads.dispatch({
      type: "thread.delete",
      commandId: CommandId.make("command:launch:allocated-deleted:delete"),
      threadId: first.threadId,
    });
    const failed = yield* launches.launch(rest).pipe(Effect.flip);
    assert.equal(failed._tag, "ThreadLaunchError");
    assert.equal(failed.operation, "create-thread");
    assert.equal(failed.threadId, first.threadId);
    assert.equal(failed.cause, "Thread not found.");
    const shells = yield* threads.listProjectThreads({ projectId, includeSubagents: true });
    assert.equal(shells.length, 0);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("does not treat an unrelated accepted command receipt as a launch", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const threadId = ThreadId.make("thread:launch:unrelated-receipt");
    yield* threads.dispatch({
      type: "thread.create",
      commandId: CommandId.make("command:launch:unrelated-receipt:create"),
      threadId,
      projectId,
      title: "Existing",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    yield* threads.dispatch({
      type: "thread.metadata.update",
      commandId: CommandId.make("command:launch:unrelated-receipt"),
      threadId,
      expectedEmpty: true,
    });
    const { threadId: _unusedThreadId, ...rest } = launchInput({
      command: "command:launch:unrelated-receipt",
      thread: "unused",
      message: "Should not become a launch",
    });
    const failed = yield* launches.launch(rest).pipe(Effect.flip);
    assert.equal(failed._tag, "ThreadLaunchError");
    assert.equal(failed.operation, "create-thread");
    assert.include(String(failed.cause), "cannot be replayed");
    const projection = yield* threads.getThreadProjection(threadId);
    assert.equal(projection.messages.length, 0);
    assert.equal(projection.runs.length, 0);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("bounds concurrent first launches to one thread per command", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const { threadId: _unusedThreadId, ...rest } = launchInput({
        command: "command:launch:concurrent-allocated",
        thread: "unused",
        message: "Race me",
      });
      // The command receipt is reserved atomically with the winning create, so
      // a loser either replays the winner's stored events or surfaces a
      // transient replay conflict that the next attempt resolves — the race
      // can never persist a second thread.
      const results = yield* Effect.all(
        [launches.launch(rest).pipe(Effect.exit), launches.launch(rest).pipe(Effect.exit)],
        { concurrency: "unbounded" },
      );
      const winner = results.find(Exit.isSuccess);
      assert.isDefined(winner);
      const threadId = winner!.value.threadId;
      for (const result of results) {
        if (Exit.isSuccess(result)) {
          assert.equal(result.value.threadId, threadId);
          continue;
        }
        const error = Cause.findErrorOption(result.cause).pipe(Option.getOrThrow);
        assert.equal(error._tag, "ThreadLaunchError");
        assert.equal(error.operation, "create-thread");
        assert.include(String(error.cause), "cannot be replayed");
        const retried = yield* launches.launch(rest);
        assert.equal(retried.threadId, threadId);
      }
      const projectThreads = yield* threads.listProjectThreads({
        projectId,
        includeSubagents: false,
      });
      assert.equal(projectThreads.length, 1);
      const projection = yield* threads.getThreadProjection(threadId);
      assert.equal(projection.messages.length, 1);
      assert.equal(projection.runs.length, 1);
      yield* Deferred.await(setupEntered);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("deduplicates retried launch side effects in-process", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const input = launchInput({
        command: "command:launch:retry",
        thread: "thread:launch:retry",
        message: "Only once",
      });
      const [first, retry] = yield* Effect.all([launches.launch(input), launches.launch(input)], {
        concurrency: "unbounded",
      });
      yield* Deferred.await(setupEntered);
      assert.equal(first.threadId, retry.threadId);
      assert.isFalse(first.resumed);
      assert.isTrue(retry.resumed);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("does not let a failing same-command caller strand a concurrent durable launch", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const command = "command:launch:failed-owner-race";
      const [failed, launched] = yield* Effect.all(
        [
          launches
            .launch({
              ...launchInput({
                command,
                thread: "thread:launch:failed-owner-race",
                message: "This invalid reuse fails",
              }),
              reuseExistingThread: true,
            })
            .pipe(Effect.exit),
          launches.launch(
            launchInput({
              command,
              thread: "thread:launch:successful-peer",
              message: "This peer persists",
            }),
          ),
        ],
        { concurrency: "unbounded" },
      );
      assert.isTrue(Exit.isFailure(failed));
      assert.equal(launched.projection.runs[0]?.status, "preparing");
      const entered = yield* Deferred.await(setupEntered).pipe(
        Effect.timeoutOption(Duration.seconds(2)),
      );
      assert.isTrue(Option.isSome(entered));
      assert.equal(harness.runSetup.mock.calls.length, 1);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("schedules an accepted preparing message exactly once across concurrent retries", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const input = launchInput({
        command: "command:launch:accepted-before-fork",
        thread: "thread:launch:accepted-before-fork",
        message: "Resume preparation",
      });
      const messageId = input.initialMessage!.messageId;

      yield* threads.dispatch({
        type: "thread.create",
        commandId: input.commandId,
        threadId: input.threadId,
        projectId: input.projectId,
        title: input.title,
        modelSelection: input.modelSelection,
        runtimeMode: input.runtimeMode,
        interactionMode: input.interactionMode,
        branch: null,
        worktreePath: null,
        createdBy: input.createdBy,
        creationSource: input.creationSource,
      });
      yield* threads.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`${input.commandId}:initial-message`),
        threadId: input.threadId,
        messageId,
        text: "Resume preparation",
        attachments: [],
        modelSelection: input.modelSelection,
        dispatchMode: { type: "defer_start" },
        createdBy: input.createdBy,
        creationSource: input.creationSource,
      });
      const preparing = yield* threads.getThreadProjection(input.threadId);
      assert.equal(preparing.runs[0]?.status, "preparing");

      const [first, second] = yield* Effect.all([launches.launch(input), launches.launch(input)], {
        concurrency: "unbounded",
      });
      yield* Deferred.await(setupEntered);
      assert.isTrue(first.resumed);
      assert.isTrue(second.resumed);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("creates a strong provider-thread mapping for an imported native session", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const input = {
      ...launchInput({
        command: "command:launch:imported-native-session",
        thread: "thread:launch:imported-native-session",
      }),
      importedNativeThread: {
        ref: {
          driver: ProviderDriverKind.make("codex"),
          nativeId: "native-session-42",
          strength: "strong" as const,
        },
        metadata: {
          title: "Native session",
          updatedAt: "2026-08-23T00:00:00Z",
        },
      },
    };

    const launched = yield* launches.launch(input);

    assert.deepInclude(launched.projection.providerThreads[0], {
      id: IdAllocator.deriveProviderThread({
        driver: input.importedNativeThread.ref.driver,
        providerInstanceId: modelSelection.instanceId,
        nativeThreadId: input.importedNativeThread.ref.nativeId,
      }),
      driver: input.importedNativeThread.ref.driver,
      providerInstanceId: modelSelection.instanceId,
      appThreadId: input.threadId,
      nativeThreadRef: input.importedNativeThread.ref,
      status: "not_loaded",
      nativeMetadata: input.importedNativeThread.metadata,
    });
    assert.equal(
      launched.projection.thread.activeProviderThreadId,
      launched.projection.providerThreads[0]?.id,
    );
  }).pipe(Effect.provide(harness.layer));
});

it.effect("shared intake preserves durable attachment bytes after a lost launch result", () => {
  const harness = makeHarness();
  const files = ServerConfig.layerTest(process.cwd(), { prefix: "t3-message-intake-" }).pipe(
    Layer.provideMerge(NodeServices.layer),
  );
  return Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    const fs = yield* FileSystem.FileSystem;
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const pendingId = createPendingAttachmentId();
    assert.isNotNull(pendingId);
    const attachment: ChatAttachment = {
      type: "image",
      id: ChatAttachmentId.make(pendingId),
      name: "image.png",
      mimeType: "image/png",
      sizeBytes: 4,
    };
    const pendingPath = resolveAttachmentPath({
      attachmentsDir: config.attachmentsDir,
      attachment,
    });
    assert.isNotNull(pendingPath);
    yield* fs.makeDirectory(config.attachmentsDir, { recursive: true });
    yield* fs.writeFile(pendingPath, new Uint8Array([1, 2, 3, 4]));
    const input = {
      ...launchInput({ command: "intake-launch", thread: "intake-thread" }),
      initialMessage: {
        messageId: MessageId.make("intake-first"),
        text: "First [file](t3-context://v1/file/intake-file)",
        context: {
          version: 1 as const,
          records: [
            {
              version: 1 as const,
              contextId: ComposerContextId.make("intake-file"),
              kind: "file" as const,
              label: attachment.name,
              attachmentId: attachment.id,
              name: attachment.name,
              mimeType: attachment.mimeType,
              sizeBytes: attachment.sizeBytes,
            },
          ],
        },
        attachments: [attachment],
      },
    };
    const failed = yield* ThreadMessageIntake.launchThread(input).pipe(
      Effect.provideService(ThreadLaunch.ThreadLaunchService, {
        ...launches,
        launch: (request) =>
          launches.launch(request).pipe(
            Effect.andThen(
              new ThreadLaunch.ThreadLaunchError({
                operation: "create-thread",
                commandId: request.commandId,
                projectId,
                cause: "lost result after acceptance",
              }),
            ),
          ),
      }),
      Effect.flip,
    );
    assert.equal(failed._tag, "ThreadLaunchError");
    // The observer failed, but the real V2 message and its bytes were accepted.
    const accepted = yield* threads.getThreadProjection(input.threadId);
    const stored = accepted.messages.find(
      (message) => message.id === input.initialMessage.messageId,
    );
    assert.isDefined(stored);
    assert.notEqual(stored.attachments[0]?.id, attachment.id);
    assert.equal(
      (stored.context?.records[0] as { attachmentId: string }).attachmentId,
      stored.attachments[0]?.id,
    );
    const userItem = accepted.turnItems.find(
      (item) => item.type === "user_message" && item.messageId === stored.id,
    );
    assert.ok(userItem?.type === "user_message");
    assert.deepEqual(userItem.context, stored.context);
    const storedPath = resolveAttachmentPath({
      attachmentsDir: config.attachmentsDir,
      attachment: stored.attachments[0]!,
    });
    assert.isNotNull(storedPath);
    assert.deepEqual(yield* fs.readFile(storedPath), new Uint8Array([1, 2, 3, 4]));
    assert.deepEqual(yield* fs.readFile(pendingPath), new Uint8Array([1, 2, 3, 4]));

    const replayed = yield* ThreadMessageIntake.launchThread(input);
    assert.equal(replayed.projection.messages[0]?.id, stored.id);
    assert.deepEqual(replayed.projection.messages[0]?.attachments, stored.attachments);
    const claimedFiles = Effect.map(fs.readDirectory(config.attachmentsDir), (files) =>
      files.filter((name) => !name.startsWith("pending-")),
    );
    assert.equal((yield* claimedFiles).length, 1);

    const missingProject = yield* ThreadMessageIntake.launchThread({
      ...input,
      commandId: CommandId.make("intake-no-project"),
      projectId: ProjectId.make("missing-project"),
    }).pipe(Effect.flip);
    assert.equal(missingProject._tag, "ThreadLaunchError");
    assert.equal((yield* claimedFiles).length, 1);
    const missingThread = yield* ThreadMessageIntake.dispatchCommand({
      type: "message.dispatch",
      commandId: CommandId.make("intake-no-thread"),
      threadId: ThreadId.make("missing-thread"),
      messageId: MessageId.make("intake-missing"),
      text: "Missing",
      attachments: [attachment],
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    }).pipe(Effect.flip);
    assert.equal(missingThread._tag, "OrchestratorProjectionError");
    assert.equal((yield* claimedFiles).length, 1);

    // Both ordinary command intake (RPC) and send intake (MCP) use the same store.
    const dispatch = ThreadMessageIntake.dispatchCommand({
      type: "message.dispatch",
      commandId: CommandId.make("intake-dispatch"),
      threadId: input.threadId,
      messageId: MessageId.make("intake-second"),
      text: "Second",
      attachments: [attachment],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "user",
      creationSource: "web",
    });
    yield* dispatch;
    yield* dispatch;
    const queuedProjection = yield* threads.getThreadProjection(input.threadId);
    const queuedRun = queuedProjection.runs.find(
      (run) => run.userMessageId === MessageId.make("intake-second"),
    );
    assert.isDefined(queuedRun);
    assert.equal(queuedRun.status, "queued");
    const queuedMessage = queuedProjection.messages.find(
      (message) => message.id === queuedRun.userMessageId,
    );
    assert.isDefined(queuedMessage);
    const file: ChatAttachment = {
      type: "file",
      id: ChatAttachmentId.make(createPendingAttachmentId("pdf")),
      name: "queued.pdf",
      mimeType: "application/pdf",
      sizeBytes: 4,
    };
    const filePath = resolveAttachmentPath({
      attachmentsDir: config.attachmentsDir,
      attachment: file,
    });
    assert.isNotNull(filePath);
    yield* fs.writeFile(filePath, new Uint8Array([5, 6, 7, 8]));
    const edit = ThreadMessageIntake.dispatchCommand({
      type: "queued-run.edit",
      commandId: CommandId.make("intake-edit"),
      threadId: input.threadId,
      runId: queuedRun.id,
      text: "Edited with a file",
      attachments: [...queuedMessage.attachments, file],
    });
    yield* edit;
    yield* edit;
    const editedProjection = yield* threads.getThreadProjection(input.threadId);
    const editedMessage = editedProjection.messages.find(
      (message) => message.id === queuedRun.userMessageId,
    );
    assert.isDefined(editedMessage);
    assert.equal(editedMessage.attachments.length, 2);
    assert.deepEqual(editedMessage.attachments[0], queuedMessage.attachments[0]);
    assert.notEqual(editedMessage.attachments[1]?.id, file.id);
    const durableFilePath = resolveAttachmentPath({
      attachmentsDir: config.attachmentsDir,
      attachment: editedMessage.attachments[1]!,
    });
    assert.isNotNull(durableFilePath);
    assert.deepEqual(yield* fs.readFile(durableFilePath), new Uint8Array([5, 6, 7, 8]));
    assert.deepEqual(yield* fs.readFile(filePath), new Uint8Array([5, 6, 7, 8]));
    const beforeRejectedEdit = (yield* claimedFiles).length;
    const rejectedEdit = yield* ThreadMessageIntake.dispatchCommand({
      type: "queued-run.edit",
      commandId: CommandId.make("intake-edit-rejected"),
      threadId: input.threadId,
      runId: queuedRun.id,
      text: "",
      attachments: [file],
    }).pipe(Effect.flip);
    assert.equal(rejectedEdit._tag, "OrchestratorCommandRejectedError");
    assert.equal((yield* claimedFiles).length, beforeRejectedEdit);
    assert.deepEqual(yield* fs.readFile(filePath), new Uint8Array([5, 6, 7, 8]));
    const send = ThreadMessageIntake.sendToThread({
      commandId: CommandId.make("intake-send"),
      projectId,
      threadId: input.threadId,
      messageId: MessageId.make("intake-third"),
      text: "Third",
      attachments: [attachment],
      mode: "queue",
      createdBy: "agent",
      creationSource: "mcp",
    });
    yield* send;
    yield* send;
    assert.equal((yield* claimedFiles).length, 4);
    const final = yield* threads.getThreadProjection(input.threadId);
    assert.equal(final.messages.length, 3);
    for (const message of final.messages) {
      const path = resolveAttachmentPath({
        attachmentsDir: config.attachmentsDir,
        attachment: message.attachments[0]!,
      });
      assert.isNotNull(path);
      assert.deepEqual(yield* fs.readFile(path), new Uint8Array([1, 2, 3, 4]));
    }
  }).pipe(Effect.provide(Layer.mergeAll(harness.layer, files)));
});

it.effect(
  "replays only the launch initial-message witness and preserves edited accepted files",
  () => {
    const harness = makeHarness();
    const files = ServerConfig.layerTest(process.cwd(), { prefix: "t3-launch-witness-" }).pipe(
      Layer.provideMerge(NodeServices.layer),
    );
    return Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const fs = yield* FileSystem.FileSystem;
      const sql = yield* SqlClient.SqlClient;
      const pendingId = createPendingAttachmentId();
      assert.isNotNull(pendingId);
      const attachment: ChatAttachment = {
        type: "image",
        id: ChatAttachmentId.make(pendingId),
        name: "original.png",
        mimeType: "image/png",
        sizeBytes: 3,
      };
      const pendingPath = resolveAttachmentPath({
        attachmentsDir: config.attachmentsDir,
        attachment,
      });
      assert.isNotNull(pendingPath);
      yield* fs.makeDirectory(config.attachmentsDir, { recursive: true });
      yield* fs.writeFile(pendingPath, new Uint8Array([1, 2, 3]));
      const input = {
        ...launchInput({ command: "launch-witness", thread: "thread-launch-witness" }),
        initialMessage: { text: "Attached", attachments: [attachment] },
      };
      const first = yield* ThreadMessageIntake.launchThread(input);
      const stored = first.projection.messages[0];
      assert.isDefined(stored);
      const acceptedPath = resolveAttachmentPath({
        attachmentsDir: config.attachmentsDir,
        attachment: stored.attachments[0]!,
      });
      assert.isNotNull(acceptedPath);
      yield* fs.writeFile(acceptedPath, new Uint8Array([9, 8, 7, 6]));
      const rows = yield* sql<{
        command_id: string;
      }>`SELECT command_id FROM command_normalization_witnesses ORDER BY command_id`;
      assert.deepEqual(
        rows.map((row) => row.command_id),
        [`${input.commandId}:initial-message`],
      );
      yield* fs.writeFile(pendingPath, new Uint8Array([3, 2, 1]));
      const changedPending = yield* ThreadMessageIntake.launchThread(input).pipe(Effect.flip);
      assert.equal(changedPending._tag, "OrchestratorCommandIdConflictError");
      yield* fs.remove(pendingPath);
      const replayed = yield* ThreadMessageIntake.launchThread(input);
      assert.equal(replayed.projection.messages[0]?.id, stored.id);
      assert.deepEqual(replayed.projection.messages[0]?.attachments, stored.attachments);
      assert.deepEqual(yield* fs.readFile(acceptedPath), new Uint8Array([9, 8, 7, 6]));
      assert.lengthOf(yield* fs.readDirectory(config.attachmentsDir), 1);
      const changedCaller = yield* ThreadMessageIntake.launchThread({
        ...input,
        createdBy: "agent",
      }).pipe(Effect.flip);
      assert.equal(changedCaller._tag, "OrchestratorCommandIdConflictError");
      assert.deepEqual(
        yield* sql<{
          command_id: string;
        }>`SELECT command_id FROM command_normalization_witnesses ORDER BY command_id`,
        rows,
      );
    }).pipe(Effect.provide(Layer.mergeAll(harness.layer, files)));
  },
);

it.effect("cancels tracked setup before provider work is released", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const input = launchInput({
        command: "launch:cancel-tracked",
        thread: "thread:cancel-tracked",
        message: "Start",
        workspace: { type: "worktree", baseRef: "main" },
      });
      const launched = yield* launches.launch(input);
      yield* Deferred.await(entered);
      assert.equal((yield* tracker.get(launched.threadId))?.phase, "running");
      assert.isTrue(yield* tracker.cancel(launched.threadId));
      assert.equal((yield* tracker.get(launched.threadId))?.phase, "cancelled");
      const projection = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(projection.runs[0]?.status, "failed");
      assert.isNull(projection.thread.worktreePath);
      assert.isEmpty(yield* outbox.listByCommandId(CommandId.make(`${input.commandId}:release`)));
    }).pipe(Effect.provide(harness.layer));
  }),
);

for (const exitCode of [0, 1]) {
  it.effect(`releases an async setup before its completion with exit ${exitCode}`, () =>
    Effect.gen(function* () {
      const completion = yield* Deferred.make<{ exitCode: number | null; durationMs: number }>();
      const harness = makeHarness({
        runSetup: (setupInput) =>
          Effect.succeed({
            status: "started" as const,
            async: true,
            scriptId: "setup",
            scriptName: "Setup",
            scriptCommand: "vp install",
            terminalId: "setup",
            cwd: setupInput.worktreePath,
            completion: Deferred.await(completion),
          }),
      });
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
        const launched = yield* launches.launch(
          launchInput({
            command: `command:launch:async-${exitCode}`,
            thread: `thread:launch:async-${exitCode}`,
            message: "Start during setup",
            workspace: { type: "worktree", baseRef: "main" },
          }),
        );
        yield* tracker.stream(launched.threadId).pipe(
          Stream.filter(
            (snapshot) =>
              snapshot?.stages.some((stage) => stage.id === "agent" && stage.status === "done") ===
              true,
          ),
          Stream.runHead,
        );
        const running = yield* threads.getThreadProjection(launched.threadId);
        assert.equal(running.runs[0]?.status, "starting");
        assert.equal((yield* tracker.get(launched.threadId))?.phase, "running");
        yield* Deferred.succeed(completion, { exitCode, durationMs: 1 });
        yield* tracker.stream(launched.threadId).pipe(
          Stream.filter((snapshot) => snapshot?.phase === "done"),
          Stream.runHead,
        );
        const settled = yield* tracker.get(launched.threadId);
        assert.equal(
          settled?.stages.find((stage) => stage.id === "setup-script")?.status,
          exitCode === 0 ? "done" : "failed",
        );
        assert.equal(
          (yield* threads.getThreadProjection(launched.threadId)).runs[0]?.status,
          "starting",
        );
      }).pipe(Effect.provide(harness.layer));
    }),
  );
}

it.effect(
  "qualified rename preserves original claims and release replay while new admissions capture the renamed target",
  () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const sink = yield* EventSink.EventSinkV2;
      const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const sql = yield* SqlClient.SqlClient;
      const input = launchInput({
        command: "launch:qualified-rename",
        thread: "thread:qualified-rename",
        message: "Prepare the feature",
        workspace: { type: "worktree", baseRef: "main", branch: "t3code/abcd1234" },
      });
      const launched = yield* launches.launch(input);
      const terminal = yield* tracker.stream(launched.threadId).pipe(
        Stream.filter((snapshot) => snapshot !== null && snapshot.phase !== "running"),
        Stream.runHead,
        Effect.map(Option.getOrThrow),
      );
      assert.equal(terminal?.phase, "done");
      const projection = yield* threads.getThreadProjection(launched.threadId);
      const run = projection.runs[0]!;
      const admission = yield* threads.readOrdinaryCheckoutAdmissionForRun({
        threadId: launched.threadId,
        runId: run.id,
      });
      assert.isNotNull(admission);
      assert.equal(admission!.capture.branch, "t3code/abcd1234");
      const originalAdmission = yield* encodeUnknownJson(admission);
      const record = yield* sink.readOrdinaryCheckoutUse(`${input.commandId}:ordinary-preparation`);
      assert.isNotNull(record);
      const use = record!.subject.use;
      const history = yield* sink.readOrdinaryCheckoutExecutionAssociations(use);
      const producer = history.participants[0]!;
      assert.equal(producer.state, "retired");
      assert.equal(producer.ref.originalUse.lease.branch, "t3code/abcd1234");
      assert.equal(history.facts.at(-1)?.eventKind, "retire");
      const current = yield* sink.resolveOrdinaryCheckoutLease(use.lease);
      assert.equal(current.branch, "generated-branch");
      assert.equal(current.leaseId, use.lease.leaseId);
      assert.equal(current.ownerIncarnation, use.lease.ownerIncarnation);
      assert.equal(current.resourcePath, use.lease.resourcePath);
      assert.equal(current.acquiredAtMs, use.lease.acquiredAtMs);
      assert.equal(projection.thread.branch, current.branch);
      const transitions = yield* sql<{
        readonly transition_json: string;
      }>`SELECT transition_json FROM orchestration_v2_ordinary_checkout_target_transitions WHERE operation_id = ${use.operationId}`;
      assert.lengthOf(transitions, 1);
      const release = {
        type: "prepared-run.release" as const,
        commandId: CommandId.make(`${input.commandId}:release`),
        threadId: launched.threadId,
        runId: run.id,
      };
      const receipt = Option.getOrThrow(
        yield* (yield* CommandReceiptStore.CommandReceiptStoreV2).getByCommandId(release.commandId),
      );
      assert.equal(
        (yield* threads.dispatchOrdinaryPreparedRunRelease(release, use, producer.ref)).sequence,
        receipt.resultSequence,
      );
      assert.equal(harness.renameBranch.mock.calls.length, 1);
      const effect = Option.getOrThrow(
        yield* outbox.claimNext({ workerId: "rename-regression", leaseDurationMs: 60_000 }),
      );
      assert.equal(effect.request.type, "provider-turn.start");
      assert.equal(effect.commandId, release.commandId);
      const link = yield* sink.readOrdinaryCheckoutEffectLink(effect.id);
      assert.isNotNull(link);
      const execution = yield* sink.joinOrdinaryCheckoutClaim({
        originalUse: use,
        predecessorExecution: producer.ref,
        claim: {
          kind: "outbox",
          link: link!,
          workerId: "rename-regression",
          expectedAttempt: effect.attemptCount,
          leaseExpiresAt: DateTime.makeUnsafe(effect.leaseExpiresAt!),
        },
      });
      assert.deepEqual(
        (yield* sink.revalidateOrdinaryCheckoutExecution(execution)).originalUse,
        use,
      );
      const now = yield* DateTime.now;
      yield* sink.renewOrdinaryCheckoutExecution({
        ref: execution,
        now,
        newExpiry: DateTime.add(now, { minutes: 5 }),
        expectedClaimExpiry: DateTime.makeUnsafe(effect.leaseExpiresAt!),
      });
      const followupCommand = CommandId.make("command:qualified-rename:followup");
      yield* threads.sendToThread({
        projectId,
        commandId: followupCommand,
        threadId: launched.threadId,
        messageId: MessageId.make("message:qualified-rename:followup"),
        text: "Continue",
        attachments: [],
        mode: "queue",
        createdBy: "user",
        creationSource: "web",
      });
      const followup = yield* sink.readOrdinaryCheckoutAdmission({
        commandId: followupCommand,
        threadId: launched.threadId,
      });
      assert.equal(followup?.capture.branch, "generated-branch");
      assert.equal(followup?.capture.lease.leaseId, use.lease.leaseId);
      assert.equal(
        yield* encodeUnknownJson(
          yield* threads.readOrdinaryCheckoutAdmissionForRun({
            threadId: launched.threadId,
            runId: run.id,
          }),
        ),
        originalAdmission,
      );
      assert.equal(
        (yield* sink
          .readOrdinaryCheckoutExecutionAssociations({
            ...use,
            lease: { ...use.lease, branch: "generated-branch" },
          })
          .pipe(Effect.result))._tag,
        "Failure",
      );
      const commitAttempted = yield* Ref.make(false);
      const forged: Extract<
        ThreadLaunch.OrdinaryPreparedPhysicalResultV1,
        { kind: "prepared_branch_renamed" }
      > = {
        version: 1,
        kind: "prepared_branch_renamed",
        producerId:
          producer.ref.executor.kind === "actual_prepared_producer"
            ? producer.ref.executor.producerId
            : "foreign",
        execution: producer.ref,
        targetSource: record!.subject.source,
        checkoutPath: use.lease.resourcePath,
        observedAt: DateTime.formatIso(now),
        oldBranch: "t3code/abcd1234",
        requestedBranch: "generated-branch",
        renamedBranch: "generated-branch",
        readback: { cwd: use.lease.resourcePath, refName: "generated-branch" },
      };
      assert.equal(
        (yield* sink
          .transitionOrdinaryPreparedBranch({
            observation: forged,
            commandId: CommandId.make(`${input.commandId}:branch-rename`),
            commitMetadata: Ref.set(commitAttempted, true).pipe(
              Effect.as({ sequence: 0, storedEvents: [] }),
            ),
          })
          .pipe(Effect.result))._tag,
        "Failure",
      );
      assert.isFalse(yield* Ref.get(commitAttempted));
      yield* threads.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("command:rename:unproved-edit"),
        threadId: launched.threadId,
        branch: "unproved",
      });
      assert.equal(
        (yield* sink.revalidateOrdinaryCheckoutExecution(execution).pipe(Effect.result))._tag,
        "Failure",
      );
    }).pipe(Effect.provide(harness.layer));
  },
);

for (const workspace of [
  { type: "root" as const },
  { type: "worktree" as const, baseRef: "main", branch: "feature" },
])
  it.effect(
    `runless ${workspace.type} preparation consumes its original completion and releases its reservation`,
    () => {
      const harness = makeHarness();
      return Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
        const sink = yield* EventSink.EventSinkV2;
        const input = launchInput({
          command: "launch:runless-completion",
          thread: "thread:runless-completion",
          workspace,
        });
        const launched = yield* launches.launch(input);
        if (workspace.type === "worktree") {
          const terminal = yield* tracker.stream(launched.threadId).pipe(
            Stream.filter((snapshot) => snapshot !== null && snapshot.phase !== "running"),
            Stream.runHead,
            Effect.map(Option.getOrThrow),
          );
          assert.equal(terminal?.phase, "done");
        }
        // Root preparation has no setup tracker; its durable use records the end.
        yield* waitUntil(() =>
          sink
            .readOrdinaryCheckoutUse(`${input.commandId}:ordinary-preparation`)
            .pipe(
              Effect.map(
                (current) =>
                  current !== null && current.state !== "reserved" && current.state !== "started",
              ),
            ),
        );
        const record = yield* sink.readOrdinaryCheckoutUse(
          `${input.commandId}:ordinary-preparation`,
        );
        assert.equal(record?.state, "released");
        assert.equal(
          (yield* sink.readOrdinaryCheckoutExecutionAssociations(record!.subject.use))
            .participants[0]?.state,
          "retired",
        );
      }).pipe(Effect.provide(harness.layer));
    },
  );

it.effect(
  "cancellation after rename entry retains uncertainty without publishing an unproved target",
  () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const harness = makeHarness({
        renameBranch: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
        runSetup: () => Effect.never,
      });
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
        const sink = yield* EventSink.EventSinkV2;
        const sql = yield* SqlClient.SqlClient;
        const input = launchInput({
          command: "launch:rename-cancelled",
          thread: "thread:rename-cancelled",
          message: "Prepare",
          workspace: { type: "worktree", baseRef: "main", branch: "t3code/abcd1234" },
        });
        const launched = yield* launches.launch(input);
        yield* Deferred.await(entered);
        assert.isTrue(yield* tracker.cancel(launched.threadId));
        const projection = yield* threads.getThreadProjection(launched.threadId);
        assert.equal(projection.thread.branch, "t3code/abcd1234");
        assert.isNotNull(projection.thread.worktreePath);
        const record = yield* sink.readOrdinaryCheckoutUse(
          `${input.commandId}:ordinary-preparation`,
        );
        assert.equal(record?.state, "unknown");
        const transitions =
          yield* sql`SELECT operation_id FROM orchestration_v2_ordinary_checkout_target_transitions WHERE operation_id = ${record!.subject.use.operationId}`;
        assert.isEmpty(transitions);
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect(
  "failure before checkout entry clears only the uncreated projection and preserves the accepted target",
  () => {
    const harness = makeHarness({ hasCommit: () => Effect.succeed(false) });
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const sink = yield* EventSink.EventSinkV2;
      const input = launchInput({
        command: "launch:uncreated-target",
        thread: "thread:uncreated-target",
        message: "Prepare",
        workspace: { type: "worktree", branch: "feature", baseRef: "main" },
      });
      const launched = yield* launches.launch(input);
      yield* threads.streamStoredEventsFrom({ threadId: launched.threadId }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "run.updated" && stored.event.payload.status === "failed",
        ),
        Stream.runHead,
      );
      const failed = yield* threads.getThreadProjection(launched.threadId);
      assert.isNull(failed.thread.worktreePath);
      assert.equal(harness.createWorktree.mock.calls.length, 0);
      const admission = yield* sink.readOrdinaryCheckoutAdmissionForRun({
        threadId: launched.threadId,
        runId: failed.runs[0]!.id,
      });
      assert.isNotNull(admission);
      assert.equal(
        admission!.capture.canonicalCheckoutPath,
        launched.projection.thread.worktreePath,
      );
      const replay = yield* launches.launch(input);
      assert.isTrue(replay.resumed);
      assert.equal(replay.projection.runs[0]?.id, failed.runs[0]?.id);
      assert.isNull(replay.projection.thread.worktreePath);
      assert.equal(harness.createWorktree.mock.calls.length, 0);
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect(
  "retires a setup start failure as qualified only from its own checkout and terminal readback",
  () => {
    const harness = makeHarness({
      runSetup: (setupInput) =>
        Effect.fail(
          new ProjectSetupScriptRunner.ProjectSetupScriptOperationError({
            threadId: setupInput.threadId,
            worktreePath: setupInput.worktreePath,
            operation: "writeCommand",
            cause: new Error("setup could not start"),
          }),
        ),
    });
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const sink = yield* EventSink.EventSinkV2;
      const input = launchInput({
        command: "launch:qualified-setup-failure",
        thread: "thread:qualified-setup-failure",
        message: "Prepare",
        workspace: { type: "worktree", branch: "feature", baseRef: "main" },
      });
      const launched = yield* launches.launch(input);
      yield* threads.streamStoredEventsFrom({ threadId: launched.threadId }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "run.updated" && stored.event.payload.status === "failed",
        ),
        Stream.runHead,
      );
      const record = yield* sink.readOrdinaryCheckoutUse(`${input.commandId}:ordinary-preparation`);
      assert.equal(record?.state, "released");
      const history = yield* sink.readOrdinaryCheckoutExecutionAssociations(record!.subject.use);
      assert.lengthOf(history.participants, 1);
      assert.equal(history.participants[0]?.state, "retired");
      const retired = history.facts.at(-1);
      const outcome =
        retired?.evidence.schema === "t3.ordinary-checkout-execution-outcome/v1"
          ? retired.evidence.actualProducerOutcome
          : undefined;
      assert.equal(outcome?.kind, "prepared_failed");
      if (outcome?.kind === "prepared_failed") {
        assert.equal(outcome.observation.worktree?.path, launched.projection.thread.worktreePath);
        assert.equal(outcome.observation.readback.refName, "feature");
        assert.equal(outcome.observation.setup.targetCount, 0);
        assert.include(outcome.observation.failure, "setup could not start");
      }
      const failed = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(failed.runs[0]?.status, "failed");
      assert.equal(failed.thread.worktreePath, launched.projection.thread.worktreePath);
      const replay = yield* launches.launch(input);
      assert.isTrue(replay.resumed);
      assert.equal(replay.projection.runs[0]?.id, failed.runs[0]?.id);
      assert.equal(harness.createWorktree.mock.calls.length, 1);
      assert.equal(harness.runSetup.mock.calls.length, 1);
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect(
  "keeps a failed preparation unknown when a managed terminal remains and rejects a forged failure",
  () =>
    Effect.gen(function* () {
      const setupEntered = yield* Deferred.make<void>();
      const failSetup = yield* Deferred.make<void>();
      const harness = makeHarness({
        runSetup: (setupInput) =>
          Deferred.succeed(setupEntered, undefined).pipe(
            Effect.andThen(Deferred.await(failSetup)),
            Effect.andThen(
              Effect.fail(
                new ProjectSetupScriptRunner.ProjectSetupScriptOperationError({
                  threadId: setupInput.threadId,
                  worktreePath: setupInput.worktreePath,
                  operation: "writeCommand",
                  cause: new Error("setup failed"),
                }),
              ),
            ),
          ),
        captureOwnedTargets: (target) =>
          Effect.succeed({
            managerId: "thread-launch-test-terminals",
            threadId: target.threadId,
            ownerBirth: target.ownerBirth,
            status: "captured" as const,
            managedTargetsOnly: true as const,
            targets: [
              {
                threadId: target.threadId,
                terminalId: "setup-left-open",
                handleId: "handle:setup-left-open",
                ownerBirth: target.ownerBirth,
              },
            ],
          }),
      });
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const sink = yield* EventSink.EventSinkV2;
        const input = launchInput({
          command: "launch:held-setup-failure",
          thread: "thread:held-setup-failure",
          message: "Prepare",
          workspace: { type: "worktree", branch: "feature", baseRef: "main" },
        });
        const launched = yield* launches.launch(input);
        yield* Deferred.await(setupEntered);
        const record = yield* sink.readOrdinaryCheckoutUse(
          `${input.commandId}:ordinary-preparation`,
        );
        assert.equal(record?.state, "started");
        const history = yield* sink.readOrdinaryCheckoutExecutionAssociations(record!.subject.use);
        const producer = history.participants[0]!;
        const admission = yield* threads.readOrdinaryCheckoutAdmissionForRun({
          threadId: launched.threadId,
          runId: launched.projection.runs[0]!.id,
        });
        assert.isNotNull(admission);
        const checkoutPath = admission!.capture.canonicalCheckoutPath;
        const forged: Extract<
          EventSink.OrdinaryCheckoutExecutorOutcomeV1,
          { kind: "prepared_failed" }
        > = {
          kind: "prepared_failed",
          observation: {
            version: 1,
            kind: "prepared_failure_observed",
            producerId:
              producer.ref.executor.kind === "actual_prepared_producer"
                ? producer.ref.executor.producerId
                : "foreign",
            execution: producer.ref,
            targetSource: record!.subject.source,
            checkoutPath,
            observedAt: DateTime.formatIso(yield* DateTime.now),
            branch: "feature",
            readback: { cwd: checkoutPath, refName: "feature", isRepo: true },
            worktree: null,
            failure: "forged failure",
            setup: {
              status: "no_managed_process",
              managerId: "forged",
              ownerBirth: admission!.capture.applicationBirth,
              targetCount: 0,
            },
          },
        };
        const rejected = yield* sink
          .recordOrdinaryCheckoutExecutorOutcome({
            ref: producer.ref,
            actualProducerOutcome: forged,
            revalidateProducer: Effect.void,
          })
          .pipe(Effect.flip);
        assert.include(rejected.message, "retained producer issuer");
        assert.equal(
          (yield* sink
            .completeOrdinaryCheckoutUse({
              originalUse: record!.subject.use,
              expectedAssociationOrdinal: history.latestOrdinal,
              completionEvidence: { ref: producer.ref, actualProducerOutcome: forged },
            })
            .pipe(Effect.result))._tag,
          "Failure",
        );
        assert.equal(
          (yield* sink.readOrdinaryCheckoutUse(`${input.commandId}:ordinary-preparation`))?.state,
          "started",
        );
        yield* Deferred.succeed(failSetup, undefined);
        yield* threads.streamStoredEventsFrom({ threadId: launched.threadId }).pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "run.updated" && stored.event.payload.status === "failed",
          ),
          Stream.runHead,
        );
        const held = yield* sink.readOrdinaryCheckoutUse(`${input.commandId}:ordinary-preparation`);
        assert.equal(held?.state, "unknown");
        assert.equal(
          (yield* sink.readOrdinaryCheckoutExecutionAssociations(held!.subject.use)).participants[0]
            ?.state,
          "unknown",
        );
      }).pipe(Effect.provide(harness.layer));
    }),
);
