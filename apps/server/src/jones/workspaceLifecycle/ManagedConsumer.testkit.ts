import * as NativeGuard from "../nativeCreation/NativeCreationProviderGuard.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EnvironmentId,
  EventId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2Command,
} from "@t3tools/contracts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as Adapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";
import * as CheckpointStore from "../../checkpointing/CheckpointStore.ts";
import * as Config from "../../config.ts";
import * as Environment from "../../environment/ServerEnvironment.ts";
import * as GitWorkflow from "../../git/GitWorkflowService.ts";
import * as McpTestkit from "../../mcp/McpSessionRegistry.testkit.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as Runtime from "../../orchestration-v2/runtimeLayer.ts";
import * as ThreadCommandExecutor from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as Registry from "../../provider/ProviderInstanceRegistry.ts";
import * as Runner from "../../processRunner.ts";
import * as Settings from "../../serverSettings.ts";
import * as SourceControl from "../../sourceControl/SourceControlProviderRegistry.ts";
import * as Git from "../../vcs/GitVcsDriver.ts";
import * as VcsRegistry from "../../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../../vcs/VcsProcess.ts";
import { canonical } from "../ordinaryCheckout/CheckoutProtocol.ts";
import * as Restore from "../settlementRestore/SettlementRestore.ts";
import * as LifecycleRuntime from "./RuntimeIntegration.ts";
import { Binding, type InventoryEntry } from "./Protocol.ts";

export const selection: ModelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "synthetic",
};
export const threadId = ThreadId.make("managed-thread");
export const projectId = ProjectId.make("managed-project");
const driver = ProviderDriverKind.make("codex");

// All durable native state and physical Git targets are synthetic and scope-owned.
// Only provider/registry effects are fixtures; production native issuers and consumers stay real.
export const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "managed-consumer-" });
  const repo = path.join(root, "repo");
  const config = yield* Config.ServerConfig.pipe(
    Effect.provide(Config.layerTest(repo, path.join(root, "state"))),
  );
  const linked = path.join(config.worktreesDir, "project", "linked");
  const templates = path.join(root, "templates");
  yield* fs.makeDirectory(repo);
  yield* fs.makeDirectory(templates);
  yield* fs.makeDirectory(path.dirname(linked), { recursive: true });
  const configLayer = Config.layer(config);
  const git = yield* Git.GitVcsDriver.pipe(
    Effect.provide(Git.layer.pipe(Layer.provide(configLayer))),
  );
  const runGit = (args: readonly string[], cwd = repo) =>
    git.execute({
      operation: "WorkspaceLifecycle.managedConsumerTest",
      cwd,
      args: [
        "-c",
        "user.name=Synthetic",
        "-c",
        "user.email=synthetic@example.invalid",
        "-c",
        `core.hooksPath=${templates}`,
        ...args,
      ],
      env: { GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null" },
    });
  yield* runGit(["init", "--template", templates, "-b", "main"]);
  yield* runGit(["commit", "--allow-empty", "-m", "synthetic"]);
  yield* runGit(["worktree", "add", "-b", "work", linked]);
  const calls: string[] = [];
  let view: InventoryEntry = {
    worktree: linked,
    generation: null,
    binding: null,
    assessment: null,
    disposition: "unknown",
    assessment_status: "absent",
    stored_readiness: null,
    physical_readiness: "unknown",
    binding_evidence: "caller_asserted",
    authority_effect: "none",
  };
  const state = {
    beforeRegistryRead: Effect.void as Effect.Effect<void>,
    afterResume: Effect.void as Effect.Effect<void>,
    beforeProviderUse: Effect.void as Effect.Effect<void>,
    loseResponse: false,
    resumes: 0,
    view: () => view,
    assess: () => {
      if (view.binding === null || view.generation === null)
        throw new Error("No binding to assess");
      view = {
        ...view,
        disposition: "cleanup_candidate",
        assessment_status: "current",
        assessment: {
          schema: "workspace-assessment-record/v1",
          generation: view.generation,
          recorded_at: "2026-10-24T00:00:00Z",
          binding_evidence: "caller_asserted",
          authority_effect: "none",
          assessment: {
            schema: "workspace-assessment/v1",
            binding: view.binding,
            role: "authoring",
            disposition: "cleanup_candidate",
            remaining_work: [],
            preservation_refs: ["synthetic-preserved-commit"],
            handoff_refs: [],
            artifact_destinations: [],
            unresolved_obligations: [],
          },
        },
      };
    },
  };
  const runner = Runner.ProcessRunner.of({
    run: (input) =>
      Effect.gen(function* () {
        const resume = input.args.includes("resume");
        calls.push(resume ? "cas" : "show");
        if (resume) {
          state.resumes++;
          view = {
            ...view,
            binding: yield* Schema.decodeEffect(Schema.fromJsonString(Binding))(input.stdin!).pipe(
              Effect.orDie,
            ),
            generation: `cas:${state.resumes}`,
            assessment: null,
            assessment_status: "stale",
            disposition: "unknown",
          };
          yield* state.afterResume;
        } else yield* state.beforeRegistryRead;
        return {
          code: (resume && state.loseResponse ? 1 : 0) as Runner.ProcessRunOutput["code"],
          stdout: canonical(
            resume
              ? {
                  schema: "workspace-lifecycle-cli/v1",
                  command: "resume",
                  result: {
                    schema: "workspace-write/v1",
                    outcome: "resumed",
                    generation: view.generation,
                    authority_effect: "none",
                  },
                }
              : {
                  schema: "workspace-lifecycle-cli/v1",
                  command: "show",
                  result: {
                    schema: "workspace-inventory/v1",
                    outcome: "unknown",
                    results: [view],
                    next_after: null,
                    evidence: "stored_only",
                    authority_effect: "none",
                  },
                },
          ),
          stderr: "",
          timedOut: false,
          stdoutTruncated: false,
          stderrTruncated: false,
          stdoutInvalidUtf8: false,
          stderrInvalidUtf8: false,
        };
      }),
  });
  const adapter: Adapter.ProviderAdapterV2Shape = {
    instanceId: selection.instanceId,
    driver,
    nativeCreationExecution: true,
    getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (input) =>
      Effect.gen(function* () {
        const generation = input.runtimeLifecycle
          ? yield* input.runtimeLifecycle.reserve(input.threadId)
          : undefined;
        if (input.runtimeLifecycle && generation)
          yield* input.runtimeLifecycle.admit({
            runtimeGeneration: generation,
            threadId: input.threadId,
          });
        calls.push("provider-open");
        const now = yield* DateTime.now;
        return {
          instanceId: selection.instanceId,
          driver,
          providerSessionId: input.providerSessionId,
          providerSession: {
            id: input.providerSessionId,
            providerInstanceId: selection.instanceId,
            driver,
            status: "ready",
            cwd: linked,
            model: selection.model,
            capabilities: CodexProviderCapabilitiesV2,
            createdAt: now,
            updatedAt: now,
            lastError: null,
          },
          events: Stream.never,
          ensureThread: (request) =>
            Effect.gen(function* () {
              if (input.runtimeLifecycle && generation)
                yield* input.runtimeLifecycle.admit({
                  runtimeGeneration: generation,
                  threadId: request.threadId,
                });
              calls.push("provider-ensure");
              if (!request.existingProviderThread)
                return yield* Effect.die("Missing synthetic provider thread");
              const providerThread = {
                ...request.existingProviderThread,
                nativeThreadRef: {
                  driver,
                  nativeId: "synthetic-native-thread",
                  strength: "strong" as const,
                },
                status: "idle" as const,
              };
              return input.runtimeLifecycle && generation
                ? yield* input.runtimeLifecycle.bind({
                    runtimeGeneration: generation,
                    providerThread,
                    requested: Adapter.requestedRuntimeIdentity(request.modelSelection, driver),
                    observed: Adapter.unobservedRuntimeIdentity(),
                  })
                : providerThread;
            }).pipe(Effect.orDie),
          resumeThread: (request) =>
            Effect.sync(() => {
              calls.push("provider-resume");
              return request.providerThread;
            }),
          startTurn: (turn) =>
            state.beforeProviderUse.pipe(
              Effect.andThen(
                Effect.sync(() => {
                  calls.push("provider-send");
                }),
              ),
              Effect.andThen(
                NativeGuard.acknowledgeNativeProviderGuard(turn.nativeCreationGuard, {
                  threadId: turn.threadId,
                  runId: turn.runId,
                  providerInstanceId: selection.instanceId,
                  attemptId: turn.attemptId,
                  providerThread: turn.providerThread,
                  runtimeGeneration: generation ?? "missing-generation",
                }),
              ),
              Effect.mapError(
                (cause) =>
                  new Adapter.ProviderAdapterProtocolError({
                    driver,
                    detail: "Synthetic native acknowledgement failed",
                    cause,
                  }),
              ),
            ),
          steerTurn: () => Effect.void,
          interruptTurn: () => Effect.void,
          unloadThread: () => Effect.void,
          forkThread: () => Effect.die("Unused fixture fork"),
          respondToRuntimeRequest: () => Effect.void,
          readThreadSnapshot: () => Effect.die("Unused fixture read"),
          rollbackThread: () => Effect.die("Unused fixture rollback"),
        } satisfies Adapter.ProviderAdapterV2SessionRuntime;
      }),
  };
  const instance: ProviderInstance = {
    instanceId: selection.instanceId,
    driverKind: driver,
    continuationIdentity: { driverKind: driver, continuationKey: "synthetic" },
    displayName: "Synthetic",
    enabled: true,
    snapshot: {
      getSnapshot: Effect.succeed({ models: [] }),
    } as unknown as ProviderInstance["snapshot"],
    orchestrationAdapter: adapter,
    textGeneration: {} as ProviderInstance["textGeneration"],
  };
  const settings = {
    environmentId: "synthetic-environment",
    projects: [{ projectId, repository: "Jones-Systems/Synthetic", projectRoot: repo }],
    registry: {
      executable: "/qualified/v3",
      arguments: [],
      database: path.join(root, "existing.sqlite"),
      cwd: root,
    },
  };
  const platform = Layer.merge(
    NodeServices.layer,
    Layer.mock(SourceControl.SourceControlProviderRegistry)({
      resolveLink: () => Effect.die("Unused title link"),
    }),
  );
  const configured = Config.layer({ ...config, workspaceLifecycle: settings });
  const database = Sqlite.layerMemory;
  const restartLifecycle = LifecycleRuntime.layer.pipe(
    Layer.provide(configured),
    Layer.provide(
      Layer.succeed(Environment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(EnvironmentId.make(settings.environmentId)),
      }),
    ),
    Layer.provide(Layer.succeed(Runner.ProcessRunner, runner)),
    Layer.provide(Layer.succeed(Git.GitVcsDriver, git)),
    Layer.provide(platform),
  );
  const vcs = VcsRegistry.layer.pipe(
    Layer.provide(VcsProcess.layer),
    Layer.provide(configured),
    Layer.provide(platform),
  );
  const runtime = Layer.mergeAll(
    Runtime.layer,
    Runtime.layerEventSink,
    ProjectStore.layer,
    ProjectionStore.layer,
    Outbox.layer,
    ThreadCommandExecutor.layer,
  ).pipe(
    Layer.provide(McpTestkit.layer),
    Layer.provide(McpProviderSessions.layer),
    Layer.provideMerge(LifecycleRuntime.layer),
    Layer.provideMerge(database),
    Layer.provide(CheckpointStore.layer.pipe(Layer.provide(vcs))),
    Layer.provide(configured),
    Layer.provide(Settings.layerTest()),
    Layer.provide(
      Layer.succeed(Environment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(EnvironmentId.make(settings.environmentId)),
      }),
    ),
    Layer.provide(Layer.succeed(Runner.ProcessRunner, runner)),
    Layer.provide(Layer.succeed(Git.GitVcsDriver, git)),
    Layer.provide(
      Layer.succeed(Registry.ProviderInstanceRegistry, {
        getInstance: (id) => Effect.succeed(id === selection.instanceId ? instance : undefined),
        listInstances: Effect.succeed([instance]),
        listUnavailable: Effect.succeed([]),
        streamChanges: Stream.empty,
        subscribeChanges: Effect.never,
      }),
    ),
    Layer.provide(
      Layer.mock(GitWorkflow.GitWorkflowService)({
        pruneWorktrees: () =>
          Effect.sync(() => {
            calls.push("git-prune");
          }).pipe(Effect.andThen(runGit(["worktree", "prune"])), Effect.asVoid, Effect.orDie),
        createWorktree: (input) =>
          Effect.sync(() => {
            calls.push("git-create");
          }).pipe(
            Effect.andThen(runGit(["worktree", "add", input.path!, input.refName])),
            Effect.as({ worktree: { path: linked, branch: "work" } } as never),
            Effect.orDie,
          ),
      }),
    ),
    Layer.provide(
      Layer.mock(ProjectService.ProjectService)({
        getById: () => Effect.succeed(Option.some({ id: projectId, workspaceRoot: repo } as never)),
      }),
    ),
    Layer.provide(platform),
  );
  const layer = Restore.layer.pipe(
    Layer.provideMerge(runtime),
    Layer.provide(
      Layer.mock(Environment.ServerEnvironment)({
        getEnvironmentId: Effect.succeed(EnvironmentId.make(settings.environmentId)),
      }),
    ),
  );
  const seedThread = (selectedThreadId = threadId, branch = "work") =>
    Effect.gen(function* () {
      const now = DateTime.formatIso(yield* DateTime.now);
      yield* (yield* ProjectStore.ProjectStoreV2).apply({
        sequence: 0,
        eventId: EventId.make("project-birth"),
        aggregateKind: "project",
        aggregateId: projectId,
        occurredAt: now,
        commandId: null,
        causationEventId: null,
        correlationId: null,
        metadata: {},
        type: "project.created",
        payload: {
          projectId,
          title: "Synthetic managed",
          workspaceRoot: repo,
          defaultModelSelection: selection,
          scripts: [],
          createdAt: now,
          updatedAt: now,
        },
      });
      yield* (yield* Orchestrator.OrchestratorV2).dispatch({
        type: "thread.create",
        commandId: CommandId.make("thread-birth"),
        threadId: selectedThreadId,
        createdBy: "user",
        creationSource: "web",
        projectId,
        title: "Synthetic managed",
        modelSelection: selection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch,
        worktreePath: linked,
      });
    });
  const seed = seedThread();
  const dispatch = (command: OrchestrationV2Command) =>
    Effect.flatMap(Orchestrator.OrchestratorV2, (service) => service.dispatch(command));
  return {
    fs,
    root,
    repo,
    linked,
    config,
    configured,
    git,
    instance,
    layer,
    restartLifecycle,
    seed,
    seedThread,
    dispatch,
    runGit,
    calls,
    state,
    sql: SqlClient.SqlClient,
    sink: EventSink.EventSinkV2,
  };
});
