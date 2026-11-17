// @effect-diagnostics-next-line nodeBuiltinImport:off -- Synthetic Git creates only its shared fixture reserved directory synchronously; that fixture owns exact-root teardown.
import * as NodeFS from "node:fs";
import * as Git from "../../git/GitWorkflowService.ts";
import * as Launch from "./LaunchPreparation.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as ThreadLaunch from "../../orchestration-v2/ThreadLaunchService.ts";
import * as Setup from "../../project/ProjectSetupScriptRunner.ts";
import * as SetupTracker from "../../project/WorktreeSetupTracker.ts";
import * as Clones from "../../project/ProjectCloneTracker.ts";
import * as Terminals from "../../terminal/Manager.ts";
import * as Checkpoints from "../../checkpointing/CheckpointStore.ts";
import * as Projects from "../../project/ProjectService.ts";
import * as Managed from "../../project/ManagedProjectFolders.ts";
import * as Providers from "../../provider/ProviderRegistry.ts";
import * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as NodePath from "@effect/platform-node/NodePath";
import * as HostProcess from "@t3tools/shared/HostProcess";
import {
  GitCommandError,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import { runMigrations } from "../../persistence/Migrations.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Replay from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Native from "../nativeCreation/NativeCreationRepositorySqlite.ts";
import * as Physical from "../nativeCreation/NativeWorkspacePhysicalPorts.ts";
import * as Workspace from "../nativeCreation/NativeCreationWorkspacePreparation.ts";
import * as Shared from "../cleanup/sharedAdmission/Protocol.ts";
import { fixture } from "../cleanup/sharedAdmission/fixtures.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Repository from "./OrdinaryCheckoutSqlite.ts";
import * as Ordinary from "./OrdinaryCheckout.ts";
import * as Targets from "./CheckoutTarget.ts";
import * as Conflicts from "./CheckoutConflictIntegration.ts";
import * as Intent from "./CheckoutIntent.ts";
import * as Preparation from "./CheckoutPreparation.ts";
import { CheckoutError } from "./CheckoutProtocol.ts";

// External Git/provider/target observations are synthetic. The launch custody,
// shared reserve/enter, SQLite, intent and EventSink issuer are actual owners.
export const makeLaunchFixture = Effect.fn("LaunchPreparationFixture.make")(function* (
  options: {
    readonly failCreate?: boolean;
    readonly wrongPath?: boolean;
    readonly setup?: Setup.ProjectSetupScriptRunner["Service"]["runForThread"];
  } = {},
) {
  const f = yield* fixture();
  const sql = f.b.local;
  yield* runMigrations().pipe(Effect.provideService(SqlClient.SqlClient, sql));
  const database = Layer.succeed(SqlClient.SqlClient, sql);
  let observations = 0;
  let publications = 0;
  let branch = "work";
  let creates = 0;
  let renames = 0;
  let reserves = 0;
  const worktreePath = `${f.bPath}/work`;
  const git = Layer.mock(Git.GitWorkflowService)({
    currentBranch: () => Effect.succeed("main"),
    hasCommit: () => Effect.succeed(true),
    resolveWorktreeBaseRemote: () => Effect.succeed(null),
    createWorktree: (input) =>
      Effect.gen(function* () {
        creates++;
        if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
          return yield* Effect.die("Git inside SQL");
        if (options.failCreate)
          return yield* new GitCommandError({
            operation: "createWorktree",
            command: "git",
            cwd: input.cwd,
            detail: "Synthetic Git failure",
          });
        NodeFS.mkdirSync(worktreePath, { recursive: true });
        branch = input.newRefName ?? "work";
        return {
          worktree: {
            path: options.wrongPath ? `${worktreePath}/wrong` : worktreePath,
            refName: branch,
          },
        };
      }),
    renameBranch: (input) =>
      Effect.gen(function* () {
        renames++;
        if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
          return yield* Effect.die("rename inside SQL");
        branch = input.newBranch;
        return { branch };
      }),
  });
  const setup = Layer.mock(Setup.ProjectSetupScriptRunner)({
    runForThread: options.setup ?? (() => Effect.succeed({ status: "no-script" as const })),
  });
  const outside = Effect.gen(function* () {
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* new CheckoutError({ reason: "identity" });
    observations++;
  });
  const observation = Layer.succeed(Targets.CheckoutTargetObservation, {
    directory: (path) =>
      outside.pipe(
        Effect.as({
          realPath: path,
          identity: { version: 1 as const, device: "fixture", inode: path, birth: "1" },
        }),
      ),
    git: (path) =>
      outside.pipe(
        Effect.as({
          root: path,
          commonDirectory: `${f.bPath}/.git`,
          gitDirectory: path === f.bPath ? `${path}/.git` : `${f.bPath}/.git/worktrees/work`,
          commonIdentity: { version: 1 as const, device: "fixture", inode: "common", birth: "1" },
          directoryIdentity: { version: 1 as const, device: "fixture", inode: path, birth: "1" },
          branch: path === f.bPath ? "refs/heads/main" : `refs/heads/${branch}`,
          head: "1111111111111111111111111111111111111111",
          registeredWorktrees: [f.bPath, `${f.bPath}/work`],
        }),
      ),
  });
  const files = Layer.succeed(Physical.NativeWorkspaceFileObservation, {
    inspect: (path) =>
      outside.pipe(
        Effect.mapError(
          (cause) =>
            new Workspace.NativeWorkspaceError({ code: "unavailable", message: cause.message }),
        ),
        Effect.as({ realPath: path, birth: "synthetic", kind: "directory" as const }),
      ),
    readFile: () => Effect.succeed(""),
  });
  const repository = Repository.layer.pipe(Layer.provide(Layer.merge(database, NodePath.layer)));
  const targets = Targets.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        observation,
        NodePath.layer,
        Layer.succeed(HostProcess.HomeDirectory, f.directory),
      ),
    ),
  );
  const conflicts = Conflicts.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        database,
        repository,
        observation,
        files,
        NodePath.layer,
        Native.layer.pipe(Layer.provide(database)),
        Receipts.layer.pipe(Layer.provide(database)),
      ),
    ),
  );
  const shared = Layer.succeedContext(
    f.b.context.pipe(
      Context.add(Shared.CommonJournal, {
        ...f.b.journal,
        reserveScopes: (binding) =>
          Effect.gen(function* () {
            if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
              return yield* Shared.failure("reserve_inside_sql");
            reserves++;
            return yield* f.b.journal.reserveScopes(binding);
          }),
      }),
    ),
  );
  const owner = Launch.layerCustody.pipe(
    Layer.provide(
      Layer.mergeAll(
        database,
        repository,
        shared,
        Layer.succeed(Launch.LaunchPreparationAllocation, {
          allocate: () => Effect.succeed(worktreePath),
        }),
      ),
    ),
  );
  const preparation = Preparation.layer.pipe(
    Layer.provide(Layer.mergeAll(database, repository, targets, conflicts, owner)),
  );
  const intent = Intent.layer.pipe(
    Layer.provide(Layer.mergeAll(database, repository, targets, conflicts, preparation)),
  );
  const store = Layer.effect(
    EventStore.EventStoreV2,
    Effect.gen(function* () {
      const service = yield* EventStore.EventStoreV2;
      return {
        ...service,
        publishCommitted: (events: Parameters<typeof service.publishCommitted>[0]) =>
          Effect.gen(function* () {
            if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
              return yield* Effect.die("published inside local SQL");
            publications += events.length;
            yield* service.publishCommitted(events);
          }),
      };
    }),
  ).pipe(Layer.provide(EventStore.layer));
  const instanceId = ProviderInstanceId.make("codex");
  const adapter = {
    instanceId,
    driver: ProviderDriverKind.make("codex"),
    getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
    openSession: () => Effect.die("No provider execution in receiving-seam tests"),
  } as ProviderAdapterV2Shape;
  const serverSettings = ServerSettings.layerTest();
  const receipts = Receipts.layer.pipe(Layer.provide(database));
  const replay = Replay.layerWithRegistry(
    { name: "checkout-commit" },
    Registry.layerFromAdapters([adapter]),
    {
      databaseLayer: database,
      checkoutIntentLayer: intent,
      eventStoreLayer: store,
      runEffectWorker: false,
    },
  );
  const threads = ThreadManagement.layer.pipe(
    Layer.provide(Layer.mergeAll(replay, serverSettings, receipts)),
  );
  const context = yield* Layer.build(
    Layer.mergeAll(
      database,
      repository,
      owner,
      targets,
      preparation,
      intent,
      serverSettings,
      receipts,
      threads,
      Launch.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            owner,
            preparation,
            intent,
            repository,
            targets,
            observation,
            git,
            setup,
            receipts,
          ),
        ),
      ),
      replay,
    ),
  );
  const launchContext = yield* Layer.build(
    ThreadLaunch.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.succeedContext(context),
          git,
          IdAllocator.layer,
          SetupTracker.layer,
          setup,
          Layer.mock(Clones.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
          Layer.mock(Terminals.TerminalManager)({
            close: () => Effect.die("ordinary material must be retained"),
          }),
          Layer.mock(Checkpoints.CheckpointStore)({ isGitRepository: () => Effect.succeed(true) }),
          Layer.mock(Projects.ProjectService)({
            getById: () =>
              Effect.succeedSome({
                id: ProjectId.make("launch-project"),
                title: "Synthetic",
                workspaceRoot: f.bPath,
                scripts: [],
                repositoryIdentity: null,
                faviconPath: null,
                defaultModelSelection: { instanceId, model: "synthetic" },
                defaultThreadEnvMode: null,
                pinnedAt: null,
                archivedAt: null,
                deletedAt: null,
                createdAt: "2026-11-16T00:00:00.000Z",
                updatedAt: "2026-11-16T00:00:00.000Z",
              }),
          }),
          Layer.mock(Managed.ManagedProjectFolders)({
            namedProjectsRoot: `${f.directory}/named-projects`,
            isScratchProject: () => Effect.succeed(false),
            folderForThread: () => Effect.succeedNone,
          }),
          Layer.mock(Providers.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
          Layer.mock(TextGeneration.TextGeneration)({
            generateBranchName: () => Effect.succeed({ branch: "final" }),
          }),
        ),
      ),
    ),
  );
  return {
    f,
    sql,
    context,
    instanceId,
    worktreePath,
    launch: Context.get(context, Launch.LaunchPreparation),
    threadLaunch: Context.get(launchContext, ThreadLaunch.ThreadLaunchService),
    threads: Context.get(context, ThreadManagement.ThreadManagementService),
    sink: Context.get(context, EventSink.EventSinkV2),
    orchestrator: Context.get(context, Orchestrator.OrchestratorV2),
    checkout: Context.get(context, Ordinary.OrdinaryCheckout),
    preparation: Context.get(context, Preparation.CheckoutPreparation),
    targets: Context.get(context, Targets.CheckoutTarget),
    intent: Context.get(context, Intent.CheckoutIntent),
    observe: () => ({ observations, publications, creates, renames, reserves }),
  };
});
