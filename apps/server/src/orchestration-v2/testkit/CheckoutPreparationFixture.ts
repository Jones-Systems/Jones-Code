import * as NodeServices from "@effect/platform-node/NodeServices";
import type { CommandId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as ServerConfig from "../../config.ts";
import * as GitManager from "../../git/GitManager.ts";
import * as GitWorkflow from "../../git/GitWorkflowService.ts";
import * as PortScanner from "../../preview/PortScanner.ts";
import * as ProcessRunner from "../../processRunner.ts";
import * as ManagedProjectFolders from "../../project/ManagedProjectFolders.ts";
import * as ProjectCloneTracker from "../../project/ProjectCloneTracker.ts";
import * as ProjectEnrichmentService from "../../project/ProjectEnrichmentService.ts";
import * as ProjectFaviconResolver from "../../project/ProjectFaviconResolver.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../../project/ProjectSetupScriptRunner.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import * as T3ProjectFileLoader from "../../project/T3ProjectFileLoader.ts";
import * as WorktreeSetupTracker from "../../project/WorktreeSetupTracker.ts";
import type { makeProviderRegistryLayer } from "../../provider/testUtils/providerRegistryMock.ts";
import * as NativeTelemetryClient from "../../resourceTelemetry/NativeTelemetryClient.ts";
import * as SourceControlProviderRegistry from "../../sourceControl/SourceControlProviderRegistry.ts";
import * as TerminalManager from "../../terminal/Manager.ts";
import * as PtyAdapter from "../../terminal/PtyAdapter.ts";
import * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";
import * as VcsDriverRegistry from "../../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../../vcs/VcsProcess.ts";
import * as WorkspacePaths from "../../workspace/WorkspacePaths.ts";
import * as CommandReceiptStore from "../CommandReceiptStore.ts";
import * as DelegatedCheckoutPlanner from "../DelegatedCheckoutPlanner.ts";
import { planStandaloneBirthPlacement } from "../DelegatedCheckoutPolicy.ts";
import * as EffectOutbox from "../EffectOutbox.ts";
import * as EventSink from "../EventSink.ts";
import { terminalOwnerObservationLive } from "../ResourceCleanupService.ts";
import * as ThreadCommandExecutor from "../ThreadCommandExecutor.ts";
import * as ThreadManagementService from "../ThreadManagementService.ts";
import type { ReplayDelegatedPreparationOwners } from "./ProviderReplayHarness.ts";

/**
 * Real checkout preparation for replay scenarios: the planner and the physical
 * services ThreadLaunchService uses to create, verify and set up a worktree.
 * Terminal control is virtual and refuses any use, so a fixture with a setup
 * script fails instead of spawning a process.
 */
export interface CheckoutPreparationAudit {
  factories: number;
  finalized: number;
  controls: string[];
  ownedDirectories: string[];
  acquisitions: Array<{
    sql: SqlClient.SqlClient;
    sink: EventSink.EventSinkV2["Service"];
    management: ThreadManagementService.ThreadManagementService["Service"];
    projects: ProjectService.ProjectService["Service"];
    terminals: TerminalManager.TerminalManager["Service"];
    receipts: CommandReceiptStore.CommandReceiptStoreV2["Service"];
    outbox: EffectOutbox.EffectOutboxV2["Service"];
  }>;
}

export const makeCheckoutPreparationAudit = (): CheckoutPreparationAudit => ({
  factories: 0,
  finalized: 0,
  controls: [],
  ownedDirectories: [],
  acquisitions: [],
});

export function makeCheckoutPreparationServices(
  owners: ReplayDelegatedPreparationOwners,
  providerRegistryLayer: ReturnType<typeof makeProviderRegistryLayer>,
  audit: CheckoutPreparationAudit = makeCheckoutPreparationAudit(),
) {
  audit.factories++;
  const unexpectedControl = (operation: string) =>
    Effect.sync(() => {
      audit.controls.push(operation);
      throw new Error(`Unexpected delegated no-script virtual terminal control: ${operation}`);
    });
  const common = Layer.mergeAll(
    owners.persistenceLayer,
    owners.legacyImporterLayer,
    owners.managementLayer,
    owners.configLayer,
    owners.settingsLayer,
    owners.platformLayer,
    ThreadCommandExecutor.layer,
    providerRegistryLayer,
    Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({}),
    Layer.mock(TextGeneration.TextGeneration)({}),
  );
  const workspace = WorkspacePaths.layer.pipe(Layer.provide(owners.platformLayer));
  const metadata = ProjectEnrichmentService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        RepositoryIdentityResolver.layer,
        ProjectFaviconResolver.layer.pipe(
          Layer.provide(Layer.merge(workspace, T3ProjectFileLoader.layer)),
        ),
      ),
    ),
    Layer.provide(owners.platformLayer),
  );
  const projects = ProjectService.layer.pipe(
    Layer.provide(Layer.merge(workspace, metadata)),
    Layer.provide(common),
  );
  const terminals = TerminalManager.layer.pipe(
    Layer.provide(terminalOwnerObservationLive.pipe(Layer.provide(owners.eventSinkLayer))),
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(PtyAdapter.PtyAdapter, {
          spawn: (input) =>
            unexpectedControl("spawn").pipe(
              Effect.andThen(
                Effect.fail(
                  new PtyAdapter.PtySpawnError({
                    adapter: "delegated-no-script-virtual",
                    shell: input.shell,
                  }),
                ),
              ),
            ),
        }),
        Layer.mock(NativeTelemetryClient.NativeTelemetryClient)({
          processTable: Effect.succeed([]),
        }),
        Layer.mock(PortScanner.PortDiscovery)({
          registerTerminalProcesses: () => unexpectedControl("registerTerminalProcesses"),
          unregisterTerminal: () => unexpectedControl("unregisterTerminal"),
        }),
        ProcessRunner.layer,
      ),
    ),
    Layer.provide(common),
  );
  const setup = ProjectSetupScriptRunner.layer.pipe(
    Layer.provide(Layer.merge(projects, terminals)),
    Layer.provide(common),
  );
  const process = VcsProcess.layer.pipe(Layer.provide(owners.platformLayer));
  const drivers = Layer.merge(GitVcsDriver.layer, VcsDriverRegistry.layer).pipe(
    Layer.provide(process),
    Layer.provide(common),
  );
  const sourceControl = Layer.effect(
    SourceControlProviderRegistry.SourceControlProviderRegistry,
    SourceControlProviderRegistry.makeWithProviders([]),
  ).pipe(Layer.provide(Layer.merge(process, drivers)), Layer.provide(common));
  const manager = GitManager.layer.pipe(
    Layer.provide(Layer.mergeAll(drivers, sourceControl, setup)),
    Layer.provide(common),
  );
  const workflow = Layer.effect(GitWorkflow.GitWorkflowService, GitWorkflow.make).pipe(
    Layer.provide(Layer.merge(drivers, manager)),
  );
  const folders = ManagedProjectFolders.layer.pipe(
    Layer.provide(Layer.mergeAll(projects, workflow, drivers)),
    Layer.provide(common),
  );
  const physical = Layer.mergeAll(
    projects,
    terminals,
    setup,
    workflow,
    folders,
    WorktreeSetupTracker.layer,
    providerRegistryLayer,
    Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({}),
    Layer.mock(TextGeneration.TextGeneration)({}),
  );
  const capture = Layer.effectDiscard(
    Effect.gen(function* () {
      audit.ownedDirectories.push((yield* ServerConfig.ServerConfig).baseDir);
      audit.acquisitions.push({
        sql: yield* SqlClient.SqlClient,
        sink: yield* EventSink.EventSinkV2,
        management: yield* ThreadManagementService.ThreadManagementService,
        projects: yield* ProjectService.ProjectService,
        terminals: yield* TerminalManager.TerminalManager,
        receipts: yield* CommandReceiptStore.CommandReceiptStoreV2,
        outbox: yield* EffectOutbox.EffectOutboxV2,
      });
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          audit.finalized++;
        }),
      );
    }),
  ).pipe(Layer.provide(Layer.merge(physical, common)));
  return Layer.merge(physical, capture);
}

/** The production planner over a scenario's own ServerConfig and real Git. */
export function makeCheckoutPlannerLayer(
  configLayer: Layer.Layer<ServerConfig.ServerConfig>,
  platformLayer: ReplayDelegatedPreparationOwners["platformLayer"],
) {
  const processLayer = VcsProcess.layer.pipe(Layer.provide(platformLayer));
  const driversLayer = Layer.merge(GitVcsDriver.layer, VcsDriverRegistry.layer).pipe(
    Layer.provide(processLayer),
    Layer.provide(configLayer),
    Layer.provide(platformLayer),
  );
  const workflowLayer = GitWorkflow.layer.pipe(
    Layer.provide(driversLayer),
    Layer.provide(Layer.mock(GitManager.GitManager)({})),
  );
  return DelegatedCheckoutPlanner.layer.pipe(
    Layer.provide(workflowLayer),
    Layer.provide(configLayer),
    Layer.provide(platformLayer),
  );
}

/**
 * Where a standalone birth places its checkout, computed with the same pure
 * placement the planner uses so a test can bind the target before it exists.
 */
const plannedStandaloneCheckout = Effect.fn("plannedStandaloneCheckout")(function* (input: {
  readonly kind: "fork" | "mcp_create";
  readonly birthCommandId: CommandId;
  readonly targetThreadId: ThreadId;
  readonly worktreesDir: string;
  readonly projectWorkspaceRoot: string;
}) {
  const fs = yield* FileSystem.FileSystem.pipe(Effect.provide(NodeServices.layer));
  return planStandaloneBirthPlacement({
    kind: input.kind,
    birthCommandId: input.birthCommandId,
    targetThreadId: input.targetThreadId,
    canonicalWorktreesDir: yield* fs.realPath(input.worktreesDir),
    projectWorkspaceRoot: input.projectWorkspaceRoot,
  });
});

/** Reads one Git value from a fixture checkout. */
const readCheckoutGit = (cwd: string, args: ReadonlyArray<string>) =>
  VcsProcess.VcsProcess.use((process) =>
    process.run({ operation: "ReplayCheckoutFixture.read", command: "git", args, cwd }),
  ).pipe(
    Effect.map((result) => result.stdout.trim()),
    Effect.provide(VcsProcess.layer.pipe(Layer.provide(NodeServices.layer))),
  );

/**
 * Scoped worktrees directory and planned fork checkouts for a standalone
 * replay, with the source checkout's committed HEAD and status before it runs.
 */
export const prepareStandaloneForkCheckouts = Effect.fn("prepareStandaloneForkCheckouts")(
  function* (input: {
    readonly projectWorkspaceRoot: string;
    readonly forks: ReadonlyArray<{
      readonly birthCommandId: CommandId;
      readonly targetThreadId: ThreadId;
    }>;
  }) {
    const fs = yield* FileSystem.FileSystem.pipe(Effect.provide(NodeServices.layer));
    const worktreesDir = yield* fs.makeTempDirectoryScoped({
      prefix: "t3-standalone-replay-worktrees-",
    });
    const planned = [];
    for (const fork of input.forks)
      planned.push(
        yield* plannedStandaloneCheckout({
          kind: "fork",
          ...fork,
          worktreesDir,
          projectWorkspaceRoot: input.projectWorkspaceRoot,
        }),
      );
    return {
      projectWorkspaceRoot: input.projectWorkspaceRoot,
      worktreesDir,
      planned,
      sourceHead: yield* readCheckoutGit(input.projectWorkspaceRoot, ["rev-parse", "HEAD"]),
      sourceStatus: yield* readCheckoutGit(input.projectWorkspaceRoot, ["status", "--porcelain"]),
    };
  },
);

/** The source and each planned fork checkout as Git reports them now. */
export const observeStandaloneForkCheckouts = Effect.fn("observeStandaloneForkCheckouts")(
  function* (fixture: Effect.Success<ReturnType<typeof prepareStandaloneForkCheckouts>>) {
    const targets = [];
    for (const target of fixture.planned)
      targets.push({
        head: yield* readCheckoutGit(target.worktreePath, ["rev-parse", "HEAD"]),
        branch: yield* readCheckoutGit(target.worktreePath, ["symbolic-ref", "--short", "HEAD"]),
      });
    return {
      sourceHead: yield* readCheckoutGit(fixture.projectWorkspaceRoot, ["rev-parse", "HEAD"]),
      sourceStatus: yield* readCheckoutGit(fixture.projectWorkspaceRoot, ["status", "--porcelain"]),
      targets,
    };
  },
);
