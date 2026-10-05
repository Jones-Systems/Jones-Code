import { assert, it, vi } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { HostProcessEnvironment, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as ServerConfig from "../config.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import { makeSqlitePersistenceLive } from "../persistence/Layers/Sqlite.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as WorktreeSetupTracker from "../project/WorktreeSetupTracker.ts";
import { makeProviderRegistryLayer } from "../provider/testUtils/providerRegistryMock.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import { makeGitVcsDriverCore } from "../vcs/GitVcsDriverCore.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as CommandReceipts from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as IdAllocator from "./IdAllocator.ts";
import { legacyBootstrapCreateCommandId, legacyPayloadHash } from "./LegacyBootstrap.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ThreadLaunch from "./ThreadLaunchService.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";

const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.1-codex" };
const adapter = {
  instanceId: modelSelection.instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Joined preparation must not enter a real provider"),
} as ProviderAdapterV2Shape;

it.layer(NodeServices.layer, { excludeTestServices: true })(
  "Legacy journal and original physical preparation",
  (it) => {
    it.effect.each([
      "qualified",
      "journal_unavailable",
      "lease_lost_before_add",
      "lease_lost_after_add",
      "strict_rejected_C",
    ] as const)("requires both independent owners for %s", (scenario) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "legacy-physical-join-" });
        const cwd = `${root}/repo`;
        const common = `${cwd}/.git`;
        const worktreesDir = `${root}/worktrees`;
        const gitDirectory = `${common}/worktrees/joined`;
        yield* fs.makeDirectory(gitDirectory, { recursive: true });
        yield* fs.makeDirectory(worktreesDir);
        const projectId = ProjectId.make(`joint:P:${scenario}`);
        const threadId = ThreadId.make(`joint:T:${scenario}`);
        const releaseCommandId = CommandId.make(`joint:C:${scenario}`);
        const createCommandId = legacyBootstrapCreateCommandId(threadId, releaseCommandId);
        const messageId = MessageId.make(`joint:M:${scenario}`);
        const oid = "a".repeat(40);
        const project = {
          id: projectId,
          title: "Joined owners",
          workspaceRoot: cwd,
          repositoryIdentity: null,
          faviconPath: null,
          defaultModelSelection: modelSelection,
          defaultThreadEnvMode: null,
          scripts: [],
          createdAt: "2026-10-05T00:00:00.000Z",
          updatedAt: "2026-10-05T00:00:00.000Z",
          deletedAt: null,
        } as const;
        let spawnCount = 0;
        const manager = yield* TerminalManager.makeWithOptions({
          logsDir: `${root}/terminal-logs`,
          env: {},
          shellResolver: () => "/bin/sh",
          processTable: Effect.succeed([]),
          processKillGraceMs: 1,
          subprocessInspector: () =>
            Effect.succeed({ hasRunningSubprocess: false, childCommand: null, processIds: [] }),
          ptyAdapter: {
            spawn: () =>
              Effect.sync(() => {
                spawnCount++;
                throw new Error("No PTY entry is allowed");
              }),
          },
        }).pipe(Effect.provide(ProcessRunner.layer));
        const managerLayer = Layer.succeed(TerminalManager.TerminalManager, manager);
        const database = makeSqlitePersistenceLive(`${root}/joined.sqlite`).pipe(
          Layer.provide(NodeServices.layer),
        );
        const runtime = makeOrchestratorV2ReplayLayerWithRegistry(
          { name: `joint-${scenario}` },
          ProviderAdapterRegistry.makeLayer([adapter]),
          {
            databaseLayer: database,
            runEffectWorker: false,
            checkoutFixture: {
              projects: [{ projectId, title: project.title, workspaceRoot: cwd }],
              resolvePath: () => undefined,
              worktreesDir,
            },
          },
        ).pipe(Layer.provide(managerLayer));
        const stores = Layer.mergeAll(
          CommandReceipts.layer,
          EffectOutbox.layer,
          EventStore.layer,
        ).pipe(Layer.provide(database));
        const management = ThreadManagement.layer.pipe(Layer.provide(runtime));
        const config = ServerConfig.layerTest(cwd, `${root}/driver-config`);
        const launchConfig = Layer.effect(
          ServerConfig.ServerConfig,
          Effect.map(ServerConfig.ServerConfig, (value) => ({ ...value, worktreesDir })),
        ).pipe(Layer.provide(config));
        const setup = yield* ProjectSetupScriptRunner.make.pipe(
          Effect.provide(
            Layer.mergeAll(
              Layer.mock(ProjectService.ProjectService)({}),
              managerLayer,
              ServerSettings.layerTest(),
              Layer.succeed(HostProcessEnvironment, {}),
              Layer.succeed(HostProcessPlatform, "linux"),
            ),
          ),
        );
        const commands: string[][] = [];
        let owned: string | null = null;
        let branch = "joined";
        let added = false;
        const gitLayer = Layer.unwrap(
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            const spawner = ChildProcessSpawner.make((command) =>
              Effect.gen(function* () {
                if (!ChildProcess.isStandardCommand(command))
                  return yield* Effect.die("Unexpected synthetic pipeline");
                const args = [...command.args];
                commands.push(args);
                if (args.includes("add")) {
                  owned = args.at(-2)!;
                  yield* fs.makeDirectory(owned, { recursive: true });
                  yield* fs.writeFileString(`${owned}/.git`, `gitdir: ${gitDirectory}\n`);
                  added = true;
                  if (scenario === "lease_lost_after_add")
                    yield* sql`UPDATE worktree_ownership_leases SET expires_at_ms = 0 WHERE owner_thread_id = ${threadId}`.pipe(
                      Effect.orDie,
                    );
                }
                let stdout = "";
                if (args.includes("--git-common-dir")) stdout = `${common}\n`;
                else if (args.includes("--absolute-git-dir")) stdout = `${gitDirectory}\n`;
                else if (args.includes("symbolic-ref")) stdout = `refs/heads/${branch}\n`;
                else if (args.includes("rev-parse")) stdout = `${oid}\n`;
                else if (args.includes("--porcelain"))
                  stdout = added
                    ? `worktree ${owned}\0HEAD ${oid}\0branch refs/heads/${branch}\0\0`
                    : "";
                else if (
                  args.includes("for-each-ref") &&
                  added &&
                  args.at(-1) === `refs/heads/${branch}`
                )
                  stdout = `refs/heads/${branch}\n`;
                const unsuccessful =
                  args.includes("--get-regexp") || (args.includes("show-ref") && !added);
                return ChildProcessSpawner.makeHandle({
                  pid: ChildProcessSpawner.ProcessId(1),
                  exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(unsuccessful ? 1 : 0)),
                  isRunning: Effect.succeed(false),
                  kill: () => Effect.void,
                  unref: Effect.succeed(Effect.void),
                  stdin: Sink.drain,
                  stdout: Stream.encodeText(Stream.make(stdout)),
                  stderr: Stream.empty,
                  all: Stream.empty,
                  getInputFd: () => Sink.drain,
                  getOutputFd: () => Stream.empty,
                });
              }),
            );
            const driver = yield* makeGitVcsDriverCore().pipe(
              Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
              Effect.provide(config),
            );
            return Layer.mock(GitWorkflow.GitWorkflowService)({
              createWorktree: (input, options) => {
                branch = input.newRefName!;
                return driver.createWorktree(input, options);
              },
              renameBranch: driver.renameBranch,
              localStatus: () =>
                Effect.succeed({
                  isRepo: true,
                  hasPrimaryRemote: false,
                  isDefaultRef: false,
                  refName: branch,
                  hasWorkingTreeChanges: false,
                  workingTree: { files: [], insertions: 0, deletions: 0 },
                }),
              resolveCommit: () => Effect.succeed({ commitSha: oid }),
              invalidateLocalStatus: () => Effect.void,
              isRepository: () => Effect.succeed(true),
              hasCommit: () => Effect.succeed(true),
              remoteExists: () => Effect.succeed(false),
            });
          }),
        ).pipe(Layer.provide(database));
        const external = Layer.mergeAll(
          WorktreeSetupTracker.layer,
          Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
          managerLayer,
          Layer.mock(ProjectService.ProjectService)({
            getById: () => Effect.succeed(Option.some(project)),
          }),
          gitLayer,
          Layer.succeed(ProjectSetupScriptRunner.ProjectSetupScriptRunner, setup),
          makeProviderRegistryLayer(),
          ServerSettings.layerTest(),
          Layer.mock(TextGeneration.TextGeneration)({
            generateBranchName: () => Effect.succeed({ branch: "joined" }),
            generateThreadTitle: () => Effect.succeed({ title: project.title }),
          }),
          Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
            namedProjectsRoot: `${root}/projects`,
            folderForThread: () => Effect.succeed(Option.none()),
          }),
        );
        const launch = ThreadLaunch.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              external,
              management,
              runtime,
              stores,
              database,
              IdAllocator.layer,
              launchConfig,
            ),
          ),
        );
        yield* Effect.gen(function* () {
          const launcher = yield* ThreadLaunch.ThreadLaunchService;
          const sink = yield* EventSink.EventSinkV2;
          const receipts = yield* CommandReceipts.CommandReceiptStoreV2;
          const outbox = yield* EffectOutbox.EffectOutboxV2;
          const threads = yield* ThreadManagement.ThreadManagementService;
          const sql = yield* SqlClient.SqlClient;
          const p = CommandId.make(`joint:project:${scenario}`);
          yield* sink.commitProjectCommand({
            commandId: p,
            projectId,
            commandType: "project.create",
            acceptedAt: yield* DateTime.now,
            event: {
              eventId: EventId.make(`${p}:event`),
              aggregateKind: "project",
              aggregateId: projectId,
              occurredAt: project.createdAt,
              commandId: p,
              causationEventId: null,
              correlationId: null,
              metadata: {},
              type: "project.created",
              payload: {
                projectId,
                title: project.title,
                workspaceRoot: cwd,
                defaultModelSelection: modelSelection,
                scripts: [],
                createdAt: project.createdAt,
                updatedAt: project.updatedAt,
              },
            },
          });
          const actualRead = receipts.getByCommandId;
          const readback = vi.spyOn(receipts, "getByCommandId").mockImplementation((id) =>
            actualRead(id).pipe(
              Effect.flatMap((receipt) => {
                if (!id.endsWith(":intent")) return Effect.succeed(receipt);
                if (scenario === "journal_unavailable") return Effect.succeed(Option.none());
                if (scenario === "lease_lost_before_add")
                  return sql`UPDATE worktree_ownership_leases SET expires_at_ms = 0 WHERE owner_thread_id = ${threadId}`.pipe(
                    Effect.orDie,
                    Effect.as(receipt),
                  );
                return Effect.succeed(receipt);
              }),
            ),
          );
          const result = yield* launcher
            .launch({
              commandId: createCommandId,
              preparationReleaseCommandId: releaseCommandId,
              threadId,
              projectId,
              title: project.title,
              modelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              workspaceStrategy: {
                type: "worktree",
                branch: "joined",
                baseRef: "main",
                startFromOrigin: false,
              },
              runSetupScript: false,
              initialMessage: { messageId, text: "Joined original owners", attachments: [] },
              createdBy: "user",
              creationSource: "web",
              legacyBootstrap: {
                version: 1,
                createCommandId,
                birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
                releaseCommandId,
                projectId,
                threadId,
                messageId,
                payloadHash: legacyPayloadHash("Joined original owners"),
                ownsNewThread: true,
                ...(scenario === "strict_rejected_C"
                  ? {
                      dispatchGuard: {
                        observedSnapshotSequence: yield* sink.latestSequence(),
                        expectedModelSelection: modelSelection,
                        expectedSessionStatus: null,
                        expectedActiveTurnId: null,
                        expectedLatestTurnId: null,
                        requireIdle: true,
                      },
                    }
                  : {}),
              },
            })
            .pipe(
              Effect.timeout("5 seconds"),
              Effect.result,
              Effect.ensuring(Effect.sync(() => readback.mockRestore())),
            );
          const projection = yield* threads.getThreadProjection(threadId);
          const c = yield* receipts.getByCommandId(releaseCommandId);
          const preparation = projection.runs[0]?.legacyPreparation;
          assert.isDefined(
            preparation,
            Result.isFailure(result) ? String(result.failure.cause) : "Missing actual journal",
          );
          assert.equal(spawnCount, 0);
          assert.isEmpty(
            commands.filter((args) => args.includes("remove") || args.includes("prune")),
          );
          if (scenario === "qualified") {
            assert.isTrue(
              Result.isSuccess(result),
              Result.isFailure(result) ? String(result.failure.cause) : undefined,
            );
            assert.isTrue(Option.isSome(c));
            if (Option.isSome(c)) assert.equal(c.value.status, "accepted");
            assert.equal(commands.filter((args) => args.includes("add")).length, 1);
            assert.equal(preparation?.steps[0]?.state, "known_succeeded");
          } else if (scenario === "strict_rejected_C") {
            assert.isTrue(Result.isFailure(result));
            assert.isTrue(Option.isSome(c));
            if (Option.isSome(c)) assert.equal(c.value.status, "rejected");
            const d = yield* receipts.getByCommandId(
              CommandId.make(`${createCommandId}:guard-rejection-delete`),
            );
            assert.isTrue(Option.isSome(d));
            if (Option.isSome(d)) assert.equal(d.value.status, "accepted");
            assert.isNotNull(projection.thread.deletedAt);
            assert.equal(commands.filter((args) => args.includes("add")).length, 1);
            assert.isTrue(owned !== null && (yield* fs.exists(owned)));
            assert.isEmpty(
              (yield* outbox.listByThreadId(threadId)).filter(
                (effect) =>
                  effect.request.type === "provider-turn.start" ||
                  effect.request.type === "terminal.cleanup",
              ),
            );
            assert.isNotEmpty(
              yield* sql`SELECT * FROM worktree_ownership_leases WHERE owner_thread_id = ${threadId}`,
            );
          } else {
            assert.isTrue(Result.isFailure(result));
            assert.isTrue(Option.isNone(c));
            assert.equal(
              commands.filter((args) => args.includes("add")).length,
              scenario === "lease_lost_after_add" ? 1 : 0,
            );
            assert.isEmpty(
              (yield* outbox.listByThreadId(threadId)).filter(
                (effect) => effect.request.type === "provider-turn.start",
              ),
            );
            assert.isNull(projection.thread.deletedAt);
          }
        }).pipe(
          Effect.provide(Layer.mergeAll(launch, runtime, management, stores, database, external)),
        );
      }),
    );
  },
);
