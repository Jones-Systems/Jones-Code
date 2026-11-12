import * as NodeCryptoServices from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
  TurnItemId,
  type VcsStatusResult,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import * as ServerConfig from "../../config.ts";
import { ServerEnvironmentIdentity } from "../../environment/ServerEnvironment.ts";
import * as GitWorkflow from "../../git/GitWorkflowService.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../../mcp/McpToolAccess.ts";
import * as WorktreeMcp from "../../mcp/WorktreeMcpService.ts";
import * as WorktreeHandlers from "../../mcp/toolkits/worktree/handlers.ts";
import { WorktreeToolkit } from "../../mcp/toolkits/worktree/tools.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ProjectSetup from "../../project/ProjectSetupScriptRunner.ts";
import * as VcsStatus from "../../vcs/VcsStatusBroadcaster.ts";
import * as Git from "../../vcs/GitVcsDriver.ts";
import * as Provenance from "./Provenance.ts";

const environmentId = EnvironmentId.make("environment:provenance-flows");
const projectId = ProjectId.make("project:provenance-flows");
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "synthetic" };
const registry = ProviderAdapterRegistry.layerFromAdapters([
  {
    instanceId,
    driver: ProviderDriverKind.make("codex"),
    getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
    openSession: () => Effect.die("Provenance tests never start a provider"),
  },
]);

it.effect(
  "explicit capture after committed rename, fork, merge-back, handoff and restore retains provenance",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const scratch = yield* fs.makeTempDirectoryScoped({ prefix: "jones-provenance-flows-" });
        const root = yield* fs.realPath(scratch);
        const repo = path.join(root, "repo");
        const linked = path.join(root, "linked");
        const moved = path.join(root, "renamed-checkout");
        const handoffPath = path.join(root, "handoff");
        const forkPath = path.join(root, "fork-workspace");
        const detachedPath = path.join(root, "detached");
        const templates = path.join(root, "templates");
        yield* fs.makeDirectory(repo);
        yield* fs.makeDirectory(templates);
        const config = ServerConfig.layerTest(root, path.join(root, "state")).pipe(
          Layer.provide(NodeServices.layer),
          Layer.orDie,
        );
        const gitLayer = Git.layer.pipe(Layer.provide(config), Layer.provide(NodeServices.layer));
        const database = SqlitePersistence.layerMemory;
        const provenance = Provenance.layerLive.pipe(
          Layer.provideMerge(database),
          Layer.provide(gitLayer),
          Layer.provide(
            Layer.succeed(ServerEnvironmentIdentity, {
              getEnvironmentId: Effect.succeed(environmentId),
            }),
          ),
          Layer.provide(NodeServices.layer),
        );
        const runtime = ProviderReplayHarness.layerWithRegistryAndServices(
          { name: "provenance-flows" },
          registry,
          { databaseLayer: database, serverConfigLayer: config, runEffectWorker: false },
        ).pipe(Layer.provideMerge(provenance));
        yield* Effect.gen(function* () {
          const git = yield* Git.GitVcsDriver;
          const run = (cwd: string, args: readonly string[]) =>
            git.execute({
              operation: "Provenance.integration.test",
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
          yield* run(repo, ["init", "--template", templates, "-b", "main"]);
          yield* run(repo, ["commit", "--allow-empty", "-m", "synthetic"]);
          yield* run(repo, ["worktree", "add", "-b", "work", linked]);
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const sink = yield* EventSink.EventSinkV2;
          const p = yield* Provenance.Provenance;
          const projects = yield* ProjectStore.ProjectStoreV2;
          const now = yield* DateTime.now;
          const at = DateTime.formatIso(now);
          yield* projects.apply({
            sequence: 0,
            eventId: EventId.make("project:seed"),
            aggregateKind: "project",
            aggregateId: projectId,
            occurredAt: at,
            commandId: null,
            causationEventId: null,
            correlationId: null,
            metadata: {},
            type: "project.created",
            payload: {
              projectId,
              title: "Synthetic",
              workspaceRoot: repo,
              defaultModelSelection: modelSelection,
              scripts: [],
              createdAt: at,
              updatedAt: at,
            },
          });
          const create = (id: ThreadId, branch: string | null, worktreePath: string | null) =>
            orchestrator.dispatch({
              type: "thread.create",
              commandId: CommandId.make(`create:${id}`),
              threadId: id,
              projectId,
              title: "Private title must not enter provenance",
              modelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch,
              worktreePath,
              createdBy: "user",
              creationSource: "mcp",
            });
          const threadId = ThreadId.make("thread:provenance-source");
          const forkId = ThreadId.make("thread:provenance-fork");
          yield* create(threadId, "work", linked);
          const initial = yield* p.capture({ threadId, reason: "launch", createdBy: "system" });
          const sidecarPath = path.join(initial.target.gitDirectory!, "workspace-provenance.json");
          const originalSidecar = yield* fs.readFileString(sidecarPath);
          yield* run(linked, ["branch", "-m", "renamed"]);
          yield* orchestrator.dispatch({
            type: "thread.metadata.update",
            commandId: CommandId.make("rename:branch"),
            threadId,
            branch: "renamed",
            expectedBranch: "work",
            expectedWorktreePath: linked,
          });
          yield* run(repo, ["worktree", "move", linked, moved]);
          yield* orchestrator.dispatch({
            type: "thread.metadata.update",
            commandId: CommandId.make("rename:path"),
            threadId,
            worktreePath: moved,
            expectedWorktreePath: linked,
          });
          yield* p.capture({
            threadId,
            reason: "reference",
            createdBy: "system",
            requireExistingPhysical: true,
          });
          const relocated = (yield* p.lookup({ path: moved }))[0]!.provenance;
          expect(relocated.predecessor).toBe(initial.incarnation_id);
          expect(relocated.birth).toEqual(initial.birth);
          expect(relocated.admin_id).toEqual(initial.admin_id);
          expect((yield* p.lookup({ path: linked }))[0]!.provenance).toEqual(initial);
          expect(yield* fs.readFileString(sidecarPath)).toBe(originalSidecar);

          const completeRun = (owner: ThreadId, id: RunId) =>
            sink.write({
              events: [
                {
                  id: EventId.make(`completed:${id}`),
                  type: "run.created",
                  threadId: owner,
                  runId: id,
                  occurredAt: now,
                  payload: {
                    id,
                    threadId: owner,
                    ordinal: 1,
                    providerInstanceId: instanceId,
                    modelSelection,
                    providerThreadId: null,
                    userMessageId: MessageId.make(`message:${id}`),
                    rootNodeId: null,
                    activeAttemptId: null,
                    status: "completed",
                    requestedAt: now,
                    startedAt: now,
                    completedAt: now,
                    checkpointId: null,
                    contextHandoffId: null,
                  },
                },
              ],
            });
          const sourceRun = RunId.make("run:source");
          yield* completeRun(threadId, sourceRun);
          yield* p.capture({
            threadId,
            reason: "reference",
            createdBy: "system",
            runId: sourceRun,
          });
          yield* sink.write({
            events: [
              {
                type: "turn-item.updated",
                id: EventId.make("first-write"),
                threadId,
                occurredAt: now,
                payload: {
                  type: "file_change",
                  id: TurnItemId.make("first-write"),
                  threadId,
                  runId: sourceRun,
                  nodeId: null,
                  providerThreadId: null,
                  providerTurnId: null,
                  nativeItemRef: null,
                  parentItemId: null,
                  ordinal: 0,
                  status: "completed",
                  title: null,
                  startedAt: now,
                  completedAt: now,
                  updatedAt: now,
                  fileName: "never-opened.txt",
                  diffStr: "private diff must not enter provenance",
                },
              },
              {
                type: "thread.pull-request-link-updated",
                id: EventId.make("merged-pr"),
                threadId,
                occurredAt: now,
                payload: {
                  host: "github.com",
                  repository: "Jones-Systems/Synthetic",
                  number: 1,
                  url: "https://github.com/Jones-Systems/Synthetic/pull/1",
                  source: "agent",
                  linkedAt: at,
                  stack: null,
                  snapshot: {
                    state: "merged",
                    title: "Private PR title",
                    headBranch: "renamed",
                    baseBranch: "main",
                    isDraft: false,
                    updatedAt: at,
                    syncedAt: at,
                    mergedAt: at,
                  },
                },
              },
            ],
          });
          expect(
            (yield* p.lookup({ path: moved }))[0]?.facts.map((fact) => fact.kind).sort(),
          ).toEqual(["first_meaningful_write", "pr_link", "pr_merge"]);
          yield* orchestrator.dispatch({
            type: "thread.fork",
            commandId: CommandId.make("fork:conversation"),
            sourceThreadId: threadId,
            targetThreadId: forkId,
            sourcePoint: { type: "run", runId: sourceRun },
            createdBy: "user",
            creationSource: "mcp",
          });
          yield* p.capture({ threadId: forkId, reason: "reference", createdBy: "system" });
          expect((yield* p.lookup({ threadId: forkId }))[0]?.provenance.incarnation_id).toBe(
            relocated.incarnation_id,
          );
          expect((yield* p.lookup({ path: moved }))[0]?.threads).toEqual([forkId, threadId].sort());
          yield* run(repo, ["worktree", "add", "-b", "fork-work", forkPath]);
          yield* orchestrator.dispatch({
            type: "thread.metadata.update",
            commandId: CommandId.make("fork:workspace"),
            threadId: forkId,
            branch: "fork-work",
            worktreePath: forkPath,
            expectedWorktreePath: moved,
          });
          expect(yield* p.lookup({ path: forkPath })).toEqual([]);
          const forkRun = RunId.make("run:fork");
          yield* completeRun(forkId, forkRun);
          yield* orchestrator.dispatch({
            type: "thread.merge_back",
            commandId: CommandId.make("merge-back:conversation"),
            sourceThreadId: forkId,
            targetThreadId: threadId,
            sourcePoint: { type: "run", runId: forkRun },
            createdBy: "user",
            creationSource: "mcp",
          });
          const merged = yield* orchestrator.getThreadRecords(threadId, ["contextTransfers"]);
          expect(
            merged.contextTransfers.some(
              (transfer) => transfer.type === "merge_back" && transfer.sourceThreadId === forkId,
            ),
          ).toBe(true);
          yield* p.capture({ threadId: forkId, reason: "reference", createdBy: "system" });
          const forkHistory = yield* p.lookup({ threadId: forkId });
          expect(forkHistory.map((entry) => entry.provenance.target.checkoutKey)).toEqual([
            moved,
            forkPath,
          ]);
          expect((yield* p.lookup({ path: forkPath }))[0]?.threads).toEqual([forkId]);
          expect(forkHistory[0]?.provenance.incarnation_id).toBe(relocated.incarnation_id);

          const settleId = CommandId.make("settle:provenance");
          yield* orchestrator.dispatch({ type: "thread.settle", commandId: settleId, threadId });
          yield* orchestrator.dispatch({
            type: "thread.settlement.restore",
            commandId: CommandId.make("restore:provenance"),
            threadId,
            settleCommandId: settleId,
          });
          const restored = yield* orchestrator.getThreadRecords(threadId, []);
          expect(restored.thread.worktreePath).toBe(moved);
          expect(restored.thread.branch).toBe("renamed");
          expect(restored.thread.settledOverride).toBe("active");
          const history = yield* p.lookup({ threadId });
          expect(history.map((entry) => entry.provenance.incarnation_id)).toEqual([
            initial.incarnation_id,
            relocated.incarnation_id,
          ]);
          expect(
            history[1]?.facts
              .filter((fact) => fact.kind === "settlement_outcome")
              .map((fact) => fact.reference),
          ).toEqual(
            expect.arrayContaining([
              expect.stringMatching(/^settled:/),
              expect.stringMatching(/^unsettled:/),
            ]),
          );

          const rootThreadId = ThreadId.make("thread:handoff");
          yield* create(rootThreadId, "main", null);
          const scope: Invocation.McpInvocationScope = {
            environmentId,
            requestNamespace: "synthetic",
            thread: {
              threadId: rootThreadId,
              providerSessionId: "synthetic-session",
              providerInstanceId: instanceId,
            },
            client: undefined,
            capabilities: new Set(["worktree", "orchestration"]),
            issuedAt: 1,
          };
          const external = Layer.mergeAll(
            Layer.mock(ProjectService.ProjectService)({
              getById: () =>
                Effect.succeedSome({
                  id: projectId,
                  title: "Synthetic",
                  workspaceRoot: repo,
                  scripts: [],
                  defaultModelSelection: modelSelection,
                  createdAt: at,
                  updatedAt: at,
                  deletedAt: null,
                } as never),
            }),
            Layer.mock(GitWorkflow.GitWorkflowService)({
              listLocalBranchNames: () => Effect.succeed(["main", "renamed"]),
              listRefs: () =>
                Effect.succeed({
                  refs: [],
                  nextCursor: null,
                  isRepo: true,
                  hasPrimaryRemote: false,
                  totalCount: 0,
                }),
              localStatus: () => Effect.succeed({ refName: "main", isRepo: true } as never),
              createWorktree: (input) =>
                run(repo, [
                  "worktree",
                  "add",
                  "-b",
                  input.newRefName!,
                  handoffPath,
                  input.refName,
                ]).pipe(
                  Effect.as({
                    worktree: { path: handoffPath, refName: input.newRefName! },
                  } as never),
                ),
            }),
            Layer.mock(ProjectSetup.ProjectSetupScriptRunner)({
              runForThread: () => Effect.die("Setup disabled"),
            }),
            Layer.mock(VcsStatus.VcsStatusBroadcaster)({
              refreshStatus: () =>
                Effect.succeed({
                  isRepo: true,
                  hasPrimaryRemote: false,
                  isDefaultRef: false,
                  refName: "handoff",
                  hasWorkingTreeChanges: false,
                  workingTree: { files: [], insertions: 0, deletions: 0 },
                  hasUpstream: false,
                  aheadCount: 0,
                  behindCount: 0,
                  pr: null,
                } satisfies VcsStatusResult),
            }),
          );
          const service = yield* WorktreeMcp.WorktreeMcpService.pipe(
            Effect.provide(WorktreeMcp.layer.pipe(Layer.provide(external))),
          );
          yield* p.capture({ threadId: rootThreadId, reason: "reference", createdBy: "agent" });
          yield* service.handoff(scope, {
            branch: "handoff",
            startFromOrigin: false,
            runSetupScript: false,
          });
          yield* p.capture({ threadId: rootThreadId, reason: "handoff", createdBy: "agent" });
          const handed = yield* p.lookup({ threadId: rootThreadId });
          expect(handed).toHaveLength(2);
          expect(handed[1]?.provenance.reason).toBe("handoff");
          expect(handed[1]?.provenance.predecessor).toBe(handed[0]?.provenance.incarnation_id);
          expect((yield* service.status(scope)).provenance?.map((entry) => entry.path)).toEqual([
            repo,
            handoffPath,
          ]);

          yield* run(repo, ["worktree", "add", "--detach", detachedPath, "main"]);
          const detachedThreadId = ThreadId.make("thread:detached");
          yield* create(detachedThreadId, null, detachedPath);
          yield* p.capture({
            threadId: detachedThreadId,
            reason: "reference",
            createdBy: "system",
          });

          const toolkit = yield* WorktreeToolkit.pipe(
            Effect.provide(
              McpToolAccess.HandlersLayer.layer(WorktreeHandlers.layer).pipe(
                Layer.provide(external),
              ),
            ),
          );
          const list = (input: { readonly provenancePath?: string }) =>
            toolkit.handle("t3_worktree_list", input).pipe(
              Stream.unwrap,
              Stream.runCollect,
              Effect.map((results) => results.at(-1)!.result),
              Effect.provideService(Invocation.McpInvocationContext, scope),
              Effect.provideService(WorktreeMcp.WorktreeMcpService, service),
              Effect.provide(external),
            );
          const status = yield* toolkit.handle("t3_worktree_status", {}).pipe(
            Stream.unwrap,
            Stream.runCollect,
            Effect.map((results) => results.at(-1)!.result),
            Effect.provideService(Invocation.McpInvocationContext, scope),
            Effect.provideService(WorktreeMcp.WorktreeMcpService, service),
          );
          expect(status).toMatchObject({
            attached: true,
            worktreePath: handoffPath,
            provenance: handed.map((entry) =>
              expect.objectContaining({ incarnationId: entry.provenance.incarnation_id }),
            ),
          });
          expect(yield* list({})).toMatchObject({
            refs: [],
            provenance: expect.arrayContaining([
              expect.objectContaining({ path: repo }),
              expect.objectContaining({ path: handoffPath }),
            ]),
          });
          expect(yield* list({ provenancePath: linked })).toMatchObject({
            refs: [],
            provenance: [expect.objectContaining({ incarnationId: initial.incarnation_id })],
          });
          expect(yield* list({ provenancePath: detachedPath })).toMatchObject({
            refs: [],
            provenance: [expect.objectContaining({ path: detachedPath, branch: null })],
          });
          expect(yield* list({ provenancePath: path.join(root, "unknown") })).toMatchObject({
            refs: [],
            provenance: [],
          });
          const serialized = JSON.stringify(yield* p.lookup({ threadId }));
          expect(serialized).not.toContain("Private title");
          expect(serialized).not.toContain("Private PR title");
          expect(serialized).not.toContain("private diff");
        }).pipe(
          Effect.provide(
            Layer.mergeAll(runtime, gitLayer, ProjectStore.layer.pipe(Layer.provide(database))),
          ),
        );
      }),
    ).pipe(Effect.provide(Layer.merge(NodeServices.layer, NodeCryptoServices.layer))),
);
