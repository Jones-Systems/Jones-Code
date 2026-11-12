import {
  CheckpointId,
  CheckpointRef,
  CheckpointScopeId,
  EventId,
  MessageId,
  NodeId,
  RunId,
} from "@t3tools/contracts";

import {
  ProjectId,
  ProviderInstanceId,
  SourceControlProviderKind,
  ThreadId,
  type OrchestrationProjectShell,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ThreadShell,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { describe, expect, it } from "@effect/vitest";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as BackgroundPolicy from "../background/BackgroundPolicy.ts";
import * as ThreadMetadataReads from "../jones/orchestration/threadMetadataReads.ts";
import * as GitManager from "../git/GitManager.ts";
import {
  asBackgroundGitWork,
  dependencies as gitDependencies,
  makeFixture as makeGitFixture,
} from "../jones/git/backgroundGitAdmission.fixture.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as PullRequestService from "../pullRequest/PullRequestService.ts";
import * as ServerActivation from "../serverActivation.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ThreadPullRequestService from "./ThreadPullRequestService.ts";

describe("ThreadPullRequestServiceV2 project guard", () => {
  it("rejects a pull-request result when the project root changes before dispatch", () => {
    const currentProject = Option.some({
      workspaceRoot: "/workspace/replaced",
    } satisfies Pick<OrchestrationProjectShell, "workspaceRoot">);

    expect(
      ThreadPullRequestService.projectWorkspaceMatchesSnapshot(
        currentProject,
        "/workspace/original",
      ),
    ).toBe(false);
  });
});

describe("ThreadPullRequestServiceV2 reads", () => {
  const NOW = DateTime.makeUnsafe("2026-09-20T00:00:00.000Z");
  const threadShell = (id: string): OrchestrationV2ThreadShell => {
    const threadId = ThreadId.make(id);
    return {
      id: threadId,
      projectId: ProjectId.make("project-1"),
      title: id,
      providerInstanceId: ProviderInstanceId.make("codex"),
      modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
      forkedFrom: null,
      createdBy: "user",
      creationSource: "web",
      activeRunId: null,
      latestRunId: null,
      status: "idle",
      pendingRuntimeRequest: null,
      latestVisibleMessage: null,
      latestUserMessageAt: null,
      hasActionableProposedPlan: false,
      itemCount: 0,
      visibleItemCount: 0,
      createdAt: NOW,
      updatedAt: NOW,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    };
  };

  it.effect("does not sweep branch pull requests while no client is connected", () =>
    Effect.scoped(
      Effect.gen(function* () {
        // Polling active branches without a connected client wastes GitHub requests.
        const thread = { ...threadShell("idle-branch-thread"), branch: "feature/idle" };
        const project: OrchestrationProjectShell = {
          id: thread.projectId,
          title: "Project",
          workspaceRoot: "/workspace/project",
          defaultModelSelection: null,
          scripts: [],
          createdAt: "2026-08-01T00:00:00.000Z",
          updatedAt: "2026-08-01T00:00:00.000Z",
        };
        const activation = yield* Deferred.make<void>();
        const domainEvents = yield* Queue.unbounded<OrchestrationV2DomainEvent>();
        const turnLookup = yield* Deferred.make<void>();
        const lookups: Array<{ readonly refresh: boolean }> = [];
        const dependencies = Layer.mergeAll(
          Layer.mock(ThreadMetadataReads.ThreadMetadataReads)({
            list: () => Effect.succeed({ snapshotSequence: 1, threads: [thread] }),
          }),
          Layer.mock(Orchestrator.OrchestratorV2)({
            streamDomainEvents: Stream.fromQueue(domainEvents),
            getThreadEventSequence: () => Effect.succeed(1),
            getThreadShell: () => Effect.succeed(thread),
          }),
          Layer.mock(ProjectStore.ProjectStoreV2)({
            listShells: () => Effect.succeed([project]),
            getShell: () => Effect.succeedSome(project),
          }),
          Layer.mock(GitManager.GitManager)({
            branchPullRequest: (_input, options) =>
              Effect.gen(function* () {
                const refresh = options?.refresh ?? false;
                lookups.push({ refresh });
                if (refresh) yield* Deferred.succeed(turnLookup, undefined);
                return null;
              }),
          }),
          Layer.mock(PullRequestService.PullRequestService)({}),
          Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
            resolve: () =>
              Effect.succeed({
                canonicalKey: "github.com/acme/project",
                locator: {
                  source: "git-remote" as const,
                  remoteName: "origin",
                  remoteUrl: "git@github.com:acme/project.git",
                },
                provider: SourceControlProviderKind.make("github"),
                owner: "acme",
                name: "project",
              }),
          }),
          // No client holds an activity lease.
          Layer.mock(BackgroundPolicy.BackgroundPolicy)({
            shouldRunScopeWork: () => Effect.succeed(false),
            shouldRunOpportunisticWork: Effect.succeed(false),
          }),
          Layer.succeed(ServerActivation.ServerActivation, Deferred.await(activation)),
          Layer.succeed(
            SecureRandom.SecureRandom,
            SecureRandom.make((size) => new Uint8Array(size).fill(1)),
          ),
          FileSystem.layerNoop({}),
          Path.layer,
        );
        yield* Effect.gen(function* () {
          const service = yield* ThreadPullRequestService.make;
          yield* service.start();
          yield* Deferred.succeed(activation, undefined);

          // A finished turn still discovers the pull request for its branch.
          yield* Queue.offer(domainEvents, {
            type: "checkpoint.captured",
            threadId: thread.id,
          } as OrchestrationV2DomainEvent);
          yield* Deferred.await(turnLookup);
          yield* service.drain;
          const lookupsBeforeIdle = lookups.length;

          yield* TestClock.adjust("10 minutes");
          yield* service.drain;

          expect(lookups.slice(lookupsBeforeIdle)).toEqual([]);
        }).pipe(Effect.provide(dependencies));
      }),
    ),
  );

  it.effect("looks up a finished turn's branch once for all its turn-end events", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = { ...threadShell("turn-end-thread"), branch: "t3/turn-end" };
        const otherThread = {
          ...threadShell("other-turn-end-thread"),
          branch: "t3/other-turn-end",
        };
        const project: OrchestrationProjectShell = {
          id: thread.projectId,
          title: "Project",
          workspaceRoot: "/workspace/project",
          defaultModelSelection: null,
          scripts: [],
          createdAt: "2026-08-01T00:00:00.000Z",
          updatedAt: "2026-08-01T00:00:00.000Z",
        };
        // A turn end commits its checkpoints and the run's end together. The queued
        // non-refresh request ahead of them must still become a refresh when they join it.
        const turnEnd = [
          { type: "thread.metadata-updated", threadId: thread.id },
          { type: "checkpoint.captured", threadId: thread.id },
          { type: "checkpoint.captured", threadId: thread.id },
          { type: "run.updated", threadId: thread.id, payload: { status: "completed" } },
          { type: "checkpoint.captured", threadId: otherThread.id },
          { type: "run.updated", threadId: otherThread.id, payload: { status: "completed" } },
        ] as Array<OrchestrationV2DomainEvent>;
        // Arrives during the thread's own lookup, which may already have read the old host state.
        const duringLookup = [
          { type: "checkpoint.captured", threadId: thread.id },
          { type: "thread.metadata-updated", threadId: thread.id },
        ] as Array<OrchestrationV2DomainEvent>;
        const activation = yield* Deferred.make<void>();
        const sweepStarted = yield* Deferred.make<void>();
        const turnEndQueued = yield* Deferred.make<void>();
        const lookupStarted = yield* Deferred.make<void>();
        const followUpQueued = yield* Deferred.make<void>();
        const lookups: Array<{ readonly branch: string; readonly refresh: boolean }> = [];
        const dependencies = Layer.mergeAll(
          Layer.mock(Orchestrator.OrchestratorV2)({
            // The batch arrives while the worker is busy with the startup sweep.
            streamDomainEvents: Stream.fromEffect(Deferred.await(sweepStarted)).pipe(
              Stream.drain,
              Stream.concat(Stream.fromIterable(turnEnd)),
              Stream.concat(
                Stream.fromEffect(Deferred.succeed(turnEndQueued, undefined)).pipe(Stream.drain),
              ),
              Stream.concat(Stream.fromEffect(Deferred.await(lookupStarted)).pipe(Stream.drain)),
              Stream.concat(Stream.fromIterable(duringLookup)),
              Stream.concat(
                Stream.fromEffect(Deferred.succeed(followUpQueued, undefined)).pipe(Stream.drain),
              ),
              Stream.concat(Stream.never),
            ),
            getThreadEventSequence: () => Effect.succeed(1),
          }),
          Layer.mock(ThreadMetadataReads.ThreadMetadataReads)({
            list: (query) =>
              Effect.succeed({
                snapshotSequence: 1,
                threads: [thread, otherThread].filter(
                  (candidate) =>
                    query?.threadIds === undefined || query.threadIds.includes(candidate.id),
                ),
              }),
          }),
          Layer.mock(ProjectStore.ProjectStoreV2)({
            listShells: () => Effect.succeed([project]),
            getShell: () => Effect.succeedSome(project),
          }),
          Layer.mock(GitManager.GitManager)({
            branchPullRequest: (input, options) =>
              Effect.gen(function* () {
                lookups.push({ branch: input.branch, refresh: options?.refresh ?? false });
                if (!(yield* Deferred.isDone(turnEndQueued))) {
                  yield* Deferred.succeed(sweepStarted, undefined);
                  yield* Deferred.await(turnEndQueued);
                } else if (
                  input.branch === thread.branch &&
                  !(yield* Deferred.isDone(followUpQueued))
                ) {
                  yield* Deferred.succeed(lookupStarted, undefined);
                  yield* Deferred.await(followUpQueued);
                }
                return null;
              }),
          }),
          Layer.mock(PullRequestService.PullRequestService)({}),
          Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
            resolve: () =>
              Effect.succeed({
                canonicalKey: "github.com/acme/project",
                locator: {
                  source: "git-remote" as const,
                  remoteName: "origin",
                  remoteUrl: "git@github.com:acme/project.git",
                },
                provider: SourceControlProviderKind.make("github"),
                owner: "acme",
                name: "project",
              }),
          }),
          Layer.mock(BackgroundPolicy.BackgroundPolicy)({
            shouldRunScopeWork: () => Effect.succeed(false),
            shouldRunOpportunisticWork: Effect.succeed(false),
          }),
          Layer.succeed(ServerActivation.ServerActivation, Deferred.await(activation)),
          Layer.succeed(
            SecureRandom.SecureRandom,
            SecureRandom.make((size) => new Uint8Array(size).fill(1)),
          ),
          FileSystem.layerNoop({}),
          Path.layer,
        );
        yield* Effect.gen(function* () {
          const service = yield* ThreadPullRequestService.make;
          yield* service.start();
          yield* Deferred.succeed(activation, undefined);
          yield* Deferred.await(followUpQueued);
          yield* service.drain;
        }).pipe(Effect.provide(dependencies));

        // The startup sweep, one refresh lookup per thread for its turn-end batch, then one
        // follow-up refresh for the events that arrived during the thread's lookup.
        expect(lookups).toEqual([
          { branch: thread.branch, refresh: false },
          { branch: otherThread.branch, refresh: false },
          { branch: thread.branch, refresh: true },
          { branch: otherThread.branch, refresh: true },
          { branch: thread.branch, refresh: true },
        ]);
      }),
    ),
  );

  it.effect.each([
    {
      worktree: "a linked worktree",
      existingPaths: ["/workspace/worktree", "/workspace/worktree/.git"],
      unreadablePaths: [],
      cwd: "/workspace/worktree",
    },
    // A folder that outlived its .git makes git fail with not_a_repository.
    {
      worktree: "a folder without .git",
      existingPaths: ["/workspace/worktree"],
      unreadablePaths: [],
      cwd: "/workspace/project",
    },
    {
      worktree: "a folder whose .git cannot be checked",
      existingPaths: ["/workspace/worktree"],
      unreadablePaths: ["/workspace/worktree/.git"],
      cwd: "/workspace/worktree",
    },
    {
      worktree: "a missing folder",
      existingPaths: [],
      unreadablePaths: [],
      cwd: "/workspace/project",
    },
  ] as const)(
    "looks up a branch whose worktree is $worktree in $cwd",
    ({ existingPaths, unreadablePaths, cwd }) =>
      Effect.scoped(
        Effect.gen(function* () {
          const thread = {
            ...threadShell("worktree-thread"),
            settledOverride: "settled" as const,
            settledAt: NOW,
            branch: "feature/worktree",
            worktreePath: "/workspace/worktree",
          };
          const project: OrchestrationProjectShell = {
            id: thread.projectId,
            title: "Project",
            workspaceRoot: "/workspace/project",
            defaultModelSelection: null,
            scripts: [],
            createdAt: "2026-08-01T00:00:00.000Z",
            updatedAt: "2026-08-01T00:00:00.000Z",
          };
          const activation = yield* Deferred.make<void>();
          const sweepStarted = yield* Deferred.make<void>();
          const lookupCwds: Array<string> = [];
          const dependencies = Layer.mergeAll(
            Layer.mock(Orchestrator.OrchestratorV2)({
              streamDomainEvents: Stream.never,
            }),
            Layer.mock(ThreadMetadataReads.ThreadMetadataReads)({
              list: () =>
                Deferred.succeed(sweepStarted, undefined).pipe(
                  Effect.as({ snapshotSequence: 1, threads: [thread] }),
                ),
            }),
            Layer.mock(ProjectStore.ProjectStoreV2)({
              listShells: () => Effect.succeed([project]),
              getShell: () => Effect.succeedSome(project),
            }),
            Layer.mock(GitManager.GitManager)({
              branchPullRequest: (input) =>
                Effect.sync(() => {
                  lookupCwds.push(input.cwd);
                  return null;
                }),
            }),
            Layer.mock(PullRequestService.PullRequestService)({}),
            Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
              resolve: () =>
                Effect.succeed({
                  canonicalKey: "github.com/acme/project",
                  locator: {
                    source: "git-remote" as const,
                    remoteName: "origin",
                    remoteUrl: "git@github.com:acme/project.git",
                  },
                  provider: SourceControlProviderKind.make("github"),
                  owner: "acme",
                  name: "project",
                }),
            }),
            Layer.mock(BackgroundPolicy.BackgroundPolicy)({}),
            Layer.succeed(ServerActivation.ServerActivation, Deferred.await(activation)),
            Layer.succeed(
              SecureRandom.SecureRandom,
              SecureRandom.make((size) => new Uint8Array(size).fill(1)),
            ),
            FileSystem.layerNoop({
              exists: (path) =>
                unreadablePaths.some((entry) => entry === path)
                  ? Effect.fail(
                      PlatformError.systemError({
                        _tag: "PermissionDenied",
                        module: "FileSystem",
                        method: "exists",
                        pathOrDescriptor: path,
                      }),
                    )
                  : Effect.succeed(existingPaths.some((entry) => entry === path)),
            }),
            Path.layer,
          );
          yield* Effect.gen(function* () {
            const service = yield* ThreadPullRequestService.make;
            yield* service.start();
            yield* Deferred.succeed(activation, undefined);
            yield* Deferred.await(sweepStarted);
            yield* service.drain;
          }).pipe(Effect.provide(dependencies));

          expect(lookupCwds).toEqual([cwd]);
        }),
      ),
  );

  it.effect("leaves delegated child threads to their own refresh instead of the sweep", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const parent = {
          ...threadShell("parent-thread"),
          branch: "feature/parent",
          worktreePath: "/workspace/parent",
        };
        const child: OrchestrationV2ThreadShell = {
          ...threadShell("child-thread"),
          branch: "t3/reply-ok",
          worktreePath: "/workspace/child",
          lineage: {
            rootThreadId: parent.id,
            parentThreadId: parent.id,
            relationshipToParent: "subagent",
          },
        };
        const project: OrchestrationProjectShell = {
          id: parent.projectId,
          title: "Project",
          workspaceRoot: "/workspace/project",
          defaultModelSelection: null,
          scripts: [],
          createdAt: "2026-08-01T00:00:00.000Z",
          updatedAt: "2026-08-01T00:00:00.000Z",
        };
        const activation = yield* Deferred.make<void>();
        const domainEvents = yield* Queue.unbounded<OrchestrationV2DomainEvent>();
        const sweeps = yield* Queue.unbounded<void>();
        const childLookup = yield* Deferred.make<void>();
        const lookups: Array<{ readonly branch: string; readonly refresh: boolean }> = [];
        const dependencies = Layer.mergeAll(
          Layer.mock(ThreadMetadataReads.ThreadMetadataReads)({
            list: (query) =>
              query?.threadIds === undefined
                ? Queue.offer(sweeps, undefined).pipe(
                    Effect.as({ snapshotSequence: 1, threads: [parent, child] }),
                  )
                : Effect.succeed({
                    snapshotSequence: 1,
                    threads: [parent, child].filter((thread) =>
                      query.threadIds!.includes(thread.id),
                    ),
                  }),
          }),
          Layer.mock(Orchestrator.OrchestratorV2)({
            streamDomainEvents: Stream.fromQueue(domainEvents),
            getShellSnapshot: () => Effect.die("PR refresh must use the bounded metadata port"),
            getThreadEventSequence: () => Effect.succeed(1),
            getThreadShell: (threadId) =>
              Effect.succeed([parent, child].find((thread) => thread.id === threadId) ?? null),
          }),
          Layer.mock(ProjectStore.ProjectStoreV2)({
            listShells: () => Effect.succeed([project]),
            getShell: () => Effect.succeedSome(project),
          }),
          Layer.mock(GitManager.GitManager)({
            branchPullRequest: (input, options) =>
              Effect.gen(function* () {
                const refresh = options?.refresh ?? false;
                lookups.push({ branch: input.branch, refresh });
                if (input.branch === child.branch && refresh) {
                  yield* Deferred.succeed(childLookup, undefined);
                }
                return null;
              }),
          }),
          Layer.mock(PullRequestService.PullRequestService)({}),
          Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
            resolve: () =>
              Effect.succeed({
                canonicalKey: "github.com/acme/project",
                locator: {
                  source: "git-remote" as const,
                  remoteName: "origin",
                  remoteUrl: "git@github.com:acme/project.git",
                },
                provider: SourceControlProviderKind.make("github"),
                owner: "acme",
                name: "project",
              }),
          }),
          Layer.mock(BackgroundPolicy.BackgroundPolicy)({
            shouldRunScopeWork: () => Effect.succeed(true),
            shouldRunOpportunisticWork: Effect.succeed(true),
          }),
          Layer.succeed(ServerActivation.ServerActivation, Deferred.await(activation)),
          Layer.succeed(
            SecureRandom.SecureRandom,
            SecureRandom.make((size) => new Uint8Array(size).fill(1)),
          ),
          FileSystem.layerNoop({}),
          Path.layer,
        );
        yield* Effect.gen(function* () {
          const service = yield* ThreadPullRequestService.make;
          yield* service.start();
          yield* Deferred.succeed(activation, undefined);

          // Startup backfill sweep, then one periodic sweep.
          yield* Queue.take(sweeps);
          yield* service.drain;
          yield* TestClock.adjust("1 minute");
          yield* Queue.take(sweeps);
          yield* service.drain;
          const sweepLookups = lookups.map((lookup) => lookup.branch).toSorted();

          // The child's own turn still discovers its pull request.
          yield* Queue.offer(domainEvents, {
            type: "checkpoint.captured",
            threadId: child.id,
          } as OrchestrationV2DomainEvent);
          yield* Deferred.await(childLookup);
          yield* service.drain;

          expect(sweepLookups).toEqual([parent.branch, parent.branch]);
          expect(lookups.slice(sweepLookups.length)).toEqual([
            { branch: child.branch, refresh: true },
          ]);
        }).pipe(Effect.provide(dependencies));
      }),
    ),
  );

  it.effect.each([
    { state: "closed", sameBranch: false },
    { state: "merged", sameBranch: false },
    { state: "closed", sameBranch: true },
    { state: "merged", sameBranch: true },
  ] as const)(
    "restores a $state automatic PR only for its own branch (sameBranch=$sameBranch)",
    ({ state, sameBranch }) =>
      Effect.scoped(
        Effect.gen(function* () {
          const savedReference = {
            projectId: ProjectId.make("project-1"),
            repository: "acme/project",
            number: 42,
            url: "https://github.com/acme/project/pull/42",
          };
          const thread = {
            ...threadShell("branch-pr-thread"),
            branch: "main",
            branchPullRequest: savedReference,
            linkedPullRequest: savedReference,
          };
          const project: OrchestrationProjectShell = {
            id: thread.projectId,
            title: "Project",
            workspaceRoot: "/workspace/project",
            defaultModelSelection: null,
            scripts: [],
            createdAt: "2026-08-01T00:00:00.000Z",
            updatedAt: "2026-08-01T00:00:00.000Z",
          };
          const activation = yield* Deferred.make<void>();
          const sweepStarted = yield* Deferred.make<void>();
          const commands: Array<OrchestrationV2ServerCommand> = [];
          const dependencies = Layer.mergeAll(
            Layer.mock(ThreadMetadataReads.ThreadMetadataReads)({
              list: () =>
                Deferred.succeed(sweepStarted, undefined).pipe(
                  Effect.as({
                    snapshotSequence: 1,
                    threads: [thread],
                  }),
                ),
            }),
            Layer.mock(Orchestrator.OrchestratorV2)({
              streamDomainEvents: Stream.never,
              dispatch: (command) =>
                Effect.sync(() => {
                  commands.push(command);
                  return { sequence: 2, storedEvents: [] };
                }),
            }),
            Layer.mock(ProjectStore.ProjectStoreV2)({
              listShells: () => Effect.succeed([project]),
              getShell: () => Effect.succeedSome(project),
            }),
            Layer.mock(GitManager.GitManager)({
              branchPullRequest: () => Effect.succeed(null),
            }),
            Layer.mock(PullRequestService.PullRequestService)({
              summary: () =>
                Effect.succeed({
                  ...savedReference,
                  provider: SourceControlProviderKind.make("github"),
                  title: "Previous branch work",
                  state,
                  headBranch: sameBranch ? thread.branch : "feature/previous-work",
                  baseBranch: "release",
                  updatedAt: "2026-09-19T00:00:00.000Z",
                }),
            }),
            Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
              resolve: () =>
                Effect.succeed({
                  canonicalKey: "github.com/acme/project",
                  locator: {
                    source: "git-remote" as const,
                    remoteName: "origin",
                    remoteUrl: "git@github.com:acme/project.git",
                  },
                  provider: SourceControlProviderKind.make("github"),
                  owner: "acme",
                  name: "project",
                }),
            }),
            Layer.mock(BackgroundPolicy.BackgroundPolicy)({}),
            Layer.succeed(ServerActivation.ServerActivation, Deferred.await(activation)),
            Layer.succeed(
              SecureRandom.SecureRandom,
              SecureRandom.make((size) => new Uint8Array(size).fill(1)),
            ),
            FileSystem.layerNoop({}),
            Path.layer,
          );
          yield* Effect.gen(function* () {
            const service = yield* ThreadPullRequestService.make;
            yield* service.start();
            yield* Deferred.succeed(activation, undefined);
            yield* Deferred.await(sweepStarted);
            yield* service.drain;
          }).pipe(Effect.provide(dependencies));

          if (sameBranch) {
            expect(commands).toEqual([]);
          } else {
            expect(commands).toHaveLength(1);
            expect(commands[0]).toMatchObject({
              type: "thread.pull-request.sync",
              threadId: thread.id,
              snapshotSequence: 1,
              expected: {
                workspaceRoot: project.workspaceRoot,
                branch: thread.branch,
                worktreePath: thread.worktreePath,
                linkedPullRequest: savedReference,
                branchPullRequest: savedReference,
              },
              branchPullRequest: null,
            });
            expect(commands[0]).not.toHaveProperty("linkedPullRequest");
          }
        }),
      ),
  );

  it.effect.each([false, true])(
    "confirms discovery before dispatch; changed identity=%s",
    (changedDuringConfirmation) =>
      Effect.scoped(
        Effect.gen(function* () {
          const thread = { ...threadShell("discovery"), branch: "feature/discovery" };
          const project: OrchestrationProjectShell = {
            id: thread.projectId,
            title: "Project",
            workspaceRoot: "/workspace/project",
            defaultModelSelection: null,
            scripts: [],
            repositoryIdentity: null,
            createdAt: "2026-08-01T00:00:00.000Z",
            updatedAt: "2026-08-01T00:00:00.000Z",
          };
          const activation = yield* Deferred.make<void>();
          const snapshotRead = yield* Deferred.make<void>();
          const stages: string[] = [];
          const optionsSeen: Array<
            Parameters<GitManager.GitManager["Service"]["branchPullRequest"]>[1]
          > = [];
          const identity = {
            canonicalKey: "github.com/pingdotgg/t3code",
            locator: {
              source: "git-remote" as const,
              remoteName: "origin",
              remoteUrl: "git@github.com:pingdotgg/t3code.git",
            },
            provider: SourceControlProviderKind.make("github"),
            displayName: "pingdotgg/t3code",
            owner: "pingdotgg",
            name: "t3code",
          };
          const dependencies = Layer.mergeAll(
            Layer.mock(ThreadMetadataReads.ThreadMetadataReads)({
              list: () =>
                Deferred.succeed(snapshotRead, undefined).pipe(
                  Effect.as({
                    snapshotSequence: 1,
                    threads: [thread],
                  }),
                ),
            }),
            Layer.mock(Orchestrator.OrchestratorV2)({
              streamDomainEvents: Stream.never,
              dispatch: () =>
                Effect.sync(() => {
                  stages.push("dispatch");
                  return { sequence: 2, storedEvents: [] };
                }),
            }),
            Layer.mock(ProjectStore.ProjectStoreV2)({
              listShells: () => Effect.succeed([project]),
              getShell: () => Effect.succeedSome(project),
            }),
            Layer.mock(GitManager.GitManager)({
              branchPullRequest: (_input, options) =>
                Effect.sync(() => {
                  optionsSeen.push(options);
                  stages.push(options?.freshGitState ? "confirm" : "discover");
                  return {
                    number: 42,
                    title: "Discovery",
                    url: "https://github.com/pingdotgg/t3code/pull/42",
                    baseRef: "main",
                    headRef: "feature/discovery",
                    state: "merged" as const,
                    updatedAt: "2026-08-01T00:00:00.000Z",
                    closedAt: null,
                    mergedAt: null,
                    headSha: null,
                    repositoryKey:
                      options?.freshGitState && changedDuringConfirmation
                        ? "github.com/replacement/t3code"
                        : identity.canonicalKey,
                  };
                }),
            }),
            Layer.mock(PullRequestService.PullRequestService)({}),
            Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
              resolve: () => Effect.succeed(identity),
            }),
            Layer.mock(BackgroundPolicy.BackgroundPolicy)({
              shouldRunOpportunisticWork: Effect.succeed(true),
            }),
            Layer.succeed(ServerActivation.ServerActivation, Deferred.await(activation)),
            Layer.succeed(
              SecureRandom.SecureRandom,
              SecureRandom.make((size) => new Uint8Array(size).fill(1)),
            ),
            FileSystem.layerNoop({}),
            Path.layer,
          );
          yield* Effect.gen(function* () {
            const service = yield* ThreadPullRequestService.make;
            yield* service.start();
            yield* Deferred.succeed(activation, undefined);
            yield* Deferred.await(snapshotRead);
            yield* service.drain;
            expect(optionsSeen).toEqual([
              { refresh: false, confirmTerminal: false },
              { freshGitState: true },
            ]);
            expect(stages).toEqual(
              changedDuringConfirmation
                ? ["discover", "confirm"]
                : ["discover", "confirm", "dispatch"],
            );
          }).pipe(Effect.provide(dependencies));
        }),
      ),
  );

  it.effect("admits initial discovery and fresh confirmation as background Git independently", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = { ...threadShell("background-discovery"), branch: "feature/discovery" };
        const project: OrchestrationProjectShell = {
          id: thread.projectId,
          title: "Project",
          workspaceRoot: "/workspace/project",
          defaultModelSelection: null,
          scripts: [],
          repositoryIdentity: null,
          createdAt: "2026-08-01T00:00:00.000Z",
          updatedAt: "2026-08-01T00:00:00.000Z",
        };
        const identity = {
          canonicalKey: "github.com/acme/project",
          locator: {
            source: "git-remote" as const,
            remoteName: "origin",
            remoteUrl: "git@github.com:acme/project.git",
          },
          provider: SourceControlProviderKind.make("github"),
          owner: "acme",
          name: "project",
        };
        const pullRequest: GitManager.GitBranchPullRequest = {
          number: 42,
          title: "Discovery",
          url: "https://github.com/acme/project/pull/42",
          baseRef: "main",
          headRef: thread.branch,
          state: "open",
          updatedAt: "2026-08-01T00:00:00.000Z",
          closedAt: null,
          mergedAt: null,
          headSha: null,
          repositoryKey: identity.canonicalKey,
        };
        const activation = yield* Deferred.make<void>();
        const confirmationBarrier = yield* Deferred.make<void>();
        const holders = {
          discovery: ["background-hold-discovery-0", "background-hold-discovery-1"],
          confirmation: ["background-hold-confirmation-0", "background-hold-confirmation-1"],
        } as const;
        // Real driver: the service's Git goes through the upstream background lane.
        const gitFixture = yield* makeGitFixture([
          ...holders.discovery,
          ...holders.confirmation,
          "background-discovery",
          "background-confirmation",
          "foreground",
        ]);
        const requested = yield* Queue.unbounded<"discovery" | "confirmation">();
        const optionsSeen: Array<
          Parameters<GitManager.GitManager["Service"]["branchPullRequest"]>[1]
        > = [];
        const commands: Array<OrchestrationV2ServerCommand> = [];
        yield* Effect.addFinalizer(() =>
          Effect.all([
            Deferred.succeed(confirmationBarrier, undefined),
            Deferred.succeed(activation, undefined),
          ]),
        );
        // The upstream background lane admits two processes.
        const fillLane = (labels: ReadonlyArray<string>) =>
          Effect.gen(function* () {
            yield* Effect.forEach(labels, (label) =>
              gitFixture.run(label).pipe(asBackgroundGitWork, Effect.forkScoped),
            );
            const starts = yield* Effect.forEach(labels, () => Queue.take(gitFixture.starts));
            expect([...starts].toSorted()).toEqual([...labels].toSorted());
          });
        const releaseLane = (labels: ReadonlyArray<string>) =>
          Effect.forEach(labels, gitFixture.release, { discard: true });
        const dependencies = Layer.mergeAll(
          Layer.mock(ThreadMetadataReads.ThreadMetadataReads)({
            list: () =>
              Effect.succeed({
                snapshotSequence: 1,
                threads: [thread],
              }),
          }),
          Layer.mock(Orchestrator.OrchestratorV2)({
            streamDomainEvents: Stream.never,
            dispatch: (command) =>
              Effect.sync(() => {
                commands.push(command);
                return { sequence: 2, storedEvents: [] };
              }),
          }),
          Layer.mock(ProjectStore.ProjectStoreV2)({
            listShells: () => Effect.succeed([project]),
            getShell: () => Effect.succeedSome(project),
          }),
          Layer.mock(GitManager.GitManager)({
            branchPullRequest: (_input, options) =>
              Effect.gen(function* () {
                optionsSeen.push(options);
                const stage = options?.freshGitState ? "confirmation" : "discovery";
                // Hold confirmation before admission so its lane can be filled independently.
                if (stage === "confirmation") yield* Deferred.await(confirmationBarrier);
                yield* Queue.offer(requested, stage);
                return yield* gitFixture
                  .run(`background-${stage}`)
                  .pipe(Effect.orDie, Effect.as(pullRequest));
              }),
          }),
          Layer.mock(PullRequestService.PullRequestService)({}),
          Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
            resolve: () => Effect.succeed(identity),
          }),
          Layer.mock(BackgroundPolicy.BackgroundPolicy)({
            shouldRunOpportunisticWork: Effect.succeed(true),
          }),
          Layer.succeed(ServerActivation.ServerActivation, Deferred.await(activation)),
          Layer.succeed(
            SecureRandom.SecureRandom,
            SecureRandom.make((size) => new Uint8Array(size).fill(1)),
          ),
          FileSystem.layerNoop({}),
          Path.layer,
        );
        yield* Effect.gen(function* () {
          yield* fillLane(holders.discovery);
          const service = yield* ThreadPullRequestService.make;
          yield* service.start();
          yield* Deferred.succeed(activation, undefined);
          expect(yield* Queue.take(requested)).toBe("discovery");
          yield* Effect.yieldNow;
          expect(yield* Queue.size(gitFixture.starts)).toBe(0);
          const foreground = yield* gitFixture.run("foreground").pipe(Effect.forkScoped);
          expect(yield* Queue.take(gitFixture.starts)).toBe("foreground");
          yield* gitFixture.release("foreground");
          yield* Fiber.join(foreground);
          yield* releaseLane(holders.discovery);
          expect(yield* Queue.take(gitFixture.starts)).toBe("background-discovery");
          yield* gitFixture.release("background-discovery");

          yield* fillLane(holders.confirmation);
          yield* Deferred.succeed(confirmationBarrier, undefined);
          expect(yield* Queue.take(requested)).toBe("confirmation");
          yield* Effect.yieldNow;
          expect(yield* Queue.size(gitFixture.starts)).toBe(0);
          yield* releaseLane(holders.confirmation);
          expect(yield* Queue.take(gitFixture.starts)).toBe("background-confirmation");
          yield* gitFixture.release("background-confirmation");
          yield* service.drain;
          expect(optionsSeen).toEqual([
            { refresh: false, confirmTerminal: false },
            { freshGitState: true },
          ]);
          expect(commands).toHaveLength(1);
          expect(commands[0]).toMatchObject({
            type: "thread.pull-request.sync",
            threadId: thread.id,
            projectId: project.id,
            snapshotSequence: 1,
            expected: {
              workspaceRoot: project.workspaceRoot,
              branch: thread.branch,
              worktreePath: null,
              linkedPullRequest: null,
              branchPullRequest: null,
            },
            branchPullRequest: {
              projectId: project.id,
              repository: "acme/project",
              number: pullRequest.number,
              url: pullRequest.url,
            },
          });
        }).pipe(Effect.provide(dependencies));
      }),
    ).pipe(Effect.provide(gitDependencies.pipe(Layer.provideMerge(NodeCrypto.layer)))),
  );

  it.effect("folds a thread's queued requests together but not one arriving mid-lookup", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = { ...threadShell("turn-thread"), branch: "feature/turn" };
        const project: OrchestrationProjectShell = {
          id: thread.projectId,
          title: "Project",
          workspaceRoot: "/workspace/project",
          defaultModelSelection: null,
          scripts: [],
          repositoryIdentity: null,
          createdAt: "2026-08-01T00:00:00.000Z",
          updatedAt: "2026-08-01T00:00:00.000Z",
        };
        const runId = RunId.make("run:turn-thread");
        const checkpointCaptured = (key: string): OrchestrationV2DomainEvent => ({
          type: "checkpoint.captured",
          id: EventId.make(`event:checkpoint:${key}`),
          threadId: thread.id,
          occurredAt: NOW,
          payload: {
            id: CheckpointId.make(`checkpoint:${key}`),
            threadId: thread.id,
            scopeId: CheckpointScopeId.make("checkpoint-scope:turn-thread"),
            runId,
            nodeId: NodeId.make("node:turn-thread"),
            parentCheckpointId: null,
            ordinalWithinScope: 0,
            appRunOrdinal: null,
            ref: CheckpointRef.make(`checkpoint-ref:${key}`),
            status: "ready",
            files: [],
            capturedAt: NOW,
          },
        });
        const runCompleted: OrchestrationV2DomainEvent = {
          type: "run.updated",
          id: EventId.make("event:run-completed"),
          threadId: thread.id,
          runId,
          occurredAt: NOW,
          payload: {
            id: runId,
            threadId: thread.id,
            ordinal: 1,
            providerInstanceId: thread.providerInstanceId,
            modelSelection: thread.modelSelection,
            providerThreadId: null,
            userMessageId: MessageId.make("message:turn-thread"),
            rootNodeId: null,
            activeAttemptId: null,
            status: "completed",
            requestedAt: NOW,
            startedAt: NOW,
            completedAt: NOW,
            checkpointId: null,
            contextHandoffId: null,
          },
        };
        const metadataUpdated = (key: string): OrchestrationV2DomainEvent => ({
          type: "thread.metadata-updated",
          id: EventId.make(`event:metadata:${key}`),
          threadId: thread.id,
          occurredAt: NOW,
          payload: {
            createdBy: thread.createdBy,
            creationSource: thread.creationSource,
            id: thread.id,
            projectId: thread.projectId,
            title: thread.title,
            providerInstanceId: thread.providerInstanceId,
            modelSelection: thread.modelSelection,
            runtimeMode: thread.runtimeMode,
            interactionMode: thread.interactionMode,
            branch: thread.branch,
            worktreePath: thread.worktreePath,
            activeProviderThreadId: thread.activeProviderThreadId,
            lineage: thread.lineage,
            forkedFrom: null,
            createdAt: thread.createdAt,
            updatedAt: thread.updatedAt,
            archivedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
            deletedAt: null,
          },
        });

        const activation = yield* Deferred.make<void>();
        const sweepStarted = yield* Deferred.make<void>();
        const releaseSweep = yield* Deferred.make<void>();
        const queuedBehindSweep = yield* Deferred.make<void>();
        const startFollowUp = yield* Deferred.make<void>();
        const lookupStarted = yield* Deferred.make<void>();
        const releaseLookup = yield* Deferred.make<void>();
        const queuedDuringLookup = yield* Deferred.make<void>();
        const threadReads: Array<ThreadId> = [];
        const identityRefreshes: Array<boolean | undefined> = [];
        const lookupRefreshes: Array<boolean | undefined> = [];
        let holdNextLookup = false;

        // The stream pulls each step only after the service has handled every
        // event before it, which orders the events against the worker.
        const domainEvents = Stream.fromEffectDrain(Deferred.await(sweepStarted)).pipe(
          Stream.concat(
            Stream.fromIterable([
              checkpointCaptured("turn"),
              runCompleted,
              metadataUpdated("turn"),
            ]),
          ),
          Stream.concat(Stream.fromEffectDrain(Deferred.succeed(queuedBehindSweep, undefined))),
          Stream.concat(Stream.fromEffectDrain(Deferred.await(startFollowUp))),
          Stream.concat(Stream.make(metadataUpdated("follow-up"))),
          Stream.concat(Stream.fromEffectDrain(Deferred.await(lookupStarted))),
          Stream.concat(Stream.make(checkpointCaptured("follow-up"))),
          Stream.concat(Stream.fromEffectDrain(Deferred.succeed(queuedDuringLookup, undefined))),
          Stream.concat(Stream.never),
        );

        const layerDependencies = Layer.mergeAll(
          Layer.mock(Orchestrator.OrchestratorV2)({
            streamDomainEvents: domainEvents,
            getThreadEventSequence: () => Effect.succeed(1),
          }),
          Layer.mock(ThreadMetadataReads.ThreadMetadataReads)({
            list: (query) =>
              query?.threadIds === undefined
                ? Deferred.succeed(sweepStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseSweep)),
                    Effect.as({ snapshotSequence: 1, threads: [] }),
                  )
                : Effect.sync(() => {
                    const threadId = query.threadIds![0]!;
                    threadReads.push(threadId);
                    return { snapshotSequence: 1, threads: threadId === thread.id ? [thread] : [] };
                  }),
          }),
          Layer.mock(ProjectStore.ProjectStoreV2)({
            listShells: () => Effect.succeed([project]),
          }),
          Layer.mock(GitManager.GitManager)({
            // The first follow-up lookup holds until an event has arrived during it.
            branchPullRequest: (_input, options) =>
              Effect.suspend(() => {
                lookupRefreshes.push(options?.refresh);
                if (!holdNextLookup) return Effect.void;
                holdNextLookup = false;
                return Deferred.succeed(lookupStarted, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseLookup)),
                );
              }).pipe(Effect.as(null)),
          }),
          Layer.mock(PullRequestService.PullRequestService)({}),
          Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
            resolve: (_root, options) =>
              Effect.sync(() => {
                identityRefreshes.push(options?.refresh);
                return {
                  canonicalKey: "github.com/pingdotgg/t3code",
                  locator: {
                    source: "git-remote" as const,
                    remoteName: "origin",
                    remoteUrl: "git@github.com:pingdotgg/t3code.git",
                  },
                  provider: SourceControlProviderKind.make("github"),
                  displayName: "pingdotgg/t3code",
                  owner: "pingdotgg",
                  name: "t3code",
                };
              }),
          }),
          Layer.mock(BackgroundPolicy.BackgroundPolicy)({
            shouldRunOpportunisticWork: Effect.succeed(true),
          }),
          Layer.succeed(ServerActivation.ServerActivation, Deferred.await(activation)),
          Layer.succeed(
            SecureRandom.SecureRandom,
            SecureRandom.make((size) => new Uint8Array(size).fill(1)),
          ),
          FileSystem.layerNoop({}),
          Path.layer,
        );

        yield* Effect.gen(function* () {
          const service = yield* ThreadPullRequestService.make;
          yield* service.start();
          yield* Deferred.succeed(activation, undefined);

          // A finished turn's two refreshing events and a metadata update, all
          // queued behind the sweep, share one shell read and one forced lookup.
          yield* Deferred.await(queuedBehindSweep);
          yield* Deferred.succeed(releaseSweep, undefined);
          yield* service.drain;
          expect(threadReads).toEqual([thread.id]);
          expect(identityRefreshes).toEqual([true]);
          expect(lookupRefreshes).toEqual([true]);

          // An event arriving after the thread's item started is not folded
          // into it; it runs afterwards with its own refresh.
          holdNextLookup = true;
          yield* Deferred.succeed(startFollowUp, undefined);
          yield* Deferred.await(queuedDuringLookup);
          yield* Deferred.succeed(releaseLookup, undefined);
          yield* service.drain;
          expect(threadReads).toEqual([thread.id, thread.id, thread.id]);
          expect(identityRefreshes).toEqual([true, false, true]);
          expect(lookupRefreshes).toEqual([true, false, true]);
        }).pipe(Effect.provide(layerDependencies));
      }),
    ),
  );
});
