import {
  CheckpointId,
  CheckpointRef,
  CheckpointScopeId,
  EventId,
  MessageId,
  NodeId,
  RunId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";

import {
  ProjectId,
  ProviderInstanceId,
  SourceControlProviderKind,
  ThreadId,
  type OrchestrationProjectShell,
  type OrchestrationV2ThreadShell,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import * as GitManager from "../git/GitManager.ts";
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
            Layer.mock(Orchestrator.OrchestratorV2)({
              streamDomainEvents: Stream.never,
              getShellSnapshot: () =>
                Deferred.succeed(sweepStarted, undefined).pipe(
                  Effect.as({
                    schemaVersion: 2,
                    snapshotSequence: 1,
                    threads: [thread],
                    archivedThreads: [],
                  }),
                ),
              dispatch: (command) =>
                Effect.sync(() => {
                  commands.push(command);
                  return { sequence: 2, storedEvents: [] };
                }),
            }),
            Layer.mock(ProjectStore.ProjectStoreV2)({
              listShells: () => Effect.succeed([project]),
              getShell: () => Effect.succeed(Option.some(project)),
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
                  provider: "github" as const,
                  owner: "acme",
                  name: "project",
                }),
            }),
            Layer.succeed(ServerActivation.ServerActivation, Deferred.await(activation)),
            Layer.succeed(
              Crypto.Crypto,
              Crypto.make({
                randomBytes: (size) => new Uint8Array(size).fill(1),
                digest: (_algorithm, data) => Effect.succeed(data),
              }),
            ),
            FileSystem.layerNoop({}),
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
            provider: "github" as const,
            displayName: "pingdotgg/t3code",
            owner: "pingdotgg",
            name: "t3code",
          };
          const dependencies = Layer.mergeAll(
            Layer.mock(Orchestrator.OrchestratorV2)({
              streamDomainEvents: Stream.never,
              getShellSnapshot: () =>
                Deferred.succeed(snapshotRead, undefined).pipe(
                  Effect.as({
                    schemaVersion: 2,
                    snapshotSequence: 1,
                    threads: [thread],
                    archivedThreads: [],
                  }),
                ),
              dispatch: () =>
                Effect.sync(() => {
                  stages.push("dispatch");
                  return { sequence: 2, storedEvents: [] };
                }),
            }),
            Layer.mock(ProjectStore.ProjectStoreV2)({
              listShells: () => Effect.succeed([project]),
              getShell: () => Effect.succeed(Option.some(project)),
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
            Layer.succeed(ServerActivation.ServerActivation, Deferred.await(activation)),
            Layer.succeed(
              Crypto.Crypto,
              Crypto.make({
                randomBytes: (size) => new Uint8Array(size).fill(1),
                digest: (_algorithm, data) => Effect.succeed(data),
              }),
            ),
            FileSystem.layerNoop({}),
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
            // The startup sweep holds the worker until the turn's events are queued.
            getShellSnapshot: () =>
              Deferred.succeed(sweepStarted, undefined).pipe(
                Effect.andThen(Deferred.await(releaseSweep)),
                Effect.as({
                  schemaVersion: 2,
                  snapshotSequence: 1,
                  threads: [],
                  archivedThreads: [],
                }),
              ),
            getThreadEventSequence: () => Effect.succeed(1),
            getThreadShell: (threadId) =>
              Effect.sync(() => {
                threadReads.push(threadId);
                return threadId === thread.id ? thread : null;
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
                  provider: "github" as const,
                  displayName: "pingdotgg/t3code",
                  owner: "pingdotgg",
                  name: "t3code",
                };
              }),
          }),
          Layer.succeed(ServerActivation.ServerActivation, Deferred.await(activation)),
          Layer.succeed(
            Crypto.Crypto,
            Crypto.make({
              randomBytes: (size) => new Uint8Array(size).fill(1),
              digest: (_algorithm, data) => Effect.succeed(data),
            }),
          ),
          FileSystem.layerNoop({}),
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
