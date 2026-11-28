import {
  clientLaunchTestAdapter,
  makeClientLaunchHarness as makeHarness,
} from "./ClientLaunch.testkit.ts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as LinkedThreads from "../../linkedThreads/LinkedThreads.ts";
import * as ServerEnvironment from "../../../environment/ServerEnvironment.ts";
import { assert, it, vi } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  RunId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  OrchestratorMcpFailure,
  type ServerProvider,
  type ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Exit from "effect/Exit";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as EventSink from "../../../orchestration-v2/EventSink.ts";
import { type Caller } from "../../../mcp/threadAccess.ts";
import type { ThreadStartGate } from "../../../mcp/McpToolAccess.ts";
import * as ClientLaunch from "./ClientLaunchService.ts";
import * as FileSystem from "effect/FileSystem";
import * as ServerConfig from "../../../config.ts";
import * as McpInvocationContext from "../../../mcp/McpInvocationContext.ts";
import * as OrchestrationMcp from "../../../mcp/OrchestratorMcpService.ts";
import { launchThread } from "../../../mcp/toolkits/project/handlers.ts";
import { clientLaunchIds } from "./ClientLaunchIdentity.ts";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import {
  DispatchModeLimit,
  type DispatchModeRefusal,
} from "../../../orchestration-v2/DispatchModeLimit.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ProjectService from "../../../project/ProjectService.ts";
import * as Git from "../../../vcs/GitVcsDriver.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as DocumentText from "../../../provider/DocumentText.ts";
import * as CommandReceiptStore from "../../../orchestration-v2/CommandReceiptStore.ts";
import * as ThreadLaunch from "../../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as ProviderReplayHarness from "../../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Selector from "../../accountRouting/selectAutoAccount.ts";
import {
  AccountLaunchIntent,
  type AccountLaunchAdmission,
} from "../../accountRouting/accountBinding.ts";
import * as ProjectStore from "../../../orchestration-v2/ProjectStore.ts";
import * as ProviderInstances from "../../../provider/ProviderInstanceRegistry.ts";
import * as ProviderAdapterRegistry from "../../../orchestration-v2/ProviderAdapterRegistry.ts";
import { buildUnavailableProviderSnapshot } from "../../../provider/unavailableProviderSnapshot.ts";
import * as RemoteDelegation from "../../../peer/RemoteDelegation.ts";
import * as ScheduledTaskService from "../../../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../../../secrets/SecretRequests.ts";

const projectId = ProjectId.make("project:launch-test");
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.1-codex",
} as const;

const namespace = "synthetic-launch-service";
const modes = {
  runtimeMode: "full-access",
  interactionMode: "default",
  linkOrigin: undefined,
} as const;
const gate: ThreadStartGate = {
  start: Effect.succeed(modes),
  continueWithin: (_target, effect) => effect,
};
const input = { clientRequestId: "key", projectId, title: "Launch", message: "Do the task" };
const caller = Effect.gen(function* () {
  return {
    scope: {
      environmentId: EnvironmentId.make("synthetic"),
      requestNamespace: namespace,
      issuedAt: 0,
      capabilities: new Set(["orchestration" as const]),
      thread: undefined,
      client: { sessionId: "synthetic", label: "test", access: "full-access" as const },
    },
    threads: yield* ThreadManagement.ThreadManagementService,
    caller: undefined,
    limits: modes,
  } satisfies Caller;
});

it.effect("Auto launches and local delegation bind a concrete account before replay", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "jones-mcp-auto-" });
    const config = yield* ProviderReplayHarness.makeReplayServerConfig("mcp-auto").pipe(
      Effect.provideService(FileSystem.FileSystem, {
        ...fs,
        makeTempDirectory: () => Effect.succeed(cwd),
      }),
    );
    const winnerId = ProviderInstanceId.make("codex-routed");
    const representative: ServerProvider = {
      ...(yield* buildUnavailableProviderSnapshot({
        driverKind: "codex",
        instanceId: modelSelection.instanceId,
        reason: "Synthetic fixture",
      })),
      availability: "available",
      enabled: true,
      installed: true,
      status: "ready",
    };
    const firstWinner = { ...representative, instanceId: winnerId };
    let currentWinner = firstWinner;
    let expectedCwd = yield* fs.realPath(cwd);
    let beforeSelectionReturns: Effect.Effect<void> = Effect.void;
    const selected = { ...modelSelection, instanceId: winnerId };
    using select = vi.spyOn(Selector, "selectAutoAccount").mockImplementation((request) =>
      Effect.gen(function* () {
        assert.equal(request.projectId, projectId);
        assert.equal(request.cwd, expectedCwd);
        assert.deepEqual(request.modelSelection, modelSelection);
        const selection = { ...modelSelection, instanceId: currentWinner.instanceId };
        assert.isTrue(request.candidateAllowed?.(currentWinner, selection));
        yield* beforeSelectionReturns;
        return {
          modelSelection: selection,
          binding: { instanceId: selection.instanceId, digest: "synthetic" },
        };
      }),
    );
    const adapters = [
      clientLaunchTestAdapter,
      { ...clientLaunchTestAdapter, instanceId: winnerId },
    ];
    const project = {
      id: projectId,
      title: "Auto",
      workspaceRoot: cwd,
      repositoryIdentity: null,
      faviconPath: null,
      defaultModelSelection: modelSelection,
      defaultThreadEnvMode: null,
      scripts: [],
      createdAt: "2026-06-20T00:00:00.000Z",
      updatedAt: "2026-06-20T00:00:00.000Z",
      deletedAt: null,
    };
    const h = makeHarness({
      adapters,
      providers: [representative, firstWinner],
      projectRecord: project,
      serverSettings: {
        projectSettingsOverrides: {
          [projectId]: { providerInstances: { [winnerId]: { enabled: true } } },
        },
      },
      serverConfigLayer: Layer.succeed(ServerConfig.ServerConfig, { ...config, cwd }),
    });
    const dependencies = Layer.mergeAll(
      h.layer,
      Layer.mock(ProviderInstances.ProviderInstanceRegistry)({ getCatalogSource: () => undefined }),
      Layer.mock(ProjectStore.ProjectStoreV2)({}),
      ProviderAdapterRegistry.layerFromAdapters(adapters),
      Layer.mock(SecretRequests.SecretRequests)({}),
      Layer.mock(RemoteDelegation.RemoteDelegation)({}),
      Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
      Layer.succeed(ServerConfig.ServerConfig, { ...config, cwd }),
      DocumentText.layer.pipe(Layer.provide(NodeServices.layer)),
    );
    yield* Effect.gen(function* () {
      const context = yield* caller;
      const auto = {
        projectId,
        title: "Auto",
        modelSelection,
        message: "Start some work",
        accountRouting: { mode: "auto" as const },
        workspaceStrategy: { type: "root" as const },
      };
      const launch = (request: Parameters<typeof launchThread>[0]) =>
        launchThread(request, gate).pipe(
          Effect.provideService(McpInvocationContext.McpInvocationContext, context.scope),
        );
      const keyed = yield* launch({ ...auto, clientRequestId: "auto-1" });
      assert.deepEqual(keyed.modelSelection, selected);
      const lookup = yield* ClientLaunch.lookupForCaller(context, "auto-1");
      assert.equal(lookup.state, "launched");
      assert.equal(lookup.initialMessage, "accepted");
      currentWinner = representative;
      const replayed = yield* launch({ ...auto, clientRequestId: "auto-1" });
      assert.equal(replayed.threadId, keyed.threadId);
      assert.deepEqual(replayed.modelSelection, selected);
      const conflict = yield* launch({
        ...auto,
        clientRequestId: "auto-1",
        accountRouting: undefined,
        modelSelection: selected,
      }).pipe(Effect.flip);
      assert.equal(conflict.code, "request_conflict");
      assert.lengthOf(select.mock.calls, 1);
      expectedCwd = cwd;
      const unkeyed = yield* launch(auto);
      assert.deepEqual(unkeyed.modelSelection, modelSelection);
      assert.lengthOf(select.mock.calls, 2);

      const mcp = yield* OrchestrationMcp.OrchestratorMcpService;
      const scope = {
        ...context.scope,
        client: undefined,
        thread: {
          threadId: unkeyed.threadId,
          providerInstanceId: modelSelection.instanceId,
          providerSessionId: "synthetic-active-parent",
        },
      };
      assert.isTrue((yield* mcp.capabilities(scope)).features.accountAutoRouting);
      currentWinner = firstWinner;
      const task = {
        task: "Delegate fresh work",
        clientRequestId: "auto-child",
        accountRouting: auto.accountRouting,
        target: { providerInstanceId: modelSelection.instanceId, model: modelSelection.model },
      };
      const denied = yield* mcp
        .delegateTask(scope, {
          ...task,
          target: { ...task.target, environmentId: EnvironmentId.make("linked-peer") },
        })
        .pipe(Effect.flip);
      assert.equal(denied.code, "invalid_request");
      const missingDirectory = yield* mcp
        .delegateTask(scope, {
          ...task,
          workspaceStrategy: { type: "worktree", baseRef: "main" },
        })
        .pipe(Effect.flip);
      assert.equal(missingDirectory.code, "invalid_request");
      assert.lengthOf(select.mock.calls, 2);
      const child = yield* mcp.delegateTask(scope, task);
      assert.equal(child.providerInstanceId, winnerId);
      currentWinner = representative;
      const retried = yield* mcp.delegateTask(scope, task);
      assert.equal(retried.taskId, child.taskId);
      assert.equal(retried.providerInstanceId, winnerId);
      assert.lengthOf(select.mock.calls, 3);

      const firstCheckout = `${yield* fs.realPath(cwd)}/checkout-a`;
      const otherCheckout = `${yield* fs.realPath(cwd)}/checkout-b`;
      const alias = `${cwd}/checkout-alias`;
      yield* fs.makeDirectory(firstCheckout);
      yield* fs.makeDirectory(otherCheckout);
      const git = yield* Git.GitVcsDriver;
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const registered = Effect.provideService(Git.GitVcsDriver, {
        ...git,
        listWorktreePaths: () => Effect.succeed([firstCheckout, otherCheckout]),
      });
      for (const step of ["claim", "thread"] as const) {
        yield* fs.symlink(firstCheckout, alias);
        expectedCwd = firstCheckout;
        currentWinner = firstWinner;
        const worktree = {
          ...auto,
          clientRequestId: `auto-worktree-alias-${step}`,
          workspaceStrategy: { type: "existing_worktree" as const, worktreePath: alias },
        };
        const stop = failAfter(launches, step);
        const interrupted = yield* launch(worktree).pipe(
          registered,
          Effect.provideService(ThreadLaunch.ThreadLaunchService, {
            ...stop,
            launch: (request) =>
              stop
                .launch(request)
                .pipe(
                  Effect.provideService(ThreadManagement.ThreadManagementService, context.threads),
                ),
          }),
          Effect.exit,
        );
        assert.isTrue(Exit.isFailure(interrupted));
        const state = yield* ClientLaunch.lookupForCaller(context, worktree.clientRequestId);
        assert.isTrue(state.claimed);
        assert.equal(state.state, step === "claim" ? "absent" : "launched");
        assert.equal(state.initialMessage, "pending");
        yield* fs.remove(alias);
        yield* fs.symlink(otherCheckout, alias);
        expectedCwd = otherCheckout;
        currentWinner = representative;
        const observeRetry = failAfter(launches, "claim");
        let observed:
          | {
              request: ThreadLaunch.ThreadLaunchInput;
              admission: AccountLaunchAdmission | undefined;
            }
          | undefined;
        yield* launch(worktree).pipe(
          registered,
          Effect.provideService(ThreadLaunch.ThreadLaunchService, {
            ...observeRetry,
            launch: (request) =>
              Effect.gen(function* () {
                observed = { request, admission: yield* AccountLaunchIntent };
                return yield* observeRetry
                  .launch(request)
                  .pipe(
                    Effect.provideService(
                      ThreadManagement.ThreadManagementService,
                      context.threads,
                    ),
                  );
              }),
          }),
          Effect.exit,
        );
        assert.deepEqual(observed?.request.workspaceStrategy, {
          type: "existing_worktree",
          worktreePath: firstCheckout,
        });
        assert.deepEqual(observed?.request.modelSelection, selected);
        assert.deepEqual(observed?.admission?.binding, {
          instanceId: winnerId,
          digest: "synthetic",
        });
        yield* fs.remove(alias);
      }
      assert.lengthOf(select.mock.calls, 5);

      const reachedProbe = yield* Deferred.make<void>();
      const releaseProbe = yield* Deferred.make<void>();
      const reachedLaunch = yield* Deferred.make<void>();
      const releaseLaunch = yield* Deferred.make<void>();
      const attempts: Array<{
        request: ThreadLaunch.ThreadLaunchInput;
        admission: AccountLaunchAdmission | undefined;
      }> = [];
      const interruptedLaunches = failAfter(launches, "claim");
      const race = {
        ...auto,
        clientRequestId: "auto-worktree-race",
        workspaceStrategy: { type: "existing_worktree" as const, worktreePath: alias },
      };
      const attempt = launch(race).pipe(
        registered,
        Effect.provideService(ThreadLaunch.ThreadLaunchService, {
          ...interruptedLaunches,
          launch: (request) =>
            Effect.gen(function* () {
              attempts.push({ request, admission: yield* AccountLaunchIntent });
              yield* Deferred.succeed(reachedLaunch, undefined);
              yield* Deferred.await(releaseLaunch);
              return yield* interruptedLaunches
                .launch(request)
                .pipe(
                  Effect.provideService(ThreadManagement.ThreadManagementService, context.threads),
                );
            }),
        }),
        Effect.exit,
      );
      yield* fs.symlink(firstCheckout, alias);
      expectedCwd = firstCheckout;
      currentWinner = firstWinner;
      beforeSelectionReturns = Deferred.succeed(reachedProbe, undefined).pipe(
        Effect.andThen(Deferred.await(releaseProbe)),
      );
      const first = yield* attempt.pipe(Effect.forkScoped);
      yield* Deferred.await(reachedProbe);
      yield* fs.remove(alias);
      yield* fs.symlink(otherCheckout, alias);
      expectedCwd = otherCheckout;
      currentWinner = representative;
      beforeSelectionReturns = Effect.void;
      const second = yield* attempt.pipe(Effect.forkScoped);
      yield* Deferred.await(reachedLaunch);
      yield* Deferred.succeed(releaseProbe, undefined);
      yield* Deferred.succeed(releaseLaunch, undefined);
      assert.isTrue(Exit.isFailure(yield* Fiber.join(first)));
      assert.isTrue(Exit.isFailure(yield* Fiber.join(second)));
      assert.lengthOf(attempts, 2);
      for (const { request, admission } of attempts) {
        assert.deepEqual(request.workspaceStrategy, {
          type: "existing_worktree",
          worktreePath: otherCheckout,
        });
        assert.deepEqual(request.modelSelection, modelSelection);
        assert.equal(admission?.binding.instanceId, modelSelection.instanceId);
      }
      assert.lengthOf(select.mock.calls, 7);
    }).pipe(Effect.provide(OrchestrationMcp.layer.pipe(Layer.provideMerge(dependencies))));
  }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);
const failAfter = (
  launches: ThreadLaunch.ThreadLaunchService["Service"],
  step: "claim" | "thread" | "message",
) => ({
  ...launches,
  launch: (request: ThreadLaunch.ThreadLaunchInput) =>
    Effect.gen(function* () {
      const threads = yield* ThreadManagement.ThreadManagementService;
      if (step !== "claim")
        yield* threads
          .dispatch({
            type: "thread.create",
            commandId: request.commandId,
            threadId: request.threadId!,
            projectId,
            title: request.title,
            modelSelection: request.modelSelection,
            runtimeMode: request.runtimeMode,
            interactionMode: request.interactionMode,
            branch: null,
            worktreePath:
              request.workspaceStrategy.type === "existing_worktree"
                ? request.workspaceStrategy.worktreePath
                : null,
            createdBy: "agent",
            creationSource: "mcp",
          })
          .pipe(Effect.orDie);
      if (step === "message") {
        const message = request.initialMessage;
        yield* threads
          .dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`${request.commandId}:initial-message`),
            threadId: request.threadId!,
            messageId: message!.messageId!,
            text: message!.text,
            attachments: [],
            modelSelection: request.modelSelection,
            dispatchMode: { type: "defer_start", workspaceStrategy: request.workspaceStrategy },
            createdBy: "agent",
            creationSource: "mcp",
          })
          .pipe(Effect.orDie);
      }
      return yield* new ThreadLaunch.ThreadLaunchError({
        operation: "create-thread",
        commandId: request.commandId,
        projectId,
        cause: "synthetic interruption",
      });
    }),
});

it.effect.each(["runtime", "interaction"] as const)(
  "F1: a %s mode refusal after real creation retains an unknown launch outcome",
  (raisedMode) =>
    Effect.gen(function* () {
      const created = yield* Deferred.make<ThreadId>();
      const resume = yield* Deferred.make<void>();
      const h = makeHarness({
        afterCreate: (threadId) =>
          Deferred.succeed(created, threadId).pipe(Effect.andThen(Deferred.await(resume))),
      });
      yield* Effect.gen(function* () {
        const restricted = {
          runtimeMode: "approval-required",
          interactionMode: "plan",
          linkOrigin: undefined,
        } as const;
        const refused = yield* Ref.make<DispatchModeRefusal | undefined>(undefined);
        const guarded: ThreadStartGate = {
          start: Effect.succeed(restricted),
          continueWithin: (_target, effect) =>
            effect.pipe(
              Effect.provideService(DispatchModeLimit, { ...restricted, refused }),
              Effect.catch((error) =>
                Effect.flatMap(Ref.get(refused), (refusal) =>
                  Effect.fail(
                    refusal === undefined
                      ? error
                      : new OrchestratorMcpFailure({
                          code:
                            refusal.mode === "runtime"
                              ? "runtime_mode_escalation_denied"
                              : "interaction_mode_escalation_denied",
                          message: "Synthetic caller observes the real dispatch refusal.",
                        }),
                  ),
                ),
              ),
            ),
        };
        const context = { ...(yield* caller), limits: restricted };
        const pending = yield* ClientLaunch.launch(input, context, restricted, guarded).pipe(
          Effect.flip,
          Effect.forkChild,
        );
        const threadId = yield* Deferred.await(created);
        yield* context.threads.dispatch(
          raisedMode === "runtime"
            ? {
                type: "thread.runtime-mode.set",
                commandId: CommandId.make("synthetic-user-raise"),
                threadId,
                runtimeMode: "full-access",
              }
            : {
                type: "thread.interaction-mode.set",
                commandId: CommandId.make("synthetic-user-raise"),
                threadId,
                interactionMode: "default",
              },
        );
        yield* Deferred.succeed(resume, undefined);
        const error = yield* Fiber.join(pending);
        assert.equal((yield* Ref.get(refused))?.mode, raisedMode);
        assert.equal(error.code, "orchestration_error");
        const state = yield* ClientLaunch.lookup(namespace, input.clientRequestId);
        assert.equal(state.state, "launched");
        assert.equal(state.threadId, threadId);
        assert.equal(state.initialMessage, "pending");
        assert.equal(state.runId, null);
      }).pipe(Effect.provide(h.layer));
    }),
);

it.effect.each(["runtime", "interaction"] as const)(
  "agent launch preserves a stricter inherited %s limit before its initial message",
  (broaderMode) =>
    Effect.gen(function* () {
      const refused = yield* Ref.make<DispatchModeRefusal | undefined>(undefined);
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const { commandId, threadId, messageId } = clientLaunchIds(namespace, "strict-inherited");
      const error = yield* launches
        .launch({
          commandId,
          threadId,
          projectId,
          title: "Inherited limit",
          modelSelection,
          runtimeMode: broaderMode === "runtime" ? "full-access" : "approval-required",
          interactionMode: broaderMode === "interaction" ? "default" : "plan",
          workspaceStrategy: { type: "root" },
          initialMessage: { messageId, text: "Task", attachments: [] },
          createdBy: "agent",
          creationSource: "mcp",
        })
        .pipe(
          Effect.provideService(DispatchModeLimit, {
            runtimeMode: "approval-required",
            interactionMode: "plan",
            refused,
          }),
          Effect.flip,
        );
      assert.equal(error._tag, "ThreadLaunchError");
      assert.equal((yield* Ref.get(refused))?.mode, broaderMode);
      const threads = yield* ThreadManagement.ThreadManagementService;
      const projection = yield* threads.getThreadProjection(threadId);
      assert.lengthOf(projection.messages, 0);
      assert.lengthOf(projection.runs, 0);
    }).pipe(Effect.provide(makeHarness().layer)),
);

it.effect(
  "T1/T2/T4: replays one thread and message, conflicts on canonical changes, ignores changed defaults",
  () =>
    Effect.gen(function* () {
      const context = yield* caller;
      const first = yield* ClientLaunch.launch(input, context, modes, gate);
      const second = yield* ClientLaunch.launch(input, context, modes, gate).pipe(
        Effect.provideService(ProjectService.ProjectService, {
          ...(yield* ProjectService.ProjectService),
          getById: () => Effect.die("Replay must not resolve defaults"),
        }),
      );
      assert.equal(first.threadId, second.threadId);
      assert.isTrue(second.replayed);
      const threads = yield* ThreadManagement.ThreadManagementService;
      const projection = yield* threads.getThreadProjection(first.threadId);
      assert.lengthOf(projection.messages, 1);
      assert.lengthOf(projection.runs, 1);
      for (const change of [
        { title: "other" },
        { message: "other" },
        { modelSelection },
        { runtimeMode: "full-access" as const },
        { interactionMode: "default" as const },
        { workspaceStrategy: { type: "worktree" as const, branch: "other" } },
      ]) {
        const error = yield* ClientLaunch.launch(
          { ...input, ...change },
          context,
          modes,
          gate,
        ).pipe(Effect.flip);
        assert.equal(error.code, "request_conflict");
      }
      assert.lengthOf((yield* threads.getThreadProjection(first.threadId)).messages, 1);
    }).pipe(Effect.provide(makeHarness().layer)),
);

it.effect("a keyed launch carries structured output and rejects a changed schema", () =>
  Effect.gen(function* () {
    const context = yield* caller;
    const outputSchema = { type: "object", properties: { count: { type: "integer" } } };
    const request = { ...input, outputSchema };
    const first = yield* ClientLaunch.launch(request, context, modes, gate);
    const replay = yield* ClientLaunch.launch(request, context, modes, gate);
    assert.equal(replay.threadId, first.threadId);
    const rejected = yield* ClientLaunch.launch(
      { ...request, outputSchema: { ...outputSchema, required: ["count"] } },
      context,
      modes,
      gate,
    ).pipe(Effect.flip);
    assert.equal(rejected.code, "request_conflict");
    const projection = yield* context.threads.getThreadProjection(first.threadId);
    assert.lengthOf(projection.runs, 1);
    assert.deepEqual(projection.runs[0]?.outputSchema, outputSchema);
  }).pipe(Effect.provide(makeHarness().layer)),
);

it.effect.each(["claim", "thread", "message"] as const)(
  "T5/T7/T12: reconciles interruption after %s",
  (step) =>
    Effect.gen(function* () {
      const context = yield* caller;
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const fault = failAfter(launches, step);
      const result = yield* Effect.exit(
        ClientLaunch.launch(input, context, modes, gate).pipe(
          Effect.provideService(ThreadLaunch.ThreadLaunchService, {
            ...launches,
            launch: (request) =>
              fault
                .launch(request)
                .pipe(Effect.provideService(ThreadManagement.ThreadManagementService, threads)),
          }),
        ),
      );
      assert.isTrue(Exit.isFailure(result));
      const state = yield* ClientLaunch.lookup(namespace, input.clientRequestId);
      assert.isTrue(state.claimed);
      assert.equal(state.state, step === "claim" ? "absent" : "launched");
      assert.equal(state.initialMessage, step === "message" ? "accepted" : "pending");
      if (step === "message") assert.equal(state.preparation, "unscheduled");
      const [first, second] = yield* Effect.all(
        [
          ClientLaunch.launch(input, context, modes, gate),
          ClientLaunch.launch(input, context, modes, gate),
        ],
        { concurrency: 2 },
      );
      assert.equal(first.threadId, second.threadId);
      const projection = yield* threads.getThreadProjection(first.threadId);
      assert.lengthOf(projection.messages, 1);
      assert.lengthOf(projection.runs, 1);
    }).pipe(Effect.provide(makeHarness().layer)),
);

it.effect("T5/T6: absent is not proof, fence is authoritative, launch cannot cross it", () =>
  Effect.gen(function* () {
    assert.equal((yield* ClientLaunch.lookup(namespace, "key")).state, "absent");
    assert.equal((yield* ClientLaunch.lookup(namespace, "key")).initialMessage, "unknown");
    assert.equal((yield* ClientLaunch.fence(namespace, "key")).basis, "fence");
    assert.equal(
      (yield* ClientLaunch.launch(input, yield* caller, modes, gate).pipe(Effect.flip)).code,
      "launch_fenced",
    );
    const threads = yield* ThreadManagement.ThreadManagementService;
    assert.isNull(yield* threads.getThreadShell(clientLaunchIds(namespace, "key").threadId));
    const next = yield* ClientLaunch.launch(
      { ...input, clientRequestId: "next" },
      yield* caller,
      modes,
      gate,
    );
    assert.equal((yield* ClientLaunch.fence(namespace, "next")).threadId, next.threadId);
  }).pipe(Effect.provide(makeHarness().layer)),
);

it.effect("T6: concurrent fence and launch have one durable winner", () =>
  Effect.gen(function* () {
    const context = yield* caller;
    const [launched, fenced] = yield* Effect.all(
      [
        Effect.exit(ClientLaunch.launch(input, context, modes, gate)),
        ClientLaunch.fence(namespace, "key"),
      ],
      { concurrency: 2 },
    );
    const state = yield* ClientLaunch.lookup(namespace, "key");
    assert.equal(state.state, fenced.state);
    if (state.state === "no_effect") {
      assert.isTrue(Exit.isFailure(launched));
      assert.isNull(
        yield* context.threads.getThreadShell(clientLaunchIds(namespace, "key").threadId),
      );
    } else {
      assert.equal(state.state, "launched");
      assert.lengthOf((yield* context.threads.getThreadProjection(state.threadId!)).messages, 1);
    }
  }).pipe(Effect.provide(makeHarness().layer)),
);

it.effect(
  "T12/T13: progress without an active preparation holds, and later runs do not replace the bound run",
  () =>
    Effect.gen(function* () {
      const context = yield* caller;
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const ids = clientLaunchIds(namespace, "key");
      const fault = failAfter(launches, "message");
      yield* Effect.exit(
        ClientLaunch.launch(input, context, modes, gate).pipe(
          Effect.provideService(ThreadLaunch.ThreadLaunchService, {
            ...launches,
            launch: (request) =>
              fault
                .launch(request)
                .pipe(Effect.provideService(ThreadManagement.ThreadManagementService, threads)),
          }),
        ),
      );
      const before = yield* ClientLaunch.lookup(namespace, "key");
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      yield* receipts.insertIfAbsent({
        commandId: CommandId.make(`${ids.commandId}:progress:setup`),
        threadId: ids.threadId,
        commandType: "prepared-run.progress",
        acceptedAt: yield* DateTime.now,
        resultSequence: 0,
        status: "accepted",
        error: null,
      });
      const held = yield* ClientLaunch.lookup(namespace, "key");
      assert.equal(held.preparation, "unknown_effect");
      yield* ClientLaunch.launch(input, context, modes, gate).pipe(
        Effect.provideService(ThreadLaunch.ThreadLaunchService, {
          ...launches,
          launch: () => Effect.die("Unknown progress must never be retried"),
        }),
      );
      const original = yield* threads.getThreadProjection(ids.threadId);
      const later = {
        ...original.runs[0]!,
        id: RunId.make("unrelated-run"),
        userMessageId: MessageId.make("unrelated-message"),
      };
      const bound = yield* ClientLaunch.lookup(namespace, "key").pipe(
        Effect.provideService(ThreadManagement.ThreadManagementService, {
          ...threads,
          getThreadProjection: () =>
            Effect.succeed({ ...original, runs: [...original.runs, later] }),
        }),
      );
      assert.equal(bound.runId, before.runId);
      assert.equal(bound.runStatus, before.runStatus);
    }).pipe(Effect.provide(makeHarness().layer)),
);

it.effect(
  "T13: omitted message, rejected message, deleted thread and rejected launch remain distinct",
  () =>
    Effect.gen(function* () {
      const context = yield* caller;
      yield* ClientLaunch.launch(
        { clientRequestId: "idle", projectId, title: "Idle" },
        context,
        modes,
        gate,
      );
      assert.equal((yield* ClientLaunch.lookup(namespace, "idle")).initialMessage, "not_requested");
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const fault = failAfter(launches, "thread");
      yield* Effect.exit(
        ClientLaunch.launch(input, context, modes, gate).pipe(
          Effect.provideService(ThreadLaunch.ThreadLaunchService, {
            ...launches,
            launch: (request) =>
              fault
                .launch(request)
                .pipe(
                  Effect.provideService(ThreadManagement.ThreadManagementService, context.threads),
                ),
          }),
        ),
      );
      const ids = clientLaunchIds(namespace, "key");
      const sink = yield* EventSink.EventSinkV2;
      yield* sink.commitRejectedCommand({
        commandId: CommandId.make(`${ids.commandId}:initial-message`),
        threadId: ids.threadId,
        commandType: "message.dispatch",
        rejectedAt: yield* DateTime.now,
        error: "synthetic refusal",
      });
      const rejected = yield* ClientLaunch.lookup(namespace, "key");
      assert.equal(rejected.initialMessage, "rejected");
      assert.equal(rejected.reason, "synthetic refusal");
      yield* ClientLaunch.launch(input, context, modes, gate).pipe(
        Effect.provideService(ThreadLaunch.ThreadLaunchService, {
          ...launches,
          launch: () => Effect.die("Rejected message must remain held"),
        }),
      );
      const deleted = yield* ClientLaunch.lookup(namespace, "key").pipe(
        Effect.provideService(ThreadManagement.ThreadManagementService, {
          ...context.threads,
          getThreadShell: () => Effect.succeed(null),
        }),
      );
      assert.isTrue(deleted.threadDeleted);
      const other = clientLaunchIds(namespace, "reject");
      yield* sink.commitRejectedCommand({
        commandId: other.commandId,
        threadId: other.threadId,
        commandType: "thread.create",
        rejectedAt: yield* DateTime.now,
        error: "ordinary rejection",
      });
      assert.equal((yield* ClientLaunch.lookup(namespace, "reject")).basis, "rejection");
    }).pipe(Effect.provide(makeHarness().layer)),
);

it.effect(
  "T4/T12: accepted worktree replay skips a deleted base and keeps one active preparation",
  () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const hold = yield* Deferred.make<void>();
      const h = makeHarness({
        runSetup: () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(hold)),
            Effect.as({ status: "no-script" as const }),
          ),
      });
      yield* Effect.gen(function* () {
        const context = yield* caller;
        const request = {
          ...input,
          workspaceStrategy: {
            type: "worktree" as const,
            baseRef: "main",
            startFromOrigin: true,
            branch: "synthetic-feature",
          },
        };
        const first = yield* ClientLaunch.launch(request, context, modes, gate);
        yield* Deferred.await(entered);
        assert.equal((yield* ClientLaunch.lookup(namespace, "key")).preparation, "in_progress");
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const replay = ClientLaunch.launch(request, context, modes, gate).pipe(
          Effect.provideService(ThreadLaunch.ThreadLaunchService, {
            ...launches,
            checkWorktreeBase: () => Effect.die("Accepted replay must not inspect a deleted base"),
            launch: () => Effect.die("Active preparation must not be relaunched"),
          }),
        );
        const results = yield* Effect.all([replay, replay], { concurrency: 2 });
        assert.isTrue(
          results.every((result) => result.replayed && result.threadId === first.threadId),
        );
        assert.lengthOf(h.runSetup.mock.calls, 1);
        assert.lengthOf(h.createWorktree.mock.calls, 1);
      }).pipe(Effect.provide(h.layer));
    }),
);

it.effect("T15: a stale unscheduled retry holds progress recorded before reservation", () =>
  Effect.gen(function* () {
    const h = makeHarness();
    yield* Effect.gen(function* () {
      const context = yield* caller;
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const request = {
        ...input,
        workspaceStrategy: { type: "worktree" as const, branch: "feature" },
      };
      const fault = failAfter(launches, "message");
      yield* Effect.exit(
        ClientLaunch.launch(request, context, modes, gate).pipe(
          Effect.provideService(ThreadLaunch.ThreadLaunchService, {
            ...launches,
            launch: (value) =>
              fault
                .launch(value)
                .pipe(Effect.provideService(ThreadManagement.ThreadManagementService, threads)),
          }),
        ),
      );
      assert.equal((yield* ClientLaunch.lookup(namespace, "key")).preparation, "unscheduled");
      const entered = yield* Deferred.make<ThreadLaunch.ThreadLaunchInput>();
      const resume = yield* Deferred.make<void>();
      const retry = yield* ClientLaunch.launch(request, context, modes, gate).pipe(
        Effect.provideService(ThreadLaunch.ThreadLaunchService, {
          ...launches,
          launch: (value) =>
            Deferred.succeed(entered, value).pipe(
              Effect.andThen(Deferred.await(resume)),
              Effect.andThen(launches.launch(value)),
            ),
        }),
        Effect.forkChild,
      );
      const captured = yield* Deferred.await(entered);
      const ids = clientLaunchIds(namespace, "key");
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      yield* receipts.insertIfAbsent({
        commandId: CommandId.make(`${ids.commandId}:progress:worktree`),
        threadId: ids.threadId,
        commandType: "prepared-run.progress",
        acceptedAt: yield* DateTime.now,
        resultSequence: 0,
        status: "accepted",
        error: null,
      });
      yield* Deferred.succeed(resume, undefined);
      const result = yield* Fiber.join(retry);
      assert.equal(result.preparation, "unknown_effect");
      assert.isTrue(result.replayed);
      assert.equal(Reflect.get(captured, "holdPreparationAfterProgress"), true);
      assert.equal(h.createWorktree.mock.calls.length, 0);
      assert.equal(h.runSetup.mock.calls.length, 0);
      assert.isFalse(yield* launches.preparationActive(ids.commandId));
    }).pipe(Effect.provide(h.layer));
  }),
);

it.effect(
  "linked navigation uses real keyed claims, excludes fences/preflight and survives initial-message refusal",
  () =>
    Effect.gen(function* () {
      const client = yield* caller;
      const parent = yield* ClientLaunch.launch(
        { ...input, clientRequestId: "parent", message: undefined },
        client,
        modes,
        gate,
      );
      const parentShell = yield* client.threads.getThreadShell(parent.threadId);
      assert.isNotNull(parentShell);
      const context: Caller = {
        ...client,
        caller: parentShell!,
        scope: {
          ...client.scope,
          client: undefined,
          thread: {
            threadId: parent.threadId,
            providerInstanceId: modelSelection.instanceId,
            providerSessionId: "synthetic",
          },
        },
      };
      yield* Effect.gen(function* () {
        const links = yield* LinkedThreads.LinkedThreads;
        const request = { ...input, clientRequestId: "child", message: undefined };
        const first = yield* ClientLaunch.launch(request, context, modes, gate);
        const replay = yield* ClientLaunch.launch(request, context, modes, gate);
        assert.equal(first.originLink?.linkId, replay.originLink?.linkId);
        yield* ClientLaunch.launch(
          { ...request, clientRequestId: "delegated-excluded" },
          context,
          modes,
          gate,
          true,
        );
        assert.equal(
          (yield* links.list(
            { type: "user" },
            { thread: { environmentId: client.scope.environmentId, threadId: parent.threadId } },
          )).links.length,
          1,
        );
        assert.isNull(
          (yield* client.threads.getThreadShell(first.threadId))?.lineage?.parentThreadId ?? null,
        );
        yield* ClientLaunch.fence(namespace, "fenced-linked");
        yield* Effect.exit(
          ClientLaunch.launch(
            { ...request, clientRequestId: "fenced-linked" },
            context,
            modes,
            gate,
          ),
        );
        yield* Effect.exit(
          ClientLaunch.launch(
            { ...request, clientRequestId: "bad-project", projectId: ProjectId.make("missing") },
            context,
            modes,
            gate,
          ),
        );
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        yield* Effect.exit(
          ClientLaunch.launch(
            { ...input, clientRequestId: "rejected-message" },
            context,
            modes,
            gate,
          ).pipe(
            Effect.provideService(ThreadLaunch.ThreadLaunchService, {
              ...launches,
              launch: (value) =>
                failAfter(launches, "thread")
                  .launch(value)
                  .pipe(
                    Effect.provideService(
                      ThreadManagement.ThreadManagementService,
                      context.threads,
                    ),
                  ),
            }),
          ),
        );
        const failedIds = clientLaunchIds(namespace, "rejected-message");
        const sink = yield* EventSink.EventSinkV2;
        yield* sink.commitRejectedCommand({
          commandId: CommandId.make(`${failedIds.commandId}:initial-message`),
          threadId: failedIds.threadId,
          commandType: "message.dispatch",
          rejectedAt: yield* DateTime.now,
          error: "synthetic refusal",
        });
        const listed = yield* links.list(
          { type: "user" },
          { thread: { environmentId: client.scope.environmentId, threadId: parent.threadId } },
        );
        assert.equal(listed.links.length, 2);
        assert.isTrue(listed.links.some((link) => link.to.threadId === failedIds.threadId));
      }).pipe(
        Effect.provide(
          LinkedThreads.layer.pipe(
            Layer.provide(
              Layer.mock(ServerEnvironment.ServerEnvironment)({
                getEnvironmentId: Effect.succeed(client.scope.environmentId),
              }),
            ),
          ),
        ),
      );
    }).pipe(Effect.provide(makeHarness().layer)),
);

it.effect.each([
  { explicitModel: false, explicitRuntime: false },
  { explicitModel: true, explicitRuntime: false },
  { explicitModel: false, explicitRuntime: true },
  { explicitModel: true, explicitRuntime: true },
])(
  "keyed and unkeyed external launches independently resolve omitted fields ($explicitModel/$explicitRuntime)",
  ({ explicitModel, explicitRuntime }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "jones-client-launch-defaults-" });
        const config = yield* ProviderReplayHarness.makeReplayServerConfig("launch-defaults").pipe(
          Effect.provideService(FileSystem.FileSystem, {
            ...fs,
            makeTempDirectory: () => Effect.succeed(root),
          }),
        );
        const effective = { ...modelSelection, model: "folded-default" };
        const explicit = { ...modelSelection, model: "explicit-model" };
        const h = makeHarness({
          serverConfigLayer: Layer.succeed(ServerConfig.ServerConfig, { ...config, cwd: root }),
          serverSettings: {
            projectSettingsFolded: true,
            defaultModelSelection: { ...modelSelection, model: "environment-default" },
            defaultRuntimeMode: "full-access",
            projectSettingsOverrides: {
              [projectId]: {
                defaultModelSelection: effective,
                defaultRuntimeMode: "approval-required",
              },
            },
          },
        });
        yield* Effect.gen(function* () {
          const context = yield* caller;
          const request = {
            projectId,
            title: "Defaults parity",
            ...(explicitModel ? { modelSelection: explicit } : {}),
            ...(explicitRuntime ? { runtimeMode: "auto" as const } : {}),
          };
          const started = {
            ...modes,
            runtimeMode: explicitRuntime ? ("auto" as const) : modes.runtimeMode,
          };
          const startGate: ThreadStartGate = {
            start: Effect.succeed(started),
            continueWithin: (_target, effect) => effect,
          };
          const keyed = yield* launchThread(
            { ...request, clientRequestId: "defaults-parity" },
            startGate,
          ).pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, context.scope));
          const unkeyed = yield* launchThread(request, startGate).pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, context.scope),
          );
          const threads = yield* ThreadManagement.ThreadManagementService;
          const keyedThread = (yield* threads.getThreadProjection(keyed.threadId)).thread;
          const unkeyedThread = (yield* threads.getThreadProjection(unkeyed.threadId)).thread;
          assert.deepEqual(keyedThread.modelSelection, explicitModel ? explicit : effective);
          assert.deepEqual(unkeyedThread.modelSelection, keyedThread.modelSelection);
          assert.equal(keyedThread.runtimeMode, explicitRuntime ? "auto" : "approval-required");
          assert.equal(unkeyedThread.runtimeMode, keyedThread.runtimeMode);
          const settings = yield* ServerSettings.ServerSettingsService;
          yield* settings.updateSettings({
            projectSettingsOverrides: {
              [projectId]: {
                defaultModelSelection: { ...modelSelection, model: "later-default" },
                defaultRuntimeMode: "full-access",
              },
            },
          });
          const replay = yield* launchThread(
            { ...request, clientRequestId: "defaults-parity" },
            startGate,
          ).pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, context.scope));
          assert.equal(replay.threadId, keyed.threadId);
          assert.isTrue("replayed" in replay && replay.replayed);
          assert.deepEqual(
            (yield* threads.getThreadProjection(replay.threadId)).thread,
            keyedThread,
          );
          assert.equal(context.scope.thread, undefined);
        }).pipe(
          Effect.provide(h.layer),
          Effect.provideService(ServerConfig.ServerConfig, { ...config, cwd: root }),
          Effect.provide(DocumentText.layer.pipe(Layer.provide(NodeServices.layer))),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer.pipe(Layer.provideMerge(NodeCrypto.layer)))),
);

it.effect("keyed external launch clamps an omitted runtime default to the caller ceiling", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "jones-client-launch-ceiling-" });
      const config = yield* ProviderReplayHarness.makeReplayServerConfig("launch-ceiling").pipe(
        Effect.provideService(FileSystem.FileSystem, {
          ...fs,
          makeTempDirectory: () => Effect.succeed(root),
        }),
      );
      const h = makeHarness({
        serverConfigLayer: Layer.succeed(ServerConfig.ServerConfig, { ...config, cwd: root }),
        serverSettings: { defaultRuntimeMode: "full-access" },
      });
      yield* Effect.gen(function* () {
        const context = yield* caller;
        const limited = { ...modes, runtimeMode: "approval-required" as const };
        const startGate: ThreadStartGate = {
          start: Effect.succeed(limited),
          continueWithin: (_target, effect) => effect,
        };
        const request = { projectId, title: "Ceiling", modelSelection };
        const keyed = yield* launchThread(
          { ...request, clientRequestId: "ceiling" },
          startGate,
        ).pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, context.scope));
        const unkeyed = yield* launchThread(request, startGate).pipe(
          Effect.provideService(McpInvocationContext.McpInvocationContext, context.scope),
        );
        const threads = yield* ThreadManagement.ThreadManagementService;
        assert.equal(
          (yield* threads.getThreadProjection(keyed.threadId)).thread.runtimeMode,
          "approval-required",
        );
        assert.equal(
          (yield* threads.getThreadProjection(unkeyed.threadId)).thread.runtimeMode,
          "approval-required",
        );
      }).pipe(
        Effect.provide(h.layer),
        Effect.provideService(ServerConfig.ServerConfig, { ...config, cwd: root }),
        Effect.provide(DocumentText.layer.pipe(Layer.provide(NodeServices.layer))),
      );
    }),
  ).pipe(Effect.provide(NodeServices.layer.pipe(Layer.provideMerge(NodeCrypto.layer)))),
);
