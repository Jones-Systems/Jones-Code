import { makeClientLaunchHarness as makeHarness } from "./ClientLaunch.testkit.ts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as LinkedThreads from "../../linkedThreads/LinkedThreads.ts";
import * as ServerEnvironment from "../../../environment/ServerEnvironment.ts";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  RunId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  OrchestratorMcpFailure,
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
import * as ServerSettings from "../../../serverSettings.ts";
import * as DocumentText from "../../../provider/DocumentText.ts";
import * as CommandReceiptStore from "../../../orchestration-v2/CommandReceiptStore.ts";
import * as ThreadLaunch from "../../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as ProviderReplayHarness from "../../../orchestration-v2/testkit/ProviderReplayHarness.ts";

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
            worktreePath: null,
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
