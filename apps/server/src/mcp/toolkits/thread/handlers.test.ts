import * as ThreadMetadataReads from "../../../jones/orchestration/threadMetadataReads.ts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  EventId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunId,
  RunAttemptId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ServerCommand,
  type ThreadConfigurationEvidence,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as ProviderRegistry from "../../../provider/ProviderRegistry.ts";
import * as ThreadSearch from "../../../orchestration-v2/ThreadSearch.ts";
import * as ScheduledTasks from "../../../scheduledTasks/ScheduledTaskService.ts";
import { emptyProjection } from "../../../orchestration-v2/ProjectionStore.ts";
import { v2PullRequestThread } from "../../../orchestration-v2/testkit/pullRequestFixtures.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as ThreadHandlers from "./handlers.ts";
import * as ThreadSelfConfiguration from "../../../jones/threadConfiguration/ThreadSelfConfiguration.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { liveThreadShell } from "../../McpToolAccess.testkit.ts";
import { ThreadToolkit } from "./tools.ts";

it.effect("blocks foreign text edits and answers while preserving self controls and readback", () =>
  Effect.gen(function* () {
    const callerId = ThreadId.make("thread:block-caller");
    const peerId = ThreadId.make("thread:block-peer");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const requestId = RuntimeRequestId.make("request:block-test");
    const scope: McpInvocationContext.McpInvocationScope = {
      environmentId: EnvironmentId.make("environment:block-test"),
      requestNamespace: "session:block-test",
      thread: { threadId: callerId, providerSessionId: "session:block-test", providerInstanceId },
      client: undefined,
      capabilities: new Set(["orchestration"]),
      issuedAt: 1,
    };
    const makeShell = (id: ThreadId) => ({
      ...v2PullRequestThread({
        id,
        projectId: ProjectId.make("project:block-test"),
        title: "Blocked thread",
        modelSelection: { instanceId: providerInstanceId, model: "gpt-5" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        pullRequests: [],
        latestUserMessageAt: null,
        createdAt: "2026-10-04T00:00:00Z",
        updatedAt: "2026-10-04T00:00:00Z",
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
      }),
      activeRunId: RunId.make(`run:${id}`),
      threadMessagesBlocked: true,
    });
    const shells = new Map([
      [callerId, makeShell(callerId)],
      [peerId, makeShell(peerId)],
    ]);
    const records = (id: ThreadId) => {
      const thread: OrchestrationV2AppThread = { ...shells.get(id)!, lastVisitedAt: null };
      const projection = emptyProjection({
        type: "thread.created",
        id: EventId.make(`fixture:${id}`),
        threadId: id,
        occurredAt: thread.createdAt,
        payload: thread,
      });
      const now = DateTime.makeUnsafe("2026-10-04T00:00:00Z");
      return {
        ...projection,
        runtimeRequests: [
          {
            id: requestId,
            nodeId: NodeId.make("node:question"),
            providerTurnId: null,
            nativeRequestRef: null,
            kind: "user_input" as const,
            status: "pending" as const,
            responseCapability: { type: "message" as const },
            createdAt: now,
            resolvedAt: null,
          },
        ],
        turnItems: [
          {
            id: TurnItemId.make("item:question"),
            type: "user_input_request" as const,
            threadId: id,
            runId: null,
            nodeId: null,
            providerThreadId: null,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 1,
            status: "pending" as const,
            title: null,
            startedAt: null,
            completedAt: null,
            updatedAt: now,
            requestId,
            questions: [],
          },
        ],
      };
    };
    const commands: OrchestrationV2ServerCommand[] = [];
    const dependencies = Layer.mergeAll(
      Layer.mock(ThreadMetadataReads.ThreadMetadataReads)({}),
      Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
      NodeCrypto.layer,
      Layer.mock(ThreadSearch.ThreadSearch)({}),
      Layer.mock(ScheduledTasks.ScheduledTaskService)({}),
      Layer.mock(ThreadManagement.ThreadManagementService)({
        getThreadShell: () => Effect.succeed(shells.get(callerId)!),
        getProjectThreadRecords: ({ threadId }) => Effect.succeed(records(threadId)),
        dispatch: (command) =>
          Effect.sync(() => {
            commands.push(command);
            return { sequence: commands.length, storedEvents: [] };
          }),
      }),
    );
    const toolkit = yield* ThreadToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ThreadHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const invoke = <Name extends keyof typeof ThreadToolkit.tools>(
      name: Name,
      args: Parameters<typeof toolkit.handle<Name>>[1],
    ) =>
      toolkit.handle(name, args).pipe(
        Stream.unwrap,
        Stream.runCollect,
        Effect.map((results) => results.at(-1)!),
        Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
        Effect.provide(dependencies),
      );
    for (const [name, args] of [
      ["t3_queue", { action: "edit", queuedRunId: RunId.make("run:queued"), text: "Foreign edit" }],
      ["t3_pending_request", { action: "respond", requestId, answers: { q: ["Foreign answer"] } }],
    ] as const) {
      const denied = yield* invoke(name, { threadId: peerId, ...args });
      expect(denied.isFailure).toBe(true);
      expect(denied.result).toMatchObject({ code: "capability_denied" });
    }
    expect(commands).toEqual([]);
    const configuration = yield* invoke("t3_thread_configure", { threadId: peerId });
    expect(configuration.isFailure).toBe(false);
    expect(configuration.result).toMatchObject({ threadMessagesBlocked: true });
    expect(configuration.encodedResult).toEqual(configuration.result);
    const pending = yield* invoke("t3_pending_request", { action: "list", threadId: peerId });
    expect(pending.isFailure).toBe(false);
    expect(pending.result).toEqual({ requestIds: [requestId] });
    const question = yield* invoke("t3_pending_request", {
      action: "read",
      threadId: peerId,
      requestId,
    });
    expect(question.isFailure).toBe(false);
    expect(question.result).toEqual({ requestId, questions: [] });
    expect(question.encodedResult).toEqual(question.result);
    expect(commands).toEqual([]);
    for (const threadId of [callerId, peerId]) {
      if (threadId === peerId)
        shells.set(peerId, { ...shells.get(peerId)!, threadMessagesBlocked: false });
      const edit = yield* invoke("t3_queue", {
        action: "edit",
        threadId,
        queuedRunId: RunId.make("run:queued"),
        text: "Allowed edit",
      });
      expect(edit.isFailure).toBe(false);
      expect(commands.at(-1)).toMatchObject({
        type: "queued-run.edit",
        senderThreadId: callerId,
        threadId,
        text: "Allowed edit",
      });
      expect(edit.encodedResult).toEqual(edit.result);
      const answer = yield* invoke("t3_pending_request", {
        action: "respond",
        threadId,
        requestId,
        answers: { q: ["Allowed answer"] },
      });
      expect(answer.isFailure).toBe(false);
      expect(commands.at(-1)).toMatchObject({
        type: "runtime-request.respond",
        senderThreadId: callerId,
        threadId,
        answers: { q: ["Allowed answer"] },
      });
      expect(answer.encodedResult).toEqual(answer.result);
    }
  }),
);

it.effect(
  "t3_queue promote_to_steer sends a bounded batch with its counts, and one message without queuedRunIds",
  () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread:batch-steer");
      const providerInstanceId = ProviderInstanceId.make("codex");
      const scope: McpInvocationContext.McpInvocationScope = {
        environmentId: EnvironmentId.make("environment:batch-steer"),
        requestNamespace: "session:batch-steer",
        thread: { threadId, providerSessionId: "session:batch-steer", providerInstanceId },
        client: undefined,
        capabilities: new Set(["orchestration"]),
        issuedAt: 1,
      };
      const shell = {
        ...v2PullRequestThread({
          id: threadId,
          projectId: ProjectId.make("project:batch-steer"),
          title: "Batch steer",
          modelSelection: { instanceId: providerInstanceId, model: "gpt-5" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          pullRequests: [],
          latestUserMessageAt: null,
          createdAt: "2026-10-04T00:00:00Z",
          updatedAt: "2026-10-04T00:00:00Z",
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
        }),
        activeRunId: RunId.make("run:active"),
      };
      const projection = emptyProjection({
        type: "thread.created",
        id: EventId.make("fixture:batch-steer"),
        threadId,
        occurredAt: shell.createdAt,
        payload: { ...shell, lastVisitedAt: null } satisfies OrchestrationV2AppThread,
      });
      const commands: OrchestrationV2ServerCommand[] = [];
      const batches: unknown[] = [];
      const layer = Layer.mergeAll(
        Layer.mock(ThreadMetadataReads.ThreadMetadataReads)({}),
        Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
        NodeCrypto.layer,
        Layer.mock(ThreadSearch.ThreadSearch)({}),
        Layer.mock(ScheduledTasks.ScheduledTaskService)({}),
        Layer.mock(ThreadManagement.ThreadManagementService)({
          getThreadShell: () => Effect.succeed(shell),
          getProjectThreadRecords: () => Effect.succeed(projection),
          dispatch: (command) =>
            Effect.sync(() => {
              commands.push(command);
              return { sequence: commands.length, storedEvents: [] };
            }),
          promoteQueuedBatch: (input) =>
            Effect.sync(() => {
              batches.push(input);
              return { sequence: 9, promotedCount: input.queuedRunIds.length, remainingCount: 1 };
            }),
        }),
      );
      const toolkit = yield* ThreadToolkit.pipe(
        Effect.provide(
          McpToolAccess.HandlersLayer.layer(ThreadHandlers.layer).pipe(Layer.provide(layer)),
        ),
      );
      const call = (args: Parameters<typeof toolkit.handle<"t3_queue">>[1]) =>
        toolkit.handle("t3_queue", args).pipe(
          Stream.unwrap,
          Stream.runCollect,
          Effect.map((results) => results.at(-1)!),
          Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
          Effect.provide(layer),
        );
      const first = RunId.make("run:queued-1");
      const second = RunId.make("run:queued-2");
      const target = RunId.make("run:active");
      const base = { action: "promote_to_steer", queuedRunId: first, targetRunId: target } as const;

      const batch = yield* call({ ...base, queuedRunIds: [first, second] });
      expect(batch.isFailure).toBe(false);
      expect(batch.result).toEqual({ sequence: 9, promotedCount: 2, remainingCount: 1 });
      expect(batch.encodedResult).toEqual(batch.result);
      expect(batches).toMatchObject([
        { threadId, targetRunId: target, queuedRunIds: [first, second] },
      ]);
      expect(commands).toEqual([]);

      const single = yield* call(base);
      expect(single.isFailure).toBe(false);
      expect(single.result).toEqual({ sequence: 1 });
      expect(single.encodedResult).toEqual({ sequence: 1 });
      expect(commands).toMatchObject([
        { type: "queued-message.promote-to-steer", queuedRunId: first, targetRunId: target },
      ]);

      const misordered = yield* call({ ...base, queuedRunIds: [second, first] });
      expect(misordered.isFailure).toBe(true);
      expect(misordered.result).toMatchObject({ code: "invalid_request" });
      expect(batches).toHaveLength(1);
      expect(commands).toHaveLength(1);
    }),
);

const makeControlToolkit = (
  configuration?: ThreadSelfConfiguration.ThreadSelfConfiguration["Service"],
) =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:controls");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const scope: McpInvocationContext.McpInvocationScope = {
      environmentId: EnvironmentId.make("environment:controls"),
      requestNamespace: "session:controls",
      thread: { threadId, providerSessionId: "credential:controls", providerInstanceId },
      client: undefined,
      capabilities: new Set(["orchestration"]),
      issuedAt: 1,
    };
    const projection = (id: ThreadId) => {
      const thread = {
        ...v2PullRequestThread({
          id,
          projectId: ProjectId.make("project:controls"),
          title: "Controls",
          modelSelection: { instanceId: providerInstanceId, model: "gpt-5" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          pullRequests: [],
          latestUserMessageAt: null,
          createdAt: "2026-10-04T00:00:00Z",
          updatedAt: "2026-10-04T00:00:00Z",
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
        }),
        activeRunId: RunId.make("run:controls"),
      };
      return emptyProjection({
        type: "thread.created",
        id: EventId.make(`fixture:${id}`),
        threadId: id,
        occurredAt: thread.createdAt,
        payload: { ...thread, lastVisitedAt: null } satisfies OrchestrationV2AppThread,
      });
    };
    const commands: OrchestrationV2ServerCommand[] = [];
    const settlements: Array<
      Parameters<ThreadManagement.ThreadManagementService["Service"]["settleThread"]>[0]
    > = [];
    const dependencies = Layer.mergeAll(
      NodeCrypto.layer,
      Layer.mock(ThreadMetadataReads.ThreadMetadataReads)({}),
      Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
      Layer.mock(ThreadSearch.ThreadSearch)({}),
      Layer.mock(ScheduledTasks.ScheduledTaskService)({}),
      configuration === undefined
        ? Layer.empty
        : Layer.succeed(ThreadSelfConfiguration.ThreadSelfConfiguration, configuration),
      Layer.mock(ThreadManagement.ThreadManagementService)({
        // The shell carries the projection's identity, modes and active run.
        getThreadShell: (id) =>
          Effect.succeed({
            ...liveThreadShell(id, { activeRunId: RunId.make("run:controls") }),
            projectId: ProjectId.make("project:controls"),
            title: "Controls",
          }),
        getProjectThreadRecords: ({ threadId: id }) => Effect.succeed(projection(id)),
        dispatch: (command) =>
          Effect.sync(() => {
            commands.push(command);
            return { sequence: commands.length, storedEvents: [] };
          }),
        settleThread: (input) =>
          Effect.sync(() => {
            settlements.push(input);
            return input.byOwnAgent ? { settlesWhenTurnEnds: true as const } : { sequence: 10 };
          }),
      }),
    );
    const toolkit = yield* ThreadToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ThreadHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const invoke = <Name extends keyof typeof ThreadToolkit.tools>(
      name: Name,
      args: Parameters<typeof toolkit.handle<Name>>[1],
      caller = scope,
    ) =>
      toolkit.handle(name, args).pipe(
        Stream.unwrap,
        Stream.runCollect,
        Effect.map((results) => results.at(-1)!),
        Effect.provideService(McpInvocationContext.McpInvocationContext, caller),
        Effect.provide(dependencies),
      );
    return { scope, threadId, providerInstanceId, invoke, commands, settlements };
  });

it.effect(
  "configuration readback retains execution evidence and self changes require the safe tool",
  () =>
    Effect.gen(function* () {
      const evidence: ThreadConfigurationEvidence = {
        configurationRevision: 17,
        savedSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
        executionPrecondition: {
          runId: RunId.make("run:controls"),
          attemptId: RunAttemptId.make("attempt:controls"),
          providerThreadId: ProviderThreadId.make("provider-thread:controls"),
          providerSessionId: ProviderSessionId.make("native-session:controls"),
          runtimeGeneration: "generation:controls",
        },
        currentExecution: {
          runId: RunId.make("run:controls"),
          attemptId: RunAttemptId.make("attempt:controls"),
          requestedSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "captured-model",
          },
          providerObservation: null,
          reasoningEffortObservation: "unavailable",
        },
      };
      const reads: Array<unknown> = [];
      let unavailable = false;
      const env = yield* makeControlToolkit({
        read: (threadId, scope) => {
          reads.push({ threadId, scope });
          return unavailable
            ? Effect.fail(
                new ThreadSelfConfiguration.ThreadSelfConfigurationError({
                  reason: "state_unavailable",
                }),
              )
            : Effect.succeed(evidence);
        },
        save: () => Effect.die("The configuration read must never save."),
      });
      const read = yield* env.invoke("t3_thread_configure", {});
      expect(read.isFailure).toBe(false);
      expect(read.result).toEqual({
        threadId: env.threadId,
        modelSelection: evidence.savedSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        threadMessagesBlocked: false,
        ...evidence,
      });
      expect(read.encodedResult).toEqual(read.result);
      expect(reads).toEqual([{ threadId: env.threadId, scope: env.scope }]);

      const selection = { instanceId: env.providerInstanceId, model: "next-model" };
      for (const args of [
        { modelSelection: selection },
        { threadId: env.threadId, modelSelection: selection },
      ]) {
        const denied = yield* env.invoke("t3_thread_configure", args);
        expect(denied.isFailure).toBe(true);
        expect(denied.result).toMatchObject({
          code: "invalid_request",
          message: expect.stringContaining("t3_thread_self_configure"),
        });
      }
      expect(env.commands).toEqual([]);
      const peerId = ThreadId.make("thread:controls-peer");
      for (const modelSelection of [
        selection,
        { instanceId: ProviderInstanceId.make("claude"), model: "peer-model" },
      ]) {
        const result = yield* env.invoke("t3_thread_configure", {
          threadId: peerId,
          modelSelection,
        });
        expect(result.isFailure).toBe(false);
        expect(result.encodedResult).toEqual(result.result);
      }
      expect(env.commands).toEqual([
        {
          type: "thread.model-selection.set",
          commandId: expect.any(String),
          threadId: peerId,
          modelSelection: selection,
        },
        {
          type: "provider.switch",
          commandId: expect.any(String),
          threadId: peerId,
          modelSelection: { instanceId: "claude", model: "peer-model" },
        },
      ]);

      unavailable = true;
      const failed = yield* env.invoke("t3_thread_configure", {});
      expect(failed.isFailure).toBe(true);
      expect(failed.result).toMatchObject({
        code: "invalid_request",
        message: expect.stringContaining("state_unavailable"),
      });
      expect(env.commands).toHaveLength(2);
    }),
);

it.effect(
  "configuration without the evidence service reads legacy fields without inventing execution evidence",
  () =>
    Effect.gen(function* () {
      const env = yield* makeControlToolkit();
      const result = yield* env.invoke("t3_thread_configure", {});
      expect(result.isFailure).toBe(false);
      expect(result.result).toEqual({
        threadId: env.threadId,
        modelSelection: { instanceId: env.providerInstanceId, model: "gpt-5" },
        runtimeMode: "full-access",
        interactionMode: "default",
        threadMessagesBlocked: false,
      });
      expect(result.encodedResult).toEqual(result.result);
      expect(env.commands).toEqual([]);
    }),
);

it.effect("settlement forwards the authenticated self caller only for its own agent thread", () =>
  Effect.gen(function* () {
    const env = yield* makeControlToolkit();
    const own = yield* env.invoke("t3_thread_organize", { action: "settle" });
    expect(own.isFailure).toBe(false);
    expect(own.result).toEqual({ settlesWhenTurnEnds: true });
    expect(own.encodedResult).toEqual(own.result);
    expect(env.settlements).toEqual([
      {
        threadId: env.threadId,
        commandId: expect.any(String),
        byOwnAgent: true,
        selfSettlementCaller: {
          mcpCredentialId: "credential:controls",
          providerInstanceId: env.providerInstanceId,
        },
      },
    ]);
    const peerId = ThreadId.make("thread:controls-peer");
    const peer = yield* env.invoke("t3_thread_organize", { action: "settle", threadId: peerId });
    expect(peer.result).toEqual({ sequence: 10 });
    expect(env.settlements[1]).toEqual({
      threadId: peerId,
      commandId: expect.any(String),
      byOwnAgent: false,
    });
    const client = yield* env.invoke(
      "t3_thread_organize",
      { action: "settle", threadId: env.threadId },
      {
        ...env.scope,
        thread: undefined,
        client: { sessionId: "client:controls", label: "Controls", access: "full-access" },
      },
    );
    expect(client.result).toEqual({ sequence: 10 });
    expect(env.settlements[2]).toEqual({
      threadId: env.threadId,
      commandId: expect.any(String),
      byOwnAgent: false,
    });
    expect(env.commands).toEqual([]);
  }),
);
