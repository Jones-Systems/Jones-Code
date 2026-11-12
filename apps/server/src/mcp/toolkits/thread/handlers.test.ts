import * as ThreadMetadataReads from "../../../jones/orchestration/threadMetadataReads.ts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  EventId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as ThreadSearch from "../../../orchestration-v2/ThreadSearch.ts";
import * as ScheduledTasks from "../../../scheduledTasks/ScheduledTaskService.ts";
import { emptyProjection } from "../../../orchestration-v2/ProjectionStore.ts";
import { v2PullRequestThread } from "../../../orchestration-v2/testkit/pullRequestFixtures.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as ThreadHandlers from "./handlers.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
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
    expect(configuration.result).toMatchObject({ threadMessagesBlocked: true });
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
      });
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
      });
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
      expect(batches).toMatchObject([
        { threadId, targetRunId: target, queuedRunIds: [first, second] },
      ]);
      expect(commands).toEqual([]);

      const single = yield* call(base);
      expect(single.isFailure).toBe(false);
      expect(single.result).toEqual({ sequence: 1 });
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
