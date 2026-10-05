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
import { ThreadToolkitHandlersLive } from "./handlers.ts";
import { ThreadToolkit } from "./tools.ts";

it.effect("blocks foreign text edits and answers while preserving self controls and readback", () =>
  Effect.gen(function* () {
    const callerId = ThreadId.make("thread:block-caller");
    const peerId = ThreadId.make("thread:block-peer");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const requestId = RuntimeRequestId.make("request:block-test");
    const scope: McpInvocationContext.McpInvocationScope = {
      environmentId: EnvironmentId.make("environment:block-test"),
      threadId: callerId,
      providerSessionId: "session:block-test",
      providerInstanceId,
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
      Effect.provide(ThreadToolkitHandlersLive.pipe(Layer.provide(dependencies))),
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
      ["t3_queue_edit", { queuedRunId: RunId.make("run:queued"), text: "Foreign edit" }],
      ["t3_pending_request_respond", { requestId, answers: { q: ["Foreign answer"] } }],
    ] as const) {
      const denied = yield* invoke(name, { threadId: peerId, ...args });
      expect(denied.isFailure).toBe(true);
      expect(denied.result).toMatchObject({ code: "capability_denied" });
    }
    expect(commands).toEqual([]);
    const configuration = yield* invoke("t3_thread_configuration", { threadId: peerId });
    expect(configuration.result).toMatchObject({ threadMessagesBlocked: true });
    for (const threadId of [callerId, peerId]) {
      if (threadId === peerId)
        shells.set(peerId, { ...shells.get(peerId)!, threadMessagesBlocked: false });
      const edit = yield* invoke("t3_queue_edit", {
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
      const answer = yield* invoke("t3_pending_request_respond", {
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
