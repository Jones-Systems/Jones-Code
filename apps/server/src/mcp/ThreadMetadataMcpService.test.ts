import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";

import { OrchestratorProjectionError } from "../orchestration-v2/Orchestrator.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import type * as McpInvocationContext from "./McpInvocationContext.ts";
import * as ThreadMetadataMcp from "./ThreadMetadataMcpService.ts";
import { emptyProjection } from "../orchestration-v2/ProjectionStore.ts";
import { v2PullRequestThread } from "../orchestration-v2/testkit/pullRequestFixtures.ts";

const threadId = ThreadId.make("thread:metadata-caller");
const scope: McpInvocationContext.McpInvocationScope = {
  environmentId: EnvironmentId.make("environment:metadata-test"),
  requestNamespace: "provider-session:metadata-test",
  thread: {
    threadId,
    providerSessionId: "provider-session:metadata-test",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  client: undefined,
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
};

function layerService(
  getThreadShell: ThreadManagement.ThreadManagementService["Service"]["getThreadShell"],
) {
  return ThreadMetadataMcp.layer.pipe(
    Layer.provide(
      Layer.merge(
        Layer.mock(ThreadManagement.ThreadManagementService)({
          getThreadShell,
          getThreadRecords: () => Effect.die("projection must not load after shell failure"),
        } satisfies Partial<ThreadManagement.ThreadManagementService["Service"]>),
        NodeCrypto.layer,
      ),
    ),
  );
}

const updateCallingThread = Effect.gen(function* () {
  const service = yield* ThreadMetadataMcp.ThreadMetadataMcpService;
  return yield* service.update(scope, {
    action: "rename",
    title: "Renamed thread",
    clientRequestId: "metadata-caller-classification",
  });
});

it.effect("reports an absent calling thread as thread_not_found", () =>
  Effect.gen(function* () {
    const error = yield* updateCallingThread.pipe(
      Effect.provide(layerService(() => Effect.succeed(null))),
      Effect.flip,
    );

    expect(error.code).toBe("thread_not_found");
  }),
);

it.effect("keeps calling-thread storage failures as orchestration errors", () =>
  Effect.gen(function* () {
    const error = yield* updateCallingThread.pipe(
      Effect.provide(
        layerService(() =>
          Effect.fail(
            new OrchestratorProjectionError({
              threadId,
              cause: new Error("storage unavailable"),
            }),
          ),
        ),
      ),
      Effect.flip,
    );

    expect(error.code).toBe("orchestration_error");
  }),
);

it.effect("blocks project peers, permits self recovery, and denies foreign unblocking", () =>
  Effect.gen(function* () {
    const peerId = ThreadId.make("thread:metadata-peer");
    const makeThread = (id: ThreadId): OrchestrationV2AppThread => ({
      ...v2PullRequestThread({
        id,
        projectId: ProjectId.make("project:metadata"),
        title: "Metadata thread",
        modelSelection: { instanceId: scope.providerInstanceId, model: "gpt-5" },
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
      lastVisitedAt: null,
    });
    const threads = new Map([
      [threadId, makeThread(threadId)],
      [peerId, makeThread(peerId)],
    ]);
    const records = (id: ThreadId) => {
      const thread = threads.get(id)!;
      return emptyProjection({
        type: "thread.created",
        id: EventId.make(`fixture:${id}`),
        threadId: id,
        occurredAt: thread.createdAt,
        payload: thread,
      });
    };
    const commands: OrchestrationV2ServerCommand[] = [];
    const dependencies = Layer.mock(ThreadManagement.ThreadManagementService)({
      getThreadShell: () =>
        Effect.succeed({
          ...makeThread(threadId),
          latestRunId: null,
          activeRunId: null,
          status: "idle" as const,
          pendingRuntimeRequest: null,
          latestVisibleMessage: null,
          latestUserMessageAt: null,
          hasActionableProposedPlan: false,
          itemCount: 0,
          visibleItemCount: 0,
        }),
      getThreadRecords: (id) => Effect.succeed(records(id)),
      getProjectThreadRecords: ({ threadId: id }) => Effect.succeed(records(id)),
      dispatch: (command) =>
        Effect.gen(function* () {
          if (command.type !== "thread.metadata.update")
            return yield* Effect.die("unexpected metadata command");
          commands.push(command);
          const current = threads.get(command.threadId)!;
          const updated = { ...current, threadMessagesBlocked: command.threadMessagesBlocked };
          threads.set(command.threadId, updated);
          return {
            sequence: commands.length,
            storedEvents: [
              {
                sequence: commands.length,
                commandId: command.commandId,
                event: {
                  type: "thread.metadata-updated",
                  id: EventId.make(`metadata:${commands.length}`),
                  threadId: command.threadId,
                  occurredAt: DateTime.makeUnsafe("2026-10-04T00:00:00Z"),
                  payload: updated,
                },
              },
            ],
          };
        }),
    });
    const service = yield* ThreadMetadataMcp.ThreadMetadataMcpService.pipe(
      Effect.provide(
        ThreadMetadataMcp.layer.pipe(Layer.provide(dependencies), Layer.provide(NodeCrypto.layer)),
      ),
    );
    const peerBlock = yield* service.update(scope, {
      threadId: peerId,
      action: "block_thread_messages",
    });
    expect(peerBlock).toMatchObject({ threadId: peerId, threadMessagesBlocked: true });
    expect(commands.at(-1)).toMatchObject({
      type: "thread.metadata.update",
      threadId: peerId,
      threadMessagesBlocked: true,
    });
    const foreignAllow = yield* service
      .update(scope, { threadId: peerId, action: "allow_thread_messages" })
      .pipe(Effect.flip);
    expect(foreignAllow.code).toBe("capability_denied");
    expect(commands).toHaveLength(1);
    const selfBlock = yield* service.update(scope, { action: "block_thread_messages" });
    expect(selfBlock.threadMessagesBlocked).toBe(true);
    const selfAllow = yield* service.update(scope, { action: "allow_thread_messages" });
    expect(selfAllow.threadMessagesBlocked).toBe(false);
    expect(threads.get(peerId)?.threadMessagesBlocked).toBe(true);
  }),
);
