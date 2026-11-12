import type { OrchestrationV2ThreadShell, ThreadId } from "@t3tools/contracts";
import {
  THREAD_ATTENTION_CAPABILITY,
  type ThreadAttention,
} from "@t3tools/contracts/jones/threadAttention";
import {
  hasUnseenCompletion,
  pendingSubagentRequestsByThread,
  resolveThreadAttentionPill,
  resolveThreadAttentionStatus,
  shellAttentionInput,
} from "@t3tools/shared/jones/threadAttention";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";

export function projectThreadAttention(input: {
  readonly threadId: ThreadId;
  readonly threads: ReadonlyArray<OrchestrationV2ThreadShell>;
  readonly snapshotSequence: number;
}): ThreadAttention | null {
  const thread = input.threads.find((thread) => thread.id === input.threadId);
  if (thread === undefined) return null;
  const pendingChildKind = pendingSubagentRequestsByThread(input.threads).get(thread.id) ?? null;
  const state = shellAttentionInput(thread, pendingChildKind);
  return {
    ...THREAD_ATTENTION_CAPABILITY,
    threadId: thread.id,
    snapshotSequence: input.snapshotSequence,
    status: resolveThreadAttentionStatus(state),
    pill: resolveThreadAttentionPill({ thread: state }),
    unseenCompletion: hasUnseenCompletion(state),
    pendingChildKind,
    ownRequest:
      thread.pendingRuntimeRequest === null
        ? null
        : {
            id: thread.pendingRuntimeRequest.id,
            kind: thread.pendingRuntimeRequest.kind,
            createdAt: DateTime.formatIso(thread.pendingRuntimeRequest.createdAt),
          },
    coverage: {
      source: "shell",
      childRequests: "active-shell",
      latestVisibleMessage: "unavailable",
      requestVersion: "unavailable",
      requestDetails: "unavailable",
      threadSequence: "unavailable",
      lastVisitedAt: thread.lastVisitedAt === undefined ? "unavailable" : "available",
    },
  };
}

export const makeThreadAttentionQuery = Effect.fn("makeThreadAttentionQuery")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const threads = yield* ThreadManagementService.ThreadManagementService;
  const events = yield* OrchestrationEventStore.OrchestrationEventStore;
  return Effect.fn("ThreadAttention.read")(function* (threadId: ThreadId) {
    // The global cursor and active roster come from one read transaction. Only the
    // selected thread's projection leaves this service; no transcript is loaded.
    const read = yield* sql
      .withTransaction(
        Effect.all({
          decodeThreads: threads.readShellSnapshot({ location: "active" }),
          snapshotSequence: events.latestApplicationSequence,
        }),
      )
      .pipe(NodeSqliteClient.readOnly);
    const snapshot = yield* read.decodeThreads;
    return projectThreadAttention({
      threadId,
      threads: snapshot.threads,
      snapshotSequence: read.snapshotSequence,
    });
  });
});
