import {
  OrchestrationV2AppThreadJson,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import { nativeCreationSha256 } from "../nativeCreation/NativeCreationPreparation.ts";
import { overlaps } from "../ordinaryCheckout/CheckoutTarget.ts";
import type { DeletionWorktreeRemovalTargetV1 } from "./DeletionWorktreeRemovalTypes.ts";

export const CompletedNativeCleanupMembers = Schema.Array(
  Schema.Struct({
    threadId: ThreadId,
    payloadSha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  }),
);

export class CompletedNativeCleanupError extends Schema.TaggedError<CompletedNativeCleanupError>()(
  "CompletedNativeCleanupError",
  { reason: Schema.String },
) {}

type Identity = Pick<
  OrchestrationV2AppThread,
  | "id"
  | "projectId"
  | "worktreePath"
  | "branch"
  | "deletedAt"
  | "createdBy"
  | "creationSource"
  | "lineage"
>;

const originalDescendants = (
  owner: string,
  target: Pick<DeletionWorktreeRemovalTargetV1, "projectId" | "path" | "branch">,
  members: readonly Identity[],
) => {
  const byId = new Map(members.map((member) => [member.id, member]));
  if (byId.size !== members.length) return false;
  return members.every((member) => {
    if (
      member.id === owner ||
      member.projectId !== target.projectId ||
      member.worktreePath !== target.path ||
      member.branch !== target.branch ||
      member.deletedAt !== null ||
      member.createdBy !== "agent" ||
      member.creationSource !== "provider" ||
      member.lineage.sideChat === true ||
      member.lineage.rootThreadId !== owner ||
      member.lineage.relationshipToParent !== "subagent"
    )
      return false;
    let parent = member.lineage.parentThreadId;
    const seen = new Set<string>([member.id]);
    while (parent !== owner) {
      if (parent === null || seen.has(parent)) return false;
      seen.add(parent);
      const next = byId.get(parent);
      if (next === undefined) return false;
      parent = next.lineage.parentThreadId;
    }
    return true;
  });
};

/** Completion is a projected policy state, not evidence that a process exited. */
export const completedNativeCleanupGroup = (
  owner: string,
  target: Pick<DeletionWorktreeRemovalTargetV1, "projectId" | "path" | "branch">,
  members: readonly OrchestrationV2ThreadShell[],
) =>
  originalDescendants(owner, target, members) &&
  members.every(
    (member) =>
      member.status === "completed" &&
      member.activeRunId === null &&
      member.pendingRuntimeRequest === null &&
      (member.pendingBackgroundTasks?.length ?? 0) === 0,
  );

const decodeThread = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2AppThreadJson),
);

/** Independent SQL identity/pending-work revalidation; live runtime checks remain caller-owned. */
export const readCompletedNativeCleanupMembers = Effect.fn("CompletedNativeCleanup.readMembers")(
  function* (
    sql: SqlClient.SqlClient,
    owner: string,
    target: DeletionWorktreeRemovalTargetV1,
    separator: string,
  ) {
    const rows = yield* sql<{ thread_id: string; payload_json: string; path: string | null }>`
    SELECT thread_id,payload_json,json_extract(payload_json,'$.worktreePath') AS path
    FROM orchestration_v2_projection_threads WHERE deleted_at IS NULL ORDER BY thread_id`;
    const relevant = rows.filter(
      (row) => row.path !== null && overlaps(row.path, target.path, separator),
    );
    const threads = yield* Effect.forEach(relevant, (row) => decodeThread(row.payload_json));
    if (
      !originalDescendants(owner, target, threads) ||
      threads.some((thread, index) => thread.id !== relevant[index]!.thread_id)
    )
      return yield* new CompletedNativeCleanupError({
        reason: "completed_native_cleanup_membership_changed",
      });
    const ids = [owner, ...threads.map((thread) => thread.id)];
    for (const table of [
      "orchestration_v2_projection_runs",
      "orchestration_v2_projection_run_attempts",
      "orchestration_v2_projection_nodes",
      "orchestration_v2_projection_provider_turns",
      "orchestration_v2_projection_subagents",
    ]) {
      const active = yield* sql`SELECT 1 FROM ${sql(table)} WHERE ${sql.in("thread_id", ids)}
      AND (json_extract(payload_json,'$.status') IS NULL OR json_extract(payload_json,'$.status') NOT IN ('completed','interrupted','failed','cancelled','rolled_back','superseded')) LIMIT 1`;
      if (active.length > 0)
        return yield* new CompletedNativeCleanupError({
          reason: "completed_native_cleanup_work_pending",
        });
    }
    const requests =
      yield* sql`SELECT 1 FROM orchestration_v2_projection_runtime_requests WHERE ${sql.in("thread_id", ids)} AND status NOT IN ('resolved','expired','cancelled') LIMIT 1`;
    const effects =
      yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox WHERE ${sql.in("thread_id", ids)} AND status NOT IN ('succeeded','cancelled') LIMIT 1`;
    const providers =
      yield* sql`SELECT 1 FROM orchestration_v2_projection_provider_threads WHERE ${sql.in("thread_id", ids)} AND json_extract(payload_json,'$.activeTurnId') IS NOT NULL LIMIT 1`;
    if (requests.length > 0 || effects.length > 0 || providers.length > 0)
      return yield* new CompletedNativeCleanupError({
        reason: "completed_native_cleanup_work_pending",
      });
    return relevant.map((row) => ({
      threadId: ThreadId.make(row.thread_id),
      payloadSha256: nativeCreationSha256(row.payload_json),
    }));
  },
);
