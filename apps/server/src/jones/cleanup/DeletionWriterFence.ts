import type { ApplicationProjectEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type * as Statement from "effect/sql/Statement";
import type { RecordedEvent } from "../../orchestration-v2/RecordedTypes.ts";
import { isPublicOrchestrationEvent } from "../../orchestration-v2/WireProjection.ts";
import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
} from "../nativeCreation/NativeCreationPreparation.ts";
import { overlaps } from "../ordinaryCheckout/CheckoutTarget.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import {
  DeletionAdmissionBinding,
  DeletionAdmissionWitnessBinding,
  DeletionAdmissionCompletedNativeBinding,
} from "./DeletionBinding.ts";
import { DeletionWorktreeRemovalStartV1 } from "./DeletionWorktreeRemovalTypes.ts";

export class DeletionWriterFenceError extends Schema.TaggedError<DeletionWriterFenceError>()(
  "DeletionWriterFenceError",
  { reason: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {}
const isFenceError = Schema.is(DeletionWriterFenceError);
const isWitnessBinding = Schema.is(DeletionAdmissionWitnessBinding);
const isCompletedNativeBinding = Schema.is(DeletionAdmissionCompletedNativeBinding);
const fenceError = (cause: unknown) =>
  isFenceError(cause)
    ? cause
    : new DeletionWriterFenceError({ reason: "deletion_writer_fence_unavailable", cause });
const decodeBinding = Schema.decodeUnknownEffect(Schema.fromJsonString(DeletionAdmissionBinding), {
  onExcessProperty: "error",
});
const decodeStart = Schema.decodeUnknownEffect(
  Schema.fromJsonString(DeletionWorktreeRemovalStartV1),
  { onExcessProperty: "error" },
);
const entities = {
  run: ["orchestration_v2_projection_runs", "run_id"],
  attempt: ["orchestration_v2_projection_run_attempts", "attempt_id"],
  node: ["orchestration_v2_projection_nodes", "node_id"],
  subagent: ["orchestration_v2_projection_subagents", "subagent_id"],
  providerThread: ["orchestration_v2_projection_provider_threads", "provider_thread_id"],
  providerTurn: ["orchestration_v2_projection_provider_turns", "provider_turn_id"],
  request: ["orchestration_v2_projection_runtime_requests", "runtime_request_id"],
} as const;
interface Mutation {
  readonly threadIds: readonly string[];
  readonly paths?: readonly string[];
  readonly projectIds?: readonly string[];
  readonly providerSessionIds?: readonly string[];
  readonly entity?: { readonly kind: keyof typeof entities; readonly id: string };
}

/** SQL only. Every hook executes this against its owning writer transaction.
 * It refuses changes to a reserved snapshot; it grants no deletion authority. */
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const hasTable = makeJonesTablePresence(sql);
  const read = Effect.fn("DeletionWriterFence.readReservations")(function* () {
    // An upstream-only store cannot contain a Jones deletion reservation or
    // supply the group runtime port. Absence is rechecked after later upgrades.
    if (!(yield* hasTable("jones_deletion_worktree_admissions"))) return [];
    const rows = yield* sql<{
      readonly effect_id: string;
      readonly worktree_path: string;
      readonly binding_json: string;
      readonly start_json: string;
    }>`
      SELECT effect_id,worktree_path,binding_json,start_json FROM jones_deletion_worktree_admissions
      WHERE state!='completed' ORDER BY effect_id`;
    return yield* Effect.forEach(rows, (row) =>
      Effect.gen(function* () {
        const binding = yield* decodeBinding(row.binding_json);
        const start = yield* decodeStart(row.start_json);
        // Preserve the original bytes/hash algorithm, including legacy bindings.
        if (
          start.effectId !== row.effect_id ||
          binding.target.path !== row.worktree_path ||
          start.bindingSha256 !== nativeCreationSha256(row.binding_json) ||
          nativeCreationCanonicalJson(start.target) !== nativeCreationCanonicalJson(binding.target)
        )
          return yield* new DeletionWriterFenceError({
            reason: "deletion_writer_reservation_corrupt",
          });
        return binding;
      }),
    );
  }, Effect.mapError(fenceError));
  const assertMutation = Effect.fn("DeletionWriterFence.assertMutation")(function* (
    input: Mutation,
  ) {
    const reservations = yield* read();
    if (reservations.length === 0) return;
    const threadIds = new Set(input.threadIds);
    if (input.entity !== undefined) {
      const [table, key] = entities[input.entity.kind];
      // Closed source-owned identifier list; the event supplies only a bound ID.
      const owners = yield* sql<{
        readonly thread_id: string;
      }>`SELECT thread_id FROM ${sql(table)} WHERE ${sql(key)}=${input.entity.id}`;
      for (const row of owners) threadIds.add(row.thread_id);
    }
    for (const sessionId of input.providerSessionIds ?? []) {
      const owners = yield* sql<{ readonly thread_id: string }>`
        SELECT thread_id FROM orchestration_v2_projection_provider_sessions WHERE provider_session_id=${sessionId}
        UNION SELECT thread_id FROM orchestration_v2_projection_provider_session_bindings WHERE provider_session_id=${sessionId}
        UNION SELECT thread_id FROM orchestration_v2_projection_provider_threads WHERE provider_session_id=${sessionId}`;
      for (const row of owners) threadIds.add(row.thread_id);
    }
    const paths = [...(input.paths ?? [])];
    if (threadIds.size > 0) {
      const scopes = yield* sql<{
        readonly path: string | null;
        readonly workspace_root: string | null;
      }>`
        SELECT json_extract(t.payload_json,'$.worktreePath') AS path,p.workspace_root
        FROM orchestration_v2_projection_threads t LEFT JOIN projection_projects p ON p.project_id=t.project_id
        WHERE ${sql.in("t.thread_id", [...threadIds])}`;
      for (const row of scopes) {
        const path = row.path ?? row.workspace_root;
        if (path !== null) paths.push(path);
      }
    }
    for (const binding of reservations) {
      const members = isWitnessBinding(binding)
        ? binding.groupWitness.members.map((member) => member.threadId)
        : isCompletedNativeBinding(binding)
          ? [binding.threadId, ...binding.completedNativeMembers.map((member) => member.threadId)]
          : [binding.threadId];
      if (
        members.some((id) => threadIds.has(id)) ||
        (input.projectIds ?? []).includes(binding.target.projectId) ||
        paths.some((path) => overlaps(path, binding.target.path, "/"))
      )
        return yield* new DeletionWriterFenceError({
          reason: "original_deletion_snapshot_reserved",
        });
    }
  }, Effect.mapError(fenceError));

  const excludeReservedThreads = Effect.fn("DeletionWriterFence.excludeReservedThreads")(function* (
    column: Statement.Fragment,
  ) {
    const reservations = yield* read();
    if (reservations.length === 0) return sql`1 = 1`;
    return sql.and(
      reservations.map((binding) => {
        const ids = isWitnessBinding(binding)
          ? binding.groupWitness.members.map((member) => member.threadId)
          : isCompletedNativeBinding(binding)
            ? [binding.threadId, ...binding.completedNativeMembers.map((member) => member.threadId)]
            : [binding.threadId];
        const path = sql`COALESCE(json_extract(reserved_thread.payload_json,'$.worktreePath'),reserved_project.workspace_root)`;
        const target = binding.target.path;
        return sql`NOT (${column} IN ${sql.in(ids)} OR EXISTS(
          SELECT 1 FROM orchestration_v2_projection_threads AS reserved_thread
          LEFT JOIN projection_projects AS reserved_project ON reserved_project.project_id=reserved_thread.project_id
          WHERE reserved_thread.thread_id=${column} AND (
            ${path}=${target}
            OR substr(${path},1,length(rtrim(${target},'/'))+1)=rtrim(${target},'/')||'/'
            OR substr(${target},1,length(rtrim(${path},'/'))+1)=rtrim(${path},'/')||'/')))`;
      }),
    );
  });

  const assertEvent = Effect.fn("DeletionWriterFence.assertEvent")(function* (
    event: RecordedEvent,
  ) {
    if (!isPublicOrchestrationEvent(event)) return;
    const threadIds: string[] = [event.threadId];
    const paths: string[] = [];
    const providerSessionIds: string[] = [];
    let entity: Mutation["entity"];
    if ("threadId" in event.payload && typeof event.payload.threadId === "string")
      threadIds.push(event.payload.threadId);
    switch (event.type) {
      case "thread.created":
      case "thread.metadata-updated":
        threadIds.push(event.payload.id);
        if (event.payload.worktreePath !== null) paths.push(event.payload.worktreePath);
        if (event.payload.lineage.parentThreadId !== null)
          threadIds.push(event.payload.lineage.parentThreadId);
        threadIds.push(event.payload.lineage.rootThreadId);
        break;
      case "run.created":
      case "run.updated":
        entity = { kind: "run", id: event.payload.id };
        break;
      case "run-attempt.created":
      case "run-attempt.updated":
        entity = { kind: "attempt", id: event.payload.id };
        break;
      case "node.updated":
        entity = { kind: "node", id: event.payload.id };
        break;
      case "subagent.updated":
        entity = { kind: "subagent", id: event.payload.id };
        break;
      case "provider-thread.updated":
        entity = { kind: "providerThread", id: event.payload.id };
        break;
      case "provider-turn.updated":
        entity = { kind: "providerTurn", id: event.payload.id };
        break;
      case "runtime-request.updated":
        entity = { kind: "request", id: event.payload.id };
        break;
      case "provider-session.attached":
      case "provider-session.updated":
        providerSessionIds.push(event.payload.id);
        break;
      case "provider-session.detached":
      case "provider-session.detach-requested":
        providerSessionIds.push(event.payload.providerSessionId);
        break;
    }
    yield* assertMutation({
      threadIds,
      paths,
      providerSessionIds,
      ...(entity === undefined ? {} : { entity }),
    });
  });
  const assertProject = Effect.fn("DeletionWriterFence.assertProject")(function* (
    event: ApplicationProjectEvent,
  ) {
    if (
      event.type !== "project.created" &&
      event.type !== "project.deleted" &&
      event.payload.workspaceRoot === undefined
    )
      return;
    yield* assertMutation({
      threadIds: [],
      projectIds: [event.payload.projectId],
      paths:
        event.type === "project.deleted"
          ? []
          : event.payload.workspaceRoot === undefined
            ? []
            : [event.payload.workspaceRoot],
    });
  });
  return { assertMutation, assertEvent, assertProject, excludeReservedThreads };
});
