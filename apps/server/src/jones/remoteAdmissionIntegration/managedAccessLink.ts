import type { RemoteAdmissionRecord } from "@t3tools/contracts";
import * as DTO from "@t3tools/contracts/jones/remoteAdmissionReview";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import type { Grant } from "../remoteAdmission/ReceiverAccess.ts";

interface ManagedScope {
  readonly managed_scope_id: string;
  readonly receiver_environment_id: string;
  readonly origin_environment_id: string;
  readonly origin_thread_id: string;
  readonly destination_thread_id: string;
}
/** Owner display link only. A durable exact scope selects its most recent grant
 * lineage; restoration follows that lineage without approving the held message. */
export const managedAccessGrantId = Effect.fn("AdmissionReview.managedAccessLink")(function* (
  sql: SqlClient.SqlClient,
  record: RemoteAdmissionRecord,
  receiverEnvironmentId: string,
) {
  const stored = (yield* sql<{
    binding_json: string | null;
    managed_scope_id: string | null;
    own_scope_id: string | null;
  }>`SELECT e.binding_json,COALESCE(b.managed_scope_id,g.managed_scope_id) managed_scope_id,
    g.managed_scope_id own_scope_id FROM jones_remote_admissions a
    LEFT JOIN jones_remote_admission_extensions e ON e.admission_id=a.id
    LEFT JOIN jones_remote_admission_blocked_marks b ON b.admission_id=a.id
    LEFT JOIN jones_remote_admission_grants g ON g.admission_id=a.id
    WHERE a.id=${record.id}`)[0];
  const binding = stored?.binding_json
    ? yield* Schema.decodeUnknownEffect(Schema.fromJsonString(DTO.RemoteAdmissionAccessBinding))(
        stored.binding_json,
      )
    : null;
  const scopes = stored?.managed_scope_id
    ? yield* sql<ManagedScope>`SELECT * FROM jones_remote_managed_scopes
        WHERE managed_scope_id=${stored.managed_scope_id}`
    : binding?.destinationThreadId
      ? yield* sql<ManagedScope>`SELECT * FROM jones_remote_managed_scopes
          WHERE receiver_environment_id=${binding.receiverEnvironmentId}
          AND origin_environment_id=${binding.originEnvironmentId} AND origin_thread_id=${binding.originThreadId}
          AND destination_thread_id=${binding.destinationThreadId} AND operation_class='messages'`
      : [];
  const scope = scopes[0];
  if (!scope || scope.receiver_environment_id !== receiverEnvironmentId) return null;
  if (
    record.envelope.target.threadId !== null &&
    record.envelope.target.threadId !== scope.destination_thread_id
  )
    return null;
  const matchesScope = (b: DTO.RemoteAdmissionAccessBinding) =>
    b.receiverEnvironmentId === scope.receiver_environment_id &&
    b.originEnvironmentId === scope.origin_environment_id &&
    b.originThreadId === scope.origin_thread_id &&
    b.destinationThreadId === scope.destination_thread_id;
  const resolvedBinding =
    binding?.destinationThreadId === null &&
    record.envelope.operation === "t3_thread_launch" &&
    stored?.own_scope_id === scope.managed_scope_id
      ? { ...binding, destinationThreadId: scope.destination_thread_id }
      : binding;
  if (resolvedBinding && !matchesScope(resolvedBinding)) return null;
  const grants = yield* sql<Grant>`SELECT * FROM jones_remote_admission_grants
    WHERE managed_scope_id=${scope.managed_scope_id} ORDER BY created_at DESC,grant_id DESC`;
  const lineage = yield* sql<{
    predecessor_grant_id: string;
    successor_grant_id: string;
  }>`SELECT r.predecessor_grant_id,r.successor_grant_id FROM jones_remote_admission_grant_restores r
    JOIN jones_remote_admission_grants g ON g.grant_id=r.predecessor_grant_id
    WHERE g.managed_scope_id=${scope.managed_scope_id}`;
  // Select the newest current head, then prove its complete retained ancestry.
  // Ordering roots instead would miss a recent restore of an older lineage.
  const byId = new Map(grants.map((g) => [g.grant_id, g]));
  const predecessors = new Set(lineage.map((r) => r.predecessor_grant_id));
  if (lineage.some((r) => !byId.has(r.successor_grant_id))) return null;
  const head = grants.find((g) => !predecessors.has(g.grant_id));
  if (!head) return null;
  let current: Grant | undefined = head;
  const visited = new Set<string>();
  while (current && !visited.has(current.grant_id)) {
    visited.add(current.grant_id);
    const ceiling = yield* Schema.decodeUnknownEffect(
      Schema.fromJsonString(DTO.RemoteAdmissionAccessBinding),
    )(current.binding_json);
    if (
      !matchesScope(ceiling) ||
      current.session_id !== ceiling.sessionId ||
      current.destination_thread_id !== scope.destination_thread_id
    )
      return null;
    const previous = (yield* sql<{ predecessor_grant_id: string }>`
      SELECT predecessor_grant_id FROM jones_remote_admission_grant_restores
      WHERE successor_grant_id=${current.grant_id}`)[0];
    if (!previous) return current.admission_id.startsWith("restore:") ? null : head.grant_id;
    const predecessor = byId.get(previous.predecessor_grant_id);
    if (!predecessor || predecessor.binding_json !== current.binding_json) return null;
    current = predecessor;
  }
  return null;
});
