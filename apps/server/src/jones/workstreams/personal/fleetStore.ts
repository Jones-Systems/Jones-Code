import type { EnvironmentId } from "@t3tools/contracts";
import * as P from "@t3tools/contracts/jones/workstreamsPersonal";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import { peerEnvironmentLinksTable } from "../../peerPersistence/table.ts";
import { canonicalJson } from "../../remoteDelegation/shared.ts";

interface WorkstreamFleetBinding {
  readonly authorityEnvironmentId: EnvironmentId;
  readonly authoritySessionId: string | null;
  readonly rosterRevision: number;
}

export interface WorkstreamFleetMember {
  readonly environmentId: EnvironmentId;
  readonly linkGeneration: string;
}

export interface WorkstreamFleetRosterMember extends WorkstreamFleetMember {
  readonly discovered: 0 | 1;
}

interface OperationRow {
  readonly ordinal: number;
  readonly requestId: string;
  readonly workstreamId: string;
  readonly kind: P.PersonalWorkstreamOperation["kind"];
  readonly inputJson: string;
  readonly baseJson: string | null;
  readonly candidateJson: string;
  readonly rosterRevision: number;
  readonly replacesRequestId: string | null;
  readonly status: P.PersonalWorkstreamOperation["status"];
  readonly reason: string | null;
}

export interface WorkstreamDeliveryRow extends P.PersonalWorkstreamDelivery, WorkstreamFleetMember {
  readonly requestId: string;
  readonly observedJson: string | null;
  readonly approvedBaseJson: string | null;
  readonly attempted: 0 | 1;
}

export interface StoredWorkstreamOperation extends Omit<
  OperationRow,
  "baseJson" | "candidateJson"
> {
  readonly base: P.PersonalWorkstream | null;
  readonly candidate: P.PersonalWorkstream;
  readonly peers: readonly WorkstreamDeliveryRow[];
}

const operationColumns = `ordinal, request_id AS requestId, workstream_id AS workstreamId, kind,
  input_json AS inputJson, base_json AS baseJson, candidate_json AS candidateJson,
  roster_revision AS rosterRevision, replaces_request_id AS replacesRequestId, status, reason`;
const deliveryColumns = `request_id AS requestId, environment_id AS environmentId,
  link_generation AS linkGeneration, state, reason, observed_json AS observedJson,
  approved_base_json AS approvedBaseJson, attempted`;
const decodeDefinition = Schema.decodeEffect(Schema.fromJsonString(P.PersonalWorkstream));
const decodeObserved = Schema.decodeSync(Schema.fromJsonString(P.PersonalWorkstream));
export const workstreamOperationView = Effect.fn("WorkstreamFleetStore.operationView")(function* (
  operation: StoredWorkstreamOperation,
  observationKey: (json: string) => Effect.Effect<string>,
): Effect.fn.Return<P.PersonalWorkstreamOperation> {
  return {
    requestId: operation.requestId,
    workstreamId: operation.workstreamId,
    kind: operation.kind,
    status: operation.status,
    rosterRevision: operation.rosterRevision,
    requestedLifecycle: operation.kind === "close" ? operation.candidate.lifecycle : null,
    reason: operation.reason,
    peers: yield* Effect.forEach(
      operation.peers,
      ({ environmentId, state, reason, observedJson }) =>
        Effect.gen(function* () {
          const delivery = { environmentId, state, reason };
          if (state !== "conflict" || observedJson === null) return delivery;
          const value = decodeObserved(observedJson);
          return {
            ...delivery,
            observed: {
              key: yield* observationKey(observedJson),
              name: value.name,
              lifecycle: value.lifecycle,
              deleted: value.deletedAt !== null,
            },
          };
        }),
    ),
  };
});

export function makeWorkstreamFleetStore(sql: SqlClient.SqlClient) {
  const binding =
    sql<WorkstreamFleetBinding>`SELECT authority_environment_id AS authorityEnvironmentId,
    authority_session_id AS authoritySessionId, roster_revision AS rosterRevision
    FROM jones_personal_workstream_fleet WHERE singleton = 1`.pipe(
      Effect.map(([row]) => row ?? null),
    );
  const members = sql<WorkstreamFleetRosterMember>`SELECT environment_id AS environmentId,
    link_generation AS linkGeneration, discovered FROM jones_personal_workstream_fleet_members
    ORDER BY environment_id`;

  const find = Effect.fn("WorkstreamFleetStore.find")(function* (requestId: string) {
    const [row] = yield* sql<OperationRow>`SELECT ${sql.unsafe(operationColumns)}
      FROM jones_personal_workstream_operations WHERE request_id = ${requestId}`;
    if (!row) return null;
    const peers = yield* sql<WorkstreamDeliveryRow>`SELECT ${sql.unsafe(deliveryColumns)}
      FROM jones_personal_workstream_operation_peers WHERE request_id = ${requestId}
      ORDER BY environment_id`;
    return yield* decodeOperation(row, peers);
  });

  const active = Effect.gen(function* () {
    const operations = yield* sql<OperationRow>`SELECT ${sql.unsafe(operationColumns)}
      FROM jones_personal_workstream_operations WHERE status IN ('pending', 'stale', 'discovered') ORDER BY ordinal`;
    if (!operations.length) return [];
    const peers = yield* sql<WorkstreamDeliveryRow>`SELECT ${sql.unsafe(deliveryColumns)}
      FROM jones_personal_workstream_operation_peers WHERE request_id IN
      ${sql.in(operations.map(({ requestId }) => requestId))} ORDER BY environment_id`;
    const byRequest = Map.groupBy(peers, (peer) => peer.requestId);
    return yield* Effect.forEach(operations, (row) =>
      decodeOperation(row, byRequest.get(row.requestId) ?? []),
    );
  });

  const openClose = Effect.fn("WorkstreamFleetStore.openClose")(function* (workstreamId: string) {
    const [row] = yield* sql<{ requestId: string }>`SELECT request_id AS requestId
      FROM jones_personal_workstream_operations WHERE workstream_id = ${workstreamId}
      AND kind = 'close' AND status IN ('pending', 'stale') ORDER BY ordinal DESC LIMIT 1`;
    return row?.requestId ?? null;
  });

  const insert = Effect.fn("WorkstreamFleetStore.insert")(function* (
    operation: Omit<StoredWorkstreamOperation, "peers" | "reason" | "ordinal">,
    roster: readonly WorkstreamFleetMember[],
  ) {
    yield* sql`INSERT INTO jones_personal_workstream_operations ${sql.insert({
      request_id: operation.requestId,
      workstream_id: operation.workstreamId,
      kind: operation.kind,
      input_json: operation.inputJson,
      base_json: operation.base === null ? null : canonicalJson(operation.base),
      candidate_json: canonicalJson(operation.candidate),
      roster_revision: operation.rosterRevision,
      replaces_request_id: operation.replacesRequestId,
      status: operation.status,
    })}`;
    for (const { environmentId, linkGeneration } of roster)
      yield* sql`INSERT INTO jones_personal_workstream_operation_peers ${sql.insert({
        request_id: operation.requestId,
        environment_id: environmentId,
        link_generation: linkGeneration,
        state: "pending",
      })}`;
  });

  const designate = (environmentId: EnvironmentId, sessionId: string | null) =>
    sql`INSERT INTO jones_personal_workstream_fleet
      (singleton, authority_environment_id, authority_session_id)
      VALUES (1, ${environmentId}, ${sessionId})`;
  const reapprove = (sessionId: string) => sql`UPDATE jones_personal_workstream_fleet
    SET authority_session_id = ${sessionId} WHERE singleton = 1 AND authority_session_id IS NOT NULL`;

  const supersedeEdits = (workstreamId: string) => sql`UPDATE jones_personal_workstream_operations
    SET status = 'superseded' WHERE workstream_id = ${workstreamId}
    AND kind IN ('put', 'delete') AND status IN ('pending', 'stale', 'discovered')`;

  const proposals = sql<P.PersonalWorkstreamFleetProposal>`
    SELECT authority_environment_id AS authorityEnvironmentId, session_id AS sessionId, label
    FROM jones_personal_workstream_fleet_proposals ORDER BY label, session_id`;
  const propose = (id: EnvironmentId, sessionId: string, label: string) =>
    sql`INSERT INTO jones_personal_workstream_fleet_proposals
      (authority_environment_id, session_id, label) VALUES (${id}, ${sessionId}, ${label})
      ON CONFLICT (session_id) DO NOTHING RETURNING session_id`;
  const clearProposals = sql`DELETE FROM jones_personal_workstream_fleet_proposals`;
  const releaseFollower = sql`DELETE FROM jones_personal_workstream_fleet WHERE singleton = 1`;
  const updateRoster = Effect.fn("WorkstreamFleetStore.updateRoster")(function* (
    peerId: EnvironmentId,
    generation: string | null,
  ) {
    if (generation === null) {
      yield* sql`DELETE FROM jones_personal_workstream_fleet_members WHERE environment_id = ${peerId}`;
    } else {
      yield* sql`INSERT INTO jones_personal_workstream_fleet_members (environment_id, link_generation)
        VALUES (${peerId}, ${generation}) ON CONFLICT (environment_id)
        DO UPDATE SET link_generation = excluded.link_generation`;
    }
    yield* sql`UPDATE jones_personal_workstream_fleet SET roster_revision = roster_revision + 1`;
    yield* sql`UPDATE jones_personal_workstream_fleet_members SET discovered = 0`;
    yield* sql`UPDATE jones_personal_workstream_operations SET status = 'superseded'
      WHERE status = 'discovered'`;
    yield* sql`UPDATE jones_personal_workstream_operations SET status = 'stale',
      reason = 'roster_changed' WHERE status = 'pending' AND kind = 'close'`;
  });
  const markDiscovered = (
    member: WorkstreamFleetMember,
  ) => sql`UPDATE jones_personal_workstream_fleet_members
    SET discovered = 1 WHERE environment_id = ${member.environmentId}
      AND link_generation = ${member.linkGeneration} AND discovered = 0 RETURNING environment_id`;
  const unavailableCloseLinks = (requestId: string, now: string) =>
    sql<{ environmentId: EnvironmentId; reason: string }>`
      SELECT leg.environment_id AS environmentId,
        CASE WHEN link.environment_id IS NULL THEN 'peer_link_required'
             WHEN link.secret_name IS NOT leg.link_generation OR link.fenced <> 1 THEN 'peer_link_changed'
             WHEN link.expires_at <= ${now} THEN 'peer_link_expired'
             ELSE 'peer_access_required' END AS reason
      FROM jones_personal_workstream_operation_peers leg
      LEFT JOIN ${sql(peerEnvironmentLinksTable)} link ON link.environment_id = leg.environment_id
      WHERE leg.request_id = ${requestId} AND (link.secret_name IS NOT leg.link_generation
        OR link.fenced <> 1 OR link.expires_at <= ${now} OR link.access = 'read-only')`;
  const rememberOrphan = (
    requestId: string,
    peerId: EnvironmentId,
    observed: P.PersonalWorkstream,
  ) =>
    sql`UPDATE jones_personal_workstream_operation_peers SET state = 'conflict',
      reason = 'peer_only', observed_json = ${canonicalJson(observed)}
      WHERE request_id = ${requestId} AND environment_id = ${peerId}`;
  const setStatus = (
    requestId: string,
    status: P.PersonalWorkstreamOperation["status"],
    reason: string | null = null,
  ) => sql`UPDATE jones_personal_workstream_operations SET status = ${status}, reason = ${reason}
    WHERE request_id = ${requestId} AND (status IS NOT ${status} OR reason IS NOT ${reason})
    RETURNING request_id`;
  const rememberDelivery = (
    peer: WorkstreamDeliveryRow,
    state: P.PersonalWorkstreamDelivery["state"],
    reason: string | null,
    observedJson: string | null,
  ) => sql`UPDATE jones_personal_workstream_operation_peers SET
    state = ${state}, reason = ${reason}, observed_json = ${observedJson},
    attempted = CASE WHEN ${state} IN ('unknown', 'delivered') THEN 1 ELSE attempted END
    WHERE request_id = ${peer.requestId} AND environment_id = ${peer.environmentId}
    AND link_generation = ${peer.linkGeneration} AND (state IS NOT ${state}
      OR reason IS NOT ${reason} OR observed_json IS NOT ${observedJson})
    AND EXISTS (SELECT 1 FROM jones_personal_workstream_operations o
      WHERE o.request_id = ${peer.requestId} AND o.status = 'pending') RETURNING request_id`;
  const knownCopy = (workstreamId: string, ordinal: number, json: string) =>
    sql`SELECT 1 FROM jones_personal_workstream_operations WHERE workstream_id = ${workstreamId}
      AND json_extract(input_json, '$.kind') IS NOT 'discover'
      AND ordinal <= ${ordinal} AND (base_json = ${json} OR candidate_json = ${json}) LIMIT 1`.pipe(
      Effect.map((rows) => rows.length > 0),
    );
  const approveBase = (requestId: string, peerId: EnvironmentId, json: string) =>
    sql`UPDATE jones_personal_workstream_operation_peers SET approved_base_json = ${json}
      WHERE request_id = ${requestId} AND environment_id = ${peerId}`;
  // Retain the authority's receipt across release and reapproval of the same session.
  const received = (authorityId: EnvironmentId, workstreamId: string) =>
    sql<{ ordinal: number; request_id: string }>`
    SELECT ordinal, request_id FROM jones_personal_workstream_fleet_received
    WHERE authority_environment_id = ${authorityId} AND workstream_id = ${workstreamId}`.pipe(
      Effect.map(([row]) => row ?? null),
    );
  const rememberReceipt = (
    authorityId: EnvironmentId,
    workstreamId: string,
    ordinal: number,
    requestId: string,
  ) =>
    sql`INSERT INTO jones_personal_workstream_fleet_received
      (authority_environment_id, workstream_id, ordinal, request_id)
      VALUES (${authorityId}, ${workstreamId}, ${ordinal}, ${requestId})
      ON CONFLICT (authority_environment_id, workstream_id)
      DO UPDATE SET ordinal = excluded.ordinal, request_id = excluded.request_id`;
  return {
    binding,
    members,
    find,
    active,
    openClose,
    insert,
    designate,
    reapprove,
    supersedeEdits,
    proposals,
    propose,
    clearProposals,
    releaseFollower,
    updateRoster,
    setStatus,
    rememberDelivery,
    knownCopy,
    approveBase,
    received,
    rememberReceipt,
    markDiscovered,
    unavailableCloseLinks,
    rememberOrphan,
  };
}

const decodeOperation = Effect.fn("WorkstreamFleetStore.decodeOperation")(function* (
  { baseJson, candidateJson, ...row }: OperationRow,
  peers: readonly WorkstreamDeliveryRow[],
) {
  return {
    ...row,
    base: baseJson === null ? null : yield* decodeDefinition(baseJson),
    candidate: yield* decodeDefinition(candidateJson),
    peers,
  } satisfies StoredWorkstreamOperation;
});
