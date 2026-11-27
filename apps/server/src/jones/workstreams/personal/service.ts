import { pinOrderKeyBetween } from "@t3tools/shared/jones/pinOrder";
import * as Digest from "effect/crypto/Digest";
import * as Hex from "effect/encoding/Hex";
import * as DateTime from "effect/DateTime";
import type { SqlError } from "effect/sql/SqlError";
import { ThreadId, type EnvironmentId } from "@t3tools/contracts";
import * as P from "@t3tools/contracts/jones/workstreamsPersonal";
import type {
  CancelPersonalWorkstreamOperationInput,
  ResolvePersonalWorkstreamConflictInput,
} from "@t3tools/contracts/jones/workstreamsPersonalRpc";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerConfig from "../../../config.ts";
import * as ServerEnvironment from "../../../environment/ServerEnvironment.ts";
import { canonicalJson } from "../../remoteDelegation/shared.ts";
import * as AutoRouteStore from "../autoRoute/AutoRouteStore.ts";
import * as Protocol from "./fleetProtocol.ts";
import {
  makeWorkstreamFleetStore,
  workstreamOperationView,
  type WorkstreamDeliveryRow,
  type StoredWorkstreamOperation,
  type WorkstreamFleetMember,
  type WorkstreamFleetRosterMember,
} from "./fleetStore.ts";

type WorkstreamEffect<A> = Effect.Effect<A, P.WorkstreamsPersonalError>;
type FleetPeer = { readonly sessionId: string; readonly label: string };

export class PersonalWorkstreams extends Context.Service<
  PersonalWorkstreams,
  {
    readonly snapshot: () => Effect.Effect<
      P.PersonalWorkstreamsSnapshot,
      P.WorkstreamsPersonalError
    >;
    readonly members: (
      id: string,
    ) => Effect.Effect<P.PersonalWorkstreamMembers, P.WorkstreamsPersonalError>;
    readonly changes: Stream.Stream<P.PersonalWorkstreamsSnapshot, P.WorkstreamsPersonalError>;
    readonly create: (
      input: Omit<P.NewPersonalWorkstreamInput, "orderKey" | "now" | "origin">,
    ) => Effect.Effect<P.PutResult, P.WorkstreamsPersonalError>;
    readonly put: (
      workstream: P.PersonalWorkstream,
      expected?: P.PersonalWorkstream,
    ) => Effect.Effect<P.PutResult, P.WorkstreamsPersonalError>;
    readonly designateFleet: (
      members?: readonly WorkstreamFleetMember[],
    ) => Effect.Effect<P.PersonalWorkstreamFleet, P.WorkstreamsPersonalError>;
    readonly requestOperation: (
      input: P.RequestPersonalWorkstreamOperation,
    ) => Effect.Effect<P.PersonalWorkstreamOperationResult, P.WorkstreamsPersonalError>;
    readonly requestAgentOperation: (
      input: Omit<P.RequestPersonalWorkstreamOperation, "requestId">,
    ) => WorkstreamEffect<P.PersonalWorkstreamOperationResult>;
    readonly cancelOperation: (
      input: CancelPersonalWorkstreamOperationInput,
    ) => WorkstreamEffect<P.PersonalWorkstreamOperationResult>;
    readonly resolveOperation: (
      input: ResolvePersonalWorkstreamConflictInput,
    ) => WorkstreamEffect<P.PersonalWorkstreamOperationResult>;
    readonly changeRoster: (
      id: EnvironmentId,
      revision: number,
      generation: string | null,
    ) => WorkstreamEffect<P.PersonalWorkstreamFleet>;
    readonly approveFollower: (
      authorityId: EnvironmentId,
      sessionId: string,
    ) => WorkstreamEffect<P.PersonalWorkstreamFleet>;
    readonly releaseFollower: (authorityId: EnvironmentId) => WorkstreamEffect<{ released: true }>;
    readonly proposeFollower: (
      authorityId: EnvironmentId,
      peer: FleetPeer,
    ) => WorkstreamEffect<typeof Protocol.FleetPeerEnrollment.Type>;
    readonly readPeerDefinition: (
      authorityId: EnvironmentId,
      workstreamId: string,
      peer: FleetPeer,
    ) => WorkstreamEffect<typeof Protocol.FleetPeerDefinition.Type>;
    readonly readPeerDefinitions: (
      authorityId: EnvironmentId,
      peer: FleetPeer,
    ) => WorkstreamEffect<typeof Protocol.FleetPeerDefinitions.Type>;
    readonly applyPeerDefinition: (
      input: Protocol.FleetPeerApplyDefinition,
      peer: FleetPeer,
    ) => WorkstreamEffect<typeof Protocol.FleetPeerApplyResult.Type>;
    readonly pendingOperations: WorkstreamEffect<readonly StoredWorkstreamOperation[]>;
    readonly currentFleetMembers: WorkstreamEffect<{
      readonly authorityId: EnvironmentId;
      readonly revision: number;
      readonly members: readonly WorkstreamFleetRosterMember[];
    } | null>;
    readonly recordPeerDiscovery: (
      member: WorkstreamFleetMember,
      revision: number,
      workstreams: readonly P.PersonalWorkstream[],
    ) => WorkstreamEffect<void>;
    readonly beginDelivery: (peer: WorkstreamDeliveryRow) => WorkstreamEffect<boolean>;
    readonly knownPeerBase: (
      operation: StoredWorkstreamOperation,
      peer: WorkstreamDeliveryRow,
      workstream: P.PersonalWorkstream | null,
    ) => WorkstreamEffect<boolean>;
    readonly recordDelivery: (
      peer: WorkstreamDeliveryRow,
      state: P.PersonalWorkstreamDelivery["state"],
      reason: string | null,
      observed?: P.PersonalWorkstream | null,
    ) => WorkstreamEffect<void>;
    readonly finalizeOperation: (requestId: string) => WorkstreamEffect<void>;
    readonly assign: (
      input: P.AssignPersonalWorkstreamInput,
      options?: { readonly supersedeInitialAutoAssignment: boolean },
    ) => Effect.Effect<P.AssignResult, P.WorkstreamsPersonalError>;
    readonly assignAutomatically: (
      input: AutoRouteStore.DecisionKey & {
        readonly workstreamId: string;
        readonly workstreamName: string;
        readonly confidence: "high";
        readonly basis: "name" | "similar_threads" | "both";
      },
    ) => Effect.Effect<void, P.WorkstreamsPersonalError>;
    readonly readThread: (id: ThreadId) => Effect.Effect<
      {
        assignment: P.PersonalWorkstreamAssignment;
        workstream: P.PersonalWorkstream | null;
        /** The thread's latest automatic-routing decision; absent without the Jones table. */
        autoRoute?: AutoRouteStore.AutoRouteDecisionView | null;
      },
      P.WorkstreamsPersonalError
    >;
    readonly readDefinitions: (
      ids?: readonly string[],
    ) => Effect.Effect<readonly P.PersonalWorkstream[], P.WorkstreamsPersonalError>;
    readonly counts: () => Effect.Effect<
      { observedAt: string; sequence: number; total: number; active: number },
      P.WorkstreamsPersonalError
    >;
  }
>()("t3/jones/workstreams/personal/service/PersonalWorkstreams") {}

const definitionColumns = `id, name, lifecycle, color, order_key AS orderKey,
  created_at AS createdAt, updated_at AS updatedAt, deleted_at AS deletedAt, rev,
  lifecycle_revision AS lifecycleRevision, snoozed_at AS snoozedAt, snoozed_until AS snoozedUntil,
  name_icon_project_key AS nameIconProjectKey, name_icon_updated_at AS nameIconUpdatedAt, origin`;
type DefinitionRow = P.PersonalWorkstream & {
  nameIconProjectKey: string | null;
  nameIconUpdatedAt: string | null;
};
const normalizeDefinition = ({
  lifecycleRevision,
  snoozedAt,
  snoozedUntil,
  nameIconProjectKey,
  nameIconUpdatedAt,
  ...definition
}: DefinitionRow): P.PersonalWorkstream => ({
  ...definition,
  ...(lifecycleRevision ? { lifecycleRevision } : {}),
  ...(snoozedAt == null && snoozedUntil == null
    ? {}
    : { snoozedAt: snoozedAt ?? null, snoozedUntil: snoozedUntil ?? null }),
  ...(nameIconUpdatedAt
    ? { nameIcon: { projectKey: nameIconProjectKey, updatedAt: nameIconUpdatedAt } }
    : {}),
});
const assignmentColumns = `a.thread_id AS threadId, a.workstream_id AS workstreamId, a.revision, a.updated_at AS updatedAt`;
const ImportFile = Schema.Struct({
  schema: Schema.Literal("jones.workstreams-personal-import/v1"),
  importId: Schema.String,
  targetEnvironmentId: Schema.String,
  workstreams: Schema.Array(P.PersonalWorkstream),
  assignments: Schema.Array(
    Schema.Struct({ threadId: ThreadId, workstreamId: P.PersonalWorkstreamId }),
  ),
});
const isPersonalError = Schema.is(P.WorkstreamsPersonalError);
const decodeDefinition = Schema.decodeEffect(P.PersonalWorkstream);
const decodeAssignment = Schema.decodeEffect(P.AssignPersonalWorkstreamInput);
const decodeOperationRequest = Schema.decodeEffect(P.RequestPersonalWorkstreamOperation);
const decodeFleetDefinition = Schema.decodeEffect(Schema.fromJsonString(P.PersonalWorkstream));
const decodeScopes = Schema.decodeEffect(Schema.fromJsonString(Schema.Array(Schema.String)));
const decodeImport = Schema.decodeEffect(Schema.fromJsonString(ImportFile));
const storage = (error: unknown) =>
  isPersonalError(error) ? error : new P.WorkstreamsPersonalError({ code: "storage_unavailable" });

const sameDefinition = (a: P.PersonalWorkstream | null, b: P.PersonalWorkstream | null) =>
  a === null || b === null
    ? a === b
    : P.canonicalPersonalWorkstream(a) === P.canonicalPersonalWorkstream(b);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const digester = yield* Digest.Digest;
  const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));
  const fs = yield* FileSystem.FileSystem;
  const config = yield* ServerConfig.ServerConfig;
  const environmentId = yield* (yield* ServerEnvironment.ServerEnvironmentIdentity)
    .getEnvironmentId;
  const invalidations = yield* PubSub.sliding<void>(1);
  const autoRoute = yield* AutoRouteStore.make;
  const fleet = makeWorkstreamFleetStore(sql);
  const fleetObservationKey = (value: string) =>
    digester.digest(Digest.sha256, new TextEncoder().encode(value)).pipe(Effect.map(Hex.encode));
  const operationView = (operation: StoredWorkstreamOperation) =>
    workstreamOperationView(operation, fleetObservationKey);
  const fleetRequestId = Effect.fn("PersonalWorkstreams.fleetRequestId")(function* (
    kind: string,
    identity: unknown,
  ) {
    return `${kind}:${yield* fleetObservationKey(canonicalJson(identity))}`;
  });
  let importIssue: P.PersonalWorkstreamsImportIssue | null = null;
  const sequence = Effect.fn("PersonalWorkstreams.sequence")(function* () {
    const rows = yield* sql<{
      value: string;
    }>`SELECT value FROM jones_personal_workstream_meta WHERE key='sequence'`;
    return Number(rows[0]?.value ?? 0);
  });
  const bump = Effect.fn("PersonalWorkstreams.bump")(function* () {
    yield* sql`INSERT INTO jones_personal_workstream_meta(key,value) VALUES('sequence','1') ON CONFLICT(key) DO UPDATE SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT)`;
  });
  const definitions = () =>
    sql
      .unsafe<DefinitionRow>(`SELECT ${definitionColumns} FROM jones_personal_workstreams`)
      .pipe(Effect.map((rows) => rows.map(normalizeDefinition)));
  const definition = Effect.fn("PersonalWorkstreams.definition")(function* (id: string) {
    const rows = yield* sql.unsafe<DefinitionRow>(
      `SELECT ${definitionColumns} FROM jones_personal_workstreams WHERE id=?`,
      [id],
    );
    return rows[0] ? normalizeDefinition(rows[0]) : null;
  });
  const exists = (id: ThreadId) =>
    sql`SELECT 1 FROM orchestration_v2_projection_threads WHERE thread_id=${id} AND deleted_at IS NULL`.pipe(
      Effect.map((rows) => rows.length > 0),
    );
  const assignment = Effect.fn("PersonalWorkstreams.assignment")(function* (id: ThreadId) {
    const rows = yield* sql.unsafe<P.PersonalWorkstreamAssignment>(
      `SELECT ${assignmentColumns} FROM jones_personal_workstream_assignments a WHERE a.thread_id=?`,
      [id],
    );
    return rows[0] ?? { threadId: id, workstreamId: null, revision: 0, updatedAt: null };
  });
  const persistAssignment = Effect.fn("PersonalWorkstreams.persistAssignment")(function* (
    threadId: ThreadId,
    workstreamId: string | null,
    revision: number,
  ) {
    const next = { threadId, workstreamId, revision, updatedAt: yield* now };
    yield* sql`INSERT INTO jones_personal_workstream_assignments ${sql.insert({ thread_id: threadId, workstream_id: workstreamId, revision, updated_at: next.updatedAt })} ON CONFLICT(thread_id) DO UPDATE SET workstream_id=excluded.workstream_id,revision=excluded.revision,updated_at=excluded.updated_at`;
    yield* bump();
    return next;
  });
  const store = Effect.fn("PersonalWorkstreams.store")(function* (w: P.PersonalWorkstream) {
    yield* sql`INSERT INTO jones_personal_workstreams ${sql.insert({
      id: w.id,
      name: w.name,
      lifecycle: w.lifecycle,
      color: w.color,
      order_key: w.orderKey,
      created_at: w.createdAt,
      updated_at: w.updatedAt,
      deleted_at: w.deletedAt,
      rev: w.rev,
      lifecycle_revision: w.lifecycleRevision ?? 0,
      snoozed_at: w.snoozedAt ?? null,
      snoozed_until: w.snoozedUntil ?? null,
      name_icon_project_key: w.nameIcon?.projectKey ?? null,
      name_icon_updated_at: w.nameIcon?.updatedAt ?? null,
      origin: w.origin,
    })} ON CONFLICT(id) DO UPDATE SET
      name=excluded.name, lifecycle=excluded.lifecycle, color=excluded.color,
      order_key=excluded.order_key, created_at=excluded.created_at, updated_at=excluded.updated_at,
      deleted_at=excluded.deleted_at, rev=excluded.rev, lifecycle_revision=excluded.lifecycle_revision,
      snoozed_at=excluded.snoozed_at, snoozed_until=excluded.snoozed_until,
      name_icon_project_key=excluded.name_icon_project_key, name_icon_updated_at=excluded.name_icon_updated_at,
      origin=excluded.origin`;
  });
  const unsettledMember = (id: string) =>
    sql`SELECT 1
      FROM jones_personal_workstream_assignments a
      JOIN orchestration_v2_projection_threads t ON t.thread_id = a.thread_id
      WHERE a.workstream_id = ${id} AND t.deleted_at IS NULL
      AND json_extract(t.payload_json, '$.settledOverride') IS NOT 'settled'
      LIMIT 1`;
  const members = Effect.fn("PersonalWorkstreams.members")((id: string) =>
    sql<{ threadId: ThreadId; title: string; settled: number }>`SELECT
        a.thread_id AS threadId,
        t.title,
        json_extract(t.payload_json, '$.settledOverride') IS 'settled' AS settled
      FROM jones_personal_workstream_assignments a
      JOIN orchestration_v2_projection_threads t ON t.thread_id = a.thread_id
      WHERE a.workstream_id = ${id} AND t.deleted_at IS NULL
      ORDER BY a.updated_at DESC, a.thread_id`.pipe(
      Effect.map((rows) => rows.map((row) => ({ ...row, settled: row.settled === 1 }))),
      Effect.mapError(storage),
    ),
  );
  const merge = Effect.fn("PersonalWorkstreams.merge")(function* (
    candidate: P.PersonalWorkstream,
    expected?: P.PersonalWorkstream,
    exact = false,
  ): Effect.fn.Return<P.PutResult, SqlError | P.WorkstreamsPersonalError> {
    const old = yield* definition(candidate.id);
    const w =
      old && !exact
        ? P.preserveWorkstreamNameIcon(P.preservePersonalWorkstreamSnooze(candidate, old), old)
        : candidate;
    const comparison = old === null ? 1 : P.comparePersonalWorkstreams(w, old);
    if (comparison <= 0)
      return {
        outcome: comparison === 0 ? "already_satisfied" : "superseded",
        workstream: old!,
        sequence: yield* sequence(),
      };
    if (
      w.deletedAt === null &&
      P.isClosedPersonalWorkstream(w.lifecycle) &&
      old?.lifecycle !== w.lifecycle &&
      (yield* unsettledMember(w.id)).length > 0
    )
      return yield* new P.WorkstreamsPersonalError({ code: "unsettled_threads" });
    if (
      old?.deletedAt === null &&
      w.deletedAt === null &&
      P.isClosedPersonalWorkstream(old.lifecycle) !== P.isClosedPersonalWorkstream(w.lifecycle) &&
      (w.lifecycleRevision ?? 0) <= (old.lifecycleRevision ?? 0)
    )
      return yield* new P.WorkstreamsPersonalError({ code: "precondition_failed" });
    if (expected) {
      let precondition = expected;
      if (old && expected.snoozedUntil === undefined)
        precondition = P.preservePersonalWorkstreamSnooze(precondition, old);
      if (old && expected.nameIcon === undefined)
        precondition = P.preserveWorkstreamNameIcon(precondition, old);
      if (!old || P.comparePersonalWorkstreams(old, precondition) !== 0)
        return yield* new P.WorkstreamsPersonalError({ code: "precondition_failed" });
    }
    yield* store(w);
    if (w.deletedAt !== null)
      yield* sql`UPDATE jones_personal_workstream_assignments SET workstream_id=NULL, revision=revision+1, updated_at=${w.updatedAt} WHERE workstream_id=${w.id}`;
    yield* bump();
    return { outcome: "applied", workstream: w, sequence: yield* sequence() };
  });
  // The mask spans commit and publication: request cancellation cannot hide a durable write.
  const write = <A, E>(body: Effect.Effect<A, E>) =>
    Effect.uninterruptible(
      sql
        .withTransaction(
          Effect.gen(function* () {
            const before = yield* sequence();
            const result = yield* body;
            return { result, changed: (yield* sequence()) !== before };
          }),
        )
        .pipe(
          Effect.flatMap(({ result, changed }) =>
            changed
              ? PubSub.publish(invalidations, undefined).pipe(Effect.as(result))
              : Effect.succeed(result),
          ),
          Effect.mapError(storage),
        ),
    );
  const fleetSnapshot = Effect.gen(function* () {
    const binding = yield* fleet.binding;
    if (!binding) return undefined;
    const isAuthority = binding.authorityEnvironmentId === environmentId;
    return {
      authorityEnvironmentId: binding.authorityEnvironmentId,
      role: isAuthority ? "authority" : "follower",
      rosterRevision: binding.rosterRevision,
      members: isAuthority ? (yield* fleet.members).map((member) => member.environmentId) : [],
      operations: isAuthority ? yield* Effect.forEach(yield* fleet.active, operationView) : [],
    } satisfies P.PersonalWorkstreamFleet;
  });
  const requireLegacy = Effect.gen(function* () {
    if (yield* fleet.binding)
      return yield* new P.WorkstreamsPersonalError({ code: "authority_required" });
  });
  const requireAuthority = Effect.gen(function* () {
    const binding = yield* fleet.binding;
    if (binding?.authorityEnvironmentId !== environmentId || binding.authoritySessionId !== null)
      return yield* new P.WorkstreamsPersonalError({ code: "authority_required" });
    return binding;
  });
  const requirePeerSession = Effect.fn("PersonalWorkstreams.requirePeerSession")(function* (
    sessionId: string,
  ) {
    const [row] = yield* sql<{ scopes: string }>`SELECT scopes FROM auth_sessions
      WHERE session_id = ${sessionId} AND subject = 'mcp-client' AND method = 'bearer-access-token'
      AND expires_at > ${yield* now} AND revoked_at IS NULL`;
    if (!row || !(yield* decodeScopes(row.scopes)).includes("orchestration:operate"))
      return yield* new P.WorkstreamsPersonalError({ code: "authority_required" });
  });
  const requireFollower = Effect.fn("PersonalWorkstreams.requireFollower")(function* (
    authorityId: EnvironmentId,
    peer: FleetPeer,
  ) {
    const binding = yield* fleet.binding;
    if (
      binding?.authorityEnvironmentId !== authorityId ||
      binding.authoritySessionId !== peer.sessionId
    )
      return yield* new P.WorkstreamsPersonalError({ code: "authority_required" });
    yield* requirePeerSession(peer.sessionId);
  });
  const proposeFollower = Effect.fn("PersonalWorkstreams.proposeFollower")(
    (authorityId: EnvironmentId, peer: FleetPeer) =>
      write(
        Effect.gen(function* () {
          yield* requirePeerSession(peer.sessionId);
          const binding = yield* fleet.binding;
          if (
            authorityId === environmentId ||
            (binding && binding.authorityEnvironmentId !== authorityId)
          )
            return { environmentId, state: "different_authority" } as const;
          if (binding?.authoritySessionId === peer.sessionId)
            return { environmentId, state: "approved" } as const;
          if ((yield* fleet.propose(authorityId, peer.sessionId, peer.label)).length) yield* bump();
          return { environmentId, state: "approval_required" } as const;
        }),
      ),
  );
  const approveFollower = Effect.fn("PersonalWorkstreams.approveFollower")(
    (authorityId: EnvironmentId, sessionId: string) =>
      write(
        Effect.gen(function* () {
          if (authorityId === environmentId)
            return yield* new P.WorkstreamsPersonalError({ code: "invalid_request" });
          yield* requirePeerSession(sessionId);
          const existing = yield* fleet.binding;
          if (
            existing?.authorityEnvironmentId === authorityId &&
            existing.authoritySessionId === sessionId
          )
            return (yield* fleetSnapshot)!;
          if (
            (existing && existing.authorityEnvironmentId !== authorityId) ||
            !(yield* fleet.proposals).some(
              (proposal) =>
                proposal.authorityEnvironmentId === authorityId && proposal.sessionId === sessionId,
            )
          )
            return yield* new P.WorkstreamsPersonalError({ code: "precondition_failed" });
          if (existing) yield* fleet.reapprove(sessionId);
          else yield* fleet.designate(authorityId, sessionId);
          yield* fleet.clearProposals;
          yield* bump();
          return (yield* fleetSnapshot)!;
        }),
      ),
  );
  const releaseFollower = Effect.fn("PersonalWorkstreams.releaseFollower")(
    (authorityId: EnvironmentId) =>
      write(
        Effect.gen(function* () {
          const binding = yield* fleet.binding;
          if (binding === null) return { released: true as const };
          if (binding.authorityEnvironmentId !== authorityId || !binding.authoritySessionId)
            return yield* new P.WorkstreamsPersonalError({ code: "precondition_failed" });
          yield* fleet.releaseFollower;
          yield* fleet.clearProposals;
          yield* bump();
          return { released: true as const };
        }),
      ),
  );
  const readPeerDefinition = Effect.fn("PersonalWorkstreams.readPeerDefinition")(
    (authorityId: EnvironmentId, id: string, peer: FleetPeer) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            yield* requireFollower(authorityId, peer);
            const receipt = yield* fleet.received(authorityId, id);
            return {
              workstream: yield* definition(id),
              lastRequestId: receipt?.request_id ?? null,
              lastOrdinal: receipt?.ordinal ?? null,
            };
          }),
        )
        .pipe(Effect.mapError(storage)),
  );
  const readPeerDefinitions = Effect.fn("PersonalWorkstreams.readPeerDefinitions")(
    (authorityId: EnvironmentId, peer: FleetPeer) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            yield* requireFollower(authorityId, peer);
            return { workstreams: yield* definitions() };
          }),
        )
        .pipe(Effect.mapError(storage)),
  );
  const applyPeerDefinition = Effect.fn("PersonalWorkstreams.applyPeerDefinition")(
    (input: Protocol.FleetPeerApplyDefinition, peer: FleetPeer) =>
      write(
        Effect.gen(function* () {
          yield* requireFollower(input.authorityEnvironmentId, peer);
          const { authorityEnvironmentId, candidate, expected, requestId, ordinal, workstreamId } =
            input;
          if (
            (candidate && candidate.id !== workstreamId) ||
            (expected && expected.id !== workstreamId) ||
            (candidate === null && (expected !== null || input.closing))
          )
            return yield* new P.WorkstreamsPersonalError({ code: "invalid_request" });
          const old = yield* definition(workstreamId);
          const receipt = yield* fleet.received(authorityEnvironmentId, workstreamId);
          if (
            receipt?.ordinal === ordinal &&
            receipt.request_id === requestId &&
            sameDefinition(old, candidate)
          )
            return { state: "delivered", reason: null } as const;
          if ((receipt && receipt.ordinal >= ordinal) || !sameDefinition(old, expected))
            return { state: "conflict", reason: "definition_changed" } as const;
          if (input.closing && (yield* unsettledMember(workstreamId)).length)
            return { state: "guard_rejected", reason: "unsettled_threads" } as const;
          if (candidate && !sameDefinition(old, candidate)) {
            const result = yield* merge(candidate, old ?? undefined, true).pipe(Effect.result);
            if (result._tag === "Failure") {
              if (isPersonalError(result.failure)) {
                const reason = result.failure.code;
                if (reason === "unsettled_threads")
                  return { state: "guard_rejected", reason } as const;
                if (reason === "precondition_failed") return { state: "conflict", reason } as const;
              }
              return yield* result.failure;
            }
            if (result.success.outcome !== "applied")
              return { state: "conflict", reason: "newer_definition" } as const;
          }
          yield* fleet.rememberReceipt(authorityEnvironmentId, workstreamId, ordinal, requestId);
          return { state: "delivered", reason: null } as const;
        }),
      ),
  );
  const requestOperation = Effect.fn("PersonalWorkstreams.requestOperation")(function* (
    input: P.RequestPersonalWorkstreamOperation,
  ) {
    const valid = yield* decodeOperationRequest(input).pipe(
      Effect.mapError(() => new P.WorkstreamsPersonalError({ code: "invalid_request" })),
    );
    return yield* write(
      Effect.gen(function* () {
        const binding = yield* requireAuthority;
        const inputJson = canonicalJson(valid);
        const prior = yield* fleet.find(valid.requestId);
        if (prior) {
          if (prior.inputJson !== inputJson)
            return yield* new P.WorkstreamsPersonalError({ code: "precondition_failed" });
          return {
            operation: yield* operationView(prior),
            workstream: yield* definition(prior.workstreamId),
            sequence: yield* sequence(),
          };
        }
        const base = yield* definition(valid.workstreamId);
        const expected = valid.expected;
        if (!sameDefinition(base, expected))
          return yield* new P.WorkstreamsPersonalError({ code: "precondition_failed" });
        if (yield* fleet.openClose(valid.workstreamId))
          return yield* new P.WorkstreamsPersonalError({ code: "operation_pending" });
        const action = valid.action;
        if (base?.deletedAt != null)
          return yield* new P.WorkstreamsPersonalError({ code: "workstream_deleted" });
        if ((base === null) !== (action.kind === "create"))
          return yield* new P.WorkstreamsPersonalError({ code: "precondition_failed" });
        const timestamp = yield* now;
        const origin = `env:${environmentId}`;
        const candidate = yield* Effect.try({
          try: () => {
            switch (action.kind) {
              case "create":
                return P.newPersonalWorkstream({
                  id: valid.workstreamId,
                  name: action.name,
                  orderKey: action.orderKey,
                  now: timestamp,
                  origin,
                  ...(action.lifecycle === undefined ? {} : { lifecycle: action.lifecycle }),
                  ...(action.color === undefined ? {} : { color: action.color }),
                  ...(action.nameIconProjectKey === undefined
                    ? {}
                    : { nameIconProjectKey: action.nameIconProjectKey }),
                });
              case "edit":
                return P.editPersonalWorkstream(base!, action.patch, timestamp, origin);
              case "delete":
                return P.deletePersonalWorkstream(base!, timestamp, origin);
            }
          },
          catch: () => new P.WorkstreamsPersonalError({ code: "invalid_request" }),
        });
        if (action.kind === "create" && P.isClosedPersonalWorkstream(candidate.lifecycle))
          return yield* new P.WorkstreamsPersonalError({ code: "closure_unavailable" });
        const closing =
          action.kind === "edit" &&
          P.isClosedPersonalWorkstream(candidate.lifecycle) &&
          candidate.lifecycle !== base?.lifecycle;
        if (closing && (yield* unsettledMember(candidate.id)).length)
          return yield* new P.WorkstreamsPersonalError({ code: "unsettled_threads" });
        const roster = yield* fleet.members;
        const pending = roster.length > 0;
        const kind = closing ? "close" : action.kind === "delete" ? "delete" : "put";
        yield* fleet.supersedeEdits(candidate.id);
        if (!closing || !pending) yield* merge(candidate, base ?? undefined, true);
        yield* fleet.insert(
          {
            requestId: valid.requestId,
            workstreamId: valid.workstreamId,
            inputJson,
            kind,
            base,
            candidate,
            replacesRequestId: null,
            rosterRevision: binding.rosterRevision,
            status: pending ? "pending" : "applied",
          },
          roster,
        );
        yield* bump();
        return {
          operation: yield* operationView((yield* fleet.find(valid.requestId))!),
          workstream: yield* definition(valid.workstreamId),
          sequence: yield* sequence(),
        };
      }),
    );
  });
  const receipt = Effect.fn("PersonalWorkstreams.receipt")(function* (
    id: string,
    inputJson: string,
  ) {
    const record = yield* fleet.find(id);
    if (!record) return null;
    if (record.inputJson !== inputJson)
      return yield* new P.WorkstreamsPersonalError({ code: "precondition_failed" });
    return {
      operation: yield* operationView(record),
      workstream: yield* definition(record.workstreamId),
      sequence: yield* sequence(),
    };
  });
  const syncRoster = Effect.fn("PersonalWorkstreams.syncRoster")(function* (revision: number) {
    const roster = yield* fleet.members;
    for (const workstream of yield* definitions()) {
      if (yield* fleet.openClose(workstream.id)) continue;
      yield* fleet.supersedeEdits(workstream.id);
      if (!roster.length) continue;
      const requestId = yield* fleetRequestId("sync", [environmentId, revision, workstream.id]);
      yield* fleet.insert(
        {
          requestId,
          workstreamId: workstream.id,
          inputJson: canonicalJson({ kind: "sync", requestId }),
          kind: workstream.deletedAt === null ? "put" : "delete",
          base: workstream,
          candidate: workstream,
          replacesRequestId: null,
          rosterRevision: revision,
          status: "pending",
        },
        roster,
      );
    }
  });
  const designateFleet = Effect.fn("PersonalWorkstreams.designateFleet")(
    (members: readonly WorkstreamFleetMember[] = []) =>
      write(
        Effect.gen(function* () {
          const existing = yield* fleetSnapshot;
          if (existing?.role === "authority") return existing;
          if (existing || members.some((member) => member.environmentId === environmentId))
            return yield* new P.WorkstreamsPersonalError({ code: "authority_required" });
          yield* fleet.designate(environmentId, null);
          for (const member of members)
            yield* fleet.updateRoster(member.environmentId, member.linkGeneration);
          yield* syncRoster(members.length);
          yield* bump();
          return (yield* fleetSnapshot)!;
        }),
      ),
  );
  const changeRoster = Effect.fn("PersonalWorkstreams.changeRoster")(
    (id: EnvironmentId, revision: number, generation: string | null) =>
      write(
        Effect.gen(function* () {
          const binding = yield* requireAuthority;
          if (binding.rosterRevision !== revision || id === environmentId)
            return yield* new P.WorkstreamsPersonalError({ code: "precondition_failed" });
          const prior = (yield* fleet.members).find((member) => member.environmentId === id);
          if (prior?.linkGeneration === generation || (!prior && generation === null))
            return (yield* fleetSnapshot)!;
          yield* fleet.updateRoster(id, generation);
          yield* syncRoster(revision + 1);
          yield* bump();
          return (yield* fleetSnapshot)!;
        }),
      ),
  );
  const cancelOperation = Effect.fn("PersonalWorkstreams.cancelOperation")(
    (input: CancelPersonalWorkstreamOperationInput) =>
      write(
        Effect.gen(function* () {
          const binding = yield* requireAuthority;
          const inputJson = canonicalJson({ kind: "cancel", ...input });
          const prior = yield* receipt(input.cancellationId, inputJson);
          if (prior) return prior;
          const operation = yield* fleet.find(input.requestId);
          if (
            operation?.kind !== "close" ||
            (operation.status !== "pending" && operation.status !== "stale") ||
            !operation.base ||
            !sameDefinition(yield* definition(operation.workstreamId), operation.base)
          )
            return yield* new P.WorkstreamsPersonalError({ code: "precondition_failed" });
          const roster = yield* fleet.members;
          const attempted = operation.peers.filter((peer) => peer.attempted === 1);
          const candidate = {
            ...operation.base,
            rev: operation.candidate.rev + 1,
            lifecycleRevision: (operation.candidate.lifecycleRevision ?? 0) + 1,
            updatedAt: yield* now,
            origin: `env:${environmentId}`,
          };
          yield* merge(candidate, operation.base, true);
          yield* fleet.setStatus(operation.requestId, "cancelled");
          yield* fleet.supersedeEdits(operation.workstreamId);
          // Recovery survives edits and enrollment changes. A dispatched close may still
          // arrive after its cancellation, even if it had not changed the peer at readback.
          if (attempted.length) {
            const requestId = yield* fleetRequestId("recover-close", input);
            yield* fleet.insert(
              {
                requestId,
                workstreamId: operation.workstreamId,
                inputJson: canonicalJson({ kind: "recover", requestId }),
                kind: "recovery",
                base: operation.candidate,
                candidate,
                replacesRequestId: operation.requestId,
                rosterRevision: binding.rosterRevision,
                status: "pending",
              },
              attempted,
            );
          }
          yield* fleet.insert(
            {
              requestId: input.cancellationId,
              workstreamId: operation.workstreamId,
              inputJson,
              kind: "put",
              base: operation.base,
              candidate,
              replacesRequestId: input.requestId,
              rosterRevision: binding.rosterRevision,
              status: roster.length ? "pending" : "applied",
            },
            roster,
          );
          return (yield* receipt(input.cancellationId, inputJson))!;
        }),
      ),
  );
  const resolveOperation = Effect.fn("PersonalWorkstreams.resolveOperation")(
    (input: ResolvePersonalWorkstreamConflictInput) =>
      write(
        Effect.gen(function* () {
          const binding = yield* requireAuthority;
          const inputJson = canonicalJson({ kind: "resolve", ...input });
          const prior = yield* receipt(input.resolutionId, inputJson);
          if (prior) return prior;
          const operation = yield* fleet.find(input.requestId);
          const leg = operation?.peers.find((peer) => peer.environmentId === input.environmentId);
          if (
            !operation ||
            !["pending", "discovered"].includes(operation.status) ||
            (operation.kind !== "put" && operation.kind !== "delete") ||
            operation.rosterRevision !== binding.rosterRevision ||
            leg?.state !== "conflict" ||
            !leg.observedJson ||
            (yield* fleetObservationKey(leg.observedJson)) !== input.expectedObservationKey
          )
            return yield* new P.WorkstreamsPersonalError({ code: "precondition_failed" });
          const base = yield* definition(operation.workstreamId);
          const discovered = operation.status === "discovered";
          if (
            discovered
              ? base !== null || input.choice !== "use_peer"
              : !sameDefinition(base, operation.candidate)
          )
            return yield* new P.WorkstreamsPersonalError({ code: "precondition_failed" });
          const observed = yield* decodeFleetDefinition(leg.observedJson);
          const selected = input.choice === "use_peer" ? observed : base!;
          if (
            (base?.deletedAt != null || observed.deletedAt !== null) &&
            selected.deletedAt === null
          )
            return yield* new P.WorkstreamsPersonalError({ code: "workstream_deleted" });
          const candidate = {
            ...selected,
            lifecycle:
              discovered && P.isClosedPersonalWorkstream(selected.lifecycle)
                ? ("active" as const)
                : selected.lifecycle,
            updatedAt: yield* now,
            rev: Math.max(base?.rev ?? 0, observed.rev) + 1,
            lifecycleRevision:
              Math.max(base?.lifecycleRevision ?? 0, observed.lifecycleRevision ?? 0) + 1,
            origin: `env:${environmentId}`,
          };
          const closing =
            candidate.deletedAt === null &&
            candidate.lifecycle !== base?.lifecycle &&
            P.isClosedPersonalWorkstream(candidate.lifecycle);
          if (closing && (yield* unsettledMember(candidate.id)).length)
            return yield* new P.WorkstreamsPersonalError({ code: "unsettled_threads" });
          const roster = yield* fleet.members;
          yield* fleet.supersedeEdits(candidate.id);
          if (!closing || !roster.length) yield* merge(candidate, base ?? undefined, true);
          yield* fleet.insert(
            {
              requestId: input.resolutionId,
              workstreamId: candidate.id,
              inputJson,
              kind: closing ? "close" : candidate.deletedAt === null ? "put" : "delete",
              base,
              candidate,
              replacesRequestId: input.requestId,
              rosterRevision: binding.rosterRevision,
              status: roster.length ? "pending" : "applied",
            },
            roster,
          );
          yield* fleet.approveBase(input.resolutionId, leg.environmentId, leg.observedJson);
          yield* bump();
          return (yield* receipt(input.resolutionId, inputJson))!;
        }),
      ),
  );
  const recordDelivery = Effect.fn("PersonalWorkstreams.recordDelivery")(
    (
      peer: WorkstreamDeliveryRow,
      state: P.PersonalWorkstreamDelivery["state"],
      reason: string | null,
      observed?: P.PersonalWorkstream | null,
    ) =>
      write(
        Effect.gen(function* () {
          const json = observed ? canonicalJson(observed) : null;
          if ((yield* fleet.rememberDelivery(peer, state, reason, json)).length) yield* bump();
        }),
      ),
  );
  const recordPeerDiscovery = Effect.fn("PersonalWorkstreams.recordPeerDiscovery")(
    (
      member: WorkstreamFleetMember,
      revision: number,
      workstreams: readonly P.PersonalWorkstream[],
    ) =>
      write(
        Effect.gen(function* () {
          const binding = yield* requireAuthority;
          if (binding.rosterRevision !== revision || !(yield* fleet.markDiscovered(member)).length)
            return;
          for (const workstream of workstreams) {
            if (workstream.deletedAt !== null || (yield* definition(workstream.id)) !== null)
              continue;
            const requestId = yield* fleetRequestId("discover", [
              revision,
              member.environmentId,
              workstream.id,
            ]);
            yield* fleet.insert(
              {
                requestId,
                workstreamId: workstream.id,
                inputJson: canonicalJson({ kind: "discover", requestId }),
                kind: "put",
                base: null,
                candidate: workstream,
                replacesRequestId: null,
                rosterRevision: revision,
                status: "discovered",
              },
              [member],
            );
            yield* fleet.rememberOrphan(requestId, member.environmentId, workstream);
            yield* bump();
          }
        }),
      ),
  );
  const beginDelivery = Effect.fn("PersonalWorkstreams.beginDelivery")(
    (peer: WorkstreamDeliveryRow) =>
      write(
        Effect.gen(function* () {
          const active = yield* fleet.find(peer.requestId);
          if (active?.status !== "pending") return false;
          if ((yield* fleet.rememberDelivery(peer, "unknown", null, null)).length) yield* bump();
          return true;
        }),
      ),
  );
  const finalizeOperation = Effect.fn("PersonalWorkstreams.finalizeOperation")((id: string) =>
    write(
      Effect.gen(function* () {
        const binding = yield* requireAuthority;
        const operation = yield* fleet.find(id);
        if (
          !operation ||
          operation.status !== "pending" ||
          (operation.kind !== "recovery" && operation.rosterRevision !== binding.rosterRevision) ||
          operation.peers.some((peer) => peer.state !== "delivered")
        )
          return;
        if (operation.kind === "close") {
          if (!sameDefinition(yield* definition(operation.workstreamId), operation.base)) return;
          const unavailableLinks = new Map(
            (yield* fleet.unavailableCloseLinks(id, yield* now)).map(
              ({ environmentId, reason }) => [environmentId, reason],
            ),
          );
          if (unavailableLinks.size) {
            for (const leg of operation.peers) {
              const reason = unavailableLinks.get(leg.environmentId);
              if (reason !== undefined) yield* fleet.rememberDelivery(leg, "blocked", reason, null);
            }
            yield* bump();
            return;
          }
          if ((yield* unsettledMember(operation.workstreamId)).length) {
            if ((yield* fleet.setStatus(id, "pending", "unsettled_threads")).length) yield* bump();
            return;
          }
          yield* merge(operation.candidate, operation.base ?? undefined, true);
        }
        yield* fleet.setStatus(id, "applied");
        yield* bump();
      }),
    ),
  );
  const snapshot = Effect.fn("PersonalWorkstreams.snapshot")(() =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const autoAssignments = yield* autoRoute.currentAutoAssignments();
          const fleetState = yield* fleetSnapshot;
          const fleetProposals = fleetState?.role === "authority" ? [] : yield* fleet.proposals;
          return {
            environmentId,
            sequence: yield* sequence(),
            workstreams: yield* definitions(),
            assignments: yield* sql.unsafe<P.PersonalWorkstreamAssignment>(
              `SELECT ${assignmentColumns} FROM jones_personal_workstream_assignments a JOIN orchestration_v2_projection_threads t ON t.thread_id=a.thread_id WHERE t.deleted_at IS NULL`,
            ),
            importIssue,
            supportsNameIcons: true as const,
            supportsSnooze: true as const,
            ...(autoAssignments === undefined ? {} : { autoAssignments }),
            ...(fleetState === undefined ? {} : { fleet: fleetState }),
            ...(fleetProposals.length ? { fleetProposals } : {}),
          };
        }),
      )
      .pipe(Effect.mapError(storage)),
  );
  const put = Effect.fn("PersonalWorkstreams.put")(
    (w: P.PersonalWorkstream, expected?: P.PersonalWorkstream) =>
      decodeDefinition(w).pipe(
        Effect.mapError(() => new P.WorkstreamsPersonalError({ code: "invalid_request" })),
        Effect.flatMap((valid) => write(Effect.andThen(requireLegacy, merge(valid, expected)))),
      ),
  );
  const create = Effect.fn("PersonalWorkstreams.create")(
    (input: Omit<P.NewPersonalWorkstreamInput, "orderKey" | "now" | "origin">) =>
      write(
        Effect.gen(function* (): Effect.fn.Return<
          P.PutResult,
          SqlError | P.WorkstreamsPersonalError
        > {
          yield* requireLegacy;
          const old = yield* definition(input.id);
          if (old) {
            if (old.deletedAt !== null)
              return yield* new P.WorkstreamsPersonalError({ code: "workstream_deleted" });
            return {
              outcome: old.name === input.name.trim() ? "already_satisfied" : "superseded",
              workstream: old,
              sequence: yield* sequence(),
            };
          }
          const all = yield* definitions();
          const last =
            all
              .filter((w) => w.deletedAt === null)
              .map((w) => w.orderKey)
              .sort()
              .at(-1) ?? null;
          const key = pinOrderKeyBetween(last, null);
          if (key === null)
            return yield* new P.WorkstreamsPersonalError({ code: "invalid_request" });
          const timestamp = yield* now;
          const candidate = yield* Effect.try({
            try: () =>
              P.newPersonalWorkstream({
                ...input,
                orderKey: key,
                now: timestamp,
                origin: `env:${environmentId}`,
              }),
            catch: () => new P.WorkstreamsPersonalError({ code: "invalid_request" }),
          });
          return yield* merge(candidate);
        }),
      ),
  );
  const assign = Effect.fn("PersonalWorkstreams.assign")(
    (
      input: P.AssignPersonalWorkstreamInput,
      options?: { readonly supersedeInitialAutoAssignment: boolean },
    ) =>
      decodeAssignment(input).pipe(
        Effect.mapError(() => new P.WorkstreamsPersonalError({ code: "invalid_request" })),
        Effect.flatMap((valid) =>
          write(
            Effect.gen(function* (): Effect.fn.Return<
              P.AssignResult,
              SqlError | P.WorkstreamsPersonalError
            > {
              if (!(yield* exists(valid.threadId)))
                return yield* new P.WorkstreamsPersonalError({ code: "thread_not_found" });
              const carried = valid.workstream;
              const local = carried === null ? null : yield* definition(carried.id);
              const fleetManaged = carried !== null && (yield* fleet.binding) !== null;
              const differentLifecycle =
                carried !== null &&
                local !== null &&
                P.isClosedPersonalWorkstream(carried.lifecycle) !==
                  P.isClosedPersonalWorkstream(local.lifecycle);
              const workstream =
                carried === null
                  ? null
                  : fleetManaged ||
                      differentLifecycle ||
                      P.isClosedPersonalWorkstream(carried.lifecycle)
                    ? local
                    : (yield* merge(carried)).workstream;
              if (carried && !workstream)
                return yield* new P.WorkstreamsPersonalError({
                  code: fleetManaged ? "workstream_not_found" : "workstream_closed",
                });
              if (workstream?.deletedAt != null)
                return yield* new P.WorkstreamsPersonalError({ code: "workstream_deleted" });
              const current = yield* assignment(valid.threadId);
              const target = workstream?.id ?? null;
              const supersedesAutomatic =
                options?.supersedeInitialAutoAssignment === true &&
                valid.expectedRevision === 0 &&
                current.revision === 1 &&
                (yield* autoRoute.hasCommittedAssignment(current));
              if (current.workstreamId === target && !supersedesAutomatic)
                return {
                  outcome: "already_satisfied",
                  assignment: current,
                  workstream,
                  sequence: yield* sequence(),
                };
              if (
                workstream &&
                (P.isClosedPersonalWorkstream(workstream.lifecycle) ||
                  (carried && P.isClosedPersonalWorkstream(carried.lifecycle)))
              )
                return yield* new P.WorkstreamsPersonalError({ code: "workstream_closed" });
              if (workstream && fleetManaged && (yield* fleet.openClose(workstream.id)))
                return yield* new P.WorkstreamsPersonalError({ code: "operation_pending" });
              if (valid.expectedRevision !== current.revision && !supersedesAutomatic)
                return yield* new P.WorkstreamsPersonalError({
                  code: "precondition_failed",
                  currentAssignment: current,
                });
              return {
                outcome: "applied",
                assignment: yield* persistAssignment(valid.threadId, target, current.revision + 1),
                workstream,
                sequence: yield* sequence(),
              };
            }),
          ),
        ),
      ),
  );
  const assignAutomatically: PersonalWorkstreams["Service"]["assignAutomatically"] = Effect.fn(
    "PersonalWorkstreams.assignAutomatically",
  )((input) =>
    write(
      Effect.gen(function* () {
        if (!(yield* autoRoute.isAssignableThread(input.threadId)))
          return yield* new P.WorkstreamsPersonalError({ code: "thread_not_found" });
        const current = yield* assignment(input.threadId);
        if (current.revision !== 0 || current.workstreamId !== null)
          return yield* new P.WorkstreamsPersonalError({
            code: "precondition_failed",
            currentAssignment: current,
          });
        const group = yield* definition(input.workstreamId);
        if (
          group === null ||
          group.lifecycle !== "active" ||
          group.name !== input.workstreamName ||
          P.isPersonalWorkstreamSnoozed(group, Date.parse(yield* now)) ||
          (yield* fleet.openClose(input.workstreamId)) !== null
        )
          return yield* new P.WorkstreamsPersonalError({ code: "workstream_not_found" });
        if (group.deletedAt !== null)
          return yield* new P.WorkstreamsPersonalError({ code: "workstream_deleted" });

        // Only our write can create provenance, and both records commit before publication.
        const next = yield* persistAssignment(input.threadId, group.id, 1);
        const recorded = yield* autoRoute.recordAssignment(input, {
          workstreamId: group.id,
          revision: next.revision,
          decidedAt: next.updatedAt,
          confidence: input.confidence,
          basis: input.basis,
        });
        if (!recorded)
          return yield* new P.WorkstreamsPersonalError({ code: "storage_unavailable" });
      }),
    ),
  );
  const readThread = Effect.fn("PersonalWorkstreams.readThread")((id: ThreadId) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          if (!(yield* exists(id)))
            return yield* new P.WorkstreamsPersonalError({ code: "thread_not_found" });
          const current = yield* assignment(id);
          const decision = (yield* autoRoute.available) ? yield* autoRoute.latest(id) : undefined;
          return {
            assignment: current,
            workstream:
              current.workstreamId === null ? null : yield* definition(current.workstreamId),
            ...(decision === undefined ? {} : { autoRoute: decision }),
          };
        }),
      )
      .pipe(Effect.mapError(storage)),
  );

  const importStaged = Effect.gen(function* () {
    const path = `${config.baseDir}/workstreams/personal-import.v1.json`;
    if (!(yield* fs.exists(path))) return;
    const bytes = yield* fs.readFile(path);
    const file = yield* decodeImport(new TextDecoder().decode(bytes));
    const digest = Hex.encode(yield* digester.digest(Digest.sha256, bytes));
    const issue = (reason: P.PersonalWorkstreamsImportIssue["reason"]) =>
      Effect.fail({ importId: file.importId, reason });
    yield* write(
      Effect.gen(function* () {
        const markers = yield* sql<{
          value: string;
        }>`SELECT value FROM jones_personal_workstream_meta WHERE key=${`import:${file.importId}`}`;
        if (markers[0]) {
          if (markers[0].value !== digest) return yield* issue("content_mismatch");
          return;
        }
        if (file.targetEnvironmentId !== environmentId) return yield* issue("wrong_environment");
        if (yield* fleet.binding) return yield* issue("conflict");
        let changed = false;
        for (const w of file.workstreams) {
          const current = yield* definition(w.id);
          if (
            current &&
            P.canonicalPersonalWorkstream(current) !== P.canonicalPersonalWorkstream(w)
          )
            return yield* issue("conflict");
          if (!current) {
            yield* store(w);
            changed = true;
          }
        }
        for (const a of file.assignments) {
          if (!(yield* exists(a.threadId))) return yield* issue("thread_not_found");
          const w = yield* definition(a.workstreamId);
          if (!w || w.deletedAt !== null) return yield* issue("conflict");
          const current = yield* assignment(a.threadId);
          if (current.revision > 0 && current.workstreamId !== a.workstreamId)
            return yield* issue("conflict");
          if (current.revision === 0) {
            yield* sql`INSERT INTO jones_personal_workstream_assignments(thread_id,workstream_id,revision,updated_at) VALUES(${a.threadId},${a.workstreamId},1,${yield* now})`;
            changed = true;
          }
        }
        yield* sql`INSERT INTO jones_personal_workstream_meta(key,value) VALUES(${`import:${file.importId}`},${digest})`;
        if (changed) yield* bump();
      }).pipe(
        Effect.catch((error) => {
          if (typeof error === "object" && error !== null && "reason" in error)
            importIssue = error as P.PersonalWorkstreamsImportIssue;
          return Effect.fail(error);
        }),
      ),
    );
  });
  yield* importStaged.pipe(
    Effect.catch(() => {
      importIssue ??= { importId: null, reason: "invalid_file" };
      return Effect.void;
    }),
  );
  return PersonalWorkstreams.of({
    snapshot,
    members,
    create,
    put,
    designateFleet,
    requestOperation,
    requestAgentOperation: Effect.fn("PersonalWorkstreams.requestAgentOperation")(function* (
      input: Omit<P.RequestPersonalWorkstreamOperation, "requestId">,
    ) {
      const requestId = yield* fleetRequestId("agent", input).pipe(Effect.mapError(storage));
      return yield* requestOperation({ ...input, requestId });
    }),
    cancelOperation,
    resolveOperation,
    approveFollower,
    releaseFollower,
    proposeFollower,
    readPeerDefinition,
    readPeerDefinitions,
    applyPeerDefinition,
    changeRoster,
    pendingOperations: fleet.active.pipe(
      Effect.map((operations) => operations.filter((operation) => operation.status === "pending")),
      Effect.mapError(storage),
    ),
    currentFleetMembers: sql
      .withTransaction(
        Effect.gen(function* () {
          const binding = yield* fleet.binding;
          return binding?.authorityEnvironmentId === environmentId &&
            binding.authoritySessionId === null
            ? {
                authorityId: environmentId,
                revision: binding.rosterRevision,
                members: yield* fleet.members,
              }
            : null;
        }),
      )
      .pipe(Effect.mapError(storage)),
    beginDelivery,
    knownPeerBase: Effect.fn("PersonalWorkstreams.knownPeerBase")(
      (
        operation: StoredWorkstreamOperation,
        peer: WorkstreamDeliveryRow,
        workstream: P.PersonalWorkstream | null,
      ) => {
        if (!workstream) return Effect.succeed(true);
        const json = canonicalJson(workstream);
        return peer.approvedBaseJson === json
          ? Effect.succeed(true)
          : fleet.knownCopy(workstream.id, operation.ordinal, json).pipe(Effect.mapError(storage));
      },
    ),
    recordDelivery,
    recordPeerDiscovery,
    finalizeOperation,
    assign,
    assignAutomatically,
    readThread,
    // Subscription acquisition precedes even the first consistent snapshot read.
    changes: Stream.unwrap(
      Effect.gen(function* () {
        const sub = yield* PubSub.subscribe(invalidations);
        return Stream.concat(
          Stream.fromEffect(snapshot()),
          Stream.fromSubscription(sub).pipe(Stream.mapEffect(() => snapshot())),
        );
      }),
    ),
    readDefinitions: Effect.fn("PersonalWorkstreams.readDefinitions")((ids?: readonly string[]) =>
      definitions().pipe(
        Effect.map((rows) => (ids === undefined ? rows : rows.filter((w) => ids.includes(w.id)))),
        Effect.mapError(storage),
      ),
    ),
    counts: Effect.fn("PersonalWorkstreams.counts")(function* () {
      const s = yield* snapshot();
      const live = s.workstreams.filter((w) => w.deletedAt === null);
      return {
        observedAt: yield* now,
        sequence: s.sequence,
        total: live.length,
        active: live.filter((w) => w.lifecycle === "active").length,
      };
    }),
  });
});
export const layer = Layer.effect(PersonalWorkstreams, make);
