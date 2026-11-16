// @effect-diagnostics nodeBuiltinImport:off -- Receiver-local IDs and canonical command fingerprints.
import * as NodeCrypto from "node:crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import type { RemoteAdmissionRecord } from "@t3tools/contracts";
import {
  RemoteAdmissionAccessBinding,
  type RemoteAdmissionBlockedReason,
} from "@t3tools/contracts/jones/remoteAdmissionReview";
import { SqlError, UnknownError } from "effect/sql/SqlError";
import { canonicalJsonStrict } from "../remoteDelegation/shared.ts";

export class ReceiverAccessConflict extends Schema.TaggedError<ReceiverAccessConflict, { readonly brand: unique symbol }>()(
  "ReceiverAccessConflict",
  { current: Schema.Unknown },
) {}
export class ReceiverAccessUnavailable extends Schema.TaggedError<ReceiverAccessUnavailable, { readonly brand: unique symbol }>()(
  "ReceiverAccessUnavailable",
  {},
) {}
export type BlockReason = RemoteAdmissionBlockedReason;
export type AccessBinding = RemoteAdmissionAccessBinding;
export interface Grant {
  readonly grant_id: string;
  readonly admission_id: string;
  readonly binding_json: string;
  readonly managed_scope_id: string | null;
  readonly destination_thread_id: string | null;
  readonly session_id: string;
  readonly state: "pending" | "active" | "revoked";
  readonly generation: number;
  readonly expires_at: string;
  readonly created_at: string;
  readonly updated_at: string;
  readonly revoked_at: string | null;
}
interface Scope {
  readonly managed_scope_id: string;
  readonly revision: number;
}
export const withinAccessCeiling = (ceiling: AccessBinding, requested: AccessBinding): boolean => {
  const runtime: Readonly<Record<string, number>> = {
    "approval-required": 0,
    "auto-accept-edits": 1,
    auto: 2,
    "full-access": 3,
  };
  const interaction: Readonly<Record<string, number>> = { plan: 0, default: 1 };
  const cr = runtime[ceiling.runtimeMode],
    rr = runtime[requested.runtimeMode];
  const ci = interaction[ceiling.interactionMode],
    ri = interaction[requested.interactionMode];
  return (
    cr !== undefined &&
    rr !== undefined &&
    ci !== undefined &&
    ri !== undefined &&
    rr <= cr &&
    ri <= ci
  );
};
const ACCESS_DURATIONS = [3600, 86400, 172800, 259200, 604800] as const;
const fingerprint = (value: unknown) =>
  NodeCrypto.createHash("sha256").update(canonicalJsonStrict(value)).digest("hex");
const expiry = (now: string, duration: number) =>
  DateTime.formatIso(DateTime.add(DateTime.makeUnsafe(now), { milliseconds: duration * 1000 }));
const validDuration = (duration: number) =>
  (ACCESS_DURATIONS as readonly number[]).includes(duration);
const decodeStoredBinding = Effect.fn("ReceiverAccess.decodeStoredBinding")((json: string) =>
  Schema.decodeEffect(Schema.fromJsonString(RemoteAdmissionAccessBinding))(json).pipe(
    Effect.mapError((cause) => new SqlError({ reason: new UnknownError({ cause }) })),
  ),
);

// These operations share the caller's SqlClient transaction. Authority and settings
// callbacks perform database reads only; the settings semaphore must already be held.
export const makeReceiverAccess = (
  sql: SqlClient.SqlClient,
  invalidate: Effect.Effect<void> = Effect.void,
) => {
  const bump = Effect.fn("ReceiverAccess.bump")(function* (
    kind: string,
    id: string,
    now: string,
    sessionId?: string,
  ) {
    yield* sql`UPDATE jones_remote_admission_review_state SET revision=revision+1 WHERE singleton=1`;
    yield* sql`INSERT INTO jones_remote_admission_changes(revision,kind,entity_id,created_at)
      SELECT revision,${kind},${id},${now} FROM jones_remote_admission_review_state WHERE singleton=1`;
    if (sessionId)
      yield* sql`INSERT INTO jones_remote_admission_peer_revisions(session_id,revision)
      VALUES(${sessionId},1) ON CONFLICT(session_id) DO UPDATE SET revision=revision+1`;
    yield* invalidate;
  });
  const getGrant = Effect.fn("ReceiverAccess.getGrant")(function* (id: string) {
    return (
      (yield* sql<Grant>`SELECT * FROM jones_remote_admission_grants WHERE grant_id=${id}`)[0] ??
      null
    );
  });
  const findScope = Effect.fn("ReceiverAccess.findScope")(function* (binding: AccessBinding) {
    return (
      (yield* sql<Scope>`SELECT managed_scope_id,revision FROM jones_remote_managed_scopes
      WHERE receiver_environment_id=${binding.receiverEnvironmentId}
      AND origin_environment_id=${binding.originEnvironmentId} AND origin_thread_id=${binding.originThreadId}
      AND destination_thread_id=${binding.destinationThreadId} AND operation_class='messages'`)[0] ??
      null
    );
  });
  const establish = Effect.fn("ReceiverAccess.establish")(function* (
    binding: AccessBinding,
    admissionId: string,
  ) {
    if (!binding.destinationThreadId) return yield* new ReceiverAccessUnavailable({});
    const id = NodeCrypto.randomUUID();
    yield* sql`INSERT INTO jones_remote_managed_scopes(managed_scope_id,receiver_environment_id,
      origin_environment_id,origin_thread_id,destination_thread_id,operation_class,revision,
      provenance_session_id,establishing_admission_id) VALUES(${id},${binding.receiverEnvironmentId},
      ${binding.originEnvironmentId},${binding.originThreadId},${binding.destinationThreadId},'messages',1,
      ${binding.sessionId},${admissionId}) ON CONFLICT(receiver_environment_id,origin_environment_id,
      origin_thread_id,destination_thread_id,operation_class) DO UPDATE SET revision=revision+1`;
    return (yield* findScope(binding))!;
  });
  const block = Effect.fn("ReceiverAccess.block")(function* (
    admissionId: string,
    scopeId: string,
    reason: BlockReason | "malicious",
    now: string,
    repeat = false,
    ordinary = false,
  ) {
    if (!ordinary)
      yield* sql`INSERT INTO jones_remote_admission_blocked_marks(admission_id,managed_scope_id,reason,created_at)
      VALUES(${admissionId},${scopeId},${reason},${now}) ON CONFLICT(admission_id) DO UPDATE SET reason=excluded.reason`;
    const keys = yield* sql<{
      folded: number;
    }>`SELECT folded FROM jones_remote_admission_blocked_event_keys
      WHERE admission_id=${admissionId} AND reason=${reason}`;
    yield* sql`INSERT INTO jones_remote_admission_blocked_event_keys(admission_id,reason,managed_scope_id,last_at)
      VALUES(${admissionId},${reason},${scopeId},${now}) ON CONFLICT(admission_id,reason) DO UPDATE SET last_at=excluded.last_at`;
    if (keys[0]?.folded) {
      yield* sql`UPDATE jones_remote_admission_blocked_summaries SET repeat_arrivals=repeat_arrivals+${repeat ? 1 : 0},
        last_at=MAX(last_at,${now}) WHERE managed_scope_id=${scopeId} AND reason=${reason}`;
      yield* bump("blocked", admissionId, now);
      return;
    }
    yield* sql`INSERT INTO jones_remote_admission_blocked_events(event_id,admission_id,managed_scope_id,reason,first_at,last_at,repeat_count)
      VALUES(${NodeCrypto.randomUUID()},${admissionId},${scopeId},${reason},${now},${now},0)
      ON CONFLICT(admission_id,reason) DO UPDATE SET last_at=excluded.last_at,repeat_count=repeat_count+${repeat ? 1 : 0}`;
    const overflow = yield* sql<{
      event_id: string;
      reason: string;
      first_at: string;
      last_at: string;
      repeat_count: number;
    }>`
      SELECT * FROM jones_remote_admission_blocked_events WHERE managed_scope_id=${scopeId}
      ORDER BY last_at DESC,event_id DESC LIMIT -1 OFFSET 100`;
    for (const event of overflow) {
      yield* sql`INSERT INTO jones_remote_admission_blocked_summaries
        (managed_scope_id,reason,logical_events,repeat_arrivals,first_at,last_at)
        VALUES(${scopeId},${event.reason},1,${event.repeat_count},${event.first_at},${event.last_at})
        ON CONFLICT(managed_scope_id,reason) DO UPDATE SET logical_events=logical_events+1,
        repeat_arrivals=repeat_arrivals+excluded.repeat_arrivals,
        first_at=MIN(first_at,excluded.first_at),last_at=MAX(last_at,excluded.last_at)`;
      yield* sql`UPDATE jones_remote_admission_blocked_event_keys SET folded=1 WHERE admission_id=
        (SELECT admission_id FROM jones_remote_admission_blocked_events WHERE event_id=${event.event_id}) AND reason=${event.reason}`;
      yield* sql`DELETE FROM jones_remote_admission_blocked_events WHERE event_id=${event.event_id}`;
    }
    yield* bump("blocked", admissionId, now);
  });
  const createGrant = Effect.fn("ReceiverAccess.createGrant")(function* (input: {
    admissionId: string;
    binding: AccessBinding;
    durationSeconds: number;
    now: string;
  }) {
    if (!validDuration(input.durationSeconds)) return yield* new ReceiverAccessUnavailable({});
    const existing =
      yield* sql<Grant>`SELECT * FROM jones_remote_admission_grants WHERE admission_id=${input.admissionId}`;
    if (existing[0]) return existing[0];
    const scope = input.binding.destinationThreadId
      ? yield* establish(input.binding, input.admissionId)
      : null;
    const id = NodeCrypto.randomUUID();
    yield* sql`INSERT INTO jones_remote_admission_grants(grant_id,admission_id,binding_json,managed_scope_id,
      destination_thread_id,session_id,state,generation,expires_at,created_at,updated_at)
      VALUES(${id},${input.admissionId},${canonicalJsonStrict(input.binding)},${scope?.managed_scope_id ?? null},
      ${input.binding.destinationThreadId},${input.binding.sessionId},${scope ? "active" : "pending"},1,
      ${expiry(input.now, input.durationSeconds)},${input.now},${input.now})`;
    yield* bump("grant", id, input.now, input.binding.sessionId);
    return (yield* getGrant(id))!;
  });
  const mutateGrant = Effect.fn("ReceiverAccess.mutateGrant")(function* (input: {
    grantId: string;
    expectedGeneration: number;
    action: "revoke" | "update";
    durationSeconds?: number;
    now: string;
  }) {
    const grant = yield* getGrant(input.grantId);
    if (!grant) return yield* new ReceiverAccessUnavailable({});
    if (grant.generation !== input.expectedGeneration || grant.state === "revoked")
      return yield* new ReceiverAccessConflict({ current: grant });
    if (
      input.action === "update" &&
      (!grant.destination_thread_id ||
        input.durationSeconds === undefined ||
        !validDuration(input.durationSeconds))
    )
      return yield* new ReceiverAccessUnavailable({});
    yield* sql`UPDATE jones_remote_admission_grants SET generation=generation+1,updated_at=${input.now},
      state=${input.action === "revoke" ? "revoked" : grant.state},
      expires_at=${input.action === "update" ? expiry(input.now, input.durationSeconds!) : grant.expires_at},
      revoked_at=${input.action === "revoke" ? input.now : null}
      WHERE grant_id=${input.grantId} AND generation=${input.expectedGeneration} AND state!='revoked'`;
    if (input.action === "revoke" && grant.managed_scope_id)
      yield* sql`UPDATE jones_remote_managed_scopes
      SET revision=revision+1 WHERE managed_scope_id=${grant.managed_scope_id}`;
    yield* bump("grant", input.grantId, input.now, grant.session_id);
    return (yield* getGrant(input.grantId))!;
  });
  const restore = Effect.fn("ReceiverAccess.restore")(function* (input: {
    predecessorId: string;
    expectedGeneration: number;
    commandKey: string;
    durationSeconds: number;
    binding: AccessBinding;
    now: string;
    authorize: (binding: AccessBinding) => Effect.Effect<boolean, ReceiverAccessUnavailable>;
  }) {
    const predecessor = yield* getGrant(input.predecessorId);
    if (!predecessor) return yield* new ReceiverAccessUnavailable({});
    const fp = fingerprint({
      predecessorId: input.predecessorId,
      generation: input.expectedGeneration,
      durationSeconds: input.durationSeconds,
      binding: input.binding,
    });
    const rows = yield* sql<{
      command_key: string;
      fingerprint: string;
      successor_grant_id: string;
    }>`
      SELECT * FROM jones_remote_admission_grant_restores WHERE predecessor_grant_id=${input.predecessorId}`;
    if (rows[0]) {
      const successor = yield* getGrant(rows[0].successor_grant_id);
      if (!successor) return yield* new ReceiverAccessUnavailable({});
      if (rows[0].command_key === input.commandKey && rows[0].fingerprint === fp) return successor;
      let head = successor;
      for (;;) {
        const chain = yield* sql<{
          successor_grant_id: string;
        }>`SELECT successor_grant_id FROM jones_remote_admission_grant_restores WHERE predecessor_grant_id=${head.grant_id}`;
        if (!chain[0]) break;
        const next = yield* getGrant(chain[0].successor_grant_id);
        if (!next) return yield* new ReceiverAccessUnavailable({});
        head = next;
      }
      return yield* new ReceiverAccessConflict({ current: head });
    }
    if (
      predecessor.state !== "revoked" ||
      predecessor.generation !== input.expectedGeneration ||
      !predecessor.destination_thread_id ||
      predecessor.binding_json !== canonicalJsonStrict(input.binding)
    )
      return yield* new ReceiverAccessConflict({ current: predecessor });
    if (!validDuration(input.durationSeconds) || !(yield* input.authorize(input.binding)))
      return yield* new ReceiverAccessUnavailable({});
    const id = NodeCrypto.randomUUID();
    yield* sql`INSERT INTO jones_remote_admission_grants(grant_id,admission_id,binding_json,managed_scope_id,
      destination_thread_id,session_id,state,generation,expires_at,created_at,updated_at)
      VALUES(${id},${`restore:${id}`},${predecessor.binding_json},${predecessor.managed_scope_id},
      ${predecessor.destination_thread_id},${predecessor.session_id},'active',1,
      ${expiry(input.now, input.durationSeconds)},${input.now},${input.now})`;
    yield* sql`INSERT INTO jones_remote_admission_grant_restores VALUES(${predecessor.grant_id},
      ${predecessor.generation},${id},${input.commandKey},${fp},${input.now})`;
    yield* bump("restore", id, input.now, predecessor.session_id);
    return (yield* getGrant(id))!;
  });
  const activate = Effect.fn("ReceiverAccess.activate")(function* (input: {
    admissionId: string;
    expectedGeneration: number;
    destinationThreadId: string;
    now: string;
  }) {
    const rows =
      yield* sql<Grant>`SELECT * FROM jones_remote_admission_grants WHERE admission_id=${input.admissionId}`;
    const grant = rows[0];
    if (!grant) return null;
    const binding = {
      ...(yield* decodeStoredBinding(grant.binding_json)),
      destinationThreadId: input.destinationThreadId,
    };
    // Exact creation evidence may establish restriction after revocation, never a lease.
    if (grant.destination_thread_id && grant.destination_thread_id !== input.destinationThreadId)
      return yield* new ReceiverAccessConflict({ current: grant });
    if (!grant.destination_thread_id) {
      const scope = yield* establish(binding, input.admissionId);
      yield* sql`UPDATE jones_remote_admission_grants SET managed_scope_id=${scope.managed_scope_id},
        destination_thread_id=${input.destinationThreadId},binding_json=${canonicalJsonStrict(binding)}
        WHERE grant_id=${grant.grant_id} AND destination_thread_id IS NULL`;
    }
    yield* sql`UPDATE jones_remote_admission_grants SET state='active',updated_at=${input.now}
      WHERE grant_id=${grant.grant_id} AND state='pending' AND generation=${input.expectedGeneration}`;
    yield* bump("grant", grant.grant_id, input.now, grant.session_id);
    return yield* getGrant(grant.grant_id);
  });
  const permit = Effect.fn("ReceiverAccess.permit")(function* (record: RemoteAdmissionRecord) {
    const marks = yield* sql<{
      managed_scope_id: string;
    }>`SELECT managed_scope_id FROM jones_remote_admission_blocked_marks WHERE admission_id=${record.id}`;
    const grants =
      yield* sql<Grant>`SELECT * FROM jones_remote_admission_grants WHERE admission_id=${record.id}`;
    const extensions = yield* sql<{
      binding_json: string | null;
    }>`SELECT binding_json FROM jones_remote_admission_extensions WHERE admission_id=${record.id}`;
    const bindingScope = extensions[0]?.binding_json
      ? yield* findScope(yield* decodeStoredBinding(extensions[0].binding_json))
      : null;
    const scopeId =
      marks[0]?.managed_scope_id ?? grants[0]?.managed_scope_id ?? bindingScope?.managed_scope_id;
    if (!scopeId) return;
    const scopes =
      yield* sql<Scope>`SELECT managed_scope_id,revision FROM jones_remote_managed_scopes WHERE managed_scope_id=${scopeId}`;
    if (!scopes[0]) return yield* new ReceiverAccessUnavailable({});
    yield* sql`INSERT INTO jones_remote_admission_access_basis(admission_id,managed_scope_id,kind,binding_digest,scope_revision)
      VALUES(${record.id},${scopes[0].managed_scope_id},'permit',${record.bindingDigest},${scopes[0].revision})
      ON CONFLICT(admission_id) DO UPDATE SET kind='permit',grant_id=NULL,generation=NULL,
      binding_digest=excluded.binding_digest,scope_revision=excluded.scope_revision`;
  });
  const enforce = Effect.fn("ReceiverAccess.enforce")(function* (input: {
    record: RemoteAdmissionRecord;
    binding: AccessBinding | null;
    now: string;
    modeAllowed: (grant: AccessBinding, requested: AccessBinding) => boolean;
    phase: "intake" | "claim";
    malicious?: boolean;
  }) {
    const { record, binding } = input;
    const destination = binding?.destinationThreadId ?? record.envelope.target.threadId;
    const scopes =
      yield* sql<Scope>`SELECT managed_scope_id,revision FROM jones_remote_managed_scopes
      WHERE destination_thread_id=${destination} ORDER BY managed_scope_id`;
    const scope = binding ? yield* findScope(binding) : (scopes[0] ?? null);
    // A known but unrelated asserted origin is unmanaged; missing identity never is.
    if (!scope) return { managed: false, authorized: true };
    if (input.phase === "intake") {
      const blocked =
        yield* sql`SELECT admission_id FROM jones_remote_admission_blocked_marks WHERE admission_id=${record.id}`;
      if (blocked[0]) return { managed: true, authorized: false };
    }
    let reason: BlockReason = "access_revoked";
    const basis = yield* sql<{
      kind: string;
      grant_id: string | null;
      generation: number | null;
      binding_digest: string | null;
      scope_revision: number | null;
    }>`
      SELECT * FROM jones_remote_admission_access_basis WHERE admission_id=${record.id} AND managed_scope_id=${scope.managed_scope_id}`;
    if (
      input.phase === "claim" &&
      basis[0]?.kind === "permit" &&
      basis[0].binding_digest === record.bindingDigest &&
      basis[0].scope_revision === scope.revision
    )
      return { managed: true, authorized: true };
    const grants = yield* sql<Grant>`SELECT * FROM jones_remote_admission_grants
      WHERE managed_scope_id=${scope.managed_scope_id} ORDER BY created_at DESC,grant_id DESC`;
    const grant =
      input.phase === "claim"
        ? grants.find(
            (g) => g.grant_id === basis[0]?.grant_id && g.generation === basis[0]?.generation,
          )
        : grants.find(
            (g) =>
              g.session_id === binding?.sessionId &&
              g.state === "active" &&
              g.expires_at > input.now,
          );
    if (!binding) reason = "identity_unresolved";
    else if (record.envelope.operation !== "t3_thread_send") reason = "operation_not_covered";
    else if (!grants.some((g) => g.session_id === binding.sessionId)) reason = "session_not_bound";
    else if (grant?.state === "active" && grant.expires_at <= input.now) reason = "access_expired";
    else if (
      !grant &&
      grants.some(
        (g) =>
          g.session_id === binding.sessionId && g.state === "active" && g.expires_at <= input.now,
      )
    )
      reason = "access_expired";
    else if (grant?.state === "active" && grant.expires_at > input.now) {
      const ceiling = yield* decodeStoredBinding(grant.binding_json);
      if (!input.modeAllowed(ceiling, binding)) reason = "mode_exceeds";
      else if (
        grant.session_id === binding.sessionId &&
        ceiling.receiverEnvironmentId === binding.receiverEnvironmentId &&
        ceiling.originEnvironmentId === binding.originEnvironmentId &&
        ceiling.originThreadId === binding.originThreadId &&
        ceiling.destinationThreadId === binding.destinationThreadId &&
        !input.malicious
      ) {
        if (input.phase === "intake")
          yield* sql`INSERT INTO jones_remote_admission_access_basis
          (admission_id,managed_scope_id,kind,grant_id,generation) VALUES(${record.id},${scope.managed_scope_id},'lease',${grant.grant_id},${grant.generation})
          ON CONFLICT(admission_id) DO NOTHING`;
        return { managed: true, authorized: true };
      }
    }
    if (input.malicious) {
      yield* block(record.id, scope.managed_scope_id, "malicious", input.now, false, true);
      return { managed: true, authorized: false };
    }
    yield* sql`UPDATE jones_remote_admissions SET status='held',category='out_of_scope',
      reason=${`Managed access requires owner review: ${reason}.`},updated_at=${input.now}
      WHERE id=${record.id} AND status IN ('evaluating','held','passed','approved')`;
    yield* sql`DELETE FROM jones_remote_admission_access_basis WHERE admission_id=${record.id}`;
    yield* block(record.id, scope.managed_scope_id, reason, input.now);
    return { managed: true, authorized: false };
  });
  const grantView = Effect.fn("ReceiverAccess.grantView")(function* (grant: Grant, now: string) {
    const lineage = yield* sql<{
      predecessor_grant_id: string;
    }>`SELECT predecessor_grant_id FROM jones_remote_admission_grant_restores WHERE successor_grant_id=${grant.grant_id}`;
    return {
      grantId: grant.grant_id,
      sourceAdmissionId: grant.admission_id,
      binding: yield* decodeStoredBinding(grant.binding_json),
      state: grant.state,
      generation: grant.generation,
      expiresAt: grant.expires_at,
      createdAt: grant.created_at,
      updatedAt: grant.updated_at,
      revokedAt: grant.revoked_at,
      predecessorGrantId: lineage[0]?.predecessor_grant_id ?? null,
      serverNow: now,
    };
  });
  const listGrants = Effect.fn("ReceiverAccess.listGrants")(function* (sessionId?: string) {
    return yield* sql<Grant>`SELECT * FROM jones_remote_admission_grants
      WHERE (${sessionId ?? null} IS NULL OR session_id=${sessionId ?? null}) ORDER BY created_at DESC,grant_id DESC`;
  });
  return {
    invalidate,
    grantView,
    listGrants,
    bumpInTransaction: bump,
    getGrant,
    findScope,
    blockInTransaction: block,
    createGrantInTransaction: createGrant,
    mutateGrant: (input: Parameters<typeof mutateGrant>[0]) =>
      sql.withTransaction(mutateGrant(input)),
    restore: (input: Parameters<typeof restore>[0]) => sql.withTransaction(restore(input)),
    activateInTransaction: activate,
    permitInTransaction: permit,
    enforceInTransaction: enforce,
  };
};
