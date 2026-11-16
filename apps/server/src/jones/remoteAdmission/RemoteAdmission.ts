import * as ReviewChanges from "./ReviewChanges.ts";
import { RemoteAdmissionAccessBinding } from "@t3tools/contracts/jones/remoteAdmissionReview";
import { makeReviewRetention } from "./ReviewRetention.ts";
import {
  makeReceiverAccess,
  withinAccessCeiling,
  type AccessBinding,
  ReceiverAccessConflict,
  ReceiverAccessUnavailable,
} from "./ReceiverAccess.ts";
import { makeReviewProjection } from "./ReviewProjection.ts";
// @effect-diagnostics nodeBuiltinImport:off -- Canonical binding hashes and opaque IDs are synchronous server-local primitives.
import { admissionOperation } from "../threadResultDependencies/RemoteRequestProtocol.ts";
import { makeDependencyStore } from "../threadResultDependencies/DependencyStore.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import * as DependencyChanges from "../threadResultDependencies/DependencyChanges.ts";
import { canonicalJsonStrict as canonicalJson } from "../remoteDelegation/shared.ts";
import * as Option from "effect/Option";
import * as NodeCrypto from "node:crypto";
import {
  RemoteAdmissionEnvelope,
  RemoteAdmissionEvaluation,
  RemoteAdmissionRecord,
  RemoteAdmissionRequest,
  RemoteAdmissionReceiverPolicy,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import {
  classifyEvaluationFailure,
  evaluationDiagnostic,
  REMOTE_ADMISSION_EVALUATION_TIMEOUT_MS,
  type EvaluationFailureKind,
} from "./evaluationDiagnostic.ts";

export class RemoteAdmissionInvalidRequest extends Schema.TaggedError<
  RemoteAdmissionInvalidRequest,
  { readonly brand: unique symbol }
>()("RemoteAdmissionInvalidRequest", { cause: Schema.Defect() }) {}
export class RemoteAdmissionPersistenceError extends Schema.TaggedError<
  RemoteAdmissionPersistenceError,
  { readonly brand: unique symbol }
>()("RemoteAdmissionPersistenceError", { cause: Schema.Defect() }) {}
export class RemoteAdmissionKeyConflict extends Schema.TaggedError<
  RemoteAdmissionKeyConflict,
  { readonly brand: unique symbol }
>()("RemoteAdmissionKeyConflict", {}) {}
export class RemoteAdmissionNotFound extends Schema.TaggedError<
  RemoteAdmissionNotFound,
  { readonly brand: unique symbol }
>()("RemoteAdmissionNotFound", {}) {}
export class RemoteAdmissionBindingMismatch extends Schema.TaggedError<
  RemoteAdmissionBindingMismatch,
  { readonly brand: unique symbol }
>()("RemoteAdmissionBindingMismatch", {}) {}
export class RemoteAdmissionForbidden extends Schema.TaggedError<
  RemoteAdmissionForbidden,
  { readonly brand: unique symbol }
>()("RemoteAdmissionForbidden", {}) {}
export class RemoteAdmissionStateConflict extends Schema.TaggedError<
  RemoteAdmissionStateConflict,
  { readonly brand: unique symbol }
>()("RemoteAdmissionStateConflict", { status: Schema.String }) {}
export class RemoteAdmissionPolicyChanged extends Schema.TaggedError<
  RemoteAdmissionPolicyChanged,
  { readonly brand: unique symbol }
>()("RemoteAdmissionPolicyChanged", {}) {}

export type AdmissionError =
  | ReceiverAccessConflict
  | ReceiverAccessUnavailable
  | RemoteAdmissionInvalidRequest
  | RemoteAdmissionPersistenceError
  | RemoteAdmissionKeyConflict
  | RemoteAdmissionNotFound
  | RemoteAdmissionBindingMismatch
  | RemoteAdmissionForbidden
  | RemoteAdmissionStateConflict
  | RemoteAdmissionPolicyChanged;
type BoundRequest = { readonly id: string; readonly bindingDigest: string };

// The authenticated transport constructs this classification from its own session principal.
// Never decode it from request JSON or accept a sender-provided owner boolean.
export interface RemoteAdmissionPrincipal {
  readonly kind: "human-owner" | "agent" | "mcp" | "link";
  readonly subject: string;
}
export type RemoteAdmissionEvaluator<E = never> = (
  envelope: RemoteAdmissionEnvelope,
) => Effect.Effect<unknown, E>;

export class RemoteAdmission extends Context.Service<
  RemoteAdmission,
  {
    readonly retention: ReturnType<typeof makeReviewRetention>;
    readonly access: ReturnType<typeof makeReceiverAccess>;
    readonly review: ReturnType<typeof makeReviewProjection>;
    readonly request: (
      input: RemoteAdmissionRequest & { readonly accessBinding?: AccessBinding },
    ) => Effect.Effect<RemoteAdmissionRecord, AdmissionError>;
    readonly get: (id: string) => Effect.Effect<RemoteAdmissionRecord | null, AdmissionError>;
    readonly listPending: () => Effect.Effect<ReadonlyArray<RemoteAdmissionRecord>, AdmissionError>;
    readonly resolve: (
      input: BoundRequest & {
        readonly decision: "approve" | "deny";
        readonly principal: RemoteAdmissionPrincipal;
        readonly durationSeconds?: number;
      },
    ) => Effect.Effect<RemoteAdmissionRecord, AdmissionError>;
    readonly withdraw: (
      input: BoundRequest & {
        readonly clientId: string;
      },
    ) => Effect.Effect<RemoteAdmissionRecord, AdmissionError>;
    readonly claimDispatch: (
      input: BoundRequest & {
        readonly commandId: string;
        readonly currentPolicy: RemoteAdmissionReceiverPolicy;
        readonly checkCurrentAuthority?: (
          record: RemoteAdmissionRecord,
        ) => Effect.Effect<
          { readonly currentPolicy: RemoteAdmissionReceiverPolicy; readonly authorized: boolean },
          AdmissionError
        >;
      },
    ) => Effect.Effect<
      { readonly record: RemoteAdmissionRecord; readonly claimed: boolean },
      AdmissionError
    >;
    readonly markDispatched: (
      input: BoundRequest & {
        readonly commandId: string;
        readonly commitReceiptInTransaction?: () => Effect.Effect<void, AdmissionError>;
      },
    ) => Effect.Effect<RemoteAdmissionRecord, AdmissionError>;
  }
>()("t3/jones/remoteAdmission/RemoteAdmission") {}

// Digests use the strict canonical form: input JSON cannot represent is rejected, not dropped.
const digest = (value: unknown) =>
  NodeCrypto.createHash("sha256").update(canonicalJson(value)).digest("hex");
const nowIso = DateTime.now.pipe(Effect.map(DateTime.formatIso));
const identity = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const validateIdentity = (value: string) =>
  Schema.decodeEffect(identity)(value).pipe(
    Effect.mapError((cause) => new RemoteAdmissionInvalidRequest({ cause })),
  );

interface Row {
  readonly id: string;
  readonly idempotency_key: string;
  readonly request_digest: string;
  readonly binding_digest: string;
  readonly envelope_json: string;
  readonly status: string;
  readonly reason: string | null;
  readonly category: string | null;
  readonly command_id: string | null;
  readonly resolved_by: string | null;
  readonly resolved_at: string | null;
  readonly created_at: string;
  readonly updated_at: string;
}
const decodeRemoteAdmissionEnvelopeJson = Schema.decodeEffect(
  Schema.fromJsonString(RemoteAdmissionEnvelope),
);
const decodeRemoteAdmissionRecord = Schema.decodeUnknownEffect(RemoteAdmissionRecord);

const decodeRow = Effect.fn("RemoteAdmission.decodeRow")(
  function* (row: Row) {
    const envelope = yield* decodeRemoteAdmissionEnvelopeJson(row.envelope_json);
    const record = yield* decodeRemoteAdmissionRecord({
      id: row.id,
      idempotencyKey: row.idempotency_key,
      requestDigest: row.request_digest,
      bindingDigest: row.binding_digest,
      envelope,
      status: row.status,
      reason: row.reason,
      category: row.category,
      commandId: row.command_id,
      resolvedBy: row.resolved_by,
      resolvedAt: row.resolved_at,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    });
    if (digest({ version: 1, envelope }) !== record.bindingDigest) {
      return yield* new RemoteAdmissionBindingMismatch({});
    }
    return record;
  },
  Effect.mapError((cause) => new RemoteAdmissionPersistenceError({ cause })),
);
const decodeRemoteAdmissionRequest = Schema.decodeEffect(RemoteAdmissionRequest);
const decodeRemoteAdmissionReceiverPolicy = Schema.decodeEffect(RemoteAdmissionReceiverPolicy);
const decodeStoredAccessBinding = Effect.fn("RemoteAdmission.decodeStoredAccessBinding")(
  (json: string) =>
    Schema.decodeEffect(Schema.fromJsonString(RemoteAdmissionAccessBinding))(json).pipe(
      Effect.mapError((cause) => new RemoteAdmissionPersistenceError({ cause })),
    ),
);

const make = Effect.fn("RemoteAdmission.make")(function* <E>(
  evaluate: RemoteAdmissionEvaluator<E>,
  timeoutMs: number,
) {
  const sql = yield* SqlClient.SqlClient;
  const presence = makeJonesTablePresence(sql);
  const reviewChanges = yield* Effect.serviceOption(ReviewChanges.ReviewChanges);
  const invalidateReview = Option.isSome(reviewChanges)
    ? reviewChanges.value.invalidate
    : Effect.void;
  const access = makeReceiverAccess(sql, invalidateReview);
  const review = makeReviewProjection(sql);
  const reviewAvailable = presence("jones_remote_managed_scopes").pipe(
    Effect.mapError((cause) => new RemoteAdmissionPersistenceError({ cause })),
  );
  const dependencies = makeDependencyStore(sql, presence);
  const changes = yield* Effect.serviceOption(DependencyChanges.DependencyChanges);
  const publishDependencyChanges = Option.isNone(changes)
    ? Effect.void
    : dependencies.sequence.pipe(
        Effect.orDie,
        Effect.flatMap((sequence) => changes.value.publish({ threadIds: [], sequence })),
      );
  const database = <A>(effect: Effect.Effect<A, AdmissionError | SqlError>) =>
    effect.pipe(
      Effect.catchTags({
        SqlError: (cause) => Effect.fail(new RemoteAdmissionPersistenceError({ cause })),
      }),
    );
  const get = (id: string) =>
    database(
      Effect.gen(function* () {
        const rows = yield* sql<Row>`SELECT * FROM jones_remote_admissions WHERE id = ${id}`;
        return rows[0] ? yield* decodeRow(rows[0]) : null;
      }),
    );
  const bound = Effect.fn("RemoteAdmission.bound")(function* (input: BoundRequest) {
    const record = yield* get(input.id);
    if (!record) return yield* new RemoteAdmissionNotFound({});
    if (record.bindingDigest !== input.bindingDigest)
      return yield* new RemoteAdmissionBindingMismatch({});
    return record;
  });
  const required = Effect.fn("RemoteAdmission.required")(function* (id: string) {
    const record = yield* get(id);
    if (!record) return yield* new RemoteAdmissionNotFound({});
    return record;
  });

  // Build once after the previous receiver service has stopped. A lost evaluation
  // never becomes a pass, and a dispatch claim remains unreplayed for reconciliation.
  yield* database(sql`UPDATE jones_remote_admissions SET status = 'held',
    reason = 'Evaluation was interrupted; owner review is required.', category = 'evaluation_interrupted',
    updated_at = ${yield* nowIso} WHERE status = 'evaluating'`);

  const request: RemoteAdmission["Service"]["request"] = Effect.fn("RemoteAdmission.request")(
    function* (input) {
      const decoded = yield* decodeRemoteAdmissionRequest(input).pipe(
        Effect.mapError((cause) => new RemoteAdmissionInvalidRequest({ cause })),
      );
      const trustedBinding = input.accessBinding
        ? yield* Schema.decodeEffect(RemoteAdmissionAccessBinding)(input.accessBinding).pipe(
            Effect.mapError((cause) => new RemoteAdmissionInvalidRequest({ cause })),
          )
        : undefined;
      if (
        trustedBinding &&
        (decoded.envelope.identity.linkId === null ||
          trustedBinding.sessionId !== decoded.envelope.identity.clientId ||
          trustedBinding.destinationThreadId !== decoded.envelope.target.threadId ||
          trustedBinding.runtimeMode !== decoded.envelope.target.runtimeMode ||
          trustedBinding.interactionMode !== decoded.envelope.target.interactionMode)
      )
        return yield* new RemoteAdmissionBindingMismatch({});
      const snapshot = yield* Effect.try({
        try: () => {
          const envelopeJson = canonicalJson(decoded.envelope);
          const { receiverPolicy: _policy, ...incoming } = decoded.envelope;
          return {
            envelopeJson,
            requestDigest: digest({ version: 1, incoming }),
            bindingDigest: digest({ version: 1, envelope: decoded.envelope }),
          };
        },
        catch: (cause) => new RemoteAdmissionInvalidRequest({ cause }),
      });
      // Mask intent commit through finalizer installation and the terminal write;
      // restore interruption only around provider work, including its cancellation.
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const inserted = yield* database(
            sql.withTransaction(
              Effect.gen(function* () {
                const existing = yield* sql<Row>`SELECT * FROM jones_remote_admissions
        WHERE client_id = ${decoded.envelope.identity.clientId} AND idempotency_key = ${decoded.idempotencyKey}`;
                if (existing[0]) {
                  const record = yield* decodeRow(existing[0]);
                  if (record.requestDigest !== snapshot.requestDigest)
                    return yield* new RemoteAdmissionKeyConflict({});
                  return { record, fresh: false };
                }
                const resultOperation = admissionOperation(decoded.envelope);
                const resultTables =
                  resultOperation !== undefined && (yield* dependencies.available);
                if (
                  resultTables &&
                  (yield* dependencies.fenced(resultOperation!.origin, resultOperation!.operation))
                )
                  return yield* new RemoteAdmissionStateConflict({ status: "fenced" });
                const id = NodeCrypto.randomUUID();
                const now = yield* nowIso;
                const status = decoded.envelope.receiverPolicy.enabled ? "evaluating" : "passed";
                yield* sql`INSERT INTO jones_remote_admissions
        (id,client_id,idempotency_key,request_digest,binding_digest,envelope_json,status,created_at,updated_at)
        VALUES (${id},${decoded.envelope.identity.clientId},${decoded.idempotencyKey},${snapshot.requestDigest},
          ${snapshot.bindingDigest},${snapshot.envelopeJson},${status},${now},${now})`;
                if (resultTables)
                  yield* sql`UPDATE jones_remote_admissions SET origin_environment_id = ${resultOperation!.origin}, operation_id = ${resultOperation!.operation} WHERE id = ${id}`;
                if (yield* reviewAvailable) {
                  if (trustedBinding)
                    yield* sql`INSERT INTO jones_remote_admission_extensions(admission_id,binding_json)
                    VALUES(${id},${canonicalJson(trustedBinding)})`;
                  if (status === "passed")
                    yield* access.enforceInTransaction({
                      record: yield* required(id),
                      binding: trustedBinding ?? null,
                      now,
                      phase: "intake",
                      modeAllowed: withinAccessCeiling,
                    });
                  yield* access.bumpInTransaction("admission", id, now);
                }
                return { record: yield* required(id), fresh: true };
              }),
            ),
          );
          if (yield* reviewAvailable) {
            yield* database(
              sql.withTransaction(
                Effect.gen(function* () {
                  if (!inserted.fresh) {
                    const marks = yield* sql<{
                      managed_scope_id: string;
                      reason: import("./ReceiverAccess.ts").BlockReason;
                    }>`SELECT * FROM jones_remote_admission_blocked_marks WHERE admission_id=${inserted.record.id}`;
                    if (inserted.record.status === "held" && marks[0])
                      yield* access.blockInTransaction(
                        inserted.record.id,
                        marks[0].managed_scope_id,
                        marks[0].reason,
                        yield* nowIso,
                        true,
                      );
                    else if (
                      inserted.record.status === "held" &&
                      inserted.record.category === "malicious"
                    ) {
                      const audit = yield* sql<{
                        managed_scope_id: string;
                      }>`SELECT managed_scope_id FROM jones_remote_admission_blocked_event_keys
                    WHERE admission_id=${inserted.record.id} AND reason='malicious'`;
                      if (audit[0])
                        yield* access.blockInTransaction(
                          inserted.record.id,
                          audit[0].managed_scope_id,
                          "malicious",
                          yield* nowIso,
                          true,
                          true,
                        );
                    }
                  }
                }),
              ),
            );
          }
          if (!inserted.fresh) return inserted.record;
          const enforce = (record: RemoteAdmissionRecord) =>
            database(
              sql.withTransaction(
                Effect.gen(function* () {
                  if (!(yield* reviewAvailable)) return record;
                  const result = yield* access.enforceInTransaction({
                    record,
                    binding: trustedBinding ?? null,
                    now: yield* nowIso,
                    phase: "intake",
                    malicious: record.category === "malicious",
                    modeAllowed: withinAccessCeiling,
                  });
                  if (result.managed && result.authorized && record.status === "held")
                    yield* sql`UPDATE jones_remote_admissions
              SET status='approved',updated_at=${yield* nowIso} WHERE id=${record.id} AND status='held'`;
                  yield* access.bumpInTransaction("admission", record.id, yield* nowIso);
                  return yield* required(record.id);
                }),
              ),
            );
          if (inserted.record.status !== "evaluating") return yield* enforce(inserted.record);
          const selection = inserted.record.envelope.receiverPolicy.modelSelection;
          const startedAt = yield* Clock.currentTimeMillis;
          const warn = (failureKind: EvaluationFailureKind) => {
            const diagnostic = evaluationDiagnostic(failureKind, selection);
            return Clock.currentTimeMillis.pipe(
              Effect.flatMap((now) =>
                Effect.logWarning("Remote admission evaluation failed").pipe(
                  Effect.annotateLogs({
                    id: inserted.record.id,
                    instanceId: diagnostic.instanceId,
                    model: diagnostic.model,
                    elapsedMs: Math.max(0, now - startedAt),
                    failureKind,
                  }),
                ),
              ),
            );
          };
          const evaluation = yield* restore(
            Effect.suspend(() =>
              selection === null ? Effect.succeed(null) : evaluate(inserted.record.envelope),
            ).pipe(
              Effect.timeout(timeoutMs),
              Effect.flatMap(Schema.decodeUnknownEffect(RemoteAdmissionEvaluation)),
              Effect.map((result) => ({
                status: result.verdict === "pass" ? "passed" : "held",
                reason: result.reason,
                category: result.category,
              })),
              Effect.catchCause((cause) => {
                if (Cause.hasInterrupts(cause))
                  return Effect.failCause(
                    Cause.fromReasons<never>(cause.reasons.filter(Cause.isInterruptReason)),
                  );
                const failureKind =
                  selection === null ? "unsupported_selection" : classifyEvaluationFailure(cause);
                return warn(failureKind).pipe(
                  Effect.as({
                    status: "held",
                    reason: evaluationDiagnostic(failureKind, selection).reason,
                    category: "evaluation_unavailable",
                  }),
                );
              }),
            ),
          ).pipe(
            Effect.onInterrupt(() =>
              Effect.gen(function* () {
                const diagnostic = evaluationDiagnostic("interrupted", selection);
                yield* database(sql`UPDATE jones_remote_admissions SET status = 'held',
              reason = ${diagnostic.reason}, category = 'evaluation_interrupted', updated_at = ${yield* nowIso}
              WHERE id = ${inserted.record.id} AND status = 'evaluating'`);
                yield* warn("interrupted");
              }),
            ),
          );
          return yield* database(
            sql.withTransaction(
              Effect.gen(function* () {
                if (yield* reviewAvailable)
                  yield* sql`INSERT INTO jones_remote_admission_extensions(admission_id,evaluator_json)
              VALUES(${inserted.record.id},${["none", "malicious", "ridiculous", "risky", "out_of_scope"].includes(evaluation.category) ? canonicalJson({ verdict: evaluation.status === "passed" ? "pass" : "hold", reason: evaluation.reason, category: evaluation.category }) : null})
              ON CONFLICT(admission_id) DO UPDATE SET evaluator_json=excluded.evaluator_json`;
                yield* sql`UPDATE jones_remote_admissions SET status = ${evaluation.status},
              reason = ${evaluation.reason}, category = ${evaluation.category}, updated_at = ${yield* nowIso}
              WHERE id = ${inserted.record.id} AND status = 'evaluating'`;
                return yield* enforce(yield* required(inserted.record.id));
              }),
            ),
          );
        }),
      );
    },
  );

  const resolve: RemoteAdmission["Service"]["resolve"] = (input) =>
    database(
      sql.withTransaction(
        Effect.gen(function* () {
          if (input.principal.kind !== "human-owner")
            return yield* new RemoteAdmissionForbidden({});
          yield* validateIdentity(input.principal.subject);
          const record = yield* bound(input);
          const status = input.decision === "approve" ? "approved" : "denied";
          if (input.decision !== "approve" && input.decision !== "deny")
            return yield* new RemoteAdmissionForbidden({});
          if (record.status === status) return record;
          if (record.status !== "held")
            return yield* new RemoteAdmissionStateConflict({ status: record.status });
          const now = yield* nowIso;
          if (yield* reviewAvailable) {
            if (input.decision === "approve") {
              if (input.durationSeconds !== undefined) {
                if (!["t3_thread_launch", "t3_thread_send"].includes(record.envelope.operation))
                  return yield* new ReceiverAccessUnavailable({});
                const extensions = yield* sql<{
                  binding_json: string | null;
                }>`SELECT binding_json FROM jones_remote_admission_extensions WHERE admission_id=${record.id}`;
                if (!extensions[0]?.binding_json) return yield* new ReceiverAccessUnavailable({});
                yield* access.createGrantInTransaction({
                  admissionId: record.id,
                  binding: yield* decodeStoredAccessBinding(extensions[0].binding_json),
                  durationSeconds: input.durationSeconds,
                  now,
                });
              }
              yield* access.permitInTransaction(record);
            }
            yield* sql`DELETE FROM jones_remote_admission_blocked_marks WHERE admission_id=${record.id}`;
            yield* access.bumpInTransaction("admission", record.id, now);
          }
          yield* sql`UPDATE jones_remote_admissions SET status = ${status}, resolved_by = ${input.principal.subject},
      resolved_at = ${now}, updated_at = ${now} WHERE id = ${input.id} AND status = 'held'`;
          return yield* required(input.id);
        }),
      ),
    );

  // clientId is supplied from authenticated connection identity, never the payload.
  const withdraw: RemoteAdmission["Service"]["withdraw"] = (input) =>
    database(
      sql.withTransaction(
        Effect.gen(function* () {
          const record = yield* bound(input);
          if (record.envelope.identity.clientId !== input.clientId)
            return yield* new RemoteAdmissionForbidden({});
          if (record.status === "cancelled") return record;
          if (!["evaluating", "held", "passed", "approved"].includes(record.status)) {
            return yield* new RemoteAdmissionStateConflict({ status: record.status });
          }
          if (yield* reviewAvailable) {
            yield* sql`DELETE FROM jones_remote_admission_blocked_marks WHERE admission_id=${record.id}`;
            yield* access.bumpInTransaction("admission", record.id, yield* nowIso);
          }
          yield* sql`UPDATE jones_remote_admissions SET status = 'cancelled' , updated_at = ${yield* nowIso} WHERE id = ${input.id}`;
          return yield* required(input.id);
        }),
      ),
    );

  const claimDispatch: RemoteAdmission["Service"]["claimDispatch"] = (input) =>
    database(
      sql.withTransaction(
        Effect.gen(function* () {
          yield* validateIdentity(input.commandId);
          const record = yield* bound(input);
          if (record.status === "dispatching" || record.status === "dispatched") {
            if (record.commandId !== input.commandId)
              return yield* new RemoteAdmissionStateConflict({ status: record.status });
            return { record, claimed: false };
          }
          if (record.status !== "passed" && record.status !== "approved")
            return yield* new RemoteAdmissionStateConflict({ status: record.status });
          const authority = input.checkCurrentAuthority
            ? yield* input.checkCurrentAuthority(record)
            : { authorized: true, currentPolicy: input.currentPolicy };
          if (!authority.authorized) return yield* new RemoteAdmissionForbidden({});
          const currentPolicy = yield* decodeRemoteAdmissionReceiverPolicy(
            authority.currentPolicy,
          ).pipe(Effect.mapError((cause) => new RemoteAdmissionInvalidRequest({ cause })));
          if (canonicalJson(currentPolicy) !== canonicalJson(record.envelope.receiverPolicy))
            return yield* new RemoteAdmissionPolicyChanged({});
          if (yield* reviewAvailable) {
            const extensions = yield* sql<{
              binding_json: string | null;
            }>`SELECT binding_json FROM jones_remote_admission_extensions WHERE admission_id=${record.id}`;
            const guard = yield* access.enforceInTransaction({
              record,
              binding: extensions[0]?.binding_json
                ? yield* decodeStoredAccessBinding(extensions[0].binding_json)
                : null,
              now: yield* nowIso,
              phase: "claim",
              modeAllowed: withinAccessCeiling,
            });
            if (!guard.authorized) return { record: yield* required(record.id), claimed: false };
            yield* access.bumpInTransaction("admission", record.id, yield* nowIso);
          }
          yield* sql`UPDATE jones_remote_admissions SET status = 'dispatching', command_id = ${input.commandId},
      updated_at = ${yield* nowIso} WHERE id = ${input.id} AND status IN ('passed','approved')`;
          return { record: yield* required(input.id), claimed: true };
        }),
      ),
    );

  // Call only after observing normal dispatch's durable receipt for this exact command.
  const markDispatched: RemoteAdmission["Service"]["markDispatched"] = (input) =>
    database(
      sql.withTransaction(
        Effect.gen(function* () {
          const record = yield* bound(input);
          if (
            record.commandId !== input.commandId ||
            (record.status !== "dispatching" && record.status !== "dispatched")
          ) {
            return yield* new RemoteAdmissionStateConflict({ status: record.status });
          }
          if (record.status === "dispatched") return record;
          if (input.commitReceiptInTransaction) yield* input.commitReceiptInTransaction();
          if (yield* reviewAvailable)
            yield* access.bumpInTransaction("admission", record.id, yield* nowIso);
          yield* sql`UPDATE jones_remote_admissions SET status = 'dispatched', updated_at = ${yield* nowIso}
      WHERE id = ${input.id} AND status = 'dispatching'`;
          return yield* required(input.id);
        }),
      ),
    );

  return RemoteAdmission.of({
    retention: makeReviewRetention(sql, invalidateReview),
    access,
    review,
    request: Effect.fn("RemoteAdmission.request")((input) =>
      request(input).pipe(Effect.tap(() => publishDependencyChanges)),
    ),
    get,
    resolve: Effect.fn("RemoteAdmission.resolve")((input) =>
      resolve(input).pipe(Effect.tap(() => publishDependencyChanges)),
    ),
    withdraw: Effect.fn("RemoteAdmission.withdraw")((input) =>
      withdraw(input).pipe(Effect.tap(() => publishDependencyChanges)),
    ),
    claimDispatch: Effect.fn("RemoteAdmission.claimDispatch")((input) =>
      claimDispatch(input).pipe(Effect.tap(() => publishDependencyChanges)),
    ),
    markDispatched: Effect.fn("RemoteAdmission.markDispatched")((input) =>
      markDispatched(input).pipe(Effect.tap(() => publishDependencyChanges)),
    ),
    listPending: Effect.fn("RemoteAdmission.listPending")(() =>
      database(
        Effect.gen(function* () {
          const rows = yield* sql<Row>`SELECT * FROM jones_remote_admissions
        WHERE status IN ('held','evaluating','approved','dispatching') ORDER BY created_at, id LIMIT 200`;
          return yield* Effect.forEach(rows, decodeRow);
        }),
      ),
    ),
  });
});

// The adapter supplies receiver-owned policy and authenticated identity; this service
// screens intent heuristically and does not contain normal local execution tools.
export const layerWithEvaluator = <E>(
  evaluate: RemoteAdmissionEvaluator<E>,
  options?: { readonly timeoutMs?: number },
) =>
  Layer.effect(
    RemoteAdmission,
    make(evaluate, options?.timeoutMs ?? REMOTE_ADMISSION_EVALUATION_TIMEOUT_MS),
  );
