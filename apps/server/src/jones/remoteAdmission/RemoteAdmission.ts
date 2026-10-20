// @effect-diagnostics nodeBuiltinImport:off -- Canonical binding hashes and opaque IDs are synchronous server-local primitives.
import * as NodeCrypto from "node:crypto";
import {
  RemoteAdmissionEnvelope,
  RemoteAdmissionEvaluation,
  RemoteAdmissionRecord,
  RemoteAdmissionRequest,
  RemoteAdmissionReceiverPolicy,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";

export class RemoteAdmissionInvalidRequest extends Schema.TaggedError<RemoteAdmissionInvalidRequest>()(
  "RemoteAdmissionInvalidRequest",
  { cause: Schema.Defect() },
) {}
export class RemoteAdmissionPersistenceError extends Schema.TaggedError<RemoteAdmissionPersistenceError>()(
  "RemoteAdmissionPersistenceError",
  { cause: Schema.Defect() },
) {}
export class RemoteAdmissionKeyConflict extends Schema.TaggedError<RemoteAdmissionKeyConflict>()(
  "RemoteAdmissionKeyConflict",
  {},
) {}
export class RemoteAdmissionNotFound extends Schema.TaggedError<RemoteAdmissionNotFound>()(
  "RemoteAdmissionNotFound",
  {},
) {}
export class RemoteAdmissionBindingMismatch extends Schema.TaggedError<RemoteAdmissionBindingMismatch>()(
  "RemoteAdmissionBindingMismatch",
  {},
) {}
export class RemoteAdmissionForbidden extends Schema.TaggedError<RemoteAdmissionForbidden>()(
  "RemoteAdmissionForbidden",
  {},
) {}
export class RemoteAdmissionStateConflict extends Schema.TaggedError<RemoteAdmissionStateConflict>()(
  "RemoteAdmissionStateConflict",
  { status: Schema.String },
) {}
export class RemoteAdmissionPolicyChanged extends Schema.TaggedError<RemoteAdmissionPolicyChanged>()(
  "RemoteAdmissionPolicyChanged",
  {},
) {}

type AdmissionError =
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
    readonly request: (
      input: RemoteAdmissionRequest,
    ) => Effect.Effect<RemoteAdmissionRecord, AdmissionError>;
    readonly get: (id: string) => Effect.Effect<RemoteAdmissionRecord | null, AdmissionError>;
    readonly listPending: () => Effect.Effect<ReadonlyArray<RemoteAdmissionRecord>, AdmissionError>;
    readonly resolve: (
      input: BoundRequest & {
        readonly decision: "approve" | "deny";
        readonly principal: RemoteAdmissionPrincipal;
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
      },
    ) => Effect.Effect<
      { readonly record: RemoteAdmissionRecord; readonly claimed: boolean },
      AdmissionError
    >;
    readonly markDispatched: (
      input: BoundRequest & {
        readonly commandId: string;
      },
    ) => Effect.Effect<RemoteAdmissionRecord, AdmissionError>;
  }
>()("t3/jones/remoteAdmission/RemoteAdmission") {}

const canonicalJson = (value: unknown): string => {
  if (value === null || typeof value === "string" || typeof value === "boolean")
    return JSON.stringify(value);
  if (typeof value === "number" && Number.isFinite(value)) return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (typeof value === "object" && value !== null) {
    return `{${Object.keys(value)
      .sort()
      .map(
        (key) => `${JSON.stringify(key)}:${canonicalJson((value as Record<string, unknown>)[key])}`,
      )
      .join(",")}}`;
  }
  throw new Error("Admission input must be JSON");
};
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
const decodeRow = (row: Row) =>
  Effect.gen(function* () {
    const envelope = yield* Schema.decodeEffect(Schema.fromJsonString(RemoteAdmissionEnvelope))(
      row.envelope_json,
    );
    const record = yield* Schema.decodeUnknownEffect(RemoteAdmissionRecord)({
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
      return yield* Effect.fail(new RemoteAdmissionBindingMismatch({}));
    }
    return record;
  }).pipe(Effect.mapError((cause) => new RemoteAdmissionPersistenceError({ cause })));

const make = <E>(evaluate: RemoteAdmissionEvaluator<E>, timeoutMs: number) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
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
    const bound = (input: BoundRequest) =>
      Effect.gen(function* () {
        const record = yield* get(input.id);
        if (!record) return yield* new RemoteAdmissionNotFound({});
        if (record.bindingDigest !== input.bindingDigest)
          return yield* new RemoteAdmissionBindingMismatch({});
        return record;
      });
    const required = (id: string) =>
      Effect.gen(function* () {
        const record = yield* get(id);
        if (!record) return yield* new RemoteAdmissionNotFound({});
        return record;
      });

    // Build once after the previous receiver service has stopped. A lost evaluation
    // never becomes a pass, and a dispatch claim remains unreplayed for reconciliation.
    yield* database(sql`UPDATE jones_remote_admissions SET status = 'held',
    reason = 'Evaluation was interrupted; owner review is required.', category = 'evaluation_interrupted',
    updated_at = ${yield* nowIso} WHERE status = 'evaluating'`);

    const request: RemoteAdmission["Service"]["request"] = (input) =>
      Effect.gen(function* () {
        const decoded = yield* Schema.decodeEffect(RemoteAdmissionRequest)(input).pipe(
          Effect.mapError((cause) => new RemoteAdmissionInvalidRequest({ cause })),
        );
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
              const id = NodeCrypto.randomUUID();
              const now = yield* nowIso;
              const status = decoded.envelope.receiverPolicy.enabled ? "evaluating" : "passed";
              yield* sql`INSERT INTO jones_remote_admissions
        (id,client_id,idempotency_key,request_digest,binding_digest,envelope_json,status,created_at,updated_at)
        VALUES (${id},${decoded.envelope.identity.clientId},${decoded.idempotencyKey},${snapshot.requestDigest},
          ${snapshot.bindingDigest},${snapshot.envelopeJson},${status},${now},${now})`;
              return { record: yield* required(id), fresh: true };
            }),
          ),
        );
        if (!inserted.fresh || inserted.record.status !== "evaluating") return inserted.record;
        const evaluation = yield* Effect.suspend(() =>
          inserted.record.envelope.receiverPolicy.modelSelection === null
            ? Effect.succeed(null)
            : evaluate(inserted.record.envelope),
        ).pipe(
          Effect.timeout(timeoutMs),
          Effect.flatMap(Schema.decodeUnknownEffect(RemoteAdmissionEvaluation)),
          Effect.map((result) => ({
            status: result.verdict === "pass" ? "passed" : "held",
            reason: result.reason,
            category: result.category,
          })),
          Effect.catchCause(() =>
            Effect.succeed({
              status: "held",
              reason: "Evaluation was unavailable; owner review is required.",
              category: "evaluation_unavailable",
            }),
          ),
        );
        yield* database(sql`UPDATE jones_remote_admissions SET status = ${evaluation.status},
      reason = ${evaluation.reason}, category = ${evaluation.category}, updated_at = ${yield* nowIso}
      WHERE id = ${inserted.record.id} AND status = 'evaluating'`);
        return yield* required(inserted.record.id);
      });

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
            yield* sql`UPDATE jones_remote_admissions SET status = 'cancelled', updated_at = ${yield* nowIso} WHERE id = ${input.id}`;
            return yield* required(input.id);
          }),
        ),
      );

    const claimDispatch: RemoteAdmission["Service"]["claimDispatch"] = (input) =>
      database(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* validateIdentity(input.commandId);
            const currentPolicy = yield* Schema.decodeEffect(RemoteAdmissionReceiverPolicy)(
              input.currentPolicy,
            ).pipe(Effect.mapError((cause) => new RemoteAdmissionInvalidRequest({ cause })));
            const record = yield* bound(input);
            if (canonicalJson(currentPolicy) !== canonicalJson(record.envelope.receiverPolicy))
              return yield* new RemoteAdmissionPolicyChanged({});
            if (record.status === "dispatching" || record.status === "dispatched") {
              if (record.commandId !== input.commandId)
                return yield* new RemoteAdmissionStateConflict({ status: record.status });
              return { record, claimed: false };
            }
            if (record.status !== "passed" && record.status !== "approved")
              return yield* new RemoteAdmissionStateConflict({ status: record.status });
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
            yield* sql`UPDATE jones_remote_admissions SET status = 'dispatched', updated_at = ${yield* nowIso}
      WHERE id = ${input.id} AND status = 'dispatching'`;
            return yield* required(input.id);
          }),
        ),
      );

    return RemoteAdmission.of({
      request,
      get,
      resolve,
      withdraw,
      claimDispatch,
      markDispatched,
      listPending: () =>
        database(
          Effect.gen(function* () {
            const rows = yield* sql<Row>`SELECT * FROM jones_remote_admissions
        WHERE status IN ('held','evaluating','approved','dispatching') ORDER BY created_at, id LIMIT 200`;
            return yield* Effect.forEach(rows, decodeRow);
          }),
        ),
    });
  });

// The adapter supplies receiver-owned policy and authenticated identity; this service
// screens intent heuristically and does not contain normal local execution tools.
export const layerWithEvaluator = <E>(
  evaluate: RemoteAdmissionEvaluator<E>,
  options?: { readonly timeoutMs?: number },
) => Layer.effect(RemoteAdmission, make(evaluate, options?.timeoutMs ?? 30_000));
