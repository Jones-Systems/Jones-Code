import { encodeJson, decodeJson } from "./json.ts";
// @effect-diagnostics nodeBuiltinImport:off -- Hashes and IDs bind durable receiving intents.
import * as NodeCrypto from "node:crypto";
import {
  OrchestratorMcpFailure,
  RemoteAdmissionReceiverPolicy,
  publicRemoteAdmission,
  type RemoteAdmissionPublicStatus,
  type RemoteAdmissionRecord,
  type RemoteAdmissionPolicyUpdate,
  type RemoteAdmissionDecision,
  type RemoteAdmissionSnapshot,
  type EnvironmentSessionPrincipalShape,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import * as McpInvocationContext from "../../mcp/McpInvocationContext.ts";
import * as RemoteAdmission from "../remoteAdmission/RemoteAdmission.ts";
import { isRemoteAdmissionOwner } from "./owner.ts";

export class RemoteAdmissionIntakeError extends Schema.TaggedError<RemoteAdmissionIntakeError>()(
  "RemoteAdmissionIntakeError",
  { cause: Schema.Defect() },
) {}

type Invocation = McpInvocationContext.McpInvocationScope;
type Handler = (
  params: unknown,
  invocation: Invocation,
) => Effect.Effect<unknown, OrchestratorMcpFailure | RemoteAdmissionIntakeError>;
type Params = Record<string, unknown>;
type Target = (
  params: Params,
  invocation: Invocation,
) => Effect.Effect<unknown, RemoteAdmissionIntakeError>;

export class RemoteAdmissionIntake extends Context.Service<
  RemoteAdmissionIntake,
  {
    readonly register: (operation: string, handler: Handler, target: Target) => void;
    readonly invoke: (
      operation: string,
      params: Params,
      invocation: Invocation,
    ) => Effect.Effect<unknown, OrchestratorMcpFailure>;
    readonly attention: () => Effect.Effect<{ readonly count: number }, RemoteAdmissionIntakeError>;
    readonly snapshot: () => Effect.Effect<RemoteAdmissionSnapshot, RemoteAdmissionIntakeError>;
    readonly updatePolicy: (
      input: RemoteAdmissionPolicyUpdate,
    ) => Effect.Effect<RemoteAdmissionSnapshot, RemoteAdmissionIntakeError>;
    readonly decide: (
      input: RemoteAdmissionDecision,
      owner: EnvironmentSessionPrincipalShape,
    ) => Effect.Effect<RemoteAdmissionRecord, RemoteAdmissionIntakeError>;
    readonly withdraw: (
      input: { readonly id: string; readonly bindingDigest: string },
      clientId: string,
    ) => Effect.Effect<RemoteAdmissionPublicStatus, RemoteAdmissionIntakeError>;
    readonly resumeApproved: () => Effect.Effect<void, RemoteAdmissionIntakeError>;
  }
>()("t3/jones/remoteAdmissionIntegration/RemoteAdmissionIntake") {}

const defaultPolicy: RemoteAdmissionReceiverPolicy = {
  enabled: false,
  revision: "initial",
  modelSelection: null,
};
const stable = (value: unknown): string => {
  if (Array.isArray(value)) return `[${value.map(stable).join(",")}]`;
  if (value !== null && typeof value === "object")
    return `{${Object.entries(value)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${encodeJson(k)}:${stable(v)}`)
      .join(",")}}`;
  return encodeJson(value);
};
const hash = (value: unknown) =>
  NodeCrypto.createHash("sha256").update(stable(value)).digest("hex");
const storedScope = (invocation: Invocation) => ({
  environmentId: invocation.environmentId,
  capabilities: [...invocation.capabilities].sort(),
  requestNamespace: invocation.requestNamespace,
  thread: invocation.thread ?? null,
  client: invocation.client ?? null,
});
const unpack = (record: RemoteAdmissionRecord) => {
  const payload = record.envelope.payload as unknown as {
    params: Params;
    target: unknown;
    invocation: ReturnType<typeof storedScope>;
  };
  return {
    params: payload.params,
    target: payload.target,
    invocation: {
      ...payload.invocation,
      issuedAt: 0,
      capabilities: new Set(payload.invocation.capabilities),
      thread: payload.invocation.thread ?? undefined,
      client: payload.invocation.client ?? undefined,
    } satisfies Invocation,
  };
};
const pending = (record: RemoteAdmissionRecord) =>
  new OrchestratorMcpFailure({
    code:
      record.status === "denied" || record.status === "cancelled"
        ? "remote_admission_denied"
        : "remote_admission_pending",
    message:
      record.status === "denied"
        ? "The receiving owner declined this request."
        : record.status === "cancelled"
          ? "The remote request was withdrawn."
          : "This request needs input from the receiving owner.",
    remoteAdmission: { id: record.id, bindingDigest: record.bindingDigest, status: record.status },
  });
const intakeError = (cause: unknown) => new RemoteAdmissionIntakeError({ cause });

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const admissions = yield* RemoteAdmission.RemoteAdmission;
  const sessions = yield* EnvironmentAuth.EnvironmentAuth;
  const handlers = new Map<string, Handler>();
  const targets = new Map<string, Target>();
  const eligible = Effect.succeed(true);
  const policy = () =>
    Effect.gen(function* () {
      const rows = yield* sql<{
        policy_json: string;
      }>`SELECT policy_json FROM jones_remote_admission_policy WHERE singleton = 1`;
      const saved = rows[0]
        ? yield* Schema.decodeUnknownEffect(Schema.fromJsonString(RemoteAdmissionReceiverPolicy))(
            rows[0].policy_json,
          )
        : defaultPolicy;
      return saved;
    });
  const result = (id: string) =>
    Effect.gen(function* () {
      const rows = yield* sql<{
        result_json: string;
      }>`SELECT result_json FROM jones_remote_admission_results WHERE admission_id = ${id}`;
      return rows[0] ? (decodeJson(rows[0].result_json) as { ok: boolean; value: unknown }) : null;
    });
  const deliver = (saved: { ok: boolean; value: unknown }) =>
    saved.ok
      ? Effect.succeed(saved.value)
      : Schema.decodeUnknownEffect(OrchestratorMcpFailure)(saved.value).pipe(
          Effect.flatMap((error) => Effect.fail(error)),
        );
  const validateTarget = (record: RemoteAdmissionRecord) =>
    Effect.gen(function* () {
      const { params, invocation, target } = unpack(record);
      const resolveTarget = targets.get(record.envelope.operation);
      if (!resolveTarget || stable(yield* resolveTarget(params, invocation)) !== stable(target)) {
        return yield* new OrchestratorMcpFailure({
          code: "request_conflict",
          message: "The receiving target configuration changed. Submit a new request.",
        });
      }
    });
  const dispatch = (record: RemoteAdmissionRecord) =>
    Effect.gen(function* () {
      const prior = yield* result(record.id);
      if (prior) return yield* deliver(prior);
      const handler = handlers.get(record.envelope.operation);
      if (!handler) return yield* Effect.fail(pending(record));
      const { params, invocation } = unpack(record);
      yield* validateTarget(record);
      const active = yield* sessions.listSessions();
      if (!active.some((session) => session.sessionId === invocation.client?.sessionId)) {
        return yield* Effect.fail(
          new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "The originating connection is no longer active.",
          }),
        );
      }
      const commandId = `admission:${record.id}`;
      const claim = yield* admissions.claimDispatch({
        id: record.id,
        bindingDigest: record.bindingDigest,
        commandId,
        currentPolicy: yield* policy(),
      });
      if (!claim.claimed) return yield* Effect.fail(pending(claim.record));
      // A claim is never blindly replayed after interruption. A recorded result is
      // reusable; an unknown handler effect remains visible for reconciliation.
      const outcome = yield* handler(params, invocation).pipe(
        Effect.map((value) => ({ ok: true, value })),
        Effect.catch((error) =>
          Schema.is(OrchestratorMcpFailure)(error)
            ? Effect.succeed({ ok: false, value: error })
            : Effect.fail(error),
        ),
      );
      yield* sql`INSERT INTO jones_remote_admission_results (admission_id, result_json) VALUES (${record.id}, ${encodeJson(outcome)})`;
      yield* admissions.markDispatched({
        id: record.id,
        bindingDigest: record.bindingDigest,
        commandId,
      });
      return yield* deliver(outcome);
    });
  const snapshot = () =>
    Effect.gen(function* () {
      return {
        eligible: yield* eligible,
        policy: yield* policy(),
        pending: yield* admissions.listPending(),
      };
    }).pipe(Effect.mapError(intakeError));
  return RemoteAdmissionIntake.of({
    register: (operation, handler, target) => {
      handlers.set(operation, handler);
      targets.set(operation, target);
    },
    invoke: (operation, params, invocation) =>
      Effect.gen(function* () {
        const handler = handlers.get(operation);
        if (!handler)
          return yield* Effect.fail(
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "The receiving operation is unavailable.",
            }),
          );
        const current = yield* policy();
        if (!invocation.client) return yield* handler(params, invocation);
        const keyed = typeof params.clientRequestId === "string";
        const key = hash({
          namespace: invocation.requestNamespace,
          operation,
          key: keyed ? params.clientRequestId : NodeCrypto.randomUUID(),
        });
        const previousRows = keyed
          ? yield* sql<{
              id: string;
            }>`SELECT id FROM jones_remote_admissions WHERE client_id = ${invocation.client.sessionId} AND idempotency_key = ${key}`
          : [];
        const previous = previousRows[0] ? yield* admissions.get(previousRows[0].id) : null;
        if (!current.enabled && previous === null) return yield* handler(params, invocation);
        const resolveTarget = targets.get(operation);
        if (!resolveTarget) return yield* Effect.fail(intakeError("target_unavailable"));
        // Stable receiver-owned scope is bound with the exact decoded request.
        // Arrival timestamps vary on retries and are not authorization identity.
        // Retry bytes use the reviewed snapshot; dispatch revalidates current state.
        const reviewedTarget = previous
          ? unpack(previous).target
          : yield* resolveTarget(params, invocation);
        const payload = decodeJson(
          stable({ params, invocation: storedScope(invocation), target: reviewedTarget }),
        );
        const record = yield* admissions.request({
          idempotencyKey: key,
          envelope: {
            identity: {
              clientId: invocation.client.sessionId,
              linkId: invocation.client.linked ? invocation.client.sessionId : null,
            },
            operation,
            payload,
            target: {
              projectId: typeof params.projectId === "string" ? params.projectId : null,
              threadId: typeof params.threadId === "string" ? params.threadId : null,
              runtimeMode:
                typeof params.runtimeMode === "string"
                  ? params.runtimeMode
                  : McpInvocationContext.clientModeCeiling(invocation.client).runtimeMode,
              interactionMode:
                typeof params.interactionMode === "string" ? params.interactionMode : "default",
            },
            receiverPolicy: current,
          },
        });
        if (["passed", "approved", "dispatching", "dispatched"].includes(record.status))
          return yield* dispatch(record);
        return yield* Effect.fail(pending(record));
      }).pipe(
        Effect.catch((error) =>
          Effect.fail(
            Schema.is(OrchestratorMcpFailure)(error)
              ? error
              : new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: "Remote request admission could not be completed.",
                }),
          ),
        ),
      ),
    snapshot,
    attention: () =>
      sql<{
        count: number;
      }>`SELECT COUNT(*) AS count FROM jones_remote_admissions WHERE status IN ('held','evaluating','approved','dispatching')`.pipe(
        Effect.map((rows) => ({ count: rows[0]?.count ?? 0 })),
        Effect.mapError(intakeError),
      ),
    withdraw: (input, clientId) =>
      admissions
        .withdraw({ ...input, clientId })
        .pipe(Effect.map(publicRemoteAdmission), Effect.mapError(intakeError)),
    updatePolicy: (input) =>
      Effect.gen(function* () {
        yield* sql.withTransaction(
          Effect.gen(function* () {
            const current = yield* policy();
            if (input.expectedRevision !== current.revision)
              return yield* Effect.fail(intakeError("policy_changed"));
            if (input.enabled && input.modelSelection === null)
              return yield* Effect.fail(intakeError("model_required"));
            const next = {
              enabled: input.enabled,
              modelSelection: input.modelSelection,
              revision: NodeCrypto.randomUUID(),
            };
            yield* sql`INSERT INTO jones_remote_admission_policy (singleton, policy_json) VALUES (1, ${encodeJson(next)}) ON CONFLICT(singleton) DO UPDATE SET policy_json = excluded.policy_json`;
          }),
        );
        return yield* snapshot();
      }).pipe(Effect.mapError(intakeError)),
    decide: (input, owner) =>
      Effect.gen(function* () {
        if (!isRemoteAdmissionOwner(owner))
          return yield* new RemoteAdmission.RemoteAdmissionForbidden({});
        const existing = yield* admissions.get(input.id);
        if (!existing) return yield* new RemoteAdmission.RemoteAdmissionNotFound({});
        if (
          input.decision === "approve" &&
          stable(existing.envelope.receiverPolicy) !== stable(yield* policy())
        )
          return yield* new RemoteAdmission.RemoteAdmissionPolicyChanged({});
        if (input.decision === "approve" && existing.status === "held")
          yield* validateTarget(existing);
        if (
          input.decision === "approve" &&
          ["approved", "dispatching", "dispatched"].includes(existing.status)
        ) {
          if (existing.bindingDigest !== input.bindingDigest)
            return yield* new RemoteAdmission.RemoteAdmissionBindingMismatch({});
          if (existing.status === "approved") yield* dispatch(existing).pipe(Effect.ignore);
        } else {
          const record = yield* admissions.resolve({
            ...input,
            principal: { kind: "human-owner", subject: owner.sessionId },
          });
          if (record.status === "approved") yield* dispatch(record).pipe(Effect.ignore);
        }
        return (yield* admissions.get(input.id))!;
      }).pipe(Effect.mapError(intakeError)),
    resumeApproved: () =>
      Effect.gen(function* () {
        const rows = yield* sql<{
          id: string;
        }>`SELECT id FROM jones_remote_admissions WHERE status = 'approved' ORDER BY created_at LIMIT 200`;
        for (const row of rows) {
          const record = yield* admissions.get(row.id);
          if (record) yield* dispatch(record).pipe(Effect.ignore);
        }
      }).pipe(Effect.mapError(intakeError)),
  });
});
export const layer = Layer.effect(RemoteAdmissionIntake, make);
