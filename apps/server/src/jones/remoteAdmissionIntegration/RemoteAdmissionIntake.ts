import { encodeJson, decodeJson } from "./json.ts";
import {
  canonicalJson as stable,
  PRUNE_BATCH_SIZE,
} from "../remoteDelegation/shared.ts";
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
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import * as DateTime from "effect/DateTime";
import * as ServerSettings from "../../serverSettings.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import type { RemoteAdmissionAccessBinding, RemoteAdmissionReviewDecision } from "@t3tools/contracts/jones/remoteAdmissionReview";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as McpInvocationContext from "../../mcp/McpInvocationContext.ts";
import * as RemoteAdmission from "../remoteAdmission/RemoteAdmission.ts";
import { makeRecoveryStore, ReceiverRecoveryEvidence } from "./RecoveryStore.ts";
import { ReceiverClaimTracker } from "./ClaimTracker.ts";
import { classifyLaunchEvidence } from "./LaunchRecovery.ts";
import { RemoteAdmissionLaunchResult } from "@t3tools/contracts/jones/remoteAdmissionReview";
import { isRemoteAdmissionOwner } from "./owner.ts";
import * as ProviderInstanceRegistry from "../../provider/ProviderInstanceRegistry.ts";
import { resolveRemoteEvaluationModel } from "../../textGeneration/jones/remoteEvaluationModel.ts";
import { readBoundLinkedRequestContext } from "../peerTransport/LinkedRequestContext.ts";

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
    readonly isLiveClaim: (id:string) => boolean;
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
      input: RemoteAdmissionDecision | RemoteAdmissionReviewDecision,
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
const StoredResult = Schema.Struct({ ok: Schema.Boolean, value: Schema.Unknown, launchResult: Schema.optionalKey(RemoteAdmissionLaunchResult) });
const decodeStoredResult = Schema.decodeUnknownEffect(Schema.fromJsonString(StoredResult));
// An admission whose handler never recorded a result: the request fiber was cut off or the
// server stopped mid-handler. Whether the handler's effect happened is unknown, so the
// admission is closed with this unknown outcome and never automatically replayed.
const interruptedFailure = () =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message:
      "The receiving environment stopped before this request finished, so it may or may not have taken effect. Check that environment before submitting a new request.",
  });
const dispatchCommandId = (admissionId: string) => `admission:${admissionId}`;
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
// Pin only display data. Every other invocation field belongs to this attempt
// and must still participate in the generic admission's digest comparison.
const pinOriginTitle = (invocation: Invocation, previous: RemoteAdmissionRecord): Invocation => {
  const original = unpack(previous).invocation.client?.remoteOrigin;
  const client = invocation.client;
  const origin = client?.remoteOrigin;
  if (
    !client ||
    !origin?.thread ||
    !original?.thread ||
    origin.schemaVersion !== original.schemaVersion ||
    origin.environmentId !== original.environmentId ||
    origin.linkSessionId !== original.linkSessionId ||
    origin.thread.threadId !== original.thread.threadId
  )
    return invocation;
  const { title: _title, ...thread } = origin.thread;
  return {
    ...invocation,
    client: {
      ...client,
      remoteOrigin: {
        ...origin,
        thread: {
          ...thread,
          ...(Object.hasOwn(original.thread, "title") ? { title: original.thread.title } : {}),
        },
      },
    },
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
const decodeRemoteAdmissionReceiverPolicyJson = Schema.decodeEffect(
  Schema.fromJsonString(RemoteAdmissionReceiverPolicy),
);
const decodeOrchestratorMcpFailure = Schema.decodeUnknownEffect(OrchestratorMcpFailure);
const isOrchestratorMcpFailure = Schema.is(OrchestratorMcpFailure);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const serviceScope = yield* Effect.scope;
  const settings = yield* Effect.serviceOption(ServerSettings.ServerSettingsService);
  const presence = makeJonesTablePresence(sql);
  const nowIso = DateTime.now.pipe(Effect.map(DateTime.formatIso));
  const admissions = yield* RemoteAdmission.RemoteAdmission;
  const recoveryEvidence = yield* Effect.serviceOption(ReceiverRecoveryEvidence);
  const recovery = Option.isSome(recoveryEvidence) ? makeRecoveryStore(sql,admissions,recoveryEvidence.value) : null;
  const providers = yield* ProviderInstanceRegistry.ProviderInstanceRegistry;
  const handlers = new Map<string, Handler>();
  const targets = new Map<string, Target>();
  const eligible = Effect.succeed(true);
  const policy = Effect.fn("RemoteAdmissionIntake.policy")(function* () {
    const rows = yield* sql<{
      policy_json: string;
    }>`SELECT policy_json FROM jones_remote_admission_policy WHERE singleton = 1`;
    const saved = rows[0]
      ? yield* decodeRemoteAdmissionReceiverPolicyJson(rows[0].policy_json)
      : defaultPolicy;
    return saved;
  });
  const result = Effect.fn("RemoteAdmissionIntake.result")(function* (id: string) {
    const rows = yield* sql<{
      result_json: string;
    }>`SELECT result_json FROM jones_remote_admission_results WHERE admission_id = ${id}`;
    return rows[0] ? yield* decodeStoredResult(rows[0].result_json) : null;
  });
  const assertUnfenced = Effect.fn("RemoteAdmissionIntake.assertUnfenced")(function* (sessionId: string, key: string) {
    if (!(yield* presence("jones_remote_admission_fences"))) return;
    const rows = yield* sql`SELECT 1 FROM jones_remote_admission_fences WHERE client_id=${sessionId} AND idempotency_key=${key}`;
    if (rows.length) return yield* new OrchestratorMcpFailure({ code: "request_conflict", message: "This request was superseded. Its retained history may be unavailable; it will not execute again." });
  });
  const withSettings = <A, E, R>(use: () => Effect.Effect<A, E, R>, requireLinked = true) => {
    if (Option.isNone(settings)) return Effect.fail(intakeError("settings_unavailable"));
    return settings.value.withSettingsSnapshot(current => !requireLinked || current.linkedEnvironmentsEnabled
      ? use() : Effect.fail(intakeError("linked_environments_disabled")));
  };
  // Database-only guard. Invoked within the claim writer while settings is already held.
  const currentAuthority = Effect.fn("RemoteAdmissionIntake.currentAuthority")(function* (record: RemoteAdmissionRecord) {
    const { invocation } = unpack(record);
    const client = invocation.client;
    if (!client) return { authorized: false, currentPolicy: yield* policy() };
    const rows = yield* sql<{ scopes: string }>`SELECT scopes FROM auth_sessions WHERE session_id=${client.sessionId}
      AND subject='mcp-client' AND method='bearer-access-token' AND revoked_at IS NULL AND expires_at>${yield* nowIso}`;
    const scopes: unknown = rows[0] ? decodeJson(rows[0].scopes) : [];
    const authorized = Array.isArray(scopes) && scopes.includes("orchestration:operate");
    yield* assertUnfenced(client.sessionId,record.idempotencyKey);
    if (authorized) yield* validateTarget(record);
    const attemptsAvailable = yield* presence("jones_remote_admission_attempts");
    const child = attemptsAvailable && (yield* sql`SELECT 1 FROM jones_remote_admission_attempts WHERE child_admission_id=${record.id}`).length > 0;
    const childAllowed = !child || (recovery !== null && (yield* recovery.guardChildInTransaction(record)));
    const managedAuthority = Option.isSome(recoveryEvidence) && (yield* recoveryEvidence.value.authorize(record));
    return { authorized: authorized && childAllowed && managedAuthority, currentPolicy: yield* policy() };
  });
  // Admissions this process claimed for dispatch. A handler that dies in this process leaves
  // its claim visible to the sender and owner, as before. A `dispatching` row outside this set
  // was claimed by an earlier process, so its handler cannot still be running.
  const tracker = yield* Effect.serviceOption(ReceiverClaimTracker);
  const claimedHere = Option.isSome(tracker) ? tracker.value : new Set<string>();
  const finishClaim = (record: RemoteAdmissionRecord) =>
    admissions.markDispatched({
      id: record.id,
      bindingDigest: record.bindingDigest,
      commandId: dispatchCommandId(record.id),
    });
  const settleInterrupted = Effect.fn("RemoteAdmissionIntake.settleInterrupted")(function* (
    record: RemoteAdmissionRecord,
  ) {
    // A concurrent settle may win the insert; read back whichever result was recorded.
    yield* sql.withTransaction(Effect.gen(function* () {
    yield* sql`INSERT OR IGNORE INTO jones_remote_admission_results (admission_id, result_json) VALUES (${record.id}, ${encodeJson({ ok: false, value: interruptedFailure() })})`;
    if (yield* presence("jones_remote_admission_extensions")) {
      yield* sql`INSERT INTO jones_remote_admission_extensions(admission_id,outcome) VALUES(${record.id},'unknown') ON CONFLICT(admission_id) DO UPDATE SET outcome=COALESCE(outcome,'unknown')`;
      yield* sql`INSERT INTO jones_remote_admission_peer_revisions(session_id,revision) VALUES(${record.envelope.identity.clientId},1)
        ON CONFLICT(session_id) DO UPDATE SET revision=revision+1`;
    }
    yield* finishClaim(record);
    }));
    return (yield* result(record.id))!;
  });
  const deliver = (saved: { ok: boolean; value: unknown }) =>
    saved.ok
      ? Effect.succeed(saved.value)
      : decodeOrchestratorMcpFailure(saved.value).pipe(
          Effect.flatMap((error) => Effect.fail(error)),
        );
  const validateTarget = Effect.fn("RemoteAdmissionIntake.validateTarget")(function* (
    record: RemoteAdmissionRecord,
  ) {
    const { params, invocation, target } = unpack(record);
    const resolveTarget = targets.get(record.envelope.operation);
    if (!resolveTarget || stable(yield* resolveTarget(params, invocation)) !== stable(target)) {
      return yield* new OrchestratorMcpFailure({
        code: "request_conflict",
        message: "The receiving target configuration changed. Submit a new request.",
      });
    }
  });
  const dispatchOnce = Effect.fn("RemoteAdmissionIntake.dispatch")(function* (
    record: RemoteAdmissionRecord,
  ) {
    const clientId=record.envelope.identity.clientId;
    yield* assertUnfenced(clientId,record.idempotencyKey);
    const prior = yield* result(record.id);
    if (prior) {
      // A result was recorded but the claim was not closed: finish it, so the row can leave
      // `dispatching`. Closing is idempotent if the live handler is closing it too.
      if (record.status === "dispatching") yield* finishClaim(record);
      return yield* deliver(prior);
    }
    const handler = handlers.get(record.envelope.operation);
    if (!handler) return yield* pending(record);
    const { params, invocation } = unpack(record);
    if (invocation.client) yield* assertUnfenced(invocation.client.sessionId, record.idempotencyKey);
    yield* validateTarget(record);
    const commandId = dispatchCommandId(record.id);
    const claim = yield* withSettings(() => sql.withTransaction(Effect.gen(function* () {
      const hasAttempt=(yield* presence("jones_remote_admission_attempts")) &&
        (yield* sql`SELECT 1 FROM jones_remote_admission_attempts WHERE child_admission_id=${record.id}`).length > 0;
      if (hasAttempt && recovery !== null && !(yield* recovery.guardChildInTransaction(record))) {
        const closed=yield* sql`SELECT 1 FROM jones_remote_admission_attempts WHERE child_admission_id=${record.id} AND state IN ('refused','expired')`;
        if (closed.length) {
          yield* sql`UPDATE jones_remote_admissions SET status='denied',reason='Retry evidence changed or the prepared attempt expired.',category='out_of_scope',updated_at=${yield* nowIso}
            WHERE id=${record.id} AND status IN ('passed','approved')`;
          yield* admissions.access.bumpInTransaction('attempt',record.id,yield* nowIso,record.envelope.identity.clientId);
        }
        return {record:(yield* admissions.get(record.id))!,claimed:false};
      }
      const claimed = yield* admissions.claimDispatch({
      id: record.id, bindingDigest: record.bindingDigest, commandId,
      currentPolicy: record.envelope.receiverPolicy,
      checkCurrentAuthority: current => currentAuthority(current).pipe(
        Effect.mapError(cause => new RemoteAdmission.RemoteAdmissionPersistenceError({ cause }))),
      });
      if (claimed.claimed && (yield* presence("jones_remote_admission_attempts"))) yield* sql`UPDATE jones_remote_admission_attempts SET state='claimed' WHERE child_admission_id=${record.id} AND state='origin_registered'`;
      return claimed;
    })));
    if (!claim.claimed) {
      // Claimed by a process that has since stopped: close it with a recorded failure
      // instead of leaving the sender to retry against `dispatching` forever.
      if (claim.record.status === "dispatching" && !claimedHere.has(record.id))
        return yield* deliver(yield* settleInterrupted(claim.record));
      return yield* pending(claim.record);
    }
    claimedHere.add(record.id);
    // A claim is never blindly replayed after interruption. A recorded result is
    // reusable; an unknown handler effect left by an earlier process is closed as interrupted
    // (settleInterrupted).
    const outcome = yield* handler(params, invocation).pipe(
      Effect.map((value) => ({ ok: true, value })),
      Effect.catchIf(Schema.is(OrchestratorMcpFailure), (error) =>
        Effect.succeed({ ok: false, value: error }),
      ),
    );
    const launchResult = record.envelope.operation === 't3_thread_launch' && Option.isSome(recoveryEvidence)
      ? yield* recoveryEvidence.value.read(record).pipe(Effect.map(raw => classifyLaunchEvidence({...raw,liveClaim:false}).result),
          Effect.orElseSucceed(() => null)) : null;
    yield* sql.withTransaction(Effect.gen(function* () {
      yield* sql`INSERT INTO jones_remote_admission_results (admission_id, result_json) VALUES (${record.id}, ${encodeJson({...outcome,...(launchResult ? {launchResult} : {})})})`;
      const now = yield* nowIso;
      if (yield* presence("jones_remote_admission_extensions")) {
        yield* sql`INSERT INTO jones_remote_admission_extensions(admission_id,outcome,terminal_at)
          VALUES(${record.id},${launchResult ? launchResult.outcome === 'unknown' ? 'unknown' : launchResult.outcome === 'created' ? 'success' : 'failure' : outcome.ok ? 'success' : 'unknown'},${now})
          ON CONFLICT(admission_id) DO UPDATE SET outcome=excluded.outcome,terminal_at=excluded.terminal_at`;
        if (launchResult?.outcome === 'created' && launchResult.threadId) {
          const grants=yield* sql<{generation:number}>`SELECT generation FROM jones_remote_admission_grants WHERE admission_id=${record.id}`;
          if (grants[0]) yield* admissions.access.activateInTransaction({admissionId:record.id,expectedGeneration:grants[0].generation,destinationThreadId:launchResult.threadId,now});
        }
        yield* sql`UPDATE jones_remote_admission_attempts SET state=${launchResult?.outcome === 'created' ? 'completed' : 'refused'},terminal_at=${now} WHERE child_admission_id=${record.id} AND state='claimed'`;
        yield* sql`INSERT INTO jones_remote_admission_peer_revisions(session_id,revision) VALUES(${record.envelope.identity.clientId},1)
          ON CONFLICT(session_id) DO UPDATE SET revision=revision+1`;
      }
      yield* admissions.markDispatched({ id: record.id, bindingDigest: record.bindingDigest, commandId });
    }));
    claimedHere.delete(record.id);
    return yield* deliver(outcome);
  });
  const entering = new Set<string>();
  const dispatch = (record:RemoteAdmissionRecord) => Effect.suspend(() => {
    if (entering.has(record.id)) return Effect.fail(pending(record));
    entering.add(record.id);
    return dispatchOnce(record).pipe(Effect.ensuring(Effect.sync(() => entering.delete(record.id))));
  });
  // Admissions still `dispatching` when the server starts were claimed by a handler that
  // died with the previous process. Close each: its result if one was recorded, otherwise
  // the interrupted failure.
  const recoverDispatching = Effect.fn("RemoteAdmissionIntake.recoverDispatching")(function* () {
    const rows = yield* sql<{
      id: string;
    }>`SELECT id FROM jones_remote_admissions WHERE status = 'dispatching' ORDER BY created_at LIMIT ${PRUNE_BATCH_SIZE}`;
    for (const row of rows) {
      if (claimedHere.has(row.id)) continue;
      const record = yield* admissions.get(row.id);
      if (record?.status !== "dispatching") continue;
      if ((yield* result(record.id)) === null) yield* settleInterrupted(record);
      else yield* finishClaim(record);
    }
  });
  const pruneFinished=Effect.fn('RemoteAdmissionIntake.pruneFinished')(function* (now:string) {
    const cutoff=new Date(Date.parse(now)-30*86400000).toISOString();
    const review=yield* presence('jones_remote_admission_extensions');
    const dependencies=yield* presence('jones_thread_result_requests');
    for (;;) {
      const removed=yield* sql.withTransaction(Effect.gen(function* () {
        const rows=yield* sql<{id:string;status:string;updated_at:string;result_json:string|null}>`SELECT a.id,a.status,a.updated_at,r.result_json FROM jones_remote_admissions a
          LEFT JOIN jones_remote_admission_results r ON r.admission_id=a.id
          WHERE a.status IN ('denied','cancelled','dispatched') AND a.updated_at<${cutoff} ORDER BY a.updated_at,a.id`;
        const ids:string[]=[];
        for (const row of rows) {
          if (dependencies && (yield* sql`SELECT 1 FROM jones_thread_result_requests request JOIN jones_remote_admissions a
            ON request.origin_environment_id=a.origin_environment_id AND request.operation_id=a.operation_id
            WHERE a.id=${row.id} AND request.state='pending'`).length) continue;
          if (review) {
            const ext=(yield* sql<{outcome:string|null;terminal_at:string|null}>`SELECT outcome,terminal_at FROM jones_remote_admission_extensions WHERE admission_id=${row.id}`)[0];
            if (ext?.terminal_at && ext.terminal_at >= cutoff) continue;
            if (ext?.outcome === 'unknown' && !(yield* sql`SELECT 1 FROM jones_remote_admission_recovered_results
              WHERE admission_id=${row.id} AND json_extract(result_json,'$.outcome') IN ('created','refused','not_created')`).length) continue;
            if ((yield* sql`SELECT 1 FROM jones_remote_admission_recovered_results WHERE admission_id=${row.id} AND created_at>=${cutoff}`).length) continue;
            if ((yield* sql`SELECT 1 FROM jones_remote_admission_recovered_notices WHERE admission_id=${row.id} AND state NOT IN ('delivered','suppressed','closed_unknown')`).length) continue;
            if ((yield* sql`SELECT 1 FROM jones_remote_admission_grants WHERE admission_id=${row.id}`).length) continue;
            if ((yield* sql`SELECT 1 FROM jones_remote_admission_attempts WHERE (child_admission_id=${row.id} OR predecessor_admission_id=${row.id}) AND state IN ('prepared','origin_registered','claimed')`).length) continue;
          }
          if (row.status === 'dispatched') {
            const saved=row.result_json ? yield* decodeStoredResult(row.result_json) : null;
            if (!saved || (!saved.ok && saved.launchResult?.outcome !== 'refused' && saved.launchResult?.outcome !== 'not_created')) continue;
            if (saved.launchResult?.outcome === 'unknown') continue;
          }
          ids.push(row.id);if (ids.length===PRUNE_BATCH_SIZE) break;
        }
        if (!ids.length) return 0;
        yield* sql`DELETE FROM jones_remote_admission_results WHERE admission_id IN ${sql.in(ids)}`;
        yield* sql`DELETE FROM jones_remote_admissions WHERE id IN ${sql.in(ids)}`;
        return ids.length;
      }));
      if (removed<PRUNE_BATCH_SIZE) return;
    }
  });
  const snapshot = Effect.fn("RemoteAdmissionIntake.snapshot")(function* () {
    return {
      eligible: yield* eligible,
      policy: yield* policy(),
      pending: yield* admissions.listPending(),
    };
  }, Effect.mapError(intakeError));
  let reactorStarted=false;
  const drainApproved = Effect.gen(function* () {
    const rows=yield* sql<{id:string}>`SELECT id FROM jones_remote_admissions WHERE status IN ('passed','approved') ORDER BY created_at`;
    for (const row of rows) {
      if (entering.has(row.id)) continue;
      const record=yield* admissions.get(row.id);
      if (record) yield* dispatch(record).pipe(Effect.ignore,Effect.forkIn(serviceScope));
    }
  });
  return RemoteAdmissionIntake.of({
    isLiveClaim: id => claimedHere.has(id),
    register: (operation, handler, target) => {
      handlers.set(operation, handler);
      targets.set(operation, target);
    },
    invoke: Effect.fn("RemoteAdmissionIntake.invoke")(
      function* (operation, params, invocation) {
        const handler = handlers.get(operation);
        if (!handler)
          return yield* new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "The receiving operation is unavailable.",
          });
        const current = yield* policy();
        if (!invocation.client) return yield* handler(params, invocation);
        const keyed = typeof params.clientRequestId === "string";
        const key = hash({
          namespace: invocation.requestNamespace,
          operation,
          key: keyed ? params.clientRequestId : NodeCrypto.randomUUID(),
        });
        const client = invocation.client;
        const selectPrevious = Effect.fn("RemoteAdmissionIntake.selectPrevious")(function* () {
          const rows = yield* sql<{
            id: string;
          }>`SELECT id FROM jones_remote_admissions WHERE client_id = ${client.sessionId} AND idempotency_key = ${key}`;
          return rows[0] ? yield* admissions.get(rows[0].id) : null;
        });
        const previous = keyed ? yield* selectPrevious() : null;
        yield* assertUnfenced(client.sessionId,key);
        const message = operation === "t3_thread_send" || operation === "t3_thread_send_attachments";
        const managed = message && typeof params.threadId === "string" && (yield* presence("jones_remote_managed_scopes"))
          ? (yield* sql`SELECT 1 FROM jones_remote_managed_scopes WHERE destination_thread_id=${params.threadId} LIMIT 1`).length > 0 : false;
        if (!current.enabled && previous === null && !managed) return yield* handler(params, invocation);
        const resolveTarget = targets.get(operation);
        if (!resolveTarget) return yield* intakeError("target_unavailable");
        // Stable receiver-owned scope is bound with the exact decoded request.
        // Arrival timestamps vary on retries and are not authorization identity.
        // Retry bytes use the reviewed snapshot; dispatch revalidates current state.
        const reviewedTarget = previous
          ? unpack(previous).target
          : yield* resolveTarget(params, invocation);
        const bound = yield* readBoundLinkedRequestContext(invocation);
        const submit = Effect.fn("RemoteAdmissionIntake.submit")(function* (
          prior: RemoteAdmissionRecord | null,
        ) {
          const oldPayload = prior?.envelope.payload as Record<string, unknown> | undefined;
          const advisory = oldPayload
            ? Object.hasOwn(oldPayload, "advisory")
              ? { advisory: oldPayload.advisory }
              : {}
            : bound
              ? {
                  advisory: {
                    schemaVersion: "t3.remote-admission-advisory/v1",
                    requestContext: bound.context,
                  },
                }
              : {};
          const payload = decodeJson(
            stable({
              params,
              invocation: storedScope(prior ? pinOriginTitle(invocation, prior) : invocation),
              target: prior ? unpack(prior).target : reviewedTarget,
              ...advisory,
            }),
          );
          const origin = client.linked && client.remoteOrigin?.linkSessionId === client.sessionId ? client.remoteOrigin : undefined;
          const accessBinding: RemoteAdmissionAccessBinding | undefined = origin?.thread &&
            (message || operation === "t3_thread_launch") ? {
              receiverEnvironmentId: invocation.environmentId, sessionId: client.sessionId,
              originEnvironmentId: origin.environmentId, originThreadId: origin.thread.threadId,
              destinationThreadId: message && typeof params.threadId === "string" ? params.threadId : null,
              operation: "t3_thread_send",
              runtimeMode: typeof params.runtimeMode === "string" ? params.runtimeMode : McpInvocationContext.clientModeCeiling(client).runtimeMode,
              interactionMode: typeof params.interactionMode === "string" ? params.interactionMode : "default",
            } : undefined;
          return yield* admissions.request({
            ...(accessBinding ? { accessBinding } : {}),
            idempotencyKey: key,
            envelope: {
              identity: {
                clientId: client.sessionId,
                linkId: client.linked ? client.sessionId : null,
              },
              operation,
              payload,
              target: {
                projectId: typeof params.projectId === "string" ? params.projectId : null,
                threadId: typeof params.threadId === "string" ? params.threadId : null,
                runtimeMode:
                  typeof params.runtimeMode === "string"
                    ? params.runtimeMode
                    : McpInvocationContext.clientModeCeiling(client).runtimeMode,
                interactionMode:
                  typeof params.interactionMode === "string" ? params.interactionMode : "default",
              },
              receiverPolicy: current,
            },
          });
        });
        const record = yield* submit(previous).pipe(
          Effect.catchTags({
            RemoteAdmissionKeyConflict: (error) =>
              keyed && previous === null
                ? selectPrevious().pipe(
                    Effect.flatMap((winner) =>
                      winner === null ? Effect.fail(error) : submit(winner),
                    ),
                  )
                : Effect.fail(error),
          }),
        );
        if (["passed", "approved", "dispatching", "dispatched"].includes(record.status))
          return yield* dispatch(record);
        return yield* pending(record);
      },
      Effect.catchTags({
        RemoteAdmissionKeyConflict: () =>
          Effect.fail(
            new OrchestratorMcpFailure({
              code: "request_conflict",
              message:
                "The same request key was used with different content. The original request is unchanged; the changed request was not executed.",
            }),
          ),
      }),
      Effect.catch((error) =>
        Effect.fail(
          isOrchestratorMcpFailure(error)
            ? error
            : new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Remote request admission could not be completed.",
              }),
        ),
      ),
    ),
    snapshot,
    attention: Effect.fn("RemoteAdmissionIntake.attention")(() =>
      sql<{
        count: number;
      }>`SELECT COUNT(*) AS count FROM jones_remote_admissions WHERE status = 'held'`.pipe(
        Effect.map((rows) => ({ count: rows[0]?.count ?? 0 })),
        Effect.mapError(intakeError),
      ),
    ),
    withdraw: Effect.fn("RemoteAdmissionIntake.withdraw")((input, clientId) =>
      admissions.withdraw({ ...input, clientId }).pipe(
        Effect.catchTags({
          RemoteAdmissionStateConflict: (error) =>
            error.status === "dispatching" ||
            error.status === "dispatched" ||
            error.status === "denied"
              ? admissions.get(input.id).pipe(
                  Effect.filterOrFail(
                    (record) => record !== null,
                    () => error,
                  ),
                )
              : Effect.fail(error),
        }),
        Effect.map(publicRemoteAdmission),
        Effect.mapError(intakeError),
      ),
    ),
    updatePolicy: Effect.fn("RemoteAdmissionIntake.updatePolicy")(function* (input) {
      yield* sql.withTransaction(
        Effect.gen(function* () {
          const current = yield* policy();
          if (input.expectedRevision !== current.revision)
            return yield* intakeError("policy_changed");
          if (input.enabled && input.modelSelection === null)
            return yield* intakeError("model_required");
          if (input.enabled && input.modelSelection !== null)
            yield* resolveRemoteEvaluationModel(input.modelSelection).pipe(
              Effect.provideService(ProviderInstanceRegistry.ProviderInstanceRegistry, providers),
            );
          const next = {
            enabled: input.enabled,
            modelSelection: input.modelSelection,
            revision: NodeCrypto.randomUUID(),
          };
          yield* sql`INSERT INTO jones_remote_admission_policy (singleton, policy_json) VALUES (1, ${encodeJson(next)}) ON CONFLICT(singleton) DO UPDATE SET policy_json = excluded.policy_json`;
        }),
      );
      return yield* snapshot();
    }, Effect.mapError(intakeError)),
    decide: Effect.fn("RemoteAdmissionIntake.decide")(function* (input, owner) {
      if (!isRemoteAdmissionOwner(owner)) return yield* new RemoteAdmission.RemoteAdmissionForbidden({});
      const committed = yield* withSettings(() => sql.withTransaction(Effect.gen(function* () {
        const existing = yield* admissions.get(input.id);
        if (!existing) return yield* new RemoteAdmission.RemoteAdmissionNotFound({});
        if (existing.bindingDigest !== input.bindingDigest) return yield* new RemoteAdmission.RemoteAdmissionBindingMismatch({});
        if (input.decision === 'approve' && ['approved','dispatching','dispatched'].includes(existing.status)) return existing;
        if (input.decision === "approve") {
          const authority = yield* currentAuthority(existing);
          if (!authority.authorized) return yield* new RemoteAdmission.RemoteAdmissionForbidden({});
          if (stable(existing.envelope.receiverPolicy) !== stable(authority.currentPolicy))
            return yield* new RemoteAdmission.RemoteAdmissionPolicyChanged({});
        }
        const decided = yield* admissions.resolve({ id: input.id, bindingDigest: input.bindingDigest,
          decision: input.decision, principal: { kind: "human-owner", subject: owner.sessionId },
          ...(input.decision === "approve" && "grant" in input && input.grant
            ? { durationSeconds: input.grant.durationSeconds } : {}),
        });
        if (yield* presence('jones_remote_admission_peer_revisions')) {
          yield* sql`INSERT INTO jones_remote_admission_peer_revisions(session_id,revision) VALUES(${existing.envelope.identity.clientId},1)
            ON CONFLICT(session_id) DO UPDATE SET revision=revision+1`;
          yield* admissions.access.invalidate;
        }
        return decided;
      })), input.decision === "approve");
      if (committed.status === "approved") yield* dispatch(committed).pipe(
        Effect.catch(cause => Effect.logWarning("Remote admission reactor deferred dispatch", { cause })),
        Effect.forkIn(serviceScope));
      return committed;
    }, Effect.mapError(intakeError)),
    resumeApproved: Effect.fn("RemoteAdmissionIntake.resumeApproved")(function* () {
      // Startup recovery of this receiver's own state, before approved requests resume.
      yield* recoverDispatching().pipe(
        Effect.catch((cause) =>
          Effect.logWarning("Unable to close interrupted remote admissions.", { cause }),
        ),
      );
      yield* nowIso.pipe(
        Effect.flatMap(now => (presence("jones_remote_admission_extensions").pipe(Effect.flatMap(present => present ? admissions.retention.prune(now) : Effect.void),Effect.andThen(pruneFinished(now))))),
        Effect.catch((cause) =>
          Effect.logWarning("Unable to prune finished remote admissions.", { cause }),
        ),
      );
      if (!reactorStarted) {
        reactorStarted=true;
        yield* drainApproved.pipe(Effect.catch(cause => Effect.logWarning("Remote admission reactor scan deferred",{cause})),
          Effect.repeat(Schedule.spaced('1 second')),Effect.forkIn(serviceScope));
      }
    }, Effect.mapError(intakeError)),
  });
});
export const layer = Layer.effect(RemoteAdmissionIntake, make);
