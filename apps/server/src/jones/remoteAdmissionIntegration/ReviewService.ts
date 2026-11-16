import { managedAccessGrantId } from "./managedAccessLink.ts";
import type { ReviewChanges } from "../remoteAdmission/ReviewChanges.ts";
import * as Buffer from "node:buffer";
import { findGrantSourceAdmission } from "./grantSource.ts";
import { lookupRestoreCommand } from "./restoreReadback.ts";
import { recordedLaunchResult } from "./recordedResult.ts";
import { classifyLaunchEvidence } from "./LaunchRecovery.ts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import * as DTO from "@t3tools/contracts/jones/remoteAdmissionReview";
import { publicRemoteAdmission, type EnvironmentSessionPrincipalShape } from "@t3tools/contracts";
import * as RemoteAdmission from "../remoteAdmission/RemoteAdmission.ts";
import type { Grant } from "../remoteAdmission/ReceiverAccess.ts";
import { ReceiverAccessUnavailable } from "../remoteAdmission/ReceiverAccess.ts";
import { ReviewHistoryUnavailable } from "../remoteAdmission/ReviewProjection.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as Intake from "./RemoteAdmissionIntake.ts";
import { isRemoteAdmissionOwner } from "./owner.ts";
import { makeRecoveryStore, ReceiverRecoveryEvidence, RecoveryConflict } from "./RecoveryStore.ts";

/** Current authority/liveness reads must use the caller's same SQL transaction. */
export class ReceiverReviewReaders extends Context.Service<
  ReceiverReviewReaders,
  {
    readonly receiverEnvironmentId: string;
    readonly liveDestinations: (
      ids: ReadonlyArray<string>,
    ) => Effect.Effect<ReadonlyArray<string>, SqlError>;
    readonly validLeaseIds: (
      grants: ReadonlyArray<Grant>,
    ) => Effect.Effect<ReadonlyArray<string>, SqlError>;
  }
>()("jones/remoteAdmissionIntegration/ReceiverReviewReaders") {}
const unavailable: DTO.RemoteAdmissionReviewUnavailable = {
  code: "item_unavailable",
  message: "This retained request is unavailable.",
};
const forbidden: DTO.RemoteAdmissionReviewForbidden = {
  code: "owner_required",
  message: "Receiving owner access is required.",
};
export function makeReviewService(
  sql: SqlClient.SqlClient,
  admissions: RemoteAdmission.RemoteAdmission["Service"],
  intake: Intake.RemoteAdmissionIntake["Service"],
  settings: ServerSettings.ServerSettingsService["Service"],
  evidence: ReceiverRecoveryEvidence["Service"],
  readers: ReceiverReviewReaders["Service"],
  signals?: ReviewChanges["Service"],
) {
  const recovery = makeRecoveryStore(sql, admissions, evidence);
  const nowIso = DateTime.now.pipe(Effect.map(DateTime.formatIso));
  const owner = (principal: EnvironmentSessionPrincipalShape) =>
    isRemoteAdmissionOwner(principal) ? Effect.void : Effect.fail(forbidden);
  const item = Effect.fn("AdmissionReview.item")(function* (
    id: string,
  ): Effect.fn.Return<DTO.RemoteAdmissionReviewItem, unknown> {
    const record = yield* admissions.get(id);
    if (!record) return yield* Effect.fail(unavailable);
    const now = yield* nowIso;
    const ext = (yield* sql<{
      evaluator_json: string | null;
      attempt_id: string | null;
      outcome: string | null;
    }>`SELECT * FROM jones_remote_admission_extensions WHERE admission_id=${id}`)[0];
    const grant =
      (yield* sql<Grant>`SELECT * FROM jones_remote_admission_grants WHERE admission_id=${id}`)[0];
    const blocked = (yield* sql<{
      managed_scope_id: string;
      reason: string;
    }>`SELECT * FROM jones_remote_admission_blocked_marks WHERE admission_id=${id}`)[0];
    const basis = (yield* sql<{
      kind: string;
      grant_id: string | null;
      generation: number | null;
      binding_digest: string | null;
      scope_revision: number | null;
    }>`SELECT * FROM jones_remote_admission_access_basis WHERE admission_id=${id}`)[0];
    const recoveredResult = yield* recovery.recovered(id);
    const acknowledgment = (yield* sql<{
      acknowledged_at: string;
    }>`SELECT acknowledged_at FROM jones_remote_admission_result_acks WHERE admission_id=${id}`)[0];
    const originalResult = yield* recordedLaunchResult(sql, id);
    const payload = record.envelope.payload as { params?: Record<string, unknown> };
    let retryEligible = false;
    if (
      record.status === "dispatched" &&
      record.envelope.operation === "t3_thread_launch" &&
      typeof payload.params?.clientRequestId === "string" &&
      !(Array.isArray(payload.params.attachments) && payload.params.attachments.length)
    ) {
      const support =
        yield* sql`SELECT 1 FROM jones_remote_peer_capabilities WHERE session_id=${record.envelope.identity.clientId}
        AND json_extract(versions_json,'$.retryObservation')='jones.remote-admission-retry-observation/v1'`;
      const fenced =
        yield* sql`SELECT 1 FROM jones_remote_admission_fences WHERE predecessor_admission_id=${id}`;
      if (support.length && !fenced.length && (yield* evidence.root(record)) !== null)
        retryEligible = yield* evidence.read(record).pipe(
          Effect.map((raw) => classifyLaunchEvidence(raw).result.outcome === "not_created"),
          Effect.orElseSucceed(() => false),
        );
    }
    const grantable =
      ["t3_thread_send", "t3_thread_launch"].includes(record.envelope.operation) &&
      (yield* sql`SELECT 1 FROM jones_remote_admission_extensions WHERE admission_id=${id} AND binding_json IS NOT NULL`)
        .length > 0;
    // Never infer creation from a handler return value or missing projection.
    return yield* Schema.decodeUnknownEffect(DTO.RemoteAdmissionReviewItem)({
      record,
      extension: {
        managedAccessGrantId: yield* managedAccessGrantId(
          sql,
          record,
          readers.receiverEnvironmentId,
        ),
        grantable,
        grantUnavailableReason: grantable ? null : "operation_not_covered",
        grant: grant ? yield* admissions.access.grantView(grant, now) : null,
        evaluator: ext?.evaluator_json ? JSON.parse(ext.evaluator_json) : null,
        blocked: blocked
          ? { admissionId: id, managedScopeId: blocked.managed_scope_id, reason: blocked.reason }
          : null,
        accessBasis: basis
          ? basis.kind === "lease"
            ? { kind: "lease", grantId: basis.grant_id, generation: basis.generation }
            : {
                kind: "permit",
                bindingDigest: basis.binding_digest,
                scopeRevision: basis.scope_revision,
              }
          : null,
        attemptId: ext?.attempt_id ?? null,
        result: originalResult,
        recoveredResult,
        originNotifiedAt: acknowledgment?.acknowledged_at ?? null,
        retryEligible,
        recheckEligible:
          record.envelope.operation === "t3_thread_launch" && record.status === "dispatched",
      },
    });
  });
  const mapFailure = <A>(effect: Effect.Effect<A, unknown>, id?: string, grantId?: string) =>
    effect.pipe(
      Effect.catch((error) =>
        Effect.gen(function* () {
          if (typeof error === "object" && error !== null && "code" in error)
            return yield* Effect.fail(error as DTO.RemoteAdmissionReviewError);
          const cause = Schema.is(Intake.RemoteAdmissionIntakeError)(error) ? error.cause : error;
          const tag =
            typeof cause === "object" && cause !== null && "_tag" in cause ? cause._tag : null;
          if (cause === "linked_environments_disabled")
            return yield* Effect.fail({
              code: "conflict",
              reason: "authority_changed",
              message: "Linked environments are disabled.",
              current: { admission: null, grant: null, chainHead: null, attempt: null },
            } satisfies DTO.RemoteAdmissionReviewConflict);
          if (tag === "RemoteAdmissionForbidden") return yield* Effect.fail(forbidden);
          if (tag === "ReviewHistoryUnavailable")
            return yield* Effect.fail({
              code: "history_unavailable",
              message: "Refresh this view to continue.",
            } satisfies DTO.RemoteAdmissionReviewUnavailable);
          const record = id
            ? yield* admissions.get(id).pipe(Effect.orElseSucceed(() => null))
            : null;
          const grant = grantId
            ? yield* admissions.access.getGrant(grantId).pipe(Effect.orElseSucceed(() => null))
            : null;
          const conflictHead =
            tag === "ReceiverAccessConflict" &&
            typeof cause === "object" &&
            cause !== null &&
            "current" in cause
              ? (cause.current as Grant)
              : null;
          const chainHead =
            conflictHead && typeof conflictHead.grant_id === "string"
              ? yield* admissions.access.grantView(conflictHead, yield* nowIso)
              : null;
          const currentGrant = grant
            ? yield* admissions.access.grantView(grant, yield* nowIso)
            : null;
          if (
            [
              "ReceiverAccessConflict",
              "RecoveryConflict",
              "RemoteAdmissionStateConflict",
              "RemoteAdmissionBindingMismatch",
              "RemoteAdmissionPolicyChanged",
            ].includes(String(tag))
          )
            return yield* Effect.fail({
              code: "conflict",
              reason:
                tag === "ReceiverAccessConflict"
                  ? "generation_changed"
                  : tag === "RemoteAdmissionBindingMismatch"
                    ? "binding_changed"
                    : tag === "RemoteAdmissionPolicyChanged"
                      ? "authority_changed"
                      : tag === "RecoveryConflict" &&
                          typeof cause === "object" &&
                          cause !== null &&
                          "reason" in cause &&
                          [
                            "state_changed",
                            "binding_changed",
                            "generation_changed",
                            "grant_ineligible",
                            "command_changed",
                            "retry_ineligible",
                            "authority_changed",
                          ].includes(String(cause.reason))
                        ? (cause.reason as DTO.RemoteAdmissionReviewConflict["reason"])
                        : "state_changed",
              message: "The receiving state changed. Review the current state.",
              current: {
                admission: record ? publicRemoteAdmission(record) : null,
                grant: currentGrant,
                chainHead: chainHead ?? currentGrant,
                attempt: null,
              },
            } satisfies DTO.RemoteAdmissionReviewConflict);
          if (
            [
              "ReceiverAccessUnavailable",
              "RecoveryUnavailable",
              "RemoteAdmissionNotFound",
            ].includes(String(tag))
          )
            return yield* Effect.fail(unavailable);
          // Persistence and decoding failures stay internal, never become absence or a conflict.
          return yield* Effect.fail(new Intake.RemoteAdmissionIntakeError({ cause: error }));
        }),
      ),
      Effect.mapError((error) =>
        Schema.is(DTO.RemoteAdmissionReviewError)(error)
          ? error
          : Schema.is(Intake.RemoteAdmissionIntakeError)(error)
            ? error
            : new Intake.RemoteAdmissionIntakeError({ cause: error }),
      ),
    );
  const locked = <A, E>(effect: Effect.Effect<A, E>) =>
    settings.withSettingsSnapshot((s) =>
      s.linkedEnvironmentsEnabled
        ? effect
        : Effect.fail({
            code: "conflict",
            reason: "authority_changed",
            message: "Linked environments are disabled.",
            current: { admission: null, grant: null, chainHead: null, attempt: null },
          } satisfies DTO.RemoteAdmissionReviewConflict),
    );
  const page = Effect.fn("AdmissionReview.page")(function* (
    query: DTO.RemoteAdmissionReviewQuery,
    principal: EnvironmentSessionPrincipalShape,
    linkedEnabled = true,
  ) {
    yield* owner(principal);
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const now = yield* nowIso;
        const shared = {
          authorizationScope: principal.sessionId,
          now,
          liveDestinations: readers.liveDestinations,
          validLeaseIds: linkedEnabled ? readers.validLeaseIds : () => Effect.succeed([]),
          projectItem: (id: string) =>
            item(id).pipe(Effect.mapError(() => new ReviewHistoryUnavailable({}))),
        };
        const projected = yield* admissions.review.page({
          ...shared,
          view: query.view === "access" ? "current" : query.view,
          ...(query.view === "access" ? {} : { cursor: query.cursor }),
          limit: query.limit,
        });
        const access =
          query.view === "access"
            ? yield* admissions.review.accessPage({
                authorizationScope: principal.sessionId,
                now,
                cursor: query.cursor,
                limit: query.limit,
                projectGrant: admissions.access.grantView,
              })
            : null;
        const diagnostics = yield* admissions.review.blockedDiagnostics();
        const snapshot = yield* intake.snapshot();
        return yield* Schema.decodeUnknownEffect(DTO.RemoteAdmissionReviewPage)({
          ...projected,
          view: query.view,
          ...(access
            ? {
                items: [],
                grants: access.grants,
                nextCursor: access.nextCursor,
                retainedRecordCount: (yield* sql<{
                  n: number;
                }>`SELECT COUNT(*) n FROM jones_remote_admission_grants`)[0]!.n,
              }
            : { grants: [] }),
          policy: snapshot.policy,
          serverNow: now,
          blockedEvents: diagnostics.events.map((e) => ({
            eventId: e.event_id,
            managedScopeId: e.managed_scope_id,
            admissionId: e.admission_id,
            reason: e.reason,
            firstAt: e.first_at,
            lastAt: e.last_at,
            repeatCount: e.repeat_count,
            reviewLink: {
              kind: "request",
              receiverEnvironmentId: readers.receiverEnvironmentId,
              admissionId: e.admission_id,
            },
          })),
          blockedSummaries: diagnostics.summaries.map((s) => ({
            managedScopeId: s.managed_scope_id,
            reason: s.reason,
            logicalEvents: s.logical_events,
            repeatArrivals: s.repeat_arrivals,
            firstAt: s.first_at,
            lastAt: s.last_at,
          })),
          blockedDiagnosticCounts: diagnostics.counts,
        });
      }),
    );
  });
  const changes = Effect.fn("AdmissionReview.changes")(function* (
    input: DTO.RemoteAdmissionChangesQuery,
    p: EnvironmentSessionPrincipalShape,
  ) {
    yield* owner(p);
    const encode = (revision: number) =>
      Buffer.Buffer.from(JSON.stringify({ scope: p.sessionId, revision })).toString("base64url");
    let after: number;
    if (input.after) {
      const cursor = yield* Effect.try({
        try: () =>
          Schema.decodeUnknownSync(Schema.Struct({ scope: Schema.String, revision: Schema.Int }))(
            JSON.parse(Buffer.Buffer.from(input.after!, "base64url").toString("utf8")),
          ),
        catch: () => new ReviewHistoryUnavailable({}),
      });
      if (cursor.scope !== p.sessionId) return yield* new ReviewHistoryUnavailable({});
      after = cursor.revision;
    } else after = (yield* admissions.review.revision()).revision;
    const read = settings.withSettingsSnapshot((currentSettings) =>
      sql.withTransaction(
        Effect.gen(function* () {
          if (signals) {
            const active = yield* sql<{
              scopes: string;
            }>`SELECT scopes FROM auth_sessions WHERE session_id=${p.sessionId} AND subject=${p.subject}
          AND method=${p.method} AND revoked_at IS NULL AND expires_at>${yield* nowIso}`;
            const scopes = active[0]
              ? yield* Schema.decodeUnknownEffect(
                  Schema.fromJsonString(Schema.Array(Schema.String)),
                )(active[0].scopes)
              : [];
            if (!scopes.includes("access:write") || !scopes.includes("settings:write"))
              return yield* Effect.fail({
                code: "review_unavailable",
                message: "Request review is unavailable for this session.",
              } satisfies DTO.RemoteAdmissionReviewUnauthorized);
          }
          const delta = yield* admissions.review.changes(after);
          const snapshot = yield* page(
            { view: "current", limit: 1 },
            p,
            currentSettings.linkedEnvironmentsEnabled,
          );
          return {
            after: encode(delta.next),
            snapshotRevision: String(delta.revision),
            counts: snapshot.counts,
            serverNow: yield* nowIso,
            changes: delta.items.map((row) => ({
              sequence: String(row.revision),
              kind:
                row.kind === "restore" ? "grant" : (row.kind as DTO.RemoteAdmissionChange["kind"]),
              link:
                row.kind === "grant" || row.kind === "restore"
                  ? {
                      kind: "access" as const,
                      receiverEnvironmentId: readers.receiverEnvironmentId,
                      grantId: row.entity_id,
                    }
                  : {
                      kind: "request" as const,
                      receiverEnvironmentId: readers.receiverEnvironmentId,
                      admissionId: row.entity_id,
                    },
            })),
          } satisfies DTO.RemoteAdmissionChanges;
        }),
      ),
    );
    return yield* signals
      ? signals.waitFor({
          read,
          isReady: (response) => !input.after || response.changes.length > 0,
          timeoutMs: input.timeoutMs ?? 0,
        })
      : read;
  });
  return {
    capabilities: (principal: EnvironmentSessionPrincipalShape) =>
      Effect.succeed({
        version: DTO.REMOTE_ADMISSION_REVIEW_VERSION,
        features: {
          reviewV2: true,
          grants: true,
          conflicts: true,
          changeFeed: signals !== undefined,
          grantRestore: true,
          blockedAudit: true,
          blockedReview: true,
          history: true,
          launchRetry: true,
          recoveredResult: true,
          managedAccess: true,
        },
        principal: { canDecide: isRemoteAdmissionOwner(principal) },
      } satisfies DTO.RemoteAdmissionReviewCapabilities),
    page: (q: DTO.RemoteAdmissionReviewQuery, p: EnvironmentSessionPrincipalShape) =>
      mapFailure(
        settings.withSettingsSnapshot((current) => page(q, p, current.linkedEnvironmentsEnabled)),
      ),
    item: (id: string, p: EnvironmentSessionPrincipalShape) =>
      mapFailure(owner(p).pipe(Effect.andThen(sql.withTransaction(item(id)))), id),
    decide: (input: DTO.RemoteAdmissionReviewDecision, p: EnvironmentSessionPrincipalShape) =>
      mapFailure(
        Effect.gen(function* () {
          yield* owner(p);
          const record = yield* intake.decide(input, p);
          const grants =
            yield* sql<Grant>`SELECT * FROM jones_remote_admission_grants WHERE admission_id=${record.id}`;
          return {
            committed: true as const,
            record,
            grant: grants[0] ? yield* admissions.access.grantView(grants[0], yield* nowIso) : null,
          };
        }),
        input.id,
      ),
    grants: (input: DTO.RemoteAdmissionGrantQuery, p: EnvironmentSessionPrincipalShape) =>
      mapFailure(
        owner(p).pipe(
          Effect.andThen(
            sql.withTransaction(
              Effect.gen(function* () {
                const now = yield* nowIso;
                if (input.restoreCommandKey !== undefined && !input.grantId)
                  return yield* Effect.fail({
                    code: "invalid_request",
                    message: "grantId is required for restore command readback.",
                  } satisfies DTO.RemoteAdmissionReviewBadRequest);
                if (input.restoreCommandKey !== undefined && input.grantId) {
                  const successor = yield* lookupRestoreCommand(
                    sql,
                    admissions.access,
                    input.grantId,
                    input.restoreCommandKey,
                  );
                  if (!successor)
                    return yield* Effect.fail({
                      code: "grant_unavailable",
                      message: "This retained restore command is unavailable.",
                    } satisfies DTO.RemoteAdmissionReviewUnavailable);
                  return {
                    snapshotRevision: String((yield* admissions.review.revision()).revision),
                    grants: [yield* admissions.access.grantView(successor, now)],
                    nextCursor: null,
                    serverNow: now,
                  };
                }
                if (input.grantId) {
                  const grant = yield* admissions.access.getGrant(input.grantId);
                  if (!grant) return yield* Effect.fail(unavailable);
                  return {
                    snapshotRevision: String((yield* admissions.review.revision()).revision),
                    grants: [yield* admissions.access.grantView(grant, now)],
                    nextCursor: null,
                    serverNow: now,
                  };
                }
                return yield* admissions.review.accessPage({
                  authorizationScope: p.sessionId,
                  cursor: input.cursor,
                  limit: input.limit,
                  now,
                  projectGrant: admissions.access.grantView,
                });
              }),
            ),
          ),
        ),
      ),
    mutateGrant: (
      input: DTO.RemoteAdmissionGrantUpdate | DTO.RemoteAdmissionGrantRevoke,
      action: "update" | "revoke",
      p: EnvironmentSessionPrincipalShape,
    ) =>
      mapFailure(
        owner(p).pipe(
          Effect.andThen(
            locked(
              sql.withTransaction(
                Effect.gen(function* () {
                  const now = yield* nowIso;
                  if (action === "update") {
                    const old = yield* admissions.access.getGrant(input.grantId);
                    const source = old
                      ? yield* findGrantSourceAdmission(sql, admissions, old)
                      : null;
                    if (!old || !source) return yield* Effect.fail(unavailable);
                    const binding = yield* Schema.decodeUnknownEffect(
                      Schema.fromJsonString(DTO.RemoteAdmissionAccessBinding),
                    )(old.binding_json);
                    const current = {
                      ...source,
                      envelope: {
                        ...source.envelope,
                        target: {
                          ...source.envelope.target,
                          threadId: binding.destinationThreadId,
                          runtimeMode: binding.runtimeMode,
                          interactionMode: binding.interactionMode,
                        },
                      },
                    };
                    if (!(yield* evidence.authorize(current)))
                      return yield* new RecoveryConflict({ reason: "authority_changed" });
                  }
                  const grant = yield* admissions.access.mutateGrant({ ...input, action, now });
                  return yield* admissions.access.grantView(grant, now);
                }),
              ),
            ),
          ),
        ),
        undefined,
        input.grantId,
      ),
    restore: (input: DTO.RemoteAdmissionGrantRestore, p: EnvironmentSessionPrincipalShape) =>
      mapFailure(
        owner(p).pipe(
          Effect.andThen(
            locked(
              Effect.gen(function* () {
                const old = yield* admissions.access.getGrant(input.grantId);
                if (!old) return yield* Effect.fail(unavailable);
                const now = yield* nowIso;
                const successor = yield* admissions.access.restore({
                  predecessorId: input.grantId,
                  expectedGeneration: input.expectedGeneration,
                  commandKey: input.commandKey,
                  durationSeconds: input.durationSeconds,
                  binding: Schema.decodeUnknownSync(
                    Schema.fromJsonString(DTO.RemoteAdmissionAccessBinding),
                  )(old.binding_json),
                  now,
                  authorize: (binding) =>
                    Effect.gen(function* () {
                      const record = yield* findGrantSourceAdmission(sql, admissions, old);
                      if (!record || !binding.destinationThreadId) return false;
                      const current = {
                        ...record,
                        envelope: {
                          ...record.envelope,
                          target: {
                            ...record.envelope.target,
                            threadId: binding.destinationThreadId,
                            runtimeMode: binding.runtimeMode,
                            interactionMode: binding.interactionMode,
                          },
                        },
                      };
                      return (
                        (yield* evidence.authorize(current)) &&
                        (yield* readers.liveDestinations([binding.destinationThreadId])).includes(
                          binding.destinationThreadId,
                        )
                      );
                    }).pipe(Effect.mapError(() => new ReceiverAccessUnavailable({}))),
                });
                return {
                  predecessorGrantId: input.grantId,
                  commandKey: input.commandKey,
                  successor: yield* admissions.access.grantView(successor, now),
                };
              }),
            ),
          ),
        ),
        undefined,
        input.grantId,
      ),
    retry: (input: DTO.RemoteAdmissionRetryInput, p: EnvironmentSessionPrincipalShape) =>
      mapFailure(owner(p).pipe(Effect.andThen(locked(recovery.retry(input)))), input.id),
    recheck: (input: DTO.RemoteAdmissionRecheckInput, p: EnvironmentSessionPrincipalShape) =>
      mapFailure(
        owner(p).pipe(
          Effect.andThen(
            locked(
              Effect.gen(function* () {
                const recoveredResult = yield* recovery.recheck(input);
                return { item: yield* item(input.id), recoveredResult };
              }),
            ),
          ),
        ),
        input.id,
      ),
    changes: (input: DTO.RemoteAdmissionChangesQuery, p: EnvironmentSessionPrincipalShape) =>
      mapFailure(changes(input, p)),
  };
}
export class RemoteAdmissionReviewService extends Context.Service<
  RemoteAdmissionReviewService,
  ReturnType<typeof makeReviewService>
>()("jones/remoteAdmissionIntegration/RemoteAdmissionReviewService") {}
