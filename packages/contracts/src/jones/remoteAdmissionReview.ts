import * as Schema from "effect/Schema";
import * as HttpApiEndpoint from "effect/http-api/HttpApiEndpoint";
import * as HttpApiGroup from "effect/http-api/HttpApiGroup";
import * as HttpApiSchema from "effect/http-api/HttpApiSchema";
import type { makeJonesHttpGroups } from "./environmentHttpGroups.ts";
import {
  RemoteAdmissionDigest,
  RemoteAdmissionEvaluation,
  RemoteAdmissionPublicStatus,
  RemoteAdmissionReceiverPolicy,
  RemoteAdmissionRecord,
} from "./remoteAdmission.ts";

export const REMOTE_ADMISSION_REVIEW_BASE = "/api/jones/remote-admission";
export const REMOTE_ADMISSION_REVIEW_VERSION = "jones.remote-admission-review/v2";
export const REMOTE_ADMISSION_OBSERVATION_VERSION = "jones.remote-admission-observation/v1";
export const REMOTE_ADMISSION_PEER_STATUS_VERSION = "jones.remote-admission-peer-status/v1";
export const REMOTE_ADMISSION_RETRY_OBSERVATION_VERSION = "jones.remote-admission-retry-observation/v1";
export const REMOTE_ADMISSION_RECOVERY_STATUS_VERSION = "jones.remote-admission-recovery-status/v1";
export const REMOTE_ADMISSION_RECOVERED_RESULT_VERSION = "jones.remote-admission-recovered-result/v1";
export const REMOTE_ADMISSION_CAPABILITIES_REGISTER_VERSION = "jones.remote-admission-capabilities-register/v1";
export const REMOTE_ADMISSION_DEFAULT_PAGE_SIZE = 50;
export const REMOTE_ADMISSION_MAX_PAGE_SIZE = 100;

const Id = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const Count = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const Generation = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1));
const Time = Schema.String;
// Cursors are server-issued capabilities for a view/auth/order snapshot, never offsets.
export const RemoteAdmissionReviewCursor = Schema.NonEmptyString.check(Schema.isMaxLength(4096));
export const RemoteAdmissionGrantDuration = Schema.Literals([3600, 86400, 172800, 259200, 604800]);
export const RemoteAdmissionGrantIntent = Schema.Struct({ durationSeconds: RemoteAdmissionGrantDuration });
export const RemoteAdmissionReviewFeatures = Schema.Struct({
  reviewV2: Schema.Boolean,
  grants: Schema.Boolean,
  conflicts: Schema.Boolean,
  changeFeed: Schema.Boolean,
  grantRestore: Schema.optionalKey(Schema.Boolean),
  blockedAudit: Schema.optionalKey(Schema.Boolean),
  blockedReview: Schema.optionalKey(Schema.Boolean),
  history: Schema.optionalKey(Schema.Boolean),
  launchRetry: Schema.optionalKey(Schema.Boolean),
  recoveredResult: Schema.optionalKey(Schema.Boolean),
  managedAccess: Schema.optionalKey(Schema.Boolean),
});
export const RemoteAdmissionReviewCapabilities = Schema.Struct({
  version: Schema.Literal(REMOTE_ADMISSION_REVIEW_VERSION),
  features: RemoteAdmissionReviewFeatures,
  principal: Schema.Struct({ canDecide: Schema.Boolean }),
});
export const RemoteAdmissionReviewCounts = Schema.Struct({
  currentHeldAdmissions: Count,
  blockedHeldAdmissions: Count,
  heldAdmissions: Count,
  inFlightAdmissions: Count,
  failedAdmissions: Count,
  unresolvedAdmissions: Count,
  activeAccessScopes: Count,
  activeThreads: Count,
}).check(Schema.makeFilter((counts) =>
  counts.heldAdmissions === counts.currentHeldAdmissions + counts.blockedHeldAdmissions
    ? true : "Held admissions must be the disjoint current and blocked sum.",
));
export const RemoteAdmissionBlockedReason = Schema.Literals([
  "access_revoked", "access_expired", "session_not_bound", "mode_exceeds",
  "operation_not_covered", "identity_unresolved",
]);
/** Audit facts include malicious holds; those remain ordinary owner holds and
 * never receive an access-blocked mark or automatic lease approval.
 */
export const RemoteAdmissionBlockedAuditReason = Schema.Union([RemoteAdmissionBlockedReason, Schema.Literal("malicious")]);
export const RemoteAdmissionAccessBinding = Schema.Struct({
  receiverEnvironmentId: Id,
  sessionId: Id,
  originEnvironmentId: Id,
  originThreadId: Id,
  destinationThreadId: Schema.NullOr(Id),
  operation: Schema.Literal("t3_thread_send"),
  runtimeMode: Id,
  interactionMode: Id,
});
export const RemoteAdmissionGrant = Schema.Struct({
  grantId: Id,
  sourceAdmissionId: Id,
  binding: RemoteAdmissionAccessBinding,
  state: Schema.Literals(["pending", "active", "revoked"]),
  generation: Generation,
  expiresAt: Time,
  createdAt: Time,
  updatedAt: Time,
  revokedAt: Schema.NullOr(Time),
  predecessorGrantId: Schema.NullOr(Id),
  serverNow: Time,
});
export const RemoteAdmissionManagedScope = Schema.Struct({
  managedScopeId: Id,
  receiverEnvironmentId: Id,
  originEnvironmentId: Id,
  originThreadId: Id,
  destinationThreadId: Id,
  operationClass: Schema.Literal("messages"),
  revision: Generation,
});
export const RemoteAdmissionAccessBasis = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("lease"), grantId: Id, generation: Generation }),
  Schema.Struct({ kind: Schema.Literal("permit"), bindingDigest: RemoteAdmissionDigest, scopeRevision: Generation }),
]);
export const RemoteAdmissionBlockedMark = Schema.Struct({
  admissionId: Id,
  managedScopeId: Id,
  reason: RemoteAdmissionBlockedReason,
});
export const RemoteAdmissionReviewLink = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("request"), receiverEnvironmentId: Id, admissionId: Id }),
  Schema.Struct({ kind: Schema.Literal("access"), receiverEnvironmentId: Id, grantId: Id }),
  Schema.Struct({ kind: Schema.Literal("blocked"), receiverEnvironmentId: Id, eventId: Id }),
]);
export const RemoteAdmissionBlockedEvent = Schema.Struct({
  eventId: Id,
  managedScopeId: Id,
  admissionId: Schema.NullOr(Id),
  reason: RemoteAdmissionBlockedAuditReason,
  firstAt: Time,
  lastAt: Time,
  repeatCount: Count,
  reviewLink: Schema.NullOr(RemoteAdmissionReviewLink),
});
export const RemoteAdmissionBlockedSummary = Schema.Struct({
  managedScopeId: Id,
  reason: RemoteAdmissionBlockedAuditReason,
  logicalEvents: Count,
  repeatArrivals: Count,
  firstAt: Time,
  lastAt: Time,
});
export const RemoteAdmissionBlockedDiagnosticCounts = Schema.Struct({
  logicalEvents: Count, repeatArrivals: Count, summaryRows: Count,
});

// Creation, availability, message acceptance and provider work are independent observations.
export const RemoteAdmissionLaunchStages = Schema.Struct({
  creation: Schema.Literals(["pending", "confirmed", "refused", "not_created", "unknown"]),
  availability: Schema.Literals(["available", "deleted", "unknown"]),
  initialMessage: Schema.Literals(["none", "accepted", "refused", "unconfirmed"]),
  preparation: Schema.Literals(["unknown", "pending", "ready", "failed"]),
  providerProgress: Schema.Literals(["unknown", "not_started", "running", "completed", "failed"]),
});
export const RemoteAdmissionLaunchResult = Schema.Struct({
  outcome: Schema.Literals(["created", "refused", "not_created", "unknown"]),
  stages: RemoteAdmissionLaunchStages,
  threadId: Schema.NullOr(Id),
  recordedAt: Time,
  reasonCode: Id,
});
export const RemoteAdmissionRecoveryEvidence = Schema.Struct({
  evidenceDigest: RemoteAdmissionDigest,
  observedAt: Time,
  identityPath: Schema.Literals(["hashed", "legacy", "neither", "contradictory"]),
  creationCommandId: Schema.NullOr(Id),
  initialMessageCommandId: Schema.NullOr(Id),
  abandonedClaim: Schema.Boolean,
  readsComplete: Schema.Boolean,
  contradictions: Schema.Array(Schema.Literals([
    "both_identities", "invalid_receipt", "shell_without_creation", "project_mismatch",
    "message_without_creation", "rejected_creation_with_effect",
  ])),
});
export const RemoteAdmissionRecoveredResult = Schema.Struct({
  admissionId: Id,
  revision: Generation,
  evidence: RemoteAdmissionRecoveryEvidence,
  result: RemoteAdmissionLaunchResult,
});
export const RemoteAdmissionReviewExtension = Schema.Struct({
  grantable: Schema.Boolean,
  grantUnavailableReason: Schema.NullOr(Id),
  grant: Schema.NullOr(RemoteAdmissionGrant),
  evaluator: Schema.NullOr(RemoteAdmissionEvaluation),
  blocked: Schema.NullOr(RemoteAdmissionBlockedMark),
  accessBasis: Schema.NullOr(RemoteAdmissionAccessBasis),
  attemptId: Schema.NullOr(Id),
  result: Schema.NullOr(RemoteAdmissionLaunchResult),
  recoveredResult: Schema.NullOr(RemoteAdmissionRecoveredResult),
  originNotifiedAt: Schema.NullOr(Time),
  retryEligible: Schema.Boolean,
  recheckEligible: Schema.Boolean,
});
export const RemoteAdmissionReviewItem = Schema.Struct({
  record: RemoteAdmissionRecord,
  extension: RemoteAdmissionReviewExtension,
});
export const RemoteAdmissionReviewView = Schema.Literals([
  "current", "blocked", "done", "unresolved", "inflight", "access",
]);
export const RemoteAdmissionReviewQuery = Schema.Struct({
  view: RemoteAdmissionReviewView,
  cursor: Schema.optionalKey(RemoteAdmissionReviewCursor),
  limit: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
});
export const RemoteAdmissionReviewPage = Schema.Struct({
  view: RemoteAdmissionReviewView,
  snapshotRevision: Id,
  counts: RemoteAdmissionReviewCounts,
  retainedRecordCount: Count,
  items: Schema.Array(RemoteAdmissionReviewItem),
  grants: Schema.Array(RemoteAdmissionGrant),
  blockedEvents: Schema.Array(RemoteAdmissionBlockedEvent),
  blockedSummaries: Schema.Array(RemoteAdmissionBlockedSummary),
  blockedDiagnosticCounts: RemoteAdmissionBlockedDiagnosticCounts,
  nextCursor: Schema.NullOr(RemoteAdmissionReviewCursor),
  policy: RemoteAdmissionReceiverPolicy,
  serverNow: Time,
});
export const RemoteAdmissionReviewItemQuery = Schema.Struct({ id: Id });
export const RemoteAdmissionReviewDecision = Schema.Union([
  Schema.Struct({ id: Id, bindingDigest: RemoteAdmissionDigest, decision: Schema.Literal("approve"), grant: Schema.optionalKey(RemoteAdmissionGrantIntent) }),
  Schema.Struct({ id: Id, bindingDigest: RemoteAdmissionDigest, decision: Schema.Literal("deny") }),
]);
export const RemoteAdmissionReviewDecisionResult = Schema.Struct({
  committed: Schema.Literal(true),
  record: RemoteAdmissionRecord,
  grant: Schema.NullOr(RemoteAdmissionGrant),
});
export const RemoteAdmissionGrantQuery = Schema.Struct({
  grantId: Schema.optionalKey(Id),
  /** Pure readback of the single successor created by this exact restore command. */
  restoreCommandKey: Schema.optionalKey(Id),
  cursor: Schema.optionalKey(RemoteAdmissionReviewCursor),
  limit: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
});
export const RemoteAdmissionGrantsPage = Schema.Struct({
  snapshotRevision: Id,
  grants: Schema.Array(RemoteAdmissionGrant),
  nextCursor: Schema.NullOr(RemoteAdmissionReviewCursor),
  serverNow: Time,
});
export const RemoteAdmissionGrantUpdate = Schema.Struct({
  grantId: Id, expectedGeneration: Generation, durationSeconds: RemoteAdmissionGrantDuration,
});
export const RemoteAdmissionGrantRevoke = Schema.Struct({ grantId: Id, expectedGeneration: Generation });
export const RemoteAdmissionGrantRestore = Schema.Struct({
  grantId: Id, expectedGeneration: Generation, durationSeconds: RemoteAdmissionGrantDuration, commandKey: Id,
});
export const RemoteAdmissionGrantRestoreResult = Schema.Struct({
  predecessorGrantId: Id, commandKey: Id, successor: RemoteAdmissionGrant,
});
export const RemoteAdmissionRetryInput = Schema.Struct({
  id: Id, bindingDigest: RemoteAdmissionDigest, commandKey: Id,
  grant: Schema.optionalKey(RemoteAdmissionGrantIntent),
});
export const RemoteAdmissionRetryAttempt = Schema.Struct({
  attemptId: Id,
  rootAdmissionId: Id,
  predecessorAdmissionId: Id,
  childAdmissionId: Id,
  receiverEnvironmentId: Id,
  locator: Id,
  state: Schema.Literals(["prepared", "origin_registered", "claimed", "completed", "refused", "expired"]),
  createdAt: Time,
  expiresAt: Time,
  grantIntent: Schema.NullOr(RemoteAdmissionGrantIntent),
});
export const RemoteAdmissionRecheckInput = Schema.Struct({ id: Id, bindingDigest: RemoteAdmissionDigest });
export const RemoteAdmissionRecheckResult = Schema.Struct({
  item: RemoteAdmissionReviewItem,
  recoveredResult: Schema.NullOr(RemoteAdmissionRecoveredResult),
});
export const RemoteAdmissionChangesQuery = Schema.Struct({
  after: Schema.optionalKey(RemoteAdmissionReviewCursor),
  timeoutMs: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 50000 }))),
});
export const RemoteAdmissionChange = Schema.Struct({
  sequence: Id,
  kind: Schema.Literals(["admission", "grant", "blocked", "attempt", "recovered_result"]),
  link: RemoteAdmissionReviewLink,
});
export const RemoteAdmissionChanges = Schema.Struct({
  after: RemoteAdmissionReviewCursor,
  snapshotRevision: Id,
  counts: RemoteAdmissionReviewCounts,
  changes: Schema.Array(RemoteAdmissionChange),
  serverNow: Time,
});

export const RemoteAdmissionReviewBadRequest = Schema.Struct({
  code: Schema.Literal("invalid_request"), message: Schema.String,
}).annotate({ httpApiStatus: 400 }).pipe(HttpApiSchema.status(400));
export const RemoteAdmissionReviewUnauthorized = Schema.Struct({
  code: Schema.Literal("review_unavailable"), message: Schema.String,
  cause: Schema.optionalKey(Schema.Literals(["session_expired", "session_revoked"])),
}).annotate({ httpApiStatus: 401 }).pipe(HttpApiSchema.status(401));
export const RemoteAdmissionReviewForbidden = Schema.Struct({
  code: Schema.Literal("owner_required"), message: Schema.String,
}).annotate({ httpApiStatus: 403 }).pipe(HttpApiSchema.status(403));
export const RemoteAdmissionReviewUnavailable = Schema.Struct({
  code: Schema.Literals(["history_unavailable", "grant_unavailable", "item_unavailable"]),
  message: Schema.String,
}).annotate({ httpApiStatus: 404 }).pipe(HttpApiSchema.status(404));
export const RemoteAdmissionReviewConflict = Schema.Struct({
  code: Schema.Literal("conflict"),
  reason: Schema.Literals(["state_changed", "binding_changed", "generation_changed", "grant_ineligible", "command_changed", "retry_ineligible", "authority_changed"]),
  message: Schema.String,
  current: Schema.Struct({
    admission: Schema.NullOr(RemoteAdmissionPublicStatus),
    grant: Schema.NullOr(RemoteAdmissionGrant),
    chainHead: Schema.NullOr(RemoteAdmissionGrant),
    attempt: Schema.NullOr(RemoteAdmissionRetryAttempt),
  }),
}).annotate({ httpApiStatus: 409 }).pipe(HttpApiSchema.status(409));
export const RemoteAdmissionReviewError = Schema.Union([
  RemoteAdmissionReviewBadRequest, RemoteAdmissionReviewUnauthorized,
  RemoteAdmissionReviewForbidden, RemoteAdmissionReviewUnavailable, RemoteAdmissionReviewConflict,
]);

// No supports/registration input belongs on these pure reads.
export const RemoteAdmissionPeerStatusInput = RemoteAdmissionChangesQuery;
export const RemoteAdmissionPeerVersions = Schema.Struct({
  peerProjection: Schema.optionalKey(Schema.Literal(REMOTE_ADMISSION_PEER_STATUS_VERSION)),
  peerLongPoll: Schema.optionalKey(Schema.Literal(REMOTE_ADMISSION_PEER_STATUS_VERSION)),
  retryObservation: Schema.optionalKey(Schema.Literal(REMOTE_ADMISSION_RETRY_OBSERVATION_VERSION)),
  recoveredResultObservation: Schema.optionalKey(Schema.Literal(REMOTE_ADMISSION_RECOVERED_RESULT_VERSION)),
  recoveryInspection: Schema.optionalKey(Schema.Literal(REMOTE_ADMISSION_RECOVERY_STATUS_VERSION)),
});
export const RemoteAdmissionCapabilitiesRegisterInput = Schema.Struct({
  coordinatorGeneration: Id, versions: RemoteAdmissionPeerVersions,
});
export const RemoteAdmissionCapabilitiesRegisterResult = Schema.Struct({
  coordinatorGeneration: Id, versions: RemoteAdmissionPeerVersions,
});
export const RemoteAdmissionReturnBinding = Schema.Struct({
  rootObservationKey: Id, originThreadId: Id, returnRouteKey: Id,
});
export const RemoteAdmissionRecoveredNoticeIdentity = Schema.Struct({
  receiverEnvironmentId: Id, admissionId: Id, recoveredRevision: Generation,
});
export const RemoteAdmissionRecoveredNotice = Schema.Struct({
  ...RemoteAdmissionRecoveredNoticeIdentity.fields,
  rootAdmissionId: Id,
  returnBinding: RemoteAdmissionReturnBinding,
  evidenceDigest: RemoteAdmissionDigest,
  result: RemoteAdmissionLaunchResult,
  deliveryState: Schema.Literals(["awaiting_support", "unregistered", "registered", "undeliverable", "delivered", "suppressed"]),
});
export const RemoteAdmissionPeerStatus = Schema.Struct({
  after: RemoteAdmissionReviewCursor,
  receiverEnvironmentId: Id,
  /** Identity of this authenticated peer session, reported by the receiver. */
  sessionId: Id,
  /** Negotiated independently from projection support. */
  peerLongPoll: Schema.Boolean,
  serverNow: Time,
  grants: Schema.Array(RemoteAdmissionGrant),
  attempts: Schema.Array(RemoteAdmissionRetryAttempt),
  notices: Schema.Array(RemoteAdmissionRecoveredNotice),
});
export const RemoteAdmissionObservationInput = Schema.Union([
  Schema.Struct({ clientRequestId: Id }),
  Schema.Struct({ attemptId: Id, observationKey: Id }),
]);
export const RemoteAdmissionObservation = Schema.Struct({
  admission: Schema.NullOr(RemoteAdmissionPublicStatus),
  result: Schema.NullOr(RemoteAdmissionLaunchResult),
});
export const RemoteAdmissionAckInput = Schema.Union([
  Schema.Struct({ clientRequestId: Id, disposition: Schema.Literals(["delivered", "suppressed"]) }),
  Schema.Struct({ attemptId: Id, observationKey: Id, disposition: Schema.Literals(["delivered", "suppressed"]) }),
]);
export const RemoteAdmissionAttemptRegisterInput = Schema.Struct({ attemptId: Id, observationKey: Id });
export const RemoteAdmissionAttemptRegisterResult = Schema.Struct({ attempt: RemoteAdmissionRetryAttempt });
export const RemoteAdmissionNoticeRegisterInput = Schema.Struct({
  ...RemoteAdmissionRecoveredNoticeIdentity.fields, observationKey: Id,
});
export const RemoteAdmissionNoticeRegisterResult = Schema.Struct({ notice: RemoteAdmissionRecoveredNotice });
export const RemoteAdmissionNoticeAckInput = Schema.Struct({
  ...RemoteAdmissionRecoveredNoticeIdentity.fields,
  disposition: Schema.Literals(["delivered", "suppressed"]),
});
export const RemoteAdmissionAckResult = Schema.Struct({ acknowledged: Schema.Literal(true) });
export const RemoteAdmissionRecoveryStatusInput = Schema.Union([
  Schema.Struct({ locator: Id }),
  Schema.Struct({ clientRequestId: Id, originThreadId: Id }),
]);
export const RemoteAdmissionRecoveryStatus = Schema.Struct({
  receiverEnvironmentId: Id,
  state: Schema.Literals(["awaiting_confirmation", "created", "refused", "not_created", "unknown", "superseded", "history_unavailable"]),
  reasonCode: Id,
  stages: RemoteAdmissionLaunchStages,
  confirmedThreadId: Schema.NullOr(Id),
  requestsRoute: Schema.NonEmptyString,
});

// HTTP query strings decode to the same numeric DTO used by local and MCP callers.
const HttpPageLimit = Schema.optionalKey(Schema.FiniteFromString.check(
  Schema.isInt(), Schema.isBetween({ minimum: 1, maximum: 100 }),
));
export const RemoteAdmissionReviewHttpQuery = Schema.Struct({
  ...RemoteAdmissionReviewQuery.fields, limit: HttpPageLimit,
});
export const RemoteAdmissionGrantHttpQuery = Schema.Struct({
  ...RemoteAdmissionGrantQuery.fields, limit: HttpPageLimit,
});
export const RemoteAdmissionChangesHttpQuery = Schema.Struct({
  ...RemoteAdmissionChangesQuery.fields,
  timeoutMs: Schema.optionalKey(Schema.FiniteFromString.check(
    Schema.isInt(), Schema.isBetween({ minimum: 0, maximum: 50000 }),
  )),
});

export function makeRemoteAdmissionReviewHttpGroup({
  OptionalBearerHeaders, EnvironmentAuthenticatedAuth, EnvironmentInternalError,
}: Pick<Parameters<typeof makeJonesHttpGroups>[0],
  "OptionalBearerHeaders" | "EnvironmentAuthenticatedAuth" | "EnvironmentInternalError">) {
  const common = {
    headers: OptionalBearerHeaders,
    error: [RemoteAdmissionReviewBadRequest, RemoteAdmissionReviewUnauthorized,
      RemoteAdmissionReviewForbidden, RemoteAdmissionReviewUnavailable,
      RemoteAdmissionReviewConflict, EnvironmentInternalError],
  } as const;
  return HttpApiGroup.make("remoteAdmissionReview")
    .add(HttpApiEndpoint.get("capabilities", `${REMOTE_ADMISSION_REVIEW_BASE}/capabilities`, {
      ...common, success: RemoteAdmissionReviewCapabilities,
    }).middleware(EnvironmentAuthenticatedAuth))
    .add(HttpApiEndpoint.get("review", `${REMOTE_ADMISSION_REVIEW_BASE}/review`, {
      ...common, query: RemoteAdmissionReviewHttpQuery, success: RemoteAdmissionReviewPage,
    }).middleware(EnvironmentAuthenticatedAuth))
    .add(HttpApiEndpoint.get("item", `${REMOTE_ADMISSION_REVIEW_BASE}/review/item`, {
      ...common, query: RemoteAdmissionReviewItemQuery, success: RemoteAdmissionReviewItem,
    }).middleware(EnvironmentAuthenticatedAuth))
    .add(HttpApiEndpoint.post("decision", `${REMOTE_ADMISSION_REVIEW_BASE}/review/decision`, {
      ...common, payload: RemoteAdmissionReviewDecision, success: RemoteAdmissionReviewDecisionResult,
    }).middleware(EnvironmentAuthenticatedAuth))
    .add(HttpApiEndpoint.get("grants", `${REMOTE_ADMISSION_REVIEW_BASE}/grants`, {
      ...common, query: RemoteAdmissionGrantHttpQuery, success: RemoteAdmissionGrantsPage,
    }).middleware(EnvironmentAuthenticatedAuth))
    .add(HttpApiEndpoint.post("updateGrant", `${REMOTE_ADMISSION_REVIEW_BASE}/grants/update`, {
      ...common, payload: RemoteAdmissionGrantUpdate, success: RemoteAdmissionGrant,
    }).middleware(EnvironmentAuthenticatedAuth))
    .add(HttpApiEndpoint.post("revokeGrant", `${REMOTE_ADMISSION_REVIEW_BASE}/grants/revoke`, {
      ...common, payload: RemoteAdmissionGrantRevoke, success: RemoteAdmissionGrant,
    }).middleware(EnvironmentAuthenticatedAuth))
    .add(HttpApiEndpoint.post("restoreGrant", `${REMOTE_ADMISSION_REVIEW_BASE}/grants/restore`, {
      ...common, payload: RemoteAdmissionGrantRestore, success: RemoteAdmissionGrantRestoreResult,
    }).middleware(EnvironmentAuthenticatedAuth))
    .add(HttpApiEndpoint.post("retry", `${REMOTE_ADMISSION_REVIEW_BASE}/review/retry`, {
      ...common, payload: RemoteAdmissionRetryInput, success: RemoteAdmissionRetryAttempt,
    }).middleware(EnvironmentAuthenticatedAuth))
    .add(HttpApiEndpoint.post("recheck", `${REMOTE_ADMISSION_REVIEW_BASE}/review/recheck`, {
      ...common, payload: RemoteAdmissionRecheckInput, success: RemoteAdmissionRecheckResult,
    }).middleware(EnvironmentAuthenticatedAuth))
    .add(HttpApiEndpoint.get("changes", `${REMOTE_ADMISSION_REVIEW_BASE}/changes`, {
      ...common, query: RemoteAdmissionChangesHttpQuery, success: RemoteAdmissionChanges,
    }).middleware(EnvironmentAuthenticatedAuth));
}

export type RemoteAdmissionReviewCursor = typeof RemoteAdmissionReviewCursor.Type;
export type RemoteAdmissionGrantDuration = typeof RemoteAdmissionGrantDuration.Type;
export type RemoteAdmissionGrantIntent = typeof RemoteAdmissionGrantIntent.Type;
export type RemoteAdmissionReviewFeatures = typeof RemoteAdmissionReviewFeatures.Type;
export type RemoteAdmissionReviewCapabilities = typeof RemoteAdmissionReviewCapabilities.Type;
export type RemoteAdmissionReviewCounts = typeof RemoteAdmissionReviewCounts.Type;
export type RemoteAdmissionBlockedReason = typeof RemoteAdmissionBlockedReason.Type;
export type RemoteAdmissionBlockedAuditReason = typeof RemoteAdmissionBlockedAuditReason.Type;
export type RemoteAdmissionAccessBinding = typeof RemoteAdmissionAccessBinding.Type;
export type RemoteAdmissionGrant = typeof RemoteAdmissionGrant.Type;
export type RemoteAdmissionManagedScope = typeof RemoteAdmissionManagedScope.Type;
export type RemoteAdmissionAccessBasis = typeof RemoteAdmissionAccessBasis.Type;
export type RemoteAdmissionBlockedMark = typeof RemoteAdmissionBlockedMark.Type;
export type RemoteAdmissionReviewLink = typeof RemoteAdmissionReviewLink.Type;
export type RemoteAdmissionBlockedEvent = typeof RemoteAdmissionBlockedEvent.Type;
export type RemoteAdmissionBlockedSummary = typeof RemoteAdmissionBlockedSummary.Type;
export type RemoteAdmissionBlockedDiagnosticCounts = typeof RemoteAdmissionBlockedDiagnosticCounts.Type;
export type RemoteAdmissionLaunchStages = typeof RemoteAdmissionLaunchStages.Type;
export type RemoteAdmissionLaunchResult = typeof RemoteAdmissionLaunchResult.Type;
export type RemoteAdmissionRecoveryEvidence = typeof RemoteAdmissionRecoveryEvidence.Type;
export type RemoteAdmissionRecoveredResult = typeof RemoteAdmissionRecoveredResult.Type;
export type RemoteAdmissionReviewExtension = typeof RemoteAdmissionReviewExtension.Type;
export type RemoteAdmissionReviewItem = typeof RemoteAdmissionReviewItem.Type;
export type RemoteAdmissionReviewView = typeof RemoteAdmissionReviewView.Type;
export type RemoteAdmissionReviewQuery = typeof RemoteAdmissionReviewQuery.Type;
export type RemoteAdmissionReviewPage = typeof RemoteAdmissionReviewPage.Type;
export type RemoteAdmissionReviewItemQuery = typeof RemoteAdmissionReviewItemQuery.Type;
export type RemoteAdmissionReviewDecision = typeof RemoteAdmissionReviewDecision.Type;
export type RemoteAdmissionReviewDecisionResult = typeof RemoteAdmissionReviewDecisionResult.Type;
export type RemoteAdmissionGrantQuery = typeof RemoteAdmissionGrantQuery.Type;
export type RemoteAdmissionGrantsPage = typeof RemoteAdmissionGrantsPage.Type;
export type RemoteAdmissionGrantUpdate = typeof RemoteAdmissionGrantUpdate.Type;
export type RemoteAdmissionGrantRevoke = typeof RemoteAdmissionGrantRevoke.Type;
export type RemoteAdmissionGrantRestore = typeof RemoteAdmissionGrantRestore.Type;
export type RemoteAdmissionGrantRestoreResult = typeof RemoteAdmissionGrantRestoreResult.Type;
export type RemoteAdmissionRetryInput = typeof RemoteAdmissionRetryInput.Type;
export type RemoteAdmissionRetryAttempt = typeof RemoteAdmissionRetryAttempt.Type;
export type RemoteAdmissionRecheckInput = typeof RemoteAdmissionRecheckInput.Type;
export type RemoteAdmissionRecheckResult = typeof RemoteAdmissionRecheckResult.Type;
export type RemoteAdmissionChangesQuery = typeof RemoteAdmissionChangesQuery.Type;
export type RemoteAdmissionChange = typeof RemoteAdmissionChange.Type;
export type RemoteAdmissionChanges = typeof RemoteAdmissionChanges.Type;
export type RemoteAdmissionReviewBadRequest = typeof RemoteAdmissionReviewBadRequest.Type;
export type RemoteAdmissionReviewUnauthorized = typeof RemoteAdmissionReviewUnauthorized.Type;
export type RemoteAdmissionReviewForbidden = typeof RemoteAdmissionReviewForbidden.Type;
export type RemoteAdmissionReviewUnavailable = typeof RemoteAdmissionReviewUnavailable.Type;
export type RemoteAdmissionReviewConflict = typeof RemoteAdmissionReviewConflict.Type;
export type RemoteAdmissionReviewError = typeof RemoteAdmissionReviewError.Type;
export type RemoteAdmissionPeerStatusInput = typeof RemoteAdmissionPeerStatusInput.Type;
export type RemoteAdmissionPeerVersions = typeof RemoteAdmissionPeerVersions.Type;
export type RemoteAdmissionCapabilitiesRegisterInput = typeof RemoteAdmissionCapabilitiesRegisterInput.Type;
export type RemoteAdmissionCapabilitiesRegisterResult = typeof RemoteAdmissionCapabilitiesRegisterResult.Type;
export type RemoteAdmissionReturnBinding = typeof RemoteAdmissionReturnBinding.Type;
export type RemoteAdmissionRecoveredNoticeIdentity = typeof RemoteAdmissionRecoveredNoticeIdentity.Type;
export type RemoteAdmissionRecoveredNotice = typeof RemoteAdmissionRecoveredNotice.Type;
export type RemoteAdmissionPeerStatus = typeof RemoteAdmissionPeerStatus.Type;
export type RemoteAdmissionObservationInput = typeof RemoteAdmissionObservationInput.Type;
export type RemoteAdmissionObservation = typeof RemoteAdmissionObservation.Type;
export type RemoteAdmissionAckInput = typeof RemoteAdmissionAckInput.Type;
export type RemoteAdmissionAttemptRegisterInput = typeof RemoteAdmissionAttemptRegisterInput.Type;
export type RemoteAdmissionAttemptRegisterResult = typeof RemoteAdmissionAttemptRegisterResult.Type;
export type RemoteAdmissionNoticeRegisterInput = typeof RemoteAdmissionNoticeRegisterInput.Type;
export type RemoteAdmissionNoticeRegisterResult = typeof RemoteAdmissionNoticeRegisterResult.Type;
export type RemoteAdmissionNoticeAckInput = typeof RemoteAdmissionNoticeAckInput.Type;
export type RemoteAdmissionAckResult = typeof RemoteAdmissionAckResult.Type;
export type RemoteAdmissionRecoveryStatusInput = typeof RemoteAdmissionRecoveryStatusInput.Type;
export type RemoteAdmissionRecoveryStatus = typeof RemoteAdmissionRecoveryStatus.Type;

export type RemoteAdmissionReviewHttpQuery = typeof RemoteAdmissionReviewHttpQuery.Type;
export type RemoteAdmissionGrantHttpQuery = typeof RemoteAdmissionGrantHttpQuery.Type;
export type RemoteAdmissionChangesHttpQuery = typeof RemoteAdmissionChangesHttpQuery.Type;
