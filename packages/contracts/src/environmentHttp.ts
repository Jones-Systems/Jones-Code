import {
  JonesUpdateState,
  JonesUpdateDownloadInput,
  JonesUpdateInstallInput,
} from "./jonesUpdates.ts";
import {
  VoiceReviewRecentList,
  VoiceReviewDiagnostics,
  VoiceReviewDraft,
  VoiceReviewDraftList,
  VoiceReviewMutationResult,
  VoiceReviewErrors,
  VoiceReviewRevisionPayload,
  VoiceReviewEditSavePayload,
  VoiceReviewEditCancelPayload,
} from "./voiceReview.ts";
import {
  ThreadRegistryComposedSnapshot,
  ThreadRegistryWorkstreams,
  ThreadRegistryEvents,
  ThreadRegistryAssociationPayload,
  ThreadRegistryLabelPayload,
  ThreadRegistryMutationReceipt,
} from "./threadRegistry.ts";
import { HostStatusSnapshot } from "./hostStatus.ts";
import * as Context from "effect/Context";
import type * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint";
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup";
import * as HttpApiMiddleware from "effect/unstable/httpapi/HttpApiMiddleware";
import * as HttpApiSchema from "effect/unstable/httpapi/HttpApiSchema";
import * as HttpServerRespondable from "effect/unstable/http/HttpServerRespondable";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";

import {
  AuthAccessTokenResult,
  AuthBrowserSessionRequest,
  AuthBrowserSessionResult,
  AuthClientSession,
  AuthCreatePairingCredentialInput,
  AuthPairingCredentialResult,
  AuthPairingLink,
  AuthRevokeClientSessionInput,
  AuthRevokePairingLinkInput,
  AuthEnvironmentScope,
  AuthTokenExchangeRequest,
  AuthSessionState,
  AuthWebSocketTicketResult,
  ServerAuthSessionMethod,
} from "./auth.ts";
import {
  ExecutionEnvironmentDescriptor,
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION_TEXT,
} from "./environment.ts";
import {
  DpopFailureReason,
  AuthSessionId,
  ThreadId,
  CommandId,
  MessageId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import {
  OrchestrationV2ShellSnapshot,
  OrchestrationV2ThreadBoundedSnapshot,
  OrchestrationV2ThreadDetailSnapshot,
  OrchestrationV2ThreadHistoryPage,
} from "./orchestrationV2.ts";
import { Project, ProjectMutation, ProjectSnapshot } from "./project.ts";
import {
  NativeCommandObservationV2,
  OrchestrationCommandObservation,
  ThreadTurnStartCommand,
} from "./orchestrationNative.ts";
import {
  PullRequestDiffInput,
  PullRequestDiffResult,
  PullRequestOperationError,
  PullRequestUnavailableError,
} from "./pullRequest.ts";
import {
  CONVERSATION_LIBRARY_PATH,
  LibraryErrorCodeSchema,
  LibraryReplySchema,
  LibraryRequestSchema,
} from "./conversationLibrary.ts";
import {
  RelayCloudEnvironmentHealthRequest,
  RelayCloudMintCredentialRequest,
  RelayEnvironmentConfigRequest,
  RelayEnvironmentHealthResponse,
  RelayEnvironmentLinkProof,
  RelayEnvironmentMintResponse,
  RelayLinkProofRequest,
} from "./relay.ts";

import {
  T3WorkstreamCommandPollParams,
  T3WorkstreamCommandRequest,
  T3WorkstreamDetailParams,
  T3WorkstreamListResult,
  T3WorkstreamPageQuery,
  T3WorkstreamReferenceParams,
  WorkstreamDeclarationPage,
  WorkstreamDetail,
  WorkstreamEdgePage,
  WorkstreamHistoryPage,
  WorkstreamMembershipPage,
  WorkstreamReferenceDetail,
  WorkstreamReferencePage,
  WorkstreamReceipt,
} from "./workstreams.ts";
import { T3PlacementLoadRequest, T3PlacementResult } from "./workstreamPlacements.ts";
import {
  WorkstreamsNativeContextResponse,
  WorkstreamsNativeAttestationRequest,
  WorkstreamsNativeAttestationResponse,
  WorkstreamsNativeSettlementRequest,
  WorkstreamsNativeSettlementResponse,
} from "./workstreamsNativeProvider.ts";
import { WorkstreamsRegistrationContextResponse } from "./workstreamsRegistrationContext.ts";
import { ProviderInstanceId } from "./providerInstance.ts";
import { ProviderQueueInventory, ProviderQueueRefreshResult } from "./providerQueue.ts";

const OptionalBearerHeaders = Schema.Struct({
  authorization: Schema.optionalKey(Schema.String),
  dpop: Schema.optionalKey(Schema.String),
});

const OrchestrationProtocolHeaders = Schema.Struct({
  authorization: Schema.optionalKey(Schema.String),
  dpop: Schema.optionalKey(Schema.String),
  [ORCHESTRATION_PROTOCOL_HEADER]: Schema.Literal(ORCHESTRATION_PROTOCOL_VERSION_TEXT),
});

const OptionalDpopProofHeaders = Schema.Struct({
  dpop: Schema.optionalKey(Schema.String),
});

export const EnvironmentRequestInvalidReason = Schema.Literals([
  "invalid_scope",
  "scope_not_granted",
  "invalid_command",
  "dispatch_guard_rejected",
  "dispatch_guard_bootstrap_unsupported",
  "observation_unsupported",
  "invalid_history_cursor",
]);
export type EnvironmentRequestInvalidReason = typeof EnvironmentRequestInvalidReason.Type;

export const EnvironmentAuthInvalidReason = Schema.Literals([
  "missing_credential",
  "invalid_credential",
]);
export type EnvironmentAuthInvalidReason = typeof EnvironmentAuthInvalidReason.Type;

export const EnvironmentOperationForbiddenReason = Schema.Literals([
  "current_session_revoke_not_allowed",
]);
export type EnvironmentOperationForbiddenReason = typeof EnvironmentOperationForbiddenReason.Type;

export const EnvironmentInternalErrorReason = Schema.Literals([
  "bootstrap_validation_failed",
  "browser_session_issuance_failed",
  "browser_session_cookie_failed",
  "access_token_issuance_failed",
  "websocket_ticket_issuance_failed",
  "pairing_credential_issuance_failed",
  "pairing_links_load_failed",
  "pairing_link_revoke_failed",
  "client_sessions_load_failed",
  "client_session_revoke_failed",
  "project_snapshot_failed",
  "project_mutation_failed",
  "orchestration_snapshot_failed",
  "orchestration_thread_snapshot_failed",
  "orchestration_thread_bounded_snapshot_failed",
  "orchestration_thread_history_failed",
  "internal_error",
]);
export type EnvironmentInternalErrorReason = typeof EnvironmentInternalErrorReason.Type;

export class EnvironmentRequestInvalidError extends Schema.TaggedError<EnvironmentRequestInvalidError>()(
  "EnvironmentRequestInvalidError",
  {
    code: Schema.Literal("invalid_request"),
    reason: EnvironmentRequestInvalidReason,
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 400 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(EnvironmentRequestInvalidError)(this, { status: 400 });
  }

  override get message(): string {
    return `The environment rejected the request (${this.reason}).`;
  }
}

export class EnvironmentAuthInvalidError extends Schema.TaggedError<EnvironmentAuthInvalidError>()(
  "EnvironmentAuthInvalidError",
  {
    code: Schema.Literal("auth_invalid"),
    reason: EnvironmentAuthInvalidReason,
    // Older servers do not send a DPoP failure category.
    dpopFailureReason: Schema.optionalKey(DpopFailureReason),
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 401 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(EnvironmentAuthInvalidError)(this, { status: 401 });
  }

  override get message(): string {
    return `The environment rejected this client's credentials (${this.reason}).`;
  }
}

export class EnvironmentScopeRequiredError extends Schema.TaggedError<EnvironmentScopeRequiredError>()(
  "EnvironmentScopeRequiredError",
  {
    code: Schema.Literal("insufficient_scope"),
    requiredScope: AuthEnvironmentScope,
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 403 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(EnvironmentScopeRequiredError)(this, { status: 403 });
  }

  override get message(): string {
    return `This request needs the ${this.requiredScope} scope, which this client does not have.`;
  }
}

export class EnvironmentOperationForbiddenError extends Schema.TaggedError<EnvironmentOperationForbiddenError>()(
  "EnvironmentOperationForbiddenError",
  {
    code: Schema.Literal("operation_forbidden"),
    reason: EnvironmentOperationForbiddenReason,
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 403 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(EnvironmentOperationForbiddenError)(this, { status: 403 });
  }

  override get message(): string {
    return `The environment refused this operation (${this.reason}).`;
  }
}

export class EnvironmentInternalError extends Schema.TaggedError<EnvironmentInternalError>()(
  "EnvironmentInternalError",
  {
    code: Schema.Literal("internal_error"),
    reason: EnvironmentInternalErrorReason,
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 500 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(EnvironmentInternalError)(this, { status: 500 });
  }

  override get message(): string {
    return `The environment failed to answer this request (${this.reason}).`;
  }
}

export const EnvironmentResourceNotFoundReason = Schema.Literals(["thread_not_found"]);
export type EnvironmentResourceNotFoundReason = typeof EnvironmentResourceNotFoundReason.Type;

export class EnvironmentResourceNotFoundError extends Schema.TaggedError<EnvironmentResourceNotFoundError>()(
  "EnvironmentResourceNotFoundError",
  {
    code: Schema.Literal("not_found"),
    reason: EnvironmentResourceNotFoundReason,
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 404 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(EnvironmentResourceNotFoundError)(this, { status: 404 });
  }

  override get message(): string {
    return `The environment could not find what this request named (${this.reason}).`;
  }
}

export const EnvironmentHttpCommonError = Schema.Union([
  EnvironmentRequestInvalidError,
  EnvironmentAuthInvalidError,
  EnvironmentScopeRequiredError,
  EnvironmentOperationForbiddenError,
  EnvironmentResourceNotFoundError,
  EnvironmentInternalError,
]);
export type EnvironmentHttpCommonError = typeof EnvironmentHttpCommonError.Type;

const EnvironmentAuthenticationErrors = [
  EnvironmentAuthInvalidError,
  EnvironmentInternalError,
] as const;

export class EnvironmentHttpBadRequestError extends Schema.TaggedError<EnvironmentHttpBadRequestError>()(
  "EnvironmentHttpBadRequestError",
  {
    message: Schema.String,
  },
  { httpApiStatus: 400 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(EnvironmentHttpBadRequestError)(this, { status: 400 });
  }
}

export class EnvironmentHttpUnauthorizedError extends Schema.TaggedError<EnvironmentHttpUnauthorizedError>()(
  "EnvironmentHttpUnauthorizedError",
  {
    message: Schema.String,
  },
  { httpApiStatus: 401 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(EnvironmentHttpUnauthorizedError)(this, { status: 401 });
  }
}

export class EnvironmentHttpForbiddenError extends Schema.TaggedError<EnvironmentHttpForbiddenError>()(
  "EnvironmentHttpForbiddenError",
  {
    message: Schema.String,
  },
  { httpApiStatus: 403 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(EnvironmentHttpForbiddenError)(this, { status: 403 });
  }
}

export class EnvironmentHttpInternalServerError extends Schema.TaggedError<EnvironmentHttpInternalServerError>()(
  "EnvironmentHttpInternalServerError",
  {
    message: Schema.String,
  },
  { httpApiStatus: 500 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(EnvironmentHttpInternalServerError)(this, { status: 500 });
  }
}

export class EnvironmentHttpConflictError extends Schema.TaggedError<EnvironmentHttpConflictError>()(
  "EnvironmentHttpConflictError",
  {
    message: Schema.String,
  },
  { httpApiStatus: 409 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(EnvironmentHttpConflictError)(this, { status: 409 });
  }
}

export class EnvironmentCloudEndpointUnavailableError extends Schema.TaggedError<EnvironmentCloudEndpointUnavailableError>()(
  "EnvironmentCloudEndpointUnavailableError",
  {
    message: Schema.String,
    endpointRuntimeStatus: Schema.Unknown,
  },
  { httpApiStatus: 503 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(EnvironmentCloudEndpointUnavailableError)(this, {
      status: 503,
    });
  }
}
const EnvironmentSessionCreationErrors = [
  EnvironmentAuthInvalidError,
  EnvironmentInternalError,
] as const;
const EnvironmentTokenExchangeErrors = [
  EnvironmentRequestInvalidError,
  EnvironmentAuthInvalidError,
  EnvironmentInternalError,
] as const;
const EnvironmentScopedOperationErrors = [
  EnvironmentScopeRequiredError,
  EnvironmentInternalError,
] as const;
const EnvironmentPairingCredentialErrors = [
  EnvironmentRequestInvalidError,
  ...EnvironmentScopedOperationErrors,
] as const;
const EnvironmentSessionRevokeErrors = [
  EnvironmentScopeRequiredError,
  EnvironmentOperationForbiddenError,
  EnvironmentInternalError,
] as const;
const EnvironmentProjectSnapshotErrors = [
  EnvironmentScopeRequiredError,
  EnvironmentInternalError,
] as const;
const EnvironmentOrchestrationSnapshotErrors = [
  EnvironmentScopeRequiredError,
  EnvironmentInternalError,
] as const;
const EnvironmentWorkstreamSnapshotErrors = [
  EnvironmentScopeRequiredError,
  EnvironmentInternalError,
] as const;
const EnvironmentWorkstreamPagedSnapshotErrors = [
  ...EnvironmentWorkstreamSnapshotErrors,
  EnvironmentHttpConflictError,
] as const;
const EnvironmentOrchestrationThreadSnapshotErrors = [
  EnvironmentScopeRequiredError,
  EnvironmentResourceNotFoundError,
  EnvironmentInternalError,
] as const;
const EnvironmentOrchestrationDispatchErrors = [
  EnvironmentRequestInvalidError,
  EnvironmentScopeRequiredError,
  EnvironmentInternalError,
] as const;
const EnvironmentProjectMutationErrors = [
  EnvironmentRequestInvalidError,
  EnvironmentScopeRequiredError,
  EnvironmentInternalError,
] as const;

const EnvironmentConversationLibraryInvalidError = Schema.Struct({
  kind: Schema.Literal("error"),
  code: Schema.Literal("invalid"),
  message: Schema.String,
  traceId: TrimmedNonEmptyString,
}).pipe(HttpApiSchema.status(400));

const EnvironmentConversationLibraryForbiddenError = Schema.Struct({
  kind: Schema.Literal("error"),
  code: Schema.Literal("forbidden"),
  message: Schema.String,
  traceId: TrimmedNonEmptyString,
}).pipe(HttpApiSchema.status(403));

const EnvironmentConversationLibraryNotFoundError = Schema.Struct({
  kind: Schema.Literal("error"),
  code: Schema.Literal("not-found"),
  message: Schema.String,
  traceId: TrimmedNonEmptyString,
}).pipe(HttpApiSchema.status(404));

const EnvironmentConversationLibraryConflictError = Schema.Struct({
  kind: Schema.Literal("error"),
  code: Schema.Literal("conflict"),
  message: Schema.String,
  traceId: TrimmedNonEmptyString,
}).pipe(HttpApiSchema.status(409));

const EnvironmentConversationLibraryTooLargeError = Schema.Struct({
  kind: Schema.Literal("error"),
  code: Schema.Literal("too-large"),
  message: Schema.String,
  traceId: TrimmedNonEmptyString,
}).pipe(HttpApiSchema.status(413));

const EnvironmentConversationLibraryStorageError = Schema.Struct({
  kind: Schema.Literal("error"),
  code: Schema.Literal("storage"),
  message: Schema.String,
  traceId: TrimmedNonEmptyString,
}).pipe(HttpApiSchema.status(500));

const EnvironmentConversationLibraryUnsupportedError = Schema.Struct({
  kind: Schema.Literal("error"),
  code: Schema.Literal("unsupported"),
  message: Schema.String,
  traceId: TrimmedNonEmptyString,
}).pipe(HttpApiSchema.status(501));

const EnvironmentConversationLibraryErrorSchemas = [
  EnvironmentConversationLibraryInvalidError,
  EnvironmentConversationLibraryForbiddenError,
  EnvironmentConversationLibraryNotFoundError,
  EnvironmentConversationLibraryConflictError,
  EnvironmentConversationLibraryTooLargeError,
  EnvironmentConversationLibraryStorageError,
  EnvironmentConversationLibraryUnsupportedError,
] as const;

export const EnvironmentConversationLibraryErrorSchema = Schema.Union(
  EnvironmentConversationLibraryErrorSchemas,
);
export type EnvironmentConversationLibraryError =
  typeof EnvironmentConversationLibraryErrorSchema.Type;

export const EnvironmentConversationLibraryErrorCode = LibraryErrorCodeSchema;

export interface EnvironmentSessionPrincipalShape {
  readonly sessionId: AuthSessionId;
  readonly subject: string;
  readonly method: ServerAuthSessionMethod;
  readonly scopes: ReadonlySet<AuthEnvironmentScope>;
  readonly proofKeyThumbprint?: string;
  readonly expiresAt?: DateTime.DateTime;
}

export class EnvironmentAuthenticatedPrincipal extends Context.Service<
  EnvironmentAuthenticatedPrincipal,
  EnvironmentSessionPrincipalShape
>()("@t3tools/contracts/environmentHttp/EnvironmentAuthenticatedPrincipal") {}

export class EnvironmentAuthenticatedAuth extends HttpApiMiddleware.Service<
  EnvironmentAuthenticatedAuth,
  { provides: EnvironmentAuthenticatedPrincipal }
>()("EnvironmentAuthenticatedAuth", {
  error: EnvironmentAuthenticationErrors,
}) {}

const EnvironmentHttpCloudErrors = [
  EnvironmentHttpBadRequestError,
  EnvironmentHttpUnauthorizedError,
  EnvironmentHttpForbiddenError,
  EnvironmentHttpConflictError,
  EnvironmentHttpInternalServerError,
  EnvironmentScopeRequiredError,
] as const;

export const EnvironmentCloudRelayConfigResult = Schema.Struct({
  ok: Schema.Boolean,
  endpointRuntimeStatus: Schema.Unknown,
});
export type EnvironmentCloudRelayConfigResult = typeof EnvironmentCloudRelayConfigResult.Type;

export const EnvironmentCloudLinkStateResult = Schema.Struct({
  linked: Schema.Boolean,
  cloudUserId: Schema.NullOr(Schema.String),
  relayUrl: Schema.NullOr(Schema.String),
  relayIssuer: Schema.NullOr(Schema.String),
  // A managed Cloudflare tunnel is provisioned for this link. False for a
  // publish-only link (activity publishing without a relay-managed tunnel), so
  // clients can present the two capabilities as independent settings.
  // Optional so newer clients tolerate older environment servers.
  managedTunnelActive: Schema.optional(Schema.Boolean),
  publishAgentActivity: Schema.Boolean,
});
export type EnvironmentCloudLinkStateResult = typeof EnvironmentCloudLinkStateResult.Type;

export const EnvironmentCloudPreferencesRequest = Schema.Struct({
  publishAgentActivity: Schema.Boolean,
});
export type EnvironmentCloudPreferencesRequest = typeof EnvironmentCloudPreferencesRequest.Type;

export const AuthPairingLinkRevokeResult = Schema.Struct({
  revoked: Schema.Boolean,
});
export type AuthPairingLinkRevokeResult = typeof AuthPairingLinkRevokeResult.Type;

export const AuthClientSessionRevokeResult = Schema.Struct({
  revoked: Schema.Boolean,
});
export type AuthClientSessionRevokeResult = typeof AuthClientSessionRevokeResult.Type;

export const AuthOtherClientSessionsRevokeResult = Schema.Struct({
  revokedCount: Schema.Number,
});
export type AuthOtherClientSessionsRevokeResult = typeof AuthOtherClientSessionsRevokeResult.Type;

class EnvironmentMetadataHttpApi extends HttpApiGroup.make("metadata").add(
  HttpApiEndpoint.get("descriptor", "/.well-known/t3/environment", {
    success: ExecutionEnvironmentDescriptor,
  }),
) {}

class EnvironmentAuthHttpApi extends HttpApiGroup.make("auth")
  .add(
    HttpApiEndpoint.get("session", "/api/auth/session", {
      headers: OptionalBearerHeaders,
      success: AuthSessionState,
      error: [EnvironmentInternalError],
    }),
  )
  .add(
    HttpApiEndpoint.post("browserSession", "/api/auth/browser-session", {
      payload: AuthBrowserSessionRequest,
      success: AuthBrowserSessionResult,
      error: EnvironmentSessionCreationErrors,
    }),
  )
  .add(
    HttpApiEndpoint.post("token", "/oauth/token", {
      headers: OptionalDpopProofHeaders,
      payload: AuthTokenExchangeRequest,
      success: AuthAccessTokenResult,
      error: EnvironmentTokenExchangeErrors,
    }),
  )
  .add(
    HttpApiEndpoint.post("webSocketTicket", "/api/auth/websocket-ticket", {
      headers: OptionalBearerHeaders,
      success: AuthWebSocketTicketResult,
      error: [EnvironmentInternalError],
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("pairingCredential", "/api/auth/pairing-token", {
      headers: OptionalBearerHeaders,
      payload: AuthCreatePairingCredentialInput,
      success: AuthPairingCredentialResult,
      error: EnvironmentPairingCredentialErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("pairingLinks", "/api/auth/pairing-links", {
      headers: OptionalBearerHeaders,
      success: Schema.Array(AuthPairingLink),
      error: EnvironmentScopedOperationErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("revokePairingLink", "/api/auth/pairing-links/revoke", {
      headers: OptionalBearerHeaders,
      payload: AuthRevokePairingLinkInput,
      success: AuthPairingLinkRevokeResult,
      error: EnvironmentScopedOperationErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("clients", "/api/auth/clients", {
      headers: OptionalBearerHeaders,
      success: Schema.Array(AuthClientSession),
      error: EnvironmentScopedOperationErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("revokeClient", "/api/auth/clients/revoke", {
      headers: OptionalBearerHeaders,
      payload: AuthRevokeClientSessionInput,
      success: AuthClientSessionRevokeResult,
      error: EnvironmentSessionRevokeErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("revokeOtherClients", "/api/auth/clients/revoke-others", {
      headers: OptionalBearerHeaders,
      success: AuthOtherClientSessionsRevokeResult,
      error: EnvironmentScopedOperationErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  ) {}

const EnvironmentOrchestrationThreadSnapshotParams = Schema.Struct({
  threadId: ThreadId,
});

const EnvironmentOrchestrationThreadHistoryQuery = Schema.Struct({
  cursor: TrimmedNonEmptyString,
});

export const ProviderGoalStateObservation = Schema.Struct({
  schema: Schema.Literal("t3.provider-goal-state/v1"),
  threadId: ThreadId,
  providerInstanceId: ProviderInstanceId,
  nativeThreadId: Schema.NullOr(TrimmedNonEmptyString),
  observedAtMs: Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
  state: Schema.Literals(["active", "inactive", "unknown"]),
  reasonCode: Schema.Literals([
    "goal_null",
    "goal_present",
    "no_session",
    "session_stopped",
    "instance_mismatch",
    "native_cursor_missing",
    "unsupported",
    "timeout",
    "malformed",
    "goal_field_omitted",
    "rpc_error",
    "context_changed",
  ]),
});
export type ProviderGoalStateObservation = typeof ProviderGoalStateObservation.Type;

const EnvironmentOrchestrationThreadHistoryErrors = [
  EnvironmentRequestInvalidError,
  EnvironmentScopeRequiredError,
  EnvironmentResourceNotFoundError,
  EnvironmentInternalError,
] as const;

export class EnvironmentOrchestrationHttpApi extends HttpApiGroup.make("orchestration")
  .add(
    HttpApiEndpoint.post("dispatch", "/api/orchestration/dispatch", {
      headers: OptionalBearerHeaders,
      payload: ThreadTurnStartCommand,
      success: Schema.Never,
      error: [...EnvironmentOrchestrationDispatchErrors, EnvironmentAuthInvalidError],
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("shellSnapshot", "/api/orchestration/shell", {
      headers: OrchestrationProtocolHeaders,
      success: OrchestrationV2ShellSnapshot,
      error: EnvironmentOrchestrationSnapshotErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("threadSnapshot", "/api/orchestration/threads/:threadId", {
      headers: OrchestrationProtocolHeaders,
      params: EnvironmentOrchestrationThreadSnapshotParams,
      success: OrchestrationV2ThreadDetailSnapshot,
      error: EnvironmentOrchestrationThreadSnapshotErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get(
      "commandObservation",
      "/api/orchestration/threads/:threadId/commands/:commandId",
      {
        headers: OptionalBearerHeaders,
        params: Schema.Struct({ threadId: ThreadId, commandId: CommandId }),
        payload: { messageId: MessageId },
        success: OrchestrationCommandObservation,
        error: [...EnvironmentOrchestrationThreadSnapshotErrors, EnvironmentRequestInvalidError],
      },
    ).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get(
      "commandObservationV2",
      "/api/orchestration/v2/threads/:threadId/commands/:commandId",
      {
        headers: OptionalBearerHeaders,
        params: Schema.Struct({ threadId: ThreadId, commandId: CommandId }),
        payload: { messageId: MessageId },
        success: NativeCommandObservationV2,
        error: [...EnvironmentOrchestrationThreadSnapshotErrors, EnvironmentRequestInvalidError],
      },
    ).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get(
      "providerGoalState",
      "/api/orchestration/threads/:threadId/provider-goal-state",
      {
        headers: OptionalBearerHeaders,
        params: EnvironmentOrchestrationThreadSnapshotParams,
        payload: { expectedInstanceId: ProviderInstanceId },
        success: ProviderGoalStateObservation,
        error: EnvironmentOrchestrationThreadSnapshotErrors,
      },
    ).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("threadBoundedSnapshot", "/api/orchestration/threads/:threadId/bounded", {
      headers: OrchestrationProtocolHeaders,
      params: EnvironmentOrchestrationThreadSnapshotParams,
      success: OrchestrationV2ThreadBoundedSnapshot,
      error: EnvironmentOrchestrationThreadSnapshotErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("threadHistoryPage", "/api/orchestration/threads/:threadId/history", {
      headers: OrchestrationProtocolHeaders,
      params: EnvironmentOrchestrationThreadSnapshotParams,
      query: EnvironmentOrchestrationThreadHistoryQuery,
      success: OrchestrationV2ThreadHistoryPage,
      error: EnvironmentOrchestrationThreadHistoryErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  ) {}

class EnvironmentProjectsHttpApi extends HttpApiGroup.make("projects")
  .add(
    HttpApiEndpoint.get("snapshot", "/api/projects", {
      headers: OptionalBearerHeaders,
      success: ProjectSnapshot,
      error: EnvironmentProjectSnapshotErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("mutate", "/api/projects/mutate", {
      headers: OptionalBearerHeaders,
      payload: ProjectMutation,
      success: Project,
      error: EnvironmentProjectMutationErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  ) {}

/** Large, compressible pull-request payloads travel over HTTP rather than the RPC socket. */
class EnvironmentPullRequestsHttpApi extends HttpApiGroup.make("pullRequests").add(
  HttpApiEndpoint.post("diff", "/api/pull-requests/diff", {
    headers: OptionalBearerHeaders,
    payload: PullRequestDiffInput,
    success: PullRequestDiffResult,
    error: [
      PullRequestUnavailableError,
      PullRequestOperationError,
      EnvironmentAuthInvalidError,
      EnvironmentScopeRequiredError,
      EnvironmentInternalError,
    ],
  }).middleware(EnvironmentAuthenticatedAuth),
) {}

class EnvironmentWorkstreamsHttpApi extends HttpApiGroup.make("workstreams")
  .add(
    HttpApiEndpoint.get("registrationContext", "/api/workstreams/registration-context", {
      headers: OptionalBearerHeaders,
      success: WorkstreamsRegistrationContextResponse,
      error: [...EnvironmentWorkstreamSnapshotErrors, EnvironmentHttpBadRequestError],
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("threadPlacements", "/api/workstreams/thread-placements", {
      headers: OptionalBearerHeaders,
      payload: T3PlacementLoadRequest,
      success: T3PlacementResult,
      error: EnvironmentWorkstreamSnapshotErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("list", "/api/workstreams", {
      headers: OptionalBearerHeaders,
      payload: T3WorkstreamPageQuery,
      success: T3WorkstreamListResult,
      error: EnvironmentWorkstreamPagedSnapshotErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("references", "/api/workstreams/references", {
      headers: OptionalBearerHeaders,
      payload: T3WorkstreamPageQuery,
      success: WorkstreamReferencePage,
      error: EnvironmentWorkstreamPagedSnapshotErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("reference", "/api/workstreams/references/:nativeReferenceId", {
      headers: OptionalBearerHeaders,
      params: T3WorkstreamReferenceParams,
      success: WorkstreamReferenceDetail,
      error: EnvironmentWorkstreamSnapshotErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("detail", "/api/workstreams/:workstreamId", {
      headers: OptionalBearerHeaders,
      params: T3WorkstreamDetailParams,
      success: WorkstreamDetail,
      error: EnvironmentWorkstreamSnapshotErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("memberships", "/api/workstreams/:workstreamId/memberships", {
      headers: OptionalBearerHeaders,
      params: T3WorkstreamDetailParams,
      payload: T3WorkstreamPageQuery,
      success: WorkstreamMembershipPage,
      error: EnvironmentWorkstreamPagedSnapshotErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("declarations", "/api/workstreams/:workstreamId/declarations", {
      headers: OptionalBearerHeaders,
      params: T3WorkstreamDetailParams,
      payload: T3WorkstreamPageQuery,
      success: WorkstreamDeclarationPage,
      error: EnvironmentWorkstreamPagedSnapshotErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("edges", "/api/workstreams/:workstreamId/edges", {
      headers: OptionalBearerHeaders,
      params: T3WorkstreamDetailParams,
      payload: T3WorkstreamPageQuery,
      success: WorkstreamEdgePage,
      error: EnvironmentWorkstreamPagedSnapshotErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("history", "/api/workstreams/:workstreamId/history", {
      headers: OptionalBearerHeaders,
      params: T3WorkstreamDetailParams,
      payload: T3WorkstreamPageQuery,
      success: WorkstreamHistoryPage,
      error: EnvironmentWorkstreamPagedSnapshotErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("command", "/api/workstreams/commands/:commandId", {
      headers: OptionalBearerHeaders,
      params: T3WorkstreamCommandPollParams,
      success: WorkstreamReceipt,
      error: EnvironmentWorkstreamSnapshotErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("submit", "/api/workstreams/commands", {
      headers: OptionalBearerHeaders,
      payload: T3WorkstreamCommandRequest,
      success: WorkstreamReceipt,
      error: EnvironmentOrchestrationDispatchErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  ) {}

class EnvironmentWorkstreamsNativeHttpApi extends HttpApiGroup.make("workstreamsNative")
  .add(
    HttpApiEndpoint.get("context", "/api/workstreams/native/v1/context", {
      headers: OptionalBearerHeaders,
      success: WorkstreamsNativeContextResponse,
      error: [...EnvironmentScopedOperationErrors, EnvironmentHttpBadRequestError],
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("attestations", "/api/workstreams/native/v1/attestations", {
      headers: OptionalBearerHeaders,
      payload: WorkstreamsNativeAttestationRequest,
      success: WorkstreamsNativeAttestationResponse,
      error: [...EnvironmentScopedOperationErrors, EnvironmentHttpBadRequestError],
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("settlements", "/api/workstreams/native/v1/settlements", {
      headers: OptionalBearerHeaders,
      payload: WorkstreamsNativeSettlementRequest,
      success: WorkstreamsNativeSettlementResponse,
      error: [...EnvironmentScopedOperationErrors, EnvironmentHttpBadRequestError],
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("settlementLookup", "/api/workstreams/native/v1/settlements/lookup", {
      headers: OptionalBearerHeaders,
      payload: WorkstreamsNativeSettlementRequest,
      success: WorkstreamsNativeSettlementResponse,
      error: [...EnvironmentScopedOperationErrors, EnvironmentHttpBadRequestError],
    }).middleware(EnvironmentAuthenticatedAuth),
  ) {}

class EnvironmentConnectHttpApi extends HttpApiGroup.make("connect")
  .add(
    HttpApiEndpoint.post("linkProof", "/api/connect/link-proof", {
      headers: OptionalBearerHeaders,
      payload: RelayLinkProofRequest,
      success: RelayEnvironmentLinkProof,
      error: EnvironmentHttpCloudErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("relayConfig", "/api/connect/relay-config", {
      headers: OptionalBearerHeaders,
      payload: RelayEnvironmentConfigRequest,
      success: EnvironmentCloudRelayConfigResult,
      error: [...EnvironmentHttpCloudErrors, EnvironmentCloudEndpointUnavailableError],
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("linkState", "/api/connect/link-state", {
      headers: OptionalBearerHeaders,
      success: EnvironmentCloudLinkStateResult,
      error: EnvironmentHttpCloudErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("unlink", "/api/connect/unlink", {
      headers: OptionalBearerHeaders,
      success: EnvironmentCloudRelayConfigResult,
      error: EnvironmentHttpCloudErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("preferences", "/api/connect/preferences", {
      headers: OptionalBearerHeaders,
      payload: EnvironmentCloudPreferencesRequest,
      success: EnvironmentCloudLinkStateResult,
      error: EnvironmentHttpCloudErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("health", "/api/t3-connect/health", {
      payload: RelayCloudEnvironmentHealthRequest,
      success: RelayEnvironmentHealthResponse,
      error: EnvironmentHttpCloudErrors,
    }),
  )
  .add(
    HttpApiEndpoint.post("mintCredential", "/api/connect/mint-credential", {
      payload: RelayCloudMintCredentialRequest,
      success: RelayEnvironmentMintResponse,
      error: EnvironmentHttpCloudErrors,
    }),
  )
  .add(
    HttpApiEndpoint.post("t3MintCredential", "/api/t3-connect/mint-credential", {
      payload: RelayCloudMintCredentialRequest,
      success: RelayEnvironmentMintResponse,
      error: EnvironmentHttpCloudErrors,
    }),
  ) {}

class EnvironmentJonesUpdatesHttpApi extends HttpApiGroup.make("jonesUpdates")
  .add(
    HttpApiEndpoint.post("prepareNative", "/api/jones-updates/prepare-native", {
      headers: OptionalBearerHeaders,
      payload: JonesUpdateInstallInput,
      success: JonesUpdateState,
      error: [EnvironmentScopeRequiredError, EnvironmentInternalError],
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("state", "/api/jones-updates", {
      query: Schema.Struct({ after: Schema.optionalKey(Schema.NumberFromString) }),
      headers: OptionalBearerHeaders,
      success: Schema.NullOr(JonesUpdateState),
      error: [EnvironmentScopeRequiredError, EnvironmentInternalError],
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("check", "/api/jones-updates/check", {
      headers: OptionalBearerHeaders,
      success: JonesUpdateState,
      error: [EnvironmentScopeRequiredError, EnvironmentInternalError],
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("download", "/api/jones-updates/download", {
      headers: OptionalBearerHeaders,
      payload: JonesUpdateDownloadInput,
      success: JonesUpdateState,
      error: [EnvironmentScopeRequiredError, EnvironmentInternalError],
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("install", "/api/jones-updates/install", {
      headers: OptionalBearerHeaders,
      payload: JonesUpdateInstallInput,
      success: JonesUpdateState,
      error: [EnvironmentScopeRequiredError, EnvironmentInternalError],
    }).middleware(EnvironmentAuthenticatedAuth),
  ) {}

class EnvironmentHostStatusHttpApi extends HttpApiGroup.make("hostStatus").add(
  HttpApiEndpoint.get("snapshot", "/api/host-status", {
    headers: OptionalBearerHeaders,
    success: HostStatusSnapshot,
    error: [EnvironmentScopeRequiredError, EnvironmentInternalError],
  }).middleware(EnvironmentAuthenticatedAuth),
) {}

export class EnvironmentConversationLibraryHttpApi extends HttpApiGroup.make(
  "conversationLibrary",
).add(
  HttpApiEndpoint.post("conversationLibrary", CONVERSATION_LIBRARY_PATH, {
    headers: OptionalBearerHeaders,
    payload: LibraryRequestSchema,
    success: LibraryReplySchema,
    error: [...EnvironmentConversationLibraryErrorSchemas, EnvironmentScopeRequiredError],
  }).middleware(EnvironmentAuthenticatedAuth),
) {}

const VoiceReviewParams = Schema.Struct({
  id: Schema.String.check(Schema.isNonEmpty(), Schema.isMaxLength(256)),
});
const VoiceReviewHeaders = OptionalBearerHeaders;
const VoiceReviewPageLimit = Schema.optional(
  Schema.FiniteFromString.check(Schema.isInt(), Schema.isBetween({ minimum: 1, maximum: 200 })),
);
class EnvironmentVoiceReviewHttpApi extends HttpApiGroup.make("voiceReview")
  .add(
    HttpApiEndpoint.get("recent", "/api/voice-review/recent", {
      headers: VoiceReviewHeaders,
      query: { limit: VoiceReviewPageLimit },
      success: VoiceReviewRecentList,
      error: VoiceReviewErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("registrySnapshot", "/api/voice-review/registry/snapshot", {
      headers: VoiceReviewHeaders,
      query: {
        limit: VoiceReviewPageLimit,
        cursor: Schema.optional(Schema.String.check(Schema.isNonEmpty(), Schema.isMaxLength(4096))),
      },
      success: ThreadRegistryComposedSnapshot,
      error: VoiceReviewErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("registryWorkstreams", "/api/voice-review/registry/workstreams", {
      headers: VoiceReviewHeaders,
      success: ThreadRegistryWorkstreams,
      error: VoiceReviewErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("registryEvents", "/api/voice-review/registry/events", {
      headers: VoiceReviewHeaders,
      query: {
        limit: VoiceReviewPageLimit,
        after: Schema.optional(
          Schema.FiniteFromString.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
        ),
      },
      success: ThreadRegistryEvents,
      error: VoiceReviewErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("correctAssociation", "/api/voice-review/registry/associations", {
      headers: VoiceReviewHeaders,
      payload: ThreadRegistryAssociationPayload,
      success: ThreadRegistryMutationReceipt,
      error: VoiceReviewErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("correctLabel", "/api/voice-review/registry/labels", {
      headers: VoiceReviewHeaders,
      payload: ThreadRegistryLabelPayload,
      success: ThreadRegistryMutationReceipt,
      error: VoiceReviewErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("diagnostics", "/api/voice-review/drafts/:id/diagnostics", {
      headers: VoiceReviewHeaders,
      params: VoiceReviewParams,
      success: VoiceReviewDiagnostics,
      error: VoiceReviewErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("list", "/api/voice-review/drafts", {
      headers: VoiceReviewHeaders,
      query: {
        scope: Schema.optional(Schema.Literals(["pending", "recent"])),
        limit: Schema.optional(
          Schema.FiniteFromString.check(
            Schema.isInt(),
            Schema.isBetween({ minimum: 1, maximum: 200 }),
          ),
        ),
      },
      success: VoiceReviewDraftList,
      error: VoiceReviewErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("get", "/api/voice-review/drafts/:id", {
      headers: VoiceReviewHeaders,
      params: VoiceReviewParams,
      success: VoiceReviewDraft,
      error: VoiceReviewErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("pause", "/api/voice-review/drafts/:id/pause", {
      headers: VoiceReviewHeaders,
      params: VoiceReviewParams,
      payload: VoiceReviewRevisionPayload,
      success: VoiceReviewMutationResult,
      error: VoiceReviewErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("play", "/api/voice-review/drafts/:id/play", {
      headers: VoiceReviewHeaders,
      params: VoiceReviewParams,
      payload: VoiceReviewRevisionPayload,
      success: VoiceReviewMutationResult,
      error: VoiceReviewErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("editBegin", "/api/voice-review/drafts/:id/edit-begin", {
      headers: VoiceReviewHeaders,
      params: VoiceReviewParams,
      payload: VoiceReviewRevisionPayload,
      success: VoiceReviewMutationResult,
      error: VoiceReviewErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("editSave", "/api/voice-review/drafts/:id/edit-save", {
      headers: VoiceReviewHeaders,
      params: VoiceReviewParams,
      payload: VoiceReviewEditSavePayload,
      success: VoiceReviewMutationResult,
      error: VoiceReviewErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("editCancel", "/api/voice-review/drafts/:id/edit-cancel", {
      headers: VoiceReviewHeaders,
      params: VoiceReviewParams,
      payload: VoiceReviewEditCancelPayload,
      success: VoiceReviewMutationResult,
      error: VoiceReviewErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("sendNow", "/api/voice-review/drafts/:id/send-now", {
      headers: VoiceReviewHeaders,
      params: VoiceReviewParams,
      payload: VoiceReviewRevisionPayload,
      success: VoiceReviewMutationResult,
      error: VoiceReviewErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("delete", "/api/voice-review/drafts/:id/delete", {
      headers: VoiceReviewHeaders,
      params: VoiceReviewParams,
      payload: VoiceReviewRevisionPayload,
      success: VoiceReviewMutationResult,
      error: VoiceReviewErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  ) {}

export class ProviderQueueHttpApi extends HttpApiGroup.make("providerQueue")
  .add(
    HttpApiEndpoint.get("inventory", "/api/provider-queue/inventory", {
      headers: OptionalBearerHeaders,
      success: ProviderQueueInventory,
      error: [EnvironmentScopeRequiredError],
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("usage", "/api/provider-queue/instances/:instanceId/usage", {
      headers: OptionalBearerHeaders,
      params: Schema.Struct({ instanceId: ProviderInstanceId }),
      success: ProviderQueueRefreshResult,
      error: [EnvironmentScopeRequiredError],
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("refresh", "/api/provider-queue/instances/:instanceId/refresh", {
      headers: OptionalBearerHeaders,
      params: Schema.Struct({ instanceId: ProviderInstanceId }),
      success: ProviderQueueRefreshResult,
      error: [EnvironmentScopeRequiredError],
    }).middleware(EnvironmentAuthenticatedAuth),
  ) {}

export class EnvironmentHttpApi extends HttpApi.make("environment")
  .add(EnvironmentHostStatusHttpApi)
  .add(EnvironmentJonesUpdatesHttpApi)
  .add(EnvironmentVoiceReviewHttpApi)
  .add(ProviderQueueHttpApi)
  .add(EnvironmentMetadataHttpApi)
  .add(EnvironmentAuthHttpApi)
  .add(EnvironmentOrchestrationHttpApi)
  .add(EnvironmentPullRequestsHttpApi)
  .add(EnvironmentWorkstreamsHttpApi)
  .add(EnvironmentWorkstreamsNativeHttpApi)
  .add(EnvironmentConnectHttpApi)
  .add(EnvironmentConversationLibraryHttpApi)
  .add(EnvironmentProjectsHttpApi) {}
