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
} from "../voiceReview.ts";
import {
  ThreadRegistryComposedSnapshot,
  ThreadRegistryWorkstreams,
  ThreadRegistryEvents,
  ThreadRegistryAssociationPayload,
  ThreadRegistryLabelPayload,
  ThreadRegistryMutationReceipt,
} from "../threadRegistry.ts";
import * as Schema from "effect/Schema";
import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint";
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup";
import * as HttpApiSchema from "effect/unstable/httpapi/HttpApiSchema";
import { TrimmedNonEmptyString } from "../baseSchemas.ts";
import { HostStatusSnapshot } from "../hostStatus.ts";
import type * as Environment from "../environmentHttp.ts";
import {
  CONVERSATION_LIBRARY_PATH,
  LibraryErrorCodeSchema,
  LibraryReplySchema,
  LibraryRequestSchema,
} from "../conversationLibrary.ts";

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

export const makeJonesHttpGroups = ({
  OptionalBearerHeaders,
  EnvironmentAuthenticatedAuth,
  EnvironmentScopeRequiredError,
  EnvironmentInternalError,
}: {
  readonly OptionalBearerHeaders: Schema.Struct<{
    authorization: Schema.optionalKey<Schema.String>;
    dpop: Schema.optionalKey<Schema.String>;
  }>;
  readonly EnvironmentAuthenticatedAuth: typeof Environment.EnvironmentAuthenticatedAuth;
  readonly EnvironmentScopeRequiredError: typeof Environment.EnvironmentScopeRequiredError;
  readonly EnvironmentInternalError: typeof Environment.EnvironmentInternalError;
}) => {
  class EnvironmentConversationLibraryHttpApi extends HttpApiGroup.make("conversationLibrary").add(
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
          cursor: Schema.optional(
            Schema.String.check(Schema.isNonEmpty(), Schema.isMaxLength(4096)),
          ),
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

  class EnvironmentHostStatusHttpApi extends HttpApiGroup.make("hostStatus").add(
    HttpApiEndpoint.get("snapshot", "/api/host-status", {
      headers: OptionalBearerHeaders,
      success: HostStatusSnapshot,
      error: [EnvironmentScopeRequiredError, EnvironmentInternalError],
    }).middleware(EnvironmentAuthenticatedAuth),
  ) {}

  return {
    EnvironmentVoiceReviewHttpApi,
    EnvironmentHostStatusHttpApi,
    EnvironmentConversationLibraryHttpApi,
  };
};
