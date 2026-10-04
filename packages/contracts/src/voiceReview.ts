import * as Schema from "effect/Schema";
import { ThreadRegistryAssociation } from "./threadRegistry.ts";
import { NonNegativeInt, PositiveInt, IsoDateTime } from "./baseSchemas.ts";

export const VoiceReviewState = Schema.Literals([
  "held",
  "paused",
  "editing",
  "blocked",
  "released",
  "deleted",
  "expired",
]);
export type VoiceReviewState = typeof VoiceReviewState.Type;
export const VoiceReviewReason = Schema.Literals([
  "source_unavailable",
  "grant_denied",
  "backend_unavailable",
  "capacity",
  "route_required",
  "awaiting_dependency",
  "owner_required",
]);
export const VoiceReviewText = Schema.String.check(
  Schema.isMaxLength(100_000),
  Schema.isPattern(/\S/),
);
export const VoiceReviewDraft = Schema.Struct({
  id: Schema.String.check(Schema.isNonEmpty(), Schema.isMaxLength(256)),
  source_id: Schema.String,
  state: VoiceReviewState,
  revision: PositiveInt,
  text: Schema.NullOr(VoiceReviewText),
  transcript_provider: Schema.String,
  language: Schema.NullOr(Schema.String),
  edited: Schema.Boolean,
  created_at: IsoDateTime,
  updated_at: IsoDateTime,
  due_at: Schema.NullOr(IsoDateTime),
  remaining_ms: Schema.NullOr(NonNegativeInt),
  expires_at: IsoDateTime,
  command_id: Schema.NullOr(Schema.String),
  command_status: Schema.NullOr(Schema.String),
  reason: Schema.NullOr(VoiceReviewReason),
  server_now: IsoDateTime,
  routing_state: Schema.optional(
    Schema.Literals([
      "unclassified",
      "classifying",
      "proposed",
      "owner_required",
      "awaiting_dependency",
      "frozen",
    ]),
  ),
  routing_ref: Schema.optional(Schema.NullOr(Schema.String)),
  routing_target: Schema.optional(Schema.NullOr(Schema.String)),
  dependencies: Schema.optional(Schema.Array(Schema.String)),
});
export type VoiceReviewDraft = typeof VoiceReviewDraft.Type;
export const VoiceReviewDraftList = Schema.Struct({
  server_now: IsoDateTime,
  drafts: Schema.Array(VoiceReviewDraft).check(Schema.isMaxLength(200)),
});
export type VoiceReviewDraftList = typeof VoiceReviewDraftList.Type;
export const VoiceReviewMutationResult = Schema.Struct({
  draft: VoiceReviewDraft,
  edit_handle: Schema.NullOr(Schema.String.check(Schema.isNonEmpty(), Schema.isMaxLength(4096))),
});
export type VoiceReviewMutationResult = typeof VoiceReviewMutationResult.Type;
export const VoiceReviewAction = Schema.Literals([
  "pause",
  "play",
  "edit-begin",
  "edit-save",
  "edit-cancel",
  "send-now",
  "delete",
]);
export type VoiceReviewAction = typeof VoiceReviewAction.Type;
const strictPayload = <const Fields extends Schema.Struct.Fields>(fields: Fields) =>
  Schema.Record(Schema.String, Schema.Unknown)
    .check(Schema.isPropertyNames(Schema.Literals(Object.keys(fields))))
    .pipe(Schema.decodeTo(Schema.Struct(fields)));

export const VoiceReviewRevisionPayload = strictPayload({
  expected_revision: PositiveInt,
});
export const VoiceReviewEditSavePayload = strictPayload({
  expected_revision: PositiveInt,
  text: VoiceReviewText,
  edit_handle: Schema.String.check(Schema.isNonEmpty(), Schema.isMaxLength(4096)),
});
export const VoiceReviewEditCancelPayload = strictPayload({
  expected_revision: PositiveInt,
  edit_handle: Schema.String.check(Schema.isNonEmpty(), Schema.isMaxLength(4096)),
});
export type VoiceReviewMutationPayload =
  | typeof VoiceReviewRevisionPayload.Type
  | typeof VoiceReviewEditSavePayload.Type
  | typeof VoiceReviewEditCancelPayload.Type;

export class VoiceReviewNotConfiguredError extends Schema.TaggedError<VoiceReviewNotConfiguredError>()(
  "VoiceReviewNotConfiguredError",
  {},
  { httpApiStatus: 503 },
) {
  override get message() {
    return "Voice review is not configured for this environment.";
  }
}
export class VoiceReviewForbiddenError extends Schema.TaggedError<VoiceReviewForbiddenError>()(
  "VoiceReviewForbiddenError",
  {},
  { httpApiStatus: 403 },
) {
  override get message() {
    return "This session cannot access voice review.";
  }
}
export class VoiceReviewNotFoundError extends Schema.TaggedError<VoiceReviewNotFoundError>()(
  "VoiceReviewNotFoundError",
  {},
  { httpApiStatus: 404 },
) {
  override get message() {
    return "The voice draft is unavailable.";
  }
}
export class VoiceReviewConflictError extends Schema.TaggedError<VoiceReviewConflictError>()(
  "VoiceReviewConflictError",
  {},
  { httpApiStatus: 409 },
) {
  override get message() {
    return "The voice draft changed. Refresh it before acting again.";
  }
}
export class VoiceReviewUnavailableError extends Schema.TaggedError<VoiceReviewUnavailableError>()(
  "VoiceReviewUnavailableError",
  {},
  { httpApiStatus: 502 },
) {
  override get message() {
    return "Voice review is unavailable. Refresh the draft to observe its current state.";
  }
}
export const VoiceReviewErrors = [
  VoiceReviewNotConfiguredError,
  VoiceReviewForbiddenError,
  VoiceReviewNotFoundError,
  VoiceReviewConflictError,
  VoiceReviewUnavailableError,
] as const;
export const VoiceReviewError = Schema.Union(VoiceReviewErrors);
export type VoiceReviewError = typeof VoiceReviewError.Type;

export const VoiceReviewRecentEntry = Schema.Struct({
  draft: VoiceReviewDraft,
  thread_key: Schema.optional(Schema.NullOr(Schema.String)),
  text: Schema.NullOr(VoiceReviewText),
  text_state: Schema.Literals(["available", "deleted", "expired", "unavailable"]),
  text_origin: Schema.NullOr(Schema.Literals(["draft", "retained_command"])),
  original_source_text: Schema.NullOr(VoiceReviewText),
  command_id: Schema.NullOr(Schema.String),
  associations: Schema.optional(Schema.Array(ThreadRegistryAssociation)),
  workstream_refs: Schema.Array(Schema.String.check(Schema.isPattern(/^(native|inferred):.+$/))),
});
export type VoiceReviewRecentEntry = typeof VoiceReviewRecentEntry.Type;
export const VoiceReviewRecentList = Schema.Struct({
  schema: Schema.Literal("voice.recent-prompts/v1"),
  server_now: IsoDateTime,
  partial: Schema.Boolean,
  unavailable: Schema.Array(Schema.String),
  entries: Schema.Array(VoiceReviewRecentEntry).check(Schema.isMaxLength(200)),
});
export type VoiceReviewRecentList = typeof VoiceReviewRecentList.Type;
export const VoiceReviewDiagnostics = Schema.Struct({
  schema: Schema.Literal("voice.routing-diagnostics/v1"),
  draft_id: Schema.String,
  source_id: Schema.String,
  draft_revision: PositiveInt,
  routing_state: Schema.String,
  jobs: Schema.Array(
    Schema.Struct({
      job_id: Schema.String,
      state: Schema.String,
      input_digest: Schema.String,
      manifest: Schema.Record(Schema.String, Schema.Unknown),
      requested_model: Schema.NullOr(Schema.String),
      observed_model: Schema.NullOr(Schema.String),
      requested_effort: Schema.NullOr(Schema.String),
      observed_effort: Schema.NullOr(Schema.String),
      validation_outcome: Schema.NullOr(Schema.String),
      void_reason: Schema.NullOr(Schema.String),
      usage: Schema.Record(Schema.String, Schema.Unknown),
    }),
  ),
  unavailable: Schema.Array(Schema.String),
});
export type VoiceReviewDiagnostics = typeof VoiceReviewDiagnostics.Type;
