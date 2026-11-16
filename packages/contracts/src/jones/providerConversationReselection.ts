import * as Schema from "effect/Schema";
import {
  CommandId,
  MessageId,
  NodeId,
  NonNegativeInt,
  ProviderSessionId,
  ProviderThreadId,
  RunId,
  ScheduledTaskId,
  ThreadId,
  TrimmedNonEmptyString,
} from "../baseSchemas.ts";
import { ProviderInstanceId } from "../providerInstance.ts";
import { ModelSelection } from "../modelSelection.ts";

export const ProviderConversationReselectionReason = Schema.Literals([
  "actor_unavailable",
  "invalid_request",
  "thread_unavailable",
  "active_pointer_changed",
  "target_already_selected",
  "current_unavailable",
  "target_unavailable",
  "row_ownership_conflict",
  "row_active",
  "background_work_pending",
  "target_identity_conflict",
  "model_selection_conflict",
  "started_history_conflict",
  "later_run_started",
  "run_pending",
  "runtime_input_pending",
  "context_transfer_pending",
  "handoff_pending",
  "runtime_attached",
  "runtime_unknown",
  "runtime_unavailable",
  "preimage_changed",
  "command_identity_conflict",
  "commit_unconfirmed",
  "state_unavailable",
  "service_unavailable",
  "startup_unavailable",
]);
export type ProviderConversationReselectionReason =
  typeof ProviderConversationReselectionReason.Type;
export class ProviderConversationReselectionError extends Schema.TaggedError<ProviderConversationReselectionError>()(
  "ProviderConversationReselectionError",
  { reason: ProviderConversationReselectionReason },
) {}
export const ProviderConversationReselectionInput = Schema.Struct({
  threadId: ThreadId,
  targetProviderThreadId: ProviderThreadId,
  expectedTargetNativeThreadId: TrimmedNonEmptyString,
  expectedActiveProviderThreadId: ProviderThreadId,
});
export type ProviderConversationReselectionInput = typeof ProviderConversationReselectionInput.Type;
export const ProviderConversationReselectionRun = Schema.Struct({
  runId: RunId,
  ordinal: NonNegativeInt,
  status: Schema.String,
  messageId: MessageId,
  creationSource: Schema.NullOr(Schema.String),
  scheduledTaskId: Schema.optional(ScheduledTaskId),
  senderThreadId: Schema.optional(ThreadId),
  delegatedTaskIds: Schema.optional(Schema.Array(NodeId)),
});
export const ProviderConversationReselectionRow = Schema.Struct({
  id: ProviderThreadId,
  instance: ProviderInstanceId,
  session: Schema.NullOr(ProviderSessionId),
  status: Schema.String,
  nativeId: Schema.NullOr(Schema.String),
  firstRunOrdinal: Schema.NullOr(NonNegativeInt),
  lastRunOrdinal: Schema.NullOr(NonNegativeInt),
  hasGeneration: Schema.Boolean,
  evidenceRevision: Schema.NullOr(NonNegativeInt),
  requestedInstance: Schema.NullOr(ProviderInstanceId),
  pendingBackgroundTaskCount: NonNegativeInt,
});
export const PreviewProviderConversationReselectionResult = Schema.Struct({
  status: Schema.Literals(["eligible", "ineligible"]),
  refusalCodes: Schema.Array(ProviderConversationReselectionReason),
  preimageDigest: Schema.String,
  thread: Schema.NullOr(
    Schema.Struct({
      activeProviderThreadId: Schema.NullOr(ProviderThreadId),
      providerInstanceId: ProviderInstanceId,
      modelSelection: ModelSelection,
      maxRunOrdinal: NonNegativeInt,
    }),
  ),
  current: Schema.NullOr(ProviderConversationReselectionRow),
  target: Schema.NullOr(ProviderConversationReselectionRow),
  runsSinceTarget: Schema.Array(ProviderConversationReselectionRun),
  runtimeAttachment: Schema.Literals(["stopped", "attached", "unknown", "unavailable"]),
});
export type PreviewProviderConversationReselectionResult =
  typeof PreviewProviderConversationReselectionResult.Type;
export const ReselectProviderConversationInput = Schema.Struct({
  ...ProviderConversationReselectionInput.fields,
  commandId: CommandId,
  expectedPreimageDigest: TrimmedNonEmptyString,
});
export type ReselectProviderConversationInput = typeof ReselectProviderConversationInput.Type;
export const ReselectProviderConversationResult = Schema.Struct({
  commandId: CommandId,
  threadId: ThreadId,
  status: Schema.Literals(["committed", "already_committed"]),
  resultSequence: NonNegativeInt,
  previousActiveProviderThreadId: ProviderThreadId,
  activeProviderThreadId: Schema.NullOr(ProviderThreadId),
});
export type ReselectProviderConversationResult = typeof ReselectProviderConversationResult.Type;
