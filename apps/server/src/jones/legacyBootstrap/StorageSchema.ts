import {
  CommandId,
  EventId,
  MessageId,
  NonNegativeInt,
  OrchestrationV2LegacyPreflightBinding,
  OrchestrationV2LegacyBootstrapPolicy,
  OrchestrationV2PrivateEvent,
  ProjectId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { CommandReceiptV2 } from "../../orchestration-v2/CommandReceiptStore.ts";
import {
  LegacyOwnedTerminalControl,
  LegacyNoTerminalControl,
  RecordedAppThread,
  RecordedRun,
  RecordedEventJson,
} from "../../orchestration-v2/RecordedTypes.ts";

export class LegacyStorageError extends Schema.TaggedError<LegacyStorageError>()(
  "LegacyStorageError",
  {
    reason: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export const ThreadMetadata = Schema.Struct({
  legacyBootstrapClaim: RecordedAppThread.fields.legacyBootstrapClaim,
});
export type ThreadMetadata = typeof ThreadMetadata.Type;
export const RunMetadata = Schema.Struct({
  legacyPreparation: RecordedRun.fields.legacyPreparation,
  legacyReleaseDecision: RecordedRun.fields.legacyReleaseDecision,
  legacyPreparationFailureDecision: RecordedRun.fields.legacyPreparationFailureDecision,
  workspaceRunSetupScript: RecordedRun.fields.workspaceRunSetupScript,
  legacyBootstrap: RecordedRun.fields.legacyBootstrap,
  legacyPreparationFailureKnown: RecordedRun.fields.legacyPreparationFailureKnown,
});
export type RunMetadata = typeof RunMetadata.Type;
export const ThreadIdentity = Schema.Struct({
  threadId: ThreadId,
  projectId: ProjectId,
  createdAt: Schema.NonEmptyString,
  claimEventId: EventId,
  claimSequence: NonNegativeInt,
  claimCommandId: CommandId,
});
export type ThreadIdentity = typeof ThreadIdentity.Type;
export const RunIdentity = Schema.Struct({
  runId: RunId,
  threadId: ThreadId,
  userMessageId: MessageId,
  requestedAt: Schema.NonEmptyString,
  birthEventId: EventId,
  birthSequence: NonNegativeInt,
  birthCommandId: CommandId,
});
export type RunIdentity = typeof RunIdentity.Type;
export const EventMetadata = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("thread"), metadata: ThreadMetadata }),
  Schema.Struct({ kind: Schema.Literal("run"), metadata: RunMetadata }),
]);
export type EventMetadata = typeof EventMetadata.Type;
export const CommandBinding = Schema.Struct({
  commandId: CommandId,
  threadId: ThreadId,
  projectId: ProjectId,
  threadCreatedAt: Schema.NullOr(Schema.NonEmptyString),
  kind: Schema.Literals(["lifecycle", "preflight-intent", "preflight-outcome", "delete"]),
  // Canonical JSON of the fixed operation's complete immutable binding. Transaction owns its semantics.
  bindingJson: Schema.NonEmptyString,
});
export type CommandBinding = typeof CommandBinding.Type;
const deleteFields = {
  commandId: CommandId,
  threadId: ThreadId,
  runId: RunId,
  legacyBootstrap: OrchestrationV2LegacyBootstrapPolicy,
};
export const DeleteCommandBinding = Schema.Union([
  Schema.Struct({
    ...deleteFields,
    type: Schema.Literal("legacy-bootstrap.guard-rejection-delete"),
    legacyOwnedControl: LegacyOwnedTerminalControl,
  }),
  Schema.Struct({
    ...deleteFields,
    type: Schema.Literals([
      "legacy-bootstrap.guard-rejection-delete",
      "legacy-bootstrap.failure-delete",
    ]),
    legacyNoControl: LegacyNoTerminalControl,
  }),
]);
export const LegacyPreflightReceipt = Schema.Struct({
  ...CommandReceiptV2.fields,
  status: Schema.Literal("accepted"),
  error: Schema.Null,
  sequenceDomain: Schema.Literals(["jones-legacy-ordinal", "historical-upstream"]),
});
export type LegacyPreflightReceipt = typeof LegacyPreflightReceipt.Type;
export type LegacyReceipt =
  | { readonly domain: "lifecycle"; readonly receipt: CommandReceiptV2 }
  | { readonly domain: "legacy-delete"; readonly receipt: CommandReceiptV2 }
  | { readonly domain: "legacy-preflight"; readonly receipt: LegacyPreflightReceipt };
export const CommandRecord = Schema.Struct({
  binding: CommandBinding,
  sequenceDomain: Schema.Literals(["lifecycle", "jones-legacy-ordinal"]),
  privateOrdinal: Schema.NullOr(NonNegativeInt),
  receiptDigest: Schema.NonEmptyString,
  receipt: Schema.NullOr(CommandReceiptV2),
  event: Schema.NullOr(OrchestrationV2PrivateEvent),
});
export type CommandRecord = typeof CommandRecord.Type;
export const CommandRecordJson = Schema.Struct({
  ...CommandRecord.fields,
  receipt: Schema.NullOr(
    CommandReceiptV2.mapFields((fields) => ({
      ...fields,
      acceptedAt: Schema.DateTimeUtcFromString,
    })),
  ),
  event: Schema.NullOr(RecordedEventJson),
});
export const TerminalCleanup = Schema.Struct({
  effectId: Schema.NonEmptyString,
  commandId: CommandId,
  threadId: ThreadId,
  control: LegacyOwnedTerminalControl,
  state: Schema.Literals(["pending", "started", "succeeded", "unknown"]),
  attemptId: Schema.NullOr(Schema.NonEmptyString),
  revision: NonNegativeInt,
  outcome: Schema.NullOr(
    Schema.Literals(["closed", "owner-unavailable", "interrupted", "unconfirmed"]),
  ),
});
export type TerminalCleanup = typeof TerminalCleanup.Type;
export type PreflightBinding = typeof OrchestrationV2LegacyPreflightBinding.Type;
