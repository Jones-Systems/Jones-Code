import { CommandId, ThreadId, WorktreeCleanupRules } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { DeletionGroupWitnessV1 } from "./DeletionGroupWitness.ts";
import { DeletionWorktreeRemovalTargetV1 } from "./DeletionWorktreeRemovalTypes.ts";
import { CompletedNativeCleanupMembers } from "./CompletedNativeCleanup.ts";

const bindingFields = {
  actor: Schema.Literal("storage-cleanup-policy"),
  threadId: ThreadId,
  birthEventId: Schema.NonEmptyString,
  birthSequence: Schema.Int,
  deletionEventId: Schema.NonEmptyString,
  deletionSequence: Schema.Int,
  commandId: CommandId,
  deletionPayload: Schema.String,
  target: DeletionWorktreeRemovalTargetV1,
  rules: WorktreeCleanupRules,
};
export const DeletionAdmissionWitnessBinding = Schema.Struct({
  ...bindingFields,
  schema: Schema.Literal("jones.deletion-admission/v2"),
  groupWitness: DeletionGroupWitnessV1,
});
export const DeletionAdmissionCompletedNativeBinding = Schema.Struct({
  ...bindingFields,
  schema: Schema.Literal("jones.deletion-admission/v3"),
  completedNativeMembers: CompletedNativeCleanupMembers,
});
export const DeletionAdmissionBinding = Schema.Union([
  DeletionAdmissionCompletedNativeBinding,
  DeletionAdmissionWitnessBinding,
  Schema.Struct(bindingFields),
]);
export type DeletionAdmissionBinding = typeof DeletionAdmissionBinding.Type;
