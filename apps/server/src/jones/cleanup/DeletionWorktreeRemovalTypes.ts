import { ProjectId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

// Original #148 start identity. These values attest no admission without their durable owner.
export const DeletionWorktreeRemovalTargetV1 = Schema.Struct({
  projectId: ProjectId,
  projectRoot: Schema.NonEmptyString,
  path: Schema.NonEmptyString,
  branch: Schema.NullOr(Schema.String),
  force: Schema.Boolean,
});
export type DeletionWorktreeRemovalTargetV1 = typeof DeletionWorktreeRemovalTargetV1.Type;
export const DeletionWorktreeRemovalStartV1 = Schema.Struct({
  schema: Schema.Literal("t3.deletion-worktree-removal-start/v1"),
  effectId: Schema.NonEmptyString,
  bindingSha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  workerId: Schema.NonEmptyString,
  expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)),
  target: DeletionWorktreeRemovalTargetV1,
  startedAt: Schema.String.check(Schema.isPattern(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)),
});
export type DeletionWorktreeRemovalStartV1 = typeof DeletionWorktreeRemovalStartV1.Type;
