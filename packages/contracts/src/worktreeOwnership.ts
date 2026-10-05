import * as Schema from "effect/Schema";

import { ThreadId } from "./baseSchemas.ts";

export class WorktreeOwnershipConflictError extends Schema.TaggedError<WorktreeOwnershipConflictError>()(
  "WorktreeOwnershipConflictError",
  {
    resourcePath: Schema.String,
    ownerThreadId: ThreadId,
    requestingThreadId: ThreadId,
    ownerBranch: Schema.NullOr(Schema.String),
    expiresAtMs: Schema.Number,
  },
) {
  override get message(): string {
    const branch =
      this.ownerBranch === null ? "its local checkout" : `branch '${this.ownerBranch}'`;
    return `Thread '${this.ownerThreadId}' owns ${branch} at '${this.resourcePath}'. Delete that owning thread and allow its cleanup to finish before starting mutating work in thread '${this.requestingThreadId}'.`;
  }
}
