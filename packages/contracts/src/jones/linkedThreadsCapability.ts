import * as Schema from "effect/Schema";

export const LINKED_THREADS_CAPABILITY = { schemaVersion: "t3.linked-threads/v1" } as const;
export const LinkedThreadsCapability = Schema.Struct({
  schemaVersion: Schema.Literal("t3.linked-threads/v1"),
});
