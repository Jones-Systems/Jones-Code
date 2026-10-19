import * as Schema from "effect/Schema";
import { IsoDateTime, NonNegativeInt, RuntimeRequestId, ThreadId } from "../baseSchemas.ts";
import { ProviderRequestKind } from "../providerPolicy.ts";

export const THREAD_ATTENTION_CAPABILITY = { schemaVersion: "t3.thread-attention/v1" } as const;
export const ThreadAttentionCapability = Schema.Struct({
  schemaVersion: Schema.Literal(THREAD_ATTENTION_CAPABILITY.schemaVersion),
});

export const ThreadAttentionStatus = Schema.Literals([
  "approval",
  "input",
  "working",
  "waiting",
  "failed",
  "limited",
  "ready",
]);
export type ThreadAttentionStatus = typeof ThreadAttentionStatus.Type;
export const ThreadAttentionPill = Schema.Literals([
  "Pending Approval",
  "Awaiting Input",
  "Working",
  "Connecting",
  "Waiting",
  "Plan Ready",
  "Completed",
]);
export type ThreadAttentionPill = typeof ThreadAttentionPill.Type;

/** Read-only UI evidence. Residual `ready` is not a review or send authorization. */
export const ThreadAttention = Schema.Struct({
  schemaVersion: ThreadAttentionCapability.fields.schemaVersion,
  threadId: ThreadId,
  // This is the global application cursor, not a request version or thread cursor.
  snapshotSequence: NonNegativeInt,
  status: ThreadAttentionStatus,
  pill: Schema.NullOr(ThreadAttentionPill),
  unseenCompletion: Schema.Boolean,
  pendingChildKind: Schema.NullOr(Schema.Literals(["approval", "user_input"])),
  ownRequest: Schema.NullOr(
    Schema.Struct({
      id: RuntimeRequestId,
      kind: Schema.Union([
        ProviderRequestKind,
        Schema.Literals(["dynamic_tool_call", "user_input", "auth_refresh"]),
      ]),
      createdAt: IsoDateTime,
    }),
  ),
  coverage: Schema.Struct({
    source: Schema.Literal("shell"),
    childRequests: Schema.Literal("active-shell"),
    latestVisibleMessage: Schema.Literal("unavailable"),
    requestVersion: Schema.Literal("unavailable"),
    requestDetails: Schema.Literal("unavailable"),
    threadSequence: Schema.Literal("unavailable"),
    lastVisitedAt: Schema.Literals(["available", "unavailable"]),
  }),
});
export type ThreadAttention = typeof ThreadAttention.Type;
