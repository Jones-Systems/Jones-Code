import * as Schema from "effect/Schema";

import { QUEUE_DISPATCH_CAPABILITY } from "./queueProtocol.ts";

const threadCorpusCapabilityFields = {
  schemaVersion: Schema.Literal("t3.thread-corpus-capability/v2"),
  shellSnapshot: Schema.Literal("t3.thread-corpus-shell/v2"),
  threadDetailPagination: Schema.Literal("t3.thread-corpus-pagination/v2"),
  placement: Schema.Literal("t3.thread-corpus-placement/v1"),
  authSession: Schema.Literal(QUEUE_DISPATCH_CAPABILITY.authSession),
};

// Validate original wire keys before a struct decoder can strip unknown claims.
export const ThreadCorpusCapability = Schema.flip(
  Schema.flip(Schema.Struct(threadCorpusCapabilityFields)).check(
    Schema.makeFilter((value) =>
      Reflect.ownKeys(value).every((key) => Object.hasOwn(threadCorpusCapabilityFields, key)),
    ),
  ),
);
export type ThreadCorpusCapability = typeof ThreadCorpusCapability.Type;

/**
 * V2 shell and progressive history reads; V1 consumers must reject this version.
 * The shell carries schemaVersion and snapshotSequence, projects, active threads
 * and archivedThreads. Threads join projects by projectId and expose nullable
 * worktreePath/branch, settlement timestamps, latestUserMessageAt, run status,
 * activeRunId and pendingRuntimeRequest. Missing activity does not prove idleness.
 *
 * Placement is observed worktreePath, or project workspaceRoot when it is null.
 * Neither the path nor repositoryIdentity verifies cwd, enrollment or access grants.
 * This advertisement grants no dispatch, creation or provider quota capability.
 *
 * Full detail contains projection and snapshotSequence. Bounded detail retains
 * control-plane state and a recent timeline window with opaque historyCursor,
 * hasMoreHistory and latestLocalTurnOrdinal. Resume live events after the snapshot
 * sequence. Fetch older history with that cursor; pages carry chronological items,
 * nextCursor, hasMoreHistory and snapshotSequence. Null marks the oldest cursor.
 * Cursors must not be parsed or reused across unrelated snapshots/threads; failed
 * or stale reads do not prove complete history. There is no V1 turnLimit/page or
 * per-thread threadSequence contract.
 *
 * Message text has no native character truncation flag. Turn/item/byte budgets
 * can be exceeded to retain complete turns or live control state; consumers must
 * report their own text/response limits and incomplete coverage. A bounded snapshot
 * can explicitly report payloadBudgetExceeded.
 */
export const THREAD_CORPUS_CAPABILITY = {
  schemaVersion: "t3.thread-corpus-capability/v2",
  shellSnapshot: "t3.thread-corpus-shell/v2",
  threadDetailPagination: "t3.thread-corpus-pagination/v2",
  placement: "t3.thread-corpus-placement/v1",
  authSession: QUEUE_DISPATCH_CAPABILITY.authSession,
} as const satisfies ThreadCorpusCapability;
