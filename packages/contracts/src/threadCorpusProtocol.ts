import * as Schema from "effect/Schema";

import { QUEUE_DISPATCH_CAPABILITY } from "./queueProtocol.ts";

const threadCorpusCapabilityFields = {
  schemaVersion: Schema.Literal("t3.thread-corpus-capability/v1"),
  shellSnapshot: Schema.Literal("t3.thread-corpus-shell/v1"),
  threadDetailPagination: Schema.Literal("t3.thread-corpus-pagination/v1"),
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
 * Read semantics of the existing orchestration shell and thread detail endpoints.
 * The shell carries global snapshotSequence and updatedAt; projects carry id,
 * workspaceRoot and optional nullable repositoryIdentity. Threads join by projectId
 * and carry nullable worktreePath/branch, archivedAt/settledAt, latestUserMessageAt,
 * updatedAt and nullable latestTurn/session. Optional lifecycle fields retain their
 * existing schema defaults; missing activity timestamps do not prove inactivity.
 *
 * Placement is observed worktreePath, or project workspaceRoot when it is null.
 * Neither the path nor repositoryIdentity verifies cwd, enrollment or access grants.
 * This advertisement grants no dispatch, creation or provider quota capability.
 *
 * Detail pagination is opt-in: turnLimit is a positive integer counting user-anchored
 * turns. Associated fan-out rides along, subject to the native 150 raw-turn page cap.
 * Without turnLimit the full snapshot has no page metadata. With it, page carries
 * hasMore, nullable opaque exclusive beforeCursor, snapshotSequence and optional
 * threadSequence. A null cursor marks the oldest page; an absent threadSequence is
 * unknown. Native malformed or foreign-thread cursors reload the first page, so a
 * consumer must check progression instead of assuming each response advances.
 *
 * Detail messages carry string text without a native character limit or truncation
 * flag. turnLimit bounds turns, not bytes; consumer text/response limits must report
 * their own incomplete coverage. Page snapshotSequence mirrors the enclosing
 * snapshot; threadSequence is the per-thread watermark needed before merging pages.
 */
export const THREAD_CORPUS_CAPABILITY = {
  schemaVersion: "t3.thread-corpus-capability/v1",
  shellSnapshot: "t3.thread-corpus-shell/v1",
  threadDetailPagination: "t3.thread-corpus-pagination/v1",
  placement: "t3.thread-corpus-placement/v1",
  authSession: QUEUE_DISPATCH_CAPABILITY.authSession,
} as const satisfies ThreadCorpusCapability;
