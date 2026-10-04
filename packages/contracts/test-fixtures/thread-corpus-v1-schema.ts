// Historical V1 wire schemas extracted from packages/contracts/src/orchestration.ts
// at ca7c1103c21a05fa7fed5df5dd9fb639aa14e57c. Test-only: these do not describe
// or advertise the live V2 endpoints. Retained shared schemas keep identical shapes.
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  CheckpointRef,
  CommandId,
  EventId,
  IsoDateTime,
  MessageId,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
  ThreadId,
  TrimmedNonEmptyString,
  TurnId,
} from "../src/baseSchemas.ts";
import { ChatAttachment } from "../src/chatAttachment.ts";
import { OrchestrationMessageContext } from "../src/composerContext.ts";
import { RepositoryIdentity, ThreadEnvMode } from "../src/environment.ts";
import { ModelSelection } from "../src/modelSelection.ts";
import { OrchestrationSessionStatus } from "../src/orchestrationNative.ts";
import { ProjectFaviconPath, ProjectIconOverride, ProjectScript } from "../src/project.ts";
import { ProviderInstanceId } from "../src/providerInstance.ts";
import {
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  ProviderInteractionMode,
  RuntimeMode,
} from "../src/providerPolicy.ts";
import { RuntimeIdentityAttestation } from "../src/runtimeIdentity.ts";
import { ThreadLinkedPullRequest, ThreadPullRequestLink } from "../src/threadPullRequest.ts";
import { ThreadTitleRegeneration } from "../src/threadTitle.ts";

/** `reasoning` carries a provider's thinking trace: a reasoning summary, or
 *  the raw chain of thought when the model exposes one. It is a sibling of the
 *  assistant text it precedes, not a replacement for it. */
const OrchestrationMessageRole = Schema.Literals(["user", "assistant", "system", "reasoning"]);

const OrchestrationMessage = Schema.Struct({
  id: MessageId,
  role: OrchestrationMessageRole,
  text: Schema.String,
  attachments: Schema.optional(Schema.Array(ChatAttachment)),
  context: Schema.optional(OrchestrationMessageContext),
  turnId: Schema.NullOr(TurnId),
  streaming: Schema.Boolean,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});

const OrchestrationProposedPlanId = TrimmedNonEmptyString;

const OrchestrationProposedPlan = Schema.Struct({
  id: OrchestrationProposedPlanId,
  turnId: Schema.NullOr(TurnId),
  planMarkdown: TrimmedNonEmptyString,
  implementedAt: Schema.NullOr(IsoDateTime).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
  implementationThreadId: Schema.NullOr(ThreadId).pipe(
    Schema.withDecodingDefault(Effect.succeed(null)),
  ),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});

const SourceProposedPlanReference = Schema.Struct({
  threadId: ThreadId,
  planId: OrchestrationProposedPlanId,
});

const OrchestrationSession = Schema.Struct({
  threadId: ThreadId,
  status: OrchestrationSessionStatus,
  providerName: Schema.NullOr(TrimmedNonEmptyString),
  providerInstanceId: Schema.optional(ProviderInstanceId),
  runtimeMode: RuntimeMode.pipe(Schema.withDecodingDefault(Effect.succeed(DEFAULT_RUNTIME_MODE))),
  activeTurnId: Schema.NullOr(TurnId),
  lastError: Schema.NullOr(TrimmedNonEmptyString),
  /** Requested route beside provider-attested identity. Optional for old snapshots. */
  runtimeIdentity: Schema.optional(RuntimeIdentityAttestation),
  updatedAt: IsoDateTime,
});

const OrchestrationCheckpointFile = Schema.Struct({
  path: TrimmedNonEmptyString,
  kind: TrimmedNonEmptyString,
  additions: NonNegativeInt,
  deletions: NonNegativeInt,
});

const OrchestrationCheckpointStatus = Schema.Literals(["ready", "missing", "error"]);

const OrchestrationCheckpointSummary = Schema.Struct({
  turnId: TurnId,
  checkpointTurnCount: NonNegativeInt,
  checkpointRef: CheckpointRef,
  status: OrchestrationCheckpointStatus,
  files: Schema.Array(OrchestrationCheckpointFile),
  assistantMessageId: Schema.NullOr(MessageId),
  completedAt: IsoDateTime,
});

const OrchestrationThreadActivityTone = Schema.Literals(["info", "tool", "approval", "error"]);

const OrchestrationThreadActivity = Schema.Struct({
  id: EventId,
  tone: OrchestrationThreadActivityTone,
  kind: TrimmedNonEmptyString,
  summary: TrimmedNonEmptyString,
  payload: Schema.Unknown,
  turnId: Schema.NullOr(TurnId),
  sequence: Schema.optional(NonNegativeInt),
  createdAt: IsoDateTime,
});

const OrchestrationLatestTurnState = Schema.Literals([
  "running",
  "interrupted",
  "completed",
  "error",
]);

const OrchestrationLatestTurn = Schema.Struct({
  turnId: TurnId,
  state: OrchestrationLatestTurnState,
  requestedAt: IsoDateTime,
  startedAt: Schema.NullOr(IsoDateTime),
  completedAt: Schema.NullOr(IsoDateTime),
  assistantMessageId: Schema.NullOr(MessageId),
  sourceProposedPlan: Schema.optional(SourceProposedPlanReference),
});

// Version changes even when a manual rename keeps the same text.
const ThreadTitleState = Schema.Struct({
  source: Schema.Literals(["manual", "generated"]),
  version: CommandId,
  needsRefinement: Schema.Boolean,
});

const OrchestrationThread = Schema.Struct({
  id: ThreadId,
  projectId: ProjectId,
  title: TrimmedNonEmptyString,
  modelSelection: ModelSelection,
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode.pipe(
    Schema.withDecodingDefault(Effect.succeed(DEFAULT_PROVIDER_INTERACTION_MODE)),
  ),
  branch: Schema.NullOr(TrimmedNonEmptyString),
  worktreePath: Schema.NullOr(TrimmedNonEmptyString),
  linkedPullRequest: Schema.optional(Schema.NullOr(ThreadLinkedPullRequest)),
  // Optional so payloads from pre-link servers still decode.
  pullRequests: Schema.Array(ThreadPullRequestLink).pipe(
    Schema.withDecodingDefault(Effect.succeed([])),
  ),
  branchPullRequest: Schema.optional(Schema.NullOr(ThreadLinkedPullRequest)),
  latestTurn: Schema.NullOr(OrchestrationLatestTurn),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
  archivedAt: Schema.NullOr(IsoDateTime).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
  settledOverride: Schema.NullOr(Schema.Literals(["settled", "active"])).pipe(
    Schema.withDecodingDefault(Effect.succeed(null)),
  ),
  settledAt: Schema.NullOr(IsoDateTime).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
  // When the thread last re-entered the active list (any thread.unsettled).
  // Anchors the active-list sort so an unsettled thread surfaces at the top
  // instead of sinking back to its creation-order slot. Cleared on settle.
  // Optional so payloads from pre-stamp servers still decode.
  unsettledAt: Schema.optional(Schema.NullOr(IsoDateTime)),
  // Snooze is an overlay on the active lifecycle, not a fourth destination:
  // a snoozed thread stays "active" in the model and is only suppressed from
  // the inbox until snoozedUntil passes (or the thread raises its hand).
  // Optional so payloads from pre-snooze servers still decode.
  snoozedUntil: Schema.optional(Schema.NullOr(IsoDateTime)),
  snoozedAt: Schema.optional(Schema.NullOr(IsoDateTime)),
  // Active pinned threads render in the pinned block. Settled and snoozed
  // threads remain in their respective shelves even when pinned.
  // Optional so payloads from pre-pinning servers still decode.
  pinnedAt: Schema.optional(Schema.NullOr(IsoDateTime)),
  // Fractional index for user-arranged pinned order. Keyed threads sort by
  // string comparison ahead of keyless ones (which keep creation order), so
  // servers never need each other's threads to agree on the merged list.
  // Optional so payloads from pre-reorder servers still decode.
  pinOrderKey: Schema.optional(Schema.NullOr(TrimmedNonEmptyString)),
  // Manual Active placement. Keyless threads retain their creation/re-entry
  // order above the arranged run. Settling clears this slot.
  activeOrderKey: Schema.optional(Schema.NullOr(TrimmedNonEmptyString)),
  // Set while the user has turned automatic settlement off for this thread.
  // Survives manual settle, un-settle, and activity: only the user clears it.
  // Optional so payloads from older servers still decode.
  autoSettleDisabledAt: Schema.optional(Schema.NullOr(IsoDateTime)),
  // Pending-only state. Optional so older servers remain compatible.
  titleRegeneration: Schema.optional(Schema.NullOr(ThreadTitleRegeneration)),
  titleState: Schema.optional(Schema.NullOr(ThreadTitleState)),
  deletedAt: Schema.NullOr(IsoDateTime),
  messages: Schema.Array(OrchestrationMessage),
  proposedPlans: Schema.Array(OrchestrationProposedPlan).pipe(
    Schema.withDecodingDefault(Effect.succeed([])),
  ),
  activities: Schema.Array(OrchestrationThreadActivity),
  checkpoints: Schema.Array(OrchestrationCheckpointSummary),
  session: Schema.NullOr(OrchestrationSession),
});

const OrchestrationProjectShell = Schema.Struct({
  id: ProjectId,
  title: TrimmedNonEmptyString,
  workspaceRoot: TrimmedNonEmptyString,
  repositoryIdentity: Schema.optional(Schema.NullOr(RepositoryIdentity)),
  defaultModelSelection: Schema.NullOr(ModelSelection),
  defaultThreadEnvMode: Schema.optional(Schema.NullOr(ThreadEnvMode)),
  autoPull: Schema.optional(Schema.Boolean),
  // Optional on the wire so cached snapshots from older servers still decode.
  faviconPath: Schema.optional(Schema.NullOr(ProjectFaviconPath)),
  projectIcon: Schema.optional(Schema.NullOr(ProjectIconOverride)),
  scripts: Schema.Array(ProjectScript),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});

const OrchestrationThreadShell = Schema.Struct({
  id: ThreadId,
  projectId: ProjectId,
  title: TrimmedNonEmptyString,
  modelSelection: ModelSelection,
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode.pipe(
    Schema.withDecodingDefault(Effect.succeed(DEFAULT_PROVIDER_INTERACTION_MODE)),
  ),
  branch: Schema.NullOr(TrimmedNonEmptyString),
  worktreePath: Schema.NullOr(TrimmedNonEmptyString),
  linkedPullRequest: Schema.optional(Schema.NullOr(ThreadLinkedPullRequest)),
  pullRequests: Schema.Array(ThreadPullRequestLink).pipe(
    Schema.withDecodingDefault(Effect.succeed([])),
  ),
  branchPullRequest: Schema.optional(Schema.NullOr(ThreadLinkedPullRequest)),
  latestTurn: Schema.NullOr(OrchestrationLatestTurn),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
  archivedAt: Schema.NullOr(IsoDateTime).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
  settledOverride: Schema.NullOr(Schema.Literals(["settled", "active"])).pipe(
    Schema.withDecodingDefault(Effect.succeed(null)),
  ),
  settledAt: Schema.NullOr(IsoDateTime).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
  // See OrchestrationThread.unsettledAt: last re-entry into the active list.
  unsettledAt: Schema.optional(Schema.NullOr(IsoDateTime)),
  snoozedUntil: Schema.optional(Schema.NullOr(IsoDateTime)),
  snoozedAt: Schema.optional(Schema.NullOr(IsoDateTime)),
  pinnedAt: Schema.optional(Schema.NullOr(IsoDateTime)),
  pinOrderKey: Schema.optional(Schema.NullOr(TrimmedNonEmptyString)),
  activeOrderKey: Schema.optional(Schema.NullOr(TrimmedNonEmptyString)),
  autoSettleDisabledAt: Schema.optional(Schema.NullOr(IsoDateTime)),
  titleRegeneration: Schema.optional(Schema.NullOr(ThreadTitleRegeneration)),
  titleState: Schema.optional(Schema.NullOr(ThreadTitleState)),
  session: Schema.NullOr(OrchestrationSession),
  latestUserMessageAt: Schema.NullOr(IsoDateTime),
  hasPendingApprovals: Schema.Boolean,
  hasPendingUserInput: Schema.Boolean,
  hasActionableProposedPlan: Schema.Boolean,
  /**
   * Native background work alive after the turn settles: "working" while
   * subagents/workflows run, "monitoring" when watch loops are the only
   * live work. Optional so old servers/clients interop; absent = none.
   */
  backgroundLiveness: Schema.optional(Schema.NullOr(Schema.Literals(["working", "monitoring"]))),
  /**
   * Current plan step while a turn runs, for the Working indicators
   * (sidebar row, in-chat working line). Cleared when the turn settles —
   * never persists as stale UI. Optional so old servers/clients interop.
   */
  planProgress: Schema.optional(
    Schema.NullOr(
      Schema.Struct({
        step: TrimmedNonEmptyString,
        completedSteps: NonNegativeInt,
        totalSteps: NonNegativeInt,
      }),
    ),
  ),
});

export const OrchestrationShellSnapshot = Schema.Struct({
  snapshotSequence: NonNegativeInt,
  projects: Schema.Array(OrchestrationProjectShell),
  threads: Schema.Array(OrchestrationThreadShell),
  updatedAt: IsoDateTime,
});

/**
 * Bounds a thread detail read to a window of recent turns. `turnLimit` counts
 * turns with a user pending message (subagent/fan-out turns between them ride
 * along), so the window always contains the last N user prompts. `beforeCursor`
 * requests the disjoint page of older turns strictly before a previously
 * returned cursor. Requests without a window get the full thread; pagination is
 * strictly opt-in so older clients keep today's behavior on both HTTP and the
 * WebSocket fallback snapshot.
 */
export const OrchestrationThreadDetailWindow = Schema.Struct({
  turnLimit: Schema.optionalKey(PositiveInt),
  beforeCursor: Schema.optionalKey(TrimmedNonEmptyString),
});

/**
 * Page metadata for a windowed thread detail read. `beforeCursor` is opaque and
 * exclusive: passing it back returns the adjacent disjoint slice of older
 * turns. `null` means the thread is fully loaded below this page. The
 * `snapshotSequence` mirrors the top-level snapshot sequence so history pages
 * can be sequence-checked against live state before merging.
 */
const OrchestrationThreadDetailPage = Schema.Struct({
  beforeCursor: Schema.NullOr(TrimmedNonEmptyString),
  hasMore: Schema.Boolean,
  snapshotSequence: NonNegativeInt,
  /**
   * Highest event sequence applied to THIS thread at page read time. The
   * global `snapshotSequence` advances with every thread's events, so a
   * client cannot wait for it via its per-thread subscription; this
   * thread-scoped watermark is reachable. A client merging an older page
   * must first have applied live events up to it — otherwise a streaming
   * turn outside the loaded window could have deltas replayed on top of
   * page content that already includes them, duplicating text.
   */
  threadSequence: Schema.optionalKey(NonNegativeInt),
});

export const OrchestrationThreadDetailSnapshot = Schema.Struct({
  snapshotSequence: NonNegativeInt,
  thread: OrchestrationThread,
  // Present only on windowed responses. Absent on full snapshots (and from
  // pre-pagination servers), which clients treat as fully loaded.
  page: Schema.optional(OrchestrationThreadDetailPage),
});
