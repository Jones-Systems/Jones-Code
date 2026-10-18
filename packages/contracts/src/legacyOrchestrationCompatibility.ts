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
} from "./baseSchemas.ts";
import { ChatAttachment } from "./chatAttachment.ts";
import { OrchestrationMessageContext } from "./composerContext.ts";
import { RepositoryIdentity, ThreadEnvMode } from "./environment.ts";
import { ModelSelection } from "./modelSelection.ts";
import { ProjectFaviconPath, ProjectIconOverride, ProjectScript } from "./project.ts";
import { ProviderInstanceId } from "./providerInstance.ts";
import {
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  ProviderInteractionMode,
  RuntimeMode,
} from "./providerPolicy.ts";
import {
  ThreadLinkedPullRequest,
  ThreadPullRequestKey,
  ThreadPullRequestLinkSource,
  ThreadPullRequestSnapshot,
  ThreadPullRequestStack,
} from "./threadPullRequest.ts";

/**
 * Legacy wire DTOs retained for queue and corpus consumers. Their TurnId and
 * session vocabulary is independent of V2 RunId and provider-turn identifiers.
 * These schemas describe the compatibility facade's required output; decoding
 * alone does not establish that a server implements or may advertise it.
 */

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

const OrchestrationSessionStatus = Schema.Literals([
  "idle",
  "starting",
  "running",
  "ready",
  "interrupted",
  "stopped",
  "error",
]);

/**
 * One provider-reported runtime identity dimension.
 *
 * `unknown` means no authoritative provider evidence has arrived for the
 * current request. `unavailable` means the provider boundary does not safely
 * attest the dimension. Neither state may be filled from routing settings,
 * authentication metadata, or a model catalog.
 */
const RuntimeIdentityObservation = Schema.Union([
  Schema.Struct({ status: Schema.Literal("unknown") }),
  Schema.Struct({
    status: Schema.Literal("unavailable"),
    reason: TrimmedNonEmptyString,
  }),
  Schema.Struct({
    status: Schema.Literal("observed"),
    value: TrimmedNonEmptyString,
    sourceEvent: TrimmedNonEmptyString,
  }),
]);

const ObservedRuntimeIdentity = Schema.Struct({
  backend: RuntimeIdentityObservation,
  model: RuntimeIdentityObservation,
  account: RuntimeIdentityObservation,
  serviceTier: RuntimeIdentityObservation,
});

const RuntimeIdentityAttestation = Schema.Struct({
  /** Opaque launch correlation. Observations must match this exact runtime. */
  runtimeGeneration: Schema.optional(TrimmedNonEmptyString),
  requested: Schema.Struct({
    providerInstanceId: ProviderInstanceId,
    providerDriver: TrimmedNonEmptyString,
    model: TrimmedNonEmptyString,
    serviceTier: Schema.NullOr(TrimmedNonEmptyString),
  }),
  observed: ObservedRuntimeIdentity,
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

const ThreadTitleRegeneration = Schema.Struct({
  requestId: CommandId,
  startedAt: IsoDateTime,
});

const ThreadPullRequestLink = Schema.Struct({
  ...ThreadPullRequestKey.fields,
  url: TrimmedNonEmptyString,
  source: ThreadPullRequestLinkSource,
  linkedAt: IsoDateTime,
  snapshot: Schema.NullOr(ThreadPullRequestSnapshot),
  stack: Schema.NullOr(ThreadPullRequestStack),
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
export type OrchestrationShellSnapshot = typeof OrchestrationShellSnapshot.Type;

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
export type OrchestrationThreadDetailWindow = typeof OrchestrationThreadDetailWindow.Type;

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
export type OrchestrationThreadDetailSnapshot = typeof OrchestrationThreadDetailSnapshot.Type;

export const ThreadTurnDispatchGuard = Schema.Struct({
  observedSnapshotSequence: NonNegativeInt,
  expectedModelSelection: ModelSelection,
  expectedSessionStatus: Schema.NullOr(OrchestrationSessionStatus),
  expectedActiveTurnId: Schema.NullOr(TurnId),
  expectedLatestTurnId: Schema.NullOr(TurnId),
  requireIdle: Schema.Literal(true),
});
export type ThreadTurnDispatchGuard = typeof ThreadTurnDispatchGuard.Type;

const OrchestrationDispatchTarget = Schema.Struct({
  modelSelection: ModelSelection,
  sessionStatus: Schema.NullOr(OrchestrationSessionStatus),
  activeTurnId: Schema.NullOr(TurnId),
  latestTurnId: Schema.NullOr(TurnId),
  requireIdle: Schema.Literal(true),
  idle: Schema.Boolean,
  blockers: Schema.Array(
    Schema.Literals([
      "archived",
      "settled",
      "pending_turn",
      "running_turn",
      "session_starting",
      "session_running",
      "active_turn",
      "pending_approval",
      "pending_user_input",
      "actionable_plan",
      "background_work",
    ]),
  ),
});

const OrchestrationObservedTurn = Schema.Struct({
  turnId: Schema.NullOr(TurnId),
  state: Schema.Literals(["pending", "running", "interrupted", "completed", "error"]),
  requestedAt: IsoDateTime,
  startedAt: Schema.NullOr(IsoDateTime),
  completedAt: Schema.NullOr(IsoDateTime),
  assistantMessageId: Schema.NullOr(MessageId),
});

const nativeCreationStruct = <Fields extends Schema.Struct.Fields>(fields: Fields) => {
  const schema = Schema.Struct(fields);
  // Flipped checks validate original wire keys that ordinary struct decoding would strip.
  return Schema.flip(
    Schema.flip(schema).check(
      Schema.makeFilter((value) =>
        Reflect.ownKeys(value).every((key) => Object.hasOwn(fields, key)),
      ),
    ),
  );
};

const NativeCreationRevision = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
);
const NativeCreationString = Schema.String.check(Schema.isNonEmpty());
const NativeCreationSha256 = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));

const NativeCreationModelSelection = nativeCreationStruct({
  instanceId: ProviderInstanceId,
  model: NativeCreationString,
  options: Schema.optionalKey(
    Schema.Array(
      nativeCreationStruct({
        id: NativeCreationString,
        value: Schema.Union([NativeCreationString, Schema.Boolean]),
      }),
    ),
  ),
});

const NativeCreationHistoricalBinding = nativeCreationStruct({
  backendInstance: NativeCreationString,
  environmentId: NativeCreationString,
  projectId: ProjectId,
  projectCwd: NativeCreationString,
  accountRef: NativeCreationString,
  accountBindingId: NativeCreationString,
  accountBindingRevision: NativeCreationRevision,
  providerModelSelection: NativeCreationModelSelection,
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
  baseBranch: NativeCreationString,
  startFromOrigin: Schema.Boolean,
  runSetupScript: Schema.Boolean,
  requestedBranch: NativeCreationString,
});

const NativeCreationIncarnation = nativeCreationStruct({
  eventId: EventId,
  sequence: NonNegativeInt,
});
const NativeCreationEffectBase = {
  effectId: NativeCreationString,
  ordinal: NonNegativeInt,
  timestamp: IsoDateTime,
};
const NativeCreationCommandDetails = {
  commandId: CommandId,
  threadId: ThreadId,
  commandType: Schema.Literals([
    "thread.create",
    "thread.meta.update",
    "thread.message.user.append",
    "thread.session.set",
    "thread.activity.append",
    "thread.turn.start",
    "thread.delete",
  ]),
  commandDigest: NativeCreationSha256,
};
const NativeCreationWorktreeDetails = {
  projectCwd: NativeCreationString,
  worktreePath: NativeCreationString,
  branch: NativeCreationString,
  baseRef: NativeCreationString,
  ownership: Schema.Literals(["claimed", "created", "unknown"]),
};
const NativeCreationCleanupDetails = {
  resource: Schema.Union([
    nativeCreationStruct({
      kind: Schema.Literal("worktree"),
      ...NativeCreationWorktreeDetails,
    }),
    nativeCreationStruct({
      kind: Schema.Literal("setup_terminal"),
      terminalId: NativeCreationString,
      worktreePath: NativeCreationString,
    }),
    nativeCreationStruct({
      kind: Schema.Literal("thread"),
      threadId: ThreadId,
      incarnation: NativeCreationIncarnation,
    }),
  ]),
  recoveryScopeId: NativeCreationString,
};
const NativeCreationExternalResult = Schema.Literals(["succeeded", "failed", "unknown"]);

const NativeCreationLifecycleDetails = {
  threadId: ThreadId,
  action: Schema.Literals([
    "normalization",
    "tracker_registration",
    "bootstrap_detachment",
    "setup_detachment",
    "setup_completion_detachment",
    "worktree_ownership",
    "deletion_drain",
    "git_status_refresh",
  ]),
};

const NativeCreationEffect = Schema.Union([
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("lifecycle"),
    phase: Schema.Literal("started"),
    ...NativeCreationLifecycleDetails,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("lifecycle"),
    phase: Schema.Literal("completed"),
    ...NativeCreationLifecycleDetails,
    result: NativeCreationExternalResult,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("native_command"),
    phase: Schema.Literal("started"),
    ...NativeCreationCommandDetails,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("native_command"),
    phase: Schema.Literal("completed"),
    ...NativeCreationCommandDetails,
    eventId: EventId,
    sequence: NonNegativeInt,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("fetch"),
    phase: Schema.Literal("started"),
    projectCwd: NativeCreationString,
    baseRef: NativeCreationString,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("fetch"),
    phase: Schema.Literal("completed"),
    projectCwd: NativeCreationString,
    baseRef: NativeCreationString,
    result: NativeCreationExternalResult,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("worktree"),
    phase: Schema.Literal("started"),
    ...NativeCreationWorktreeDetails,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("worktree"),
    phase: Schema.Literal("completed"),
    ...NativeCreationWorktreeDetails,
    result: NativeCreationExternalResult,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("setup"),
    phase: Schema.Literal("started"),
    worktreePath: NativeCreationString,
    terminalId: Schema.NullOr(NativeCreationString),
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("setup"),
    phase: Schema.Literal("completed"),
    worktreePath: NativeCreationString,
    terminalId: Schema.NullOr(NativeCreationString),
    exitCode: Schema.NullOr(Schema.Int),
    result: NativeCreationExternalResult,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("cleanup"),
    phase: Schema.Literal("started"),
    ...NativeCreationCleanupDetails,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("cleanup"),
    phase: Schema.Literal("completed"),
    ...NativeCreationCleanupDetails,
    result: NativeCreationExternalResult,
  }),
]);

/** Historical creation attestation does not attest a terminal turn or release capacity. */
const NativeCreationObservation = nativeCreationStruct({
  schema: Schema.Literal("t3.native-creation-observation/v1"),
  preparationId: NativeCreationString,
  operationId: NativeCreationString,
  preparationSha256: NativeCreationSha256,
  bindingDigest: NativeCreationSha256,
  promptDigest: NativeCreationSha256,
  commandDigest: NativeCreationSha256,
  normalizedCommandDigest: NativeCreationSha256,
  claimId: NativeCreationString,
  claimedBootId: NativeCreationString,
  claimedAt: IsoDateTime,
  actorSessionId: NativeCreationString,
  grantId: NativeCreationString,
  grantRevision: NativeCreationRevision,
  binding: NativeCreationHistoricalBinding,
  incarnation: Schema.NullOr(NativeCreationIncarnation),
  effects: Schema.Array(NativeCreationEffect),
  unresolvedEffects: Schema.Array(NativeCreationString),
  outcome: Schema.Literals(["complete", "in_progress", "incomplete", "unknown"]),
});

// Correlation is historical projection evidence, not proof of task delivery.
export const OrchestrationCommandObservation = Schema.Struct({
  threadId: ThreadId,
  commandId: CommandId,
  messageId: MessageId,
  snapshotSequence: NonNegativeInt,
  commandStatus: Schema.Literals(["accepted", "rejected", "not_found"]),
  acceptedSequence: Schema.NullOr(NonNegativeInt),
  correlation: Schema.Literals(["exact", "pending", "missing", "ambiguous", "mismatched"]),
  turn: Schema.NullOr(OrchestrationObservedTurn),
  target: Schema.NullOr(OrchestrationDispatchTarget),
  creation: Schema.optionalKey(NativeCreationObservation),
});
export type OrchestrationCommandObservation = typeof OrchestrationCommandObservation.Type;
