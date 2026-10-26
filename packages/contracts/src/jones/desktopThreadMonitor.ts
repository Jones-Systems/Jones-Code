import * as Schema from "effect/Schema";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  NonNegativeInt,
  PlanId,
  RuntimeRequestId,
  ThreadId,
} from "../baseSchemas.ts";

export const DESKTOP_THREAD_MONITOR_PROTOCOL = "jones.desktop-thread-monitor/v1" as const;
export const THREAD_MONITOR_LIMITS = {
  frameBytes: 1_048_576,
  rowUpserts: 64,
  draftCodeUnits: 65_536,
  detailEntries: 12,
  detailEntryCodeUnits: 8_192,
  invalidationTimeoutMs: 5_000,
  actionTimeoutMs: 15_000,
  doneWindowMs: 3_600_000,
  retiredIds: 4_096,
} as const;
const text = (max: number) => Schema.String.check(Schema.isMaxLength(max));
const id = text(512).check(Schema.isMinLength(1));
const timestamp = text(64).check(
  Schema.makeFilter(
    (value) =>
      (/^\d{4}-\d{2}-\d{2}T/.test(value) && Number.isFinite(Date.parse(value))) ||
      "Expected an ISO timestamp",
  ),
);
export const ThreadMonitorLifetime = Schema.Struct({
  publisherGeneration: id,
  environmentId: EnvironmentId,
  environmentEpoch: NonNegativeInt,
});
export type ThreadMonitorLifetime = typeof ThreadMonitorLifetime.Type;
export const ThreadMonitorRef = Schema.Struct({
  ...ThreadMonitorLifetime.fields,
  threadId: ThreadId,
});
export type ThreadMonitorRef = typeof ThreadMonitorRef.Type;
export const ThreadMonitorGroup = Schema.Literals(["needsYou", "working", "waiting", "done"]);
export type ThreadMonitorGroup = typeof ThreadMonitorGroup.Type;
export const ThreadMonitorStatus = Schema.Literals([
  "approval",
  "input",
  "plan",
  "working",
  "connecting",
  "checkpoint",
  "waiting",
  "idle",
  "failed",
  "limited",
  "completed",
  "interrupted",
  "cancelled",
  "rolled_back",
  "unknown",
]);
export type ThreadMonitorStatus = typeof ThreadMonitorStatus.Type;
export const ThreadMonitorAvailability = Schema.Struct({
  available: Schema.Boolean,
  reason: Schema.NullOr(text(512)),
});
export type ThreadMonitorAvailability = typeof ThreadMonitorAvailability.Type;
export const ThreadMonitorActionAvailability = Schema.Struct({
  reply: ThreadMonitorAvailability,
  settle: ThreadMonitorAvailability,
  respondToApproval: ThreadMonitorAvailability,
  respondToUserInput: ThreadMonitorAvailability,
  usePlan: ThreadMonitorAvailability,
  restore: ThreadMonitorAvailability,
});
export const ThreadMonitorEnvironment = Schema.Struct({
  ...ThreadMonitorLifetime.fields,
  label: text(256),
  transport: Schema.Literals(["connected", "disconnected", "connecting", "unavailable"]),
  freshness: Schema.Literals(["live", "cached", "loading", "unavailable"]),
  reason: Schema.NullOr(text(512)),
});
export type ThreadMonitorEnvironment = typeof ThreadMonitorEnvironment.Type;
export const ThreadMonitorRow = Schema.Struct({
  ...ThreadMonitorRef.fields,
  title: text(1024),
  projectLabel: text(512),
  group: ThreadMonitorGroup,
  status: ThreadMonitorStatus,
  statusLabel: text(256),
  updatedAt: timestamp,
  completedAt: Schema.NullOr(timestamp),
  completionTimeUnavailable: Schema.Boolean,
  pendingChildAttention: Schema.NullOr(Schema.Literals(["approval", "user_input"])),
  actions: ThreadMonitorActionAvailability,
});
export type ThreadMonitorRow = typeof ThreadMonitorRow.Type;
export const ThreadMonitorCounts = Schema.Struct({
  needsYou: NonNegativeInt,
  working: NonNegativeInt,
  waiting: NonNegativeInt,
  done: NonNegativeInt,
});
export type ThreadMonitorCounts = typeof ThreadMonitorCounts.Type;
export const THREAD_MONITOR_THEME_KEYS = [
  "--background",
  "--foreground",
  "--card",
  "--card-foreground",
  "--popover",
  "--popover-foreground",
  "--primary",
  "--primary-foreground",
  "--secondary",
  "--secondary-foreground",
  "--muted",
  "--muted-foreground",
  "--accent",
  "--accent-foreground",
  "--destructive",
  "--border",
  "--input",
  "--ring",
  "--radius",
  "--sidebar",
  "--sidebar-foreground",
  "--sidebar-border",
  "--sidebar-accent",
  "--sidebar-accent-foreground",
  "--font-sans",
  "--font-mono",
  "--status-working",
  "--status-input",
  "--status-approval",
  "--status-plan",
  "--status-done",
] as const;
const themeValue = text(512).check(
  Schema.makeFilter(
    (value) =>
      (!/[;{}<>\\\u0000-\u001f]/.test(value) && !/(?:url|expression|@import)\s*\(/i.test(value)) ||
      "Unsafe theme value",
  ),
);
export const ThreadMonitorTheme = Schema.Struct({
  mode: Schema.Literals(["light", "dark"]),
  values: Schema.Record(Schema.String, themeValue).check(
    Schema.makeFilter(
      (values) =>
        Object.keys(values).every((key) =>
          (THREAD_MONITOR_THEME_KEYS as readonly string[]).includes(key),
        ) || "Unknown theme property",
    ),
  ),
});
export type ThreadMonitorTheme = typeof ThreadMonitorTheme.Type;
const answer = Schema.Union([
  text(THREAD_MONITOR_LIMITS.draftCodeUnits),
  Schema.Array(text(8192)).check(Schema.isMaxLength(128)),
]);
export const ThreadMonitorAnswers = Schema.Record(id, answer).check(
  Schema.makeFilter((answers) => Object.keys(answers).length <= 128 || "Too many answers"),
);
export type ThreadMonitorAnswers = typeof ThreadMonitorAnswers.Type;
export const ThreadMonitorDraft = Schema.Struct({
  ...ThreadMonitorRef.fields,
  revision: NonNegativeInt,
  kind: Schema.Literals(["reply", "decision"]),
  requestId: Schema.NullOr(RuntimeRequestId),
  text: text(THREAD_MONITOR_LIMITS.draftCodeUnits),
  answers: ThreadMonitorAnswers,
});
export type ThreadMonitorDraft = typeof ThreadMonitorDraft.Type;
export const ThreadMonitorApproval = Schema.Struct({
  kind: Schema.Literal("approval"),
  requestId: RuntimeRequestId,
  prompt: text(8192),
  options: Schema.Array(
    Schema.Struct({
      decision: Schema.Literals([
        "accept",
        "decline",
        "acceptForSession",
        "acceptAlways",
        "cancel",
      ]),
      label: text(512),
      warning: Schema.optional(text(2048)),
    }),
  ).check(Schema.isMaxLength(32)),
  availability: ThreadMonitorAvailability,
});
export const ThreadMonitorQuestion = Schema.Struct({
  id,
  header: text(512),
  question: text(8192),
  options: Schema.Array(
    Schema.Struct({
      label: text(2048),
      description: Schema.optional(text(8192)),
      value: text(8192),
    }),
  ).check(Schema.isMaxLength(128)),
  allowCustomAnswer: Schema.Boolean,
  multiSelect: Schema.Boolean,
});
export const ThreadMonitorUserInput = Schema.Struct({
  kind: Schema.Literal("user_input"),
  requestId: RuntimeRequestId,
  questions: Schema.Array(ThreadMonitorQuestion).check(Schema.isMaxLength(128)),
  availability: ThreadMonitorAvailability,
});
export const ThreadMonitorPlan = Schema.Struct({
  planId: PlanId,
  markdown: text(65_536),
  availability: ThreadMonitorAvailability,
});
export const ThreadMonitorDetail = Schema.Struct({
  ...ThreadMonitorRef.fields,
  selectionId: id,
  revision: NonNegativeInt,
  entries: Schema.Array(
    Schema.Struct({
      id,
      role: Schema.Literals(["user", "assistant", "system"]),
      text: text(8192),
      excerpt: Schema.Boolean,
    }),
  ).check(Schema.isMaxLength(12)),
  request: Schema.NullOr(Schema.Union([ThreadMonitorApproval, ThreadMonitorUserInput])),
  plan: Schema.NullOr(ThreadMonitorPlan),
  unavailableReason: Schema.NullOr(text(512)),
});
export type ThreadMonitorDetail = typeof ThreadMonitorDetail.Type;
const action = { ...ThreadMonitorRef.fields, actionId: id, commandId: CommandId } as const;
export const ThreadMonitorAction = Schema.Union([
  Schema.Struct({
    ...action,
    type: Schema.Literal("send"),
    messageId: MessageId,
    draftRevision: NonNegativeInt,
    text: text(65_536),
  }),
  Schema.Struct({
    ...action,
    type: Schema.Literal("reconcile"),
    originalActionId: id,
    originalCommandId: CommandId,
    messageId: Schema.optional(MessageId),
  }),
  Schema.Struct({ ...action, type: Schema.Literal("settle"), expectedUpdatedAt: timestamp }),
  Schema.Struct({ ...action, type: Schema.Literal("restore"), settleCommandId: CommandId }),
  Schema.Struct({ ...action, type: Schema.Literal("open") }),
  Schema.Struct({
    ...action,
    type: Schema.Literal("respondToApproval"),
    requestId: RuntimeRequestId,
    decision: Schema.Literals(["accept", "decline"]),
  }),
  Schema.Struct({
    ...action,
    type: Schema.Literal("respondToUserInput"),
    requestId: RuntimeRequestId,
    answers: ThreadMonitorAnswers,
    draftRevision: NonNegativeInt,
  }),
  Schema.Struct({
    ...action,
    type: Schema.Literal("usePlan"),
    planId: PlanId,
    messageId: MessageId,
  }),
]);
export type ThreadMonitorAction = typeof ThreadMonitorAction.Type;
export const ThreadMonitorActionResult = Schema.Struct({
  ...action,
  type: Schema.Literal("actionResult"),
  status: Schema.Literals(["pending", "accepted", "rejected", "unavailable", "unknown"]),
  reason: Schema.NullOr(text(512)),
  submittedDraftRevision: Schema.optional(NonNegativeInt),
  settleCommandId: Schema.optional(CommandId),
});
export type ThreadMonitorActionResult = typeof ThreadMonitorActionResult.Type;
export const ThreadMonitorInvalidation = Schema.Struct({
  ...ThreadMonitorLifetime.fields,
  type: Schema.Literal("invalidateEnvironment"),
  requestId: id,
});
export type ThreadMonitorInvalidation = typeof ThreadMonitorInvalidation.Type;
export const ThreadMonitorInvalidationAck = Schema.Struct({
  ...ThreadMonitorLifetime.fields,
  type: Schema.Literal("environmentInvalidated"),
  requestId: id,
});
export type ThreadMonitorInvalidationAck = typeof ThreadMonitorInvalidationAck.Type;
export const ThreadMonitorViewChunk = Schema.Struct({
  type: Schema.Literal("view"),
  publisherGeneration: id,
  snapshotId: id,
  revision: NonNegativeInt,
  chunkIndex: NonNegativeInt,
  chunkCount: NonNegativeInt.check(Schema.isGreaterThanOrEqualTo(1)),
  complete: Schema.Boolean,
  replace: Schema.Boolean,
  environments: Schema.Array(ThreadMonitorEnvironment).check(Schema.isMaxLength(64)),
  upserts: Schema.Array(ThreadMonitorRow).check(Schema.isMaxLength(64)),
  removed: Schema.Array(ThreadMonitorRef).check(Schema.isMaxLength(64)),
  counts: ThreadMonitorCounts,
}).check(
  Schema.makeFilter(
    (value) =>
      (value.chunkIndex < value.chunkCount &&
        value.complete === (value.chunkIndex === value.chunkCount - 1)) ||
      "Invalid chunk ordering",
  ),
);
export type ThreadMonitorViewChunk = typeof ThreadMonitorViewChunk.Type;
const scrollCoordinate = Schema.Number.check(
  Schema.makeFilter(
    (value) =>
      (Number.isFinite(value) && Math.abs(value) <= 10_000_000) || "Invalid scroll coordinate",
  ),
);
const monitorScrollState = Schema.Struct({
  scrollTop: scrollCoordinate.check(Schema.isGreaterThanOrEqualTo(0)),
  anchors: Schema.Array(Schema.Struct({ key: text(2048), offset: scrollCoordinate })).check(
    Schema.isMaxLength(8),
  ),
});
export const ThreadMonitorViewState = Schema.Struct({
  publisherGeneration: id,
  revision: NonNegativeInt,
  collapsed: Schema.Struct({
    needsYou: Schema.Boolean,
    working: Schema.Boolean,
    waiting: Schema.Boolean,
    done: Schema.Boolean,
  }),
  list: monitorScrollState,
  detail: monitorScrollState,
  editor: Schema.NullOr(
    Schema.Struct({
      ref: ThreadMonitorRef,
      quick: Schema.Boolean,
      focused: Schema.Boolean,
      selectionStart: NonNegativeInt.check(Schema.isLessThanOrEqualTo(65_536)),
      selectionEnd: NonNegativeInt.check(Schema.isLessThanOrEqualTo(65_536)),
    }).check(
      Schema.makeFilter(
        (value) => value.selectionStart <= value.selectionEnd || "Invalid editor selection",
      ),
    ),
  ),
});
export type ThreadMonitorViewState = typeof ThreadMonitorViewState.Type;

export const ThreadMonitorPublisherFrame = Schema.Union([
  ThreadMonitorViewChunk,
  ThreadMonitorInvalidation,
  Schema.Struct({ type: Schema.Literal("viewState"), state: ThreadMonitorViewState }),
  Schema.Struct({
    type: Schema.Literal("theme"),
    publisherGeneration: id,
    theme: ThreadMonitorTheme,
  }),
  Schema.Struct({ type: Schema.Literal("detail"), detail: ThreadMonitorDetail }),
  Schema.Struct({ type: Schema.Literal("draft"), draft: ThreadMonitorDraft }),
  ThreadMonitorActionResult,
  Schema.Struct({ type: Schema.Literal("reset"), publisherGeneration: id }),
]);
export type ThreadMonitorPublisherFrame = typeof ThreadMonitorPublisherFrame.Type;
export const ThreadMonitorPeerIntent = Schema.Union([
  ThreadMonitorAction,
  ThreadMonitorInvalidationAck,
  Schema.Struct({ type: Schema.Literal("setViewState"), state: ThreadMonitorViewState }),
  Schema.Struct({ type: Schema.Literal("editDraft"), draft: ThreadMonitorDraft }),
  Schema.Struct({
    type: Schema.Literal("selectDetail"),
    ...ThreadMonitorRef.fields,
    selectionId: id,
  }),
  Schema.Struct({
    type: Schema.Literal("releaseDetail"),
    ...ThreadMonitorRef.fields,
    selectionId: id,
  }),
  Schema.Struct({ type: Schema.Literal("setExpanded"), expanded: Schema.Boolean }),
  Schema.Struct({ type: Schema.Literal("hide") }),
  Schema.Struct({ type: Schema.Literal("requestMore"), group: ThreadMonitorGroup }),
  Schema.Struct({ type: Schema.Literal("requestSnapshot") }),
  Schema.Struct({ type: Schema.Literal("ready") }),
]);
export type ThreadMonitorPeerIntent = typeof ThreadMonitorPeerIntent.Type;
export const ThreadMonitorFrame = Schema.Struct({
  schemaVersion: Schema.Literal(DESKTOP_THREAD_MONITOR_PROTOCOL),
  payload: Schema.Union([ThreadMonitorPublisherFrame, ThreadMonitorPeerIntent]),
});
export type ThreadMonitorFrame = typeof ThreadMonitorFrame.Type;
export const threadMonitorFrameByteLength = (value: unknown): number =>
  new TextEncoder().encode(JSON.stringify(value)).byteLength;
export function decodeThreadMonitorFrame(value: unknown): ThreadMonitorFrame {
  if (threadMonitorFrameByteLength(value) > THREAD_MONITOR_LIMITS.frameBytes)
    throw new Error("Monitor frame exceeds byte limit");
  return Schema.decodeUnknownSync(ThreadMonitorFrame, { onExcessProperty: "error" })(value);
}
export const ThreadMonitorNativeState = Schema.Struct({
  supported: Schema.Boolean,
  enabled: Schema.Boolean,
  visible: Schema.Boolean,
  mainVisible: Schema.Boolean,
  mainFocused: Schema.Boolean,
  expanded: Schema.Boolean,
  reason: Schema.NullOr(text(512)),
});
export type ThreadMonitorNativeState = typeof ThreadMonitorNativeState.Type;
export interface DesktopThreadMonitorMainBridge {
  readonly getState: () => Promise<ThreadMonitorNativeState>;
  readonly setEnabled: (enabled: boolean) => Promise<ThreadMonitorNativeState>;
  readonly publish: (frame: ThreadMonitorPublisherFrame) => Promise<void>;
  readonly invalidateEnvironment: (
    frame: ThreadMonitorInvalidation,
  ) => Promise<ThreadMonitorInvalidationAck>;
  readonly onIntent: (listener: (intent: ThreadMonitorPeerIntent) => void) => () => void;
  readonly onState: (listener: (state: ThreadMonitorNativeState) => void) => () => void;
}
export interface DesktopThreadMonitorPeerBridge {
  readonly ready: () => Promise<void>;
  readonly dispatch: (intent: ThreadMonitorPeerIntent) => Promise<void>;
  readonly onFrame: (listener: (frame: ThreadMonitorPublisherFrame) => void) => () => void;
}
