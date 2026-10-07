import * as Schema from "effect/Schema";
import {
  CommandId,
  MessageId,
  ThreadId,
  TurnId,
  NonNegativeInt,
  IsoDateTime,
} from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";

export const QualifiedQuotaWindow = Schema.Struct({
  usedPercent: Schema.Finite,
  resetsAt: Schema.optionalKey(Schema.NullOr(Schema.Finite)),
  windowDurationMins: Schema.optionalKey(Schema.NullOr(Schema.Finite)),
});
export const QualifiedQuotaBucket = Schema.Struct({
  credits: Schema.optionalKey(
    Schema.NullOr(
      Schema.Struct({
        balance: Schema.optionalKey(Schema.NullOr(Schema.String)),
        hasCredits: Schema.Boolean,
        unlimited: Schema.Boolean,
      }),
    ),
  ),
  individualLimit: Schema.optionalKey(
    Schema.NullOr(
      Schema.Struct({
        limit: Schema.String,
        remainingPercent: Schema.Finite,
        resetsAt: Schema.Finite,
        used: Schema.String,
      }),
    ),
  ),
  limitId: Schema.optionalKey(Schema.NullOr(Schema.String)),
  limitName: Schema.optionalKey(Schema.NullOr(Schema.String)),
  normalModelSlug: Schema.optionalKey(Schema.NullOr(Schema.String)),
  planType: Schema.optionalKey(Schema.NullOr(Schema.String)),
  primary: Schema.optionalKey(Schema.NullOr(QualifiedQuotaWindow)),
  secondary: Schema.optionalKey(Schema.NullOr(QualifiedQuotaWindow)),
  rateLimitReachedType: Schema.optionalKey(Schema.NullOr(Schema.String)),
  spendControlReached: Schema.optionalKey(Schema.NullOr(Schema.Boolean)),
});
export const QualifiedQuotaMap = Schema.Record(Schema.String, QualifiedQuotaBucket);
export type QualifiedQuotaMap = typeof QualifiedQuotaMap.Type;
export const ProviderQueueCapabilityRef = Schema.Struct({
  modelId: Schema.String,
  reasoningEfforts: Schema.Array(Schema.String),
  source: Schema.Literal("model/list"),
  probeId: Schema.String.check(Schema.isUUID()),
  observedAt: IsoDateTime,
});
export type ProviderQueueCapabilityRef = typeof ProviderQueueCapabilityRef.Type;
export const QualifiedQuotaFailureCode = Schema.Literals([
  "probe_failed",
  "probe_timeout",
  "quota_unavailable",
  "missing_map",
  "incomplete_windows",
  "unsupported_account",
  "disabled",
  "refresh_not_observed",
  "interrupted_attempt",
  "reset_crossed",
]);
export const QualifiedQuota = Schema.Struct({
  schemaVersion: Schema.Literal("codex.t3-qualified-quota/v1"),
  instanceId: ProviderInstanceId,
  probeId: Schema.String.check(Schema.isUUID()),
  status: Schema.Literals(["qualified", "unknown", "failed", "unsupported"]),
  attemptedAt: IsoDateTime,
  quotaReceivedAt: Schema.NullOr(IsoDateTime),
  probeCompletedAt: IsoDateTime,
  complete: Schema.Boolean,
  rateLimitsByLimitId: Schema.NullOr(QualifiedQuotaMap),
  windowProvenance: Schema.Array(
    Schema.Struct({
      limitId: Schema.String,
      windowId: Schema.Literals(["primary", "secondary"]),
      probeId: Schema.String.check(Schema.isUUID()),
      quotaReceivedAt: IsoDateTime,
    }),
  ),
  failureCode: Schema.NullOr(QualifiedQuotaFailureCode),
  capabilityRefs: Schema.Array(ProviderQueueCapabilityRef),
});
export type QualifiedQuota = typeof QualifiedQuota.Type;
export const ProviderQueueInventory = Schema.Struct({
  schemaVersion: Schema.Literal("t3.provider-queue-inventory/v1"),
  inventoryRevision: Schema.String,
  observedAt: IsoDateTime,
  evidence: Schema.Literal("configured-provider-registry"),
  instances: Schema.Array(
    Schema.Struct({
      instanceId: ProviderInstanceId,
      displayName: Schema.String,
      driver: Schema.Literal("codex"),
      enabled: Schema.Boolean,
      capabilityRefs: Schema.Array(ProviderQueueCapabilityRef),
    }),
  ),
});
export type ProviderQueueInventory = typeof ProviderQueueInventory.Type;
export const ProviderQueueRefreshResult = Schema.Struct({
  instanceId: ProviderInstanceId,
  status: Schema.Literals([
    "refreshed",
    "cached",
    "unknown_instance",
    "unsupported",
    "unavailable",
  ]),
  nextRefreshAt: Schema.NullOr(IsoDateTime),
  quota: Schema.NullOr(QualifiedQuota),
});
export type ProviderQueueRefreshResult = typeof ProviderQueueRefreshResult.Type;

import { ModelSelection } from "./modelSelection.ts";

export const OrchestrationSessionStatus = Schema.Literals([
  "idle",
  "starting",
  "running",
  "ready",
  "interrupted",
  "stopped",
  "error",
]);
export type OrchestrationSessionStatus = typeof OrchestrationSessionStatus.Type;

export const ThreadTurnDispatchGuard = Schema.Struct({
  observedSnapshotSequence: NonNegativeInt,
  expectedModelSelection: ModelSelection,
  expectedSessionStatus: Schema.NullOr(OrchestrationSessionStatus),
  expectedActiveTurnId: Schema.NullOr(TurnId),
  expectedLatestTurnId: Schema.NullOr(TurnId),
  requireIdle: Schema.Literal(true),
});
export type ThreadTurnDispatchGuard = typeof ThreadTurnDispatchGuard.Type;

export const OrchestrationDispatchTarget = Schema.Struct({
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
export type OrchestrationDispatchTarget = typeof OrchestrationDispatchTarget.Type;

export const OrchestrationObservedTurn = Schema.Struct({
  turnId: Schema.NullOr(TurnId),
  state: Schema.Literals(["pending", "running", "interrupted", "completed", "error"]),
  requestedAt: IsoDateTime,
  startedAt: Schema.NullOr(IsoDateTime),
  completedAt: Schema.NullOr(IsoDateTime),
  assistantMessageId: Schema.NullOr(MessageId),
});
export type OrchestrationObservedTurn = typeof OrchestrationObservedTurn.Type;

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
});
export type OrchestrationCommandObservation = typeof OrchestrationCommandObservation.Type;
