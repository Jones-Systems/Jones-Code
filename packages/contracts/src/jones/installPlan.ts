import * as Schema from "effect/Schema";
import { EnvironmentId, TrimmedNonEmptyString } from "../baseSchemas.ts";
import { JonesBuildStartedAt } from "./fleetUpdates/source.ts";
import {
  FleetOperationId,
  FleetPlanDirective,
  FleetPlanDirectiveInput,
  FleetHostStatus,
} from "./fleetUpdates/host.ts";
import {
  JonesOperationId,
  JonesSourceSha,
  JonesOperationCode,
  JonesRecoveryPolicy,
  JonesOperationAction,
  JonesOperationStartInput,
  JonesOperationStopInput,
  JonesOperationFenceInput,
  JonesOperationReceipt,
  JonesRecoveryPreflight,
  JonesRecoveryPreflightInput,
} from "./jonesOperations.ts";

const Bounded = (max: number) => Schema.String.check(Schema.isMaxLength(max));

export const JonesInstallPlanOrigin = Schema.Literals([
  "update-button",
  "update-everything",
  "settings",
  "exact-source-notice",
  "connections",
]);
export type JonesInstallPlanOrigin = typeof JonesInstallPlanOrigin.Type;

export const JonesInstallPlanTarget = Schema.Struct({
  repository: Schema.Literal("Jones-Systems/Jones-Code"),
  sourceSha: JonesSourceSha,
  sourceTree: JonesSourceSha,
  version: TrimmedNonEmptyString,
  buildStartedAt: Schema.optionalKey(JonesBuildStartedAt),
});
export type JonesInstallPlanTarget = typeof JonesInstallPlanTarget.Type;

export const JonesInstallPlanRoute = Schema.Literals(["operations", "follower-operations"]);
export type JonesInstallPlanRoute = typeof JonesInstallPlanRoute.Type;

export const JonesInstallPlanSelection = Schema.Struct({
  environmentId: EnvironmentId,
  label: Bounded(64),
  route: JonesInstallPlanRoute,
  enrollmentId: Schema.optionalKey(FleetOperationId),
});
export type JonesInstallPlanSelection = typeof JonesInstallPlanSelection.Type;

export const JonesInstallPlanReview = Schema.Struct({
  reviewedAt: Schema.String,
  stagedHandle: TrimmedNonEmptyString,
  targetSource: JonesSourceSha,
  currentVersion: TrimmedNonEmptyString,
  expectedInstalledSource: JonesSourceSha,
  continueRunningThreads: Schema.optionalKey(Schema.Boolean),
  recovery: Schema.optionalKey(
    Schema.Struct({ policy: JonesRecoveryPolicy, preflightId: JonesOperationId }),
  ),
});
export type JonesInstallPlanReview = typeof JonesInstallPlanReview.Type;

export const JonesInstallPlanLease = Schema.Struct({
  claimant: Bounded(64),
  reservedAt: Schema.String,
  leaseUntil: Schema.String,
});
export type JonesInstallPlanLease = typeof JonesInstallPlanLease.Type;

const DirectiveRefusal = Schema.Literals([
  "binding",
  "not-enrolled",
  "bootstrap-required",
  "conflict",
  "storage",
]);
export const JonesInstallPlanHostOperation = Schema.Struct({
  operationId: JonesOperationId, // the directiveId for the directive step
  step: Schema.Literals(["directive", "download", "install"]),
  request: Schema.Union([
    Schema.Struct({ kind: Schema.Literal("operation-start"), input: JonesOperationStartInput }),
    Schema.Struct({ kind: Schema.Literal("plan-directive"), input: FleetPlanDirectiveInput }),
  ]),
  lease: JonesInstallPlanLease,
  stop: Schema.optionalKey(
    Schema.Struct({ stopId: JonesOperationId, lease: JonesInstallPlanLease }),
  ),
  fence: Schema.optionalKey(Schema.Struct({ lease: JonesInstallPlanLease })),
  receipt: Schema.optionalKey(JonesOperationReceipt), // latest validated target receipt
  directive: Schema.optionalKey(
    Schema.Union([
      Schema.Struct({
        kind: Schema.Literal("applied"),
        observedAt: Schema.String,
        directive: FleetPlanDirective,
      }),
      Schema.Struct({
        kind: Schema.Literal("refused"),
        observedAt: Schema.String,
        reason: DirectiveRefusal,
        message: Bounded(1024),
      }),
    ]),
  ),
  review: Schema.optionalKey(JonesInstallPlanReview),
});
export type JonesInstallPlanHostOperation = typeof JonesInstallPlanHostOperation.Type;

export const JonesInstallPlanDevice = Schema.Struct({
  environmentId: EnvironmentId,
  label: Bounded(64),
  route: JonesInstallPlanRoute,
  enrollmentId: Schema.optionalKey(FleetOperationId),
  membership: Schema.Literals(["selected", "removed"]),
  addedAt: Schema.String,
  removedAt: Schema.optionalKey(Schema.String),
  operations: Schema.Array(JonesInstallPlanHostOperation).check(Schema.isMaxLength(16)),
  ended: Schema.optionalKey(Schema.Struct({ at: Schema.String })), // "Stop update" on a staged device
});
export type JonesInstallPlanDevice = typeof JonesInstallPlanDevice.Type;

export const JonesInstallPlanLaptopOperation = Schema.Struct({
  operationId: JonesOperationId,
  step: Schema.Literals(["download", "stop", "install"]),
  requestedAt: Schema.String,
  of: Schema.optionalKey(JonesOperationId), // stop: the download it stops
  review: Schema.optionalKey(JonesInstallPlanReview), // install
  worker: Schema.optionalKey(Schema.Struct({ pid: Schema.Number, processGroup: Schema.Number })), // held-exit proof after restart
  result: Schema.optionalKey(
    Schema.Struct({
      status: Schema.Literals([
        "running",
        "stop-requested",
        "held",
        "completed",
        "stopped",
        "refused",
        "failed",
        "interrupted",
        "committed",
        "rolled-back",
        "blocked",
      ]),
      at: Schema.String,
      code: Schema.optionalKey(JonesOperationCode),
      message: Schema.optionalKey(Bounded(1024)),
    }),
  ),
});
export type JonesInstallPlanLaptopOperation = typeof JonesInstallPlanLaptopOperation.Type;

export const JonesInstallPlanLaptop = Schema.Struct({
  environmentId: EnvironmentId,
  operations: Schema.Array(JonesInstallPlanLaptopOperation).check(Schema.isMaxLength(16)),
  // Native-only fields: written by coordinator hooks, never derived from a renderer request.
  binding: Schema.optionalKey(
    Schema.Struct({
      operationId: JonesOperationId,
      stagedHandle: TrimmedNonEmptyString,
      targetSource: JonesSourceSha,
      targetTree: JonesSourceSha,
      transactionId: TrimmedNonEmptyString,
      fromGeneration: TrimmedNonEmptyString,
      boundAt: Schema.String,
    }),
  ),
  gate: Schema.optionalKey(
    Schema.Struct({
      openedAt: Schema.String,
      basis: Schema.Literals(["plan-install", "prior-verified"]),
      transactionId: TrimmedNonEmptyString,
      activeGeneration: TrimmedNonEmptyString,
      activeSource: JonesSourceSha,
      activeTree: JonesSourceSha,
    }),
  ),
  gateClosed: Schema.optionalKey(
    Schema.Struct({
      at: Schema.String,
      reason: Schema.Literals(["rolled-back", "changed", "blocked"]),
    }),
  ),
});
export type JonesInstallPlanLaptop = typeof JonesInstallPlanLaptop.Type;

export const JonesInstallPlan = Schema.Struct({
  schema: Schema.Literal(1),
  planId: JonesOperationId,
  revision: Schema.Number,
  createdAt: Schema.String,
  origin: JonesInstallPlanOrigin,
  target: JonesInstallPlanTarget,
  from: Schema.Struct({
    version: TrimmedNonEmptyString,
    sourceSha: Schema.optionalKey(JonesSourceSha),
  }),
  laptop: JonesInstallPlanLaptop,
  devices: Schema.Array(JonesInstallPlanDevice).check(Schema.isMaxLength(16)),
  dismissedAt: Schema.optionalKey(Schema.String),
  supersededAt: Schema.optionalKey(Schema.String),
});
export type JonesInstallPlan = typeof JonesInstallPlan.Type;

export const JonesInstallPlanLaptopEvidence = Schema.Struct({
  observedAt: Schema.String,
  environmentId: Schema.optionalKey(EnvironmentId),
  activeVersion: Schema.optionalKey(TrimmedNonEmptyString),
  activeGeneration: Schema.optionalKey(TrimmedNonEmptyString),
  activeSource: Schema.optionalKey(JonesSourceSha),
  activeTree: Schema.optionalKey(JonesSourceSha),
  committedTransactionId: Schema.optionalKey(TrimmedNonEmptyString), // validated resumed journal of the active generation
  activationBlocked: Schema.Boolean,
});
export type JonesInstallPlanLaptopEvidence = typeof JonesInstallPlanLaptopEvidence.Type;

export const JonesInstallPlanState = Schema.Struct({
  schema: Schema.Literal(1),
  current: Schema.NullOr(JonesInstallPlan),
  last: Schema.NullOr(JonesInstallPlan),
  laptop: JonesInstallPlanLaptopEvidence,
  view: Schema.Struct({ complete: Schema.Boolean }),
  readOnly: Schema.optionalKey(
    Schema.Struct({ code: Schema.Literals(["newer-schema", "invalid"]), message: Bounded(512) }),
  ),
});
export type JonesInstallPlanState = typeof JonesInstallPlanState.Type;

const Evidence = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("receipt"), receipt: JonesOperationReceipt }),
  Schema.Struct({ kind: Schema.Literal("directive"), status: FleetHostStatus }),
  Schema.Struct({
    kind: Schema.Literal("directive-refused"),
    reason: DirectiveRefusal,
    message: Bounded(1024),
  }),
]);
export const JonesInstallPlanRequest = Schema.Union([
  Schema.Struct({ type: Schema.Literal("read") }),
  Schema.Struct({ type: Schema.Literal("publication") }),
  Schema.Struct({ type: Schema.Literal("recovery-preflight"), input: JonesRecoveryPreflightInput }),
  Schema.Struct({
    type: Schema.Literal("start"),
    origin: JonesInstallPlanOrigin,
    target: JonesInstallPlanTarget,
    select: Schema.Array(JonesInstallPlanSelection).check(Schema.isMaxLength(16)),
    laptop: Schema.optionalKey(
      Schema.Struct({
        action: Schema.Literals(["download", "install"]),
        review: Schema.optionalKey(JonesInstallPlanReview),
      }),
    ),
  }),
  Schema.Struct({
    type: Schema.Literal("edit"),
    planId: JonesOperationId,
    expectedRevision: Schema.Number,
    select: Schema.Array(JonesInstallPlanSelection).check(Schema.isMaxLength(16)),
    deselect: Schema.Array(EnvironmentId).check(Schema.isMaxLength(16)),
  }),
  Schema.Struct({
    type: Schema.Literal("laptop"),
    planId: JonesOperationId,
    action: Schema.Literals(["download", "stop", "install", "preflight"]),
    review: Schema.optionalKey(JonesInstallPlanReview),
  }),
  Schema.Struct({
    type: Schema.Literal("claim"),
    planId: JonesOperationId,
    environmentId: EnvironmentId,
    claimant: Bounded(64),
    step: Schema.Literals(["directive", "download", "install", "stop", "fence"]),
    explicit: Schema.Boolean, // true only for owner clicks
    operationId: Schema.optionalKey(JonesOperationId), // stop/fence: target operation
    action: Schema.optionalKey(JonesOperationAction),
    expectedInstalledSource: Schema.optionalKey(JonesSourceSha), // download/install: plan-target action and fresh installed source
    enrollmentId: Schema.optionalKey(FleetOperationId), // directive: fresh fleet status
    expectedDirectiveId: Schema.optionalKey(Schema.NullOr(FleetOperationId)),
    review: Schema.optionalKey(JonesInstallPlanReview),
  }),
  Schema.Struct({
    type: Schema.Literal("report"),
    planId: JonesOperationId,
    environmentId: EnvironmentId,
    operationId: JonesOperationId,
    evidence: Evidence,
  }),
  Schema.Struct({
    type: Schema.Literal("end"),
    planId: JonesOperationId,
    environmentId: EnvironmentId,
    expectedRevision: Schema.Number,
  }),
  Schema.Struct({
    type: Schema.Literal("dismiss"),
    planId: JonesOperationId,
    expectedRevision: Schema.Number,
  }),
]);
export type JonesInstallPlanRequest = typeof JonesInstallPlanRequest.Type;

export const JonesInstallPlanGrant = Schema.Struct({
  planId: JonesOperationId,
  environmentId: EnvironmentId,
  operationId: JonesOperationId,
  step: Schema.Literals(["directive", "download", "install", "stop", "fence"]),
  request: Schema.Union([
    Schema.Struct({ kind: Schema.Literal("operation-start"), input: JonesOperationStartInput }),
    Schema.Struct({ kind: Schema.Literal("operation-stop"), input: JonesOperationStopInput }),
    Schema.Struct({ kind: Schema.Literal("operation-fence"), input: JonesOperationFenceInput }),
    Schema.Struct({ kind: Schema.Literal("plan-directive"), input: FleetPlanDirectiveInput }),
  ]),
  leaseUntil: Schema.String,
});
export type JonesInstallPlanGrant = typeof JonesInstallPlanGrant.Type;

export const JonesInstallPlanRefusal = Schema.Literals([
  "stale-revision",
  "plan-mismatch",
  "plan-active",
  "laptop-action-required",
  "gate-closed",
  "laptop-changed",
  "claimed",
  "outstanding-operation",
  "directive-required",
  "not-selectable",
  "invalid-evidence",
  "not-complete",
  "unsupported",
  "bounds",
  "newer-schema",
  "storage",
]);
export type JonesInstallPlanRefusal = typeof JonesInstallPlanRefusal.Type;

export const JonesInstallPlanResult = Schema.Struct({
  state: JonesInstallPlanState,
  grant: Schema.optionalKey(JonesInstallPlanGrant),
  applied: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        environmentId: EnvironmentId,
        result: Schema.Literals([
          "added",
          "removed",
          "locked-started",
          "not-selectable",
          "unchanged",
        ]),
        reason: Schema.optionalKey(Bounded(256)),
      }),
    ).check(Schema.isMaxLength(16)),
  ),
  publication: Schema.optionalKey(
    Schema.Union([
      Schema.Struct({
        kind: Schema.Literal("committed"),
        source: Schema.Struct({
          repository: Schema.Literal("Jones-Systems/Jones-Code"),
          sha: JonesSourceSha,
          tree: JonesSourceSha,
        }),
      }),
      Schema.Struct({
        kind: Schema.Literal("withheld"),
        code: Schema.Literals(["plan-managed", "unproved", "blocked", "unavailable"]),
        message: Bounded(512),
      }),
    ]),
  ),
  preflight: Schema.optionalKey(JonesRecoveryPreflight),
  refused: Schema.optionalKey(
    Schema.Struct({ code: JonesInstallPlanRefusal, message: Bounded(512) }),
  ),
});
export type JonesInstallPlanResult = typeof JonesInstallPlanResult.Type;
