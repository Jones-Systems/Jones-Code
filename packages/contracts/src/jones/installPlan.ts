import * as Schema from "effect/Schema";
import { EnvironmentId, TrimmedNonEmptyString } from "../baseSchemas.ts";
import { JonesBuildStartedAt } from "./fleetUpdates/source.ts";
import {
  FleetOperationId,
  FleetPlanDirective,
  FleetPlanDirectiveFenceEvidence,
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
const Revision = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

export const JONES_INSTALL_PLAN_HISTORY_LIMIT = 8; // Total retained plans, counting current when present.
export const JONES_INSTALL_PLAN_CLI_RESPONSE_BYTES = 2 * 1024 * 1024;

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
  laptopArtifactId: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
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

export const JonesInstallPlanClientCommand = Schema.Struct({
  commandId: JonesOperationId,
  environmentId: EnvironmentId,
  targetSource: JonesSourceSha,
  action: Schema.Literals(["download", "download-exact", "install"]),
  stagedHandle: Schema.optionalKey(Bounded(1024)),
  planId: Schema.optionalKey(JonesOperationId),
  expectedRevision: Schema.optionalKey(Schema.Number),
});
export type JonesInstallPlanClientCommand = typeof JonesInstallPlanClientCommand.Type;

export const JonesInstallPlanCommandReceipt = Schema.Struct({
  command: JonesInstallPlanClientCommand,
  bodyHash: Schema.optionalKey(Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/))),
  requestType: Schema.Literals(["start", "edit", "laptop", "claim"]),
  planId: Schema.optionalKey(JonesOperationId),
  operationId: Schema.optionalKey(JonesOperationId),
  outcome: Schema.Literals(["reserved", "applied", "terminal", "refused"]),
  code: Schema.optionalKey(JonesOperationCode),
});
export type JonesInstallPlanCommandReceipt = typeof JonesInstallPlanCommandReceipt.Type;

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
  unsupportedReceiptVerified: Schema.optionalKey(Schema.Literal(true)),
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
  // Kept separate so older readers can decode the original directive ledger.
  directiveFence: Schema.optionalKey(
    Schema.Struct({
      outcome: Schema.Literals(["fenced", "superseded"]),
      observedAt: Schema.String,
    }),
  ),
  review: Schema.optionalKey(JonesInstallPlanReview),
});
export type JonesInstallPlanHostOperation = typeof JonesInstallPlanHostOperation.Type;

const StageReplacement = Schema.Struct({
  operationId: JonesOperationId,
  stagedHandle: TrimmedNonEmptyString,
  observedAt: Schema.Int,
});

export const JonesInstallPlanDevice = Schema.Struct({
  environmentId: EnvironmentId,
  label: Bounded(64),
  route: JonesInstallPlanRoute,
  enrollmentId: Schema.optionalKey(FleetOperationId),
  membership: Schema.Literals(["selected", "removed"]),
  addedAt: Schema.String,
  removedAt: Schema.optionalKey(Schema.String),
  operations: Schema.Array(JonesInstallPlanHostOperation).check(Schema.isMaxLength(16)),
  abandoning: Schema.optionalKey(Schema.Literal(true)),
  archivedOperationCount: Schema.optionalKey(
    Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(16)),
  ),
  ended: Schema.optionalKey(Schema.Struct({ at: Schema.String })), // "Stop update" on a staged device
  verifiedElsewhere: Schema.optionalKey(
    Schema.Struct({ version: TrimmedNonEmptyString, observedAt: Schema.Int }),
  ),
  stageReplacement: Schema.optionalKey(StageReplacement),
});
export type JonesInstallPlanDevice = typeof JonesInstallPlanDevice.Type;

export const JonesInstallPlanLaptopOperation = Schema.Struct({
  operationId: JonesOperationId,
  step: Schema.Literals(["download", "stop", "install"]),
  requestedAt: Schema.String,
  createdStagedHandle: Schema.optionalKey(TrimmedNonEmptyString),
  of: Schema.optionalKey(JonesOperationId), // stop: the download it stops
  review: Schema.optionalKey(JonesInstallPlanReview), // install
  // Reserved before native binding. The coordinator consumes this marker when it commits the binding.
  prebinding: Schema.optionalKey(
    Schema.Struct({
      fromGeneration: TrimmedNonEmptyString,
      fromSource: JonesSourceSha,
      fromTree: JonesSourceSha,
      stagedHandle: TrimmedNonEmptyString,
    }),
  ),
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

const OrdinaryRecovery = Schema.Struct({
  transactionId: Schema.String.check(Schema.isPattern(/^manual-[a-f0-9]{64}$/)),
  activeGeneration: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  manifestSha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  receiptSha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
});

export const JonesInstallPlanLaptop = Schema.Struct({
  environmentId: EnvironmentId,
  operations: Schema.Array(JonesInstallPlanLaptopOperation).check(Schema.isMaxLength(16)),
  stageAbandonment: Schema.optionalKey(
    Schema.Struct({
      operationId: JonesOperationId,
      handle: TrimmedNonEmptyString,
      selectionName: Schema.optionalKey(TrimmedNonEmptyString),
      status: Schema.Literals(["pending", "retired", "unconfirmed"]),
    }),
  ),
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
      verificationKind: Schema.optionalKey(Schema.Literal("ordinary-recovery")),
      ordinaryRecovery: Schema.optionalKey(OrdinaryRecovery),
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
  // A temporary verification pause also writes a blocked gateClosed for older readers.
  gatePause: Schema.optionalKey(Schema.Literal(true)),
});
export type JonesInstallPlanLaptop = typeof JonesInstallPlanLaptop.Type;

const RunningBuildVersion = TrimmedNonEmptyString.check(Schema.isMaxLength(128));
const RunningBuild = Schema.Struct({
  version: RunningBuildVersion,
  sourceSha: JonesSourceSha,
  sourceTree: JonesSourceSha,
});
const RunningTargetObservation = Schema.Struct({ build: RunningBuild, observedAt: Schema.Int });

const ManualInstallObservationFields = {
  environmentId: EnvironmentId,
  observedAt: Schema.Int,
  directive: Schema.optionalKey(
    Schema.Struct({ directiveId: FleetOperationId, targetSource: JonesSourceSha }),
  ),
};
const ManualInstallObservation = Schema.Union([
  Schema.Struct({
    ...ManualInstallObservationFields,
    kind: Schema.Literal("running"),
    build: Schema.Struct({
      version: RunningBuildVersion,
      source: Schema.optionalKey(Schema.Struct({ sha: JonesSourceSha, tree: JonesSourceSha })),
    }),
  }),
  Schema.Struct({ ...ManualInstallObservationFields, kind: Schema.Literal("unverified") }),
]);

export const JonesInstallPlan = Schema.Struct({
  schema: Schema.Literal(1),
  planId: JonesOperationId,
  revision: Schema.Number,
  createdAt: Schema.String,
  origin: JonesInstallPlanOrigin,
  hostOnly: Schema.optionalKey(Schema.Literal(true)),
  target: JonesInstallPlanTarget,
  from: Schema.Struct({
    version: TrimmedNonEmptyString,
    sourceSha: Schema.optionalKey(JonesSourceSha),
  }),
  laptop: JonesInstallPlanLaptop,
  devices: Schema.Array(JonesInstallPlanDevice).check(Schema.isMaxLength(16)),
  dismissedAt: Schema.optionalKey(Schema.String),
  cancelled: Schema.optionalKey(Schema.Literal(true)),
  cancelledFromRevision: Schema.optionalKey(Revision),
  supersededAt: Schema.optionalKey(Schema.String),
  manualClosure: Schema.optionalKey(
    Schema.Struct({
      laptop: RunningBuild,
      activeGeneration: TrimmedNonEmptyString,
      devices: Schema.Array(ManualInstallObservation).check(Schema.isMaxLength(16)),
    }),
  ),
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
  ordinaryRecovery: Schema.optionalKey(OrdinaryRecovery),
  activationBlocked: Schema.Boolean,
  // Native startup verified the active preimage and stage and ruled out ambiguous preparation.
  startupInspectionComplete: Schema.optionalKey(Schema.Literal(true)),
  reopenToCheckInstall: Schema.optionalKey(Schema.Literal(true)),
});
export type JonesInstallPlanLaptopEvidence = typeof JonesInstallPlanLaptopEvidence.Type;

export const VERIFIED_LAPTOP_MAX_AGE_MS = 120_000;

export function ordinaryJonesLaptopProof(evidence: JonesInstallPlanLaptopEvidence) {
  const proof = evidence.ordinaryRecovery;
  return proof !== undefined &&
    evidence.committedTransactionId === undefined &&
    proof.activeGeneration === evidence.activeGeneration &&
    proof.transactionId === `manual-${proof.activeGeneration}`
    ? proof
    : undefined;
}

/** Native evidence for the active Laptop generation, independent of offers and artifacts. */
export function verifiedRunningLaptop(
  evidence: JonesInstallPlanLaptopEvidence,
  environmentId: EnvironmentId | undefined,
  now: number,
) {
  const { activeSource, activeTree, activeVersion, activeGeneration } = evidence;
  const age = now - Date.parse(evidence.observedAt);
  const ordinaryRecovery = ordinaryJonesLaptopProof(evidence);
  const transactionId = evidence.committedTransactionId ?? ordinaryRecovery?.transactionId;
  if (
    environmentId === undefined ||
    evidence.environmentId !== environmentId ||
    !Number.isFinite(age) ||
    age < 0 ||
    age >= VERIFIED_LAPTOP_MAX_AGE_MS ||
    evidence.activationBlocked ||
    evidence.reopenToCheckInstall === true ||
    activeSource === undefined ||
    activeTree === undefined ||
    activeVersion === undefined ||
    activeGeneration === undefined ||
    transactionId === undefined ||
    (evidence.ordinaryRecovery !== undefined && ordinaryRecovery === undefined)
  )
    return undefined;
  return {
    environmentId,
    generation: activeGeneration,
    target: {
      repository: "Jones-Systems/Jones-Code",
      sourceSha: activeSource,
      sourceTree: activeTree,
      version: activeVersion,
    } satisfies JonesInstallPlanTarget,
    transactionId,
    ordinaryRecovery,
  };
}

const JonesInstallPlanRecoverySummary = Schema.Struct({
  planId: JonesOperationId,
  revision: Schema.Number,
  targetVersion: Bounded(128),
  closedAt: Schema.String,
  pendingLaptop: Schema.Boolean,
  pendingDevices: Schema.Array(
    Schema.Struct({ environmentId: EnvironmentId, label: Bounded(64) }),
  ).check(Schema.isMaxLength(16)),
});

export const JonesInstallPlanState = Schema.Struct({
  schema: Schema.Literal(1),
  current: Schema.NullOr(JonesInstallPlan),
  last: Schema.NullOr(JonesInstallPlan),
  documentRevision: Schema.optionalKey(Revision),
  recoveryHistory: Schema.optionalKey(
    Schema.Array(JonesInstallPlanRecoverySummary).check(
      Schema.isMaxLength(JONES_INSTALL_PLAN_HISTORY_LIMIT),
    ),
  ),
  orphanedCommands: Schema.optionalKey(
    Schema.Array(JonesInstallPlanCommandReceipt).check(Schema.isMaxLength(1024)),
  ),
  laptop: JonesInstallPlanLaptopEvidence,
  view: Schema.Struct({
    complete: Schema.Boolean,
    cancelPlan: Schema.optionalKey(Schema.Literal(1)),
    deviceRecovery: Schema.optionalKey(Schema.Literal(1)),
    recoveryHistory: Schema.optionalKey(Schema.Literal(1)),
    directiveFence: Schema.optionalKey(Schema.Literal(1)),
    orphanRecovery: Schema.optionalKey(Schema.Literal(1)),
    boundAdmission: Schema.optionalKey(Schema.Literal(1)),
    matchRunningLaptop: Schema.optionalKey(Schema.Literal(1)),
    manualClose: Schema.optionalKey(
      Schema.Struct({
        laptop: RunningBuild,
        activeGeneration: TrimmedNonEmptyString,
      }),
    ),
  }),
  clientCommands: Schema.optionalKey(Schema.Literal(1)),
  readOnly: Schema.optionalKey(
    Schema.Struct({ code: Schema.Literals(["newer-schema", "invalid"]), message: Bounded(512) }),
  ),
});
export type JonesInstallPlanState = typeof JonesInstallPlanState.Type;

const Evidence = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("receipt"),
    receipt: JonesOperationReceipt,
    /** Set only for an exact, non-null receipt returned by operationRead. */
    ledgerRead: Schema.optionalKey(Schema.Literal(true)),
  }),
  Schema.Struct({ kind: Schema.Literal("directive"), status: FleetHostStatus }),
  Schema.Struct({
    kind: Schema.Literal("directive-fence"),
    expectedRevision: Revision,
    proof: FleetPlanDirectiveFenceEvidence,
  }),
  Schema.Struct({
    kind: Schema.Literal("directive-refused"),
    reason: DirectiveRefusal,
    message: Bounded(1024),
  }),
]);
export const JonesInstallPlanRequest = Schema.Union([
  Schema.Struct({ type: Schema.Literal("read"), commandId: Schema.optionalKey(JonesOperationId) }),
  Schema.Struct({ type: Schema.Literal("read-history"), planId: JonesOperationId }),
  Schema.Struct({
    type: Schema.Literal("reconcile-orphan"),
    commandId: JonesOperationId,
    bodyHash: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
    receipt: JonesOperationReceipt,
  }),
  Schema.Struct({
    type: Schema.Literal("publication"),
    acceptOrdinaryRecovery: Schema.optionalKey(Schema.Literal(1)),
    acceptHostOnlyHolds: Schema.optionalKey(Schema.Literal(1)),
  }),
  Schema.Struct({ type: Schema.Literal("recovery-preflight"), input: JonesRecoveryPreflightInput }),
  Schema.Struct({
    type: Schema.Literal("start"),
    command: Schema.optionalKey(JonesInstallPlanClientCommand),
    origin: JonesInstallPlanOrigin,
    target: JonesInstallPlanTarget,
    select: Schema.Array(JonesInstallPlanSelection).check(Schema.isMaxLength(16)),
    expectedRunningGeneration: Schema.optionalKey(TrimmedNonEmptyString),
    laptop: Schema.optionalKey(
      Schema.Struct({
        action: Schema.Literals(["download", "install"]),
        review: Schema.optionalKey(JonesInstallPlanReview),
      }),
    ),
  }),
  Schema.Struct({
    type: Schema.Literal("edit"),
    command: Schema.optionalKey(JonesInstallPlanClientCommand),
    planId: JonesOperationId,
    expectedRevision: Schema.Number,
    select: Schema.Array(JonesInstallPlanSelection).check(Schema.isMaxLength(16)),
    deselect: Schema.Array(EnvironmentId).check(Schema.isMaxLength(16)),
  }),
  Schema.Struct({
    type: Schema.Literal("laptop"),
    command: Schema.optionalKey(JonesInstallPlanClientCommand),
    planId: JonesOperationId,
    action: Schema.Literals(["download", "stop", "install", "preflight", "abandon-stage"]),
    expectedRevision: Schema.optionalKey(Revision),
    operationId: Schema.optionalKey(JonesOperationId), // Stop or retained stage retirement: exact download.
    review: Schema.optionalKey(JonesInstallPlanReview),
  }),
  Schema.Struct({
    type: Schema.Literal("claim"),
    command: Schema.optionalKey(JonesInstallPlanClientCommand),
    planId: JonesOperationId,
    expectedRevision: Schema.optionalKey(Revision),
    environmentId: EnvironmentId,
    claimant: Bounded(64),
    step: Schema.Literals(["directive", "download", "install", "stop", "fence"]),
    explicit: Schema.Boolean, // true only for owner clicks
    operationId: Schema.optionalKey(JonesOperationId), // Stop, fence or exact forward reservation replay.
    action: Schema.optionalKey(JonesOperationAction),
    expectedInstalledSource: Schema.optionalKey(JonesSourceSha), // download/install: plan-target action and fresh installed source
    boundAdmission: Schema.optionalKey(Schema.Literal(1)), // The target advertised bound admission.
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
    runningTarget: Schema.optionalKey(RunningTargetObservation),
  }),
  Schema.Struct({
    type: Schema.Literal("abandon-device"),
    phase: Schema.Literals(["begin", "finish"]),
    planId: JonesOperationId,
    environmentId: EnvironmentId,
    expectedRevision: Schema.Number,
    directiveRelease: Schema.optionalKey(FleetHostStatus),
    runningTarget: Schema.optionalKey(RunningTargetObservation),
    stageReplacement: Schema.optionalKey(StageReplacement),
  }),
  Schema.Struct({
    type: Schema.Literal("dismiss"),
    planId: JonesOperationId,
    expectedRevision: Schema.Number,
  }),
  Schema.Struct({
    type: Schema.Literal("cancel-plan"),
    planId: JonesOperationId,
    expectedRevision: Revision,
    environmentId: Schema.optionalKey(EnvironmentId), // Native CLI binds the reviewed Laptop.
  }),
  Schema.Struct({
    type: Schema.Literal("close-after-manual-install"),
    planId: JonesOperationId,
    expectedRevision: Schema.Number,
    laptop: RunningBuild,
    activeGeneration: TrimmedNonEmptyString,
    devices: Schema.Array(ManualInstallObservation).check(Schema.isMaxLength(16)),
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
    Schema.Struct({
      kind: Schema.Literal("plan-directive-fence"),
      input: FleetPlanDirectiveInput,
      expectedRevision: Revision,
    }),
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

const PublicationHolds = Schema.optionalKey(
  Schema.Array(EnvironmentId).check(Schema.isMaxLength(JONES_INSTALL_PLAN_HISTORY_LIMIT * 16)),
);

export const JonesInstallPlanResult = Schema.Struct({
  state: JonesInstallPlanState,
  historyPlan: Schema.optionalKey(JonesInstallPlan),
  command: Schema.optionalKey(JonesInstallPlanCommandReceipt),
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
        heldEnvironments: PublicationHolds,
        source: Schema.Struct({
          repository: Schema.Literal("Jones-Systems/Jones-Code"),
          sha: JonesSourceSha,
          tree: JonesSourceSha,
        }),
      }),
      Schema.Struct({
        kind: Schema.Literal("ordinary-verified"),
        heldEnvironments: PublicationHolds,
        source: Schema.Struct({
          repository: Schema.Literal("Jones-Systems/Jones-Code"),
          sha: JonesSourceSha,
          tree: JonesSourceSha,
        }),
      }),
      Schema.Struct({
        kind: Schema.Literal("withheld"),
        heldEnvironments: PublicationHolds,
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

export function findInstallPlanResultPlan(result: JonesInstallPlanResult, planId: string) {
  return [result.state.current, result.state.last, result.historyPlan].find(
    (plan): plan is JonesInstallPlan => plan?.planId === planId,
  );
}

// Local desktop control accepts intent only. Native proof and remote reports are
// produced by the existing coordinator and runner.
export const JonesInstallPlanCliRequest = Schema.Struct({
  version: Schema.Literal(1),
  requestId: JonesOperationId,
  type: Schema.Literal("jones-install-plan"),
  action: Schema.Union([
    Schema.Struct({ type: Schema.Literal("read"), planId: Schema.optionalKey(JonesOperationId) }),
    Schema.Struct({
      type: Schema.Literal("cancel-plan"),
      confirmed: Schema.Literal(true),
      environmentId: EnvironmentId,
      planId: JonesOperationId,
      expectedRevision: Revision,
    }),
  ]),
});
export type JonesInstallPlanCliRequest = typeof JonesInstallPlanCliRequest.Type;

export function confirmsInstallPlanCancellation(
  result: JonesInstallPlanResult,
  action: Extract<JonesInstallPlanCliRequest["action"], { type: "cancel-plan" }>,
): boolean {
  const plan = findInstallPlanResultPlan(result, action.planId);
  return (
    result.state.readOnly === undefined &&
    result.state.laptop.environmentId === action.environmentId &&
    plan?.laptop.environmentId === action.environmentId &&
    plan.cancelled === true &&
    (plan.cancelledFromRevision === undefined
      ? action.expectedRevision < plan.revision
      : action.expectedRevision === plan.cancelledFromRevision)
  );
}

export const JonesInstallPlanCliResponse = Schema.Union([
  Schema.Struct({
    version: Schema.Literal(1),
    requestId: JonesOperationId,
    ok: Schema.Literal(true),
    result: JonesInstallPlanResult,
    projection: Schema.optionalKey(Schema.Literal("plan-detail")),
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    requestId: JonesOperationId,
    ok: Schema.Literal(false),
    code: Schema.Literals(["unsupported", "environment-mismatch", "unavailable"]),
    message: Bounded(512),
  }),
]);
export type JonesInstallPlanCliResponse = typeof JonesInstallPlanCliResponse.Type;
