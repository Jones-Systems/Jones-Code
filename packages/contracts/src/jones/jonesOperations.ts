import * as Schema from "effect/Schema";
import { EnvironmentId, TrimmedNonEmptyString } from "../baseSchemas.ts";

const Uuid4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
export const JonesOperationId = Schema.String.check(Schema.isPattern(Uuid4));
export type JonesOperationId = typeof JonesOperationId.Type;

export const JonesSourceSha = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));
export type JonesSourceSha = typeof JonesSourceSha.Type;

const Bounded = (max: number) => Schema.String.check(Schema.isMaxLength(max));
/** Open, bounded code; unknown codes render generically, so new codes need no new field. */
export const JonesOperationCode = Schema.String.check(Schema.isPattern(/^[a-z][a-z0-9-]{0,47}$/));
export type JonesOperationCode = typeof JonesOperationCode.Type;

export const JonesRecoveryPolicy = Schema.Literals(["clone-required", "clone-or-copy"]);
export type JonesRecoveryPolicy = typeof JonesRecoveryPolicy.Type;

export const JonesOperationAction = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("download-exact"), targetSource: JonesSourceSha }),
  Schema.Struct({
    kind: Schema.Literal("download"),
    artifactId: Schema.Number,
    sourceSha: JonesSourceSha,
  }),
  Schema.Struct({
    kind: Schema.Literal("install"),
    stagedHandle: TrimmedNonEmptyString,
    currentVersion: TrimmedNonEmptyString,
    targetSource: JonesSourceSha,
    continueRunningThreads: Schema.optionalKey(Schema.Boolean),
    recovery: Schema.optionalKey(
      Schema.Struct({ policy: JonesRecoveryPolicy, preflightId: JonesOperationId }),
    ),
  }),
]);
export type JonesOperationAction = typeof JonesOperationAction.Type;

export const JonesOperationCorrelation = Schema.Struct({
  planId: JonesOperationId,
  deviceKey: Bounded(128),
});
export type JonesOperationCorrelation = typeof JonesOperationCorrelation.Type;

export const JonesOperationStartInput = Schema.Struct({
  operationId: JonesOperationId,
  environmentId: EnvironmentId,
  expectedInstalledSource: JonesSourceSha,
  action: JonesOperationAction,
  correlation: Schema.optionalKey(JonesOperationCorrelation),
});
export type JonesOperationStartInput = typeof JonesOperationStartInput.Type;

export const JonesOperationStopInput = Schema.Struct({
  operationId: JonesOperationId,
  stopId: JonesOperationId,
  environmentId: EnvironmentId,
});
export type JonesOperationStopInput = typeof JonesOperationStopInput.Type;

// Fencing preserves the unsent intent without claiming it was accepted or executed.
export const JonesOperationFenceInput = JonesOperationStartInput;
export type JonesOperationFenceInput = typeof JonesOperationFenceInput.Type;

export const JonesOperationReadInput = Schema.Struct({
  environmentId: EnvironmentId,
  operationIds: Schema.Array(JonesOperationId).check(Schema.isMaxLength(32)),
});
export type JonesOperationReadInput = typeof JonesOperationReadInput.Type;

export const JonesOperationPhase = Schema.Literals([
  // These first six phases are not terminal.
  "accepted",
  "running",
  "stop-requested",
  "adopting",
  "handed-off",
  "held",
  "completed",
  "rolled-back",
  "stopped",
  "refused",
  "failed",
  "blocked",
  "fenced",
  "interrupted",
]);
export type JonesOperationPhase = typeof JonesOperationPhase.Type;

export const JonesOperationReceipt = Schema.Struct({
  protocol: Schema.Literal(1),
  operationId: JonesOperationId,
  environmentId: EnvironmentId,
  action: JonesOperationAction,
  expectedInstalledSource: JonesSourceSha,
  correlation: Schema.optionalKey(JonesOperationCorrelation),
  phase: JonesOperationPhase,
  acceptedAt: Schema.optionalKey(Schema.String), // absent: never accepted (refused before acceptance, or fenced)
  updatedAt: Schema.String,
  jobId: Schema.optionalKey(JonesOperationId), // persisted before the worker spawns
  staged: Schema.optionalKey(
    Schema.Struct({
      stagedHandle: TrimmedNonEmptyString,
      sourceSha: JonesSourceSha,
      sourceTree: JonesSourceSha,
      artifactId: Schema.Number,
      version: Schema.optionalKey(TrimmedNonEmptyString),
    }),
  ),
  updateId: Schema.optionalKey(TrimmedNonEmptyString), // launcher update ID; equals operationId for install
  outcome: Schema.optionalKey(
    Schema.Struct({
      status: Schema.Literals(["committed", "rolled-back", "blocked"]),
      fromVersion: TrimmedNonEmptyString,
      targetVersion: TrimmedNonEmptyString,
      reason: Schema.optionalKey(Bounded(1024)),
    }),
  ),
  recovery: Schema.optionalKey(
    Schema.Struct({
      requested: JonesRecoveryPolicy,
      observed: Schema.optionalKey(Schema.Literals(["clone", "copy"])),
    }),
  ),
  stop: Schema.optionalKey(
    Schema.Struct({
      stopId: JonesOperationId,
      requestedAt: Schema.String,
      result: Schema.optionalKey(
        Schema.Literals(["stopped", "too-late", "held", "not-stoppable-phase"]),
      ),
    }),
  ),
  code: Schema.optionalKey(JonesOperationCode),
  message: Schema.optionalKey(Bounded(1024)),
});
export type JonesOperationReceipt = typeof JonesOperationReceipt.Type;

export const JonesOperationReadResult = Schema.Struct({
  receipts: Schema.Array(
    Schema.Struct({ operationId: JonesOperationId, receipt: Schema.NullOr(JonesOperationReceipt) }),
  ).check(Schema.isMaxLength(32)),
});
export type JonesOperationReadResult = typeof JonesOperationReadResult.Type;

export const JonesRecoveryPreflightInput = Schema.Struct({
  environmentId: EnvironmentId,
  stagedHandle: TrimmedNonEmptyString,
  currentVersion: TrimmedNonEmptyString,
  expectedInstalledSource: JonesSourceSha,
});
export type JonesRecoveryPreflightInput = typeof JonesRecoveryPreflightInput.Type;

export const JonesRecoveryPreflight = Schema.Struct({
  protocol: Schema.Literal(1),
  preflightId: JonesOperationId,
  measuredAt: Schema.String,
  expiresAt: Schema.String, // measuredAt + 5 minutes
  binding: JonesRecoveryPreflightInput,
  firstInstall: Schema.Boolean, // no existing state: no backup is claimed or offered
  stateBytes: Schema.Number, // DB + WAL + SHM + required settings only
  candidateBytes: Schema.optionalKey(Schema.Number),
  volumes: Schema.Array(
    Schema.Struct({
      role: Schema.Literals(["state", "candidate"]),
      availableBytes: Schema.Number,
      copyRequiredBytes: Schema.Number,
      cloneRequiredBytes: Schema.Number,
    }),
  ).check(Schema.isMaxLength(4)),
  clone: Schema.Struct({
    status: Schema.Literals(["proved", "unsupported", "unknown"]),
    reason: Schema.optionalKey(Bounded(512)),
  }),
  policies: Schema.Struct({
    cloneOrCopy: Schema.Struct({
      admissible: Schema.Boolean,
      reason: Schema.optionalKey(Bounded(512)),
    }),
    cloneRequired: Schema.Struct({
      admissible: Schema.Boolean,
      reason: Schema.optionalKey(Bounded(512)),
    }),
  }),
});
export type JonesRecoveryPreflight = typeof JonesRecoveryPreflight.Type;

