import * as Schema from "effect/Schema";
import { EnvironmentId, TrimmedNonEmptyString } from "../../baseSchemas.ts";
import { JonesUpdateState } from "../jonesUpdates.ts";

export const FleetOperationId = Schema.String.check(
  Schema.isPattern(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/),
);
export const FleetSourceSha = Schema.String.check(Schema.isPattern(/^[0-9a-f]{40}$/));

/** Enrollment authorizes only the already-qualified service in this environment. */
export const FleetEnrollment = Schema.Struct({
  enrollmentId: FleetOperationId,
  environmentId: EnvironmentId,
  enabled: Schema.Boolean,
  continueRunningThreads: Schema.Boolean,
});
export type FleetEnrollment = typeof FleetEnrollment.Type;
export const FleetEnrollmentInput = Schema.Struct({
  enrollment: FleetEnrollment,
  expectedEnrollmentId: Schema.NullOr(FleetOperationId),
});
export type FleetEnrollmentInput = typeof FleetEnrollmentInput.Type;

export const FleetStageInput = Schema.Struct({
  operationId: FleetOperationId,
  enrollmentId: FleetOperationId,
  environmentId: EnvironmentId,
  targetSource: FleetSourceSha,
  expectedInstalledSource: FleetSourceSha,
});
export type FleetStageInput = typeof FleetStageInput.Type;
export const FleetRetireInput = FleetStageInput;
export type FleetRetireInput = FleetStageInput;
export const FleetActivateInput = Schema.Struct({
  operationId: FleetOperationId,
  enrollmentId: FleetOperationId,
  environmentId: EnvironmentId,
});
export type FleetActivateInput = typeof FleetActivateInput.Type;

export const FleetHostOperation = Schema.Struct({
  input: FleetStageInput,
  phase: Schema.Literals([
    "staging",
    "stage-blocked",
    "staged",
    "dispatching",
    "install-blocked",
    "reconciling",
    "pending",
    "current",
    "committed",
    "rolled-back",
    "blocked",
    "superseded",
  ]),
  continueRunningThreads: Schema.Boolean,
  currentVersion: TrimmedNonEmptyString,
  stagedHandle: Schema.optionalKey(TrimmedNonEmptyString),
  updateId: Schema.optionalKey(TrimmedNonEmptyString),
  reason: Schema.optionalKey(Schema.String),
});
export type FleetHostOperation = typeof FleetHostOperation.Type;
/**
 * The laptop's committed source, published to a host enrolled to follow it. The laptop sends the
 * provenance of its native active install; the host stages it only when the repository's record of
 * that commit has the same tree.
 */
export const FleetLeaderInput = Schema.Struct({
  enrollmentId: FleetOperationId,
  environmentId: EnvironmentId,
  repository: Schema.Literal("Jones-Systems/Jones-Code"),
  committedSource: FleetSourceSha,
  committedTree: FleetSourceSha,
});
export type FleetLeaderInput = typeof FleetLeaderInput.Type;

/** Work that a restart would cut or lose, so an automatic activation waits for it. */
export const FleetFollowerBlocker = Schema.Struct({
  kind: Schema.Literals([
    "terminal",
    "run",
    "queued-run",
    "process-effect",
    "background-task",
    "approval",
    "tool-call",
    "unknown",
  ]),
  count: Schema.Number,
});
export type FleetFollowerBlocker = typeof FleetFollowerBlocker.Type;

/**
 * A host that follows the laptop prepares and activates the laptop's committed source itself.
 * Activation waits for a quiet window: no recent input from any client and no work a restart
 * would cut. Older hosts omit this and are driven by the laptop's campaigns.
 */
export const FleetFollowerStatus = Schema.Struct({
  protocol: Schema.Literal(1),
  leaderSource: Schema.NullOr(FleetSourceSha),
  leaderPublishedAt: Schema.optionalKey(Schema.String),
  state: Schema.Literals([
    "unavailable",
    "waiting-for-leader",
    "current",
    "preparing",
    "prepare-blocked",
    "waiting-for-quiet",
    "activating",
    "reconciling",
  ]),
  reason: Schema.optionalKey(Schema.String),
  quiet: Schema.optionalKey(
    Schema.Struct({
      inputIdleMs: Schema.Number,
      requiredInputIdleMs: Schema.Number,
      blockers: Schema.Array(FleetFollowerBlocker),
      evaluatedAt: Schema.String,
    }),
  ),
  /** The earliest time the next automatic activation may start. */
  notBefore: Schema.optionalKey(Schema.String),
  operationId: Schema.optionalKey(FleetOperationId),
});
export type FleetFollowerStatus = typeof FleetFollowerStatus.Type;

export const FleetHostStatus = Schema.Struct({
  operationProtocol: Schema.NullOr(Schema.Literal(1)),
  environmentId: EnvironmentId,
  enrollment: Schema.NullOr(FleetEnrollment),
  operation: Schema.NullOr(FleetHostOperation),
  update: Schema.NullOr(JonesUpdateState),
  follower: Schema.optionalKey(FleetFollowerStatus),
});
export type FleetHostStatus = typeof FleetHostStatus.Type;

export class FleetHostError extends Schema.TaggedError<FleetHostError>()("FleetHostError", {
  reason: Schema.Literals(["binding", "not-enrolled", "bootstrap-required", "conflict", "storage"]),
  message: Schema.String,
}) {}
export const FLEET_UPDATES_HTTP_BASE = "/api/jones-fleet-updates";
