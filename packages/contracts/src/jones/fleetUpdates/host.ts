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
export const FleetActivateInput = Schema.Struct({
  operationId: FleetOperationId,
  enrollmentId: FleetOperationId,
  environmentId: EnvironmentId,
});
export type FleetActivateInput = typeof FleetActivateInput.Type;

export const FleetHostOperation = Schema.Struct({
  input: FleetStageInput,
  phase: Schema.Literals([
    "staging", "stage-blocked", "staged", "dispatching", "pending", "current", "committed", "rolled-back", "blocked",
  ]),
  continueRunningThreads: Schema.Boolean,
  currentVersion: TrimmedNonEmptyString,
  stagedHandle: Schema.optionalKey(TrimmedNonEmptyString),
  updateId: Schema.optionalKey(TrimmedNonEmptyString),
  reason: Schema.optionalKey(Schema.String),
});
export type FleetHostOperation = typeof FleetHostOperation.Type;
export const FleetHostStatus = Schema.Struct({
  operationProtocol: Schema.NullOr(Schema.Literal(1)),
  environmentId: EnvironmentId,
  enrollment: Schema.NullOr(FleetEnrollment),
  operation: Schema.NullOr(FleetHostOperation),
  update: Schema.NullOr(JonesUpdateState),
});
export type FleetHostStatus = typeof FleetHostStatus.Type;

export class FleetHostError extends Schema.TaggedError<FleetHostError>()("FleetHostError", {
  reason: Schema.Literals(["binding", "not-enrolled", "bootstrap-required", "conflict", "storage"]),
  message: Schema.String,
}) {}
export const FLEET_UPDATES_HTTP_BASE = "/api/jones-fleet-updates";
