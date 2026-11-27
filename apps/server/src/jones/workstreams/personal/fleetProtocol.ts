import { EnvironmentId, NonNegativeInt } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as P from "@t3tools/contracts/jones/workstreamsPersonal";

export const FleetPeerEnrollmentInput = Schema.Struct({
  authorityEnvironmentId: EnvironmentId,
});
export const FleetPeerEnrollment = Schema.Struct({
  environmentId: EnvironmentId,
  state: Schema.Literals(["approved", "approval_required", "different_authority"]),
});
export const FleetPeerDefinitions = Schema.Struct({
  workstreams: Schema.Array(P.PersonalWorkstream),
});
export const FleetPeerReadDefinition = Schema.Struct({
  authorityEnvironmentId: EnvironmentId,
  workstreamId: P.PersonalWorkstreamId,
});
export const FleetPeerDefinition = Schema.Struct({
  workstream: Schema.NullOr(P.PersonalWorkstream),
  lastRequestId: Schema.NullOr(P.PersonalWorkstreamId),
  lastOrdinal: Schema.NullOr(NonNegativeInt),
});
export const FleetPeerApplyDefinition = Schema.Struct({
  authorityEnvironmentId: EnvironmentId,
  requestId: P.PersonalWorkstreamId,
  workstreamId: P.PersonalWorkstreamId,
  ordinal: NonNegativeInt,
  closing: Schema.Boolean,
  candidate: Schema.NullOr(P.PersonalWorkstream),
  expected: Schema.NullOr(P.PersonalWorkstream),
});
export type FleetPeerApplyDefinition = typeof FleetPeerApplyDefinition.Type;
export const FleetPeerApplyResult = Schema.Struct({
  state: Schema.Literals(["delivered", "guard_rejected", "conflict", "blocked"]),
  reason: Schema.NullOr(Schema.String),
});
