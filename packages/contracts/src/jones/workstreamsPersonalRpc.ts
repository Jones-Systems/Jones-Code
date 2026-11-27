import * as Schema from "effect/Schema";
import * as Rpc from "effect/rpc/Rpc";
import { EnvironmentAuthorizationError } from "../auth.ts";
import { EnvironmentId, NonNegativeInt } from "../baseSchemas.ts";
import {
  AssignPersonalWorkstreamInput,
  AssignPersonalWorkstreamResult,
  PersonalWorkstreamsSnapshot,
  PersonalWorkstreamMembers,
  PersonalWorkstreamMembersInput,
  PersonalWorkstreamFleet,
  PersonalWorkstreamId,
  PersonalWorkstreamOperationResult,
  RequestPersonalWorkstreamOperation,
  PutPersonalWorkstreamInput,
  PutPersonalWorkstreamResult,
  WorkstreamsPersonalError,
} from "./workstreamsPersonal.ts";

export {
  AssignPersonalWorkstreamInput,
  AssignPersonalWorkstreamResult,
  PutPersonalWorkstreamInput,
  PutPersonalWorkstreamResult,
  WorkstreamsPersonalError,
} from "./workstreamsPersonal.ts";

const Failure = Schema.Union([WorkstreamsPersonalError, EnvironmentAuthorizationError]);

export const SubscribePersonalWorkstreamsRpc = Rpc.make("workstreamsPersonal.subscribe", {
  payload: Schema.Struct({}),
  success: PersonalWorkstreamsSnapshot,
  error: Failure,
  stream: true,
});

export const ReadPersonalWorkstreamMembersRpc = Rpc.make("workstreamsPersonal.members", {
  payload: PersonalWorkstreamMembersInput,
  success: PersonalWorkstreamMembers,
  error: Failure,
});

export const PutPersonalWorkstreamRpc = Rpc.make("workstreamsPersonal.put", {
  payload: PutPersonalWorkstreamInput,
  success: PutPersonalWorkstreamResult,
  error: Failure,
});

export const AssignPersonalWorkstreamRpc = Rpc.make("workstreamsPersonal.assign", {
  payload: AssignPersonalWorkstreamInput,
  success: AssignPersonalWorkstreamResult,
  error: Failure,
});

export const ManagePersonalWorkstreamFleetInput = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("designate"),
    peerEnvironmentIds: Schema.optionalKey(Schema.Array(EnvironmentId)),
  }),
  Schema.Struct({
    kind: Schema.Literal("follow"),
    authorityEnvironmentId: EnvironmentId,
    sessionId: Schema.String,
  }),
  Schema.Struct({
    kind: Schema.Literals(["enroll", "remove"]),
    environmentId: EnvironmentId,
    expectedRosterRevision: NonNegativeInt,
  }),
]);
export type ManagePersonalWorkstreamFleetInput = typeof ManagePersonalWorkstreamFleetInput.Type;

export const RequestPersonalWorkstreamOperationRpc = Rpc.make("workstreamsPersonal.request", {
  payload: RequestPersonalWorkstreamOperation,
  success: PersonalWorkstreamOperationResult,
  error: Failure,
});

export const CancelPersonalWorkstreamOperationInput = Schema.Struct({
  requestId: PersonalWorkstreamId,
  cancellationId: PersonalWorkstreamId,
});
export type CancelPersonalWorkstreamOperationInput =
  typeof CancelPersonalWorkstreamOperationInput.Type;
export const CancelPersonalWorkstreamOperationRpc = Rpc.make("workstreamsPersonal.cancel", {
  payload: CancelPersonalWorkstreamOperationInput,
  success: PersonalWorkstreamOperationResult,
  error: Failure,
});

export const ManagePersonalWorkstreamFleetRpc = Rpc.make("workstreamsPersonal.fleet", {
  payload: ManagePersonalWorkstreamFleetInput,
  success: PersonalWorkstreamFleet,
  error: Failure,
});
export const ReleasePersonalWorkstreamFollowerRpc = Rpc.make("workstreamsPersonal.release", {
  payload: Schema.Struct({ authorityEnvironmentId: EnvironmentId }),
  success: Schema.Struct({ released: Schema.Literal(true) }),
  error: Failure,
});

export const ResolvePersonalWorkstreamConflictInput = Schema.Struct({
  requestId: PersonalWorkstreamId,
  resolutionId: PersonalWorkstreamId,
  environmentId: EnvironmentId,
  expectedObservationKey: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)),
  choice: Schema.Literals(["use_authority", "use_peer"]),
});
export type ResolvePersonalWorkstreamConflictInput =
  typeof ResolvePersonalWorkstreamConflictInput.Type;
export const ResolvePersonalWorkstreamConflictRpc = Rpc.make("workstreamsPersonal.resolve", {
  payload: ResolvePersonalWorkstreamConflictInput,
  success: PersonalWorkstreamOperationResult,
  error: Failure,
});
