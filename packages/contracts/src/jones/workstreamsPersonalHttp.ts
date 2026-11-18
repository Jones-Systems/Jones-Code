import * as Schema from "effect/Schema";
import * as HttpApiEndpoint from "effect/http-api/HttpApiEndpoint";
import * as HttpApiGroup from "effect/http-api/HttpApiGroup";
import { EnvironmentId, NonNegativeInt, ThreadId } from "../baseSchemas.ts";
import type { makeJonesHttpGroups } from "./environmentHttpGroups.ts";
import { exactStruct } from "./exactStruct.ts";
import {
  PersonalTimestamp,
  PersonalWorkstream,
  PersonalWorkstreamAssignment,
  PersonalWorkstreamId,
} from "./workstreamsPersonal.ts";

/**
 * Bearer HTTP contract for personal Workstreams, advertised as
 * `capabilities.workstreamsPersonalHttp.version >= 1`. External queue dispatchers use it to
 * list definitions and place a thread. Additive fields stay under this capability; a
 * breaking change uses a new capability key.
 */
export const PERSONAL_WORKSTREAMS_LIST_PATH = "/api/jones/personal-workstreams";
export const PERSONAL_WORKSTREAMS_ASSIGN_PATH = "/api/jones/personal-workstreams/assign";

const PersonalWorkstreamHttpDefinition = Schema.Struct({
  id: PersonalWorkstream.fields.id,
  name: PersonalWorkstream.fields.name,
  lifecycle: PersonalWorkstream.fields.lifecycle,
  color: PersonalWorkstream.fields.color,
  orderKey: PersonalWorkstream.fields.orderKey,
  rev: PersonalWorkstream.fields.rev,
  updatedAt: PersonalWorkstream.fields.updatedAt,
});
export type PersonalWorkstreamHttpDefinition = typeof PersonalWorkstreamHttpDefinition.Type;

/** Live definitions only, ordered by `orderKey` then `id`. */
export const PersonalWorkstreamsHttpList = Schema.Struct({
  schema: Schema.Literal("jones.personal-workstreams-list/v1"),
  environmentId: EnvironmentId,
  sequence: NonNegativeInt,
  observedAt: PersonalTimestamp,
  workstreams: Schema.Array(PersonalWorkstreamHttpDefinition),
});
export type PersonalWorkstreamsHttpList = typeof PersonalWorkstreamsHttpList.Type;

const PersonalWorkstreamHttpOperationId = Schema.String.check(
  Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]{15,127}$/),
);
export type PersonalWorkstreamHttpOperationId = typeof PersonalWorkstreamHttpOperationId.Type;

const PersonalWorkstreamHttpOrigin = exactStruct({
  kind: Schema.Literals(["queue", "cli", "agent"]),
  requestId: Schema.optionalKey(PersonalWorkstreamId),
});
export type PersonalWorkstreamHttpOrigin = typeof PersonalWorkstreamHttpOrigin.Type;

const PersonalWorkstreamHttpExpectation = Schema.Union([
  exactStruct({ kind: Schema.Literal("unassigned") }),
  exactStruct({ kind: Schema.Literal("revision"), revision: NonNegativeInt }),
]);
export type PersonalWorkstreamHttpExpectation = typeof PersonalWorkstreamHttpExpectation.Type;

/**
 * Stateless: `operationId` is echoed, never stored, so its reuse with another body is not
 * detected. Retries are safe because a held target reports `already_satisfied`.
 * `workstreamId: null` unassigns. Excess properties are rejected.
 */
export const PersonalWorkstreamHttpAssignRequest = exactStruct({
  schema: Schema.Literal("jones.personal-workstream-assign/v1"),
  operationId: PersonalWorkstreamHttpOperationId,
  origin: Schema.optionalKey(PersonalWorkstreamHttpOrigin),
  threadId: ThreadId,
  workstreamId: Schema.NullOr(PersonalWorkstreamId),
  expected: PersonalWorkstreamHttpExpectation,
});
export type PersonalWorkstreamHttpAssignRequest = typeof PersonalWorkstreamHttpAssignRequest.Type;

/**
 * Evaluated in order: `thread_not_found`, `workstream_not_found` (unknown or deleted target),
 * `already_satisfied`, `precondition_failed`, then `applied`.
 */
export const PersonalWorkstreamHttpAssignOutcome = Schema.Literals([
  "applied",
  "already_satisfied",
  "precondition_failed",
  "workstream_not_found",
  "thread_not_found",
]);
export type PersonalWorkstreamHttpAssignOutcome = typeof PersonalWorkstreamHttpAssignOutcome.Type;

export const PersonalWorkstreamHttpAssignResult = Schema.Struct({
  schema: Schema.Literal("jones.personal-workstream-assign-result/v1"),
  operationId: PersonalWorkstreamHttpOperationId,
  environmentId: EnvironmentId,
  outcome: PersonalWorkstreamHttpAssignOutcome,
  /** The thread's assignment after the call; null only for `thread_not_found`. */
  assignment: Schema.NullOr(PersonalWorkstreamAssignment),
  /** Definition for `assignment.workstreamId`; null when unassigned or missing. */
  workstream: Schema.NullOr(
    Schema.Struct({
      id: PersonalWorkstream.fields.id,
      name: PersonalWorkstream.fields.name,
      rev: PersonalWorkstream.fields.rev,
    }),
  ),
  sequence: NonNegativeInt,
  observedAt: PersonalTimestamp,
});
export type PersonalWorkstreamHttpAssignResult = typeof PersonalWorkstreamHttpAssignResult.Type;

/** Every domain outcome is a 200 with `outcome`; error statuses mean transport or authority. */
export function makePersonalWorkstreamsHttpGroup({
  OptionalBearerHeaders,
  EnvironmentAuthenticatedAuth,
  EnvironmentScopeRequiredError,
  EnvironmentInternalError,
  EnvironmentHttpBadRequestError,
}: Parameters<typeof makeJonesHttpGroups>[0]) {
  const error = [
    EnvironmentScopeRequiredError,
    EnvironmentInternalError,
    EnvironmentHttpBadRequestError,
  ] as const;
  return HttpApiGroup.make("workstreamsPersonal")
    .add(
      HttpApiEndpoint.get("list", PERSONAL_WORKSTREAMS_LIST_PATH, {
        headers: OptionalBearerHeaders,
        success: PersonalWorkstreamsHttpList,
        error,
      }).middleware(EnvironmentAuthenticatedAuth),
    )
    .add(
      HttpApiEndpoint.post("assign", PERSONAL_WORKSTREAMS_ASSIGN_PATH, {
        headers: OptionalBearerHeaders,
        payload: PersonalWorkstreamHttpAssignRequest,
        success: PersonalWorkstreamHttpAssignResult,
        error,
      }).middleware(EnvironmentAuthenticatedAuth),
    );
}
