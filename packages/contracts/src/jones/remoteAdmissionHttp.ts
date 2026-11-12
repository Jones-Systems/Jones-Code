import * as Schema from "effect/Schema";
import * as HttpApiEndpoint from "effect/http-api/HttpApiEndpoint";
import * as HttpApiGroup from "effect/http-api/HttpApiGroup";
import type { makeJonesHttpGroups } from "./environmentHttpGroups.ts";
import {
  RemoteAdmissionDigest,
  RemoteAdmissionReceiverPolicy,
  RemoteAdmissionRecord,
} from "./remoteAdmission.ts";

export const RemoteAdmissionPolicyUpdate = Schema.Struct({
  expectedRevision: Schema.NonEmptyString,
  enabled: Schema.Boolean,
  modelSelection: RemoteAdmissionReceiverPolicy.fields.modelSelection,
});
export type RemoteAdmissionPolicyUpdate = typeof RemoteAdmissionPolicyUpdate.Type;
export const RemoteAdmissionDecision = Schema.Struct({
  id: Schema.NonEmptyString,
  bindingDigest: RemoteAdmissionDigest,
  decision: Schema.Literals(["approve", "deny"]),
});
export type RemoteAdmissionDecision = typeof RemoteAdmissionDecision.Type;
export const RemoteAdmissionSnapshot = Schema.Struct({
  eligible: Schema.Boolean,
  policy: RemoteAdmissionReceiverPolicy,
  pending: Schema.Array(RemoteAdmissionRecord),
});
export type RemoteAdmissionSnapshot = typeof RemoteAdmissionSnapshot.Type;

export function makeRemoteAdmissionHttpGroup({
  OptionalBearerHeaders,
  EnvironmentAuthenticatedAuth,
  EnvironmentScopeRequiredError,
  EnvironmentInternalError,
  EnvironmentHttpBadRequestError,
}: Parameters<typeof makeJonesHttpGroups>[0]) {
  const common = {
    headers: OptionalBearerHeaders,
    error: [
      EnvironmentScopeRequiredError,
      EnvironmentInternalError,
      EnvironmentHttpBadRequestError,
    ],
  } as const;
  return HttpApiGroup.make("remoteAdmission")
    .add(
      HttpApiEndpoint.get("attention", "/api/jones/remote-admission/attention", {
        ...common,
        success: Schema.Struct({ count: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)) }),
      }).middleware(EnvironmentAuthenticatedAuth),
    )
    .add(
      HttpApiEndpoint.get("snapshot", "/api/jones/remote-admission", {
        ...common,
        success: RemoteAdmissionSnapshot,
      }).middleware(EnvironmentAuthenticatedAuth),
    )
    .add(
      HttpApiEndpoint.put("policy", "/api/jones/remote-admission/policy", {
        ...common,
        payload: RemoteAdmissionPolicyUpdate,
        success: RemoteAdmissionSnapshot,
      }).middleware(EnvironmentAuthenticatedAuth),
    )
    .add(
      HttpApiEndpoint.post("decide", "/api/jones/remote-admission/decision", {
        ...common,
        payload: RemoteAdmissionDecision,
        success: RemoteAdmissionRecord,
      }).middleware(EnvironmentAuthenticatedAuth),
    );
}
