import * as Schema from "effect/Schema";
import * as HttpApiEndpoint from "effect/http-api/HttpApiEndpoint";
import * as HttpApiGroup from "effect/http-api/HttpApiGroup";
import * as HttpApiSchema from "effect/http-api/HttpApiSchema";
import { EnvironmentId } from "../baseSchemas.ts";
import { RemoteAdmissionDigest, RemoteAdmissionStatus } from "./remoteAdmission.ts";
import type { makeJonesHttpGroups } from "./environmentHttpGroups.ts";

export const RequestId = Schema.NonEmptyString.check(Schema.isMaxLength(256));
export const RequestLabel = Schema.Trimmed.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(120),
  Schema.isPattern(/^[^\p{Cc}\p{Cf}\p{Cs}\p{Zl}\p{Zp}]+$/u),
);
export const RequestPageSize = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }));
export const RequestListFields = {
  view: Schema.optionalKey(Schema.Literals(["pending", "all", "dismissed"])),
  after: Schema.optionalKey(RequestId),
  limit: Schema.optionalKey(RequestPageSize),
};
const RequestListInput = Schema.Struct(RequestListFields);
export type RequestListInput = typeof RequestListInput.Type;

const RequestSummary = Schema.Struct({
  id: RequestId,
  operation: Schema.String,
  status: RemoteAdmissionStatus,
  createdAt: Schema.String,
  updatedAt: Schema.String,
  recoveryOutcome: Schema.NullOr(Schema.Literals(["created", "refused", "not_created", "unknown"])),
});
export type RequestSummary = typeof RequestSummary.Type;
export const RequestListResult = Schema.Struct({
  receiverEnvironmentId: EnvironmentId,
  requests: Schema.Array(RequestSummary),
  next: Schema.NullOr(RequestId),
});
export const RequestInspectResult = Schema.Struct({
  receiverEnvironmentId: EnvironmentId,
  request: Schema.NullOr(RequestSummary),
});

const ManagedRequest = Schema.Struct({
  ...RequestSummary.fields,
  label: Schema.NullOr(RequestLabel),
  revision: RemoteAdmissionDigest,
  dismissed: Schema.Boolean,
  canDismiss: Schema.Boolean,
});
export type ManagedRequest = typeof ManagedRequest.Type;
const target = { receiverEnvironmentId: EnvironmentId };
export const OwnerRequestsQuery = Schema.Union([
  Schema.Struct({ ...target, action: Schema.Literal("list"), ...RequestListFields }),
  Schema.Struct({ ...target, action: Schema.Literal("inspect"), id: RequestId }),
]);
export type OwnerRequestsQuery = typeof OwnerRequestsQuery.Type;
export const OwnerRequestsResult = Schema.Union([
  Schema.Struct({
    ...target,
    action: Schema.Literal("list"),
    requests: Schema.Array(ManagedRequest),
    next: Schema.NullOr(RequestId),
  }),
  Schema.Struct({ ...target, action: Schema.Literal("inspect"), request: ManagedRequest }),
]);
export type OwnerRequestsResult = typeof OwnerRequestsResult.Type;
const exactTarget = { ...target, id: RequestId, expectedRevision: RemoteAdmissionDigest };
export const OwnerRequestEdit = Schema.Union([
  Schema.Struct({
    ...exactTarget,
    action: Schema.Literal("label"),
    label: Schema.NullOr(RequestLabel),
  }),
  Schema.Struct({ ...exactTarget, action: Schema.Literals(["dismiss", "restore"]) }),
]);
export type OwnerRequestEdit = typeof OwnerRequestEdit.Type;
export const OwnerRequestEditResult = Schema.Struct({ ...target, request: ManagedRequest });

const RequestManagementForbidden = Schema.Struct({
  code: Schema.Literal("requests_owner_required"),
  message: Schema.String,
}).pipe(HttpApiSchema.status(403));
const RequestManagementMissing = Schema.Struct({
  code: Schema.Literal("request_unavailable"),
  message: Schema.String,
}).pipe(HttpApiSchema.status(404));
const RequestManagementConflict = Schema.Struct({
  code: Schema.Literals(["receiver_changed", "revision_changed", "request_not_dismissible"]),
  message: Schema.String,
}).pipe(HttpApiSchema.status(409));

export type RequestManagementError =
  | typeof RequestManagementForbidden.Type
  | typeof RequestManagementMissing.Type
  | typeof RequestManagementConflict.Type;

export const REQUESTS_CLI_SUBJECT = "jones-requests-cli";

export function makeRequestManagementHttpGroup({
  OptionalBearerHeaders,
  EnvironmentAuthenticatedAuth,
  EnvironmentInternalError,
}: Pick<
  Parameters<typeof makeJonesHttpGroups>[0],
  "OptionalBearerHeaders" | "EnvironmentAuthenticatedAuth" | "EnvironmentInternalError"
>) {
  const common = {
    headers: OptionalBearerHeaders,
    error: [
      RequestManagementForbidden,
      RequestManagementMissing,
      RequestManagementConflict,
      EnvironmentInternalError,
    ],
  } as const;
  return HttpApiGroup.make("requestManagement")
    .add(
      HttpApiEndpoint.post("query", "/api/jones/requests/query", {
        ...common,
        payload: OwnerRequestsQuery,
        success: OwnerRequestsResult,
      }).middleware(EnvironmentAuthenticatedAuth),
    )
    .add(
      HttpApiEndpoint.post("edit", "/api/jones/requests/edit", {
        ...common,
        payload: OwnerRequestEdit,
        success: OwnerRequestEditResult,
      }).middleware(EnvironmentAuthenticatedAuth),
    );
}
