import * as Schema from "effect/Schema";
import { AuthEnvironmentScopes, AuthMcpClientAccess, ServerAuthSessionMethod } from "../auth.ts";
import { RuntimeMode, ProviderInteractionMode } from "../providerPolicy.ts";
import {
  closedWorkstreamsNative as closed,
  WorkstreamsNativeId as Id,
  WorkstreamsNativeTextId as TextId,
  WorkstreamsNativeVersion as Version,
  WorkstreamsNativeTimestamp as Timestamp,
  WorkstreamsNativeDigest as Digest,
  WorkstreamsNativeHomeTarget,
  WorkstreamsNativeSequence as Sequence,
} from "./workstreamsNative.ts";

export const WorkstreamsSourceAuthority = closed(
  Schema.Struct({
    environment_id: Id,
    authority_namespace: TextId,
    store_generation: Version,
  }),
);
export type WorkstreamsSourceAuthority = typeof WorkstreamsSourceAuthority.Type;
const runtimeRank = { "approval-required": 0, "auto-accept-edits": 1, auto: 2, "full-access": 3 };
export const WorkstreamsTransportCeiling = closed(
  Schema.Struct({
    access: AuthMcpClientAccess,
    runtime_mode: RuntimeMode,
    interaction_mode: ProviderInteractionMode,
    scopes: AuthEnvironmentScopes.check(
      Schema.isMaxLength(64),
      Schema.makeFilter((xs) => new Set(xs).size === xs.length),
    ),
    permissions: Schema.Array(Schema.Literals(["workstreams:read", "workstreams:write"])).check(
      Schema.isMaxLength(2),
      Schema.makeFilter((xs) => new Set(xs).size === xs.length),
    ),
  }),
).check(
  Schema.makeFilter(
    (c) =>
      runtimeRank[c.runtime_mode] <= (c.access === "read-only" ? 0 : runtimeRank[c.access]) &&
      (c.access !== "read-only" || !c.permissions.includes("workstreams:write")),
  ),
);
export type WorkstreamsTransportCeiling = typeof WorkstreamsTransportCeiling.Type;

// A closed retained record is not authentication evidence. Only the receiving authenticator
// and qualified setup services may mint it after checking actual native session facts.
export const WorkstreamsPeerSessionPinV1 = closed(
  Schema.Struct({
    schema: Schema.Literal("jones-workstreams-peer-session-pin/1"),
    pin_id: Id,
    revision: Version,
    receiver_authority: WorkstreamsSourceAuthority,
    session: closed(
      Schema.Struct({
        session_id: TextId,
        subject: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(512)),
        method: ServerAuthSessionMethod,
        issued_at: Timestamp,
        expires_at: Timestamp,
        peer_link: Schema.Literal(true),
        ceiling: WorkstreamsTransportCeiling,
      }),
    ),
    peer_environment_id: Id,
    roles: Schema.Array(Schema.Literals(["member_home", "home_source", "thread_origin"])).check(
      Schema.isMinLength(1),
      Schema.isMaxLength(3),
      Schema.makeFilter((xs) => new Set(xs).size === xs.length),
    ),
    expected_home: WorkstreamsNativeHomeTarget,
    source_authority: WorkstreamsSourceAuthority,
    source_enrollment_id: Id,
    source_binding_version: Schema.NullOr(Version),
    singleton_binding_version: Schema.NullOr(Version),
    state: Schema.Literals(["prepared", "bound", "revoked"]),
    expires_at: Timestamp,
    setup_request_id: Id,
    setup_request_sha256: Digest,
    owner_qualification_digest: Digest,
    bind_receipt: Schema.NullOr(
      closed(
        Schema.Struct({
          principal_id: Id,
          binding_version: Version,
          registry_version: Sequence,
          request_sha256: Digest,
        }),
      ),
    ),
  }),
).check(
  Schema.makeFilter(
    (p) =>
      Date.parse(p.session.issued_at) < Date.parse(p.expires_at) &&
      Date.parse(p.expires_at) <= Date.parse(p.session.expires_at) &&
      (p.state !== "bound" ||
        (p.source_binding_version !== null &&
          p.singleton_binding_version !== null &&
          p.bind_receipt !== null &&
          p.bind_receipt.binding_version === p.source_binding_version)),
  ),
);
export type WorkstreamsPeerSessionPinV1 = typeof WorkstreamsPeerSessionPinV1.Type;
