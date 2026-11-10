import * as Schema from "effect/Schema";
import { WorkstreamCommand, WorkstreamReceipt } from "../workstreams.ts";
import { WorkstreamAppearance, WorkstreamAppearanceWrite } from "./workstreamAppearance.ts";
import { WorkstreamOperationRequest, WorkstreamOperationResult } from "./workstreamOperations.ts";
import {
  closedWorkstreamsNative as closed,
  WorkstreamsNativeCommandId,
  WorkstreamsNativeDigest as Digest,
  WorkstreamsNativeHomeCommand,
  WorkstreamsNativeHomeOperation,
  WorkstreamsNativeHomeTarget,
  WorkstreamsNativeId as Id,
  WorkstreamsNativeOrigin,
  WorkstreamsNativeRegistryContext,
  WorkstreamsNativeRelay,
  WorkstreamsNativeVersion as Version,
} from "./workstreamsNative.ts";
import { WorkstreamsSourceAuthority } from "./workstreamsPeerAuthority.ts";
import { canonicalWorkstreamsSnapshotJson as canonical } from "./workstreamsSnapshot.ts";

export const WORKSTREAMS_NATIVE_FORWARDING_CONTRACT = "workstreams-native-forwarding/1.0.0";
// Keep historical codecs intact; new submissions use an absolute end-of-input fence.
export const WorkstreamsForwardCommandId = WorkstreamsNativeCommandId.check(
  Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]{15,127}$(?![\s\S])/),
);
const CommandId = WorkstreamsForwardCommandId;
const isCommandId = Schema.is(CommandId);
export const WorkstreamsForwardIdentity = closed(
  Schema.Struct({
    forward_id: CommandId,
    source_operation_id: CommandId,
    parent_operation_id: Schema.NullOr(CommandId),
    command_ids: Schema.Array(CommandId).check(
      Schema.isMaxLength(16),
      Schema.makeFilter((ids) => new Set(ids).size === ids.length),
    ),
    request_sha256: Digest,
    recorded_caller_sha256: Digest,
    expected_home: WorkstreamsNativeHomeTarget,
    canonical_origin: WorkstreamsNativeOrigin,
    source_authority: WorkstreamsSourceAuthority,
    source_enrollment_id: Id,
    source_enrollment_session_id: Id,
    principal_id: Id,
    singleton_binding_version: Version,
    source_binding_version: Version,
    member_pin_id: Id,
    member_pin_bound_revision: Version,
    outbound_link_generation: Schema.NonEmptyString.check(Schema.isMaxLength(512)),
  }),
);
export type WorkstreamsForwardIdentity = typeof WorkstreamsForwardIdentity.Type;
const decodeCommand = Schema.decodeUnknownSync(WorkstreamCommand);
const decodeOperation = Schema.decodeUnknownSync(WorkstreamOperationRequest);
const Appearance = closed(
  Schema.Struct({
    ...WorkstreamAppearanceWrite.fields,
    command_id: CommandId,
  }),
);
const decodeAppearance = Schema.decodeUnknownSync(Appearance);
const retainedBody = <A>(decode: (input: unknown) => A) =>
  Schema.String.check(
    Schema.makeFilter((body) => {
      if (new TextEncoder().encode(body).length > 1_048_576) return false;
      try {
        decode(JSON.parse(body));
        return true;
      } catch {
        return false;
      }
    }),
  );
const common = {
  contract: Schema.Literal(WORKSTREAMS_NATIVE_FORWARDING_CONTRACT),
  identity: WorkstreamsForwardIdentity,
};
const relayFields = {
  expected_home: WorkstreamsNativeHomeTarget,
  relay: WorkstreamsNativeRelay,
};
export const WorkstreamsNativeHomeAppearance = closed(
  Schema.Struct({
    ...relayFields,
    request: Appearance,
    request_body: retainedBody(decodeAppearance),
    request_sha256: Digest,
  }),
).check(
  Schema.makeFilter(
    (r) => canonical(decodeAppearance(JSON.parse(r.request_body))) === canonical(r.request),
  ),
);
export type WorkstreamsNativeHomeAppearance = typeof WorkstreamsNativeHomeAppearance.Type;
export const WorkstreamsForwardRequest = Schema.Union([
  closed(
    Schema.Struct({
      ...common,
      kind: Schema.Literal("command"),
      request: WorkstreamsNativeHomeCommand,
    }),
  ),
  closed(
    Schema.Struct({
      ...common,
      kind: Schema.Literal("operation"),
      request: WorkstreamsNativeHomeOperation,
      request_body: retainedBody(decodeOperation),
    }),
  ),
  closed(
    Schema.Struct({
      ...common,
      kind: Schema.Literal("appearance"),
      request: WorkstreamsNativeHomeAppearance,
    }),
  ),
]).check(
  Schema.makeFilter((r) => {
    const i = r.identity;
    if (
      canonical(i.expected_home) !== canonical(r.request.expected_home) ||
      i.source_authority.environment_id !== r.request.relay.source_environment_id
    )
      return false;
    if (r.kind === "command") {
      const command = decodeCommand(JSON.parse(r.request.body));
      return (
        isCommandId(command.command_id) &&
        i.command_ids.length === 1 &&
        i.command_ids[0] === command.command_id &&
        i.request_sha256 === r.request.body_sha256 &&
        command.expected_server_generation === i.expected_home.server_generation
      );
    }
    if (r.kind === "operation")
      return (
        canonical(decodeOperation(JSON.parse(r.request_body))) === canonical(r.request.request) &&
        i.source_operation_id === r.request.request.operation_id &&
        isCommandId(r.request.request.operation_id) &&
        i.request_sha256 === r.request.request_sha256 &&
        canonical(r.request.relay.origin) === canonical(r.request.request.origin)
      );
    return (
      i.command_ids.length === 1 &&
      i.command_ids[0] === r.request.request.command_id &&
      i.request_sha256 === r.request.request_sha256 &&
      r.request.request.expected_server_generation === i.expected_home.server_generation
    );
  }),
);
export type WorkstreamsForwardRequest = typeof WorkstreamsForwardRequest.Type;
const decodeForward = Schema.decodeUnknownSync(WorkstreamsForwardRequest);
/** @public Decodes retained exact wire bytes; digest verification belongs to the accepting store. */
export const decodeWorkstreamsForwardBody = (body: string): WorkstreamsForwardRequest =>
  decodeForward(JSON.parse(body));
export const WorkstreamsForwardRequestBody = retainedBody(decodeForward);

export const WorkstreamsForwardHomeProvenance = closed(
  Schema.Struct({
    registry: WorkstreamsNativeRegistryContext,
    principal_id: Id,
    authorization_revision: Version,
    source_binding_version: Version,
    incoming_peer_pin_id: Id,
    incoming_peer_pin_revision: Version,
  }),
);
export type WorkstreamsForwardHomeProvenance = typeof WorkstreamsForwardHomeProvenance.Type;
const resultFields = { ...common, home: WorkstreamsForwardHomeProvenance };
export const WorkstreamsForwardResult = Schema.Union([
  closed(
    Schema.Struct({ ...resultFields, kind: Schema.Literal("command"), result: WorkstreamReceipt }),
  ),
  closed(
    Schema.Struct({
      ...resultFields,
      kind: Schema.Literal("operation"),
      result: WorkstreamOperationResult,
    }),
  ),
  closed(
    Schema.Struct({
      ...resultFields,
      kind: Schema.Literal("appearance"),
      result: closed(WorkstreamAppearance),
    }),
  ),
]).check(
  Schema.makeFilter((r) => {
    const i = r.identity,
      h = r.home;
    if (
      canonical(i.expected_home) !==
        canonical({
          registry_instance_id: h.registry.registry_instance_id,
          owner_id: h.registry.owner_id,
          home_environment_id: h.registry.home_environment_id,
          server_generation: h.registry.server_generation,
        }) ||
      h.registry.canonical_origin !== i.canonical_origin ||
      h.principal_id !== i.principal_id ||
      h.source_binding_version !== i.source_binding_version
    )
      return false;
    if (r.kind === "command")
      return (
        i.command_ids.length === 1 &&
        r.result.command_id === i.command_ids[0] &&
        r.result.request_sha256 === i.request_sha256 &&
        r.result.owner_id === i.expected_home.owner_id &&
        r.result.actor.principal_id === i.principal_id &&
        r.result.server_generation === i.expected_home.server_generation
      );
    if (r.kind === "operation")
      return (
        r.result.operation_id === i.source_operation_id &&
        // Public v1 uses the canonical decoded operation digest. The accepting boundary
        // verifies it against the retained request, independently of the raw identity digest.
        r.result.children.every((c) => i.command_ids.includes(c.command_id))
      );
    return true;
  }),
);
export type WorkstreamsForwardResult = typeof WorkstreamsForwardResult.Type;
export const WorkstreamsForwardReadbackRequest = closed(Schema.Struct(common));
export type WorkstreamsForwardReadbackRequest = typeof WorkstreamsForwardReadbackRequest.Type;
export const WorkstreamsForwardReadbackResult = Schema.Union([
  closed(
    Schema.Struct({
      ...common,
      state: Schema.Literal("present"),
      result: WorkstreamsForwardResult,
    }),
  ).check(Schema.makeFilter((r) => canonical(r.identity) === canonical(r.result.identity))),
  closed(
    Schema.Struct({
      ...common,
      state: Schema.Literal("unknown"),
      reason: Schema.Literals([
        "not_found",
        "in_flight",
        "authority_unavailable",
        "home_unreachable",
        "generation_mismatch",
        "key_unavailable",
        "unsupported_protocol",
      ]),
    }),
  ),
]);
export type WorkstreamsForwardReadbackResult = typeof WorkstreamsForwardReadbackResult.Type;
