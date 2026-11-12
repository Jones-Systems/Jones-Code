// @effect-diagnostics nodeBuiltinImport:off -- Persisted recovery binds exact original UTF-8 command bytes.
import * as NodeCrypto from "node:crypto";
import { WorkstreamCommand, WorkstreamOwnerGrant } from "@t3tools/contracts";
import { WorkstreamsForwardIdentity } from "@t3tools/contracts/jones/workstreamsForwarding";
import { WorkstreamsPeerEffectRecoveryV2 } from "@t3tools/contracts/jones/workstreamsForwardReadmission";
import {
  WorkstreamsNativeDigest,
  WorkstreamsNativeId,
  closedWorkstreamsNative,
} from "@t3tools/contracts/jones/workstreamsNative";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export class PeerEffectEnvelopeError extends Schema.TaggedError<PeerEffectEnvelopeError>()(
  "PeerEffectEnvelopeError",
  {},
) {}

const body = Schema.String.check(
  Schema.makeFilter((value) => Buffer.byteLength(value, "utf8") <= 1_048_576),
);
/** Stored data only. Decoding this envelope establishes no current authority or receiving-session proof. */
export const StoredPeerEffectV2 = closedWorkstreamsNative(
  Schema.Struct({
    format: Schema.Literal("jones-native-effect-recovery/2"),
    kind: Schema.Literal("PEER"),
    intent_body: body,
    request_body: body,
    request_sha256: WorkstreamsNativeDigest,
    recovery: WorkstreamsPeerEffectRecoveryV2,
    forward_request_sha256: WorkstreamsNativeDigest,
    source_link_generation: WorkstreamsForwardIdentity.fields.outbound_link_generation,
  }),
);
export type StoredPeerEffectV2 = typeof StoredPeerEffectV2.Type;
const decodeEnvelope = Schema.decodeUnknownEffect(StoredPeerEffectV2, {
  onExcessProperty: "error",
});
const decodeCommand = Schema.decodeUnknownEffect(Schema.fromJsonString(WorkstreamCommand), {
  onExcessProperty: "error",
});
const actor = Schema.Struct({ principal_id: WorkstreamsNativeId });
// This is a validation view of the full retained domain intent. Its exact bytes remain untouched.
const decodeIntentAssociation = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      kind: Schema.Literals(["verify_reference", "native_t3_settlement", "refresh_linked_pr"]),
      command: WorkstreamCommand,
      owner_id: WorkstreamsNativeId,
      actor,
      request_sha256: WorkstreamsNativeDigest,
      authorization: Schema.Struct({
        owner_id: WorkstreamsNativeId,
        actor,
        server_generation: Schema.Int,
        grant: WorkstreamOwnerGrant,
      }),
      reference: Schema.Struct({
        native_reference_id: WorkstreamsNativeId,
        owner_id: WorkstreamsNativeId,
      }),
      membership: Schema.optionalKey(
        Schema.NullOr(
          Schema.Struct({
            membership_id: WorkstreamsNativeId,
            workstream_id: WorkstreamsNativeId,
            native_reference_id: WorkstreamsNativeId,
          }),
        ),
      ),
    }),
  ),
);
const hash = (value: string) => NodeCrypto.createHash("sha256").update(value, "utf8").digest("hex");
const invalid = () => new PeerEffectEnvelopeError();

export const decodeStoredPeerEffect = Effect.fn("PeerEffectEnvelope.decode")(function* (
  input: unknown,
) {
  const value = yield* decodeEnvelope(input).pipe(Effect.mapError(invalid));
  const command = yield* decodeCommand(value.request_body).pipe(Effect.mapError(invalid));
  const intent = yield* decodeIntentAssociation(value.intent_body).pipe(Effect.mapError(invalid));
  const r = value.recovery.source_readmission;
  const i = r.accepted_forward;
  const action = command.action;
  if (
    action.operation !== "verify_reference" &&
    action.operation !== "request_native_t3_settlement" &&
    action.operation !== "refresh_linked_pr"
  )
    return yield* invalid();
  const kind =
    action.operation === "request_native_t3_settlement" ? "native_t3_settlement" : action.operation;
  if (
    hash(value.request_body) !== value.request_sha256 ||
    r.command_request_sha256 !== value.request_sha256 ||
    i.request_sha256 !== value.request_sha256 ||
    r.command_id !== command.command_id ||
    i.command_ids.length !== 1 ||
    i.command_ids[0] !== command.command_id ||
    command.expected_server_generation !== i.expected_home.server_generation ||
    canonical(intent.command) !== canonical(command) ||
    intent.kind !== kind ||
    intent.request_sha256 !== value.request_sha256 ||
    intent.owner_id !== i.expected_home.owner_id ||
    intent.actor.principal_id !== i.principal_id ||
    intent.authorization.owner_id !== i.expected_home.owner_id ||
    intent.authorization.actor.principal_id !== i.principal_id ||
    intent.authorization.server_generation !== i.expected_home.server_generation ||
    intent.authorization.grant.owner_id !== i.expected_home.owner_id ||
    intent.authorization.grant.principal_id !== i.principal_id ||
    intent.reference.owner_id !== i.expected_home.owner_id ||
    (action.operation === "refresh_linked_pr"
      ? !intent.membership ||
        intent.membership.membership_id !== action.membership_id ||
        intent.membership.workstream_id !== action.workstream_id ||
        intent.membership.native_reference_id !== intent.reference.native_reference_id
      : intent.reference.native_reference_id !== action.native_reference_id)
  )
    return yield* invalid();
  return value;
});

export const wrapPeerEffectIntent = (
  intentBody: string,
  requestBody: string,
  requestSha256: string,
  recovery: WorkstreamsPeerEffectRecoveryV2,
  forwardRequestSha256: string,
  sourceLinkGeneration: string,
) =>
  decodeStoredPeerEffect({
    format: "jones-native-effect-recovery/2",
    kind: "PEER",
    intent_body: intentBody,
    request_body: requestBody,
    request_sha256: requestSha256,
    recovery,
    forward_request_sha256: forwardRequestSha256,
    source_link_generation: sourceLinkGeneration,
  });
