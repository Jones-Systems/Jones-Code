// @effect-diagnostics nodeBuiltinImport:off -- Exact retained wire bytes bind forwarding and public operation digests.
import * as NodeCrypto from "node:crypto";
import { EnvironmentId, WorkstreamCommand } from "@t3tools/contracts";
import * as Wire from "@t3tools/contracts/jones/workstreamsForwarding";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Peer from "../../../../peer/PeerMcpClient.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import { RegistryError } from "../registry/session.ts";
import type { AdmittedMemberWrite } from "../runtime/memberAdmission.ts";

const sha = (body: string) => NodeCrypto.createHash("sha256").update(body).digest("hex");
const same = (a: unknown, b: unknown) => canonicalJson(a) === canonicalJson(b);
const invalid = () => new RegistryError({ code: "invalid_response" });
const decodeEnvironment = Schema.decodeUnknownEffect(EnvironmentId);
const decodeCommand = Schema.decodeUnknownEffect(Schema.fromJsonString(WorkstreamCommand));
const decodeRequest = Schema.decodeUnknownEffect(Wire.WorkstreamsForwardRequest);
const decodeResult = Schema.decodeUnknownEffect(Wire.WorkstreamsForwardResult);
const decodeReadback = Schema.decodeUnknownEffect(Wire.WorkstreamsForwardReadbackResult);

/** Checks both RAW forwarding identity and the unchanged canonical public operation digest. */
export const validateForwardResult = Effect.fn("HomeForwardClient.validateResult")(function* (
  request: Wire.WorkstreamsForwardRequest,
  input: unknown,
  admitted: AdmittedMemberWrite,
) {
  const body =
    request.kind === "command"
      ? request.request.body
      : request.kind === "operation"
        ? request.request_body
        : request.request.request_body;
  if (sha(body) !== request.identity.request_sha256) return yield* invalid();
  const result = yield* decodeResult(input, { onExcessProperty: "error" }).pipe(
    Effect.mapError(invalid),
  );
  const a = admitted.record.association;
  if (
    !same(result.identity, request.identity) ||
    result.kind !== request.kind ||
    result.home.incoming_peer_pin_id !== a.home_pin_id ||
    result.home.incoming_peer_pin_revision !== a.home_pin_bound_revision
  )
    return yield* invalid();
  if (request.kind === "command" && result.kind === "command") {
    const command = yield* decodeCommand(request.request.body).pipe(Effect.mapError(invalid));
    if (result.result.operation !== command.action.operation) return yield* invalid();
  } else if (request.kind === "operation" && result.kind === "operation") {
    if (
      result.result.request_sha256 !== sha(canonicalJson(request.request.request)) ||
      result.result.kind !== request.request.request.kind ||
      !same(result.result.origin, request.request.request.origin)
    )
      return yield* invalid();
    if (request.request.request.kind === "set_primary") {
      const target = request.request.request.target;
      if (
        result.result.state === "committed" &&
        result.result.result.workstream_id !==
          (target.kind === "workstream" ? target.workstream_id : null)
      )
        return yield* invalid();
      if (
        result.result.state === "already_satisfied" &&
        !same(
          result.result.observed,
          target.kind === "workstream"
            ? { ...result.result.observed, kind: "primary", workstream_id: target.workstream_id }
            : { kind: "unassigned" },
        )
      )
        return yield* invalid();
    }
  } else if (request.kind === "appearance" && result.kind === "appearance") {
    const expected = request.request.request;
    if (
      result.result.workstream_id !== expected.workstream_id ||
      result.result.border_color !== expected.border_color ||
      result.result.version !== expected.expected_version + 1
    )
      return yield* invalid();
  }
  return result;
});

export class HomeForwardClient extends Context.Service<
  HomeForwardClient,
  {
    readonly preflight: (
      request: Wire.WorkstreamsForwardRequest,
      caller: AdmittedMemberWrite,
    ) => Effect.Effect<void, RegistryError>;
    readonly forward: (
      request: Wire.WorkstreamsForwardRequest,
      caller: AdmittedMemberWrite,
    ) => Effect.Effect<Wire.WorkstreamsForwardResult, RegistryError>;
    readonly readback: (
      request: Wire.WorkstreamsForwardRequest,
      caller: AdmittedMemberWrite,
    ) => Effect.Effect<Wire.WorkstreamsForwardReadbackResult, RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/homeClient/HomeForwardClient") {}

const make = Effect.gen(function* () {
  const peer = yield* Peer.PeerMcpClient;
  const check = Effect.fn("HomeForwardClient.check")(function* (
    request: Wire.WorkstreamsForwardRequest,
    admitted: AdmittedMemberWrite,
  ) {
    const a = admitted.record.association,
      i = request.identity;
    if (
      !same(a.home, i.expected_home) ||
      a.canonical_origin !== i.canonical_origin ||
      a.principal_id !== i.principal_id ||
      !same(a.source, i.source_authority) ||
      a.enrollment_id !== i.source_enrollment_id ||
      a.enrollment_session_id !== i.source_enrollment_session_id ||
      a.singleton_binding_version !== i.singleton_binding_version ||
      a.source_binding_version !== i.source_binding_version ||
      a.member_pin_id !== i.member_pin_id ||
      a.pin_bound_revision !== i.member_pin_bound_revision ||
      a.link_generation !== i.outbound_link_generation ||
      sha(canonicalJson(admitted.record)) !== i.recorded_caller_sha256 ||
      admitted.record.local.link_origin !== null ||
      !same(yield* admitted.checkCurrent(), admitted.limits) ||
      admitted.limits.interactionMode !== "default"
    )
      return yield* new RegistryError({ code: "forbidden" });
    return yield* decodeEnvironment(a.home.home_environment_id).pipe(
      Effect.mapError(() => new RegistryError({ code: "invalid_home_environment" })),
    );
  });
  const readback = Effect.fn("HomeForwardClient.readback")(function* (
    input: Wire.WorkstreamsForwardRequest,
    admitted: AdmittedMemberWrite,
  ) {
    const request = yield* decodeRequest(input, {
      onExcessProperty: "error",
    }).pipe(Effect.mapError(invalid));
    const environmentId = yield* check(request, admitted);
    const raw = yield* peer
      .call({
        environmentId,
        expectedGeneration: request.identity.outbound_link_generation,
        tool: "jones_workstreams_home_forward_readback",
        arguments: {
          contract: Wire.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
          identity: request.identity,
        },
        limits: admitted.limits,
        success: Schema.Unknown,
      })
      .pipe(Effect.mapError(() => new RegistryError({ code: "unavailable" })));
    yield* check(request, admitted);
    const result = yield* decodeReadback(raw, { onExcessProperty: "error" }).pipe(
      Effect.mapError(invalid),
    );
    if (!same(result.identity, request.identity)) return yield* invalid();
    if (result.state === "present") yield* validateForwardResult(request, result.result, admitted);
    return result;
  });
  return HomeForwardClient.of({
    readback,
    preflight: Effect.fn("HomeForwardClient.preflight")(function* (request, admitted) {
      const result = yield* readback(request, admitted);
      if (result.state === "present" || result.reason === "in_flight")
        return yield* new RegistryError({ code: "operation_id_conflict" });
      if (result.reason !== "not_found") return yield* new RegistryError({ code: "unavailable" });
    }),
    forward: Effect.fn("HomeForwardClient.forward")(function* (input, admitted) {
      const request = yield* decodeRequest(input, {
        onExcessProperty: "error",
      }).pipe(Effect.mapError(invalid));
      const environmentId = yield* check(request, admitted);
      const raw = yield* peer
        .call({
          environmentId,
          expectedGeneration: request.identity.outbound_link_generation,
          tool: "jones_workstreams_home_forward",
          arguments: request,
          limits: admitted.limits,
          success: Schema.Unknown,
        })
        .pipe(Effect.mapError(() => new RegistryError({ code: "unavailable" })));
      yield* check(request, admitted);
      return yield* validateForwardResult(request, raw, admitted);
    }),
  });
});
/** Fixed invocation negotiation only; no tools/list, generic routing, URL, origin escape or retry. */
export const layer = Layer.effect(HomeForwardClient, make);
