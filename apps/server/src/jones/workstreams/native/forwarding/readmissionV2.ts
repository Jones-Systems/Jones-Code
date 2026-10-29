import { SourceReadmission } from "./sourceReadmissionPort.ts";
export { SourceReadmission } from "./sourceReadmissionPort.ts";
// @effect-diagnostics nodeBuiltinImport:off -- Readmission binds exact retained RAW bytes, not decoded/canonical command JSON.
import * as NodeCrypto from "node:crypto";
import { EnvironmentId, WorkstreamCommand } from "@t3tools/contracts";
import * as Raw from "@t3tools/contracts/jones/workstreamsForwardReadmission";
import * as Parent from "@t3tools/contracts/jones/workstreamsForwardQualification";
import * as Forward from "@t3tools/contracts/jones/workstreamsForwarding";
import type { WorkstreamsTransportCeiling } from "@t3tools/contracts/jones/workstreamsPeerAuthority";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { McpInvocationScope } from "../../../../mcp/McpInvocationContext.ts";
import * as Links from "../../../../peer/PeerLinks.ts";
import { RegistryError } from "../registry/session.ts";
import * as Admission from "../runtime/memberAdmission.ts";
import * as Members from "../runtime/memberEnrollment.ts";
import type { MemberLocalWriteCallerV2 } from "../runtime/memberRecordedCaller.ts";
import * as Pins from "../runtime/peerSessionPins.ts";
import * as Operations from "../store/operationStore.ts";

type Denied = Extract<Parent.WorkstreamsSourceParentReadmissionResponseV3, { state: "denied" }>;
type Unavailable = Extract<
  Parent.WorkstreamsSourceParentReadmissionResponseV3,
  { state: "unavailable" }
>;
class LookupDenied extends Schema.TaggedError<LookupDenied>()("SourceReadmissionDenied", {
  reason: Schema.Literals([
    "not_found",
    "not_granted",
    "caller_changed",
    "source_changed",
    "home_changed",
    "request_conflict",
    "historical_evidence_missing",
  ]),
}) {}
class LookupUnavailable extends Schema.TaggedError<LookupUnavailable>()(
  "SourceReadmissionUnavailable",
  {
    reason: Schema.Literals([
      "authority_unavailable",
      "home_unreachable",
      "key_unavailable",
      "provider_unconfigured",
      "unsupported_protocol",
    ]),
  },
) {}

const sha = (bytes: string) => NodeCrypto.createHash("sha256").update(bytes, "utf8").digest("hex");
const same = (a: unknown, b: unknown) => canonical(a) === canonical(b);
const denied = (reason: Denied["reason"]) => new LookupDenied({ reason });
const unavailable = (reason: Unavailable["reason"] = "authority_unavailable") =>
  new LookupUnavailable({ reason });
const decodeParent = Schema.decodeUnknownEffect(Parent.WorkstreamsSourceParentReadmissionRequestV3);
const decodeRaw = Schema.decodeUnknownEffect(Raw.WorkstreamsSourceReadmissionRequestV2);
const decodeForward = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Forward.WorkstreamsForwardRequest),
);
const decodeCommand = Schema.decodeUnknownEffect(Schema.fromJsonString(WorkstreamCommand));
const decodeEnvironment = Schema.decodeUnknownEffect(EnvironmentId);
const decodeRawResponse = Schema.decodeUnknownEffect(Raw.WorkstreamsSourceReadmissionResponseV2);
const decodeParentResponse = Schema.decodeUnknownEffect(
  Parent.WorkstreamsSourceParentReadmissionResponseV3,
);
const ranks = { "approval-required": 0, "auto-accept-edits": 1, auto: 2, "full-access": 3 };
const lower = <A extends keyof typeof ranks>(a: A, b: A): A => (ranks[a] <= ranks[b] ? a : b);
const intersect = (
  a: WorkstreamsTransportCeiling,
  b: WorkstreamsTransportCeiling,
): WorkstreamsTransportCeiling => ({
  access:
    a.access === "read-only" || b.access === "read-only" ? "read-only" : lower(a.access, b.access),
  runtime_mode: lower(a.runtime_mode, b.runtime_mode),
  interaction_mode:
    a.interaction_mode === "plan" || b.interaction_mode === "plan" ? "plan" : "default",
  scopes: a.scopes.filter((s) => b.scopes.includes(s)),
  permissions: a.permissions.filter((p) => b.permissions.includes(p)),
});
const localCeiling = (
  local: MemberLocalWriteCallerV2,
  ceiling: MemberLocalWriteCallerV2["ceiling"],
  outbound: WorkstreamsTransportCeiling,
): WorkstreamsTransportCeiling => {
  const mode = ceiling.limits?.runtimeMode ?? "approval-required";
  return intersect(outbound, {
    access:
      local.auth.kind === "mcp_client" && local.auth.approved_access === "read-only"
        ? "read-only"
        : mode,
    runtime_mode: mode,
    interaction_mode: ceiling.limits?.interactionMode ?? "plan",
    // Agent authority is capability/permission based, not an HTTP session with scopes.
    // Only the corresponding orchestration scopes may cross that adapter boundary.
    scopes:
      local.auth.kind === "agent"
        ? outbound.scopes.filter(
            (s) =>
              ceiling.capabilities.includes("orchestration") &&
              ((s === "orchestration:read" && ceiling.permissions.includes("workstreams:read")) ||
                (s === "orchestration:operate" &&
                  ceiling.permissions.includes("workstreams:write"))),
          )
        : outbound.scopes.filter((scope) => ceiling.scopes.includes(scope)),
    permissions: ceiling.permissions,
  });
};
const storageFailure = (error: OperationsFailure) =>
  error._tag === "KeyUnavailable"
    ? unavailable("key_unavailable")
    : error._tag === "WorkstreamsStoreError" || error._tag === "EnvelopeError"
      ? denied("historical_evidence_missing")
      : unavailable();
type OperationsFailure = Effect.Error<
  ReturnType<Operations.PreparedOperations["readMemberForward"]>
>;
const pinFailure = (error: Pins.PeerSessionError) =>
  error.code === "unavailable" || error.code === "owner_unqualified"
    ? unavailable()
    : denied("home_changed");

// Recheck the encrypted store's associations at the producer boundary. Nothing from
// the home supplies caller JSON, target selection, command bytes or envelope bytes.
const validate = Effect.fn("SourceReadmission.validate")(function* (
  row: Operations.MemberForwardOperation,
  identity: Forward.WorkstreamsForwardIdentity,
) {
  const retained = row.member_forward;
  const forward = yield* decodeForward(retained.forward_request_body, {
    onExcessProperty: "error",
  }).pipe(Effect.mapError(() => denied("historical_evidence_missing")));
  const caller = retained.recorded_caller,
    a = caller.association,
    i = forward.identity;
  const body =
    forward.kind === "command"
      ? forward.request.body
      : forward.kind === "operation"
        ? forward.request_body
        : forward.request.request_body;
  const target = caller.local.target;
  if (!same(identity, i)) return yield* denied("request_conflict");
  if (
    sha(retained.forward_request_body) !== retained.forward_request_sha256 ||
    sha(body) !== i.request_sha256 ||
    sha(row.request_body) !== row.request_sha256 ||
    row.request_sha256 !== i.request_sha256 ||
    body !== row.request_body ||
    sha(canonical(caller)) !== i.recorded_caller_sha256 ||
    row.operation_id !== i.source_operation_id ||
    row.kind !== (forward.kind === "operation" ? forward.request.request.kind : forward.kind) ||
    !same(row.origin, forward.request.relay.origin) ||
    !same(row.origin, caller.local.origin) ||
    !same(row.child_command_ids, i.command_ids) ||
    !same(row.destination, {
      registry_instance_id: i.expected_home.registry_instance_id,
      owner_id: i.expected_home.owner_id,
      home_environment_id: i.expected_home.home_environment_id,
      home_generation: i.expected_home.server_generation,
      principal_id: i.principal_id,
      binding_version: i.singleton_binding_version,
    }) ||
    !same(a.home, i.expected_home) ||
    a.canonical_origin !== i.canonical_origin ||
    !same(a.source, i.source_authority) ||
    caller.local.environment_id !== a.source.environment_id ||
    a.enrollment_id !== i.source_enrollment_id ||
    a.enrollment_session_id !== i.source_enrollment_session_id ||
    a.principal_id !== i.principal_id ||
    a.singleton_binding_version !== i.singleton_binding_version ||
    a.source_binding_version !== i.source_binding_version ||
    a.member_pin_id !== i.member_pin_id ||
    a.pin_bound_revision !== i.member_pin_bound_revision ||
    a.link_generation !== i.outbound_link_generation ||
    caller.local.link_origin !== null ||
    (target.kind === "metadata"
      ? row.thread_id !== null || row.thread_environment_id !== null
      : target.environment_id !== a.source.environment_id ||
        row.thread_id !== target.thread_id ||
        row.thread_environment_id !== target.environment_id) ||
    (forward.kind === "operation" &&
      forward.request.request.kind === "set_primary" &&
      (target.kind !== "thread" ||
        !same(forward.request.request.thread, {
          environment_id: target.environment_id,
          thread_id: target.thread_id,
        })))
  )
    return yield* denied("historical_evidence_missing");
  return { forward, caller };
});
const checkAssociation = Effect.fn("SourceReadmission.checkAssociation")(function* (
  pin: Pins.QualifiedPeerPin,
  caller: Admission.MemberRecordedWriteCallerV2,
  request:
    | Parent.WorkstreamsSourceParentReadmissionRequestV3
    | Raw.WorkstreamsSourceReadmissionRequestV2,
) {
  const p = pin.pin,
    a = caller.association;
  if (
    p.state !== "bound" ||
    p.pin_id !== a.member_pin_id ||
    p.revision !== a.pin_bound_revision ||
    !same(p.receiver_authority, a.source) ||
    !same(p.source_authority, a.source) ||
    !same(p.expected_home, a.home) ||
    p.peer_environment_id !== a.home.home_environment_id ||
    p.source_enrollment_id !== a.enrollment_id ||
    p.source_binding_version !== a.source_binding_version ||
    p.singleton_binding_version !== a.singleton_binding_version ||
    p.bind_receipt?.principal_id !== a.principal_id ||
    p.bind_receipt?.binding_version !== a.source_binding_version ||
    !p.roles.includes("home_source")
  )
    return yield* denied("home_changed");
  if (
    request.contract === Parent.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3 &&
    (request.receiving_home_pin_id !== a.home_pin_id ||
      request.receiving_home_pin_revision !== a.home_pin_bound_revision)
  )
    return yield* denied("home_changed");
});

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const { withPrepared } = yield* Operations.OperationStore;
  const { forInvocation } = yield* Pins.PeerSessionPins;
  const { readmitWriteRetained: readmit } = yield* Admission.MemberAdmission;
  const { ownerCurrent: verifyOwner } = yield* Members.MemberEnrollments;
  const { currentGeneration } = yield* Links.PeerLinks;
  // Capture only the lookup/preparation and currentness methods. No mutation facade
  // or admitted-caller object is exposed by SourceReadmission.
  const read = (id: string) =>
    withPrepared([{ kind: "read", id }], (prepared) => prepared.readMemberForward(id)).pipe(
      Effect.mapError(storageFailure),
    );
  const ownerCurrent = verifyOwner.pipe(Effect.mapError(() => unavailable()));
  const ambient = Effect.gen(function* () {
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* new RegistryError({ code: "ambient_transaction" });
  });
  const observe = Effect.fn("SourceReadmission.observe")(function* (
    request:
      | Parent.WorkstreamsSourceParentReadmissionRequestV3
      | Raw.WorkstreamsSourceReadmissionRequestV2,
    invocation: McpInvocationScope,
  ) {
    const pin = yield* forInvocation(invocation, { role: "home_source" }).pipe(
      Effect.mapError(pinFailure),
    );
    const row = yield* read(request.accepted_forward.source_operation_id);
    if (!row) return yield* denied("not_found");
    const { forward, caller } = yield* validate(row, request.accepted_forward);
    yield* checkAssociation(pin, caller, request);
    if (request.contract === Raw.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V2) {
      if (forward.kind !== "command" || forward.request.evidence !== undefined)
        return yield* denied("request_conflict");
      const command = yield* decodeCommand(forward.request.body, {
        onExcessProperty: "error",
      }).pipe(Effect.mapError(() => denied("historical_evidence_missing")));
      if (
        command.command_id !== request.command_id ||
        sha(forward.request.body) !== request.command_request_sha256
      )
        return yield* denied("request_conflict");
    }
    const home = yield* decodeEnvironment(caller.association.home.home_environment_id).pipe(
      Effect.mapError(() => denied("historical_evidence_missing")),
    );
    const generationCurrent = Effect.gen(function* () {
      const generation = yield* currentGeneration(home).pipe(Effect.mapError(() => unavailable()));
      if (Option.isNone(generation) || generation.value !== caller.association.link_generation)
        return yield* denied("source_changed");
    });
    yield* ownerCurrent;
    yield* generationCurrent;
    const admitted = yield* readmit(caller).pipe(
      Effect.mapError((error) =>
        error.code === "unavailable" ? unavailable() : denied("caller_changed"),
      ),
    );
    if (!same(admitted.record, caller)) return yield* denied("caller_changed");
    const original = localCeiling(
      caller.local,
      caller.local.ceiling,
      caller.original_outbound_ceiling,
    );
    const current = intersect(
      original,
      intersect(
        localCeiling(caller.local, admitted.ceiling, caller.original_outbound_ceiling),
        pin.ceiling,
      ),
    );
    if (
      !Pins.ceilingAllows(original, "workstreams:write") ||
      !Pins.ceilingAllows(current, "workstreams:write")
    )
      return yield* denied("not_granted");
    // All external refresh/key/authority work stays outside the prepared store's
    // bounded writer. This observation is fresh here, not a distributed revocation lock.
    yield* ownerCurrent;
    const limits = yield* admitted
      .checkCurrent()
      .pipe(Effect.mapError(() => denied("caller_changed")));
    if (!same(limits, admitted.limits) || limits.interactionMode !== "default")
      return yield* denied("caller_changed");
    yield* generationCurrent;
    yield* pin.checkCurrent().pipe(Effect.mapError(pinFailure));
    return {
      original_ceiling: original,
      current_ceiling: current,
      forward_request_sha256: row.member_forward.forward_request_sha256,
      observed_at: DateTime.formatIso(yield* DateTime.now),
    };
  });
  const lookupParent: SourceReadmission["Service"]["lookupParent"] = Effect.fn(
    "SourceReadmission.lookupParent",
  )(function* (input, exactInvocation) {
    yield* ambient;
    const request = yield* decodeParent(input, { onExcessProperty: "error" }).pipe(
      Effect.mapError((cause) => new RegistryError({ code: "invalid_request", cause })),
    );
    return yield* observe(request, exactInvocation).pipe(
      Effect.flatMap((observation) =>
        decodeParentResponse({
          contract: request.contract,
          request,
          state: "authorized",
          ...observation,
        }).pipe(Effect.mapError((cause) => new RegistryError({ code: "invalid_response", cause }))),
      ),
      Effect.catchTags({
        SourceReadmissionDenied: ({ reason }) =>
          Effect.succeed({ contract: request.contract, request, state: "denied" as const, reason }),
        SourceReadmissionUnavailable: ({ reason }) =>
          Effect.succeed({
            contract: request.contract,
            request,
            state: "unavailable" as const,
            reason,
          }),
      }),
    );
  });
  const lookupRaw: SourceReadmission["Service"]["lookupRaw"] = Effect.fn(
    "SourceReadmission.lookupRaw",
  )(function* (input, exactInvocation) {
    yield* ambient;
    const request = yield* decodeRaw(input, { onExcessProperty: "error" }).pipe(
      Effect.mapError((cause) => new RegistryError({ code: "invalid_request", cause })),
    );
    return yield* observe(request, exactInvocation).pipe(
      Effect.flatMap(({ forward_request_sha256: _digest, ...observation }) =>
        decodeRawResponse({
          contract: request.contract,
          request,
          state: "authorized",
          ...observation,
        }).pipe(Effect.mapError((cause) => new RegistryError({ code: "invalid_response", cause }))),
      ),
      Effect.catchTags({
        SourceReadmissionDenied: ({ reason }) =>
          Effect.succeed({ contract: request.contract, request, state: "denied" as const, reason }),
        SourceReadmissionUnavailable: ({ reason }) =>
          Effect.succeed({
            contract: request.contract,
            request,
            state: "unavailable" as const,
            reason,
          }),
      }),
    );
  });
  return SourceReadmission.of({ lookupParent, lookupRaw });
});
/** Root supplies the application-owned authentication instance and mounts the fixed tool. */
export const layer = Layer.effect(SourceReadmission, make);
