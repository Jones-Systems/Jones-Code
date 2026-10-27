import {
  WorkstreamsNativeSettlementRequest,
  type WorkstreamsNativeSettlementResponseV2,
} from "@t3tools/contracts";
import { decodeWorkstreamsHomeBindBody } from "@t3tools/contracts/jones/workstreamsHomeBind";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import { deriveNativeEnrollmentBinding } from "../../enrollment/request.ts";
import { NativeProviderAttempts } from "../../nativeProvider/attemptRepository.ts";
import { NATIVE_PROVIDER_SCOPES } from "../../nativeProvider/enrollment.ts";
import { sha256Bytes } from "../../nativeProvider/service.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import * as Provider from "../providers/localAttestation.ts";
import * as Members from "../runtime/memberEnrollment.ts";
import * as Admission from "../runtime/memberAdmission.ts";
import * as Pins from "../runtime/peerSessionPins.ts";

/** Retained bytes must come from the original operation, never from a home receipt. */
export interface RetainedSourceLookup {
  readonly command_id: string;
  readonly command_request_sha256: string;
  readonly request_body: string;
  readonly request_sha256: string;
  readonly caller: Admission.MemberRecordedCallerV1;
}
export class MemberSourceReadAdmission extends Context.Service<
  MemberSourceReadAdmission,
  {
    readonly lookup: (
      input: RetainedSourceLookup,
    ) => Effect.Effect<WorkstreamsNativeSettlementResponseV2, Provider.ProviderError>;
  }
>()("t3/jones/workstreams/native/forwarding/memberSourceReadAdmission") {}
const refused = () => new Provider.ProviderError({ code: "not_granted" });
const decode = Schema.decodeUnknownEffect(
  Schema.fromJsonString(WorkstreamsNativeSettlementRequest),
);
const make = Effect.gen(function* () {
  const members = yield* Members.MemberEnrollments;
  const callers = yield* Admission.MemberAdmission;
  const pins = yield* Pins.PeerSessionPins;
  const attempts = yield* NativeProviderAttempts;
  const provider = yield* Provider.NativeProvider;
  return MemberSourceReadAdmission.of({
    lookup: Effect.fn("MemberSourceReadAdmission.lookup")(function* (input) {
      const invocation = yield* Effect.serviceOption(Mcp.McpInvocationContext);
      if (
        Option.isNone(invocation) ||
        !invocation.value.client?.linked ||
        invocation.value.thread ||
        invocation.value.client.remoteOrigin
      )
        return yield* refused();
      const admitted = yield* callers.readmitRetained(input.caller).pipe(Effect.mapError(refused));
      const a = admitted.record.association;
      const request = yield* decode(input.request_body, { onExcessProperty: "error" }).pipe(
        Effect.mapError(refused),
      );
      if (
        request.command_id !== input.command_id ||
        request.request_sha256 !== input.command_request_sha256 ||
        !/^[a-f0-9]{64}$/.test(input.request_sha256) ||
        sha256Bytes(input.request_body) !== input.request_sha256 ||
        request.owner_id !== a.home.owner_id ||
        request.principal_id !== a.principal_id ||
        request.identity.source_instance_id !== a.source.environment_id ||
        request.expected_authority_namespace !== a.source.authority_namespace ||
        request.expected_store_generation !== a.source.store_generation ||
        admitted.record.local.auth.kind !== "agent" ||
        request.identity.native_id !== admitted.record.local.auth.calling_thread_id
      )
        return yield* refused();
      const pin = yield* pins
        .forInvocation(invocation.value, { role: "home_source" })
        .pipe(Effect.mapError(refused));
      const bound = yield* members
        .getValidatedByEnrollmentId(a.enrollment_id)
        .pipe(Effect.mapError(refused));
      if (Option.isNone(bound)) return yield* refused();
      const b = decodeWorkstreamsHomeBindBody(bound.value.intent.bind_request_body);
      if (
        pin.pin.state !== "bound" ||
        canonicalJson(pin.pin.expected_home) !== canonicalJson(a.home) ||
        pin.pin.pin_id !== b.member_setup.pin_id ||
        pin.pin.revision !== a.pin_bound_revision ||
        pin.pin.source_enrollment_id !== a.enrollment_id ||
        pin.pin.source_binding_version !== a.source_binding_version ||
        pin.pin.singleton_binding_version !== a.singleton_binding_version ||
        canonicalJson(pin.pin.bind_receipt) !== canonicalJson(bound.value.home_result.receipt) ||
        !Pins.ceilingAllows(pin.ceiling, "workstreams:read")
      )
        return yield* refused();
      const binding = deriveNativeEnrollmentBinding(b.source.enrollment);
      if (!binding.scopes.includes(NATIVE_PROVIDER_SCOPES.reconciliation)) return yield* refused();
      const prior = yield* attempts
        .get(request)
        .pipe(Effect.mapError(() => new Provider.ProviderError({ code: "provider_unavailable" })));
      if (
        Option.isSome(prior) &&
        (prior.value.requestBytesSha256 !== input.request_sha256 ||
          canonicalJson(prior.value.request) !== canonicalJson(request) ||
          canonicalJson(prior.value.enrollment) !== canonicalJson(binding))
      )
        return yield* refused();
      yield* pin.checkCurrent().pipe(Effect.mapError(refused));
      yield* admitted.checkCurrent().pipe(Effect.mapError(refused));
      // NativeProvider.lookup never reserves an attempt or dispatches a native command.
      const result = yield* provider.lookup(binding, request, input.request_sha256);
      yield* pin.checkCurrent().pipe(Effect.mapError(refused));
      yield* admitted.checkCurrent().pipe(Effect.mapError(refused));
      return result;
    }),
  });
});
/** @public Mount separately from SourceAdmission: its old interface cannot bind command bytes. */
export const layer = Layer.effect(MemberSourceReadAdmission, make);
