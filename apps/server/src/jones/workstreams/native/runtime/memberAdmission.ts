import { decodeWorkstreamsHomeBindBody } from "@t3tools/contracts/jones/workstreamsHomeBind";
import type { WorkstreamsMemberEnrollmentBoundV1 } from "@t3tools/contracts/jones/workstreamsMemberEnrollment";
import {
  WorkstreamsNativeHomeTarget,
  WorkstreamsNativeOrigin,
  WorkstreamsNativeVersion,
  WorkstreamsNativeDigest,
} from "@t3tools/contracts/jones/workstreamsNative";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { PeerCallLimits } from "../../../../peer/PeerMcpClient.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import { RegistryError } from "../registry/session.ts";
import * as Members from "./memberEnrollment.ts";
import * as Local from "./memberRecordedCaller.ts";
import * as Startup from "./startup.ts";

const Id = Schema.NonEmptyString;
const Association = Schema.Struct({
  enrollment_id: Id,
  enrollment_session_id: Id,
  intent_sha256: WorkstreamsNativeDigest,
  home: WorkstreamsNativeHomeTarget,
  canonical_origin: WorkstreamsNativeOrigin,
  principal_id: Id,
  singleton_binding_version: WorkstreamsNativeVersion,
  pin_bound_revision: WorkstreamsNativeVersion,
  source_binding_version: WorkstreamsNativeVersion,
  source: Schema.Struct({
    environment_id: Id,
    authority_namespace: Id,
    store_generation: WorkstreamsNativeVersion,
  }),
  link_generation: Id,
});
/** Retained local evidence and historical association, never a current home grant. */
export const MemberRecordedCallerV1 = Schema.Struct({
  schema: Schema.Literal("jones-workstreams-member-recorded-caller/1"),
  local: Local.MemberLocalCallerV1,
  association: Association,
});
export type MemberRecordedCallerV1 = typeof MemberRecordedCallerV1.Type;
const decode = Schema.decodeUnknownEffect(MemberRecordedCallerV1, { onExcessProperty: "error" });
const associationOf = (record: WorkstreamsMemberEnrollmentBoundV1): typeof Association.Type => {
  const b = decodeWorkstreamsHomeBindBody(record.intent.bind_request_body);
  return {
    enrollment_id: b.source.enrollment.enrollment_id,
    enrollment_session_id: b.source.enrollment.session.session_id,
    intent_sha256: record.intent_sha256,
    home: b.expected_home,
    canonical_origin: b.expected_canonical_origin,
    principal_id: record.home_result.receipt.principal_id,
    singleton_binding_version: record.local_binding.singleton_binding_version,
    pin_bound_revision: record.local_binding.pin_bound_revision,
    source_binding_version: record.home_result.receipt.binding_version,
    source: b.source.authority,
    link_generation: b.member_setup.outbound_link_generation,
  };
};
export interface AdmittedMember {
  readonly record: MemberRecordedCallerV1;
  readonly limits: PeerCallLimits;
  readonly checkCurrent: () => Effect.Effect<PeerCallLimits, RegistryError>;
}
export class MemberAdmission extends Context.Service<
  MemberAdmission,
  {
    readonly captureOwnPlacement: () => Effect.Effect<MemberRecordedCallerV1, RegistryError>;
    readonly readmitOwnPlacement: (record: unknown) => Effect.Effect<AdmittedMember, RegistryError>;
    readonly capture: () => Effect.Effect<MemberRecordedCallerV1, RegistryError>;
    readonly readmitRetained: (record: unknown) => Effect.Effect<AdmittedMember, RegistryError>;
    readonly readmit: (record: unknown) => Effect.Effect<AdmittedMember, RegistryError>;
    readonly current: () => Effect.Effect<AdmittedMember, RegistryError>;
  }
>()("t3/jones/workstreams/native/runtime/memberAdmission") {}
/** The application provides a retained record only for that operation's request. */
export class RetainedMemberCaller extends Context.Service<
  RetainedMemberCaller,
  MemberRecordedCallerV1
>()("t3/jones/workstreams/native/runtime/memberAdmission/RetainedMemberCaller") {}
const refused = () => new RegistryError({ code: "forbidden" });
const make = Effect.gen(function* () {
  const members = yield* Members.MemberEnrollments;
  const local = yield* Local.MemberLocalCallers;
  const selection = yield* Startup.Startup;
  const selected = Effect.fn("MemberAdmission.selected")(function* () {
    if (
      selection.mode !== "native-member" ||
      selection.problem !== null ||
      !selection.sourceSessionId
    )
      return yield* refused();
    const bound = yield* members
      .getValidatedBySessionId(selection.sourceSessionId)
      .pipe(Effect.mapError(refused));
    if (Option.isNone(bound)) return yield* refused();
    return bound.value;
  });
  const readmit = Effect.fn("MemberAdmission.readmit")(function* (
    input: unknown,
    retained = false,
    ownPlacementOnly = false,
  ) {
    const record = yield* decode(input).pipe(
      Effect.mapError((cause) => new RegistryError({ code: "invalid_request", cause })),
    );
    const checkCurrent = Effect.fn("MemberAdmission.checkCurrent")(function* () {
      const bound = yield* selected();
      const current = yield* members
        .getValidatedByEnrollmentId(record.association.enrollment_id)
        .pipe(Effect.mapError(refused));
      if (
        Option.isNone(current) ||
        canonicalJson(associationOf(bound)) !== canonicalJson(record.association) ||
        canonicalJson(associationOf(current.value)) !== canonicalJson(record.association) ||
        record.local.environment_id !== record.association.source.environment_id
      )
        return yield* refused();
      const caller = yield* (
        retained
          ? local.readmitRetained(record.local)
          : local.readmit(record.local, ownPlacementOnly)
      ).pipe(Effect.mapError(refused));
      if (ownPlacementOnly && record.local.auth.kind !== "agent") return yield* refused();
      yield* caller
        .checkTarget(
          ownPlacementOnly && record.local.auth.kind === "agent"
            ? record.local.auth.calling_thread_id
            : null,
        )
        .pipe(Effect.mapError(refused));
      const outbound = bound.intent.home_setup.pin.session.ceiling;
      if (
        !outbound.permissions.includes("workstreams:read") ||
        !outbound.scopes.includes("orchestration:read")
      )
        return yield* refused();
      const effective = Local.intersectMemberCeilings(caller.ceiling, {
        ...caller.ceiling,
        limits: { runtimeMode: outbound.runtime_mode, interactionMode: outbound.interaction_mode },
      }).limits;
      if (!effective) return yield* refused();
      return effective;
    });
    const limits = yield* checkCurrent();
    return { record, limits, checkCurrent };
  });
  const capture = Effect.fn("MemberAdmission.capture")(function* (ownPlacementOnly = false) {
    const bound = yield* selected();
    const association = associationOf(bound);
    const caller = yield* (
      ownPlacementOnly
        ? local.captureOwnPlacement(association.source.environment_id)
        : local.capture(association.source.environment_id)
    ).pipe(Effect.mapError(refused));
    const record: MemberRecordedCallerV1 = {
      schema: "jones-workstreams-member-recorded-caller/1",
      local: caller,
      association,
    };
    yield* readmit(record, false, ownPlacementOnly);
    return record;
  });
  return MemberAdmission.of({
    capture,
    readmit,
    captureOwnPlacement: () => capture(true),
    readmitOwnPlacement: (record) => readmit(record, false, true),
    readmitRetained: (record) => readmit(record, true),
    current: Effect.fn("MemberAdmission.current")(function* () {
      const retained = yield* Effect.serviceOption(RetainedMemberCaller);
      return yield* readmit(Option.isSome(retained) ? retained.value : yield* capture());
    }),
  });
});
/** @public Build in the request environment; selected member startup remains root-owned. */
export const layer = Layer.effect(MemberAdmission, make);
