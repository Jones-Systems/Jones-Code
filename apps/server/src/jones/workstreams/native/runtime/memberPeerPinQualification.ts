import { EnvironmentId } from "@t3tools/contracts";
import * as Wire from "@t3tools/contracts/jones/workstreamsHomeBind";
import { WorkstreamsMemberEnrollmentIntentV1 as Intent } from "@t3tools/contracts/jones/workstreamsMemberEnrollment";
import type { WorkstreamsPeerSessionPinV1 as Pin } from "@t3tools/contracts/jones/workstreamsPeerAuthority";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Links from "../../../../peer/PeerLinks.ts";
import * as Client from "../../../../peer/PeerMcpClient.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import * as Owner from "./sourceEnrollment.ts";
import * as Setup from "./peerSessionPinSetup.ts";
import * as Pins from "./peerSessionPins.ts";
import { decodeSetupRequest } from "../store/peerSessionPinStore.ts";
import { validateMemberEnrollment } from "../store/memberEnrollmentStore.ts";
import { canonicalWorkstreamsMemberEnrollmentIntent } from "@t3tools/contracts/jones/workstreamsMemberEnrollment";
import { sha256EnrollmentBytes as sha } from "../../enrollment/request.ts";

export class MemberQualificationError extends Schema.TaggedError<MemberQualificationError>()(
  "MemberQualificationError",
  {
    code: Schema.Literals([
      "invalid_request",
      "owner_unqualified",
      "unauthenticated",
      "not_current",
      "link_changed",
      "home_mismatch",
      "conflict",
      "remote_unknown",
      "unavailable",
    ]),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {
  override get message() {
    return `Native member qualification refused: ${this.code}.`;
  }
}
declare const setupBrand: unique symbol;
declare const homeBrand: unique symbol;
export interface QualifiedMemberSetup {
  readonly [setupBrand]: true;
}
export interface VerifiedMemberHomeBind {
  readonly [homeBrand]: true;
}
export class MemberPeerPinQualification extends Context.Service<
  MemberPeerPinQualification,
  {
    readonly qualify: (
      intent: Intent,
    ) => Effect.Effect<QualifiedMemberSetup, MemberQualificationError>;
    readonly checkCurrent: (
      setup: QualifiedMemberSetup,
    ) => Effect.Effect<void, MemberQualificationError>;
    readonly verifyHomeSetup: (
      setup: QualifiedMemberSetup,
    ) => Effect.Effect<void, MemberQualificationError>;
    readonly readback: (
      setup: QualifiedMemberSetup,
    ) => Effect.Effect<VerifiedMemberHomeBind, MemberQualificationError>;
    readonly result: (
      setup: QualifiedMemberSetup,
      home: VerifiedMemberHomeBind,
    ) => Effect.Effect<Wire.WorkstreamsHomeBindCommittedV2, MemberQualificationError>;
    readonly verifyBinding: (
      setup: QualifiedMemberSetup,
      home: VerifiedMemberHomeBind,
      pin: Pin,
      receipt: NonNullable<Pin["bind_receipt"]>,
      singletonVersion: number,
    ) => Effect.Effect<void, MemberQualificationError>;
  }
>()("t3/jones/workstreams/native/runtime/memberPeerPinQualification") {}

const isMemberQualificationError = Schema.is(MemberQualificationError);
const failure = (cause: unknown) =>
  isMemberQualificationError(cause)
    ? cause
    : new MemberQualificationError({ code: "unavailable", cause });
const refuse = (code: MemberQualificationError["code"]) => new MemberQualificationError({ code });
const same = (a: unknown, b: unknown) => canonical(a) === canonical(b);
const decodeIntentBytes = Schema.decodeUnknownSync(Schema.fromJsonString(Intent));
const decodeHomeBytes = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Wire.WorkstreamsHomeBindCommittedV2),
);
const make = Effect.gen(function* () {
  const links = yield* Links.PeerLinks,
    client = yield* Client.PeerMcpClient,
    native = yield* Pins.NativePeerSessions,
    authority = yield* Authority.NativeStoreAuthority;
  const owner = yield* Effect.serviceOption(Owner.SourceSetupOwner),
    qualification = yield* Effect.serviceOption(Setup.PeerPinSetupQualification);
  type SetupFacts = {
    readonly intent: Intent;
    readonly pin: Pin;
    readonly check: () => Effect.Effect<void, MemberQualificationError>;
    readonly limits: Client.PeerCallLimits;
  };
  const setups = new WeakMap<QualifiedMemberSetup, SetupFacts>();
  const homes = new WeakMap<
    VerifiedMemberHomeBind,
    { readonly setup: QualifiedMemberSetup; readonly bytes: string }
  >();
  const ownerCurrent = Effect.fn("MemberPeerPinQualification.owner")(function* () {
    if (Option.isNone(owner) || Option.isNone(qualification))
      return yield* refuse("owner_unqualified");
    yield* owner.value.verifyUnchanged.pipe(Effect.mapError(() => refuse("owner_unqualified")));
    return qualification.value;
  });
  const facts = (token: QualifiedMemberSetup) =>
    Effect.fromNullishOr(setups.get(token)).pipe(Effect.mapError(() => refuse("unauthenticated")));
  const qualify = Effect.fn("MemberPeerPinQualification.qualify")(function* (input: Intent) {
    const intent = yield* Effect.try({
      try: () => {
        const r = validateMemberEnrollment({
          schema: "jones-workstreams-member-enrollment/1",
          intent: input,
          intent_sha256: sha(canonicalWorkstreamsMemberEnrollmentIntent(input)),
          state: "prepared",
          revision: 1,
          home_result: null,
          local_binding: null,
        });
        return decodeIntentBytes(canonical(r.intent));
      },
      catch: () => refuse("invalid_request"),
    });
    const b = Wire.decodeWorkstreamsHomeBindBody(intent.bind_request_body);
    const pin = decodeSetupRequest(intent.member_setup_request_body).pin;
    const port = yield* ownerCurrent();
    const q = yield* port
      .qualify(intent.member_setup_request_body, intent.member_setup_request_sha256)
      .pipe(Effect.mapError(failure));
    const check = Effect.fn("MemberPeerPinQualification.localCurrent")(function* () {
      yield* ownerCurrent();
      yield* q.checkCurrent().pipe(Effect.mapError(failure));
      const authenticated = yield* native
        .current(q.invocation)
        .pipe(Effect.mapError(() => refuse("unauthenticated")));
      const current = yield* authority.readCurrent.pipe(Effect.mapError(failure));
      const generation = yield* links
        .currentGeneration(EnvironmentId.make(b.expected_home.home_environment_id))
        .pipe(Effect.mapError(failure));
      const now = DateTime.toEpochMillis(yield* DateTime.now);
      if (Option.isNone(generation) || generation.value !== b.member_setup.outbound_link_generation)
        return yield* refuse("link_changed");
      if (
        !same(authenticated.session, pin.session) ||
        q.owner_qualification_digest !== pin.owner_qualification_digest ||
        pin.roles.length !== 1 ||
        pin.roles[0] !== "home_source" ||
        Date.parse(pin.expires_at) <= now ||
        q.invocation.environmentId !== current.environmentId ||
        current.environmentId !== b.source.authority.environment_id ||
        current.authorityNamespace !== b.source.authority.authority_namespace ||
        current.storeGeneration !== b.source.authority.store_generation
      )
        return yield* refuse("not_current");
    });
    yield* check();
    const token = Object.freeze({}) as QualifiedMemberSetup;
    setups.set(token, {
      intent,
      pin,
      check,
      limits: {
        runtimeMode: pin.session.ceiling.runtime_mode,
        interactionMode: pin.session.ceiling.interaction_mode,
      },
    });
    return token;
  });
  const checkCurrent = Effect.fn("MemberPeerPinQualification.checkCurrent")(function* (
    token: QualifiedMemberSetup,
  ) {
    yield* (yield* facts(token)).check();
  });
  const verifyHomeSetup = Effect.fn("MemberPeerPinQualification.verifyHomeSetup")(function* (
    token: QualifiedMemberSetup,
  ) {
    const f = yield* facts(token),
      b = Wire.decodeWorkstreamsHomeBindBody(f.intent.bind_request_body);
    yield* f.check();
    const request: Wire.WorkstreamsHomeBindSetupReadbackRequestV2 = {
      contract: Wire.WORKSTREAMS_HOME_BIND_CONTRACT,
      expected_home: b.expected_home,
      home_setup_request_id: b.home_setup.setup_request_id,
    };
    const result = yield* client
      .call({
        environmentId: EnvironmentId.make(b.expected_home.home_environment_id),
        expectedGeneration: b.member_setup.outbound_link_generation,
        tool: "jones_workstreams_home_bind_setup",
        arguments: request,
        limits: f.limits,
        success: Wire.WorkstreamsHomeBindSetupReadbackV2,
      })
      .pipe(
        Effect.mapError((cause) => new MemberQualificationError({ code: "remote_unknown", cause })),
      );
    if (
      !same(result, f.intent.home_setup) ||
      sha(result.setup_request_body) !== b.home_setup.setup_request_sha256
    )
      return yield* refuse("home_mismatch");
    yield* f.check();
  });
  const readback = Effect.fn("MemberPeerPinQualification.readback")(function* (
    token: QualifiedMemberSetup,
  ) {
    const f = yield* facts(token),
      b = Wire.decodeWorkstreamsHomeBindBody(f.intent.bind_request_body);
    yield* f.check();
    const request: Wire.WorkstreamsHomeBindReadbackRequestV2 = {
      contract: Wire.WORKSTREAMS_HOME_BIND_CONTRACT,
      expected_home: b.expected_home,
      bind_id: b.bind_id,
      request_sha256: f.intent.bind_request_sha256,
    };
    const response = yield* client
      .call({
        environmentId: EnvironmentId.make(b.expected_home.home_environment_id),
        expectedGeneration: b.member_setup.outbound_link_generation,
        tool: "jones_workstreams_home_bind_readback",
        arguments: request,
        limits: f.limits,
        success: Wire.WorkstreamsHomeBindReadbackV2,
      })
      .pipe(
        Effect.mapError((cause) => new MemberQualificationError({ code: "remote_unknown", cause })),
      );
    if (!same(response.request, request)) return yield* refuse("home_mismatch");
    if (response.kind === "conflict") return yield* refuse("conflict");
    if (response.kind !== "committed") return yield* refuse("remote_unknown");
    if (
      response.result.request_body !== f.intent.bind_request_body ||
      response.result.receipt.request_sha256 !== sha(f.intent.bind_request_body)
    )
      return yield* refuse("home_mismatch");
    yield* f.check();
    const witness = Object.freeze({}) as VerifiedMemberHomeBind;
    homes.set(witness, { setup: token, bytes: canonical(response.result) });
    return witness;
  });
  const result = Effect.fn("MemberPeerPinQualification.result")(function* (
    token: QualifiedMemberSetup,
    home: VerifiedMemberHomeBind,
  ) {
    yield* checkCurrent(token);
    const h = homes.get(home);
    if (!h || h.setup !== token) return yield* refuse("unauthenticated");
    return yield* decodeHomeBytes(h.bytes).pipe(Effect.mapError(() => refuse("home_mismatch")));
  });
  const verifyBinding = Effect.fn("MemberPeerPinQualification.verifyBinding")(function* (
    token: QualifiedMemberSetup,
    home: VerifiedMemberHomeBind,
    pin: Pin,
    receipt: NonNullable<Pin["bind_receipt"]>,
    singletonVersion: number,
  ) {
    const f = yield* facts(token),
      committed = yield* result(token, home),
      b = Wire.decodeWorkstreamsHomeBindBody(f.intent.bind_request_body);
    const expected =
      pin.state === "prepared"
        ? f.pin
        : {
            ...f.pin,
            revision: 2,
            state: "bound",
            bind_receipt: committed.receipt,
            source_binding_version: committed.receipt.binding_version,
            singleton_binding_version: singletonVersion,
          };
    if (
      !same(pin, expected) ||
      !same(receipt, committed.receipt) ||
      singletonVersion !== b.member_setup.singleton_binding_version
    )
      return yield* refuse("conflict");
  });
  return MemberPeerPinQualification.of({
    qualify,
    checkCurrent,
    verifyHomeSetup,
    readback,
    result,
    verifyBinding,
  });
});
/** Requires genuine owner/native authentication and the real PeerMcpClient; no fallback ports are supplied. */
export const layer = Layer.effect(MemberPeerPinQualification, make);
