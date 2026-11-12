import * as F from "@t3tools/contracts/jones/workstreamsHomeBind";
import type { WorkstreamsNativeHomeTarget as Home } from "@t3tools/contracts/jones/workstreamsNative";
import type { WorkstreamsPeerSessionPinV1 as Pin } from "@t3tools/contracts/jones/workstreamsPeerAuthority";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import { sha256EnrollmentBytes as digest } from "../../enrollment/request.ts";
import * as Records from "../store/peerSessionPinStore.ts";
import * as Registry from "../store/registryStore.ts";
import * as Pins from "./peerSessionPins.ts";
import * as Setup from "./peerSessionPinSetup.ts";
import * as Owner from "./sourceEnrollment.ts";

// Only this module's adapter can mint an admitted binding. The symbol is not exported.
const admitted: unique symbol = Symbol("HomeBindingEvidence");
export interface HomeBindingEvidence {
  readonly [admitted]: true;
}
export interface QualifiedHomePeer {
  readonly record: Records.PeerPinRecord;
  readonly canonical_origin: string;
  readonly checkLocal: (expectedPin: Pin) => Effect.Effect<void, Pins.PeerSessionError>;
}
export class HomePeerPinQualification extends Context.Service<
  HomePeerPinQualification,
  {
    readonly current: (
      invocation: Mcp.McpInvocationScope,
      home: Home,
      setupId?: string,
    ) => Effect.Effect<QualifiedHomePeer, Pins.PeerSessionError>;
    readonly prepare: (
      peer: QualifiedHomePeer,
      requestBody: string,
    ) => Effect.Effect<HomeBindingEvidence, Pins.PeerSessionError>;
    readonly verifyBinding: (
      evidence: HomeBindingEvidence,
      pin: Pin,
      receipt: F.WorkstreamsHomeBindReceiptV2,
      singletonVersion: number,
    ) => Effect.Effect<void, Pins.PeerSessionError>;
  }
>()("t3/jones/workstreams/native/runtime/homePeerPinQualification") {}
const decodePermissions = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Array(Schema.String)),
);
const same = (a: unknown, b: unknown) => canonical(a) === canonical(b);
const make = Effect.gen(function* () {
  const native = yield* Pins.NativePeerSessions,
    records = yield* Records.PeerSessionPinStore,
    authority = yield* Authority.NativeStoreAuthority,
    registries = yield* Registry.RegistryStore;
  const owner = yield* Effect.serviceOption(Owner.SourceSetupOwner),
    qualification = yield* Effect.serviceOption(Setup.PeerPinSetupQualification);
  const peers = new WeakMap<
    QualifiedHomePeer,
    { invocation: Mcp.McpInvocationScope; ownerDigest: string }
  >();
  const bindings = new WeakMap<
    HomeBindingEvidence,
    { peer: QualifiedHomePeer; body: F.WorkstreamsHomeBindBodyV2; requestDigest: string }
  >();
  const ownerCurrent = Effect.fn("HomePeerPinQualification.ownerCurrent")(function* () {
    if (Option.isNone(owner) || Option.isNone(qualification))
      return yield* new Pins.PeerSessionError({ code: "owner_unqualified" });
    yield* owner.value.verifyUnchanged.pipe(Effect.mapError(Pins.mapFailure));
    return qualification.value;
  });
  const current = Effect.fn("HomePeerPinQualification.current")(function* (
    invocation: Mcp.McpInvocationScope,
    home: Home,
    setupId?: string,
  ) {
    const facts = yield* native.current(invocation);
    const port = yield* ownerCurrent();
    const record = yield* records
      .read({ session_id: facts.session.session_id })
      .pipe(Effect.mapError(Pins.mapFailure));
    if (!record || (setupId !== undefined && record.pin.setup_request_id !== setupId))
      return yield* new Pins.PeerSessionError({ code: "not_current" });
    const p = record.pin;
    const original = Records.decodeSetupRequest(record.request_body).pin;
    const qualified = yield* port.qualify(record.request_body, digest(record.request_body));
    if (
      qualified.invocation !== invocation ||
      qualified.owner_qualification_digest !== p.owner_qualification_digest
    )
      return yield* new Pins.PeerSessionError({ code: "owner_unqualified" });
    // External owner qualification is completed before admission to a SQL writer.
    yield* qualified.checkCurrent();
    const checkLocal = Effect.fn("HomePeerPinQualification.checkLocal")(function* (
      expectedPin: Pin,
    ) {
      yield* ownerCurrent();
      const session = yield* facts.checkCurrent(),
        local = yield* authority.readCurrent.pipe(Effect.mapError(Pins.mapFailure)),
        now = DateTime.toEpochMillis(yield* DateTime.now),
        row = yield* records.read({ pin_id: p.pin_id }).pipe(Effect.mapError(Pins.mapFailure)),
        c = yield* registries.context(home).pipe(Effect.mapError(Pins.mapFailure));
      if (
        !row ||
        !same(row.pin, expectedPin) ||
        row.request_body !== record.request_body ||
        expectedPin.state === "revoked" ||
        Date.parse(p.expires_at) <= now ||
        !same(session, p.session) ||
        !Pins.ceilingAllows(session.ceiling, "workstreams:read")
      )
        return yield* new Pins.PeerSessionError({ code: "not_current" });
      if (
        !same(p.expected_home, home) ||
        !p.roles.includes("member_home") ||
        p.peer_environment_id !== p.source_authority.environment_id ||
        p.peer_environment_id === home.home_environment_id ||
        invocation.environmentId !== local.environmentId ||
        local.environmentId !== home.home_environment_id ||
        local.environmentId !== p.receiver_authority.environment_id ||
        local.authorityNamespace !== p.receiver_authority.authority_namespace ||
        local.storeGeneration !== p.receiver_authority.store_generation ||
        !c ||
        c.state !== "active" ||
        c.home_environment_id !== home.home_environment_id ||
        c.server_generation !== home.server_generation
      )
        return yield* new Pins.PeerSessionError({ code: "authority_mismatch" });
      const principals = (yield* registries
        .listPlain(home, "principals")
        .pipe(Effect.mapError(Pins.mapFailure))).filter(
        (v) => v.kind === "environment" && v.environment_id === p.peer_environment_id,
      );
      if (principals.length !== 1) return yield* new Pins.PeerSessionError({ code: "conflict" });
      const grant = (yield* registries
        .listPlain(home, "grants")
        .pipe(Effect.mapError(Pins.mapFailure)))
        .filter((v) => v.principal_id === principals[0]!.principal_id)
        .toSorted((a, b) => Number(b.grant_version) - Number(a.grant_version))[0];
      const permissions = yield* decodePermissions(String(grant?.permissions)).pipe(
        Effect.orElseSucceed((): readonly string[] => []),
      );
      if (
        !grant ||
        grant.state !== "active" ||
        !Array.isArray(permissions) ||
        !permissions.includes("workstreams:read")
      )
        return yield* new Pins.PeerSessionError({ code: "not_current" });
      if (expectedPin.state === "bound") {
        const latest = (yield* registries
          .listPlain(home, "source_bindings")
          .pipe(Effect.mapError(Pins.mapFailure)))
          .filter(
            (b) =>
              b.provider === "t3" && b.source_instance_id === p.source_authority.environment_id,
          )
          .toSorted((a, b) => Number(b.binding_version) - Number(a.binding_version))[0];
        if (
          !latest ||
          latest.state !== "active" ||
          latest.authority_namespace !== p.source_authority.authority_namespace ||
          Number(latest.store_generation) !== p.source_authority.store_generation ||
          Number(latest.binding_version) !== expectedPin.source_binding_version ||
          expectedPin.bind_receipt?.principal_id !== principals[0]!.principal_id
        )
          return yield* new Pins.PeerSessionError({ code: "not_current" });
      }
    });
    yield* checkLocal(p);
    const c = yield* registries.context(home).pipe(Effect.mapError(Pins.mapFailure));
    const result: QualifiedHomePeer = { record, canonical_origin: c!.canonical_origin, checkLocal };
    peers.set(result, { invocation, ownerDigest: original.owner_qualification_digest });
    return result;
  });
  const prepare = Effect.fn("HomePeerPinQualification.prepare")(function* (
    peer: QualifiedHomePeer,
    requestBody: string,
  ) {
    const body = yield* Effect.try({
      try: () => F.decodeWorkstreamsHomeBindBody(requestBody),
      catch: () => new Pins.PeerSessionError({ code: "invalid_request" }),
    });
    const captured = peers.get(peer);
    if (!captured) return yield* new Pins.PeerSessionError({ code: "owner_unqualified" });
    const port = yield* ownerCurrent();
    // The root's genuine owner hook must qualify this exact bind request as well as the setup.
    // Existing setup-only implementations refuse it; there is no permissive default.
    const q = yield* port.qualify(requestBody, digest(requestBody));
    if (
      q.invocation !== captured.invocation ||
      q.owner_qualification_digest !== captured.ownerDigest
    )
      return yield* new Pins.PeerSessionError({ code: "owner_unqualified" });
    yield* q.checkCurrent();
    const p = peer.record.pin;
    if (
      p.state !== "prepared" ||
      p.revision !== body.home_setup.prepared_revision ||
      p.pin_id !== body.home_setup.pin_id ||
      p.setup_request_id !== body.home_setup.setup_request_id ||
      p.setup_request_sha256 !== body.home_setup.setup_request_sha256 ||
      !same(p.expected_home, body.expected_home) ||
      !same(p.source_authority, body.source.authority) ||
      !same(p.receiver_authority, body.expected_home_authority) ||
      peer.canonical_origin !== body.expected_canonical_origin ||
      p.source_enrollment_id !== body.source.enrollment.enrollment_id ||
      p.session.session_id === body.source.enrollment.session.session_id
    )
      return yield* new Pins.PeerSessionError({ code: "authority_mismatch" });
    yield* peer.checkLocal(p);
    const evidence: HomeBindingEvidence = { [admitted]: true };
    bindings.set(evidence, { peer, body, requestDigest: digest(requestBody) });
    return evidence;
  });
  const verifyBinding = Effect.fn("HomePeerPinQualification.verifyBinding")(function* (
    evidence: HomeBindingEvidence,
    pin: Pin,
    receipt: F.WorkstreamsHomeBindReceiptV2,
    singletonVersion: number,
  ) {
    const e = bindings.get(evidence);
    if (
      !e ||
      !same(receipt, {
        principal_id: e.body.expected.principal_id,
        binding_version: (e.body.expected.source_binding?.binding_version ?? 0) + 1,
        registry_version: e.body.expected.registry_version + 1,
        request_sha256: e.requestDigest,
      }) ||
      singletonVersion !== e.body.member_setup.singleton_binding_version ||
      pin.pin_id !== e.peer.record.pin.pin_id
    )
      return yield* new Pins.PeerSessionError({ code: "conflict" });
    const expected: Pin = {
      ...e.peer.record.pin,
      state: "bound",
      revision: 2,
      bind_receipt: receipt,
      source_binding_version: receipt.binding_version,
      singleton_binding_version: singletonVersion,
    };
    if (!same(pin, e.peer.record.pin) && !same(pin, expected))
      return yield* new Pins.PeerSessionError({ code: "conflict" });
    yield* e.peer.checkLocal(pin);
  });
  return HomePeerPinQualification.of({ current, prepare, verifyBinding });
});
/** Requires genuine NativePeerSessions and root owner/setup qualification. Missing hooks refuse. */
export const layer = Layer.effect(HomePeerPinQualification, make);
