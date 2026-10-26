import type { WorkstreamsPeerSessionPinV1 as Pin } from "@t3tools/contracts/jones/workstreamsPeerAuthority";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import * as Records from "../store/peerSessionPinStore.ts";
import * as Database from "../store/sql.ts";
import * as Registries from "../store/registryStore.ts";
import * as Owner from "./sourceEnrollment.ts";
import * as Pins from "./peerSessionPins.ts";

/** Qualified owner envelope and genuine incoming native invocation, supplied by root CLI/MCP hooks.
 * No implementation is installed by this source capsule. */
export class PeerPinSetupQualification extends Context.Service<
  PeerPinSetupQualification,
  {
    readonly qualify: (
      requestBody: string,
      requestSha256: string,
    ) => Effect.Effect<
      {
        readonly invocation: Mcp.McpInvocationScope;
        readonly owner_qualification_digest: string;
        readonly checkCurrent: () => Effect.Effect<void, Pins.PeerSessionError>;
      },
      Pins.PeerSessionError
    >;
    readonly verifyBinding: (
      pin: Pin,
      receipt: NonNullable<Pin["bind_receipt"]>,
      singletonVersion: number,
    ) => Effect.Effect<void, Pins.PeerSessionError>;
  }
>()("t3/jones/workstreams/native/runtime/peerSessionPinSetup/PeerPinSetupQualification") {}
export class PeerSessionPinSetup extends Context.Service<
  PeerSessionPinSetup,
  {
    readonly plan: (
      requestBody: string,
    ) => Effect.Effect<
      { readonly state: "ready" | "applied"; readonly request_sha256: string; readonly pin: Pin },
      Pins.PeerSessionError
    >;
    readonly apply: (
      requestBody: string,
      expectedRevision: number,
    ) => Effect.Effect<Pin, Pins.PeerSessionError>;
    readonly readback: (
      setupRequestId: string,
    ) => Effect.Effect<Records.PeerPinRecord | null, Pins.PeerSessionError>;
    readonly bind: (
      pinId: string,
      expectedRevision: number,
      receipt: NonNullable<Pin["bind_receipt"]>,
      singletonVersion: number,
    ) => Effect.Effect<Pin, Pins.PeerSessionError>;
    readonly revoke: (
      pinId: string,
      expectedRevision: number,
    ) => Effect.Effect<Pin, Pins.PeerSessionError>;
  }
>()("t3/jones/workstreams/native/runtime/peerSessionPinSetup") {}
const equal = (a: unknown, b: unknown) => canonical(a) === canonical(b);
const make = Effect.gen(function* () {
  const records = yield* Records.PeerSessionPinStore,
    db = yield* Database.NativeSql,
    sessions = yield* Pins.NativePeerSessions,
    authority = yield* Authority.NativeStoreAuthority,
    registries = yield* Registries.RegistryStore;
  const owner = yield* Effect.serviceOption(Owner.SourceSetupOwner),
    qualification = yield* Effect.serviceOption(PeerPinSetupQualification);
  const ownerCurrent = Effect.fn("PeerSessionPinSetup.ownerCurrent")(function* () {
    if (Option.isNone(owner) || Option.isNone(qualification))
      return yield* new Pins.PeerSessionError({ code: "owner_unqualified" });
    yield* owner.value.verifyUnchanged.pipe(Effect.mapError(Pins.mapFailure));
    return qualification.value;
  });
  const qualify = Effect.fn("PeerSessionPinSetup.qualify")(function* (body: string) {
    const decoded = yield* Effect.try({
      try: () => Records.decodeSetupRequest(body),
      catch: () => new Pins.PeerSessionError({ code: "invalid_request" }),
    });
    const port = yield* ownerCurrent();
    const q = yield* port.qualify(body, decoded.pin.setup_request_sha256);
    const check = Effect.fn("PeerSessionPinSetup.check")(function* () {
      yield* ownerCurrent();
      yield* q.checkCurrent();
      const native = yield* sessions.current(q.invocation),
        local = yield* authority.readCurrent.pipe(Effect.mapError(Pins.mapFailure));
      const p = decoded.pin,
        now = DateTime.toEpochMillis(yield* DateTime.now);
      if (
        q.owner_qualification_digest !== p.owner_qualification_digest ||
        !equal(native.session, p.session) ||
        Date.parse(p.expires_at) <= now ||
        q.invocation.environmentId !== local.environmentId ||
        local.environmentId !== p.receiver_authority.environment_id ||
        local.authorityNamespace !== p.receiver_authority.authority_namespace ||
        local.storeGeneration !== p.receiver_authority.store_generation
      )
        return yield* new Pins.PeerSessionError({ code: "authority_mismatch" });
      for (const role of p.roles) {
        if (
          role === "home_source"
            ? !equal(p.receiver_authority, p.source_authority) ||
              p.peer_environment_id !== p.expected_home.home_environment_id
            : p.peer_environment_id !== p.source_authority.environment_id ||
              p.receiver_authority.environment_id !== p.expected_home.home_environment_id
        )
          return yield* new Pins.PeerSessionError({ code: "wrong_role" });
      }
    });
    yield* check();
    return { ...decoded, check, port };
  });
  const homePreconditions = Effect.fn("PeerSessionPinSetup.homePreconditions")(function* (
    request: Records.PeerPinSetupRequest,
    pin: Pin,
  ) {
    // A source receiver has no home rows; its exact remote envelope is qualified by the owner port.
    if (pin.receiver_authority.environment_id !== pin.expected_home.home_environment_id) return;
    const c = yield* registries.context(pin.expected_home);
    if (
      !c ||
      c.state !== "active" ||
      c.home_environment_id !== pin.expected_home.home_environment_id ||
      c.server_generation !== pin.expected_home.server_generation ||
      c.registry_version !== request.expected.registry_version
    )
      return yield* new Pins.PeerSessionError({ code: "conflict" });
    const principals = (yield* registries.listPlain(pin.expected_home, "principals")).filter(
      (p) => p.kind === "environment" && p.environment_id === pin.peer_environment_id,
    );
    if (principals.length !== 1) return yield* new Pins.PeerSessionError({ code: "conflict" });
    const grant = (yield* registries.listPlain(pin.expected_home, "grants"))
      .filter((g) => g.principal_id === principals[0]!.principal_id)
      .toSorted((a, b) => Number(b.grant_version) - Number(a.grant_version))[0];
    if (
      !grant ||
      grant.state !== "active" ||
      grant.grant_id !== request.expected.grant_id ||
      Number(grant.grant_version) !== request.expected.grant_version
    )
      return yield* new Pins.PeerSessionError({ code: "conflict" });
  });
  const existing = Effect.fn("PeerSessionPinSetup.existing")(function* (body: string, pin: Pin) {
    const bySetup = yield* records.read({ setup_request_id: pin.setup_request_id }),
      bySession = yield* records.read({ session_id: pin.session.session_id }),
      byId = yield* records.read({ pin_id: pin.pin_id });
    if (!bySetup && !bySession && !byId) return null;
    if (
      !bySetup ||
      !bySession ||
      !byId ||
      bySetup.request_body !== body ||
      !equal(bySetup, bySession) ||
      !equal(bySetup, byId)
    )
      return yield* new Pins.PeerSessionError({ code: "conflict" });
    return bySetup.pin;
  });
  const plan = Effect.fn("PeerSessionPinSetup.plan")(function* (body: string) {
    const q = yield* qualify(body);
    return yield* db
      .transaction(
        q.pin.expected_home,
        Effect.gen(function* () {
          const p = yield* existing(body, q.pin);
          if (!p) yield* homePreconditions(q.request, q.pin);
          yield* q.check();
          return {
            state: p ? ("applied" as const) : ("ready" as const),
            request_sha256: q.pin.setup_request_sha256,
            pin: p ?? q.pin,
          };
        }),
      )
      .pipe(Effect.mapError(Pins.mapFailure));
  });
  const apply = Effect.fn("PeerSessionPinSetup.apply")(function* (
    body: string,
    expectedRevision: number,
  ) {
    const q = yield* qualify(body);
    return yield* db
      .transaction(
        q.pin.expected_home,
        Effect.gen(function* () {
          const p = yield* existing(body, q.pin);
          // Lookup-first recovery returns the same retained identity even after a successful bind/revoke.
          if (p) {
            if (expectedRevision !== 0 && expectedRevision !== p.revision)
              return yield* new Pins.PeerSessionError({ code: "conflict" });
            yield* q.check();
            return p;
          }
          if (expectedRevision !== 0) return yield* new Pins.PeerSessionError({ code: "conflict" });
          yield* homePreconditions(q.request, q.pin);
          yield* q.check();
          yield* records.insert({ pin: q.pin, request_body: body });
          yield* q.check();
          const result = yield* records.read({ setup_request_id: q.pin.setup_request_id });
          if (!result || result.request_body !== body || !equal(result.pin, q.pin))
            return yield* new Pins.PeerSessionError({ code: "conflict" });
          return result.pin;
        }),
      )
      .pipe(Effect.mapError(Pins.mapFailure));
  });
  const readback = Effect.fn("PeerSessionPinSetup.readback")(function* (id: string) {
    yield* ownerCurrent();
    const r = yield* records.read({ setup_request_id: id }).pipe(Effect.mapError(Pins.mapFailure));
    if (!r) return null;
    const q = yield* qualify(r.request_body);
    yield* q.check();
    return r;
  });
  const bind = Effect.fn("PeerSessionPinSetup.bind")(function* (
    id: string,
    revision: number,
    receipt: NonNullable<Pin["bind_receipt"]>,
    singletonVersion: number,
  ) {
    const port = yield* ownerCurrent();
    const r = yield* records.read({ pin_id: id }).pipe(Effect.mapError(Pins.mapFailure));
    if (!r) return yield* new Pins.PeerSessionError({ code: "conflict" });
    const q = yield* qualify(r.request_body);
    return yield* db
      .transaction(
        r.pin.expected_home,
        Effect.gen(function* () {
          const current = yield* records.read({ pin_id: id });
          if (!current || current.pin.revision !== revision || current.pin.state !== "prepared")
            return yield* new Pins.PeerSessionError({ code: "conflict" });
          yield* q.check();
          yield* port.verifyBinding(current.pin, receipt, singletonVersion);
          const next: Pin = {
            ...current.pin,
            revision: revision + 1,
            state: "bound",
            source_binding_version: receipt.binding_version,
            singleton_binding_version: singletonVersion,
            bind_receipt: receipt,
          };
          yield* records.transition(next, revision);
          yield* q.check();
          yield* port.verifyBinding(next, receipt, singletonVersion);
          return next;
        }),
      )
      .pipe(Effect.mapError(Pins.mapFailure));
  });
  const revoke = Effect.fn("PeerSessionPinSetup.revoke")(function* (id: string, revision: number) {
    yield* ownerCurrent();
    const r = yield* records.read({ pin_id: id }).pipe(Effect.mapError(Pins.mapFailure));
    if (!r) return yield* new Pins.PeerSessionError({ code: "conflict" });
    return yield* db
      .transaction(
        r.pin.expected_home,
        Effect.gen(function* () {
          yield* ownerCurrent();
          if (r.pin.revision !== revision || r.pin.state === "revoked")
            return yield* new Pins.PeerSessionError({ code: "conflict" });
          const next: Pin = { ...r.pin, revision: revision + 1, state: "revoked" };
          yield* records.transition(next, revision);
          yield* ownerCurrent();
          return next;
        }),
      )
      .pipe(Effect.mapError(Pins.mapFailure));
  });
  return PeerSessionPinSetup.of({ plan, apply, readback, bind, revoke });
});
/** @public Canonical service construction. Missing qualification ports fail closed. */
export const layer = Layer.effect(PeerSessionPinSetup, make);
