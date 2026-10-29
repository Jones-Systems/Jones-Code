import * as Wire from "@t3tools/contracts";
import type {
  WorkstreamsForwardIdentity,
  WorkstreamsForwardRequest,
  WorkstreamsForwardReadbackRequest,
  WorkstreamsForwardHomeProvenance,
} from "@t3tools/contracts/jones/workstreamsForwarding";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import type { NativeStoreAuthorityTuple } from "../../../../environment/NativeStoreAuthority.ts";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";
import * as Session from "../registry/session.ts";
import * as Guard from "./guard.ts";
import * as Pins from "../runtime/peerSessionPins.ts";
import * as Keys from "../crypto/keyProvider.ts";
import type * as Store from "../store/registryStore.ts";

/** Root prepares a current in-process revocation fence before opening SQL.
 * checkCurrent MUST NOT read files, call providers, fetch keys or wait for authority.
 * Missing production support keeps the home mount disabled; a captured tuple is insufficient.
 */
export class ReceivingAuthorityFence extends Context.Service<
  ReceivingAuthorityFence,
  {
    readonly prepare: (invocation: Mcp.McpInvocationScope) => Effect.Effect<
      {
        readonly authority: NativeStoreAuthorityTuple;
        readonly checkCurrent: () => Effect.Effect<void, Session.RegistryError>;
      },
      Session.RegistryError
    >;
  }
>()("t3/jones/workstreams/native/forwarding/peerWriteAdmission/ReceivingAuthorityFence") {}

/** Request-local current source fence; prepared by the original-source lookup outside SQL. */
export class CurrentSourceForwardFence extends Context.Service<
  CurrentSourceForwardFence,
  {
    readonly forward_request_sha256: string;
    readonly checkCurrent: () => Effect.Effect<void, Session.RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/peerWriteAdmission/CurrentSourceForwardFence") {}
const admissionBrand: unique symbol = Symbol("HomeWriteAdmission");
export interface HomeWriteAdmission {
  readonly [admissionBrand]: true;
  readonly caller: AdmittedCaller;
  readonly identity: WorkstreamsForwardIdentity;
}
type Authorization = Effect.Success<ReturnType<Session.Session["Service"]["authorize"]>>;
type GuardAdmission = Effect.Success<ReturnType<Guard.Guard["Service"]["authorize"]>>;
export class PeerWriteBoundary extends Context.Service<
  PeerWriteBoundary,
  {
    readonly current: (
      request: WorkstreamsForwardRequest | WorkstreamsForwardReadbackRequest,
      readback?: boolean,
    ) => Effect.Effect<HomeWriteAdmission, Session.RegistryError>;
    readonly authorize: (
      caller: AdmittedCaller,
      permission: "workstreams:read" | "workstreams:write",
    ) => Effect.Effect<Authorization, Session.RegistryError>;
    readonly checkCurrent: (
      proof: HomeWriteAdmission,
    ) => Effect.Effect<void, Session.RegistryError>;
    /** Only observed authoritative registry receipts can advance this request's own expected version. */
    readonly observe: (
      proof: HomeWriteAdmission,
      receipt: Wire.WorkstreamReceipt,
    ) => Effect.Effect<void, Session.RegistryError>;
    readonly provenance: (
      proof: HomeWriteAdmission,
    ) => Effect.Effect<WorkstreamsForwardHomeProvenance, Session.RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/peerWriteAdmission/PeerWriteBoundary") {}
const denied = () => new Session.RegistryError({ code: "not_granted" });
const isRegistry = Schema.is(Session.RegistryError);
const isKeyUnavailable = Schema.is(Keys.KeyUnavailable);
export const mapFailure = (cause: unknown) =>
  isRegistry(cause)
    ? cause
    : new Session.RegistryError({
        code: isKeyUnavailable(cause) ? "key_unavailable" : "unavailable",
        cause,
      });
const make = Effect.gen(function* () {
  const guard = yield* Guard.Guard,
    native = yield* Pins.NativePeerSessions,
    home = yield* NativeHome;
  type Proof = {
    nativeFacts: Effect.Success<ReturnType<Pins.NativePeerSessions["Service"]["current"]>>;
    scope: Mcp.McpInvocationScope;
    admission: HomeWriteAdmission;
    request: string;
    caller: string;
    initial: GuardAdmission;
    version: number;
    readback: boolean;
  };
  const proofs = new WeakMap<HomeWriteAdmission, Proof>();
  const callers = new WeakMap<AdmittedCaller, Proof>();
  const fresh = Effect.fn("PeerWriteBoundary.fresh")(function* (
    p: Proof,
    permission: "workstreams:read" | "workstreams:write",
  ) {
    const scope = yield* Effect.serviceOption(Mcp.McpInvocationContext);
    if (
      Option.isNone(scope) ||
      scope.value !== p.scope ||
      !scope.value.client?.linked ||
      canonical(p.admission.caller) !== p.caller ||
      canonical(p.admission.identity) !== p.request ||
      (p.readback && permission !== "workstreams:read")
    )
      return yield* denied();
    const sourceFence = yield* Effect.serviceOption(CurrentSourceForwardFence);
    if (Option.isSome(sourceFence)) yield* sourceFence.value.checkCurrent();
    // Hold the original authenticator registration across legitimate own-version advances.
    const session = yield* p.nativeFacts.checkCurrent().pipe(Effect.mapError(mapFailure));
    if (!Pins.ceilingAllows(session.ceiling, permission)) return yield* denied();
    const now = yield* guard.authorize(permission);
    if (
      canonical(now.pin) !== canonical(p.initial.pin) ||
      canonical(now.grant) !== canonical(p.initial.grant) ||
      canonical(now.caller) !== canonical({ ...p.initial.caller, origin: { kind: "agent" } }) ||
      canonical({ ...now.context, registry_version: p.version }) !== canonical(now.context) ||
      canonical({ ...now.context, registry_version: p.initial.context.registry_version }) !==
        canonical(p.initial.context)
    )
      return yield* denied();
    yield* now.checkCurrent();
    return now;
  });
  const proofOf = (proof: HomeWriteAdmission) => {
    const p = proofs.get(proof);
    return p && p.admission === proof ? Effect.succeed(p) : Effect.fail(denied());
  };
  const authorize = Effect.fn("PeerWriteBoundary.authorize")(function* (
    caller: AdmittedCaller,
    permission: "workstreams:read" | "workstreams:write",
  ) {
    const sourceFence = yield* Effect.serviceOption(CurrentSourceForwardFence);
    if (Option.isNone(sourceFence)) return yield* denied();
    yield* sourceFence.value.checkCurrent();
    const p = callers.get(caller);
    if (!p || p.admission.caller !== caller) return yield* denied();
    const now = yield* fresh(p, permission);
    return {
      context: now.context,
      authorization: {
        owner_id: home.owner_id,
        actor: { principal_id: caller.principal_id },
        grant: now.grant,
        server_generation: home.server_generation,
      },
    };
  });
  const current = Effect.fn("PeerWriteBoundary.current")(function* (
    request: WorkstreamsForwardRequest | WorkstreamsForwardReadbackRequest,
    readback = false,
  ) {
    const scope = yield* Effect.serviceOption(Mcp.McpInvocationContext);
    if (Option.isNone(scope) || !scope.value.client?.linked) return yield* denied();
    // Guard and NativePeerSessions both demand the authenticator-registered object.
    const facts = yield* native.current(scope.value).pipe(Effect.mapError(mapFailure));
    const permission = readback ? "workstreams:read" : "workstreams:write";
    if (!Pins.ceilingAllows(facts.session.ceiling, permission)) return yield* denied();
    const admitted = yield* guard.authorize(
      permission,
      "kind" in request ? request.request.relay : undefined,
    );
    const i = request.identity,
      pin = admitted.pin;
    yield* Guard.assertTarget(i.expected_home, admitted.context);
    if (
      pin.state !== "bound" ||
      i.canonical_origin !== home.canonical_origin ||
      i.principal_id !== admitted.caller.principal_id ||
      i.source_binding_version !== admitted.caller.binding_version ||
      canonical(i.source_authority) !== canonical(pin.source_authority) ||
      i.source_enrollment_id !== pin.source_enrollment_id ||
      i.singleton_binding_version !== pin.singleton_binding_version ||
      i.source_binding_version !== pin.source_binding_version ||
      i.source_authority.environment_id !== pin.peer_environment_id ||
      ("kind" in request &&
        (request.request.relay.linked_origin_environment_id !== null ||
          (request.kind === "operation" &&
            request.request.request.kind === "set_primary" &&
            request.request.request.thread.environment_id !== pin.peer_environment_id)))
    )
      return yield* denied();
    const caller = Object.freeze({
      ...admitted.caller,
      origin: Object.freeze(
        "kind" in request ? { ...request.request.relay.origin } : { kind: "agent" as const },
      ),
    });
    const identity = structuredClone(i);
    const admission: HomeWriteAdmission = Object.freeze({
      [admissionBrand]: true as const,
      caller,
      identity,
    });
    const p: Proof = {
      nativeFacts: facts,
      scope: scope.value,
      admission,
      request: canonical(i),
      caller: canonical(caller),
      initial: admitted,
      version: admitted.context.registry_version,
      readback,
    };
    // Fresh Guard without a relay uses the default agent attribution; identity is checked separately.
    p.initial = { ...admitted, caller: { ...admitted.caller, origin: { kind: "agent" } } };
    proofs.set(admission, p);
    callers.set(caller, p);
    yield* fresh(p, permission);
    return admission;
  });
  const checkCurrent = Effect.fn("PeerWriteBoundary.checkCurrent")(function* (
    proof: HomeWriteAdmission,
  ) {
    const p = yield* proofOf(proof);
    yield* fresh(p, p.readback ? "workstreams:read" : "workstreams:write");
  });
  const observe = Effect.fn("PeerWriteBoundary.observe")(function* (
    proof: HomeWriteAdmission,
    receipt: Wire.WorkstreamReceipt,
  ) {
    const p = yield* proofOf(proof);
    if (
      p.readback ||
      receipt.owner_id !== home.owner_id ||
      receipt.actor.principal_id !== proof.caller.principal_id ||
      receipt.server_generation !== home.server_generation ||
      !proof.identity.command_ids.includes(receipt.command_id)
    )
      return yield* denied();
    if (receipt.state === "committed") {
      if (
        receipt.registry_version < p.version ||
        receipt.registry_version > p.version + 1 ||
        (receipt.registry_version === p.version + 1 && receipt.state !== "committed")
      )
        return yield* denied();
      p.version = receipt.registry_version;
    }
    yield* checkCurrent(proof);
  });
  const provenance = Effect.fn("PeerWriteBoundary.provenance")(function* (
    proof: HomeWriteAdmission,
  ) {
    const p = yield* proofOf(proof);
    const now = yield* fresh(p, p.readback ? "workstreams:read" : "workstreams:write");
    const c: Store.RegistryContext = now.context;
    return {
      registry: {
        registry_instance_id: c.registry_instance_id,
        owner_id: c.owner_id,
        home_environment_id: c.home_environment_id,
        canonical_origin: c.canonical_origin,
        server_generation: c.server_generation,
        registry_version: c.registry_version,
      },
      principal_id: proof.caller.principal_id,
      authorization_revision: now.grant.grant_version,
      source_binding_version: proof.identity.source_binding_version,
      incoming_peer_pin_id: now.pin.pin_id,
      incoming_peer_pin_revision: now.pin.revision,
    };
  });
  return PeerWriteBoundary.of({ current, authorize, observe, checkCurrent, provenance });
});
/** @public Construct once per prepared home execution graph. */
export const layer = Layer.effect(PeerWriteBoundary, make);
/** @public Root composes alongside the existing read boundary; never replace it globally. */
export const sessionLayer = Layer.effect(
  Session.Session,
  Effect.gen(function* () {
    const local = yield* Session.Session,
      boundary = yield* PeerWriteBoundary;
    return Session.Session.of({
      authorize: Effect.fn("PeerWriteBoundary.session")(function* (caller, permission) {
        const scope = yield* Effect.serviceOption(Mcp.McpInvocationContext);
        return Option.isSome(scope) && scope.value.client?.linked
          ? yield* boundary.authorize(caller, permission)
          : yield* local.authorize(caller, permission);
      }),
    });
  }),
);
