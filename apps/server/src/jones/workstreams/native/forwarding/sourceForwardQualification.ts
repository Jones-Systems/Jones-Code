// @effect-diagnostics nodeBuiltinImport:off -- Hash exact serialized forwarding bytes without external preparation.
import * as NodeCrypto from "node:crypto";
import { EnvironmentId } from "@t3tools/contracts";
import * as Forward from "@t3tools/contracts/jones/workstreamsForwarding";
import * as Wire from "@t3tools/contracts/jones/workstreamsForwardQualification";
import {
  WorkstreamsPeerSessionPinV1,
  type WorkstreamsTransportCeiling,
} from "@t3tools/contracts/jones/workstreamsPeerAuthority";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Links from "../../../../peer/PeerLinks.ts";
import * as Peer from "../../../../peer/PeerMcpClient.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import { RegistryError } from "../registry/session.ts";
import * as Pins from "../runtime/peerSessionPins.ts";
import * as Supplied from "./suppliedEvidence.ts";

const decodeForward = Schema.decodeUnknownEffect(Forward.WorkstreamsForwardRequest);
const decodeReadback = Schema.decodeUnknownEffect(Forward.WorkstreamsForwardReadbackRequest);
const decodePin = Schema.decodeUnknownEffect(WorkstreamsPeerSessionPinV1);
const decodeResponse = Schema.decodeUnknownEffect(
  Wire.WorkstreamsSourceParentReadmissionResponseV3,
);
const decodeEnvironment = Schema.decodeUnknownEffect(EnvironmentId);
const encodeForward = Schema.encodeSync(Schema.fromJsonString(Forward.WorkstreamsForwardRequest));
const same = (a: unknown, b: unknown) => canonical(a) === canonical(b);
const rank = { "approval-required": 0, "auto-accept-edits": 1, auto: 2, "full-access": 3 };
const lowerMode = <A extends keyof typeof rank>(a: A, b: A): A => (rank[a] <= rank[b] ? a : b);
const narrow = (
  first: WorkstreamsTransportCeiling,
  ...rest: ReadonlyArray<WorkstreamsTransportCeiling>
): WorkstreamsTransportCeiling =>
  rest.reduce(
    (a, b) => ({
      access:
        a.access === "read-only" || b.access === "read-only"
          ? "read-only"
          : lowerMode(a.access, b.access),
      runtime_mode: lowerMode(a.runtime_mode, b.runtime_mode),
      interaction_mode:
        a.interaction_mode === "plan" || b.interaction_mode === "plan" ? "plan" : "default",
      scopes: a.scopes.filter((s) => b.scopes.includes(s)),
      permissions: a.permissions.filter((p) => b.permissions.includes(p)),
    }),
    first,
  );
type Registration = Effect.Success<ReturnType<Pins.NativePeerSessions["Service"]["current"]>>;
interface InvocationLifetime {
  active: boolean;
  readonly invocation: Mcp.McpInvocationScope;
  readonly registration: Registration;
  readonly sql: SqlClient.SqlClient;
}
class QualificationScope extends Context.Service<QualificationScope, { readonly token: object }>()(
  "t3/jones/workstreams/native/forwarding/sourceForwardQualification/QualificationScope",
) {}
// Membership, rather than equality to the invocation, establishes the callback lifetime.
const lifetimes = new WeakMap<object, InvocationLifetime>();

/** @public Root wraps the complete authenticated receiving call, including the writer.
 * This observes the original native registration; it never authenticates or replaces one.
 * The existing ReceivingAuthority and PeerWriteBoundary still own live home/pin/grant checks.
 */
export const withQualificationScope: <A, E, R>(
  invocation: Mcp.McpInvocationScope,
  effect: Effect.Effect<A, E, R>,
) => Effect.Effect<A, E | RegistryError, R | SqlClient.SqlClient | Pins.NativePeerSessions> =
  Effect.fn("SourceForwardQualification.withScope")(function* <A, E, R>(
    invocation: Mcp.McpInvocationScope,
    effect: Effect.Effect<A, E, R>,
  ) {
    const sql = yield* SqlClient.SqlClient;
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* new RegistryError({ code: "preparation_in_transaction" });
    const selected = yield* Effect.serviceOption(Mcp.McpInvocationContext);
    if (Option.isNone(selected) || selected.value !== invocation || !invocation.client?.linked)
      return yield* new RegistryError({ code: "not_granted" });
    const native = yield* Pins.NativePeerSessions;
    const registration = yield* native
      .current(invocation)
      .pipe(Effect.mapError((cause) => new RegistryError({ code: "not_granted", cause })));
    const token = Object.freeze({});
    const lifetime: InvocationLifetime = { active: true, invocation, registration, sql };
    lifetimes.set(token, lifetime);
    return yield* effect.pipe(
      Effect.provideService(QualificationScope, { token }),
      Effect.ensuring(
        Effect.sync(() => {
          lifetime.active = false;
        }),
      ),
    );
  });
interface PreparedObservation {
  readonly state: "authorized";
  readonly forward_request_sha256: string;
  readonly checkCurrent: () => Effect.Effect<void, RegistryError>;
}
const make = Effect.gen(function* () {
  const peer = yield* Peer.PeerMcpClient;
  const links = yield* Links.PeerLinks;
  const sql = yield* SqlClient.SqlClient;
  const home = yield* NativeHome;
  interface Observation {
    readonly token: object;
    readonly lifetime: InvocationLifetime;
    readonly environmentId: EnvironmentId;
    readonly generation: string;
    readonly pin: WorkstreamsPeerSessionPinV1;
    readonly permission: "workstreams:read" | "workstreams:write";
    readonly original: WorkstreamsTransportCeiling;
    readonly current: WorkstreamsTransportCeiling;
    readonly ceiling: WorkstreamsTransportCeiling;
  }
  const observations = new WeakMap<PreparedObservation, Observation>();
  const live = Effect.fn("SourceForwardQualification.live")(function* (
    token: object,
    lifetime: InvocationLifetime,
  ) {
    const scope = yield* Effect.serviceOption(QualificationScope);
    const invocation = yield* Effect.serviceOption(Mcp.McpInvocationContext);
    if (
      !lifetime.active ||
      lifetimes.get(token) !== lifetime ||
      lifetime.sql !== sql ||
      Option.isNone(scope) ||
      scope.value.token !== token ||
      Option.isNone(invocation) ||
      invocation.value !== lifetime.invocation
    )
      return yield* new RegistryError({ code: "not_granted" });
    return yield* lifetime.registration
      .checkCurrent()
      .pipe(Effect.mapError((cause) => new RegistryError({ code: "not_granted", cause })));
  });
  const generationOf = Effect.fn("SourceForwardQualification.generation")(function* (
    environmentId: EnvironmentId,
  ) {
    const generation = yield* links
      .currentGeneration(environmentId)
      .pipe(Effect.mapError((cause) => new RegistryError({ code: "unavailable", cause })));
    if (Option.isNone(generation) || generation.value.length === 0 || generation.value.length > 512)
      return yield* new RegistryError({ code: "not_granted" });
    return generation.value;
  });
  const check = Effect.fn("SourceForwardQualification.checkCurrent")(function* (
    observation: Observation | undefined,
  ) {
    if (!observation) return yield* new RegistryError({ code: "not_granted" });
    const o = observation;
    const session = yield* live(o.token, o.lifetime);
    const now = DateTime.toEpochMillis(yield* DateTime.now);
    if (
      Date.parse(o.pin.expires_at) <= now ||
      !same({ ...session, ceiling: null }, { ...o.pin.session, ceiling: null }) ||
      !same(narrow(session.ceiling, o.pin.session.ceiling, o.original, o.current), o.ceiling) ||
      !Pins.ceilingAllows(o.ceiling, o.permission) ||
      (yield* generationOf(o.environmentId)) !== o.generation
    )
      return yield* new RegistryError({ code: "not_granted" });
  });
  const prepare = Effect.fn("SourceForwardQualification.prepare")(function* (
    input: Forward.WorkstreamsForwardRequest | Forward.WorkstreamsForwardReadbackRequest,
    receivingPin: WorkstreamsPeerSessionPinV1,
    readback: boolean,
  ) {
    // Remote preparation is never allowed to borrow an ambient application writer.
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* new RegistryError({ code: "preparation_in_transaction" });
    const scoped = yield* Effect.serviceOption(QualificationScope);
    const token = Option.isSome(scoped) ? scoped.value.token : undefined;
    const lifetime = token === undefined ? undefined : lifetimes.get(token);
    if (!token || !lifetime) return yield* new RegistryError({ code: "not_granted" });
    const session = yield* live(token, lifetime);
    const request = yield* (readback ? decodeReadback(input) : decodeForward(input)).pipe(
      Effect.mapError((cause) => new RegistryError({ code: "invalid_request", cause })),
    );
    const pin = structuredClone(
      yield* decodePin(receivingPin).pipe(
        Effect.mapError((cause) => new RegistryError({ code: "not_granted", cause })),
      ),
    );
    const i = request.identity;
    const permission = readback ? "workstreams:read" : "workstreams:write";
    const receivingCeiling = narrow(session.ceiling, pin.session.ceiling);
    const now = DateTime.toEpochMillis(yield* DateTime.now);
    if (
      pin.state !== "bound" ||
      !pin.roles.includes("member_home") ||
      Date.parse(pin.expires_at) <= now ||
      !same({ ...pin.session, ceiling: null }, { ...session, ceiling: null }) ||
      lifetime.invocation.environmentId !== pin.receiver_authority.environment_id ||
      pin.receiver_authority.environment_id !== home.home_environment_id ||
      home.served_by_environment_id !== home.home_environment_id ||
      !same(pin.expected_home, i.expected_home) ||
      !same(i.expected_home, {
        registry_instance_id: home.registry_instance_id,
        owner_id: home.owner_id,
        home_environment_id: home.home_environment_id,
        server_generation: home.server_generation,
      }) ||
      i.canonical_origin !== home.canonical_origin ||
      i.source_authority.environment_id === home.home_environment_id ||
      pin.peer_environment_id !== i.source_authority.environment_id ||
      !same(pin.source_authority, i.source_authority) ||
      pin.source_enrollment_id !== i.source_enrollment_id ||
      pin.source_binding_version !== i.source_binding_version ||
      pin.singleton_binding_version !== i.singleton_binding_version ||
      pin.bind_receipt?.principal_id !== i.principal_id ||
      !Pins.ceilingAllows(receivingCeiling, permission)
    )
      return yield* new RegistryError({ code: "not_granted" });
    const environmentId = yield* decodeEnvironment(pin.source_authority.environment_id).pipe(
      Effect.mapError((cause) => new RegistryError({ code: "not_granted", cause })),
    );
    // This is HOME -> SOURCE, distinct from the identity's original MEMBER -> HOME.
    const generation = yield* generationOf(environmentId);
    const lookup: Wire.WorkstreamsSourceParentReadmissionRequestV3 = {
      contract: Wire.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3,
      accepted_forward: structuredClone(i),
      receiving_home_pin_id: pin.pin_id,
      receiving_home_pin_revision: pin.revision,
    };
    const expectedLookup = canonical(lookup);
    const response = yield* peer
      .call({
        environmentId,
        expectedGeneration: generation,
        tool: "jones_workstreams_source_readmission",
        arguments: lookup,
        limits: {
          runtimeMode: receivingCeiling.runtime_mode,
          interactionMode: receivingCeiling.interaction_mode,
        },
        success: Wire.WorkstreamsSourceParentReadmissionResponseV3,
      })
      .pipe(Effect.mapError((cause) => new RegistryError({ code: "unavailable", cause })));
    // Decode again at this authority boundary, including with alternate PeerMcpClient providers.
    const result = yield* decodeResponse(response, { onExcessProperty: "error" }).pipe(
      Effect.mapError((cause) => new RegistryError({ code: "invalid_response", cause })),
    );
    yield* live(token, lifetime);
    if (
      (yield* generationOf(environmentId)) !== generation ||
      canonical(result.request) !== expectedLookup ||
      Date.parse(pin.expires_at) <= DateTime.toEpochMillis(yield* DateTime.now)
    )
      return yield* new RegistryError({ code: "invalid_response" });
    if (result.state === "denied") {
      if (readback && result.reason === "not_found")
        return { state: "unknown" as const, reason: "not_found" as const };
      return yield* new RegistryError({ code: "not_granted" });
    }
    if (result.state === "unavailable")
      return yield* new RegistryError({
        code:
          result.reason === "unsupported_protocol"
            ? "unsupported_protocol"
            : result.reason === "key_unavailable"
              ? "key_unavailable"
              : "unavailable",
      });
    if (!readback) {
      const exact = encodeForward(request as Forward.WorkstreamsForwardRequest);
      if (
        NodeCrypto.createHash("sha256").update(exact).digest("hex") !==
        result.forward_request_sha256
      )
        return yield* new RegistryError({ code: "invalid_response" });
    }
    const ceiling = narrow(receivingCeiling, result.original_ceiling, result.current_ceiling);
    if (!Pins.ceilingAllows(ceiling, permission))
      return yield* new RegistryError({ code: "not_granted" });
    const prepared: PreparedObservation = Object.freeze({
      state: "authorized" as const,
      forward_request_sha256: result.forward_request_sha256,
      // A copied method is insufficient: only the exact object is a member of this private map.
      checkCurrent(this: PreparedObservation) {
        return check(observations.get(this));
      },
    });
    observations.set(prepared, {
      token,
      lifetime,
      environmentId,
      generation,
      pin: structuredClone(pin),
      permission,
      original: structuredClone(result.original_ceiling),
      current: structuredClone(result.current_ceiling),
      ceiling,
    });
    yield* prepared.checkCurrent();
    return prepared;
  });
  return Supplied.SourceForwardQualification.of({
    prepare: Effect.fn("SourceForwardQualification.prepareWrite")(function* (request, pin) {
      const prepared = yield* prepare(request, pin, false);
      if (prepared.state !== "authorized") return yield* new RegistryError({ code: "not_granted" });
      return prepared;
    }),
    prepareReadback: (request, pin) => prepare(request, pin, true),
  });
});

/** @public Fixed V3 parent lookup; remote observation is not an invisible remote-revocation lock.
 * The local checker performs original-registration and local-generation reads only.
 * Root must compose the real current receiving authority and home boundary around every use.
 * Every independent admission/readback/recovery stage must prepare a fresh observation.
 */
export const layer = Layer.effect(Supplied.SourceForwardQualification, make);
