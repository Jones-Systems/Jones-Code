// @effect-diagnostics nodeBuiltinImport:off -- IDs are allocated only for an explicitly prepared encrypted setup request.
import * as NodeCrypto from "node:crypto";
import { EnvironmentId, WORKSTREAMS_T3_PROVIDER_PROTOCOL } from "@t3tools/contracts";
import * as W from "@t3tools/contracts/jones/workstreamsPeerSetup";
import * as F from "@t3tools/contracts/jones/workstreamsHomeBind";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as ServerConfig from "../../../../config.ts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as AuthRows from "../../../../persistence/AuthSessions.ts";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import * as Links from "../../../../peer/PeerLinks.ts";
import * as Client from "../../../../peer/PeerMcpClient.ts";
import * as Pins from "./peerSessionPins.ts";
import * as PinSetup from "./peerSessionPinSetup.ts";
import * as Owner from "./setupOwner.ts";
import * as Home from "./homeBind.ts";
import * as Member from "./memberSetup.ts";
import * as MemberQualification from "./memberPeerPinQualification.ts";
import * as Files from "./peerSetupRequestFile.ts";
import * as Keys from "../crypto/keyProvider.ts";
import * as Envelope from "../crypto/envelope.ts";
import * as Database from "../store/sql.ts";
import * as Registry from "../store/registryStore.ts";
import * as PinRecords from "../store/peerSessionPinStore.ts";
import * as MemberRecords from "../store/memberEnrollmentStore.ts";
import { canonicalWorkstreamsMemberEnrollmentIntent } from "@t3tools/contracts/jones/workstreamsMemberEnrollment";
import { sha256EnrollmentBytes as sha } from "../../enrollment/request.ts";
import type { WorkstreamsPeerSessionPinV1 as Pin } from "@t3tools/contracts/jones/workstreamsPeerAuthority";

const decodeSetupRead = Schema.decodeUnknownEffect(W.WorkstreamsPeerSetupReadRequest);
const decodePermissions = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Array(Schema.String)),
);
const decodePrepare = Schema.decodeUnknownEffect(W.WorkstreamsPeerSetupPrepare);
const decodeIssuance = Schema.decodeUnknownEffect(F.WorkstreamsSourceIssuanceRequestV1);
const decodeSetupApply = Schema.decodeUnknownEffect(W.WorkstreamsPeerSetupApplyRequest);
const decodeOwnerRequest = Schema.decodeUnknownEffect(W.WorkstreamsPeerSetupOwnerRequest);

declare const exchangeBrand: unique symbol;
export interface VerifiedSetupExchange {
  readonly [exchangeBrand]: true;
}
type ExchangeFacts = {
  readonly operation: Owner.QualifiedSetupOperation;
  readonly invocation: Mcp.McpInvocationScope;
  readonly request: W.WorkstreamsPeerSetupRequest;
  readonly local: W.WorkstreamsPeerSetupObservation;
  readonly check: Effect.Effect<void, Pins.PeerSessionError>;
};
const exchanges = new WeakMap<VerifiedSetupExchange, ExchangeFacts>();
// Only completed real H/M producers populate this map; wire receipts are never imported.
const completed = new WeakMap<VerifiedSetupExchange, F.WorkstreamsHomeBindCommittedV2>();
const same = (a: unknown, b: unknown) => canonical(a) === canonical(b);
const refuse = (code: Pins.PeerSessionError["code"]) => new Pins.PeerSessionError({ code });
const decodeRequest = Effect.fn("PeerSetup.decode")(function* (body: string) {
  return yield* Effect.try({
    try: () => {
      const r = W.decodeWorkstreamsPeerSetupRequest(body);
      MemberRecords.validateMemberEnrollment({
        schema: "jones-workstreams-member-enrollment/1",
        intent: r.intent,
        intent_sha256: sha(canonicalWorkstreamsMemberEnrollmentIntent(r.intent)),
        state: "prepared",
        revision: 1,
        home_result: null,
        local_binding: null,
      });
      if (r.intent.home_setup.pin.setup_request_id !== r.setup_id) throw refuse("invalid_request");
      return r;
    },
    catch: () => refuse("invalid_request"),
  });
});
const address = (r: W.WorkstreamsPeerSetupAddress): W.WorkstreamsPeerSetupAddress => ({
  setup_id: r.setup_id,
  expected_home: r.expected_home,
  source_environment_id: r.source_environment_id,
});
const identity = (
  r: W.WorkstreamsPeerSetupRequest,
  bytes: string,
): W.WorkstreamsPeerSetupIdentity => ({ ...address(r), request_sha256: sha(bytes) });
const identityFields = (r: W.WorkstreamsPeerSetupIdentity): W.WorkstreamsPeerSetupIdentity => ({
  ...address(r),
  request_sha256: r.request_sha256,
});
const summary = (
  r: W.WorkstreamsPeerSetupRequest,
  bytes: string,
  state: W.WorkstreamsPeerSetupResult["state"],
): W.WorkstreamsPeerSetupResult => {
  const b = F.decodeWorkstreamsHomeBindBody(r.intent.bind_request_body);
  return {
    contract: W.WORKSTREAMS_PEER_SETUP_CONTRACT,
    ...identity(r, bytes),
    state,
    enrollment_id: b.source.enrollment.enrollment_id,
    bind_id: b.bind_id,
  };
};
const stableObservation = (o: W.WorkstreamsPeerSetupObservation) => ({
  owner: o.owner,
  session: o.session,
  outbound_link_generation: o.outbound_link_generation,
});
/** Private membership, exact retained bytes, and actual final invocation are all required. */
export const makePeerSetupQualification = Effect.fn("PeerSetup.qualification")(function* (
  operation: Owner.QualifiedSetupOperation,
  invocation: Mcp.McpInvocationScope,
  exchange: VerifiedSetupExchange,
) {
  const e = exchanges.get(exchange);
  if (!e || e.operation !== operation || e.invocation !== invocation)
    return yield* refuse("owner_unqualified");
  yield* e.check;
  const b = F.decodeWorkstreamsHomeBindBody(e.request.intent.bind_request_body);
  const h = invocation.environmentId === b.expected_home.home_environment_id;
  const setupBody = h
    ? e.request.intent.home_setup.setup_request_body
    : e.request.intent.member_setup_request_body;
  const original = PinRecords.decodeSetupRequest(setupBody).pin;
  const qualify = Effect.fn("PeerSetup.qualifyExact")(function* (bytes: string, digest: string) {
    if (
      !(
        (bytes === setupBody || bytes === e.request.intent.bind_request_body) &&
        sha(bytes) === digest
      )
    )
      return yield* refuse("owner_unqualified");
    yield* e.check;
    return {
      invocation,
      owner_qualification_digest: e.local.owner.qualification_sha256,
      checkCurrent: () => e.check,
    };
  });
  // The H/M services use their existing private witnesses for actual committed postconditions.
  // A caller with JSON alone cannot obtain this exchange or use this port to import a receipt.
  const verifyBinding = Effect.fn("PeerSetup.verifyBinding")(function* (
    pin: Pin,
    receipt: NonNullable<Pin["bind_receipt"]>,
    singletonVersion: number,
  ) {
    yield* e.check;
    const witness = completed.get(exchange);
    if (
      !witness ||
      pin.state !== "bound" ||
      witness.request_body !== e.request.intent.bind_request_body ||
      !same(witness.receipt, receipt)
    )
      return yield* refuse("owner_unqualified");
    const expectedReceipt = {
      principal_id: b.expected.principal_id,
      binding_version: (b.expected.source_binding?.binding_version ?? 0) + 1,
      registry_version: b.expected.registry_version + 1,
      request_sha256: sha(e.request.intent.bind_request_body),
    };
    const expected = {
      ...original,
      state: "bound",
      revision: 2,
      source_binding_version: receipt.binding_version,
      singleton_binding_version: singletonVersion,
      bind_receipt: receipt,
    };
    if (
      !same(pin, expected) ||
      !same(receipt, expectedReceipt) ||
      singletonVersion !== b.member_setup.singleton_binding_version
    )
      return yield* refuse("conflict");
  });
  return PinSetup.PeerPinSetupQualification.of({ qualify, verifyBinding });
});

export class PeerSetup extends Context.Service<
  PeerSetup,
  {
    readonly prepare: (
      input: W.WorkstreamsPeerSetupPrepare,
    ) => Effect.Effect<W.WorkstreamsPeerSetupResult, Pins.PeerSessionError>;
    readonly read: (
      input: W.WorkstreamsPeerSetupReadRequest,
      invocation: Mcp.McpInvocationScope,
    ) => Effect.Effect<W.WorkstreamsPeerSetupReadResult, Pins.PeerSessionError>;
    readonly apply: (
      input: W.WorkstreamsPeerSetupApplyRequest,
      invocation: Mcp.McpInvocationScope,
    ) => Effect.Effect<W.WorkstreamsPeerSetupResult, Pins.PeerSessionError>;
    readonly ownerOperation: (
      input: W.WorkstreamsPeerSetupOwnerRequest,
    ) => Effect.Effect<W.WorkstreamsPeerSetupResult, Pins.PeerSessionError>;
    readonly homeBind: Home.HomeBind["Service"];
  }
>()("t3/jones/workstreams/native/runtime/peerSetup") {}

type SetupDependencies =
  | SqlClient.SqlClient
  | Pins.NativePeerSessions
  | Links.PeerLinks
  | Client.PeerMcpClient
  | AuthRows.AuthSessionRepository
  | Secrets.ServerSecretStore
  | Keys.WorkstreamsKeyProvider
  | Registry.RegistryStore;
const make = Effect.gen(function* () {
  const shared = yield* Effect.context<SetupDependencies>();
  const run = <A, E>(effect: Effect.Effect<A, E, SetupDependencies>) =>
    effect.pipe(Effect.provide(shared), Effect.mapError(Pins.mapFailure));
  const owner = yield* Owner.SetupOwner,
    native = yield* Pins.NativePeerSessions,
    links = yield* Links.PeerLinks,
    client = yield* Client.PeerMcpClient,
    sql = yield* SqlClient.SqlClient,
    files = yield* Files.PeerSetupRequestFiles,
    keys = yield* Keys.WorkstreamsKeyProvider;
  const observed = new Map<
    string,
    { invocation: Mcp.McpInvocationScope; observation: W.WorkstreamsPeerSetupObservation }
  >();
  const refresh = () =>
    owner.current.pipe(Effect.flatMap(Owner.refreshSetupOwner), Effect.mapError(Pins.mapFailure));
  const facts = (op: Owner.QualifiedSetupOperation) =>
    Owner.setupOperationFacts(op).pipe(Effect.mapError(Pins.mapFailure));
  const generation = Effect.fn("PeerSetup.link")(function* (peer: string) {
    const g = yield* links
      .currentGeneration(EnvironmentId.make(peer))
      .pipe(Effect.mapError(Pins.mapFailure));
    if (Option.isNone(g)) return yield* refuse("not_current");
    return g.value;
  });
  const call = <S extends Schema.Decoder<unknown>>(
    peer: string,
    gen: string,
    tool: string,
    args: Record<string, unknown>,
    success: S,
    ceiling: Pin["session"]["ceiling"],
  ) =>
    client
      .call({
        environmentId: EnvironmentId.make(peer),
        expectedGeneration: gen,
        tool,
        arguments: args,
        limits: { runtimeMode: ceiling.runtime_mode, interactionMode: ceiling.interaction_mode },
        success,
      })
      .pipe(Effect.mapError(Pins.mapFailure));
  const owned = Effect.fn("PeerSetup.observe")(function* (
    op: Owner.QualifiedSetupOperation,
    a: W.WorkstreamsPeerSetupAddress,
    invocation: Mcp.McpInvocationScope,
  ) {
    const f = yield* facts(op),
      n = yield* native.current(invocation);
    const env = f.descriptor.authority.environment_id;
    if (
      invocation.environmentId !== env ||
      a.source_environment_id === a.expected_home.home_environment_id ||
      ![a.source_environment_id, a.expected_home.home_environment_id].includes(env) ||
      !Pins.ceilingAllows(n.session.ceiling, "workstreams:write")
    )
      return yield* refuse("authority_mismatch");
    const peer =
      env === a.source_environment_id
        ? a.expected_home.home_environment_id
        : a.source_environment_id;
    const rows = yield* sql<{
      binding_version: number;
    }>`SELECT binding_version FROM jones_workstreams_binding_state WHERE singleton=1`.pipe(
      Effect.mapError(Pins.mapFailure),
    );
    const observation: W.WorkstreamsPeerSetupObservation = {
      owner: f.descriptor,
      session: n.session,
      outbound_link_generation: yield* generation(peer),
      singleton_version: rows[0]?.binding_version ?? null,
    };
    // This short-lived rendezvous only completes a currently waiting prepare call.
    if (observed.size >= 64) observed.delete(observed.keys().next().value!);
    observed.set(a.setup_id, { invocation, observation });
    return observation;
  });
  const localCurrent = Effect.fn("PeerSetup.localCurrent")(function* (
    op: Owner.QualifiedSetupOperation,
    invocation: Mcp.McpInvocationScope,
    r: W.WorkstreamsPeerSetupRequest,
  ) {
    const f = yield* facts(op),
      session = yield* native.current(invocation);
    const atHome = invocation.environmentId === r.expected_home.home_environment_id;
    const local = atHome ? r.home_observation : r.member_observation;
    const remote = atHome ? r.member_observation : r.home_observation;
    if (
      !same(f.descriptor, local.owner) ||
      !same(session.session, local.session) ||
      remote.owner.authority.environment_id !==
        (atHome ? r.source_environment_id : r.expected_home.home_environment_id)
    )
      return yield* refuse("owner_unqualified");
    if (
      (yield* generation(remote.owner.authority.environment_id)) !== local.outbound_link_generation
    )
      return yield* refuse("not_current");
    return local;
  });
  const retained = Effect.fn("PeerSetup.retained")(function* (id: W.WorkstreamsPeerSetupIdentity) {
    const bytes = yield* files.read(id).pipe(Effect.mapError(Pins.mapFailure));
    if (bytes === null || sha(bytes) !== id.request_sha256) return yield* refuse("conflict");
    const r = yield* decodeRequest(bytes);
    if (!same(identity(r, bytes), identityFields(id))) return yield* refuse("conflict");
    return { r, bytes };
  });
  const exchange = Effect.fn("PeerSetup.exchange")(function* (
    op: Owner.QualifiedSetupOperation,
    invocation: Mcp.McpInvocationScope,
    r: W.WorkstreamsPeerSetupRequest,
  ) {
    const local = yield* localCurrent(op, invocation, r);
    const token = Object.freeze({}) as VerifiedSetupExchange;
    exchanges.set(token, {
      operation: op,
      invocation,
      request: r,
      local,
      check: localCurrent(op, invocation, r).pipe(Effect.asVoid),
    });
    return token;
  });
  // All secret acquisition precedes H's preparation writer; cache misses fail closed.
  const pinLayer = Effect.fn("PeerSetup.pinLayer")(function* (
    op: Owner.QualifiedSetupOperation,
    r: W.WorkstreamsPeerSetupRequest,
    port: PinSetup.PeerPinSetupQualification["Service"],
  ) {
    const scope = { ...r.expected_home, key_scope: "local" as const };
    const active = yield* keys.activeKeyId(scope).pipe(Effect.mapError(Pins.mapFailure));
    const bytes = yield* keys.get(active).pipe(Effect.mapError(Pins.mapFailure));
    const cached = Layer.succeed(Keys.WorkstreamsKeyProvider, {
      activeKeyId: (s) =>
        keys
          .activeKeyId(s)
          .pipe(
            Effect.flatMap((id) =>
              id === active &&
              s.key_scope === "local" &&
              s.registry_instance_id === scope.registry_instance_id &&
              s.owner_id === scope.owner_id
                ? Effect.succeed(id)
                : Effect.fail(new Keys.KeyUnavailable({ reason: "missing" })),
            ),
          ),
      get: (id) =>
        id === active
          ? Effect.succeed(Uint8Array.from(bytes))
          : Effect.fail(new Keys.KeyUnavailable({ reason: "missing" })),
      indexKey: () => Effect.fail(new Keys.KeyUnavailable({ reason: "missing" })),
    });
    const db = Database.layer.pipe(
      Layer.provideMerge(Envelope.layer.pipe(Layer.provideMerge(cached))),
    );
    const stores = Layer.mergeAll(PinRecords.layer, Registry.layer).pipe(Layer.provideMerge(db));
    return PinSetup.layer.pipe(
      Layer.provideMerge(stores),
      Layer.provide(Owner.setupOwnerLayer(op)),
      Layer.provide(Layer.succeed(PinSetup.PeerPinSetupQualification, port)),
    );
  });
  const read = Effect.fn("PeerSetup.read")(function* (
    input: W.WorkstreamsPeerSetupReadRequest,
    invocation: Mcp.McpInvocationScope,
  ) {
    const request = yield* decodeSetupRead(input).pipe(
      Effect.mapError(() => refuse("invalid_request")),
    );
    const op = yield* refresh(),
      f = yield* facts(op);
    if (request.receiver_environment_id !== f.descriptor.authority.environment_id)
      return yield* refuse("authority_mismatch");
    const local = yield* owned(op, request, invocation);
    if (request.operation === "source_intent") {
      if (request.receiver_environment_id !== request.source_environment_id)
        return yield* refuse("wrong_role");
      const { r, bytes } = yield* retained(request);
      yield* localCurrent(op, invocation, r);
      return {
        contract: W.WORKSTREAMS_PEER_SETUP_CONTRACT as typeof W.WORKSTREAMS_PEER_SETUP_CONTRACT,
        operation: "source_intent" as const,
        request,
        request_body: bytes,
      };
    }
    if (request.receiver_environment_id !== request.expected_home.home_environment_id)
      return {
        contract: W.WORKSTREAMS_PEER_SETUP_CONTRACT as typeof W.WORKSTREAMS_PEER_SETUP_CONTRACT,
        operation: "context" as const,
        request,
        local,
        reverse: null,
        home: null,
      };
    const reverseRequest = { ...request, receiver_environment_id: request.source_environment_id };
    const reverse = yield* call(
      request.source_environment_id,
      local.outbound_link_generation,
      "jones_workstreams_peer_setup_read",
      reverseRequest,
      W.WorkstreamsPeerSetupContext,
      local.session.ceiling,
    );
    if (
      !same(reverse.request, reverseRequest) ||
      reverse.home !== null ||
      reverse.reverse !== null ||
      reverse.local.owner.authority.environment_id !== request.source_environment_id
    )
      return yield* refuse("authority_mismatch");
    const registry = yield* Registry.RegistryStore;
    const c = yield* registry.context(request.expected_home).pipe(Effect.mapError(Pins.mapFailure));
    const principals = (yield* registry
      .listPlain(request.expected_home, "principals")
      .pipe(Effect.mapError(Pins.mapFailure))).filter(
      (p) => p.kind === "environment" && p.environment_id === request.source_environment_id,
    );
    if (
      !c ||
      c.state !== "active" ||
      c.home_environment_id !== request.expected_home.home_environment_id ||
      c.server_generation !== request.expected_home.server_generation ||
      principals.length !== 1
    )
      return yield* refuse("conflict");
    const principal = principals[0]!;
    const grant = (yield* registry
      .listPlain(request.expected_home, "grants")
      .pipe(Effect.mapError(Pins.mapFailure)))
      .filter((g) => g.principal_id === principal.principal_id)
      .toSorted((a, b) => Number(b.grant_version) - Number(a.grant_version))[0];
    const permissions = yield* decodePermissions(String(grant?.permissions)).pipe(
      Effect.mapError(() => refuse("conflict")),
    );
    if (
      !grant ||
      grant.state !== "active" ||
      !Array.isArray(permissions) ||
      !permissions.includes("workstreams:write") ||
      !permissions.includes("workstreams:read")
    )
      return yield* refuse("conflict");
    const binding = (yield* registry
      .listPlain(request.expected_home, "source_bindings")
      .pipe(Effect.mapError(Pins.mapFailure)))
      .filter((b) => b.provider === "t3" && b.source_instance_id === request.source_environment_id)
      .toSorted((a, b) => Number(b.binding_version) - Number(a.binding_version))[0];
    yield* facts(op);
    return {
      contract: W.WORKSTREAMS_PEER_SETUP_CONTRACT as typeof W.WORKSTREAMS_PEER_SETUP_CONTRACT,
      operation: "context" as const,
      request,
      local,
      reverse: reverse.local,
      home: {
        canonical_origin: c.canonical_origin,
        expected: {
          principal_id: String(principal.principal_id),
          registry_version: c.registry_version,
          grant_id: String(grant.grant_id),
          grant_version: Number(grant.grant_version),
          source_binding: binding
            ? {
                binding_version: Number(binding.binding_version),
                authority_namespace: String(binding.authority_namespace),
                store_generation: Number(binding.store_generation),
                state: binding.state as "active" | "revoked",
              }
            : null,
        },
      },
    };
  });
  const prepare = Effect.fn("PeerSetup.prepare")(function* (input: W.WorkstreamsPeerSetupPrepare) {
    const request = yield* decodePrepare(input).pipe(
      Effect.mapError(() => refuse("invalid_request")),
    );
    const op = yield* refresh(),
      f = yield* facts(op);
    if (f.descriptor.authority.environment_id !== request.source_environment_id)
      return yield* refuse("wrong_role");
    const prior = yield* files.read(request).pipe(Effect.mapError(Pins.mapFailure));
    if (prior !== null) return summary(yield* decodeRequest(prior), prior, "retained");
    const gen = yield* generation(request.expected_home.home_environment_id);
    const contextRequest = {
      contract: W.WORKSTREAMS_PEER_SETUP_CONTRACT,
      operation: "context" as const,
      ...address(request),
      receiver_environment_id: request.expected_home.home_environment_id,
    };
    const context = yield* call(
      request.expected_home.home_environment_id,
      gen,
      "jones_workstreams_peer_setup_read",
      contextRequest,
      W.WorkstreamsPeerSetupContext,
      {
        access: "full-access",
        runtime_mode: "full-access",
        interaction_mode: "default",
        scopes: ["orchestration:read", "orchestration:operate"],
        permissions: ["workstreams:read", "workstreams:write"],
      },
    );
    const reverse = observed.get(request.setup_id);
    observed.delete(request.setup_id);
    if (
      !same(context.request, contextRequest) ||
      !context.home ||
      !context.reverse ||
      !reverse ||
      !same(context.reverse, reverse.observation) ||
      !same(context.reverse.owner, f.descriptor) ||
      context.reverse.outbound_link_generation !== gen
    )
      return yield* refuse("owner_unqualified");
    yield* native.current(reverse.invocation);
    const now = DateTime.toEpochMillis(yield* DateTime.now),
      enrollment = NodeCrypto.randomUUID();
    const issuance: F.WorkstreamsSourceIssuanceRequestV1 = yield* decodeIssuance({
      schema: "jones-code.workstreams-native-enrollment/v1",
      enrollment_id: enrollment,
      registry_origin: context.home.canonical_origin,
      context: {
        owner_id: request.expected_home.owner_id,
        principal_id: context.home.expected.principal_id,
        source_instance_id: request.source_environment_id,
        authority_namespace: f.descriptor.authority.authority_namespace,
        store_generation: f.descriptor.authority.store_generation,
        enrollment_id: enrollment,
        protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
        build: f.descriptor.build,
      },
      session: {
        session_id: NodeCrypto.randomUUID(),
        issued_at: DateTime.formatIso(DateTime.makeUnsafe(now)),
        expires_at: DateTime.formatIso(DateTime.makeUnsafe(now + request.lifetime_days * 86400000)),
      },
    }).pipe(Effect.mapError(() => refuse("invalid_request")));
    const expiry = DateTime.formatIso(
      DateTime.makeUnsafe(
        Math.min(
          now + 20 * 60000,
          Date.parse(context.local.session.expires_at),
          Date.parse(context.reverse.session.expires_at),
        ),
      ),
    );
    const setup = (home: boolean) =>
      canonical({
        schema: "jones-workstreams-peer-pin-setup/1",
        assertion: {
          schema: "jones-workstreams-peer-session-pin/1",
          pin_id: NodeCrypto.randomUUID(),
          revision: 1,
          receiver_authority: home ? context.local.owner.authority : f.descriptor.authority,
          session: home ? context.local.session : context.reverse!.session,
          peer_environment_id: home
            ? request.source_environment_id
            : request.expected_home.home_environment_id,
          roles: [home ? "member_home" : "home_source"],
          expected_home: request.expected_home,
          source_authority: f.descriptor.authority,
          source_enrollment_id: enrollment,
          source_binding_version: null,
          singleton_binding_version: null,
          state: "prepared",
          expires_at: expiry,
          setup_request_id: home ? request.setup_id : NodeCrypto.randomUUID(),
          owner_qualification_digest: home
            ? context.local.owner.qualification_sha256
            : f.descriptor.qualification_sha256,
          bind_receipt: null,
        },
        expected: {
          registry_version: context.home!.expected.registry_version,
          grant_id: context.home!.expected.grant_id,
          grant_version: context.home!.expected.grant_version,
        },
      });
    const homeBody = setup(true),
      memberBody = setup(false);
    const h = PinRecords.decodeSetupRequest(homeBody).pin,
      m = PinRecords.decodeSetupRequest(memberBody).pin;
    const bindBody = F.canonicalWorkstreamsHomeBindBody({
      bind_id: NodeCrypto.randomUUID(),
      expected_home: request.expected_home,
      expected_canonical_origin: context.home.canonical_origin,
      expected_home_authority: context.local.owner.authority,
      source: { authority: f.descriptor.authority, enrollment: issuance },
      home_setup: {
        pin_id: h.pin_id,
        prepared_revision: 1,
        setup_request_id: h.setup_request_id,
        setup_request_sha256: sha(homeBody),
      },
      member_setup: {
        pin_id: m.pin_id,
        prepared_revision: 1,
        setup_request_id: m.setup_request_id,
        outbound_link_generation: gen,
        expected_singleton_version: context.reverse.singleton_version,
        singleton_binding_version: (context.reverse.singleton_version ?? 0) + 1,
      },
      expected: context.home.expected,
      rebind: context.home.expected.source_binding !== null,
    });
    const r: W.WorkstreamsPeerSetupRequest = {
      contract: W.WORKSTREAMS_PEER_SETUP_CONTRACT,
      ...address(request),
      home_observation: context.local,
      member_observation: context.reverse,
      intent: {
        issuance_body: canonical(issuance),
        bind_request_body: bindBody,
        bind_request_sha256: sha(bindBody),
        member_setup_request_body: memberBody,
        member_setup_request_sha256: sha(memberBody),
        home_setup: {
          contract: F.WORKSTREAMS_HOME_BIND_CONTRACT,
          kind: "prepared",
          request: {
            contract: F.WORKSTREAMS_HOME_BIND_CONTRACT,
            expected_home: request.expected_home,
            home_setup_request_id: h.setup_request_id,
          },
          canonical_origin: context.home.canonical_origin,
          home_authority: context.local.owner.authority,
          setup_request_body: homeBody,
          pin: h,
        },
      },
    };
    const bytes = canonical(r);
    yield* decodeRequest(bytes);
    yield* facts(op);
    yield* files.retain(bytes).pipe(Effect.mapError(Pins.mapFailure));
    return summary(r, bytes, "retained");
  });
  const homeExchange = Effect.fn("PeerSetup.homeExchange")(function* (
    op: Owner.QualifiedSetupOperation,
    request: W.WorkstreamsPeerSetupIdentity,
    invocation: Mcp.McpInvocationScope,
  ) {
    const f = yield* facts(op),
      n = yield* native.current(invocation);
    if (f.descriptor.authority.environment_id !== request.expected_home.home_environment_id)
      return yield* refuse("wrong_role");
    const readRequest = {
      contract: W.WORKSTREAMS_PEER_SETUP_CONTRACT,
      operation: "source_intent" as const,
      ...identityFields(request),
      receiver_environment_id: request.source_environment_id,
    };
    const response = yield* call(
      request.source_environment_id,
      yield* generation(request.source_environment_id),
      "jones_workstreams_peer_setup_read",
      readRequest,
      W.WorkstreamsPeerSetupReadResult,
      n.session.ceiling,
    );
    if (
      response.operation !== "source_intent" ||
      !same(response.request, readRequest) ||
      sha(response.request_body) !== request.request_sha256
    )
      return yield* refuse("authority_mismatch");
    const r = yield* decodeRequest(response.request_body);
    if (!same(identity(r, response.request_body), identityFields(request)))
      return yield* refuse("conflict");
    const verified = yield* exchange(op, invocation, r);
    yield* files.retain(response.request_body).pipe(Effect.mapError(Pins.mapFailure));
    return { r, bytes: response.request_body, verified };
  });
  const apply = Effect.fn("PeerSetup.apply")(function* (
    input: W.WorkstreamsPeerSetupApplyRequest,
    invocation: Mcp.McpInvocationScope,
  ) {
    const request = yield* decodeSetupApply(input).pipe(
      Effect.mapError(() => refuse("invalid_request")),
    );
    const op = yield* refresh(),
      f = yield* facts(op);
    if (request.receiver_environment_id !== f.descriptor.authority.environment_id)
      return yield* refuse("authority_mismatch");
    yield* native.current(invocation);
    if (request.receiver_environment_id === request.expected_home.home_environment_id) {
      if (request.operation === "accept_member") return yield* refuse("wrong_role");
      const { r, bytes, verified } = yield* homeExchange(op, request, invocation);
      const port = yield* makePeerSetupQualification(op, invocation, verified);
      if (request.operation === "prepare_home") {
        const first = yield* files
          .begin(identity(r, bytes), "home_prepare")
          .pipe(Effect.mapError(Pins.mapFailure));
        const layer = yield* pinLayer(op, r, port);
        yield* Effect.flatMap(PinSetup.PeerSessionPinSetup, (s) =>
          first
            ? s.apply(r.intent.home_setup.setup_request_body, 0)
            : s
                .readback(r.setup_id)
                .pipe(
                  Effect.flatMap((record) =>
                    record ? Effect.succeed(record.pin) : Effect.fail(refuse("unavailable")),
                  ),
                ),
        ).pipe(Effect.provide(layer));
      }
      // H's transaction ends before M is entered through genuine H->M native authentication.
      const response = yield* call(
        r.source_environment_id,
        r.home_observation.outbound_link_generation,
        "jones_workstreams_peer_setup_apply",
        {
          ...request,
          operation: request.operation === "resume_member" ? "resume_member" : "accept_member",
          receiver_environment_id: r.source_environment_id,
        },
        W.WorkstreamsPeerSetupResult,
        r.member_observation.session.ceiling,
      );
      if (!same(identityFields(response), identity(r, bytes))) return yield* refuse("conflict");
      return response;
    }
    if (
      request.receiver_environment_id !== request.source_environment_id ||
      request.operation === "prepare_home"
    )
      return yield* refuse("wrong_role");
    const { r, bytes } = yield* retained(request),
      verified = yield* exchange(op, invocation, r);
    const port = yield* makePeerSetupQualification(op, invocation, verified);
    const q = MemberQualification.layer.pipe(
      Layer.provide(Owner.setupOwnerLayer(op)),
      Layer.provide(Layer.succeed(PinSetup.PeerPinSetupQualification, port)),
    );
    const layer = Member.layer.pipe(
      Layer.provideMerge(q),
      Layer.provide(Owner.setupOwnerLayer(op)),
    );
    const first =
      request.operation === "accept_member"
        ? yield* files
            .begin(identity(r, bytes), "member_apply")
            .pipe(Effect.mapError(Pins.mapFailure))
        : false;
    const result = yield* Effect.flatMap(Member.MemberSetup, (s) =>
      first ? s.apply(r.intent) : s.readback(r.intent),
    ).pipe(Effect.provide(layer), Effect.mapError(Pins.mapFailure));
    if (result) completed.set(verified, result.home_result);
    return summary(r, bytes, result ? "bound" : "unknown");
  });
  const homeFor = Effect.fn("PeerSetup.homeFor")(function* (invocation: Mcp.McpInvocationScope) {
    const op = yield* refresh(),
      n = yield* native.current(invocation);
    const db = Database.layer.pipe(Layer.provideMerge(Envelope.layer));
    const record = yield* Effect.flatMap(PinRecords.PeerSessionPinStore, (s) =>
      s.read({ session_id: n.session.session_id }),
    ).pipe(
      Effect.provide(PinRecords.layer.pipe(Layer.provide(db))),
      Effect.mapError(Pins.mapFailure),
    );
    if (!record) return yield* refuse("not_current");
    const a = {
      setup_id: record.pin.setup_request_id,
      expected_home: record.pin.expected_home,
      source_environment_id: record.pin.source_authority.environment_id,
    };
    const local = yield* files.read(a).pipe(Effect.mapError(Pins.mapFailure));
    if (!local) return yield* refuse("owner_unqualified");
    const r = yield* decodeRequest(local);
    const { verified } = yield* homeExchange(op, identity(r, local), invocation);
    const port = yield* makePeerSetupQualification(op, invocation, verified);
    return {
      verified,
      request: r,
      bytes: local,
      layer: Home.layer.pipe(
        Layer.provide(Owner.setupOwnerLayer(op)),
        Layer.provide(Layer.succeed(PinSetup.PeerPinSetupQualification, port)),
      ),
    };
  });
  const homeBind = {
    setupReadback: (
      r: F.WorkstreamsHomeBindSetupReadbackRequestV2,
      invocation: Mcp.McpInvocationScope,
    ) =>
      Effect.flatMap(homeFor(invocation), ({ layer }) =>
        Effect.flatMap(Home.HomeBind, (s) => s.setupReadback(r, invocation)).pipe(
          Effect.provide(layer),
        ),
      ),
    bind: (r: F.WorkstreamsHomeBindRequestV2, invocation: Mcp.McpInvocationScope) =>
      Effect.flatMap(homeFor(invocation), (f) =>
        Effect.gen(function* () {
          if (r.request_body !== f.request.intent.bind_request_body)
            return yield* refuse("conflict");
          const first = yield* files
            .begin(identity(f.request, f.bytes), "home_bind")
            .pipe(Effect.mapError(Pins.mapFailure));
          const result = yield* Effect.flatMap(Home.HomeBind, (s) =>
            first
              ? s.bind(r, invocation)
              : s
                  .readback(
                    {
                      contract: F.WORKSTREAMS_HOME_BIND_CONTRACT,
                      expected_home: f.request.expected_home,
                      bind_id: F.decodeWorkstreamsHomeBindBody(r.request_body).bind_id,
                      request_sha256: sha(r.request_body),
                    },
                    invocation,
                  )
                  .pipe(
                    Effect.flatMap((value) =>
                      value.kind === "committed"
                        ? Effect.succeed(value.result)
                        : Effect.fail(refuse("unavailable")),
                    ),
                  ),
          ).pipe(Effect.provide(f.layer));
          completed.set(f.verified, result);
          return result;
        }),
      ),
    readback: (r: F.WorkstreamsHomeBindReadbackRequestV2, invocation: Mcp.McpInvocationScope) =>
      Effect.flatMap(homeFor(invocation), ({ layer }) =>
        Effect.flatMap(Home.HomeBind, (s) => s.readback(r, invocation)).pipe(Effect.provide(layer)),
      ),
  };
  const ownerOperation = Effect.fn("PeerSetup.ownerOperation")(function* (
    input: W.WorkstreamsPeerSetupOwnerRequest,
  ) {
    const request = yield* decodeOwnerRequest(input).pipe(
      Effect.mapError(() => refuse("invalid_request")),
    );
    if (request.operation === "prepare")
      return yield* prepare({ ...address(request), lifetime_days: request.lifetime_days });
    const op = yield* refresh(),
      f = yield* facts(op),
      { r, bytes } = yield* retained(request);
    if (!same(f.descriptor, r.member_observation.owner)) return yield* refuse("owner_unqualified");
    if (request.operation === "status") {
      const b = F.decodeWorkstreamsHomeBindBody(r.intent.bind_request_body);
      const rows = yield* sql<{
        state: string;
      }>`SELECT state FROM jones_workstreams_member_enrollments WHERE enrollment_id=${b.source.enrollment.enrollment_id} AND intent_sha256=${sha(canonical(r.intent))}`.pipe(
        Effect.mapError(Pins.mapFailure),
      );
      const attempted = yield* files
        .attempted(identity(r, bytes), "owner_apply")
        .pipe(Effect.mapError(Pins.mapFailure));
      return summary(
        r,
        bytes,
        rows[0]?.state === "bound"
          ? "bound"
          : attempted
            ? "unknown"
            : rows[0]?.state === "prepared"
              ? "prepared"
              : "retained",
      );
    }
    // Re-establish both final native scopes on every apply/resume, including after restart.
    const contextRequest = {
      contract: W.WORKSTREAMS_PEER_SETUP_CONTRACT,
      operation: "context" as const,
      ...address(r),
      receiver_environment_id: r.expected_home.home_environment_id,
    };
    const context = yield* call(
      r.expected_home.home_environment_id,
      r.member_observation.outbound_link_generation,
      "jones_workstreams_peer_setup_read",
      contextRequest,
      W.WorkstreamsPeerSetupContext,
      r.home_observation.session.ceiling,
    );
    if (
      !context.reverse ||
      !same(stableObservation(context.local), stableObservation(r.home_observation)) ||
      !same(stableObservation(context.reverse), stableObservation(r.member_observation))
    )
      return yield* refuse("not_current");
    const first =
      request.operation === "apply"
        ? yield* files
            .begin(identity(r, bytes), "owner_apply")
            .pipe(Effect.mapError(Pins.mapFailure))
        : false;
    const reply = yield* call(
      r.expected_home.home_environment_id,
      r.member_observation.outbound_link_generation,
      "jones_workstreams_peer_setup_apply",
      {
        contract: W.WORKSTREAMS_PEER_SETUP_CONTRACT,
        ...identity(r, bytes),
        receiver_environment_id: r.expected_home.home_environment_id,
        operation: first ? "prepare_home" : "resume_member",
      },
      W.WorkstreamsPeerSetupResult,
      r.home_observation.session.ceiling,
    );
    if (!same(identityFields(reply), identity(r, bytes))) return yield* refuse("conflict");
    return reply;
  });
  return PeerSetup.of({
    prepare: (r) => run(prepare(r)),
    read: (r, i) => run(read(r, i)),
    apply: (r, i) => run(apply(r, i)),
    ownerOperation: (r) => run(ownerOperation(r)),
    homeBind: {
      setupReadback: (r, i) => run(homeBind.setupReadback(r, i)),
      bind: (r, i) => run(homeBind.bind(r, i)),
      readback: (r, i) => run(homeBind.readback(r, i)),
    },
  });
});
/** @public Protected setup readiness is independent of ordinary member startup readiness. */
export const layer = Layer.effect(PeerSetup, make);

/** Production application composition, independent of the ordinary native startup selection.
 * The shared native authentication, selected SQL, keys, links and client are required ports. */
export const runtimeLayer = Layer.unwrap(
  Effect.map(ServerConfig.ServerConfig, (config) =>
    layer.pipe(Layer.provideMerge(Files.layer(config.baseDir)), Layer.provide(Owner.runtimeLayer)),
  ),
);
