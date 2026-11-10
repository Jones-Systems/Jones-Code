import * as F from "@t3tools/contracts/jones/workstreamsHomeBind";
import type { WorkstreamsNativeHomeTarget as Home } from "@t3tools/contracts/jones/workstreamsNative";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import { sha256EnrollmentBytes as digest } from "../../enrollment/request.ts";
import * as Keys from "../crypto/keyProvider.ts";
import * as Envelope from "../crypto/envelope.ts";
import * as Database from "../store/sql.ts";
import * as Registry from "../store/registryStore.ts";
import * as Records from "../store/homeBindReceiptStore.ts";
import * as PinRecords from "../store/peerSessionPinStore.ts";
import * as Pins from "./peerSessionPins.ts";
import * as Setup from "./peerSessionPinSetup.ts";
import * as Owner from "./sourceEnrollment.ts";
import * as Qualification from "./homePeerPinQualification.ts";

export class HomeBind extends Context.Service<
  HomeBind,
  {
    readonly setupReadback: (
      request: F.WorkstreamsHomeBindSetupReadbackRequestV2,
      invocation: Mcp.McpInvocationScope,
    ) => Effect.Effect<F.WorkstreamsHomeBindSetupReadbackV2, Pins.PeerSessionError>;
    readonly bind: (
      request: F.WorkstreamsHomeBindRequestV2,
      invocation: Mcp.McpInvocationScope,
    ) => Effect.Effect<F.WorkstreamsHomeBindCommittedV2, Pins.PeerSessionError>;
    readonly readback: (
      request: F.WorkstreamsHomeBindReadbackRequestV2,
      invocation: Mcp.McpInvocationScope,
    ) => Effect.Effect<F.WorkstreamsHomeBindReadbackV2, Pins.PeerSessionError>;
  }
>()("t3/jones/workstreams/native/runtime/homeBind") {}
const same = (a: unknown, b: unknown) => canonical(a) === canonical(b);
const conflict = () => new Pins.PeerSessionError({ code: "conflict" });
const decodeSetup = Schema.decodeUnknownEffect(F.WorkstreamsHomeBindSetupReadbackRequestV2);
const decodeSetupResult = Schema.decodeUnknownEffect(F.WorkstreamsHomeBindSetupReadbackV2);
const decodeBind = Schema.decodeUnknownEffect(F.WorkstreamsHomeBindRequestV2);
const decodeCommitted = Schema.decodeUnknownEffect(F.WorkstreamsHomeBindCommittedV2);
const decodeReadback = Schema.decodeUnknownEffect(F.WorkstreamsHomeBindReadbackRequestV2);
const decodeReadbackResult = Schema.decodeUnknownEffect(F.WorkstreamsHomeBindReadbackV2);
const decodePermissions = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Array(Schema.String)),
);
const decodeEnvelopeKey = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Struct({ key_id: Schema.String })),
);
const invalid = () => new Pins.PeerSessionError({ code: "invalid_request" });
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient,
    keys = yield* Keys.WorkstreamsKeyProvider,
    native = yield* Pins.NativePeerSessions,
    authority = yield* Authority.NativeStoreAuthority;
  const owner = yield* Effect.serviceOption(Owner.SourceSetupOwner),
    qualification = yield* Effect.serviceOption(Setup.PeerPinSetupQualification);
  const dependencies = Layer.mergeAll(
    Layer.succeed(SqlClient.SqlClient, sql),
    Layer.succeed(Pins.NativePeerSessions, native),
    Layer.succeed(Authority.NativeStoreAuthority, authority),
    Option.isSome(owner) ? Layer.succeed(Owner.SourceSetupOwner, owner.value) : Layer.empty,
    Option.isSome(qualification)
      ? Layer.succeed(Setup.PeerPinSetupQualification, qualification.value)
      : Layer.empty,
  );
  // Fetch only the two active keys and keys referenced by the selected pin/receipt. Secret/key
  // service access finishes before the writer; cache misses refuse instead of acquiring a key.
  const stores = Effect.fn("HomeBind.stores")(function* (
    home: Home,
    invocation: Mcp.McpInvocationScope,
    bindId: string | null,
  ) {
    yield* native.current(invocation);
    if (Option.isNone(owner) || Option.isNone(qualification))
      return yield* new Pins.PeerSessionError({ code: "owner_unqualified" });
    yield* owner.value.verifyUnchanged.pipe(Effect.mapError(Pins.mapFailure));
    const local = yield* authority.readCurrent.pipe(Effect.mapError(Pins.mapFailure));
    if (
      local.environmentId !== invocation.environmentId ||
      local.environmentId !== home.home_environment_id
    )
      return yield* new Pins.PeerSessionError({ code: "authority_mismatch" });
    const pins = yield* sql<{
      envelope: string;
      registry_instance_id: string;
      owner_id: string;
    }>`SELECT envelope,registry_instance_id,owner_id FROM jones_workstreams_peer_session_pins WHERE session_id=${invocation.client?.sessionId ?? ""}`.pipe(
      Effect.mapError(Pins.mapFailure),
    );
    if (
      pins.length !== 1 ||
      pins[0]!.registry_instance_id !== home.registry_instance_id ||
      pins[0]!.owner_id !== home.owner_id
    )
      return yield* new Pins.PeerSessionError({ code: "authority_mismatch" });
    const homeKey = yield* keys
      .activeKeyId({ ...home, key_scope: "home" })
      .pipe(Effect.mapError(Pins.mapFailure));
    const localKey = yield* keys
      .activeKeyId({ ...home, key_scope: "local" })
      .pipe(Effect.mapError(Pins.mapFailure));
    const receipts =
      bindId === null
        ? []
        : yield* sql<{
            request_envelope: string;
            result_envelope: string;
          }>`SELECT request_envelope,result_envelope FROM jones_workstreams_home_bind_receipts WHERE registry_instance_id=${home.registry_instance_id} AND owner_id=${home.owner_id} AND bind_id=${bindId}`.pipe(
            Effect.mapError(Pins.mapFailure),
          );
    const refs = yield* Effect.try({
      try: () => [
        { id: homeKey, scope: "home" },
        { id: localKey, scope: "local" },
        ...pins.map((p) => ({ id: decodeEnvelopeKey(p.envelope).key_id, scope: "local" })),
        ...receipts.flatMap((p) => [
          { id: decodeEnvelopeKey(p.request_envelope).key_id, scope: "home" },
          { id: decodeEnvelopeKey(p.result_envelope).key_id, scope: "home" },
        ]),
      ],
      catch: Pins.mapFailure,
    });
    const cached = new Map<string, { bytes: Uint8Array; scope: string }>();
    for (const ref of refs) {
      const metadata =
        yield* sql`SELECT key_id FROM jones_workstreams_keys WHERE key_id=${ref.id} AND registry_instance_id=${home.registry_instance_id} AND scope=${ref.scope} AND state IN ('active','decrypt_only')`.pipe(
          Effect.mapError(Pins.mapFailure),
        );
      if (metadata.length !== 1) return yield* new Pins.PeerSessionError({ code: "unavailable" });
      if (!cached.has(ref.id))
        cached.set(ref.id, {
          bytes: yield* keys.get(ref.id).pipe(Effect.mapError(Pins.mapFailure)),
          scope: ref.scope,
        });
    }
    const cachedGet = Effect.fn("HomeBind.cachedKey")(function* (id: string) {
      const loaded = cached.get(id);
      if (!loaded) return yield* new Keys.KeyUnavailable({ reason: "missing" });
      const rows =
        yield* sql`SELECT key_id FROM jones_workstreams_keys WHERE key_id=${id} AND registry_instance_id=${home.registry_instance_id} AND scope=${loaded.scope} AND state IN ('active','decrypt_only')`.pipe(
          Effect.mapError(() => new Keys.KeyUnavailable({ reason: "store_unavailable" })),
        );
      if (rows.length !== 1) return yield* new Keys.KeyUnavailable({ reason: "missing" });
      return Uint8Array.from(loaded.bytes);
    });
    const pinned = Layer.succeed(Keys.WorkstreamsKeyProvider, {
      activeKeyId: keys.activeKeyId,
      get: cachedGet,
      indexKey: () => Effect.fail(new Keys.KeyUnavailable({ reason: "missing" })),
    });
    const db = Database.layer.pipe(Layer.provideMerge(Envelope.layer.pipe(Layer.provide(pinned))));
    const records = Layer.mergeAll(Registry.layer, PinRecords.layer, Records.layer).pipe(
      Layer.provideMerge(db),
    );
    return Layer.fresh(
      Qualification.layer.pipe(Layer.provideMerge(records), Layer.provide(dependencies)),
    );
  });
  const preconditions = Effect.fn("HomeBind.preconditions")(function* (
    body: F.WorkstreamsHomeBindBodyV2,
  ) {
    const registry = yield* Registry.RegistryStore;
    const home = body.expected_home,
      c = yield* registry.context(home);
    if (
      !c ||
      c.state !== "active" ||
      c.canonical_origin !== body.expected_canonical_origin ||
      c.home_environment_id !== home.home_environment_id ||
      c.server_generation !== home.server_generation ||
      c.registry_version !== body.expected.registry_version
    )
      return yield* conflict();
    const principals = (yield* registry.listPlain(home, "principals")).filter(
      (p) => p.kind === "environment" && p.environment_id === body.source.authority.environment_id,
    );
    if (principals.length !== 1 || principals[0]!.principal_id !== body.expected.principal_id)
      return yield* conflict();
    const grant = (yield* registry.listPlain(home, "grants"))
      .filter((g) => g.principal_id === body.expected.principal_id)
      .toSorted((a, b) => Number(b.grant_version) - Number(a.grant_version))[0];
    const permissions = yield* decodePermissions(String(grant?.permissions)).pipe(
      Effect.orElseSucceed((): readonly string[] => []),
    );
    if (
      !grant ||
      grant.state !== "active" ||
      grant.grant_id !== body.expected.grant_id ||
      Number(grant.grant_version) !== body.expected.grant_version ||
      !Array.isArray(permissions) ||
      !permissions.includes("workstreams:write")
    )
      return yield* conflict();
    const bindings = (yield* registry.listPlain(home, "source_bindings"))
      .filter(
        (b) => b.provider === "t3" && b.source_instance_id === body.source.authority.environment_id,
      )
      .toSorted((a, b) => Number(b.binding_version) - Number(a.binding_version));
    const b = bindings[0];
    const preimage = b
      ? {
          binding_version: Number(b.binding_version),
          authority_namespace: b.authority_namespace,
          store_generation: Number(b.store_generation),
          state: b.state,
        }
      : null;
    if (!same(preimage, body.expected.source_binding) || (b && !body.rebind))
      return yield* conflict();
    return { context: c, principal_id: String(principals[0]!.principal_id) };
  });
  const setupReadback = Effect.fn("HomeBind.setupReadback")(function* (
    input: F.WorkstreamsHomeBindSetupReadbackRequestV2,
    invocation: Mcp.McpInvocationScope,
  ) {
    const request = yield* decodeSetup(input).pipe(Effect.mapError(invalid));
    const layer = yield* stores(request.expected_home, invocation, null);
    return yield* Effect.gen(function* () {
      const q = yield* Qualification.HomePeerPinQualification;
      const peer = yield* q.current(
        invocation,
        request.expected_home,
        request.home_setup_request_id,
      );
      if (peer.record.pin.state !== "prepared" || peer.record.pin.revision !== 1)
        return yield* conflict();
      const result = {
        contract: F.WORKSTREAMS_HOME_BIND_CONTRACT,
        kind: "prepared" as const,
        request,
        canonical_origin: peer.canonical_origin,
        home_authority: peer.record.pin.receiver_authority,
        setup_request_body: peer.record.request_body,
        pin: peer.record.pin,
      };
      yield* peer.checkLocal(peer.record.pin);
      return yield* decodeSetupResult(result).pipe(Effect.mapError(invalid));
    }).pipe(Effect.provide(layer), Effect.mapError(Pins.mapFailure));
  });
  const bind = Effect.fn("HomeBind.bind")(function* (
    input: F.WorkstreamsHomeBindRequestV2,
    invocation: Mcp.McpInvocationScope,
  ) {
    const request = yield* decodeBind(input).pipe(Effect.mapError(invalid));
    const body = F.decodeWorkstreamsHomeBindBody(request.request_body);
    const layer = yield* stores(body.expected_home, invocation, body.bind_id);
    return yield* Effect.gen(function* () {
      const q = yield* Qualification.HomePeerPinQualification,
        receipts = yield* Records.HomeBindReceiptStore,
        pins = yield* PinRecords.PeerSessionPinStore,
        registry = yield* Registry.RegistryStore,
        db = yield* Database.NativeSql;
      const peer = yield* q.current(
        invocation,
        body.expected_home,
        body.home_setup.setup_request_id,
      );
      // A retained terminal outcome precedes old version/CAS checks. Current authentication still applies.
      const retained = yield* receipts.read(body.expected_home, body.bind_id);
      if (retained) {
        if (retained.request_body !== request.request_body) return yield* conflict();
        const current = yield* pins.read({ pin_id: peer.record.pin.pin_id });
        if (!current) return yield* conflict();
        yield* peer.checkLocal(current.pin);
        return retained.result;
      }
      const evidence = yield* q.prepare(peer, request.request_body);
      return yield* db.transaction(
        body.expected_home,
        Effect.gen(function* () {
          const concurrent = yield* receipts.read(body.expected_home, body.bind_id);
          if (concurrent) {
            if (concurrent.request_body !== request.request_body) return yield* conflict();
            const current = yield* pins.read({ pin_id: peer.record.pin.pin_id });
            if (!current) return yield* conflict();
            yield* peer.checkLocal(current.pin);
            return concurrent.result;
          }
          yield* peer.checkLocal(peer.record.pin);
          const { context: c, principal_id } = yield* preconditions(body);
          const bindingVersion = (body.expected.source_binding?.binding_version ?? 0) + 1;
          yield* registry.appendPlain(
            {
              registry_instance_id: body.expected_home.registry_instance_id,
              owner_id: body.expected_home.owner_id,
            },
            "source_bindings",
            {
              provider: "t3",
              source_instance_id: body.source.authority.environment_id,
              binding_version: bindingVersion,
              authority_namespace: body.source.authority.authority_namespace,
              store_generation: body.source.authority.store_generation,
              state: "active",
            },
          );
          // Migration 125 advances the registry in the source-binding insert trigger.
          const advanced = yield* registry.context(body.expected_home);
          if (
            !advanced ||
            advanced.registry_version !== c.registry_version + 1 ||
            advanced.server_generation !== c.server_generation ||
            advanced.state !== "active"
          )
            return yield* conflict();
          const actual = (yield* registry.listPlain(body.expected_home, "source_bindings"))
            .filter(
              (b) =>
                b.provider === "t3" &&
                b.source_instance_id === body.source.authority.environment_id,
            )
            .toSorted((a, b) => Number(b.binding_version) - Number(a.binding_version))[0];
          if (
            !actual ||
            actual.state !== "active" ||
            actual.authority_namespace !== body.source.authority.authority_namespace ||
            Number(actual.store_generation) !== body.source.authority.store_generation ||
            Number(actual.binding_version) !== bindingVersion
          )
            return yield* conflict();
          const receipt = {
            principal_id,
            binding_version: Number(actual.binding_version),
            registry_version: advanced.registry_version,
            request_sha256: digest(request.request_body),
          };
          yield* q.verifyBinding(
            evidence,
            peer.record.pin,
            receipt,
            body.member_setup.singleton_binding_version,
          );
          const next = {
            ...peer.record.pin,
            state: "bound" as const,
            revision: peer.record.pin.revision + 1,
            source_binding_version: receipt.binding_version,
            singleton_binding_version: body.member_setup.singleton_binding_version,
            bind_receipt: receipt,
          };
          yield* pins.transition(next, peer.record.pin.revision);
          const bound = yield* pins.read({ pin_id: next.pin_id });
          if (!bound || !same(bound.pin, next)) return yield* conflict();
          const result = yield* decodeCommitted({
            contract: F.WORKSTREAMS_HOME_BIND_CONTRACT,
            kind: "committed",
            request_body: request.request_body,
            receipt,
            home_pin_bound_revision: bound.pin.revision,
          });
          yield* receipts.insert({
            request_body: request.request_body,
            home_setup_request_body: peer.record.request_body,
            result,
          });
          yield* q.verifyBinding(
            evidence,
            bound.pin,
            receipt,
            body.member_setup.singleton_binding_version,
          );
          const post = yield* registry.context(body.expected_home);
          if (!post || !same(post, advanced)) return yield* conflict();
          return result;
        }),
      );
    }).pipe(Effect.provide(layer), Effect.mapError(Pins.mapFailure));
  });
  const readback = Effect.fn("HomeBind.readback")(function* (
    input: F.WorkstreamsHomeBindReadbackRequestV2,
    invocation: Mcp.McpInvocationScope,
  ) {
    const request = yield* decodeReadback(input).pipe(Effect.mapError(invalid));
    const layer = yield* stores(request.expected_home, invocation, request.bind_id);
    return yield* Effect.gen(function* () {
      const q = yield* Qualification.HomePeerPinQualification,
        receipts = yield* Records.HomeBindReceiptStore;
      const peer = yield* q.current(invocation, request.expected_home);
      const retained = yield* receipts.read(request.expected_home, request.bind_id);
      yield* peer.checkLocal(peer.record.pin);
      if (!retained)
        return yield* decodeReadbackResult({
          contract: F.WORKSTREAMS_HOME_BIND_CONTRACT,
          request,
          kind: "unknown",
          reason: "coverage_incomplete",
        });
      const b = F.decodeWorkstreamsHomeBindBody(retained.request_body);
      if (
        b.home_setup.pin_id !== peer.record.pin.pin_id ||
        !same(b.source.authority, peer.record.pin.source_authority)
      )
        return yield* conflict();
      return yield* decodeReadbackResult(
        retained.result.receipt.request_sha256 === request.request_sha256
          ? {
              contract: F.WORKSTREAMS_HOME_BIND_CONTRACT,
              request,
              kind: "committed",
              result: retained.result,
            }
          : {
              contract: F.WORKSTREAMS_HOME_BIND_CONTRACT,
              request,
              kind: "conflict",
              retained_request_sha256: retained.result.receipt.request_sha256,
            },
      );
    }).pipe(Effect.provide(layer), Effect.mapError(Pins.mapFailure));
  });
  return HomeBind.of({ setupReadback, bind, readback });
});
/** Fixed-tool producer only. Root owns MCP/CLI registration and genuine owner qualification. */
export const layer = Layer.effect(HomeBind, make);
