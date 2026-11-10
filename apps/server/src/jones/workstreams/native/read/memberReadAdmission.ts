import { WORKSTREAMS_HOME_READ_RESULT_CONTRACT } from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { canonicalJson } from "../domain/validation.ts";
import { RegistryError } from "../registry/session.ts";
import * as Admission from "../runtime/memberAdmission.ts";
import type { ReadResult } from "./routes.ts";
import * as Member from "./memberRead.ts";
import { WorkstreamsHomeReadResult } from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import * as Schema from "effect/Schema";
import type { WorkstreamsNativeHomeTarget } from "@t3tools/contracts/jones/workstreamsNative";
import type { WorkstreamsHomeReadResultRequest } from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import * as Option from "effect/Option";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Protocol from "../forwarding/homeReadProtocol.ts";
import { validateReadResult } from "./routes.ts";

/** Request-local home permission. Construction performs one coherent counts/result call. */
export class CurrentMemberRead extends Context.Service<
  CurrentMemberRead,
  {
    readonly read: Member.HomeReadPort["Service"]["read"];
    readonly admission: Effect.Success<
      ReturnType<Member.MemberReadAdmission["Service"]["current"]>
    >;
  }
>()("t3/jones/workstreams/native/read/memberReadAdmission/CurrentMemberRead") {}
const denied = () => new RegistryError({ code: "forbidden" });
const make = Effect.gen(function* () {
  const member = yield* Admission.MemberAdmission;
  const port = yield* Member.HomeReadPort;
  const local = yield* member.current();
  const a = local.record.association;
  const limits = yield* local.checkCurrent();
  const beforeAfter = Effect.fn("CurrentMemberRead.localCurrent")(function* () {
    if (canonicalJson(yield* local.checkCurrent()) !== canonicalJson(limits))
      return yield* denied();
  });
  // HomeReadPort is the strict homeReadProtocol decoder over the fixed result client.
  const counts = yield* port
    .read(a.home, { route: "counts", params: {} })
    .pipe(Effect.provideService(Admission.RetainedMemberCaller, local.record));
  yield* beforeAfter();
  if (counts.route !== "counts") return yield* new RegistryError({ code: "invalid_response" });
  const envelope = counts.envelope;
  const { value } = yield* ProtocolCounts(counts);
  if (
    envelope.registry.registry_instance_id !== a.home.registry_instance_id ||
    envelope.registry.owner_id !== a.home.owner_id ||
    envelope.registry.canonical_origin !== a.canonical_origin ||
    envelope.registry.server_generation !== a.home.server_generation ||
    envelope.provenance.home_environment_id !== a.home.home_environment_id ||
    envelope.provenance.served_by_environment_id !== a.home.home_environment_id ||
    envelope.provenance.source !== "live" ||
    envelope.provenance.stale ||
    envelope.provenance.authority !== "native-home" ||
    envelope.coverage.registry !== "complete" ||
    value.principal_id !== a.principal_id ||
    value.context.owner_id !== a.home.owner_id ||
    value.context.server_generation !== a.home.server_generation ||
    value.context.registry_version !== envelope.registry.registry_version ||
    value.observed_at !== envelope.provenance.observed_at
  )
    return yield* denied();
  yield* beforeAfter();
  const read: Member.HomeReadPort["Service"]["read"] = Effect.fn("CurrentMemberRead.read")(
    function* (home, request, placementContract) {
      if (canonicalJson(home) !== canonicalJson(a.home)) return yield* denied();
      yield* beforeAfter();
      const result = yield* port
        .read(home, request, placementContract)
        .pipe(Effect.provideService(Admission.RetainedMemberCaller, local.record));
      yield* beforeAfter();
      const e = result.envelope;
      if (
        result.authorization_revision !== counts.authorization_revision ||
        e.registry.registry_instance_id !== a.home.registry_instance_id ||
        e.registry.owner_id !== a.home.owner_id ||
        e.registry.canonical_origin !== a.canonical_origin ||
        e.registry.server_generation !== a.home.server_generation ||
        e.provenance.home_environment_id !== a.home.home_environment_id ||
        e.provenance.served_by_environment_id !== a.home.home_environment_id ||
        e.provenance.source !== "live" ||
        e.provenance.stale ||
        e.provenance.authority !== "native-home" ||
        e.coverage.registry !== "complete"
      )
        return yield* denied();
      return result;
    },
  );
  return CurrentMemberRead.of({
    read,
    admission: {
      expected_home: a.home,
      principal_id: a.principal_id,
      authorization_revision: counts.authorization_revision,
      check: beforeAfter,
    },
  });
});

const decode = Schema.decodeUnknownEffect(WorkstreamsHomeReadResult, { onExcessProperty: "error" });
const ProtocolCounts = Effect.fn("CurrentMemberRead.decodeCounts")(function* (result: ReadResult) {
  const decoded = yield* decode({
    contract: WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
    ...result,
  }).pipe(Effect.mapError((cause) => new RegistryError({ code: "invalid_response", cause })));
  if (decoded.route !== "counts") return yield* new RegistryError({ code: "invalid_response" });
  return decoded;
});
/** @public Construct per request before MemberReads; never install as a singleton layer. */
export const layer = Layer.effect(CurrentMemberRead, make);
/** @public Mount this port only in the same request context as CurrentMemberRead. */
export const admissionLayer = Layer.unwrap(
  Effect.sync(() =>
    Layer.effect(
      Member.MemberReadAdmission,
      Effect.gen(function* () {
        const permission = yield* CurrentMemberRead;
        return Member.MemberReadAdmission.of({
          current: () => Effect.succeed(permission.admission),
        });
      }),
    ),
  ),
);

/** @public Replaces the base protocol port only for reads in this admitted request. */
export const portLayer = Layer.unwrap(
  Effect.sync(() =>
    Layer.effect(
      Member.HomeReadPort,
      Effect.gen(function* () {
        const permission = yield* CurrentMemberRead;
        return Member.HomeReadPort.of({ read: permission.read });
      }),
    ),
  ),
);

// This class is never exported or decoded. Only the own-thread read below constructs it.
type OwnPlacementRoute = {
  readonly home: WorkstreamsNativeHomeTarget;
  readonly link_generation: string;
  readonly limits: Admission.AdmittedMember["limits"];
  readonly checkCurrent: () => Effect.Effect<void, RegistryError>;
};
class OwnPlacementProof {
  readonly homeBytes: string;
  readonly requestBytes: string;
  readonly scope: Mcp.McpInvocationScope;
  readonly home: WorkstreamsNativeHomeTarget;
  readonly request: WorkstreamsHomeReadResultRequest;
  readonly route: OwnPlacementRoute;
  constructor(
    scope: Mcp.McpInvocationScope,
    home: WorkstreamsNativeHomeTarget,
    request: WorkstreamsHomeReadResultRequest,
    route: OwnPlacementRoute,
  ) {
    this.homeBytes = canonicalJson(home);
    this.requestBytes = canonicalJson(request);
    this.scope = scope;
    this.home = home;
    this.request = request;
    this.route = route;
  }
}
class OwnPlacementRequest extends Context.Service<OwnPlacementRequest, OwnPlacementProof>()(
  "t3/jones/workstreams/native/read/memberReadAdmission/OwnPlacementRequest",
) {}
/** @public Root's fixed client may exempt only this exact request from its own-thread refusal. */
export const checkOwnPlacementForwarding = Effect.fn("OwnPlacement.checkForwarding")(function* (
  home: WorkstreamsNativeHomeTarget,
  request: WorkstreamsHomeReadResultRequest,
) {
  const proof = yield* Effect.serviceOption(OwnPlacementRequest);
  const scope = yield* Effect.serviceOption(Mcp.McpInvocationContext);
  if (
    Option.isNone(proof) ||
    !(proof.value instanceof OwnPlacementProof) ||
    Option.isNone(scope) ||
    proof.value.scope !== scope.value ||
    scope.value.client ||
    !scope.value.thread ||
    request.request.route !== "placements" ||
    canonicalJson(home) !== proof.value.homeBytes ||
    canonicalJson(request) !== proof.value.requestBytes
  )
    return yield* denied();
  yield* proof.value.route.checkCurrent();
});
/** @public The routing producer accepts this private proof only during its exact own-thread call. */
export const ownPlacementRouting = Effect.fn("OwnPlacement.routing")(function* () {
  const proof = yield* Effect.serviceOption(OwnPlacementRequest);
  if (Option.isNone(proof)) return Option.none<OwnPlacementProof["route"]>();
  yield* checkOwnPlacementForwarding(proof.value.home, proof.value.request);
  return Option.some(proof.value.route);
});
/** Distinct from metadata/counts permission; callers cannot choose an inventory or destination. */
export class MemberOwnThreadPlacements extends Context.Service<
  MemberOwnThreadPlacements,
  {
    readonly read: () => Effect.Effect<ReadResult, RegistryError>;
  }
>()("t3/jones/workstreams/native/read/memberReadAdmission/MemberOwnThreadPlacements") {}
/** @public Requires root's exact fixed-client proof hook; generic linked forwarding stays refused. */
export const ownPlacementLayer = Layer.unwrap(
  Effect.sync(() =>
    Layer.effect(
      MemberOwnThreadPlacements,
      Effect.gen(function* () {
        const member = yield* Admission.MemberAdmission;
        const client = yield* Protocol.HomeReadResultClient;
        return MemberOwnThreadPlacements.of({
          read: Effect.fn("MemberOwnThreadPlacements.read")(function* () {
            const scope = yield* Effect.serviceOption(Mcp.McpInvocationContext);
            if (Option.isNone(scope) || scope.value.client || !scope.value.thread)
              return yield* denied();
            const record = yield* member.captureOwnPlacement();
            if (
              record.local.auth.kind !== "agent" ||
              record.local.auth.calling_thread_id !== scope.value.thread.threadId
            )
              return yield* denied();
            const admitted = yield* member.readmitOwnPlacement(record);
            const a = admitted.record.association;
            const limits = yield* admitted.checkCurrent();
            const checkCurrent = Effect.fn("MemberOwnThreadPlacements.checkCurrent")(function* () {
              if (canonicalJson(yield* admitted.checkCurrent()) !== canonicalJson(limits))
                return yield* denied();
            });
            const request: WorkstreamsHomeReadResultRequest = {
              contract: WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
              request: {
                route: "placements",
                params: {
                  identities: [
                    {
                      source_instance_id: a.source.environment_id,
                      native_thread_id: record.local.auth.calling_thread_id,
                    },
                  ],
                },
              },
              placement_contract: "workstreams-t3-placement/2.0.0",
            };
            const route = {
              home: a.home,
              link_generation: a.link_generation,
              limits,
              checkCurrent,
            };
            const proof = new OwnPlacementProof(scope.value, a.home, request, route);
            yield* checkCurrent();
            const raw = yield* client
              .read(a.home, request)
              .pipe(Effect.provideService(OwnPlacementRequest, proof));
            yield* checkCurrent();
            const decoded = yield* decode(raw).pipe(
              Effect.mapError((cause) => new RegistryError({ code: "invalid_response", cause })),
            );
            const result = yield* validateReadResult(
              request.request,
              decoded,
              request.placement_contract,
            );
            if (
              decoded.route !== "placements" ||
              decoded.placement_contract !== "workstreams-t3-placement/2.0.0"
            )
              return yield* denied();
            const e = result.envelope,
              page = decoded.value;
            if (
              e.registry.registry_instance_id !== a.home.registry_instance_id ||
              e.registry.owner_id !== a.home.owner_id ||
              e.registry.canonical_origin !== a.canonical_origin ||
              e.registry.server_generation !== a.home.server_generation ||
              e.provenance.home_environment_id !== a.home.home_environment_id ||
              e.provenance.served_by_environment_id !== a.home.home_environment_id ||
              e.provenance.source !== "live" ||
              e.provenance.stale ||
              e.provenance.authority !== "native-home" ||
              e.coverage.registry !== "complete" ||
              page.context.principal_id !== a.principal_id ||
              page.context.authorization_revision !== result.authorization_revision ||
              page.provenance.registry_instance_id !== a.home.registry_instance_id ||
              page.provenance.home_environment_id !== a.home.home_environment_id ||
              page.provenance.served_by_environment_id !== a.home.home_environment_id ||
              page.provenance.home_generation !== a.home.server_generation ||
              page.provenance.observed_at !== e.provenance.observed_at ||
              page.next_cursor !== null ||
              page.items.some(
                (item) =>
                  item.authority_namespace !== a.source.authority_namespace ||
                  item.store_generation !== a.source.store_generation ||
                  item.source_binding_version !== a.source_binding_version,
              )
            )
              return yield* denied();
            yield* checkCurrent();
            return result;
          }),
        });
      }),
    ),
  ),
);
