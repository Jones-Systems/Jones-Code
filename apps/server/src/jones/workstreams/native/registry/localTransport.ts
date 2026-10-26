// @effect-diagnostics nodeBuiltinImport:off -- Process-local cursor key; no persisted content cache.
import * as NodeCrypto from "node:crypto";
import * as Wire from "@t3tools/contracts";
import type { WorkstreamsNativePlacementResult } from "@t3tools/contracts/jones/workstreamsNativePlacement";
import * as Context from "effect/Context";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Gateway from "../../../../workstreams/WorkstreamGateway.ts";
import * as Registry from "./service.ts";
import * as Appearance from "./appearance.ts";
import * as Counts from "./counts.ts";
import * as Placements from "./placements.ts";
import * as Runner from "./effectRunner.ts";
import * as HomeExecutor from "../operations/homeExecutor.ts";
import * as BindingStore from "../store/bindingStore.ts";
import * as NativeSql from "../store/sql.ts";
import * as HistoryStore from "../store/historyStore.ts";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";
import { RegistryError } from "./session.ts";
import { RegistryCursorCodec } from "../domain/cursor.ts";
import { collectionPosition, pageCollection, referenceForRead } from "../domain/collections.ts";
import type { RegistryCollections, PageQuery } from "../domain/state.ts";
import { WorkstreamDomainError } from "../domain/errors.ts";
import { canonicalJson } from "../domain/validation.ts";

export interface NativeAdapter {
  readonly transport: Gateway.WorkstreamTransport;
  readonly gateway: Gateway.WorkstreamGateway["Service"];
  readonly homeTransport: HomeExecutor.HomeTransport["Service"];
  readonly guardExecutor: (
    executor: HomeExecutor.HomeExecutor["Service"],
  ) => HomeExecutor.HomeExecutor["Service"];
  readonly placementResponse: (
    input: Wire.T3PlacementLoadRequest,
    contract?: string,
  ) => Effect.Effect<WorkstreamsNativePlacementResult | Wire.T3PlacementResult, RegistryError>;
}
export class LocalTransport extends Context.Service<
  LocalTransport,
  {
    readonly forCaller: (caller: AdmittedCaller) => Effect.Effect<NativeAdapter, RegistryError>;
  }
>()("t3/jones/workstreams/native/registry/localTransport") {}

const isRegistryError = Schema.is(RegistryError);
const decodeCommandJsonSync = Schema.decodeUnknownSync(
  Schema.fromJsonString(Wire.WorkstreamCommand),
);
const decodeAuditEvent = Schema.decodeUnknownEffect(Wire.WorkstreamAuditEvent);
const decodeCommandJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Wire.WorkstreamCommand));
export const layer = Layer.effect(
  LocalTransport,
  Effect.gen(function* () {
    const home = yield* NativeHome,
      registry = yield* Registry.Registry,
      appearance = yield* Appearance.Appearance,
      counts = yield* Counts.Counts,
      placements = yield* Placements.Placements,
      runner = yield* Runner.EffectRunner,
      bindings = yield* BindingStore.BindingStore,
      db = yield* NativeSql.NativeSql,
      history = yield* HistoryStore.HistoryStore,
      trust = yield* Placements.PlacementTrust;
    const scope = { registry_instance_id: home.registry_instance_id, owner_id: home.owner_id };
    const clock = yield* Clock.Clock;
    const codec = new RegistryCursorCodec(NodeCrypto.randomBytes(32));
    const forCaller = Effect.fn("NativeTransport.forCaller")(function* (caller: AdmittedCaller) {
      const authorizationRevision = (yield* registry.snapshot(caller)).authorization.grant
        .grant_version;
      const context = (s: Registry.Snapshot) => ({
        owner_id: s.context.owner_id,
        server_generation: s.context.server_generation,
        registry_version: s.context.registry_version,
      });
      const map = <A, E>(operation: string, effect: Effect.Effect<A, E>, write = false) =>
        effect.pipe(
          Effect.mapError(
            (cause) =>
              new Gateway.WorkstreamTransportError({
                operation,
                effect: write ? "unknown-effect" : "no-effect",
                detail: isRegistryError(cause)
                  ? cause.code
                  : cause instanceof WorkstreamDomainError
                    ? cause.code
                    : "unavailable",
                cause,
              }),
          ),
        );
      const atomic = <A, E>(effect: Effect.Effect<A, E>) => db.transaction(scope, effect);
      const decode = <A>(f: () => A) =>
        Effect.try({
          try: f,
          catch: (cause) =>
            new RegistryError({
              code: cause instanceof WorkstreamDomainError ? cause.code : "invalid_request",
              cause,
            }),
        });
      const capabilities = Effect.fn("NativeTransport.capabilities")(function* (): Effect.fn.Return<
        Wire.WorkstreamCapabilities,
        RegistryError
      > {
        const s = yield* registry.snapshot(caller);
        return {
          contract_family: Wire.WORKSTREAM_CONTRACT_FAMILY,
          contract_version: Wire.WORKSTREAM_CONTRACT_VERSION,
          manifest_sha256: Wire.WORKSTREAM_CONTRACT_MANIFEST_SHA256,
          context: context(s),
          permissions: s.authorization.grant.permissions,
          max_request_bytes: 32768,
          max_response_bytes: 1048576,
          max_json_depth: 10,
          max_page_items: 100,
          max_pr_response_bytes: 262144,
          max_pr_request_seconds: 15,
          cursor_ttl_seconds: 900,
        };
      });
      const reference = (s: Registry.Snapshot, id: string, at: string) =>
        referenceForRead(
          s.state.references.get(id) ?? null,
          home.owner_id,
          caller.principal_id,
          s.state.source_bindings.get(
            s.state.references.get(id)?.identity.source_instance_id ?? "",
          ),
          at,
        );
      const page = <K extends Exclude<keyof RegistryCollections, "history">>(
        collection: K,
        query: PageQuery,
        workstreamId?: string,
      ) =>
        atomic(
          Effect.gen(function* () {
            const s = yield* registry.snapshot(caller),
              time = yield* DateTime.now;
            if (workstreamId && !s.state.workstreams.has(workstreamId))
              return yield* new RegistryError({ code: "not_found" });
            const route = collection + ":" + (workstreamId ?? "global"),
              ctx = context(s);
            const position = yield* decode(() =>
              codec.decode(query, s.authorization, route, ctx, DateTime.toEpochMillis(time)),
            );
            let values: readonly RegistryCollections[K][];
            switch (collection) {
              case "workstreams":
                values = [...s.state.workstreams.values()] as RegistryCollections[K][];
                break;
              case "references":
                values = [...s.state.references.keys()]
                  .map((id) => reference(s, id, DateTime.formatIso(time)))
                  .filter((x) => x !== null) as RegistryCollections[K][];
                break;
              case "memberships":
                values = [...s.state.memberships.values()].filter(
                  (x) =>
                    x.workstream_id === workstreamId &&
                    reference(s, x.native_reference_id, DateTime.formatIso(time)) !== null,
                ) as RegistryCollections[K][];
                break;
              case "declarations":
                values = [...s.state.declarations.values()]
                  .flat()
                  .filter((x) => x.workstream_id === workstreamId) as RegistryCollections[K][];
                break;
              case "edges":
                values = [...s.state.edges.values()].filter(
                  (x) =>
                    x.from_workstream_id === workstreamId || x.to_workstream_id === workstreamId,
                ) as RegistryCollections[K][];
                break;
            }
            const found = yield* decode(() =>
              pageCollection(collection, values, position.after, position.limit + 1),
            );
            const items = found.slice(0, position.limit);
            return {
              context: ctx,
              items,
              next_cursor:
                found.length > position.limit
                  ? codec.encode(
                      { ...position, after: collectionPosition(collection, items.at(-1)!) },
                      s.authorization,
                      route,
                      ctx,
                    )
                  : null,
            };
          }),
        );
      const readReceipt = Effect.fn("NativeTransport.receipt")(function* (id: string) {
        const value = yield* registry.lookup(caller, id, home.server_generation);
        if (value.kind === "unknown") return yield* new RegistryError({ code: "unknown_effect" });
        return value.kind === "present"
          ? { kind: "receipt" as const, receipt: value.receipt }
          : { kind: "absent" as const };
      });
      const submit = Effect.fn("NativeTransport.submit")(function* (input: {
        body: string;
        command: Wire.WorkstreamCommand;
        idempotencyKey: string;
      }) {
        yield* decode(() => {
          const command = decodeCommandJsonSync(input.body, { onExcessProperty: "error" });
          if (
            command.command_id !== input.idempotencyKey ||
            canonicalJson(command) !== canonicalJson(input.command)
          )
            throw new WorkstreamDomainError("idempotency_conflict");
        });
        const accepted = yield* registry.submit(caller, input.body);
        if (!accepted.intent) return accepted.receipt;
        return (yield* runner.run(accepted.intent)) ?? accepted.receipt;
      });
      const resubmit = Effect.fn("NativeTransport.resubmit")(function* (input: {
        body: string;
        command: Wire.WorkstreamCommand;
        idempotencyKey: string;
      }) {
        return yield* submit(input).pipe(
          Effect.map((receipt) => ({ kind: "receipt" as const, receipt })),
          Effect.catchTags({
            NativeRegistryError: (error) =>
              error.code === "idempotency_conflict"
                ? Effect.succeed({ kind: "idempotency_conflict" as const })
                : Effect.fail(error),
          }),
        );
      });
      const transport: Gateway.WorkstreamTransport = {
        getCapabilities: () => map("capabilities", capabilities()),
        getCountsCapabilities: () =>
          map(
            "counts-capabilities",
            capabilities().pipe(
              Effect.map((v) => ({
                ...v,
                manifest_sha256: Wire.WORKSTREAM_COUNTS_MANIFEST_SHA256,
                registry_counts: {
                  version: 1 as const,
                  path: "/workstreams/v1/counts" as const,
                  active_definition: "latest_owner_lifecycle_declaration_active" as const,
                },
              })),
            ),
          ),
        getRegistryCounts: () => map("counts", counts.read(caller)),
        readAppearance: (input) => map("appearance", appearance.read(caller, input.workstream_ids)),
        saveAppearance: (input) => map("appearance", appearance.save(caller, input), true),
        listThreadPlacements: (input) =>
          map(
            "placements",
            placements.page(caller, input).pipe(
              Effect.map(({ provenance: _p, items, ...rest }) => ({
                ...rest,
                items: items.map(
                  ({ attestation_source: _s, evidence_contract: _c, ...item }) => item,
                ),
              })),
            ),
          ),
        listWorkstreams: (input) => map("workstreams", page("workstreams", input)),
        getWorkstream: (input) =>
          map(
            "detail",
            registry.snapshot(caller).pipe(
              Effect.flatMap((s) => {
                const workstream = s.state.workstreams.get(input.workstreamId);
                return workstream
                  ? Effect.succeed({ context: context(s), workstream })
                  : Effect.fail(new RegistryError({ code: "not_found" }));
              }),
            ),
          ),
        listReferences: (input) => map("references", page("references", input)),
        getReference: (input) =>
          map(
            "reference",
            Effect.gen(function* () {
              const s = yield* registry.snapshot(caller),
                time = yield* DateTime.now;
              const r = reference(s, input.nativeReferenceId, DateTime.formatIso(time));
              if (!r) return yield* new RegistryError({ code: "not_found" });
              const observation = s.state.observations.get(r.native_reference_id)?.at(-1) ?? null;
              const { observation_cache: _cache, ...publicObservation } = observation ?? {};
              return {
                context: context(s),
                reference: r,
                latest_observation: observation
                  ? (publicObservation as NonNullable<
                      Wire.WorkstreamReferenceDetail["latest_observation"]
                    >)
                  : null,
              };
            }),
          ),
        listMemberships: (input) =>
          map("memberships", page("memberships", input, input.workstreamId)),
        listDeclarations: (input) =>
          map("declarations", page("declarations", input, input.workstreamId)),
        listEdges: (input) => map("edges", page("edges", input, input.workstreamId)),
        listHistory: (input) =>
          map(
            "history",
            atomic(
              Effect.gen(function* () {
                const s = yield* registry.snapshot(caller),
                  time = yield* DateTime.now,
                  ctx = context(s),
                  route = "history:" + (input.workstreamId ?? "global");
                if (input.workstreamId && !s.state.workstreams.has(input.workstreamId))
                  return yield* new RegistryError({ code: "not_found" });
                const position = yield* decode(() =>
                  codec.decode(input, s.authorization, route, ctx, DateTime.toEpochMillis(time)),
                );
                const found = yield* history.page(scope, {
                  limit: position.limit + 1,
                  scope_key: input.workstreamId ? "workstream:" + input.workstreamId : "global",
                  ...(position.highWater === null ? {} : { high_water: position.highWater }),
                  expected: ctx,
                  ...(position.after
                    ? {
                        after: {
                          registry_version: Number(position.after[0]),
                          event_id: String(position.after[1]),
                        },
                      }
                    : {}),
                });
                const items: (typeof Wire.WorkstreamAuditEvent.Type)[] = [];
                for (const item of found.events.slice(0, position.limit))
                  items.push(
                    yield* decodeAuditEvent(item.value).pipe(
                      Effect.mapError((cause) => new RegistryError({ code: "unavailable", cause })),
                    ),
                  );
                return {
                  context: ctx,
                  items,
                  next_cursor:
                    found.events.length > position.limit
                      ? codec.encode(
                          {
                            ...position,
                            highWater: found.high_water,
                            after: collectionPosition("history", items.at(-1)!),
                          },
                          s.authorization,
                          route,
                          ctx,
                        )
                      : null,
                };
              }),
            ),
          ),
        getCommand: (input) =>
          map(
            "command",
            readReceipt(input.commandId).pipe(
              Effect.flatMap((v) =>
                v.kind === "receipt"
                  ? Effect.succeed(v.receipt)
                  : Effect.fail(new RegistryError({ code: "not_found" })),
              ),
            ),
          ),
        readCommandReceipt: (input) => map("receipt", readReceipt(input.commandId)),
        submitCommand: (input) => map("submit", submit(input), true),
        resubmitCommand: (input) => map("resubmit", resubmit(input), true),
      };
      const gateway = yield* Gateway.make(transport, {
        binding: {
          registryId: home.canonical_origin,
          ownerId: home.owner_id,
          principalId: caller.principal_id,
          authorizationRevision: authorizationRevision,
        },
        now: () => clock.currentTimeMillisUnsafe(),
        placementTrustProvider: trust,
      });
      const pinned = Effect.fn("NativeTransport.pinned")(function* (
        destination: Parameters<HomeExecutor.HomeTransport["Service"]["lookup"]>[0],
      ) {
        yield* bindings.assertDestination(destination).pipe(
          Effect.mapError(
            () =>
              new Gateway.WorkstreamGatewayError({
                reason: "unknown-effect",
                detail: "Native destination is not current.",
              }),
          ),
        );
        if (
          destination.registry_instance_id !== home.registry_instance_id ||
          destination.owner_id !== home.owner_id ||
          destination.home_environment_id !== home.home_environment_id ||
          destination.home_generation !== home.server_generation ||
          destination.principal_id !== caller.principal_id ||
          destination.binding_version !== caller.binding_version
        )
          return yield* new Gateway.WorkstreamGatewayError({
            reason: "unknown-effect",
            detail: "Native destination does not match admission.",
          });
      });
      const assertOperation = Effect.fn("NativeTransport.assertOperation")(function* (
        operation: Parameters<HomeExecutor.HomeExecutor["Service"]["assertSession"]>[0],
      ) {
        yield* pinned(operation.destination);
        if (
          canonicalJson(operation.origin) !== canonicalJson(caller.origin) ||
          (operation.thread_environment_id !== null &&
            operation.thread_environment_id !== caller.source_environment_id)
        )
          return yield* new Gateway.WorkstreamGatewayError({
            reason: "unknown-effect",
            detail: "Operation origin does not match source admission.",
          });
      });
      const guardExecutor = (
        executor: HomeExecutor.HomeExecutor["Service"],
      ): HomeExecutor.HomeExecutor["Service"] => ({
        assertSession: (operation, session) =>
          assertOperation(operation).pipe(
            Effect.andThen(executor.assertSession(operation, session)),
          ),
        readReceipt: (operation, id) =>
          assertOperation(operation).pipe(Effect.andThen(executor.readReceipt(operation, id))),
        dispatch: (operation, id, body) =>
          assertOperation(operation).pipe(Effect.andThen(executor.dispatch(operation, id, body))),
      });
      const homeTransport: HomeExecutor.HomeTransport["Service"] = {
        lookup: Effect.fn("NativeTransport.homeLookup")(function* (destination, id) {
          yield* pinned(destination);
          return yield* registry.lookup(caller, id, destination.home_generation).pipe(
            Effect.mapError(
              (cause) =>
                new Gateway.WorkstreamGatewayError({
                  reason: "unknown-effect",
                  detail: "Native receipt lookup unavailable.",
                  cause,
                }),
            ),
          );
        }),
        resubmit: Effect.fn("NativeTransport.homeResubmit")(function* (destination, id, body) {
          yield* pinned(destination);
          const command = yield* decodeCommandJson(body, { onExcessProperty: "error" }).pipe(
            Effect.mapError(
              (cause) =>
                new Gateway.WorkstreamGatewayError({
                  reason: "unknown-effect",
                  detail: "Retained native command is invalid.",
                  cause,
                }),
            ),
          );
          if (
            command.command_id !== id ||
            command.expected_server_generation !== destination.home_generation
          )
            return yield* new Gateway.WorkstreamGatewayError({
              reason: "unknown-effect",
              detail: "Retained command destination mismatch.",
            });
          return yield* resubmit({ body, command, idempotencyKey: id }).pipe(
            Effect.mapError(
              (cause) =>
                new Gateway.WorkstreamGatewayError({
                  reason: "unknown-effect",
                  detail: "Native command outcome unavailable.",
                  cause,
                }),
            ),
          );
        }),
      };
      const placementResponse = Effect.fn("NativeTransport.placementResponse")(function* (
        input: Wire.T3PlacementLoadRequest,
        contract?: string,
      ) {
        if (contract !== undefined && contract !== "workstreams-t3-placement/2.0.0")
          return yield* new RegistryError({ code: "contract_mismatch" });
        const result = yield* placements.load(caller, input);
        if (contract !== undefined) return result;
        const { provenance: _provenance, items, ...page } = result.page;
        return {
          ...result,
          page: {
            ...page,
            items: items.map(({ attestation_source: _s, evidence_contract: _e, ...item }) => item),
          },
        };
      });
      return { transport, gateway, homeTransport, guardExecutor, placementResponse };
    });
    return LocalTransport.of({ forCaller });
  }),
);
