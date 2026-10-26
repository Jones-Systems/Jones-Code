// @effect-diagnostics nodeBuiltinImport:off -- Inventory hashing and process-local cursor signing are protocol operations.
import * as NodeCrypto from "node:crypto";
import * as Wire from "@t3tools/contracts";
import type {
  WorkstreamsNativePlacementPage,
  WorkstreamsNativePlacementResult,
  WorkstreamsNativeThreadPlacement,
} from "@t3tools/contracts/jones/workstreamsNativePlacement";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type { T3PlacementTrustProvider } from "../../../../environment/NativePlacementTrust.ts";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";
import * as Registry from "./service.ts";
import { RegistryError } from "./session.ts";
import * as Store from "../store/registryStore.ts";
import * as NativeSql from "../store/sql.ts";
import { WorkstreamDomainError } from "../domain/errors.ts";
import { requireAttested } from "../domain/decider.ts";
import { RegistryCursorCodec } from "../domain/cursor.ts";

export class PlacementTrust extends Context.Service<PlacementTrust, T3PlacementTrustProvider>()(
  "t3/jones/workstreams/native/registry/placements/PlacementTrust",
) {}
export class Placements extends Context.Service<
  Placements,
  {
    readonly page: (
      caller: AdmittedCaller,
      input: Wire.T3PlacementRequest,
    ) => Effect.Effect<WorkstreamsNativePlacementPage, RegistryError>;
    readonly load: (
      caller: AdmittedCaller,
      input: Wire.T3PlacementLoadRequest,
    ) => Effect.Effect<WorkstreamsNativePlacementResult, RegistryError>;
  }
>()("t3/jones/workstreams/native/registry/placements") {}
const decodePlacementRequest = Schema.decodeUnknownEffect(Wire.T3PlacementRequest);
const isRegistryError = Schema.is(RegistryError);
const decodePlacementLoad = Schema.decodeUnknownEffect(Wire.T3PlacementLoadRequest);
export const layer = Layer.effect(
  Placements,
  Effect.gen(function* () {
    const home = yield* NativeHome,
      registry = yield* Registry.Registry,
      store = yield* Store.RegistryStore,
      db = yield* NativeSql.NativeSql,
      trust = yield* PlacementTrust;
    const scope = { registry_instance_id: home.registry_instance_id, owner_id: home.owner_id };
    const codec = new RegistryCursorCodec(NodeCrypto.randomBytes(32));
    const page = Effect.fn("NativePlacements.page")(
      (caller: AdmittedCaller, input: Wire.T3PlacementRequest) =>
        db
          .transaction(
            scope,
            Effect.gen(function* () {
              yield* decodePlacementRequest(input).pipe(
                Effect.mapError((cause) => new RegistryError({ code: "invalid_request", cause })),
              );
              const s = yield* registry.snapshot(caller),
                time = yield* DateTime.now;
              const observed = DateTime.formatIso(time),
                inventory_sha256 = NodeCrypto.createHash("sha256")
                  .update(Wire.t3PlacementInventoryJson(input.identities))
                  .digest("hex");
              const route = "placements:" + inventory_sha256;
              const context = {
                owner_id: home.owner_id,
                server_generation: s.context.server_generation,
                registry_version: s.context.registry_version,
              };
              const position = yield* Effect.try({
                try: () =>
                  codec.decode(
                    {
                      limit: input.limit,
                      ...(input.cursor === undefined ? {} : { cursor: input.cursor }),
                    },
                    s.authorization,
                    route,
                    context,
                    DateTime.toEpochMillis(time),
                  ),
                catch: (cause) =>
                  new RegistryError({
                    code: cause instanceof WorkstreamDomainError ? cause.code : "cursor_invalid",
                    cause,
                  }),
              });
              const inventory = new Set(input.identities.map(Wire.t3PlacementIdentityKey));
              const bindings = (yield* store.listPlain(scope, "source_bindings")).toSorted(
                (a, b) => Number(b.binding_version) - Number(a.binding_version),
              );
              const stored = yield* store.list(scope, "references");
              const items: WorkstreamsNativeThreadPlacement[] = [];
              for (const m of s.state.memberships.values()) {
                if (m.closed) continue;
                const r = s.state.references.get(m.native_reference_id);
                if (
                  !r ||
                  r.identity.provider !== "t3" ||
                  !inventory.has(
                    Wire.t3PlacementIdentityKey({
                      source_instance_id: r.identity.source_instance_id,
                      native_thread_id: r.identity.native_id,
                    }),
                  )
                )
                  continue;
                const binding = bindings.find(
                  (b) =>
                    b.provider === "t3" && b.source_instance_id === r.identity.source_instance_id,
                );
                if (!binding || binding.state !== "active") continue;
                const attested = yield* Effect.try({
                  try: () => {
                    requireAttested(
                      r,
                      observed,
                      s.state.source_bindings.get(r.identity.source_instance_id),
                    );
                    return true;
                  },
                  catch: () => false,
                }).pipe(Effect.catch(() => Effect.succeed(false)));
                if (
                  !attested ||
                  !r.registration.evidence ||
                  !r.registration.attested_at ||
                  !r.registration.expires_at
                )
                  continue;
                const e = r.registration.evidence;
                const raw = stored.find(
                  (row) => row.metadata.native_reference_id === r.native_reference_id,
                )?.value;
                const marker =
                  typeof raw === "object" && raw !== null && "native_attestation" in raw
                    ? raw.native_attestation
                    : null;
                if (
                  typeof marker !== "object" ||
                  marker === null ||
                  !("contract" in marker) ||
                  marker.contract !== "workstreams-t3-provider/2.0.0" ||
                  !("evidence_sha256" in marker) ||
                  marker.evidence_sha256 !== e.evidence_sha256 ||
                  !("authority_namespace" in marker) ||
                  marker.authority_namespace !== e.authority_namespace ||
                  !("store_generation" in marker) ||
                  marker.store_generation !== e.store_generation ||
                  !("attested_at" in marker) ||
                  marker.attested_at !== r.registration.attested_at ||
                  !("expires_at" in marker) ||
                  marker.expires_at !== r.registration.expires_at
                )
                  return yield* new RegistryError({ code: "registration_unverified" });
                items.push({
                  membership_id: m.membership_id,
                  workstream_id: m.workstream_id,
                  native_reference_id: r.native_reference_id,
                  kind: m.kind,
                  source_instance_id: r.identity.source_instance_id,
                  native_thread_id: r.identity.native_id,
                  attestation_version: r.registration.attestation_version,
                  attested_at: r.registration.attested_at,
                  expires_at: r.registration.expires_at,
                  evidence_sha256: e.evidence_sha256,
                  source_binding_version: Number(binding.binding_version),
                  authority_namespace: e.authority_namespace,
                  store_generation: e.store_generation,
                  attestation_source: "native",
                  evidence_contract: "workstreams-t3-provider/2.0.0",
                });
              }
              const sorted = items
                .toSorted(
                  (a, b) =>
                    Buffer.compare(
                      Buffer.from(a.native_reference_id),
                      Buffer.from(b.native_reference_id),
                    ) || Buffer.compare(Buffer.from(a.membership_id), Buffer.from(b.membership_id)),
                )
                .filter(
                  (x) =>
                    !position.after ||
                    (Buffer.compare(
                      Buffer.from(x.native_reference_id),
                      Buffer.from(String(position.after[0])),
                    ) ||
                      Buffer.compare(
                        Buffer.from(x.membership_id),
                        Buffer.from(String(position.after[1])),
                      )) > 0,
                );
              const selected = sorted.slice(0, input.limit);
              return {
                inventory_sha256,
                context: {
                  ...context,
                  principal_id: caller.principal_id,
                  authorization_revision: s.authorization.grant.grant_version,
                },
                provenance: {
                  authority: "native-home" as const,
                  registry_instance_id: home.registry_instance_id,
                  home_environment_id: home.home_environment_id,
                  home_generation: home.server_generation,
                  served_by_environment_id: home.served_by_environment_id,
                  observed_at: observed,
                },
                items: selected,
                next_cursor:
                  sorted.length > input.limit
                    ? codec.encode(
                        {
                          ...position,
                          after: [
                            selected.at(-1)!.native_reference_id,
                            selected.at(-1)!.membership_id,
                          ],
                        },
                        s.authorization,
                        route,
                        context,
                      )
                    : null,
              };
            }),
          )
          .pipe(
            Effect.mapError((cause) =>
              isRegistryError(cause) ? cause : new RegistryError({ code: "unavailable", cause }),
            ),
          ),
    );
    const load = Effect.fn("NativePlacements.load")(
      (caller: AdmittedCaller, input: Wire.T3PlacementLoadRequest) =>
        db
          .transaction(
            scope,
            Effect.gen(function* () {
              yield* decodePlacementLoad(input).pipe(
                Effect.mapError((cause) => new RegistryError({ code: "invalid_request", cause })),
              );
              let current = yield* page(caller, { ...input, limit: 100 });
              const first = current,
                items = [...current.items];
              for (let pages = 1; current.next_cursor !== null; pages++) {
                if (pages >= Wire.T3_PLACEMENT_MAX_PAGES)
                  return yield* new RegistryError({ code: "backpressure" });
                current = yield* page(caller, {
                  ...input,
                  limit: 100,
                  cursor: current.next_cursor,
                });
                items.push(...current.items);
              }
              const trusted = trust.readTrustSnapshot();
              return { page: { ...first, items, next_cursor: null }, ...trusted };
            }),
          )
          .pipe(
            Effect.mapError((cause) =>
              isRegistryError(cause) ? cause : new RegistryError({ code: "unavailable", cause }),
            ),
          ),
    );
    return Placements.of({ page, load });
  }),
);
