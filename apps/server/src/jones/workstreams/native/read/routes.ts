// @effect-diagnostics nodeBuiltinImport:off -- Process-local lifecycle cursor key, never persisted.
import * as NodeCrypto from "node:crypto";
import * as Wire from "@t3tools/contracts";
import {
  WorkstreamsNativeHomeRead,
  type WorkstreamsNativeReadEnvelope,
} from "@t3tools/contracts/jones/workstreamsNativeRead";
import {
  WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
  WorkstreamsHomeReadResult,
  WorkstreamsHomeReadResultRequest,
} from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import { canonicalJson } from "../domain/validation.ts";
import { WorkstreamsRollupSummary } from "@t3tools/contracts/jones/workstreamsRollups";
import * as Context from "effect/Context";
import * as Clock from "effect/Clock";
import { CoherentSnapshots } from "./coherentSnapshot.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Projection from "./projection.ts";
import { WorkstreamTransportError } from "../../../../workstreams/WorkstreamGateway.ts";
import * as Local from "../registry/localTransport.ts";
import * as Registry from "../registry/service.ts";
import * as Database from "../store/sql.ts";
import * as Admission from "../runtime/admission.ts";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";
import { RegistryError } from "../registry/session.ts";
import * as Session from "../registry/session.ts";
import { RegistryCursorCodec } from "../domain/cursor.ts";

export type PlacementContract = NonNullable<WorkstreamsHomeReadResultRequest["placement_contract"]>;

export interface ReadResult {
  readonly placement_contract?: PlacementContract;
  readonly route: WorkstreamsNativeHomeRead["route"];
  readonly envelope: WorkstreamsNativeReadEnvelope;
  readonly value: unknown;
  readonly authorization_revision: number;
}
/** J8 supplies authorization, re-authorization and digest verification inside this port. */
export class RollupSummaryRead extends Context.Service<
  RollupSummaryRead,
  {
    readonly read: (
      caller: AdmittedCaller,
      workstreamId: string,
    ) => Effect.Effect<WorkstreamsRollupSummary, RegistryError>;
  }
>()("t3/jones/workstreams/native/read/routes/RollupSummaryRead") {}

export class HomeReads extends Context.Service<
  HomeReads,
  {
    readonly read: (
      caller: AdmittedCaller,
      request: WorkstreamsNativeHomeRead,
      placementContract?: PlacementContract,
    ) => Effect.Effect<ReadResult, RegistryError>;
  }
>()("t3/jones/workstreams/native/read/routes/HomeReads") {}

const isTransportError = Schema.is(WorkstreamTransportError);
const decodeRequest = Schema.decodeUnknownEffect(WorkstreamsNativeHomeRead);
const contract = {
  contractVersion: Wire.WORKSTREAM_CONTRACT_HEADER_VERSION,
  contractManifest: Wire.WORKSTREAM_CONTRACT_MANIFEST_SHA256,
};
export const layer = Layer.effect(
  HomeReads,
  Effect.gen(function* () {
    const home = yield* NativeHome,
      projection = yield* Projection.Projection,
      local = yield* Local.LocalTransport,
      registry = yield* Registry.Registry,
      db = yield* Database.NativeSql;
    const summary = yield* Effect.serviceOption(RollupSummaryRead);
    const codec = new RegistryCursorCodec(NodeCrypto.randomBytes(32));
    const snapshots = new CoherentSnapshots<Projection.ProjectionSnapshot>();
    const produce = Effect.fn("NativeRead.produce")(function* (
      caller: AdmittedCaller,
      request: WorkstreamsNativeHomeRead,
      snapshot: Projection.ProjectionSnapshot,
      placementContract?: PlacementContract,
    ): Effect.fn.Return<ReadResult, RegistryError | WorkstreamTransportError> {
      const envelope = snapshot.envelope;
      const authorization_revision = snapshot.authorization.grant.grant_version;
      if (request.route === "read-projection") {
        const offset = Number(request.params.cursor ?? 0);
        const limit = request.params.limit ?? 50;
        if (!Number.isSafeInteger(offset) || offset < 0)
          return yield* new RegistryError({ code: "cursor_invalid" });
        const items = snapshot.items.slice(offset, offset + limit);
        return {
          route: request.route,
          envelope,
          authorization_revision,
          value: {
            items,
            next_cursor: offset + limit < snapshot.items.length ? String(offset + limit) : null,
          },
        };
      }
      if (request.route === "counts")
        return {
          route: request.route,
          envelope,
          authorization_revision,
          value: snapshot.counts,
        };
      if (request.route === "rollup-summary") {
        if (Option.isNone(summary))
          return yield* new RegistryError({ code: "provider_unconfigured" });
        const value = yield* summary.value.read(caller, request.params.workstream_id);
        return { route: request.route, envelope, value, authorization_revision };
      }
      if (request.route === "lifecycle-declarations") {
        const s = yield* registry.snapshot(caller);
        if (!s.state.workstreams.has(request.params.workstream_id))
          return yield* new RegistryError({ code: "not_found" });
        const route = "lifecycle-declarations:" + request.params.workstream_id;
        const position = yield* Effect.try({
          try: () =>
            codec.decode(
              request.params,
              s.authorization,
              route,
              snapshot.counts.context,
              Date.parse(envelope.provenance.observed_at),
            ),
          catch: Projection.readFailure,
        });
        if (
          position.after !== null &&
          (position.after.length !== 1 || !Number.isSafeInteger(position.after[0]))
        )
          return yield* new RegistryError({ code: "cursor_invalid" });
        const found = (s.state.lifecycle_declarations.get(request.params.workstream_id) ?? [])
          .filter((x) => position.after === null || x.revision > Number(position.after[0]))
          .toSorted((a, b) => a.revision - b.revision)
          .slice(0, position.limit + 1);
        const items = found.slice(0, position.limit);
        return {
          route: request.route,
          envelope,
          authorization_revision,
          value: {
            context: snapshot.counts.context,
            items,
            next_cursor:
              found.length > position.limit
                ? codec.encode(
                    { ...position, after: [items.at(-1)!.revision] },
                    s.authorization,
                    route,
                    snapshot.counts.context,
                  )
                : null,
          },
        };
      }
      const adapter = yield* local.forCaller(caller);
      const page = (params: { readonly limit?: number; readonly cursor?: string }) => ({
        ...contract,
        limit: params.limit ?? 50,
        ...(params.cursor ? { cursor: params.cursor } : {}),
      });
      const transport = adapter.transport;
      let value: unknown;
      switch (request.route) {
        case "workstreams":
          value = yield* transport.listWorkstreams(page(request.params));
          break;
        case "references":
          value = yield* transport.listReferences(page(request.params));
          break;
        case "workstream":
          value = yield* transport.getWorkstream({
            ...contract,
            workstreamId: request.params.workstream_id,
          });
          break;
        case "reference":
          value = yield* transport.getReference({
            ...contract,
            nativeReferenceId: request.params.native_reference_id,
          });
          break;
        case "memberships":
          value = yield* transport.listMemberships({
            ...page(request.params),
            workstreamId: request.params.workstream_id,
          });
          break;
        case "declarations":
          value = yield* transport.listDeclarations({
            ...page(request.params),
            workstreamId: request.params.workstream_id,
          });
          break;
        case "edges":
          value = yield* transport.listEdges({
            ...page(request.params),
            workstreamId: request.params.workstream_id,
          });
          break;
        case "history":
          value = yield* transport.listHistory({
            ...page(request.params),
            ...(request.params.workstream_id ? { workstreamId: request.params.workstream_id } : {}),
          });
          break;
        case "appearance":
          value = yield* transport.readAppearance!({
            workstream_ids: request.params.workstream_ids,
          });
          break;
        case "placements":
          value = yield* adapter.placementPage(
            {
              ...request.params,
              limit: request.params.limit ?? 50,
            },
            placementContract,
          );
          break;
      }
      return {
        route: request.route,
        envelope,
        value,
        authorization_revision,
        ...(request.route === "placements"
          ? {
              placement_contract: placementContract ?? ("workstreams-t3-placement/1.0.0" as const),
            }
          : {}),
      };
    });
    return HomeReads.of({
      read: Effect.fn("NativeRead.route")((caller, request, placementContract) => {
        const partition = canonicalJson({ home, caller });
        return db
          .transaction(
            home,
            Effect.gen(function* () {
              yield* decodeRequest(request, { onExcessProperty: "error" }).pipe(
                Effect.mapError(() => new RegistryError({ code: "invalid_request" })),
              );
              if (
                placementContract !== undefined &&
                (request.route !== "placements" ||
                  (placementContract !== "workstreams-t3-placement/1.0.0" &&
                    placementContract !== "workstreams-t3-placement/2.0.0"))
              )
                return yield* new RegistryError({ code: "invalid_request" });
              const current = yield* projection.capture(caller);
              const now = yield* Clock.currentTimeMillis;
              const { cursor: _cursor, ...params } = request.params as typeof request.params & {
                readonly cursor?: string;
                readonly limit?: number;
              };
              const cursor = _cursor;
              const query = canonicalJson({
                route: request.route,
                params: { ...params, limit: params.limit ?? 50 },
                placementContract: placementContract ?? "workstreams-t3-placement/1.0.0",
              });
              const prior =
                cursor === undefined
                  ? undefined
                  : yield* Effect.try({
                      try: () =>
                        snapshots.resume(cursor, partition, query, current.fingerprint, now),
                      catch: Projection.readFailure,
                    });
              const snapshot = prior?.lease.snapshot ?? current;
              const innerRequest = {
                ...request,
                params: { ...params, ...(prior ? { cursor: prior.inner } : {}) },
              } as WorkstreamsNativeHomeRead;
              // The whole authoritative read state was captured in this SQL transaction,
              // and on continuation its complete fingerprint was verified at current time.
              // Run the existing producers at that proved observation, including placement
              // provenance. Current authorization/coverage are checked with the real clock
              // again before exposure; a pinned clock is never an authority check.
              const clock = yield* Clock.Clock;
              const at = Date.parse(snapshot.envelope.provenance.observed_at);
              const pinnedClock: Clock.Clock = {
                ...clock,
                currentTimeMillisUnsafe: () => at,
                currentTimeMillis: Effect.succeed(at),
                currentTimeNanosUnsafe: () => BigInt(at) * 1_000_000n,
                currentTimeNanos: Effect.succeed(BigInt(at) * 1_000_000n),
              };
              const result = yield* produce(caller, innerRequest, snapshot, placementContract).pipe(
                Effect.provideService(Clock.Clock, pinnedClock),
              );
              yield* projection.assertCurrent(caller, snapshot);
              const returnedAt = yield* Clock.currentTimeMillis;
              // BEGIN IMMEDIATE keeps this complete captured state stable while the
              // read-only producers run. Check real time after production as evidence
              // can expire without a SQL write; do not add key/provider waits here.
              if (
                returnedAt < at ||
                returnedAt - at >= 900_000 ||
                (current.valid_until !== null && returnedAt >= current.valid_until)
              )
                return yield* new RegistryError({ code: "cursor_invalid" });
              const value = result.value;
              if (typeof value === "object" && value !== null && "next_cursor" in value) {
                const inner = value.next_cursor;
                if (inner !== null && typeof inner !== "string")
                  return yield* new RegistryError({ code: "invalid_response" });
                const next = yield* Effect.try({
                  try: () => snapshots.next(snapshot, partition, query, inner, returnedAt, prior),
                  catch: Projection.readFailure,
                });
                return structuredClone({ ...result, value: { ...value, next_cursor: next } });
              }
              return structuredClone(result);
            }),
          )
          .pipe(
            Effect.mapError((cause) =>
              isTransportError(cause)
                ? Projection.readFailure(cause.cause)
                : Projection.readFailure(cause),
            ),
            Effect.onExit((exit) =>
              exit._tag === "Failure" ? Effect.sync(() => snapshots.purge(partition)) : Effect.void,
            ),
          );
      }),
    });
  }),
);

/** Parent HTTP/MCP/CLI hooks call this boundary, never pass request-supplied caller identities. */
export class AuthorizedReads extends Context.Service<
  AuthorizedReads,
  {
    readonly read: (request: WorkstreamsNativeHomeRead) => Effect.Effect<ReadResult, RegistryError>;
  }
>()("t3/jones/workstreams/native/read/routes/AuthorizedReads") {}
export const authorizedLayer = Layer.effect(
  AuthorizedReads,
  Effect.gen(function* () {
    const boundary = yield* Admission.CallerBoundary,
      admission = yield* Admission.Admission,
      homeReads = yield* HomeReads,
      session = yield* Session.Session;
    return AuthorizedReads.of({
      read: Effect.fn("NativeRead.authorized")(function* (request) {
        const evidence = yield* boundary.current().pipe(Effect.mapError(Projection.readFailure));
        return yield* Effect.gen(function* () {
          const caller = yield* admission.admit("workstreams:read");
          const result = yield* homeReads.read(caller, request);
          yield* evidence.check("workstreams:read", null);
          const current = yield* session.authorize(caller, "workstreams:read");
          if (current.authorization.grant.grant_version !== result.authorization_revision)
            return yield* new RegistryError({ code: "forbidden" });
          return result;
        }).pipe(
          Effect.provideService(Admission.RequestCaller, evidence),
          Effect.mapError(Projection.readFailure),
        );
      }),
    });
  }),
);

export const responseHeaders = {
  "cache-control": "private, no-store",
  "x-content-type-options": "nosniff",
} as const;

const decodeResult = Schema.decodeUnknownEffect(WorkstreamsHomeReadResult);

/** Decode the route-specific value with its selected contract before checking coherence. */
export const validateReadResult = Effect.fn("NativeRead.validateForwarded")(
  function* (
    request: WorkstreamsNativeHomeRead,
    result: ReadResult,
    placementContract?: PlacementContract,
  ) {
    const selected = placementContract ?? "workstreams-t3-placement/1.0.0";
    if (placementContract !== undefined && request.route !== "placements")
      return yield* new RegistryError({ code: "invalid_response" });
    const decoded = yield* decodeResult(
      {
        contract: WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
        ...result,
        ...(result.route === "placements"
          ? { placement_contract: result.placement_contract ?? selected }
          : {}),
      },
      { onExcessProperty: "error" },
    );
    if (
      decoded.route !== request.route ||
      (decoded.route === "placements" && decoded.placement_contract !== selected)
    )
      return yield* new RegistryError({ code: "invalid_response" });
    const value = decoded.value;
    if (typeof value === "object" && value !== null && "context" in value) {
      const context = value.context;
      if (
        context.owner_id !== decoded.envelope.registry.owner_id ||
        context.server_generation !== decoded.envelope.registry.server_generation ||
        context.registry_version !== decoded.envelope.registry.registry_version
      )
        return yield* new RegistryError({ code: "invalid_response" });
    }
    if (
      decoded.route === "counts" &&
      decoded.value.observed_at !== decoded.envelope.provenance.observed_at
    )
      return yield* new RegistryError({ code: "invalid_response" });
    if (
      decoded.route === "workstream" &&
      request.route === "workstream" &&
      decoded.value.workstream.workstream_id !== request.params.workstream_id
    )
      return yield* new RegistryError({ code: "invalid_response" });
    if (
      decoded.route === "reference" &&
      request.route === "reference" &&
      decoded.value.reference.native_reference_id !== request.params.native_reference_id
    )
      return yield* new RegistryError({ code: "invalid_response" });
    if (
      decoded.route === "rollup-summary" &&
      request.route === "rollup-summary" &&
      decoded.value.workstream_id !== request.params.workstream_id
    )
      return yield* new RegistryError({ code: "invalid_response" });
    if (
      (decoded.route === "memberships" ||
        decoded.route === "declarations" ||
        decoded.route === "lifecycle-declarations") &&
      (request.route === "memberships" ||
        request.route === "declarations" ||
        request.route === "lifecycle-declarations") &&
      decoded.value.items.some((item) => item.workstream_id !== request.params.workstream_id)
    )
      return yield* new RegistryError({ code: "invalid_response" });
    if (
      decoded.route === "edges" &&
      request.route === "edges" &&
      decoded.value.items.some(
        (item) =>
          item.from_workstream_id !== request.params.workstream_id &&
          item.to_workstream_id !== request.params.workstream_id,
      )
    )
      return yield* new RegistryError({ code: "invalid_response" });
    if (decoded.route === "placements" && request.route === "placements") {
      const page = decoded.value;
      if (
        page.context.authorization_revision !== decoded.authorization_revision ||
        page.inventory_sha256 !==
          NodeCrypto.createHash("sha256")
            .update(Wire.t3PlacementInventoryJson(request.params.identities))
            .digest("hex")
      )
        return yield* new RegistryError({ code: "invalid_response" });
      const identities = new Set(request.params.identities.map(Wire.t3PlacementIdentityKey));
      if (page.items.some((item) => !identities.has(Wire.t3PlacementIdentityKey(item))))
        return yield* new RegistryError({ code: "invalid_response" });
      if (decoded.placement_contract === "workstreams-t3-placement/2.0.0") {
        const provenance = decoded.value.provenance;
        if (
          provenance.observed_at !== decoded.envelope.provenance.observed_at ||
          provenance.registry_instance_id !== decoded.envelope.registry.registry_instance_id ||
          provenance.home_environment_id !== decoded.envelope.provenance.home_environment_id ||
          provenance.home_generation !== decoded.envelope.registry.server_generation ||
          provenance.served_by_environment_id !==
            decoded.envelope.provenance.served_by_environment_id
        )
          return yield* new RegistryError({ code: "invalid_response" });
      }
    }
    const { contract: _contract, ...validated } = decoded;
    return validated;
  },
  (effect) =>
    effect.pipe(Effect.mapError((cause) => new RegistryError({ code: "invalid_response", cause }))),
);
