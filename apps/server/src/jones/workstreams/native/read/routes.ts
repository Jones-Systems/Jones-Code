// @effect-diagnostics nodeBuiltinImport:off -- Process-local lifecycle cursor key, never persisted.
import * as NodeCrypto from "node:crypto";
import * as Wire from "@t3tools/contracts";
import {
  WorkstreamsNativeHomeRead,
  type WorkstreamsNativeReadEnvelope,
} from "@t3tools/contracts/jones/workstreamsNativeRead";
import { WorkstreamsRollupSummary } from "@t3tools/contracts/jones/workstreamsRollups";
import * as Context from "effect/Context";
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

export interface ReadResult {
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
    ) => Effect.Effect<ReadResult, RegistryError>;
  }
>()("t3/jones/workstreams/native/read/routes/HomeReads") {}

const isTransportError = Schema.is(WorkstreamTransportError);
const decodeContext = Schema.decodeUnknownEffect(Wire.WorkstreamReadContext);
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
    return HomeReads.of({
      read: Effect.fn("NativeRead.route")((caller, request) =>
        db
          .transaction(
            home,
            Effect.gen(function* () {
              yield* decodeRequest(request).pipe(
                Effect.mapError(() => new RegistryError({ code: "invalid_request" })),
              );
              if (request.route === "read-projection") {
                const snapshot = yield* projection.capture(caller);
                const { envelope, ...value } = yield* projection.page(caller, request.params);
                return {
                  route: request.route,
                  envelope,
                  value,
                  authorization_revision: snapshot.authorization.grant.grant_version,
                };
              }
              const snapshot = yield* projection.capture(caller);
              const envelope = snapshot.envelope;
              const authorization_revision = snapshot.authorization.grant.grant_version;
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
                yield* projection.assertCurrent(caller, snapshot);
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
                const found = (
                  s.state.lifecycle_declarations.get(request.params.workstream_id) ?? []
                )
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
                    ...(request.params.workstream_id
                      ? { workstreamId: request.params.workstream_id }
                      : {}),
                  });
                  break;
                case "appearance":
                  value = yield* transport.readAppearance!({
                    workstream_ids: request.params.workstream_ids,
                  });
                  break;
                case "placements":
                  value = yield* transport.listThreadPlacements!({
                    ...request.params,
                    limit: request.params.limit ?? 50,
                  });
                  break;
              }
              return { route: request.route, envelope, value, authorization_revision };
            }),
          )
          .pipe(
            Effect.mapError((cause) =>
              isTransportError(cause)
                ? Projection.readFailure(cause.cause)
                : Projection.readFailure(cause),
            ),
          ),
      ),
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

const projectionValue = Schema.Struct({
  items: Schema.Array(
    Schema.Struct({
      workstream: Wire.Workstream,
      active_memberships: Schema.Struct({
        primary: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
        secondary: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
      }),
      appearance: Wire.WorkstreamAppearance,
      lifecycle: Schema.NullOr(Wire.WorkstreamLifecycleDeclaration),
    }),
  ).check(Schema.isMaxLength(100)),
  next_cursor: Schema.NullOr(Schema.String),
});
const lifecycleValue = Schema.Struct({
  context: Wire.WorkstreamReadContext,
  items: Schema.Array(Wire.WorkstreamLifecycleDeclaration).check(Schema.isMaxLength(100)),
  next_cursor: Schema.NullOr(Schema.String),
});

const readDecoders = {
  counts: Schema.decodeUnknownEffect(Wire.WorkstreamRegistryCounts),
  workstreams: Schema.decodeUnknownEffect(Wire.WorkstreamPage),
  references: Schema.decodeUnknownEffect(Wire.WorkstreamReferencePage),
  workstream: Schema.decodeUnknownEffect(Wire.WorkstreamDetail),
  reference: Schema.decodeUnknownEffect(Wire.WorkstreamReferenceDetail),
  memberships: Schema.decodeUnknownEffect(Wire.WorkstreamMembershipPage),
  declarations: Schema.decodeUnknownEffect(Wire.WorkstreamDeclarationPage),
  edges: Schema.decodeUnknownEffect(Wire.WorkstreamEdgePage),
  history: Schema.decodeUnknownEffect(Wire.WorkstreamHistoryPage),
  placements: Schema.decodeUnknownEffect(Wire.T3PlacementPage),
  appearance: Schema.decodeUnknownEffect(Wire.WorkstreamAppearanceResult),
  "lifecycle-declarations": Schema.decodeUnknownEffect(lifecycleValue),
  "read-projection": Schema.decodeUnknownEffect(projectionValue),
  "rollup-summary": Schema.decodeUnknownEffect(WorkstreamsRollupSummary),
};

/** Validate forwarded payloads as well as their envelope; provenance cannot bless a different v1 context. */
export const validateReadResult = Effect.fn("NativeRead.validateForwarded")(
  function* (request: WorkstreamsNativeHomeRead, result: ReadResult) {
    if (!Number.isSafeInteger(result.authorization_revision) || result.authorization_revision < 1)
      return yield* new RegistryError({ code: "invalid_response" });
    const value: unknown = yield* readDecoders[request.route](result.value, {
      onExcessProperty: "error",
    });
    if (typeof value === "object" && value !== null && "context" in value) {
      const context = yield* decodeContext(value.context);
      if (
        context.owner_id !== result.envelope.registry.owner_id ||
        context.server_generation !== result.envelope.registry.server_generation ||
        context.registry_version !== result.envelope.registry.registry_version
      )
        return yield* new RegistryError({ code: "invalid_response" });
    }
    if (
      request.route === "counts" &&
      typeof value === "object" &&
      value !== null &&
      "observed_at" in value &&
      value.observed_at !== result.envelope.provenance.observed_at
    )
      return yield* new RegistryError({ code: "invalid_response" });
    return { ...result, value };
  },
  (effect) => effect.pipe(Effect.mapError(() => new RegistryError({ code: "invalid_response" }))),
);
