import * as Wire from "@t3tools/contracts";
import * as Access from "@t3tools/contracts/jones/workstreamsHomeAccess";
import type { WorkstreamsNativeHomeRead } from "@t3tools/contracts/jones/workstreamsNativeRead";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Gateway from "../../../../workstreams/WorkstreamGateway.ts";
import * as Application from "../runtime/memberApplication.ts";
import { RegistryError } from "../registry/session.ts";
import { canonicalJson } from "../domain/validation.ts";
import { validateReadResult } from "./routes.ts";

const isRegistryError = Schema.is(RegistryError);
const decodeAccess = Schema.decodeUnknownEffect(Access.WorkstreamsHomeAccessResult);
const nativeFailure = (error: RegistryError): Gateway.WorkstreamGatewayError => {
  const reason: Gateway.WorkstreamGatewayError["reason"] =
    error.code === "forbidden" || error.code === "authority_fenced"
      ? "authorization-not-current"
      : error.code === "contract_mismatch"
        ? "contract-mismatch"
        : error.code === "cursor_stale" || error.code === "cursor_invalid"
          ? "cursor-stale"
          : error.code === "generation_conflict" || error.code === "registry_mismatch"
            ? "stale"
            : error.code === "invalid_request" || error.code === "invalid_response"
              ? "invalid-response"
              : "offline";
  return new Gateway.WorkstreamGatewayError({ reason, detail: "Member Workstreams read refused." });
};
const unsupported = () =>
  Effect.fail(
    new Gateway.WorkstreamGatewayError({
      reason: "not-configured",
      detail: "This member Gateway does not support writes or command receipt recovery.",
    }),
  );
const accessKey = ({ observed_at: _time, ...value }: Access.WorkstreamsHomeAccessResult) =>
  canonicalJson(value);
const page = (input: { readonly limit: number; readonly cursor?: string | undefined }) => ({
  limit: input.limit,
  ...(input.cursor === undefined ? {} : { cursor: input.cursor }),
});

const make = Effect.gen(function* () {
  const app = yield* Application.MemberApplication;
  const clock = yield* Clock.Clock;
  let epoch = 0;
  const access = Effect.fn("MemberGateway.access")(function* () {
    return yield* app.access().pipe(
      Effect.flatMap((value) => decodeAccess(value, { onExcessProperty: "error" })),
      Effect.mapError((error) =>
        isRegistryError(error)
          ? nativeFailure(error)
          : new Gateway.WorkstreamGatewayError({
              reason: "invalid-response",
              detail: "Invalid member home access observation.",
            }),
      ),
    );
  });
  const run = Effect.fn("MemberGateway.read")(function* <A>(
    use: (
      gateway: Gateway.WorkstreamGateway["Service"],
    ) => Effect.Effect<A, Gateway.WorkstreamGatewayError>,
  ) {
    const started = epoch;
    const before = yield* access();
    const read = Effect.fn("MemberGateway.route")(function* <S extends Schema.Decoder<unknown>>(
      request: WorkstreamsNativeHomeRead,
      schema: S,
    ) {
      return yield* Effect.gen(function* () {
        const result = yield* validateReadResult(
          request,
          yield* app.read(
            request,
            request.route === "placements" ? "workstreams-t3-placement/1.0.0" : undefined,
          ),
          request.route === "placements" ? "workstreams-t3-placement/1.0.0" : undefined,
        );
        const e = result.envelope;
        if (
          result.authorization_revision !== before.authorization_revision ||
          canonicalJson({
            ...e.registry,
            home_environment_id: e.provenance.home_environment_id,
          }) !== canonicalJson(before.home) ||
          e.provenance.home_environment_id !== before.home.home_environment_id ||
          e.provenance.served_by_environment_id !== before.home.home_environment_id ||
          e.provenance.authority !== "native-home" ||
          e.provenance.source !== "live" ||
          e.provenance.stale ||
          e.coverage.registry !== "complete" ||
          (result.route === "counts" && result.value.principal_id !== before.principal_id) ||
          (result.route === "placements" &&
            (result.value.context.principal_id !== before.principal_id ||
              result.value.context.authorization_revision !== before.authorization_revision))
        )
          return yield* new RegistryError({ code: "forbidden" });
        return yield* Schema.decodeUnknownEffect(schema)(result.value, {
          onExcessProperty: "error",
        }).pipe(Effect.mapError(() => new RegistryError({ code: "invalid_response" })));
      }).pipe(
        // The legacy Gateway transport carries only a closed public reason, never native causes or payloads.
        Effect.mapError(
          (error) =>
            new Gateway.WorkstreamTransportError({
              operation: "member-read",
              effect: "no-effect",
              detail: nativeFailure(error).reason,
            }),
        ),
      );
    });
    // A fresh legacy validator per call retains no caller, terminal receipt or offline page cache.
    // Write permission is deliberately withheld from this read adapter's public binding.
    const capabilities = { ...before.capabilities, permissions: ["workstreams:read" as const] };
    const transport: Gateway.WorkstreamTransport = {
      getCapabilities: () => Effect.succeed(capabilities),
      getCountsCapabilities: () =>
        Effect.succeed({
          ...capabilities,
          manifest_sha256: Wire.WORKSTREAM_COUNTS_MANIFEST_SHA256,
          registry_counts: {
            version: 1,
            path: "/workstreams/v1/counts",
            active_definition: "latest_owner_lifecycle_declaration_active",
          },
        }),
      getRegistryCounts: () => read({ route: "counts", params: {} }, Wire.WorkstreamRegistryCounts),
      listWorkstreams: (input) =>
        read({ route: "workstreams", params: page(input) }, Wire.WorkstreamPage),
      getWorkstream: (input) =>
        read(
          { route: "workstream", params: { workstream_id: input.workstreamId } },
          Wire.WorkstreamDetail,
        ),
      listReferences: (input) =>
        read({ route: "references", params: page(input) }, Wire.WorkstreamReferencePage),
      getReference: (input) =>
        read(
          { route: "reference", params: { native_reference_id: input.nativeReferenceId } },
          Wire.WorkstreamReferenceDetail,
        ),
      listMemberships: (input) =>
        read(
          { route: "memberships", params: { ...page(input), workstream_id: input.workstreamId } },
          Wire.WorkstreamMembershipPage,
        ),
      listDeclarations: (input) =>
        read(
          { route: "declarations", params: { ...page(input), workstream_id: input.workstreamId } },
          Wire.WorkstreamDeclarationPage,
        ),
      listEdges: (input) =>
        read(
          { route: "edges", params: { ...page(input), workstream_id: input.workstreamId } },
          Wire.WorkstreamEdgePage,
        ),
      listHistory: (input) =>
        read(
          {
            route: "history",
            params: {
              ...page(input),
              ...(input.workstreamId === undefined ? {} : { workstream_id: input.workstreamId }),
            },
          },
          Wire.WorkstreamHistoryPage,
        ),
      readAppearance: (input) =>
        read({ route: "appearance", params: input }, Wire.WorkstreamAppearanceResult).pipe(
          Effect.map((value) =>
            value.supported
              ? { ...value, page: { ...value.page, permissions: capabilities.permissions } }
              : value,
          ),
        ),
      // Frozen v1 cannot represent a native v2 proof. The home refuses nonempty native pages;
      // this adapter never strips their evidence to make them fit.
      listThreadPlacements: (input) =>
        read(
          { route: "placements", params: { ...page(input), identities: input.identities } },
          Wire.T3PlacementPage,
        ),
      getCommand: () =>
        Effect.fail(
          new Gateway.WorkstreamTransportError({
            operation: "receipt",
            effect: "unknown-effect",
            detail: "Receipt lookup is unavailable.",
          }),
        ),
      submitCommand: () =>
        Effect.fail(
          new Gateway.WorkstreamTransportError({
            operation: "submit",
            effect: "no-effect",
            detail: "Member Gateway writes are unavailable.",
          }),
        ),
    };
    const gateway = yield* Gateway.make(transport, {
      binding: {
        registryId: before.home.canonical_origin,
        ownerId: before.home.owner_id,
        principalId: before.principal_id,
        authorizationRevision: before.authorization_revision,
      },
      now: () => clock.currentTimeMillisUnsafe(),
    });
    const result = yield* use(gateway).pipe(
      Effect.mapError((error) => {
        const reasons = [
          "authorization-not-current",
          "contract-mismatch",
          "cursor-stale",
          "stale",
          "invalid-response",
          "offline",
        ] as const;
        const reason = reasons.find((value) => value === error.detail);
        return error.reason === "offline" && reason !== undefined
          ? new Gateway.WorkstreamGatewayError({
              reason,
              detail: "Member Workstreams read refused.",
            })
          : error;
      }),
    );
    const after = yield* access();
    if (started !== epoch || accessKey(before) !== accessKey(after))
      return yield* new Gateway.WorkstreamGatewayError({
        reason: "authorization-not-current",
        detail: "Member home access changed during observation.",
      });
    return result;
  });
  return Gateway.WorkstreamGateway.of({
    readSession: () => run((gateway) => gateway.readSession()),
    readMetadata: (input) => run((gateway) => gateway.readMetadata(input)),
    readDetail: (id) => run((gateway) => gateway.readDetail(id)),
    readReferences: (input) => run((gateway) => gateway.readReferences(input)),
    readReference: (id) => run((gateway) => gateway.readReference(id)),
    readMemberships: (id, input) => run((gateway) => gateway.readMemberships(id, input)),
    readDeclarations: (id, input) => run((gateway) => gateway.readDeclarations(id, input)),
    readEdges: (id, input) => run((gateway) => gateway.readEdges(id, input)),
    readHistory: (id, input) => run((gateway) => gateway.readHistory(id, input)),
    readRegistryCounts: () => run((gateway) => gateway.readRegistryCounts()),
    readAppearance: (ids) => run((gateway) => gateway.readAppearance(ids)),
    readThreadPlacements: (input) => run((gateway) => gateway.readThreadPlacements(input)),
    saveAppearance: unsupported,
    submit: unsupported,
    pollCommand: unsupported,
    readReceipt: unsupported,
    resubmit: unsupported,
    purgeAuthorization: () => {
      epoch++;
    },
  });
});

/** Application-owned adapter; all caller and home observations are acquired in the original request. */
export const layer = Layer.effect(Gateway.WorkstreamGateway, make);
