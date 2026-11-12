// @effect-diagnostics nodeBuiltinImport:off -- Exact public submission bytes bind the existing synchronous SHA-256 contract.
import * as NodeCrypto from "node:crypto";
import * as Wire from "@t3tools/contracts";
import * as Forward from "@t3tools/contracts/jones/workstreamsForwarding";
import type { ForwardView } from "../forwarding/forwarder.ts";
import { canonicalJson as requestJson } from "../../operations/encoding.ts";
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
const decodeForward = Schema.decodeUnknownEffect(Forward.WorkstreamsForwardResult);
const sha = (body: string) => NodeCrypto.createHash("sha256").update(body).digest("hex");
const unknown = () =>
  new Gateway.WorkstreamGatewayError({
    reason: "unknown-effect",
    detail: "The original member command outcome remains unresolved.",
  });
const accessIdentity = (value: Access.WorkstreamsHomeAccessResult) =>
  accessKey({
    ...value,
    home: { ...value.home, registry_version: 0 },
    capabilities: {
      ...value.capabilities,
      context: { ...value.capabilities.context, registry_version: 0 },
    },
  });
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
    write = false,
  ) {
    const started = epoch;
    const before = yield* access();
    let mutationEntered = false;
    const project = Effect.fn("MemberGateway.projectForward")(function* (
      view: ForwardView,
      id: string,
      body: string,
    ) {
      const result = yield* decodeForward(view.result, { onExcessProperty: "error" });
      if (
        view.operation_id !== id ||
        result.identity.source_operation_id !== id ||
        result.identity.parent_operation_id !== null ||
        result.identity.request_sha256 !== sha(body) ||
        result.home.registry.registry_instance_id !== before.home.registry_instance_id ||
        result.home.registry.owner_id !== before.home.owner_id ||
        result.home.registry.home_environment_id !== before.home.home_environment_id ||
        result.home.registry.server_generation !== before.home.server_generation ||
        result.home.registry.canonical_origin !== before.home.canonical_origin ||
        result.home.principal_id !== before.principal_id
      )
        return yield* new RegistryError({ code: "invalid_response" });
      return result;
    });
    const transportFailure = () =>
      new Gateway.WorkstreamTransportError({
        operation: "member-command",
        effect: "unknown-effect",
        detail: "Member command outcome is unknown.",
      });
    const receipt = (commandId: string, body?: string) =>
      app.commandReceipt({
        command_id: commandId,
        ...(body === undefined ? {} : { retained_body: body }),
      });
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
        return yield* Schema.decodeEffect(schema)(result.value, {
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
    // Reuse the public validators per call, without retaining auth, terminal receipts or offline pages.
    // MemberAccess has already intersected the real home grant with the original local write ceiling.
    const capabilities = before.capabilities;
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
      getCommand: (input) => receipt(input.commandId).pipe(Effect.mapError(transportFailure)),
      readCommandReceipt: (input) =>
        receipt(input.commandId).pipe(
          Effect.map((receipt) => ({ kind: "receipt" as const, receipt })),
          Effect.mapError(transportFailure),
        ),
      resubmitCommand: (input) =>
        receipt(input.idempotencyKey, input.body).pipe(
          Effect.map((receipt) => ({ kind: "receipt" as const, receipt })),
          Effect.catch((error) =>
            error.code === "operation_id_conflict"
              ? Effect.succeed({ kind: "idempotency_conflict" as const })
              : Effect.fail(transportFailure()),
          ),
        ),
      submitCommand: Effect.fn("MemberGateway.submitCommand")(function* (
        input: Parameters<Gateway.WorkstreamTransport["submitCommand"]>[0],
      ) {
        mutationEntered = true;
        const value = yield* project(
          yield* app.submitCommand(input),
          input.idempotencyKey,
          input.body,
        );
        if (value.kind !== "command") return yield* new RegistryError({ code: "invalid_response" });
        return value.result;
      }, Effect.mapError(transportFailure)),
      saveAppearance: Effect.fn("MemberGateway.saveAppearance")(function* (
        input: Parameters<NonNullable<Gateway.WorkstreamTransport["saveAppearance"]>>[0],
      ) {
        mutationEntered = true;
        const value = yield* project(
          yield* app.saveAppearance(input),
          input.command_id,
          requestJson(input),
        );
        if (value.kind !== "appearance")
          return yield* new RegistryError({ code: "invalid_response" });
        return value.result;
      }, Effect.mapError(transportFailure)),
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
        if (mutationEntered) return unknown();
        const reason = reasons.find((value) => value === error.detail);
        return error.reason === "offline" && reason !== undefined
          ? new Gateway.WorkstreamGatewayError({
              reason,
              detail: "Member Workstreams read refused.",
            })
          : error;
      }),
    );
    const after = yield* access().pipe(
      Effect.mapError((error) => (mutationEntered ? unknown() : error)),
    );
    if (
      started !== epoch ||
      (write
        ? accessIdentity(before) !== accessIdentity(after) ||
          after.home.registry_version < before.home.registry_version
        : accessKey(before) !== accessKey(after))
    )
      return yield* new Gateway.WorkstreamGatewayError({
        reason: mutationEntered ? "unknown-effect" : "authorization-not-current",
        detail: "Member home access changed during observation.",
      });
    return result;
  });
  return Gateway.WorkstreamGateway.of({
    readSession: Effect.fn("Gateway.WorkstreamGateway.readSession")(() =>
      run((gateway) => gateway.readSession()),
    ),
    readMetadata: Effect.fn("Gateway.WorkstreamGateway.readMetadata")((input) =>
      run((gateway) => gateway.readMetadata(input)),
    ),
    readDetail: Effect.fn("Gateway.WorkstreamGateway.readDetail")((id) =>
      run((gateway) => gateway.readDetail(id)),
    ),
    readReferences: Effect.fn("Gateway.WorkstreamGateway.readReferences")((input) =>
      run((gateway) => gateway.readReferences(input)),
    ),
    readReference: Effect.fn("Gateway.WorkstreamGateway.readReference")((id) =>
      run((gateway) => gateway.readReference(id)),
    ),
    readMemberships: Effect.fn("Gateway.WorkstreamGateway.readMemberships")((id, input) =>
      run((gateway) => gateway.readMemberships(id, input)),
    ),
    readDeclarations: Effect.fn("Gateway.WorkstreamGateway.readDeclarations")((id, input) =>
      run((gateway) => gateway.readDeclarations(id, input)),
    ),
    readEdges: Effect.fn("Gateway.WorkstreamGateway.readEdges")((id, input) =>
      run((gateway) => gateway.readEdges(id, input)),
    ),
    readHistory: Effect.fn("Gateway.WorkstreamGateway.readHistory")((id, input) =>
      run((gateway) => gateway.readHistory(id, input)),
    ),
    readRegistryCounts: Effect.fn("Gateway.WorkstreamGateway.readRegistryCounts")(() =>
      run((gateway) => gateway.readRegistryCounts()),
    ),
    readAppearance: Effect.fn("Gateway.WorkstreamGateway.readAppearance")((ids) =>
      run((gateway) => gateway.readAppearance(ids)),
    ),
    readThreadPlacements: Effect.fn("Gateway.WorkstreamGateway.readThreadPlacements")((input) =>
      run((gateway) => gateway.readThreadPlacements(input)),
    ),
    saveAppearance: Effect.fn("Gateway.WorkstreamGateway.saveAppearance")((input) =>
      run((gateway) => gateway.saveAppearance(input), true),
    ),
    submit: Effect.fn("Gateway.WorkstreamGateway.submit")((command) =>
      run((gateway) => gateway.submit(command), true),
    ),
    pollCommand: Effect.fn("Gateway.WorkstreamGateway.pollCommand")((id) =>
      run((gateway) => gateway.pollCommand(id)),
    ),
    readReceipt: Effect.fn("Gateway.WorkstreamGateway.readReceipt")((id) =>
      run((gateway) => gateway.readReceipt(id)),
    ),
    // The native transport's retained resubmit is lookup-only, even when source intent is missing.
    resubmit: Effect.fn("Gateway.WorkstreamGateway.resubmit")((id, body) =>
      run((gateway) => gateway.resubmit(id, body), true),
    ),
    purgeAuthorization: () => {
      epoch++;
    },
  });
});

/** Application-owned adapter; all caller and home observations are acquired in the original request. */
export const layer = Layer.effect(Gateway.WorkstreamGateway, make);
