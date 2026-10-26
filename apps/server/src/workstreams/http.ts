import { NativeEntry } from "../jones/workstreams/native/integration.ts";
import { decodeRegistryBinding } from "@t3tools/shared/jones/workstreamsRegistryBinding";
import * as WorkstreamOperations from "../jones/workstreams/operations/service.ts";
import * as WorkstreamOperationsLive from "../jones/workstreams/operations/live.ts";
import { Activation } from "../jones/workstreams/operations/ports.ts";
import * as Option from "effect/Option";
import * as DesktopNativeOwner from "../jones/workstreams/DesktopNativeOwner.ts";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpConflictError,
  EnvironmentHttpApi,
  EnvironmentInternalError,
  T3_PLACEMENT_MAX_REQUEST_BYTES,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import { ServerConfig } from "../config.ts";
import { resolveWorkstreamActivation } from "../jones/workstreams/activation.ts";
import * as Layer from "effect/Layer";
import * as ByteSize from "effect/ByteSize";
import * as HttpEffect from "effect/http/HttpEffect";
import {
  HttpIncomingMessage,
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/http";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

import {
  annotateEnvironmentRequest,
  failEnvironmentInternal,
  requireEnvironmentScope,
} from "../auth/http.ts";
import type { T3PlacementTrustProvider } from "../environment/NativePlacementTrust.ts";
import * as NativeStoreAuthority from "../environment/NativeStoreAuthority.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import { makeControlPlaneWorkstreamTransport } from "./ControlPlaneWorkstreamTransport.ts";
import { WorkstreamGateway, make, type WorkstreamGatewayError } from "./WorkstreamGateway.ts";

import { createRegistrationContextHandler } from "../jones/workstreams/registrationContext/http.ts";
import {
  WorkstreamsRegistrationContext,
  makeWorkstreamsRegistrationContext,
} from "../jones/workstreams/registrationContext/service.ts";

export const WORKSTREAM_RESPONSE_HEADERS = {
  "cache-control": "private, no-store",
  "x-content-type-options": "nosniff",
} as const;

const workstreamHttpPath = (originalUrl: string): string | undefined => {
  if (originalUrl.startsWith("/")) return originalUrl.split(/[?#]/, 1)[0];
  try {
    return new URL(originalUrl).pathname;
  } catch {
    return undefined;
  }
};

export const isWorkstreamHttpTarget = (originalUrl: string): boolean => {
  const path = workstreamHttpPath(originalUrl);
  return path === "/api/workstreams" || path?.startsWith("/api/workstreams/") === true;
};

const appendWorkstreamResponseHeaders = HttpEffect.appendPreResponseHandler((_request, response) =>
  Effect.succeed(HttpServerResponse.setHeaders(response, WORKSTREAM_RESPONSE_HEADERS)),
);

export const withWorkstreamBodyLimit = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  request: { readonly originalUrl: string; readonly method: string },
): Effect.Effect<A, E, R> =>
  request.method === "POST" &&
  workstreamHttpPath(request.originalUrl) === "/api/workstreams/thread-placements"
    ? effect.pipe(
        Effect.provideService(
          HttpIncomingMessage.MaxBodySize,
          ByteSize.bytes(T3_PLACEMENT_MAX_REQUEST_BYTES),
        ),
      )
    : effect;

export const workstreamResponseHeadersLayer = HttpRouter.middleware(
  (httpEffect) =>
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      if (isWorkstreamHttpTarget(request.originalUrl)) yield* appendWorkstreamResponseHeaders;
      return yield* withWorkstreamBodyLimit(httpEffect, request);
    }),
  { global: true },
);

class WorkstreamControlPlane extends Context.Service<
  WorkstreamControlPlane,
  ReturnType<typeof makeControlPlaneWorkstreamTransport> & {
    readonly registryInstanceId: string | null;
  }
>()("t3/workstreams/http/WorkstreamControlPlane") {}

// Both consumers provide this same layer identity, so one server layer graph owns one snapshot.
const workstreamControlPlaneLayer = Layer.effect(
  WorkstreamControlPlane,
  Effect.gen(function* () {
    const { baseDir } = yield* ServerConfig;
    const activation = yield* Effect.sync(() => resolveWorkstreamActivation(baseDir));
    const configured = makeControlPlaneWorkstreamTransport(activation);
    // Operations use the registry instance ID, while the legacy gateway binding uses its origin.
    const registryInstanceId =
      activation.state === "enabled" && activation.value.registryBinding !== undefined
        ? decodeRegistryBinding(activation.value.registryBinding).registry_instance_id
        : null;
    return { ...configured, registryInstanceId };
  }),
);

const makeWorkstreamGatewayLayerLive = (placementTrustProvider?: T3PlacementTrustProvider) =>
  Layer.effect(
    WorkstreamGateway,
    Effect.gen(function* () {
      const configured = yield* WorkstreamControlPlane;
      const nativeAuthority =
        placementTrustProvider === undefined
          ? yield* NativeStoreAuthority.NativeStoreAuthority
          : undefined;
      const resolvedPlacementTrustProvider =
        placementTrustProvider ?? nativeAuthority?.trustProvider;
      return yield* make(configured.transport, {
        binding: configured.binding,
        ...(resolvedPlacementTrustProvider === undefined
          ? {}
          : { placementTrustProvider: resolvedPlacementTrustProvider }),
      });
    }),
  );
export const workstreamNativeAuthorityLayerLive = NativeStoreAuthority.layer.pipe(
  Layer.provideMerge(DesktopNativeOwner.layer),
  Layer.provide(ServerEnvironment.layerIdentity),
);

export const workstreamGatewayLayerLive = makeWorkstreamGatewayLayerLive().pipe(
  Layer.provide(workstreamNativeAuthorityLayerLive),
  Layer.provide(workstreamControlPlaneLayer),
);

export const workstreamRegistrationContextLayerLive = Layer.effect(
  WorkstreamsRegistrationContext,
  Effect.gen(function* () {
    const configured = yield* WorkstreamControlPlane;
    const authority = yield* NativeStoreAuthority.NativeStoreAuthority;
    return makeWorkstreamsRegistrationContext({ ...configured.registrationContext, authority });
  }),
).pipe(
  Layer.provide(workstreamNativeAuthorityLayerLive),
  Layer.provide(workstreamControlPlaneLayer),
);

export const workstreamOperationsLayerLive = WorkstreamOperationsLive.layer.pipe(
  Layer.provide(
    Layer.effect(
      Activation,
      Effect.gen(function* () {
        const configured = yield* WorkstreamControlPlane;
        // Reuse startup qualification. Do not re-open activation files or expose values here.
        return Activation.of({
          registryId: configured.registryInstanceId,
          status: {
            state: configured.activationState,
            missing_keys:
              configured.activationState === "disabled"
                ? [
                    "T3_WORKSTREAM_CONTROL_PLANE_URL",
                    "T3_WORKSTREAM_OWNER_ID",
                    "T3_WORKSTREAM_PRINCIPAL_ID",
                    "T3_WORKSTREAM_KEY_ID",
                    "T3_WORKSTREAM_SIGNING_SECRET",
                    "T3_WORKSTREAM_AUTHORIZATION_REVISION",
                    "T3_WORKSTREAM_REGISTRY_BINDING",
                    "T3_WORKSTREAM_REGISTRATION_CONTEXT_VERSION",
                  ]
                : [],
          },
        });
      }),
    ),
  ),
  Layer.provide(workstreamControlPlaneLayer),
);

const internal = <A>(
  operation: string,
  effect: Effect.Effect<A, WorkstreamGatewayError>,
): Effect.Effect<A, EnvironmentInternalError> =>
  effect.pipe(
    Effect.catch((error) =>
      failEnvironmentInternal("internal_error", { operation, reason: error.reason }),
    ),
  );

const restartable = <A>(
  operation: string,
  effect: Effect.Effect<A, WorkstreamGatewayError>,
): Effect.Effect<A, EnvironmentHttpConflictError | EnvironmentInternalError> =>
  effect.pipe(
    Effect.catch(
      (error): Effect.Effect<never, EnvironmentHttpConflictError | EnvironmentInternalError> => {
        if (error.reason === "cursor-stale")
          return Effect.fail(
            new EnvironmentHttpConflictError({ message: "workstream_cursor_stale" }),
          );
        // Unconfigured activation is a stable server state, not an outage clients should retry.
        if (error.reason === "not-configured")
          return Effect.fail(
            new EnvironmentHttpConflictError({ message: "workstreams_not_configured" }),
          );
        return failEnvironmentInternal("internal_error", { operation, reason: error.reason });
      },
    ),
  );

const pageInput = (payload: {
  readonly limit?: number | undefined;
  readonly cursor?: string | undefined;
}): { readonly limit?: number; readonly cursor?: string } => {
  const input: { limit?: number; cursor?: string } = {};
  if (payload.limit !== undefined) input.limit = payload.limit;
  if (payload.cursor !== undefined) input.cursor = payload.cursor;
  return input;
};

export const workstreamHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "workstreams",
  Effect.fnUntraced(function* (handlers) {
    const gateway = yield* WorkstreamGateway;
    const registrationContext = yield* WorkstreamsRegistrationContext;
    // Legacy isolated consumers may provide only the original gateway. Production always
    // provides the operations layer; absent capability fails closed on the new routes.
    const operationService = yield* Effect.serviceOption(WorkstreamOperations.WorkstreamOperations);
    const operations = <A, E>(
      run: (service: WorkstreamOperations.WorkstreamOperations["Service"]) => Effect.Effect<A, E>,
    ): Effect.Effect<A, E | EnvironmentInternalError> =>
      Option.isNone(operationService)
        ? failEnvironmentInternal("internal_error", {
            operation: "workstreamOperations",
            reason: "service_unavailable",
          })
        : run(operationService.value);
    const read = Effect.fn("WorkstreamHttp.read")(function* (name: string) {
      yield* annotateEnvironmentRequest(name);
      yield* requireEnvironmentScope(AuthOrchestrationReadScope);
    });
    return handlers
      .handle("operationSubmit", (args) =>
        Effect.gen(function* () {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          return yield* operations((service) => service.submit(args.payload)).pipe(
            Effect.catchTags({
              OperationConflict: (error) =>
                Effect.fail({
                  code: error.code,
                  ...(error.open_operation_id === undefined
                    ? {}
                    : { open_operation_id: error.open_operation_id }),
                }),
              OperationInternalError: () =>
                failEnvironmentInternal("internal_error", {
                  operation: "workstreamOperations",
                  reason: "internal_error",
                }),
            }),
          );
        }),
      )
      .handle("operationRead", (args) =>
        read(args.endpoint.name).pipe(
          Effect.andThen(operations((service) => service.read(args.payload))),
        ),
      )
      .handle("setupStatus", (args) =>
        read(args.endpoint.name).pipe(
          Effect.andThen(operations((service) => service.setupStatus())),
        ),
      )
      .handle("registrationContext", createRegistrationContextHandler(registrationContext))
      .handle("appearanceRead", (args) =>
        read(args.endpoint.name).pipe(
          Effect.andThen(
            internal("appearanceRead", gateway.readAppearance(args.payload.workstream_ids)),
          ),
        ),
      )
      .handle("appearanceSave", (args) =>
        Effect.gen(function* () {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          return yield* internal("appearanceSave", gateway.saveAppearance(args.payload));
        }),
      )
      .handle("threadPlacements", (args) =>
        Effect.gen(function* () {
          yield* read(args.endpoint.name);
          const contract = args.headers["x-t3-workstreams-placement-contract"];
          if (contract === undefined)
            return yield* internal("threadPlacements", gateway.readThreadPlacements(args.payload));
          const native = yield* Effect.serviceOption(NativeEntry);
          if (Option.isNone(native))
            return yield* new EnvironmentHttpConflictError({
              message: "workstreams_native_unavailable",
            });
          return yield* native.value.adapter
            .placementResponse(args.payload, contract)
            .pipe(
              Effect.mapError(
                () =>
                  new EnvironmentHttpConflictError({ message: "workstreams_native_unavailable" }),
              ),
            );
        }),
      )
      .handle("legacyReceiptLookup", (args) =>
        Effect.gen(function* () {
          yield* read(args.endpoint.name);
          const native = yield* Effect.serviceOption(NativeEntry);
          if (Option.isNone(native))
            return yield* new EnvironmentHttpConflictError({
              message: "workstreams_legacy_receipts_unavailable",
            });
          return yield* native.value.adapter.lookupLegacyReceipt(args.payload).pipe(
            Effect.mapError(
              () =>
                new EnvironmentHttpConflictError({
                  message: "workstreams_legacy_receipts_unavailable",
                }),
            ),
          );
        }),
      )
      .handle("list", (args) =>
        read(args.endpoint.name).pipe(
          Effect.andThen(restartable("list", gateway.readMetadata(pageInput(args.payload)))),
        ),
      )
      .handle("detail", (args) =>
        read(args.endpoint.name).pipe(
          Effect.andThen(internal("detail", gateway.readDetail(args.params.workstreamId))),
        ),
      )
      .handle("references", (args) =>
        read(args.endpoint.name).pipe(
          Effect.andThen(
            restartable("references", gateway.readReferences(pageInput(args.payload))),
          ),
        ),
      )
      .handle("reference", (args) =>
        read(args.endpoint.name).pipe(
          Effect.andThen(
            internal("reference", gateway.readReference(args.params.nativeReferenceId)),
          ),
        ),
      )
      .handle("memberships", (args) =>
        read(args.endpoint.name).pipe(
          Effect.andThen(
            restartable(
              "memberships",
              gateway.readMemberships(args.params.workstreamId, pageInput(args.payload)),
            ),
          ),
        ),
      )
      .handle("declarations", (args) =>
        read(args.endpoint.name).pipe(
          Effect.andThen(
            restartable(
              "declarations",
              gateway.readDeclarations(args.params.workstreamId, pageInput(args.payload)),
            ),
          ),
        ),
      )
      .handle("edges", (args) =>
        read(args.endpoint.name).pipe(
          Effect.andThen(
            restartable(
              "edges",
              gateway.readEdges(args.params.workstreamId, pageInput(args.payload)),
            ),
          ),
        ),
      )
      .handle("history", (args) =>
        read(args.endpoint.name).pipe(
          Effect.andThen(
            restartable(
              "history",
              gateway.readHistory(args.params.workstreamId, pageInput(args.payload)),
            ),
          ),
        ),
      )
      .handle("command", (args) =>
        read(args.endpoint.name).pipe(
          Effect.andThen(internal("command", gateway.pollCommand(args.params.commandId))),
        ),
      )
      .handle("submit", (args) =>
        Effect.gen(function* () {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          // Only gateway checks before dispatch establish no effect; transport and replay failures do not.
          return yield* gateway.submit(args.payload.command).pipe(
            Effect.catch(
              (
                error,
              ): Effect.Effect<never, EnvironmentHttpConflictError | EnvironmentInternalError> =>
                error.reason === "version-conflict" || error.reason === "authorization-not-current"
                  ? Effect.fail(
                      new EnvironmentHttpConflictError({
                        message: "workstream_command_not_current",
                      }),
                    )
                  : failEnvironmentInternal("internal_error", {
                      operation: "submit",
                      reason: error.reason,
                    }),
            ),
          );
        }),
      );
  }),
);
