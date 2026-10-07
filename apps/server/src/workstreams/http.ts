import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpConflictError,
  EnvironmentHttpApi,
  EnvironmentInternalError,
  T3_PLACEMENT_MAX_REQUEST_BYTES,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ByteSize from "effect/ByteSize";
import * as HttpEffect from "effect/unstable/http/HttpEffect";
import {
  HttpIncomingMessage,
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/unstable/http";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";

import {
  annotateEnvironmentRequest,
  failEnvironmentInternal,
  requireEnvironmentScope,
} from "../auth/http.ts";
import type { T3PlacementTrustProvider } from "../environment/NativePlacementTrust.ts";
import * as NativeStoreAuthority from "../environment/NativeStoreAuthority.ts";
import { makeControlPlaneWorkstreamTransport } from "./ControlPlaneWorkstreamTransport.ts";
import { WorkstreamGateway, make, type WorkstreamGatewayError } from "./WorkstreamGateway.ts";
import { createRegistrationContextHandler } from "./registrationContext/http.ts";
import {
  WorkstreamsRegistrationContext,
  makeWorkstreamsRegistrationContext,
} from "./registrationContext/service.ts";

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

const makeWorkstreamGatewayLayerLive = (placementTrustProvider?: T3PlacementTrustProvider) =>
  Layer.effect(
    WorkstreamGateway,
    Effect.gen(function* () {
      const configured = makeControlPlaneWorkstreamTransport();
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
export const workstreamGatewayLayerLive = makeWorkstreamGatewayLayerLive().pipe(
  Layer.provide(NativeStoreAuthority.layer),
);
export const workstreamRegistrationContextLayerLive = Layer.effect(
  WorkstreamsRegistrationContext,
  Effect.gen(function* () {
    const configured = makeControlPlaneWorkstreamTransport();
    const authority = yield* NativeStoreAuthority.NativeStoreAuthority;
    return makeWorkstreamsRegistrationContext({ ...configured.registrationContext, authority });
  }),
).pipe(Layer.provide(NativeStoreAuthority.layer));

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
    const read = (name: string) =>
      Effect.gen(function* () {
        yield* annotateEnvironmentRequest(name);
        yield* requireEnvironmentScope(AuthOrchestrationReadScope);
      });
    return handlers
      .handle("registrationContext", createRegistrationContextHandler(registrationContext))
      .handle("threadPlacements", (args) =>
        read(args.endpoint.name).pipe(
          Effect.andThen(internal("threadPlacements", gateway.readThreadPlacements(args.payload))),
        ),
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
          return yield* internal("submit", gateway.submit(args.payload.command));
        }),
      );
  }),
);
