/**
 * Same-origin proxy in front of expo-device-hub.
 *
 * The hub binds loopback and is never reachable directly: serve-sim exposes a
 * shell-exec route and serve-emu's action routes are unauthenticated, so the
 * proxy and restricted direct gateway require an environment session with read
 * scope (operate scope for input and tuning). Reusing the T3
 * origin is also what makes remote connections work unchanged — Tailscale and
 * T3 Connect already carry `/api/*` and WebSocket upgrades for the app itself.
 *
 * Only the routes the Device panel needs are forwarded. Anything under the
 * hub's dashboard, exec, or WebRTC surface is rejected here.
 */
import {
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Layer from "effect/Layer";
import {
  HttpClient,
  HttpClientRequest,
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/unstable/http";
import * as Socket from "effect/unstable/socket/Socket";
import * as NodeSocket from "@effect/platform-node/NodeSocket";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import {
  failEnvironmentAuthInvalid,
  failEnvironmentInternal,
  failEnvironmentScopeRequired,
} from "../auth/http.ts";
import * as DeviceService from "./DeviceService.ts";
import { DeviceDirectAccessInput, DeviceDirectGrants } from "./DeviceDirectGrants.ts";
import {
  DEVICE_HUB_POLICY,
  deviceHubRoutePolicy,
  deviceHubForwardHeaders,
  stripDeviceHubQuery,
} from "./DeviceHubPolicy.ts";

const decodeDirectAccess = Schema.decodeUnknownEffect(DeviceDirectAccessInput);

const isWebSocketUpgrade = (request: HttpServerRequest.HttpServerRequest) =>
  request.headers.upgrade?.toLowerCase() === "websocket";

/**
 * `<img>` and WebSocket cannot set headers, so every proxied request
 * authenticates the way the `/ws` upgrade does: a cookie for browser
 * sessions, or a short-lived `wsTicket` minted over authenticated HTTP for
 * bearer and DPoP clients. The upgrade authenticator already implements that
 * fallback order, so it is used for plain requests as well.
 */
const authenticate = (requiredScope: AuthEnvironmentScope) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
    const session = yield* serverAuth.authenticateWebSocketUpgrade(request).pipe(
      Effect.catch((error) =>
        Effect.gen(function* () {
          if (EnvironmentAuth.isServerAuthCredentialError(error)) {
            return yield* failEnvironmentAuthInvalid(
              EnvironmentAuth.serverAuthCredentialReason(error),
              EnvironmentAuth.serverAuthDpopFailureReason(error),
            );
          }
          return yield* failEnvironmentInternal("internal_error", error);
        }),
      ),
    );
    if (!session.scopes.includes(requiredScope)) {
      return yield* failEnvironmentScopeRequired(requiredScope);
    }
    return session;
  });

/**
 * Pipe a client WebSocket to the hub's with no framing changes. Frames are
 * opaque: H.264 access units one way, input packets the other.
 */
const proxyWebSocket = Effect.fn("DeviceHubProxy.proxyWebSocket")(function* (
  request: HttpServerRequest.HttpServerRequest,
  upstreamUrl: string,
) {
  const client = yield* request.upgrade;
  const upstream = yield* Socket.makeWebSocket(upstreamUrl, {
    openTimeout: "10 seconds",
  }).pipe(Effect.provide(NodeSocket.layerWebSocketConstructor));
  yield* Effect.scoped(
    Effect.gen(function* () {
      const writeToClient = yield* client.writer;
      const writeToUpstream = yield* upstream.writer;
      // Whichever side closes first ends the other via scope teardown: a close
      // fails the pull with a SocketError, which loses the race.
      return yield* Effect.raceFirst(
        pumpFrames(upstream, writeToClient),
        pumpFrames(client, writeToUpstream),
      );
    }),
  ).pipe(Effect.ignoreCause);
  return HttpServerResponse.empty();
});

const pumpFrames = (source: Socket.Socket, sink: Socket.Writer) =>
  Effect.gen(function* () {
    const { pull } = yield* source.reader;
    while (true) {
      yield* sink.writeAll(yield* pull);
    }
  });

const proxyHttp = Effect.fn("DeviceHubProxy.proxyHttp")(function* (
  request: HttpServerRequest.HttpServerRequest,
  upstreamUrl: string,
  hubOrigin: string,
) {
  const httpClient = HttpClient.withScope(yield* HttpClient.HttpClient);
  const method = request.method;
  const upstreamRequest = HttpClientRequest.make(method)(upstreamUrl).pipe(
    HttpClientRequest.setHeaders(
      deviceHubForwardHeaders(DEVICE_HUB_POLICY, request.headers, hubOrigin),
    ),
    method === "GET" || method === "HEAD"
      ? (self) => self
      : HttpClientRequest.bodyStream(request.stream),
  );
  const response = yield* httpClient.execute(upstreamRequest);
  const headers: Record<string, string> = {};
  for (const [name, value] of Object.entries(response.headers)) {
    if (
      ["content-encoding", "transfer-encoding", "connection", "set-cookie", "location"].includes(
        name,
      )
    ) {
      continue;
    }
    if (value !== undefined) headers[name] = value;
  }
  // Long-lived MJPEG and AVCC responses must not be buffered by compression.
  headers["cache-control"] = "no-store, no-transform";
  return HttpServerResponse.stream(response.stream, {
    status: response.status,
    headers,
    ...(headers["content-type"] ? { contentType: headers["content-type"] } : {}),
  });
});

const handler = Effect.gen(function* () {
  const grants = yield* DeviceDirectGrants;
  const request = yield* HttpServerRequest.HttpServerRequest;
  const url = HttpServerRequest.toURL(request);
  if (Option.isNone(url)) {
    return HttpServerResponse.text("Bad Request", { status: 400 });
  }
  const hubPath = url.value.pathname.slice(DeviceService.DEVICE_HUB_ROUTE_PREFIX.length) || "/";
  if (hubPath === "/direct-access") {
    if (request.method !== "GET" || isWebSocketUpgrade(request))
      return HttpServerResponse.empty({ status: 405 });
    const session = yield* authenticate(AuthOrchestrationReadScope);
    const input = yield* decodeDirectAccess(Object.fromEntries(url.value.searchParams)).pipe(
      Effect.result,
    );
    if (
      input._tag === "Failure" ||
      ["hostId", "deviceId", "platform", "clientOrigin"].some(
        (key) => url.value.searchParams.getAll(key).length !== 1,
      )
    )
      return HttpServerResponse.empty({ status: 400 });
    if (request.headers.origin && request.headers.origin !== input.success.clientOrigin)
      return HttpServerResponse.empty({ status: 403 });
    const devices = yield* DeviceService.DeviceService;
    const ready = yield* devices.currentReadiness(input.success.hostId);
    if (!ready?.directMedia) return HttpServerResponse.empty({ status: 204 });
    const state = yield* devices.state;
    if (
      !state.devices.some(
        (device) =>
          device.hostId === input.success.hostId &&
          device.id === input.success.deviceId &&
          device.platform === input.success.platform,
      )
    )
      return HttpServerResponse.empty({ status: 404 });
    const access = yield* grants
      .issue(input.success, ready.directMedia, session)
      .pipe(Effect.catch((error) => failEnvironmentInternal("internal_error", error)));
    return access
      ? HttpServerResponse.jsonUnsafe(access, { headers: { "cache-control": "no-store" } })
      : HttpServerResponse.empty({ status: 503 });
  }
  const upgrade = isWebSocketUpgrade(request);
  const policy = deviceHubRoutePolicy(DEVICE_HUB_POLICY, hubPath, request.method, upgrade);
  if (typeof policy === "number") return HttpServerResponse.empty({ status: policy });
  yield* authenticate(
    policy === "operate" ? AuthOrchestrationOperateScope : AuthOrchestrationReadScope,
  );
  const devices = yield* DeviceService.DeviceService;
  const ready = yield* devices.currentReadiness(url.value.searchParams.get("hostId") ?? undefined);
  if (!ready) {
    return HttpServerResponse.text("Device hub is not running", { status: 503 });
  }
  // The hub runs in standalone mode at its origin root; the panel builds every
  // stream and socket URL itself, so nothing depends on the hub knowing the
  // T3 prefix.
  // The ticket authenticates here and must not travel on to the hub.
  const upstreamPath = `${hubPath}${stripDeviceHubQuery(url.value.search)}`;
  if (upgrade) {
    return yield* proxyWebSocket(
      request,
      `${ready.hub.origin.replace(/^http/, "ws")}${upstreamPath}`,
    );
  }
  return yield* proxyHttp(request, `${ready.hub.origin}${upstreamPath}`, ready.hub.origin);
});

export const deviceHubProxyRouteLayer = Layer.unwrap(
  Effect.sync(() => HttpRouter.add("*", `${DeviceService.DEVICE_HUB_ROUTE_PREFIX}/*`, handler)),
);
