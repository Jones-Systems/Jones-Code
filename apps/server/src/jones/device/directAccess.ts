import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { type HttpServerRequest, HttpServerResponse } from "effect/http";
import { failEnvironmentInternal } from "../../auth/http.ts";
import type { EnvironmentAuth } from "../../auth/EnvironmentAuth.ts";
import * as DeviceService from "../../device/DeviceService.ts";
import * as DeviceDirectGrants from "./DeviceDirectGrants.ts";

const decodeDirectAccess = Schema.decodeUnknownEffect(DeviceDirectGrants.DeviceDirectAccessInput);

export const directAccess = Effect.fn("DeviceDirectAccess.handle")(function* <E, R>(
  request: HttpServerRequest.HttpServerRequest,
  url: URL,
  authentication: Effect.Effect<
    Effect.Success<ReturnType<EnvironmentAuth["Service"]["authenticateWebSocketUpgrade"]>>,
    E,
    R
  >,
) {
  if (request.method !== "GET" || request.headers.upgrade?.toLowerCase() === "websocket")
    return HttpServerResponse.empty({ status: 405 });
  const session = yield* authentication;
  const input = yield* decodeDirectAccess(Object.fromEntries(url.searchParams)).pipe(Effect.result);
  if (
    input._tag === "Failure" ||
    ["hostId", "deviceId", "platform", "clientOrigin"].some(
      (key) => url.searchParams.getAll(key).length !== 1,
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
  const grants = yield* DeviceDirectGrants.DeviceDirectGrants;
  const access = yield* grants
    .issue(input.success, ready.directMedia, session)
    .pipe(Effect.catch((error) => failEnvironmentInternal("internal_error", error)));
  return access
    ? HttpServerResponse.jsonUnsafe(access, { headers: { "cache-control": "no-store" } })
    : HttpServerResponse.empty({ status: 503 });
});
