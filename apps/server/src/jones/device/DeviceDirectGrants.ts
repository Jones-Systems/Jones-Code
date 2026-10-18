// @effect-diagnostics nodeBuiltinImport:off -- The server-local grant boundary retains its synchronous CSPRNG token encoding without adding a new service or error contract.
import * as NodeCrypto from "node:crypto";
import {
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  DeviceHostId,
  DeviceId,
  DevicePlatform,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  SessionStore,
  isSessionCredentialInvalidError,
  type SessionCredentialInternalError,
} from "../../auth/SessionStore.ts";
import type { EnvironmentAuth } from "../../auth/EnvironmentAuth.ts";
import type { DeviceDirectMediaEndpoint } from "../../device/DeviceHost.ts";
import {
  DEVICE_HUB_POLICY,
  deviceHubRoutePolicy,
  directDeviceRouteMatches,
  validDirectClientOrigin,
} from "./directMediaPolicy.ts";

export const DeviceDirectAccessInput = Schema.Struct({
  hostId: DeviceHostId,
  deviceId: DeviceId,
  platform: DevicePlatform,
  clientOrigin: Schema.String.check(Schema.makeFilter((origin) => validDirectClientOrigin(origin))),
});

export const DeviceDirectAdmissionInput = Schema.Struct({
  grant: Schema.String.check(Schema.isMaxLength(256)),
  hostId: DeviceHostId,
  generation: Schema.String.check(Schema.isMaxLength(128)),
  origin: Schema.String.check(Schema.isMaxLength(512)),
  path: Schema.String.check(Schema.isMaxLength(2048)),
  search: Schema.String.check(Schema.isMaxLength(4096)),
  method: Schema.String.check(Schema.isMaxLength(16)),
  upgrade: Schema.Boolean,
});

interface DirectGrant {
  readonly endpoint: DeviceDirectMediaEndpoint;
  readonly input: typeof DeviceDirectAccessInput.Type;
  readonly sessionToken: string;
  readonly expiresAt: number;
  readonly operate: boolean;
}

export interface DirectAdmissionBinding {
  readonly hostId: string;
  readonly owner: string;
  readonly generation: string;
  readonly gatewayPort: number;
  readonly currentEndpoint: Effect.Effect<DeviceDirectMediaEndpoint | null>;
  readonly isActive: () => boolean;
}

export type DirectAdmission =
  | { readonly _tag: "Denied" }
  | {
      readonly _tag: "Allowed";
      readonly allowed: true;
      readonly expiresAt: number;
      readonly origin: string;
      readonly owner: string;
      readonly generation: string;
    };

const denied: DirectAdmission = { _tag: "Denied" };
const matches = (endpoint: DeviceDirectMediaEndpoint | null, binding: DirectAdmissionBinding) =>
  endpoint !== null &&
  endpoint.owner === binding.owner &&
  endpoint.generation === binding.generation &&
  endpoint.gatewayPort === binding.gatewayPort;

/** Captures the existing session store; private credentials and the bounded map remain on the environment server. */
export const makeDeviceDirectGrants = Effect.gen(function* () {
  const sessions = yield* SessionStore;
  const grants = new Map<string, DirectGrant>();
  const prune = (now: number) => {
    for (const [key, grant] of grants) if (grant.expiresAt <= now) grants.delete(key);
  };
  const issue = Effect.fn("DeviceDirectGrants.issue")(function* (
    input: typeof DeviceDirectAccessInput.Type,
    endpoint: DeviceDirectMediaEndpoint,
    session: Effect.Success<ReturnType<EnvironmentAuth["Service"]["authenticateWebSocketUpgrade"]>>,
  ) {
    const now = (yield* DateTime.now).epochMilliseconds;
    prune(now);
    if (grants.size >= 1024) return null;
    const credential = yield* sessions.issueWebSocketToken(session.sessionId, {
      ttl: Duration.minutes(5),
    });
    const expiresAt = Math.min(
      credential.expiresAt.epochMilliseconds,
      now + 300_000,
      session.expiresAt?.epochMilliseconds ?? Infinity,
    );
    if (grants.size >= 1024) return null;
    const grant = NodeCrypto.randomBytes(32).toString("base64url");
    grants.set(grant, {
      endpoint,
      input,
      sessionToken: credential.token,
      expiresAt,
      operate: session.scopes.includes(AuthOrchestrationOperateScope),
    });
    return {
      target: endpoint.target,
      gatewayPort: endpoint.gatewayPort,
      owner: endpoint.owner,
      generation: endpoint.generation,
      grant,
      expiresAt,
    };
  });
  const admit = Effect.fn("DeviceDirectGrants.admit")(function* (
    request: typeof DeviceDirectAdmissionInput.Type,
    binding: DirectAdmissionBinding,
  ): Effect.fn.Return<DirectAdmission, SessionCredentialInternalError> {
    const now = (yield* DateTime.now).epochMilliseconds;
    prune(now);
    const grant = grants.get(request.grant);
    if (
      !binding.isActive() ||
      request.hostId !== binding.hostId ||
      request.generation !== binding.generation ||
      !grant ||
      !matches(grant.endpoint, binding) ||
      request.hostId !== grant.input.hostId ||
      request.origin !== grant.input.clientOrigin ||
      !directDeviceRouteMatches(
        request.path,
        request.search,
        grant.input.deviceId,
        grant.input.platform,
      )
    )
      return denied;
    const policy =
      request.path === "/readyz" && !request.upgrade && request.method === "GET"
        ? "read"
        : deviceHubRoutePolicy(DEVICE_HUB_POLICY, request.path, request.method, request.upgrade);
    if (typeof policy === "number" || (policy === "operate" && !grant.operate)) return denied;
    if (!matches(yield* binding.currentEndpoint, binding) || !binding.isActive()) return denied;
    const current = yield* sessions
      .verifyWebSocketToken(grant.sessionToken)
      .pipe(
        Effect.catch((error) =>
          isSessionCredentialInvalidError(error)
            ? Effect.succeed(null)
            : Effect.fail(error as SessionCredentialInternalError),
        ),
      );
    if (
      !current ||
      !current.scopes.includes(AuthOrchestrationReadScope) ||
      (policy === "operate" && !current.scopes.includes(AuthOrchestrationOperateScope))
    )
      return denied;
    const endpoint = yield* binding.currentEndpoint;
    const checkedAt = (yield* DateTime.now).epochMilliseconds;
    if (
      !binding.isActive() ||
      grants.get(request.grant) !== grant ||
      grant.expiresAt <= checkedAt ||
      (current.expiresAt !== undefined && current.expiresAt.epochMilliseconds <= checkedAt) ||
      !matches(endpoint, binding) ||
      !matches(grant.endpoint, binding)
    )
      return denied;
    return {
      _tag: "Allowed",
      allowed: true,
      expiresAt: Math.min(grant.expiresAt, current.expiresAt?.epochMilliseconds ?? Infinity),
      origin: grant.input.clientOrigin,
      owner: binding.owner,
      generation: binding.generation,
    };
  });
  return { issue, admit };
});

export class DeviceDirectGrants extends Context.Service<
  DeviceDirectGrants,
  Effect.Success<typeof makeDeviceDirectGrants>
>()("t3/jones/device/DeviceDirectGrants") {}

export const layer = Layer.effect(DeviceDirectGrants, makeDeviceDirectGrants);
