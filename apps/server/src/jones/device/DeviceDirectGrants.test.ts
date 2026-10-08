import { expect, it } from "@effect/vitest";
import {
  AuthSessionId,
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import * as Schema from "effect/Schema";
import {
  SessionStore,
  MalformedWebSocketTokenError,
  SessionCredentialVerificationError,
} from "../../auth/SessionStore.ts";
import {
  makeDeviceDirectGrants,
  DeviceDirectAccessInput,
  DeviceDirectAdmissionInput,
} from "./DeviceDirectGrants.ts";
import type { DeviceDirectMediaEndpoint } from "../../device/DeviceHost.ts";

const decodeAccess = Schema.decodeUnknownSync(DeviceDirectAccessInput);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeAdmission = Schema.decodeUnknownSync(DeviceDirectAdmissionInput);

const setup = Effect.gen(function* () {
  let endpoint: DeviceDirectMediaEndpoint = {
    target: "mini",
    owner: "owner",
    generation: "generation",
    gatewayPort: 1234,
  };
  let active = true;
  let revoked = false;
  let internal = false;
  let wait = false;
  const entered = yield* Deferred.make<void>();
  const release = yield* Deferred.make<void>();
  const scopes = [AuthOrchestrationReadScope, AuthOrchestrationOperateScope];
  const sessions = {
    issueWebSocketToken: () =>
      DateTime.now.pipe(
        Effect.map((now) => ({
          token: "private-vps-token",
          expiresAt: DateTime.makeUnsafe(now.epochMilliseconds + 300000),
        })),
      ),
    verifyWebSocketToken: () =>
      Effect.gen(function* () {
        if (wait) {
          yield* Deferred.succeed(entered, undefined);
          yield* Deferred.await(release);
        }
        if (internal)
          return yield* new SessionCredentialVerificationError({
            sessionId: AuthSessionId.make("test"),
            cause: new Error("offline"),
          });
        if (revoked) return yield* new MalformedWebSocketTokenError({});
        return { scopes };
      }),
  } as unknown as SessionStore["Service"];
  const grants = yield* makeDeviceDirectGrants.pipe(Effect.provideService(SessionStore, sessions));
  const input = decodeAccess({
    hostId: "mini",
    deviceId: "phone",
    platform: "ios",
    clientOrigin: "t3code://app",
  });
  const session = {
    sessionId: AuthSessionId.make("test"),
    subject: "test",
    method: "bearer-access-token" as const,
    scopes,
  };
  const binding = {
    hostId: "mini",
    owner: endpoint.owner,
    generation: endpoint.generation,
    gatewayPort: endpoint.gatewayPort,
    currentEndpoint: Effect.sync(() => endpoint),
    isActive: () => active,
  };
  const issue = grants.issue(input, endpoint, session);
  const request = (grant: string, overrides: Record<string, unknown> = {}) =>
    decodeAdmission({
      grant,
      hostId: "mini",
      generation: "generation",
      origin: "t3code://app",
      path: "/vendor/serve-sim/helper/phone/stream.avcc",
      search: "",
      method: "GET",
      upgrade: false,
      ...overrides,
    });
  return {
    grants,
    issue,
    request,
    binding,
    scopes,
    entered,
    release,
    revoke: () => {
      revoked = true;
    },
    internal: () => {
      internal = true;
    },
    defer: () => {
      wait = true;
    },
    retire: () => {
      active = false;
    },
    rotate: () => {
      endpoint = { ...endpoint, generation: "replacement" };
    },
  };
});

it.effect(
  "binds grants to the current host, generation, origin, device and both operate ceilings",
  () =>
    Effect.gen(function* () {
      const f = yield* setup;
      const access = yield* f.issue;
      expect(access).not.toBeNull();
      if (!access) return;
      expect(access.grant).toHaveLength(43);
      expect(access.expiresAt).toBeLessThanOrEqual(
        (yield* DateTime.now).epochMilliseconds + 300000,
      );
      expect(encodeJson(access)).not.toContain("private-vps-token");
      expect((yield* f.grants.admit(f.request(access.grant), f.binding))._tag).toBe("Allowed");
      for (const override of [
        { grant: "wrong" },
        { hostId: "other" },
        { generation: "old" },
        { origin: "http://localhost:9999" },
        { path: "/vendor/serve-sim/helper/another/stream.avcc" },
        { path: "/api/devices" },
        { path: "/vendor/serve-sim/exec", method: "POST" },
        { search: "?device=another" },
        { search: "?device=phone&device=another" },
      ])
        expect((yield* f.grants.admit(f.request(access.grant, override), f.binding))._tag).toBe(
          "Denied",
        );
      const input = { path: "/vendor/serve-sim/helper/ws", search: "?device=phone", upgrade: true };
      expect((yield* f.grants.admit(f.request(access.grant, input), f.binding))._tag).toBe(
        "Allowed",
      );
      f.scopes.splice(f.scopes.indexOf(AuthOrchestrationOperateScope), 1);
      expect((yield* f.grants.admit(f.request(access.grant, input), f.binding))._tag).toBe(
        "Denied",
      );
      const reader = yield* f.issue;
      f.scopes.push(AuthOrchestrationOperateScope);
      expect((yield* f.grants.admit(f.request(reader!.grant, input), f.binding))._tag).toBe(
        "Denied",
      );
      f.rotate();
      expect((yield* f.grants.admit(f.request(access.grant), f.binding))._tag).toBe("Denied");
    }),
);

it.effect("distinguishes typed credential denial from verifier outage and bounds issuance", () =>
  Effect.gen(function* () {
    const f = yield* setup;
    const access = yield* f.issue;
    f.revoke();
    expect((yield* f.grants.admit(f.request(access!.grant), f.binding))._tag).toBe("Denied");
    f.internal();
    const result = yield* f.grants.admit(f.request(access!.grant), f.binding).pipe(Effect.result);
    expect(result._tag).toBe("Failure");
    if (result._tag === "Failure")
      expect(result.failure._tag).toBe("SessionCredentialVerificationError");
    for (let i = 1; i < 1024; i++) expect(yield* f.issue).not.toBeNull();
    expect(yield* f.issue).toBeNull();
  }),
);

it.effect("rechecks retirement, replacement and expiry after deferred verification", () =>
  Effect.gen(function* () {
    for (const action of ["retire", "rotate", "expire"] as const) {
      const f = yield* setup;
      const access = yield* f.issue;
      f.defer();
      const validation = yield* f.grants
        .admit(f.request(access!.grant), f.binding)
        .pipe(Effect.forkChild);
      yield* Deferred.await(f.entered);
      if (action === "expire") yield* TestClock.adjust("5 minutes");
      else f[action]();
      yield* Deferred.succeed(f.release, undefined);
      expect((yield* Fiber.join(validation))._tag).toBe("Denied");
    }
  }),
);
