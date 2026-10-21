import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import {
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  AuthSessionId,
  LOCAL_DEVICE_HOST_ID,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import { SessionStore } from "../auth/SessionStore.ts";
import { DEVICE_HUB_POLICY, deviceHubRoutePolicy, directDeviceRouteMatches, validDirectClientOrigin } from "../jones/device/directMediaPolicy.ts";
import * as DeviceDirectGrants from "../jones/device/DeviceDirectGrants.ts";

import { HttpClient, HttpClientResponse, HttpRouter } from "effect/http";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import type * as DeviceHost from "./DeviceHost.ts";
import * as DeviceService from "./DeviceService.ts";
import * as DeviceHubProxy from "./DeviceHubProxy.ts";

const disposers: Array<() => Promise<void>> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const dispose of disposers.splice(0)) await dispose();
});

const fixture = (
  scopes: ReadonlyArray<AuthEnvironmentScope>,
  fail = false,
  authError?: EnvironmentAuth.ServerAuthCredentialError | EnvironmentAuth.ServerAuthInternalError,
  upstreamOrDirect?: DeviceHost.DeviceHostReady["upstream"] | boolean,
) => {
  const direct = upstreamOrDirect === true;
  const upstream = typeof upstreamOrDirect === "object" ? upstreamOrDirect : undefined;
  let finalized = 0;
  let revoked = false;
  let generation = "generation";
  const tokenScopes = [...scopes];
  const forwardedHeaders: Array<Readonly<Record<string, string>>> = [];
  const endpoint = () => ({ target: "mac-mini", gatewayPort: 1234, owner: "owner", generation });
  const requests: string[] = [];
  const headers: Array<Readonly<Record<string, string>>> = [];
  const client = HttpClient.make((request, _url, signal) =>
    Effect.gen(function* () {
      requests.push(request.url);
      headers.push(request.headers);
      forwardedHeaders.push(request.headers);
      signal.addEventListener("abort", () => {
        finalized++;
      });
      if (fail) return yield* Effect.die(new Error("upstream failed"));
      return HttpClientResponse.fromWeb(
        request,
        new Response("frame", { headers: { "set-cookie": "upstream=1" } }),
      );
    }),
  );
  const { handler, dispose } = HttpRouter.toWebHandler(
    DeviceHubProxy.layer.pipe(
      Layer.provideMerge(DeviceDirectGrants.layer),
      Layer.provideMerge(
        Layer.succeed(EnvironmentAuth.EnvironmentAuth, {
          authenticateWebSocketUpgrade: () =>
            authError
              ? Effect.fail(authError)
              : Effect.succeed({
                  sessionId: AuthSessionId.make("test"),
                  subject: "test",
                  method: "bearer-access-token",
                  scopes,
                }),
        } as unknown as EnvironmentAuth.EnvironmentAuth["Service"]),
      ),
      Layer.provideMerge(
        Layer.succeed(DeviceService.DeviceService, {
          currentReadiness: () =>
            Effect.succeed({
              hostId: LOCAL_DEVICE_HOST_ID,
              hub: { origin: "http://hub.test" },
              ...(direct ? { directMedia: endpoint() } : {}),
              ...(upstream ? { upstream } : {}),
            }),
          state: Effect.succeed({ devices: [{ hostId: "local", id: "phone", platform: "ios" }] }),
        } as unknown as DeviceService.DeviceService["Service"]),
      ),
      Layer.provideMerge(
        Layer.succeed(SessionStore, {
          issueWebSocketToken: () =>
            DateTime.now.pipe(
              Effect.map((now) => ({
                token: "private-vps-token",
                expiresAt: DateTime.makeUnsafe(now.epochMilliseconds + 300000),
              })),
            ),
          verifyWebSocketToken: (token: string) =>
            token !== "private-vps-token" || revoked
              ? Effect.fail(new EnvironmentAuth.ServerAuthMissingCredentialError({}))
              : Effect.succeed({ scopes: tokenScopes }),
        } as unknown as SessionStore["Service"]),
      ),
      Layer.provideMerge(Layer.succeed(HttpClient.HttpClient, client)),
    ),
    { disableLogger: true },
  );
  disposers.push(dispose);
  return {
    handler,
    headers,
    requests,
    forwardedHeaders,
    finalized: () => finalized,
    revoke: () => {
      revoked = true;
    },
    rotate: () => {
      generation = "next-generation";
    },
    tokenScopes,
  };
};

const accessSchema = Schema.Struct({
  grant: Schema.String,
  expiresAt: Schema.Number,
  target: Schema.String,
  gatewayPort: Schema.Number,
  owner: Schema.String,
  generation: Schema.String,
});
const getDirect = async (f: ReturnType<typeof fixture>, extra = "") => {
  const response = await f.handler(
    new Request(
      "http://t3.test/api/device-hub/direct-access?hostId=local&deviceId=phone&platform=ios&clientOrigin=t3code%3A%2F%2Fapp" +
        extra,
    ),
  );
  expect(response.status).toBe(200);
  const text = await response.text();
  expect(text).not.toContain("private-vps-token");
  return Schema.decodeUnknownSync(Schema.fromJsonString(accessSchema))(text);
};

describe("device hub proxy", () => {
  it("releases the upstream response after forwarding its body and strips tickets", async () => {
    const { handler, requests, finalized } = fixture([AuthOrchestrationReadScope]);
    const response = await handler(
      new Request("http://t3.test/api/device-hub/api/devices?wsTicket=secret"),
    );
    expect(response.status).toBe(200);
    expect(await response.text()).toBe("frame");
    expect(requests).toEqual(["http://hub.test/api/devices"]);
    expect(finalized()).toBe(1);
  });

  it.each(["/vendor/serve-sim/helper/ws", "/vendor/serve-emu/ws"])(
    "rejects input socket %s for a read-only session",
    async (path) => {
      const { handler, requests } = fixture([AuthOrchestrationReadScope]);
      const response = await handler(
        new Request(`http://t3.test/api/device-hub${path}`, { headers: { upgrade: "websocket" } }),
      );
      expect(response.status).toBe(403);
      expect(requests).toEqual([]);
    },
  );

  it("requires operate scope for stream tuning", async () => {
    const readOnly = fixture([AuthOrchestrationReadScope]);
    const path = "http://t3.test/api/device-hub/vendor/serve-emu/api/stream-settings";
    expect((await readOnly.handler(new Request(path, { method: "POST" }))).status).toBe(403);
    const operator = fixture([AuthOrchestrationOperateScope]);
    const response = await operator.handler(new Request(path, { method: "POST" }));
    expect(response.status).toBe(200);
    await response.text();
  });

  it("reads Android fold state but requires operate scope to change it", async () => {
    const path = "http://t3.test/api/device-hub/vendor/serve-emu/api/fold?device=emulator-5554";
    const reader = fixture([AuthOrchestrationReadScope]);
    const read = await reader.handler(new Request(path));
    expect(read.status).toBe(200);
    await read.text();
    expect(reader.requests).toEqual([
      "http://hub.test/vendor/serve-emu/api/fold?device=emulator-5554",
    ]);
    const denied = await reader.handler(
      new Request(path, { method: "POST", body: '{"posture":"closed"}' }),
    );
    expect(denied.status).toBe(403);
    expect(reader.requests).toHaveLength(1);

    const operator = fixture([AuthOrchestrationOperateScope]);
    const changed = await operator.handler(
      new Request(path, { method: "POST", body: '{"posture":"closed"}' }),
    );
    expect(changed.status).toBe(200);
    await changed.text();
    expect(operator.requests).toEqual([
      "http://hub.test/vendor/serve-emu/api/fold?device=emulator-5554",
    ]);
  });

  it("presents fold changes from an HTTPS client as same-origin hub requests", async () => {
    const { handler, headers } = fixture([AuthOrchestrationOperateScope]);
    const response = await handler(
      new Request(
        "https://relay.test/api/device-hub/vendor/serve-emu/api/fold?device=emulator-5554",
        {
          method: "POST",
          headers: { origin: "https://relay.test", "x-forwarded-proto": "https" },
          body: '{"posture":"opened"}',
        },
      ),
    );
    expect(response.status).toBe(200);
    await response.text();
    expect(headers).toHaveLength(1);
    expect(headers[0]?.origin).toBe("http://hub.test");
    expect(headers[0]).not.toHaveProperty("x-forwarded-proto");
  });

  it("lets a viewer negotiate a WebRTC stream with read scope", async () => {
    const { handler, requests, headers } = fixture([AuthOrchestrationReadScope]);
    const offer = await handler(
      new Request("http://t3.test/api/device-hub/vendor/serve-sim/helper/udid-1/webrtc/offer", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: '{"type":"offer"}',
      }),
    );
    expect(offer.status).toBe(200);
    await offer.text();
    expect(requests).toEqual(["http://hub.test/vendor/serve-sim/helper/udid-1/webrtc/offer"]);
    expect(headers[0]?.["content-type"]).toBe("application/json");
  });

  it("sends requests a host routes itself to its upstream with its credentials", async () => {
    const { handler, requests, headers } = fixture(
      [AuthOrchestrationReadScope],
      false,
      undefined,
      (hubPath, search) =>
        Effect.succeed(
          hubPath === "/vendor/serve-sim/api"
            ? {
                url: `https://session.test${hubPath.slice("/vendor/serve-sim".length)}?${search}`,
                headers: { authorization: "Bearer session-token" },
                protocols: [],
              }
            : null,
        ),
    );
    const routed = await handler(
      new Request("http://t3.test/api/device-hub/vendor/serve-sim/api?device=udid-1&hostId=eas", {
        headers: { authorization: "Bearer environment-token" },
      }),
    );
    expect(routed.status).toBe(200);
    expect(routed.headers.get("set-cookie")).toBeNull();
    await routed.text();
    expect(requests).toEqual(["https://session.test/api?device=udid-1"]);
    expect(headers[0]?.authorization).toBe("Bearer session-token");
    const unrouted = await handler(
      new Request("http://t3.test/api/device-hub/vendor/serve-sim/helper/udid-2/stream.mjpeg"),
    );
    expect(unrouted.status).toBe(404);
    expect(requests).toHaveLength(1);
  });

  it("never forwards the vendor shell endpoint", async () => {
    const { handler, requests } = fixture([AuthOrchestrationOperateScope]);
    expect(
      (
        await handler(
          new Request("http://t3.test/api/device-hub/vendor/serve-sim/exec", { method: "POST" }),
        )
      ).status,
    ).toBe(404);
    expect(requests).toEqual([]);
  });
});

it.each([
  [new EnvironmentAuth.ServerAuthMissingCredentialError({}), 401],
  [
    new EnvironmentAuth.ServerAuthSessionCredentialValidationError({
      cause: new Error("private credential diagnostic"),
    }),
    500,
  ],
] as const)("translates authentication failure to HTTP %s", async (error, status) => {
  const { handler, requests } = fixture([], false, error);
  const response = await handler(new Request("http://t3.test/api/device-hub/api/devices"));
  expect(response.status).toBe(status);
  expect(await response.text()).not.toContain("private credential diagnostic");
  expect(requests).toEqual([]);
});

it("rejects unsupported Duo routes", async () => {
  const { handler, requests } = fixture([AuthOrchestrationReadScope]);
  const response = await handler(
    new Request("http://t3.test/api/device-hub/vendor/serve-sim/helper/duo/panel/1/webrtc/offer"),
  );
  expect(response.status).toBe(404);
  expect(requests).toEqual([]);
});

it("keeps the private callback absent from the main router", async () => {
  const f = fixture([AuthOrchestrationReadScope], false, undefined, true);
  const access = await getDirect(f);
  expect(access.target).toBe("mac-mini");
  for (const method of ["GET", "POST", "PUT"]) {
    const response = await f.handler(
      new Request("http://t3.test/api/device-hub/direct-admission", {
        method,
        ...(method === "GET" ? {} : { body: JSON.stringify({ grant: access.grant }) }),
      }),
    );
    expect(response.status).toBe(404);
  }
  expect(f.requests).toEqual([]);
});

it("preserves fallback and validates grant issuance origin, device, authentication and method", async () => {
  const unsupported = fixture([AuthOrchestrationReadScope]);
  const path =
    "http://t3.test/api/device-hub/direct-access?hostId=local&deviceId=phone&platform=ios&clientOrigin=t3code%3A%2F%2Fapp";
  expect((await unsupported.handler(new Request(path))).status).toBe(204);
  const f = fixture([AuthOrchestrationReadScope], false, undefined, true);
  expect(
    (await f.handler(new Request(path.replace("deviceId=phone", "deviceId=unknown")))).status,
  ).toBe(404);
  expect((await f.handler(new Request(path, { method: "POST" }))).status).toBe(405);
  expect(
    (await f.handler(new Request(path, { headers: { origin: "http://localhost:9999" } }))).status,
  ).toBe(403);
  expect((await f.handler(new Request(path.replace("t3code%3A%2F%2Fapp", "null")))).status).toBe(
    400,
  );
  expect((await fixture([]).handler(new Request(path))).status).toBe(403);
  expect((await f.handler(new Request(path + "&deviceId=another"))).status).toBe(400);
});

it("strips client credentials from the shared forwarding surface", async () => {
  const f = fixture([AuthOrchestrationReadScope]);
  const response = await f.handler(
    new Request(
      "http://t3.test/api/device-hub/vendor/serve-sim/helper/phone/stream.avcc?wsTicket=secret&grant=direct-secret&clientOrigin=t3code%3A%2F%2Fapp&hostId=local",
      {
        headers: {
          authorization: "Bearer private",
          cookie: "session=secret",
          dpop: "proof",
          "proxy-authorization": "private",
          origin: "t3code://app",
        },
      },
    ),
  );
  expect(response.status).toBe(200);
  await response.text();
  expect(f.requests).toEqual(["http://hub.test/vendor/serve-sim/helper/phone/stream.avcc"]);
  expect(f.forwardedHeaders[0]).not.toHaveProperty("authorization");
  expect(f.forwardedHeaders[0]).not.toHaveProperty("cookie");
  expect(f.forwardedHeaders[0]).not.toHaveProperty("dpop");
  expect(f.forwardedHeaders[0]).not.toHaveProperty("proxy-authorization");
  expect(f.forwardedHeaders[0]?.origin).toBe("http://hub.test");
});

it("bounds direct origins and applies the shared method policy to both platforms", () => {
  for (const origin of [
    "t3code://app",
    "t3code-dev://app",
    "http://localhost:3000",
    "https://127.0.0.1:8443",
    "http://[::1]:3000",
  ])
    expect(validDirectClientOrigin(origin)).toBe(true);
  for (const origin of [
    "null",
    "*",
    "https://other.example",
    "http://localhost:3000/",
    "http://user@localhost:3000",
    "t3code://other",
  ])
    expect(validDirectClientOrigin(origin)).toBe(false);
  expect(deviceHubRoutePolicy(DEVICE_HUB_POLICY, "/vendor/serve-emu/ws", "GET", true)).toBe(
    "operate",
  );
  expect(
    deviceHubRoutePolicy(
      DEVICE_HUB_POLICY,
      "/vendor/serve-sim/helper/phone/stream.avcc",
      "POST",
      false,
    ),
  ).toBe(405);
  expect(
    deviceHubRoutePolicy(
      DEVICE_HUB_POLICY,
      "/vendor/serve-sim/helper/phone/webrtc/offer",
      "POST",
      false,
    ),
  ).toBe(404);
  expect(
    directDeviceRouteMatches(
      "/vendor/serve-emu/ws",
      "?device=emulator-5554",
      "emulator-5554",
      "android",
    ),
  ).toBe(true);
  expect(
    directDeviceRouteMatches("/vendor/serve-emu/ws", "?device=emulator-5554", "phone", "ios"),
  ).toBe(false);
});

it.each([1, 3])(
  "forwards fixed Duo display %s through the authenticated read proxy",
  async (panel) => {
    const { handler, requests } = fixture([AuthOrchestrationReadScope]);
    const route = `/vendor/serve-sim/helper/duo/panel/${panel}/stream.avcc`;
    const response = await handler(
      new Request(`http://t3.test/api/device-hub${route}?wsTicket=secret`),
    );
    expect(response.status).toBe(200);
    await response.text();
    expect(requests).toEqual([`http://hub.test${route}`]);
  },
);

it.each(["/panel/2/stream.avcc", "/panel/1/webrtc/offer", "/panel/3/exec"])(
  "rejects unsupported Duo route %s",
  async (route) => {
    const { handler, requests } = fixture([AuthOrchestrationReadScope]);
    const response = await handler(
      new Request(`http://t3.test/api/device-hub/vendor/serve-sim/helper/duo${route}`),
    );
    expect(response.status).toBe(404);
    expect(requests).toEqual([]);
  },
);
