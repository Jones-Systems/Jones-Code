// @effect-diagnostics nodeBuiltinImport:off -- Disposable HTTP fixtures exercise Vite's actual upgrade proxy.
import * as NodeHttp from "node:http";
import * as NodeCrypto from "node:crypto";
import type * as NodeNet from "node:net";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  DEV_PROXIED_ORIGIN_PRESERVING_PREFIXES,
  DEV_PROXIED_PATH_PREFIXES,
} from "@t3tools/shared/devProxy";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { HttpServerRequest } from "effect/http";
import { createServer } from "vite-plus";

import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import * as ServerSecretStore from "../../auth/ServerSecretStore.ts";
import * as SessionStore from "../../auth/SessionStore.ts";
import * as ServerConfig from "../../config.ts";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";

const authLayer = EnvironmentAuth.layer.pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
  Layer.provide(ServerSecretStore.layer),
  Layer.provide(ServerEnvironment.layerIdentity),
  Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "dev-proxy-auth-" })),
  Layer.provide(NodeServices.layer),
);

it.live("preserves browser authority through API and RPC development proxies", () =>
  Effect.gen(function* () {
    const auth = yield* EnvironmentAuth.EnvironmentAuth;
    const sessions = yield* SessionStore.SessionStore;
    const pairing = yield* auth.issuePairingCredential({
      scopes: ["orchestration:read", "orchestration:operate", "terminal:operate", "relay:read"],
    });
    const session = yield* auth.createBrowserSession(pairing.credential, {
      deviceType: "desktop",
      os: "Linux",
      browser: "fixture",
      ipAddress: "127.0.0.1",
    });
    const cookie = `${sessions.cookieName}=${session.sessionToken}`;
    const verified = yield* sessions.verify(session.sessionToken);
    const ticket = yield* auth.issueWebSocketTicket(verified, { use: "media" });

    yield* Effect.promise(async () => {
      const observations: { path: string; host: string | undefined; origin: string | undefined }[] =
        [];
      const authenticate = async (request: NodeHttp.IncomingMessage, upgrade: boolean) => {
        observations.push({
          path: request.url!,
          host: request.headers.host,
          origin: request.headers.origin,
        });
        const headers = new Headers();
        for (const [name, value] of Object.entries(request.headers)) {
          if (value !== undefined)
            headers.set(name, Array.isArray(value) ? value.join(", ") : value);
        }
        const input = HttpServerRequest.fromWeb(
          new Request(`http://${request.headers.host}${request.url}`, { headers }),
        );
        // oxlint-disable-next-line t3code/no-manual-effect-runtime-in-tests -- Node HTTP callbacks must bridge the real proxy into the test-scoped production auth service.
        return Effect.runPromise(
          (upgrade
            ? auth.authenticateWebSocketUpgrade(input, request.url === "/ws" ? "rpc" : "media")
            : auth.authenticateHttpRequest(input)
          ).pipe(
            Effect.map(() => 200),
            Effect.catchTags({
              ServerAuthInvalidCredentialError: () => Effect.succeed(401),
              ServerAuthMissingCredentialError: () => Effect.succeed(401),
            }),
          ),
        );
      };
      const backend = NodeHttp.createServer((request, response) => {
        void authenticate(request, false).then((status) => {
          response.writeHead(status);
          response.end();
        });
      });
      backend.on("upgrade", (request, socket) => {
        void authenticate(request, true).then((status) => {
          if (status !== 200) {
            socket.end("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
            return;
          }
          const accept = NodeCrypto.createHash("sha1")
            .update(`${request.headers["sec-websocket-key"]}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
            .digest("base64");
          socket.end(
            `HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`,
          );
        });
      });
      await new Promise<void>((resolve) => backend.listen(0, "127.0.0.1", resolve));
      const target = `http://127.0.0.1:${(backend.address() as NodeNet.AddressInfo).port}`;
      let proxy: Awaited<ReturnType<typeof createServer>> | undefined;
      try {
        proxy = await createServer({
          configFile: false,
          envFile: false,
          logLevel: "silent",
          server: {
            host: "127.0.0.1",
            port: 0,
            proxy: Object.fromEntries(
              DEV_PROXIED_PATH_PREFIXES.map((prefix) => [
                prefix,
                {
                  target,
                  changeOrigin: !DEV_PROXIED_ORIGIN_PRESERVING_PREFIXES.has(prefix),
                  ...(prefix === "/ws" || prefix === "/api" ? { ws: true } : {}),
                },
              ]),
            ),
          },
        });
        await proxy.listen();
        const port = (proxy.httpServer!.address() as NodeNet.AddressInfo).port;
        // Model the SSH-forwarded browser authority, independently of either fixture port.
        const host = "127.0.0.1:26633";
        const origin = `http://${host}`;
        const request = (path: string, upgrade: boolean, extra: Record<string, string> = {}) =>
          new Promise<number>((resolve, reject) => {
            const req = NodeHttp.request({
              host: "127.0.0.1",
              port,
              path,
              headers: {
                host,
                origin,
                cookie,
                ...(upgrade
                  ? {
                      connection: "Upgrade",
                      upgrade: "websocket",
                      "sec-websocket-version": "13",
                      "sec-websocket-key": "dGhlIHNhbXBsZSBub25jZQ==",
                    }
                  : {}),
                ...extra,
              },
            });
            req.on("response", (response) => {
              response.resume();
              resolve(response.statusCode!);
            });
            req.on("upgrade", (response, socket) => {
              socket.destroy();
              resolve(response.statusCode!);
            });
            req.on("error", reject);
            req.setTimeout(5_000, () => req.destroy(new Error("proxy fixture timed out")));
            req.end();
          });

        expect(await request("/api/auth/session", false, { "sec-fetch-site": "same-origin" })).toBe(
          200,
        );
        expect(await request("/ws", true)).toBe(101);
        expect(await request("/ws", true, { origin: "http://unrelated.invalid" })).toBe(401);
        expect(
          await request(`/api/preview-stream/ws?wsTicket=${ticket.ticket}`, true, { cookie: "" }),
        ).toBe(101);
        expect(await request("/api/preview-stream/ws", true, { cookie: "" })).toBe(401);
        expect(
          await request("/api/preview-stream/ws", true, {
            cookie: `${sessions.cookieName}=invalid`,
          }),
        ).toBe(401);
        expect(
          await request("/api/preview-stream/ws", true, { origin: "http://unrelated.invalid" }),
        ).toBe(401);
        expect(
          await request("/api/preview-stream/ws", true, { origin: "http://127.0.0.1:26634" }),
        ).toBe(401);
        expect(
          await request("/api/device-hub/api/devices/ws", true, {
            origin: "http://unrelated.invalid",
          }),
        ).toBe(401);
        const previewStatus = await request("/api/preview-stream/ws", true);
        expect({ status: previewStatus, ...observations.at(-1) }).toEqual({
          status: 101,
          path: "/api/preview-stream/ws",
          host,
          origin,
        });
        expect(await request("/api/device-hub/api/devices/ws", true)).toBe(101);
        expect(await request("/api/device-hub/stream.mjpeg", false)).toBe(200);
        expect(await request("/api/auth/session", false)).toBe(200);
        expect(
          await request("/api/auth/session", false, { origin: "http://unrelated.invalid" }),
        ).toBe(401);
        expect(observations.every((entry) => entry.host === host)).toBe(true);
        expect(observations.find((entry) => entry.path === "/api/preview-stream/ws")?.origin).toBe(
          origin,
        );
      } finally {
        await proxy?.close();
        backend.closeAllConnections();
        await new Promise<void>((resolve, reject) =>
          backend.close((error) => (error ? reject(error) : resolve())),
        );
      }
    });
  }).pipe(Effect.provide(authLayer)),
);
