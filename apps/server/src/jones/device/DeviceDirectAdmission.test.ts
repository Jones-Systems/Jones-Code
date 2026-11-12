// @effect-diagnostics nodeBuiltinImport:off - native protocol adapter tests a cleanup-owned scoped loopback listener.
import * as NodeHttp from "node:http";
import * as NodeNet from "node:net";
import { expect, it } from "@effect/vitest";
import { afterEach, vi } from "vite-plus/test";
import * as NodeEvents from "node:events";
import { AuthSessionId, AuthOrchestrationReadScope } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import {
  SessionStore,
  MalformedWebSocketTokenError,
  SessionCredentialVerificationError,
} from "../../auth/SessionStore.ts";
import { layer, DeviceDirectGrants, DeviceDirectAccessInput } from "./DeviceDirectGrants.ts";
import { openDirectAdmission } from "./DeviceDirectAdmission.ts";
import { DeviceHostError } from "../../device/DeviceHost.ts";

const decodeAccess = Schema.decodeUnknownSync(DeviceDirectAccessInput);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

const request = (
  port: number,
  body: string,
  path = "/api/device-hub/direct-admission",
  method = "POST",
  headers: NodeHttp.OutgoingHttpHeaders = { "content-type": "application/json" },
) =>
  Effect.tryPromise({
    try: (signal) =>
      new Promise<{ status: number; headers: NodeHttp.IncomingHttpHeaders; body: string }>(
        (resolve, reject) => {
          const req = NodeHttp.request(
            { host: "127.0.0.1", port, path, method, headers, signal },
            (res) => {
              const chunks: Buffer[] = [];
              res.on("data", (chunk: Buffer) => chunks.push(chunk));
              res.once("error", reject);
              res.once("end", () =>
                resolve({
                  status: res.statusCode!,
                  headers: res.headers,
                  body: Buffer.concat(chunks).toString("utf8"),
                }),
              );
            },
          );
          req.once("error", reject);
          req.end(body);
        },
      ),
    catch: (cause) =>
      new DeviceHostError({ hostId: "mini", step: "native admission request", cause }),
  });
const rawRequest = (port: number, value: string) =>
  Effect.tryPromise({
    try: (signal) =>
      new Promise<string>((resolve, reject) => {
        const socket = NodeNet.connect({ host: "127.0.0.1", port });
        const abort = () => socket.destroy();
        signal.addEventListener("abort", abort, { once: true });
        let output = "";
        socket.once("connect", () => socket.write(value));
        socket.on("data", (chunk) => {
          output += chunk.toString();
        });
        socket.once("error", reject);
        socket.once("close", () => {
          signal.removeEventListener("abort", abort);
          resolve(output);
        });
      }),
    catch: (cause) =>
      new DeviceHostError({ hostId: "mini", step: "native admission socket", cause }),
  });
const setup = Effect.gen(function* () {
  let active = true;
  const grants = yield* DeviceDirectGrants;
  const endpoint = { target: "mini", owner: "owner", generation: "generation", gatewayPort: 1234 };
  const input = decodeAccess({
    hostId: "mini",
    deviceId: "phone",
    platform: "ios",
    clientOrigin: "t3code://app",
  });
  const issued = yield* grants.issue(input, endpoint, {
    sessionId: AuthSessionId.make("test"),
    method: "bearer-access-token",
    subject: "test",
    scopes: [AuthOrchestrationReadScope],
  });
  const scope = yield* Scope.make();
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
  const listener = yield* openDirectAdmission({
    hostId: "mini",
    ...endpoint,
    currentEndpoint: Effect.succeed(endpoint),
    isActive: () => active,
  }).pipe(Effect.provideService(Scope.Scope, scope));
  const body = encodeJson({
    grant: issued!.grant,
    hostId: "mini",
    generation: "generation",
    origin: "t3code://app",
    path: "/readyz",
    search: "",
    method: "GET",
    upgrade: false,
  });
  return {
    ...listener,
    body,
    scope,
    retire: () => {
      active = false;
    },
  };
});
// The same layer instance supplies issuance and listener validation; no DeviceService is needed.
const fixture = Effect.gen(function* () {
  let fail: "none" | "denied" | "internal" | "pending" = "none";
  const entered = yield* Deferred.make<void>();
  const cancelling = yield* Deferred.make<void>();
  const releaseCleanup = yield* Deferred.make<void>();
  let waitForCleanup = false;
  let cancelled = false;
  let verifications = 0;
  const sessions = {
    issueWebSocketToken: () =>
      DateTime.now.pipe(
        Effect.map((now) => ({
          token: "private",
          expiresAt: DateTime.makeUnsafe(now.epochMilliseconds + 300000),
        })),
      ),
    verifyWebSocketToken: () =>
      Effect.gen(function* () {
        verifications++;
        if (fail === "denied") return yield* new MalformedWebSocketTokenError({});
        if (fail === "internal")
          return yield* new SessionCredentialVerificationError({
            sessionId: AuthSessionId.make("test"),
            cause: new Error("fixture outage"),
          });
        if (fail === "pending") {
          return yield* Effect.gen(function* () {
            yield* Deferred.succeed(entered, undefined);
            return yield* Effect.never;
          }).pipe(
            Effect.ensuring(
              Effect.gen(function* () {
                yield* Deferred.succeed(cancelling, undefined);
                if (waitForCleanup) yield* Deferred.await(releaseCleanup);
                cancelled = true;
              }),
            ),
          );
        }
        return { scopes: [AuthOrchestrationReadScope] };
      }),
  } as unknown as SessionStore["Service"];
  const f = yield* setup.pipe(
    Effect.provide(layer.pipe(Layer.provide(Layer.succeed(SessionStore, sessions)))),
  );
  return {
    ...f,
    entered,
    cancelling,
    releaseCleanup,
    blockCleanup: () => {
      waitForCleanup = true;
    },
    fail: (mode: typeof fail) => {
      fail = mode;
    },
    cancelled: () => cancelled,
    verifications: () => verifications,
  };
});

it.effect(
  "shares issuance with the narrow listener and keeps broad routes and methods absent",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const accepted = yield* request(f.port, f.body);
      expect(accepted.status).toBe(200);
      expect(accepted.body).not.toContain("private");
      expect(Buffer.byteLength(accepted.body)).toBeLessThanOrEqual(2048);
      expect(accepted.headers["cache-control"]).toBe("no-store");
      expect(accepted.headers["access-control-allow-origin"]).toBeUndefined();
      for (const path of [
        "/api/devices",
        "/api/device-hub/direct-access",
        "/api/device-hub/direct-admission?x=1",
        "/api/device-hub/direct-admission/",
        "/ws",
      ])
        expect((yield* request(f.port, f.body, path)).status).toBe(404);
      for (const method of ["GET", "PUT", "OPTIONS"])
        expect((yield* request(f.port, "", undefined, method)).status).toBe(405);
      expect(
        yield* rawRequest(
          f.port,
          "GET /api/device-hub/direct-admission HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n",
        ),
      ).toContain("405");
      expect(
        yield* rawRequest(f.port, "CONNECT localhost:80 HTTP/1.1\r\nHost: localhost\r\n\r\n"),
      ).toContain("405");
    }).pipe(Effect.scoped),
);

it.effect("bounds content and distinguishes denial, retirement and internal outage", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    expect(
      (yield* request(f.port, f.body, undefined, undefined, { "content-type": "text/plain" }))
        .status,
    ).toBe(400);
    expect((yield* request(f.port, "{broken")).status).toBe(400);
    expect(
      (yield* request(f.port, f.body, undefined, undefined, {
        "content-type": "application/json",
        "content-encoding": "gzip",
      })).status,
    ).toBe(400);
    const invalid = f.body.replace(/"grant":"[^"]+"/, '"grant":"wrong"');
    expect(
      (yield* request(f.port, invalid, undefined, undefined, {
        "content-type": "application/json",
        cookie: "session=private",
        authorization: "Bearer private",
      })).status,
    ).toBe(403);
    expect((yield* request(f.port, " ".repeat(8193))).status).toBe(413);
    expect(
      yield* rawRequest(
        f.port,
        "POST /api/device-hub/direct-admission HTTP/1.1\r\nHost: localhost\r\nX-Large: " +
          "x".repeat(8193) +
          "\r\nContent-Length: 0\r\n\r\n",
      ),
    ).toContain("413");
    f.fail("denied");
    expect((yield* request(f.port, f.body)).status).toBe(403);
    f.fail("internal");
    expect((yield* request(f.port, f.body)).status).toBe(503);
    f.fail("none");
    f.retire();
    expect((yield* request(f.port, f.body)).status).toBe(403);
  }).pipe(Effect.scoped),
);

it.effect("scope retirement interrupts a deferred verifier and closes its owned socket", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    f.fail("pending");
    f.blockCleanup();
    const response = yield* request(f.port, f.body).pipe(Effect.result, Effect.forkChild);
    yield* Deferred.await(f.entered);
    f.retire();
    let closed = false;
    const closing = yield* Scope.close(f.scope, Exit.void).pipe(
      Effect.tap(() =>
        Effect.sync(() => {
          closed = true;
        }),
      ),
      Effect.forkChild,
    );
    yield* Deferred.await(f.cancelling);
    const beforeRelease = { closed, cancelled: f.cancelled() };
    yield* Deferred.succeed(f.releaseCleanup, undefined);
    yield* Fiber.join(closing);
    expect(beforeRelease).toEqual({ closed: false, cancelled: false });
    expect(closed).toBe(true);
    expect(f.cancelled()).toBe(true);
    const result = yield* Fiber.join(response);
    expect(result._tag).toBe("Failure");
  }).pipe(Effect.scoped),
);

it.effect("maps a private bind failure to DeviceHostError and closes the acquired server", () =>
  Effect.gen(function* () {
    const servers: NodeHttp.Server[] = [];
    const closeCounts: Array<() => number> = [];
    vi.spyOn(NodeHttp.Server.prototype, "listen").mockImplementation(function (
      this: NodeHttp.Server,
    ) {
      servers.push(this);
      const close = vi.spyOn(this, "close");
      closeCounts.push(() => close.mock.calls.length);
      queueMicrotask(() => this.emit("error", new Error("fixture bind failure")));
      return this;
    } as typeof NodeHttp.Server.prototype.listen);
    const result = yield* fixture.pipe(Effect.scoped, Effect.result);
    expect(result._tag).toBe("Failure");
    if (result._tag === "Failure") {
      expect(result.failure._tag).toBe("DeviceHostError");
      if (result.failure._tag === "DeviceHostError")
        expect(result.failure.step).toBe("binding direct admission");
    }
    expect(servers).toHaveLength(1);
    expect(servers[0]!.listening).toBe(false);
    expect(closeCounts[0]!()).toBeGreaterThan(0);
  }),
);

it.effect(
  "enforces the absolute five-second deadline on partial headers and body without verifying",
  () =>
    Effect.gen(function* () {
      const listen = NodeHttp.Server.prototype.listen;
      let accepted!: (socket: NodeNet.Socket) => void;
      vi.spyOn(NodeHttp.Server.prototype, "listen").mockImplementation(function (
        this: NodeHttp.Server,
        ...args: Parameters<typeof listen>
      ) {
        this.on("connection", (socket) => accepted(socket));
        return listen.apply(this, args);
      } as typeof listen);
      for (const bodyStarted of [false, true]) {
        const f = yield* fixture;
        vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
        yield* Effect.promise((signal) =>
          (async () => {
            const connected = new Promise<NodeNet.Socket>((resolve) => {
              accepted = resolve;
            });
            const socket = NodeNet.connect({ host: "127.0.0.1", port: f.port });
            const abort = () => socket.destroy();
            signal.addEventListener("abort", abort, { once: true });
            try {
              socket.on("error", () => {});
              const peer = await connected;
              const received = NodeEvents.EventEmitter.once(peer, "data");
              const closed = new Promise<void>((resolve) => socket.once("close", () => resolve()));
              socket.write(
                "POST /api/device-hub/direct-admission HTTP/1.1\r\nHost: localhost\r\n" +
                  (bodyStarted
                    ? "Content-Type: application/json\r\nContent-Length: 8192\r\n\r\n{"
                    : "X-Partial: incomplete"),
              );
              await received;
              await vi.advanceTimersByTimeAsync(5000);
              await closed;
              expect(f.verifications()).toBe(0);
            } finally {
              signal.removeEventListener("abort", abort);
              socket.destroy();
              vi.useRealTimers();
            }
          })(),
        );
      }
    }).pipe(Effect.scoped),
);
