// @effect-diagnostics nodeBuiltinImport:off globalFetch:off globalDate:off preferSchemaOverJson:off - Native HTTP and clock exercise the generated Node gateway in a cleanup-owned protocol fixture.
import { afterEach, expect, it, vi } from "vite-plus/test";
import * as NodeHttp from "node:http";
import * as NodeVM from "node:vm";
import * as NodeEvents from "node:events";
import * as NodeModule from "node:module";
import { directDeviceGatewayScript } from "./directDeviceGateway.ts";

const require = NodeModule.createRequire(import.meta.url);
const nodeSocket = require.resolve("@effect/platform-node/NodeSocket");
interface FixtureWebSocket extends NodeEvents.EventEmitter {
  send(data: Buffer, options?: { binary: boolean }): void;
  terminate(): void;
  close(code?: number): void;
}
interface FixtureWebSocketServer extends NodeEvents.EventEmitter {
  close(callback: () => void): void;
}
const { WebSocket, WebSocketServer } = NodeModule.createRequire(nodeSocket)("ws") as {
  WebSocket: new (url: string, options?: { origin: string }) => FixtureWebSocket;
  WebSocketServer: new (options: { server: NodeHttp.Server }) => FixtureWebSocketServer;
};
const disposers: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const dispose of disposers.splice(0).toReversed()) await dispose();
});
const listen = async (server: NodeHttp.Server) => {
  server.listen(0, "127.0.0.1");
  await NodeEvents.EventEmitter.once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing fixture port");
  disposers.push(async () => {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  });
  return address.port;
};

const fixture = async () => {
  const requests: Array<{ url: string; headers: NodeHttp.IncomingHttpHeaders }> = [];
  const admissions: Array<Record<string, unknown>> = [];
  const upstreamClosed: Array<Promise<unknown[]>> = [];
  let authorized = true;
  let admissionLarge = false;
  let admissionStatus = 200;
  let admissionMalformed = false;
  let admissionExpired = false;
  let admissionMismatch = false;
  let blocked: Promise<void> | undefined;
  let releaseAdmission: (() => void) | undefined;
  let enteredAdmission: (() => void) | undefined;
  let cancelledAdmission: (() => void) | undefined;
  let wsConnections = 0;
  const hub = NodeHttp.createServer((req, res) => {
    requests.push({ url: req.url ?? "", headers: req.headers });
    if (req.url?.includes("stream.avcc")) {
      res.writeHead(200, { "content-type": "application/octet-stream" });
      res.write("frame");
      upstreamClosed.push(NodeEvents.EventEmitter.once(res, "close"));
      return;
    }
    res.writeHead(200, {
      "set-cookie": "private-hub=secret",
      location: "http://secret.test",
      "access-control-allow-origin": "*",
    });
    res.end("result");
  });
  const hubWs = new WebSocketServer({ server: hub });
  const peers = new Set<InstanceType<typeof WebSocket>>();
  hubWs.on("connection", (socket: FixtureWebSocket) => {
    wsConnections++;
    peers.add(socket);
    socket.once("close", () => peers.delete(socket));
    upstreamClosed.push(NodeEvents.EventEmitter.once(socket, "close"));
    socket.on("message", (data: Buffer, binary: boolean) => socket.send(data, { binary }));
  });
  const hubPort = await listen(hub);
  disposers.push(async () => {
    for (const socket of peers) socket.terminate();
    await new Promise<void>((resolve) => hubWs.close(() => resolve()));
  });
  const callback = NodeHttp.createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(Buffer.from(chunk));
    const input = JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>;
    admissions.push(input);
    if (blocked) {
      const gate = blocked;
      res.once("close", () => cancelledAdmission?.());
      enteredAdmission?.();
      await gate;
      if (res.destroyed) return;
    }
    const permitted =
      authorized &&
      input.grant === "opaque-grant" &&
      input.origin === "t3code://app" &&
      input.hostId === "mini" &&
      input.generation === "generation";
    res.writeHead(permitted ? admissionStatus : 403, {
      "content-type": "application/json",
      ...(admissionStatus === 302 ? { location: `http://127.0.0.1:${hubPort}/redirected` } : {}),
    });
    res.end(
      permitted
        ? admissionMalformed
          ? "{malformed"
          : JSON.stringify({
              allowed: true,
              owner: admissionMismatch ? "wrong-owner" : "owner",
              generation: "generation",
              origin: "t3code://app",
              expiresAt: Date.now() + (admissionExpired ? -1 : 300000),
              ...(admissionLarge ? { padding: "x".repeat(4096) } : {}),
            })
        : "{}",
    );
  });
  const admissionPort = await listen(callback);
  const intervals = new Set<() => Promise<void>>();
  const timeouts = new Set<() => void>();
  const handlers = new Map<string, () => void>();
  let bound!: (value: { port: number }) => void;
  const listening = new Promise<{ port: number }>((resolve) => {
    bound = resolve;
  });
  let exited!: () => void;
  const exit = new Promise<void>((resolve) => {
    exited = resolve;
  });
  NodeVM.runInNewContext(
    directDeviceGatewayScript({
      hostId: "mini",
      owner: "owner",
      generation: "generation",
      hubPort,
      hubEntry: nodeSocket,
      admissionPort,
    }),
    {
      require,
      Buffer,
      URL,
      URLSearchParams,
      AbortSignal,
      AbortController,
      fetch,
      setInterval: (callback: () => Promise<void>) => {
        intervals.add(callback);
        return callback;
      },
      clearInterval: (callback: () => Promise<void>) => intervals.delete(callback),
      setTimeout: (callback: () => void) => {
        timeouts.add(callback);
        return callback;
      },
      clearTimeout: (callback: () => void) => timeouts.delete(callback),
      process: {
        send: bound,
        disconnect: () => {},
        once: (signal: string, handler: () => void) => handlers.set(signal, handler),
        exit: exited,
      },
    },
  );
  disposers.push(async () => {
    handlers.get("SIGTERM")?.();
    await exit;
  });
  const gateway = await listening;
  const base = `http://127.0.0.1:${gateway.port}/api/device-hub`;
  const query =
    "grant=opaque-grant&clientOrigin=t3code%3A%2F%2Fapp&wsTicket=private-vps&hostId=mini";
  return {
    base,
    query,
    hubPort,
    gatewayPort: gateway.port,
    requests,
    admissions,
    intervals,
    timeouts,
    upstreamClosed,
    revoke: () => {
      authorized = false;
    },
    largeResponse: () => {
      admissionLarge = true;
    },
    status: (status: number) => {
      admissionStatus = status;
    },
    malformed: () => {
      admissionMalformed = true;
    },
    expireVerdict: () => {
      admissionExpired = true;
    },
    mismatch: () => {
      admissionMismatch = true;
    },
    wsConnections: () => wsConnections,
    shutdown: async () => {
      handlers.get("SIGTERM")?.();
      await exit;
    },
    block: () => {
      blocked = new Promise<void>((resolve) => {
        releaseAdmission = resolve;
      });
      const entered = new Promise<void>((resolve) => {
        enteredAdmission = resolve;
      });
      const cancelled = new Promise<void>((resolve) => {
        cancelledAdmission = resolve;
      });
      return {
        entered,
        cancelled,
        release: () => {
          blocked = undefined;
          releaseAdmission?.();
        },
      };
    },
    revalidate: async () => {
      const ticks = [...intervals];
      for (const tick of ticks) await tick();
    },
  };
};

it("authenticates the owner probe and strips Hub credentials and wildcard CORS", async () => {
  const f = await fixture();
  expect((await fetch(`${f.base}/readyz`)).status).toBe(403);
  const probe = await fetch(`${f.base}/readyz?${f.query}`, { headers: { origin: "t3code://app" } });
  expect(probe.status).toBe(200);
  expect(await probe.json()).toEqual({ owner: "owner", generation: "generation" });
  expect(f.requests).toEqual([]);
  const response = await fetch(`${f.base}/vendor/serve-sim/helper/phone/config?${f.query}`, {
    headers: {
      origin: "t3code://app",
      cookie: "session=private",
      authorization: "Bearer private",
      dpop: "proof",
    },
  });
  expect(await response.text()).toBe("result");
  expect(response.headers.get("set-cookie")).toBeNull();
  expect(response.headers.get("location")).toBeNull();
  expect(response.headers.get("access-control-allow-origin")).toBe("t3code://app");
  expect(response.headers.get("access-control-allow-credentials")).toBeNull();
  expect(f.requests[0]?.url).toBe("/vendor/serve-sim/helper/phone/config");
  expect(f.requests[0]?.headers.cookie).toBeUndefined();
  expect(f.requests[0]?.headers.authorization).toBeUndefined();
  expect(f.requests[0]?.headers.dpop).toBeUndefined();
  expect(f.requests[0]?.headers.origin).toBe(`http://127.0.0.1:${f.hubPort}`);
  expect(f.intervals.size).toBe(0);
  expect(f.timeouts.size).toBe(0);
});

it("fails closed for rejected origin, grant, route and oversized admission responses", async () => {
  const f = await fixture();
  for (const [path, headers] of [
    [`/readyz?${f.query.replace("opaque-grant", "wrong")}`, {}],
    [`/readyz?${f.query}`, { origin: "https://other.test" }],
    [`/vendor/serve-sim/exec?${f.query}`, {}],
    [`/vendor/serve-sim/helper/phone/webrtc/offer?${f.query}`, {}],
  ] as const)
    expect((await fetch(f.base + path, { headers })).status).toBeGreaterThanOrEqual(400);
  f.largeResponse();
  expect((await fetch(`${f.base}/readyz?${f.query}`)).status).toBe(503);
  expect(f.requests).toEqual([]);
});

it("tears down both halves of an active HTTP stream when admission is revoked", async () => {
  const f = await fixture();
  const response = await fetch(`${f.base}/vendor/serve-sim/helper/phone/stream.avcc?${f.query}`);
  const reader = response.body!.getReader();
  expect(new TextDecoder().decode((await reader.read()).value)).toBe("frame");
  const pending = reader.read();
  f.revoke();
  await f.revalidate();
  await expect(pending).rejects.toThrow();
  await Promise.all(f.upstreamClosed);
  expect(f.intervals.size).toBe(0);
  expect(f.timeouts.size).toBe(0);
});

it("relays binary WebSocket input and closes both peers when validation is lost", async () => {
  const f = await fixture();
  const socket = new WebSocket(
    `${f.base.replace(/^http/, "ws")}/vendor/serve-sim/helper/ws?device=phone&${f.query}`,
    { origin: "t3code://app" },
  );
  disposers.push(async () => {
    socket.terminate();
  });
  await NodeEvents.EventEmitter.once(socket, "open");
  const reply = NodeEvents.EventEmitter.once(socket, "message");
  socket.send(Buffer.from([0x10, 0x20, 0x30]));
  expect(Buffer.from((await reply)[0])).toEqual(Buffer.from([0x10, 0x20, 0x30]));
  const closed = NodeEvents.EventEmitter.once(socket, "close");
  f.revoke();
  await f.revalidate();
  expect((await closed)[0]).toBe(1008);
  await Promise.all(f.upstreamClosed);
  expect(f.intervals.size).toBe(0);
  await vi.waitFor(() => expect(f.timeouts.size).toBe(0), { interval: 1, timeout: 1000 });
});

it("expires active streams and handles admitted preflight without opening the Hub", async () => {
  const f = await fixture();
  const preflight = await fetch(`${f.base}/vendor/serve-emu/api/fold?device=phone&${f.query}`, {
    method: "OPTIONS",
    headers: {
      origin: "t3code://app",
      "access-control-request-method": "POST",
      "access-control-request-headers": "content-type",
    },
  });
  expect(preflight.status).toBe(204);
  expect(preflight.headers.get("access-control-allow-origin")).toBe("t3code://app");
  expect(f.requests).toEqual([]);
  const response = await fetch(`${f.base}/vendor/serve-sim/helper/phone/stream.avcc?${f.query}`);
  const reader = response.body!.getReader();
  await reader.read();
  const pending = reader.read();
  const expires = [...f.timeouts];
  for (const expire of expires) expire();
  await expect(pending).rejects.toThrow();
  await Promise.all(f.upstreamClosed);
  expect(f.intervals.size).toBe(0);
  expect(f.timeouts.size).toBe(0);
});

it("exposes explicit denial and verifier/protocol outages with exact-origin CORS and no upstream", async () => {
  for (const mode of [
    "denied",
    "outage",
    "redirect",
    "malformed",
    "mismatch",
    "expired",
  ] as const) {
    const f = await fixture();
    if (mode === "denied") f.revoke();
    if (mode === "outage") f.status(503);
    if (mode === "redirect") f.status(302);
    if (mode === "malformed") f.malformed();
    if (mode === "mismatch") f.mismatch();
    if (mode === "expired") f.expireVerdict();
    const response = await fetch(`${f.base}/readyz?${f.query}`);
    expect(response.status).toBe(mode === "denied" || mode === "expired" ? 403 : 503);
    expect(response.headers.get("access-control-allow-origin")).toBe("t3code://app");
    expect(response.headers.get("vary")).toBe("Origin");
    expect(response.headers.get("access-control-allow-credentials")).toBeNull();
    expect(f.requests).toEqual([]);
  }
});

it("allows no-Origin MJPEG only with one matching bound origin hint", async () => {
  const f = await fixture();
  const path = `${f.base}/vendor/serve-sim/helper/phone/stream.mjpeg?${f.query}`;
  expect((await fetch(path)).status).toBe(200);
  for (const url of [
    path + "&clientOrigin=t3code%3A%2F%2Fapp",
    path.replace("clientOrigin=t3code%3A%2F%2Fapp&", ""),
  ])
    expect((await fetch(url)).status).toBe(403);
  expect((await fetch(path, { headers: { origin: "http://localhost:9999" } })).status).toBe(403);
  expect(f.requests).toHaveLength(1);
});

it("sends terminal 1008 on initial explicit WS denial without opening a Hub socket", async () => {
  const f = await fixture();
  f.revoke();
  const socket = new WebSocket(
    `${f.base.replace(/^http/, "ws")}/vendor/serve-sim/helper/ws?device=phone&${f.query}`,
    { origin: "t3code://app" },
  );
  disposers.push(async () => socket.terminate());
  const closed = NodeEvents.EventEmitter.once(socket, "close");
  await NodeEvents.EventEmitter.once(socket, "open");
  expect((await closed)[0]).toBe(1008);
  expect(f.wsConnections()).toBe(0);
  expect(f.intervals.size).toBe(0);
  await vi.waitFor(() => expect(f.timeouts.size).toBe(0), { interval: 1, timeout: 1000 });
});

it("uses 1013 for an active WS verifier outage and tears down upstream", async () => {
  const f = await fixture();
  const socket = new WebSocket(
    `${f.base.replace(/^http/, "ws")}/vendor/serve-sim/helper/ws?device=phone&${f.query}`,
    { origin: "t3code://app" },
  );
  disposers.push(async () => socket.terminate());
  await NodeEvents.EventEmitter.once(socket, "open");
  const closed = NodeEvents.EventEmitter.once(socket, "close");
  f.status(503);
  await f.revalidate();
  expect((await closed)[0]).toBe(1013);
  await Promise.all(f.upstreamClosed);
  expect(f.intervals.size).toBe(0);
  await vi.waitFor(() => expect(f.timeouts.size).toBe(0), { interval: 1, timeout: 1000 });
});

it("cancels deferred admission on disconnect and shutdown and gates late successful replies", async () => {
  for (const shutdown of [false, true]) {
    const f = await fixture();
    const gate = f.block();
    const controller = new AbortController();
    const result = fetch(`${f.base}/vendor/serve-sim/helper/phone/config?${f.query}`, {
      signal: controller.signal,
    });
    const failed = expect(result).rejects.toThrow();
    await gate.entered;
    if (shutdown) await f.shutdown();
    else controller.abort();
    await failed;
    await gate.cancelled;
    gate.release();
    if (!shutdown) expect((await fetch(`${f.base}/readyz?${f.query}`)).status).toBe(200);
    expect(f.requests).toEqual([]);
    expect(f.intervals.size).toBe(0);
    expect(f.timeouts.size).toBe(0);
  }
});

it("rejects initial WS verifier outage with 503 and never opens the Hub", async () => {
  const f = await fixture();
  f.status(503);
  const socket = new WebSocket(
    `${f.base.replace(/^http/, "ws")}/vendor/serve-sim/helper/ws?device=phone&${f.query}`,
    { origin: "t3code://app" },
  );
  disposers.push(async () => socket.terminate());
  const response = await new Promise<NodeHttp.IncomingMessage>((resolve, reject) => {
    socket.once("unexpected-response", (_request: unknown, res: NodeHttp.IncomingMessage) =>
      resolve(res),
    );
    socket.once("error", reject);
  });
  expect(response.statusCode).toBe(503);
  expect(response.headers["access-control-allow-origin"]).toBe("t3code://app");
  response.resume();
  socket.terminate();
  expect(f.wsConnections()).toBe(0);
});

it("cancels an active deferred revalidation when its WS disconnects", async () => {
  const f = await fixture();
  const socket = new WebSocket(
    `${f.base.replace(/^http/, "ws")}/vendor/serve-sim/helper/ws?device=phone&${f.query}`,
    { origin: "t3code://app" },
  );
  disposers.push(async () => socket.terminate());
  await NodeEvents.EventEmitter.once(socket, "open");
  const gate = f.block();
  const validation = f.revalidate();
  await gate.entered;
  socket.terminate();
  await gate.cancelled;
  gate.release();
  await validation;
  await Promise.all(f.upstreamClosed);
  expect(f.wsConnections()).toBe(1);
  expect(f.intervals.size).toBe(0);
  expect(f.timeouts.size).toBe(0);
});
