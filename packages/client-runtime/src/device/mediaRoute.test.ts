import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import {
  createDeviceMediaRouteManager,
  type DeviceMediaRoute,
  type DeviceMediaTunnelBridge,
} from "./mediaRoute.ts";
import type { DeviceHubAccess } from "./hubAccess.ts";
import { createDeviceStreamClient } from "./stream.ts";

const proxy: DeviceHubAccess = {
  httpBase: "https://vps.example/api/device-hub",
  wsBase: "wss://vps.example/api/device-hub",
  query: { wsTicket: "vps-ticket", hostId: "mini" },
  credentials: false,
};
const grant = {
  target: "mini",
  gatewayPort: 12345,
  owner: "owner",
  generation: "generation-1",
  grant: "restricted-media-grant",
  expiresAt: 300_000,
};
const tunnel = { id: "tunnel-1", httpBase: "http://127.0.0.1:32123/api/device-hub" };
const managers: Array<ReturnType<typeof createDeviceMediaRouteManager>> = [];

function controlled<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((finish) => {
    resolve = finish;
  });
  return { promise, resolve };
}

function setup(
  options: {
    unsupported?: boolean;
    fetch?: typeof globalThis.fetch;
    open?: DeviceMediaTunnelBridge["openDeviceMediaTunnel"];
    retire?: () => void;
    platform?: "ios" | "android";
  } = {},
) {
  const routes: DeviceMediaRoute[] = [];
  const receipts: Array<(route: DeviceMediaRoute) => void> = [];
  const order: string[] = [];
  const close = vi.fn(async (id: string) => {
    order.push(`close:${id}`);
  });
  const open = vi.fn(options.open ?? (async () => tunnel));
  const fetch = vi.fn(
    options.fetch ??
      (async (url) => {
        return String(url).includes("/direct-access")
          ? Response.json(grant)
          : Response.json({ owner: grant.owner, generation: grant.generation });
      }),
  );
  const refreshAccess = vi.fn();
  const manager = createDeviceMediaRouteManager({
    proxy,
    hostId: "mini",
    deviceId: "simulator",
    platform: options.platform ?? "ios",
    clientOrigin: "https://client.example",
    ...(options.unsupported
      ? {}
      : {
          bridge: { openDeviceMediaTunnel: open, closeDeviceMediaTunnel: close },
        }),
    fetch,
    // @effect-diagnostics-next-line globalDate:off -- Grant deadlines must share the Vitest fake timer clock.
    now: () => Date.now(),
    retire: () => {
      order.push("retire");
      options.retire?.();
    },
    onRoute: (route) => {
      routes.push(route);
      for (const resolve of receipts.splice(0)) resolve(route);
    },
    refreshAccess,
  });
  managers.push(manager);
  const current = () => routes.at(-1)!;
  const nextRoute = () =>
    new Promise<DeviceMediaRoute>((resolve) => {
      receipts.push(resolve);
    });
  const ready = () => {
    manager.report(current().access!, "video", true);
    manager.report(current().access!, "input", true);
  };
  return { manager, routes, order, fetch, open, close, refreshAccess, current, nextRoute, ready };
}

function setupStreamRoute() {
  vi.stubGlobal("VideoDecoder", vi.fn());
  vi.stubGlobal("EncodedVideoChunk", vi.fn());
  const sockets: FakeSocket[] = [];
  class FakeSocket {
    static OPEN = 1;
    readyState = 1;
    onopen: (() => void) | null = null;
    onclose: ((event: { code: number; reason: string }) => void) | null = null;
    send = vi.fn();
    close = vi.fn();
    readonly url: string;
    constructor(url: string) {
      this.url = url;
      sockets.push(this);
    }
  }
  vi.stubGlobal("WebSocket", FakeSocket);
  let client: ReturnType<typeof createDeviceStreamClient> | null = null;
  const run = setup({
    platform: "android",
    retire: () => {
      client?.stop();
      client = null;
    },
  });
  const attach = (access: DeviceHubAccess) => {
    client = createDeviceStreamClient(
      { platform: "android", deviceId: "simulator", access },
      { getContext: () => null } as unknown as HTMLCanvasElement,
      {
        onStatus: (status, detail) =>
          run.manager.report(access, "video", status === "streaming", detail),
        onInputConnected: (connected, detail) =>
          run.manager.report(access, "input", connected, detail),
        onUnauthorized: () => run.manager.unauthorized(access),
        onScreen: () => {},
        onMjpegFallback: () => {},
      },
    );
    const attached = client;
    attached.start();
    return attached;
  };
  return { ...run, attach, sockets };
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(0);
});
afterEach(() => {
  for (const manager of managers.splice(0)) manager.stop();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("device media route", () => {
  it("keeps old desktop/web clients on authenticated VPS access without opening a tunnel", async () => {
    const run = setup({ unsupported: true });
    await run.manager.start();
    expect(run.current()).toMatchObject({ access: proxy, kind: "proxy", phase: "connecting" });
    expect(run.fetch).not.toHaveBeenCalled();
    expect(run.open).not.toHaveBeenCalled();
    run.ready();
    expect(run.current().phase).toBe("connected");
    run.manager.stop();
  });

  it("uses restricted credentials for direct video and input and requires both to connect", async () => {
    const run = setup();
    await run.manager.start();
    const selected = run.current().access!;
    expect(run.current()).toMatchObject({
      kind: "direct",
      phase: "connecting",
      generation: grant.generation,
    });
    expect(selected).toEqual({
      httpBase: tunnel.httpBase,
      wsBase: tunnel.httpBase.replace(/^http/, "ws"),
      query: {
        grant: grant.grant,
        hostId: "mini",
        clientOrigin: "https://client.example",
      },
      credentials: false,
    });
    expect(String(run.fetch.mock.calls[0]![0])).toContain("wsTicket=vps-ticket");
    expect(String(run.fetch.mock.calls[0]![0])).toContain(
      "clientOrigin=https%3A%2F%2Fclient.example",
    );
    expect(String(run.fetch.mock.calls[1]![0])).toBe(
      `${tunnel.httpBase}/readyz?grant=${grant.grant}&hostId=mini&clientOrigin=https%3A%2F%2Fclient.example`,
    );
    expect(String(run.fetch.mock.calls[1]![0])).not.toContain("vps-ticket");
    run.manager.report(selected, "video", true);
    expect(run.current().phase).toBe("connecting");
    run.manager.report(selected, "input", true);
    expect(run.current().phase).toBe("connected");
    run.manager.stop();
    expect(run.order.slice(-2)).toEqual(["retire", "close:tunnel-1"]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("falls back when the authenticated probe succeeds but input never connects", async () => {
    const run = setup();
    await run.manager.start();
    const stale = run.current().access!;
    run.manager.report(stale, "video", true);
    await vi.advanceTimersByTimeAsync(6_000);
    expect(run.current()).toMatchObject({
      kind: "proxy",
      access: proxy,
      reason: "Direct video or input did not connect.",
    });
    expect(run.close).toHaveBeenCalledWith(tunnel.id);
    const count = run.routes.length;
    run.manager.report(stale, "input", true);
    run.manager.unauthorized(stale);
    expect(run.routes).toHaveLength(count);
    expect(run.refreshAccess).not.toHaveBeenCalled();
    run.manager.stop();
  });

  it("retires direct input immediately after a transport failure and ignores its stale events", async () => {
    const run = setup();
    await run.manager.start();
    run.ready();
    const stale = run.current().access!;
    run.manager.report(stale, "input", false, "socket refused");
    expect(run.current()).toMatchObject({ access: null, kind: "direct", phase: "connecting" });
    expect(run.order.at(-1)).toBe("retire");
    expect(run.close).not.toHaveBeenCalled();
    await run.nextRoute();
    expect(run.current().access).toBe(proxy);
    expect(run.order.slice(-2)).toEqual(["retire", "close:tunnel-1"]);
    const count = run.routes.length;
    run.manager.report(stale, "video", true);
    expect(run.routes).toHaveLength(count);
    run.manager.stop();
  });

  it.each([
    { status: 401, first: "video" },
    { status: 401, first: "input" },
    { status: 403, first: "video" },
    { status: 403, first: "input" },
    { status: 503, first: "video" },
    { status: 503, first: "input" },
  ] as const)(
    "classifies same-grant $status after $first loss and serializes concurrent channel events",
    async ({ status, first }) => {
      const run = setup();
      await run.manager.start();
      run.ready();
      const access = run.current().access!;
      const response = controlled<Response>();
      run.fetch.mockImplementationOnce(() => {
        expect(run.order.at(-1)).toBe("retire");
        return response.promise;
      });
      const retirements = run.order.length;
      run.manager.report(access, first, false);
      run.manager.report(access, first === "video" ? "input" : "video", false, "closed");
      run.manager.report(access, "video", true);
      run.manager.report(access, "input", true);
      expect(run.current()).toMatchObject({ access: null, kind: "direct", phase: "connecting" });
      expect(run.order.slice(retirements)).toEqual(["retire"]);
      expect(run.open).toHaveBeenCalledOnce();
      expect(run.close).not.toHaveBeenCalled();
      expect(run.fetch).toHaveBeenCalledTimes(3);
      const [url, options] = run.fetch.mock.calls[2]!;
      expect(new URL(String(url)).searchParams).toEqual(new URLSearchParams(access.query));
      expect(new URL(String(url)).pathname).toBe("/api/device-hub/readyz");
      expect(String(url)).not.toContain("vps-ticket");
      expect(options).toMatchObject({ credentials: "omit", redirect: "error" });
      const classified = run.nextRoute();
      response.resolve(new Response(null, { status }));
      await classified;
      if (status === 503) {
        expect(run.current()).toMatchObject({ access: proxy, kind: "proxy", phase: "connecting" });
        expect(run.refreshAccess).not.toHaveBeenCalled();
      } else {
        expect(run.current()).toMatchObject({ access: null, phase: "denied" });
        expect(run.routes.some((route) => route.access === proxy)).toBe(false);
        expect(run.refreshAccess).toHaveBeenCalledOnce();
        expect(vi.getTimerCount()).toBe(0);
      }
      expect(run.close).toHaveBeenCalledExactlyOnceWith(tunnel.id);
      run.manager.stop();
      expect(run.close).toHaveBeenCalledOnce();
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it.each(["network", "redirect", "malformed", "identity", "healthy"] as const)(
    "uses authenticated fallback after a same-grant %s result",
    async (outcome) => {
      const run = setup();
      await run.manager.start();
      run.ready();
      run.fetch.mockImplementationOnce(async () => {
        if (outcome === "network") throw new TypeError("network unavailable");
        if (outcome === "redirect") return new Response(null, { status: 302 });
        if (outcome === "malformed") return new Response("not JSON");
        return Response.json({
          owner: grant.owner,
          generation: outcome === "identity" ? "replacement" : grant.generation,
        });
      });
      run.manager.report(run.current().access!, "video", false, "MJPEG ended");
      await run.nextRoute();
      expect(run.current()).toMatchObject({ access: proxy, kind: "proxy" });
      expect(run.refreshAccess).not.toHaveBeenCalled();
      expect(run.open).toHaveBeenCalledOnce();
      expect(run.close).toHaveBeenCalledExactlyOnceWith(tunnel.id);
      run.manager.stop();
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it("refreshes locally expired access on loss without probing or selecting proxy", async () => {
    const run = setup();
    await run.manager.start();
    run.ready();
    vi.setSystemTime(grant.expiresAt);
    run.manager.report(run.current().access!, "video", false);
    expect(run.current()).toMatchObject({ access: null, phase: "denied" });
    expect(run.fetch).toHaveBeenCalledTimes(2);
    expect(run.refreshAccess).toHaveBeenCalledOnce();
    expect(run.routes.some((route) => route.access === proxy)).toBe(false);
    expect(run.close).toHaveBeenCalledExactlyOnceWith(tunnel.id);
    run.manager.stop();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("denies when the same grant expires during a pending loss check", async () => {
    const run = setup({
      fetch: async (url) =>
        String(url).includes("/direct-access")
          ? Response.json({ ...grant, expiresAt: 2_000 })
          : Response.json({ owner: grant.owner, generation: grant.generation }),
    });
    await run.manager.start();
    run.ready();
    const response = controlled<Response>();
    run.fetch.mockImplementationOnce(() => response.promise);
    run.manager.report(run.current().access!, "input", false, "closed");
    const signal = run.fetch.mock.calls[2]![1]!.signal!;
    await vi.advanceTimersByTimeAsync(2_000);
    expect(run.current()).toMatchObject({ access: null, phase: "denied" });
    expect(signal.aborted).toBe(true);
    expect(run.refreshAccess).toHaveBeenCalledOnce();
    response.resolve(new Response(null, { status: 503 }));
    await vi.advanceTimersByTimeAsync(0);
    expect(run.routes.some((route) => route.access === proxy)).toBe(false);
    expect(run.close).toHaveBeenCalledExactlyOnceWith(tunnel.id);
    run.manager.stop();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("bounds a hung loss check and ignores a late denial after fallback", async () => {
    const run = setup();
    await run.manager.start();
    run.ready();
    const response = controlled<Response>();
    run.fetch.mockImplementationOnce(() => response.promise);
    run.manager.report(run.current().access!, "video", false, "MJPEG ended");
    const signal = run.fetch.mock.calls[2]![1]!.signal!;
    await vi.advanceTimersByTimeAsync(4_999);
    expect(run.current().access).toBeNull();
    expect(run.close).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(run.current().access).toBe(proxy);
    expect(signal.aborted).toBe(true);
    const count = run.routes.length;
    response.resolve(new Response(null, { status: 403 }));
    await vi.advanceTimersByTimeAsync(0);
    expect(run.routes).toHaveLength(count);
    expect(run.refreshAccess).not.toHaveBeenCalled();
    expect(run.open).toHaveBeenCalledOnce();
    expect(run.close).toHaveBeenCalledExactlyOnceWith(tunnel.id);
    run.manager.stop();
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([403, 503])(
    "lets explicit authorization denial cancel a pending %s check",
    async (status) => {
      const run = setup();
      await run.manager.start();
      run.ready();
      const access = run.current().access!;
      const response = controlled<Response>();
      run.fetch.mockImplementationOnce(() => response.promise);
      run.manager.report(access, "video", false);
      const signal = run.fetch.mock.calls[2]![1]!.signal!;
      run.manager.unauthorized(access);
      expect(run.current()).toMatchObject({ access: null, phase: "denied" });
      expect(signal.aborted).toBe(true);
      const count = run.routes.length;
      response.resolve(new Response(null, { status }));
      await vi.advanceTimersByTimeAsync(0);
      run.manager.report(access, "input", false, "closed 1008");
      run.manager.unauthorized(access);
      expect(run.routes).toHaveLength(count);
      expect(run.refreshAccess).toHaveBeenCalledOnce();
      expect(run.routes.some((route) => route.access === proxy)).toBe(false);
      expect(run.close).toHaveBeenCalledExactlyOnceWith(tunnel.id);
      run.manager.stop();
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it.each(["same", "changed"] as const)(
    "discards a superseded loss check with a %s gateway generation",
    async (replacementKind) => {
      let opened = 0;
      const run = setup({ open: async () => ({ ...tunnel, id: `tunnel-${++opened}` }) });
      await run.manager.start();
      run.ready();
      const old = run.current().access!;
      const response = controlled<Response>();
      run.fetch.mockImplementationOnce(() => response.promise);
      run.manager.report(old, "video", false);
      const signal = run.fetch.mock.calls[2]![1]!.signal!;
      const generation = replacementKind === "same" ? grant.generation : "generation-2";
      run.fetch.mockResolvedValueOnce(Response.json({ ...grant, generation }));
      run.fetch.mockResolvedValueOnce(Response.json({ owner: grant.owner, generation }));
      await run.manager.start();
      expect(signal.aborted).toBe(true);
      expect(run.current()).toMatchObject({ kind: "direct", generation });
      const replacement = run.current().access;
      const count = run.routes.length;
      response.resolve(new Response(null, { status: 403 }));
      await vi.advanceTimersByTimeAsync(0);
      run.manager.report(old, "input", false, "closed");
      run.manager.unauthorized(old);
      expect(run.current().access).toBe(replacement);
      expect(run.routes).toHaveLength(count);
      expect(run.refreshAccess).not.toHaveBeenCalled();
      expect(run.close).toHaveBeenCalledExactlyOnceWith("tunnel-1");
      run.manager.stop();
      expect(run.close.mock.calls).toEqual([["tunnel-1"], ["tunnel-2"]]);
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it("discards identity parsing that completes after a loss check is superseded", async () => {
    const run = setup();
    await run.manager.start();
    run.ready();
    const body = controlled<unknown>();
    const reading = controlled<void>();
    const response = Response.json({});
    vi.spyOn(response, "json").mockImplementation(() => {
      reading.resolve(undefined);
      return body.promise;
    });
    run.fetch.mockResolvedValueOnce(response);
    run.manager.report(run.current().access!, "input", false, "closed");
    await reading.promise;
    await run.manager.start();
    const replacement = run.current().access;
    const count = run.routes.length;
    body.resolve({ owner: "stale-owner", generation: "stale-generation" });
    await vi.advanceTimersByTimeAsync(0);
    expect(run.current().access).toBe(replacement);
    expect(run.routes).toHaveLength(count);
    expect(run.refreshAccess).not.toHaveBeenCalled();
    run.manager.stop();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("cancels a pending loss check and closes its retained tunnel once when stopped", async () => {
    const run = setup();
    await run.manager.start();
    run.ready();
    const response = controlled<Response>();
    run.fetch.mockImplementationOnce(() => response.promise);
    run.manager.report(run.current().access!, "video", false);
    const signal = run.fetch.mock.calls[2]![1]!.signal!;
    const count = run.routes.length;
    run.manager.stop();
    run.manager.stop();
    expect(signal.aborted).toBe(true);
    response.resolve(new Response(null, { status: 403 }));
    await vi.advanceTimersByTimeAsync(0);
    expect(run.routes).toHaveLength(count);
    expect(run.refreshAccess).not.toHaveBeenCalled();
    expect(run.close).toHaveBeenCalledExactlyOnceWith(tunnel.id);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([1008, 1013])(
    "preserves WebSocket close %s disposition during stream retirement",
    async (code) => {
      const run = setupStreamRoute();
      await run.manager.start();
      const access = run.current().access!;
      const stream = run.attach(access);
      const socket = run.sockets[0]!;
      expect(new URL(socket.url).searchParams.get("clientOrigin")).toBe("https://client.example");
      socket.onopen?.();
      run.ready();
      const response = controlled<Response>();
      run.fetch.mockImplementationOnce(() => response.promise);
      socket.onclose?.({ code, reason: code === 1008 ? "unauthorized" : "admission unavailable" });
      if (code === 1008) {
        expect(run.current()).toMatchObject({ access: null, phase: "denied" });
        expect(run.fetch).toHaveBeenCalledTimes(2);
        expect(run.refreshAccess).toHaveBeenCalledOnce();
        expect(vi.getTimerCount()).toBe(0);
      } else {
        expect(run.current()).toMatchObject({ access: null, kind: "direct", phase: "connecting" });
        expect(run.fetch).toHaveBeenCalledTimes(3);
        expect(vi.getTimerCount()).toBe(1);
        const classified = run.nextRoute();
        response.resolve(new Response(null, { status: 503 }));
        await classified;
        expect(run.current()).toMatchObject({ access: proxy, kind: "proxy" });
        expect(run.refreshAccess).not.toHaveBeenCalled();
      }
      stream.sendTouch("begin", 0.2, 0.8);
      stream.sendKey({ key: "a", code: "KeyA" } as KeyboardEvent, "down");
      expect(socket.send).not.toHaveBeenCalled();
      run.manager.report(access, "video", false, "video also ended");
      expect(run.close).toHaveBeenCalledExactlyOnceWith(tunnel.id);
      run.manager.stop();
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it("stops the old input synchronously and never replays gestures or keys onto proxy", async () => {
    const run = setupStreamRoute();
    await run.manager.start();
    const old = run.attach(run.current().access!);
    const socket = run.sockets[0]!;
    socket.onopen?.();
    run.ready();
    old.sendTouch("begin", 0.2, 0.8);
    old.sendKey({ key: "a", code: "KeyA" } as KeyboardEvent, "down");
    expect(socket.send).toHaveBeenCalledTimes(2);
    const response = controlled<Response>();
    run.fetch.mockImplementationOnce(() => response.promise);
    run.manager.report(run.current().access!, "video", false, "video ended");
    expect(socket.close).toHaveBeenCalledOnce();
    expect(run.current().access).toBeNull();
    old.sendTouch("move", 0.4, 0.6);
    old.sendTouch("end", 0.4, 0.6);
    old.sendKey({ key: "b", code: "KeyB" } as KeyboardEvent, "down");
    expect(socket.send).toHaveBeenCalledTimes(2);
    const classified = run.nextRoute();
    response.resolve(new Response(null, { status: 503 }));
    await classified;
    const replacement = run.attach(run.current().access!);
    const proxySocket = run.sockets[1]!;
    proxySocket.onopen?.();
    expect(run.sockets).toHaveLength(2);
    expect(new URL(proxySocket.url).searchParams.get("wsTicket")).toBe("vps-ticket");
    expect(new URL(proxySocket.url).searchParams.has("grant")).toBe(false);
    expect(proxySocket.send).not.toHaveBeenCalled();
    old.sendTouch("end", 0.4, 0.6);
    old.sendKey({ key: "c", code: "KeyC" } as KeyboardEvent, "down");
    expect(proxySocket.send).not.toHaveBeenCalled();
    replacement.sendTouch("begin", 0.1, 0.9);
    expect(proxySocket.send.mock.calls).toEqual([
      [JSON.stringify({ type: "touch", action: "down", x: 0.1, y: 0.9 })],
    ]);
    expect(run.open).toHaveBeenCalledOnce();
    expect(run.close).toHaveBeenCalledExactlyOnceWith(tunnel.id);
    run.manager.stop();
    expect(socket.close).toHaveBeenCalledOnce();
    expect(proxySocket.close).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([401, 403])(
    "refreshes current VPS access on denial %s without opening a proxy route",
    async (status) => {
      const run = setup({ fetch: async () => new Response(null, { status }) });
      await run.manager.start();
      expect(run.current()).toMatchObject({ access: null, phase: "denied" });
      expect(run.routes.some((route) => route.access === proxy)).toBe(false);
      expect(run.refreshAccess).toHaveBeenCalledOnce();
      expect(run.open).not.toHaveBeenCalled();
      run.manager.stop();
    },
  );

  it("refreshes an expired grant before opening any tunnel", async () => {
    const run = setup({ fetch: async () => Response.json({ ...grant, expiresAt: 0 }) });
    await run.manager.start();
    expect(run.current()).toMatchObject({ access: null, phase: "denied" });
    expect(run.open).not.toHaveBeenCalled();
    expect(run.refreshAccess).toHaveBeenCalledOnce();
    run.manager.stop();
  });

  it("bounds an unresolved authenticated request and ignores its late response", async () => {
    let finish!: (value: Response) => void;
    const run = setup({
      fetch: () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    });
    const started = run.manager.start();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(run.current()).toMatchObject({ access: proxy, kind: "proxy" });
    finish(Response.json(grant));
    await started;
    expect(run.open).not.toHaveBeenCalled();
    run.manager.stop();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("closes a tunnel and refreshes access when the direct probe is denied", async () => {
    const run = setup({
      fetch: async (url) =>
        String(url).includes("/direct-access")
          ? Response.json(grant)
          : new Response(null, { status: 403 }),
    });
    await run.manager.start();
    expect(run.current().phase).toBe("denied");
    expect(run.close).toHaveBeenCalledWith(tunnel.id);
    expect(run.refreshAccess).toHaveBeenCalledOnce();
    run.manager.stop();
  });

  it("does not downgrade explicit stream authorization denial to proxy fallback", async () => {
    const run = setup();
    await run.manager.start();
    const access = run.current().access!;
    run.ready();
    run.manager.unauthorized(access);
    run.manager.report(access, "input", false, "unauthorized");
    expect(run.current()).toMatchObject({ access: null, phase: "denied" });
    expect(run.refreshAccess).toHaveBeenCalledOnce();
    run.manager.stop();
  });

  it.each([
    { owner: "another-owner", generation: grant.generation },
    { owner: grant.owner, generation: "another-generation" },
  ])("rejects a host whose owner or generation changed", async (identity) => {
    const run = setup({
      fetch: async (url) =>
        String(url).includes("/direct-access") ? Response.json(grant) : Response.json(identity),
    });
    await run.manager.start();
    expect(run.current()).toMatchObject({ access: proxy, kind: "proxy" });
    expect(run.close).toHaveBeenCalledWith(tunnel.id);
    run.manager.stop();
  });

  it("falls back on unreachable direct transport and retries with bounded backoff", async () => {
    const run = setup({
      fetch: async () => {
        throw new Error("network unavailable");
      },
    });
    await run.manager.start();
    expect(run.current().access).toBe(proxy);
    run.ready();
    await vi.advanceTimersByTimeAsync(4_999);
    expect(run.fetch).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(run.fetch).toHaveBeenCalledTimes(2);
    expect(run.current().phase).toBe("connected");
    expect(run.order.filter((event) => event === "retire")).toHaveLength(1);
    run.ready();
    await vi.advanceTimersByTimeAsync(9_999);
    expect(run.fetch).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1);
    expect(run.fetch).toHaveBeenCalledTimes(3);
    run.manager.stop();
  });

  it("uses proxy when the host has no direct configuration and does not retry it", async () => {
    const run = setup({ fetch: async () => new Response(null, { status: 204 }) });
    await run.manager.start();
    expect(run.current()).toMatchObject({ kind: "proxy", access: proxy });
    await vi.advanceTimersByTimeAsync(120_000);
    expect(run.fetch).toHaveBeenCalledOnce();
    expect(run.open).not.toHaveBeenCalled();
    run.manager.stop();
  });

  it("times out pending fetch and closes a tunnel that opens after fallback", async () => {
    let finish!: (value: typeof tunnel) => void;
    let notify!: () => void;
    const opened = new Promise<void>((resolve) => {
      notify = resolve;
    });
    const run = setup({
      open: () =>
        new Promise((resolve) => {
          finish = resolve;
          notify();
        }),
    });
    const started = run.manager.start();
    await opened;
    await vi.advanceTimersByTimeAsync(5_000);
    expect(run.current().access).toBe(proxy);
    finish(tunnel);
    await started;
    expect(run.close).toHaveBeenCalledWith(tunnel.id);
    expect(run.routes.some((route) => route.kind === "direct")).toBe(false);
    run.manager.stop();
  });

  it("closes a late tunnel after hiding the panel without publishing stale access", async () => {
    let finish!: (value: typeof tunnel) => void;
    let notify!: () => void;
    const opened = new Promise<void>((resolve) => {
      notify = resolve;
    });
    const run = setup({
      open: () =>
        new Promise((resolve) => {
          finish = resolve;
          notify();
        }),
    });
    const started = run.manager.start();
    await opened;
    run.manager.stop();
    finish(tunnel);
    await started;
    expect(run.routes).toHaveLength(0);
    expect(run.close).toHaveBeenCalledWith(tunnel.id);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("renews grants and retires the old route before closing the previous tunnel", async () => {
    let leases = 0;
    const run = setup({
      open: async () => ({ ...tunnel, id: `tunnel-${++leases}` }),
      fetch: async (url) =>
        String(url).includes("/direct-access")
          ? // @effect-diagnostics-next-line globalDate:off -- Renewal advances with the same fake clock as the route manager.
            Response.json({ ...grant, expiresAt: Date.now() + 300_000 })
          : Response.json({ owner: grant.owner, generation: grant.generation }),
    });
    await run.manager.start();
    run.ready();
    const old = run.current().access!;
    await vi.advanceTimersByTimeAsync(285_000);
    expect(run.open).toHaveBeenCalledTimes(2);
    expect(run.current().access).not.toBe(old);
    expect(run.current().kind).toBe("direct");
    expect(run.order.slice(-2)).toEqual(["retire", "close:tunnel-1"]);
    run.manager.stop();
    expect(run.close).toHaveBeenLastCalledWith("tunnel-2");
  });
});
