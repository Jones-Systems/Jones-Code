import type { DeviceStreamClient, DeviceStreamEvents } from "@t3tools/client-runtime/device/stream";
import { EnvironmentId } from "@t3tools/contracts";
import { AtomRegistry } from "effect/reactivity";
import { afterEach, expect, it, vi } from "vite-plus/test";
import {
  deviceStreamScreenAtom,
  deviceStreamSessionAtom,
  type DeviceStreamLifecycle,
} from "../../components/device/deviceStreamSession";

const streams = vi.hoisted(
  () =>
    [] as {
      events: DeviceStreamEvents;
      start: ReturnType<typeof vi.fn>;
      stop: ReturnType<typeof vi.fn>;
    }[],
);
vi.mock("@t3tools/client-runtime/device/stream", () => ({
  createDeviceStreamClient: (_target: unknown, _sink: unknown, events: DeviceStreamEvents) => {
    const stream = { events, start: vi.fn(), stop: vi.fn() };
    streams.push(stream);
    return stream as unknown as DeviceStreamClient;
  },
}));
vi.mock("@t3tools/client-runtime/device/frame", () => ({ createCanvasFrameSink: () => ({}) }));
vi.mock("~/state/device", () => ({ refreshDeviceHubAccess: vi.fn() }));

afterEach(() => {
  streams.length = 0;
  vi.unstubAllGlobals();
});

it("fences callbacks immediately when a route report retires an atom-owned stream", () => {
  vi.stubGlobal("document", { createElement: () => ({ style: {} }) });
  const registry = AtomRegistry.make();
  try {
    let retire = () => {};
    const unbind = vi.fn();
    const report = vi.fn<DeviceStreamLifecycle["report"]>((channel, connected) => {
      if (channel === "input" && !connected) retire();
    });
    const unauthorized = vi.fn();
    const lifecycle: DeviceStreamLifecycle = {
      bind: (stop) => {
        retire = stop;
        return unbind;
      },
      report,
      unauthorized,
    };
    const atom = deviceStreamSessionAtom({
      environmentId: EnvironmentId.make("test"),
      hostId: "host",
      platform: "ios",
      deviceId: "device",
      access: { httpBase: "http://test", wsBase: "ws://test", query: {}, credentials: false },
      viewId: "view",
      lifecycle,
    });
    registry.mount(deviceStreamScreenAtom("view"));
    registry.mount(atom);
    registry.get(atom);
    const stream = streams[0]!;
    expect(stream.start).toHaveBeenCalledOnce();
    stream.events.onStatus("streaming");
    stream.events.onInputConnected(true);
    stream.events.onMjpegFallback("http://current");
    stream.events.onScreen({ width: 400, height: 800, orientation: "portrait" });
    expect(registry.get(deviceStreamScreenAtom("view"))?.width).toBe(400);
    const connected = registry.get(atom);
    expect(connected.state.input.connected).toBe(true);
    stream.events.onInputConnected(false, "lost");
    expect(stream.stop).toHaveBeenCalledOnce();
    expect(registry.get(deviceStreamScreenAtom("view"))).toBeNull();
    const retired = registry.get(atom);
    expect(retired.state).toMatchObject({
      status: "connecting",
      input: { connected: false },
      mjpegUrl: null,
      screen: null,
      phoneUnavailable: false,
    });

    report.mockClear();
    stream.events.onStatus("error", "stale");
    stream.events.onInputConnected(true);
    stream.events.onScreen({ width: 800, height: 400, orientation: "landscape_right" });
    stream.events.onUnauthorized();
    stream.events.onMjpegFallback("http://stale");
    connected.reconnect();
    expect(report).not.toHaveBeenCalled();
    expect(unauthorized).not.toHaveBeenCalled();
    expect(stream.start).toHaveBeenCalledOnce();
    expect(registry.get(atom)).toBe(retired);
    expect(registry.get(deviceStreamScreenAtom("view"))).toBeNull();
    registry.dispose();
    expect(unbind).toHaveBeenCalledOnce();
    expect(stream.stop).toHaveBeenCalledOnce();
  } finally {
    registry.dispose();
  }
});
