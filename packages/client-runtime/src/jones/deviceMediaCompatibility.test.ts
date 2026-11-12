import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { createDeviceStreamClient } from "../device/stream.ts";

describe("native device stream transport", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  function setup(
    platform: "ios" | "android",
    query: Record<string, string> = { wsTicket: "stream-ticket", hostId: "ssh-host" },
  ) {
    vi.useFakeTimers();
    vi.stubGlobal("VideoDecoder", vi.fn());
    vi.stubGlobal("EncodedVideoChunk", vi.fn());
    let resolveOpened!: (socket: FakeSocket) => void;
    const waitForSocket = () =>
      new Promise<FakeSocket>((resolve) => {
        resolveOpened = resolve;
      });
    const opened = waitForSocket();
    class FakeSocket {
      static OPEN = 1;
      readyState = 1;
      binaryType = "";
      onopen: (() => void) | null = null;
      onmessage: ((event: { data: ArrayBuffer }) => void) | null = null;
      onclose: ((event: { code: number; reason: string }) => void) | null = null;
      send = vi.fn();
      close = vi.fn();
      readonly url: string;
      constructor(url: string) {
        this.url = url;
        resolveOpened(this);
      }
    }
    vi.stubGlobal("WebSocket", FakeSocket);
    const fetch = vi.fn(() => Promise.resolve(new Response("frame")));
    vi.stubGlobal("fetch", fetch);
    const events = {
      onStatus: vi.fn(),
      onScreen: vi.fn(),
      onUnauthorized: vi.fn(),
      onMjpegFallback: vi.fn(),
      onInputConnected: vi.fn(),
    };
    const client = createDeviceStreamClient(
      {
        platform,
        deviceId: "test device",
        preferMjpeg: platform === "ios",
        access: {
          httpBase: "https://environment.test/api/device-hub",
          wsBase: "wss://environment.test/api/device-hub",
          credentials: false,
          query,
        },
      },
      { getContext: () => null } as unknown as HTMLCanvasElement,
      events,
    );
    return { client, opened, waitForSocket, events, fetch };
  }

  it.each(["ios", "android"] as const)(
    "reports %s media grant denial before a disconnect can select fallback and drops later input",
    async (platform) => {
      const { client, opened, events } = setup(platform, { grant: "device-grant", hostId: "mini" });
      client.start();
      const socket = await opened;
      socket.onopen?.();
      socket.send.mockClear();
      events.onInputConnected.mockClear();
      socket.onclose?.({ code: 1008, reason: "unauthorized" });
      expect(events.onUnauthorized).toHaveBeenCalledOnce();
      expect(events.onInputConnected).toHaveBeenCalledWith(false);
      expect(events.onUnauthorized.mock.invocationCallOrder[0]).toBeLessThan(
        events.onInputConnected.mock.invocationCallOrder[0]!,
      );
      client.sendTouch("begin", 0.2, 0.8);
      client.pressButton("home");
      expect(socket.send).not.toHaveBeenCalled();
      expect(vi.getTimerCount()).toBe(0);
      client.stop();
    },
  );
});
