import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { createPreviewStreamClient } from "../../preview/serverBrowserStream.ts";
import {
  decodePreviewHostUnavailable,
  previewHostUnavailableMessage,
} from "./streamUnavailable.ts";
class Socket extends EventTarget {
  static readonly OPEN = 1;
  static current: Socket;
  static count = 0;
  readyState = 1;
  binaryType = "";
  constructor(_url: string) {
    super();
    Socket.current = this;
    Socket.count++;
  }
  send() {}
  close() {
    this.readyState = 3;
  }
}
afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
  Socket.count = 0;
});
describe("render host unavailable", () => {
  it.each(["", "not json", "null", "[]"])(
    "stops 4504 before decoding %j and never retries",
    (reason) => {
      vi.useFakeTimers();
      vi.stubGlobal("WebSocket", Socket);
      const onHostUnavailable = vi.fn();
      const onUnauthorized = vi.fn();
      const onGone = vi.fn();
      const client = createPreviewStreamClient(
        {
          access: {
            httpBase: "http://preview.test/",
            wsBase: "ws://preview.test/",
            credentials: true,
            query: {},
          },
          threadId: "thread",
          tabId: "tab",
          maxWidth: 800,
          maxHeight: 600,
        },
        {
          onFrame: vi.fn(),
          onViewport: vi.fn(),
          onConnectedChange: vi.fn(),
          onUnauthorized,
          onGone,
          onHostUnavailable,
        },
      );
      Socket.current.dispatchEvent(Object.assign(new Event("close"), { code: 4504, reason }));
      expect(onHostUnavailable).toHaveBeenCalledWith({
        hostId: null,
        label: "",
        state: "unavailable",
      });
      vi.advanceTimersByTime(60_000);
      expect(Socket.count).toBe(1);
      expect(onUnauthorized).not.toHaveBeenCalled();
      expect(onGone).not.toHaveBeenCalled();
      client.stop();
    },
  );
  it("accepts bounded ASCII identity and any label while normalizing unknown states", () => {
    expect(
      decodePreviewHostUnavailable(
        JSON.stringify({ hostId: "mini_1", label: "A long label 🖥", state: "offline" }),
      ),
    ).toEqual({ hostId: "mini_1", label: "A long label 🖥", state: "offline" });
    expect(
      decodePreviewHostUnavailable(JSON.stringify({ hostId: "é", label: 5, state: "bogus" })),
    ).toEqual({ hostId: null, label: "", state: "unavailable" });
    expect(
      decodePreviewHostUnavailable(JSON.stringify({ hostId: "a".repeat(65) })).hostId,
    ).toBeNull();
    expect(previewHostUnavailableMessage(decodePreviewHostUnavailable(""))).toContain(
      "This tab stays on that host",
    );
  });
  it("reports generic disconnected state to consumers without the optional host callback", () => {
    vi.stubGlobal("WebSocket", Socket);
    const onConnectedChange = vi.fn();
    const client = createPreviewStreamClient(
      {
        access: {
          httpBase: "http://preview.test/",
          wsBase: "ws://preview.test/",
          credentials: true,
          query: {},
        },
        threadId: "thread",
        tabId: "tab",
        maxWidth: 800,
        maxHeight: 600,
      },
      { onFrame: vi.fn(), onViewport: vi.fn(), onConnectedChange, onUnauthorized: vi.fn() },
    );
    Socket.current.dispatchEvent(Object.assign(new Event("close"), { code: 4504, reason: "" }));
    expect(onConnectedChange).toHaveBeenCalledWith(false);
    client.stop();
  });
});
