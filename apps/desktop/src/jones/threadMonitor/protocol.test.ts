import type { Session } from "electron";
import { describe, expect, it, vi } from "vite-plus/test";
import { monitorCsp, registerMonitorProtocol } from "./protocol.ts";

vi.mock("electron", () => ({ net: { fetch: vi.fn() } }));

function isolatedSession() {
  let filter:
    | ((
        details: { url: string; resourceType: string },
        callback: (result: { cancel: boolean }) => void,
      ) => void)
    | null = null;
  let permission:
    | ((_contents: unknown, _permission: unknown, callback: (allowed: boolean) => void) => void)
    | null = null;
  const session = {
    protocol: { handle: vi.fn(), unhandle: vi.fn() },
    setPermissionRequestHandler: vi.fn((handler: typeof permission) => {
      permission = handler;
    }),
    setPermissionCheckHandler: vi.fn(),
    setDevicePermissionHandler: vi.fn(),
    on: vi.fn(),
    removeListener: vi.fn(),
    webRequest: {
      onBeforeRequest: vi.fn((handler: typeof filter) => {
        filter = handler;
      }),
    },
  };
  return {
    session: session as unknown as Session,
    native: session,
    allowed: (url: string, resourceType: string) => {
      let allowed = true;
      filter?.({ url, resourceType }, (result) => {
        allowed = !result.cancel;
      });
      return allowed;
    },
    requestPermission: () => {
      let allowed = true;
      permission?.({}, "media", (value) => {
        allowed = value;
      });
      return allowed;
    },
  };
}

describe("isolated monitor asset session", () => {
  it("registers only on the peer session and blocks remote requests, frames, alternate entries and permissions", () => {
    const h = isolatedSession();
    const dispose = registerMonitorProtocol(h.session, {
      scheme: "t3code",
      assetDirectory: "/synthetic/assets",
      developmentOrigin: null,
    });
    expect(h.native.protocol.handle).toHaveBeenCalledWith("t3code", expect.any(Function));
    expect(h.allowed("t3code://app/thread-monitor.html", "mainFrame")).toBe(true);
    expect(h.allowed("t3code://app/assets/monitor.js", "script")).toBe(true);
    for (const [url, type] of [
      ["https://example.test/secret", "xhr"],
      ["ws://localhost:1234", "webSocket"],
      ["t3code://app/index.html", "mainFrame"],
      ["t3code://app/thread-monitor.html", "subFrame"],
      ["t3code://other/thread-monitor.html", "mainFrame"],
      ["t3code://app/assets/object", "object"],
    ])
      expect(h.allowed(url!, type!)).toBe(false);
    expect(h.requestPermission()).toBe(false);
    dispose();
    expect(h.native.protocol.unhandle).toHaveBeenCalledWith("t3code");
    expect(h.native.webRequest.onBeforeRequest).toHaveBeenLastCalledWith(null);
    expect(h.native.removeListener).toHaveBeenCalledWith("will-download", expect.any(Function));
  });

  it("rejects arbitrary dev proxies and gives dev no remote connect, frame or object policy", () => {
    const h = isolatedSession();
    for (const url of [
      "https://remote.example/",
      "http://user:password@localhost:5173/",
      "http://localhost:5173/arbitrary/",
    ]) {
      expect(() =>
        registerMonitorProtocol(h.session, {
          scheme: "t3code-dev",
          assetDirectory: "/assets",
          developmentOrigin: new URL(url),
        }),
      ).toThrow();
    }
    expect(h.native.protocol.handle).not.toHaveBeenCalled();
    for (const development of [true, false]) {
      const directives = monitorCsp(development).split("; ");
      expect(directives).toContain("connect-src 'none'");
      expect(directives).toContain("frame-src 'none'");
      expect(directives).toContain("object-src 'none'");
      expect(directives).toContain("form-action 'none'");
    }
  });
});
