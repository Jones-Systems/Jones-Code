import { describe, expect, it } from "vite-plus/test";
import {
  authorizeMonitorSender,
  monitorBackend,
  monitorBounds,
  monitorVertical,
  readMonitorPreferences,
} from "./policy.ts";

describe("native monitor placement and availability", () => {
  it("defaults to visible while preserving explicit false and only safe geometry", () => {
    expect(readMonitorPreferences(undefined)).toEqual({
      enabled: true,
      displayId: null,
      vertical: 0.5,
    });
    expect(
      readMonitorPreferences({ enabled: false, displayId: 4, vertical: 2, draft: "private" }),
    ).toEqual({ enabled: false, displayId: 4, vertical: 1 });
    expect(
      readMonitorPreferences({ enabled: "false", displayId: Infinity, vertical: NaN }),
    ).toEqual({ enabled: true, displayId: null, vertical: 0.5 });
  });
  it("shrinks actual bounds, fits small displays, and keeps the right edge across scale/workarea changes", () => {
    const display = { x: -1920, y: 40, width: 1920, height: 1040 };
    expect(monitorBounds(display, false, 0.5)).toEqual({ x: -46, y: 496, width: 46, height: 128 });
    const expanded = monitorBounds(display, true, 1)!;
    expect(expanded).toEqual({ x: -384, y: 376, width: 384, height: 704 });
    expect(monitorVertical(expanded, display)).toBe(1);
    expect(monitorBounds({ x: 0, y: 0, width: 320, height: 480 }, true, 0.5)).toEqual({
      x: 0,
      y: 0,
      width: 320,
      height: 480,
    });
    expect(monitorBounds({ x: 0, y: 0, width: 319, height: 900 }, false, 0.5)).toBeNull();
    expect(monitorBounds({ x: 0, y: 0, width: 1920, height: 100 }, true, 0.5)).toBeNull();
  });
  it("supports explicit X11 but fails closed for native or ambiguous Wayland", () => {
    const linux = {
      platform: "linux",
      ozonePlatform: "",
      display: ":99",
      waylandDisplay: undefined,
      sessionType: "x11",
    };
    expect(monitorBackend(linux).supported).toBe(true);
    expect(monitorBackend({ ...linux, waylandDisplay: "wayland-0" }).supported).toBe(false);
    expect(monitorBackend({ ...linux, sessionType: "wayland" }).supported).toBe(false);
    expect(monitorBackend({ ...linux, ozonePlatform: "wayland" }).supported).toBe(false);
    expect(
      monitorBackend({ ...linux, ozonePlatform: "x11", waylandDisplay: "wayland-0" }).supported,
    ).toBe(true);
    expect(monitorBackend({ ...linux, ozonePlatform: "auto" }).supported).toBe(false);
    expect(monitorBackend({ ...linux, display: undefined }).supported).toBe(false);
  });
});

describe("monitor sender authorization", () => {
  it("requires the exact current webContents, main frame, ephemeral session and local entry together", () => {
    const sender = {},
      frame = {},
      session = {};
    const evidence = {
      sender,
      expectedSender: sender,
      frame,
      mainFrame: frame,
      session,
      expectedSession: session,
      frameUrl: "t3code://app/thread-monitor.html",
      contentsUrl: "t3code://app/thread-monitor.html",
      expectedOrigin: "t3code://app",
      expectedEntry: "/thread-monitor.html",
      destroyed: false,
    };
    expect(authorizeMonitorSender(evidence)).toBe(true);
    for (const changes of [
      { sender: {} },
      { frame: {} },
      { session: {} },
      { destroyed: true },
      { frameUrl: "t3code://app/" },
      { frameUrl: "t3code://other/thread-monitor.html" },
      { frameUrl: "https://app/thread-monitor.html" },
      { contentsUrl: "t3code://app/" },
      {
        frameUrl: "t3code://app/thread-monitor.html?command=run",
        contentsUrl: "t3code://app/thread-monitor.html?command=run",
      },
      {
        frameUrl: "t3code://user:password@app/thread-monitor.html",
        contentsUrl: "t3code://user:password@app/thread-monitor.html",
      },
    ])
      expect(authorizeMonitorSender({ ...evidence, ...changes })).toBe(false);
  });
});
