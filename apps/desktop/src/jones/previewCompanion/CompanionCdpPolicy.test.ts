import { describe, expect, it, vi } from "vite-plus/test";
import { createCdpRelayConnection, type CdpRelayTarget } from "../../preview/CdpRelay.ts";
import { companionCdpPolicy, isCompanionNavigationAllowed } from "./CompanionCdpPolicy.ts";

const makeRelay = (restricted = true) => {
  const send = vi.fn<CdpRelayTarget["send"]>(async () => ({}));
  const setDownloadDirectory = vi.fn();
  const replies: Array<Record<string, unknown>> = [];
  const relay = createCdpRelayConnection(
    {
      send,
      setDownloadDirectory,
      targetId: async () => "guest",
      url: () => "https://example.test",
      title: () => "Fixture",
      userAgent: () => "Electron",
    },
    (raw) => replies.push(JSON.parse(raw)),
    restricted ? companionCdpPolicy : undefined,
  );
  return { relay, send, setDownloadDirectory, replies };
};
const settle = () => new Promise<void>((resolve) => setImmediate(resolve));

describe("companion CDP policy", () => {
  it.each([
    ["DOM.setFileInputFiles", { files: ["/local/secret"] }],
    ["DOM.getFileInfo", { objectId: "file" }],
    ["Input.dispatchDragEvent", { data: { files: ["/local/secret"] } }],
    ["Input.dispatchDragEvent", { data: { files: "/local/secret" } }],
    ["Input.setInterceptDrags", {}],
    ["Page.setDownloadBehavior", {}],
    ["IO.read", {}],
    ["Tracing.start", {}],
    ["HeapProfiler.takeHeapSnapshot", {}],
    ["Memory.getDOMCounters", {}],
    ["Debugger.enable", {}],
    ["Profiler.enable", {}],
    ["SystemInfo.getInfo", {}],
    ["Target.createTarget", {}],
    ["Target.sendMessageToTarget", {}],
    ["Storage.getCookies", {}],
    ["FileSystem.getDirectory", {}],
    ["Unknown.method", {}],
  ])("blocks %s before every debugger route", async (method, params) => {
    const { relay, send, replies } = makeRelay();
    for (const sessionId of [undefined, "t3-preview-page", "child-session"]) {
      relay.receive(JSON.stringify({ id: 1, method, params, sessionId }));
    }
    await settle();
    expect(send).not.toHaveBeenCalled();
    expect(replies).toHaveLength(3);
    for (const reply of replies)
      expect(reply).toMatchObject({
        id: 1,
        error: { code: -32000, message: expect.stringContaining(String(method)) },
      });
  });

  it.each([
    "file:///local/secret",
    "chrome://settings",
    "javascript:alert(1)",
    "data:text/plain,no",
    "about:config",
    "about:blank#fragment",
    "relative/path",
    "",
  ])("blocks navigation to %s", async (url) => {
    const { relay, send, replies } = makeRelay();
    relay.receive(
      JSON.stringify({ id: 1, method: "Page.navigate", params: { url }, sessionId: "child" }),
    );
    await settle();
    expect(send).not.toHaveBeenCalled();
    expect(replies[0]).toHaveProperty("error");
    expect(isCompanionNavigationAllowed(url)).toBe(false);
  });

  it.each(["http://localhost:3000", "https://example.test", "about:blank"])(
    "forwards permitted navigation to %s",
    async (url) => {
      const { relay, send } = makeRelay();
      relay.receive(
        JSON.stringify({ id: 1, method: "Page.navigate", params: { url }, sessionId: "child" }),
      );
      await settle();
      expect(send).toHaveBeenCalledWith("Page.navigate", { url }, "child");
    },
  );

  it("rewrites downloads on root, page and child routes without a local directory", async () => {
    const { relay, send, setDownloadDirectory } = makeRelay();
    for (const sessionId of [undefined, "t3-preview-page", "child"])
      relay.receive(
        JSON.stringify({
          id: 1,
          method: "Browser.setDownloadBehavior",
          sessionId,
          params: {
            behavior: "allowAndName",
            downloadPath: "/local/secret",
            browserContextId: "other",
          },
        }),
      );
    await settle();
    expect(send.mock.calls).toEqual([
      ["Browser.setDownloadBehavior", { behavior: "deny" }, undefined],
      ["Browser.setDownloadBehavior", { behavior: "deny" }, undefined],
      ["Browser.setDownloadBehavior", { behavior: "deny" }, "child"],
    ]);
    expect(setDownloadDirectory.mock.calls).toEqual([[null]]);
  });

  it("does not let a page route tunnel a browser-wide query or permission grant", async () => {
    const { relay, send, replies } = makeRelay();
    for (const method of ["Target.getTargets", "Target.attachToTarget", "Browser.grantPermissions"])
      relay.receive(JSON.stringify({ id: 1, method, sessionId: "child" }));
    await settle();
    expect(send).not.toHaveBeenCalled();
    expect(replies.every((reply) => "error" in reply)).toBe(true);
  });

  it.each([
    "Network.loadNetworkResource",
    "Fetch.continueRequest",
    "Network.continueInterceptedRequest",
  ])("does not let %s fetch a host-local file", async (method) => {
    const { relay, send, replies } = makeRelay();
    relay.receive(
      JSON.stringify({
        id: 1,
        method,
        params: { url: "file:///local/secret" },
        sessionId: "child",
      }),
    );
    relay.receive(
      JSON.stringify({
        id: 2,
        method,
        params: { url: "https://example.test/fixture" },
        sessionId: "child",
      }),
    );
    await settle();
    expect(replies.find((reply) => reply.id === 1)).toHaveProperty("error");
    expect(send.mock.calls).toEqual([[method, { url: "https://example.test/fixture" }, "child"]]);
  });

  it("creates Playwright's isolated world without universal origin access", async () => {
    const { relay, send } = makeRelay();
    relay.receive(
      JSON.stringify({
        id: 1,
        method: "Page.createIsolatedWorld",
        sessionId: "child",
        params: {
          frameId: "frame",
          worldName: "utility",
          grantUniveralAccess: true,
          grantUniversalAccess: true,
        },
      }),
    );
    await settle();
    expect(send).toHaveBeenCalledWith(
      "Page.createIsolatedWorld",
      {
        frameId: "frame",
        worldName: "utility",
        grantUniveralAccess: false,
      },
      "child",
    );
  });

  it("allows relay handshake and only the owned page attachment", async () => {
    const { relay, replies } = makeRelay();
    relay.receive(JSON.stringify({ id: 1, method: "Target.setAutoAttach" }));
    relay.receive(
      JSON.stringify({ id: 2, method: "Target.attachToTarget", params: { targetId: "guest" } }),
    );
    relay.receive(
      JSON.stringify({ id: 3, method: "Target.attachToTarget", params: { targetId: "other" } }),
    );
    await settle();
    expect(replies.find((reply) => reply.id === 1)).toHaveProperty("result");
    expect(replies.find((reply) => reply.id === 2)).toHaveProperty("result");
    expect(replies.find((reply) => reply.id === 3)).toHaveProperty("error");
  });

  it("allows page automation and child auto-attach required by the server", async () => {
    const { relay, send } = makeRelay();
    const methods = [
      "Page.enable",
      "Page.captureScreenshot",
      "Page.getLayoutMetrics",
      "Page.getNavigationHistory",
      "Page.reload",
      "Page.startScreencast",
      "Page.stopScreencast",
      "Page.screencastFrameAck",
      "Runtime.evaluate",
      "Runtime.callFunctionOn",
      "Runtime.runIfWaitingForDebugger",
      "DOM.enable",
      "Input.insertText",
      "Input.dispatchMouseEvent",
      "Input.dispatchKeyEvent",
      "Network.clearBrowserCache",
      "Network.clearBrowserCookies",
      "Network.enable",
      "Fetch.enable",
      "Emulation.setDeviceMetricsOverride",
      "Log.enable",
      "CSS.enable",
      "Accessibility.getFullAXTree",
      "Overlay.enable",
      "Performance.getMetrics",
      "Security.setIgnoreCertificateErrors",
      "Storage.clearDataForOrigin",
      "Target.setAutoAttach",
    ];
    for (const method of methods)
      relay.receive(JSON.stringify({ id: 1, method, sessionId: "child" }));
    await settle();
    expect(send.mock.calls.map((call) => call[0])).toEqual(methods);
  });

  it("allows text-only drag data and leaves an ordinary relay unrestricted", async () => {
    const restricted = makeRelay();
    restricted.relay.receive(
      JSON.stringify({
        id: 1,
        method: "Input.dispatchDragEvent",
        params: { data: { files: [], items: [{ mimeType: "text/plain", data: "fixture" }] } },
        sessionId: "child",
      }),
    );
    const ordinary = makeRelay(false);
    ordinary.relay.receive(
      JSON.stringify({
        id: 2,
        method: "DOM.setFileInputFiles",
        params: { files: ["/local/fixture"] },
        sessionId: "t3-preview-page",
      }),
    );
    await settle();
    expect(restricted.send).toHaveBeenCalledTimes(1);
    expect(ordinary.send).toHaveBeenCalledWith(
      "DOM.setFileInputFiles",
      { files: ["/local/fixture"] },
      undefined,
    );
  });
});
