import type { DesktopBridge } from "@t3tools/contracts";
import { afterAll, beforeAll, describe, expect, it, vi } from "vite-plus/test";
import { UPDATE_INSTALL_CHANNEL } from "../../ipc/channels.ts";

const mocks = vi.hoisted(() => ({
  invoke: vi.fn().mockResolvedValue({ accepted: true }),
  expose: vi.fn(),
}));

vi.mock("electron", () => ({
  contextBridge: { exposeInMainWorld: mocks.expose },
  ipcRenderer: { invoke: mocks.invoke, sendSync: vi.fn() },
  webFrame: {},
  webUtils: {},
}));
vi.mock("@clerk/electron/preload", () => ({ exposeClerkBridge: vi.fn() }));

let bridge: DesktopBridge;
beforeAll(async () => {
  vi.stubGlobal("window", { addEventListener: vi.fn() });
  await import("../../preload.ts");
  bridge = mocks.expose.mock.calls.find(([name]) => name === "desktopBridge")![1];
});
afterAll(() => vi.unstubAllGlobals());

describe("desktop preload install bridge", () => {
  it("preserves the exact Jones staged handle across Electron IPC", async () => {
    await bridge.installUpdate("staged-build-123");
    expect(mocks.invoke).toHaveBeenLastCalledWith(UPDATE_INSTALL_CHANNEL, "staged-build-123");
  });

  it("preserves upstream's no-argument install request", async () => {
    await bridge.installUpdate();
    expect(mocks.invoke).toHaveBeenLastCalledWith(UPDATE_INSTALL_CHANNEL);
  });
});
