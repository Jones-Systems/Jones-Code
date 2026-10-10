import type { DesktopBridge } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { afterAll, beforeAll, describe, expect, it, vi } from "vite-plus/test";
import { UPDATE_INSTALL_CHANNEL } from "../../ipc/channels.ts";
import { installUpdate } from "../../ipc/methods/updates.ts";
import * as DesktopUpdates from "../../updates/DesktopUpdates.ts";
import { createInitialDesktopUpdateState } from "../../updates/updateMachine.ts";

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
vi.mock("electron-updater", () => ({ autoUpdater: {} }));

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

describe("desktop install IPC decoding", () => {
  function harness() {
    const state = createInitialDesktopUpdateState(
      "1.0.0",
      { hostArch: "arm64", appArch: "arm64", runningUnderArm64Translation: false },
      "stable",
    );
    const result = { accepted: true, completed: false, state };
    const installStaged = vi.fn(() => Effect.succeed(result));
    const install = vi.fn(() => result);
    const service = DesktopUpdates.DesktopUpdates.of({
      getState: Effect.succeed(state),
      isActionActive: Effect.succeed(false),
      isInstallActive: Effect.succeed(false),
      subscribe: Effect.succeed({ latest: state, changes: Stream.empty }),
      emitState: Effect.void,
      disabledReason: Effect.succeed(Option.none()),
      configure: Effect.void,
      setChannel: () => Effect.succeed(state),
      check: () => Effect.succeed({ checked: true, state }),
      download: Effect.succeed(result),
      install: Effect.sync(install),
      installStaged,
      installPrepared: () => Effect.succeed({ ...result, failed: false }),
    });
    const invoke = (payload: unknown) =>
      Effect.runPromise(
        installUpdate.handler(payload).pipe(
          Effect.provideService(DesktopUpdates.DesktopUpdates, service),
        ),
      );
    return { invoke, installStaged, install };
  }

  it("routes the preload handle through the real IPC decoder to the staged installer", async () => {
    const { invoke, installStaged, install } = harness();
    mocks.invoke.mockImplementationOnce((_channel, payload) => invoke(payload));
    await bridge.installUpdate("staged-build-123");
    expect(installStaged).toHaveBeenCalledExactlyOnceWith("staged-build-123");
    expect(install).not.toHaveBeenCalled();
  });

  it("routes the no-argument preload request to the upstream installer", async () => {
    const { invoke, installStaged, install } = harness();
    mocks.invoke.mockImplementationOnce((_channel, payload) => invoke(payload));
    await bridge.installUpdate();
    expect(install).toHaveBeenCalledOnce();
    expect(installStaged).not.toHaveBeenCalled();
  });

  it("rejects malformed install payloads before either installer runs", async () => {
    const { invoke, installStaged, install } = harness();
    await expect(invoke(123)).rejects.toThrow();
    expect(install).not.toHaveBeenCalled();
    expect(installStaged).not.toHaveBeenCalled();
  });
});
