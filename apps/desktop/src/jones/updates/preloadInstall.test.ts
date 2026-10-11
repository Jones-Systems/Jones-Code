import { it as effectIt } from "@effect/vitest";
import type { DesktopBridge } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { afterAll, beforeAll, describe, expect, it, vi } from "vite-plus/test";
import { UPDATE_INSTALL_CHANNEL, UPDATE_DOWNLOAD_CHANNEL } from "../../ipc/channels.ts";
import { downloadUpdate, installUpdate } from "../../ipc/methods/updates.ts";
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
  it("forwards the exact Download selection and preserves argument-free upstream requests", async () => {
    const selection = { artifactId: 123, sourceSha: "a".repeat(40) };
    await bridge.downloadUpdate(selection);
    expect(mocks.invoke).toHaveBeenLastCalledWith(UPDATE_DOWNLOAD_CHANNEL, selection);
    await bridge.downloadUpdate();
    expect(mocks.invoke).toHaveBeenLastCalledWith(UPDATE_DOWNLOAD_CHANNEL);
  });

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
  function harness(jones = false) {
    const state = createInitialDesktopUpdateState(
      "1.0.0",
      { hostArch: "arm64", appArch: "arm64", runningUnderArm64Translation: false },
      "latest",
    );
    if (jones) state.jones = {
      source: "jones-actions", channel: "jones-main", phase: "available",
      capability: { check: true, download: true, install: false },
    };
    const result = { accepted: true, completed: false, state };
    const downloadSelected = vi.fn(() => Effect.succeed(result));
    const download = vi.fn(() => result);
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
      download: Effect.sync(download),
      downloadSelected,
      install: Effect.sync(install),
      installStaged,
      installPrepared: () => Effect.succeed({ ...result, failed: false }),
    });
    const invoke = (payload: unknown) =>
      installUpdate
        .handler(payload)
        .pipe(Effect.provideService(DesktopUpdates.DesktopUpdates, service));
    const invokeDownload = (payload: unknown) => downloadUpdate.handler(payload).pipe(
      Effect.provideService(DesktopUpdates.DesktopUpdates, service),
    );
    return { invoke, installStaged, install, invokeDownload, download, downloadSelected };
  }

  effectIt.effect("decodes Download selection and refuses missing Jones selection", () =>
    Effect.gen(function* () {
      const { invokeDownload, download, downloadSelected } = harness(true);
      const selection = { artifactId: 123, sourceSha: "a".repeat(40) };
      yield* invokeDownload(selection);
      expect(downloadSelected).toHaveBeenCalledExactlyOnceWith(selection);
      expect((yield* invokeDownload(undefined)).accepted).toBe(false);
      expect(download).not.toHaveBeenCalled();
      expect(Exit.isFailure(yield* Effect.exit(invokeDownload({ artifactId: "bad" })))).toBe(true);
      expect(downloadSelected).toHaveBeenCalledOnce();
    }),
  );

  effectIt.effect("routes argument-free upstream Download to its existing updater", () =>
    Effect.gen(function* () {
      const { invokeDownload, download, downloadSelected } = harness();
      yield* invokeDownload(undefined);
      expect(download).toHaveBeenCalledOnce();
      expect(downloadSelected).not.toHaveBeenCalled();
    }),
  );

  effectIt.effect("routes the handle through the real IPC decoder to the staged installer", () =>
    Effect.gen(function* () {
      const { invoke, installStaged, install } = harness();
      yield* invoke("staged-build-123");
      expect(installStaged).toHaveBeenCalledExactlyOnceWith("staged-build-123");
      expect(install).not.toHaveBeenCalled();
    }),
  );

  effectIt.effect("routes the no-argument request to the upstream installer", () =>
    Effect.gen(function* () {
      const { invoke, installStaged, install } = harness();
      yield* invoke(undefined);
      expect(install).toHaveBeenCalledOnce();
      expect(installStaged).not.toHaveBeenCalled();
    }),
  );

  effectIt.effect("rejects malformed install payloads before either installer runs", () =>
    Effect.gen(function* () {
      const { invoke, installStaged, install } = harness();
      const exit = yield* Effect.exit(invoke(123));
      expect(Exit.isFailure(exit)).toBe(true);
      expect(install).not.toHaveBeenCalled();
      expect(installStaged).not.toHaveBeenCalled();
    }),
  );
});
