import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { vi } from "vite-plus/test";

const { focusedWebContents, ownerWindow } = vi.hoisted(() => ({
  focusedWebContents: vi.fn(),
  ownerWindow: vi.fn(),
  exposeInMainWorld: vi.fn(),
  invoke: vi.fn(),
}));
vi.mock("electron", () => ({
  webContents: { getFocusedWebContents: focusedWebContents },
  BrowserWindow: { fromWebContents: ownerWindow },
  contextBridge: { exposeInMainWorld },
  ipcRenderer: { invoke },
  webFrame: {},
  webUtils: {},
}));

vi.mock("@clerk/electron/preload", () => ({ exposeClerkBridge: vi.fn() }));

import type { DesktopBridge } from "@t3tools/contracts";
import * as DesktopAppIdentity from "../../app/DesktopAppIdentity.ts";
import * as DesktopIpc from "../DesktopIpc.ts";
import * as IpcChannels from "../channels.ts";
import * as DesktopBackendConfiguration from "../../backend/DesktopBackendConfiguration.ts";
import * as DesktopBackendManager from "../../backend/DesktopBackendManager.ts";
import * as DesktopBackendPool from "../../backend/DesktopBackendPool.ts";
import {
  getLocalEnvironmentBootstraps,
  getRelayTelemetryEnabled,
  probeRemoteEditors,
} from "./window.ts";

const readyWslConfig: DesktopBackendManager.DesktopBackendStartConfig = {
  executablePath: "wsl.exe",
  args: ["-d", "Ubuntu", "--", "node", "/app/bin.mjs"],
  entryPath: "/app/bin.mjs",
  cwd: "/app",
  env: {},
  extendEnv: false,
  bootstrap: {
    mode: "desktop",
    noBrowser: true,
    port: 3774,
    host: "0.0.0.0",
    desktopBootstrapToken: "bootstrap-token",
    tailscaleServeEnabled: false,
    tailscaleServePort: 443,
  },
  bootstrapDelivery: "stdin",
  httpBaseUrl: new URL("http://127.0.0.1:3774"),
  captureOutput: true,
  preflightFailure: Option.none(),
  runningDistro: "Ubuntu",
};

const defaultWslInstance: DesktopBackendManager.DesktopBackendInstance = {
  id: DesktopBackendManager.BackendInstanceId("wsl:default"),
  label: Effect.succeed("WSL (default distro)"),
  start: Effect.void,
  stop: () => Effect.void,
  currentConfig: Effect.succeedSome(readyWslConfig),
  snapshot: Effect.succeed({
    desiredRunning: true,
    ready: true,
    activePid: Option.some(123),
    restartAttempt: 0,
    restartScheduled: false,
  }),
  waitForReady: () => Effect.succeed(true),
};

const backendConfigurationLayer = Layer.succeed(
  DesktopBackendConfiguration.DesktopBackendConfiguration,
  {
    resolvePrimary: Effect.die("unexpected resolvePrimary"),
    resolvePrimaryLabel: Effect.succeed("Windows"),
    resolveWsl: () => Effect.die("unexpected resolveWsl"),
    currentBootstrapToken: Effect.succeed("current-window-token"),
  } satisfies DesktopBackendConfiguration.DesktopBackendConfiguration["Service"],
);

const bootstrapsLayer = (instances: ReadonlyArray<DesktopBackendManager.DesktopBackendInstance>) =>
  Layer.merge(DesktopBackendPool.layerTest([...instances]), backendConfigurationLayer);

describe("getLocalEnvironmentBootstraps", () => {
  it.effect("hands out the current window's token to a backend launched with the secret", () =>
    Effect.gen(function* () {
      const result = yield* getLocalEnvironmentBootstraps.handler();

      assert.deepEqual(result, [
        {
          id: "wsl:default",
          label: "WSL (Ubuntu)",
          runningDistro: "Ubuntu",
          httpBaseUrl: "http://127.0.0.1:3774/",
          wsBaseUrl: "ws://127.0.0.1:3774/",
          bootstrapToken: "current-window-token",
        },
      ]);
    }).pipe(
      Effect.provide(
        bootstrapsLayer([
          {
            ...defaultWslInstance,
            currentConfig: Effect.succeedSome({
              ...readyWslConfig,
              bootstrap: {
                ...readyWslConfig.bootstrap,
                desktopBootstrapSecret: "desktop-secret",
              },
            }),
          },
        ]),
      ),
    ),
  );

  it.effect("publishes a pending bootstrap only while a transient retry is scheduled", () => {
    const retryingConfig: DesktopBackendManager.DesktopBackendStartConfig = {
      ...readyWslConfig,
      preflightFailure: Option.some({
        reason: "WSL probe timed out",
        fatal: false,
        retryLimit: 12,
      }),
    };
    const retryingInstance: DesktopBackendManager.DesktopBackendInstance = {
      ...defaultWslInstance,
      currentConfig: Effect.succeedSome(retryingConfig),
      snapshot: Effect.succeed({
        desiredRunning: true,
        ready: false,
        activePid: Option.none(),
        restartAttempt: 2,
        restartScheduled: true,
      }),
    };

    return Effect.gen(function* () {
      const result = yield* getLocalEnvironmentBootstraps.handler();
      assert.deepEqual(result, [
        {
          id: "wsl:default",
          label: "WSL (default distro)",
          runningDistro: null,
          httpBaseUrl: null,
          wsBaseUrl: null,
        },
      ]);
    }).pipe(Effect.provide(bootstrapsLayer([retryingInstance])));
  });

  it.effect("omits a bounded transient bootstrap after retries stop", () => {
    const stoppedInstance: DesktopBackendManager.DesktopBackendInstance = {
      ...defaultWslInstance,
      currentConfig: Effect.succeedSome({
        ...readyWslConfig,
        preflightFailure: Option.some({
          reason: "WSL probe timed out",
          fatal: false,
          retryLimit: 12,
        }),
      }),
      snapshot: Effect.succeed({
        desiredRunning: false,
        ready: false,
        activePid: Option.none(),
        restartAttempt: 12,
        restartScheduled: false,
      }),
    };

    return Effect.gen(function* () {
      const result = yield* getLocalEnvironmentBootstraps.handler();
      assert.deepEqual(result, []);
    }).pipe(Effect.provide(bootstrapsLayer([stoppedInstance])));
  });
});

describe("getRelayTelemetryEnabled", () => {
  it.effect("reads the desktop host opt-out before returning the renderer policy", () =>
    Effect.gen(function* () {
      assert.isFalse(
        yield* getRelayTelemetryEnabled.handler().pipe(
          Effect.provideService(HostProcess.Environment, {
            T3CODE_TELEMETRY_ENABLED: "false",
          }),
        ),
      );
      assert.isTrue(
        yield* getRelayTelemetryEnabled
          .handler()
          .pipe(Effect.provideService(HostProcess.Environment, {})),
      );
    }),
  );
});

it.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
  "finds remote editors installed without PATH launchers",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3-remote-editors-" });
      for (const app of ["Cursor", "Visual Studio Code", "WebStorm"]) {
        const executable = path.join(
          home,
          "Applications",
          `${app}.app`,
          app === "WebStorm" ? "Contents/MacOS/webstorm" : "Contents/Resources/app/bin/code",
        );
        yield* fs.makeDirectory(path.dirname(executable), { recursive: true });
        yield* fs.writeFileString(executable, "#!/bin/sh\n");
        yield* fs.chmod(executable, 0o755);
      }
      const editors = yield* probeRemoteEditors.handler(undefined).pipe(
        Effect.provideService(HostProcess.Environment, {
          HOME: home,
          PATH: path.join(home, "empty"),
        }),
        Effect.provideService(HostProcess.Platform, "darwin"),
      );
      assert.include(editors, "cursor");
      assert.include(editors, "vscode");
      assert.include(editors, "webstorm");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

describe("getPreviewAutomationRuntimeIdentity", () => {
  const descriptor = {
    schemaVersion: 1,
    runtimeKind: "electron",
    runtimeInstanceId: "synthetic-runtime",
    appVersion: "1.2.3",
    buildCommit: "a".repeat(40),
  } as const;

  it.effect("returns the service descriptor through the registered async IPC method", () =>
    Effect.gen(function* () {
      let listener: DesktopIpc.DesktopIpcHandleListener | undefined;
      const ipc = DesktopIpc.make({
        removeHandler: vi.fn(),
        removeAllListeners: vi.fn(),
        on: vi.fn(),
        handle: (channel, registered) => {
          assert.equal(channel, IpcChannels.GET_PREVIEW_AUTOMATION_RUNTIME_IDENTITY_CHANNEL);
          listener = registered;
        },
      });
      yield* ipc.handle(getPreviewAutomationRuntimeIdentity);
      assert.deepEqual(
        yield* Effect.promise(async () => listener!({ sender: { id: 1 } }, undefined)),
        descriptor,
      );
      const invalidPayload = yield* Effect.exit(
        getPreviewAutomationRuntimeIdentity.handler("unexpected"),
      );
      assert.equal(invalidPayload._tag, "Failure");
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mock(DesktopAppIdentity.DesktopAppIdentity)({
          previewAutomationRuntimeIdentity: Effect.succeed(descriptor),
        }),
      ),
    ),
  );

  it.effect("rejects a descriptor that violates the IPC result schema", () =>
    getPreviewAutomationRuntimeIdentity.handler(undefined).pipe(
      Effect.provide(
        Layer.mock(DesktopAppIdentity.DesktopAppIdentity)({
          previewAutomationRuntimeIdentity: Effect.succeed({
            ...descriptor,
            schemaVersion: 2,
          } as unknown as typeof descriptor),
        }),
      ),
      Effect.exit,
      Effect.tap((exit) => Effect.sync(() => assert.equal(exit._tag, "Failure"))),
    ),
  );

  it("exposes an async preload getter using the identity channel", async () => {
    vi.stubGlobal("window", { addEventListener: vi.fn() });
    try {
      await import("../../preload.ts");
      const bridge = exposeInMainWorld.mock.calls.find(
        ([name]) => name === "desktopBridge",
      )?.[1] as DesktopBridge;
      invoke.mockResolvedValueOnce(descriptor);
      assert.deepEqual(await bridge.getPreviewAutomationRuntimeIdentity!(), descriptor);
      assert.deepEqual(invoke.mock.calls.at(-1), [
        IpcChannels.GET_PREVIEW_AUTOMATION_RUNTIME_IDENTITY_CHANNEL,
      ]);
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
