import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as NetService from "@t3tools/shared/Net";
import { DesktopCompanionConfig } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as DesktopApp from "../../app/DesktopApp.ts";
import * as DesktopAppIdentity from "../../app/DesktopAppIdentity.ts";
import * as DesktopAppActivation from "../../app/DesktopAppActivation.ts";
import * as DesktopEnvironment from "../../app/DesktopEnvironment.ts";
import * as DesktopLegacyLocalStorage from "../../app/DesktopLegacyLocalStorage.ts";
import * as DesktopState from "../../app/DesktopState.ts";
import * as DesktopConfig from "../../app/DesktopConfig.ts";
import * as DesktopShutdown from "../../app/DesktopShutdown.ts";
import * as ElectronApp from "../../electron/ElectronApp.ts";
import * as ElectronTheme from "../../electron/ElectronTheme.ts";
import * as DesktopLifecycle from "../../app/DesktopLifecycle.ts";
import * as DesktopWindow from "../../window/DesktopWindow.ts";
import * as DesktopSnapShot from "../../snapShot/DesktopSnapShot.ts";
import * as DesktopAppSettings from "../../settings/DesktopAppSettings.ts";
import * as ElectronProtocol from "../../electron/ElectronProtocol.ts";
import * as DesktopBackendPool from "../../backend/DesktopBackendPool.ts";
import * as DesktopBackendManager from "../../backend/DesktopBackendManager.ts";
import * as DesktopServerExposure from "../../backend/DesktopServerExposure.ts";
import * as DesktopWslBackend from "../../wsl/DesktopWslBackend.ts";
import { setLocalEnvironmentEnabled } from "../../ipc/methods/localEnvironment.ts";
import * as CompanionConfig from "./CompanionConfig.ts";

const config = Schema.decodeUnknownSync(DesktopCompanionConfig)({
  enabled: true,
  environmentId: "env",
  hostId: "mini",
  label: "Mini",
  browserOnly: true,
});
function fixture(companion = config, localEnabled = true) {
  const counts = {
    backend: 0,
    port: 0,
    exposure: 0,
    appActivation: 0,
    window: 0,
    relaunch: 0,
    snapshot: 0,
  };
  const count = (key: keyof typeof counts) =>
    Effect.sync(() => {
      counts[key]++;
    });
  const backend: DesktopBackendManager.DesktopBackendInstance = {
    id: DesktopBackendManager.PRIMARY_INSTANCE_ID,
    label: Effect.succeed("fixture"),
    start: count("backend"),
    stop: () => Effect.die("Unexpected backend stop"),
    currentConfig: Effect.die("Unexpected backend configuration read"),
    snapshot: Effect.die("Unexpected backend snapshot read"),
    waitForReady: () => Effect.die("Unexpected backend readiness wait"),
  };
  const layer = Layer.mergeAll(
    DesktopState.layer,
    DesktopShutdown.layer,
    Layer.mock(ElectronApp.ElectronApp, {}),
    Layer.mock(ElectronTheme.ElectronTheme, {}),
    DesktopAppSettings.layerTest({
      ...DesktopAppSettings.DEFAULT_DESKTOP_SETTINGS,
      localEnvironmentEnabled: localEnabled,
    }),
    Layer.succeed(CompanionConfig.CompanionConfig, {
      get: Effect.succeed(companion),
      set: () => Effect.succeed(companion),
    }),
    Layer.succeed(DesktopAppIdentity.DesktopAppIdentity, {
      resolveUserDataPath: Effect.succeed("/fixture/companion"),
      configure: Effect.void,
      previewAutomationRuntimeIdentity: Effect.succeed({
        schemaVersion: 1,
        runtimeKind: "electron",
        runtimeInstanceId: "runtime",
        appVersion: "1",
        buildCommit: null,
      } as const),
    }),
    DesktopEnvironment.layer({
      dirname: "/fixture/apps/desktop/dist-electron",
      homeDirectory: "/fixture/home",
      platform: "darwin",
      processArch: "arm64",
      appVersion: "1.0.0",
      appPath: "/fixture/app",
      isPackaged: true,
      resourcesPath: "/fixture/resources",
      runningUnderArm64Translation: false,
    }).pipe(Layer.provide(Layer.mergeAll(Path.layer, DesktopConfig.layerTest({})))),
    // Native effects are counted; unexpected mock members fail rather than silently succeeding.
    Layer.mock(DesktopWindow.DesktopWindow, {
      createMainIfBackendReady: count("window"),
    }),
    Layer.mock(DesktopSnapShot.DesktopSnapShot, {
      initialize: count("snapshot"),
    }),
    Layer.mock(DesktopAppActivation.DesktopAppActivation, {
      start: count("appActivation"),
    }),
    Layer.mock(ElectronProtocol.ElectronProtocol, {
      registerDesktopProtocol: () => Effect.void,
    }),
    Layer.mock(DesktopLegacyLocalStorage.DesktopLegacyLocalStorage, {
      load: () => Effect.void,
    }),
    DesktopBackendPool.layerTest([backend]),
    Layer.mock(NetService.NetService, {
      canListenOnHost: () => count("port").pipe(Effect.as(true)),
    }),
    Layer.mock(DesktopServerExposure.DesktopServerExposure, {
      configureFromSettings: () =>
        count("exposure").pipe(
          Effect.as({
            mode: "local-only" as const,
            endpointUrl: null,
            advertisedHost: null,
            tailscaleServeEnabled: false,
            tailscaleServePort: 443,
          }),
        ),
      backendConfig: Effect.succeed({
        port: 3773,
        bindHost: "127.0.0.1",
        httpBaseUrl: new URL("http://127.0.0.1:3773"),
        tailscaleServeEnabled: false,
        tailscaleServePort: 443,
      }),
    }),
    Layer.succeed(DesktopWslBackend.DesktopWslBackend, {
      reconcile: Effect.void,
      lastPreflightError: Effect.succeedNone,
    }),
    Layer.succeed(DesktopLifecycle.DesktopLifecycle, {
      relaunch: () => count("relaunch"),
      register: Effect.void,
    }),
  );
  // Production and tests run the same startup body; IPC registration has independent sender tests.
  const bootstrap = DesktopApp.bootstrapWithIpcRegistration(Effect.void);
  const reenable = setLocalEnvironmentEnabled.handler(true);
  return { counts, layer, bootstrap, reenable };
}

describe("browser-only actual desktop bootstrap", () => {
  it.effect(
    "overrides saved local=true before port allocation, backend spawn, and app-control listener",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        yield* f.bootstrap.pipe(Effect.provide(f.layer), Effect.scoped);
        expect(f.counts).toEqual({
          backend: 0,
          port: 0,
          exposure: 0,
          appActivation: 0,
          window: 1,
          relaunch: 0,
          snapshot: 1,
        });
      }),
  );

  it.effect("keeps an unpaired browser-only host from starting a backend or listener", () =>
    Effect.gen(function* () {
      const f = fixture({ ...config, enabled: false, environmentId: null });
      yield* f.bootstrap.pipe(Effect.provide(f.layer), Effect.scoped);
      expect(f.counts).toEqual({
        backend: 0,
        port: 0,
        exposure: 0,
        appActivation: 0,
        window: 1,
        relaunch: 0,
        snapshot: 1,
      });
    }),
  );

  it.effect("rejects real IPC enablement for an unpaired browser-only host", () =>
    Effect.gen(function* () {
      const f = fixture({ ...config, enabled: false, environmentId: null }, false);
      yield* Effect.gen(function* () {
        const attempted = yield* Effect.exit(f.reenable);
        expect(Exit.isFailure(attempted)).toBe(true);
        if (Exit.isFailure(attempted)) {
          expect(Cause.pretty(attempted.cause)).toContain(
            "Disable browser-only companion mode before enabling a local environment.",
          );
        }
        expect(
          (yield* (yield* DesktopAppSettings.DesktopAppSettings).get).localEnvironmentEnabled,
        ).toBe(false);
        expect(f.counts.relaunch).toBe(0);
      }).pipe(Effect.provide(f.layer), Effect.scoped);
    }),
  );

  it.effect("rejects IPC reenable before a browser-only host can race WSL reconciliation", () =>
    Effect.gen(function* () {
      const f = fixture();
      yield* Effect.gen(function* () {
        yield* f.bootstrap;
        const attempted = yield* Effect.exit(f.reenable);
        expect(Exit.isFailure(attempted)).toBe(true);
        if (Exit.isFailure(attempted)) {
          expect(Cause.pretty(attempted.cause)).toContain(
            "Disable browser-only companion mode before enabling a local environment.",
          );
        }
        expect(f.counts.relaunch).toBe(0);
        expect(
          (yield* (yield* DesktopAppSettings.DesktopAppSettings).get).localEnvironmentEnabled,
        ).toBe(false);
        yield* f.bootstrap;
        expect(
          (yield* (yield* DesktopAppSettings.DesktopAppSettings).get).localEnvironmentEnabled,
        ).toBe(false);
      }).pipe(Effect.provide(f.layer), Effect.scoped);
      expect(f.counts.backend).toBe(0);
      expect(f.counts.port).toBe(0);
      expect(f.counts.appActivation).toBe(0);
      expect(f.counts.window).toBe(2);
    }),
  );

  it.effect.each([
    { ...config, enabled: false, environmentId: null, browserOnly: false },
    { ...config, browserOnly: false },
  ])("retains ordinary non-browser-only startup with transport enabled or disabled", (companion) =>
    Effect.gen(function* () {
      const f = fixture(companion);
      yield* f.bootstrap.pipe(Effect.provide(f.layer), Effect.scoped);
      expect(f.counts.backend).toBe(1);
      expect(f.counts.port).toBe(3);
      expect(f.counts.appActivation).toBe(1);
    }),
  );

  it.effect("preserves ordinary explicitly disabled local startup", () =>
    Effect.gen(function* () {
      const f = fixture(
        { ...config, enabled: false, environmentId: null, browserOnly: false },
        false,
      );
      yield* f.bootstrap.pipe(Effect.provide(f.layer), Effect.scoped);
      expect(f.counts.backend).toBe(0);
      expect(f.counts.port).toBe(0);
      expect(f.counts.appActivation).toBe(0);
      expect(f.counts.window).toBe(1);
    }),
  );

  it.effect.each([
    { ...config, enabled: false, environmentId: null, browserOnly: false },
    { ...config, browserOnly: false },
  ])("allows IPC reenable and relaunch outside browser-only mode", (companion) =>
    Effect.gen(function* () {
      const f = fixture(companion, false);
      yield* Effect.gen(function* () {
        yield* f.reenable;
        expect(
          (yield* (yield* DesktopAppSettings.DesktopAppSettings).get).localEnvironmentEnabled,
        ).toBe(true);
      }).pipe(Effect.provide(f.layer), Effect.scoped);
      expect(f.counts.relaunch).toBe(1);
    }),
  );
});
