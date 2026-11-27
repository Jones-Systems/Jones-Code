import { describe, expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NetService from "@t3tools/shared/Net";
import { DesktopCompanionConfig } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as DesktopApp from "../../app/DesktopApp.ts";
import * as DesktopAppIdentity from "../../app/DesktopAppIdentity.ts";
import * as DesktopAppActivation from "../../app/DesktopAppActivation.ts";
import * as DesktopCliCommand from "../../app/DesktopCliCommand.ts";
import * as DesktopEnvironment from "../../app/DesktopEnvironment.ts";
import * as DesktopLegacyLocalStorage from "../../app/DesktopLegacyLocalStorage.ts";
import * as DesktopState from "../../app/DesktopState.ts";
import * as DesktopConfig from "../../app/DesktopConfig.ts";
import * as DesktopShutdown from "../../app/DesktopShutdown.ts";
import * as ElectronApp from "../../electron/ElectronApp.ts";
import * as ElectronTheme from "../../electron/ElectronTheme.ts";
import * as DesktopLifecycle from "../../app/DesktopLifecycle.ts";
import * as DesktopWindow from "../../window/DesktopWindow.ts";
import * as DesktopShellEnvironment from "../../shell/DesktopShellEnvironment.ts";
import * as DesktopSnapShot from "../../snapShot/DesktopSnapShot.ts";
import * as DesktopAppSettings from "../../settings/DesktopAppSettings.ts";
import * as ElectronProtocol from "../../electron/ElectronProtocol.ts";
import * as DesktopBackendPool from "../../backend/DesktopBackendPool.ts";
import * as DesktopBackendManager from "../../backend/DesktopBackendManager.ts";
import * as DesktopBackendRecoveryProcess from "../../backend/DesktopBackendRecoveryProcess.ts";
import * as DesktopHomeOwner from "../../backend/DesktopHomeOwner.ts";
import * as DesktopLocalEnvironmentAuth from "../../backend/DesktopLocalEnvironmentAuth.ts";
import * as DesktopServerExposure from "../../backend/DesktopServerExposure.ts";
import * as DesktopWslBackend from "../../wsl/DesktopWslBackend.ts";
import * as ElectronDialog from "../../electron/ElectronDialog.ts";
import { setLocalEnvironmentEnabled } from "../../ipc/methods/localEnvironment.ts";
import * as CompanionConfig from "./CompanionConfig.ts";
import { afterEach, vi } from "vite-plus/test";

// The home-lock probe is native SQLite/process inspection, independently tested in
// DesktopHomeOwner.test.ts. Every startup fixture supplies an explicit admission result.
vi.mock("../../backend/DesktopHomeOwner.ts", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../backend/DesktopHomeOwner.ts")>()),
  awaitHomeOwnershipRelease: vi.fn(),
  readLiveHomeOwner: vi.fn(),
}));
vi.mock("../../backend/DesktopBackendRecoveryProcess.ts", () => ({ make: vi.fn() }));
vi.mock("electron", () => ({}));
vi.mock("@clerk/electron", () => ({ createClerkBridge: vi.fn() }));
vi.mock("@clerk/electron/storage", () => ({ storage: vi.fn() }));

afterEach(() => vi.clearAllMocks());

const config = Schema.decodeSync(DesktopCompanionConfig)({
  enabled: true,
  environmentId: "env",
  hostId: "mini",
  label: "Mini",
  browserOnly: true,
});
const stateHome = "/fixture/state";
const userData = "/fixture/companion";

function fixture(
  companion = config,
  localEnabled = true,
  options: {
    readonly homeAvailable?: boolean;
    readonly settingsWriteError?: DesktopAppSettings.DesktopSettingsWriteError;
    readonly shellUpdates?: DesktopShellEnvironment.DesktopShellEnvironment["Service"]["updates"];
    readonly backends?: (
      primary: DesktopBackendManager.DesktopBackendInstance,
    ) => readonly DesktopBackendManager.DesktopBackendInstance[];
  } = {},
) {
  const events: string[] = [];
  const launcherPaths: string[] = [];
  const authRequested = Deferred.makeUnsafe<void>();
  const wslRequested = Deferred.makeUnsafe<void>();
  const counts = {
    backend: 0,
    recoverBackend: 0,
    startupRecovery: 0,
    port: 0,
    exposure: 0,
    appActivation: 0,
    window: 0,
    relaunch: 0,
    snapshot: 0,
    homeOwner: 0,
    bearer: 0,
    wsl: 0,
    dialog: 0,
    quit: 0,
  };
  const count = (key: keyof typeof counts) =>
    Effect.sync(() => {
      counts[key]++;
    });
  const record = (event: string) =>
    Effect.sync(() => {
      events.push(event);
    });
  vi.mocked(DesktopHomeOwner.awaitHomeOwnershipRelease).mockImplementation(() =>
    count("homeOwner").pipe(Effect.as(options.homeAvailable !== false)),
  );
  vi.mocked(DesktopHomeOwner.readLiveHomeOwner).mockImplementation(() =>
    Effect.succeedSome({ pid: 1234, port: 3773, serviceManaged: true }),
  );
  vi.mocked(DesktopBackendRecoveryProcess.make).mockReturnValue({
    recover: () => {
      counts.recoverBackend++;
      return Promise.resolve(undefined);
    },
    track: () => Promise.reject(new Error("Unexpected recovery worker tracking")),
  });
  const settingsLayer = Layer.effect(
    DesktopAppSettings.DesktopAppSettings,
    Effect.map(DesktopAppSettings.DesktopAppSettings, (settings) => ({
      ...settings,
      get: record("settings.read").pipe(Effect.andThen(settings.get)),
      setLocalEnvironmentEnabled: (enabled: boolean) =>
        record(`local=${enabled}`).pipe(
          Effect.andThen(
            options.settingsWriteError === undefined
              ? settings.setLocalEnvironmentEnabled(enabled)
              : Effect.fail(options.settingsWriteError),
          ),
        ),
    })),
  ).pipe(
    Layer.provide(
      DesktopAppSettings.layerTest({
        ...DesktopAppSettings.DEFAULT_DESKTOP_SETTINGS,
        localEnvironmentEnabled: localEnabled,
      }),
    ),
  );
  const backend: DesktopBackendManager.DesktopBackendInstance = {
    id: DesktopBackendManager.PRIMARY_INSTANCE_ID,
    label: Effect.succeed("fixture"),
    start: count("backend"),
    stop: () => Effect.die("Unexpected backend stop"),
    currentConfig: companion.browserOnly
      ? Effect.die("Unexpected backend configuration read")
      : Effect.succeedNone,
    snapshot: Effect.die("Unexpected backend snapshot read"),
    waitForReady: () => Effect.die("Unexpected backend readiness wait"),
  };
  const layer = Layer.mergeAll(
    DesktopState.layer,
    DesktopShutdown.layer,
    NodeCrypto.layerMac,
    Layer.mock(ElectronApp.ElectronApp, { quit: count("quit") }),
    Layer.mock(ElectronDialog.ElectronDialog, { showErrorBox: () => count("dialog") }),
    Layer.mock(ElectronTheme.ElectronTheme, {}),
    settingsLayer,
    Layer.succeed(CompanionConfig.CompanionConfig, {
      get: Effect.succeed(companion),
      set: () => Effect.succeed(companion),
    }),
    Layer.succeed(DesktopAppIdentity.DesktopAppIdentity, {
      resolveUserDataPath: Effect.succeed(userData),
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
    }).pipe(
      Layer.provide(
        Layer.mergeAll(
          Path.layer,
          DesktopConfig.layerTest({
            T3CODE_HOME: stateHome,
            T3CODE_DESKTOP_USER_DATA_DIR: userData,
          }),
        ),
      ),
    ),
    // Native effects are counted; unexpected mock members fail rather than silently succeeding.
    Layer.mock(DesktopWindow.DesktopWindow, {
      createMainIfBackendReady: count("window"),
    }),
    Layer.mock(DesktopSnapShot.DesktopSnapShot, {
      initialize: count("snapshot"),
    }),
    // Bootstrap only forks the first-launch CLI install behind shell discovery; no PATH was found here.
    Layer.mock(DesktopCliCommand.DesktopCliCommand, { installOnFirstLaunch: Effect.void }),
    Layer.mock(DesktopShellEnvironment.DesktopShellEnvironment, {
      awaitUserShellDiscovery: Effect.succeed(false),
      updates: options.shellUpdates ?? Stream.empty,
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
    DesktopBackendPool.layerTest(options.backends?.(backend) ?? [backend]),
    Layer.succeed(DesktopLocalEnvironmentAuth.DesktopLocalEnvironmentAuth, {
      getBearerToken: count("bearer").pipe(
        Effect.andThen(Deferred.succeed(authRequested, undefined)),
        Effect.as("synthetic-startup-bearer"),
      ),
    }),
    Layer.mock(NetService.NetService, {
      canListenOnHost: () => count("port").pipe(Effect.as(true)),
    }),
    FileSystem.layerNoop({
      makeDirectory: () => Effect.void,
      writeFileString: () => Effect.void,
      rename: (_from, to) =>
        Effect.sync(() => {
          launcherPaths.push(to);
        }),
    }),
    Path.layer,
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
      reconcile: count("wsl").pipe(Effect.andThen(Deferred.succeed(wslRequested, undefined))),
      lastPreflightError: Effect.succeedNone,
    }),
    Layer.succeed(DesktopLifecycle.DesktopLifecycle, {
      relaunch: () => count("relaunch"),
      register: Effect.void,
    }),
  );
  // Production and tests run the same startup body; IPC registration has independent sender tests.
  const bootstrap = Effect.gen(function* () {
    const backendStarted = yield* DesktopApp.startLocalBackend(
      Effect.void,
      count("startupRecovery"),
    );
    yield* DesktopApp.bootstrap(backendStarted);
    return backendStarted;
  }).pipe(Effect.provideService(HostProcess.Environment, {}));
  const reenable = setLocalEnvironmentEnabled.handler(true);
  return { counts, events, launcherPaths, layer, bootstrap, reenable, authRequested, wslRequested };
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
          recoverBackend: 0,
          startupRecovery: 0,
          port: 0,
          exposure: 0,
          appActivation: 0,
          window: 1,
          relaunch: 0,
          snapshot: 1,
          homeOwner: 0,
          bearer: 0,
          wsl: 0,
          dialog: 0,
          quit: 0,
        });
        expect(DesktopBackendRecoveryProcess.make).not.toHaveBeenCalled();
        expect(f.launcherPaths).toEqual([]);
        expect(f.events.slice(0, 2)).toEqual(["local=false", "settings.read"]);
      }),
  );

  it.effect("keeps an unpaired browser-only host from starting a backend or listener", () =>
    Effect.gen(function* () {
      const f = fixture({ ...config, enabled: false, environmentId: null });
      yield* f.bootstrap.pipe(Effect.provide(f.layer), Effect.scoped);
      expect(f.counts).toEqual({
        backend: 0,
        recoverBackend: 0,
        startupRecovery: 0,
        port: 0,
        exposure: 0,
        appActivation: 0,
        window: 1,
        relaunch: 0,
        snapshot: 1,
        homeOwner: 0,
        bearer: 0,
        wsl: 0,
        dialog: 0,
        quit: 0,
      });
      expect(f.launcherPaths).toEqual([]);
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
      expect(f.counts.recoverBackend).toBe(0);
      expect(f.counts.startupRecovery).toBe(0);
      expect(f.counts.port).toBe(0);
      expect(f.counts.appActivation).toBe(0);
      expect(f.counts.exposure).toBe(0);
      expect(f.counts.homeOwner).toBe(0);
      expect(f.counts.bearer).toBe(0);
      expect(f.counts.wsl).toBe(0);
      expect(f.counts.window).toBe(2);
      expect(f.launcherPaths).toEqual([]);
    }),
  );

  it.effect.each([
    { ...config, enabled: false, environmentId: null, browserOnly: false },
    { ...config, browserOnly: false },
  ])("retains ordinary non-browser-only startup with transport enabled or disabled", (companion) =>
    Effect.gen(function* () {
      const f = fixture(companion);
      yield* Effect.gen(function* () {
        expect(yield* f.bootstrap).toBe(true);
        // Join the observed fork effects before closing the same startup scope.
        yield* Deferred.await(f.authRequested);
        yield* Deferred.await(f.wslRequested);
      }).pipe(Effect.provide(f.layer), Effect.scoped);
      expect(f.counts.backend).toBe(1);
      expect(f.counts.recoverBackend).toBe(1);
      expect(f.counts.startupRecovery).toBe(1);
      expect(DesktopBackendRecoveryProcess.make).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          appDataDirectory: "/fixture/home/Library/Application Support",
          stateDir: `${stateHome}/userdata`,
        }),
      );
      expect(f.counts.port).toBe(3);
      expect(f.counts.exposure).toBe(1);
      expect(f.counts.appActivation).toBe(1);
      expect(f.counts.homeOwner).toBe(1);
      expect(f.counts.bearer).toBe(1);
      expect(f.counts.wsl).toBe(1);
      expect(f.events).toEqual(["settings.read", "settings.read"]);
    }),
  );

  it.effect("refreshes the ordinary CLI launcher without starting a disabled local backend", () =>
    Effect.gen(function* () {
      const f = fixture(
        { ...config, enabled: false, environmentId: null, browserOnly: false },
        false,
      );
      yield* f.bootstrap.pipe(Effect.provide(f.layer), Effect.scoped);
      expect(f.counts.backend).toBe(0);
      expect(f.counts.recoverBackend).toBe(0);
      expect(f.counts.startupRecovery).toBe(0);
      expect(f.counts.port).toBe(0);
      expect(f.counts.appActivation).toBe(1);
      expect(f.counts.homeOwner).toBe(0);
      expect(f.counts.bearer).toBe(0);
      expect(f.counts.wsl).toBe(0);
      expect(f.counts.window).toBe(1);
      expect(f.launcherPaths).toEqual([`${stateHome}/bin/t3`]);
    }),
  );

  it.effect("prepares only the backend restarted for changed shell feature flags", () =>
    Effect.gen(function* () {
      const restarted = yield* Deferred.make<void>();
      const events: string[] = [];
      const notReady: DesktopBackendManager.DesktopBackendSnapshot = {
        desiredRunning: true,
        ready: false,
        activePid: Option.none(),
        restartAttempt: 0,
        restartScheduled: false,
      };
      const observePreparation = (target: string) =>
        Effect.sync(() => {
          events.push(`prepare ${target}`);
          return notReady;
        });
      const f = fixture({ ...config, browserOnly: false }, true, {
        shellUpdates: Stream.make({
          version: 1,
          type: "shellEnvironment",
          platform: "darwin",
          discovered: true,
          environment: { T3CODE_FEATURE_FLAGS: "fixture" },
        }),
        backends: (primary) => [
          {
            ...primary,
            snapshot: observePreparation("primary"),
            stop: () => Effect.sync(() => void events.push("stop primary")),
            start: Effect.gen(function* () {
              const restarting = events.includes("stop primary");
              events.push("start primary");
              if (restarting) yield* Deferred.succeed(restarted, undefined);
            }),
          },
          {
            ...primary,
            id: DesktopBackendPool.BackendInstanceId("secondary"),
            snapshot: observePreparation("secondary"),
          },
        ],
      });
      yield* Effect.gen(function* () {
        expect(yield* f.bootstrap).toBe(true);
        yield* Deferred.await(restarted);
        expect(events).toEqual([
          "start primary",
          "prepare primary",
          "stop primary",
          "start primary",
        ]);
      }).pipe(Effect.provide(f.layer), Effect.scoped);
    }),
  );

  it.effect(
    "propagates failed browser-only settings persistence before either startup phase admits work",
    () =>
      Effect.gen(function* () {
        const writeError = new DesktopAppSettings.DesktopSettingsWriteError({
          operation: "replace-settings-file",
          path: "/fixture/companion/settings.json",
          cause: new Error("synthetic persistence refusal"),
        });
        const f = fixture(config, true, { settingsWriteError: writeError });
        const result = yield* Effect.exit(f.bootstrap.pipe(Effect.provide(f.layer), Effect.scoped));
        expect(Exit.isFailure(result)).toBe(true);
        if (Exit.isFailure(result)) {
          expect(Option.getOrThrow(Cause.findErrorOption(result.cause))).toBe(writeError);
        }
        expect(f.events).toEqual(["local=false"]);
        expect(f.counts).toEqual({
          backend: 0,
          recoverBackend: 0,
          startupRecovery: 0,
          port: 0,
          exposure: 0,
          appActivation: 0,
          window: 0,
          relaunch: 0,
          snapshot: 0,
          homeOwner: 0,
          bearer: 0,
          wsl: 0,
          dialog: 0,
          quit: 0,
        });
      }),
  );

  it.effect("preserves ordinary HomeOwner refusal before port, exposure, auth or WSL work", () =>
    Effect.gen(function* () {
      const f = fixture({ ...config, browserOnly: false }, true, { homeAvailable: false });
      expect(yield* f.bootstrap.pipe(Effect.provide(f.layer), Effect.scoped)).toBe(false);
      expect(DesktopHomeOwner.readLiveHomeOwner).toHaveBeenCalledOnce();
      expect(f.counts).toEqual({
        backend: 0,
        recoverBackend: 1,
        startupRecovery: 0,
        port: 0,
        exposure: 0,
        appActivation: 0,
        window: 0,
        relaunch: 0,
        snapshot: 0,
        homeOwner: 1,
        bearer: 0,
        wsl: 0,
        dialog: 1,
        quit: 1,
      });
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
