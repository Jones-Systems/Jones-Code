import * as NodePath from "@effect/platform-node/NodePath";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PlatformError from "effect/PlatformError";

import type * as Electron from "electron";

import * as ElectronApp from "../electron/ElectronApp.ts";
import * as DesktopAppIdentity from "./DesktopAppIdentity.ts";
import * as DesktopAssets from "./DesktopAssets.ts";
import * as DesktopConfig from "./DesktopConfig.ts";
import * as DesktopEnvironment from "./DesktopEnvironment.ts";
import * as DesktopUserData from "./DesktopUserData.ts";

const defaultEnvironmentInput = {
  dirname: "/repo/apps/desktop/dist-electron",
  homeDirectory: "/Users/alice",
  platform: "darwin",
  processArch: "arm64",
  appVersion: "1.2.3",
  appPath: "/Applications/T3 Code.app/Contents/Resources/app.asar",
  isPackaged: true,
  resourcesPath: "/Applications/T3 Code.app/Contents/Resources",
  runningUnderArm64Translation: false,
} satisfies DesktopEnvironment.MakeDesktopEnvironmentInput;

type TestEnvironmentInput = Partial<DesktopEnvironment.MakeDesktopEnvironmentInput> & {
  readonly env?: Record<string, string | undefined>;
};

interface ElectronAppCalls {
  readonly setAboutPanelOptions: Array<Electron.AboutPanelOptionsOptions>;
  readonly setDockIcon: string[];
  readonly setName: string[];
}

const layerElectronApp = (calls: ElectronAppCalls) =>
  Layer.succeed(ElectronApp.ElectronApp, {
    metadata: Effect.die("unexpected metadata read"),
    name: Effect.succeed("T3 Code"),
    systemLocale: Effect.succeed("en-US"),
    whenReady: Effect.void,
    quit: Effect.void,
    exit: () => Effect.void,
    relaunch: () => Effect.void,
    setPath: () => Effect.void,
    setName: (name) =>
      Effect.sync(() => {
        calls.setName.push(name);
      }),
    setAboutPanelOptions: (options) =>
      Effect.sync(() => {
        calls.setAboutPanelOptions.push(options);
      }),
    setAppUserModelId: () => Effect.void,
    getAppMetrics: Effect.succeed([]),
    setAsDefaultProtocolClient: () => Effect.succeed(true),
    setDesktopName: () => Effect.void,
    setDockIcon: (iconPath) =>
      Effect.sync(() => {
        calls.setDockIcon.push(iconPath);
      }),
    appendCommandLineSwitch: () => Effect.void,
    onBeforeQuitForUpdate: () => Effect.void,
    removeCommandLineSwitch: () => Effect.void,
    on: () => Effect.void,
  } satisfies ElectronApp.ElectronApp["Service"]);

const layerAssets = (png: Option.Option<string>) =>
  Layer.succeed(DesktopAssets.DesktopAssets, {
    iconPaths: Effect.succeed({
      ico: Option.none(),
      icns: Option.none(),
      png,
    }),
    resolveResourcePath: () => Effect.succeedNone,
  } satisfies DesktopAssets.DesktopAssets["Service"]);

const layerEnvironment = (overrides: TestEnvironmentInput = {}) => {
  const { env, ...environmentOverrides } = overrides;
  return DesktopEnvironment.layer({
    ...defaultEnvironmentInput,
    ...environmentOverrides,
  }).pipe(
    Layer.provide(
      Layer.mergeAll(
        NodeServices.layer,
        NodePath.layerPosix,
        DesktopConfig.layerTest({
          ...env,
        }),
      ),
    ),
  );
};

const withIdentity = <A, E, R>(
  effect: Effect.Effect<
    A,
    E,
    | R
    | DesktopAppIdentity.DesktopAppIdentity
    | DesktopEnvironment.DesktopEnvironment
    | FileSystem.FileSystem
  >,
  input: {
    readonly calls?: ElectronAppCalls;
    readonly environment?: TestEnvironmentInput;
    readonly legacyPathExists?: boolean;
    readonly legacyPathProbeError?: PlatformError.PlatformError;
    readonly packageJson?: string;
    readonly readPackageJson?: () => Effect.Effect<string, PlatformError.PlatformError>;
    readonly pngIconPath?: Option.Option<string>;
  } = {},
) => {
  const calls: ElectronAppCalls = input.calls ?? {
    setAboutPanelOptions: [],
    setDockIcon: [],
    setName: [],
  };

  return effect.pipe(
    Effect.provide(
      DesktopAppIdentity.layer.pipe(
        Layer.provide(NodePath.layerPosix),
        Layer.provideMerge(
          FileSystem.layerNoop({
            exists: (path) =>
              input.legacyPathProbeError
                ? Effect.fail(input.legacyPathProbeError)
                : Effect.succeed(
                    input.legacyPathExists === true && /T3 Code \((Alpha|Dev)\)/.test(path),
                  ),
            readFileString:
              input.readPackageJson ??
              (() =>
                Effect.succeed(input.packageJson ?? '{"t3codeCommitHash":"abcdef1234567890"}')),
          }),
        ),
        Layer.provideMerge(layerAssets(input.pngIconPath ?? Option.none())),
        Layer.provideMerge(layerElectronApp(calls)),
        Layer.provideMerge(layerEnvironment(input.environment)),
      ),
    ),
  );
};

describe("DesktopAppIdentity", () => {
  it.effect("keeps a process runtime identity with only the full embedded commit", () =>
    withIdentity(
      Effect.gen(function* () {
        const identity = yield* DesktopAppIdentity.DesktopAppIdentity;
        const first = yield* identity.previewAutomationRuntimeIdentity;
        const second = yield* identity.previewAutomationRuntimeIdentity;
        assert.deepEqual(second, first);
        assert.match(first.runtimeInstanceId, /^[0-9a-f-]{36}$/i);
        assert.deepEqual(first, {
          schemaVersion: 1,
          runtimeKind: "electron",
          runtimeInstanceId: first.runtimeInstanceId,
          appVersion: "1.2.3",
          buildCommit: "abcdef1234567890abcdef1234567890abcdef12",
        });
      }),
      {
        packageJson: '{"t3codeCommitHash":"ABCDEF1234567890ABCDEF1234567890ABCDEF12"}',
        environment: { env: { T3CODE_COMMIT_HASH: "0123456789abcdef" } },
      },
    ),
  );

  it.effect("reports no build commit when embedded metadata is abbreviated", () =>
    withIdentity(
      Effect.gen(function* () {
        const identity = yield* DesktopAppIdentity.DesktopAppIdentity;
        const runtime = yield* identity.previewAutomationRuntimeIdentity;
        assert.equal(runtime.buildCommit, null);
      }),
      { packageJson: '{"t3codeCommitHash":"abcdef123456"}' },
    ),
  );

  it.effect("uses an explicit client profile independently of the server home", () =>
    withIdentity(
      Effect.gen(function* () {
        const identity = yield* DesktopAppIdentity.DesktopAppIdentity;
        const environment = yield* DesktopEnvironment.DesktopEnvironment;
        assert.equal(yield* identity.resolveUserDataPath, "/isolated/client-profile");
        assert.equal(environment.baseDir, "/isolated/server-home");
      }),
      {
        legacyPathExists: true,
        environment: {
          env: {
            T3CODE_HOME: "/isolated/server-home",
            T3CODE_DESKTOP_USER_DATA_DIR: " /isolated/other/../client-profile ",
          },
        },
      },
    ),
  );

  it.effect("never probes the legacy profile for an explicit development profile", () =>
    withIdentity(
      Effect.gen(function* () {
        const identity = yield* DesktopAppIdentity.DesktopAppIdentity;
        assert.equal(yield* identity.resolveUserDataPath, "/isolated/client-profile");
      }),
      {
        legacyPathProbeError: PlatformError.systemError({
          _tag: "PermissionDenied",
          module: "FileSystem",
          method: "exists",
          pathOrDescriptor: "/legacy",
          description: "must not read legacy profile",
        }),
        environment: {
          env: {
            VITE_DEV_SERVER_URL: "http://localhost:5173",
            T3CODE_DESKTOP_USER_DATA_DIR: "/isolated/client-profile",
          },
        },
      },
    ),
  );
  it.effect("isolates the V2 profile even when the legacy V1 profile exists", () =>
    withIdentity(
      Effect.gen(function* () {
        const identity = yield* DesktopAppIdentity.DesktopAppIdentity;
        const userDataPath = yield* identity.resolveUserDataPath;

        assert.equal(userDataPath, "/Users/alice/Library/Application Support/t3code-v2");
      }),
      { legacyPathExists: true },
    ),
  );

  it.effect("keeps using the legacy development profile", () =>
    withIdentity(
      Effect.gen(function* () {
        const identity = yield* DesktopAppIdentity.DesktopAppIdentity;
        assert.equal(
          yield* identity.resolveUserDataPath,
          "/Users/alice/Library/Application Support/T3 Code (Dev)",
        );
      }),
      {
        legacyPathExists: true,
        environment: { env: { VITE_DEV_SERVER_URL: "http://localhost:5173" } },
      },
    ),
  );

  it.effect("preserves failures while inspecting the legacy userData path", () => {
    const legacyPath = "/Users/alice/Library/Application Support/T3 Code (Dev)";
    const cause = PlatformError.systemError({
      _tag: "PermissionDenied",
      module: "FileSystem",
      method: "exists",
      description: "permission denied",
      pathOrDescriptor: legacyPath,
    });

    return withIdentity(
      Effect.gen(function* () {
        const identity = yield* DesktopAppIdentity.DesktopAppIdentity;
        const error = yield* identity.resolveUserDataPath.pipe(Effect.flip);

        assert.instanceOf(error, DesktopUserData.DesktopUserDataInitializationError);
        assert.equal(error.resourcePath, legacyPath);
        assert.strictEqual(error.cause, cause);
        assert.equal(
          error.message,
          `Could not initialize Electron user data during inspect at ${legacyPath} (PermissionDenied).`,
        );
      }),
      {
        legacyPathProbeError: cause,
        environment: { env: { VITE_DEV_SERVER_URL: "http://localhost:5173" } },
      },
    );
  });

  it.effect("configures app identity from the environment commit override", () => {
    const calls: ElectronAppCalls = {
      setAboutPanelOptions: [],
      setDockIcon: [],
      setName: [],
    };

    return withIdentity(
      Effect.gen(function* () {
        const identity = yield* DesktopAppIdentity.DesktopAppIdentity;
        yield* identity.configure;

        assert.deepEqual(calls.setName, ["Jones Code"]);
        assert.equal(calls.setAboutPanelOptions[0]?.applicationName, "Jones Code");
        assert.equal(calls.setAboutPanelOptions[0]?.applicationVersion, "1.2.3");
        assert.equal(calls.setAboutPanelOptions[0]?.version, "0123456789ab");
        // Packaged: the bundle's own icon stands, so a custom one the user
        // attached survives.
        assert.deepEqual(calls.setDockIcon, []);
      }),
      {
        calls,
        environment: {
          env: {
            T3CODE_COMMIT_HASH: "0123456789abcdef",
          },
        },
        pngIconPath: Option.some("/icon.png"),
      },
    );
  });

  it.effect("sets the dock icon only when running unpackaged", () => {
    const calls: ElectronAppCalls = {
      setAboutPanelOptions: [],
      setDockIcon: [],
      setName: [],
    };

    return withIdentity(
      Effect.gen(function* () {
        const identity = yield* DesktopAppIdentity.DesktopAppIdentity;
        yield* identity.configure;

        // Electron shows a generic icon for an unpackaged run, which is the
        // reason this call exists at all.
        assert.deepEqual(calls.setDockIcon, ["/icon.png"]);
      }),
      {
        calls,
        environment: { isPackaged: false },
        pngIconPath: Option.some("/icon.png"),
      },
    );
  });
});

it.effect.each(["{}", '{"t3codeCommitHash":42}', "{broken", '{"t3codeCommitHash":"z"}'])(
  "reports no runtime commit for invalid or missing metadata: %s",
  (packageJson) =>
    withIdentity(
      Effect.gen(function* () {
        const identity = yield* DesktopAppIdentity.DesktopAppIdentity;
        assert.equal((yield* identity.previewAutomationRuntimeIdentity).buildCommit, null);
      }),
      { packageJson },
    ),
);

it.effect("caches the embedded descriptor independently from the About override", () => {
  let reads = 0;
  const calls: ElectronAppCalls = { setAboutPanelOptions: [], setDockIcon: [], setName: [] };
  return withIdentity(
    Effect.gen(function* () {
      const identity = yield* DesktopAppIdentity.DesktopAppIdentity;
      const first = yield* identity.previewAutomationRuntimeIdentity;
      const second = yield* identity.previewAutomationRuntimeIdentity;
      assert.strictEqual(second, first);
      assert.equal(reads, 1);
      yield* identity.configure;
      assert.equal(calls.setAboutPanelOptions[0]?.version, "0123456789ab");
      assert.equal(first.buildCommit, "a".repeat(40));
      assert.equal(reads, 1);
    }),
    {
      calls,
      environment: { env: { T3CODE_COMMIT_HASH: "0123456789abcdef" } },
      readPackageJson: () =>
        Effect.sync(() => {
          reads += 1;
          return `{"t3codeCommitHash":"${(reads === 1 ? "A" : "B").repeat(40)}"}`;
        }),
    },
  );
});

it.effect("reports no runtime commit when package metadata is unreadable", () =>
  withIdentity(
    Effect.gen(function* () {
      const identity = yield* DesktopAppIdentity.DesktopAppIdentity;
      assert.equal((yield* identity.previewAutomationRuntimeIdentity).buildCommit, null);
    }),
    {
      readPackageJson: () =>
        Effect.fail(
          PlatformError.systemError({
            _tag: "PermissionDenied",
            module: "FileSystem",
            method: "readFileString",
            pathOrDescriptor: "/synthetic/package.json",
            description: "synthetic denied read",
          }),
        ),
    },
  ),
);
