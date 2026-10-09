import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as FileSystem from "effect/FileSystem";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as DesktopConfig from "../../app/DesktopConfig.ts";
import * as DesktopEnvironment from "../../app/DesktopEnvironment.ts";
import * as DesktopLinuxUrlHandler from "../../app/DesktopLinuxUrlHandler.ts";
import { resolveEarlyLinuxElectronOptions } from "../../app/DesktopEarlyElectronStartup.ts";
import * as DesktopClerk from "../../app/DesktopClerk.ts";
import * as DesktopAssets from "../../app/DesktopAssets.ts";
import * as ElectronApp from "../../electron/ElectronApp.ts";
import * as ElectronWindow from "../../electron/ElectronWindow.ts";
import * as ElectronShell from "../../electron/ElectronShell.ts";
import {
  companionLinuxIdentity,
  configureCompanionProduct,
  isCompanionPackage,
  readDesktopProductMetadata,
} from "./CompanionProduct.ts";

const metadata = { name: "jones-preview-companion", jonesDesktopProduct: "preview-companion" };
function bootFixture(
  path: Path.Path,
  packageMetadata: unknown = metadata,
  env: Record<string, string | undefined> = {},
) {
  const actions: Array<readonly string[]> = [];
  configureCompanionProduct({
    metadata: packageMetadata,
    appDataDirectory: "/home/test/AppData",
    homeDirectory: "/home/test",
    join: path.join,
    env,
    createDirectory: (directory) => {
      actions.push(["mkdir", directory]);
    },
    setPath: (name, directory) => {
      actions.push([name, directory]);
    },
  });
  return { env, actions };
}
const environmentLayer = (
  env: Record<string, string | undefined>,
  platform: NodeJS.Platform = "darwin",
) =>
  DesktopEnvironment.layer({
    dirname: "/app/apps/desktop/dist-electron",
    homeDirectory: "/home/test",
    platform,
    processArch: "arm64",
    appVersion: "1.0.0-preview.20261008.1",
    appPath: "/app",
    isPackaged: true,
    resourcesPath: "/resources",
    runningUnderArm64Translation: false,
  }).pipe(Layer.provide(Layer.mergeAll(Path.layer, DesktopConfig.layerTest(env))));

describe("companion product boot", () => {
  it.effect("keeps early Linux identity separate without changing password-store decisions", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path.pipe(Effect.provide(Path.layer));
      const ordinary = resolveEarlyLinuxElectronOptions({
        env: {},
        homeDirectory: "/home/test",
        joinPath: path.join,
        readFileString: () => {
          throw Object.assign(new Error("missing"), { code: "ENOENT" });
        },
      });
      const companion = companionLinuxIdentity(ordinary, true);
      expect(companion).toEqual({
        ...ordinary,
        linuxDesktopEntryName: "jones-preview-companion.desktop",
        linuxWmClass: "jones-preview-companion",
      });
      expect(companionLinuxIdentity(ordinary, false)).toBe(ordinary);
      expect(companionLinuxIdentity(null, true)).toBeNull();
    }),
  );

  it.effect("binds independent Electron and T3 state before main without shell environment", () =>
    Effect.gen(function* () {
      const f = bootFixture(yield* Path.Path.pipe(Effect.provide(Path.layer)));
      expect(f.actions).toEqual([
        ["mkdir", "/home/test/AppData/Jones Preview Companion"],
        ["mkdir", "/home/test/AppData/Jones Preview Companion/Session"],
        ["mkdir", "/home/test/.jones-preview-companion"],
        ["userData", "/home/test/AppData/Jones Preview Companion"],
        ["sessionData", "/home/test/AppData/Jones Preview Companion/Session"],
      ]);
      const environment = yield* DesktopEnvironment.DesktopEnvironment.pipe(
        Effect.provide(environmentLayer(f.env)),
      );
      expect(environment).toMatchObject({
        previewCompanionProduct: true,
        displayName: "Jones Preview Companion",
        appUserModelId: "com.jonessystems.jonespreviewcompanion",
        baseDir: "/home/test/.jones-preview-companion",
        stateDir: "/home/test/.jones-preview-companion/userdata",
        linuxWmClass: "jones-preview-companion",
      });
      expect(Option.getOrThrow(environment.userDataDirectoryOverride)).toBe(
        "/home/test/AppData/Jones Preview Companion",
      );
      expect(environment.desktopSettingsPath).toBe(
        "/home/test/.jones-preview-companion/userdata/desktop-settings.json",
      );
    }),
  );

  it.effect(
    "does not inherit ordinary profile overrides or permit an environment product switch",
    () =>
      Effect.gen(function* () {
        const f = bootFixture(yield* Path.Path.pipe(Effect.provide(Path.layer)), metadata, {
          T3CODE_HOME: "/ordinary",
          T3CODE_DESKTOP_USER_DATA_DIR: "/ordinary/profile",
          JONES_PREVIEW_COMPANION_PRODUCT: "false",
          T3CODE_DESKTOP_MOCK_UPDATES: "true",
        });
        expect(f.env.T3CODE_HOME).toBe("/home/test/.jones-preview-companion");
        expect(f.env.T3CODE_DESKTOP_USER_DATA_DIR).toBe(
          "/home/test/AppData/Jones Preview Companion",
        );
        expect(f.env.JONES_PREVIEW_COMPANION_PRODUCT).toBe("true");
        expect(f.env.T3CODE_DESKTOP_MOCK_UPDATES).toBe("false");
      }),
  );

  it.effect(
    "preserves ordinary storage defaults and ignores a forged shell-only product selector",
    () =>
      Effect.gen(function* () {
        const f = bootFixture(
          yield* Path.Path.pipe(Effect.provide(Path.layer)),
          { name: "t3code" },
          { JONES_PREVIEW_COMPANION_PRODUCT: "true" },
        );
        expect(f.actions).toEqual([]);
        expect(f.env).toEqual({ JONES_PREVIEW_COMPANION_PRODUCT: "false" });
        const environment = yield* DesktopEnvironment.DesktopEnvironment.pipe(
          Effect.provide(environmentLayer(f.env)),
        );
        expect(environment.baseDir).toBe("/home/test/.t3");
        expect(environment.displayName).toBe("Jones Code");
        expect(environment.previewCompanionProduct).toBe(false);
      }),
  );

  it.each([
    null,
    { ...metadata, jonesDesktopProduct: "unknown" },
    { name: metadata.name },
    { name: "t3code", jonesDesktopProduct: "preview-companion" },
  ])("fails closed on an invalid or missing dedicated marker", (value) =>
    expect(() => isCompanionPackage(value)).toThrow(),
  );

  it("allows unbundled absolute boot.cjs launches without an app-root package", () => {
    for (const code of ["ENOENT", "ENOTDIR"]) {
      const missing = () => {
        throw Object.assign(new Error("missing"), { code });
      };
      const ordinary = readDesktopProductMetadata({ isPackaged: false, readPackage: missing });
      expect(isCompanionPackage(ordinary)).toBe(false);
      expect(() => readDesktopProductMetadata({ isPackaged: true, readPackage: missing })).toThrow(
        "missing",
      );
    }
    for (const isPackaged of [false, true]) {
      expect(() =>
        readDesktopProductMetadata({ isPackaged, readPackage: () => "{invalid" }),
      ).toThrow();
      expect(() =>
        readDesktopProductMetadata({
          isPackaged,
          readPackage: () => {
            throw Object.assign(new Error("denied"), { code: "EACCES" });
          },
        }),
      ).toThrow("denied");
    }
  });

  it.effect(
    "never creates the Clerk bridge or Linux OS URL handler for the dedicated product",
    () =>
      Effect.gen(function* () {
        const f = bootFixture(yield* Path.Path.pipe(Effect.provide(Path.layer)));
        const layer = Layer.mergeAll(
          environmentLayer(f.env, "linux"),
          Layer.mock(ElectronApp.ElectronApp, {}),
          Layer.mock(ElectronWindow.ElectronWindow, {}),
          Layer.mock(ElectronShell.ElectronShell, {}),
          Layer.mock(DesktopAssets.DesktopAssets, {}),
          Layer.mock(ChildProcessSpawner.ChildProcessSpawner, {}),
          FileSystem.layerNoop({}),
          Path.layer,
        );
        yield* Effect.gen(function* () {
          const clerk = yield* DesktopClerk.make;
          yield* clerk.configure;
          const linux = yield* DesktopLinuxUrlHandler.make;
          yield* linux.register;
        }).pipe(Effect.provide(layer), Effect.scoped);
      }),
  );
});
