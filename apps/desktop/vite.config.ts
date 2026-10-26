import { typecheckTask } from "../../scripts/jones/tsrs/task-cache.ts";
import "vite-plus/test/config";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Vite resolves the fixed custody artifact synchronously outside an Effect runtime.
import * as NodePath from "node:path";
import { defaultInclude, defineConfig } from "vite-plus";

import { isDesktopRuntimeExternalDependency } from "../../scripts/lib/desktop-external-packages.ts";
import { loadRepoEnv } from "../../scripts/lib/public-config.ts";

const repoEnv = loadRepoEnv();

// Install only bundle externals; the server bundle already carries its dependency tree.
const isMainProcessExternal = (id: string) =>
  id === "electron" || id.startsWith("electron/") || isDesktopRuntimeExternalDependency(id);
// Both independently bundled entry points must use Node's one module cache entry.
const coldCustodyExternal = {
  name: "jones-cold-custody-single-instance",
  resolveId(id: string, importer?: string) {
    if (!importer || !id.startsWith(".")) return null;
    const normalizedImporter = importer.replaceAll("\\", "/");
    const offset = normalizedImporter.lastIndexOf("/src/");
    if (offset < 0) return null;
    const canonical = NodePath.posix.join(
      normalizedImporter.slice(0, offset),
      "src/jones/updates/jonesColdBackendCustody.ts",
    );
    return NodePath.posix.join(
      NodePath.posix.dirname(normalizedImporter),
      id.replaceAll("\\", "/"),
    ) === canonical
      ? { id: "./jones-cold-backend-custody.cjs", external: true }
      : null;
  },
};
const shouldLaunchElectronAfterPack = process.env.T3CODE_DESKTOP_DEV === "1";
const publicConfigDefine = {
  __T3CODE_BUILD_CLERK_PUBLISHABLE_KEY__: JSON.stringify(
    repoEnv.T3CODE_CLERK_PUBLISHABLE_KEY?.trim() ?? "",
  ),
};

export default defineConfig({
  run: {
    tasks: {
      build: {
        command:
          "node scripts/build-browser-secret.mjs && node scripts/build-preview-annotation-css.mjs && vp pack",
        dependsOn: ["t3#build"],
        cache: false,
      },
      dev: {
        command:
          "node scripts/build-browser-secret.mjs && node scripts/build-preview-annotation-css.mjs && cross-env T3CODE_DESKTOP_DEV=1 vp pack --watch",
        dependsOn: ["t3#build"],
        cache: false,
      },
      "dev:bundle": {
        command:
          "node scripts/build-browser-secret.mjs && node scripts/build-preview-annotation-css.mjs && vp pack --watch",
        cache: false,
      },
      "dev:electron": {
        command: "node scripts/dev-electron.mjs",
        dependsOn: ["t3#build"],
        cache: false,
      },
      typecheck: typecheckTask("node ../../scripts/jones/tsrs/typecheck.ts --noEmit"),
    },
  },
  pack: [
    {
      format: "cjs",
      outDir: "dist-electron",
      dts: false,
      sourcemap: true,
      outExtensions: () => ({ js: ".cjs" }),
      define: publicConfigDefine,
      outputOptions: { codeSplitting: false },
      entry: ["src/main.ts"],
      plugins: [coldCustodyExternal],
      clean: true,
      deps: {
        alwaysBundle: (id) => !id.startsWith("node:") && !isMainProcessExternal(id),
        neverBundle: isMainProcessExternal,
        onlyBundle: false,
      },
      ...(shouldLaunchElectronAfterPack ? { onSuccess: "node scripts/dev-electron.mjs" } : {}),
    },
    {
      format: "cjs",
      outDir: "dist-electron",
      dts: false,
      sourcemap: true,
      outExtensions: () => ({ js: ".cjs" }),
      define: publicConfigDefine,
      entry: [
        "src/electron/WindowsForegroundFocusWorker.ts",
        "src/snapShot/GlobalShiftShortcutWorker.ts",
        "src/snapShot/RegionSnapShotWorker.ts",
        "src/snapShot/SnapShotAccessibilityWorker.ts",
        "src/jones/updates/jonesStagingWorker.ts",
      ],
      clean: false,
      deps: {
        alwaysBundle: (id) => !id.startsWith("node:") && !isMainProcessExternal(id),
        neverBundle: isMainProcessExternal,
        onlyBundle: false,
      },
    },
    {
      // boot and main require the same custody artifact; its private permits must never be duplicated.
      format: "cjs",
      outDir: "dist-electron",
      dts: false,
      sourcemap: true,
      outExtensions: () => ({ js: ".cjs" }),
      entry: {
        boot: "src/boot.ts",
        compileCache: "src/compileCache.ts",
        "jones-cold-backend-custody": "src/jones/updates/jonesColdBackendCustody.ts",
      },
      plugins: [coldCustodyExternal],
      clean: false,
      deps: {
        alwaysBundle: (id) =>
          id === "@t3tools/shared/jones/previewCompanionProduct" ||
          id === "@t3tools/shared/jones/nativeWriterFence",
        neverBundle: (id) =>
          id === "ffi-rs" ||
          id.startsWith("@yuuang/ffi-rs-") ||
          id === "./main.cjs" ||
          id === "./compileCache.cjs" ||
          id === "./jones-cold-backend-custody.cjs",
      },
    },
    {
      format: "cjs",
      outDir: "dist-electron",
      dts: false,
      sourcemap: true,
      outExtensions: () => ({ js: ".cjs" }),
      define: publicConfigDefine,
      entry: ["src/preload.ts"],
      // Electron's sandbox wrapper already binds setImmediate/clearImmediate.
      // Keep bundled Effect internals inside a separate lexical scope.
      outputOptions: { codeSplitting: false, banner: "(() => {", footer: "})();" },
      deps: {
        // Sandboxed Electron preloads cannot reliably resolve package imports
        // from inside the packaged ASAR. Bundle Clerk's preload bridge into the
        // preload artifact instead of leaving a runtime require() behind.
        alwaysBundle: (id) =>
          id === "@clerk/electron" ||
          id.startsWith("@clerk/electron/") ||
          id === "effect" ||
          id.startsWith("effect/") ||
          id.startsWith("@t3tools/contracts"),
      },
    },
    {
      // The isolated peer gets no Clerk or application bridge; keep its sandbox preload self-contained.
      format: "cjs",
      outDir: "dist-electron",
      dts: false,
      sourcemap: true,
      outExtensions: () => ({ js: ".cjs" }),
      entry: { "jones/threadMonitor/preload": "src/jones/threadMonitor/preload.ts" },
      clean: false,
      outputOptions: { codeSplitting: false, banner: "(() => {", footer: "})();" },
      deps: {
        alwaysBundle: (id) => id !== "electron" && !id.startsWith("node:"),
        neverBundle: (id) => id === "electron" || id.startsWith("node:"),
      },
    },
    {
      format: "cjs",
      outDir: "dist-electron",
      dts: false,
      sourcemap: true,
      outExtensions: () => ({ js: ".cjs" }),
      entry: ["src/preview-pick-preload.ts"],
      deps: {
        alwaysBundle: (id) => id === "react-grab" || id.startsWith("react-grab/"),
      },
    },
    {
      format: "cjs",
      outDir: "dist-electron",
      dts: false,
      sourcemap: true,
      outExtensions: () => ({ js: ".cjs" }),
      entry: ["src/preview-pip-preload.ts"],
    },
    {
      // Sandboxed preloads must be self-contained, without shared runtime chunks.
      format: "cjs",
      outDir: "dist-electron",
      dts: false,
      sourcemap: true,
      outExtensions: () => ({ js: ".cjs" }),
      entry: ["src/mac-permission-preload.ts"],
    },
  ],
  test: {
    include: [...defaultInclude, "../../native/kde-snap-shot/src/*.test.ts"],
    // The Windows lane runs workspace suites concurrently; filesystem-heavy
    // desktop integration tests can exceed Vitest's 5 second default there.
    testTimeout: 15_000,
    setupFiles: [
      "../../packages/shared/src/testing/longTempDir.ts",
      "../../packages/shared/src/testing/providerEnv.ts",
    ],
  },
});
