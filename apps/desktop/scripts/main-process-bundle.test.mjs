import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeVM from "node:vm";
import { build } from "vite-plus/pack";
import { assert, it } from "vite-plus/test";

import desktopConfig from "../vite.config.ts";

it("keeps lazy Linux imports and worker bundles from executing desktop startup twice", async () => {
  const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-desktop-bundle-"));
  try {
    const workerEntries = [
      "src/electron/WindowsForegroundFocusWorker.ts",
      "src/snapShot/GlobalShiftShortcutWorker.ts",
      "src/snapShot/RegionSnapShotWorker.ts",
      "src/snapShot/SnapShotAccessibilityWorker.ts",
    ];
    await Promise.all([
      NodeFSP.mkdir(NodePath.join(directory, "src/electron"), { recursive: true }),
      NodeFSP.mkdir(NodePath.join(directory, "src/snapShot"), { recursive: true }),
    ]);
    await Promise.all([
      NodeFSP.writeFile(
        NodePath.join(directory, "src/main.ts"),
        `import { shared } from "./shared.ts";
process.emit("startup", shared.value);
void import("./linux.ts").then(({ result }) => process.emit("ready", result));`,
      ),
      NodeFSP.writeFile(
        NodePath.join(directory, "src/shared.ts"),
        "export const shared = { value: 42 };",
      ),
      NodeFSP.writeFile(
        NodePath.join(directory, "src/linux.ts"),
        'import { shared } from "./shared.ts"; export const result = shared.value + 1;',
      ),
      ...workerEntries.map((entry) =>
        NodeFSP.writeFile(
          NodePath.join(directory, entry),
          'import { shared } from "../shared.ts"; process.emit("worker", shared.value);',
        ),
      ),
    ]);
    assert.ok(Array.isArray(desktopConfig.pack));
    const fixtureEntries = new Set(["src/main.ts", ...workerEntries]);
    for (const packConfig of desktopConfig.pack) {
      if (!Array.isArray(packConfig.entry)) continue;
      if (!packConfig.entry.some((entry) => fixtureEntries.has(entry))) continue;
      await build({
        ...packConfig,
        config: false,
        cwd: directory,
        tsconfig: false,
        sourcemap: false,
        onSuccess: undefined,
        logLevel: "silent",
      });
    }

    const outputDirectory = NodePath.join(directory, "dist-electron");
    const filenames = (await NodeFSP.readdir(outputDirectory, { recursive: true })).filter(
      (filename) => filename.endsWith(".cjs"),
    );
    const sources = new Map(
      await Promise.all(
        filenames.map(async (filename) => {
          const path = NodePath.join(outputDirectory, filename);
          return [path, await NodeFSP.readFile(path, "utf8")];
        }),
      ),
    );
    const modules = new Map();
    const startups = [];
    const workers = [];
    const ready = Promise.withResolvers();
    const load = (filename, cacheModule = true) => {
      const cached = modules.get(filename);
      if (cached) return cached.exports;
      const module = { exports: {} };
      if (cacheModule) modules.set(filename, module);
      const source = sources.get(filename);
      assert.ok(source, `Missing bundle: ${filename}`);
      NodeVM.runInNewContext(source, {
        exports: module.exports,
        module,
        require: (specifier) => load(NodePath.resolve(NodePath.dirname(filename), specifier)),
        process: {
          emit: (event, value) => {
            if (event === "startup") startups.push(value);
            if (event === "worker") workers.push(value);
            if (event === "ready") ready.resolve(value);
          },
        },
      });
      return module.exports;
    };

    load(NodePath.join(outputDirectory, "main.cjs"), false);
    assert.equal(await ready.promise, 43);
    assert.deepEqual(startups, [42]);
    for (const entry of workerEntries) {
      load(NodePath.join(outputDirectory, entry.replace(/^src\//, "").replace(/\.ts$/, ".cjs")));
    }
    assert.deepEqual(workers, [42, 42, 42, 42]);
    assert.deepEqual(startups, [42]);
  } finally {
    await NodeFSP.rm(directory, { recursive: true, force: true });
  }
});

it("loads the emitted packaged boot entry and backend cache preload", async () => {
  const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-desktop-boot-"));
  try {
    const entries = ["src/boot.ts", "src/compileCache.ts"];
    const sources = [
      ...entries,
      "src/app/DesktopUserDataOverride.ts",
      "src/jones/previewCompanion/CompanionProduct.ts",
      "src/jones/updates/jonesNativeStartup.ts",
    ];
    const sharedPackageDirectory = NodePath.join(directory, "node_modules/@t3tools/shared");
    const electronPackageDirectory = NodePath.join(directory, "node_modules/electron");
    await Promise.all([
      ...sources.map((entry) =>
        NodeFSP.mkdir(NodePath.dirname(NodePath.join(directory, entry)), { recursive: true }),
      ),
      NodeFSP.mkdir(sharedPackageDirectory, { recursive: true }),
      NodeFSP.mkdir(electronPackageDirectory, { recursive: true }),
    ]);
    await Promise.all([
      NodeFSP.copyFile(
        new URL("../../../packages/shared/src/jones/previewCompanionProduct.ts", import.meta.url),
        NodePath.join(sharedPackageDirectory, "previewCompanionProduct.ts"),
      ),
      NodeFSP.copyFile(
        new URL("../../../packages/shared/src/jones/nativeWriterFence.ts", import.meta.url),
        NodePath.join(sharedPackageDirectory, "nativeWriterFence.ts"),
      ),
      NodeFSP.writeFile(
        NodePath.join(sharedPackageDirectory, "package.json"),
        JSON.stringify({
          name: "@t3tools/shared",
          type: "module",
          exports: {
            "./jones/previewCompanionProduct": "./previewCompanionProduct.ts",
            "./jones/nativeWriterFence": "./nativeWriterFence.ts",
          },
        }),
      ),
      NodeFSP.writeFile(
        NodePath.join(directory, "package.json"),
        JSON.stringify({
          name: "t3code",
          dependencies: { "@t3tools/shared": "0.0.0", electron: "44.4.2" },
        }),
      ),
      NodeFSP.writeFile(
        NodePath.join(electronPackageDirectory, "package.json"),
        JSON.stringify({ name: "electron", main: "index.cjs" }),
      ),
      NodeFSP.writeFile(
        NodePath.join(electronPackageDirectory, "index.cjs"),
        `
const path = require("node:path");
const appRoot = path.resolve(__dirname, "../..");
module.exports = { app: {
  isPackaged: true,
  getAppPath: () => appRoot,
  getVersion: () => "fixture",
  getPath: (role) => path.join(appRoot, "electron-" + role),
  setPath: () => {},
  exit: (code) => process.exit(code),
} };`,
      ),
    ]);
    await Promise.all(
      sources.map((entry) =>
        NodeFSP.copyFile(new URL(`../${entry}`, import.meta.url), NodePath.join(directory, entry)),
      ),
    );
    assert.ok(Array.isArray(desktopConfig.pack));
    for (const packConfig of desktopConfig.pack) {
      if (!Array.isArray(packConfig.entry)) continue;
      if (!packConfig.entry.some((entry) => entries.includes(entry))) continue;
      await build({
        ...packConfig,
        config: false,
        cwd: directory,
        tsconfig: false,
        sourcemap: false,
        onSuccess: undefined,
        logLevel: "silent",
      });
    }
    const outputDirectory = NodePath.join(directory, "dist-electron");
    const emittedSources = new Map(
      await Promise.all(
        (await NodeFSP.readdir(outputDirectory))
          .filter((name) => name.endsWith(".cjs"))
          .map(async (name) => [
            name,
            await NodeFSP.readFile(NodePath.join(outputDirectory, name), "utf8"),
          ]),
      ),
    );
    for (const [name, source] of emittedSources) {
      assert.notMatch(
        source,
        /\brequire\s*\(\s*["']@t3tools\/shared(?:\/[^"']*)?["']\s*\)/,
        `${name} must not require workspace-only shared source at runtime`,
      );
    }
    assert.match(
      emittedSources.get("boot.cjs"),
      /\brequire\s*\(\s*["']electron["']\s*\)/,
      "boot.cjs must retain Electron as a runtime dependency",
    );
    const runBoot = (override, metadata = { name: "t3code" }, admissionFailure) => {
      const operations = [];
      const env = { T3CODE_DESKTOP_USER_DATA_DIR: override, T3CODE_HOME: "/ordinary/state" };
      const modules = new Map();
      const load = (name) => {
        if (modules.has(name)) return modules.get(name).exports;
        const module = { exports: {} };
        modules.set(name, module);
        const source = emittedSources.get(name);
        assert.ok(source, `Missing packaged bootstrap dependency: ${name}`);
        NodeVM.runInNewContext(source, {
          module,
          exports: module.exports,
          process: { env, platform: admissionFailure ? "darwin" : "linux" },
          require: (specifier) => {
            if (specifier === "node:path") return NodePath.posix;
            if (specifier === "node:crypto") return NodeCrypto;
            if (specifier === "node:fs")
              return {
                lstatSync: () => {
                  throw new Error(admissionFailure);
                },
                writeSync: (fd, value) => {
                  assert.equal(fd, 2);
                  operations.push(`stderr:${value}`);
                },
                readFileSync: (path, encoding) => {
                  assert.equal(path, "/fixture/app/package.json");
                  assert.equal(encoding, "utf8");
                  return JSON.stringify(metadata);
                },
                mkdirSync: (path, options) => {
                  assert.equal(options.recursive, true);
                  operations.push(`mkdir:${path}`);
                },
              };
            if (specifier === "electron")
              return {
                app: {
                  isPackaged: true,
                  getAppPath: () => "/fixture/app",
                  getVersion: () => "fixture",
                  exit: (code) => operations.push(`exit:${code}`),
                  getPath: (role) => {
                    assert.ok(role === "appData" || role === "home");
                    return role === "appData" ? "/fixture/appData" : "/fixture/home";
                  },
                  setPath: (role, path) => operations.push(`${role}:${path}`),
                },
              };
            if (specifier === "./compileCache.cjs") {
              operations.push("cache");
              return {};
            }
            if (specifier === "./main.cjs") {
              operations.push("startup");
              return {};
            }
            return load(NodePath.posix.basename(specifier));
          },
        });
        return module.exports;
      };
      return { operations, env, load: () => load("boot.cjs") };
    };
    const isolated = runBoot(" /isolated/other/../profile ");
    isolated.load();
    assert.deepEqual(isolated.operations, [
      "mkdir:/isolated/profile",
      "userData:/isolated/profile",
      "sessionData:/isolated/profile",
      "cache",
      "startup",
    ]);
    const defaults = runBoot(undefined);
    defaults.load();
    assert.deepEqual(defaults.operations, ["cache", "startup"]);
    const refused = runBoot(undefined, undefined, "native failure\n" + "x".repeat(2000));
    refused.load();
    assert.equal(refused.operations.length, 2);
    assert.match(refused.operations[0], /^stderr:Jones Code refused native startup: /);
    assert.ok(refused.operations[0].length < 600);
    assert.equal(refused.operations[0].split("\n").length, 2);
    assert.equal(refused.operations[1], "exit:1");
    const invalid = runBoot("relative/profile");
    assert.throws(invalid.load, /must be an absolute path/);
    assert.deepEqual(invalid.operations, []);
    const companion = runBoot("/ordinary/profile", {
      name: "jones-preview-companion",
      jonesDesktopProduct: "preview-companion",
    });
    companion.load();
    assert.deepEqual(companion.operations, [
      "mkdir:/fixture/appData/Jones Preview Companion",
      "mkdir:/fixture/appData/Jones Preview Companion/Session",
      "mkdir:/fixture/home/.jones-preview-companion",
      "userData:/fixture/appData/Jones Preview Companion",
      "sessionData:/fixture/appData/Jones Preview Companion/Session",
      "cache",
      "startup",
    ]);
    assert.equal(companion.env.T3CODE_HOME, "/fixture/home/.jones-preview-companion");
    assert.equal(companion.env.JONES_PREVIEW_COMPANION_PRODUCT, "true");
    assert.equal(
      companion.env.T3CODE_DESKTOP_USER_DATA_DIR,
      "/fixture/appData/Jones Preview Companion",
    );
    const missingMarker = runBoot(undefined, { name: "jones-preview-companion" });
    assert.throws(missingMarker.load, /Invalid preview companion product marker/);
    assert.deepEqual(missingMarker.operations, []);
    const fixture = `console.log(require('node:module').getCompileCacheDir() ? 'cached' : 'uncached');`;
    await NodeFSP.writeFile(NodePath.join(outputDirectory, "main.cjs"), fixture);
    await NodeFSP.writeFile(
      NodePath.join(outputDirectory, "backend.mjs"),
      `import { getCompileCacheDir } from 'node:module'; console.log(getCompileCacheDir() ? 'cached' : 'uncached');`,
    );
    for (const disabled of [false, true]) {
      for (const args of [
        [NodePath.join(outputDirectory, "boot.cjs")],
        [
          "--require",
          NodePath.join(outputDirectory, "compileCache.cjs"),
          NodePath.join(outputDirectory, "backend.mjs"),
        ],
      ]) {
        const child = NodeChildProcess.spawnSync(process.execPath, args, {
          encoding: "utf8",
          env: {
            ...process.env,
            T3CODE_HOME: NodePath.join(directory, "synthetic-state"),
            T3CODE_DESKTOP_USER_DATA_DIR: undefined,
            T3CODE_JONES_TRIAL_DESCRIPTOR: undefined,
            APPIMAGE: "",
            NODE_COMPILE_CACHE: undefined,
            NODE_DISABLE_COMPILE_CACHE: disabled ? "1" : undefined,
            XDG_CACHE_HOME: directory,
            TMPDIR: directory,
            TEMP: directory,
            TMP: directory,
          },
        });
        assert.equal(child.status, 0, child.stderr);
        assert.equal(child.stdout.trim(), disabled ? "uncached" : "cached");
      }
    }
  } finally {
    await NodeFSP.rm(directory, { recursive: true, force: true });
  }
});
