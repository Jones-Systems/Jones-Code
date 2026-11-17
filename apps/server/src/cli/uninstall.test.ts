import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Config from "effect/Config";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as TestConsole from "effect/testing/TestConsole";
import { Command } from "effect/cli";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NetService from "@t3tools/shared/Net";
import { afterEach, vi } from "vite-plus/test";

import packageJson from "../../package.json" with { type: "json" };
import { SERVICE_LAUNCHER_PROTOCOL } from "../cloud/serviceProtocol.ts";
import * as BootService from "../cloud/bootService.ts";
import { posixLauncherScript } from "../cloud/pinnedRuntime.ts";
import {
  findOwnedLauncher,
  findRecordedLaunchers,
  runUninstall,
  spawnWindowsRemoval,
  uninstallCommand,
} from "./uninstall.ts";

afterEach(() => vi.restoreAllMocks());

it.layer(Layer.mergeAll(NodeServices.layer, NetService.layer))("t3 uninstall command", (it) => {
  it.effect("names another home's service it leaves installed", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-uninstall-cli-test-" });
      const uninstalls: Array<true> = [];
      vi.spyOn(BootService, "layer").mockReturnValue(
        Layer.succeed(
          BootService.BootService,
          BootService.BootService.of({
            status: Effect.succeed({
              supported: true,
              installed: true,
              current: true,
              installedBaseDir: "/home/me/.t3",
              unitPath: "/home/me/.config/systemd/user/t3code.service",
              logPath: "/home/me/.t3/userdata/logs/boot-service.log",
            }),
            withStopped: (operation) => operation,
            restart: Effect.die("unexpected restart"),
            install: () => Effect.die("unexpected install"),
            uninstall: Effect.sync(() => {
              uninstalls.push(true);
              return true;
            }),
          }),
        ),
      );

      const output = yield* Command.runWith(uninstallCommand, { version: packageJson.version })([
        "--base-dir",
        baseDir,
        "--yes",
      ]).pipe(
        Effect.andThen(TestConsole.logLines),
        Effect.map((lines) => lines.join("\n")),
        Effect.provideService(HostProcess.Environment, {}),
        Effect.provideService(HostProcess.IsExecutable, false),
        Effect.provide(
          Layer.mergeAll(
            ConfigProvider.layer(ConfigProvider.fromEnv({ env: {} })),
            Layer.fresh(TestConsole.layer),
          ),
        ),
      );
      assert.deepStrictEqual(uninstalls, []);
      assert.include(output, "--base-dir '/home/me/.t3'");
    }),
  );
});

it.layer(NodeServices.layer)("t3 uninstall launcher", (it) => {
  it.effect("claims only a launcher that points into this home's runtime tree", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      // macOS spells the temp folder /var but resolves it to /private/var.
      const root = yield* fs
        .makeTempDirectoryScoped({ prefix: "t3-uninstall-" })
        .pipe(Effect.flatMap(fs.realPath));
      const versionsDir = path.join(root, "runtime/versions");
      const exe = path.join(versionsDir, "1.0.0/t3");
      const otherExe = path.join(root, "other/runtime/versions/1.0.0/t3");
      const copy = path.join(root, "copy/t3");
      for (const file of [exe, otherExe, copy]) {
        yield* fs.makeDirectory(path.dirname(file), { recursive: true });
        yield* fs.writeFileString(file, "");
      }
      const ours = path.join(root, "bin/t3");
      const theirs = path.join(root, "other/bin/t3");
      yield* fs.makeDirectory(path.dirname(ours), { recursive: true });
      yield* fs.makeDirectory(path.dirname(theirs), { recursive: true });
      yield* fs.symlink(exe, ours);
      yield* fs.symlink(otherExe, theirs);

      assert.equal(yield* findOwnedLauncher({ launchedAs: ours, versionsDir }), ours);
      assert.isUndefined(yield* findOwnedLauncher({ launchedAs: theirs, versionsDir }));
      assert.isUndefined(yield* findOwnedLauncher({ launchedAs: copy, versionsDir }));
      assert.isUndefined(yield* findOwnedLauncher({ launchedAs: undefined, versionsDir }));
      // A script launcher execs by absolute path, so argv0 names the executable.
      const scriptDir = path.join(root, "script-bin");
      const script = path.join(scriptDir, "t3");
      yield* fs.makeDirectory(scriptDir);
      yield* fs.writeFileString(script, posixLauncherScript(exe), { mode: 0o755 });
      assert.equal(
        yield* findOwnedLauncher({ launchedAs: exe, versionsDir }).pipe(
          Effect.provideService(HostProcess.Environment, { PATH: scriptDir }),
        ),
        script,
      );
      const editedScript = path.join(scriptDir, "t3-edited");
      yield* fs.writeFileString(editedScript, `${posixLauncherScript(exe)}# mine\n`);

      // On Windows, only the shim exactly as written is ours; a user's wrapper stays.
      const shimDir = path.join(root, "win-bin");
      const shim = path.join(shimDir, "t3.cmd");
      yield* fs.makeDirectory(shimDir);
      const windowsOwner = (contents: string) =>
        fs
          .writeFileString(shim, contents)
          .pipe(
            Effect.andThen(findOwnedLauncher({ launchedAs: exe, versionsDir })),
            Effect.provideService(HostProcess.Platform, "win32"),
            Effect.provideService(HostProcess.Environment, { PATH: shimDir }),
          );
      assert.equal(yield* windowsOwner(`@echo off\r\n"${exe}" %*`), shim);
      assert.isUndefined(yield* windowsOwner(`@echo off\r\nset FOO=1\r\n"${exe}" %*`));

      // npx can run another version than the installed one, so uninstall also reads the
      // recorded launchers: through a symlinked home, and dangling after a prune.
      const throughAlias = path.join(root, "bin/t3-alias");
      const pruned = path.join(root, "bin/t3-pruned");
      yield* fs.symlink(root, path.join(root, "alias"));
      yield* fs.symlink(path.join(root, "alias/runtime/versions/1.0.0/t3"), throughAlias);
      yield* fs.symlink(path.join(versionsDir, "0.9.0/t3"), pruned);
      // Another program inside a version is not a launcher.
      const sibling = path.join(root, "bin/t3-sibling");
      yield* fs.symlink(path.join(versionsDir, "1.0.0/node"), sibling);
      yield* fs.writeFileString(
        path.join(root, "runtime/launchers"),
        [
          ours,
          theirs,
          copy,
          throughAlias,
          pruned,
          sibling,
          script,
          editedScript,
          path.join(root, "missing"),
        ].join("\n") + "\n",
      );
      for (const dir of [versionsDir, path.join(root, "alias/runtime/versions")]) {
        assert.deepStrictEqual(yield* findRecordedLaunchers(dir), [
          ours,
          throughAlias,
          pruned,
          script,
        ]);
      }

      // On Windows only an unedited t3.cmd is ours; a hand-edited wrapper belongs to the user.
      const cmd = path.join(root, "bin/t3.cmd");
      const edited = path.join(root, "bin/edited.cmd");
      yield* fs.writeFileString(cmd, `@echo off\r\n"${exe}" %*`);
      yield* fs.writeFileString(edited, `@echo off\r\nset "T3CODE_HOME=D:\\other"\r\n"${exe}" %*`);
      yield* fs.writeFileString(path.join(root, "runtime/launchers"), `${cmd}\r\n${edited}\r\n`);
      assert.deepStrictEqual(
        yield* findRecordedLaunchers(versionsDir).pipe(
          Effect.provideService(HostProcess.Platform, "win32"),
        ),
        [cmd],
      );
    }).pipe(Effect.provideService(HostProcess.Platform, "linux")),
  );
});

it.live.runIf(HostProcess.Platform.defaultValue() === "win32")(
  "t3 uninstall has cmd.exe remove the runtime directory, preserving spaces and literal environment-variable syntax",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-uninstall-" });
      const runtimeDir = path.join(root, "t3 home %COMSPEC% !COMSPEC! & data", "runtime");
      yield* fs.makeDirectory(path.join(runtimeDir, "versions", "1.0.0"), { recursive: true });
      yield* fs.writeFileString(path.join(runtimeDir, "versions", "1.0.0", "t3.exe"), "");

      const child = spawnWindowsRemoval(
        yield* Config.String("ComSpec").pipe(Config.withDefault("cmd.exe")),
        runtimeDir,
      );
      yield* Effect.callback<void, Error>((resume) => {
        child.once("error", (error) => resume(Effect.fail(error)));
        child.once("exit", (code) =>
          resume(code === 0 ? Effect.void : Effect.die(`cmd.exe exited with ${code}`)),
        );
      });

      assert.isFalse(yield* fs.exists(runtimeDir));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

const ORDINARY_VERSION = "0.0.44";
const PREVIEW_VERSION = "0.0.44-preview.20261002.101.2";

/** An installed home with a service, a recorded launcher and one downloaded version. */
const seedInstalledHome = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fs
    .makeTempDirectoryScoped({ prefix: "t3-uninstall-home-" })
    .pipe(Effect.flatMap(fs.realPath));
  const runtimeDir = path.join(root, "runtime");
  const exe = path.join(runtimeDir, "versions", ORDINARY_VERSION, "t3");
  const launcher = path.join(root, "bin", "t3");
  yield* fs.makeDirectory(path.dirname(exe), { recursive: true });
  yield* fs.makeDirectory(path.dirname(launcher), { recursive: true });
  yield* fs.writeFileString(exe, "");
  yield* fs.symlink(exe, launcher);
  yield* fs.writeFileString(path.join(runtimeDir, "launchers"), `${launcher}\n`);
  return { root, runtimeDir, launcher, exe, path, fs };
});

/** Runs uninstall without a terminal's prompt and reports what the service was asked to do. */
const uninstallHome = (
  root: string,
  input: { readonly version: string; readonly yes: boolean },
) => {
  const serviceUninstalls: Array<true> = [];
  const service = Layer.mock(BootService.BootService, {
    status: Effect.succeed<BootService.BootServiceStatus>({
      supported: true,
      installed: true,
      current: true,
      installedBaseDir: root,
      unitPath: `${root}/t3code.service`,
      logPath: `${root}/boot-service.log`,
    }),
    uninstall: Effect.sync(() => {
      serviceUninstalls.push(true);
      return true;
    }),
  });
  return runUninstall({ baseDir: root, version: input.version, assumeYes: input.yes }).pipe(
    Effect.provide(service),
    Effect.provideService(HostProcess.IsExecutable, false),
    Effect.provideService(HostProcess.Platform, "linux"),
    Effect.provideService(HostProcess.Environment, {}),
    Effect.exit,
    Effect.map((exit) => ({ exit, serviceUninstalls })),
  );
};

it.layer(NodeServices.layer)("t3 uninstall on a Jones-managed runtime", (it) => {
  const assertRefusedUntouched = (
    result: Effect.Success<ReturnType<typeof uninstallHome>>,
    home: Effect.Success<typeof seedInstalledHome>,
    expected: string,
  ) =>
    Effect.gen(function* () {
      assert.isTrue(result.exit._tag === "Failure");
      const text = String(result.exit._tag === "Failure" ? result.exit.cause : "");
      assert.include(text, "CliUninstallError");
      assert.include(text, expected);
      assert.include(text, "nothing was removed");
      assert.include(text, `${home.runtimeDir}`);
      assert.deepStrictEqual(result.serviceUninstalls, []);
      assert.isTrue(yield* home.fs.exists(home.exe));
      assert.isTrue(yield* home.fs.exists(home.path.join(home.runtimeDir, "launchers")));
      assert.equal(yield* home.fs.readLink(home.launcher), home.exe);
    });

  it.effect("refuses an admitted Jones install before any removal, even with --yes", () =>
    Effect.gen(function* () {
      const home = yield* seedInstalledHome;
      const record = home.path.join(home.runtimeDir, "jones-active-install.json");
      const evidence = home.path.join(home.runtimeDir, "jones-updates", "txn-1", "recovery.json");
      yield* home.fs.makeDirectory(home.path.dirname(evidence), { recursive: true });
      yield* home.fs.writeFileString(record, '{"generation":"g1"}');
      yield* home.fs.writeFileString(evidence, "recovery");

      for (const yes of [true, false]) {
        const result = yield* uninstallHome(home.root, { version: ORDINARY_VERSION, yes });
        yield* assertRefusedUntouched(
          result,
          home,
          "This is a Jones Code runtime managed by its host updater",
        );
        assert.equal(yield* home.fs.readFileString(record), '{"generation":"g1"}');
        assert.equal(yield* home.fs.readFileString(evidence), "recovery");
      }
    }).pipe(Effect.scoped),
  );

  it.effect("refuses service state that names a receipted or preview active version", () =>
    Effect.gen(function* () {
      const home = yield* seedInstalledHome;
      const stateFile = home.path.join(home.runtimeDir, "service-state.json");
      const writeState = (activeVersion: string) =>
        home.fs.writeFileString(
          stateFile,
          `{"protocol":${SERVICE_LAUNCHER_PROTOCOL},"activeVersion":"${activeVersion}"}`,
        );

      // A Jones runtime receipt on the version the service runs.
      yield* writeState(ORDINARY_VERSION);
      yield* home.fs.writeFileString(
        home.path.join(
          home.runtimeDir,
          "versions",
          ORDINARY_VERSION,
          ".jones-runtime-receipt.json",
        ),
        "{}",
      );
      yield* assertRefusedUntouched(
        yield* uninstallHome(home.root, { version: "0.0.1", yes: true }),
        home,
        "managed by its host updater",
      );

      // A preview active version, with no receipt anywhere.
      yield* home.fs.remove(
        home.path.join(
          home.runtimeDir,
          "versions",
          ORDINARY_VERSION,
          ".jones-runtime-receipt.json",
        ),
      );
      yield* writeState(PREVIEW_VERSION);
      yield* assertRefusedUntouched(
        yield* uninstallHome(home.root, { version: "0.0.1", yes: true }),
        home,
        "managed by its host updater",
      );
      assert.isTrue(yield* home.fs.exists(stateFile));
    }).pipe(Effect.scoped),
  );

  it.effect("refuses when the service state cannot be read, and for a preview t3", () =>
    Effect.gen(function* () {
      const home = yield* seedInstalledHome;
      const stateFile = home.path.join(home.runtimeDir, "service-state.json");
      yield* home.fs.writeFileString(stateFile, "{not json");
      yield* assertRefusedUntouched(
        yield* uninstallHome(home.root, { version: ORDINARY_VERSION, yes: true }),
        home,
        "could not be read",
      );
      assert.equal(yield* home.fs.readFileString(stateFile), "{not json");

      yield* home.fs.remove(stateFile);
      yield* assertRefusedUntouched(
        yield* uninstallHome(home.root, { version: PREVIEW_VERSION, yes: true }),
        home,
        "managed by its host updater",
      );
    }).pipe(Effect.scoped),
  );

  it.effect("still uninstalls an ordinary home, whose service state is stable", () =>
    Effect.gen(function* () {
      const home = yield* seedInstalledHome;
      yield* home.fs.writeFileString(
        home.path.join(home.runtimeDir, "service-state.json"),
        `{"protocol":${SERVICE_LAUNCHER_PROTOCOL},"activeVersion":"${ORDINARY_VERSION}"}`,
      );
      const result = yield* uninstallHome(home.root, { version: ORDINARY_VERSION, yes: true });
      assert.isTrue(result.exit._tag === "Success");
      assert.deepStrictEqual(result.serviceUninstalls, [true]);
      assert.isFalse(yield* home.fs.exists(home.runtimeDir));
      assert.isFalse(yield* home.fs.exists(home.launcher));
    }).pipe(Effect.scoped),
  );
});
