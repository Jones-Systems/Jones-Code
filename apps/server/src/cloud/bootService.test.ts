// @effect-diagnostics-next-line nodeBuiltinImport:off - Synchronous fixture provenance hashes must match the staged executable bytes.
import * as NodeCrypto from "node:crypto";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native pointer bootstrap shares this test's scoped filesystem fixture.
import * as NodeFSP from "node:fs/promises";
import { bootstrapLauncherPointer } from "../jones/cloud/launcherPointer.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { cliArchiveFileName, cliArchivePlatformKey } from "@t3tools/shared/cliRelease";
import { CliError } from "effect/cli";
import { HttpClient } from "effect/http";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProcessRunner from "../processRunner.ts";
import * as BootService from "./bootService.ts";
import {
  JonesRuntimeProvenance,
  JONES_RUNTIME_PROVENANCE_FILE,
} from "../jones/hostService/artifactVerification.ts";
import { JONES_BOOT_SERVICE_IDENTITY } from "../jones/hostService/identity.ts";
import { pinnedRuntimePaths } from "./pinnedRuntime.ts";
import {
  qualifiedPayloadDigest,
  QUALIFIED_RUNTIME_RECEIPT,
} from "../jones/cloud/qualifiedRuntime.ts";
import {
  parseServiceState,
  SERVICE_ENV_FILE,
  SERVICE_LAUNCHER_PROTOCOL,
  SERVICE_RESTART_PENDING_FILE,
  serviceStateHasPendingUpdate,
} from "./serviceProtocol.ts";

const encodeJonesRuntimeProvenance = Schema.encodeEffect(
  Schema.fromJsonString(JonesRuntimeProvenance),
);

const NativeFixtureJson = Schema.fromJsonString(Schema.Unknown);

const encodeNativeFixtureJson = Schema.encodeSync(NativeFixtureJson);

const decodeNativeFixtureJson = Schema.decodeUnknownSync(NativeFixtureJson);

const linuxRuntime = "/home/theo/.t3/runtime/versions/1.2.3/t3";

const linuxPlan = {
  program: [linuxRuntime, "__service-launcher"],
  baseDir: "/home/theo/.t3",
  logPath: "/home/theo/.t3/userdata/logs/boot-service.log",
  unitPath: "/home/theo/.config/systemd/user/t3code.service",
};

const macRuntime = "/Users/theo/.t3/runtime/versions/1.2.3/t3";

const macPlan = {
  program: [macRuntime, "__service-launcher"],
  baseDir: "/Users/theo/.t3",
  logPath: "/Users/theo/.t3/userdata/logs/boot-service.log",
  unitPath: "/Users/theo/Library/LaunchAgents/com.t3tools.t3code.service.plist",
};

const UNIT_PATH_READ =
  "busctl --user --json=short get-property org.freedesktop.systemd1 /org/freedesktop/systemd1 org.freedesktop.systemd1.Manager UnitPath";

const macInstallerPath =
  "/opt/homebrew/bin:/Users/theo/.npm-global/bin:/Users/theo/.nvm/versions/node/v22.16.0/bin:/usr/bin:/bin";

const macRenderOptions = { homeDir: "/Users/theo", environmentPath: macInstallerPath };

it("runs the pinned runtime's own executable as the systemd launcher", () => {
  const unit = BootService.renderBootServiceUnit(linuxPlan);

  expect(unit).toContain(`ExecStart=${linuxRuntime} __service-launcher`);
  expect(unit).toContain("KillMode=mixed");
  expect(unit).toContain("TimeoutStopSec=90");
  expect(unit).not.toContain("node");
});

it("reads the served T3 home back out of a rendered unit or plist", () => {
  const plan = (baseDir: string) => ({
    program: [`${baseDir}/runtime/versions/1.2.3/t3`, "__service-launcher"],
    baseDir,
    logPath: `${baseDir}/userdata/logs/boot-service.log`,
    unitPath: "/home/theo/.config/systemd/user/t3code.service",
  });

  expect(
    BootService.bootServiceBaseDirOf(BootService.renderBootServiceUnit(plan("/home/theo/.t3"))),
  ).toBe("/home/theo/.t3");
  expect(
    BootService.bootServiceBaseDirOf(
      BootService.renderBootServiceUnit(plan("/home/theo/T3 Data/100%")),
    ),
  ).toBe("/home/theo/T3 Data/100%");
  expect(
    BootService.bootServiceBaseDirOf(
      BootService.renderBootServicePlist(plan("/Users/theo/a&b"), {
        homeDir: "/Users/theo",
        environmentPath: "/usr/bin",
      }),
    ),
  ).toBe("/Users/theo/a&b");
  expect(BootService.bootServiceBaseDirOf("[Service]\nExecStart=/x\n")).toBeUndefined();
});

it("protects the server unit from agents under CPU and memory pressure", () => {
  const unit = BootService.renderBootServiceUnit({
    program: ["/home/theo/.t3/runtime/versions/1.2.3/t3", "__service-launcher"],
    baseDir: "/home/theo/.t3",
    logPath: "/home/theo/.t3/userdata/logs/boot-service.log",
    unitPath: "/home/theo/.config/systemd/user/t3code.service",
  });
  const service = unit.slice(unit.indexOf("[Service]"), unit.indexOf("[Install]"));
  for (const line of [
    "CPUWeight=1000",
    "IOWeight=1000",
    "MemoryLow=1536M",
    "OOMScoreAdjust=100",
    "OOMPolicy=continue",
  ]) {
    expect(service.split("\n")).toContain(line);
  }
});

it.each([
  ["a unit", '/home/theo/T3 "Data"/100%\\x', BootService.renderBootServiceUnit],
  [
    "a plist",
    "/Users/theo/a&b <c>",
    (plan: BootService.BootServicePlan) =>
      BootService.renderBootServicePlist(plan, { homeDir: "/Users/theo", environmentPath: "" }),
  ],
] as const)("reads the launcher back out of %s", (_, baseDir, render) => {
  const program = `${baseDir}/runtime/versions/1.2.3/t3`;
  expect(
    BootService.bootServiceProgramOf(
      render({ program: [program, "__service-launcher"], baseDir, logPath: "/log", unitPath: "" }),
    ),
  ).toBe(program);
});

const makeHarness = Effect.fn("test.make_boot_service_harness")(function* (
  platform: NodeJS.Platform = "linux",
  installerPath = macInstallerPath,
  xdgConfigHome?: string,
  managerXdgConfigHome = xdgConfigHome,
  serviceOptions: Partial<Parameters<typeof BootService.make>[0]> = {},
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3-boot-service-test-" });
  const managerUnitDir = path.join(
    managerXdgConfigHome !== undefined && path.isAbsolute(managerXdgConfigHome)
      ? managerXdgConfigHome
      : path.join(home, ".config"),
    "systemd",
    "user",
  );
  const baseDir = path.join(home, ".t3");
  const arch = platform === "darwin" ? "arm64" : "x64";
  const statePath = path.join(baseDir, "runtime", "service-state.json");
  const runtime = pinnedRuntimePaths(path, baseDir, "1.2.3", platform);
  yield* fs.makeDirectory(path.dirname(runtime.entryPath), { recursive: true });
  yield* fs.writeFileString(runtime.entryPath, "#!/bin/sh\n");
  yield* fs.writeFileString(runtime.sentinelPath, "1.2.3\n");

  const unitFile = serviceOptions.identity?.systemdUnitFile ?? "t3code.service";
  const commands: string[] = [];
  const invocations: ProcessRunner.ProcessRunInput[] = [];
  const timeouts = new Map<string, unknown>();
  const logModesAtStart: Array<number | undefined> = [];
  const control: {
    failCommand: string | undefined;
    stateAfterStop?: string;
    linger: string;
    enabled: boolean;
    active: boolean;
    unitPathsOutput?: string;
    unitPathsThrows?: boolean;
    versionOutput?: string;
    versionIncomplete?: boolean;
  } = {
    failCommand: undefined,
    linger: "yes",
    enabled: true,
    active: true,
  };
  const runner = ProcessRunner.ProcessRunner.of({
    run: Effect.fn("test.run_boot_service_command")(function* (
      input: ProcessRunner.ProcessRunInput,
    ) {
      const command = `${input.command} ${input.args.join(" ")}`;
      if (
        command.startsWith("launchctl bootstrap ") ||
        command.startsWith("systemctl --user restart ")
      ) {
        const log = path.join(baseDir, "userdata", "logs", BootService.BOOT_SERVICE_LOG_FILE);
        logModesAtStart.push(
          (yield* fs.exists(log).pipe(Effect.orDie))
            ? (yield* fs.stat(log).pipe(Effect.orDie)).mode & 0o777
            : undefined,
        );
      }
      commands.push(command);
      invocations.push(input);
      timeouts.set(command, input.timeout);
      if (input.command === "busctl" && control.unitPathsThrows)
        return yield* new ProcessRunner.ProcessSpawnError({
          command: input.command,
          argumentCount: input.args.length,
          cause: "synthetic UnitPath runner failure",
        });
      const failed = command === control.failCommand;
      if (!failed && command === "loginctl enable-linger --no-ask-password 501")
        control.linger = "yes";
      if (!failed && command === "systemctl --user enable t3code.service") control.enabled = true;
      if (!failed && command === "systemctl --user restart t3code.service") control.active = true;
      // A timed-out stop may still stop the service.
      if (command === "systemctl --user stop t3code.service") control.active = false;
      if (
        control.stateAfterStop !== undefined &&
        (command === `systemctl --user stop ${unitFile}` ||
          command.startsWith("launchctl bootout --wait "))
      ) {
        yield* fs.writeFileString(statePath, control.stateAfterStop).pipe(Effect.orDie);
      }
      return {
        stdout:
          input.command === "busctl"
            ? (control.unitPathsOutput ??
              JSON.stringify({
                type: "as",
                data: [
                  "/usr/lib/systemd/user",
                  `${managerUnitDir}.control`,
                  "/run/user/501/systemd/transient",
                  managerUnitDir,
                  "/etc/systemd/user",
                ],
              }))
            : input.args[0] === "--version"
              ? (control.versionOutput ??
                `t3 v${/versions\/([^/]+)\//.exec(input.command)?.[1] ?? "1.2.3"}\n`)
              : input.command === "loginctl" && input.args[0] === "show-user"
                ? `${control.linger}\n`
                : input.args[1] === "is-enabled"
                  ? control.enabled
                    ? "enabled\n"
                    : "disabled\n"
                  : "",
        stderr: "",
        code: ChildProcessSpawner.ExitCode(
          failed ||
            (input.args[1] === "is-active" && !control.active) ||
            (input.args[1] === "is-enabled" && !control.enabled)
            ? 1
            : 0,
        ),
        timedOut: false,
        stdoutTruncated: input.args[0] === "--version" && control.versionIncomplete === true,
        stderrTruncated: false,
        stdoutInvalidUtf8: false,
        stderrInvalidUtf8: false,
      };
    }),
  });
  const makeService = (
    environmentPath: string | undefined = installerPath,
    cliVersion = "1.2.3",
    serviceBaseDir = baseDir,
    insideService = false,
    hostExecPath = "/usr/bin/t3",
  ) =>
    Effect.gen(function* () {
      const paths = pinnedRuntimePaths(path, serviceBaseDir, cliVersion, platform);
      yield* fs.makeDirectory(path.dirname(paths.entryPath), { recursive: true });
      yield* fs.writeFileString(paths.entryPath, "#!/bin/sh\n");
      yield* fs.writeFileString(paths.sentinelPath, `${cliVersion}\n`);
      const platformKey = cliArchivePlatformKey(platform, arch)!;
      yield* fs.writeFileString(
        path.join(paths.versionDir, JONES_RUNTIME_PROVENANCE_FILE),
        yield* encodeJonesRuntimeProvenance({
          schema: 1,
          repository: "Jones-Systems/Jones-Code",
          source: "a".repeat(40),
          version: cliVersion,
          platform,
          architecture: arch,
          artifact: cliArchiveFileName(cliVersion, platformKey),
          sha256: "b".repeat(64),
          entrySha256: NodeCrypto.createHash("sha256").update("#!/bin/sh\n").digest("hex"),
        }),
      );
      return yield* BootService.make({
        ...serviceOptions,
        baseDir: serviceBaseDir,
        logsDir: path.join(serviceBaseDir, "userdata", "logs"),
        cliVersion,
        host: { execPath: hostExecPath },
      });
    }).pipe(
      Effect.provideService(ProcessRunner.ProcessRunner, runner),
      Effect.provide(
        Layer.mergeAll(
          Layer.succeed(HostProcess.Platform, platform),
          Layer.succeed(HostProcess.Architecture, arch),
          Layer.succeed(HostProcess.UserId, 501),
          Layer.succeed(HostProcess.ExecutablePath, hostExecPath),
          Layer.succeed(
            HttpClient.HttpClient,
            HttpClient.make(() => Effect.die("no release download expected")),
          ),
          ConfigProvider.layer(
            ConfigProvider.fromEnv({
              env: {
                HOME: home,
                ...(xdgConfigHome === undefined ? {} : { XDG_CONFIG_HOME: xdgConfigHome }),
                ...(environmentPath === undefined || environmentPath === ""
                  ? {}
                  : { PATH: environmentPath }),
                ...(insideService ? { T3_BOOT_SERVICE_UNIT: unitFile } : {}),
              },
            }),
          ),
        ),
      ),
    );
  const service = yield* makeService();
  return {
    service,
    makeService,
    fs,
    home,
    baseDir,
    statePath,
    commands,
    invocations,
    timeouts,
    control,
    runtime,
    logModesAtStart,
  };
});

it.layer(NodeServices.layer)("boot service install", (it) => {
  it.effect.each(["linux", "darwin"] as const)(
    "creates a private future service log before manager start on %s",
    (platform) =>
      Effect.gen(function* () {
        const { service, fs, logModesAtStart } = yield* makeHarness(platform);
        const plan = yield* service.install();
        expect(logModesAtStart).toEqual([0o600]);
        expect((yield* fs.stat(plan.logPath)).mode & 0o777).toBe(0o600);
        expect(yield* fs.readFileString(plan.logPath)).toBe("");
      }),
  );

  it.effect("preserves existing service log content and mode", () =>
    Effect.gen(function* () {
      const { service, fs } = yield* makeHarness("darwin");
      const { logPath } = yield* service.status;
      const path = yield* Path.Path;
      yield* fs.makeDirectory(path.dirname(logPath), { recursive: true });
      yield* fs.writeFileString(logPath, "existing log\n", { mode: 0o640 });
      const before = yield* fs.stat(logPath);
      yield* service.install();
      expect((yield* fs.stat(logPath)).mode).toBe(before.mode);
      expect(yield* fs.readFileString(logPath)).toBe("existing log\n");
    }),
  );

  it.effect("creates a private missing service log when a command fails", () =>
    Effect.gen(function* () {
      const { service, fs, control } = yield* makeHarness();
      const plan = yield* service.install();
      yield* fs.remove(plan.logPath).pipe(Effect.ignore);
      control.failCommand = "systemctl --user restart t3code.service";
      yield* service.restart.pipe(Effect.flip);
      expect((yield* fs.stat(plan.logPath)).mode & 0o777).toBe(0o600);
      expect(yield* fs.readFileString(plan.logPath)).not.toBe("");
    }),
  );

  it.effect("refuses a symlink service log before manager mutation", () =>
    Effect.gen(function* () {
      const { service, fs, commands } = yield* makeHarness("darwin");
      const { logPath } = yield* service.status;
      const path = yield* Path.Path;
      yield* fs.makeDirectory(path.dirname(logPath), { recursive: true });
      const target = path.join(path.dirname(logPath), "untouched.log");
      yield* fs.writeFileString(target, "untouched\n", { mode: 0o640 });
      const before = yield* fs.stat(target);
      yield* fs.symlink(target, logPath);
      const commandsBefore = commands.length;
      expect((yield* service.install().pipe(Effect.flip))._tag).toBe("BootServiceInstallError");
      expect(
        commands.slice(commandsBefore).some((command) => command.startsWith("launchctl ")),
      ).toBe(false);
      expect(yield* fs.readFileString(target)).toBe("untouched\n");
      expect((yield* fs.stat(target)).mode).toBe(before.mode);
    }),
  );

  it.effect("does not append failure text through a symlink service log", () =>
    Effect.gen(function* () {
      const { service, fs, control } = yield* makeHarness();
      const plan = yield* service.install();
      const target = `${plan.logPath}.target`;
      yield* fs.writeFileString(target, "untouched\n", { mode: 0o640 });
      const before = yield* fs.stat(target);
      yield* fs.remove(plan.logPath);
      yield* fs.symlink(target, plan.logPath);
      control.failCommand = "systemctl --user restart t3code.service";
      expect((yield* service.restart.pipe(Effect.flip))._tag).toBe("BootServiceCommandError");
      expect(yield* fs.readFileString(target)).toBe("untouched\n");
      expect((yield* fs.stat(target)).mode).toBe(before.mode);
    }),
  );

  it.effect(
    "fails before installing files or validating a runtime when lingering needs an administrator",
    () =>
      Effect.gen(function* () {
        const { service, fs, statePath, commands, control, runtime } = yield* makeHarness();
        const before = yield* service.status;
        control.linger = "no";
        control.failCommand = "loginctl enable-linger --no-ask-password 501";
        yield* fs.remove(runtime.sentinelPath);

        const error = yield* service.install().pipe(Effect.flip);

        expect(error).toMatchObject({
          _tag: "BootServicePrerequisiteError",
          problem: "linger-disabled",
        });
        expect(error.message).toContain('sudo loginctl enable-linger "$(id -un)"');
        expect(error.message).toContain("last login session ends");
        expect(yield* fs.exists(before.unitPath)).toBe(false);
        expect(yield* fs.exists(statePath)).toBe(false);
        expect(commands.some((command) => command.includes("--version"))).toBe(false);
        expect(
          commands.some(
            (command) => command.includes("daemon-reload") || command.includes("restart"),
          ),
        ).toBe(false);
        expect(yield* fs.readFileString(before.logPath)).toContain("[linger-disabled]");
      }),
  );

  it.effect(
    "detects a partial install and preserves the running service when repair lacks permission",
    () =>
      Effect.gen(function* () {
        const { service, fs, statePath, commands, control } = yield* makeHarness();
        const plan = yield* service.install();
        const before = yield* fs.readFileString(statePath);
        const unit = yield* fs.readFileString(plan.unitPath);
        control.linger = "no";
        control.failCommand = "loginctl enable-linger --no-ask-password 501";

        expect(yield* service.status).toMatchObject({
          current: false,
          problems: ["linger-disabled"],
        });
        commands.length = 0;
        expect((yield* service.install().pipe(Effect.flip))._tag).toBe(
          "BootServicePrerequisiteError",
        );
        expect(yield* fs.readFileString(statePath)).toBe(before);
        expect(yield* fs.readFileString(plan.unitPath)).toBe(unit);
        expect(commands).not.toContain("systemctl --user stop t3code.service");
      }),
  );

  it.effect("enables lingering before installing and repairs stopped or disabled services", () =>
    Effect.gen(function* () {
      const { service, commands, control } = yield* makeHarness();
      control.linger = "no";
      yield* service.install();
      expect(control.linger).toBe("yes");
      expect(commands.indexOf("loginctl enable-linger --no-ask-password 501")).toBeLessThan(
        commands.indexOf("systemctl --user daemon-reload"),
      );

      control.enabled = false;
      control.active = false;
      expect(yield* service.status).toMatchObject({
        current: false,
        problems: ["service-disabled", "service-stopped"],
      });
      yield* service.install();
      expect((yield* service.status).current).toBe(true);
    }),
  );

  it.effect.each([
    { command: "systemctl --user show-environment", problem: "user-manager-unavailable" },
    { command: "loginctl show-user 501 --property=Linger --value", problem: "linger-unavailable" },
  ])("reports failed prerequisite probes without installing: $command", ({ command, problem }) =>
    Effect.gen(function* () {
      const { service, fs, statePath, control } = yield* makeHarness();
      control.failCommand = command;
      expect(yield* service.install().pipe(Effect.flip)).toMatchObject({
        _tag: "BootServicePrerequisiteError",
        problem,
      });
      expect(yield* fs.exists(statePath)).toBe(false);
    }),
  );

  it.effect.each([
    "nonzero",
    "runner-failure",
    "malformed",
    "schema-invalid",
    "missing-control",
    "missing-user",
    "relative-pair",
    "unrelated-pair",
  ] as const)(
    "UnitPath %s falls back to the installer config without any other command",
    (failure) =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        for (const absolute of [false, true]) {
          const fs = yield* FileSystem.FileSystem;
          const root = yield* fs.makeTempDirectoryScoped({ prefix: "unit-path-fallback-" });
          const xdg = absolute ? path.join(root, "selected config") : "relative-config";
          const f = yield* makeHarness("linux", undefined, xdg);
          f.commands.length = 0;
          if (failure === "nonzero") f.control.failCommand = UNIT_PATH_READ;
          if (failure === "runner-failure") f.control.unitPathsThrows = true;
          const responses = {
            malformed: "{",
            "schema-invalid": JSON.stringify({ data: 7 }),
            "missing-control": JSON.stringify({ data: ["/unexpected/systemd/user"] }),
            "missing-user": JSON.stringify({ data: ["/unexpected/systemd/user.control"] }),
            "relative-pair": JSON.stringify({
              data: ["relative/systemd/user.control", "relative/systemd/user"],
            }),
            "unrelated-pair": JSON.stringify({
              data: ["/unexpected/systemd/other.control", "/unexpected/systemd/other"],
            }),
          };
          if (failure !== "nonzero" && failure !== "runner-failure")
            f.control.unitPathsOutput = responses[failure];
          const service = yield* f.makeService();
          expect((yield* service.status).unitPath).toBe(
            path.join(
              absolute ? xdg : path.join(f.home, ".config"),
              "systemd",
              "user",
              "t3code.service",
            ),
          );
          expect(f.commands).toEqual([UNIT_PATH_READ]);
          expect(f.timeouts.get(UNIT_PATH_READ)).toEqual(Duration.seconds(5));
        }
      }),
  );

  it.effect.each([
    "unset",
    "empty",
    "relative",
    "relocated",
    "shell-only",
    "manager-only",
    "manager-unavailable",
  ] as const)("installs, discovers, restarts, and uninstalls with $XDG_CONFIG_HOME %s", (setting) =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const fs = yield* FileSystem.FileSystem;
      const configRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-xdg-config-" });
      const xdgConfigHome = {
        unset: undefined,
        empty: "",
        relative: "relative-config",
        relocated: path.join(configRoot, "config root"),
        "shell-only": path.join(configRoot, "config root"),
        "manager-only": undefined,
        "manager-unavailable": path.join(configRoot, "config root"),
      }[setting];
      const managerXdgConfigHome =
        setting === "shell-only"
          ? ""
          : setting === "manager-only"
            ? path.join(configRoot, "config root")
            : undefined;
      const { service, makeService, home, statePath, timeouts, runtime, control } =
        yield* makeHarness("linux", undefined, xdgConfigHome, managerXdgConfigHome);
      const unitPath = path.join(
        setting === "relocated" || setting === "manager-only" || setting === "manager-unavailable"
          ? path.join(configRoot, "config root")
          : path.join(home, ".config"),
        "systemd",
        "user",
        "t3code.service",
      );
      expect(yield* service.status).toMatchObject({ installed: false, unitPath });
      const plan = yield* service.install();
      expect(plan.unitPath).toBe(unitPath);
      if (setting === "manager-unavailable") {
        control.failCommand =
          "busctl --user --json=short get-property org.freedesktop.systemd1 /org/freedesktop/systemd1 org.freedesktop.systemd1.Manager UnitPath";
      }
      const discoveredService = yield* makeService();

      expect(parseServiceState(yield* fs.readFileString(statePath))).toEqual({
        protocol: SERVICE_LAUNCHER_PROTOCOL,
        activeVersion: "1.2.3",
      });
      expect(plan.program).toEqual([runtime.entryPath, "__service-launcher"]);
      expect(yield* fs.readFileString(plan.unitPath)).toContain(
        `ExecStart=${runtime.entryPath} __service-launcher`,
      );
      expect(yield* service.status).toMatchObject({
        current: true,
        installedVersion: "1.2.3",
      });
      expect(yield* discoveredService.status).toMatchObject({
        installed: true,
        current: true,
        unitPath,
      });
      expect(yield* discoveredService.restart).toBe("restarted");
      const pendingState = JSON.stringify({
        protocol: SERVICE_LAUNCHER_PROTOCOL,
        activeVersion: "1.2.3",
        update: {
          id: "u",
          fromVersion: "1.2.3",
          targetVersion: "1.2.4",
          dbPath: "/tmp/state.sqlite",
          status: "pending",
        },
      });
      yield* fs.writeFileString(statePath, pendingState);
      expect((yield* service.status).current).toBe(false);
      expect(yield* discoveredService.uninstall).toBe(true);
      expect(yield* fs.exists(unitPath)).toBe(false);
      expect((yield* discoveredService.status).installed).toBe(false);
      // systemd can take 90s to stop, exceeding the runner's 60s default.
      expect(timeouts.get("systemctl --user disable --now t3code.service")).toEqual(
        Duration.seconds(120),
      );
    }),
  );

  it.effect.each(["linux", "darwin"] as const)(
    "reports the installed version across launcher protocols on %s",
    (platform) =>
      Effect.gen(function* () {
        const { service, fs, statePath } = yield* makeHarness(platform);
        yield* service.install();

        for (const protocol of [SERVICE_LAUNCHER_PROTOCOL - 1, SERVICE_LAUNCHER_PROTOCOL + 1]) {
          yield* fs.writeFileString(
            statePath,
            `{"protocol":${protocol},"activeVersion":"1.2.4-nightly.1","update":{"status":"unknown"}}`,
          );
          expect(yield* service.status).toMatchObject({
            current: false,
            installedVersion: "1.2.4-nightly.1",
          });
        }
      }),
  );

  // regression: #5302 prevent accidental service downgrades
  it.effect("reports an unknown version for invalid service state", () =>
    Effect.gen(function* () {
      const { service, fs, statePath } = yield* makeHarness();
      yield* service.install();

      for (const stateText of [
        "{",
        '{"activeVersion":"latest"}',
        '{"activeVersion":"1.2"}',
        '{"activeVersion":123}',
      ]) {
        yield* fs.writeFileString(statePath, stateText);
        const status = yield* service.status;
        expect(status.current).toBe(false);
        expect(status.installedVersion).toBeUndefined();
      }
    }),
  );

  it.effect.each(["linux", "darwin"] as const)(
    "preserves a newer version that finishes updating during stop on %s",
    (platform) =>
      Effect.gen(function* () {
        const { service, fs, statePath, commands, control } = yield* makeHarness(platform);
        const plan = yield* service.install();
        const unit = yield* fs.readFileString(plan.unitPath);
        control.stateAfterStop = `{"protocol":${SERVICE_LAUNCHER_PROTOCOL + 1},"activeVersion":"1.2.4"}`;
        commands.length = 0;

        const error = yield* service.install().pipe(Effect.flip);

        expect(error).toMatchObject({
          _tag: "BootServiceDowngradeRefusedError",
          installedVersion: "1.2.4",
          targetVersion: "1.2.3",
        });
        expect(yield* fs.readFileString(statePath)).toBe(control.stateAfterStop);
        expect(yield* fs.readFileString(plan.unitPath)).toBe(unit);
        expect(
          commands.filter(
            (command) =>
              command.startsWith(platform === "linux" ? "systemctl " : "launchctl ") &&
              !command.includes("show-environment"),
          ),
        ).toEqual(
          platform === "linux"
            ? ["systemctl --user stop t3code.service", "systemctl --user restart t3code.service"]
            : [
                "launchctl bootout --wait gui/501/com.t3tools.t3code.service",
                `launchctl bootstrap gui/501 ${plan.unitPath}`,
              ],
        );
      }),
  );

  // regression: #5302 prevent accidental service downgrades
  it.effect("allows an explicit downgrade", () =>
    Effect.gen(function* () {
      const { service, fs, statePath } = yield* makeHarness();
      yield* service.install();
      yield* fs.writeFileString(
        statePath,
        `{"protocol":${SERVICE_LAUNCHER_PROTOCOL},"activeVersion":"1.2.4"}`,
      );

      yield* service.install({ allowDowngrade: true });

      expect(parseServiceState(yield* fs.readFileString(statePath))?.activeVersion).toBe("1.2.3");
      expect((yield* service.status).current).toBe(true);
    }),
  );

  it.effect("repairs versions with equal SemVer precedence without an override", () =>
    Effect.gen(function* () {
      const { service, fs, statePath } = yield* makeHarness();
      yield* service.install();
      yield* fs.writeFileString(
        statePath,
        `{"protocol":${SERVICE_LAUNCHER_PROTOCOL},"activeVersion":"1.2.3+previous-build"}`,
      );

      yield* service.install();

      expect((yield* service.status).current).toBe(true);
    }),
  );

  it.effect.each(["linux", "darwin"] as const)(
    "install with start=false rewrites files, preserves service.env, and marks a restart pending on %s",
    (platform) =>
      Effect.gen(function* () {
        const { service, fs, baseDir, statePath, commands, makeService } =
          yield* makeHarness(platform);
        const path = yield* Path.Path;
        const envPath = path.join(baseDir, SERVICE_ENV_FILE);
        const envBytes = new TextEncoder().encode(
          '# user configuration\r\nT3CODE_TOKEN="literal $HOME # token"',
        );
        yield* fs.writeFile(envPath, envBytes);
        yield* service.install();
        expect(new Uint8Array(yield* fs.readFile(envPath))).toEqual(envBytes);
        yield* service.install();
        expect(new Uint8Array(yield* fs.readFile(envPath))).toEqual(envBytes);
        commands.length = 0;

        const newer = yield* makeService(undefined, "1.2.4");
        const plan = yield* newer.install({ start: false });
        expect(new Uint8Array(yield* fs.readFile(envPath))).toEqual(envBytes);

        expect(parseServiceState(yield* fs.readFileString(statePath))).toEqual({
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.2.4",
        });
        expect(yield* fs.readFileString(plan.unitPath)).toContain("versions/1.2.4/t3");
        expect(
          commands.filter(
            (command) =>
              /^(systemctl|launchctl) /.test(command) && !command.includes("show-environment"),
          ),
        ).toEqual([]);
        const status = yield* newer.status;
        expect(status.current).toBe(false);
        expect(status.problems).toContain("restart-pending");

        commands.length = 0;
        expect(yield* newer.restart).toBe("restarted");
        expect((yield* newer.status).problems).not.toContain("restart-pending");
        expect((yield* newer.status).current).toBe(true);
      }),
  );

  it.effect("install with start=false keeps the marker when a later write fails", () =>
    Effect.gen(function* () {
      const { service, fs, statePath, makeService } = yield* makeHarness();
      const path = yield* Path.Path;
      yield* service.install();
      const newer = yield* makeService(undefined, "1.2.4");
      const unitPath = (yield* service.status).unitPath;
      yield* fs.remove(unitPath);
      yield* fs.makeDirectory(unitPath);
      yield* fs.writeFileString(path.join(unitPath, "occupied"), "");

      const error = yield* newer.install({ start: false }).pipe(Effect.flip);
      expect(error._tag).toBe("BootServiceInstallError");
      expect(
        yield* fs.exists(path.join(path.dirname(statePath), SERVICE_RESTART_PENDING_FILE)),
      ).toBe(true);
    }),
  );

  it.effect("install with start=false refuses while a remote update is pending", () =>
    Effect.gen(function* () {
      const { service, fs, statePath } = yield* makeHarness();
      yield* service.install();
      const pendingState = JSON.stringify({
        protocol: SERVICE_LAUNCHER_PROTOCOL,
        activeVersion: "1.2.3",
        update: {
          id: "u",
          fromVersion: "1.2.3",
          targetVersion: "1.2.4",
          dbPath: "/tmp/state.sqlite",
          status: "pending",
        },
      });
      yield* fs.writeFileString(statePath, pendingState);

      const error = yield* service.install({ start: false }).pipe(Effect.flip);
      expect(error._tag).toBe("BootServiceUpdatePendingError");
      expect(yield* fs.readFileString(statePath)).toBe(pendingState);
    }),
  );

  it.effect("pauses only this home's running service for maintenance, even when it fails", () =>
    Effect.gen(function* () {
      const { service, commands, control, makeService } = yield* makeHarness();
      yield* service.install();
      const failure = yield* service
        .withStopped(Effect.failSync(() => (control.active ? "running" : "repair failed")))
        .pipe(Effect.flip);
      expect(failure).toBe("repair failed");
      expect(control.active).toBe(true);

      control.failCommand = "systemctl --user stop t3code.service";
      let ran = false;
      const stopFailure = yield* service
        .withStopped(
          Effect.sync(() => {
            ran = true;
          }),
        )
        .pipe(Effect.flip);
      expect(stopFailure).toMatchObject({
        _tag: "BootServiceCommandError",
        step: "stopping the installed service",
      });
      expect(ran).toBe(false);
      expect(control.active).toBe(true);
      control.failCommand = undefined;

      control.active = false;
      commands.length = 0;
      yield* service.withStopped(Effect.sync(() => commands.push("repair")));
      expect(commands).toEqual([
        "systemctl --user is-enabled t3code.service",
        "systemctl --user is-active t3code.service",
        "systemctl --user stop t3code.service",
        "repair",
        "systemctl --user restart t3code.service",
      ]);
      expect(control.active).toBe(true);
      control.enabled = false;
      yield* service.withStopped(Effect.void);
      expect(control).toMatchObject({ active: true, enabled: false });
      control.active = false;
      control.enabled = false;
      commands.length = 0;
      yield* service.withStopped(Effect.sync(() => commands.push("repair")));
      expect(commands).toContain("systemctl --user stop t3code.service");
      expect(commands).not.toContain("systemctl --user restart t3code.service");
      expect(control.active).toBe(false);
      control.enabled = true;
      control.active = true;

      const fs = yield* FileSystem.FileSystem;
      const otherHome = yield* fs.makeTempDirectoryScoped({ prefix: "t3-doctor-other-" });
      const other = yield* makeService(undefined, "1.2.3", otherHome);
      commands.length = 0;
      expect(yield* other.withStopped(Effect.sync(() => control.active))).toBe(true);
      expect(commands).toEqual([]);
    }),
  );

  it.effect("restart and install leave a service that serves another T3 home alone", () =>
    Effect.gen(function* () {
      const { service, fs, baseDir, commands, makeService } = yield* makeHarness();
      const plan = yield* service.install();
      const unit = yield* fs.readFileString(plan.unitPath);
      const path = yield* Path.Path;
      const otherHome = yield* fs.makeTempDirectoryScoped({ prefix: "t3-other-home-" });

      const other = yield* makeService(undefined, "1.2.3", path.join(otherHome, ".t3"));
      commands.length = 0;
      expect(yield* other.restart).toBe("skipped");
      const refused = yield* other.install().pipe(Effect.flip);
      expect(refused._tag).toBe("BootServiceOtherBaseDirError");
      // The CLI prints a user message without a stack trace.
      expect(CliError.getUserMessage(refused)).toBe(refused.message);
      expect(yield* fs.readFileString(plan.unitPath)).toBe(unit);
      expect(commands).toEqual([]);
      expect((yield* other.status).otherBaseDir).toBe(baseDir);
    }),
  );

  it.effect("hands the whole install to systemd without changing the running service", () =>
    Effect.gen(function* () {
      const { fs, commands, invocations, control, makeService } = yield* makeHarness();
      const path = yield* Path.Path;
      const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3-handoff-" });
      const baseDir = path.join(home, "T3 Data 100%", "${UNSET}", "$literal");
      const outside = yield* makeService(undefined, "1.2.3", baseDir);
      const plan = yield* outside.install();
      const statePath = path.join(baseDir, "runtime", "service-state.json");
      const state = yield* fs.readFileString(statePath);
      const unit = yield* fs.readFileString(plan.unitPath);
      const inside = yield* makeService(undefined, "1.2.4", baseDir, true);
      commands.length = 0;
      invocations.length = 0;

      const result = yield* inside.install({ allowDowngrade: true });

      expect(yield* fs.readFileString(statePath)).toBe(state);
      expect(yield* fs.readFileString(plan.unitPath)).toBe(unit);
      expect(commands).not.toContain("systemctl --user stop t3code.service");
      expect(result).toMatchObject({ handedOff: true, logPath: plan.logPath });
      const handoffs = invocations.filter(({ command }) => command === "systemd-run");
      expect(handoffs).toHaveLength(1);
      expect(handoffs[0]?.args).toEqual([
        "--user",
        "--collect",
        "--quiet",
        `--property=StandardOutput=append:${plan.logPath}`,
        `--property=StandardError=append:${plan.logPath}`,
        "--setenv=T3_BOOT_SERVICE_UNIT=",
        pinnedRuntimePaths(path, baseDir, "1.2.4", "linux").entryPath,
        "service",
        "install",
        "--base-dir",
        path.join(home, "T3 Data 100%", "$${UNSET}", "$$literal"),
        "--allow-downgrade",
      ]);

      const handoff = handoffs[0]!;
      control.failCommand = `${handoff.command} ${handoff.args.join(" ")}`;
      const error = yield* inside.install({ allowDowngrade: true }).pipe(Effect.flip);
      expect(error).toMatchObject({ _tag: "BootServiceCommandError", exitCode: 1 });
      expect(yield* fs.readFileString(statePath)).toBe(state);
      expect(yield* fs.readFileString(plan.unitPath)).toBe(unit);
      expect(control.active).toBe(true);
      expect(commands).not.toContain("systemctl --user stop t3code.service");
    }),
  );

  it.effect(
    "queues an in-service restart without stopping the caller, even on handoff failure",
    () =>
      Effect.gen(function* () {
        const { service, fs, statePath, commands, control, makeService } = yield* makeHarness();
        const plan = yield* service.install();
        const state = yield* fs.readFileString(statePath);
        const marker = `${plan.baseDir}/runtime/${SERVICE_RESTART_PENDING_FILE}`;
        yield* fs.writeFileString(marker, "1.2.3\n");
        const inside = yield* makeService(undefined, "1.2.3", undefined, true);
        commands.length = 0;
        control.failCommand = "systemctl --user daemon-reload";

        const error = yield* inside.restart.pipe(Effect.flip);
        expect(error).toMatchObject({ _tag: "BootServiceCommandError", exitCode: 1 });
        expect(commands).toEqual(["systemctl --user daemon-reload"]);
        expect(control.active).toBe(true);

        control.failCommand = undefined;
        commands.length = 0;
        const outcome = yield* inside.restart;
        expect(commands).toEqual([
          "systemctl --user daemon-reload",
          "systemctl --user enable t3code.service",
          "systemctl --user restart --no-block t3code.service",
        ]);
        expect(outcome).toBe("queued");
        expect(yield* fs.readFileString(statePath)).toBe(state);
        expect(yield* fs.readFileString(marker)).toBe("1.2.3\n");
      }),
  );

  it.effect("restarts an installed service when repair fails", () =>
    Effect.gen(function* () {
      const { service, commands, control } = yield* makeHarness();
      yield* service.install();
      commands.length = 0;
      control.failCommand = "systemctl --user daemon-reload";

      const error = yield* service.install().pipe(Effect.flip);
      expect(error._tag).toBe("BootServiceCommandError");
      expect(
        commands.filter(
          (command) => command.startsWith("systemctl ") && !command.includes("show-environment"),
        ),
      ).toEqual([
        "systemctl --user stop t3code.service",
        "systemctl --user daemon-reload",
        "systemctl --user restart t3code.service",
      ]);
    }),
  );

  it.effect("restarts without overwriting a pending remote update", () =>
    Effect.gen(function* () {
      const { service, fs, statePath, commands } = yield* makeHarness();
      yield* service.install();
      const pendingState = JSON.stringify({
        protocol: SERVICE_LAUNCHER_PROTOCOL - 1,
        activeVersion: "1.2.3",
        update: {
          id: "remote-update",
          fromVersion: "1.2.3",
          targetVersion: "1.2.4",
          status: "pending",
        },
      });
      yield* fs.writeFileString(statePath, pendingState);
      for (const allowDowngrade of [false, true]) {
        commands.length = 0;

        expect((yield* service.install({ allowDowngrade }).pipe(Effect.flip))._tag).toBe(
          "BootServiceUpdatePendingError",
        );
        expect(serviceStateHasPendingUpdate(yield* fs.readFileString(statePath))).toBe(true);
        expect(
          commands.filter(
            (command) => command.startsWith("systemctl ") && !command.includes("show-environment"),
          ),
        ).toEqual([
          "systemctl --user stop t3code.service",
          "systemctl --user restart t3code.service",
        ]);
      }
    }),
  );

  it.effect("installs, reports current state, and uninstalls on macOS", () =>
    Effect.gen(function* () {
      const { service, fs, statePath, commands, timeouts, runtime } = yield* makeHarness("darwin");
      const path = yield* Path.Path;
      const plan = yield* service.install();

      expect(
        plan.unitPath.endsWith(
          path.join("Library", "LaunchAgents", "com.t3tools.t3code.service.plist"),
        ),
      ).toBe(true);
      expect(yield* fs.readFileString(plan.unitPath)).toContain(
        `    <key>PATH</key>\n    <string>${macInstallerPath}:/usr/local/bin:/usr/sbin:/sbin</string>`,
      );
      expect(parseServiceState(yield* fs.readFileString(statePath))).toEqual({
        protocol: SERVICE_LAUNCHER_PROTOCOL,
        activeVersion: "1.2.3",
      });
      expect(yield* fs.readFileString(plan.unitPath)).toContain(
        `    <string>${runtime.entryPath}</string>\n    <string>__service-launcher</string>`,
      );
      expect(yield* service.status).toMatchObject({
        current: true,
        installedVersion: "1.2.3",
      });
      expect(yield* service.uninstall).toBe(true);
      expect((yield* service.status).installed).toBe(false);
      expect(commands.some((command) => command.startsWith("systemctl "))).toBe(false);
      // launchd can take 90s to bootout; the runner's 60s default could let bootstrap race it.
      expect(timeouts.get("launchctl bootout --wait gui/501/com.t3tools.t3code.service")).toEqual(
        Duration.seconds(120),
      );
    }),
  );

  // regression: #8173 keep provider CLIs available in the macOS service
  it.effect("reconstructs a launch agent search path when the installer has no PATH", () =>
    Effect.gen(function* () {
      const { service, fs } = yield* makeHarness("darwin", "");
      const plan = yield* service.install();

      expect(yield* fs.readFileString(plan.unitPath)).toContain(
        "    <key>PATH</key>\n    <string>/usr/bin:/opt/homebrew/bin:/usr/local/bin:/bin:/usr/sbin:/sbin</string>",
      );
      expect((yield* service.status).current).toBe(true);
    }),
  );

  it.effect("adds missing provider directories to a minimal installer PATH", () =>
    Effect.gen(function* () {
      const { service, fs } = yield* makeHarness("darwin", "/usr/bin:/bin");
      const plan = yield* service.install();

      expect(yield* fs.readFileString(plan.unitPath)).toContain(
        "    <key>PATH</key>\n    <string>/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin:/usr/sbin:/sbin</string>",
      );
      expect((yield* service.status).current).toBe(true);
    }),
  );

  it.effect("keeps an installed launch agent current when the process PATH changes", () =>
    Effect.gen(function* () {
      const { service, makeService } = yield* makeHarness("darwin");
      yield* service.install();

      const restartedService = yield* makeService("/usr/local/bin:/usr/bin:/bin");
      expect((yield* restartedService.status).current).toBe(true);
    }),
  );

  // regression: #17532
  it.effect("puts the runtime an update installs on the launch agent PATH", () =>
    Effect.gen(function* () {
      const { makeService, fs, baseDir, runtime } = yield* makeHarness("darwin");
      const path = yield* Path.Path;
      const previous = runtime.versionDir;
      const installed = pinnedRuntimePaths(path, baseDir, "1.2.4", "darwin").versionDir;
      // The 1.2.3 CLI, or the 1.2.3 service's own PATH, installs 1.2.4.
      const updated = yield* makeService(
        `/usr/bin:${previous}:/bin`,
        "1.2.4",
        undefined,
        false,
        runtime.entryPath,
      );
      const plan = yield* updated.install();

      expect(yield* fs.readFileString(plan.unitPath)).toContain(
        `    <key>PATH</key>\n    <string>/usr/bin:${installed}:/bin:/opt/homebrew/bin:/usr/local/bin:/usr/sbin:/sbin</string>`,
      );
    }),
  );

  it.effect("drops PATH directories that cannot be represented in a launch agent plist", () =>
    Effect.gen(function* () {
      const { service, fs } = yield* makeHarness(
        "darwin",
        "/opt/homebrew/bin:/Users/theo/\u0001invalid:/usr/bin",
      );
      const plan = yield* service.install();
      const plist = yield* fs.readFileString(plan.unitPath);

      expect(plist).toContain(
        "    <key>PATH</key>\n    <string>/opt/homebrew/bin:/usr/bin:/usr/local/bin:/bin:/usr/sbin:/sbin</string>",
      );
      expect(plist).not.toContain("\u0001");
      expect((yield* service.status).current).toBe(true);
    }),
  );

  it.effect("ignores a bootout for an agent that is not loaded", () =>
    Effect.gen(function* () {
      const { service, control } = yield* makeHarness("darwin");
      yield* service.install();
      control.failCommand = "launchctl bootout --wait gui/501/com.t3tools.t3code.service";

      yield* service.install();
      expect((yield* service.status).current).toBe(true);
    }),
  );

  it.effect("keeps failed launch agent installs pending until activation succeeds", () =>
    Effect.gen(function* () {
      const { service, fs, statePath, commands, control } = yield* makeHarness("darwin");
      const path = yield* Path.Path;
      const plan = yield* service.install();
      const markerPath = path.join(path.dirname(statePath), SERVICE_RESTART_PENDING_FILE);
      const bootstrap = `launchctl bootstrap gui/501 ${plan.unitPath}`;
      control.failCommand = bootstrap;

      expect((yield* service.install().pipe(Effect.flip))._tag).toBe("BootServiceCommandError");
      expect(yield* service.status).toMatchObject({
        current: false,
        problems: ["restart-pending"],
      });
      expect(yield* fs.readFileString(markerPath)).toBe("1.2.3\n");

      control.failCommand = undefined;
      commands.length = 0;
      yield* service.install();

      expect(commands).toContain(bootstrap);
      expect(yield* fs.exists(markerPath)).toBe(false);
      expect((yield* service.status).current).toBe(true);
    }),
  );

  it.effect("installs, reports current state, and uninstalls", () =>
    Effect.gen(function* () {
      const { service, fs, statePath, timeouts, runtime } = yield* makeHarness();
      const plan = yield* service.install();

      expect(parseServiceState(yield* fs.readFileString(statePath))).toEqual({
        protocol: SERVICE_LAUNCHER_PROTOCOL,
        activeVersion: "1.2.3",
      });
      expect(plan.program).toEqual([runtime.entryPath, "__service-launcher"]);
      expect(yield* fs.readFileString(plan.unitPath)).toContain(
        `ExecStart=${runtime.entryPath} __service-launcher`,
      );
      expect(yield* service.status).toMatchObject({
        current: true,
        installedVersion: "1.2.3",
      });
      const pendingState = JSON.stringify({
        protocol: SERVICE_LAUNCHER_PROTOCOL,
        activeVersion: "1.2.3",
        update: {
          id: "u",
          fromVersion: "1.2.3",
          targetVersion: "1.2.4",
          dbPath: "/tmp/state.sqlite",
          status: "pending",
        },
      });
      yield* fs.writeFileString(statePath, pendingState);
      expect((yield* service.status).current).toBe(false);
      expect(yield* service.uninstall).toBe(true);
      expect((yield* service.status).installed).toBe(false);
      // systemd can take 90s to stop, exceeding the runner's 60s default.
      expect(timeouts.get("systemctl --user disable --now t3code.service")).toEqual(
        Duration.seconds(120),
      );
    }),
  );
});

it("renders Jones identity with a deterministic escaped service environment", () => {
  const environment = { Z_VALUE: '100% "quoted" \\ path & <tag>', A_VALUE: "first" };
  const plan = { ...linuxPlan, environment };
  const unit = BootService.renderBootServiceUnit(plan, JONES_BOOT_SERVICE_IDENTITY);
  expect(unit).toContain("Description=Jones Code server");
  expect(unit).toContain("Environment=T3_BOOT_SERVICE_UNIT=jones-code.service");
  expect(unit).toContain('Environment=Z_VALUE="100%% \\"quoted\\" \\\\ path & <tag>"');
  expect(unit.indexOf("Environment=A_VALUE=")).toBeLessThan(unit.indexOf("Environment=Z_VALUE="));
  expect(
    BootService.renderBootServiceUnit(
      {
        ...plan,
        environment: {
          A_VALUE: environment.A_VALUE,
          Z_VALUE: environment.Z_VALUE,
        },
      },
      JONES_BOOT_SERVICE_IDENTITY,
    ),
  ).toBe(unit);
  expect(BootService.bootServiceBaseDirOf(unit)).toBe(plan.baseDir);

  const plist = BootService.renderBootServicePlist(
    plan,
    macRenderOptions,
    JONES_BOOT_SERVICE_IDENTITY,
  );
  expect(plist).toContain("<string>com.jones-systems.jones-code.service</string>");
  expect(plist).toContain("<string>com.jones-systems.jones-code.service.plist</string>");
  expect(plist).toContain('<string>100% "quoted" \\ path &amp; &lt;tag&gt;</string>');
  expect(plist.indexOf("<key>A_VALUE</key>")).toBeLessThan(plist.indexOf("<key>Z_VALUE</key>"));
  expect(BootService.renderBootServiceUnit({ ...linuxPlan, environment: {} })).toBe(
    BootService.renderBootServiceUnit(linuxPlan),
  );
  expect(
    BootService.renderBootServicePlist({ ...macPlan, environment: {} }, macRenderOptions),
  ).toBe(BootService.renderBootServicePlist(macPlan, macRenderOptions));
});

it.layer(NodeServices.layer)("Jones boot service identity", (it) => {
  it.effect("queues an in-service Jones restart and repairs only its selected unit", () =>
    Effect.gen(function* () {
      const { service, commands, makeService, control } = yield* makeHarness(
        "linux",
        macInstallerPath,
        undefined,
        undefined,
        {
          identity: JONES_BOOT_SERVICE_IDENTITY,
        },
      );
      yield* service.install();
      commands.length = 0;
      const inside = yield* makeService(undefined, "1.2.3", undefined, true);
      expect(yield* inside.restart).toBe("queued");
      expect(commands).toEqual([
        UNIT_PATH_READ,
        "systemctl --user daemon-reload",
        "systemctl --user enable jones-code.service",
        "systemctl --user restart --no-block jones-code.service",
      ]);
      commands.length = 0;
      control.active = true;
      yield* service.withStopped(Effect.sync(() => commands.push("repair")));
      expect(commands).toContain("systemctl --user is-enabled jones-code.service");
      expect(commands).toContain("systemctl --user is-active jones-code.service");
      expect(commands).toContain("systemctl --user stop jones-code.service");
      expect(commands).toContain("systemctl --user restart jones-code.service");
      expect(commands.indexOf("repair")).toBeGreaterThan(
        commands.indexOf("systemctl --user stop jones-code.service"),
      );
      expect(commands.some((command) => command.includes("t3code.service"))).toBe(false);
    }),
  );

  it.effect.each(["linux", "darwin"] as const)(
    "controls only the Jones service and preserves upstream units on %s",
    (platform) =>
      Effect.gen(function* () {
        const { service, fs, commands } = yield* makeHarness(
          platform,
          macInstallerPath,
          undefined,
          undefined,
          {
            identity: JONES_BOOT_SERVICE_IDENTITY,
            environment: {
              T3CODE_HOST: "127.0.0.1",
              T3CODE_PORT: "4321",
              T3CODE_TAILSCALE_SERVE: "false",
            },
          },
        );
        const path = yield* Path.Path;
        const status = yield* service.status;
        const upstreamPath = path.join(
          path.dirname(status.unitPath),
          platform === "linux" ? "t3code.service" : "com.t3tools.t3code.service.plist",
        );
        yield* fs.makeDirectory(path.dirname(upstreamPath), { recursive: true });
        yield* fs.writeFileString(upstreamPath, "upstream-owned-unit");
        const plan = yield* service.install();
        expect(plan.unitPath).toBe(status.unitPath);
        expect((yield* service.status).current).toBe(true);
        yield* service.restart;
        yield* service.uninstall;
        expect(yield* fs.readFileString(upstreamPath)).toBe("upstream-owned-unit");
        expect(commands.some((command) => command.includes("t3code.service"))).toBe(false);
        expect(
          commands.some((command) =>
            command.includes(
              platform === "linux" ? "jones-code.service" : "com.jones-systems.jones-code.service",
            ),
          ),
        ).toBe(true);
      }),
  );

  it.effect("preserves runtime-policy recovery text without controlling the service", () =>
    Effect.gen(function* () {
      const { service, fs, commands, runtime } = yield* makeHarness(
        "linux",
        macInstallerPath,
        undefined,
        undefined,
        {
          identity: JONES_BOOT_SERVICE_IDENTITY,
        },
      );
      const path = yield* Path.Path;
      yield* fs.remove(path.join(runtime.versionDir, JONES_RUNTIME_PROVENANCE_FILE));
      const error = yield* service.install().pipe(Effect.flip);
      expect(error._tag).toBe("BootServiceInstallError");
      expect(error.message).toContain("t3 jones host stage-runtime");
      expect(
        commands.some((command) => command.includes("restart") || command.includes("stop")),
      ).toBe(false);
      expect(yield* fs.readFileString(runtime.entryPath)).toBe("#!/bin/sh\n");
    }),
  );

  it.effect("refuses implicit linger enable before runtime checks or unit writes", () =>
    Effect.gen(function* () {
      const { service, fs, commands, control } = yield* makeHarness(
        "linux",
        macInstallerPath,
        undefined,
        undefined,
        {
          identity: JONES_BOOT_SERVICE_IDENTITY,
          allowEnableLinger: false,
        },
      );
      const status = yield* service.status;
      control.linger = "no";
      const error = yield* service.install().pipe(Effect.flip);
      expect(error).toMatchObject({
        _tag: "BootServicePrerequisiteError",
        problem: "linger-disabled",
      });
      expect(commands.some((command) => command.includes("enable-linger"))).toBe(false);
      expect(commands.some((command) => command.includes("--version"))).toBe(false);
      expect(yield* fs.exists(status.unitPath)).toBe(false);
    }),
  );
});

it.layer(NodeServices.layer)("qualified boot service setup boundary", (it) => {
  it.effect("blocks a version-only preview before probes, downloads or service stops", () =>
    Effect.gen(function* () {
      const { makeService, fs, statePath, commands } = yield* makeHarness();
      const preview = yield* makeService(undefined, "0.0.0-preview.20261002.100.1");
      expect(commands).toEqual([UNIT_PATH_READ, UNIT_PATH_READ]);
      const error = yield* preview.install({ allowDowngrade: true }).pipe(Effect.flip);
      expect(error).toMatchObject({
        _tag: "BootServiceBootstrapRequiredError",
        reason: "qualified-activation-required",
      });
      expect(commands).toEqual([UNIT_PATH_READ, UNIT_PATH_READ]);
      expect(yield* fs.exists(statePath)).toBe(false);
    }),
  );

  it.effect(
    "refuses replacing an installed Jones source with a stable version even with allow-downgrade",
    () =>
      Effect.gen(function* () {
        const { service, fs, statePath, commands } = yield* makeHarness();
        yield* service.install();
        const previewState = encodeNativeFixtureJson({
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "0.0.0-preview.20261002.100.1",
        });
        yield* fs.writeFileString(statePath, previewState);
        commands.length = 0;
        const error = yield* service
          .install({ allowDowngrade: true, start: false })
          .pipe(Effect.flip);
        expect(error._tag).toBe("BootServiceBootstrapRequiredError");
        expect(commands).toEqual([]);
        expect(yield* fs.readFileString(statePath)).toBe(previewState);
      }),
  );

  it.effect.each([undefined, "verified-private-artifact"] as const)(
    "retains an exact existing qualified install as a read-only no-op in %s mode",
    (runtimeMode) =>
      Effect.gen(function* () {
        const { makeService, fs, statePath, commands } = yield* makeHarness(
          "linux",
          macInstallerPath,
          undefined,
          undefined,
          runtimeMode === undefined ? {} : { runtimeMode },
        );
        const path = yield* Path.Path;
        const arch = "x64";
        const version = "0.0.0-preview.20261002.100.1";
        const preview = yield* makeService(undefined, version);
        const baseDir = path.dirname(path.dirname(statePath));
        const runtime = pinnedRuntimePaths(path, baseDir, version, "linux");
        yield* fs.chmod(runtime.entryPath, 0o700);
        yield* fs.remove(path.join(runtime.versionDir, JONES_RUNTIME_PROVENANCE_FILE));
        const status = yield* preview.status;
        const plan = {
          program: [runtime.entryPath, "__service-launcher"],
          baseDir,
          logPath: status.logPath,
          unitPath: status.unitPath,
        };
        yield* fs.makeDirectory(path.dirname(status.unitPath), { recursive: true });
        const unit = BootService.renderBootServiceUnit(plan);
        const state = encodeNativeFixtureJson({
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: version,
        });
        yield* fs.writeFileString(status.unitPath, unit);
        yield* fs.writeFileString(statePath, state);
        yield* fs.writeFileString(
          path.join(runtime.versionDir, QUALIFIED_RUNTIME_RECEIPT),
          encodeNativeFixtureJson({
            protocol: 1,
            repository: "Jones-Systems/Jones-Code",
            channel: "jones-main",
            version,
            sourceSha: "a".repeat(40),
            sourceTree: "b".repeat(40),
            installedSourceSha: "c".repeat(40),
            runId: 100,
            runAttempt: 1,
            artifactId: 101,
            workflow: ".github/workflows/artifact-cli-linux.yml",
            artifactDigest: `sha256:${"d".repeat(64)}`,
            archiveSha256: "e".repeat(64),
            platform: "linux",
            architecture: arch,
            payloadSha256: yield* Effect.promise(() => qualifiedPayloadDigest(runtime.versionDir)),
          }),
          { mode: 0o600 },
        );
        commands.length = 0;
        expect(yield* preview.install({ start: false })).toEqual(plan);
        expect(commands).toEqual([]);
        expect(yield* fs.readFileString(statePath)).toBe(state);
        expect(yield* fs.readFileString(status.unitPath)).toBe(unit);
      }),
  );

  it.effect("preserves desktop native ownership before any generic service setup side effect", () =>
    Effect.gen(function* () {
      const { service, fs, statePath, commands } = yield* makeHarness();
      expect(commands).toEqual([UNIT_PATH_READ]);
      const marker = statePath.replace("service-state.json", "jones-active-install.json");
      yield* fs.writeFileString(
        marker,
        encodeNativeFixtureJson({ owner: "desktop", home: "native-fixture" }),
      );
      const error = yield* service.install().pipe(Effect.flip);
      expect(error).toMatchObject({
        _tag: "BootServiceBootstrapRequiredError",
        reason: "desktop-owned-home",
      });
      expect(commands).toEqual([UNIT_PATH_READ]);
      expect(yield* fs.exists(statePath)).toBe(false);
      expect(decodeNativeFixtureJson(yield* fs.readFileString(marker))).toMatchObject({
        owner: "desktop",
      });
    }),
  );
});

const decodeEffectFromJsonStringJonesRuntimeProvenance = Schema.decodeEffect(
  Schema.fromJsonString(JonesRuntimeProvenance),
);

it.layer(NodeServices.layer)("verified private artifact boot service", (it) => {
  const version = "1.2.4-preview.20261008.101.1";
  const privateOptions = {
    identity: JONES_BOOT_SERVICE_IDENTITY,
    runtimeMode: "verified-private-artifact" as const,
    allowEnableLinger: false,
  };

  it.effect("installs and repeats a cached preview without downloads or archive extraction", () =>
    Effect.gen(function* () {
      const { makeService, commands, fs, statePath } = yield* makeHarness(
        "linux",
        macInstallerPath,
        undefined,
        undefined,
        privateOptions,
      );
      const preview = yield* makeService(undefined, version);
      const plan = yield* preview.install();
      expect(plan.unitPath).toContain("jones-code.service");
      expect(parseServiceState(yield* fs.readFileString(statePath))?.activeVersion).toBe(version);
      expect(commands.slice(0, 3)).toEqual([
        UNIT_PATH_READ,
        UNIT_PATH_READ,
        `${plan.program[0]} --version`,
      ]);
      expect((yield* preview.status).current).toBe(true);
      commands.length = 0;
      yield* preview.install();
      expect(commands[0]).toBe(`${plan.program[0]} --version`);
      expect(commands).not.toContain(UNIT_PATH_READ);
      expect(
        commands.some((command) => command.startsWith("tar ") || command.includes("enable-linger")),
      ).toBe(false);
      expect((yield* preview.status).current).toBe(true);
    }),
  );

  it.effect.each([
    "desktop",
    "target-qualified",
    "active-qualified",
    "tampered",
    "missing-provenance",
    "missing-executable",
    "missing-marker",
    "wrong-marker",
    "wrong-platform",
    "wrong-version-output",
    "incomplete-output",
    "unverified-active",
  ] as const)("refuses %s before service manager effects or unit writes", (failure) =>
    Effect.gen(function* () {
      const { makeService, commands, fs, statePath, runtime, control } = yield* makeHarness(
        "linux",
        macInstallerPath,
        undefined,
        undefined,
        privateOptions,
      );
      const path = yield* Path.Path;
      const baseDir = path.dirname(path.dirname(statePath));
      const preview = yield* makeService(undefined, version);
      const paths = pinnedRuntimePaths(path, baseDir, version, "linux");
      const status = yield* preview.status;
      if (failure === "desktop") {
        yield* fs.writeFileString(path.join(baseDir, "runtime", "jones-active-install.json"), "{}");
      } else if (failure === "target-qualified") {
        yield* fs.writeFileString(path.join(paths.versionDir, QUALIFIED_RUNTIME_RECEIPT), "{}");
      } else if (failure === "active-qualified" || failure === "unverified-active") {
        yield* fs.writeFileString(
          statePath,
          encodeNativeFixtureJson({
            protocol: SERVICE_LAUNCHER_PROTOCOL,
            activeVersion: "1.2.3",
          }),
        );
        if (failure === "active-qualified") {
          yield* fs.writeFileString(path.join(runtime.versionDir, QUALIFIED_RUNTIME_RECEIPT), "{}");
        } else {
          yield* fs.remove(path.join(runtime.versionDir, JONES_RUNTIME_PROVENANCE_FILE));
        }
      } else if (failure === "tampered") {
        yield* fs.writeFileString(paths.entryPath, "tampered executable");
      } else if (failure === "missing-provenance") {
        yield* fs.remove(path.join(paths.versionDir, JONES_RUNTIME_PROVENANCE_FILE));
      } else if (failure === "missing-executable") {
        yield* fs.remove(paths.entryPath);
      } else if (failure === "missing-marker") {
        yield* fs.remove(paths.sentinelPath);
      } else if (failure === "wrong-marker") {
        yield* fs.writeFileString(paths.sentinelPath, "0.0.44");
      } else if (failure === "wrong-platform") {
        const provenance = yield* decodeEffectFromJsonStringJonesRuntimeProvenance(
          yield* fs.readFileString(path.join(paths.versionDir, JONES_RUNTIME_PROVENANCE_FILE)),
        );
        yield* fs.writeFileString(
          path.join(paths.versionDir, JONES_RUNTIME_PROVENANCE_FILE),
          yield* encodeJonesRuntimeProvenance({ ...provenance, platform: "darwin" }),
        );
      } else if (failure === "wrong-version-output") {
        control.versionOutput = "t3 v0.0.44\n";
      } else {
        control.versionIncomplete = true;
      }
      commands.length = 0;
      const error = yield* preview.install().pipe(Effect.flip);
      expect(error._tag).toBe(
        ["desktop", "target-qualified", "active-qualified"].includes(failure)
          ? "BootServiceBootstrapRequiredError"
          : "BootServiceInstallError",
      );
      expect(commands.every((command) => command.endsWith(" --version"))).toBe(true);
      expect(yield* fs.exists(status.unitPath)).toBe(false);
      if (failure === "missing-executable") {
        expect(yield* fs.exists(paths.entryPath)).toBe(false);
      } else {
        expect(yield* fs.readFileString(paths.entryPath)).toBe(
          failure === "tampered" ? "tampered executable" : "#!/bin/sh\n",
        );
      }
    }),
  );

  it.effect(
    "verifies both the active private runtime and the target before switching previews",
    () =>
      Effect.gen(function* () {
        const { makeService, commands, fs, statePath, runtime } = yield* makeHarness(
          "linux",
          macInstallerPath,
          undefined,
          undefined,
          privateOptions,
        );
        const current = yield* makeService();
        yield* current.install();
        const preview = yield* makeService(undefined, version);
        commands.length = 0;
        const plan = yield* preview.install();
        expect(commands.slice(0, 2)).toEqual([
          `${runtime.entryPath} --version`,
          `${plan.program[0]} --version`,
        ]);
        expect(parseServiceState(yield* fs.readFileString(statePath))?.activeVersion).toBe(version);
      }),
  );
});

it.layer(NodeServices.layer)("stable launcher pointer boot service", (it) => {
  it.effect(
    "preserves an enrolled stable launcher pointer when rendering an existing private service",
    () =>
      Effect.gen(function* () {
        const f = yield* makeHarness("linux", macInstallerPath, undefined, undefined, {
          runtimeMode: "verified-private-artifact",
        });
        const base = yield* Effect.promise(() => NodeFSP.realpath(f.baseDir));
        const version = "0.1.2-preview.20261026.100";
        const directory = `${base}/runtime/versions/${version}`;
        yield* f.fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
        yield* f.fs.writeFileString(`${directory}/t3`, "#!/bin/sh\n", { mode: 0o700 });
        yield* f.fs.writeFileString(
          `${directory}/${QUALIFIED_RUNTIME_RECEIPT}`,
          encodeNativeFixtureJson({
            protocol: 1,
            repository: "Jones-Systems/Jones-Code",
            channel: "jones-main",
            version,
            sourceSha: "a".repeat(40),
            sourceTree: "b".repeat(40),
            installedSourceSha: "c".repeat(40),
            runId: 100,
            runAttempt: 1,
            artifactId: 101,
            workflow: ".github/workflows/artifact-cli-linux.yml",
            artifactDigest: `sha256:${"d".repeat(64)}`,
            archiveSha256: "e".repeat(64),
            platform: "linux",
            architecture: "x64",
            payloadSha256: yield* Effect.promise(() => qualifiedPayloadDigest(directory)),
          }),
          { mode: 0o600 },
        );
        const pointer = yield* Effect.promise(() =>
          bootstrapLauncherPointer(base, version, "retained service definition"),
        );
        const service = yield* f.makeService(macInstallerPath, "1.2.3", base);
        const installed = yield* service.install();
        expect(installed.program).toEqual([pointer.program, "__service-launcher"]);
        expect(yield* f.fs.readFileString(installed.unitPath)).toContain(pointer.program);
        expect(
          yield* Effect.promise(() => NodeFSP.readlink(`${base}/runtime/launcher/current`)),
        ).toBe(`../versions/${version}`);
        f.commands.length = 0;
        yield* f.fs.writeFileString(`${directory}/t3`, "tampered launcher");
        const refused = yield* f.makeService(macInstallerPath, "1.2.3", base).pipe(Effect.flip);
        expect(refused._tag).toBe("BootServiceInstallError");
        expect(f.commands).toEqual([]);
      }),
  );
});
