// @effect-diagnostics-next-line nodeBuiltinImport:off - Synchronous fixture provenance hashes must match the staged executable bytes.
import * as NodeCrypto from "node:crypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  HostProcessArchitecture,
  HostProcessExecutablePath,
  HostProcessPlatform,
  HostProcessUserId,
} from "@t3tools/shared/hostProcess";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { cliArchiveFileName, cliArchivePlatformKey } from "@t3tools/shared/cliRelease";
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
  // Spaces and specifiers are quoted and escaped on the way in.
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

it("survives the kernel OOM-killing a greedy agent child", () => {
  const unit = BootService.renderBootServiceUnit(linuxPlan);

  expect(unit).toContain("OOMPolicy=continue");
});

const macRuntime = "/Users/theo/.t3/runtime/versions/1.2.3/t3";
const macPlan = {
  program: [macRuntime, "__service-launcher"],
  baseDir: "/Users/theo/.t3",
  logPath: "/Users/theo/.t3/userdata/logs/boot-service.log",
  unitPath: "/Users/theo/Library/LaunchAgents/com.t3tools.t3code.service.plist",
};
const macInstallerPath =
  "/opt/homebrew/bin:/Users/theo/.npm-global/bin:/Users/theo/.nvm/versions/node/v22.16.0/bin:/usr/bin:/bin";
const macRenderOptions = { homeDir: "/Users/theo", environmentPath: macInstallerPath };

it("runs the pinned runtime's own executable as the launch agent", () => {
  const plist = BootService.renderBootServicePlist(macPlan, macRenderOptions);

  expect(plist).toContain(
    `  <array>\n    <string>${macRuntime}</string>\n    <string>__service-launcher</string>\n  </array>`,
  );
  expect(plist).not.toContain("node</string>");
});

it("preserves the installer's provider search path in the launch agent", () => {
  const plist = BootService.renderBootServicePlist(macPlan, macRenderOptions);

  expect(plist).toContain(`    <key>PATH</key>\n    <string>${macInstallerPath}</string>`);
});

it("restarts the launch agent on the systemd cadence", () => {
  const plist = BootService.renderBootServicePlist(macPlan, macRenderOptions);

  expect(plist).toContain("<key>RunAtLoad</key>\n  <true/>");
  expect(plist).toContain("<key>KeepAlive</key>\n  <true/>");
  expect(plist).toContain("<key>ThrottleInterval</key>\n  <integer>5</integer>");
  expect(plist).toContain("<key>ExitTimeOut</key>\n  <integer>90</integer>");
});

it("appends both stdio streams to the boot service log", () => {
  const plist = BootService.renderBootServicePlist(macPlan, macRenderOptions);

  expect(plist).toContain(
    "<key>StandardOutPath</key>\n  <string>/Users/theo/.t3/userdata/logs/boot-service.log</string>",
  );
  expect(plist).toContain(
    "<key>StandardErrorPath</key>\n  <string>/Users/theo/.t3/userdata/logs/boot-service.log</string>",
  );
});

it("escapes XML in host paths", () => {
  const plist = BootService.renderBootServicePlist(
    { ...macPlan, baseDir: "/Users/theo/T3 & <Co>" },
    { homeDir: "/Users/theo", environmentPath: "/Users/theo/Tools & <Scripts>:/usr/bin" },
  );

  expect(plist).toContain("<string>/Users/theo/T3 &amp; &lt;Co&gt;</string>");
  expect(plist).toContain("<string>/Users/theo/Tools &amp; &lt;Scripts&gt;:/usr/bin</string>");
});

const makeHarness = Effect.fn("test.make_boot_service_harness")(function* (
  platform: NodeJS.Platform = "linux",
  installerPath = macInstallerPath,
  serviceOptions: {
    readonly identity?: BootService.BootServiceIdentity;
    readonly environment?: Readonly<Record<string, string>>;
    readonly allowEnableLinger?: boolean;
    readonly runtimeMode?: "verified-private-artifact";
  } = {},
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3-boot-service-test-" });
  const baseDir = path.join(home, ".t3");
  const arch = platform === "darwin" ? "arm64" : "x64";
  const statePath = path.join(baseDir, "runtime", "service-state.json");
  // A complete pinned runtime is already present, so install only validates
  // it and never downloads a release archive.
  const runtime = pinnedRuntimePaths(path, baseDir, "1.2.3", platform);
  yield* fs.makeDirectory(path.dirname(runtime.entryPath), { recursive: true });
  yield* fs.writeFileString(runtime.entryPath, "#!/bin/sh\n");
  yield* fs.writeFileString(runtime.sentinelPath, "1.2.3\n");

  const unitFile = serviceOptions.identity?.systemdUnitFile ?? "t3code.service";
  const commands: string[] = [];
  const timeouts = new Map<string, unknown>();
  const control: {
    failCommand: string | undefined;
    stateAfterStop?: string;
    linger: string;
    enabled: boolean;
    active: boolean;
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
      commands.push(command);
      timeouts.set(command, input.timeout);
      const failed = command === control.failCommand;
      if (!failed && command === "loginctl enable-linger --no-ask-password 501")
        control.linger = "yes";
      if (!failed && command === `systemctl --user enable ${unitFile}`) control.enabled = true;
      if (!failed && command === `systemctl --user restart ${unitFile}`) control.active = true;
      if (
        control.stateAfterStop !== undefined &&
        (command === `systemctl --user stop ${unitFile}` ||
          command.startsWith("launchctl bootout --wait "))
      ) {
        yield* fs.writeFileString(statePath, control.stateAfterStop).pipe(Effect.orDie);
      }
      return {
        stdout:
          input.args[0] === "--version"
            ? // The runtime under test reports the version of the directory it
              // was launched from, like the real executable.
              (control.versionOutput ??
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
          failed || (input.args[1] === "is-active" && !control.active) ? 1 : 0,
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
  ) =>
    Effect.gen(function* () {
      // Every version the tests install is present and verified on disk, so
      // install never downloads.
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
        baseDir: serviceBaseDir,
        logsDir: path.join(serviceBaseDir, "userdata", "logs"),
        cliVersion,
        host: { execPath: "/usr/bin/t3" },
        ...serviceOptions,
      });
    }).pipe(
      Effect.provideService(ProcessRunner.ProcessRunner, runner),
      Effect.provide(
        Layer.mergeAll(
          Layer.succeed(HostProcessPlatform, platform),
          Layer.succeed(HostProcessArchitecture, arch),
          Layer.succeed(HostProcessUserId, 501),
          Layer.succeed(HostProcessExecutablePath, "/usr/bin/t3"),
          Layer.succeed(
            HttpClient.HttpClient,
            HttpClient.make(() => Effect.die("no release download expected")),
          ),
          ConfigProvider.layer(
            ConfigProvider.fromEnv({
              env: {
                HOME: home,
                ...(environmentPath === undefined || environmentPath === ""
                  ? {}
                  : { PATH: environmentPath }),
              },
            }),
          ),
        ),
      ),
    );
  const service = yield* makeService();
  return { service, makeService, fs, statePath, commands, timeouts, control, runtime };
});

it.layer(NodeServices.layer)("boot service install", (it) => {
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
      // The stop can block up to systemd's 90s TimeoutStopSec; the runner's
      // 60s default would cancel it mid-shutdown.
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

  it.effect("install with start=false rewrites the files and marks a restart pending", () =>
    Effect.gen(function* () {
      const { service, fs, statePath, commands, makeService } = yield* makeHarness();
      yield* service.install();
      commands.length = 0;

      const newer = yield* makeService(undefined, "1.2.4");
      const plan = yield* newer.install({ start: false });

      expect(parseServiceState(yield* fs.readFileString(statePath))).toEqual({
        protocol: SERVICE_LAUNCHER_PROTOCOL,
        activeVersion: "1.2.4",
      });
      expect(yield* fs.readFileString(plan.unitPath)).toContain("versions/1.2.4/t3");
      expect(
        commands.filter(
          (command) => command.startsWith("systemctl ") && !command.includes("show-environment"),
        ),
      ).toEqual([]);
      // The files say 1.2.4 but the process is still 1.2.3: not current, and
      // the reason is named so `t3 service status` can point at restart.
      const status = yield* newer.status;
      expect(status.current).toBe(false);
      expect(status.problems).toContain("restart-pending");

      commands.length = 0;
      expect(yield* newer.restart).toBe(true);
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
      // A non-empty directory in the unit's place: it still counts as an
      // installed unit, and the rename that writes the new unit fails.
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

  it.effect("restart stops and starts an installed service, and is a no-op otherwise", () =>
    Effect.gen(function* () {
      const { service, commands } = yield* makeHarness();
      expect(yield* service.restart).toBe(false);
      yield* service.install();
      commands.length = 0;

      expect(yield* service.restart).toBe(true);
      expect(
        commands.filter(
          (command) => command.startsWith("systemctl ") && !command.includes("show-environment"),
        ),
      ).toEqual([
        "systemctl --user stop t3code.service",
        "systemctl --user daemon-reload",
        "systemctl --user enable t3code.service",
        "systemctl --user restart t3code.service",
      ]);
    }),
  );

  it.effect("restart leaves a service that serves another T3 home alone", () =>
    Effect.gen(function* () {
      const { service, fs, commands, makeService } = yield* makeHarness();
      yield* service.install();
      commands.length = 0;
      const path = yield* Path.Path;
      const otherHome = yield* fs.makeTempDirectoryScoped({ prefix: "t3-other-home-" });

      const other = yield* makeService(undefined, "1.2.3", path.join(otherHome, ".t3"));
      expect(yield* other.restart).toBe(false);
      expect(commands.filter((command) => command.startsWith("systemctl "))).toEqual([]);
    }),
  );

  it.effect("restart brings the service back when activation fails", () =>
    Effect.gen(function* () {
      const { service, commands, control } = yield* makeHarness();
      yield* service.install();
      commands.length = 0;
      control.failCommand = "systemctl --user daemon-reload";

      const error = yield* service.restart.pipe(Effect.flip);
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

  it.effect("fails closed on Windows", () =>
    Effect.gen(function* () {
      const { service } = yield* makeHarness("win32");
      expect((yield* service.status).supported).toBe(false);
      expect((yield* service.install().pipe(Effect.flip))._tag).toBe("BootServiceUnsupportedError");
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
      // A bootout can block up to the plist's 90s ExitTimeOut; the runner's
      // 60s default would cancel it and let bootstrap race a loaded job.
      expect(timeouts.get("launchctl bootout --wait gui/501/com.t3tools.t3code.service")).toEqual(
        Duration.seconds(120),
      );
    }),
  );

  it.effect("restarts the launch agent when repair fails", () =>
    Effect.gen(function* () {
      const { service, commands, control } = yield* makeHarness("darwin");
      yield* service.install();
      const plistPath = (yield* service.status).unitPath;
      commands.length = 0;
      control.failCommand = `launchctl bootstrap gui/501 ${plistPath}`;

      const error = yield* service.install().pipe(Effect.flip);
      expect(error._tag).toBe("BootServiceCommandError");
      expect(commands.filter((command) => command.startsWith("launchctl "))).toEqual([
        "launchctl bootout --wait gui/501/com.t3tools.t3code.service",
        "launchctl enable gui/501/com.t3tools.t3code.service",
        `launchctl bootstrap gui/501 ${plistPath}`,
        `launchctl bootstrap gui/501 ${plistPath}`,
      ]);
    }),
  );

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

  it.effect("restarts without overwriting a pending remote update on macOS", () =>
    Effect.gen(function* () {
      const { service, fs, statePath, commands } = yield* makeHarness("darwin");
      yield* service.install();
      const plistPath = (yield* service.status).unitPath;
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
        expect(commands.filter((command) => command.startsWith("launchctl "))).toEqual([
          "launchctl bootout --wait gui/501/com.t3tools.t3code.service",
          `launchctl bootstrap gui/501 ${plistPath}`,
        ]);
      }
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
  it.effect.each(["linux", "darwin"] as const)(
    "controls only the Jones service and preserves upstream units on %s",
    (platform) =>
      Effect.gen(function* () {
        const { service, fs, commands } = yield* makeHarness(platform, macInstallerPath, {
          identity: JONES_BOOT_SERVICE_IDENTITY,
          environment: {
            T3CODE_HOST: "127.0.0.1",
            T3CODE_PORT: "4321",
            T3CODE_TAILSCALE_SERVE: "false",
          },
        });
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
      const { service, fs, commands, runtime } = yield* makeHarness("linux", macInstallerPath, {
        identity: JONES_BOOT_SERVICE_IDENTITY,
      });
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
      const { service, fs, commands, control } = yield* makeHarness("linux", macInstallerPath, {
        identity: JONES_BOOT_SERVICE_IDENTITY,
        allowEnableLinger: false,
      });
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
      const error = yield* preview.install({ allowDowngrade: true }).pipe(Effect.flip);
      expect(error._tag).toBe("BootServiceBootstrapRequiredError");
      expect(commands).toEqual([]);
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
          {
            ...(runtimeMode === undefined ? {} : { runtimeMode }),
          },
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
      expect(commands).toEqual([]);
      expect(yield* fs.exists(statePath)).toBe(false);
      expect(decodeNativeFixtureJson(yield* fs.readFileString(marker))).toMatchObject({
        owner: "desktop",
      });
    }),
  );
});

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
        privateOptions,
      );
      const preview = yield* makeService(undefined, version);
      const plan = yield* preview.install();
      expect(plan.unitPath).toContain("jones-code.service");
      expect(parseServiceState(yield* fs.readFileString(statePath))?.activeVersion).toBe(version);
      expect(commands[0]).toBe(`${plan.program[0]} --version`);
      expect((yield* preview.status).current).toBe(true);
      commands.length = 0;
      yield* preview.install();
      expect(commands[0]).toBe(`${plan.program[0]} --version`);
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
        const provenance = yield* Schema.decodeUnknownEffect(
          Schema.fromJsonString(JonesRuntimeProvenance),
        )(yield* fs.readFileString(path.join(paths.versionDir, JONES_RUNTIME_PROVENANCE_FILE)));
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
