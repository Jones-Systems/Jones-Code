import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as ProcessRunner from "../../processRunner.ts";
import { SERVICE_LAUNCHER_PROTOCOL } from "../../cloud/serviceProtocol.ts";
import { LINUX_PRIORITY_DROPIN, LINUX_PRIORITY_POLICY } from "./linuxPriorityPolicy.ts";
import * as SystemBootService from "./systemBootService.ts";
import {
  OWNED_SYSTEM_DROPIN,
  sha256,
  systemUnitDigest,
  systemUnitRecord,
} from "./systemServiceControl.ts";

it.each([
  ["0::/hostdev.slice/host-tooling-t3.service\n", "host-tooling-t3.service"],
  ["0::/system.slice/t3code.service/child\n", "t3code.service"],
  ["0::/user.slice/user-1000.slice/user@1000.service/app.slice/t3code.service\n", undefined],
  ["0::/user.slice/user-1000.slice/session-1.scope\n", undefined],
  ["0::/system.slice/a.service\n0::/system.slice/b.service\n", undefined],
  ["1:name=systemd:/system.slice/t3code.service\n", undefined],
])("resolves unified system scope from %s", (contents, expected) => {
  expect(SystemBootService.systemServiceUnitFromCgroup(contents)).toBe(expected);
});

const fixture = Effect.fn("systemBootService.test.fixture")(function* (options?: {
  readonly nice?: string;
  readonly scope?: string;
  readonly status?: string;
  readonly lateDropin?: string;
  readonly priorityMode?: number;
  readonly priorityGid?: number;
  readonly beforePriority?: boolean;
  readonly fragmentDrift?: boolean;
  readonly fragmentUid?: number;
  readonly priorityBytes?: string;
  readonly foreignDropin?: boolean;
  readonly execStart?: string;
}) {
  const fs = yield* FileSystem.FileSystem;
  const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-system-binding-" });
  yield* fs.makeDirectory(`${baseDir}/runtime`);
  const unit = "t3code.service";
  const unitPath = `/etc/systemd/system/${unit}`;
  const directory = `${unitPath}.d`;
  const receiptPath = `${baseDir}/runtime/jones-system-adoption.json`;
  const files = new Map<string, string>([
    [unitPath, "[Service]\nUser=fixture-user\nNoNewPrivileges=yes\n"],
    [`${directory}/${LINUX_PRIORITY_DROPIN}`, LINUX_PRIORITY_POLICY],
    [
      `${directory}/${OWNED_SYSTEM_DROPIN}`,
      "[Service]\nExecStart=/fixture/t3 __service-launcher\n",
    ],
  ]);
  if (options?.lateDropin !== undefined) files.set(`${directory}/z-later.conf`, options.lateDropin);
  const names = [...files.keys()]
    .filter((file) => file !== unitPath)
    .map((file) => file.slice(directory.length + 1))
    .sort();
  const records = [
    systemUnitRecord(
      "fragment",
      { uid: 0, mode: 0o100644 },
      new TextEncoder().encode(files.get(unitPath)!),
    ),
    ...names
      .filter((name) => name !== OWNED_SYSTEM_DROPIN)
      .map((name) =>
        systemUnitRecord(
          `dropin ${name}`,
          { uid: 0, mode: 0o100644 },
          new TextEncoder().encode(files.get(`${directory}/${name}`)!),
        ),
      ),
  ];
  const plan = {
    schema: 1,
    kind: "system-unit",
    baseDir,
    uid: 1000,
    user: "fixture-user",
    systemUnit: unit,
    launcher: { version: "1.2.3" },
    unit: {
      killMode: "control-group",
      slice: "system.slice",
      digest: systemUnitDigest(
        options?.beforePriority
          ? records.filter((entry) => !entry.startsWith(`dropin ${LINUX_PRIORITY_DROPIN} `))
          : records,
      ),
      records: options?.beforePriority
        ? records.filter((entry) => !entry.startsWith(`dropin ${LINUX_PRIORITY_DROPIN} `))
        : records,
    },
    retained: { controlGroup: `/system.slice/${unit}` },
    ownedDropin: {
      path: `${directory}/${OWNED_SYSTEM_DROPIN}`,
      text: files.get(`${directory}/${OWNED_SYSTEM_DROPIN}`)!,
      sha256: sha256(files.get(`${directory}/${OWNED_SYSTEM_DROPIN}`)!),
    },
  };
  yield* fs.writeFileString(
    receiptPath,
    JSON.stringify({
      schema: 1,
      kind: "system-unit",
      status: options?.status ?? "applied",
      phase: "applied",
      plan,
      planSha256: sha256(JSON.stringify(plan)),
    }),
    { mode: 0o600 },
  );
  yield* fs.writeFileString(
    `${baseDir}/runtime/service-state.json`,
    JSON.stringify({ protocol: SERVICE_LAUNCHER_PROTOCOL, activeVersion: "1.2.3" }),
  );
  if (options?.fragmentDrift) files.set(unitPath, `${files.get(unitPath)!}CPUWeight=999\n`);
  if (options?.priorityBytes !== undefined)
    files.set(`${directory}/${LINUX_PRIORITY_DROPIN}`, options.priorityBytes);
  const receiptMetadata = yield* fs.stat(receiptPath);
  const fakeFs: FileSystem.FileSystem = {
    ...fs,
    stat: (file) =>
      files.has(file) || file === receiptPath
        ? Effect.succeed({
            ...receiptMetadata,
            mode:
              file === `${directory}/${LINUX_PRIORITY_DROPIN}`
                ? (options?.priorityMode ?? 0o100644)
                : file === receiptPath || file === `${directory}/${OWNED_SYSTEM_DROPIN}`
                  ? 0o100600
                  : 0o100644,
            uid: Option.some(
              file === receiptPath ? 1000 : file === unitPath ? (options?.fragmentUid ?? 0) : 0,
            ),
            gid: Option.some(
              file === `${directory}/${LINUX_PRIORITY_DROPIN}` ? (options?.priorityGid ?? 0) : 0,
            ),
          })
        : fs.stat(file),
    realPath: (file) => (files.has(file) ? Effect.succeed(file) : fs.realPath(file)),
    readFile: (file) =>
      file === `${directory}/${OWNED_SYSTEM_DROPIN}`
        ? Effect.die("Protected launcher drop-in bytes must not be read")
        : files.has(file)
          ? Effect.succeed(new TextEncoder().encode(files.get(file)!))
          : fs.readFile(file),
    readFileString: (file, encoding) =>
      file === "/proc/self/cgroup"
        ? Effect.succeed(options?.scope ?? "0::/user.slice/user-1000.slice/session-1.scope\n")
        : fs.readFileString(file, encoding),
    readDirectory: (file, input) =>
      file === directory ? Effect.succeed(names) : fs.readDirectory(file, input),
  };
  const commands: Array<{ readonly command: string; readonly args: ReadonlyArray<string> }> = [];
  const runner = ProcessRunner.ProcessRunner.of({
    run: (command) =>
      Effect.sync(() => {
        commands.push(command);
        return {
          code: ChildProcessSpawner.ExitCode(0),
          stdout: [
            `Id=${unit}`,
            "LoadState=loaded",
            `FragmentPath=${unitPath}`,
            `DropInPaths=${names.map((name) => `${directory}/${name}`).join(" ")}${options?.foreignDropin ? " /usr/lib/systemd/system/service.d/foreign.conf" : ""}`,
            "User=fixture-user",
            "NoNewPrivileges=yes",
            `Nice=${options?.nice ?? "-15"}`,
            `ControlGroup=/system.slice/${unit}`,
            "ActiveState=active",
            "UnitFileState=enabled",
            `ExecStart=${options?.execStart ?? `{ path=${baseDir}/runtime/versions/1.2.3/t3 ; argv[]=${baseDir}/runtime/versions/1.2.3/t3 __service-launcher ; ignore_errors=no ; }`}`,
            "KillMode=control-group",
            "Slice=system.slice",
            "",
          ].join("\n"),
          stderr: "",
          timedOut: false,
          stdoutTruncated: false,
          stderrTruncated: false,
          stdoutInvalidUtf8: false,
          stderrInvalidUtf8: false,
        };
      }),
  });
  const provide = <A, E, R>(operation: Effect.Effect<A, E, R>) =>
    operation.pipe(
      Effect.provideService(FileSystem.FileSystem, fakeFs),
      Effect.provideService(HostProcess.Platform, "linux"),
      Effect.provideService(HostProcess.UserId, 1000),
      Effect.provideService(HostProcess.Environment, { T3CODE_HOME: baseDir }),
      Effect.provideService(ProcessRunner.ProcessRunner, runner),
    );
  return {
    baseDir,
    receiptPath,
    commands,
    provide,
    service: provide(SystemBootService.make({ baseDir, logsDir: `${baseDir}/logs` })),
  };
});

it.layer(NodeServices.layer)("system service binding", (it) => {
  it.effect(
    "reports the adopted unit and blocks protected lifecycle commands without user-unit effects",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const service = yield* f.service;
        expect(service).toBeDefined();
        if (service === undefined) return;
        const status = yield* f.provide(service.status);
        expect(status).toMatchObject({
          installed: true,
          current: true,
          installedVersion: "1.2.3",
          installedBaseDir: f.baseDir,
          unitPath: "/etc/systemd/system/t3code.service",
        });
        let operationRan = false;
        for (const operation of [
          service.install(),
          service.restart,
          service.uninstall,
          service.withStopped(
            Effect.sync(() => {
              operationRan = true;
            }),
          ),
        ]) {
          const result = yield* Effect.result(operation);
          expect(result._tag).toBe("Failure");
          if (result._tag === "Failure")
            expect(result.failure.message).toContain("external administrator");
        }
        expect(operationRan).toBe(false);
        expect(f.commands).toHaveLength(1);
        expect(f.commands[0]).toMatchObject({
          command: "systemctl",
          args: ["show", "t3code.service", expect.any(String)],
        });
        expect(f.commands[0]?.args).not.toContain("--user");
      }),
  );

  it.effect("accepts the exact policy added after an earlier completed adoption", () =>
    Effect.gen(function* () {
      const f = yield* fixture({ beforePriority: true });
      const service = yield* f.service;
      expect(service).toBeDefined();
      if (service === undefined) return;
      expect((yield* f.provide(service.status)).current).toBe(true);
    }),
  );

  it.effect.each([
    { nice: "0" },
    { priorityMode: 0o100664 },
    { priorityGid: 1000 },
    { lateDropin: "[Service]\nNice=-15\n" },
    { beforePriority: true, fragmentDrift: true },
    { beforePriority: true, fragmentUid: 1000 },
    { beforePriority: true, priorityBytes: "[Service]\nNice=-15\nCPUWeight=1000\n" },
    { foreignDropin: true },
    { execStart: "{ path=/other/t3 ; argv[]=/other/t3 __service-launcher ; }" },
    {
      execStart:
        "{ path=/one/t3 ; argv[]=/one/t3 __service-launcher ; } { path=/two/t3 ; argv[]=/two/t3 __service-launcher ; }",
    },
  ])("reports policy drift without attempting repair: %j", (options) =>
    Effect.gen(function* () {
      const f = yield* fixture(options);
      const service = yield* f.service;
      expect(service).toBeDefined();
      if (service === undefined) return;
      const result = yield* f.provide(Effect.result(service.status));
      expect(result._tag).toBe("Failure");
      expect(f.commands).toHaveLength(1);
    }),
  );

  it.effect.each(["pending", "partial", "unknown"])("refuses an unsettled %s receipt", (status) =>
    Effect.gen(function* () {
      const f = yield* fixture({ status });
      expect((yield* Effect.result(f.service))._tag).toBe("Failure");
      expect(f.commands).toHaveLength(0);
    }),
  );

  it.effect("refuses a missing receipt when the caller runs in the known system unit", () =>
    Effect.gen(function* () {
      const f = yield* fixture({ scope: "0::/system.slice/t3code.service\n" });
      const fs = yield* FileSystem.FileSystem;
      yield* fs.remove(f.receiptPath);
      expect((yield* Effect.result(f.service))._tag).toBe("Failure");
    }),
  );

  it.effect.each([{ T3_BOOT_SERVICE_UNIT: "other.service" }, { T3_BOOT_SERVICE_SCOPE: "user" }])(
    "rejects manager metadata contradicting an existing binding: %j",
    (environment) =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const result = yield* f.provide(
          SystemBootService.make({ baseDir: f.baseDir, logsDir: `${f.baseDir}/logs` }).pipe(
            Effect.provideService(HostProcess.Environment, environment),
            Effect.result,
          ),
        );
        expect(result._tag).toBe("Failure");
      }),
  );

  it.effect("rejects an existing dangling receipt symlink", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const fs = yield* FileSystem.FileSystem;
      yield* fs.remove(f.receiptPath);
      yield* fs.symlink(`${f.baseDir}/missing`, f.receiptPath);
      expect((yield* Effect.result(f.service))._tag).toBe("Failure");
    }),
  );

  it.effect("preserves legacy user scope and the macOS route when no system receipt exists", () =>
    Effect.gen(function* () {
      const f = yield* fixture({
        scope: "0::/user.slice/user-1000.slice/user@1000.service/app.slice/t3code.service\n",
      });
      const fs = yield* FileSystem.FileSystem;
      yield* fs.remove(f.receiptPath);
      const legacy = yield* f.provide(
        SystemBootService.make({ baseDir: f.baseDir, logsDir: `${f.baseDir}/logs` }).pipe(
          Effect.provideService(HostProcess.Environment, {
            T3_BOOT_SERVICE_UNIT: "t3code.service",
          }),
        ),
      );
      expect(legacy).toBeUndefined();
      const mac = yield* f.provide(
        SystemBootService.make({ baseDir: f.baseDir, logsDir: `${f.baseDir}/logs` }).pipe(
          Effect.provideService(HostProcess.Platform, "darwin"),
        ),
      );
      expect(mac).toBeUndefined();
    }),
  );
});
