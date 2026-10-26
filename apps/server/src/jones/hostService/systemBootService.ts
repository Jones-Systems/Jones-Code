import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as BootService from "../../cloud/bootService.ts";
import {
  isExactServiceVersion,
  parseServiceState,
  SERVICE_STATE_FILE,
} from "../../cloud/serviceProtocol.ts";
import * as ProcessRunner from "../../processRunner.ts";
import { ADOPTION_RECEIPT, SYSTEM_ADOPTION_RECEIPT } from "./adoption.ts";
import {
  assertLinuxPriorityPolicy,
  assertNoLaterLinuxPriorityOverride,
  LINUX_PRIORITY_DROPIN,
} from "./linuxPriorityPolicy.ts";
import {
  OWNED_SYSTEM_DROPIN,
  sha256,
  systemUnitDigest,
  systemUnitRecord,
} from "./systemServiceControl.ts";

const UNIT = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,200}\.service$/;
const SHA = /^[a-f0-9]{64}$/;
const record = (value: unknown): Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
const refuse = (reason: string) =>
  new BootService.BootServiceInstallError({ cause: undefined, reason });

interface SystemServiceBinding {
  readonly systemUnit: string;
  readonly unitPath: string;
  readonly uid: number;
  readonly user: string;
  readonly unitDigest: string;
  readonly planSha256: string;
  readonly controlGroup: string;
  readonly launcherEntry: string;
  readonly killMode: string;
  readonly slice: string;
}

function systemServiceUnitsFromCgroup(contents: string): Set<string> {
  const units = new Set<string>();
  for (const line of contents.split("\n")) {
    if (!line.startsWith("0::/")) continue;
    const parts = line.slice(4).split("/");
    if (parts.includes("user.slice") || parts.some((part) => /^user@\d+\.service$/.test(part)))
      continue;
    const unit = parts.find((part) => UNIT.test(part));
    if (unit !== undefined) units.add(unit);
  }
  return units;
}

export function systemServiceUnitFromCgroup(contents: string): string | undefined {
  const units = systemServiceUnitsFromCgroup(contents);
  return units.size === 1 ? [...units][0] : undefined;
}

export const readBinding = Effect.fn("jones.system_boot_service.read_binding")(function* (
  baseDir: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const platform = yield* HostProcess.Platform;
  const uid = yield* HostProcess.UserId;
  const environment = yield* HostProcess.Environment;
  const executable = yield* HostProcess.ExecutablePath;
  const receiptPath = path.join(baseDir, "runtime", SYSTEM_ADOPTION_RECEIPT);
  const link = yield* fs.readLink(receiptPath).pipe(Effect.option);
  if (Option.isSome(link))
    return yield* refuse("A system adoption receipt is a symlink; reconcile it before retrying.");
  const receipt = yield* fs.readFileString(receiptPath).pipe(
    Effect.asSome,
    Effect.catchIf(
      (cause) => cause.reason._tag === "NotFound",
      () => Effect.succeed(Option.none<string>()),
    ),
    Effect.mapError((cause) => new BootService.BootServiceInstallError({ cause })),
  );
  if (Option.isNone(receipt)) {
    if (platform === "linux") {
      const cgroup = yield* fs.readFileString("/proc/self/cgroup").pipe(Effect.option);
      const systemUnit = Option.isSome(cgroup)
        ? systemServiceUnitFromCgroup(cgroup.value)
        : undefined;
      const servesThisHome =
        (environment["T3CODE_HOME"] !== undefined &&
          path.resolve(environment["T3CODE_HOME"]) === path.resolve(baseDir)) ||
        executable.startsWith(`${path.join(baseDir, "runtime", "versions")}/`);
      if (
        environment["T3_BOOT_SERVICE_SCOPE"] === "system" ||
        (servesThisHome &&
          ((Option.isSome(cgroup) && systemServiceUnitsFromCgroup(cgroup.value).size > 1) ||
            (systemUnit !== undefined &&
              (systemUnit === environment["T3_BOOT_SERVICE_UNIT"] ||
                ["t3code.service", "host-tooling-t3.service", "jones-code.service"].includes(
                  systemUnit,
                ))) ||
            (environment["T3_BOOT_SERVICE_UNIT"] !== undefined && Option.isNone(cgroup))))
      )
        return yield* refuse(
          "A system service or unresolved manager scope has no completed system adoption receipt. Reconcile this home's binding through the external administrator; user-service setup is unavailable.",
        );
    }
    return undefined;
  }
  const invalid =
    "The system adoption receipt is damaged, incomplete or inconsistent with this home. Reconcile it through the external administrator; user-service fallback is unavailable.";
  const parsed = record(
    yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(receipt.value).pipe(
      Effect.mapError(() => refuse(invalid)),
    ),
  );
  const plan = record(parsed.plan);
  const unit = record(plan.unit);
  const owned = record(plan.ownedDropin);
  const retained = record(plan.retained);
  const launcher = record(plan.launcher);
  // @effect-diagnostics-next-line preferSchemaOverJson:off -- Native adoption hashes the exact JSON.stringify serialization, including property order.
  const planSha256 = sha256(JSON.stringify(plan));
  const systemUnit = plan.systemUnit;
  if (
    platform !== "linux" ||
    parsed.schema !== 1 ||
    parsed.kind !== "system-unit" ||
    parsed.status !== "applied" ||
    parsed.phase !== "applied" ||
    plan.schema !== 1 ||
    plan.kind !== "system-unit" ||
    plan.baseDir !== baseDir ||
    !path.isAbsolute(baseDir) ||
    path.resolve(baseDir) !== baseDir ||
    uid === undefined ||
    uid < 1 ||
    plan.uid !== uid ||
    typeof plan.user !== "string" ||
    !/^[a-z_][a-z0-9_-]*[$]?$/.test(plan.user) ||
    typeof systemUnit !== "string" ||
    !UNIT.test(systemUnit) ||
    typeof parsed.planSha256 !== "string" ||
    !SHA.test(parsed.planSha256) ||
    planSha256 !== parsed.planSha256 ||
    typeof launcher.version !== "string" ||
    !isExactServiceVersion(launcher.version) ||
    typeof unit.killMode !== "string" ||
    !["control-group", "mixed"].includes(unit.killMode) ||
    typeof unit.slice !== "string" ||
    !/^[A-Za-z0-9][A-Za-z0-9_.-]*\.slice$/.test(unit.slice) ||
    typeof unit.digest !== "string" ||
    !SHA.test(unit.digest) ||
    !Array.isArray(unit.records) ||
    unit.records.length === 0 ||
    !unit.records.every((item) => typeof item === "string") ||
    systemUnitDigest(unit.records as string[]) !== unit.digest ||
    typeof owned.sha256 !== "string" ||
    !SHA.test(owned.sha256) ||
    typeof owned.text !== "string" ||
    sha256(owned.text) !== owned.sha256 ||
    owned.path !== `/etc/systemd/system/${systemUnit}.d/${OWNED_SYSTEM_DROPIN}` ||
    typeof retained.controlGroup !== "string" ||
    !retained.controlGroup.endsWith(`/${systemUnit}`) ||
    (environment["T3_BOOT_SERVICE_UNIT"] !== undefined &&
      environment["T3_BOOT_SERVICE_UNIT"] !== systemUnit) ||
    (environment["T3_BOOT_SERVICE_SCOPE"] !== undefined &&
      environment["T3_BOOT_SERVICE_SCOPE"] !== "system")
  )
    return yield* refuse(invalid);
  const metadata = yield* fs
    .stat(receiptPath)
    .pipe(Effect.mapError((cause) => new BootService.BootServiceInstallError({ cause })));
  const canonical = yield* fs
    .realPath(receiptPath)
    .pipe(Effect.mapError((cause) => new BootService.BootServiceInstallError({ cause })));
  if (
    metadata.type !== "File" ||
    Option.getOrUndefined(metadata.uid) !== uid ||
    (metadata.mode & 0o077) !== 0 ||
    canonical !== receiptPath ||
    (yield* fs.exists(path.join(baseDir, "runtime", ADOPTION_RECEIPT))) ||
    (yield* fs.exists(path.join(baseDir, "runtime", "jones-active-install.json")))
  )
    return yield* refuse(invalid);
  return {
    systemUnit,
    unitPath: `/etc/systemd/system/${systemUnit}`,
    uid,
    user: plan.user,
    unitDigest: unit.digest,
    planSha256: parsed.planSha256,
    controlGroup: retained.controlGroup,
    launcherEntry: path.join(baseDir, "runtime", "versions", launcher.version, "t3"),
    killMode: unit.killMode,
    slice: unit.slice,
  } satisfies SystemServiceBinding;
});

export const make = Effect.fn("jones.system_boot_service.make")(function* (input: {
  readonly baseDir: string;
  readonly logsDir: string;
}) {
  const binding = yield* readBinding(input.baseDir);
  if (binding === undefined) return undefined;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const runner = yield* ProcessRunner.ProcessRunner;
  const administrator = (operation: string) =>
    refuse(
      `The adopted system service ${binding.systemUnit} cannot ${operation} through user-service commands. An external administrator must perform that operation on this exact system unit while preserving its adoption binding and root-owned priority policy. Qualified child updates remain available through the host updater.`,
    );
  const status: BootService.BootService["Service"]["status"] = Effect.gen(function* () {
    const currentBinding = yield* readBinding(input.baseDir).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
    );
    if (currentBinding?.planSha256 !== binding.planSha256)
      return yield* refuse("The system adoption binding changed; reconcile it before retrying.");
    const result = yield* runner.run({
      command: "systemctl",
      args: [
        "show",
        binding.systemUnit,
        "--property=Id,LoadState,FragmentPath,DropInPaths,User,NoNewPrivileges,Nice,ControlGroup,ActiveState,UnitFileState,ExecStart,KillMode,Slice",
      ],
      timeout: Duration.seconds(10),
    });
    const values = new Map<string, string>();
    for (const line of result.stdout.split("\n")) {
      if (line === "") continue;
      const match = /^([A-Za-z]+)=(.*)$/.exec(line);
      if (match === null || values.has(match[1]!))
        return yield* administrator("validate its status");
      values.set(match[1]!, match[2]!);
    }
    const directory = `${binding.unitPath}.d`;
    const names = yield* fs.readDirectory(directory);
    const expectedPaths = names.sort().map((name) => `${directory}/${name}`);
    const effectivePaths = (values.get("DropInPaths") ?? "").split(" ").filter(Boolean).sort();
    const execStart = /^\{\s*path=([^;]+?)\s*;\s*argv\[\]=([^;]+?)\s*;[^{}]*\}$/.exec(
      values.get("ExecStart") ?? "",
    );
    if (
      result.code !== 0 ||
      [
        "Id",
        "LoadState",
        "FragmentPath",
        "DropInPaths",
        "User",
        "NoNewPrivileges",
        "Nice",
        "ControlGroup",
        "ActiveState",
        "UnitFileState",
        "ExecStart",
        "KillMode",
        "Slice",
      ].some((key) => !values.has(key)) ||
      values.get("Id") !== binding.systemUnit ||
      values.get("LoadState") !== "loaded" ||
      values.get("FragmentPath") !== binding.unitPath ||
      values.get("User") !== binding.user ||
      values.get("NoNewPrivileges") !== "yes" ||
      !names.includes(LINUX_PRIORITY_DROPIN) ||
      !names.includes(OWNED_SYSTEM_DROPIN) ||
      names.some(
        (name) =>
          !/^[A-Za-z0-9][A-Za-z0-9_.@-]{0,200}\.conf$/.test(name) || name > OWNED_SYSTEM_DROPIN,
      ) ||
      effectivePaths.length !== expectedPaths.length ||
      effectivePaths.some((file, index) => file !== expectedPaths[index]) ||
      execStart?.[1] !== binding.launcherEntry ||
      execStart?.[2] !== `${binding.launcherEntry} __service-launcher` ||
      (values.get("ExecStart")?.match(/argv\[\]=/g)?.length ?? 0) !== 1 ||
      values.get("KillMode") !== binding.killMode ||
      values.get("Slice") !== binding.slice ||
      (values.get("ControlGroup") !== "" && values.get("ControlGroup") !== binding.controlGroup)
    )
      return yield* administrator("validate its status");
    const records: string[] = [];
    for (const [label, file] of [
      ["fragment", binding.unitPath],
      ...names.map((name) => [`dropin ${name}`, `${directory}/${name}`]),
    ]) {
      const metadata = yield* fs.stat(file!);
      const canonical = yield* fs.realPath(file!);
      const uid = Option.getOrUndefined(metadata.uid);
      if (metadata.type !== "File" || canonical !== file || uid === undefined)
        return yield* administrator("validate its unit files");
      if (file === `${directory}/${OWNED_SYSTEM_DROPIN}`) {
        if (uid !== 0 || (metadata.mode & 0o022) !== 0)
          return yield* administrator("validate its launcher drop-in metadata");
        continue;
      }
      const bytes = yield* fs.readFile(file!);
      yield* Effect.try({
        try: () => assertNoLaterLinuxPriorityOverride(path.basename(file!), bytes),
        catch: (cause) =>
          new BootService.BootServiceInstallError({
            cause,
            reason:
              "A later system service drop-in overrides the root-owned priority policy; external administrator reconciliation is required.",
          }),
      });
      if (file === `${directory}/${LINUX_PRIORITY_DROPIN}`) {
        yield* Effect.try({
          try: () =>
            assertLinuxPriorityPolicy(
              { uid, gid: Option.getOrUndefined(metadata.gid) ?? -1, mode: metadata.mode, bytes },
              values.get("Nice") ?? "",
            ),
          catch: (cause) =>
            new BootService.BootServiceInstallError({
              cause,
              reason:
                "System service priority policy drift: external administrator reconciliation is required.",
            }),
        });
      }
      records.push(systemUnitRecord(label!, { uid, mode: metadata.mode }, bytes));
    }
    // Older adoption receipts predate this independently installed, validated root policy.
    const beforePriority = records.filter(
      (entry) => !entry.startsWith(`dropin ${LINUX_PRIORITY_DROPIN} `),
    );
    if (
      systemUnitDigest(records) !== binding.unitDigest &&
      systemUnitDigest(beforePriority) !== binding.unitDigest
    )
      return yield* administrator("validate its changed unit preimage");
    const state = parseServiceState(
      yield* fs.readFileString(path.join(input.baseDir, "runtime", SERVICE_STATE_FILE)),
    );
    if (state === undefined) return yield* administrator("validate its native service state");
    const running = values.get("ActiveState") === "active";
    const enabled = ["enabled", "enabled-runtime", "static"].includes(
      values.get("UnitFileState") ?? "",
    );
    return {
      supported: true,
      installed: true,
      current: running && enabled && state.update?.status !== "pending",
      installedVersion: state.activeVersion,
      installedBaseDir: input.baseDir,
      unitPath: binding.unitPath,
      logPath: path.join(input.logsDir, BootService.BOOT_SERVICE_LOG_FILE),
    };
  }).pipe(
    Effect.mapError((cause) =>
      Schema.is(BootService.BootServiceInstallError)(cause)
        ? cause
        : new BootService.BootServiceInstallError({ cause }),
    ),
  );
  return BootService.BootService.of({
    status,
    install: () => Effect.fail(administrator("install, repair or reinstall")),
    restart: Effect.fail(administrator("restart")),
    withStopped: () => Effect.fail(administrator("stop for a release update")),
    uninstall: Effect.fail(administrator("uninstall")),
  });
});
