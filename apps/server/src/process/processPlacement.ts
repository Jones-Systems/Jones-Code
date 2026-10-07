// @effect-diagnostics nodeBuiltinImport:off - synchronous pre-exec boundary for a future standalone launcher.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";

const PROCESS_PLACEMENT_ENV = "T3_PROCESS_PLACEMENT";
export const PROCESS_PLACEMENT_BOOTSTRAP_ENV = "T3_PROCESS_PLACEMENT_BOOTSTRAP";
export type ProcessRole = "control" | "workload";
export interface CgroupBinding {
  readonly path: string;
  readonly device: string;
  readonly inode: string;
}
export interface ProcessPlacementBinding {
  readonly version: 1;
  readonly helperPath: string;
  readonly helperSha256: string;
  readonly helperDevice: string;
  readonly helperInode: string;
  readonly control: CgroupBinding;
  readonly workload: CgroupBinding;
  readonly serverPriority?: { readonly nice: -15; readonly resetOnFork: true };
}

export class ProcessPlacementError extends Error {
  constructor(message: string) {
    super(`Process placement required: ${message}`);
    this.name = "ProcessPlacementError";
  }
}

const validIdentity = (value: unknown, positive: boolean): value is string =>
  typeof value === "string" &&
  (positive ? /^[1-9][0-9]{0,19}$/ : /^(0|[1-9][0-9]{0,19})$/).test(value) &&
  BigInt(value) <= 18446744073709551615n;

const fail = (message: string): never => {
  throw new ProcessPlacementError(message);
};

// Keep parsing synchronous so a launcher can validate before spawning its payload.
export function readProcessPlacementBinding(
  environment: NodeJS.ProcessEnv = process.env,
): ProcessPlacementBinding | undefined {
  const text = environment[PROCESS_PLACEMENT_ENV];
  if (text === undefined) {
    if (environment[PROCESS_PLACEMENT_BOOTSTRAP_ENV] !== undefined)
      return fail("bootstrap must establish a ready binding before T3 executes");
    return undefined;
  }
  if (Buffer.byteLength(text, "utf8") > 16 * 1024) return fail("binding exceeds 16 KiB");
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    return fail("invalid binding JSON");
  }
  if (typeof value !== "object" || value === null || Array.isArray(value))
    return fail("invalid binding");
  const record = value as Record<string, unknown>;
  if (
    Object.keys(record).some(
      (key) =>
        ![
          "version",
          "helperPath",
          "helperSha256",
          "helperDevice",
          "helperInode",
          "control",
          "workload",
          "serverPriority",
        ].includes(key),
    )
  )
    return fail("unknown binding field");
  if (
    record.version !== 1 ||
    typeof record.helperPath !== "string" ||
    !NodePath.isAbsolute(record.helperPath) ||
    NodePath.normalize(record.helperPath) !== record.helperPath ||
    /[\u0000-\u001f\u007f]/.test(record.helperPath) ||
    typeof record.helperSha256 !== "string" ||
    !/^[0-9a-f]{64}$/.test(record.helperSha256) ||
    !validIdentity(record.helperDevice, false) ||
    !validIdentity(record.helperInode, true)
  ) {
    return fail("unsupported binding or helper identity");
  }
  const group = (input: unknown): CgroupBinding => {
    if (typeof input !== "object" || input === null || Array.isArray(input))
      return fail("invalid cgroup binding");
    const item = input as Record<string, unknown>;
    if (Object.keys(item).some((key) => !["path", "device", "inode"].includes(key)))
      return fail("unknown cgroup field");
    if (
      typeof item.path !== "string" ||
      !item.path.startsWith("/sys/fs/cgroup/") ||
      /[\u0000-\u001f\u007f]/.test(item.path) ||
      NodePath.normalize(item.path) !== item.path ||
      typeof item.device !== "string" ||
      !/^(0|[1-9][0-9]{0,19})$/.test(item.device) ||
      BigInt(item.device) > 18446744073709551615n ||
      typeof item.inode !== "string" ||
      !/^[1-9][0-9]{0,19}$/.test(item.inode) ||
      BigInt(item.inode) > 18446744073709551615n
    ) {
      return fail("invalid cgroup identity");
    }
    return { path: item.path, device: item.device, inode: item.inode };
  };
  const control = group(record.control);
  const workload = group(record.workload);
  if (NodePath.dirname(control.path) !== NodePath.dirname(workload.path))
    return fail("roles must share one delegated parent");
  if (
    control.path === workload.path ||
    control.path.startsWith(`${workload.path}/`) ||
    workload.path.startsWith(`${control.path}/`)
  )
    return fail("roles require separate cgroup branches");
  if (record.serverPriority !== undefined) {
    const priority = record.serverPriority as Record<string, unknown> | null;
    if (
      typeof priority !== "object" ||
      priority === null ||
      Object.keys(priority).some((key) => !["nice", "resetOnFork"].includes(key)) ||
      priority.nice !== -15 ||
      priority.resetOnFork !== true
    ) {
      return fail("unsupported server scheduling policy");
    }
  }
  return {
    version: 1,
    helperPath: record.helperPath,
    helperSha256: record.helperSha256,
    helperDevice: record.helperDevice as string,
    helperInode: record.helperInode as string,
    control,
    workload,
    ...(record.serverPriority === undefined
      ? {}
      : { serverPriority: { nice: -15, resetOnFork: true } }),
  };
}

export function validateProcessPlacementBinding(
  binding: ProcessPlacementBinding,
  platform = HostProcessPlatform.defaultValue(),
): void {
  if (platform !== "linux") fail("Linux cgroup v2 is required");
  try {
    if (NodeFS.realpathSync(binding.helperPath) !== binding.helperPath)
      fail("helper symlink rejected");
    const helper = NodeFS.lstatSync(binding.helperPath, { bigint: true });
    if (
      !helper.isFile() ||
      helper.isSymbolicLink() ||
      (helper.mode & 0o111n) === 0n ||
      (helper.mode & 0o022n) !== 0n ||
      helper.uid !== BigInt(process.getuid?.() ?? -1) ||
      String(helper.dev) !== binding.helperDevice ||
      String(helper.ino) !== binding.helperInode
    )
      fail("unsafe helper executable");
    if (
      NodeCrypto.createHash("sha256")
        .update(NodeFS.readFileSync(binding.helperPath))
        .digest("hex") !== binding.helperSha256
    ) {
      fail("helper identity changed");
    }
    for (const role of ["control", "workload"] as const) {
      const target = binding[role];
      if (NodeFS.realpathSync(target.path) !== target.path) fail("cgroup symlink rejected");
      const stat = NodeFS.statSync(target.path, { bigint: true });
      if (
        !stat.isDirectory() ||
        String(stat.dev) !== target.device ||
        String(stat.ino) !== target.inode
      ) {
        fail("cgroup identity changed");
      }
      NodeFS.accessSync(NodePath.join(target.path, "cgroup.procs"), NodeFS.constants.W_OK);
      NodeFS.accessSync(NodePath.join(target.path, "cpu.max"), NodeFS.constants.R_OK);
    }
  } catch (cause) {
    if (cause instanceof ProcessPlacementError) throw cause;
    fail("helper or delegated CPU cgroup unavailable");
  }
}

export function placementCommand(
  command: string,
  args: ReadonlyArray<string>,
  role: ProcessRole = "workload",
  binding = readProcessPlacementBinding(),
  server = false,
  platform = HostProcessPlatform.defaultValue(),
): { readonly command: string; readonly args: ReadonlyArray<string> } {
  if (binding === undefined) return { command, args };
  if (server && role !== "control") fail("server priority requires control role");
  validateProcessPlacementBinding(binding, platform);
  return {
    command: binding.helperPath,
    args: [...placementArguments(binding, role, server), command, ...args],
  };
}

export function assertControlProcessPlacement(
  binding = readProcessPlacementBinding(),
  platform = HostProcessPlatform.defaultValue(),
): void {
  if (binding === undefined) return;
  validateProcessPlacementBinding(binding, platform);
  let membership: string;
  try {
    membership = NodeFS.readFileSync("/proc/self/cgroup", "utf8");
  } catch {
    return fail("cannot verify server membership");
  }
  const cgroup = membership
    .split("\n")
    .find((line) => line.startsWith("0::"))
    ?.slice(3);
  if (cgroup === undefined || NodePath.join("/sys/fs/cgroup", cgroup) !== binding.control.path) {
    fail("server must start in its control cgroup before execution");
  }
}

export function assertServerProcessPlacement(
  binding = readProcessPlacementBinding(),
  platform = HostProcessPlatform.defaultValue(),
): void {
  assertControlProcessPlacement(binding, platform);
  if (binding?.serverPriority !== undefined) {
    const result = NodeChildProcess.spawnSync(
      binding.helperPath,
      [...helperIdentityArguments(binding), "--verify-server-priority", String(process.pid)],
      { stdio: "ignore" },
    );
    if (result.error !== undefined || result.status !== 0)
      fail("server priority or reset-on-fork not established at startup");
  }
}

export function helperIdentityArguments(binding: ProcessPlacementBinding): ReadonlyArray<string> {
  return [
    "--helper-path",
    binding.helperPath,
    "--helper-sha256",
    binding.helperSha256,
    "--helper-device",
    binding.helperDevice,
    "--helper-inode",
    binding.helperInode,
  ];
}

export function placementArguments(
  binding: ProcessPlacementBinding,
  role: ProcessRole,
  server = false,
): ReadonlyArray<string> {
  const group = binding[role];
  return [
    ...helperIdentityArguments(binding),
    "--role",
    role,
    "--cgroup",
    group.path,
    "--device",
    group.device,
    "--inode",
    group.inode,
    ...(server && binding.serverPriority !== undefined ? ["--server-priority", "-15"] : []),
    "--",
  ];
}
