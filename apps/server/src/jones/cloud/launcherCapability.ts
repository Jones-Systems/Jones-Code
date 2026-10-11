// @effect-diagnostics nodeBuiltinImport:off
// The detached launcher publishes native process evidence without loading the server runtime.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

export interface LauncherCapabilityReceipt {
  readonly schema: 1;
  readonly baseDir: string;
  readonly launcherVersion: string;
  readonly launcherPid: number;
  readonly launcherProtocol: 4;
  readonly qualifiedUpdatesProtocol: 1;
  readonly startupGateProtocol: 1;
  readonly childPid: number;
  readonly childVersion: string;
}

export interface LauncherCapabilityProcessGuard {
  readonly uid: number;
  readonly isOwnedLive: (pid: number) => Promise<boolean>;
}

export type LauncherCapabilityIdentity = Pick<
  LauncherCapabilityReceipt,
  "launcherVersion" | "launcherPid" | "childVersion" | "childPid"
>;

const receiptPath = (baseDir: string) =>
  NodePath.join(baseDir, "runtime", "jones-launcher-capability.json");

function decode(value: unknown): LauncherCapabilityReceipt {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new Error("Launcher capability receipt is malformed.");
  const receipt = value as Record<string, unknown>;
  if (
    receipt.schema !== 1 ||
    receipt.launcherProtocol !== 4 ||
    receipt.qualifiedUpdatesProtocol !== 1 ||
    receipt.startupGateProtocol !== 1 ||
    typeof receipt.baseDir !== "string" ||
    !NodePath.isAbsolute(receipt.baseDir) ||
    typeof receipt.launcherVersion !== "string" ||
    receipt.launcherVersion.trim() === "" ||
    typeof receipt.childVersion !== "string" ||
    receipt.childVersion.trim() === "" ||
    !Number.isSafeInteger(receipt.launcherPid) ||
    (receipt.launcherPid as number) <= 0 ||
    !Number.isSafeInteger(receipt.childPid) ||
    (receipt.childPid as number) <= 0 ||
    receipt.childPid === receipt.launcherPid
  )
    throw new Error("Launcher capability receipt is malformed.");
  return receipt as unknown as LauncherCapabilityReceipt;
}

export async function assertLauncherCapabilityDirectory(
  baseDir: string,
  uid: number,
): Promise<void> {
  if ((await NodeFSP.realpath(baseDir)) !== baseDir)
    throw new Error("Launcher capability home is not canonical.");
  const directory = await NodeFSP.lstat(NodePath.dirname(receiptPath(baseDir)));
  if (!directory.isDirectory() || directory.uid !== uid || (directory.mode & 0o022) !== 0)
    throw new Error("Launcher capability directory has unknown ownership.");
}

async function readOwned(baseDir: string, uid: number) {
  await assertLauncherCapabilityDirectory(baseDir, uid);
  const handle = await NodeFSP.open(
    receiptPath(baseDir),
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  ).catch((cause: NodeJS.ErrnoException) => {
    if (cause.code === "ENOENT") return undefined;
    throw cause;
  });
  if (handle === undefined) return undefined;
  try {
    const stat = await handle.stat();
    if (!stat.isFile() || stat.uid !== uid || (stat.mode & 0o077) !== 0 || stat.size > 4096)
      throw new Error("Launcher capability receipt has unknown ownership.");
    const receipt = decode(JSON.parse(await handle.readFile("utf8")));
    if (receipt.baseDir !== baseDir)
      throw new Error("Launcher capability receipt has a different native home.");
    return { receipt, stat };
  } finally {
    await handle.close();
  }
}

const nativeProcessGuard: LauncherCapabilityProcessGuard = {
  uid: NodeOS.userInfo().uid,
  async isOwnedLive(pid) {
    try {
      process.kill(pid, 0);
    } catch (cause) {
      if ((cause as NodeJS.ErrnoException).code === "ESRCH") return false;
      throw cause;
    }
    // oxlint-disable-next-line t3code/no-global-process-runtime -- The detached receipt reader must select the native process ownership proof.
    const platform = NodeOS.platform();
    if (platform === "linux") {
      const status = await NodeFSP.readFile(`/proc/${pid}/status`, "utf8").catch(
        (cause: NodeJS.ErrnoException) => {
          if (cause.code === "ENOENT") return "";
          throw cause;
        },
      );
      const uids = /^Uid:\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)$/m.exec(status);
      return uids !== null && uids.slice(1).every((uid) => Number(uid) === NodeOS.userInfo().uid);
    }
    if (platform === "darwin") {
      const owner = await new Promise<string>((resolve, reject) => {
        NodeChildProcess.execFile(
          "/bin/ps",
          ["-o", "uid=", "-p", String(pid)],
          { timeout: 5000, maxBuffer: 4096 },
          (error, stdout) => {
            if (error !== null && !(typeof error.code === "number" && error.code === 1))
              reject(error);
            else resolve(stdout.trim());
          },
        );
      });
      return /^\d+$/.test(owner) && Number(owner) === NodeOS.userInfo().uid;
    }
    throw new Error("Launcher capability process ownership is unsupported on this host.");
  },
};

export async function readLauncherCapabilityReceipt(
  baseDir: string,
  expected: LauncherCapabilityIdentity,
  processGuard: LauncherCapabilityProcessGuard = nativeProcessGuard,
): Promise<LauncherCapabilityReceipt | undefined> {
  const owned = await readOwned(baseDir, processGuard.uid);
  if (owned === undefined) return undefined;
  const { receipt } = owned;
  if (
    receipt.launcherVersion !== expected.launcherVersion ||
    receipt.launcherPid !== expected.launcherPid ||
    receipt.childVersion !== expected.childVersion ||
    receipt.childPid !== expected.childPid
  )
    return undefined;
  if (
    !(await processGuard.isOwnedLive(receipt.launcherPid)) ||
    !(await processGuard.isOwnedLive(receipt.childPid))
  )
    return undefined;
  return receipt;
}

export async function publishLauncherCapabilityReceipt(
  receipt: LauncherCapabilityReceipt,
): Promise<void> {
  decode(receipt);
  const directory = NodePath.dirname(receiptPath(receipt.baseDir));
  await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
  await readOwned(receipt.baseDir, NodeOS.userInfo().uid);
  const scratch = NodePath.join(directory, `.jones-launcher-capability-${NodeCrypto.randomUUID()}`);
  const handle = await NodeFSP.open(scratch, "wx", 0o600);
  try {
    await handle.writeFile(`${JSON.stringify(receipt)}\n`);
    await handle.sync();
    await handle.close();
    await NodeFSP.rename(scratch, receiptPath(receipt.baseDir));
  } finally {
    await handle.close();
    await NodeFSP.rm(scratch, { force: true });
  }
}

export async function retractLauncherCapabilityReceipt(
  baseDir: string,
  launcherPid: number,
  childPid: number,
): Promise<void> {
  const owned = await readOwned(baseDir, NodeOS.userInfo().uid);
  if (owned === undefined) return;
  if (owned.receipt.launcherPid !== launcherPid || owned.receipt.childPid !== childPid)
    throw new Error("Launcher capability receipt belongs to another child.");
  const current = await NodeFSP.lstat(receiptPath(baseDir));
  if (current.dev !== owned.stat.dev || current.ino !== owned.stat.ino)
    throw new Error("Launcher capability receipt changed during retraction.");
  await NodeFSP.unlink(receiptPath(baseDir));
}
