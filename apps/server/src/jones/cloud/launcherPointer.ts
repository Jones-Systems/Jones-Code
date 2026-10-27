// @effect-diagnostics nodeBuiltinImport:off
// This detached launch boundary must remain usable without the server's Effect runtime.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

const VERSION = /^\d{1,9}\.\d{1,9}\.\d{1,9}-preview\.\d{8}\.\d{1,20}(?:\.\d{1,20})?$/;
const quoteShell = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;

export interface LauncherPointer {
  readonly program: string;
  readonly currentVersion: string;
  readonly pendingVersion?: string;
  readonly attempts: number;
  readonly shimSha256: string;
  readonly previousServiceSha256: string;
}

function directoryOf(baseDir: string): string {
  if (
    !NodePath.isAbsolute(baseDir) ||
    NodePath.resolve(baseDir) !== baseDir ||
    /[\x00-\x1f\x7f]/.test(baseDir)
  )
    throw new Error("Launcher pointer home is not canonical.");
  return NodePath.join(baseDir, "runtime/launcher");
}

function targetOf(version: string): string {
  if (!VERSION.test(version)) throw new Error("Launcher pointer version is malformed.");
  return `../versions/${version}`;
}

function shimFor(baseDir: string): string {
  // Absolute utility paths and shell builtins work with an empty service PATH.
  // A failed pending exec consumes its attempt; it must never reset the budget.
  return `#!/bin/sh
set -eu
umask 077
CDPATH= cd -P ${quoteShell(directoryOf(baseDir))} || exit 70
if [ -L pending ]; then
  attempts=
  IFS= read -r attempts < attempts || exit 70
  case "$attempts" in 0|1|2|3) ;; *) exit 70 ;; esac
  if [ "$attempts" -lt 3 ]; then
    next=$((attempts + 1))
    (set -C; printf '%s\\n' "$next" > ".attempts-$$") || exit 70
    /bin/mv -f ".attempts-$$" attempts || exit 70
    exec ./pending/t3 "$@"
  fi
  # Keep a failed candidate for diagnosis without overwriting earlier evidence.
  [ ! -e "rejected-$$" ] && [ ! -L "rejected-$$" ] || exit 70
  /bin/mv pending "rejected-$$" || exit 70
fi
exec ./current/t3 "$@"
`;
}

export function launcherPointerBootstrapPlan(
  baseDir: string,
  version: string,
  previousService: string,
) {
  targetOf(version);
  if (Buffer.byteLength(previousService) > 65536)
    throw new Error("Previous service definition exceeds the pointer archive limit.");
  return {
    program: NodePath.join(directoryOf(baseDir), "t3-launcher"),
    currentVersion: version,
    shimSha256: NodeCrypto.createHash("sha256").update(shimFor(baseDir)).digest("hex"),
    previousServiceSha256: NodeCrypto.createHash("sha256").update(previousService).digest("hex"),
  };
}

async function privateDirectory(directory: string, uid: number): Promise<void> {
  const stat = await NodeFSP.lstat(directory);
  if (
    !stat.isDirectory() ||
    stat.uid !== uid ||
    (stat.mode & 0o077) !== 0 ||
    (await NodeFSP.realpath(directory)) !== directory
  )
    throw new Error("Launcher pointer directory has unknown ownership.");
}

async function readPrivate(
  file: string,
  uid: number,
  mode: number,
  limit: number,
): Promise<string> {
  const descriptor = await NodeFSP.open(
    file,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  );
  try {
    const stat = await descriptor.stat();
    if (!stat.isFile() || stat.uid !== uid || (stat.mode & 0o777) !== mode || stat.size > limit)
      throw new Error("Launcher pointer file has unknown ownership.");
    return await descriptor.readFile("utf8");
  } finally {
    await descriptor.close();
  }
}

async function readTarget(
  directory: string,
  name: "current" | "pending",
  uid: number,
): Promise<string | undefined> {
  const file = NodePath.join(directory, name);
  const stat = await NodeFSP.lstat(file).catch((cause: NodeJS.ErrnoException) => {
    if (cause.code === "ENOENT" && name === "pending") return undefined;
    throw cause;
  });
  if (stat === undefined) return undefined;
  if (!stat.isSymbolicLink() || stat.uid !== uid)
    throw new Error("Launcher pointer target has unknown ownership.");
  const target = await NodeFSP.readlink(file);
  const version = target.slice("../versions/".length);
  if (target !== targetOf(version))
    throw new Error("Launcher pointer target escapes the runtime cache.");
  await validateRuntime(directory, version, uid);
  return version;
}

async function validateRuntime(directory: string, version: string, uid: number): Promise<void> {
  const expected = NodePath.resolve(directory, targetOf(version));
  const runtime = await NodeFSP.lstat(expected);
  const executable = await NodeFSP.lstat(NodePath.join(expected, "t3"));
  if (
    !runtime.isDirectory() ||
    runtime.uid !== uid ||
    (runtime.mode & 0o022) !== 0 ||
    (await NodeFSP.realpath(expected)) !== expected ||
    !executable.isFile() ||
    executable.uid !== uid ||
    (executable.mode & 0o022) !== 0 ||
    (executable.mode & 0o111) === 0
  )
    throw new Error("Launcher pointer runtime has unknown ownership.");
}

async function syncDirectory(directory: string): Promise<void> {
  const descriptor = await NodeFSP.open(directory, "r");
  try {
    await descriptor.sync();
  } finally {
    await descriptor.close();
  }
}

async function createFile(file: string, contents: string, mode: number): Promise<void> {
  const descriptor = await NodeFSP.open(file, "wx", mode);
  try {
    await descriptor.writeFile(contents);
    await descriptor.sync();
  } finally {
    await descriptor.close();
  }
}

export async function readLauncherPointer(baseDir: string): Promise<LauncherPointer | undefined> {
  const directory = directoryOf(baseDir);
  const stat = await NodeFSP.lstat(directory).catch((cause: NodeJS.ErrnoException) => {
    if (cause.code === "ENOENT") return undefined;
    throw cause;
  });
  if (stat === undefined) return undefined;
  const uid = NodeOS.userInfo().uid;
  await privateDirectory(directory, uid);
  const shim = await readPrivate(NodePath.join(directory, "t3-launcher"), uid, 0o700, 8192);
  if (shim !== shimFor(baseDir))
    throw new Error("Launcher pointer shim differs from its immutable format.");
  const attempts = await readPrivate(NodePath.join(directory, "attempts"), uid, 0o600, 2);
  if (!/^[0-3]\n$/.test(attempts)) throw new Error("Launcher pointer attempt budget is malformed.");
  const currentVersion = await readTarget(directory, "current", uid);
  if (currentVersion === undefined) throw new Error("Launcher pointer current target is missing.");
  const pendingVersion = await readTarget(directory, "pending", uid);
  const previousService = await readPrivate(
    NodePath.join(directory, "previous-service-definition"),
    uid,
    0o600,
    65536,
  );
  return {
    program: NodePath.join(directory, "t3-launcher"),
    currentVersion,
    ...(pendingVersion === undefined ? {} : { pendingVersion }),
    attempts: Number(attempts.trim()),
    shimSha256: NodeCrypto.createHash("sha256").update(shim).digest("hex"),
    previousServiceSha256: NodeCrypto.createHash("sha256").update(previousService).digest("hex"),
  };
}

/** Called inside the qualified-runtime lock, after enrollment and before service-definition publication. */
export async function bootstrapLauncherPointer(
  baseDir: string,
  version: string,
  previousService: string,
): Promise<LauncherPointer> {
  const directory = directoryOf(baseDir);
  const target = targetOf(version);
  const plan = launcherPointerBootstrapPlan(baseDir, version, previousService);
  const existing = await readLauncherPointer(baseDir);
  if (existing !== undefined) {
    if (
      existing.currentVersion !== version ||
      existing.pendingVersion !== undefined ||
      existing.attempts !== 0 ||
      existing.previousServiceSha256 !== plan.previousServiceSha256
    )
      throw new Error("Launcher pointer already has a different or unsettled operation.");
    await syncDirectory(directory);
    return existing;
  }
  const uid = NodeOS.userInfo().uid;
  const parent = NodePath.dirname(directory);
  const parentStat = await NodeFSP.lstat(parent);
  if (
    !parentStat.isDirectory() ||
    parentStat.uid !== uid ||
    (parentStat.mode & 0o022) !== 0 ||
    (await NodeFSP.realpath(parent)) !== parent
  )
    throw new Error("Launcher pointer parent has unknown ownership.");
  await validateRuntime(directory, version, uid);
  // Exclusive creation leaves an interrupted bootstrap visible for explicit reconciliation.
  await NodeFSP.mkdir(directory, { mode: 0o700 });
  await NodeFSP.mkdir(NodePath.join(directory, "handoff"), { mode: 0o700 });
  await createFile(NodePath.join(directory, "t3-launcher"), shimFor(baseDir), 0o700);
  await createFile(NodePath.join(directory, "attempts"), "0\n", 0o600);
  await createFile(NodePath.join(directory, "previous-service-definition"), previousService, 0o600);
  const temporary = NodePath.join(directory, `.current-${NodeCrypto.randomUUID()}`);
  await NodeFSP.symlink(target, temporary);
  await NodeFSP.rename(temporary, NodePath.join(directory, "current"));
  await syncDirectory(NodePath.join(directory, "handoff"));
  await syncDirectory(directory);
  await syncDirectory(parent);
  const result = await readLauncherPointer(baseDir);
  if (result === undefined) throw new Error("Launcher pointer bootstrap readback is missing.");
  return result;
}
