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

export interface LauncherHandoffReceipt {
  readonly schema: 1;
  readonly requestId: string;
  readonly fromVersion: string;
  readonly targetVersion: string;
  readonly fromSha256: string;
  readonly targetSha256: string;
  readonly status: "requested" | "committed" | "rejected";
}

const HANDOFF_ID = /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;
const HANDOFF_FILE = /^([a-f0-9-]{36})\.(requested|committed|rejected)\.json$/;

async function executableDigest(directory: string, version: string, uid: number): Promise<string> {
  await validateRuntime(directory, version, uid);
  const file = NodePath.join(NodePath.resolve(directory, targetOf(version)), "t3");
  const descriptor = await NodeFSP.open(
    file,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  );
  try {
    const before = await descriptor.stat({ bigint: true });
    if (
      !before.isFile() ||
      before.uid !== BigInt(uid) ||
      (before.mode & 0o022n) !== 0n ||
      (before.mode & 0o111n) === 0n ||
      before.size > 512n * 1024n * 1024n
    )
      throw new Error("Launcher executable has unknown ownership or size.");
    const hash = NodeCrypto.createHash("sha256");
    const buffer = Buffer.alloc(64 * 1024);
    let offset = 0;
    const size = Number(before.size);
    while (offset < size) {
      const { bytesRead } = await descriptor.read(
        buffer,
        0,
        Math.min(buffer.length, size - offset),
        offset,
      );
      if (bytesRead === 0) throw new Error("Launcher executable changed while hashing.");
      hash.update(buffer.subarray(0, bytesRead));
      offset += bytesRead;
    }
    const after = await descriptor.stat({ bigint: true });
    if (
      after.size !== before.size ||
      after.mtimeNs !== before.mtimeNs ||
      after.ctimeNs !== before.ctimeNs
    )
      throw new Error("Launcher executable changed while hashing.");
    return hash.digest("hex");
  } finally {
    await descriptor.close();
  }
}

export async function readLauncherHandoff(
  baseDir: string,
): Promise<LauncherHandoffReceipt | undefined> {
  const directory = NodePath.join(directoryOf(baseDir), "handoff");
  const uid = NodeOS.userInfo().uid;
  await privateDirectory(directory, uid);
  const latest = NodePath.join(directory, "latest");
  const stat = await NodeFSP.lstat(latest).catch((cause: NodeJS.ErrnoException) => {
    if (cause.code === "ENOENT") return undefined;
    throw cause;
  });
  if (stat === undefined) return undefined;
  if (!stat.isSymbolicLink() || stat.uid !== uid)
    throw new Error("Launcher handoff selector has unknown ownership.");
  const name = await NodeFSP.readlink(latest);
  const match = HANDOFF_FILE.exec(name);
  if (match === null || !HANDOFF_ID.test(match[1] ?? ""))
    throw new Error("Launcher handoff selector escapes its receipt directory.");
  const value: unknown = JSON.parse(
    await readPrivate(NodePath.join(directory, name), uid, 0o600, 2048),
  );
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new Error("Launcher handoff receipt is malformed.");
  const receipt = decodeLauncherHandoffReceipt(value);
  if (receipt === undefined || receipt.requestId !== match[1] || receipt.status !== match[2])
    throw new Error("Launcher handoff receipt is malformed.");
  return receipt;
}

export const isLauncherPointerVersion = (value: unknown): value is string =>
  typeof value === "string" && VERSION.test(value);

export function decodeLauncherHandoffReceipt(value: unknown): LauncherHandoffReceipt | undefined {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const receipt = value as LauncherHandoffReceipt;
  return receipt.schema === 1 &&
    typeof receipt.requestId === "string" &&
    HANDOFF_ID.test(receipt.requestId) &&
    (receipt.status === "requested" ||
      receipt.status === "committed" ||
      receipt.status === "rejected") &&
    isLauncherPointerVersion(receipt.fromVersion) &&
    isLauncherPointerVersion(receipt.targetVersion) &&
    receipt.fromVersion !== receipt.targetVersion &&
    typeof receipt.fromSha256 === "string" &&
    /^[a-f0-9]{64}$/.test(receipt.fromSha256) &&
    typeof receipt.targetSha256 === "string" &&
    /^[a-f0-9]{64}$/.test(receipt.targetSha256)
    ? receipt
    : undefined;
}

async function retainHandoff(baseDir: string, receipt: LauncherHandoffReceipt): Promise<void> {
  const directory = NodePath.join(directoryOf(baseDir), "handoff");
  await privateDirectory(directory, NodeOS.userInfo().uid);
  const name = receipt.requestId + "." + receipt.status + ".json";
  const file = NodePath.join(directory, name);
  const contents = JSON.stringify(receipt) + "\n";
  try {
    await createFile(file, contents, 0o600);
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code !== "EEXIST") throw cause;
    if ((await readPrivate(file, NodeOS.userInfo().uid, 0o600, 2048)) !== contents)
      throw new Error("Launcher handoff receipt collides with retained history.", { cause });
    const descriptor = await NodeFSP.open(
      file,
      NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
    );
    try {
      await descriptor.sync();
    } finally {
      await descriptor.close();
    }
  }
  await syncDirectory(directory);
  const temporary = NodePath.join(directory, ".latest-" + NodeCrypto.randomUUID());
  await NodeFSP.symlink(name, temporary);
  await NodeFSP.rename(temporary, NodePath.join(directory, "latest"));
  await syncDirectory(directory);
}

async function resetAttempts(directory: string): Promise<void> {
  const temporary = NodePath.join(directory, ".attempts-" + NodeCrypto.randomUUID());
  await createFile(temporary, "0\n", 0o600);
  await NodeFSP.rename(temporary, NodePath.join(directory, "attempts"));
  await syncDirectory(directory);
}

/** Caller owns the existing launcher lock and has fenced work and verified manager membership. */
export async function requestLauncherHandoff(
  baseDir: string,
  input: {
    readonly requestId: string;
    readonly fromVersion: string;
    readonly targetVersion: string;
  },
  beforePending?: () => Promise<boolean>,
): Promise<LauncherHandoffReceipt | { readonly status: "deferred" } | undefined> {
  if (!HANDOFF_ID.test(input.requestId))
    throw new Error("Launcher handoff requires a fixed operation UUID.");
  const pointer = await readLauncherPointer(baseDir);
  if (pointer === undefined) return undefined;
  if (pointer.currentVersion !== input.fromVersion || input.fromVersion === input.targetVersion)
    throw new Error("Launcher handoff is not bound to the retained current launcher.");
  const directory = directoryOf(baseDir);
  const uid = NodeOS.userInfo().uid;
  const fromSha256 = await executableDigest(directory, input.fromVersion, uid);
  const targetSha256 = await executableDigest(directory, input.targetVersion, uid);
  if (fromSha256 === targetSha256) return undefined;
  const receipt: LauncherHandoffReceipt = {
    schema: 1,
    ...input,
    fromSha256,
    targetSha256,
    status: "requested",
  };
  const previous = await readLauncherHandoff(baseDir);
  if (previous?.status === "requested") {
    if (JSON.stringify(previous) !== JSON.stringify(receipt))
      throw new Error("Another launcher handoff is unsettled.");
    if (pointer.pendingVersion === undefined && pointer.attempts === 3)
      throw new Error("Launcher handoff exhausted its retained attempt budget.");
  } else {
    if (pointer.pendingVersion !== undefined)
      throw new Error("Launcher pending pointer has no matching request.");
    if (
      previous?.requestId === input.requestId ||
      (previous?.status === "rejected" && previous.targetVersion === input.targetVersion)
    )
      throw new Error("Launcher handoff was already settled.");
  }
  if (pointer.pendingVersion !== undefined && pointer.pendingVersion !== input.targetVersion)
    throw new Error("Launcher pending pointer differs from its request.");
  if (beforePending !== undefined && !(await beforePending())) return { status: "deferred" };
  if (previous?.status !== "requested") {
    await resetAttempts(directory);
    await retainHandoff(baseDir, receipt);
  }
  if (pointer.pendingVersion === undefined) {
    // The request is durable before publishing the next manager start.
    await NodeFSP.symlink(targetOf(input.targetVersion), NodePath.join(directory, "pending"));
  }
  await syncDirectory(directory);
  return receipt;
}

/** Only the captured healthy child may reach this seam, after capability publication. */
export async function settleLauncherHandoff(
  baseDir: string,
  launcherVersion: string,
  healthyChildVersion: string,
): Promise<LauncherHandoffReceipt | undefined> {
  const pointer = await readLauncherPointer(baseDir);
  if (pointer === undefined) return undefined;
  const receipt = await readLauncherHandoff(baseDir);
  if (receipt === undefined || receipt.status !== "requested") return receipt;
  const directory = directoryOf(baseDir);
  const uid = NodeOS.userInfo().uid;
  if (
    healthyChildVersion !== receipt.targetVersion ||
    (await executableDigest(directory, receipt.fromVersion, uid)) !== receipt.fromSha256 ||
    (await executableDigest(directory, receipt.targetVersion, uid)) !== receipt.targetSha256
  )
    throw new Error("Launcher handoff health or immutable executable binding differs.");
  if (
    launcherVersion === receipt.targetVersion &&
    (pointer.currentVersion === receipt.fromVersion ||
      pointer.currentVersion === receipt.targetVersion) &&
    (pointer.pendingVersion === receipt.targetVersion ||
      (pointer.pendingVersion === undefined && pointer.currentVersion === receipt.targetVersion))
  ) {
    if (pointer.pendingVersion !== undefined) {
      if (pointer.attempts < 1)
        throw new Error("Pending launcher was not started through the stable shim.");
      await NodeFSP.rename(
        NodePath.join(directory, "pending"),
        NodePath.join(directory, "current"),
      );
      await syncDirectory(directory);
    }
    await resetAttempts(directory);
    const committed = { ...receipt, status: "committed" as const };
    await retainHandoff(baseDir, committed);
    return committed;
  }
  if (
    launcherVersion === receipt.fromVersion &&
    pointer.currentVersion === receipt.fromVersion &&
    pointer.pendingVersion === undefined &&
    pointer.attempts === 3
  ) {
    const rejected = { ...receipt, status: "rejected" as const };
    await retainHandoff(baseDir, rejected);
    return rejected;
  }
  if (
    launcherVersion === receipt.fromVersion &&
    pointer.currentVersion === receipt.fromVersion &&
    pointer.pendingVersion === undefined &&
    pointer.attempts < 3
  )
    return receipt;
  throw new Error("Launcher handoff is not a healthy pending start or bounded fallback.");
}
