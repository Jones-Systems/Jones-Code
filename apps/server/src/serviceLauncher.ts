// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalTimers:off
// @effect-diagnostics globalDate:off
// The launcher supervises the server child for the boot service and must keep
// working across server versions, so it stays on Node built-ins with no Effect
// runtime: it is the one part of the executable that cannot depend on the
// rest of it being loadable.
import * as NodeChildProcess from "node:child_process";
import { SERVER_CHILD_SHUTDOWN_GRACE_MS } from "./jones/cloud/shutdownBudget.ts";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import packageJson from "../package.json" with { type: "json" };
import {
  publishLauncherCapabilityReceipt,
  retractLauncherCapabilityReceipt,
} from "./jones/cloud/launcherCapability.ts";
import {
  launcherDriftNotice,
  QualifiedCommitFailure,
  qualifiedRollbackRefusal,
  type QualifiedCommitAttempt,
} from "./jones/cloud/qualifiedCommitAttempt.ts";
import {
  verifyStagedQualifiedRuntime,
  readQualifiedRuntimeReceipt,
  withQualifiedRuntimeLock,
  QUALIFIED_UPDATES_PROTOCOL,
  type StagedQualifiedRuntime,
} from "./jones/cloud/qualifiedRuntime.ts";

import { withStoppedServerOwnership } from "./jones/cloud/stoppedServerOwnership.ts";
import {
  assertQualifiedTrialBinding,
  readJonesStartupGateProtocol,
  type QualifiedTrialReceipt,
} from "./jones/cloud/qualifiedStartup.ts";
import {
  QUALIFIED_STARTUP_STATE_FILES,
  copyQualifiedBackupFile,
  preflightQualifiedBackup,
  retainQualifiedAdvancedState,
  restoreQualifiedBackupFile,
  writeQualifiedBackupReceipt,
  type QualifiedBackupAdapter,
} from "./jones/cloud/qualifiedBackup.ts";

import {
  decodeServicePreflightResult,
  qualifiedServicePreflightFailure,
} from "./cloud/servicePreflight.ts";
import type { MigrationPlan } from "./jones/updates/migrationPlan.ts";
import {
  archiveUpdateOperation,
  assertNoUnreconciledUpdateOperations,
  operationBinding,
  readOperationReservation,
  reconcileUpdateOperation,
  reserveUpdateOperation,
} from "./jones/updates/launcherOperation.ts";

import { assertStageNotRevoked, retireNativeStage } from "./jones/updates/stageRevocation.ts";
import * as NodeUtil from "node:util";

import type {
  PendingServiceUpdate,
  ServiceLauncherChildMessage,
  ServiceLauncherContext,
  ServiceLauncherParentMessage,
  ServiceState,
  ServiceUpdateRecord,
} from "./cloud/serviceProtocol.ts";
import {
  compareExactServiceVersions,
  decodeServiceLauncherChildMessage,
  isExactServiceVersion,
  parseServiceState,
  SERVICE_ENV_FILE,
  SERVICE_LAUNCHER_CONTEXT_ENV,
  SERVICE_LAUNCHER_PROTOCOL,
  SERVICE_STATE_FILE,
  SERVICE_RESTART_PENDING_FILE,
  SERVICE_STOP_MARKER_FILE,
} from "./cloud/serviceProtocol.ts";

import {
  advanceNativeStoreAuthorityForBaseDir,
  fenceNativeStoreAuthorityForBaseDir,
} from "./environment/nativeStoreAuthorityPersistence.ts";

const HANDOFF_DELAY_MS = 2_000;
/**
 * Migrations count toward it and can scale with the database: tens of GB on a cold page cache
 * take minutes. A missed window rolls back the database, so the next attempt pays the same cost.
 */
const PREPARED_TIMEOUT_MS = 30 * 60_000;
const TERMINATE_GRACE_MS = 5_000;
// Matches SERVER_EXIT_CODE_STATE_DIR_OWNED in contracts. The standalone
// launcher stays independent of the Effect-backed contracts package.
const STATE_DIR_OWNED_EXIT_CODE = 78;
// The server abandons a launcher exchange after 30 seconds (serviceLauncherClient).
// Past this fence, measured from message receipt, a qualified Install is not
// accepted and a qualified pointer is not committed. The margin covers the
// durable write and reply on a responsive host; it is not a guarantee.
const QUALIFIED_EXCHANGE_FENCE_MS = 25_000;

type TerminalStatus = "committed" | "rolled-back" | "failed";
type ChildRole = "active" | "trial";

interface ManagedChild {
  readonly version: string;
  role: ChildRole;
  readonly process: NodeChildProcess.ChildProcess;
}

const runtimePaths = (baseDir: string, version: string) => {
  const versionDir = NodePath.join(baseDir, "runtime", "versions", version);
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone launcher has no Effect runtime.
  const executableName = process.platform === "win32" ? "t3.exe" : "t3";
  return {
    versionDir,
    entryPath: NodePath.join(versionDir, executableName),
    sentinelPath: NodePath.join(versionDir, ".install-complete"),
  };
};

class QualifiedCandidatePreflightError extends Error {}

const preflightQualifiedCandidate = async (
  baseDir: string,
  staged: StagedQualifiedRuntime,
): Promise<MigrationPlan | undefined> => {
  const env = { ...process.env };
  delete env[SERVICE_LAUNCHER_CONTEXT_ENV];
  const result = await new Promise<{ readonly code: number | null; readonly stdout: string }>(
    (resolve, reject) => {
      NodeChildProcess.execFile(
        runtimePaths(baseDir, staged.receipt.version).entryPath,
        [
          "__service-preflight",
          "--database-path",
          staged.binding.dbPath,
          "--launcher-protocol",
          String(SERVICE_LAUNCHER_PROTOCOL),
        ],
        { env, timeout: 30_000, maxBuffer: 64 * 1024, encoding: "utf8" },
        (error, stdout) => {
          if (
            error !== null &&
            (typeof error.code !== "number" ||
              error.killed ||
              (error.signal !== null && error.signal !== undefined))
          ) {
            reject(
              new Error("startup-gate-unavailable: Could not preflight the qualified candidate.", {
                cause: error,
              }),
            );
            return;
          }
          resolve({ code: error === null ? 0 : Number(error.code), stdout });
        },
      );
    },
  );
  const reason = qualifiedServicePreflightFailure({ ...result, version: staged.receipt.version });
  if (reason !== undefined) throw new QualifiedCandidatePreflightError(reason);
  const preflight = decodeServicePreflightResult(JSON.parse(result.stdout.trim()) as unknown);
  return preflight?.status === "ready" ? preflight.migrationPlan : undefined;
};

const runtimeSpawnArguments = (paths: ReturnType<typeof runtimePaths>) => ({
  command: paths.entryPath,
  args: ["serve"],
});

const DB_FILE_SUFFIXES = ["", "-wal", "-shm"] as const;
const RESTORE_MARKER = ".restore-pending";
const PAIRED_SETTINGS = QUALIFIED_STARTUP_STATE_FILES;

const databaseBackupDir = (baseDir: string, updateId: string) =>
  NodePath.join(baseDir, "runtime", "db-backup", updateId);

const databaseBackupFile = (backupDir: string, suffix: (typeof DB_FILE_SUFFIXES)[number]) =>
  NodePath.join(backupDir, suffix === "" ? "database" : `database${suffix}`);

export const configuredDatabasePathForBaseDir = (baseDir: string): string =>
  NodePath.resolve(baseDir, "userdata", "statev2.sqlite");

export const validateDatabasePathForBaseDir = (baseDir: string, databasePath: string): string => {
  const configuredPath = configuredDatabasePathForBaseDir(baseDir);
  if (NodePath.resolve(databasePath) !== configuredPath) {
    throw new Error("Service update database path must be the configured userdata/statev2.sqlite.");
  }
  return configuredPath;
};

async function pathExists(target: string): Promise<boolean> {
  try {
    await NodeFSP.access(target);
    return true;
  } catch (cause) {
    if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return false;
    throw cause;
  }
}

const MANAGED_SERVICE_ENV_NAMES = new Set([
  "PATH",
  "T3CODE_HOME",
  "T3_BOOT_SERVICE_UNIT",
  SERVICE_LAUNCHER_CONTEXT_ENV,
]);

export async function readServiceEnvironment(
  baseDir: string,
  inherited: NodeJS.ProcessEnv = process.env,
): Promise<NodeJS.ProcessEnv> {
  const environment = { ...inherited };
  let contents: string;
  try {
    contents = await NodeFSP.readFile(NodePath.join(baseDir, SERVICE_ENV_FILE), "utf8");
  } catch (cause) {
    if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return environment;
    throw cause;
  }
  for (const [name, value] of Object.entries(NodeUtil.parseEnv(contents))) {
    if (
      /^[A-Za-z_][A-Za-z0-9_]*$/.test(name) &&
      !MANAGED_SERVICE_ENV_NAMES.has(name.toUpperCase())
    ) {
      environment[name] = value;
    }
  }
  return environment;
}

// Opened read-write: Windows refuses to flush a handle without write access.
async function syncFile(filePath: string): Promise<void> {
  const handle = await NodeFSP.open(filePath, "r+");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

// Flushes a directory entry so a rename into it survives power loss. Windows
// has no directory fsync: the handle opens but sync fails with EPERM, and
// NTFS journals the rename on its own.
async function syncDirectory(directory: string): Promise<void> {
  const handle = await NodeFSP.open(directory, "r");
  try {
    await handle.sync();
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EPERM") throw error;
  } finally {
    await handle.close();
  }
}

function runCp(args: ReadonlyArray<string>): Promise<boolean> {
  return new Promise((resolve) => {
    NodeChildProcess.execFile("cp", args, (error) => resolve(error === null));
  });
}

/**
 * Node's own clone flag is not used: on macOS `fs.copyFile` falls back to a full copy.
 */
async function copyDatabaseFile(source: string, destination: string): Promise<void> {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone launcher has no Effect runtime.
  const platform = process.platform;
  // `cp -c` replaces the destination with a new file, which would cut a
  // hard-linked database off from its other links; copy into it instead.
  const hardLinked =
    platform === "darwin" &&
    ((await NodeFSP.stat(destination).catch(() => undefined))?.nlink ?? 1) > 1;
  const args =
    platform === "darwin" && !hardLinked
      ? ["-c", source, destination]
      : platform === "linux"
        ? ["--reflink=auto", source, destination]
        : undefined;
  if (args === undefined || !(await runCp(args))) await NodeFSP.copyFile(source, destination);
}

const GIB = 1024 ** 3;
/**
 * Clones share blocks, but the trial server diverges from them as it writes,
 * migrations included, so a cloned backup still needs room for that churn.
 */
const CLONED_BACKUP_MIN_HEADROOM_BYTES = GIB;
const CLONED_BACKUP_HEADROOM_FRACTION = 0.1;

async function statDatabaseFiles(dbPath: string): Promise<ReadonlyArray<NodeFS.Stats>> {
  const stats: Array<NodeFS.Stats> = [];
  for (const suffix of DB_FILE_SUFFIXES) {
    const stat = await NodeFSP.stat(`${dbPath}${suffix}`).catch((cause: unknown) => {
      if (suffix !== "" && (cause as NodeJS.ErrnoException).code === "ENOENT") return undefined;
      throw cause;
    });
    if (stat !== undefined) stats.push(stat);
  }
  return stats;
}

/**
 * - macOS: `cp -c` clones within one APFS volume and silently copies
 *   anywhere else, so every existing database file must share the backup
 *   directory's device, whose filesystem type must match the system volume's,
 *   which is always APFS.
 */
async function backupCanClone(dbPath: string, backupRoot: string): Promise<boolean> {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone launcher has no Effect runtime.
  const platform = process.platform;
  if (platform === "darwin") {
    const [backupDirectory, backupFs, systemFs] = await Promise.all([
      NodeFSP.stat(backupRoot),
      NodeFSP.statfs(backupRoot),
      NodeFSP.statfs("/"),
    ]);
    if (backupFs.type !== systemFs.type) return false;
    return (await statDatabaseFiles(dbPath)).every(
      (source) => source.dev === backupDirectory.dev && source.nlink === 1,
    );
  }
  if (platform !== "linux") return false;
  const probe = NodePath.join(backupRoot, `.clone-probe-${NodeCrypto.randomUUID()}`);
  try {
    for (const suffix of DB_FILE_SUFFIXES) {
      const source = `${dbPath}${suffix}`;
      if (suffix !== "" && !(await pathExists(source))) continue;
      if (!(await runCp(["--reflink=always", source, `${probe}${suffix}`]))) return false;
    }
    return true;
  } finally {
    for (const suffix of DB_FILE_SUFFIXES) {
      await NodeFSP.rm(`${probe}${suffix}`, { force: true }).catch(() => undefined);
    }
  }
}

export interface BackupSpaceProbe {
  readonly readFreeBytes: (directory: string) => Promise<number>;
  readonly canClone: (dbPath: string, backupRoot: string) => Promise<boolean>;
}

const filesystemBackupSpaceProbe: BackupSpaceProbe = {
  readFreeBytes: async (directory) => {
    const stats = await NodeFSP.statfs(directory);
    return stats.bavail * stats.bsize;
  },
  canClone: backupCanClone,
};

/**
 * Runs before the server is stopped so a full disk refuses the update instead of causing an
 * outage and a cold restart.
 */
export async function databaseBackupBlocker(
  baseDir: string,
  dbPath: string,
  probe: Partial<BackupSpaceProbe> = {},
): Promise<string | undefined> {
  const { readFreeBytes, canClone } = { ...filesystemBackupSpaceProbe, ...probe };
  const backupRoot = NodePath.join(baseDir, "runtime", "db-backup");
  await NodeFSP.mkdir(backupRoot, { recursive: true, mode: 0o700 });
  const [files, free] = await Promise.all([statDatabaseFiles(dbPath), readFreeBytes(backupRoot)]);
  const size = files.reduce((total, file) => total + file.size, 0);
  const fullCopyBytes = size * 1.05;
  if (free >= fullCopyBytes) return undefined;
  const clonedBytes = Math.max(
    CLONED_BACKUP_MIN_HEADROOM_BYTES,
    size * CLONED_BACKUP_HEADROOM_FRACTION,
  );
  const needed = (await canClone(dbPath, backupRoot)) ? clonedBytes : fullCopyBytes;
  if (free >= needed) return undefined;
  const gb = (bytes: number) => (bytes / GIB).toFixed(1);
  return `Not enough free disk space to back up the database before updating: ${gb(needed)} GB needed, ${gb(free)} GB free. Free up disk space and try again. The server was not stopped.`;
}

/**
 * A completed backup is never overwritten because a restarted launcher may be looking at
 * database writes from an earlier attempt by the same trial.
 */
async function backupDatabaseOnce(
  baseDir: string,
  pending: PendingServiceUpdate,
  adapter?: QualifiedBackupAdapter,
): Promise<void> {
  validateDatabasePathForBaseDir(baseDir, pending.dbPath);
  const backupDir = databaseBackupDir(baseDir, pending.id);
  if (await pathExists(backupDir)) {
    if (!(await databaseBackupAvailable(baseDir, pending.id))) {
      throw new Error("Cannot use an incomplete database backup.");
    }
    if (pending.qualified !== undefined) await validatePairedBackup(baseDir, pending, backupDir);
    return;
  }

  const stagingDir = `${backupDir}.staging`;
  if (pending.qualified !== undefined && (await pathExists(stagingDir)))
    throw new Error("An occupied paired snapshot staging path was preserved for reconciliation.");
  if (pending.qualified === undefined)
    await NodeFSP.rm(stagingDir, { recursive: true, force: true });
  await NodeFSP.mkdir(stagingDir, { recursive: true, mode: 0o700 });
  try {
    const startedAt = new Date().toISOString();
    const started = performance.now();
    let method: "clone" | "copy" = "clone";
    let bytes = 0;
    let copyMs = 0;
    let syncMs = 0;
    if (pending.qualified !== undefined) {
      for (const suffix of DB_FILE_SUFFIXES) {
        const source = `${pending.dbPath}${suffix}`;
        if (suffix !== "" && !(await pathExists(source))) continue;
        const destination = databaseBackupFile(stagingDir, suffix);
        const copyStarted = performance.now();
        const copied = await copyQualifiedBackupFile(source, destination, adapter);
        copyMs += performance.now() - copyStarted;
        bytes += copied.bytes;
        if (copied.method === "copy") method = "copy";
        const syncStarted = performance.now();
        await syncFile(destination);
        syncMs += performance.now() - syncStarted;
      }
      const settingsTiming = await backupPairedSettings(baseDir, stagingDir);
      copyMs += settingsTiming.copyMs;
      syncMs += settingsTiming.syncMs;
      const bindingPath = NodePath.join(stagingDir, "paired-binding.json");
      await NodeFSP.writeFile(
        bindingPath,
        JSON.stringify({ transactionId: pending.id, qualified: pending.qualified }),
        { flag: "wx", mode: 0o600 },
      );
      const bindingSyncStarted = performance.now();
      await syncFile(bindingPath);
      syncMs += performance.now() - bindingSyncStarted;
      await writeQualifiedBackupReceipt(stagingDir, {
        protocol: 1,
        updateId: pending.id,
        method,
        bytes,
        startedAt,
        completedAt: new Date().toISOString(),
        durationMs: performance.now() - started,
        copyMs,
        syncMs,
      });
    }
    for (const suffix of pending.qualified === undefined ? DB_FILE_SUFFIXES : []) {
      const source = `${pending.dbPath}${suffix}`;
      if (suffix !== "" && !(await pathExists(source))) continue;
      const destination = databaseBackupFile(stagingDir, suffix);
      await copyDatabaseFile(source, destination);
      await syncFile(destination);
    }
    await NodeFSP.rename(stagingDir, backupDir);
    await syncDirectory(NodePath.dirname(backupDir));
  } catch (cause) {
    await NodeFSP.rm(stagingDir, { recursive: true, force: true }).catch(() => undefined);
    throw cause;
  }
}

async function backupPairedSettings(
  baseDir: string,
  destination: string,
): Promise<{ readonly copyMs: number; readonly syncMs: number }> {
  const present: string[] = [];
  let copyMs = 0;
  let syncMs = 0;
  for (const name of PAIRED_SETTINGS) {
    const source = NodePath.join(baseDir, "userdata", name);
    if (!(await pathExists(source))) continue;
    const stat = await NodeFSP.lstat(source);
    if (!stat.isFile() || stat.isSymbolicLink())
      throw new Error("Startup settings must be regular files.");
    const target = NodePath.join(destination, name);
    const copyStarted = performance.now();
    await NodeFSP.copyFile(source, target);
    copyMs += performance.now() - copyStarted;
    const syncStarted = performance.now();
    await syncFile(target);
    syncMs += performance.now() - syncStarted;
    present.push(name);
  }
  const manifest = NodePath.join(destination, "paired-settings.json");
  await NodeFSP.writeFile(manifest, JSON.stringify(present), { flag: "wx", mode: 0o600 });
  const syncStarted = performance.now();
  await syncFile(manifest);
  syncMs += performance.now() - syncStarted;
  return { copyMs, syncMs };
}

async function restorePairedSettings(baseDir: string, backupDir: string): Promise<void> {
  const manifest: unknown = JSON.parse(
    await NodeFSP.readFile(NodePath.join(backupDir, "paired-settings.json"), "utf8"),
  );
  if (
    !Array.isArray(manifest) ||
    manifest.some(
      (name) => typeof name !== "string" || !PAIRED_SETTINGS.some((allowed) => name === allowed),
    )
  )
    throw new Error("Paired settings receipt is invalid.");
  for (const name of PAIRED_SETTINGS) {
    const target = NodePath.join(baseDir, "userdata", name);
    if (manifest.includes(name)) {
      await restoreQualifiedBackupFile(NodePath.join(backupDir, name), target);
    } else await NodeFSP.rm(target, { force: true });
  }
  await syncDirectory(NodePath.join(baseDir, "userdata"));
}

async function validatePairedBackup(
  baseDir: string,
  pending: PendingServiceUpdate,
  directory: string,
): Promise<void> {
  const bindingPath = NodePath.join(directory, "paired-binding.json");
  const stat = await NodeFSP.lstat(bindingPath);
  if (!stat.isFile() || stat.isSymbolicLink())
    throw new Error("Paired backup binding is not a regular file.");
  const binding: unknown = JSON.parse(await NodeFSP.readFile(bindingPath, "utf8"));
  if (
    JSON.stringify(binding) !==
      JSON.stringify({ transactionId: pending.id, qualified: pending.qualified }) ||
    pending.qualified?.binding.baseDir !== (await NodeFSP.realpath(baseDir))
  )
    throw new Error("Paired backup belongs to a different transaction or native home.");
  const settings = await NodeFSP.lstat(NodePath.join(directory, "paired-settings.json"));
  if (!settings.isFile() || settings.isSymbolicLink())
    throw new Error("Paired backup settings receipt is incomplete.");
}

const databaseBackupAvailable = async (baseDir: string, updateId: string): Promise<boolean> => {
  try {
    const stat = await NodeFSP.lstat(databaseBackupFile(databaseBackupDir(baseDir, updateId), ""));
    return stat.isFile() && !stat.isSymbolicLink();
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw cause;
  }
};

const restoreMarkerPath = (baseDir: string, updateId: string) =>
  NodePath.join(databaseBackupDir(baseDir, updateId), RESTORE_MARKER);

const databaseRestorePending = (baseDir: string, pending: PendingServiceUpdate) =>
  pathExists(restoreMarkerPath(baseDir, pending.id));

/** Mark rollback before changing live files so launcher recovery cannot boot a partial restore. */
async function markDatabaseRestorePending(backupDir: string): Promise<void> {
  const markerPath = NodePath.join(backupDir, RESTORE_MARKER);
  if (!(await pathExists(markerPath))) {
    const handle = await NodeFSP.open(markerPath, "wx", 0o600);
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
    await syncDirectory(backupDir);
  }
}

/**
 * The file a restore writes for `path`: where its symlinks lead, even when the
 * last target is missing, as after a crash between `cp -c` removing a file
 * and cloning over it.
 */
async function restoreTarget(path: string): Promise<string> {
  let current = path;
  for (let hops = 0; hops < 40; hops++) {
    try {
      return await NodeFSP.realpath(current);
    } catch (cause) {
      if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
    }
    const link = await NodeFSP.readlink(current).catch((cause: unknown) => {
      const code = (cause as NodeJS.ErrnoException).code;
      if (code === "ENOENT" || code === "EINVAL") return undefined;
      throw cause;
    });
    if (link === undefined) return current;
    // Joined without normalizing: a `..` in the link must follow directory links
    // the way the filesystem does, not cancel a path segment.
    current = NodePath.isAbsolute(link)
      ? link
      : `${await NodeFSP.realpath(NodePath.dirname(current))}${NodePath.sep}${link}`;
  }
  throw new Error(`Too many symbolic links resolving ${path}.`);
}

async function restoreDatabaseBackup(
  baseDir: string,
  pending: PendingServiceUpdate,
): Promise<void> {
  validateDatabasePathForBaseDir(baseDir, pending.dbPath);
  const backupDir = databaseBackupDir(baseDir, pending.id);
  if (!(await databaseBackupAvailable(baseDir, pending.id))) {
    fenceNativeStoreAuthorityForBaseDir(baseDir);
    throw new Error("Cannot rollback while the native database backup is missing.");
  }
  // Persist restore intent before moving live files or fencing authority; restart
  // resumes the same rename journal and never captures partly restored state.
  if (pending.qualified !== undefined) {
    await validatePairedBackup(baseDir, pending, backupDir);
  }
  await markDatabaseRestorePending(backupDir);
  fenceNativeStoreAuthorityForBaseDir(baseDir);
  if (pending.qualified !== undefined)
    await retainQualifiedAdvancedState(baseDir, pending.dbPath, backupDir);
  const directories = new Set([NodePath.dirname(pending.dbPath)]);
  for (const suffix of DB_FILE_SUFFIXES) {
    const livePath = `${pending.dbPath}${suffix}`;
    const source = databaseBackupFile(backupDir, suffix);
    if (await pathExists(source)) {
      // Restore through the symlink target so replacing a clone preserves the link itself.
      const target = await restoreTarget(livePath);
      if (pending.qualified !== undefined) await restoreQualifiedBackupFile(source, target);
      else {
        await copyDatabaseFile(source, target);
        await syncFile(target);
      }
      directories.add(NodePath.dirname(target));
    } else {
      await NodeFSP.rm(livePath, { force: true });
    }
  }
  for (const directory of directories) await syncDirectory(directory);
  if (pending.qualified !== undefined) await restorePairedSettings(baseDir, backupDir);
  advanceNativeStoreAuthorityForBaseDir(baseDir, pending.dbPath);
}

async function discardDatabaseBackup(
  baseDir: string,
  updateId: string,
  retainReceipts = false,
): Promise<void> {
  const backupDir = databaseBackupDir(baseDir, updateId);
  if (!(await pathExists(backupDir))) return;
  if (retainReceipts) {
    // Startup still reserves resume here, and update status reads the timing receipt.
    // Remove only recovery payload; leave their directory and all metadata in place.
    for (const name of ["database", "database-wal", "database-shm", ...PAIRED_SETTINGS]) {
      await NodeFSP.rm(NodePath.join(backupDir, name), { force: true });
    }
    await NodeFSP.rm(NodePath.join(backupDir, "advanced-state"), { recursive: true, force: true });
    await syncDirectory(backupDir);
  } else {
    await NodeFSP.rm(backupDir, { recursive: true, force: true });
    await syncDirectory(NodePath.dirname(backupDir));
  }
}

async function cleanupTerminalDatabaseBackup(
  baseDir: string,
  updateId: string,
  retainReceipts: boolean,
): Promise<void> {
  try {
    await discardDatabaseBackup(baseDir, updateId, retainReceipts);
  } catch {
    process.stderr.write(
      "[service-launcher] Recovery payload cleanup did not finish; the durable update outcome is unchanged and remaining recovery files were retained.\n",
    );
  }
}

export async function readServiceState(filePath: string): Promise<ServiceState> {
  const contents = await NodeFSP.readFile(filePath, "utf8");
  const state = parseServiceState(contents);
  if (state === undefined) throw new Error("Service state is invalid or unsupported.");
  return state;
}

export async function writeServiceState(filePath: string, state: ServiceState): Promise<void> {
  const directory = NodePath.dirname(filePath);
  await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
  const tempPath = NodePath.join(
    directory,
    `.${NodePath.basename(filePath)}.${process.pid}.${NodeCrypto.randomUUID()}`,
  );
  let handle: NodeFSP.FileHandle | undefined;
  try {
    handle = await NodeFSP.open(tempPath, "wx", 0o600);
    await handle.writeFile(`${JSON.stringify(state, null, 2)}\n`, "utf8");
    await handle.sync();
    await handle.close();
    handle = undefined;
    await NodeFSP.rename(tempPath, filePath);
    await syncDirectory(directory);
  } finally {
    await handle?.close().catch(() => undefined);
    await NodeFSP.rm(tempPath, { force: true }).catch(() => undefined);
  }
}

async function runtimeExists(baseDir: string, version: string): Promise<boolean> {
  const paths = runtimePaths(baseDir, version);
  try {
    const [entry, sentinel] = await Promise.all([
      NodeFSP.stat(paths.entryPath),
      NodeFSP.readFile(paths.sentinelPath, "utf8"),
    ]);
    return entry.isFile() && sentinel.trim() === version;
  } catch {
    return false;
  }
}

function terminalUpdate<S extends TerminalStatus>(input: {
  readonly pending: PendingServiceUpdate;
  readonly status: S;
  readonly reason?: string;
}): Exclude<ServiceUpdateRecord, PendingServiceUpdate> & { readonly status: S } {
  return {
    id: input.pending.id,
    fromVersion: input.pending.fromVersion,
    targetVersion: input.pending.targetVersion,
    status: input.status,
    ...(input.pending.qualified === undefined ? {} : { qualified: input.pending.qualified }),
    ...(input.pending.migrationPlan === undefined
      ? {}
      : { migrationPlan: input.pending.migrationPlan }),
    ...(input.reason === undefined ? {} : { reason: input.reason }),
  };
}

function sendMessage(
  child: NodeChildProcess.ChildProcess,
  message: ServiceLauncherParentMessage,
): Promise<void> {
  return new Promise((resolve, reject) => {
    if (!child.connected || child.send === undefined) {
      reject(new Error("service child IPC is disconnected."));
      return;
    }
    child.send(message, (error) => (error === null ? resolve() : reject(error)));
  });
}

function waitForExit(child: NodeChildProcess.ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve();
  return new Promise((resolve) => child.once("exit", () => resolve()));
}

async function terminateChild(
  child: NodeChildProcess.ChildProcess,
  signal: NodeJS.Signals = "SIGTERM",
): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.kill(signal);
  const force = setTimeout(() => child.kill("SIGKILL"), SERVER_CHILD_SHUTDOWN_GRACE_MS);
  try {
    await waitForExit(child);
  } finally {
    clearTimeout(force);
  }
}

const stopMarkerPath = (baseDir: string) =>
  NodePath.join(baseDir, "runtime", SERVICE_STOP_MARKER_FILE);
const restartPendingPath = (baseDir: string) =>
  NodePath.join(baseDir, "runtime", SERVICE_RESTART_PENDING_FILE);

export class Launcher {
  readonly #baseDir: string;
  readonly #statePath: string;
  readonly #startupGateProtocol: 1 | undefined;
  readonly #preparedTimeoutMs: number;
  readonly #backupAdapter: QualifiedBackupAdapter | undefined;
  readonly #updateOperationIO: {
    readonly archive: typeof archiveUpdateOperation;
    readonly reserve: typeof reserveUpdateOperation;
  };
  readonly #now: () => number;
  #state: ServiceState;
  #child: ManagedChild | null = null;
  #timer: NodeJS.Timeout | undefined;
  #transitions: Promise<void> = Promise.resolve();
  #stopRequested = false;
  #stopping = false;
  #done = false;
  // Bound to one update: a commit attempt never blocks a later update's rollback.
  #qualifiedCommitAttempt: QualifiedCommitAttempt | undefined;
  readonly #completion = Promise.withResolvers<void>();
  readonly #backupSpaceProbe: Partial<BackupSpaceProbe> | undefined;
  readonly #environment: Readonly<NodeJS.ProcessEnv>;

  constructor(
    baseDir: string,
    state: ServiceState,
    options: {
      readonly backupSpaceProbe?: Partial<BackupSpaceProbe>;
      readonly startupGateProtocol?: 1;
      readonly preparedTimeoutMs?: number;
      readonly backupAdapter?: QualifiedBackupAdapter;
      readonly updateOperationIO?: {
        readonly archive: typeof archiveUpdateOperation;
        readonly reserve: typeof reserveUpdateOperation;
      };
      /** Monotonic milliseconds for the qualified exchange fence; injectable in tests. */
      readonly now?: () => number;
      readonly environment?: Readonly<NodeJS.ProcessEnv>;
    } = {},
  ) {
    this.#baseDir = baseDir;
    this.#backupSpaceProbe = options.backupSpaceProbe;
    this.#environment = { ...(options.environment ?? process.env) };
    this.#statePath = NodePath.join(baseDir, "runtime", SERVICE_STATE_FILE);
    this.#state = state;
    this.#startupGateProtocol = options.startupGateProtocol;
    this.#preparedTimeoutMs = options.preparedTimeoutMs ?? PREPARED_TIMEOUT_MS;
    this.#backupAdapter = options.backupAdapter;
    this.#updateOperationIO = options.updateOperationIO ?? {
      archive: archiveUpdateOperation,
      reserve: reserveUpdateOperation,
    };
    this.#now = options.now ?? (() => performance.now());
  }

  #exchangeExpired(receivedAt: number): boolean {
    return this.#now() - receivedAt > QUALIFIED_EXCHANGE_FENCE_MS;
  }

  async run(): Promise<void> {
    const ownership = Promise.withResolvers<void>();
    // Queue recovery before stop() can enqueue shutdown, even while acquiring
    // the cross-process lock. Signal receipt still writes its marker promptly.
    this.#enqueue(async () => {
      await ownership.promise;
      await this.#recover();
    });
    try {
      await withQualifiedRuntimeLock(this.#baseDir, "service-launcher-lock", async () => {
        // Read the selected pointer after acquiring ownership, never trust the
        // snapshot supplied before a competing launcher/CLI transition.
        this.#state = await readServiceState(this.#statePath);
        ownership.resolve();
        return this.#runOwned();
      });
    } catch (cause) {
      ownership.reject(cause);
      await this.#transitions;
      await this.#completion.promise.catch(() => undefined);
      throw cause;
    }
  }

  async #runOwned(): Promise<void> {
    const onSigterm = () => void this.stop("SIGTERM");
    const onSigint = () => void this.stop("SIGINT");
    process.once("SIGTERM", onSigterm);
    process.once("SIGINT", onSigint);
    try {
      await this.#completion.promise;
    } finally {
      process.off("SIGTERM", onSigterm);
      process.off("SIGINT", onSigint);
    }
  }

  #enqueue(transition: () => Promise<void>): void {
    this.#transitions = this.#transitions
      .then(transition, transition)
      .catch((cause: unknown) =>
        this.#fatal(cause instanceof Error ? cause : new Error(String(cause))),
      );
  }

  async #fatal(error: Error): Promise<void> {
    if (this.#done) return;
    this.#done = true;
    this.#stopping = true;
    this.#clearTimer();
    const child = this.#child?.process;
    this.#child = null;
    try {
      if (child !== undefined) {
        await terminateChild(child);
        await this.#retractCapability(child);
      }
    } catch (cause) {
      this.#completion.reject(
        new Error("Launcher shutdown evidence could not be reconciled.", { cause }),
      );
      return;
    }
    this.#completion.reject(error);
  }

  async #retractCapability(child: NodeChildProcess.ChildProcess): Promise<void> {
    if (this.#startupGateProtocol === 1 && child.pid !== undefined)
      await retractLauncherCapabilityReceipt(this.#baseDir, process.pid, child.pid);
  }

  async stop(signal: NodeJS.Signals): Promise<void> {
    const alreadyRequested = this.#stopRequested;
    this.#stopRequested = true;
    // This must happen synchronously at signal receipt. A queued update
    // transition may already be terminating the active child, and that child
    // needs to see the marker in its shutdown finalizer. KillMode=mixed also
    // ensures systemd signals the launcher before the rest of the cgroup, and
    // launchd signals only the job's main process (this launcher), so the
    // marker lands before the child sees any signal on both platforms.
    try {
      NodeFS.writeFileSync(stopMarkerPath(this.#baseDir), "", { mode: 0o600 });
    } catch {
      // Err toward keeping the tunnel; the next link or unlink reconciles it.
    }
    if (alreadyRequested || this.#stopping) {
      await this.#completion.promise.catch(() => undefined);
      return;
    }
    this.#clearTimer();
    this.#enqueue(async () => {
      // Let an ordinary update transition already in progress start its replacement
      // before this queued stop tears it down. That replacement owns the
      // pre-activation tunnel cleanup path and observes the marker above.
      this.#stopping = true;
      const child = this.#child?.process;
      this.#child = null;
      if (child !== undefined) {
        await terminateChild(child, signal);
        await this.#retractCapability(child);
      }
      this.#done = true;
      this.#completion.resolve();
    });
    await this.#completion.promise.catch(() => undefined);
  }

  #clearTimer(): void {
    clearTimeout(this.#timer);
    this.#timer = undefined;
  }

  async #recover(): Promise<void> {
    // A fresh launcher means servers are running again: any stop marker from
    // a previous explicit stop is stale and must not make a future update
    // handoff release its tunnel. A restart deferred by `t3 update` is done
    // no matter who restarted the service, but only once this launcher is
    // the version the marker waits for: a launcher that came up between the
    // CLI writing the marker and writing the new state still runs the old
    // version, and the marker has to outlive it.
    await NodeFSP.rm(stopMarkerPath(this.#baseDir), { force: true }).catch(() => undefined);
    const restartPending = restartPendingPath(this.#baseDir);
    const awaitedVersion = await NodeFSP.readFile(restartPending, "utf8").catch(() => undefined);
    if (awaitedVersion?.trim() === this.#state.activeVersion) {
      await NodeFSP.rm(restartPending, { force: true }).catch(() => undefined);
    }
    const update = this.#state.update;
    if (update?.status !== "pending") {
      if (update !== undefined && update.qualified === undefined) {
        await discardDatabaseBackup(this.#baseDir, update.id).catch(() => undefined);
      }
      await this.#startChild(this.#state.activeVersion, "active", update);
      return;
    }
    if (update.qualified !== undefined) {
      // Pending restart recovery holds state ownership before changing the old pair.
      if (update.phase === "accepted") {
        await withStoppedServerOwnership(this.#baseDir, () =>
          this.#finishWithoutTrial(update, "launcher-restarted", false),
        );
        await this.#startChild(this.#state.activeVersion, "active", this.#state.update);
      } else await this.#returnToPrevious(update, "rolled-back", "launcher-restarted");
      return;
    }
    if (update.phase === "accepted") {
      if (!(await runtimeExists(this.#baseDir, update.targetVersion))) {
        await this.#finishWithoutTrial(update, "target-runtime-missing");
        return;
      }
      await this.#startTrial(update);
      return;
    }
    if (await databaseRestorePending(this.#baseDir, update)) {
      await this.#returnToPrevious(update, "failed", "rollback-interrupted");
      return;
    }
    if (!(await databaseBackupAvailable(this.#baseDir, update.id))) {
      fenceNativeStoreAuthorityForBaseDir(this.#baseDir);
      throw new Error("Cannot recover a trial-ready update without its database backup.");
    }
    if (!(await runtimeExists(this.#baseDir, update.targetVersion))) {
      await this.#returnToPrevious(update, "failed", "target-runtime-missing");
      return;
    }
    await this.#startTrial(update);
  }

  async #startTrial(pending: PendingServiceUpdate): Promise<void> {
    if (pending.qualified !== undefined) {
      const staged = await verifyStagedQualifiedRuntime(
        this.#baseDir,
        pending.fromVersion,
        pending.qualified.stagedHandle,
      );
      if (JSON.stringify(staged) !== JSON.stringify(pending.qualified))
        throw new Error("The qualified trial binding changed after handoff.");
    }
    if (pending.phase === "accepted") {
      try {
        const backup = () => backupDatabaseOnce(this.#baseDir, pending, this.#backupAdapter);
        if (pending.qualified !== undefined)
          await withStoppedServerOwnership(this.#baseDir, backup);
        else await backup();
      } catch (cause) {
        if (pending.qualified !== undefined) throw cause;
        await this.#finishWithoutTrial(pending, "db-backup-failed");
        return;
      }
    } else if (!(await databaseBackupAvailable(this.#baseDir, pending.id))) {
      fenceNativeStoreAuthorityForBaseDir(this.#baseDir);
      throw new Error("Cannot start a trial-ready update without its database backup.");
    }
    let trialReady = pending;
    if (pending.phase === "accepted") {
      trialReady = { ...pending, phase: "trial-ready" };
      const next: ServiceState = { ...this.#state, update: trialReady };
      await writeServiceState(this.#statePath, next);
      this.#state = next;
    }
    try {
      await this.#startChild(trialReady.targetVersion, "trial", trialReady);
    } catch (cause) {
      if (pending.qualified !== undefined) throw cause;
      await this.#returnToPrevious(trialReady, "failed", "candidate-start-failed");
    }
  }

  async #finishWithoutTrial(
    pending: PendingServiceUpdate,
    reason: string,
    startChild = true,
  ): Promise<void> {
    const outcome = terminalUpdate({ pending, status: "failed", reason });
    const next: ServiceState = {
      ...this.#state,
      activeVersion: pending.fromVersion,
      update: outcome,
    };
    await writeServiceState(this.#statePath, next);
    this.#state = next;
    await cleanupTerminalDatabaseBackup(this.#baseDir, pending.id, pending.qualified !== undefined);
    if (startChild) await this.#startChild(next.activeVersion, "active", outcome);
  }

  async #startChild(version: string, role: ChildRole, update?: ServiceUpdateRecord): Promise<void> {
    if (this.#stopping) return;
    if (!(await runtimeExists(this.#baseDir, version))) {
      throw new Error(`Selected t3@${version} runtime is missing or incomplete.`);
    }
    if (this.#stopping) return;
    if (update?.qualified !== undefined) {
      const receipt = await readQualifiedRuntimeReceipt(this.#baseDir, version);
      const expectedSource =
        version === update.qualified.receipt.version
          ? update.qualified.receipt.sourceSha
          : update.qualified.binding.activeSourceSha;
      if (receipt.sourceSha !== expectedSource)
        throw new Error(
          "Selected runtime source no longer matches its native installation transaction.",
        );
    }
    const paths = runtimePaths(this.#baseDir, version);
    const context: ServiceLauncherContext = {
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      childVersion: version,
      qualifiedUpdatesProtocol: QUALIFIED_UPDATES_PROTOCOL,
      updateOperationsProtocol: 1,
      updateRetirementProtocol: 1,
      ...(this.#startupGateProtocol === undefined
        ? {}
        : { startupGateProtocol: this.#startupGateProtocol }),
      ...(update === undefined ? {} : { update }),
    };
    const spawnArguments = runtimeSpawnArguments(paths);
    const child = NodeChildProcess.spawn(spawnArguments.command, spawnArguments.args, {
      env: { ...this.#environment, [SERVICE_LAUNCHER_CONTEXT_ENV]: JSON.stringify(context) },
      stdio: ["inherit", "inherit", "inherit", "ipc"],
    });
    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => reject(error);
      child.once("error", onError);
      child.once("spawn", () => {
        child.removeListener("error", onError);
        child.on("error", (error) => this.#enqueue(() => Promise.reject(error)));
        resolve();
      });
    });
    if (this.#stopping) {
      await terminateChild(child);
      return;
    }

    const managed: ManagedChild = {
      version,
      role,
      process: child,
    };
    this.#child = managed;
    child.on("message", (value) => {
      // Receipt time, before queueing, is the closest launcher-side bound on
      // when the child's exchange timer started.
      const receivedAt = this.#now();
      const message = decodeServiceLauncherChildMessage(value);
      if (message !== undefined)
        this.#enqueue(() => this.#handleMessage(managed, message, receivedAt));
      else if (
        update?.qualified !== undefined &&
        typeof value === "object" &&
        value !== null &&
        "type" in value &&
        value.type === "prepared"
      ) {
        this.#enqueue(() =>
          Promise.reject(
            new Error("Qualified child supplied malformed startup proof; transaction retained."),
          ),
        );
      }
    });
    child.once("exit", (code, signal) =>
      this.#enqueue(() => this.#handleExit(managed, code, signal)),
    );

    if (this.#startupGateProtocol === 1) {
      if (child.pid === undefined) throw new Error("Spawned child has no native process identity.");
      await publishLauncherCapabilityReceipt({
        schema: 1,
        baseDir: this.#baseDir,
        launcherVersion: packageJson.version,
        launcherPid: process.pid,
        launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
        qualifiedUpdatesProtocol: QUALIFIED_UPDATES_PROTOCOL,
        startupGateProtocol: 1,
        childPid: child.pid,
        childVersion: version,
      });
    }

    if (role === "active") {
      const notice = launcherDriftNotice(packageJson.version, version);
      if (notice !== undefined) process.stderr.write(notice);
    }

    if (role === "trial") {
      this.#timer = setTimeout(
        () => this.#enqueue(() => this.#handlePreparedTimeout(managed)),
        this.#preparedTimeoutMs,
      );
    }
  }

  async #handleMessage(
    child: ManagedChild,
    message: ServiceLauncherChildMessage,
    receivedAt: number,
  ): Promise<void> {
    if (this.#child !== child || this.#stopping) return;
    if (message.type === "request-retire-update") {
      let retired = false;
      let reason: string | undefined;
      try {
        if (
          child.role !== "active" ||
          child.version !== this.#state.activeVersion ||
          this.#startupGateProtocol !== 1 ||
          this.#stopRequested
        )
          throw new Error("Only the selected active qualified server can retire a stage.");
        await retireNativeStage(this.#baseDir, child.version, this.#state.update, message);
        retired = true;
      } catch (cause) {
        reason = cause instanceof Error ? cause.message : "Native stage retirement is uncertain.";
      }
      await sendMessage(child.process, {
        type: "update-retired",
        operationId: message.operationId,
        stagedHandle: message.stagedHandle,
        retired,
        ...(reason === undefined ? {} : { reason }),
      });
      return;
    }
    if (message.type === "request-update") {
      await this.#handleUpdateRequest(child, message, receivedAt);
      return;
    }
    await this.#handlePrepared(child, message, receivedAt);
  }

  async #handleUpdateRequest(
    child: ManagedChild,
    message: Extract<ServiceLauncherChildMessage, { readonly type: "request-update" }>,
    receivedAt: number,
  ): Promise<void> {
    const reject = (reason: string) =>
      sendMessage(child.process, {
        type: "update-rejected",
        reason,
        ...(message.operationId === undefined ? {} : { operationId: message.operationId }),
      });
    if (child.role !== "active") {
      await reject("Only the active server can request an update.");
      return;
    }
    if (child.version !== this.#state.activeVersion) {
      await reject("The requesting server is not the selected active version.");
      return;
    }
    if (message.stagedHandle !== undefined) {
      try {
        await assertStageNotRevoked(this.#baseDir, message.stagedHandle, message.operationId);
      } catch (cause) {
        await reject(cause instanceof Error ? cause.message : "Stage revocation is uncertain.");
        return;
      }
    }
    if (message.operationId !== undefined) {
      const existing = await reconcileUpdateOperation(
        this.#baseDir,
        message.operationId,
        this.#state.update,
      );
      if (existing.state !== "absent") {
        if (existing.state === "blocked") {
          await reject("The update operation is ambiguous and requires reconciliation.");
          return;
        }
        const reserved = await readOperationReservation(this.#baseDir, message.operationId).catch(
          () => undefined,
        );
        if (
          reserved === undefined ||
          reserved.binding.stagedHandle !== message.stagedHandle ||
          reserved.binding.dbPath !== message.dbPath ||
          reserved.binding.targetVersion !== message.targetVersion
        ) {
          await reject("The update operation is ambiguous or its immutable binding changed.");
          return;
        }
        await sendMessage(child.process, {
          type: "update-accepted",
          updateId: message.operationId,
        });
        return;
      }
    }
    if (this.#state.update?.status === "pending") {
      await reject("Another server update is already pending.");
      return;
    }
    if (!isExactServiceVersion(message.targetVersion)) {
      await reject("The requested target is not an exact version.");
      return;
    }
    if (
      (message.targetVersion.includes("-preview.") || child.version.includes("-preview.")) &&
      message.stagedHandle === undefined
    ) {
      await reject("Jones previews require a qualified staged handle and explicit Install.");
      return;
    }
    if (
      message.stagedHandle === undefined &&
      compareExactServiceVersions(message.targetVersion, child.version) <= 0
    ) {
      await reject("Remote updates must select a newer server version.");
      return;
    }
    if (!NodePath.isAbsolute(message.dbPath)) {
      await reject("The requested database path is not absolute.");
      return;
    }
    try {
      validateDatabasePathForBaseDir(this.#baseDir, message.dbPath);
    } catch {
      await reject("The requested database path is not the configured userdata/statev2.sqlite.");
      return;
    }
    if (!(await runtimeExists(this.#baseDir, message.targetVersion))) {
      await reject("The requested target runtime is missing or incomplete.");
      return;
    }
    if (message.stagedHandle !== undefined && this.#startupGateProtocol !== 1) {
      await reject("bootstrap-required: The qualified startup gate is not bound on this launcher.");
      return;
    }
    let qualified: StagedQualifiedRuntime | undefined;
    let migrationPlan: MigrationPlan | undefined;
    if (message.stagedHandle !== undefined) {
      try {
        qualified = await verifyStagedQualifiedRuntime(
          this.#baseDir,
          child.version,
          message.stagedHandle,
        );
        if (
          qualified.receipt.version !== message.targetVersion ||
          qualified.binding.dbPath !== message.dbPath
        )
          throw new Error("Install target differs from its staged handle.");
        if (child.process.pid === undefined)
          throw new Error("The requesting child has no native process identity.");
        migrationPlan = await preflightQualifiedCandidate(this.#baseDir, qualified);
        await preflightQualifiedBackup(this.#baseDir, message.dbPath, this.#backupAdapter);
        if (this.#child !== child || this.#stopRequested || this.#stopping)
          throw new Error("The requesting child stopped during candidate preflight.");
      } catch (cause) {
        await reject(
          cause instanceof QualifiedCandidatePreflightError ||
            (cause instanceof Error &&
              (cause.message.startsWith("startup-gate-unavailable:") ||
                cause.message.startsWith("recovery-capacity:")))
            ? cause.message
            : "Qualified staged runtime, candidate startup gate, active source or native environment binding is invalid.",
        );
        return;
      }
    }
    // A check that cannot run refuses the update too: the alternative is
    // stopping the server without knowing whether the backup fits.
    const backupBlocker = await databaseBackupBlocker(
      this.#baseDir,
      message.dbPath,
      this.#backupSpaceProbe,
    ).catch(
      (error: unknown) =>
        `Could not check that the database backup fits before updating: ${error instanceof Error ? error.message : String(error)}. The server was not stopped.`,
    );
    if (!child.process.connected) return;
    if (backupBlocker !== undefined) {
      await reject(backupBlocker);
      return;
    }
    if (message.stagedHandle !== undefined) {
      if (this.#child !== child || this.#stopRequested || this.#stopping) return;
      if (this.#exchangeExpired(receivedAt)) {
        // The requester may already have reported a timeout; never accept,
        // record or stop behind it. A disconnected requester needs no reply.
        await reject(
          "request-expired: Install preflight outlasted the server's launcher exchange; Install was not admitted.",
        ).catch(() => undefined);
        return;
      }
    }
    try {
      if (message.stagedHandle !== undefined)
        await assertStageNotRevoked(this.#baseDir, message.stagedHandle, message.operationId);
      await this.#updateOperationIO.archive(this.#baseDir, this.#state.update);
      await assertNoUnreconciledUpdateOperations(this.#baseDir, this.#state.update);
      if (message.operationId !== undefined) {
        if (qualified === undefined) {
          await reject("An operation ID requires a qualified staged runtime.");
          return;
        }
        await this.#updateOperationIO.reserve(
          this.#baseDir,
          message.operationId,
          operationBinding(qualified),
        );
      }
    } catch {
      // A failed fsync may leave a reservation. Preserve it for reconciliation
      // while the unchanged active child continues serving.
      await reject(
        "operation-reconciliation-required: Native operation receipts could not be durably prepared; the current server remains active.",
      );
      return;
    }
    if (qualified !== undefined && this.#exchangeExpired(receivedAt)) {
      // Receipt preparation can consume the exchange budget too. A reservation
      // already written stays available for reconciliation; no Install was accepted.
      await reject(
        "request-expired: Native operation preparation outlasted the server's launcher exchange; Install was not admitted.",
      ).catch(() => undefined);
      return;
    }
    const pending: PendingServiceUpdate = {
      id: message.operationId ?? NodeCrypto.randomUUID(),
      fromVersion: child.version,
      targetVersion: message.targetVersion,
      dbPath: message.dbPath,
      status: "pending",
      phase: "accepted",
      ...(qualified === undefined ? {} : { qualified }),
      ...(migrationPlan === undefined ? {} : { migrationPlan }),
    };
    const next: ServiceState = { ...this.#state, update: pending };
    await writeServiceState(this.#statePath, next);
    this.#state = next;
    await sendMessage(child.process, {
      type: "update-accepted",
      updateId: pending.id,
      ...(migrationPlan === undefined ? {} : { migrationPlan }),
    });
    this.#timer = setTimeout(() => this.#enqueue(() => this.#beginTrial(child)), HANDOFF_DELAY_MS);
  }

  async #beginTrial(child: ManagedChild): Promise<void> {
    const pending = this.#state.update;
    if (this.#child !== child || child.role !== "active" || pending?.status !== "pending") {
      return;
    }
    this.#timer = undefined;
    this.#child = null;
    await terminateChild(child.process);
    await this.#retractCapability(child.process);
    await this.#startTrial(pending);
  }

  async #handlePrepared(
    child: ManagedChild,
    message: Extract<ServiceLauncherChildMessage, { readonly type: "prepared" }>,
    receivedAt: number,
  ): Promise<void> {
    const updateId = message.updateId;
    const pending = this.#state.update;
    if (
      child.role !== "trial" ||
      pending?.status !== "pending" ||
      pending.id !== updateId ||
      pending.targetVersion !== child.version
    ) {
      if (child.role === "trial" && pending?.status === "pending") {
        await this.#returnToPrevious(pending, "rolled-back", "invalid-prepared", child);
        return;
      }
      throw new Error("Trial child reported prepared for an unexpected update.");
    }
    let startupReceipt: QualifiedTrialReceipt | undefined;
    if (pending.qualified !== undefined) {
      if (
        message.startupGateProtocol !== 1 ||
        message.qualified === undefined ||
        this.#startupGateProtocol !== 1
      )
        throw new Error(
          "Qualified startup proof is required before pointer commit; transaction retained.",
        );
      startupReceipt = message.qualified;
      assertQualifiedTrialBinding({
        updateId: pending.id,
        qualified: pending.qualified,
        receipt: startupReceipt,
      });
      const home = await NodeFSP.realpath(this.#baseDir);
      const serviceUserdata = await NodeFSP.realpath(NodePath.join(home, "userdata"));
      const database = await NodeFSP.realpath(pending.dbPath);
      const receipt = await readQualifiedRuntimeReceipt(this.#baseDir, pending.targetVersion);
      const environment = (
        await NodeFSP.readFile(NodePath.join(this.#baseDir, "userdata", "environment-id"), "utf8")
      ).trim();
      if (
        JSON.stringify(receipt) !== JSON.stringify(pending.qualified.receipt) ||
        environment !== pending.qualified.binding.environmentId ||
        startupReceipt.home !== home ||
        startupReceipt.serviceUserdata !== serviceUserdata ||
        startupReceipt.databasePath !== database ||
        startupReceipt.processId !== child.process.pid ||
        startupReceipt.version !== child.version ||
        startupReceipt.sourceSha !== receipt.sourceSha ||
        startupReceipt.sourceTree !== receipt.sourceTree
      ) {
        throw new Error(
          "Qualified startup observed identity mismatched the captured child; transaction retained.",
        );
      }
    }
    if (pending.qualified !== undefined) {
      if (child.process.pid === undefined) throw new Error("Trial writer identity is unavailable.");
      this.#assertQualifiedChildLive(child);
      if (this.#exchangeExpired(receivedAt)) {
        // The trial may have abandoned its prepared exchange and could never
        // reserve resume. No pointer write was attempted, so the ordinary
        // pre-commit rollback applies.
        this.#clearTimer();
        await this.#returnToPrevious(pending, "rolled-back", "prepared-exchange-expired", child);
        return;
      }
    }
    this.#clearTimer();
    const committed = {
      ...terminalUpdate({ pending, status: "committed" }),
      ...(startupReceipt === undefined ? {} : { startupReceipt }),
    };
    const next: ServiceState = {
      ...this.#state,
      activeVersion: pending.targetVersion,
      update: committed,
    };
    if (pending.qualified !== undefined) {
      this.#assertQualifiedChildLive(child);
      // From this point a failed fsync/response can leave a committed pointer; rollback is forbidden.
      this.#qualifiedCommitAttempt = { updateId: pending.id, step: "pointer-write" };
    }
    try {
      await writeServiceState(this.#statePath, next);
      this.#state = next;
      if (pending.qualified !== undefined) {
        this.#qualifiedCommitAttempt = { updateId: pending.id, step: "post-write-liveness" };
        this.#assertQualifiedChildLive(child);
      }
      child.role = "active";
      const notice = launcherDriftNotice(packageJson.version, child.version);
      if (notice !== undefined) process.stderr.write(notice);
      if (pending.qualified !== undefined)
        this.#qualifiedCommitAttempt = { updateId: pending.id, step: "committed-message" };
      await sendMessage(child.process, {
        type: "committed",
        updateId: committed.id,
        ...(startupReceipt === undefined
          ? {}
          : {
              startupGateProtocol: 1 as const,
              qualified: { ...startupReceipt, generation: committed.id },
            }),
      });
      await cleanupTerminalDatabaseBackup(
        this.#baseDir,
        committed.id,
        pending.qualified !== undefined,
      );
    } catch (cause) {
      const attempt = this.#qualifiedCommitAttempt;
      if (pending.qualified === undefined || attempt?.updateId !== pending.id) throw cause;
      throw new QualifiedCommitFailure(attempt, cause);
    }
  }

  #assertQualifiedChildLive(child: ManagedChild): void {
    if (
      this.#stopRequested ||
      this.#stopping ||
      this.#child !== child ||
      child.process.pid === undefined ||
      child.process.exitCode !== null ||
      child.process.signalCode !== null ||
      !child.process.connected
    )
      throw new Error("Qualified child liveness or stop intent changed; transaction retained.");
    // The PID belongs to this captured spawn, never a path/name search.
    process.kill(child.process.pid, 0);
  }

  async #handlePreparedTimeout(child: ManagedChild): Promise<void> {
    const pending = this.#state.update;
    if (this.#child !== child || child.role !== "trial" || pending?.status !== "pending") {
      return;
    }
    this.#timer = undefined;
    await this.#returnToPrevious(pending, "rolled-back", "prepared-timeout", child);
  }

  async #handleExit(
    child: ManagedChild,
    code: number | null,
    signal: NodeJS.Signals | null,
  ): Promise<void> {
    if (this.#child !== child || this.#stopping) return;
    this.#child = null;
    await this.#retractCapability(child.process);
    if (code === STATE_DIR_OWNED_EXIT_CODE) {
      await this.#idleWhileStateDirOwned();
      return;
    }
    if (child.role === "trial") {
      this.#clearTimer();
      const pending = this.#state.update;
      if (pending?.status !== "pending") {
        throw new Error("Trial child exited without matching pending state.");
      }
      await this.#returnToPrevious(
        pending,
        "rolled-back",
        `candidate-exited:${String(code ?? signal ?? "unknown")}`,
        child,
      );
      return;
    }

    this.#clearTimer();
    const pending = this.#state.update;
    if (pending?.status === "pending") {
      await this.#startTrial(pending);
      return;
    }
    throw new Error(`Active child exited unexpectedly (${String(code ?? signal ?? "unknown")}).`);
  }

  /**
   * Exiting would make systemd or launchd start another refused server, so stay
   * idle until stopped or restarted. A pending update fails without restoring
   * its backup: the other owner may have written to the database since.
   */
  async #idleWhileStateDirOwned(): Promise<void> {
    this.#clearTimer();
    const pending = this.#state.update;
    if (pending?.status === "pending") {
      const next: ServiceState = {
        ...this.#state,
        activeVersion: pending.fromVersion,
        update: terminalUpdate({ pending, status: "failed", reason: "state-dir-owned" }),
      };
      await writeServiceState(this.#statePath, next);
      this.#state = next;
      if (pending.qualified === undefined)
        await discardDatabaseBackup(this.#baseDir, pending.id).catch(() => undefined);
    }
    // Signal listeners alone do not keep Node alive.
    this.#timer = setInterval(() => {}, 2_147_483_647);
    process.stderr.write(
      "[service-launcher] Another T3 Code server owns this T3 home. Stop it, then restart this service.\n",
    );
  }

  async #returnToPrevious(
    pending: PendingServiceUpdate,
    status: "rolled-back" | "failed",
    reason: string,
    child?: ManagedChild,
  ): Promise<void> {
    const refusal =
      pending.qualified === undefined
        ? undefined
        : qualifiedRollbackRefusal(this.#qualifiedCommitAttempt, pending.id, reason);
    if (refusal !== undefined) throw new Error(refusal);
    if (child !== undefined) {
      this.#child = null;
      await terminateChild(child.process);
      await this.#retractCapability(child.process);
    }
    const restore = async () => {
      await restoreDatabaseBackup(this.#baseDir, pending);
      const outcome = terminalUpdate({ pending, status, reason });
      const next: ServiceState = {
        ...this.#state,
        activeVersion: pending.fromVersion,
        update: outcome,
      };
      await writeServiceState(this.#statePath, next);
      this.#state = next;
      await cleanupTerminalDatabaseBackup(
        this.#baseDir,
        pending.id,
        pending.qualified !== undefined,
      );
    };
    if (pending.qualified !== undefined) await withStoppedServerOwnership(this.#baseDir, restore);
    else await restore();
    await this.#startChild(this.#state.activeVersion, "active", this.#state.update);
  }
}

export async function main(): Promise<void> {
  const baseDir = process.env.T3CODE_HOME?.trim();
  if (baseDir === undefined || baseDir === "") {
    throw new Error("T3CODE_HOME is required by the T3 Code service launcher.");
  }
  const environment = await readServiceEnvironment(baseDir);
  const statePath = NodePath.join(baseDir, "runtime", SERVICE_STATE_FILE);
  const state = await readServiceState(statePath);
  await new Launcher(baseDir, state, { environment }).run();
}
