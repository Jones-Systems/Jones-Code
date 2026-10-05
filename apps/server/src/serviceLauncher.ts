// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalTimers:off
// The launcher supervises the server child for the boot service and must keep
// working across server versions, so it stays on Node built-ins with no Effect
// runtime: it is the one part of the executable that cannot depend on the
// rest of it being loadable.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import {
  verifyStagedQualifiedRuntime,
  readQualifiedRuntimeReceipt,
  withQualifiedRuntimeLock,
  QUALIFIED_UPDATES_PROTOCOL,
  type StagedQualifiedRuntime,
} from "./cloud/qualifiedRuntime.ts";

import {
  proveQualifiedStateQuiescence,
  QUALIFIED_STARTUP_STATE_FILES,
  type QualifiedQuiescenceAdapter,
} from "./cloud/qualifiedQuiescence.ts";

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
const PREPARED_TIMEOUT_MS = 120_000;
const TERMINATE_GRACE_MS = 5_000;

type TerminalStatus = "committed" | "rolled-back" | "failed";
type ChildRole = "active" | "trial";

interface ManagedChild {
  readonly version: string;
  role: ChildRole;
  readonly process: NodeChildProcess.ChildProcess;
}

// Mirrors pinnedRuntimePaths: a runtime is an unpacked release archive whose
// executable runs on its own. Kept inline so this file stays on Node
// built-ins only.
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

const runtimeSpawnArguments = (paths: ReturnType<typeof runtimePaths>) => ({
  command: paths.entryPath,
  args: ["serve"],
});

/** SQLite persists across the main file plus its WAL and shared-memory sidecars. */
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

/**
 * Snapshots the database once per update before the first trial. A completed
 * backup is never overwritten because a restarted launcher may be looking at
 * database writes from an earlier attempt by the same trial.
 */
async function backupDatabaseOnce(baseDir: string, pending: PendingServiceUpdate): Promise<void> {
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
    if (pending.qualified !== undefined) {
      const database = new NodeSqlite.DatabaseSync(pending.dbPath, { readOnly: true });
      try {
        await NodeSqlite.backup(database, databaseBackupFile(stagingDir, ""));
      } finally {
        database.close();
      }
      await syncFile(databaseBackupFile(stagingDir, ""));
      await backupPairedSettings(baseDir, stagingDir);
      const bindingPath = NodePath.join(stagingDir, "paired-binding.json");
      await NodeFSP.writeFile(
        bindingPath,
        JSON.stringify({ transactionId: pending.id, qualified: pending.qualified }),
        { flag: "wx", mode: 0o600 },
      );
      await syncFile(bindingPath);
    }
    for (const suffix of pending.qualified === undefined ? DB_FILE_SUFFIXES : []) {
      const source = `${pending.dbPath}${suffix}`;
      if (suffix !== "" && !(await pathExists(source))) continue;
      const destination = databaseBackupFile(stagingDir, suffix);
      await NodeFSP.copyFile(source, destination);
      await syncFile(destination);
    }
    await NodeFSP.rename(stagingDir, backupDir);
    await syncDirectory(NodePath.dirname(backupDir));
  } catch (cause) {
    await NodeFSP.rm(stagingDir, { recursive: true, force: true }).catch(() => undefined);
    throw cause;
  }
}

async function backupPairedSettings(baseDir: string, destination: string): Promise<void> {
  const present: string[] = [];
  for (const name of PAIRED_SETTINGS) {
    const source = NodePath.join(baseDir, "userdata", name);
    if (!(await pathExists(source))) continue;
    const stat = await NodeFSP.lstat(source);
    if (!stat.isFile() || stat.isSymbolicLink())
      throw new Error("Startup settings must be regular files.");
    const target = NodePath.join(destination, name);
    await NodeFSP.copyFile(source, target);
    await syncFile(target);
    present.push(name);
  }
  const manifest = NodePath.join(destination, "paired-settings.json");
  await NodeFSP.writeFile(manifest, JSON.stringify(present), { flag: "wx", mode: 0o600 });
  await syncFile(manifest);
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
      await NodeFSP.copyFile(NodePath.join(backupDir, name), target);
      await syncFile(target);
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

async function retainAdvancedState(baseDir: string, pending: PendingServiceUpdate): Promise<void> {
  const directory = NodePath.join(databaseBackupDir(baseDir, pending.id), "advanced-state");
  if (await pathExists(directory)) {
    if (
      !(await pathExists(NodePath.join(directory, "database"))) ||
      !(await pathExists(NodePath.join(directory, "paired-settings.json")))
    )
      throw new Error(
        "Advanced state snapshot is incomplete; it was preserved for reconciliation.",
      );
    return;
  }
  await NodeFSP.mkdir(directory, { mode: 0o700 });
  const database = new NodeSqlite.DatabaseSync(pending.dbPath, { readOnly: true });
  try {
    await NodeSqlite.backup(database, NodePath.join(directory, "database"));
  } finally {
    database.close();
  }
  await syncFile(NodePath.join(directory, "database"));
  await backupPairedSettings(baseDir, directory);
  await syncDirectory(directory);
}

const databaseBackupAvailable = async (baseDir: string, updateId: string): Promise<boolean> => {
  try {
    const stat = await NodeFSP.lstat(databaseBackupFile(databaseBackupDir(baseDir, updateId), ""));
    return stat.isFile();
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

/** Resume durable restore intent; missing backups or unproved writer ownership stop recovery. */
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
  // Persist restore intent before fencing so recovery cannot restart or commit
  // a trial while the native authority remains fenced.
  if (pending.qualified !== undefined) {
    await validatePairedBackup(baseDir, pending, backupDir);
    if (!(await databaseRestorePending(baseDir, pending)))
      await retainAdvancedState(baseDir, pending);
  }
  await markDatabaseRestorePending(backupDir);
  fenceNativeStoreAuthorityForBaseDir(baseDir);
  for (const suffix of DB_FILE_SUFFIXES) {
    const target = `${pending.dbPath}${suffix}`;
    const source = databaseBackupFile(backupDir, suffix);
    if (await pathExists(source)) {
      await NodeFSP.copyFile(source, target);
      await syncFile(target);
    } else {
      await NodeFSP.rm(target, { force: true });
    }
  }
  await syncDirectory(NodePath.dirname(pending.dbPath));
  if (pending.qualified !== undefined) await restorePairedSettings(baseDir, backupDir);
  advanceNativeStoreAuthorityForBaseDir(baseDir, pending.dbPath);
}

async function discardDatabaseBackup(baseDir: string, updateId: string): Promise<void> {
  const backupDir = databaseBackupDir(baseDir, updateId);
  if (!(await pathExists(backupDir))) return;
  await NodeFSP.rm(backupDir, { recursive: true, force: true });
  await syncDirectory(NodePath.dirname(backupDir));
}

export async function readServiceState(filePath: string): Promise<ServiceState> {
  const contents = await NodeFSP.readFile(filePath, "utf8");
  const state = parseServiceState(contents);
  if (state === undefined) throw new Error("Service state is invalid or unsupported.");
  return state;
}

/** Durable same-directory replacement used for every runtime state transition. */
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
  const force = setTimeout(() => child.kill("SIGKILL"), TERMINATE_GRACE_MS);
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
  readonly #quiescenceAdapter: QualifiedQuiescenceAdapter | undefined;
  #state: ServiceState;
  #child: ManagedChild | null = null;
  #timer: NodeJS.Timeout | undefined;
  #transitions: Promise<void> = Promise.resolve();
  #stopRequested = false;
  #stopping = false;
  #done = false;
  readonly #completion = Promise.withResolvers<void>();

  constructor(
    baseDir: string,
    state: ServiceState,
    options: { readonly quiescenceAdapter?: QualifiedQuiescenceAdapter } = {},
  ) {
    this.#baseDir = baseDir;
    this.#statePath = NodePath.join(baseDir, "runtime", SERVICE_STATE_FILE);
    this.#state = state;
    this.#quiescenceAdapter = options.quiescenceAdapter;
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
    if (child !== undefined) await terminateChild(child);
    this.#completion.reject(error);
  }

  async stop(signal: NodeJS.Signals): Promise<void> {
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
    if (this.#stopRequested || this.#stopping) {
      await this.#completion.promise.catch(() => undefined);
      return;
    }
    this.#stopRequested = true;
    this.#clearTimer();
    this.#enqueue(async () => {
      // Let an update transition already in progress start its replacement
      // before this queued stop tears it down. That replacement owns the
      // pre-activation tunnel cleanup path and observes the marker above.
      this.#stopping = true;
      const child = this.#child?.process;
      this.#child = null;
      if (child !== undefined) await terminateChild(child, signal);
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
      throw new Error(
        "Qualified update recovery requires reconciliation of prior writer ownership; state and paired backups were retained.",
      );
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

  async #proveQuiescence(allowedProcessIds: readonly number[] = []): Promise<void> {
    await proveQualifiedStateQuiescence({
      baseDir: this.#baseDir,
      allowedProcessIds,
      ...(this.#quiescenceAdapter === undefined ? {} : { adapter: this.#quiescenceAdapter }),
    });
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
    // Owned child exit alone does not prove another same-home writer stopped.
    // Keep this proof outside backup failure recovery: uncertainty must retain
    // pending state rather than restart a potentially competing old writer.
    if (pending.qualified !== undefined) await this.#proveQuiescence();
    if (pending.phase === "accepted") {
      try {
        await backupDatabaseOnce(this.#baseDir, pending);
      } catch {
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
    } catch {
      await this.#returnToPrevious(trialReady, "failed", "candidate-start-failed");
    }
  }

  async #finishWithoutTrial(pending: PendingServiceUpdate, reason: string): Promise<void> {
    const outcome = terminalUpdate({ pending, status: "failed", reason });
    const next: ServiceState = {
      ...this.#state,
      activeVersion: pending.fromVersion,
      update: outcome,
    };
    await writeServiceState(this.#statePath, next);
    this.#state = next;
    if (pending.qualified === undefined)
      await discardDatabaseBackup(this.#baseDir, pending.id).catch(() => undefined);
    await this.#startChild(next.activeVersion, "active", outcome);
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
      ...(update === undefined ? {} : { update }),
    };
    const spawnArguments = runtimeSpawnArguments(paths);
    const child = NodeChildProcess.spawn(spawnArguments.command, spawnArguments.args, {
      env: {
        ...process.env,
        T3CODE_HOME: this.#baseDir,
        [SERVICE_LAUNCHER_CONTEXT_ENV]: JSON.stringify(context),
      },
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
      const message = decodeServiceLauncherChildMessage(value);
      if (message !== undefined) this.#enqueue(() => this.#handleMessage(managed, message));
    });
    child.once("exit", (code, signal) =>
      this.#enqueue(() => this.#handleExit(managed, code, signal)),
    );

    if (role === "trial") {
      this.#timer = setTimeout(
        () => this.#enqueue(() => this.#handlePreparedTimeout(managed)),
        PREPARED_TIMEOUT_MS,
      );
    }
  }

  async #handleMessage(child: ManagedChild, message: ServiceLauncherChildMessage): Promise<void> {
    if (this.#child !== child || this.#stopping) return;
    if (message.type === "request-update") {
      await this.#handleUpdateRequest(child, message);
      return;
    }
    await this.#handlePrepared(child, message.updateId);
  }

  async #handleUpdateRequest(
    child: ManagedChild,
    message: Extract<ServiceLauncherChildMessage, { readonly type: "request-update" }>,
  ): Promise<void> {
    const reject = (reason: string) =>
      sendMessage(child.process, { type: "update-rejected", reason });
    if (child.role !== "active") {
      await reject("Only the active server can request an update.");
      return;
    }
    if (child.version !== this.#state.activeVersion) {
      await reject("The requesting server is not the selected active version.");
      return;
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

    let qualified: StagedQualifiedRuntime | undefined;
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
      } catch {
        await reject(
          "Qualified staged runtime, active source or native environment binding is invalid.",
        );
        return;
      }
    }
    const pending: PendingServiceUpdate = {
      id: NodeCrypto.randomUUID(),
      fromVersion: child.version,
      targetVersion: message.targetVersion,
      dbPath: message.dbPath,
      status: "pending",
      phase: "accepted",
      ...(qualified === undefined ? {} : { qualified }),
    };
    const next: ServiceState = { ...this.#state, update: pending };
    await writeServiceState(this.#statePath, next);
    this.#state = next;
    await sendMessage(child.process, { type: "update-accepted", updateId: pending.id });
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
    await this.#startTrial(pending);
  }

  async #handlePrepared(child: ManagedChild, updateId: string): Promise<void> {
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
    if (pending.qualified !== undefined) {
      const receipt = await readQualifiedRuntimeReceipt(this.#baseDir, pending.targetVersion);
      const environment = (
        await NodeFSP.readFile(NodePath.join(this.#baseDir, "userdata", "environment-id"), "utf8")
      ).trim();
      if (
        JSON.stringify(receipt) !== JSON.stringify(pending.qualified.receipt) ||
        environment !== pending.qualified.binding.environmentId
      ) {
        await this.#returnToPrevious(pending, "rolled-back", "candidate-identity-mismatch", child);
        return;
      }
    }
    if (pending.qualified !== undefined) {
      if (child.process.pid === undefined) throw new Error("Trial writer identity is unavailable.");
      // Only the captured trial may own writable state at the pointer commit.
      await this.#proveQuiescence([child.process.pid]);
    }
    this.#clearTimer();
    const committed = terminalUpdate({ pending, status: "committed" });
    const next: ServiceState = {
      ...this.#state,
      activeVersion: pending.targetVersion,
      update: committed,
    };
    await writeServiceState(this.#statePath, next);
    this.#state = next;
    child.role = "active";
    if (pending.qualified === undefined)
      await discardDatabaseBackup(this.#baseDir, committed.id).catch(() => undefined);
    await sendMessage(child.process, { type: "committed", updateId: committed.id });
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

  async #returnToPrevious(
    pending: PendingServiceUpdate,
    status: "rolled-back" | "failed",
    reason: string,
    child?: ManagedChild,
  ): Promise<void> {
    if (child !== undefined) {
      this.#child = null;
      await terminateChild(child.process);
    }
    if (pending.qualified !== undefined) await this.#proveQuiescence();
    await restoreDatabaseBackup(this.#baseDir, pending);
    const outcome = terminalUpdate({ pending, status, reason });
    const next: ServiceState = {
      ...this.#state,
      activeVersion: pending.fromVersion,
      update: outcome,
    };
    await writeServiceState(this.#statePath, next);
    this.#state = next;
    if (pending.qualified === undefined)
      await discardDatabaseBackup(this.#baseDir, pending.id).catch(() => undefined);
    await this.#startChild(next.activeVersion, "active", outcome);
  }
}

export async function main(): Promise<void> {
  const baseDir = process.env.T3CODE_HOME?.trim();
  if (baseDir === undefined || baseDir === "") {
    throw new Error("T3CODE_HOME is required by the T3 Code service launcher.");
  }
  const statePath = NodePath.join(baseDir, "runtime", SERVICE_STATE_FILE);
  const state = await readServiceState(statePath);
  await new Launcher(baseDir, state).run();
}
