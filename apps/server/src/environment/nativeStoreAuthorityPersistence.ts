// @effect-diagnostics nodeBuiltinImport:off
// This module is also imported by the standalone service launcher. Keep its
// runtime dependencies limited to Node built-ins.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { validateNativeStoreAuthorityPath } from "./nativeStoreAuthorityPath.ts";
import { parseServiceState, SERVICE_RESTART_PENDING_FILE } from "../cloud/serviceProtocol.ts";

const NATIVE_STORE_AUTHORITY_RECORD_VERSION = "t3-native-store-authority/1.0.0" as const;
const NATIVE_STORE_AUTHORITY_FILE = "native-store-authority-v1.json" as const;
const NATIVE_STORE_AUTHORITY_LOCK_FILE = "native-store-authority-v1.lock" as const;
const NATIVE_STORE_AUTHORITY_NAMESPACE_PREFIX = "t3-native:" as const;

export type NativeStoreAuthorityState = {
  readonly record_version: typeof NATIVE_STORE_AUTHORITY_RECORD_VERSION;
  readonly environment_id: string;
  readonly authority_namespace: string;
  readonly store_generation: number;
  readonly state: "active" | "fenced";
  readonly transition_id: string | null;
};

export type NativeStoreAuthorityErrorCode =
  | "missing"
  | "corrupt"
  | "fenced"
  | "launcher_upgrade_required"
  | "environment_mismatch"
  | "generation_regression"
  | "source_unavailable";

export class NativeStoreAuthorityPersistenceError extends Error {
  readonly code: NativeStoreAuthorityErrorCode;
  override readonly cause: unknown;

  constructor(code: NativeStoreAuthorityErrorCode, message: string, cause?: unknown) {
    super(message, { cause });
    this.name = "NativeStoreAuthorityPersistenceError";
    this.code = code;
    this.cause = cause;
  }
}

const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const AUTHORITY_NAMESPACE =
  /^t3-native:[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const NOFOLLOW = NodeFS.constants.O_NOFOLLOW ?? 0;
const DIRECTORY = NodeFS.constants.O_DIRECTORY ?? 0;
const MAX_SAFE_GENERATION = Number.MAX_SAFE_INTEGER;
const REQUIRED_STATE_KEYS = [
  "record_version",
  "environment_id",
  "authority_namespace",
  "store_generation",
  "state",
  "transition_id",
] as const;
const WRITER_LOCKS = new Set<string>();

export const nativeStoreAuthorityPaths = (authorityStateDir: string) => ({
  authorityStateDir,
  statePath: NodePath.join(authorityStateDir, NATIVE_STORE_AUTHORITY_FILE),
  lockPath: NodePath.join(authorityStateDir, NATIVE_STORE_AUTHORITY_LOCK_FILE),
  lockDatabasePath: NodePath.join(authorityStateDir, `${NATIVE_STORE_AUTHORITY_LOCK_FILE}.sqlite`),
});

/** The launcher has only the T3 home, so it uses the same explicit override as the server. */
const nativeStoreAuthorityStateDirForBaseDir = (baseDir: string): string => {
  const configured = process.env.T3CODE_NATIVE_AUTHORITY_STATE_DIR?.trim();
  const expanded =
    configured === undefined || configured === ""
      ? NodePath.join(baseDir, "native-store-authority")
      : configured === "~"
        ? NodeOS.homedir()
        : configured.startsWith("~/") || configured.startsWith("~\\")
          ? NodePath.join(NodeOS.homedir(), configured.slice(2))
          : configured;
  const directory = NodePath.resolve(expanded);
  validateNativeStoreAuthorityPath(baseDir, directory);
  return directory;
};

const readEnvironmentIdForBaseDir = (baseDir: string): string => {
  const environmentIdPath = NodePath.join(baseDir, "userdata", "environment-id");
  let fd: number | undefined;
  try {
    fd = NodeFS.openSync(environmentIdPath, NodeFS.constants.O_RDONLY | NOFOLLOW);
    const environmentId = NodeFS.readFileSync(fd, "utf8").trim();
    validateEnvironmentId(environmentId);
    return environmentId;
  } catch (cause) {
    if (cause instanceof NativeStoreAuthorityPersistenceError) throw cause;
    throw persistenceError(
      "source_unavailable",
      "Native environment identity is unavailable.",
      cause,
    );
  } finally {
    if (fd !== undefined) NodeFS.closeSync(fd);
  }
};

const persistenceError = (code: NativeStoreAuthorityErrorCode, message: string, cause?: unknown) =>
  new NativeStoreAuthorityPersistenceError(code, message, cause);

const isErrno = (cause: unknown, code: string): boolean =>
  cause instanceof Error && "code" in cause && cause.code === code;

const mode = (value: NodeFS.Stats): number => value.mode & 0o777;

const expectedUid = (): number | undefined =>
  typeof process.getuid === "function" ? process.getuid() : undefined;

const verifyOwner = (stat: NodeFS.Stats, path: string): void => {
  const uid = expectedUid();
  if (uid !== undefined && stat.uid !== uid) {
    throw persistenceError(
      "source_unavailable",
      `Native authority path is not owner-controlled: ${path}`,
    );
  }
};

const verifyAuthorityDirectory = (authorityStateDir: string): void => {
  if (!NodePath.isAbsolute(authorityStateDir)) {
    throw persistenceError("source_unavailable", "Native authority directory must be absolute.");
  }
  let stat: NodeFS.Stats;
  try {
    stat = NodeFS.lstatSync(authorityStateDir);
  } catch (cause) {
    if (!isErrno(cause, "ENOENT")) {
      throw persistenceError(
        "source_unavailable",
        "Native authority directory is unavailable.",
        cause,
      );
    }
    try {
      NodeFS.mkdirSync(authorityStateDir, { recursive: true, mode: 0o700 });
      stat = NodeFS.lstatSync(authorityStateDir);
    } catch (mkdirCause) {
      throw persistenceError(
        "source_unavailable",
        "Native authority directory could not be created.",
        mkdirCause,
      );
    }
  }
  if (stat.isSymbolicLink() || !stat.isDirectory() || mode(stat) !== 0o700) {
    throw persistenceError(
      "source_unavailable",
      "Native authority directory is not a private directory.",
    );
  }
  verifyOwner(stat, authorityStateDir);
};

const verifyRegularPrivateFile = (
  path: string,
  code: NativeStoreAuthorityErrorCode,
): NodeFS.Stats => {
  let stat: NodeFS.Stats;
  try {
    stat = NodeFS.lstatSync(path);
  } catch (cause) {
    if (isErrno(cause, "ENOENT"))
      throw persistenceError(code, `Native authority file is missing: ${path}`);
    throw persistenceError(
      "source_unavailable",
      `Native authority file is unavailable: ${path}`,
      cause,
    );
  }
  if (stat.isSymbolicLink() || !stat.isFile() || mode(stat) !== 0o600) {
    throw persistenceError(
      "corrupt",
      `Native authority file is not a private regular file: ${path}`,
    );
  }
  verifyOwner(stat, path);
  return stat;
};

const validateEnvironmentId = (environmentId: string): void => {
  if (
    typeof environmentId !== "string" ||
    environmentId.length < 1 ||
    environmentId.length > 512 ||
    !/^\S(?:[\s\S]*\S)?$/.test(environmentId)
  ) {
    throw persistenceError("environment_mismatch", "Native environment identity is invalid.");
  }
};

export const decodeNativeStoreAuthorityState = (value: unknown): NativeStoreAuthorityState => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw persistenceError("corrupt", "Native authority state is not an object.");
  }
  const record = value as Record<string, unknown>;
  if (
    Object.keys(record).length !== REQUIRED_STATE_KEYS.length ||
    REQUIRED_STATE_KEYS.some((key) => !Object.hasOwn(record, key)) ||
    record.record_version !== NATIVE_STORE_AUTHORITY_RECORD_VERSION ||
    typeof record.environment_id !== "string" ||
    typeof record.authority_namespace !== "string" ||
    typeof record.store_generation !== "number" ||
    !Number.isSafeInteger(record.store_generation) ||
    record.store_generation < 1 ||
    record.store_generation > MAX_SAFE_GENERATION ||
    (record.state !== "active" && record.state !== "fenced") ||
    (record.transition_id !== null && typeof record.transition_id !== "string") ||
    (record.transition_id !== null && !UUID_V4.test(record.transition_id)) ||
    !AUTHORITY_NAMESPACE.test(record.authority_namespace)
  ) {
    throw persistenceError("corrupt", "Native authority state failed its schema checks.");
  }
  validateEnvironmentId(record.environment_id);
  if (record.state === "active" && record.transition_id !== null) {
    throw persistenceError("corrupt", "Active native authority state has a transition ID.");
  }
  if (record.state === "fenced" && record.transition_id === null) {
    throw persistenceError("corrupt", "Fenced native authority state has no transition ID.");
  }
  return {
    record_version: NATIVE_STORE_AUTHORITY_RECORD_VERSION,
    environment_id: record.environment_id,
    authority_namespace: record.authority_namespace,
    store_generation: record.store_generation,
    state: record.state,
    transition_id: record.transition_id,
  };
};

const readStateUnlocked = (authorityStateDir: string): NativeStoreAuthorityState => {
  const { statePath } = nativeStoreAuthorityPaths(authorityStateDir);
  verifyAuthorityDirectory(authorityStateDir);
  verifyRegularPrivateFile(statePath, "missing");
  let fd: number | undefined;
  try {
    fd = NodeFS.openSync(statePath, NodeFS.constants.O_RDONLY | NOFOLLOW);
    const raw = NodeFS.readFileSync(fd, "utf8");
    return decodeNativeStoreAuthorityState(JSON.parse(raw) as unknown);
  } catch (cause) {
    if (cause instanceof NativeStoreAuthorityPersistenceError) throw cause;
    throw persistenceError("corrupt", "Native authority state could not be read.", cause);
  } finally {
    if (fd !== undefined) NodeFS.closeSync(fd);
  }
};

const hasNativeStoreAuthorityState = (authorityStateDir: string): boolean => {
  try {
    NodeFS.lstatSync(nativeStoreAuthorityPaths(authorityStateDir).statePath);
    return true;
  } catch (cause) {
    if (isErrno(cause, "ENOENT")) return false;
    // Any present-but-unreadable state must be treated as present so launcher
    // fencing fails closed instead of silently taking the legacy no-authority
    // path. The authority reader will report the typed reason.
    return true;
  }
};

/**
 * Fence only an already initialized authority. A launcher operating on an
 * older T3 version has no authority to create one, and no trust was enabled
 * when the state file is absent, so that case is a no-op for compatibility.
 */
export const fenceNativeStoreAuthorityForBaseDir = (
  baseDir: string,
): NativeStoreAuthorityState | null => {
  const authorityStateDir = nativeStoreAuthorityStateDirForBaseDir(baseDir);
  if (!hasNativeStoreAuthorityState(authorityStateDir)) return null;
  return fenceNativeStoreAuthority(authorityStateDir, readEnvironmentIdForBaseDir(baseDir));
};

/** Explicitly enroll the current T3-owned environment and authority directory. */
export const initializeNativeStoreAuthorityForBaseDir = (
  baseDir: string,
  requiredLauncherProtocol: number,
): NativeStoreAuthorityState => {
  requireNativeStoreAuthorityLauncherProtocolForBaseDir(baseDir, requiredLauncherProtocol);
  const state = initializeNativeStoreAuthority(
    nativeStoreAuthorityStateDirForBaseDir(baseDir),
    readEnvironmentIdForBaseDir(baseDir),
  );
  if (state.state !== "active") {
    throw persistenceError(
      "fenced",
      "Native authority remains fenced and must be recovered by the service launcher.",
    );
  }
  return state;
};

export const requireNativeStoreAuthorityLauncherProtocolForBaseDir = (
  baseDir: string,
  requiredLauncherProtocol: number,
  runningVersion?: string,
): void => {
  const statePath = NodePath.join(baseDir, "runtime", "service-state.json");
  let fd: number | undefined;
  try {
    if (NodeFS.existsSync(NodePath.join(baseDir, "runtime", SERVICE_RESTART_PENDING_FILE))) {
      throw persistenceError(
        "launcher_upgrade_required",
        "The service launcher must be restarted before native authority can be trusted.",
      );
    }
    fd = NodeFS.openSync(statePath, NodeFS.constants.O_RDONLY | NOFOLLOW);
    const stat = NodeFS.fstatSync(fd);
    verifyOwner(stat, statePath);
    if (!stat.isFile() || (mode(stat) & 0o022) !== 0) {
      throw persistenceError(
        "launcher_upgrade_required",
        "The installed service launcher state is not owner-controlled.",
      );
    }
    const value = parseServiceState(NodeFS.readFileSync(fd, "utf8"));
    const expectedVersion =
      value?.update?.status === "pending" ? value.update.targetVersion : value?.activeVersion;
    if (
      value?.protocol !== requiredLauncherProtocol ||
      (runningVersion !== undefined && runningVersion !== expectedVersion)
    ) {
      throw persistenceError(
        "launcher_upgrade_required",
        "The installed service launcher must be upgraded before native authority can be trusted.",
      );
    }
  } catch (cause) {
    if (cause instanceof NativeStoreAuthorityPersistenceError) throw cause;
    throw persistenceError(
      "launcher_upgrade_required",
      "The installed service launcher must be upgraded before native authority can be trusted.",
      cause,
    );
  } finally {
    if (fd !== undefined) NodeFS.closeSync(fd);
  }
};

/** Complete a fenced launcher restore and advance the trusted generation. */
export const advanceNativeStoreAuthorityForBaseDir = (
  baseDir: string,
  databasePath: string,
): NativeStoreAuthorityState | null => {
  const authorityStateDir = nativeStoreAuthorityStateDirForBaseDir(baseDir);
  if (!hasNativeStoreAuthorityState(authorityStateDir)) return null;
  return advanceNativeStoreAuthority(
    authorityStateDir,
    readEnvironmentIdForBaseDir(baseDir),
    databasePath,
  );
};

const syncDirectory = (directory: string): void => {
  let fd: number | undefined;
  try {
    fd = NodeFS.openSync(directory, NodeFS.constants.O_RDONLY | DIRECTORY | NOFOLLOW);
    NodeFS.fsyncSync(fd);
  } catch (cause) {
    throw persistenceError(
      "source_unavailable",
      "Native authority directory could not be synced.",
      cause,
    );
  } finally {
    if (fd !== undefined) NodeFS.closeSync(fd);
  }
};

const writeStateUnlocked = (authorityStateDir: string, state: NativeStoreAuthorityState): void => {
  const { statePath } = nativeStoreAuthorityPaths(authorityStateDir);
  verifyAuthorityDirectory(authorityStateDir);
  try {
    verifyRegularPrivateFile(statePath, "missing");
  } catch (cause) {
    if (!(cause instanceof NativeStoreAuthorityPersistenceError) || cause.code !== "missing")
      throw cause;
  }
  const tempPath = NodePath.join(
    authorityStateDir,
    `.${NATIVE_STORE_AUTHORITY_FILE}.${process.pid}.${NodeCrypto.randomUUID()}.tmp`,
  );
  let fd: number | undefined;
  try {
    fd = NodeFS.openSync(
      tempPath,
      NodeFS.constants.O_WRONLY | NodeFS.constants.O_CREAT | NodeFS.constants.O_EXCL | NOFOLLOW,
      0o600,
    );
    NodeFS.writeFileSync(fd, `${JSON.stringify(state)}\n`, "utf8");
    NodeFS.fsyncSync(fd);
    NodeFS.closeSync(fd);
    fd = undefined;
    NodeFS.renameSync(tempPath, statePath);
    syncDirectory(authorityStateDir);
  } catch (cause) {
    throw cause instanceof NativeStoreAuthorityPersistenceError
      ? cause
      : persistenceError(
          "source_unavailable",
          "Native authority state could not be committed.",
          cause,
        );
  } finally {
    if (fd !== undefined) NodeFS.closeSync(fd);
    try {
      NodeFS.rmSync(tempPath, { force: true });
    } catch {
      // A successful rename leaves no temporary file. Preserve the durable
      // result if cleanup itself has an unknown effect.
    }
  }
};

const verifyPrivateFileIfPresent = (path: string): void => {
  try {
    verifyRegularPrivateFile(path, "missing");
  } catch (cause) {
    if (!(cause instanceof NativeStoreAuthorityPersistenceError) || cause.code !== "missing") {
      throw cause;
    }
  }
};

const withWriterLock = <A>(authorityStateDir: string, operation: () => A): A => {
  const paths = nativeStoreAuthorityPaths(authorityStateDir);
  verifyAuthorityDirectory(authorityStateDir);
  if (WRITER_LOCKS.has(authorityStateDir)) {
    throw persistenceError("source_unavailable", "Native authority writer is already active.");
  }
  let lockFd: number | undefined;
  let database: NodeSqlite.DatabaseSync | undefined;
  let transactionOpen = false;
  WRITER_LOCKS.add(authorityStateDir);
  try {
    // Legacy sentinels have no owner identity. Neither age nor a new lock format
    // proves their writer is gone, so they require explicit offline recovery.
    try {
      NodeFS.lstatSync(paths.lockPath);
      throw persistenceError(
        "source_unavailable",
        "Legacy native authority writer ownership is unknown.",
      );
    } catch (cause) {
      if (!isErrno(cause, "ENOENT")) throw cause;
    }
    // Do not open and close an extra descriptor for an existing SQLite file:
    // POSIX close can release another connection's locks in this process.
    try {
      lockFd = NodeFS.openSync(
        paths.lockDatabasePath,
        NodeFS.constants.O_WRONLY | NodeFS.constants.O_CREAT | NodeFS.constants.O_EXCL | NOFOLLOW,
        0o600,
      );
      NodeFS.closeSync(lockFd);
      lockFd = undefined;
    } catch (cause) {
      if (!isErrno(cause, "EEXIST")) throw cause;
    }
    verifyRegularPrivateFile(paths.lockDatabasePath, "missing");
    for (const suffix of ["-journal", "-wal", "-shm"]) {
      verifyPrivateFileIfPresent(`${paths.lockDatabasePath}${suffix}`);
    }
    database = new NodeSqlite.DatabaseSync(paths.lockDatabasePath);
    database.exec("PRAGMA busy_timeout = 0; PRAGMA journal_mode = DELETE; BEGIN EXCLUSIVE");
    transactionOpen = true;
    // SQLite releases this process-owned lock after abrupt death on every
    // supported platform. Keep its stable file: unlinking it could let another
    // writer lock a different inode while this transaction still owns the old one.
    return operation();
  } catch (cause) {
    if (cause instanceof NativeStoreAuthorityPersistenceError) throw cause;
    throw persistenceError(
      "source_unavailable",
      "Native authority writer lock is unavailable.",
      cause,
    );
  } finally {
    try {
      if (lockFd !== undefined) NodeFS.closeSync(lockFd);
      if (database !== undefined) {
        try {
          if (transactionOpen) database.exec("ROLLBACK");
        } finally {
          database.close();
        }
      }
    } finally {
      WRITER_LOCKS.delete(authorityStateDir);
    }
  }
};

export const readNativeStoreAuthorityState = (
  authorityStateDir: string,
): NativeStoreAuthorityState => readStateUnlocked(authorityStateDir);

/**
 * Explicit enrollment for a new native store. Normal startup never calls this:
 * losing this record requires owner-controlled re-enrollment, not a reset.
 */
export const initializeNativeStoreAuthority = (
  authorityStateDir: string,
  environmentId: string,
): NativeStoreAuthorityState =>
  withWriterLock(authorityStateDir, () => {
    validateEnvironmentId(environmentId);
    try {
      const current = readStateUnlocked(authorityStateDir);
      if (current.environment_id !== environmentId) {
        throw persistenceError("environment_mismatch", "Native authority environment changed.");
      }
      return current;
    } catch (cause) {
      if (!(cause instanceof NativeStoreAuthorityPersistenceError) || cause.code !== "missing")
        throw cause;
      const state: NativeStoreAuthorityState = {
        record_version: NATIVE_STORE_AUTHORITY_RECORD_VERSION,
        environment_id: environmentId,
        authority_namespace: `${NATIVE_STORE_AUTHORITY_NAMESPACE_PREFIX}${NodeCrypto.randomUUID()}`,
        store_generation: 1,
        state: "active",
        transition_id: null,
      };
      writeStateUnlocked(authorityStateDir, state);
      return state;
    }
  });

export const fenceNativeStoreAuthority = (
  authorityStateDir: string,
  environmentId: string,
): NativeStoreAuthorityState =>
  withWriterLock(authorityStateDir, () => {
    validateEnvironmentId(environmentId);
    const current = readStateUnlocked(authorityStateDir);
    if (current.environment_id !== environmentId) {
      throw persistenceError("environment_mismatch", "Native authority environment changed.");
    }
    if (current.state === "fenced") return current;
    const fenced: NativeStoreAuthorityState = {
      ...current,
      state: "fenced",
      transition_id: NodeCrypto.randomUUID(),
    };
    writeStateUnlocked(authorityStateDir, fenced);
    return fenced;
  });

export const advanceNativeStoreAuthority = (
  authorityStateDir: string,
  environmentId: string,
  databasePath: string,
): NativeStoreAuthorityState =>
  withWriterLock(authorityStateDir, () => {
    validateEnvironmentId(environmentId);
    const current = readStateUnlocked(authorityStateDir);
    if (current.environment_id !== environmentId) {
      throw persistenceError("environment_mismatch", "Native authority environment changed.");
    }
    if (current.state !== "fenced") {
      throw persistenceError(
        "generation_regression",
        "Native authority was not fenced before advancing.",
      );
    }
    let dbStat: NodeFS.Stats;
    try {
      dbStat = NodeFS.lstatSync(databasePath);
    } catch (cause) {
      throw persistenceError(
        "source_unavailable",
        "Restored native database is unavailable.",
        cause,
      );
    }
    if (dbStat.isSymbolicLink() || !dbStat.isFile()) {
      throw persistenceError(
        "source_unavailable",
        "Restored native database is not a regular file.",
      );
    }
    verifyOwner(dbStat, databasePath);
    let dbFd: number | undefined;
    try {
      dbFd = NodeFS.openSync(databasePath, NodeFS.constants.O_RDONLY | NOFOLLOW);
      const header = Buffer.allocUnsafe(16);
      const bytesRead = NodeFS.readSync(dbFd, header, 0, header.byteLength, 0);
      if (bytesRead !== header.byteLength || header.toString("utf8") !== "SQLite format 3\0") {
        throw persistenceError(
          "source_unavailable",
          "Restored native database is not a SQLite database.",
        );
      }
    } catch (cause) {
      if (cause instanceof NativeStoreAuthorityPersistenceError) throw cause;
      throw persistenceError(
        "source_unavailable",
        "Restored native database could not be verified.",
        cause,
      );
    } finally {
      if (dbFd !== undefined) NodeFS.closeSync(dbFd);
    }
    const nextGeneration = current.store_generation + 1;
    if (!Number.isSafeInteger(nextGeneration) || nextGeneration > MAX_SAFE_GENERATION) {
      throw persistenceError(
        "generation_regression",
        "Native authority generation cannot advance.",
      );
    }
    const active: NativeStoreAuthorityState = {
      ...current,
      store_generation: nextGeneration,
      state: "active",
      transition_id: null,
    };
    writeStateUnlocked(authorityStateDir, active);
    return active;
  });
