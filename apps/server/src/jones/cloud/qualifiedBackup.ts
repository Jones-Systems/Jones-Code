// The detached launcher must load this recovery boundary using Node built-ins only.
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
export const QUALIFIED_STARTUP_STATE_FILES = [
  "settings.json",
  "keybindings.json",
  "server-runtime.json",
  "environment-id",
  "anonymous-id",
] as const;

const SUFFIXES = ["", "-wal", "-shm"] as const;
const METADATA_RESERVE_BYTES = 1024 * 1024;
const QUALIFIED_BACKUP_RECEIPT = "backup-receipt.json";

export interface QualifiedBackupReceipt {
  readonly protocol: 1;
  readonly updateId: string;
  readonly method: "clone" | "copy";
  readonly bytes: number;
  readonly startedAt: string;
  readonly completedAt: string;
  readonly durationMs: number;
  readonly copyMs?: number;
  readonly syncMs?: number;
}

export interface QualifiedBackupAdapter {
  readonly copyFile: (source: string, target: string, flags: number) => Promise<void>;
  readonly availableBytes: (directory: string) => Promise<number>;
}

// Node's forced-clone path can return ENOSYS on macOS. cp -c is unsuitable here:
// it silently copies when cloning fails, before the full-copy capacity guard.
const DARWIN_CLONE_SCRIPT = `
ObjC.import("stdlib");
ObjC.bindFunction("clonefile", ["int", ["char *", "char *", "int"]]);
ObjC.bindFunction("__error", ["int *", []]);
function run(argv) {
  var error = $.__error();
  return $.clonefile(argv[0], argv[1], 0) === 0 ? 0 : error[0];
}
`;

// These are Darwin errno values, including when a test runs on another host.
const DARWIN_CLONE_ERRORS: Readonly<Record<number, string>> = {
  1: "EPERM",
  2: "ENOENT",
  5: "EIO",
  13: "EACCES",
  17: "EEXIST",
  18: "EXDEV",
  22: "EINVAL",
  28: "ENOSPC",
  45: "ENOTSUP",
  78: "ENOSYS",
  102: "EOPNOTSUPP",
};

async function cloneDarwinFile(source: string, target: string): Promise<void> {
  const temporary = await NodeFSP.mkdtemp(
    NodePath.join(NodePath.dirname(target), ".qualified-clone-"),
  );
  const cloned = NodePath.join(temporary, "file");
  try {
    const errno = await new Promise<number>((resolve, reject) => {
      NodeChildProcess.execFile(
        "/usr/bin/osascript",
        [
          "-l",
          "JavaScript",
          "-e",
          DARWIN_CLONE_SCRIPT,
          NodePath.resolve(source),
          NodePath.resolve(cloned),
        ],
        { encoding: "utf8", timeout: 30_000, killSignal: "SIGKILL", maxBuffer: 4096 },
        (cause, stdout) => {
          if (cause !== null) {
            reject(new Error("Native recovery clone process failed.", { cause }));
          } else if (!/^(0|[1-9][0-9]{0,2})\n?$/.test(stdout)) {
            reject(new Error("Native recovery clone returned an invalid result."));
          } else {
            resolve(Number(stdout));
          }
        },
      );
    });
    if (errno !== 0) {
      throw Object.assign(new Error("Native recovery clone failed."), {
        code: DARWIN_CLONE_ERRORS[errno] ?? "UNKNOWN",
        errno: -errno,
        syscall: "clonefile",
      });
    }
    await fileSize(cloned);
    // Only publish after confirmed syscall success. A lost subprocess response
    // leaves output inside our private directory; link preserves EXCL atomically.
    await NodeFSP.link(cloned, target);
  } finally {
    await NodeFSP.rm(temporary, { recursive: true, force: true });
  }
}

// Copying a synthetic 8 GiB file on ext4, fs.copyFile's single kernel stream ran at about
// 380 MiB/s and four concurrent 8 MiB positioned copies at about 760 MiB/s.
// Keep that ceiling; clone-capable filesystems never reach this fallback.
const COPY_CHUNK_BYTES = 8 * 1024 * 1024;
const COPY_CONCURRENCY = 4;

function copyConcurrencyCap(): number {
  try {
    const available = NodeOS.availableParallelism();
    return Number.isSafeInteger(available) && available >= 1
      ? Math.min(COPY_CONCURRENCY, available)
      : COPY_CONCURRENCY;
  } catch {
    return COPY_CONCURRENCY;
  }
}

function sampleCopyConcurrency(cap: number) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const selected = new Promise<number>((resolve) => {
    try {
      const before = NodeOS.cpus();
      // @effect-diagnostics-next-line globalTimers:off - The detached launcher must use Node built-ins and cannot load Effect.
      timer = setTimeout(() => {
        try {
          const after = NodeOS.cpus();
          if (before.length === 0 || before.length !== after.length) {
            resolve(cap);
            return;
          }
          let busy = 0;
          let idle = 0;
          for (const [index, cpu] of after.entries()) {
            for (const field of ["user", "nice", "sys", "irq", "idle"] as const) {
              const start = before[index]!.times[field];
              const end = cpu.times[field];
              const delta = end - start;
              if (!Number.isFinite(start) || start < 0 || !Number.isFinite(delta) || delta < 0) {
                resolve(cap);
                return;
              }
              if (field === "idle") idle += delta;
              else busy += delta;
            }
          }
          const total = busy + idle;
          if (!Number.isFinite(total) || total <= 0) {
            resolve(cap);
            return;
          }
          // Host CPU times are advisory, not cgroup-local usage. The baseline is already running.
          const idleCores = (1 - busy / total) * after.length;
          resolve(Math.min(cap, Math.max(1, 1 + Math.floor(idleCores))));
        } catch {
          resolve(cap);
        }
      }, 100);
      timer.unref();
    } catch {
      resolve(cap);
    }
  });
  return { selected, cancel: () => clearTimeout(timer) };
}

/** Linux full-copy fallback with fs.copyFile's EXCL, mode and failed-target cleanup; callers fsync. */
async function copyFileConcurrently(source: string, target: string): Promise<void> {
  const input = await NodeFSP.open(source, "r");
  try {
    const { size, mode } = await input.stat();
    const output = await NodeFSP.open(target, "wx", mode & 0o7777);
    const failures: unknown[] = [];
    let next = 0;
    const copyChunks = async () => {
      try {
        const buffer = Buffer.allocUnsafe(Math.min(COPY_CHUNK_BYTES, size));
        while (failures.length === 0 && next < size) {
          const offset = next;
          next += COPY_CHUNK_BYTES;
          const length = Math.min(COPY_CHUNK_BYTES, size - offset);
          for (let read = 0; read < length;) {
            const { bytesRead } = await input.read(buffer, read, length - read, offset + read);
            if (bytesRead === 0) throw new Error("Recovery copy source changed size.");
            read += bytesRead;
          }
          for (let written = 0; written < length;)
            written += (await output.write(buffer, written, length - written, offset + written))
              .bytesWritten;
        }
      } catch (cause) {
        failures.push(cause);
      }
    };
    const baseline = copyChunks();
    const loops = [baseline];
    const cap = Math.min(copyConcurrencyCap(), Math.ceil(size / COPY_CHUNK_BYTES));
    if (cap > 1) {
      const sample = sampleCopyConcurrency(cap);
      try {
        const selected = await Promise.race([sample.selected, baseline.then(() => 1)]);
        for (let worker = 1; worker < selected && failures.length === 0 && next < size; worker++)
          loops.push(copyChunks());
      } finally {
        sample.cancel();
      }
    }
    // Every chunk settles before the handle closes, so no write lands after cleanup.
    await Promise.all(loops);
    if (failures.length === 0)
      await output.chmod(mode & 0o7777).catch((cause: unknown) => failures.push(cause));
    await output.close().catch((cause: unknown) => failures.push(cause));
    if (failures.length > 0) {
      await NodeFSP.rm(target, { force: true });
      throw failures[0];
    }
  } finally {
    await input.close();
  }
}

const nativeAdapter: QualifiedBackupAdapter = {
  copyFile: (source, target, flags) => {
    // oxlint-disable-next-line t3code/no-global-process-runtime -- Detached launcher has no Effect runtime.
    const platform = NodeOS.platform();
    if (platform === "darwin" && flags & NodeFS.constants.COPYFILE_FICLONE_FORCE)
      return cloneDarwinFile(source, target);
    if (platform === "linux" && flags === NodeFS.constants.COPYFILE_EXCL)
      return copyFileConcurrently(source, target);
    return NodeFSP.copyFile(source, target, flags);
  },
  availableBytes: async (directory) => {
    const capacity = await NodeFSP.statfs(directory, { bigint: true });
    const bytes = capacity.bavail * capacity.bsize;
    return Number(
      bytes > BigInt(Number.MAX_SAFE_INTEGER) ? BigInt(Number.MAX_SAFE_INTEGER) : bytes,
    );
  },
};

const isMissing = (cause: unknown) => (cause as NodeJS.ErrnoException)?.code === "ENOENT";
const cloneUnavailable = (cause: unknown) =>
  ["ENOTSUP", "EOPNOTSUPP", "ENOSYS", "EXDEV", "EINVAL"].includes(
    (cause as NodeJS.ErrnoException)?.code ?? "",
  );

async function fileSize(file: string, optional = false): Promise<number | undefined> {
  try {
    const stat = await NodeFSP.lstat(file);
    if (!stat.isFile() || stat.isSymbolicLink())
      throw new Error("Qualified recovery requires regular state files.");
    return stat.size;
  } catch (cause) {
    if (optional && isMissing(cause)) return undefined;
    throw cause;
  }
}

async function requireDirectory(directory: string): Promise<void> {
  const stat = await NodeFSP.lstat(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink())
    throw new Error("Qualified recovery requires real directories.");
}

async function requireCapacity(
  directory: string,
  bytes: number,
  adapter: QualifiedBackupAdapter,
): Promise<void> {
  const available = await adapter.availableBytes(directory);
  if (
    !Number.isSafeInteger(bytes) ||
    !Number.isFinite(available) ||
    available < bytes + METADATA_RESERVE_BYTES
  )
    throw new Error(
      "recovery-capacity: Insufficient free space for the recovery copy; retained generations were preserved.",
    );
}

/** Probe only owned metadata files; a live database is never copied before writer shutdown. */
export async function preflightQualifiedBackup(
  baseDir: string,
  dbPath: string,
  adapter: QualifiedBackupAdapter = nativeAdapter,
): Promise<{ readonly method: "clone" | "copy"; readonly bytes: number }> {
  let bytes = 0;
  for (const suffix of SUFFIXES)
    bytes += (await fileSize(`${dbPath}${suffix}`, suffix !== "")) ?? 0;
  for (const name of QUALIFIED_STARTUP_STATE_FILES)
    bytes += (await fileSize(NodePath.join(baseDir, "userdata", name), true)) ?? 0;
  const parent = NodePath.join(baseDir, "runtime", "db-backup");
  await requireDirectory(NodePath.dirname(parent));
  await NodeFSP.mkdir(parent, { recursive: true, mode: 0o700 });
  await requireDirectory(parent);
  const sourceDevice = (await NodeFSP.stat(dbPath)).dev;
  if (sourceDevice !== (await NodeFSP.stat(parent)).dev)
    throw new Error(
      "recovery-capacity: Recovery and database must share a filesystem for rollback rename.",
    );
  const probe = await NodeFSP.mkdtemp(NodePath.join(parent, ".clone-probe-"));
  try {
    const source = NodePath.join(probe, "source");
    await NodeFSP.writeFile(source, "qualified-clone-probe", { flag: "wx", mode: 0o600 });
    try {
      await adapter.copyFile(
        source,
        NodePath.join(probe, "target"),
        NodeFS.constants.COPYFILE_EXCL | NodeFS.constants.COPYFILE_FICLONE_FORCE,
      );
      await requireCapacity(parent, 0, adapter);
      return { method: "clone", bytes };
    } catch (cause) {
      if (!cloneUnavailable(cause)) throw cause;
      await requireCapacity(parent, 2 * bytes + Math.ceil(0.05 * bytes), adapter);
      return { method: "copy", bytes };
    }
  } finally {
    await NodeFSP.rm(probe, { recursive: true, force: true });
  }
}

/** Caller owns a fresh destination and proves all writers stopped before copying state. */
export async function copyQualifiedBackupFile(
  source: string,
  target: string,
  adapter: QualifiedBackupAdapter = nativeAdapter,
): Promise<{ readonly method: "clone" | "copy"; readonly bytes: number }> {
  const bytes = (await fileSize(source))!;
  try {
    await adapter.copyFile(
      source,
      target,
      NodeFS.constants.COPYFILE_EXCL | NodeFS.constants.COPYFILE_FICLONE_FORCE,
    );
    return { method: "clone", bytes };
  } catch (cause) {
    if (!cloneUnavailable(cause)) throw cause;
    await requireCapacity(NodePath.dirname(target), bytes, adapter);
    await adapter.copyFile(source, target, NodeFS.constants.COPYFILE_EXCL);
    return { method: "copy", bytes };
  }
}

async function syncDirectory(directory: string): Promise<void> {
  const handle = await NodeFSP.open(directory, "r");
  try {
    await handle.sync();
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code !== "EPERM") throw cause;
  } finally {
    await handle.close();
  }
}

async function writeJournal(file: string, value: unknown): Promise<void> {
  const handle = await NodeFSP.open(file, "wx", 0o600);
  try {
    await handle.writeFile(`${JSON.stringify(value)}\n`);
    await handle.sync();
  } finally {
    await handle.close();
  }
  await syncDirectory(NodePath.dirname(file));
}

export async function writeQualifiedBackupReceipt(
  directory: string,
  receipt: QualifiedBackupReceipt,
): Promise<void> {
  await writeJournal(NodePath.join(directory, QUALIFIED_BACKUP_RECEIPT), receipt);
}

export async function readQualifiedBackupReceipt(
  baseDir: string,
  updateId: string,
): Promise<QualifiedBackupReceipt | undefined> {
  if (
    !updateId ||
    NodePath.basename(updateId) !== updateId ||
    updateId === "." ||
    updateId === ".."
  )
    throw new Error("Qualified recovery update identity is invalid.");
  let value: unknown;
  try {
    await fileSize(
      NodePath.join(baseDir, "runtime", "db-backup", updateId, QUALIFIED_BACKUP_RECEIPT),
    );
    value = JSON.parse(
      await NodeFSP.readFile(
        NodePath.join(baseDir, "runtime", "db-backup", updateId, QUALIFIED_BACKUP_RECEIPT),
        "utf8",
      ),
    );
  } catch (cause) {
    if (isMissing(cause)) return undefined;
    throw cause;
  }
  if (typeof value !== "object" || value === null)
    throw new Error("Qualified recovery receipt is invalid.");
  const receipt = value as Record<string, unknown>;
  if (
    receipt.protocol !== 1 ||
    receipt.updateId !== updateId ||
    (receipt.method !== "clone" && receipt.method !== "copy") ||
    typeof receipt.bytes !== "number" ||
    !Number.isSafeInteger(receipt.bytes) ||
    receipt.bytes < 0 ||
    typeof receipt.durationMs !== "number" ||
    !Number.isFinite(receipt.durationMs) ||
    receipt.durationMs < 0 ||
    [receipt.copyMs, receipt.syncMs].some(
      (duration) =>
        duration !== undefined &&
        (typeof duration !== "number" || !Number.isFinite(duration) || duration < 0),
    ) ||
    typeof receipt.startedAt !== "string" ||
    !Number.isFinite(Date.parse(receipt.startedAt)) ||
    typeof receipt.completedAt !== "string" ||
    !Number.isFinite(Date.parse(receipt.completedAt))
  )
    throw new Error("Qualified recovery receipt is invalid.");
  return receipt as unknown as QualifiedBackupReceipt;
}

interface AdvancedEntry {
  readonly name: string;
  readonly device: string;
  readonly inode: string;
}

/** The durable file identities distinguish an interrupted rename from later restored live files. */
export async function retainQualifiedAdvancedState(
  baseDir: string,
  dbPath: string,
  backupDir: string,
): Promise<void> {
  const directory = NodePath.join(backupDir, "advanced-state");
  await requireDirectory(backupDir);
  await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
  await requireDirectory(directory);
  const journalPath = NodePath.join(directory, "rename-journal.json");
  const completion = NodePath.join(directory, "paired-settings.json");
  const completed = (await fileSize(completion, true)) !== undefined;
  const names = [
    ...SUFFIXES.map((suffix) => `database${suffix}`),
    ...QUALIFIED_STARTUP_STATE_FILES,
  ];
  const sourcePath = (name: string) =>
    name.startsWith("database")
      ? `${dbPath}${name.slice("database".length)}`
      : NodePath.join(baseDir, "userdata", name);
  let entries: AdvancedEntry[];
  try {
    const parsed: unknown = JSON.parse(await NodeFSP.readFile(journalPath, "utf8"));
    if (
      !Array.isArray(parsed) ||
      parsed.some(
        (entry) =>
          typeof entry !== "object" ||
          entry === null ||
          !names.includes(entry.name) ||
          typeof entry.device !== "string" ||
          !/^\d+$/.test(entry.device) ||
          typeof entry.inode !== "string" ||
          !/^\d+$/.test(entry.inode),
      ) ||
      new Set(parsed.map((entry) => entry.name)).size !== parsed.length ||
      !parsed.some((entry) => entry.name === "database")
    )
      throw new Error("Advanced state rename journal is invalid; recovery held.");
    entries = parsed;
  } catch (cause) {
    if (!isMissing(cause)) throw cause;
    if (completed) {
      // Previous launchers wrote a completed advanced copy without a rename journal.
      await fileSize(NodePath.join(directory, "database"));
      const settings: unknown = JSON.parse(await NodeFSP.readFile(completion, "utf8"));
      if (
        !Array.isArray(settings) ||
        settings.some((name) => !QUALIFIED_STARTUP_STATE_FILES.some((allowed) => allowed === name))
      )
        throw new Error("Advanced state settings receipt is invalid; recovery held.", { cause });
      for (const name of settings) await fileSize(NodePath.join(directory, name));
      return;
    }
    if ((await NodeFSP.readdir(directory)).length !== 0)
      throw new Error("Advanced state has unknown prior effects; recovery held.", { cause });
    entries = [];
    for (const name of names) {
      if ((await fileSize(sourcePath(name), name !== "database")) === undefined) continue;
      const stat = await NodeFSP.stat(sourcePath(name), { bigint: true });
      entries.push({ name, device: stat.dev.toString(), inode: stat.ino.toString() });
    }
    await writeJournal(journalPath, entries);
  }
  for (const entry of entries) {
    const target = NodePath.join(directory, entry.name);
    let destination;
    try {
      destination = await NodeFSP.lstat(target, { bigint: true });
    } catch (cause) {
      if (!isMissing(cause)) throw cause;
    }
    if (destination === undefined) {
      const source = await NodeFSP.lstat(sourcePath(entry.name), { bigint: true });
      if (
        !source.isFile() ||
        source.isSymbolicLink() ||
        source.dev.toString() !== entry.device ||
        source.ino.toString() !== entry.inode
      )
        throw new Error("Advanced state identity changed; recovery held.");
      await NodeFSP.rename(sourcePath(entry.name), target);
    } else if (
      !destination.isFile() ||
      destination.isSymbolicLink() ||
      destination.dev.toString() !== entry.device ||
      destination.ino.toString() !== entry.inode
    )
      throw new Error("Advanced state destination has unknown prior effects; recovery held.");
    await syncDirectory(directory);
    await syncDirectory(NodePath.dirname(dbPath));
  }
  if (!completed)
    await writeJournal(
      completion,
      entries
        .filter((entry) => QUALIFIED_STARTUP_STATE_FILES.some((name) => name === entry.name))
        .map((entry) => entry.name),
    );
}

/** Durable intent owns the one restore buffer; completed copies resume by inode without recopying. */
export async function restoreQualifiedBackupFile(source: string, target: string): Promise<void> {
  const { temporary, intentPath, completedPath, intent } = restoreFileJournal(source, target);
  const readJson = async (file: string): Promise<unknown> => {
    await fileSize(file);
    return JSON.parse(await NodeFSP.readFile(file, "utf8"));
  };
  let recorded: unknown;
  try {
    recorded = await readJson(intentPath);
  } catch (cause) {
    if (!isMissing(cause)) throw cause;
  }
  if (recorded === undefined) {
    if (
      (await fileSize(temporary, true)) !== undefined ||
      (await fileSize(completedPath, true)) !== undefined
    )
      throw new Error("Restore buffer has unknown prior effects; recovery held.");
    await writeJournal(intentPath, intent);
  } else if (JSON.stringify(recorded) !== JSON.stringify(intent))
    throw new Error("Restore intent belongs to another state pair; recovery held.");
  let completed: unknown;
  try {
    completed = await readJson(completedPath);
  } catch (cause) {
    if (!isMissing(cause)) throw cause;
  }
  if (completed === undefined) {
    // This exact buffer is owned by the durable intent, including a power-loss partial copy.
    if ((await fileSize(temporary, true)) !== undefined) {
      await NodeFSP.rm(temporary);
      await syncDirectory(NodePath.dirname(target));
    }
    await copyQualifiedBackupFile(source, temporary);
    const handle = await NodeFSP.open(temporary, "r+");
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
    const stat = await NodeFSP.stat(temporary, { bigint: true });
    completed = { device: stat.dev.toString(), inode: stat.ino.toString() };
    await syncDirectory(NodePath.dirname(target));
    await writeJournal(completedPath, completed);
  }
  if (!isRestoreCompletion(completed))
    throw new Error("Restore completion receipt is invalid; recovery held.");
  const exists = (await fileSize(temporary, true)) !== undefined;
  const stat = await NodeFSP.lstat(exists ? temporary : target, { bigint: true });
  if (
    !stat.isFile() ||
    stat.isSymbolicLink() ||
    stat.dev.toString() !== completed.device ||
    stat.ino.toString() !== completed.inode
  )
    throw new Error("Restore completion identity changed; recovery held.");
  if (exists) {
    await NodeFSP.rename(temporary, target);
    await syncDirectory(NodePath.dirname(target));
  }
}

function restoreFileJournal(source: string, target: string) {
  const identity = NodeCrypto.createHash("sha256").update(source).digest("hex").slice(0, 16);
  const temporary = `${target}.restore-${identity}`;
  const intentPath = NodePath.join(
    NodePath.dirname(source),
    `.${NodePath.basename(source)}-restore.json`,
  );
  return {
    temporary,
    intentPath,
    completedPath: `${intentPath}.complete`,
    intent: { source, target, temporary },
  };
}

function isRestoreCompletion(value: unknown): value is { device: string; inode: string } {
  return (
    typeof value === "object" &&
    value !== null &&
    "device" in value &&
    typeof value.device === "string" &&
    /^\d+$/.test(value.device) &&
    "inode" in value &&
    typeof value.inode === "string" &&
    /^\d+$/.test(value.inode)
  );
}

/** Terminal authority comes from service state; receipts alone never authorize payload removal. */
export async function removeQualifiedRecoveryPayload(
  baseDir: string,
  updateId: string,
  liveDbPath: string,
  expectedBinding: { readonly transactionId: string; readonly qualified: unknown },
  terminal: "committed" | "rolled-back" | "failed-before-trial",
): Promise<void> {
  const fail = (category: string): never => {
    throw new Error(`recovery-cleanup: ${category}`);
  };
  if (
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(updateId) ||
    NodePath.basename(updateId) !== updateId ||
    expectedBinding.transactionId !== updateId
  )
    fail("invalid-update-id");
  if (
    (await NodeFSP.realpath(baseDir)) !== baseDir ||
    liveDbPath !== NodePath.join(baseDir, "userdata", "statev2.sqlite")
  )
    fail("invalid-native-home");
  const uid = process.getuid?.();
  if (uid === undefined) fail("ownership-unavailable");
  const same = (a: NodeFS.BigIntStats, b: NodeFS.BigIntStats) => a.dev === b.dev && a.ino === b.ino;
  const statOptional = async (path: string) => {
    try {
      return await NodeFSP.lstat(path, { bigint: true });
    } catch (cause) {
      if (!isMissing(cause)) throw cause;
      return undefined;
    }
  };
  const directories: {
    path: string;
    handle: NodeFSP.FileHandle;
    identity: NodeFS.BigIntStats;
    access: string;
  }[] = [];
  const checkDirectories = async () => {
    for (const directory of directories) {
      const current = await statOptional(directory.path);
      if (
        current === undefined ||
        !same(current, directory.identity) ||
        !current.isDirectory() ||
        current.uid !== BigInt(uid!) ||
        (current.mode & 0o022n) !== 0n
      )
        fail("directory-changed");
    }
  };
  const pin = async (path: string, access: string, optional = false) => {
    const identity = await statOptional(access);
    if (identity === undefined) {
      if (optional) return undefined;
      return fail("directory-missing");
    }
    if (!identity.isDirectory() || identity.uid !== BigInt(uid!) || (identity.mode & 0o022n) !== 0n)
      fail("unsafe-directory");
    const handle = await NodeFSP.open(
      access,
      NodeFS.constants.O_RDONLY | NodeFS.constants.O_DIRECTORY | NodeFS.constants.O_NOFOLLOW,
    );
    const directory = {
      path,
      handle,
      identity,
      // oxlint-disable-next-line t3code/no-global-process-runtime -- Detached launcher cleanup uses the native host and Node built-ins only.
      access: process.platform === "linux" ? `/proc/self/fd/${handle.fd}` : path,
    };
    directories.push(directory);
    if (!same(identity, await handle.stat({ bigint: true }))) fail("directory-changed");
    const accessible = await NodeFSP.stat(directory.access, { bigint: true }).catch(() =>
      fail("directory-access-unavailable"),
    );
    if (!same(identity, accessible)) fail("directory-changed");
    await checkDirectories();
    return directory;
  };
  const names = ["database", "database-wal", "database-shm", ...QUALIFIED_STARTUP_STATE_FILES];
  try {
    let parent = await pin(baseDir, baseDir);
    for (const name of ["runtime", "db-backup", updateId]) {
      parent = await pin(
        NodePath.join(parent!.path, name),
        NodePath.join(parent!.access, name),
        true,
      );
      if (parent === undefined) return;
    }
    const generation = parent!;
    const readJournal = async (name: string) => {
      const path = NodePath.join(generation.access, name);
      const identity = await statOptional(path);
      if (identity === undefined || !identity.isFile() || identity.nlink !== 1n)
        fail("unsafe-journal");
      const handle = await NodeFSP.open(
        path,
        NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
      );
      try {
        if (!same(identity!, await handle.stat({ bigint: true }))) fail("journal-changed");
        return JSON.parse(await handle.readFile("utf8")) as unknown;
      } finally {
        await handle.close();
      }
    };
    if (!NodeUtil.isDeepStrictEqual(await readJournal("paired-binding.json"), expectedBinding))
      fail("binding-mismatch");
    const advanced = await pin(
      NodePath.join(generation.path, "advanced-state"),
      NodePath.join(generation.access, "advanced-state"),
      true,
    );
    const payloadDirectories = advanced === undefined ? [generation] : [generation, advanced];
    const candidates: {
      directory: typeof generation;
      name: string;
      identity: NodeFS.BigIntStats;
    }[] = [];
    for (const directory of payloadDirectories) {
      for (const name of names) {
        const identity = await statOptional(NodePath.join(directory.access, name));
        if (identity === undefined) continue;
        if (!identity.isFile() || identity.nlink !== 1n) fail("unsafe-payload");
        candidates.push({ directory, name, identity });
      }
    }
    const completions: { device: string; inode: string }[] = [];
    if (
      terminal !== "rolled-back" &&
      (await statOptional(NodePath.join(generation.access, ".restore-pending"))) !== undefined
    )
      fail("restore-intent-unexpected");
    for (const directory of payloadDirectories) {
      for (const name of names) {
        const intentName = `.${name}-restore.json`;
        const intentExists = await statOptional(NodePath.join(directory.access, intentName));
        const completeExists = await statOptional(
          NodePath.join(directory.access, `${intentName}.complete`),
        );
        if (terminal !== "rolled-back" || directory !== generation) {
          if (intentExists !== undefined || completeExists !== undefined)
            fail("restore-intent-unexpected");
          continue;
        }
        if (intentExists === undefined && completeExists === undefined && name !== "database")
          continue;
        if (intentExists === undefined || completeExists === undefined) fail("restore-incomplete");
        const intent = await readJournal(intentName);
        if (
          typeof intent !== "object" ||
          intent === null ||
          !("target" in intent) ||
          typeof intent.target !== "string" ||
          !NodePath.isAbsolute(intent.target)
        )
          fail("restore-incomplete");
        const expected = restoreFileJournal(
          NodePath.join(generation.path, name),
          (intent as { target: string }).target,
        );
        if (
          JSON.stringify(intent) !== JSON.stringify(expected.intent) ||
          (await statOptional(expected.temporary)) !== undefined
        )
          fail("restore-incomplete");
        const completed = await readJournal(`${intentName}.complete`);
        if (!isRestoreCompletion(completed)) fail("restore-incomplete");
        completions.push(completed as { device: string; inode: string });
      }
    }
    const livePaths = [
      ...SUFFIXES.map((suffix) => `${liveDbPath}${suffix}`),
      ...QUALIFIED_STARTUP_STATE_FILES.map((name) => NodePath.join(baseDir, "userdata", name)),
    ];
    const assertDisjoint = async (identity: NodeFS.BigIntStats) => {
      if (
        completions.some(
          (complete) =>
            complete.device === identity.dev.toString() &&
            complete.inode === identity.ino.toString(),
        )
      )
        fail("restore-incomplete");
      for (const path of livePaths) {
        const live = await statOptional(path);
        if (live !== undefined && (live.isSymbolicLink() || same(live, identity)))
          fail("live-payload-overlap");
      }
    };
    for (const candidate of candidates) await assertDisjoint(candidate.identity);
    // Linux unlinks through the pinned fd. Other platforms revalidate the full walk on both sides.
    // Same-uid writers remain trusted across the final lstat/unlink window.
    for (const candidate of candidates) {
      await checkDirectories();
      const path = NodePath.join(candidate.directory.access, candidate.name);
      const current = await statOptional(path);
      if (current === undefined) continue;
      if (!current.isFile() || current.nlink !== 1n || !same(current, candidate.identity))
        fail("payload-changed");
      await assertDisjoint(current);
      await checkDirectories();
      await NodeFSP.unlink(path).catch((cause: unknown) => {
        if (!isMissing(cause)) throw cause;
      });
      await checkDirectories();
    }
    for (const directory of payloadDirectories) await directory.handle.sync();
  } finally {
    for (const directory of directories.toReversed()) await directory.handle.close();
  }
}
