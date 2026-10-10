// The detached launcher must load this recovery boundary using Node built-ins only.
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { QUALIFIED_STARTUP_STATE_FILES } from "./qualifiedQuiescence.ts";

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
}

export interface QualifiedBackupAdapter {
  readonly copyFile: (source: string, target: string, flags: number) => Promise<void>;
  readonly availableBytes: (directory: string) => Promise<number>;
}

const nativeAdapter: QualifiedBackupAdapter = {
  copyFile: NodeFSP.copyFile,
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
      await requireCapacity(parent, bytes, adapter);
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
  const identity = NodeCrypto.createHash("sha256").update(source).digest("hex").slice(0, 16);
  const temporary = `${target}.restore-${identity}`;
  const directory = NodePath.dirname(source);
  const intentPath = NodePath.join(directory, `.${NodePath.basename(source)}-restore.json`);
  const completedPath = `${intentPath}.complete`;
  const intent = { source, target, temporary };
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
  if (
    typeof completed !== "object" ||
    completed === null ||
    !("device" in completed) ||
    typeof completed.device !== "string" ||
    !/^\d+$/.test(completed.device) ||
    !("inode" in completed) ||
    typeof completed.inode !== "string" ||
    !/^\d+$/.test(completed.inode)
  )
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
