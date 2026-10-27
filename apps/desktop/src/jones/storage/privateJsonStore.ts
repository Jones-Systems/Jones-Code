// @effect-diagnostics nodeBuiltinImport:off - Private native journals use ordinary-UID filesystem operations.
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as NodeSqlite from "node:sqlite";

export async function ownedDirectory(directory: string, privateMode = true): Promise<void> {
  await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
  const stat = await NodeFSP.lstat(directory);
  if (
    !stat.isDirectory() ||
    stat.isSymbolicLink() ||
    (stat.mode & (privateMode ? 0o077 : 0o022)) !== 0 ||
    (typeof process.getuid === "function" && stat.uid !== process.getuid())
  )
    throw new Error("Private journal directory ownership is unknown.");
  const parent = await NodeFSP.open(NodePath.dirname(directory), "r");
  try {
    await parent.sync();
  } finally {
    await parent.close();
  }
}

export async function readPrivateJson(file: string, limit: number): Promise<unknown | null> {
  const input = await NodeFSP.open(
    file,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  ).catch((error: NodeJS.ErrnoException) => {
    if (error.code === "ENOENT") return null;
    throw error;
  });
  if (input === null) return null;
  try {
    const stat = await input.stat();
    if (
      !stat.isFile() ||
      stat.nlink !== 1 ||
      stat.size > limit ||
      (stat.mode & 0o077) !== 0 ||
      (typeof process.getuid === "function" && stat.uid !== process.getuid())
    )
      throw new Error("Private journal file ownership or size is invalid.");
    // A fixed-size read also bounds a file that grows after fstat.
    const bytes = Buffer.alloc(limit + 1);
    let offset = 0;
    while (offset < bytes.length) {
      const result = await input.read(bytes, offset, bytes.length - offset, null);
      if (result.bytesRead === 0) break;
      offset += result.bytesRead;
    }
    if (offset > limit) throw new Error("Private journal exceeds its storage bound.");
    return JSON.parse(bytes.subarray(0, offset).toString("utf8"));
  } finally {
    await input.close();
  }
}

export async function replacePrivateJson(
  file: string,
  value: unknown,
  limit: number,
): Promise<void> {
  const bytes = `${JSON.stringify(value)}\n`;
  if (Buffer.byteLength(bytes) > limit)
    throw new Error("Private journal exceeds its storage bound.");
  const temporary = NodePath.join(NodePath.dirname(file), `${NodeCrypto.randomUUID()}.tmp`);
  const output = await NodeFSP.open(temporary, "wx", 0o600);
  try {
    try {
      await output.writeFile(bytes);
      await output.sync();
    } finally {
      await output.close();
    }
    await NodeFSP.rename(temporary, file);
    const directory = await NodeFSP.open(NodePath.dirname(file), "r");
    try {
      await directory.sync();
    } finally {
      await directory.close();
    }
  } finally {
    await NodeFSP.unlink(temporary).catch((error: NodeJS.ErrnoException) => {
      if (error.code !== "ENOENT") throw error;
    });
  }
}

/** Cross-process CAS guard; no shared enrollment or application database is opened. */
export async function withPrivateJournalLock<T>(
  directory: string,
  work: () => Promise<T>,
): Promise<T> {
  const file = NodePath.join(directory, "lock.sqlite");
  await NodeFSP.open(file, "ax", 0o600)
    .then((handle) => handle.close())
    .catch((error: NodeJS.ErrnoException) => {
      if (error.code !== "EEXIST") throw error;
    });
  const stat = await NodeFSP.lstat(file);
  if (
    !stat.isFile() ||
    stat.isSymbolicLink() ||
    stat.nlink !== 1 ||
    (stat.mode & 0o077) !== 0 ||
    (typeof process.getuid === "function" && stat.uid !== process.getuid())
  )
    throw new Error("Private journal lock ownership is unknown.");
  const lock = new NodeSqlite.DatabaseSync(file);
  try {
    lock.exec("PRAGMA busy_timeout=0; BEGIN EXCLUSIVE");
    return await work();
  } finally {
    lock.close();
  }
}
