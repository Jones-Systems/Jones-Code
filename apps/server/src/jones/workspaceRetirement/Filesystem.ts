// @effect-diagnostics nodeBuiltinImport:off -- Native no-follow descriptor adapter; callers own the Effect lifetime.
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import { darwinFilesystem } from "./DarwinFilesystem.ts";

export const sha256 = (value: string | Uint8Array) =>
  NodeCrypto.createHash("sha256").update(value).digest("hex");
export function canonical(value: unknown): string {
  const sorted = (v: unknown): unknown =>
    Array.isArray(v)
      ? v.map(sorted)
      : v !== null && typeof v === "object"
        ? Object.fromEntries(
            Object.entries(v)
              .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
              .map(([k, x]) => [k, sorted(x)]),
          )
        : v;
  return JSON.stringify(sorted(value)).replace(
    /[\u007f-\uffff]/g,
    (c) => `\\u${c.charCodeAt(0).toString(16).padStart(4, "0")}`,
  );
}
export const inside = (path: string, root: string) =>
  path === root || path.startsWith(root + NodePath.sep);
export const snapshot = (s: NodeFS.BigIntStats) =>
  [s.dev, s.ino, s.mode, s.size, s.mtimeNs, s.ctimeNs].join(":");
export const sensitive = (name: string) => {
  const n = name.toLowerCase();
  return (
    (n.startsWith(".env") && ![".example", ".sample", ".template"].some((x) => n.endsWith(x))) ||
    /\.(pem|key|p12)$/.test(n) ||
    n.startsWith("id_") ||
    n === ".netrc" ||
    n === ".npmrc"
  );
};
const directoryPaths = new Map<number, string>();
// oxlint-disable-next-line t3code/no-global-process-runtime -- Descriptor paths and no-follow syscalls bind to the actual host ABI, including standalone setup; an injected application platform cannot select that ABI.
export const nativePlatform = process.platform;
export const fdPath = (fd: number) =>
  nativePlatform === "darwin" ? directoryPaths.get(fd)! : `/proc/self/fd/${fd}`;
export async function openRelative(root: NodeFSP.FileHandle, relpath: string, directory = false) {
  const parts = relpath.split("/").filter(Boolean);
  if (parts.some((x) => x === "." || x === ".." || x.includes("\0")))
    throw new Error("identity_mismatch");
  let parent = root;
  try {
    for (let i = 0; i < parts.length; i++) {
      const next = await NodeFSP.open(
        `${fdPath(parent.fd)}/${parts[i]}`,
        NodeFS.constants.O_RDONLY |
          NodeFS.constants.O_NOFOLLOW |
          NodeFS.constants.O_NONBLOCK |
          (i < parts.length - 1 || directory ? NodeFS.constants.O_DIRECTORY : 0),
      );
      if (nativePlatform === "darwin")
        directoryPaths.set(next.fd, NodePath.join(fdPath(parent.fd), parts[i]!));
      if (parent !== root) await parent.close();
      parent = next;
    }
    if (parent === root) {
      // The descriptor link is an intermediate anchor on Linux. Opening it as
      // the final component with O_NOFOLLOW rejects /proc/self/fd/N itself.
      const copy = await NodeFSP.open(
        `${fdPath(root.fd)}/.`,
        NodeFS.constants.O_RDONLY | NodeFS.constants.O_DIRECTORY | NodeFS.constants.O_NOFOLLOW,
      );
      if (nativePlatform === "darwin") directoryPaths.set(copy.fd, fdPath(root.fd));
      return copy;
    }
    return parent;
  } catch (cause) {
    if (parent !== root) await parent.close();
    throw cause;
  }
}
export async function openAbsolute(path: string, directory = false) {
  if (!NodePath.isAbsolute(path) || NodePath.normalize(path) !== path)
    throw new Error("identity_mismatch");
  const anchor = await NodeFSP.open("/", NodeFS.constants.O_RDONLY | NodeFS.constants.O_DIRECTORY);
  if (nativePlatform === "darwin") directoryPaths.set(anchor.fd, "/");
  try {
    return await openRelative(anchor, path.slice(1), directory);
  } finally {
    await anchor.close();
  }
}
export async function readBounded(path: string, limit: number) {
  if (nativePlatform === "darwin") {
    const value = (await darwinFilesystem({ operation: "read", path, limit })) as { bytes: string };
    return Buffer.from(value.bytes, "base64");
  }
  const file = await openAbsolute(path);
  try {
    const before = await file.stat({ bigint: true });
    if (!before.isFile() || before.size > BigInt(limit)) throw new Error("bound_exceeded");
    const bytes = Buffer.alloc(Number(before.size) + 1);
    let offset = 0;
    while (offset < bytes.length) {
      const read = await file.read(bytes, offset, bytes.length - offset, null);
      if (!read.bytesRead) break;
      offset += read.bytesRead;
    }
    if (
      offset !== Number(before.size) ||
      snapshot(before) !== snapshot(await file.stat({ bigint: true }))
    )
      throw new Error("observation_failed");
    return bytes.subarray(0, offset);
  } finally {
    await file.close();
  }
}
export function git(cwd: string, args: readonly string[], timeout = 10000): Promise<Buffer> {
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    GIT_OPTIONAL_LOCKS: "0",
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: "/dev/null",
    GIT_TERMINAL_PROMPT: "0",
    GIT_NO_LAZY_FETCH: "1",
  };
  for (const key of Object.keys(env))
    if (
      /^GIT_(DIR|WORK_TREE|INDEX_FILE|OBJECT_DIRECTORY|ALTERNATE_OBJECT_DIRECTORIES|CONFIG_COUNT|CONFIG_KEY_\d+|CONFIG_VALUE_\d+)$/.test(
        key,
      )
    )
      delete env[key as keyof typeof env];
  return new Promise((resolve, reject) =>
    NodeChildProcess.execFile(
      "git",
      [
        "--no-optional-locks",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.untrackedCache=false",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.quotePath=false",
        "-C",
        cwd,
        ...args,
      ],
      { env, encoding: "buffer", timeout, maxBuffer: 32 * 1024 * 1024 },
      (error, stdout) => (error ? reject(error) : resolve(stdout)),
    ),
  );
}
export async function exists(path: string) {
  try {
    await NodeFSP.lstat(path);
    return true;
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw cause;
  }
}
