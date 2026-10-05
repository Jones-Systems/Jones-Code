// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalTimers:off
// Native observation at the detached launcher boundary. This proves ordinary
// same-user writers; privileged/system actors remain outside the service model.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

export const QUALIFIED_STARTUP_STATE_FILES = [
  "settings.json",
  "keybindings.json",
  "server-runtime.json",
  "environment-id",
  "anonymous-id",
] as const;

export class QualifiedQuiescenceError extends Error {
  readonly reason: "writer-active" | "unavailable" | "proof-limit" | "state-identity-changed";
  constructor(reason: QualifiedQuiescenceError["reason"]) {
    super(`Qualified state writer proof blocked: ${reason}. Previous binary/state pair retained.`);
    this.name = "QualifiedQuiescenceError";
    this.reason = reason;
  }
}
export interface QualifiedStateFile {
  readonly path: string;
  readonly device: bigint;
  readonly inode: bigint;
}
export interface QualifiedWriterScan {
  readonly files: readonly QualifiedStateFile[];
  readonly uid: number;
  readonly allowedProcessIds: readonly number[];
}
export interface QualifiedQuiescenceAdapter {
  scan(
    input: QualifiedWriterScan,
  ): Promise<readonly { readonly pid: number; readonly path: string }[]>;
}
const unavailable = () => new QualifiedQuiescenceError("unavailable");
const isGone = (cause: unknown) => (cause as NodeJS.ErrnoException)?.code === "ENOENT";

async function boundedRead(file: string, limit: number): Promise<string> {
  const handle = await NodeFSP.open(file, "r");
  try {
    const buffer = Buffer.alloc(limit + 1);
    let offset = 0;
    while (offset <= limit) {
      const { bytesRead } = await handle.read(buffer, offset, buffer.length - offset, offset);
      if (bytesRead === 0) return buffer.subarray(0, offset).toString("utf8");
      offset += bytesRead;
    }
    throw new QualifiedQuiescenceError("proof-limit");
  } finally {
    await handle.close();
  }
}
const processBirth = (stat: string) => {
  const end = stat.lastIndexOf(")");
  const value = stat.slice(end + 2).split(/\s+/)[19];
  if (end < 0 || value === undefined || !/^\d+$/.test(value)) throw unavailable();
  return value;
};
const deviceText = (dev: bigint) => {
  const major = ((dev >> 8n) & 0xfffn) | ((dev >> 32n) & 0xfffff000n);
  const minor = (dev & 0xffn) | ((dev >> 12n) & 0xffffff00n);
  return `${major.toString(16)}:${minor.toString(16)}`;
};

export function linuxQualifiedQuiescenceAdapter(
  options: {
    readonly procRoot?: string;
    readonly maximumProcesses?: number;
    readonly maximumDescriptors?: number;
    readonly now?: () => number;
  } = {},
): QualifiedQuiescenceAdapter {
  const procRoot = options.procRoot ?? "/proc";
  const now = options.now ?? Date.now;
  return {
    async scan(input) {
      const started = now();
      let descriptors = 0;
      const bound = () => {
        if (now() - started > 5_000 || descriptors > (options.maximumDescriptors ?? 100_000))
          throw new QualifiedQuiescenceError("proof-limit");
      };
      const names = (await NodeFSP.readdir(procRoot)).filter((name) => /^\d+$/.test(name));
      if (names.length > (options.maximumProcesses ?? 4_096))
        throw new QualifiedQuiescenceError("proof-limit");
      const writers: { pid: number; path: string }[] = [];
      for (const name of names) {
        bound();
        const directory = NodePath.join(procRoot, name);
        const pid = Number(name);
        try {
          // /proc directory ownership gates observation without privileged reads.
          if ((await NodeFSP.stat(directory)).uid !== input.uid) continue;
          const status = await boundedRead(NodePath.join(directory, "status"), 64 * 1024);
          const effectiveUid = /^Uid:\s+\d+\s+(\d+)/m.exec(status)?.[1];
          if (effectiveUid === undefined) throw unavailable();
          if (Number(effectiveUid) !== input.uid) continue;
          const birth = processBirth(
            await boundedRead(NodePath.join(directory, "stat"), 16 * 1024),
          );
          if (!input.allowedProcessIds.includes(pid)) {
            const fds = (await NodeFSP.readdir(NodePath.join(directory, "fd"))).filter((fd) =>
              /^\d+$/.test(fd),
            );
            descriptors += fds.length;
            bound();
            for (const fd of fds) {
              bound();
              try {
                const stat = await NodeFSP.stat(NodePath.join(directory, "fd", fd), {
                  bigint: true,
                });
                const target = input.files.find(
                  (file) => file.device === stat.dev && file.inode === stat.ino,
                );
                if (target === undefined) continue;
                const info = await boundedRead(NodePath.join(directory, "fdinfo", fd), 8 * 1024);
                const flags = /^flags:\s+([0-7]+)$/m.exec(info)?.[1];
                if (flags === undefined) throw unavailable();
                if ((Number.parseInt(flags, 8) & 3) !== 0) writers.push({ pid, path: target.path });
              } catch (cause) {
                if (!isGone(cause)) throw cause;
                // A closed descriptor is harmless; an existing descriptor with
                // unavailable access flags leaves the writer proof incomplete.
                if (
                  await NodeFSP.stat(NodePath.join(directory, "fd", fd)).then(
                    () => true,
                    (error: unknown) => {
                      if (isGone(error)) return false;
                      throw error;
                    },
                  )
                )
                  throw unavailable();
              }
            }
            // A writable shared mapping can outlive the descriptor that opened it.
            const maps = await boundedRead(NodePath.join(directory, "maps"), 4 * 1024 * 1024);
            for (const line of maps.split("\n")) {
              if (!line) continue;
              const fields = line.split(/\s+/);
              if (fields.length < 5) throw unavailable();
              if (!fields[1]?.includes("w") || !fields[1]?.includes("s")) continue;
              const mappedInode = fields[4];
              if (mappedInode === undefined || !/^\d+$/.test(mappedInode)) throw unavailable();
              const target = input.files.find(
                (file) =>
                  BigInt(mappedInode) === file.inode &&
                  fields[3]
                    ?.split(":")
                    .map((part) => BigInt(`0x${part}`).toString(16))
                    .join(":") === deviceText(file.device),
              );
              if (target !== undefined) writers.push({ pid, path: target.path });
            }
          }
          if (
            processBirth(await boundedRead(NodePath.join(directory, "stat"), 16 * 1024)) !== birth
          )
            throw unavailable();
        } catch (cause) {
          if (isGone(cause)) {
            // Ignore a vanished process only when the process directory vanished too.
            if (
              await NodeFSP.stat(directory).then(
                () => true,
                (error: unknown) => {
                  if (isGone(error)) return false;
                  throw error;
                },
              )
            )
              throw unavailable();
          } else throw cause;
        }
      }
      // A same-user process born during this bounded scan has not been observed.
      const finalNames = (await NodeFSP.readdir(procRoot)).filter((name) => /^\d+$/.test(name));
      if (finalNames.length > (options.maximumProcesses ?? 4_096))
        throw new QualifiedQuiescenceError("proof-limit");
      for (const name of finalNames) {
        bound();
        if (names.includes(name)) continue;
        try {
          if ((await NodeFSP.stat(NodePath.join(procRoot, name))).uid === input.uid)
            throw unavailable();
        } catch (cause) {
          if (!isGone(cause)) throw cause;
        }
      }
      bound();
      return writers;
    },
  };
}

export function parseDarwinWriterOutput(output: string, input: QualifiedWriterScan) {
  const writers: { pid: number; path: string }[] = [];
  let pid: number | undefined;
  let access: string | undefined;
  let descriptor: string | undefined;
  for (const line of output.split("\n")) {
    if (!line) continue;
    const value = line.slice(1);
    switch (line[0]) {
      case "p":
        if (!/^\d+$/.test(value)) throw unavailable();
        pid = Number(value);
        access = undefined;
        descriptor = undefined;
        break;
      case "f":
        descriptor = value;
        access = undefined;
        break;
      case "a":
        access = value;
        break;
      case "n": {
        const file = input.files.find((target) => target.path === value);
        if (pid === undefined || descriptor === undefined || file === undefined)
          throw unavailable();
        if (!input.allowedProcessIds.includes(pid) && access !== "r")
          writers.push({ pid, path: file.path });
        break;
      }
      default:
        throw unavailable();
    }
  }
  return writers;
}

function darwinQualifiedQuiescenceAdapter(): QualifiedQuiescenceAdapter {
  return {
    async scan(input) {
      if (input.files.length === 0) return [];
      const output = await new Promise<string>((resolve, reject) => {
        const child = NodeChildProcess.spawn(
          "/usr/sbin/lsof",
          [
            "-nP",
            "-a",
            "-u",
            String(input.uid),
            "-F",
            "pfan",
            "--",
            ...input.files.map((file) => file.path),
          ],
          { stdio: ["ignore", "pipe", "pipe"] },
        );
        let stdout = "";
        let stderrSize = 0;
        const timeout = setTimeout(() => {
          child.kill("SIGKILL");
          reject(new QualifiedQuiescenceError("proof-limit"));
        }, 5_000);
        child.stdout.on("data", (data: Buffer) => {
          stdout += data.toString("utf8");
          if (Buffer.byteLength(stdout) > 1024 * 1024) {
            child.kill("SIGKILL");
            reject(new QualifiedQuiescenceError("proof-limit"));
          }
        });
        child.stderr.on("data", (data: Buffer) => {
          stderrSize += data.length;
          if (stderrSize > 16 * 1024) {
            child.kill("SIGKILL");
            reject(new QualifiedQuiescenceError("proof-limit"));
          }
        });
        child.once("error", () => {
          clearTimeout(timeout);
          reject(unavailable());
        });
        child.once("close", (code) => {
          clearTimeout(timeout);
          if (stderrSize !== 0 || (code !== 0 && !(code === 1 && stdout === "")))
            reject(unavailable());
          else resolve(stdout);
        });
      });
      return parseDarwinWriterOutput(output, input);
    },
  };
}

export function nativeQualifiedQuiescenceAdapter(platform: string): QualifiedQuiescenceAdapter {
  if (platform === "linux") return linuxQualifiedQuiescenceAdapter();
  if (platform === "darwin") return darwinQualifiedQuiescenceAdapter();
  return {
    scan: async () => {
      throw unavailable();
    },
  };
}

export async function proveQualifiedStateQuiescence(input: {
  readonly baseDir: string;
  readonly allowedProcessIds?: readonly number[];
  readonly adapter?: QualifiedQuiescenceAdapter;
}): Promise<void> {
  try {
    const baseDir = await NodeFSP.realpath(input.baseDir);
    const userdata = NodePath.join(baseDir, "userdata");
    const uid = (await NodeFSP.lstat(userdata)).uid;
    if (process.getuid?.() !== uid || (await NodeFSP.realpath(userdata)) !== userdata)
      throw unavailable();
    const paths = [
      "statev2.sqlite",
      "statev2.sqlite-wal",
      "statev2.sqlite-shm",
      ...QUALIFIED_STARTUP_STATE_FILES,
    ].map((name) => NodePath.join(userdata, name));
    const snapshot = async () =>
      Promise.all(
        paths.map(async (path) => {
          try {
            const stat = await NodeFSP.lstat(path, { bigint: true });
            if (!stat.isFile() || stat.uid !== BigInt(uid)) throw unavailable();
            return {
              path,
              device: stat.dev,
              inode: stat.ino,
              identity: `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`,
            };
          } catch (cause) {
            if (isGone(cause) && path !== paths[0]) return undefined;
            throw cause;
          }
        }),
      );
    const before = await snapshot();
    // oxlint-disable-next-line t3code/no-global-process-runtime -- Native adapter selection is confined to the detached launcher boundary and injectable in tests.
    const adapter = input.adapter ?? nativeQualifiedQuiescenceAdapter(process.platform);
    const writers = await adapter.scan({
      files: before.filter((file) => file !== undefined),
      uid,
      allowedProcessIds: input.allowedProcessIds ?? [],
    });
    if (writers.length !== 0) throw new QualifiedQuiescenceError("writer-active");
    const after = await snapshot();
    if (before.some((file, index) => file?.identity !== after[index]?.identity))
      throw new QualifiedQuiescenceError("state-identity-changed");
  } catch (cause) {
    if (cause instanceof QualifiedQuiescenceError) throw cause;
    throw unavailable();
  }
}
