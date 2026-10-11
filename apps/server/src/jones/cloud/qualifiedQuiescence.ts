// @effect-diagnostics nodeBuiltinImport:off
// Native observation at the detached launcher boundary. With no allowed writer,
// Linux uses a kernel-wide lease proof. The readiness commit allows its
// captured trial, so Linux uses the root-owned writer observer, or the
// same-user descriptor scan where that helper cannot run; both fail closed on
// processes they cannot read. Process names are not evidence that a state
// inode is idle.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { leaseGrantedOnEveryDevice, observeLinuxStateLeases } from "./qualifiedLinuxLease.ts";
import { observeQualifiedWriters } from "./qualifiedWriterObserver.ts";

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
/** `pid` is absent when the kernel proves a write-open inode without naming its holder. */
export interface QualifiedWriter {
  readonly pid?: number;
  readonly path: string;
}
export interface QualifiedQuiescenceAdapter {
  scan(input: QualifiedWriterScan): Promise<readonly QualifiedWriter[]>;
  /** Install admission: throws unless a readiness commit with these writers allowed can prove. */
  assertReadinessObservable?(input: QualifiedWriterScan): Promise<void>;
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
    /** Retained for caller compatibility; unrelated process count never gates an update. */
    readonly maximumProcesses?: number;
    /** Retained for caller compatibility; unrelated descriptors never gate an update. */
    readonly maximumDescriptors?: number;
    /** Retained for caller compatibility; elapsed whole-host time never gates an update. */
    readonly now?: () => number;
    readonly processNames?: () => Promise<readonly string[]>;
  } = {},
): QualifiedQuiescenceAdapter {
  const procRoot = options.procRoot ?? "/proc";
  const processNames = options.processNames ?? (() => NodeFSP.readdir(procRoot));
  return {
    async scan(input) {
      const writers: { pid: number; path: string }[] = [];
      const observed = new Set<string>();
      // A second discovery checks new processes for exact-inode writers without
      // holding an update merely because unrelated processes appeared.
      for (let discovery = 0; discovery < 2; discovery++) {
        const names = (await processNames()).filter((name) => /^\d+$/.test(name));
        for (const name of names) {
          if (observed.has(name)) continue;
          observed.add(name);
          const directory = NodePath.join(procRoot, name);
          const pid = Number(name);
          try {
            if ((await NodeFSP.stat(directory)).uid !== input.uid) continue;
            if (input.allowedProcessIds.includes(pid)) continue;
            const status = await boundedRead(NodePath.join(directory, "status"), 64 * 1024);
            const effectiveUid = /^Uid:\s+\d+\s+(\d+)/m.exec(status)?.[1];
            if (effectiveUid === undefined) throw unavailable();
            if (Number(effectiveUid) !== input.uid) continue;
            const birth = processBirth(
              await boundedRead(NodePath.join(directory, "stat"), 16 * 1024),
            );
            const fds = (await NodeFSP.readdir(NodePath.join(directory, "fd"))).filter((fd) =>
              /^\d+$/.test(fd),
            );
            let selectedInodeObserved = false;
            for (const fd of fds) {
              try {
                const stat = await NodeFSP.stat(NodePath.join(directory, "fd", fd), {
                  bigint: true,
                });
                const target = input.files.find(
                  (file) => file.device === stat.dev && file.inode === stat.ino,
                );
                if (target === undefined) continue;
                selectedInodeObserved = true;
                const info = await boundedRead(NodePath.join(directory, "fdinfo", fd), 8 * 1024);
                const flags = /^flags:\s+([0-7]+)$/m.exec(info)?.[1];
                if (flags === undefined) throw unavailable();
                if ((Number.parseInt(flags, 8) & 3) !== 0) writers.push({ pid, path: target.path });
              } catch (cause) {
                if (!isGone(cause)) throw cause;
                // A closed descriptor is harmless; an existing scoped descriptor
                // with unavailable access flags leaves the writer proof incomplete.
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
              if (target !== undefined) {
                selectedInodeObserved = true;
                writers.push({ pid, path: target.path });
              }
            }
            if (
              selectedInodeObserved &&
              processBirth(await boundedRead(NodePath.join(directory, "stat"), 16 * 1024)) !== birth
            )
              throw unavailable();
          } catch (cause) {
            if (!isGone(cause)) throw cause;
            // Missing evidence is harmless only when the observed process vanished.
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
          }
        }
      }
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

export function parseDarwinWriterResult(
  result: { readonly code: number | null; readonly stdout: string; readonly stderr: string },
  input: QualifiedWriterScan,
) {
  if (result.code !== 0 && !(result.code === 1 && result.stdout === "")) throw unavailable();
  const diagnostics = result.stderr.split("\n").filter((line) => line.trim() !== "");
  // lsof can warn about unrelated mounts even when every exact-file result is
  // usable. Permission, argument and selected-file errors still block the proof.
  let filesystemWarning = false;
  for (const line of diagnostics) {
    const mount = /^lsof: WARNING: can't stat\(\) .+ file system (.+)$/.exec(line)?.[1];
    if (mount !== undefined) {
      if (input.files.some((file) => file.path === mount || file.path.startsWith(`${mount}/`)))
        throw unavailable();
      filesystemWarning = true;
    } else if (!filesystemWarning || !/^\s+Output information may be incomplete\.$/.test(line)) {
      throw unavailable();
    }
  }
  return parseDarwinWriterOutput(result.stdout, input);
}

function darwinQualifiedQuiescenceAdapter(): QualifiedQuiescenceAdapter {
  return {
    async scan(input) {
      if (input.files.length === 0) return [];
      const output = await new Promise<{ code: number | null; stdout: string; stderr: string }>(
        (resolve, reject) => {
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
          let stderr = "";
          child.stdout.on("data", (data: Buffer) => {
            stdout += data.toString("utf8");
            if (Buffer.byteLength(stdout) > 1024 * 1024) {
              child.kill("SIGKILL");
              reject(new QualifiedQuiescenceError("proof-limit"));
            }
          });
          child.stderr.on("data", (data: Buffer) => {
            stderr += data.toString("utf8");
            if (Buffer.byteLength(stderr) > 16 * 1024) {
              child.kill("SIGKILL");
              reject(new QualifiedQuiescenceError("proof-limit"));
            }
          });
          child.once("error", () => {
            reject(unavailable());
          });
          child.once("close", (code) => {
            resolve({ code, stdout, stderr });
          });
        },
      );
      return parseDarwinWriterResult(output, input);
    },
  };
}

/** Kernel-wide proof that no process holds a selected inode open for writing. */
export function linuxLeaseQualifiedQuiescenceAdapter(
  options: Parameters<typeof observeLinuxStateLeases>[1] = {},
): QualifiedQuiescenceAdapter {
  return {
    async scan(input) {
      // A lease cannot tell an allowed writer from any other writer.
      if (input.allowedProcessIds.length !== 0) throw unavailable();
      const observed = await observeLinuxStateLeases(input.files, options);
      switch (observed.status) {
        case "complete":
          return input.files
            .filter((_, index) => observed.busy.includes(index))
            .map((file) => ({ path: file.path }));
        case "identity-changed":
          throw new QualifiedQuiescenceError("state-identity-changed");
        case "proof-limit":
          throw new QualifiedQuiescenceError("proof-limit");
        default:
          throw unavailable();
      }
    },
  };
}

/**
 * Readiness observation that allows captured writers. A root-owned helper
 * observes every process; without a usable helper the same-user descriptor
 * scan answers and fails closed on processes it cannot read.
 */
export function linuxReadinessQualifiedQuiescenceAdapter(
  options: Parameters<typeof observeQualifiedWriters>[1] & {
    readonly fallback?: QualifiedQuiescenceAdapter;
  } = {},
): QualifiedQuiescenceAdapter {
  const { fallback = linuxQualifiedQuiescenceAdapter(), ...observer } = options;
  return {
    async scan(input) {
      const observed = await observeQualifiedWriters(input, observer);
      switch (observed.status) {
        case "complete":
          return observed.writers;
        case "unprivileged":
          return fallback.scan(input);
        case "identity-changed":
          throw new QualifiedQuiescenceError("state-identity-changed");
        default:
          throw unavailable();
      }
    },
  };
}

/**
 * statfs f_type values (include/uapi/linux/magic.h) of local kernel
 * filesystems that take leases through the generic VFS path
 * (kernel_setlease → generic_setlease), where a grant means no write-open
 * description exists. Network filesystems answer leases from per-file
 * delegations or oplocks, and FUSE and overlay add another layer, so those
 * and every type not listed here are refused.
 */
const LEASE_PROOF_FILESYSTEMS: ReadonlySet<bigint> = new Set([
  0xef53n, // ext2, ext3, ext4
  0x58465342n, // XFS
  0x9123683en, // Btrfs
  0x01021994n, // tmpfs
]);

/** statfs f_type for one path; injectable for portable fixtures. */
export type FilesystemTypeReader = (path: string) => Promise<bigint>;

const statfsType: FilesystemTypeReader = async (path) =>
  (await NodeFSP.statfs(path, { bigint: true })).type;

/**
 * Refuses unless every selected file is on a filesystem whose lease grant is a
 * kernel proof. Metadata only, on each file rather than its directory, so a
 * network or FUSE bind mount over one state file is refused as well. A failed
 * read is refused like an unlisted type. Run it before any lease probe.
 */
export async function assertLeaseProofFilesystems(
  files: readonly { readonly path: string }[],
  filesystemType: FilesystemTypeReader = statfsType,
): Promise<void> {
  for (const file of files) {
    const type = await filesystemType(file.path).catch(() => undefined);
    if (type === undefined || !LEASE_PROOF_FILESYSTEMS.has(type)) throw unavailable();
  }
}

/**
 * Linux routing: proofs with no allowed writer use kernel leases, the readiness
 * commit uses the readiness observer. Install admission exercises both before
 * acceptance, because the post-stop, restart and rollback proofs all need
 * leases on these exact files.
 */
export function linuxNativeQualifiedQuiescenceAdapter(
  options: {
    readonly leases?: Parameters<typeof observeLinuxStateLeases>[1];
    readonly readiness?: Parameters<typeof linuxReadinessQualifiedQuiescenceAdapter>[0];
    readonly filesystemType?: FilesystemTypeReader;
  } = {},
): QualifiedQuiescenceAdapter {
  const leases = linuxLeaseQualifiedQuiescenceAdapter(options.leases);
  const readiness = linuxReadinessQualifiedQuiescenceAdapter(options.readiness);
  return {
    scan: (input) => (input.allowedProcessIds.length === 0 ? leases : readiness).scan(input),
    async assertReadinessObservable(input) {
      await assertLeaseProofFilesystems(input.files, options.filesystemType);
      // The requesting server holds the state open for writing, so busy files
      // are expected here. Later lease proofs can answer only where a lease on
      // this exact snapshot was actually granted: busy alone does not show
      // support, so every selected device needs one non-busy grant. A server
      // holding every selected file on a device open for writing is refused
      // here and keeps running.
      const support = await observeLinuxStateLeases(input.files, options.leases);
      if (support.status === "identity-changed")
        throw new QualifiedQuiescenceError("state-identity-changed");
      if (support.status === "proof-limit") throw new QualifiedQuiescenceError("proof-limit");
      if (support.status !== "complete" || !leaseGrantedOnEveryDevice(input.files, support.busy))
        throw unavailable();
      if ((await readiness.scan(input)).length !== 0)
        throw new QualifiedQuiescenceError("writer-active");
    },
  };
}

export function nativeQualifiedQuiescenceAdapter(platform: string): QualifiedQuiescenceAdapter {
  if (platform === "linux") return linuxNativeQualifiedQuiescenceAdapter();
  if (platform === "darwin") return darwinQualifiedQuiescenceAdapter();
  return {
    scan: async () => {
      throw unavailable();
    },
    assertReadinessObservable: async () => {
      throw unavailable();
    },
  };
}

async function selectedQualifiedState(baseDirInput: string) {
  const baseDir = await NodeFSP.realpath(baseDirInput);
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
  return { uid, snapshot };
}

// oxlint-disable-next-line t3code/no-global-process-runtime -- Native adapter selection is confined to the detached launcher boundary and injectable in tests.
const nativeAdapter = () => nativeQualifiedQuiescenceAdapter(process.platform);

/**
 * Install admission: refuses a transaction whose readiness commit cannot prove
 * that the captured writers alone hold the selected state. Adapters without a
 * readiness check (injected fixtures, Darwin) admit unchanged.
 */
export async function assertQualifiedReadinessObservable(input: {
  readonly baseDir: string;
  readonly allowedProcessIds: readonly number[];
  readonly adapter?: QualifiedQuiescenceAdapter;
}): Promise<void> {
  try {
    const adapter = input.adapter ?? nativeAdapter();
    if (adapter.assertReadinessObservable === undefined) return;
    const state = await selectedQualifiedState(input.baseDir);
    await adapter.assertReadinessObservable({
      files: (await state.snapshot()).filter((file) => file !== undefined),
      uid: state.uid,
      allowedProcessIds: input.allowedProcessIds,
    });
  } catch (cause) {
    if (cause instanceof QualifiedQuiescenceError) throw cause;
    throw unavailable();
  }
}

export async function proveQualifiedStateQuiescence(input: {
  readonly baseDir: string;
  readonly allowedProcessIds?: readonly number[];
  readonly adapter?: QualifiedQuiescenceAdapter;
}): Promise<void> {
  try {
    const { uid, snapshot } = await selectedQualifiedState(input.baseDir);
    const before = await snapshot();
    const adapter = input.adapter ?? nativeAdapter();
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
