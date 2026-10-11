// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalTimers:off
// Launcher client for the root-owned observer in qualifiedWriterObserver.pl.
// The readiness commit allows exactly the captured trial writer, which a kernel
// lease cannot express, and same-user /proc descriptors are unreadable for
// non-dumpable processes. The helper reads that metadata as root through one
// exact NOPASSWD command. This client binds its answer to the launcher's own
// state snapshot and excludes only an allowed PID whose process birth (start
// time) matches, read before and after the helper; a reused PID has a different
// birth. Node reaps children asynchronously, so the launcher separately checks
// after the proof that its captured child is still live.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { runCapturedProcess } from "./qualifiedLinuxLease.ts";

export const QUALIFIED_WRITER_OBSERVER_PATH =
  "/usr/local/libexec/jones-code/jones-qualified-writer-observer";
/** The helper's fixed protocol order; tests keep it equal to the launcher snapshot. */
export const QUALIFIED_WRITER_OBSERVER_FILES = [
  "statev2.sqlite",
  "statev2.sqlite-wal",
  "statev2.sqlite-shm",
  "settings.json",
  "keybindings.json",
  "server-runtime.json",
  "environment-id",
  "anonymous-id",
] as const;
const HEADER = "jones-qualified-writer-observer 1";
const OUTPUT_LIMIT = 64 * 1024;
// The helper stops itself at 10 seconds; this also bounds sudo. Both sit inside
// the server's 30 second launcher exchange, which the launcher fences separately.
const DEADLINE_MS = 15_000;

interface ObservedStateFile {
  readonly path: string;
  readonly device: bigint;
  readonly inode: bigint;
}

export type WriterObserverResult =
  | {
      readonly status: "complete";
      readonly files: readonly { index: number; device: bigint; inode: bigint }[];
      readonly writers: readonly {
        index: number;
        pid: number;
        birth: string;
        access: "write" | "read-write" | "shared-map";
      }[];
    }
  | { readonly status: "incomplete" | "rejected"; readonly reason: string };

export type QualifiedWriterObservation =
  | { readonly status: "complete"; readonly writers: readonly { pid: number; path: string }[] }
  /** The helper cannot raise privileges here; only the unprivileged scan may answer. */
  | { readonly status: "unprivileged"; readonly reason: "not-installed" | "no-new-privileges" }
  | { readonly status: "unavailable" | "identity-changed" };

/** Returns undefined unless the output is exactly one well-formed helper result. */
export function parseWriterObserverResult(result: {
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly stdout: string;
  readonly stderr: string;
}): WriterObserverResult | undefined {
  if (result.signal !== null || result.stderr !== "") return undefined;
  const lines = result.stdout.split("\n");
  if (lines.pop() !== "" || lines.shift() !== HEADER) return undefined;
  const terminal = lines.pop() ?? "";
  const files: { index: number; device: bigint; inode: bigint }[] = [];
  const writers: Extract<WriterObserverResult, { status: "complete" }>["writers"][number][] = [];
  const seen = new Set<string>();
  for (const line of lines) {
    const file = /^file ([0-7]) (\d{1,20}) (\d{1,20})$/.exec(line);
    if (file !== null) {
      const [, index = "", device = "", inode = ""] = file;
      if (writers.length > 0 || Number(index) <= (files[files.length - 1]?.index ?? -1))
        return undefined;
      files.push({ index: Number(index), device: BigInt(device), inode: BigInt(inode) });
      continue;
    }
    const writer = /^writer ([0-7]) ([1-9]\d{0,9}) (\d{1,20}) (write|read-write|shared-map)$/.exec(
      line,
    );
    if (writer === null || seen.has(line)) return undefined;
    const [, index = "", pid = "", birth = "", access = ""] = writer;
    if (!files.some((entry) => entry.index === Number(index))) return undefined;
    seen.add(line);
    writers.push({
      index: Number(index),
      pid: Number(pid),
      birth,
      access: access as "write" | "read-write" | "shared-map",
    });
  }
  if (/^complete \d+$/.test(terminal))
    return result.code === 0 ? { status: "complete", files, writers } : undefined;
  const failed = /^(incomplete|rejected) ([a-z-]+)$/.exec(terminal);
  if (failed === null || writers.length > 0) return undefined;
  const [, status = "", reason = ""] = failed;
  if (result.code !== (status === "rejected" ? 2 : 3)) return undefined;
  return { status: status as "incomplete" | "rejected", reason };
}

/** Binds a helper result to the launcher snapshot and the allowed PID+birth pairs. */
export function evaluateWriterObservation(
  result: WriterObserverResult,
  files: readonly ObservedStateFile[],
  allowed: ReadonlyMap<number, string>,
): QualifiedWriterObservation {
  if (result.status !== "complete") return { status: "unavailable" };
  if (result.files.length !== files.length) return { status: "identity-changed" };
  const selected = new Map<number, ObservedStateFile>();
  for (const file of files) {
    const index = (QUALIFIED_WRITER_OBSERVER_FILES as readonly string[]).indexOf(
      NodePath.basename(file.path),
    );
    const observed = result.files.find((entry) => entry.index === index);
    if (observed === undefined || observed.device !== file.device || observed.inode !== file.inode)
      return { status: "identity-changed" };
    selected.set(index, file);
  }
  const writers: { pid: number; path: string }[] = [];
  for (const writer of result.writers) {
    if (allowed.get(writer.pid) === writer.birth) continue;
    const file = selected.get(writer.index);
    if (file === undefined) return { status: "identity-changed" };
    writers.push({ pid: writer.pid, path: file.path });
  }
  return { status: "complete", writers };
}

/**
 * sudo runs this file as root, so it and every directory up to `/` must be real
 * (no symlink), root-owned and not group- or other-writable: anyone able to
 * rename an ancestor could substitute the helper. The fixed path runs through
 * /usr, which is a real directory under the merged-/usr layout; only /bin,
 * /sbin and /lib are symlinks there.
 */
async function helperInstallation(path: string): Promise<"installed" | "absent" | "untrusted"> {
  if (!NodePath.isAbsolute(path) || NodePath.normalize(path) !== path) return "untrusted";
  const lstat = (entry: string) =>
    NodeFSP.lstat(entry).catch((cause: unknown) =>
      (cause as NodeJS.ErrnoException)?.code === "ENOENT" ? ("absent" as const) : undefined,
    );
  const trusted = (stat: Awaited<ReturnType<typeof lstat>>, kind: "file" | "directory") =>
    typeof stat === "object" &&
    (kind === "file" ? stat.isFile() : stat.isDirectory()) &&
    stat.uid === 0 &&
    (stat.mode & 0o022) === 0;
  const helper = await lstat(path);
  if (helper === "absent") return "absent";
  if (!trusted(helper, "file")) return "untrusted";
  for (let directory = NodePath.dirname(path); ; directory = NodePath.dirname(directory)) {
    if (!trusted(await lstat(directory), "directory")) return "untrusted";
    if (directory === NodePath.dirname(directory)) return "installed";
  }
}

async function processBirth(procRoot: string, pid: number): Promise<string | undefined> {
  const stat = await NodeFSP.readFile(NodePath.join(procRoot, String(pid), "stat"), "utf8").catch(
    () => undefined,
  );
  const end = stat?.lastIndexOf(")") ?? -1;
  const value = end < 0 ? undefined : stat?.slice(end + 2).split(/\s+/)[19];
  return value !== undefined && /^\d+$/.test(value) ? value : undefined;
}

async function runHelper(
  sudoPath: string,
  helperPath: string,
  base: string,
  deadlineMs: number,
): Promise<WriterObserverResult | undefined> {
  // Only the captured sudo child is signalled. In its own process group sudo
  // relays SIGTERM to the helper, which installs no handler, so the signal also
  // ends killable filesystem waits; SIGKILL follows only after a grace period.
  const result = await runCapturedProcess({
    command: sudoPath,
    args: ["-n", "--", helperPath, "--base", base],
    deadlineMs,
    outputLimit: OUTPUT_LIMIT,
    newProcessGroup: true,
  });
  return result.outcome === "exited" ? parseWriterObserverResult(result) : undefined;
}

export async function observeQualifiedWriters(
  input: {
    readonly files: readonly ObservedStateFile[];
    readonly allowedProcessIds: readonly number[];
  },
  options: {
    readonly helperPath?: string;
    readonly sudoPath?: string;
    readonly procRoot?: string;
    readonly deadlineMs?: number;
  } = {},
): Promise<QualifiedWriterObservation> {
  const helperPath = options.helperPath ?? QUALIFIED_WRITER_OBSERVER_PATH;
  const procRoot = options.procRoot ?? "/proc";
  const installation = await helperInstallation(helperPath);
  if (installation === "absent") return { status: "unprivileged", reason: "not-installed" };
  if (installation === "untrusted") return { status: "unavailable" };
  const status = await NodeFSP.readFile(NodePath.join(procRoot, "self", "status"), "utf8").catch(
    () => undefined,
  );
  const noNewPrivileges = /^NoNewPrivs:\s+([01])$/m.exec(status ?? "")?.[1];
  if (noNewPrivileges === undefined) return { status: "unavailable" };
  if (noNewPrivileges === "1") return { status: "unprivileged", reason: "no-new-privileges" };
  const database = input.files[0]?.path;
  const userdata = database === undefined ? undefined : NodePath.dirname(database);
  if (
    userdata === undefined ||
    NodePath.basename(userdata) !== "userdata" ||
    input.files.some(
      (file) =>
        NodePath.dirname(file.path) !== userdata ||
        !(QUALIFIED_WRITER_OBSERVER_FILES as readonly string[]).includes(
          NodePath.basename(file.path),
        ),
    )
  )
    return { status: "unavailable" };
  const allowed = new Map<number, string>();
  for (const pid of input.allowedProcessIds) {
    const birth = await processBirth(procRoot, pid);
    if (birth === undefined) return { status: "unavailable" };
    allowed.set(pid, birth);
  }
  const result = await runHelper(
    options.sudoPath ?? "/usr/bin/sudo",
    helperPath,
    NodePath.dirname(userdata),
    options.deadlineMs ?? DEADLINE_MS,
  );
  if (result === undefined) return { status: "unavailable" };
  for (const [pid, birth] of allowed)
    if ((await processBirth(procRoot, pid)) !== birth) return { status: "unavailable" };
  return evaluateWriterObservation(result, input.files, allowed);
}
