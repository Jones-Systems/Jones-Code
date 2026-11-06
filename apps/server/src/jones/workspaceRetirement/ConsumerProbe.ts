// @effect-diagnostics nodeBuiltinImport:off -- Bounded same-user native probes; argv is transient and never returned.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";
import * as DateTime from "effect/DateTime";
import { inside, nativePlatform, readBounded } from "./Filesystem.ts";

const CONSUMER_LIMITS = [
  "other-user/root processes",
  "remote consumers",
  "unmanaged writers and tools without claims or reservations",
  "general open-file handles are not scanned",
  "lifecycle bindings to other Git directories are outside the participant guard",
] as const;
export interface Probe {
  observedAt: number;
  status: "complete" | "partial" | "unavailable";
  paths: readonly string[];
  limits: readonly string[];
}
export interface ConsumerEvidence {
  native: Probe;
  services: Probe;
  registry: Probe;
  workloads: Probe;
  obligations: Probe;
  process: Probe;
}
export function consumerDecision(
  target: string,
  evidence: ConsumerEvidence,
  now = DateTime.toEpochMillis(DateTime.nowUnsafe()),
) {
  const holds: string[] = [],
    unknown: string[] = [],
    coverage: Record<
      string,
      { status: string; ageMs: number; limits: readonly string[]; reason: string | null }
    > = {};
  for (const [name, probe] of Object.entries(evidence)) {
    const ageMs = now - probe.observedAt;
    coverage[name] = {
      status: probe.status,
      ageMs,
      limits: probe.limits,
      reason:
        !Number.isFinite(ageMs) || ageMs < 0 || ageMs > 120000
          ? "stale_or_invalid_observation"
          : probe.status !== "complete"
            ? "incomplete_" + name + "_coverage"
            : null,
    };
    if (probe.paths.some((p: string) => inside(p, target)))
      holds.push(name === "obligations" ? "obligation" : "active_consumer");
    if (probe.status !== "complete" || !Number.isFinite(ageMs) || ageMs < 0 || ageMs > 120000)
      unknown.push("consumer_evidence_unavailable");
  }
  return {
    holds: [...new Set(holds)],
    unknown: [...new Set(unknown)],
    coverage,
    limits: CONSUMER_LIMITS,
  };
}
function command(file: string, args: string[], timeout = 1500): Promise<string> {
  return new Promise((resolve, reject) =>
    NodeChildProcess.execFile(
      file,
      args,
      { timeout, maxBuffer: 2 * 1024 * 1024, encoding: "utf8" },
      (error, stdout) => (error ? reject(error) : resolve(stdout)),
    ),
  );
}
function argvUses(bytes: string, target: string) {
  return bytes
    .split(/[\0\s"']+/)
    .some(
      (arg) =>
        arg === target ||
        arg.startsWith(target + NodePath.sep) ||
        arg.includes("=" + target + NodePath.sep) ||
        arg.endsWith("=" + target),
    );
}
async function procText(path: string, limit: number) {
  const file = await NodeFSP.open(path, "r");
  try {
    const bytes = Buffer.alloc(limit + 1);
    let offset = 0;
    while (offset < bytes.length) {
      const read = await file.read(bytes, offset, bytes.length - offset, null);
      if (!read.bytesRead) break;
      offset += read.bytesRead;
    }
    if (offset > limit) throw new Error("bound_exceeded");
    return bytes.subarray(0, offset).toString();
  } finally {
    await file.close();
  }
}
export interface ProcessProbeIO {
  command: (file: string, args: string[]) => Promise<string>;
  list: (path: string) => Promise<string[]>;
  uid: () => number | undefined;
  statUid: (path: string) => Promise<number>;
  readlink: (path: string) => Promise<string>;
  read: (path: string, limit: number) => Promise<string>;
}
const nativeIO: ProcessProbeIO = {
  command,
  list: (path) => NodeFSP.readdir(path),
  uid: () => process.getuid?.(),
  statUid: async (path) => (await NodeFSP.stat(path)).uid,
  readlink: (path) => NodeFSP.readlink(path),
  read: procText,
};
export async function processProbe(
  target: string,
  platform: NodeJS.Platform = nativePlatform,
  io: ProcessProbeIO = nativeIO,
): Promise<Probe> {
  const started = performance.now();
  const observedAt = DateTime.toEpochMillis(DateTime.nowUnsafe()),
    paths = new Set<string>();
  let complete = true;
  try {
    if (platform === "linux") {
      const pids = (await io.list("/proc")).filter((p) => /^\d+$/.test(p));
      if (pids.length > 4096) throw new Error("bound");
      for (const pid of pids) {
        if (performance.now() - started > 5000) {
          complete = false;
          break;
        }
        const base = "/proc/" + pid;
        try {
          if ((await io.statUid(base)) !== io.uid()) continue;
          for (const name of ["cwd", "exe"]) {
            try {
              const p = await io.readlink(base + "/" + name);
              if (inside(p, target)) paths.add(p);
            } catch (cause) {
              if ((cause as NodeJS.ErrnoException).code !== "ENOENT") complete = false;
            }
          }
          const argv = await io.read(base + "/cmdline", 1024 * 1024);
          if (argv.length > 1024 * 1024) {
            complete = false;
            continue;
          }
          if (argvUses(argv, target)) paths.add(target);
          const maps = await io.read(base + "/maps", 2 * 1024 * 1024);
          if (maps.length > 2 * 1024 * 1024) {
            complete = false;
            continue;
          }
          for (const line of maps.split("\n"))
            if (/^[\da-f]+-[\da-f]+\s+\S*x\S*\s/.test(line)) {
              const p = line.match(/\s(\/.*)$/)?.[1];
              if (p && inside(p, target)) paths.add(p);
            }
        } catch (cause) {
          if (
            (cause as NodeJS.ErrnoException).code !== "ENOENT" &&
            (cause as NodeJS.ErrnoException).code !== "ESRCH"
          )
            complete = false;
        }
      }
    } else if (platform === "darwin") {
      const rows = (await io.command("/bin/ps", ["-axo", "pid=,uid="])).trim().split("\n");
      const pids = rows
        .map((r) => r.trim().split(/\s+/))
        .filter((r) => Number(r[1]) === io.uid())
        .map((r) => r[0]!);
      if (pids.length > 4096) throw new Error("bound");
      for (let i = 0; i < pids.length; i += 64) {
        if (performance.now() - started > 5000) {
          complete = false;
          break;
        }
        const batch = pids.slice(i, i + 64).join(",");
        try {
          const output = await io.command("/usr/sbin/lsof", [
            "-a",
            "-p",
            batch,
            "-d",
            "cwd,txt",
            "-Fn",
          ]);
          for (const line of output.split("\n"))
            if (line.startsWith("n") && inside(line.slice(1), target)) paths.add(line.slice(1));
        } catch {
          complete = false;
        }
        try {
          if (argvUses(await io.command("/bin/ps", ["-ww", "-p", batch, "-o", "command="]), target))
            paths.add(target);
        } catch {
          complete = false;
        }
      }
    } else
      return {
        observedAt,
        status: "unavailable",
        paths: [],
        limits: [...CONSUMER_LIMITS, "unsupported platform"],
      };
    return {
      observedAt,
      status: complete ? "complete" : "partial",
      paths: [...paths],
      limits: CONSUMER_LIMITS,
    };
  } catch {
    return { observedAt, status: "unavailable", paths: [...paths], limits: CONSUMER_LIMITS };
  }
}
export async function serviceProbe(target: string, files: readonly string[]): Promise<Probe> {
  const observedAt = DateTime.toEpochMillis(DateTime.nowUnsafe());
  const paths: string[] = [];
  try {
    if (files.length > 2000) throw new Error("bound");
    for (const file of files)
      if (argvUses((await readBounded(file, 1024 * 1024)).toString(), target)) paths.push(target);
    // Exact supplied files are only partial coverage of loaded user/system services.
    return {
      observedAt,
      status: "partial",
      paths,
      limits: ["supplied service definitions only; loaded units require an owning adapter"],
    };
  } catch {
    return {
      observedAt,
      status: "unavailable",
      paths,
      limits: ["service definitions unreadable or bounded"],
    };
  }
}
