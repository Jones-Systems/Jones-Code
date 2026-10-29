// @effect-diagnostics nodeBuiltinImport:off -- Qualification-only OS observations of task-captured processes.
import { readFile, readdir, stat, mkdtemp, rm } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { tmpdir } from "node:os";
import { join } from "node:path";
const execute = promisify(execFile);
let clockTicks: Promise<number> | undefined;
export interface NativePoolObservation {
  readonly pid: number;
  readonly method: "linux-proc-task-comm" | "macos-sample";
  readonly status: "complete" | "unavailable";
  readonly threads: readonly { readonly tid: string; readonly name: string }[];
  readonly raw: string;
  readonly error?: string;
}
export interface MacPerformanceCores {
  readonly source: "sysctl:hw.perflevel0.physicalcpu";
  readonly status: "observed" | "unavailable";
  readonly value: number | null;
  readonly raw: string;
}
export async function sampleMacPerformanceCores(): Promise<MacPerformanceCores> {
  const source = "sysctl:hw.perflevel0.physicalcpu" as const;
  if (process.platform !== "darwin")
    return { source, status: "unavailable", value: null, raw: "not-macos" };
  try {
    const { stdout } = await execute("/usr/sbin/sysctl", ["-n", "hw.perflevel0.physicalcpu"], {
      timeout: 2_000,
      maxBuffer: 4096,
    });
    const value = /^\s*[1-9][0-9]*\s*$/.test(stdout) ? Number(stdout.trim()) : NaN;
    return {
      source,
      status: Number.isSafeInteger(value) ? "observed" : "unavailable",
      value: Number.isSafeInteger(value) ? value : null,
      raw: stdout,
    };
  } catch {
    return { source, status: "unavailable", value: null, raw: "sysctl-unavailable" };
  }
}

/** Only named OS threads from one captured PID; no host-wide process discovery. */
export function parseMacNativeThreads(pid: number, raw: string): NativePoolObservation {
  const unavailable = (): NativePoolObservation => ({
    pid,
    method: "macos-sample",
    status: "unavailable",
    threads: [],
    raw,
    error: "unattributed-or-malformed-sample",
  });
  if (
    !Number.isSafeInteger(pid) ||
    pid <= 0 ||
    raw.length > 4 * 1024 * 1024 ||
    !new RegExp(`^Process:\\s+.+\\[${pid}\\]\\s*$`, "m").test(raw)
  )
    return unavailable();
  // sample's call-graph thread headers carry stable OS thread IDs, unlike stack frame rows.
  const threads = [...raw.matchAll(/^\s*\d+ Thread_(0x[0-9a-f]+|[0-9]+):?\s+(.+)$/gim)].map(
    (match) => ({ tid: match[1]!, name: match[2]!.trim() }),
  );
  if (threads.length === 0 || new Set(threads.map((row) => row.tid)).size !== threads.length)
    return unavailable();
  // Unnamed main/runtime threads remain in the raw receipt; only exact names attest native pools.
  return { pid, method: "macos-sample", status: "complete", threads, raw };
}

export async function sampleNativePools(pid: number): Promise<NativePoolObservation> {
  if (!Number.isSafeInteger(pid) || pid <= 0) throw new Error("Invalid captured PID");
  const method = process.platform === "linux" ? "linux-proc-task-comm" : "macos-sample";
  let scratch: string | undefined;
  try {
    if (process.platform === "linux") {
      const before = await readFile(`/proc/${pid}/stat`, "utf8");
      const tids = await readdir(`/proc/${pid}/task`);
      const threads = await Promise.all(
        tids.map(async (tid) => {
          if (!/^[1-9][0-9]*$/.test(tid)) throw new Error("Invalid task ID");
          return { tid, name: (await readFile(`/proc/${pid}/task/${tid}/comm`, "utf8")).trimEnd() };
        }),
      );
      const after = await readFile(`/proc/${pid}/stat`, "utf8");
      const start = (stat: string) => stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19];
      if (!start(before) || start(before) !== start(after)) throw new Error("PID identity changed");
      return {
        pid,
        method,
        status: "complete",
        threads,
        raw: JSON.stringify({ pid, start: start(before), threads }),
      };
    }
    if (process.platform !== "darwin") throw new Error("Unsupported OS");
    scratch = await mkdtemp(join(tmpdir(), "jones-native-pools-"));
    const output = join(scratch, "sample.txt");
    // Bounded, nonprivileged, exact captured PID. Failure never supplies a physical-CPU fallback.
    await execute("/usr/bin/sample", [String(pid), "1", "10", "-file", output], {
      timeout: 3_000,
      maxBuffer: 64 * 1024,
    });
    if ((await stat(output)).size > 4 * 1024 * 1024) throw new Error("Sample output exceeds bound");
    const raw = await readFile(output, "utf8");
    return parseMacNativeThreads(pid, raw);
  } catch {
    return {
      pid,
      method,
      status: "unavailable",
      threads: [],
      raw: "",
      error: "native-pool-observer-unavailable",
    };
  } finally {
    if (scratch) await rm(scratch, { recursive: true });
  }
}
export interface ProcessResources {
  readonly pid: number;
  readonly rssBytes: number;
  readonly cpuMs: number;
  readonly threads?: number;
  readonly inotifyDescriptors?: number;
  readonly inotifyWatches?: number;
}
export interface ProcessResourceSample {
  readonly requestedPids: readonly number[];
  readonly processes: readonly ProcessResources[];
  readonly missingPids: readonly number[];
  readonly status: "complete" | "partial" | "unavailable";
  readonly errors: readonly string[];
}
const cpuTime = (value: string) => {
  if (!/^(?:\d+-)?\d+(?::\d{2}){1,2}(?:\.\d+)?$/.test(value)) return NaN;
  const [days, clock] = value.includes("-") ? value.split("-") : ["0", value];
  return (
    (Number(days) * 86_400 +
      clock!
        .split(":")
        .map(Number)
        .reduce((total, part) => total * 60 + part, 0)) *
    1000
  );
};
const valid = (sample: ProcessResources) =>
  Number.isSafeInteger(sample.pid) &&
  sample.pid > 0 &&
  Number.isFinite(sample.rssBytes) &&
  sample.rssBytes > 0 &&
  Number.isFinite(sample.cpuMs) &&
  sample.cpuMs >= 0;

/** A missing process or unreadable field is unknown, never exit/reaping proof. */
export function processResourceSample(
  pids: readonly number[],
  rows: readonly ProcessResources[],
  errors: readonly string[] = [],
): ProcessResourceSample {
  const requestedPids = [...new Set(pids)];
  const processes = rows.filter((row) => requestedPids.includes(row.pid) && valid(row));
  const missingPids = requestedPids.filter(
    (pid) => processes.filter((row) => row.pid === pid).length !== 1,
  );
  return {
    requestedPids,
    processes,
    missingPids,
    errors,
    status:
      requestedPids.length === 0 || processes.length === 0
        ? "unavailable"
        : missingPids.length || errors.length
          ? "partial"
          : "complete",
  };
}

export type QualificationPredicate = boolean | "unknown";
export function resourcePredicates(
  samples: readonly ProcessResourceSample[],
  parentPid: number,
  count: 2 | 3,
) {
  const complete = samples.length > 0 && samples.every((sample) => sample.status === "complete");
  const children = samples.flatMap((sample) =>
    sample.processes.filter((row) => row.pid !== parentPid),
  );
  const overBudget =
    children.some((row) => row.rssBytes > 512 * 1024 * 1024) ||
    samples.some(
      (sample) =>
        sample.processes
          .filter((row) => row.pid !== parentPid)
          .reduce((total, row) => total + row.rssBytes, 0) >
        count * 512 * 1024 * 1024,
    );
  return {
    resourcesComplete: complete,
    childRssWithinBudget: overBudget
      ? false
      : complete && children.length > 0
        ? true
        : ("unknown" as QualificationPredicate),
  };
}

/** Per-PID failures stay in the result so later successful samples cannot erase them. */
export async function sampleProcesses(pids: readonly number[]): Promise<ProcessResourceSample> {
  if (!pids.every((pid) => Number.isSafeInteger(pid) && pid > 0))
    throw new Error("Invalid captured PID");
  if (pids.length === 0) return processResourceSample(pids, []);
  const rows: ProcessResources[] = [],
    errors: string[] = [];
  try {
    if (process.platform === "linux") {
      const hz = await (clockTicks ??= execute("getconf", ["CLK_TCK"], { timeout: 2_000 }).then(
        (result) => Number(result.stdout.trim()),
      ));
      if (!Number.isFinite(hz) || hz <= 0) throw new Error("Unavailable OS clock frequency");
      for (const pid of pids) {
        try {
          const [text, status, tasks, descriptors] = await Promise.all([
            readFile(`/proc/${pid}/stat`, "utf8"),
            readFile(`/proc/${pid}/status`, "utf8"),
            readdir(`/proc/${pid}/task`),
            readdir(`/proc/${pid}/fd`),
          ]);
          const fields = text.slice(text.lastIndexOf(")") + 2).split(" ");
          let inotifyDescriptors = 0,
            inotifyWatches = 0,
            watchesKnown = true;
          for (const descriptor of descriptors) {
            try {
              const info = await readFile(`/proc/${pid}/fdinfo/${descriptor}`, "utf8");
              if (info.includes("inotify")) inotifyDescriptors++;
              inotifyWatches += [...info.matchAll(/^inotify wd:/gm)].length;
            } catch {
              watchesKnown = false;
            }
          }
          const rss = /^VmRSS:\s+(\d+)/m.exec(status)?.[1];
          rows.push({
            pid,
            cpuMs: ((Number(fields[11]) + Number(fields[12])) * 1000) / hz,
            rssBytes: rss === undefined ? NaN : Number(rss) * 1024,
            threads: tasks.length,
            ...(watchesKnown ? { inotifyDescriptors, inotifyWatches } : {}),
          });
        } catch {
          errors.push(`pid:${pid}:unavailable`);
        }
      }
    } else if (process.platform === "darwin") {
      const result = await execute("ps", ["-o", "pid=,time=,rss=", "-p", pids.join(",")], {
        timeout: 2_000,
      });
      for (const line of result.stdout.trim().split("\n").filter(Boolean)) {
        const [pid, time, rss] = line.trim().split(/\s+/);
        let threads: number | undefined;
        try {
          const result = await execute("ps", ["-M", "-p", pid!, "-o", "pid="], { timeout: 2_000 });
          const lines = result.stdout.trim().split("\n").filter(Boolean);
          if (lines.length && lines.every((value) => Number(value.trim()) === Number(pid)))
            threads = lines.length;
        } catch {
          /* The platform field remains absent, never zero. */
        }
        rows.push({
          pid: Number(pid),
          cpuMs: cpuTime(time ?? ""),
          rssBytes: Number(rss) * 1024,
          ...(threads === undefined ? {} : { threads }),
        });
      }
    } else errors.push("unsupported-platform");
  } catch {
    errors.push("collector-unavailable");
  }
  return processResourceSample(pids, rows, errors);
}
