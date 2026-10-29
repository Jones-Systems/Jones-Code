// @effect-diagnostics nodeBuiltinImport:off -- Qualification-only OS observations of task-captured processes.
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeFSP from "node:fs/promises";
import * as NodeChildProcess from "node:child_process";
import * as NodeUtil from "node:util";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { diagnosticError, type DiagnosticIdentity, type DiagnosticSink, type ResourceStage } from "./diagnosticObservation.ts";
import { observeSafely } from "./observations.ts";

export interface ResourceDiagnostics {
  readonly sink: DiagnosticSink;
  readonly readId: number;
  readonly phase: string;
  readonly identities?: ReadonlyMap<number, DiagnosticIdentity>;
  readonly trackPending?: (pending: Promise<unknown>) => void;
}
function resourceEmitter(diagnostic: ResourceDiagnostics | undefined, pid?: number) {
  const sink = observeSafely(diagnostic?.sink);
  return (stage: ResourceStage, edge: "begin" | "end" | "error", detail: {
    attempted?: number; completed?: number; errors?: number; errno?: string; raw?: string;
    startTimeTicks?: string | undefined; clockTicks?: number;
  } = {}) => {
    if (diagnostic) sink({
      kind: "resource", atMs: performance.now(), phase: diagnostic.phase,
      readId: diagnostic.readId, ...(pid === undefined ? {} : { pid, ...diagnostic.identities?.get(pid) }),
      stage, edge, ...detail,
    });
  };
}
const statStart = (text: string) => text.slice(text.lastIndexOf(")") + 2).split(" ")[19];
const execute = NodeUtil.promisify(NodeChildProcess.execFile);
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
  if (HostProcess.Platform.defaultValue() !== "darwin")
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
  const method =
    HostProcess.Platform.defaultValue() === "linux" ? "linux-proc-task-comm" : "macos-sample";
  let scratch: string | undefined;
  try {
    if (HostProcess.Platform.defaultValue() === "linux") {
      const before = await NodeFSP.readFile(`/proc/${pid}/stat`, "utf8");
      const tids = await NodeFSP.readdir(`/proc/${pid}/task`);
      const threads = await Promise.all(
        tids.map(async (tid) => {
          if (!/^[1-9][0-9]*$/.test(tid)) throw new Error("Invalid task ID");
          return {
            tid,
            name: (await NodeFSP.readFile(`/proc/${pid}/task/${tid}/comm`, "utf8")).trimEnd(),
          };
        }),
      );
      const after = await NodeFSP.readFile(`/proc/${pid}/stat`, "utf8");
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
    if (HostProcess.Platform.defaultValue() !== "darwin") throw new Error("Unsupported OS");
    scratch = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-native-pools-"));
    const output = NodePath.join(scratch, "sample.txt");
    // Bounded, nonprivileged, exact captured PID. Failure never supplies a physical-CPU fallback.
    await execute("/usr/bin/sample", [String(pid), "1", "10", "-file", output], {
      timeout: 3_000,
      maxBuffer: 64 * 1024,
    });
    if ((await NodeFSP.stat(output)).size > 4 * 1024 * 1024)
      throw new Error("Sample output exceeds bound");
    const raw = await NodeFSP.readFile(output, "utf8");
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
    if (scratch) await NodeFSP.rm(scratch, { recursive: true });
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
export async function sampleProcesses(
  pids: readonly number[],
  diagnostic?: ResourceDiagnostics,
): Promise<ProcessResourceSample> {
  if (!pids.every((pid) => Number.isSafeInteger(pid) && pid > 0))
    throw new Error("Invalid captured PID");
  if (pids.length === 0) return processResourceSample(pids, []);
  const rows: ProcessResources[] = [],
    errors: string[] = [];
  try {
    if (HostProcess.Platform.defaultValue() === "linux") {
      const clockEvent = resourceEmitter(diagnostic);
      clockEvent("clock-ticks", "begin", { attempted: 1 });
      const hz = await (clockTicks ??= execute("getconf", ["CLK_TCK"], { timeout: 2_000 }).then(
        (result) => Number(result.stdout.trim()),
      ));
      if (!Number.isFinite(hz) || hz <= 0) throw new Error("Unavailable OS clock frequency");
      clockEvent("clock-ticks", "end", { attempted: 1, completed: 1, errors: 0, clockTicks: hz });
      for (const pid of pids) {
        const emit = resourceEmitter(diagnostic, pid);
        const counts = { attempted: 0, completed: 0, errors: 0 };
        const read = <T>(stage: ResourceStage, run: () => Promise<T>): Promise<T> => {
          if (!diagnostic) return run();
          counts.attempted++;
          emit(stage, "begin", { attempted: 1 });
          const pending = run().then((value) => {
            counts.completed++;
            emit(stage, "end", {
              attempted: 1, completed: 1, errors: 0,
              ...(stage === "stat" && typeof value === "string"
                ? { startTimeTicks: statStart(value), clockTicks: hz, raw: value.slice(0, 2_048) } : {}),
            });
            return value;
          }, (cause) => {
            counts.errors++;
            emit(stage, "error", { attempted: 1, completed: 0, errors: 1, ...diagnosticError(cause) });
            throw cause;
          });
          observeSafely(diagnostic.trackPending)(pending);
          return pending;
        };
        emit("pid", "begin");
        try {
          const [text, status, tasks, descriptors] = await Promise.all([
            read("stat", () => NodeFSP.readFile(`/proc/${pid}/stat`, "utf8")),
            read("status", () => NodeFSP.readFile(`/proc/${pid}/status`, "utf8")),
            read("task", () => NodeFSP.readdir(`/proc/${pid}/task`)),
            read("fd", () => NodeFSP.readdir(`/proc/${pid}/fd`)),
          ]);
          const fields = text.slice(text.lastIndexOf(")") + 2).split(" ");
          let inotifyDescriptors = 0,
            inotifyWatches = 0,
            watchesKnown = true;
          let completed = 0, errors = 0;
          emit("fdinfo", "begin", { attempted: descriptors.length });
          for (const descriptor of descriptors) {
            counts.attempted++;
            try {
              const info = await NodeFSP.readFile(`/proc/${pid}/fdinfo/${descriptor}`, "utf8");
              completed++;
              counts.completed++;
              if (info.includes("inotify")) inotifyDescriptors++;
              inotifyWatches += [...info.matchAll(/^inotify wd:/gm)].length;
            } catch (cause) {
              errors++;
              counts.errors++;
              emit("fdinfo", "error", { attempted: 1, completed: 0, errors: 1, ...diagnosticError(cause) });
              watchesKnown = false;
            }
          }
          emit("fdinfo", "end", { attempted: descriptors.length, completed, errors });
          const rss = /^VmRSS:\s+(\d+)/m.exec(status)?.[1];
          rows.push({
            pid,
            cpuMs: ((Number(fields[11]) + Number(fields[12])) * 1000) / hz,
            rssBytes: rss === undefined ? NaN : Number(rss) * 1024,
            threads: tasks.length,
            ...(watchesKnown ? { inotifyDescriptors, inotifyWatches } : {}),
          });
        } catch (cause) {
          emit("pid", "error", { ...counts, ...diagnosticError(cause) });
          errors.push(`pid:${pid}:unavailable`);
        } finally {
          emit("pid", "end", counts);
        }
      }
    } else if (HostProcess.Platform.defaultValue() === "darwin") {
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
  } catch (cause) {
    resourceEmitter(diagnostic)("clock-ticks", "error", { errors: 1, ...diagnosticError(cause) });
    errors.push("collector-unavailable");
  }
  return processResourceSample(pids, rows, errors);
}


export interface CpuStatReceipt {
  readonly mode: "cpu-stat-only";
  readonly eligible: false;
  readonly requestedPids: readonly number[];
  readonly processes: readonly {
    readonly pid: number;
    readonly cpuMs: number;
    readonly startTimeTicks: string;
    readonly clockTicks: number;
  }[];
  readonly missingPids: readonly number[];
  readonly errors: readonly string[];
  readonly unavailable: readonly ["rssBytes", "threads", "inotifyDescriptors", "inotifyWatches"];
}

/** CPU-only evidence has its own shape; it cannot masquerade as a resource qualification. */
export async function sampleCpuStats(pids: readonly number[], diagnostic?: ResourceDiagnostics): Promise<CpuStatReceipt> {
  if (!pids.every((pid) => Number.isSafeInteger(pid) && pid > 0)) throw new Error("Invalid captured PID");
  const processes: CpuStatReceipt["processes"][number][] = [], errors: string[] = [];
  const clockEvent = resourceEmitter(diagnostic);
  try {
    if (HostProcess.Platform.defaultValue() !== "linux") throw new Error("CPU-stat diagnostic requires Linux");
    clockEvent("clock-ticks", "begin", { attempted: 1 });
    const hz = await (clockTicks ??= execute("getconf", ["CLK_TCK"], { timeout: 2_000 }).then((result) => Number(result.stdout.trim())));
    if (!Number.isFinite(hz) || hz <= 0) throw new Error("Unavailable OS clock frequency");
    clockEvent("clock-ticks", "end", { attempted: 1, completed: 1, errors: 0, clockTicks: hz });
    for (const pid of pids) {
      const emit = resourceEmitter(diagnostic, pid);
      emit("pid", "begin");
      emit("stat", "begin", { attempted: 1 });
      try {
        const text = await NodeFSP.readFile(`/proc/${pid}/stat`, "utf8");
        const fields = text.slice(text.lastIndexOf(")") + 2).split(" ");
        const startTimeTicks = statStart(text), cpuMs = ((Number(fields[11]) + Number(fields[12])) * 1000) / hz;
        emit("stat", "end", { attempted: 1, completed: 1, errors: 0, startTimeTicks, clockTicks: hz, raw: text.slice(0, 2_048) });
        if (!startTimeTicks || !/^[0-9]+$/.test(startTimeTicks) || !Number.isFinite(cpuMs) || cpuMs < 0)
          throw new Error("Malformed CPU stat");
        processes.push({ pid, cpuMs, startTimeTicks, clockTicks: hz });
      } catch (cause) {
        emit("stat", "error", { errors: 1, ...diagnosticError(cause) });
        errors.push(`pid:${pid}:unavailable`);
      } finally { emit("pid", "end"); }
    }
  } catch (cause) {
    clockEvent("clock-ticks", "error", { errors: 1, ...diagnosticError(cause) });
    errors.push("collector-unavailable");
  }
  return {
    mode: "cpu-stat-only", eligible: false, requestedPids: [...pids], processes,
    missingPids: pids.filter((pid) => processes.filter((row) => row.pid === pid).length !== 1), errors,
    unavailable: ["rssBytes", "threads", "inotifyDescriptors", "inotifyWatches"],
  };
}
