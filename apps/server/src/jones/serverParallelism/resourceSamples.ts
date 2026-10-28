// @effect-diagnostics nodeBuiltinImport:off -- Qualification-only OS observations of task-captured processes.
import { readFile, readdir } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
const execute = promisify(execFile);
let clockTicks: Promise<number> | undefined;
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
  return (Number(days) * 86_400 + clock!.split(":").map(Number).reduce((total, part) => total * 60 + part, 0)) * 1000;
};
const valid = (sample: ProcessResources) => Number.isSafeInteger(sample.pid) && sample.pid > 0 &&
  Number.isFinite(sample.rssBytes) && sample.rssBytes > 0 && Number.isFinite(sample.cpuMs) && sample.cpuMs >= 0;

/** A missing process or unreadable field is unknown, never exit/reaping proof. */
export function processResourceSample(pids: readonly number[], rows: readonly ProcessResources[], errors: readonly string[] = []): ProcessResourceSample {
  const requestedPids = [...new Set(pids)];
  const processes = rows.filter((row) => requestedPids.includes(row.pid) && valid(row));
  const missingPids = requestedPids.filter((pid) => processes.filter((row) => row.pid === pid).length !== 1);
  return { requestedPids, processes, missingPids, errors,
    status: requestedPids.length === 0 || processes.length === 0 ? "unavailable" :
      missingPids.length || errors.length ? "partial" : "complete" };
}

export type QualificationPredicate = boolean | "unknown";
export function resourcePredicates(samples: readonly ProcessResourceSample[], parentPid: number, count: 2 | 3) {
  const complete = samples.length > 0 && samples.every((sample) => sample.status === "complete");
  const children = samples.flatMap((sample) => sample.processes.filter((row) => row.pid !== parentPid));
  const overBudget = children.some((row) => row.rssBytes > 512 * 1024 * 1024) || samples.some((sample) =>
    sample.processes.filter((row) => row.pid !== parentPid).reduce((total, row) => total + row.rssBytes, 0) > count * 512 * 1024 * 1024);
  return { resourcesComplete: complete,
    childRssWithinBudget: overBudget ? false : complete && children.length > 0 ? true : "unknown" as QualificationPredicate };
}

/** Per-PID failures stay in the result so later successful samples cannot erase them. */
export async function sampleProcesses(pids: readonly number[]): Promise<ProcessResourceSample> {
  if (!pids.every((pid) => Number.isSafeInteger(pid) && pid > 0)) throw new Error("Invalid captured PID");
  if (pids.length === 0) return processResourceSample(pids, []);
  const rows: ProcessResources[] = [], errors: string[] = [];
  try {
    if (process.platform === "linux") {
      const hz = await (clockTicks ??= execute("getconf", ["CLK_TCK"], { timeout: 2_000 })
        .then((result) => Number(result.stdout.trim())));
      if (!Number.isFinite(hz) || hz <= 0) throw new Error("Unavailable OS clock frequency");
      for (const pid of pids) {
        try {
          const [text, status, tasks, descriptors] = await Promise.all([
            readFile(`/proc/${pid}/stat`, "utf8"), readFile(`/proc/${pid}/status`, "utf8"),
            readdir(`/proc/${pid}/task`), readdir(`/proc/${pid}/fd`),
          ]);
          const fields = text.slice(text.lastIndexOf(")") + 2).split(" ");
          let inotifyDescriptors = 0, inotifyWatches = 0, watchesKnown = true;
          for (const descriptor of descriptors) {
            try {
              const info = await readFile(`/proc/${pid}/fdinfo/${descriptor}`, "utf8");
              if (info.includes("inotify")) inotifyDescriptors++;
              inotifyWatches += [...info.matchAll(/^inotify wd:/gm)].length;
            } catch { watchesKnown = false; }
          }
          const rss = /^VmRSS:\s+(\d+)/m.exec(status)?.[1];
          rows.push({ pid, cpuMs: ((Number(fields[11]) + Number(fields[12])) * 1000) / hz,
            rssBytes: rss === undefined ? NaN : Number(rss) * 1024, threads: tasks.length,
            ...(watchesKnown ? { inotifyDescriptors, inotifyWatches } : {}) });
        } catch { errors.push(`pid:${pid}:unavailable`); }
      }
    } else if (process.platform === "darwin") {
      const result = await execute("ps", ["-o", "pid=,time=,rss=", "-p", pids.join(",")], { timeout: 2_000 });
      for (const line of result.stdout.trim().split("\n").filter(Boolean)) {
        const [pid, time, rss] = line.trim().split(/\s+/);
        let threads: number | undefined;
        try {
          const result = await execute("ps", ["-M", "-p", pid!, "-o", "pid="], { timeout: 2_000 });
          const lines = result.stdout.trim().split("\n").filter(Boolean);
          if (lines.length && lines.every((value) => Number(value.trim()) === Number(pid))) threads = lines.length;
        } catch { /* The platform field remains absent, never zero. */ }
        rows.push({ pid: Number(pid), cpuMs: cpuTime(time ?? ""), rssBytes: Number(rss) * 1024, ...(threads === undefined ? {} : { threads }) });
      }
    } else errors.push("unsupported-platform");
  } catch { errors.push("collector-unavailable"); }
  return processResourceSample(pids, rows, errors);
}
