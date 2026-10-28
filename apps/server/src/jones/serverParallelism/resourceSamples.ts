// @effect-diagnostics nodeBuiltinImport:off -- Qualification-only OS observations of task-captured processes.
import { readFile, readdir } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
const execute = promisify(execFile);
export interface ProcessResources {
  readonly pid: number;
  readonly rssBytes: number;
  readonly cpuMs: number;
  readonly threads?: number;
  readonly inotifyDescriptors?: number;
  readonly inotifyWatches?: number;
}
const cpuTime = (value: string) => {
  const parts = value.split(":").map(Number);
  return parts.reduce((total, part) => total * 60 + part, 0) * 1000;
};
/** A missing process is a missing sample, never exit/reaping proof. */
export async function sampleProcesses(pids: readonly number[]): Promise<ProcessResources[]> {
  if (!pids.every((pid) => Number.isSafeInteger(pid) && pid > 0))
    throw new Error("Invalid captured PID");
  if (pids.length === 0) return [];
  if (process.platform === "linux") {
    const hz = Number((await execute("getconf", ["CLK_TCK"], { timeout: 2_000 })).stdout.trim());
    if (!Number.isFinite(hz) || hz <= 0) throw new Error("Unavailable OS clock frequency");
    return (
      await Promise.all(
        pids.map(async (pid) => {
          try {
            const [text, status, tasks, descriptors] = await Promise.all([
              readFile(`/proc/${pid}/stat`, "utf8"),
              readFile(`/proc/${pid}/status`, "utf8"),
              readdir(`/proc/${pid}/task`),
              readdir(`/proc/${pid}/fd`),
            ]);
            const fields = text.slice(text.lastIndexOf(")") + 2).split(" ");
            let inotifyDescriptors = 0,
              inotifyWatches = 0;
            for (const descriptor of descriptors) {
              const info = await readFile(`/proc/${pid}/fdinfo/${descriptor}`, "utf8").catch(
                () => "",
              );
              if (info.includes("inotify")) inotifyDescriptors++;
              inotifyWatches += [...info.matchAll(/^inotify wd:/gm)].length;
            }
            return {
              pid,
              cpuMs: ((Number(fields[11]) + Number(fields[12])) * 1000) / hz,
              rssBytes: Number(/^VmRSS:\s+(\d+)/m.exec(status)?.[1] ?? 0) * 1024,
              threads: tasks.length,
              inotifyDescriptors,
              inotifyWatches,
            };
          } catch {
            return undefined;
          }
        }),
      )
    ).filter((sample) => sample !== undefined);
  }
  if (process.platform === "darwin") {
    const result = await execute("ps", ["-o", "pid=,time=,rss=", "-p", pids.join(",")], {
      timeout: 2_000,
    });
    return result.stdout
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((line) => {
        const [pid, time, rss] = line.trim().split(/\s+/);
        return { pid: Number(pid), cpuMs: cpuTime(time!), rssBytes: Number(rss) * 1024 };
      });
  }
  throw new Error("Unsupported process resource observation");
}
