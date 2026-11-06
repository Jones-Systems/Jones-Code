// @effect-diagnostics nodeBuiltinImport:off -- Metadata-only observation of the exact owned PTY; deadline termination drains its worker.
import * as NodeChildProcess from "node:child_process";
import * as NodeWorkerThreads from "node:worker_threads";
import * as NodePerfHooks from "node:perf_hooks";
import * as NodeTimersPromises from "node:timers/promises";
import * as Context from "effect/Context";
import { nativePlatform } from "./Filesystem.ts";
import type { DutyIdentity } from "./DutyIdentity.ts";

// Only metadata is read in this owned worker. There are no child processes,
// hashes, file contents or optional provenance reads to outlive termination.
const PROGRAM = String.raw`
const { parentPort, workerData } = require('node:worker_threads');
const fs = require('node:fs/promises');
const path = require('node:path');
(async () => {
  const { pid, physical, suppliedCwd } = workerData;
  const cwd = suppliedCwd ?? await fs.readlink('/proc/' + pid + '/cwd');
  if (!path.isAbsolute(cwd) || cwd.includes('\0')) return null;
  const root = physical.binding.path;
  const [stat, real] = await Promise.all([fs.lstat(root, {bigint:true}), fs.realpath(cwd)]);
  if (!stat.isDirectory() || stat.isSymbolicLink() || String(stat.dev)!==physical.binding.dev || String(stat.ino)!==physical.binding.ino) return null;
  return real===root || real.startsWith(root+path.sep) ? real : null;
})().then(value=>parentPort.postMessage(value),()=>parentPort.postMessage(null));
`;
const observing = new Set<number>();
async function observe(pid: number, physical: DutyIdentity | undefined): Promise<string | null> {
  if (!Number.isSafeInteger(pid) || pid <= 0 || physical?.status !== "bound" || observing.has(pid))
    return null;
  observing.add(pid);
  const started = NodePerfHooks.performance.now();
  try {
    let suppliedCwd: string | undefined;
    if (nativePlatform === "darwin") {
      // execFile's timeout owns this exact lsof child; its callback waits for
      // close/drained output before any filesystem worker is admitted.
      const output = await new Promise<string>((resolve, reject) =>
        NodeChildProcess.execFile(
          "/usr/sbin/lsof",
          ["-a", "-p", String(pid), "-d", "cwd", "-Fn"],
          { timeout: 80, maxBuffer: 8192, encoding: "utf8" },
          (error, result) => (error ? reject(error) : resolve(result)),
        ),
      );
      const paths = output
        .split("\n")
        .filter((line) => line.startsWith("n"))
        .map((line) => line.slice(1));
      if (paths.length !== 1) return null;
      suppliedCwd = paths[0]!;
    } else if (nativePlatform !== "linux") return null;
    const remaining = 100 - (NodePerfHooks.performance.now() - started);
    if (remaining <= 0) return null;
    const worker = new NodeWorkerThreads.Worker(PROGRAM, {
      eval: true,
      execArgv: [],
      workerData: { pid, physical, suppliedCwd },
    });
    const deadline = new AbortController();
    return await new Promise<string | null>((resolve) => {
      let settled = false;
      const finish = async (value: unknown) => {
        if (settled) return;
        settled = true;
        deadline.abort();
        // A timeout cannot leave an uncancellable filesystem race/backlog.
        try {
          await worker.terminate();
        } catch {
          // Already failed/closed is unknown.
        } finally {
          worker.removeAllListeners();
          resolve(typeof value === "string" ? value : null);
        }
      };
      worker.once("message", (value) => void finish(value));
      worker.once("error", () => void finish(null));
      worker.once("exit", () => void finish(null));
      void NodeTimersPromises.setTimeout(remaining, undefined, { signal: deadline.signal, ref: false }).then(
        () => void finish(null),
        () => {
          if (!deadline.signal.aborted) void finish(null);
        },
      );
    });
  } catch {
    return null;
  } finally {
    observing.delete(pid);
  }
}
export class TerminalCwdObservation extends Context.Reference<{
  readonly observe: typeof observe;
}>("jones/workspaceRetirement/TerminalCwdObservation", { defaultValue: () => ({ observe }) }) {}
