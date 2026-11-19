// Manual diagnostic only. Keep the existing procfs reader and move its I/O to another process.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as HostProcess from "../../../packages/shared/src/HostProcess.ts";

function checkPids(pids, owner, helper) {
  if (
    !Array.isArray(pids) ||
    pids.length < 1 ||
    pids.length > 4 ||
    pids[0] !== owner ||
    new Set(pids).size !== pids.length ||
    pids.some((pid) => !Number.isInteger(pid) || pid < 1 || pid > 2_147_483_647 || pid === helper)
  ) {
    throw new Error("Invalid captured observer PID set");
  }
}

async function deadline(promise, ms) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error("Linux resource observer timed out")), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

export async function startLinuxResourceObserver({
  modulePath = import.meta.url,
  timeoutMs = 2_000,
} = {}) {
  const child = NodeChildProcess.fork(new URL(modulePath), [], {
    execPath: process.execPath,
    execArgv: [],
    stdio: ["ignore", "ignore", "ignore", "ipc"],
    env: { PATH: "/usr/bin:/bin" },
  });
  let pending, failure, closing;
  let sampleId = 0;
  const observer = { pid: child.pid, samples: 0, cpuMs: 0, maxRssBytes: 0, maxThreads: 0 };
  const fail = (cause) => {
    failure ??= cause;
    pending?.reject(failure);
  };
  const exited = new Promise((resolve) => {
    // With no stdio pipes, the captured exit is the lifecycle proof. Node may
    // omit close after the parent disconnects an IPC-only child.
    child.once("exit", (code, signal) => {
      fail(new Error("Linux resource observer exited"));
      resolve({ capturedExit: Number.isInteger(child.pid), code, signal });
    });
    child.once("error", () => {
      if (child.pid === undefined) resolve({ capturedExit: false, code: null, signal: null });
    });
  });
  child.on("error", fail);
  child.on("disconnect", () => fail(new Error("Linux resource observer disconnected")));
  child.on("message", (message) => {
    if (!pending || !message || message.id !== pending.id) {
      fail(new Error("Unexpected Linux resource observer reply"));
      return;
    }
    const request = pending;
    pending = undefined;
    request.resolve(message);
  });
  const close = () =>
    (closing ??= (async () => {
      if (child.connected) child.disconnect();
      try {
        return await deadline(exited, timeoutMs);
      } catch {
        child.kill("SIGKILL");
        return deadline(exited, timeoutMs).catch(() => ({
          capturedExit: false,
          code: null,
          signal: null,
        }));
      }
    })());
  async function exchange(id, pids) {
    if (failure) throw failure;
    if (closing || pending) throw new Error("Linux resource observer is busy or closed");
    pending = { id, ...Promise.withResolvers() };
    const result = deadline(pending.promise, timeoutMs);
    if (pids) {
      try {
        child.send({ id, pids }, (error) => {
          if (error) fail(error);
        });
      } catch (cause) {
        fail(cause);
      }
    }
    try {
      return await result;
    } catch (cause) {
      fail(cause);
      await close();
      throw cause;
    } finally {
      pending = undefined;
    }
  }
  try {
    if ((await exchange(0)).ready !== true) throw new Error("Linux resource observer is not ready");
  } catch (cause) {
    const exit = await close();
    throw new Error(
      "Linux resource observer could not start (captured exit: " + exit.capturedExit + ")",
      {
        cause,
      },
    );
  }
  async function collect(pids) {
    checkPids(pids, process.pid, child.pid);
    try {
      const { sample, usage } = await exchange(++sampleId, pids);
      if (
        !sample ||
        JSON.stringify(sample.requestedPids) !== JSON.stringify(pids) ||
        !Array.isArray(sample.processes) ||
        !Array.isArray(sample.errors) ||
        !Array.isArray(sample.missingPids) ||
        !["complete", "partial", "unavailable"].includes(sample.status) ||
        !usage ||
        ![usage.cpuMs, usage.maxRssBytes, usage.threads].every((n) => Number.isFinite(n) && n >= 0)
      ) {
        throw new Error("Invalid Linux resource observer reply");
      }
      observer.samples++;
      observer.cpuMs = Math.max(observer.cpuMs, usage.cpuMs);
      observer.maxRssBytes = Math.max(observer.maxRssBytes, usage.maxRssBytes);
      observer.maxThreads = Math.max(observer.maxThreads, usage.threads);
      return sample;
    } catch (cause) {
      fail(cause);
      await close();
      throw cause;
    }
  }
  return {
    sample(pids, _diagnostic, trackRead) {
      const result = collect(pids);
      trackRead?.(result);
      return result;
    },
    close,
    observer: () => ({ ...observer }),
  };
}

if (import.meta.main) {
  if (HostProcess.Platform.defaultValue() !== "linux" || !process.send) {
    throw new Error("Linux resource observer requires a captured IPC parent on Linux");
  }
  const owner = process.ppid;
  process.once("disconnect", () => process.exit());
  const { sampleProcesses } =
    await import("../../../apps/server/src/jones/serverParallelism/resourceSamples.ts");
  let active = false;
  process.on("message", async ({ id, pids }) => {
    if (active || !Number.isSafeInteger(id) || id <= 0) process.exit(1);
    active = true;
    try {
      checkPids(pids, owner, process.pid);
      const sample = await sampleProcesses(pids);
      const { userCPUTime, systemCPUTime, maxRSS } = process.resourceUsage();
      const threads = (await NodeFSP.readdir("/proc/self/task")).length;
      active = false;
      process.send(
        {
          id,
          sample,
          usage: {
            cpuMs: (userCPUTime + systemCPUTime) / 1_000,
            maxRssBytes: maxRSS * 1_024,
            threads,
          },
        },
        (error) => {
          if (error) process.exit(1);
        },
      );
    } catch {
      process.exit(1);
    }
  });
  process.send({ id: 0, ready: true }, (error) => {
    if (error) process.exit(1);
  });
}
