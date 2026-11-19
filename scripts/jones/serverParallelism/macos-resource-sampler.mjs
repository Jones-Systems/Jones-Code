import * as NodeChildProcess from "node:child_process";
import { processResourceSample } from "../../../apps/server/src/jones/serverParallelism/resourceSamples.ts";

const deadlineMs = 2_000;
const maxReply = 512;
const maxPid = 2_147_483_647;

async function withDeadline(pending) {
  let timer;
  try {
    return await Promise.race([
      pending,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error("macOS resource sampler timed out")), deadlineMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function parseReply(text, pids) {
  const [tag, ...fields] = text.split(" ");
  if (tag !== "rows" || fields.length !== pids.length) throw new Error("Invalid sampler reply");
  return fields.map((field, index) => {
    const [pid, cpu, rss, threads, seconds, micros, ...rest] = field.split(":");
    if (pid !== String(pids[index]) || rest.length) throw new Error("Invalid sampler PID");
    if (cpu === "unavailable" && rss === undefined) return null;
    const raw = [cpu, rss, threads, seconds, micros];
    const [cpuNs, rssBytes, threadCount, startSec, startUsec] = raw.map(Number);
    if (
      !raw.every((value) => /^(?:0|[1-9]\d*)$/.test(value ?? "")) ||
      ![cpuNs, rssBytes, threadCount, startSec, startUsec].every(Number.isSafeInteger) ||
      rssBytes <= 0 ||
      threadCount <= 0 ||
      startSec <= 0 ||
      startUsec >= 1_000_000
    )
      throw new Error("Invalid sampler row");
    return {
      birth: seconds + ":" + micros,
      resources: { pid: pids[index], cpuMs: cpuNs / 1_000_000, rssBytes, threads: threadCount },
    };
  });
}

export async function startMacResourceSampler(executable, args = []) {
  const child = NodeChildProcess.spawn(executable, args, {
    stdio: ["pipe", "pipe", "ignore"],
    env: {},
  });
  let failure, waiting, closePending;
  let buffer = "";
  const births = new Map();
  const observer = { pid: child.pid, samples: 0, maxRssBytes: 0, maxThreads: 0, cpuMs: 0 };
  const fail = (cause) => {
    failure ??= cause;
    waiting?.reject(failure);
  };
  const exited = new Promise((resolve) => {
    child.once("close", (code, signal) => {
      fail(new Error("macOS resource sampler closed"));
      resolve({ capturedExit: true, code, signal });
    });
  });
  child.on("error", fail);
  child.stdin.on("error", fail);
  child.stdout.on("error", fail);
  child.stdout.setEncoding("ascii");
  child.stdout.on("data", (chunk) => {
    if (failure) return;
    buffer += chunk;
    if (!waiting || buffer.length > maxReply) {
      fail(new Error("Unexpected sampler output"));
      return;
    }
    const end = buffer.indexOf("\n");
    if (end === -1) return;
    if (end !== buffer.length - 1) {
      fail(new Error("Multiple sampler replies"));
      return;
    }
    const frame = buffer.slice(0, end);
    buffer = "";
    const pending = waiting;
    waiting = undefined;
    pending.resolve(frame);
  });
  const exchange = async (request) => {
    if (failure) throw failure;
    if (waiting) throw new Error("Overlapping macOS sampler requests");
    waiting = Promise.withResolvers();
    const pending = withDeadline(waiting.promise);
    if (request !== undefined) {
      try {
        child.stdin.write(request);
      } catch (cause) {
        fail(cause);
      }
    }
    try {
      return await pending;
    } catch (cause) {
      fail(cause);
      throw cause;
    } finally {
      waiting = undefined;
    }
  };
  const close = () =>
    (closePending ??= (async () => {
      child.stdin.end();
      try {
        return await withDeadline(exited);
      } catch {
        child.kill("SIGKILL");
        return withDeadline(exited).catch(() => ({
          capturedExit: false,
          code: null,
          signal: null,
        }));
      }
    })());

  try {
    if ((await exchange()) !== "ready") throw new Error("Missing sampler handshake");
  } catch (cause) {
    const exit = await close();
    throw new Error(
      "macOS resource sampler failed to start (exit captured: " + exit.capturedExit + ")",
      { cause },
    );
  }

  const collect = async (pids) => {
    if (
      pids.length === 0 ||
      pids.length > 4 ||
      pids[0] !== process.pid ||
      new Set(pids).size !== pids.length ||
      pids.some((pid) => !Number.isInteger(pid) || pid < 1 || pid > maxPid || pid === child.pid)
    )
      throw new Error("Invalid captured PID set");
    const rows = parseReply(await exchange(pids.join(" ") + "\n"), [...pids, child.pid]);
    const errors = [];
    const observed = rows.pop();
    if (!observed) errors.push("macos-sampler-self-unavailable");
    for (const row of [...rows, observed]) {
      if (!row) continue;
      const pid = row.resources.pid;
      if (births.has(pid) && births.get(pid) !== row.birth)
        throw new Error("macOS sampler process identity changed");
      births.set(pid, row.birth);
    }
    if (observed) {
      observer.samples++;
      observer.maxRssBytes = Math.max(observer.maxRssBytes, observed.resources.rssBytes);
      observer.maxThreads = Math.max(observer.maxThreads, observed.resources.threads);
      observer.cpuMs = Math.max(observer.cpuMs, observed.resources.cpuMs);
    }
    return processResourceSample(
      pids,
      rows.flatMap((row) => (row ? [row.resources] : [])),
      errors,
    );
  };

  return {
    sample(pids, diagnostic, trackRead) {
      const pending = collect(pids);
      trackRead?.(pending);
      diagnostic?.trackPending?.(pending);
      return pending;
    },
    close,
    observer: () => ({ method: "macos-proc-info", ...observer }),
  };
}
