// @effect-diagnostics nodeBuiltinImport:off - preinstall code uses Node built-ins only.
import { spawn } from "node:child_process";
import { cpus } from "node:os";
import NodeTimers from "node:timers";
import { setTimeout as delay } from "node:timers/promises";

export const admissionContract = "jones.local-mac-setup-admission/v1";
export class UnknownSamplerClosure extends Error {}
const now = () => performance.now();
type Cpu = { times: { user: number; nice: number; sys: number; irq: number; idle: number } };
const fields = ["user", "nice", "sys", "irq", "idle"] as const;
function counter(value: number) {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error("Invalid native counter");
  return value;
}
export function cpuEvidence(before: Cpu[], after: Cpu[]) {
  if (!before.length || before.length !== after.length) throw new Error("CPU count changed/empty");
  let total = 0;
  let idle = 0;
  for (let index = 0; index < before.length; index++) {
    for (const field of fields) {
      const delta = counter(after[index]!.times[field]) - counter(before[index]!.times[field]);
      counter(delta);
      total = counter(total + delta);
      if (field === "idle") idle = counter(idle + delta);
    }
  }
  if (!total) throw new Error("CPU interval has no ticks");
  return { usedPercent: ((total - idle) / total) * 100, oneCorePercent: 100 / before.length };
}
export function parseVmStat(output: string) {
  const page = [
    ...output.matchAll(/^Mach Virtual Memory Statistics: \(page size of ([0-9]+) bytes\)\s*$/gm),
  ];
  if (
    page.length !== 1 ||
    output.split("\n").filter((line) => line.startsWith("Mach Virtual Memory Statistics:"))
      .length !== 1
  )
    throw new Error("Missing/duplicate vm_stat page size");
  const pageSize = counter(Number(page[0]![1]));
  if (!pageSize) throw new Error("Invalid page size");
  function read(name: string) {
    const matches = [...output.matchAll(new RegExp(`^${name}:\\s+([0-9]+)\\.\\s*$`, "gm"))];
    if (
      matches.length !== 1 ||
      output.split("\n").filter((line) => line.startsWith(`${name}:`)).length !== 1
    )
      throw new Error(`Missing/duplicate vm_stat ${name}`);
    return counter(Number(matches[0]![1]));
  }
  return { pageSize, swapins: read("Swapins"), swapouts: read("Swapouts") };
}
export function memoryEvidence(before: string, after: string, elapsedMs: number, pressure: string) {
  if (!Number.isFinite(elapsedMs) || elapsedMs <= 0 || elapsedMs > 5_000)
    throw new Error("Invalid memory interval");
  const a = parseVmStat(before);
  const b = parseVmStat(after);
  if (a.pageSize !== b.pageSize) throw new Error("Page size changed");
  const pages = counter(counter(b.swapins - a.swapins) + counter(b.swapouts - a.swapouts));
  const bytes = counter(pages * b.pageSize);
  const rate = bytes / (elapsedMs / 1_000);
  if (!Number.isFinite(rate)) throw new Error("Invalid swap rate");
  if (!/^[124]\s*$/.test(pressure)) throw new Error("Invalid native pressure");
  return { pressure: Number(pressure), swapBytesPerSecond: rate };
}

// Timeout is a refusal; only close proves that a captured utility and its pipes drained.
export function utilityReader(spawnUtility: typeof spawn = spawn) {
  let outputBytes = 0;
  return (command: string, args: string[], signal: AbortSignal): Promise<string> =>
    new Promise((resolve, reject) => {
      signal.throwIfAborted();
      const child = spawnUtility(command, args, {
        shell: false,
        stdio: ["ignore", "pipe", "pipe"],
        env: { ...process.env, LC_ALL: "C", LANG: "C" },
      });
      let output = "";
      let failure: Error | undefined;
      let closureTimer: ReturnType<typeof NodeTimers.setTimeout> | undefined;
      const stop = (error: Error) => {
        failure ??= error;
        if (closureTimer) return;
        try {
          child.kill("SIGKILL");
        } catch (error) {
          failure = error instanceof Error ? error : new Error(String(error));
        }
        closureTimer = NodeTimers.setTimeout(() => {
          cleanup();
          reject(new UnknownSamplerClosure("Sampler utility output closure unknown"));
        }, 250);
      };
      const abort = () => stop(new Error("Sampler aborted"));
      const timer = NodeTimers.setTimeout(() => stop(new Error("Sampler utility timeout")), 2_000);
      const cleanup = () => {
        NodeTimers.clearTimeout(timer);
        NodeTimers.clearTimeout(closureTimer);
        signal.removeEventListener("abort", abort);
      };
      signal.addEventListener("abort", abort, { once: true });
      child.on("error", (error) => {
        failure ??= error;
      });
      for (const [stream, stdout] of [
        [child.stdout, true],
        [child.stderr, false],
      ] as const) {
        stream?.on("data", (chunk: Buffer) => {
          outputBytes += chunk.length;
          if (outputBytes > 65_536) stop(new Error("Sampler output exceeds 64 KiB"));
          else if (stdout) output += chunk.toString("utf8");
        });
      }
      child.once("close", (code) => {
        cleanup();
        if (failure || code !== 0) reject(failure ?? new Error("Sampler utility failed"));
        else resolve(output);
      });
      if (signal.aborted) abort();
    });
}

type SamplerDependencies = {
  cpu: () => Cpu[];
  now: () => number;
  wait: (signal: AbortSignal) => Promise<unknown>;
  utility: (command: string, args: string[], signal: AbortSignal) => Promise<string>;
};
export function createLocalMacAdmission(dependencies?: SamplerDependencies) {
  const d = dependencies ?? {
    cpu: cpus,
    now,
    wait: (signal: AbortSignal) => delay(1_000, undefined, { signal }),
    utility: undefined,
  };
  const receipts = new WeakMap<object, { attempt: object; finished: number }>();
  return {
    async collect(attempt: object, signal: AbortSignal) {
      signal.throwIfAborted();
      const controller = new AbortController();
      const abort = () => controller.abort();
      signal.addEventListener("abort", abort, { once: true });
      const deadline = NodeTimers.setTimeout(abort, 5_000);
      const started = d.now();
      const utility = d.utility ?? utilityReader();
      try {
        const firstCpu = d.cpu();
        const firstVm = await utility("/usr/bin/vm_stat", [], controller.signal);
        const firstAt = d.now();
        await d.wait(controller.signal);
        const secondCpu = d.cpu();
        const cpu = cpuEvidence(firstCpu, secondCpu);
        const secondVm = await utility("/usr/bin/vm_stat", [], controller.signal);
        const secondAt = d.now();
        const pressure = await utility(
          "/usr/sbin/sysctl",
          ["-n", "kern.memorystatus_vm_pressure_level"],
          controller.signal,
        );
        const memory = memoryEvidence(firstVm, secondVm, secondAt - firstAt, pressure);
        const finished = d.now();
        controller.signal.throwIfAborted();
        if (
          !Number.isFinite(finished - started) ||
          finished - started <= 0 ||
          finished - started > 5_000
        )
          throw new Error("Sampler deadline/interval invalid");
        if (
          cpu.usedPercent >= 85 ||
          (memory.pressure === 4 && memory.swapBytesPerSecond > 10_485_760)
        )
          throw new Error("Local Mac setup resource admission held");
        const receipt = Object.freeze({ contract: admissionContract, cpu, memory });
        receipts.set(receipt, { attempt, finished });
        return receipt;
      } finally {
        NodeTimers.clearTimeout(deadline);
        signal.removeEventListener("abort", abort);
      }
    },
    consume(receipt: object, attempt: object, signal: AbortSignal) {
      signal.throwIfAborted();
      const bound = receipts.get(receipt);
      receipts.delete(receipt);
      const age = bound ? d.now() - bound.finished : NaN;
      if (!bound || bound.attempt !== attempt || !Number.isFinite(age) || age < 0 || age > 1_000)
        throw new Error("Invalid, replayed or stale setup admission");
    },
  };
}
export type LocalMacAdmission = ReturnType<typeof createLocalMacAdmission>;

export function producerEnvironment(kind: "install" | "warm", environment = process.env) {
  const env = { ...environment };
  const controls =
    kind === "install"
      ? { PNPM_CONFIG_CHILD_CONCURRENCY: "1" }
      : { ROLLDOWN_WORKER_THREADS: "1", ROLLDOWN_MAX_BLOCKING_THREADS: "1" };
  for (const key of Object.keys(env)) {
    if (Object.keys(controls).some((control) => control.toLowerCase() === key.toLowerCase()))
      delete env[key];
  }
  return { ...env, ...controls };
}
