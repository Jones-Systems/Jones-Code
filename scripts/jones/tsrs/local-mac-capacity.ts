// @effect-diagnostics nodeBuiltinImport:off -- Pre-compiler admission uses only Node built-ins.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";
import { createLocalMacAdmission } from "../worktree-setup/local-mac-admission.ts";

/** GitHub's hosted-runner metadata retains its separate policy; CI alone selects nothing. */
export function usesLocalMacCapacity(platform: string, environment: NodeJS.ProcessEnv) {
  return (
    platform === "darwin" &&
    !(
      environment.GITHUB_ACTIONS === "true" &&
      environment.RUNNER_ENVIRONMENT === "github-hosted" &&
      environment.RUNNER_OS === "macOS"
    )
  );
}

export function compilerBudget(cores: number, average: number, recent: number, requested = 6) {
  if (
    !Number.isInteger(cores) ||
    cores < 1 ||
    !Number.isInteger(requested) ||
    requested < 1 ||
    ![average, recent].every((x) => Number.isFinite(x) && x >= 0 && x <= 1)
  )
    throw new Error("Invalid compiler capacity evidence/request");
  return Math.max(0, Math.min(6, requested, Math.floor(cores * (0.8 - Math.max(average, recent)))));
}

function cpu() {
  return NodeOS.cpus().map((c) => ({
    idle: c.times.idle,
    total: Object.values(c.times).reduce((a, b) => a + b, 0),
  }));
}
type Cpu = ReturnType<typeof cpu>;
function busy(a: Cpu, b: Cpu) {
  if (!a.length || a.length !== b.length) throw new Error("CPU topology changed/missing");
  let total = 0,
    idle = 0;
  for (let i = 0; i < a.length; i++) {
    const t = b[i]!.total - a[i]!.total,
      d = b[i]!.idle - a[i]!.idle;
    if (!(t > 0) || d < 0 || d > t) throw new Error("Invalid CPU counters");
    total += t;
    idle += d;
  }
  return 1 - idle / total;
}
function privateDirectory(dir: string) {
  NodeFS.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const s = NodeFS.lstatSync(dir);
  if (!s.isDirectory() || s.isSymbolicLink() || s.uid !== process.getuid!() || s.mode & 0o077)
    throw new Error(`Capacity directory must be private and owned: ${dir}`);
}

export type CapacityOptions = {
  root: string;
  signal: AbortSignal;
  jobs: number;
  platform?: string;
  directory?: string;
  deadlineMs?: number;
  /** Warm's in-process native pools retain the lease until normal process exit. */
  processLifetime?: boolean;
};

type Dependencies = {
  now: () => number;
  wait: (ms: number, signal: AbortSignal) => Promise<unknown>;
  cpu: typeof cpu;
  memory: (signal: AbortSignal) => Promise<unknown>;
  storage: (root: string) => void;
  report: (evidence: object) => void;
};
const defaults: Dependencies = {
  now: () => performance.now(),
  wait: (ms, signal) => NodeTimersPromises.setTimeout(ms, undefined, { signal }),
  cpu,
  memory: async (signal) => {
    const a = createLocalMacAdmission(),
      attempt = {};
    const receipt = await a.collect(attempt, signal);
    a.consume(receipt, attempt, signal);
    return receipt;
  },
  storage: (root) => {
    for (const destination of [root, NodeOS.tmpdir()]) {
      const s = NodeFS.statfsSync(destination, { bigint: true });
      if (s.bavail < 0n || s.bsize <= 0n || s.bavail * s.bsize < 40n * 1024n ** 3n)
        throw new Error(
          `Compiler storage refused: ${destination}; requires30GiB reserve+10GiB peak`,
        );
    }
  },
  report: (evidence) => process.stderr.write(`[local-mac-capacity] ${JSON.stringify(evidence)}\n`),
};

/** Participating launches only: a sample is neither a reservation nor an OS quota. */
export async function withLocalMacCapacity<T>(
  options: CapacityOptions,
  run: (budget: number) => Promise<T>,
  dependencies: Dependencies = defaults,
): Promise<T> {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Admission measures the physical host before starting the application/compiler runtime.
  if (!usesLocalMacCapacity(options.platform ?? NodeOS.platform(), process.env))
    return run(options.jobs);
  compilerBudget(1, 0, 0, options.jobs);
  const d = dependencies,
    deadline = d.now() + (options.deadlineMs ?? 600_000);
  const directory =
    options.directory ?? NodePath.join(NodeOS.homedir(), ".cache/jones/local-mac-capacity");
  privateDirectory(directory);
  const lock = NodePath.join(directory, "build.lock");
  let held = false;
  const check = () => {
    options.signal.throwIfAborted();
    if (d.now() >= deadline) throw new Error("Compiler lock/admission deadline exceeded");
  };
  for (;;) {
    check();
    try {
      NodeFS.mkdirSync(lock, { mode: 0o700 });
      held = true;
      break;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      await d.wait(100, options.signal);
    }
  }
  const owner = JSON.stringify({ pid: process.pid, nonce: NodeCrypto.randomUUID() });
  NodeFS.writeFileSync(NodePath.join(lock, "owner.json"), owner, { flag: "wx", mode: 0o600 });
  const release = () => {
    if (!held) return;
    if (NodeFS.readFileSync(NodePath.join(lock, "owner.json"), "utf8") !== owner)
      throw new Error("Capacity lock ownership changed; retained");
    NodeFS.unlinkSync(NodePath.join(lock, "owner.json"));
    NodeFS.rmdirSync(lock);
    held = false;
  };
  let entered = false;
  try {
    for (;;) {
      check();
      const started = d.now(),
        first = d.cpu();
      await d.wait(25_000, options.signal);
      check();
      const lastFive = d.cpu();
      await d.wait(5_000, options.signal);
      check();
      const final = d.cpu();
      const elapsed = d.now() - started;
      if (!Number.isFinite(elapsed) || elapsed < 30_000 || elapsed > 35_000)
        throw new Error("Compiler CPU window stale/invalid");
      const average = busy(first, final),
        recent = busy(lastFive, final);
      const budget = compilerBudget(final.length, average, recent, options.jobs);
      d.report({ elapsed, average, recent, budget, required: options.jobs, lock });
      if (budget < options.jobs) continue;
      const memory = await d.memory(options.signal);
      check();
      d.storage(options.root);
      check();
      if (d.now() - started > 35_000) throw new Error("Compiler admission stale before launch");
      d.report({ memory, root: options.root, budget });
      entered = true;
      if (options.processLifetime) process.once("exit", release);
      const value = await run(budget);
      if (!options.processLifetime) release();
      return value;
    }
  } catch (error) {
    // A callback rejection can mean unknown native closure. Never unlock for a replacement.
    if (!entered) release();
    else {
      if (options.processLifetime) process.removeListener("exit", release);
      d.report({ retainedLock: lock, reason: "producer closure not established after rejection" });
    }
    throw error;
  }
}
