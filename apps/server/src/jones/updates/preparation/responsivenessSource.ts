// @effect-diagnostics nodeBuiltinImport:off globalTimers:off
// Private test entry. It measures one synthetic operation, not a loaded server or equal hash work.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodePerfHooks from "node:perf_hooks";
import * as NodeURL from "node:url";
import { isEntrypoint } from "../../../entrypoint.ts";
import { verifyStagedQualifiedRuntime } from "../../cloud/qualifiedRuntime.ts";
import { preparationLifecycleJobIds } from "./lifecycle.ts";
import { isPreparationJobId } from "./protocol.ts";
import {
  BASELINE,
  fixtureHost,
  makePreparationHome,
  sourceWorkerCommand,
  stageFixtureRuntime,
} from "./Preparation.testkit.ts";
import {
  makePreparationSupervisor,
  type PreparationJoin,
  type PreparationSupervisor,
  type PreparationWorkerExit,
} from "./supervisor.ts";

export const PAYLOAD_BYTES = 128 * 1024 * 1024;
const TICK_MS = 5;
const OUTPUT_LIMIT = 64 * 1024;

export interface LoopCost {
  readonly elapsedMs: number;
  readonly activeMs: number;
  /** Process CPU diagnostic, not an assertion or exclusive callback attribution. */
  readonly cpuMs: number;
  readonly utilization: number;
  readonly maxDelayMs: number;
  readonly ticks: number;
  readonly expectedTicks: number;
}

async function loopCost(work: () => Promise<unknown>): Promise<LoopCost> {
  const delay = NodePerfHooks.monitorEventLoopDelay({ resolution: 1 });
  let ticks = 0;
  const ticker = setInterval(() => {
    ticks += 1;
  }, TICK_MS);
  delay.enable();
  const beforeCpu = process.cpuUsage();
  const before = NodePerfHooks.performance.eventLoopUtilization();
  const started = NodePerfHooks.performance.now();
  try {
    await work();
  } finally {
    clearInterval(ticker);
    delay.disable();
  }
  const elapsedMs = NodePerfHooks.performance.now() - started;
  const loop = NodePerfHooks.performance.eventLoopUtilization(before);
  const cpu = process.cpuUsage(beforeCpu);
  return {
    elapsedMs,
    activeMs: loop.active,
    cpuMs: (cpu.user + cpu.system) / 1000,
    utilization: loop.utilization,
    maxDelayMs: delay.max / 1e6,
    ticks,
    expectedTicks: Math.floor(elapsedMs / TICK_MS),
  };
}

/** Wait for the actual job, then fence behind the failure sweep enqueued by the supervisor. */
export async function joinResponsivenessJobs(input: {
  readonly supervisor: PreparationSupervisor;
  readonly captured: ReadonlyArray<Promise<PreparationJoin>>;
  readonly failed: boolean;
  readonly cacheRoot: string;
  readonly jobIds: () => Promise<ReadonlyArray<string>>;
}): Promise<ReadonlyArray<PreparationJoin>> {
  await Promise.all(input.captured);
  if (input.failed) {
    // start's rejection callback enqueues its automatic sweep before settled resolves. This
    // final sweep is a queue barrier, not a replacement for joining either earlier operation.
    const barrier = input.supervisor.start(
      { kind: "sweep", cacheRoot: input.cacheRoot },
      { jobId: NodeCrypto.randomUUID() },
    );
    try {
      await barrier.outcome;
    } finally {
      await barrier.settled;
    }
  }
  const joins: PreparationJoin[] = [];
  for (const id of await input.jobIds()) {
    const join = input.supervisor.inspect(id);
    if (
      join === "running" ||
      join === "unknown" ||
      !["completed", "stopped", "failed", "not-started"].includes(join.kind)
    )
      throw new Error(`Preparation cleanup is unconfirmed for ${id}.`);
    joins.push(join);
  }
  if ((await input.supervisor.unreleasedJobs?.())?.length !== 0)
    throw new Error("Preparation lifecycle release is unconfirmed.");
  return joins;
}

type WorkerReadback = Pick<
  PreparationWorkerExit,
  "jobId" | "kind" | "pid" | "code" | "signal" | "stopped" | "reaped"
>;
interface Cleanup {
  readonly removed: boolean;
  readonly jobs: ReadonlyArray<PreparationJoin>;
  readonly workers: ReadonlyArray<WorkerReadback>;
}
interface Measured {
  readonly schema: 1;
  readonly status: "measured";
  readonly payloadBytes: number;
  readonly worker: LoopCost;
  readonly inProcess: LoopCost;
  readonly homeRoot: string;
  readonly cleanup: Cleanup;
}
interface Failed {
  readonly schema: 1;
  readonly status: "failed";
  readonly primary: string;
  readonly cleanupError?: string;
  readonly homeRoot?: string;
  readonly cleanup: Cleanup;
}
type Frame = Measured | Failed;
const object = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const finite = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value) && value >= 0;
const integer = (value: unknown): value is number => finite(value) && Number.isSafeInteger(value);
const keys = (
  value: Record<string, unknown>,
  required: ReadonlyArray<string>,
  optional: ReadonlyArray<string> = [],
) =>
  required.every((key) => Object.hasOwn(value, key)) &&
  Object.keys(value).every((key) => required.includes(key) || optional.includes(key));
function cost(value: unknown): value is LoopCost {
  return (
    object(value) &&
    keys(value, [
      "elapsedMs",
      "activeMs",
      "cpuMs",
      "utilization",
      "maxDelayMs",
      "ticks",
      "expectedTicks",
    ]) &&
    finite(value.elapsedMs) &&
    value.elapsedMs > 0 &&
    finite(value.activeMs) &&
    finite(value.cpuMs) &&
    finite(value.utilization) &&
    value.utilization <= 1 &&
    finite(value.maxDelayMs) &&
    integer(value.ticks) &&
    integer(value.expectedTicks) &&
    value.expectedTicks === Math.floor(value.elapsedMs / TICK_MS)
  );
}
function join(value: unknown): value is PreparationJoin {
  return (
    object(value) &&
    keys(value, ["jobId", "kind"], ["reason"]) &&
    typeof value.jobId === "string" &&
    isPreparationJobId(value.jobId) &&
    (value.kind === "completed" ||
      value.kind === "stopped" ||
      value.kind === "failed" ||
      value.kind === "not-started") &&
    (value.reason === undefined || typeof value.reason === "string")
  );
}
function worker(value: unknown): value is WorkerReadback {
  return (
    object(value) &&
    keys(value, ["jobId", "kind", "code", "signal", "reaped"], ["pid", "stopped"]) &&
    typeof value.jobId === "string" &&
    isPreparationJobId(value.jobId) &&
    (value.kind === "verify" ||
      value.kind === "stage" ||
      value.kind === "verify-runtime" ||
      value.kind === "sweep") &&
    value.reaped === true &&
    ((value.pid === undefined && value.stopped === "spawn-failed") ||
      (integer(value.pid) && value.pid > 1)) &&
    (value.code === null || integer(value.code)) &&
    (value.signal === null ||
      (typeof value.signal === "string" &&
        Object.hasOwn(NodeOS.constants.signals, value.signal))) &&
    (value.stopped === undefined ||
      (typeof value.stopped === "string" &&
        ["cancelled", "deadline", "stalled", "protocol", "output-limit", "spawn-failed"].includes(
          value.stopped,
        )))
  );
}
function confirmedCleanup(value: unknown): value is Cleanup {
  if (
    !object(value) ||
    !keys(value, ["removed", "jobs", "workers"]) ||
    value.removed !== true ||
    !Array.isArray(value.jobs) ||
    !value.jobs.every(join) ||
    !Array.isArray(value.workers) ||
    !value.workers.every(worker) ||
    value.jobs.length > 3 ||
    value.workers.length > 3
  )
    return false;
  const ids = new Set(value.jobs.map((job) => job.jobId));
  const workers = new Set(value.workers.map((exit) => exit.jobId));
  return (
    ids.size === value.jobs.length &&
    workers.size === value.workers.length &&
    value.workers.every((exit) => ids.has(exit.jobId)) &&
    value.jobs.every((job) => job.kind === "not-started" || workers.has(job.jobId))
  );
}

/** The output is one closed, bounded record; uncertainty never becomes performance evidence. */
export function decodeResponsivenessFrame(text: string): Frame {
  if (Buffer.byteLength(text) > OUTPUT_LIMIT)
    throw new Error("Responsiveness output exceeded its bound.");
  const value: unknown = JSON.parse(text);
  if (!object(value) || value.schema !== 1 || !confirmedCleanup(value.cleanup))
    throw new Error("Responsiveness cleanup was not confirmed.");
  if (
    value.status === "measured" &&
    keys(value, [
      "schema",
      "status",
      "payloadBytes",
      "worker",
      "inProcess",
      "homeRoot",
      "cleanup",
    ]) &&
    value.payloadBytes === PAYLOAD_BYTES &&
    typeof value.homeRoot === "string" &&
    cost(value.worker) &&
    cost(value.inProcess) &&
    value.cleanup.jobs.length === 1 &&
    value.cleanup.workers.length === 1
  ) {
    const job = value.cleanup.jobs[0]!;
    const exit = value.cleanup.workers[0]!;
    if (
      job.kind !== "completed" ||
      exit.kind !== "verify" ||
      exit.pid === undefined ||
      exit.code !== 0 ||
      exit.signal !== null ||
      exit.stopped !== undefined
    )
      throw new Error("Measurement worker did not complete normally.");
    return {
      schema: 1,
      status: "measured",
      payloadBytes: value.payloadBytes,
      worker: value.worker,
      inProcess: value.inProcess,
      homeRoot: value.homeRoot,
      cleanup: value.cleanup,
    };
  }
  if (
    value.status === "failed" &&
    keys(value, ["schema", "status", "primary", "cleanup"], ["homeRoot", "cleanupError"]) &&
    typeof value.primary === "string" &&
    (value.homeRoot === undefined || typeof value.homeRoot === "string") &&
    value.cleanupError === undefined
  )
    return {
      schema: 1,
      status: "failed",
      primary: value.primary,
      ...(value.homeRoot === undefined ? {} : { homeRoot: value.homeRoot }),
      cleanup: value.cleanup,
    };
  throw new Error("Invalid responsiveness measurement.");
}
export function decodeResponsivenessExit(
  text: string,
  code: number | null,
  signal: NodeJS.Signals | null,
): Frame {
  if (signal !== null) throw new Error(`Responsiveness helper exited by ${signal}.`);
  const frame = decodeResponsivenessFrame(text);
  if (frame.status === "measured" ? code !== 0 : code !== 1)
    throw new Error(`Unexpected helper exit ${code}.`);
  return frame;
}

async function measureSourceParent(signal: AbortSignal): Promise<Frame> {
  let home: Awaited<ReturnType<typeof makePreparationHome>> | undefined;
  let supervisor: PreparationSupervisor | undefined;
  const captured: Promise<PreparationJoin>[] = [];
  const workers: WorkerReadback[] = [];
  let jobs: ReadonlyArray<PreparationJoin> = [];
  let measurement: { worker: LoopCost; inProcess: LoopCost } | undefined;
  let failed = false;
  let primary = "";
  let cleanupError: string | undefined;
  try {
    signal.throwIfAborted();
    home = await makePreparationHome();
    const root = home.root;
    await new Promise<void>((resolve, reject) => {
      if (!process.connected || process.send === undefined) {
        reject(new Error("Owning helper channel is unavailable."));
        return;
      }
      process.send({ type: "home", root }, (error) => (error ? reject(error) : resolve()));
    });
    const staged = await stageFixtureRuntime(home, { bytes: PAYLOAD_BYTES });
    signal.throwIfAborted();
    supervisor = makePreparationSupervisor({
      baseDir: home.base,
      cacheRoot: home.cacheRoot,
      command: sourceWorkerCommand,
      // bin.ts must not fall back to an ambient home cache for this synthetic worker.
      env: { ...process.env, XDG_CACHE_HOME: NodePath.join(home.root, "compile-cache") },
      observe: ({ jobId, kind, pid, code, signal, stopped, reaped }) =>
        workers.push({ jobId, kind, pid, code, signal, stopped, reaped }),
    });
    const owned = supervisor;
    const worker = await loopCost(async () => {
      const running = owned.start(
        {
          kind: "verify",
          stagedHandle: staged.stagedHandle,
          activeVersion: BASELINE,
          host: fixtureHost,
        },
        { jobId: NodeCrypto.randomUUID(), signal },
      );
      captured.push(running.settled);
      try {
        await running.outcome;
      } finally {
        await running.settled;
      }
    });
    signal.throwIfAborted();
    const inProcess = await loopCost(() =>
      verifyStagedQualifiedRuntime(home!.base, BASELINE, staged.stagedHandle, fixtureHost),
    );
    signal.throwIfAborted();
    measurement = { worker, inProcess };
  } catch (cause) {
    failed = true;
    primary = String(cause);
  }
  let removed = home === undefined;
  try {
    if (supervisor !== undefined && home !== undefined) {
      const base = home.base;
      jobs = await joinResponsivenessJobs({
        supervisor,
        captured,
        failed,
        cacheRoot: home.cacheRoot,
        jobIds: () => preparationLifecycleJobIds(base),
      });
      if (!confirmedCleanup({ removed: true, jobs, workers }))
        throw new Error("Captured preparation cleanup is unconfirmed.");
    }
    if (home !== undefined) {
      await home.remove();
      removed = true;
    }
  } catch (cause) {
    cleanupError = String(cause);
  }
  const cleanup = { removed, jobs, workers };
  if (!failed && cleanupError === undefined && measurement !== undefined && home !== undefined)
    return {
      schema: 1,
      status: "measured",
      payloadBytes: PAYLOAD_BYTES,
      ...measurement,
      homeRoot: home.root,
      cleanup,
    };
  return {
    schema: 1,
    status: "failed",
    primary: failed ? primary : "Measurement cleanup failed.",
    ...(cleanupError === undefined ? {} : { cleanupError }),
    ...(home === undefined ? {} : { homeRoot: home.root }),
    cleanup,
  };
}

export class ResponsivenessSourceError extends Error {
  readonly scratch: string;
  readonly stderr: string;
  readonly receipt: Failed | undefined;
  readonly output: string;
  constructor(message: string, scratch: string, stderr: string, receipt?: Failed, output = "") {
    super(message);
    this.name = "ResponsivenessSourceError";
    this.scratch = scratch;
    this.stderr = stderr;
    this.receipt = receipt;
    this.output = output;
  }
}

/** The caller owns this helper, never its detached preparation groups. Cancellation asks it to join. */
export async function startResponsivenessSource(
  options: { readonly signal?: AbortSignal; readonly onHome?: () => void } = {},
) {
  options.signal?.throwIfAborted();
  const scratch = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-responsiveness-parent-")),
  );
  const child = NodeChildProcess.spawn(process.execPath, [NodeURL.fileURLToPath(import.meta.url)], {
    cwd: scratch,
    env: { ...process.env, TMPDIR: scratch },
    stdio: ["ignore", "pipe", "pipe", "ipc"],
  });
  const pid = child.pid;
  const chunks: Buffer[] = [];
  let outputBytes = 0;
  let homeRoot: string | undefined;
  let exitReadback: { code: number | null; signal: NodeJS.Signals | null } | undefined;
  let stderr = "";
  let invalid: string | undefined;
  let closed = false;
  const cancel = () => {
    if (!closed && child.connected) {
      try {
        child.send({ type: "cancel" }, (error) => {
          if (error) invalid ??= String(error);
        });
      } catch (cause) {
        invalid ??= String(cause);
      }
    }
  };
  if (child.stdout === null || child.stderr === null) {
    invalid = "Helper output pipes are unavailable.";
    cancel();
  }
  child.on("message", (message) => {
    if (
      !object(message) ||
      message.type !== "home" ||
      typeof message.root !== "string" ||
      homeRoot !== undefined ||
      !keys(message, ["type", "root"]) ||
      NodePath.dirname(message.root) !== scratch
    ) {
      invalid ??= "Invalid helper ownership message.";
      cancel();
      return;
    }
    homeRoot = message.root;
    try {
      options.onHome?.();
    } catch (cause) {
      invalid ??= String(cause);
      cancel();
    }
  });
  child.stdout?.on("data", (chunk: Buffer) => {
    if (outputBytes + chunk.length > OUTPUT_LIMIT) {
      invalid ??= "Responsiveness output exceeded its bound.";
      cancel();
    } else {
      chunks.push(chunk);
      outputBytes += chunk.length;
    }
  });
  child.stderr?.on("data", (chunk: Buffer) => {
    if (Buffer.byteLength(stderr) + chunk.length > OUTPUT_LIMIT) {
      invalid ??= "Helper diagnostics exceeded their bound.";
      cancel();
    } else stderr += chunk.toString("utf8");
  });
  child.on("error", (cause) => {
    invalid ??= String(cause);
  });
  child.stdout?.on("error", (cause) => {
    invalid ??= String(cause);
    cancel();
  });
  child.stderr?.on("error", (cause) => {
    invalid ??= String(cause);
    cancel();
  });
  options.signal?.addEventListener("abort", cancel, { once: true });
  if (options.signal?.aborted) cancel();
  const joined = new Promise<Frame>((resolve, reject) => {
    child.once("close", async (code, signal) => {
      closed = true;
      exitReadback = { code, signal };
      const output = Buffer.concat(chunks).toString("utf8");
      options.signal?.removeEventListener("abort", cancel);
      try {
        if (invalid !== undefined || signal !== null || pid === undefined)
          throw new Error(invalid ?? `Helper exited by ${signal}.`);
        const frame = decodeResponsivenessExit(output, code, signal);
        if (frame.homeRoot !== homeRoot)
          throw new Error("Helper result does not match the observed home.");
        if (frame.homeRoot !== undefined) {
          if (NodePath.dirname(frame.homeRoot) !== scratch)
            throw new Error("Helper home escaped its task scratch.");
          try {
            await NodeFSP.lstat(frame.homeRoot);
            throw new Error("Helper home was not removed.");
          } catch (cause) {
            if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
          }
        }
        // Nonrecursive: unexpected or uncertain material is retained, never swept by the test.
        await NodeFSP.rmdir(scratch);
        resolve(frame);
      } catch (cause) {
        reject(new ResponsivenessSourceError(String(cause), scratch, stderr, undefined, output));
      }
    });
  });
  const result = joined.then((frame) => {
    if (frame.status === "failed")
      throw new ResponsivenessSourceError(
        frame.primary,
        scratch,
        stderr,
        frame,
        Buffer.concat(chunks).toString("utf8"),
      );
    return frame;
  });
  void result.catch(() => undefined);
  void joined.catch(() => undefined);
  return {
    pid,
    scratch,
    readback: () => ({
      pid,
      exit: exitReadback,
      stdout: Buffer.concat(chunks).toString("utf8"),
      stderr,
    }),
    result,
    cancel,
    close: async () => {
      cancel();
      await joined;
    },
  };
}

if (
  isEntrypoint({
    moduleUrl: import.meta.url,
    entryPath: process.argv[1],
    runtimeMain: import.meta.main,
  })
) {
  const controller = new AbortController();
  const abort = () => controller.abort(new Error("Responsiveness helper cancelled."));
  const message = (value: unknown) => {
    if (!object(value) || value.type !== "cancel")
      controller.abort(new Error("Invalid helper control message."));
    else abort();
  };
  process.on("message", message);
  process.on("disconnect", abort);
  process.on("SIGTERM", abort);
  process.on("SIGINT", abort);
  try {
    const frame = await measureSourceParent(controller.signal);
    process.stdout.write(JSON.stringify(frame) + "\n");
    process.exitCode = frame.status === "measured" ? 0 : 1;
  } finally {
    process.removeListener("message", message);
    process.removeListener("disconnect", abort);
    process.removeListener("SIGTERM", abort);
    process.removeListener("SIGINT", abort);
    if (process.connected) process.disconnect();
  }
}
