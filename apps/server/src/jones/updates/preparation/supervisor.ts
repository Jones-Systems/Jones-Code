// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off
// Parent side of off-process update preparation. The serving process only spawns, watches and
// reads bounded results; every hash, extraction and validation runs in the worker's group.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { ownedScratchRemover } from "../../cloud/ownedScratch.ts";
import { withQualifiedRuntimeLock } from "../../cloud/qualifiedRuntime.ts";
import {
  decodePreparationEvent,
  decodePreparationResult,
  PREPARATION_COMMAND,
  PREPARATION_LIMITS,
  PREPARATION_PROTOCOL,
  preparationFailureError,
  preparationJobLock,
  preparationJobsDirectory,
  readBoundedJson,
  REQUEST_FILE,
  RESULT_FILE,
  writeDurableJson,
  type PreparationJob,
  type PreparationOutcomeFor,
  type PreparationPhase,
  type PreparationRequest,
} from "./protocol.ts";

/** Program and leading arguments that start this server's own build; the worker command follows. */
export interface PreparationWorkerCommand {
  readonly file: string;
  readonly args: ReadonlyArray<string>;
}

const NICE = "/usr/bin/nice";

/**
 * A single executable runs the worker as its own hidden command; a script install runs its entry
 * again under the same runtime (Electron keeps `ELECTRON_RUN_AS_NODE` from the environment).
 * With `nice` available the whole worker, including every thread, starts at the lowest priority.
 */
export function resolvePreparationWorkerCommand(input: {
  readonly isExecutable: boolean;
  readonly execPath: string;
  readonly entryPath: string | undefined;
  readonly entryExists?: (path: string) => boolean;
  readonly niceAvailable?: boolean;
}): PreparationWorkerCommand | undefined {
  const exists = input.entryExists ?? ((path: string) => NodeFS.existsSync(path));
  const program: readonly [string, ...string[]] | undefined = input.isExecutable
    ? [input.execPath]
    : input.entryPath !== undefined &&
        NodePath.isAbsolute(input.entryPath) &&
        exists(input.entryPath)
      ? [input.execPath, input.entryPath]
      : undefined;
  if (program === undefined) return undefined;
  const nice =
    input.niceAvailable ??
    (() => {
      try {
        NodeFS.accessSync(NICE, NodeFS.constants.X_OK);
        return true;
      } catch {
        return false;
      }
    })();
  return nice
    ? { file: NICE, args: ["-n", "19", ...program] }
    : { file: program[0], args: program.slice(1) };
}

export type PreparationStopReason =
  | "cancelled"
  | "deadline"
  | "stalled"
  | "protocol"
  | "output-limit"
  | "spawn-failed";

/** Why a worker did not produce a usable result; its message is safe to publish. */
export class PreparationWorkerError extends Error {
  readonly reason: PreparationStopReason | "crashed" | "unreaped" | "result-invalid";
  constructor(reason: PreparationWorkerError["reason"]) {
    super(
      {
        cancelled: "Update preparation was cancelled.",
        deadline: "Update preparation exceeded its deadline and was stopped.",
        stalled: "The update preparation worker stopped responding and was stopped.",
        protocol: "The update preparation worker broke its protocol and was stopped.",
        "output-limit": "The update preparation worker exceeded its output bound and was stopped.",
        "spawn-failed": "The update preparation worker could not start.",
        crashed: "The update preparation worker exited without a result.",
        unreaped:
          "The update preparation worker did not exit after it was killed; its job directory was retained.",
        "result-invalid": "The update preparation worker returned an invalid result.",
      }[reason],
    );
    this.name = "PreparationWorkerError";
    this.reason = reason;
  }
}

export interface PreparationLimits {
  readonly heartbeatTimeoutMs: number;
  readonly graceMs: number;
  readonly reapMs: number;
  readonly deadlineMs: Readonly<Record<PreparationJob["kind"], number>>;
}

export const DEFAULT_PREPARATION_LIMITS: PreparationLimits = {
  heartbeatTimeoutMs: 60_000,
  graceMs: 30_000,
  reapMs: 10_000,
  deadlineMs: { stage: 60 * 60_000, verify: 15 * 60_000, sweep: 5 * 60_000 },
};

/** One finished worker, as observed by the parent. */
export interface PreparationWorkerExit {
  readonly jobId: string;
  readonly kind: PreparationJob["kind"];
  readonly pid: number | undefined;
  readonly priority: number | null | undefined;
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly stopped: PreparationStopReason | undefined;
  readonly reaped: boolean;
  readonly durationMs: number;
  readonly stderrTail: string;
}

export interface PreparationSupervisor {
  readonly run: <J extends PreparationJob>(
    job: J,
    options?: {
      readonly signal?: AbortSignal;
      readonly onPhase?: (phase: PreparationPhase) => void;
    },
  ) => Promise<PreparationOutcomeFor<J["kind"]>>;
  /** Removes dead leftovers of earlier workers; also queued after any failed job. */
  readonly sweep: () => Promise<PreparationOutcomeFor<"sweep">>;
}

function superviseWorker(input: {
  readonly command: PreparationWorkerCommand;
  readonly jobId: string;
  readonly jobDir: string;
  readonly kind: PreparationJob["kind"];
  readonly env: NodeJS.ProcessEnv;
  readonly limits: PreparationLimits;
  readonly signal?: AbortSignal;
  readonly onPhase?: (phase: PreparationPhase) => void;
}): Promise<PreparationWorkerExit> {
  return new Promise((resolve) => {
    const started = Date.now();
    const timers = new Set<NodeJS.Timeout>();
    const later = (ms: number, action: () => void) => {
      const timer = setTimeout(() => {
        timers.delete(timer);
        action();
      }, ms);
      timers.add(timer);
      return timer;
    };
    const spawned = (() => {
      try {
        return NodeChildProcess.spawn(
          input.command.file,
          [...input.command.args, PREPARATION_COMMAND, input.jobDir],
          {
            cwd: input.jobDir,
            env: { ...input.env, TMPDIR: NodePath.join(input.jobDir, "tmp") },
            // Its own process group: cancellation reaches gh, preflight and Darwin tools too.
            detached: true,
            stdio: ["pipe", "pipe", "pipe"],
          },
        );
      } catch {
        return undefined;
      }
    })();
    if (spawned === undefined) {
      resolve({
        jobId: input.jobId,
        kind: input.kind,
        pid: undefined,
        priority: undefined,
        code: null,
        signal: null,
        stopped: "spawn-failed",
        reaped: true,
        durationMs: 0,
        stderrTail: "",
      });
      return;
    }
    const child = spawned;
    const pid = child.pid;
    if (pid !== undefined)
      try {
        NodeOS.setPriority(pid, NodeOS.constants.priority.PRIORITY_LOW);
      } catch {
        // The worker lowers itself as well; neither is a CPU cap.
      }
    let stopped: PreparationStopReason | undefined;
    let exited = false;
    let settled = false;
    let ready = false;
    let priority: number | null | undefined;
    let code: number | null = null;
    let exitSignal: NodeJS.Signals | null = null;
    let stderrTail = "";
    let buffered = Buffer.alloc(0);
    let streamed = 0;
    // Only the group this call spawned, identified by the captured spawn PID.
    const signalGroup = (signal: NodeJS.Signals) => {
      if (pid === undefined) return;
      try {
        process.kill(-pid, signal);
      } catch {
        // The group is already gone.
      }
    };
    const onAbort = () => stop("cancelled");
    const settle = (reaped: boolean) => {
      if (settled) return;
      settled = true;
      for (const timer of timers) clearTimeout(timer);
      timers.clear();
      input.signal?.removeEventListener("abort", onAbort);
      child.stdin.destroy();
      child.stdout.removeAllListeners("data");
      child.stderr.removeAllListeners("data");
      child.stdout.destroy();
      child.stderr.destroy();
      resolve({
        jobId: input.jobId,
        kind: input.kind,
        pid,
        priority,
        code,
        signal: exitSignal,
        stopped,
        reaped,
        durationMs: Date.now() - started,
        stderrTail,
      });
    };
    function stop(reason: PreparationStopReason) {
      if (stopped !== undefined || exited || settled) return;
      stopped = reason;
      signalGroup("SIGTERM");
      later(input.limits.graceMs, () => signalGroup("SIGKILL"));
      later(input.limits.graceMs + input.limits.reapMs, () => settle(false));
    }
    let stall: NodeJS.Timeout | undefined;
    const armStall = () => {
      if (stall !== undefined) {
        clearTimeout(stall);
        timers.delete(stall);
      }
      stall = later(input.limits.heartbeatTimeoutMs, () => stop("stalled"));
    };
    armStall();
    later(input.limits.deadlineMs[input.kind], () => stop("deadline"));
    input.signal?.addEventListener("abort", onAbort, { once: true });
    if (input.signal?.aborted) stop("cancelled");

    child.stdin.on("error", () => {});
    child.stdout.on("data", (chunk: Buffer) => {
      // Keep draining after a stop so a blocked write cannot delay the worker's own exit.
      if (stopped !== undefined) return;
      streamed += chunk.length;
      if (streamed > PREPARATION_LIMITS.eventStreamBytes) return stop("output-limit");
      buffered = Buffer.concat([buffered, chunk]);
      for (;;) {
        const newline = buffered.indexOf(10);
        if (newline < 0) break;
        const event = decodePreparationEvent(buffered.subarray(0, newline).toString("utf8"));
        buffered = buffered.subarray(newline + 1);
        if (event === undefined || (event.type === "ready") === ready) return stop("protocol");
        if (event.type === "ready") {
          if (event.protocol !== PREPARATION_PROTOCOL || event.pid !== pid) return stop("protocol");
          ready = true;
          priority = event.priority;
        } else if (event.type === "phase")
          try {
            input.onPhase?.(event.phase);
          } catch {
            // A progress observer never stops the job.
          }
        armStall();
      }
      if (buffered.length > PREPARATION_LIMITS.eventLineBytes) stop("output-limit");
    });
    child.stderr.on("data", (chunk: Buffer) => {
      if (stderrTail.length < PREPARATION_LIMITS.stderrBytes)
        stderrTail = (stderrTail + chunk.toString("utf8")).slice(0, PREPARATION_LIMITS.stderrBytes);
    });
    child.once("error", () => {
      if (pid === undefined) {
        stopped ??= "spawn-failed";
        settle(true);
      }
    });
    child.once("exit", (exitCode, signal) => {
      exited = true;
      code = exitCode;
      exitSignal = signal;
      // A member the worker left behind, such as a child that ignored SIGTERM, ends with it.
      signalGroup("SIGKILL");
      later(input.limits.reapMs, () => settle(true));
    });
    child.once("close", () => {
      if (exited) settle(true);
    });
  });
}

export function makePreparationSupervisor(options: {
  /** The service home; job storage and the preparation lock live under it. */
  readonly baseDir: string;
  readonly cacheRoot: string;
  readonly command: PreparationWorkerCommand;
  readonly env?: NodeJS.ProcessEnv;
  readonly limits?: Partial<Omit<PreparationLimits, "deadlineMs">> & {
    readonly deadlineMs?: Partial<PreparationLimits["deadlineMs"]>;
  };
  readonly observe?: (exit: PreparationWorkerExit) => void;
}): PreparationSupervisor {
  const limits: PreparationLimits = {
    ...DEFAULT_PREPARATION_LIMITS,
    ...options.limits,
    deadlineMs: { ...DEFAULT_PREPARATION_LIMITS.deadlineMs, ...options.limits?.deadlineMs },
  };
  const remove = ownedScratchRemover();
  // One worker per host: jobs wait here, and the worker's lock refuses any other holder.
  let tail: Promise<unknown> = Promise.resolve();
  const enqueue = <A>(task: () => Promise<A>): Promise<A> => {
    const next = tail.then(task, task);
    tail = next.catch(() => undefined);
    return next;
  };

  const runOnce = async <J extends PreparationJob>(
    job: J,
    signal: AbortSignal | undefined,
    onPhase: ((phase: PreparationPhase) => void) | undefined,
  ): Promise<PreparationOutcomeFor<J["kind"]>> => {
    signal?.throwIfAborted();
    const baseDir = await NodeFSP.realpath(options.baseDir);
    const jobs = preparationJobsDirectory(baseDir);
    await NodeFSP.mkdir(jobs, { recursive: true, mode: 0o700 });
    const jobId = NodeCrypto.randomUUID();
    const jobDir = NodePath.join(jobs, jobId);
    await NodeFSP.mkdir(jobDir, { mode: 0o700 });
    let retainJob = false;
    try {
      // Held until the result is read: the lock, not the directory, is what marks a job live.
      return await withQualifiedRuntimeLock(baseDir, preparationJobLock(jobId), async () => {
        await NodeFSP.mkdir(NodePath.join(jobDir, "tmp"), { mode: 0o700 });
        const request: PreparationRequest = {
          protocol: PREPARATION_PROTOCOL,
          jobId,
          baseDir,
          // The worker stops itself first, leaving the parent's deadline as the backstop.
          deadlineMs: Math.max(
            Math.floor(limits.deadlineMs[job.kind] / 2),
            limits.deadlineMs[job.kind] - limits.graceMs,
            1,
          ),
          processGroup: true,
          job,
        };
        await writeDurableJson(
          NodePath.join(jobDir, REQUEST_FILE),
          request,
          PREPARATION_LIMITS.requestBytes,
        );
        const exit = await superviseWorker({
          command: options.command,
          jobId,
          jobDir,
          kind: job.kind,
          env: options.env ?? process.env,
          limits,
          ...(signal === undefined ? {} : { signal }),
          ...(onPhase === undefined ? {} : { onPhase }),
        });
        try {
          options.observe?.(exit);
        } catch {
          // Observation is diagnostic only.
        }
        if (!exit.reaped) {
          retainJob = true;
          throw new PreparationWorkerError("unreaped");
        }
        if (exit.stopped !== undefined) throw new PreparationWorkerError(exit.stopped);
        if (exit.code !== 0) throw new PreparationWorkerError("crashed");
        const result = decodePreparationResult(
          await readBoundedJson(
            NodePath.join(jobDir, RESULT_FILE),
            PREPARATION_LIMITS.resultBytes,
          ).catch(() => undefined),
          jobId,
          job.kind,
        );
        if (result === undefined) throw new PreparationWorkerError("result-invalid");
        if (!result.ok) throw preparationFailureError(result.error);
        return result.outcome as PreparationOutcomeFor<J["kind"]>;
      });
    } finally {
      // A reaped worker can no longer write here; an unreaped one is left for a later sweep.
      if (!retainJob) await remove(jobDir).catch(() => undefined);
    }
  };

  const sweep = () =>
    enqueue(() => runOnce({ kind: "sweep", cacheRoot: options.cacheRoot }, undefined, undefined));

  return {
    run: (job, runOptions) => {
      const signal = runOptions?.signal;
      const queued = enqueue(() => runOnce(job, signal, runOptions?.onPhase));
      // Whatever a failed or killed job left behind is swept by the next worker.
      if (job.kind !== "sweep") void queued.catch(() => sweep().catch(() => undefined));
      if (signal === undefined) return queued;
      // A cancelled caller is released at once; the queue still reaps the worker behind it.
      return new Promise((resolve, reject) => {
        const onAbort = () => reject(signal.reason);
        if (signal.aborted) return onAbort();
        signal.addEventListener("abort", onAbort, { once: true });
        queued.then(
          (outcome) => {
            signal.removeEventListener("abort", onAbort);
            resolve(outcome);
          },
          (cause: unknown) => {
            signal.removeEventListener("abort", onAbort);
            reject(cause);
          },
        );
      });
    },
    sweep,
  };
}
