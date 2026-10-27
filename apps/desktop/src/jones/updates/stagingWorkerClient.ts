// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - Owns forked preparation processes outside Effect fibers.
import * as NodeChildProcess from "node:child_process";
import { randomUUID } from "node:crypto";
import { JonesActionsError } from "@t3tools/shared/jones/jonesActions";
import type { JonesStagingJob, JonesStagingJobResults } from "./stagingJobs.ts";
import { JONES_STAGING_WORKER_PROTOCOL, JonesStagingStopped, JonesStagingExitUnproved, type JonesStagingWorkerMessage } from "./stagingWorkerProtocol.ts";

const JOB_TIMEOUT_MS = 45 * 60_000;
const TERMINATION_GRACE_MS = 5_000;
export interface JonesStagingChild {
  readonly pid: number | undefined;
  readonly send: (message: unknown) => void;
  readonly onMessage: (listener: (message: unknown) => void) => void;
  readonly onExit: (listener: (code: number | null, signal: string | null) => void) => void;
  readonly onError?: (listener: (error: Error) => void) => void;
  readonly signal: (signal: NodeJS.Signals) => void;
}
export function forkJonesStagingChild(workerPath: string, options: { readonly execPath?: string; readonly electron?: boolean } = {}): JonesStagingChild {
  const child = NodeChildProcess.fork(workerPath, [], {
    ...(options.execPath === undefined ? {} : { execPath: options.execPath }),
    env: options.electron === false ? process.env : { ...process.env, ELECTRON_RUN_AS_NODE: "1" },
    execArgv: [], detached: true, stdio: ["ignore", "ignore", "inherit", "ipc"],
  });
  return {
    pid: child.pid,
    send: (message) => child.send(message as NodeChildProcess.Serializable),
    onMessage: (listener) => { child.on("message", listener); },
    onExit: (listener) => { child.once("exit", listener); },
    onError: (listener) => { child.on("error", listener); },
    signal: (signal) => {
      // Only this captured disposable preparation group; never fall back to a different process.
      if (child.pid !== undefined) {
        try { process.kill(-child.pid, signal); } catch { /* Absence is observed through exit, never inferred here. */ }
      }
    },
  };
}
function rebuildFailure(error: { readonly name: string; readonly message: string; readonly code?: string; readonly reason?: JonesActionsError["reason"] }): Error {
  if (error.reason !== undefined) return new JonesActionsError(error.reason);
  if (error.name === "JonesStagingStopped") return new JonesStagingStopped();
  const failure = new Error(error.message.slice(0, 512));
  failure.name = error.name.slice(0, 64);
  if (error.code !== undefined) Object.assign(failure, { code: error.code });
  return failure;
}
export type JonesPreparationCancel = "stopped" | "completed" | "not-running" | "not-stoppable-phase" | "exit-unproved";
export interface JonesStagingWorker {
  readonly run: <K extends JonesStagingJob["kind"]>(job: Extract<JonesStagingJob, { readonly kind: K }>, onVerifying?: () => void | Promise<void>, options?: { readonly onStarted?: (worker: { readonly pid: number; readonly processGroup: number }) => Promise<void> }) => Promise<JonesStagingJobResults[K]>;
  readonly cancel: (token: string) => Promise<JonesPreparationCancel>;
  readonly reaped: (token: string) => boolean;
  readonly heldWorker: () => { readonly pid: number; readonly processGroup: number } | undefined;
  readonly close: () => Promise<void>;
}
export function createJonesStagingWorker(spawn: () => JonesStagingChild, options: {
  readonly jobTimeoutMs?: number; readonly graceMs?: number; readonly stopGraceMs?: number; readonly reapMs?: number;
} = {}): JonesStagingWorker {
  let queue: Promise<unknown> = Promise.resolve();
  let closed = false;
  const pendingTokens = new Set<string>();
  const cancelledTokens = new Set<string>();
  const reapedTokens = new Set<string>();
  type Running = {
    child: JonesStagingChild; token: string; kind: JonesStagingJob["kind"]; exited: Promise<void>; exit: () => void;
    didExit: boolean; admitted: boolean; stopRequested: boolean; held: boolean; hasResult: boolean;
    value?: unknown; failure?: Error; reject: (error: Error) => void; clearDeadline: () => void; cancellation?: Promise<JonesPreparationCancel>;
  };
  let running: Running | undefined;
  const waitExit = async (target: Running, ms: number) => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([target.exited, new Promise<void>((resolve) => { timer = setTimeout(resolve, ms); })]);
      return target.didExit;
    } finally { clearTimeout(timer); }
  };
  const terminate = async (target: Running, grace: number, reap: number): Promise<boolean> => {
    if (target.didExit) return true;
    target.child.signal("SIGTERM");
    if (await waitExit(target, grace)) return true;
    target.child.signal("SIGKILL");
    return waitExit(target, reap);
  };
  const execute = (job: JonesStagingJob, onVerifying?: () => void | Promise<void>, hooks?: { readonly onStarted?: (worker: { readonly pid: number; readonly processGroup: number }) => Promise<void> }) => {
    let current: Running | undefined;
    const outcome = new Promise<unknown>((resolve, reject) => {
      if (closed) return reject(new Error("The preparation worker is closed."));
      if (running !== undefined) return reject(new Error("The preparation worker exit is unproved."));
      if (job.token !== undefined && cancelledTokens.delete(job.token)) {
        pendingTokens.delete(job.token);
        reapedTokens.add(job.token);
        return reject(new JonesStagingStopped());
      }
      if (job.token !== undefined) pendingTokens.delete(job.token);
      const child = spawn();
      const joined = Promise.withResolvers<void>();
      const target: Running = { child, token: job.token ?? randomUUID(), kind: job.kind, exited: joined.promise, exit: () => joined.resolve(),
        didExit: false, admitted: false, stopRequested: false, held: false, hasResult: false, reject, clearDeadline: () => undefined };
      current = target;
      running = target;
      const timer = setTimeout(() => {
        target.failure = new Error("The preparation worker timed out.");
        void terminate(target, options.graceMs ?? TERMINATION_GRACE_MS, options.reapMs ?? 5_000).then((exited) => {
          if (!exited) { target.held = true; if (child.pid !== undefined) reject(new JonesStagingExitUnproved({ pid: child.pid, processGroup: child.pid }, target.admitted)); }
        });
      }, options.jobTimeoutMs ?? JOB_TIMEOUT_MS);
      target.clearDeadline = () => clearTimeout(timer);
      const onExit = (code: number | null, signal: string | null) => {
        if (target.didExit) return;
        target.didExit = true;
        reapedTokens.add(target.token);
        clearTimeout(timer);
        target.exit();
        if (running === target) running = undefined;
        if (target.hasResult) resolve(target.value);
        else if (target.admitted && child.pid !== undefined) reject(new JonesStagingExitUnproved({ pid: child.pid, processGroup: child.pid }, true));
        else reject(target.stopRequested ? new JonesStagingStopped() : target.failure ?? new Error(`The preparation worker exited before replying (${signal ?? `code ${code ?? "unknown"}`}).`));
      };
      child.onExit(onExit);
      child.onError?.((error) => {
        target.failure = error;
        // Spawn failure has no worker to join. An error from a spawned child never proves exit.
        if (child.pid === undefined) onExit(null, null);
      });
      child.onMessage((raw) => {
        if (typeof raw !== "object" || raw === null || !("jobId" in raw) || raw.jobId !== target.token || target.didExit) return;
        const message = raw as JonesStagingWorkerMessage;
        if (message.type === "admit-request" && target.kind === "prepare" && !target.admitted) {
          if (target.stopRequested) child.send({ type: "admit", jobId: target.token, decision: "stop" });
          else {
            // Admission is recorded before replying; Stop cannot race a granted mount/copy.
            target.admitted = true;
            void Promise.resolve().then(() => onVerifying?.()).then(() => {
              if (!target.didExit) child.send({ type: "admit", jobId: target.token, decision: "continue" });
            }).catch((cause: unknown) => {
              target.failure = cause instanceof Error ? cause : new Error("Copy admission could not be recorded.");
              target.admitted = false; target.stopRequested = true;
              if (!target.didExit) {
                try { child.send({ type: "admit", jobId: target.token, decision: "stop" }); } catch { /* Exit accounting still owns the slot. */ }
              }
            });
          }
        } else if (message.type === "result") { target.hasResult = true; target.value = message.value; }
        else if (message.type === "error") {
          const error = message.error;
          target.failure = error !== null && typeof error === "object" && typeof error.name === "string" && typeof error.message === "string"
            ? rebuildFailure(error) : new Error("The preparation worker returned an invalid failure.");
        }
      });
      void (async () => {
        try {
          if (child.pid !== undefined) await hooks?.onStarted?.({ pid: child.pid, processGroup: child.pid });
          if (target.didExit || target.stopRequested) return;
          child.send({ protocol: JONES_STAGING_WORKER_PROTOCOL, job: { ...job, token: target.token } });
        } catch (cause) {
          target.failure = cause instanceof Error ? cause : new Error("The preparation request could not be sent.");
          if (child.pid === undefined) onExit(null, null);
          else if (!await terminate(target, options.graceMs ?? TERMINATION_GRACE_MS, options.reapMs ?? 5_000)) {
            target.held = true; target.clearDeadline(); reject(new JonesStagingExitUnproved({ pid: child.pid, processGroup: child.pid }, target.admitted));
          }
        }
      })();
    });
    // Even a timeout's early outcome cannot release the serialized worker slot.
    const joined = outcome.then(() => current?.exited, () => current?.exited);
    return { outcome, joined };
  };
  return {
    run: (job, onVerifying, hooks) => {
      if (closed) return Promise.reject(new Error("The preparation worker is closed."));
      if (running?.held) return Promise.reject(new Error("The preparation worker exit is unproved."));
      if (job.token !== undefined) pendingTokens.add(job.token);
      const start = queue.then(() => execute(job, onVerifying, hooks));
      queue = start.then((run) => run.joined).catch(() => undefined);
      return start.then((run) => run.outcome) as never;
    },
    cancel: async (token) => {
      const target = running;
      if (pendingTokens.has(token)) { cancelledTokens.add(token); return "stopped"; }
      if (target === undefined || target.token !== token || target.kind !== "prepare") return "not-running";
      if (target.cancellation !== undefined) return target.cancellation;
      if (target.hasResult) return "completed";
      if (target.admitted) return "not-stoppable-phase";
      target.stopRequested = true;
      target.cancellation = (async () => {
        const exited = await terminate(target, options.stopGraceMs ?? 15_000, options.reapMs ?? 5_000);
        if (target.hasResult) return "completed" as const;
        if (exited) return "stopped" as const;
        target.held = true;
        target.clearDeadline();
        if (target.child.pid !== undefined) target.reject(new JonesStagingExitUnproved({ pid: target.child.pid, processGroup: target.child.pid }, target.admitted));
        return "exit-unproved" as const;
      })();
      return target.cancellation;
    },
    reaped: (token) => reapedTokens.has(token),
    heldWorker: () => running?.held && running.child.pid !== undefined ? { pid: running.child.pid, processGroup: running.child.pid } : undefined,
    close: async () => {
      closed = true;
      if (running !== undefined) await terminate(running, options.graceMs ?? TERMINATION_GRACE_MS, 0);
    },
  };
}
