// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - Owns forked preparation processes outside Effect fibers.
import * as NodeChildProcess from "node:child_process";
import { JonesActionsError } from "@t3tools/shared/jones/jonesActions";

import type { JonesStagingJob, JonesStagingJobResults } from "./stagingJobs.ts";
import {
  JONES_STAGING_WORKER_PROTOCOL,
  type JonesStagingWorkerMessage,
} from "./stagingWorkerProtocol.ts";

/** Bounds a wedged job; the gh transfer alone may take 15 minutes. */
const JOB_TIMEOUT_MS = 45 * 60_000;
const TERMINATION_GRACE_MS = 5_000;

export interface JonesStagingChild {
  readonly pid: number | undefined;
  readonly send: (message: unknown) => void;
  readonly onMessage: (listener: (message: unknown) => void) => void;
  readonly onExit: (listener: (code: number | null, signal: string | null) => void) => void;
  /** Signals the child's whole process group, which includes its gh/hdiutil/ditto children. */
  readonly signal: (signal: NodeJS.Signals) => void;
}

export function forkJonesStagingChild(
  workerPath: string,
  options: { readonly execPath?: string; readonly electron?: boolean } = {},
): JonesStagingChild {
  const child = NodeChildProcess.fork(workerPath, [], {
    ...(options.execPath === undefined ? {} : { execPath: options.execPath }),
    env: options.electron === false ? process.env : { ...process.env, ELECTRON_RUN_AS_NODE: "1" },
    execArgv: [],
    // Its own process group, so termination reaches the transfer and mount children too.
    detached: true,
    stdio: ["ignore", "ignore", "inherit", "ipc"],
  });
  return {
    pid: child.pid,
    send: (message) => child.send(message as NodeChildProcess.Serializable),
    onMessage: (listener) => child.on("message", listener),
    onExit: (listener) => {
      child.once("exit", (code, signal) => listener(code, signal));
      child.once("error", () => listener(null, null));
    },
    signal: (signal) => {
      try {
        if (child.pid !== undefined) process.kill(-child.pid, signal);
      } catch {
        child.kill(signal);
      }
    },
  };
}

function rebuildFailure(error: {
  readonly name: string;
  readonly message: string;
  readonly code?: string;
  readonly reason?: JonesActionsError["reason"];
}): Error {
  if (error.reason !== undefined) return new JonesActionsError(error.reason);
  const failure = new Error(error.message.slice(0, 512));
  failure.name = error.name.slice(0, 64);
  if (error.code !== undefined) Object.assign(failure, { code: error.code });
  return failure;
}

export interface JonesStagingWorker {
  readonly run: <K extends JonesStagingJob["kind"]>(
    job: Extract<JonesStagingJob, { readonly kind: K }>,
    onVerifying?: () => void | Promise<void>,
  ) => Promise<JonesStagingJobResults[K]>;
  /** Stops the running job's process group and refuses further jobs. */
  readonly close: () => Promise<void>;
}

/**
 * The single preparation worker of this desktop: jobs run one at a time, each in a fresh
 * child process that exits with its job.
 */
export function createJonesStagingWorker(
  spawn: () => JonesStagingChild,
  options: { readonly jobTimeoutMs?: number; readonly graceMs?: number } = {},
): JonesStagingWorker {
  let queue: Promise<unknown> = Promise.resolve();
  let closed = false;
  let running: { readonly child: JonesStagingChild; readonly exited: Promise<void> } | undefined;

  const stop = async (target: NonNullable<typeof running>) => {
    target.child.signal("SIGTERM");
    let timer: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([
      target.exited,
      new Promise<void>((resolve) => {
        timer = setTimeout(() => {
          target.child.signal("SIGKILL");
          resolve();
        }, options.graceMs ?? TERMINATION_GRACE_MS);
      }),
    ]);
    clearTimeout(timer);
  };

  const execute = <T>(job: JonesStagingJob, onVerifying?: () => void | Promise<void>) =>
    new Promise<T>((resolve, reject) => {
      if (closed) return reject(new Error("The preparation worker is closed."));
      let settled = false;
      let exit!: () => void;
      const exited = new Promise<void>((resolveExit) => (exit = resolveExit));
      const child = spawn();
      const current = { child, exited };
      running = current;
      const timer = setTimeout(() => {
        finish(new Error("The preparation worker timed out."));
        void stop(current);
      }, options.jobTimeoutMs ?? JOB_TIMEOUT_MS);
      const finish = (failure: Error | undefined, value?: T) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        if (failure === undefined) resolve(value as T);
        else reject(failure);
      };
      child.onMessage((raw) => {
        const message = raw as JonesStagingWorkerMessage;
        if (message.type === "phase") void Promise.resolve(onVerifying?.()).catch(() => undefined);
        else if (message.type === "result") finish(undefined, message.value as T);
        else if (message.type === "error") finish(rebuildFailure(message.error));
      });
      child.onExit((code, signal) => {
        exit();
        if (running === current) running = undefined;
        finish(
          new Error(
            `The preparation worker exited before replying (${signal ?? `code ${code ?? "unknown"}`}).`,
          ),
        );
      });
      child.send({ protocol: JONES_STAGING_WORKER_PROTOCOL, job });
    });

  return {
    run: (job, onVerifying) => {
      const result = queue.then(() => execute(job, onVerifying));
      queue = result.catch(() => undefined);
      return result as never;
    },
    close: async () => {
      closed = true;
      if (running !== undefined) await stop(running);
    },
  };
}
