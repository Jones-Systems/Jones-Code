// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalTimers:off
import * as NodeChildProcess from "node:child_process";

const TERMINATE_GRACE_MS = 2_000;
const REAP_GRACE_MS = 2_000;

export interface CapturedProcessResult {
  /** "unreaped": the captured child outlived SIGKILL plus its reap grace. */
  readonly outcome: "exited" | "deadline" | "output-limit" | "spawn-failed" | "unreaped";
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly stdout: string;
  readonly stderr: string;
}

/**
 * Runs one fixed command with an empty environment and bounded output. On its
 * deadline or output limit only the captured child is signalled: SIGTERM, then
 * SIGKILL after a grace period. The result settles once that child is reaped,
 * or reports "unreaped" after a final bound; timers and output listeners end with it.
 */
export function runCapturedProcess(input: {
  readonly command: string;
  readonly args: readonly string[];
  readonly descriptors?: readonly number[];
  readonly deadlineMs: number;
  readonly outputLimit: number;
  /** sudo relays SIGTERM only from a sender outside the command's process group. */
  readonly newProcessGroup?: boolean;
}): Promise<CapturedProcessResult> {
  return new Promise((resolve) => {
    const output = { stdout: "", stderr: "" };
    let stopped: "deadline" | "output-limit" | undefined;
    let settled = false;
    const timers: NodeJS.Timeout[] = [];
    const child = NodeChildProcess.spawn(input.command, [...input.args], {
      cwd: "/",
      env: {},
      detached: input.newProcessGroup === true,
      stdio: ["ignore", "pipe", "pipe", ...(input.descriptors ?? [])],
    });
    const streams = [child.stdout, child.stderr];
    const detachOutput = () => {
      for (const stream of streams) {
        stream?.removeAllListeners("data");
        stream?.destroy();
      }
    };
    const settle = (
      outcome: CapturedProcessResult["outcome"],
      code: number | null = null,
      signal: NodeJS.Signals | null = null,
    ) => {
      if (settled) return;
      settled = true;
      for (const timer of timers) clearTimeout(timer);
      detachOutput();
      resolve({ outcome, code, signal, ...output });
    };
    const stop = (reason: "deadline" | "output-limit") => {
      if (stopped !== undefined || settled) return;
      stopped = reason;
      detachOutput();
      child.kill("SIGTERM");
      timers.push(setTimeout(() => child.kill("SIGKILL"), TERMINATE_GRACE_MS));
      timers.push(setTimeout(() => settle("unreaped"), TERMINATE_GRACE_MS + REAP_GRACE_MS));
    };
    timers.push(setTimeout(() => stop("deadline"), input.deadlineMs));
    for (const [index, name] of (["stdout", "stderr"] as const).entries()) {
      streams[index]?.on("data", (data: Buffer) => {
        output[name] += data.toString("utf8");
        if (Buffer.byteLength(output[name]) > input.outputLimit) stop("output-limit");
      });
    }
    // A failed signal also reports here; only a failed spawn ends the run.
    child.on("error", () => {
      if (child.pid === undefined) settle("spawn-failed");
    });
    child.once("close", (code, signal) => settle(stopped ?? "exited", code, signal));
  });
}
