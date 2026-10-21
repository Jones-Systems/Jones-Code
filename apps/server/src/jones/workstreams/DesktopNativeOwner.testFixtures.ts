// @effect-diagnostics nodeBuiltinImport:off -- Owns the synthetic inherited-pipe experiment and closes its captured child.
// @effect-diagnostics globalTimers:off -- Native deadlines and delayed replies are cleared before returning.
import * as NodeTimers from "node:timers";
import * as NodeChildProcess from "node:child_process";
import type * as NodeStream from "node:stream";

export async function observeLateOwnerReplies(script: string) {
  const child = NodeChildProcess.spawn(process.execPath, ["--input-type=module", "-e", script], {
    stdio: [
      "pipe",
      "pipe",
      "pipe",
      "ignore",
      "ignore",
      "ignore",
      "ignore",
      "ignore",
      "pipe",
      "pipe",
    ],
  });
  const closed = new Promise<void>((resolve) => child.once("close", () => resolve()));
  const descriptors: readonly (NodeStream.Readable | NodeStream.Writable | null | undefined)[] =
    child.stdio;
  const replies = descriptors[8] as NodeStream.Writable;
  const requests = descriptors[9] as NodeStream.Readable;
  const observations: Record<string, unknown>[] = [];
  const lateWrites: { sequence: number; elapsedMs: number; error: string | null }[] = [];
  const delayed: ReturnType<typeof setTimeout>[] = [];
  let deadline: ReturnType<typeof setTimeout> | undefined;
  let stdout = "";
  let requestBuffer = "";
  let stderr = "";
  let requestCount = 0;
  let freshReply: string | undefined;
  let allowFreshReply = false;
  let childAliveAfterLate = false;
  // A timed-out pre-fix reader destroys its pipe; retain that actual write outcome.
  replies.on("error", () => {});
  try {
    await new Promise<void>((resolve, reject) => {
      deadline = NodeTimers.setTimeout(
        () => reject(new Error(`Delayed owner fixture timed out: ${stderr}`)),
        15000,
      );
      child.once("error", reject);
      child.stderr!.on("data", (chunk) => {
        stderr += String(chunk);
      });
      child.stdout!.on("data", (chunk) => {
        stdout += String(chunk);
        let newline: number;
        while ((newline = stdout.indexOf("\n")) >= 0) {
          const observation = JSON.parse(stdout.slice(0, newline)) as Record<string, unknown>;
          stdout = stdout.slice(newline + 1);
          observations.push(observation);
          if (observation.phase === "after-late") {
            childAliveAfterLate = child.exitCode === null && child.signalCode === null;
            allowFreshReply = true;
            if (freshReply !== undefined) {
              replies.write(freshReply);
              freshReply = undefined;
            }
          }
        }
      });
      requests.on("data", (chunk) => {
        requestBuffer += String(chunk);
        let newline: number;
        while ((newline = requestBuffer.indexOf("\n")) >= 0) {
          const request = JSON.parse(requestBuffer.slice(0, newline));
          requestBuffer = requestBuffer.slice(newline + 1);
          requestCount++;
          const reply =
            JSON.stringify({ ...request, parentPid: process.pid, runId: "d".repeat(64) }) + "\n";
          if (request.sequence === 1) replies.write(reply);
          else if (request.sequence === 2 || request.sequence === 4) {
            allowFreshReply = false;
            const sentAt = performance.now();
            delayed.push(
              NodeTimers.setTimeout(() => {
                replies.write(reply, (error) => {
                  lateWrites.push({
                    sequence: request.sequence,
                    elapsedMs: performance.now() - sentAt,
                    error: error?.message ?? null,
                  });
                  if (error) child.stdin!.write("prove\n");
                });
              }, 3500),
            );
          } else {
            // Hold fresh proof until the live child proves that late proof alone is insufficient.
            freshReply = reply;
            child.stdin!.write("prove\n");
            if (allowFreshReply) {
              replies.write(freshReply);
              freshReply = undefined;
            }
          }
        }
      });
      child.once("close", (code) =>
        code === 0 ? resolve() : reject(new Error(`Child exited ${code}: ${stderr}`)),
      );
    });
    return { observations, lateWrites, requestCount, childAliveAfterLate, stderr, child };
  } finally {
    if (deadline !== undefined) clearTimeout(deadline);
    for (const timer of delayed) clearTimeout(timer);
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    await closed;
    for (const stream of child.stdio) stream?.destroy();
  }
}
