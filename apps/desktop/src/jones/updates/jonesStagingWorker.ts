// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - Forked Node-mode child; no Effect runtime.
// Runs one staging job in a separate low-priority process group so archive hashing, extraction
// and payload removal never block Electron's main process.
import * as NodeOS from "node:os";

import { JonesActionsError } from "@t3tools/shared/jones/jonesActions";

import { runJonesStagingJob, type JonesStagingJob } from "./stagingJobs.ts";
import {
  JONES_STAGING_WORKER_PROTOCOL,
  type JonesStagingWorkerMessage,
} from "./stagingWorkerProtocol.ts";

const send = (message: JonesStagingWorkerMessage) =>
  new Promise<void>((resolve) => {
    if (process.send === undefined || !process.connected) return resolve();
    process.send(message, undefined, {}, () => resolve());
  });

try {
  // Niceness lowers scheduling priority for this process and its gh, hdiutil and ditto
  // children. It is not a CPU cap: an idle core is still used.
  NodeOS.setPriority(NodeOS.constants.priority.PRIORITY_LOW);
} catch {
  /* Priority is best effort; the job runs at normal priority. */
}

let finished = false;
// The parent signals the whole group: children stop, the job's own cleanup runs, then exit.
process.on("SIGTERM", () => {
  setTimeout(() => process.exit(143), 10_000).unref();
});
process.once("disconnect", () => {
  if (!finished) process.exit(1);
});

process.once("message", (raw: unknown) => {
  const job = raw as { readonly protocol?: unknown; readonly job?: JonesStagingJob };
  void (async () => {
    if (job.protocol !== JONES_STAGING_WORKER_PROTOCOL || job.job === undefined)
      throw new Error("Unknown staging worker request.");
    const value = await runJonesStagingJob(job.job, {
      onVerifying: () => send({ type: "phase", phase: "verifying" }),
    });
    await send({ type: "result", value });
  })()
    .catch((cause: unknown) =>
      send({
        type: "error",
        error: {
          name: cause instanceof Error ? cause.name : "Error",
          message: cause instanceof Error ? cause.message : "Staging failed.",
          ...(cause instanceof JonesActionsError ? { reason: cause.reason } : {}),
          ...(typeof cause === "object" &&
          cause !== null &&
          "code" in cause &&
          typeof cause.code === "string"
            ? { code: cause.code }
            : {}),
        },
      }),
    )
    .finally(() => {
      finished = true;
      process.disconnect?.();
    });
});
