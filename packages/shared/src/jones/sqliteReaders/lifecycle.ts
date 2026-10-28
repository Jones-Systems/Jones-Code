// @effect-diagnostics-next-line nodeBuiltinImport:off -- This native Promise boundary bounds termination with a cleanup-owned real-time deadline.
import * as NodeTimers from "node:timers";

import type { Worker } from "node:worker_threads";

/** A timeout is uncertainty, never permission to replace a native connection. */
export const retireReaderWorker = async (worker: Worker, timeoutMs = 10_000): Promise<boolean> => {
  let timer: ReturnType<typeof NodeTimers.setTimeout> | undefined;
  try {
    return await Promise.race([
      worker.terminate().then(
        () => true,
        () => false,
      ),
      new Promise<false>((resolve) => {
        // @effect-diagnostics-next-line globalTimers:off -- Worker termination must time out independently of any caller fiber or Effect test clock; finally clears this timer.
        timer = NodeTimers.setTimeout(() => resolve(false), timeoutMs);
      }),
    ]);
  } catch {
    return false;
  } finally {
    NodeTimers.clearTimeout(timer);
  }
};
