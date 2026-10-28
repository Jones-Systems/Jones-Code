// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native worker deadlines must still run while a caller is masked or awaiting synchronous SQLite.
import * as NodeTimers from "node:timers";

import type { Worker } from "node:worker_threads";

/** A timeout is uncertainty, never permission to replace a native connection. */
export const retireReaderWorker = async (
  worker: Worker,
  timeoutMs = 10_000,
): Promise<boolean> => {
  let timer: ReturnType<typeof NodeTimers.setTimeout> | undefined;
  try {
    return await Promise.race([
      worker.terminate().then(() => true, () => false),
      new Promise<false>((resolve) => { timer = NodeTimers.setTimeout(() => resolve(false), timeoutMs); }),
    ]);
  } catch {
    return false;
  } finally {
    NodeTimers.clearTimeout(timer);
  }
};
