// @effect-diagnostics nodeBuiltinImport:off -- Owns the supplied synthetic receipt watcher until observation or cancellation.
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

export async function watchFile(file: string, signal: AbortSignal) {
  const watcher = NodeFS.watch(NodePath.dirname(file));
  let rejectWait: ((cause: Error) => void) | undefined;
  const aborted = () => rejectWait?.(new Error("Synthetic receipt observation cancelled."));
  let recheck: ReturnType<typeof setInterval> | undefined;
  try {
    await new Promise<void>((resolve, reject) => {
      rejectWait = reject;
      const inspect = () => {
        void NodeFSP.stat(file).then(
          () => resolve(),
          (error: NodeJS.ErrnoException) => {
            if (error.code !== "ENOENT") reject(error);
          },
        );
      };
      watcher.on("change", inspect);
      watcher.on("error", reject);
      // @effect-diagnostics-next-line globalTimers:off -- Native fs.watch test observer uses a timer to cover coalesced events and is always cleared in finally.
      recheck = setInterval(inspect, 250);
      signal.addEventListener("abort", aborted, { once: true });
      if (signal.aborted) aborted();
      else inspect();
    });
  } finally {
    clearInterval(recheck);
    signal.removeEventListener("abort", aborted);
    watcher.close();
  }
}
