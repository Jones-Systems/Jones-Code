import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as DesktopBackendPool from "../../backend/DesktopBackendPool.ts";
import * as DesktopObservability from "../../app/DesktopObservability.ts";
import { jonesNativeBootstrapCredential, prepareJonesQuit } from "./jonesNativePreparation.ts";

const { logInfo, logWarning } =
  DesktopObservability.makeComponentLogger("desktop-quit-preparation");

export const prepareLocalBackendsForQuit = Effect.fn("desktop.quit.prepareLocalBackends")(
  function* (): Effect.fn.Return<void, never, DesktopBackendPool.DesktopBackendPool> {
    const pool = yield* DesktopBackendPool.DesktopBackendPool;
    const instances = yield* pool.list;
    yield* Effect.forEach(
      instances,
      (instance) =>
        Effect.gen(function* () {
          if (!(yield* instance.snapshot).ready) return;
          const config = yield* instance.currentConfig;
          if (Option.isNone(config)) return;
          const listener = config.value.httpBaseUrl;
          if (
            listener.protocol !== "http:" ||
            !["127.0.0.1", "[::1]", "localhost"].includes(listener.hostname)
          ) {
            return;
          }
          const bootstrapToken = yield* jonesNativeBootstrapCredential(config.value.bootstrap);
          const result = yield* Effect.tryPromise(() =>
            prepareJonesQuit({ listener: listener.href, bootstrapToken, timeoutMs: 1500 }),
          );
          yield* logInfo("backend quit preparation finished", {
            instanceId: instance.id,
            prepared: result.prepared,
            threadCount: result.threadCount,
          });
        }).pipe(
          Effect.catchCause(() =>
            logWarning("backend quit preparation unavailable", { instanceId: instance.id }),
          ),
        ),
      { concurrency: "unbounded", discard: true },
    );
  },
  // Finalizers are uninterruptible; the preparation wait must still be bounded.
  Effect.interruptible,
  Effect.timeoutOption("3 seconds"),
  Effect.flatMap((result) =>
    Option.isNone(result) ? logWarning("backend quit preparation wait expired") : Effect.void,
  ),
  Effect.catchCause(() => logWarning("backend quit preparation unavailable")),
);
