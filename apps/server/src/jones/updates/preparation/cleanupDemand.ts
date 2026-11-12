import type { PreparationSweep } from "./protocol.ts";

/** A cleanup transition buys one pass and at most two retries, driven by the service ticks. */
export function preparationCleanupDemand(sweep: () => Promise<PreparationSweep>) {
  let demanded = true; // One startup pass, even when there is no operation ledger.
  let retries = 2;
  let pending: Promise<void> | undefined;
  const request = () => {
    demanded = true;
    retries = 2;
  };
  const flush = (): Promise<void> => {
    if (pending !== undefined) return pending;
    if (!demanded) return Promise.resolve();
    demanded = false;
    const retry = () => {
      // New demand has its own retry allowance and must survive this pass.
      if (!demanded && retries > 0) {
        retries -= 1;
        demanded = true;
      }
    };
    pending = Promise.resolve()
      .then(sweep)
      .then(
        (result) => {
          if (result.failed > 0) retry();
          // Expected retained downloads and standing holds deliberately raise no demand.
        },
        (cause: unknown) => {
          retry();
          throw cause;
        },
      )
      .finally(() => {
        pending = undefined;
      });
    return pending;
  };
  return { request, flush };
}
