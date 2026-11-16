/** Abort stops qualification acceptance, not an already admitted external OS operation. */
export function untilSamplingAbort<T>(pending: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal?.aborted) {
    void pending.catch(() => {});
    return Promise.reject(new Error("Sampling drain aborted"));
  }
  return new Promise((resolve, reject) => {
    const dispose = () => signal?.removeEventListener("abort", abort);
    const abort = () => {
      dispose();
      reject(new Error("Sampling drain aborted"));
    };
    signal?.addEventListener("abort", abort, { once: true });
    void pending.then(
      (value) => {
        dispose();
        resolve(value);
      },
      (cause) => {
        dispose();
        reject(cause);
      },
    );
  });
}

/** Qualification-only read admission; this never proves process exit or releases runtime custody. */
export class CapturedSampling {
  private accepting = true;
  private readonly pending = new Set<Promise<void>>();

  acquire(): (() => void) | undefined {
    if (!this.accepting) return undefined;
    const gate = Promise.withResolvers<void>();
    this.pending.add(gate.promise);
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.pending.delete(gate.promise);
      gate.resolve();
    };
  }

  closeAndDrain(signal?: AbortSignal): Promise<void> {
    this.accepting = false;
    if (this.pending.size === 0) return Promise.resolve();
    return untilSamplingAbort(
      Promise.all(this.pending).then(() => {}),
      signal,
    );
  }
}
