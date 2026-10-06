import type { T3WorkstreamListResult } from "@t3tools/contracts";
import { EnvironmentHttpConflictError } from "@t3tools/contracts";
import { appendWorkstreamListResult } from "@t3tools/client-runtime/state/workstreams";
import * as Schema from "effect/Schema";

const isCursorStale = Schema.is(EnvironmentHttpConflictError);
const CURSOR_RESTART_ATTEMPTS = 3;

const isRestartableCursorStale = (cause: unknown): boolean =>
  isCursorStale(cause) && cause.message === "workstream_cursor_stale";

export interface CursorRestartOptions {
  readonly signal?: AbortSignal;
  readonly wait?: (delayMs: number, signal?: AbortSignal) => Promise<void>;
}

const throwIfAborted = (signal?: AbortSignal): void => signal?.throwIfAborted();

const waitForRetry = (delayMs: number, signal?: AbortSignal): Promise<void> =>
  new Promise<void>((resolve, reject) => {
    throwIfAborted(signal);
    const abort = () => {
      globalThis.clearTimeout(timer);
      reject(signal?.reason ?? new DOMException("The operation was aborted.", "AbortError"));
    };
    const timer = globalThis.setTimeout(() => {
      signal?.removeEventListener("abort", abort);
      resolve();
    }, delayMs);
    signal?.addEventListener("abort", abort, { once: true });
  });

async function withCursorRestart<A>(
  load: () => Promise<A>,
  options: CursorRestartOptions = {},
): Promise<A> {
  const wait = options.wait ?? waitForRetry;
  for (let attempt = 0; attempt < CURSOR_RESTART_ATTEMPTS; attempt += 1) {
    throwIfAborted(options.signal);
    try {
      const result = await load();
      throwIfAborted(options.signal);
      return result;
    } catch (cause) {
      throwIfAborted(options.signal);
      if (!isRestartableCursorStale(cause) || attempt + 1 === CURSOR_RESTART_ATTEMPTS) throw cause;
      const delayMs = 50 * 2 ** attempt;
      if (options.signal === undefined) await wait(delayMs);
      else await wait(delayMs, options.signal);
    }
  }
  throw new Error("Workstream cursor restart policy is invalid.");
}

export async function loadCompleteWorkstreamList(
  load: (cursor?: string) => Promise<T3WorkstreamListResult>,
  options: CursorRestartOptions = {},
): Promise<T3WorkstreamListResult> {
  return withCursorRestart(async () => {
    throwIfAborted(options.signal);
    let result = await load();
    throwIfAborted(options.signal);
    const cursors = new Set<string>();
    while (result.nextCursor !== null) {
      throwIfAborted(options.signal);
      if (cursors.has(result.nextCursor)) throw new Error("Workstream list cursor repeated.");
      cursors.add(result.nextCursor);
      result = appendWorkstreamListResult(result, await load(result.nextCursor));
      throwIfAborted(options.signal);
    }
    return result;
  }, options);
}
