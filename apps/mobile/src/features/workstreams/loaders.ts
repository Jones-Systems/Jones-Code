import type { T3WorkstreamListResult } from "@t3tools/contracts";
import { EnvironmentHttpConflictError } from "@t3tools/contracts";
import {
  appendWorkstreamListResult,
  loadWorkstreamAssignmentIndex,
  type WorkstreamAssignmentIndex,
  type WorkstreamAssignmentIndexFetchers,
} from "@t3tools/client-runtime/state/workstreams";
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

// The same classes the web registry treats as signed out; everything else is transient.
const AUTHORIZATION_FAILURES = new Set([
  "EnvironmentAuthInvalidError",
  "EnvironmentAuthorizationError",
  "EnvironmentHttpUnauthorizedError",
  "EnvironmentHttpForbiddenError",
  "EnvironmentScopeRequiredError",
]);

/**
 * Only lost authorization hides loaded Workstreams. Transport, timeout and server failures,
 * including a control-plane denial surfaced as an opaque internal error, keep the last good
 * load as stale: none of them proves that an assignment is absent.
 */
export function classifyMobileWorkstreamFailure(cause: unknown): "authorization" | "transient" {
  const value = cause && typeof cause === "object" ? (cause as Record<string, unknown>) : {};
  if (typeof value._tag === "string" && AUTHORIZATION_FAILURES.has(value._tag))
    return "authorization";
  const response = value.response as { status?: unknown } | undefined;
  const status = typeof value.status === "number" ? value.status : response?.status;
  return status === 401 || status === 403 ? "authorization" : "transient";
}

export interface MobileWorkstreamRegistry {
  readonly data: T3WorkstreamListResult;
  readonly index: WorkstreamAssignmentIndex | null;
}

/**
 * Reads the list and every stored assignment at one registry version. With `listOnly`
 * fallback, a transient index failure still yields the list so an environment without a
 * previous load can show and create Workstreams; its rows then stay unconfirmed.
 */
export async function loadMobileWorkstreamRegistry(
  fetchers: WorkstreamAssignmentIndexFetchers,
  options: { readonly signal: AbortSignal; readonly listOnlyFallback: boolean },
): Promise<MobileWorkstreamRegistry> {
  try {
    const loaded = await loadWorkstreamAssignmentIndex(fetchers, { signal: options.signal });
    return { data: loaded.list, index: loaded.index };
  } catch (cause) {
    throwIfAborted(options.signal);
    if (!options.listOnlyFallback || classifyMobileWorkstreamFailure(cause) === "authorization")
      throw cause;
    const data = await loadCompleteWorkstreamList(
      (cursor) => fetchers.listWorkstreams({ cursor, signal: options.signal }),
      { signal: options.signal },
    );
    return { data, index: null };
  }
}

export type MobileWorkstreamLoadOutcome<Snapshot> =
  | { readonly kind: "loaded"; readonly snapshot: Snapshot }
  | { readonly kind: "failed"; readonly cause: unknown };

/**
 * Applies one refresh round at once. A successful load replaces its environment's snapshot; a
 * transient failure keeps the previous one marked stale; lost authorization removes it.
 * Environments without an outcome keep their snapshot unless `retained` rejects them.
 */
export function settleMobileWorkstreamSnapshots<Key, Snapshot extends { readonly stale: boolean }>(
  previous: ReadonlyMap<Key, Snapshot>,
  outcomes: ReadonlyMap<Key, MobileWorkstreamLoadOutcome<Snapshot>>,
  retained: (key: Key) => boolean = () => true,
): ReadonlyMap<Key, Snapshot> {
  const next = new Map<Key, Snapshot>();
  for (const [key, snapshot] of previous) if (retained(key)) next.set(key, snapshot);
  for (const [key, outcome] of outcomes) {
    if (!retained(key)) continue;
    if (outcome.kind === "loaded") next.set(key, outcome.snapshot);
    else if (classifyMobileWorkstreamFailure(outcome.cause) === "authorization") next.delete(key);
    else {
      const last = next.get(key);
      if (last !== undefined && !last.stale) next.set(key, { ...last, stale: true });
    }
  }
  return next;
}
