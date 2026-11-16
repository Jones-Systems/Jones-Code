// @effect-diagnostics globalTimers:off - The scheduler runs in web and native clients, outside an Effect runtime.
import type { JonesInstallPlanDevice } from "@t3tools/contracts/jones/installPlan";
import type { JonesUpdateObservation } from "../updates/jonesUpdates.ts";
import { operationOutstanding } from "./model.ts";

/** Owner-set status refresh cadence. Tune values here; tests read them from this object. */
export const STATUS_POLL_CADENCE = {
  /** Nothing in progress and the window is visible and focused. */
  idleVisibleMs: 10 * 60_000,
  /** A device is preparing, downloading, verifying, installing or restarting. */
  activeMs: 60_000,
  /** Nothing in progress and the window is hidden or unfocused. */
  idleHiddenMs: 30 * 60_000,
  /** Work in progress while the window is hidden or unfocused. */
  activeHiddenMs: 2 * 60_000,
  /** Regaining focus or the network refreshes a device only after this long without success. */
  regainedAfterMs: 2 * 60_000,
  /** Symmetric random spread applied to every delay. */
  jitter: 0.1,
  /** Consecutive-failure backoff: doubles from the minimum up to the maximum; success resets it. */
  failureBackoffMinMs: 60_000,
  failureBackoffMaxMs: 10 * 60_000,
  /** One device refresh is abandoned after this long and counts as a failure. */
  timeoutMs: 10_000,
  maxDevices: 16,
  maxConcurrent: 4,
} as const;
export type StatusPollCadence = { readonly [K in keyof typeof STATUS_POLL_CADENCE]: number };

export interface StatusPollContext {
  /** The device has work in progress or an outstanding ledger operation. */
  readonly active: boolean;
  /** The window is visible and focused. */
  readonly attention: boolean;
  /** Consecutive failed refreshes for this device. */
  readonly failures: number;
}

/** Delay before the next refresh of one device. `random` returns a value in [0, 1). */
export function statusPollDelayMs(
  context: StatusPollContext,
  random: () => number = Math.random,
  cadence: StatusPollCadence = STATUS_POLL_CADENCE,
): number {
  const base =
    context.failures > 0
      ? Math.min(
          cadence.failureBackoffMaxMs,
          cadence.failureBackoffMinMs * 2 ** Math.min(context.failures - 1, 16),
        )
      : context.active
        ? context.attention
          ? cadence.activeMs
          : cadence.activeHiddenMs
        : context.attention
          ? cadence.idleVisibleMs
          : cadence.idleHiddenMs;
  return Math.round(base * (1 + cadence.jitter * (2 * random() - 1)));
}

const ACTIVE_OPERATION_PHASES = new Set([
  "accepted",
  "running",
  "stop-requested",
  "adopting",
  "handed-off",
]);
const ACTIVE_HOST_PHASES = new Set(["preparing", "downloading", "verifying", "installing"]);

/** Whether a device is mid-operation or expected to restart, by host status or the plan ledger. */
export function deviceStatusActive(
  observation: JonesUpdateObservation | undefined,
  planDevice: Pick<JonesInstallPlanDevice, "operations"> | undefined,
): boolean {
  const state = observation?.state;
  return (
    (state?.operation !== undefined && ACTIVE_OPERATION_PHASES.has(state.operation.phase)) ||
    (state !== null && state !== undefined && ACTIVE_HOST_PHASES.has(state.phase)) ||
    (planDevice?.operations.some(operationOutstanding) ?? false)
  );
}

export interface StatusPollHealth {
  readonly failures: number;
  readonly lastSuccessAt?: number;
}

export interface StatusPollerOptions {
  /** Devices to refresh; only the first `maxDevices` are polled. */
  readonly devices: () => readonly string[];
  readonly isActive: (id: string) => boolean;
  readonly attention: () => boolean;
  /** Reads one device. A rejection counts as a failure. It must never start host work. */
  readonly tick: (id: string, signal: AbortSignal) => Promise<void>;
  readonly onHealth?: (id: string, health: StatusPollHealth) => void;
  readonly now: () => number;
  readonly random?: () => number;
  readonly setTimer?: (run: () => void, ms: number) => unknown;
  readonly clearTimer?: (handle: unknown) => void;
  readonly cadence?: StatusPollCadence;
}

interface DeviceSchedule {
  timer?: unknown;
  queued: boolean;
  inFlight: boolean;
  failures: number;
  lastSuccessAt?: number;
  /** Start of the current wait; the next refresh is due one delay after it. */
  since: number;
  /** Jitter draw for the current wait, kept so rescheduling does not re-roll it. */
  draw: number;
}

/**
 * Per-device status refresh scheduler: single-flight, bounded concurrency, timeout and failure
 * backoff. It decides only when to read; the injected tick decides what a read does.
 */
export function createStatusPoller(options: StatusPollerOptions) {
  const cadence = options.cadence ?? STATUS_POLL_CADENCE;
  const random = options.random ?? Math.random;
  const setTimer = options.setTimer ?? ((run, ms) => setTimeout(run, ms));
  const clearTimer =
    options.clearTimer ?? ((handle) => clearTimeout(handle as ReturnType<typeof setTimeout>));
  const schedules = new Map<string, DeviceSchedule>();
  const queue: string[] = [];
  let running = 0;
  let closed = false;

  const clear = (schedule: DeviceSchedule) => {
    if (schedule.timer !== undefined) clearTimer(schedule.timer);
    schedule.timer = undefined;
  };
  const schedule = (id: string) => {
    const entry = schedules.get(id);
    if (closed || entry === undefined || entry.inFlight || entry.queued) return;
    clear(entry);
    const delay = statusPollDelayMs(
      {
        active: options.isActive(id),
        attention: options.attention(),
        failures: entry.failures,
      },
      () => entry.draw,
      cadence,
    );
    entry.timer = setTimer(
      () => {
        entry.timer = undefined;
        enqueue(id);
      },
      Math.max(0, entry.since + delay - options.now()),
    );
  };
  const enqueue = (id: string) => {
    const entry = schedules.get(id);
    if (closed || entry === undefined || entry.inFlight || entry.queued) return;
    clear(entry);
    entry.queued = true;
    queue.push(id);
    pump();
  };
  const pump = () => {
    if (closed) return;
    while (running < cadence.maxConcurrent && queue.length > 0) {
      const id = queue.shift()!;
      const entry = schedules.get(id);
      if (entry === undefined) continue;
      entry.queued = false;
      entry.inFlight = true;
      running++;
      void run(id, entry);
    }
  };
  const run = async (id: string, entry: DeviceSchedule) => {
    const controller = new AbortController();
    let timeout: unknown;
    let ok = false;
    try {
      await Promise.race([
        options.tick(id, controller.signal),
        new Promise<never>((_, reject) => {
          timeout = setTimer(() => {
            controller.abort();
            reject(new Error("Status refresh timed out."));
          }, cadence.timeoutMs);
        }),
      ]);
      ok = true;
    } catch {
      ok = false;
    } finally {
      if (timeout !== undefined) clearTimer(timeout);
    }
    running--;
    entry.inFlight = false;
    if (ok) {
      entry.failures = 0;
      entry.lastSuccessAt = options.now();
    } else entry.failures++;
    entry.since = options.now();
    entry.draw = random();
    if (!closed && schedules.get(id) === entry) {
      options.onHealth?.(id, {
        failures: entry.failures,
        ...(entry.lastSuccessAt === undefined ? {} : { lastSuccessAt: entry.lastSuccessAt }),
      });
      schedule(id);
    }
    pump();
  };

  /** Track the current device list: new devices wait one interval, removed ones stop. */
  const sync = () => {
    if (closed) return;
    const ids = new Set(options.devices().slice(0, cadence.maxDevices));
    for (const [id, entry] of schedules)
      if (!ids.has(id)) {
        clear(entry);
        schedules.delete(id);
      }
    for (const id of ids)
      if (!schedules.has(id)) {
        schedules.set(id, {
          queued: false,
          inFlight: false,
          failures: 0,
          since: options.now(),
          draw: random(),
        });
        schedule(id);
      }
  };

  return {
    sync,
    /** Recompute waits after activity or device work changed; nothing is read early. */
    reschedule() {
      sync();
      for (const id of schedules.keys()) schedule(id);
    },
    /** Focus or network regained: refresh devices that have not succeeded recently. */
    regained() {
      sync();
      const now = options.now();
      for (const [id, entry] of schedules)
        if (now - (entry.lastSuccessAt ?? entry.since) > cadence.regainedAfterMs) enqueue(id);
        else schedule(id);
    },
    health(id: string): StatusPollHealth | undefined {
      const entry = schedules.get(id);
      return entry === undefined
        ? undefined
        : {
            failures: entry.failures,
            ...(entry.lastSuccessAt === undefined ? {} : { lastSuccessAt: entry.lastSuccessAt }),
          };
    },
    close() {
      closed = true;
      for (const entry of schedules.values()) clear(entry);
      schedules.clear();
      queue.length = 0;
    },
  };
}
export type StatusPoller = ReturnType<typeof createStatusPoller>;
