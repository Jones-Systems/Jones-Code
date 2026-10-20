import { EnvironmentId, type ThreadId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";

import type { EnvironmentShellState } from "../../state/shell.ts";
import { observeEnvironment } from "./detect.ts";
import { addArrivals, dismiss, mergeInbox, refreshThreads, visibleEntries } from "./inbox.ts";
import {
  SettlementOriginDetails,
  BASELINE_DEBOUNCE_MS,
  decodeSettlementBaseline,
  decodeSettlementInbox,
  emptyInbox,
  encodeSettlementBaseline,
  encodeSettlementInbox,
  type EnvironmentSettlementBaseline,
  type SettlementEntry,
  type SettlementArrival,
  type SettlementOriginResolver,
  type SettlementInboxState,
} from "./model.ts";

export interface SettlementNotificationStore {
  loadInbox(): Promise<string | null> | string | null;
  saveInbox(json: string): Promise<void> | void;
  loadBaseline(envId: EnvironmentId): Promise<string | null> | string | null;
  saveBaseline(envId: EnvironmentId, json: string): Promise<void> | void;
  removeBaseline(envId: EnvironmentId): Promise<void> | void;
  listBaselineEnvironmentIds(): Promise<string[]> | string[];
  subscribeExternalInbox?(listener: (json: string | null) => void): () => void;
}

export interface SettlementNotificationSnapshot {
  readonly visible: readonly SettlementEntry[];
  readonly persistence: "loading" | "ok" | "degraded";
}

const isOriginDetails = Schema.is(SettlementOriginDetails);

export function createSettlementNotificationController(deps: {
  store: SettlementNotificationStore;
  now?: () => number;
  log?: (event: string, detail?: unknown) => void;
}) {
  const now = deps.now ?? Date.now;
  const { store } = deps;
  const listeners = new Set<() => void>();
  const errors = new Set<string>();
  const baselines = new Map<EnvironmentId, EnvironmentSettlementBaseline | null>();
  const baselineLoads = new Map<EnvironmentId, Promise<void>>();
  const dirtyBaselines = new Map<EnvironmentId, EnvironmentSettlementBaseline>();
  const observations = new Map<
    EnvironmentId,
    {
      state: EnvironmentShellState;
      resolveOrigins?: SettlementOriginResolver;
    }
  >();
  const pendingClassifications = new Map<EnvironmentId, number>();
  const confirmed = new Map<EnvironmentId, EnvironmentShellState>();
  const staleSince = new Map<EnvironmentId, number>();
  const catalog = new Set<EnvironmentId>();
  let inbox = emptyInbox();
  let inboxLoaded = false;
  let inboxRevision = 0;
  let changeRevision = 0;
  let persistedInboxRevision = 0;
  let hydrated = false;
  let hydration: Promise<void> | null = null;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let writeTail: Promise<void> = Promise.resolve();
  let snapshot: SettlementNotificationSnapshot = { visible: [], persistence: "loading" };

  function log(event: string, detail?: unknown) {
    try {
      deps.log?.(event, detail);
    } catch {
      // Diagnostics must not break the persistence queue or rendering.
    }
  }

  function publish() {
    const visible = visibleEntries(inbox);
    const persistence = !hydrated ? "loading" : errors.size > 0 ? "degraded" : "ok";
    const sameVisible =
      visible.length === snapshot.visible.length &&
      visible.every((entry, index) => {
        const previous = snapshot.visible[index];
        return (
          previous !== undefined &&
          entry.id === previous.id &&
          entry.title === previous.title &&
          entry.threadGone === previous.threadGone &&
          entry.detectedAtMs === previous.detectedAtMs &&
          JSON.stringify(entry.projectRemaining) === JSON.stringify(previous.projectRemaining)
        );
      });
    if (sameVisible && persistence === snapshot.persistence) return;
    snapshot = { visible: sameVisible ? snapshot.visible : visible, persistence };
    for (const listener of listeners) {
      try {
        listener();
      } catch {
        log("settlement-notifications.listener-failed");
      }
    }
  }

  function fail(key: string, event: string) {
    errors.add(key);
    log(event);
    publish();
  }

  function updateInbox(next: SettlementInboxState) {
    if (next === inbox || encodeSettlementInbox(next) === encodeSettlementInbox(inbox))
      return false;
    inbox = next;
    inboxRevision++;
    changeRevision++;
    publish();
    return true;
  }

  function refreshFromState(
    value: SettlementInboxState,
    environmentId: EnvironmentId,
    state: EnvironmentShellState,
  ) {
    if (Option.isNone(state.snapshot)) return value;
    const titles = new Map<ThreadId, string>();
    const gone = new Set<ThreadId>();
    for (const thread of [
      ...state.snapshot.value.threads,
      ...state.snapshot.value.archivedThreads,
    ]) {
      if (thread.deletedAt !== null) gone.add(thread.id);
      else titles.set(thread.id, thread.title);
    }
    return refreshThreads(value, environmentId, titles, gone);
  }

  function refreshConfirmed(value: SettlementInboxState) {
    for (const [environmentId, state] of confirmed)
      value = refreshFromState(value, environmentId, state);
    return value;
  }

  async function loadInbox(): Promise<boolean> {
    let json: string | null;
    try {
      json = await store.loadInbox();
    } catch {
      fail("inbox", "settlement-notifications.inbox-load-failed");
      return false;
    }
    inboxLoaded = true;
    try {
      if (json !== null)
        updateInbox(refreshConfirmed(mergeInbox(inbox, decodeSettlementInbox(json))));
    } catch {
      fail("inbox", "settlement-notifications.inbox-load-failed");
      inboxRevision++;
    }
    return true;
  }

  async function persistBatch() {
    // A failed read leaves durable entries and acknowledgements unknown.
    // Recover and merge them before either the inbox or its baselines can advance.
    if (!inboxLoaded) {
      if (!(await loadInbox())) return;
      inboxRevision++;
    }
    const pendingBaselines = [...dirtyBaselines].filter(
      ([environmentId]) => !pendingClassifications.has(environmentId),
    );
    if (inboxRevision === persistedInboxRevision && pendingBaselines.length === 0) return;
    // Capture one coherent pair before saving. A later observation may
    // change memory while the adapter is saving this inbox.
    const revision = inboxRevision;
    const inboxJson = encodeSettlementInbox(inbox);
    try {
      await store.saveInbox(inboxJson);
      persistedInboxRevision = revision;
      errors.delete("inbox");
    } catch {
      fail("inbox", "settlement-notifications.inbox-save-failed");
      return; // Never advance a baseline beyond an unsaved pending event.
    }
    for (const [environmentId, baseline] of pendingBaselines) {
      try {
        await store.saveBaseline(environmentId, encodeSettlementBaseline(baseline));
        if (dirtyBaselines.get(environmentId) === baseline) dirtyBaselines.delete(environmentId);
        errors.delete(`baseline:${environmentId}`);
      } catch {
        fail(`baseline:${environmentId}`, "settlement-notifications.baseline-save-failed");
      }
    }
    publish();
  }

  function requestWrite(): Promise<void> {
    writeTail = writeTail.then(persistBatch).catch(() => {
      fail("inbox", "settlement-notifications.persistence-failed");
    });
    return writeTail;
  }

  function scheduleWrite(immediate: boolean) {
    if (!hydrated) return;
    if (timer !== null) clearTimeout(timer);
    timer = null;
    if (immediate) void requestWrite();
    else {
      // @effect-diagnostics-next-line globalTimers:off - This React/external-store controller has no Effect runtime; flush owns cancellation of its persistence debounce.
      timer = setTimeout(() => {
        timer = null;
        void requestWrite();
      }, BASELINE_DEBOUNCE_MS);
    }
  }

  function classifyArrivals(
    environmentId: EnvironmentId,
    arrivals: readonly SettlementArrival[],
    detectedAtMs: number,
    resolveOrigins: SettlementOriginResolver,
  ) {
    pendingClassifications.set(environmentId, (pendingClassifications.get(environmentId) ?? 0) + 1);
    let finished = false;
    let retryTimer: ReturnType<typeof setTimeout> | undefined;
    const finish = (origins: unknown) => {
      if (finished) return;
      finished = true;
      clearTimeout(retryTimer);
      // Reject the entire positional result if any row is malformed. Only
      // positive evidence bound to every requested identity may silence a row.
      const valid =
        Array.isArray(origins) &&
        origins.length === arrivals.length &&
        arrivals.every((_, index) => {
          const origin = origins[index];
          return (
            origin === "client" ||
            origin === "other" ||
            origin === "unknown" ||
            isOriginDetails(origin)
          );
        });
      const retained = valid
        ? arrivals.flatMap((arrival, index) => {
            const result = origins[index];
            const details = typeof result === "string" ? { origin: result } : result;
            return details.origin === "client"
              ? []
              : [
                  {
                    ...arrival,
                    ...(details.projectRemaining === undefined
                      ? {}
                      : { projectRemaining: details.projectRemaining }),
                  },
                ];
          })
        : arrivals;
      updateInbox(refreshConfirmed(addArrivals(inbox, retained, detectedAtMs)));
      const remaining = (pendingClassifications.get(environmentId) ?? 1) - 1;
      if (remaining === 0) pendingClassifications.delete(environmentId);
      else pendingClassifications.set(environmentId, remaining);
      changeRevision++;
      scheduleWrite(true);
    };
    const exhaust = () => {
      if (finished) return;
      log("settlement-notifications.origin-unresolved");
      finish(null);
    };
    // These are classification attempts, not transport reconnects. A stalled
    // resolver cannot pin a baseline forever: four attempts and one final
    // response deadline are the complete lifetime of this classification.
    const offsets = [0, 2_000, 5_000, 15_000, 25_000];
    const attempt = (index: number) => {
      if (finished) return;
      if (index === offsets.length - 1) {
        exhaust();
        return;
      }
      // @effect-diagnostics-next-line globalTimers:off - Promise-based external-store boundary; finish cancels this bounded classification timer.
      retryTimer = setTimeout(() => attempt(index + 1), offsets[index + 1]! - offsets[index]!);
      void Promise.resolve()
        .then(() => resolveOrigins(arrivals))
        .then(finish, () => {
          if (index === offsets.length - 2) exhaust();
        });
    };
    attempt(0);
  }

  function processObservation(environmentId: EnvironmentId) {
    const observation = observations.get(environmentId);
    if (!hydrated || !baselines.has(environmentId) || observation === undefined) return;
    const { state, resolveOrigins } = observation;
    observations.delete(environmentId);
    try {
      const previous = baselines.get(environmentId) ?? null;
      const result = observeEnvironment(previous, {
        environmentId,
        state,
        nowMs: now(),
        staleSince: staleSince.get(environmentId) ?? null,
      });
      if (result.kind === "skip") {
        if (result.staleSince === undefined) staleSince.delete(environmentId);
        else staleSince.set(environmentId, result.staleSince);
        return;
      }
      if (previous !== null && result.kind === "baseline")
        log("settlement-notifications.sequence-reset", { environmentId });
      staleSince.delete(environmentId);
      confirmed.set(environmentId, state);
      const detectedAtMs = now();
      const arrivals = result.kind === "compare" ? result.arrivals : [];
      const needsClassification = resolveOrigins !== undefined && arrivals.length > 0;
      if (needsClassification)
        classifyArrivals(environmentId, arrivals, detectedAtMs, resolveOrigins);
      const next = needsClassification ? inbox : addArrivals(inbox, arrivals, detectedAtMs);
      const inboxChanged = updateInbox(refreshFromState(next, environmentId, state));
      baselines.set(environmentId, result.baseline);
      dirtyBaselines.set(environmentId, result.baseline);
      changeRevision++;
      scheduleWrite(inboxChanged);
    } catch {
      log("settlement-notifications.unsupported-shell", { environmentId });
    }
  }

  function loadBaseline(environmentId: EnvironmentId): Promise<void> {
    const existing = baselineLoads.get(environmentId);
    if (existing !== undefined) return existing;
    if (baselines.has(environmentId)) return Promise.resolve();
    const promise = Promise.resolve().then(async () => {
      let baseline: EnvironmentSettlementBaseline | null = null;
      try {
        const json = await store.loadBaseline(environmentId);
        if (json !== null) {
          baseline = decodeSettlementBaseline(json);
          if (baseline.environmentId !== environmentId)
            throw new Error("Baseline environment mismatch.");
        }
      } catch {
        baseline = null;
        fail(`baseline:${environmentId}`, "settlement-notifications.baseline-load-failed");
      }
      baselines.set(environmentId, baseline);
      baselineLoads.delete(environmentId);
      if (hydrated) processObservation(environmentId);
    });
    baselineLoads.set(environmentId, promise);
    return promise;
  }

  function receiveExternalInbox(json: string | null) {
    // Removing a key is not an explicit dismissal and cannot erase local state.
    if (json === null) return;
    try {
      const incoming = decodeSettlementInbox(json);
      const merged = refreshConfirmed(mergeInbox(inbox, incoming));
      const changed = updateInbox(merged);
      const incomingIds = new Set(incoming.entries.map((entry) => entry.id));
      const incomingAcks = new Set(incoming.acknowledged);
      const needsWriteBack =
        merged.entries.some((entry) => !incomingIds.has(entry.id)) ||
        merged.acknowledged.some((id) => !incomingAcks.has(id));
      if (needsWriteBack && !changed) {
        inboxRevision++;
        changeRevision++;
      }
      // External records are already durable. Repair missing identities/acks,
      // not metadata-only differences that could echo between stale shell views.
      if (needsWriteBack) scheduleWrite(true);
    } catch {
      fail("inbox", "settlement-notifications.external-inbox-invalid");
    }
  }

  function hydrate(): Promise<void> {
    if (hydration !== null) return hydration;
    hydration = Promise.resolve()
      .then(async () => {
        try {
          store.subscribeExternalInbox?.(receiveExternalInbox);
        } catch {
          fail("subscription", "settlement-notifications.external-subscribe-failed");
        }
        await loadInbox();
        try {
          const ids = await store.listBaselineEnvironmentIds();
          for (const id of ids) catalog.add(EnvironmentId.make(id));
        } catch {
          fail("catalog", "settlement-notifications.baseline-list-failed");
        }
        do {
          for (const environmentId of [...catalog, ...observations.keys()])
            void loadBaseline(environmentId);
          await Promise.all([...baselineLoads.values()]);
        } while ([...observations.keys()].some((id) => !baselines.has(id)));
        hydrated = true;
        for (const environmentId of observations.keys()) processObservation(environmentId);
        publish();
        if (inboxRevision !== persistedInboxRevision) scheduleWrite(true);
      })
      .catch(() => {
        hydrated = true;
        fail("hydration", "settlement-notifications.hydration-failed");
      });
    return hydration;
  }

  return {
    hydrate,
    observe(
      environmentId: EnvironmentId,
      state: EnvironmentShellState,
      options?: { resolveOrigins?: SettlementOriginResolver },
    ): void {
      observations.set(environmentId, { state, ...options });
      if (!hydrated) {
        void hydrate();
        return;
      }
      if (baselines.has(environmentId)) processObservation(environmentId);
      else void loadBaseline(environmentId);
    },
    noteCatalog(environmentIds: readonly EnvironmentId[]): void {
      // Catalog absence is not permission to discard pending entries or history.
      for (const id of environmentIds) catalog.add(id);
    },
    dismiss(shownIds: readonly string[]): void {
      if (updateInbox(dismiss(inbox, shownIds))) scheduleWrite(true);
      if (!hydrated) void hydrate();
    },
    subscribe(listener: () => void): () => void {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    getSnapshot(): SettlementNotificationSnapshot {
      return snapshot;
    },
    async flush(): Promise<void> {
      await hydrate();
      while (baselineLoads.size > 0) await Promise.all([...baselineLoads.values()]);
      if (timer !== null) clearTimeout(timer);
      timer = null;
      let observedRevision = changeRevision;
      let pending = requestWrite();
      for (;;) {
        await pending;
        while (baselineLoads.size > 0) await Promise.all([...baselineLoads.values()]);
        if (pending !== writeTail) {
          pending = writeTail;
        } else if (observedRevision !== changeRevision) {
          observedRevision = changeRevision;
          if (timer !== null) clearTimeout(timer);
          timer = null;
          pending = requestWrite();
        } else {
          return;
        }
      }
    },
  };
}
