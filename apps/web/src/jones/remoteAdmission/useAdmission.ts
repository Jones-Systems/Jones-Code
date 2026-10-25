import { useEffect, useSyncExternalStore } from "react";
import type { EnvironmentId } from "@t3tools/contracts";
import { actionableAdmissions, createAdmissionStore } from "./store";
import { useEnvironmentIds } from "../../state/environments";
import { remoteAdmissionRequest } from "./runtime";

const stores = new Map<EnvironmentId, ReturnType<typeof createAdmissionStore>>();
const watchers = new Map<
  ReturnType<typeof createAdmissionStore>,
  { count: number; stop: () => void }
>();
function storeFor(environmentId: EnvironmentId) {
  let store = stores.get(environmentId);
  if (!store) {
    store = createAdmissionStore((request) => remoteAdmissionRequest(environmentId, request));
    stores.set(environmentId, store);
  }
  return store;
}

function watch(store: ReturnType<typeof createAdmissionStore>) {
  let watcher = watchers.get(store);
  if (!watcher) {
    const refresh = () => {
      if (document.visibilityState !== "hidden") void store.refresh();
    };
    const interval = window.setInterval(refresh, 10_000);
    document.addEventListener("visibilitychange", refresh);
    watcher = {
      count: 0,
      stop: () => {
        window.clearInterval(interval);
        document.removeEventListener("visibilitychange", refresh);
      },
    };
    watchers.set(store, watcher);
    refresh();
  }
  watcher.count++;
  return () => {
    if (--watcher.count === 0) {
      watcher.stop();
      watchers.delete(store);
    }
  };
}

export function useAdmission(environmentId: EnvironmentId) {
  const store = storeFor(environmentId);
  const state = useSyncExternalStore(store.subscribe, store.getSnapshot);
  useEffect(() => watch(store), [store]);
  return { ...state, store };
}

export function useAdmissionOverview() {
  const environmentIds = useEnvironmentIds();
  const entries = environmentIds.map(storeFor);
  const overview = {
    subscribe: (listener: () => void) => {
      const unsubscribes = entries.map((store) => store.subscribe(listener));
      return () => {
        for (const unsubscribe of unsubscribes) unsubscribe();
      };
    },
    getSnapshot: () => {
      let held = 0;
      let unknown = 0;
      let capped = false;
      for (const store of entries) {
        const state = store.getSnapshot();
        held += actionableAdmissions(state).length;
        if (state.loadFailed || !state.snapshot) unknown++;
        capped = capped || state.atLimit;
      }
      return `${held}:${unknown}:${capped ? 1 : 0}`;
    },
    watch: () => {
      const stops = entries.map(watch);
      return () => {
        for (const stop of stops) stop();
      };
    },
  };
  const value = useSyncExternalStore(overview.subscribe, overview.getSnapshot);
  useEffect(() => overview.watch(), [overview]);
  const [held, unknown, capped] = value.split(":").map(Number);
  return { held: held ?? 0, unknown: unknown ?? 0, capped: capped === 1 };
}
