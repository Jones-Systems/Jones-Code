import { useSyncExternalStore } from "react";
import { installStatusStore, type InstallStatusStore } from "./panelUi";
import { installPlanServices, subscribeInstallPlanServices } from "./services";

export function useInstallPlan(store: InstallStatusStore = installStatusStore) {
  return useSyncExternalStore(store.subscribe, store.get, store.get);
}

export function useInstallPlanBinding() {
  return useSyncExternalStore(subscribeInstallPlanServices, installPlanServices, () => undefined);
}
