import { useSyncExternalStore } from "react";
import { installStatusStore, type InstallStatusStore } from "./panelUi";

export function useInstallPlan(store: InstallStatusStore = installStatusStore) {
  return useSyncExternalStore(store.subscribe, store.get, store.get);
}
