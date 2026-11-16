import type { EnvironmentId } from "@t3tools/contracts";
import {
  createStatusPoller,
  deviceStatusActive,
  type StatusPollerOptions,
} from "@t3tools/client-runtime/jones/install-plan";
import { getDocumentActivity, subscribeDocumentActivity } from "../../lib/documentActivity";
import { refreshDeviceEvidence } from "./actions";
import { installStatusStore, type InstallStatusStore } from "./panelUi";
import { requestInstallPlan, type InstallPlanServices } from "./services";

export interface InstallStatusPollerOptions {
  readonly store?: InstallStatusStore;
  /** Test seams; production uses the shared document activity listeners and window events. */
  readonly attention?: () => boolean;
  readonly subscribeAttention?: (listener: () => void) => () => void;
  readonly subscribeOnline?: (listener: () => void) => () => void;
  readonly poller?: Pick<StatusPollerOptions, "random" | "setTimer" | "clearTimer" | "cadence">;
}

function subscribeWindowOnline(listener: () => void) {
  if (typeof window === "undefined") return () => {};
  window.addEventListener("online", listener);
  return () => window.removeEventListener("online", listener);
}

/**
 * Keeps install status current without a click. A tick only reads: it restarts a waiting status
 * stream, reads fleet status and the latest plan operation, and reports validated evidence. It
 * never checks packages, claims, starts, stops, fences, directs, refreshes outcomes or makes threads.
 */
export function startInstallStatusPoller(
  services: InstallPlanServices,
  options: InstallStatusPollerOptions = {},
) {
  const store = options.store ?? installStatusStore;
  const binding = { services, store };
  const attention =
    options.attention ??
    (() => {
      const activity = getDocumentActivity();
      return activity.visible && activity.focused;
    });
  const planDevice = (id: string) =>
    store.get().native?.current?.devices.find((d) => d.environmentId === id);
  const isActive = (id: string) =>
    deviceStatusActive(
      store.get().devices.find((d) => d.environmentId === id)?.observation,
      planDevice(id),
    );
  const tick = async (id: string) => {
    const environmentId = id as EnvironmentId;
    const before = services.devices().find((d) => d.environmentId === environmentId);
    if (before !== undefined && before.observation.freshness !== "fresh")
      services.kick?.(environmentId);
    const observed = await services.observe(environmentId);
    store.update({
      devices: store.get().devices.map((d) => (d.environmentId === environmentId ? observed : d)),
    });
    if (observed.observation.freshness === "unsupported") return;
    if (observed.observation.freshness !== "fresh")
      throw new Error("Status for this device is not fresh.");
    const { state } = await requestInstallPlan({ type: "read" }, binding);
    if (state.current !== null)
      await refreshDeviceEvidence(binding, environmentId, state.current, observed, {
        refreshInstallOutcome: false,
      });
  };
  const poller = createStatusPoller({
    ...options.poller,
    devices: () => services.devices().map((d) => d.environmentId),
    isActive,
    attention,
    tick,
    onHealth: (id, health) =>
      store.update({ pollHealth: { ...store.get().pollHealth, [id]: health } }),
    now: services.now,
  });
  poller.sync();
  let hadAttention = attention();
  const stopAttention = (options.subscribeAttention ?? subscribeDocumentActivity)(() => {
    const next = attention();
    if (next && !hadAttention) poller.regained();
    else poller.reschedule();
    hadAttention = next;
  });
  const stopOnline = (options.subscribeOnline ?? subscribeWindowOnline)(() => poller.regained());
  // Device work or the device list changing moves a device between cadence tiers.
  let tiers = "";
  const stopStore = store.subscribe(() => {
    const ids = services.devices().map((d) => d.environmentId);
    const next = `${ids.join(",")}|${ids.filter(isActive).join(",")}`;
    if (next === tiers) return;
    tiers = next;
    poller.reschedule();
  });
  return {
    poller,
    close() {
      stopStore();
      stopOnline();
      stopAttention();
      poller.close();
    },
  };
}
