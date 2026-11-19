import type { EnvironmentId } from "@t3tools/contracts";
import {
  createStatusPoller,
  deviceHasRecoveryDuty,
  deviceStatusActive,
  type StatusPollerOptions,
} from "@t3tools/client-runtime/jones/install-plan";
import { getDocumentActivity, subscribeDocumentActivity } from "../../lib/documentActivity";
import { refreshDeviceEvidence } from "./actions";
import { installStatusStore, selectedHistoryPlan, type InstallStatusStore } from "./panelUi";
import {
  installPlanServices,
  nativePlanIssue,
  readTargetPlan,
  requestInstallPlan,
  type InstallPlanServices,
} from "./services";

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
  const active = installPlanServices();
  const bound = active?.services === services && active.store === store ? active : undefined;
  const binding = bound ?? { services, store };
  let closed = false;
  const current = () => !closed && (bound === undefined || installPlanServices() === bound);
  const attention =
    options.attention ??
    (() => {
      const activity = getDocumentActivity();
      return activity.visible && activity.focused;
    });
  const archiveDevice = (id: string) => {
    const snapshot = store.get();
    if (!snapshot.open || !snapshot.showLast || snapshot.native?.view.recoveryHistory !== 1)
      return undefined;
    return selectedHistoryPlan(snapshot)?.devices.find((device) => device.environmentId === id);
  };
  const planDevice = (id: string) =>
    store.get().native?.current?.devices.find((d) => d.environmentId === id) ?? archiveDevice(id);
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
    if (!current()) return;
    store.update({
      devices: store.get().devices.map((d) => (d.environmentId === environmentId ? observed : d)),
    });
    if (observed.observation.freshness === "unsupported") return;
    if (observed.observation.freshness !== "fresh")
      throw new Error("Status for this device is not fresh.");
    const read = await requestInstallPlan({ type: "read" }, binding);
    if (!current() || nativePlanIssue(read) !== undefined) return;
    const { state } = read;
    if (state.current !== null)
      await refreshDeviceEvidence(binding, environmentId, state.current, observed, {
        refreshInstallOutcome: false,
      });
    const device = archiveDevice(id);
    const planId = store.get().historyPlanId;
    if (device === undefined || !deviceHasRecoveryDuty(device) || planId === null) return;
    const history = await readTargetPlan(binding, { planId, archived: true });
    if (current() && history.plan !== null && nativePlanIssue(history.result) === undefined)
      await refreshDeviceEvidence(binding, environmentId, history.plan, observed, {
        refreshInstallOutcome: false,
        archived: true,
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
    const next = `${ids.join(",")}|${ids.filter(isActive).join(",")}|${store.get().historyPlanId ?? ""}`;
    if (next === tiers) return;
    tiers = next;
    poller.reschedule();
  });
  return {
    poller,
    close() {
      closed = true;
      stopStore();
      stopOnline();
      stopAttention();
      poller.close();
    },
  };
}
