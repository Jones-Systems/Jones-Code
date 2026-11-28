import type { EnvironmentId } from "@t3tools/contracts";
import {
  createStatusPoller,
  deviceHasRecoveryDuty,
  deviceStatusActive,
  type StatusPollerOptions,
} from "@t3tools/client-runtime/jones/install-plan";
import { getDocumentActivity, subscribeDocumentActivity } from "../../lib/documentActivity";
import { refreshArchivedDeviceEvidence, refreshDeviceEvidence } from "./actions";
import { targetForDesktop } from "./entrypoints";
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
 * stream, reads fleet status and exact plan operations, and reports validated evidence. It
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
  const openArchiveId = () => {
    const snapshot = store.get();
    if (
      !snapshot.open ||
      snapshot.native?.view.recoveryHistory !== 1 ||
      (!snapshot.showLast &&
        (snapshot.native.current !== null || targetForDesktop(snapshot.desktop) !== undefined))
    )
      return undefined;
    return snapshot.historyPlanId ?? snapshot.native.last?.planId;
  };
  const pendingArchiveDevice = (id: string, planId: string) => {
    const snapshot = store.get();
    const history = selectedHistoryPlan(snapshot);
    const device =
      history?.planId === planId
        ? history.devices.find((item) => item.environmentId === id)
        : undefined;
    return (
      (device !== undefined && deviceHasRecoveryDuty(device)) ||
      snapshot.native?.recoveryHistory
        ?.find((entry) => entry.planId === planId)
        ?.pendingDevices.some((item) => item.environmentId === id) === true
    );
  };
  const isActive = (id: string) => {
    const snapshot = store.get();
    const planId = openArchiveId();
    return (
      deviceStatusActive(
        snapshot.devices.find((d) => d.environmentId === id)?.observation,
        snapshot.native?.current?.devices.find((d) => d.environmentId === id),
      ) ||
      (planId !== undefined && pendingArchiveDevice(id, planId))
    );
  };
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
    const planId = openArchiveId();
    if (planId !== undefined && pendingArchiveDevice(id, planId)) {
      const selection = store.historySelection();
      const stillShowing = () =>
        current() && store.historySelection() === selection && openArchiveId() === planId;
      const history = await readTargetPlan(binding, { planId, archived: true });
      if (history.plan !== null && nativePlanIssue(history.result) === undefined)
        await refreshArchivedDeviceEvidence(
          binding,
          environmentId,
          history.plan,
          observed,
          stillShowing,
        );
    }
    const plan = read.state.current;
    if (current() && plan !== null && store.get().native?.current?.planId === plan.planId)
      await refreshDeviceEvidence(binding, environmentId, plan, observed, {
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
