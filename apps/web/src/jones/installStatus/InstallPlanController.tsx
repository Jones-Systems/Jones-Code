import { useEffect, useRef } from "react";
import { advanceInstallPlan, type InstallPlanDriver } from "@t3tools/client-runtime/jones/install-plan";
import { useEnvironments } from "../../state/environments";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { environmentSession } from "../../state/session";
import { jonesUpdates } from "../updates/jonesUpdates";
import { installStatusStore, type InstallStatusStore, type InstallStatusDevice } from "./panelUi";
import { bindInstallPlanServices, requestInstallPlan, type InstallPlanServices } from "./services";
import { claimantId, openInstallStatus } from "./entrypoints";
import { productionInstallPlanServices } from "./production";
import { RecoveryReview } from "./recoveryReview";

/** One pass per wakeup. Native leases arbitrate windows; this loop never retries a mutation. */
export function startInstallPlanController(services: InstallPlanServices, store: InstallStatusStore = installStatusStore) {
  if (services.bridge.installPlan === undefined) return { wake: () => {}, close: () => {} };
  const binding = { services, store };
  const unbind = bindInstallPlanServices(services, store);
  let cancelled = false;
  let running = false;
  let dirty = false;
  let eventRevision = 0;
  let observedThisPass = new Map<InstallStatusDevice["environmentId"], InstallStatusDevice>();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const driver: InstallPlanDriver = {
    claimant: `window:${claimantId()}`, now: services.now, cancelled: () => cancelled,
    desktop: async (request) => {
      const firstRead = store.get().native === null;
      const result = await requestInstallPlan(request, binding);
      if (!cancelled && firstRead && result.state.current !== null) openInstallStatus();
      return result;
    },
    observe: async (id) => {
      const observed = observedThisPass.get(id) ?? await services.observe(id);
      if (!cancelled) store.update({ devices: store.get().devices.map((d) => d.environmentId === id ? observed : d) });
      return observed;
    },
    operation: async (id, request) => {
      const result = await services.operation(id, request);
      if ("binding" in result) throw new Error("Unexpected recovery result for an operation.");
      return result;
    },
    host: services.host,
  };
  const wake = () => {
    if (cancelled) return;
    if (running) { dirty = true; return; }
    running = true;
    dirty = false;
    void (async () => {
      const revision = eventRevision;
      const desktop = await services.bridge.getUpdateState();
      if (!cancelled && revision === eventRevision) store.update({ desktop });
      const devices = services.devices();
      const observations = await Promise.allSettled(devices.slice(0, 16).map((d) => services.observe(d.environmentId)));
      observedThisPass = new Map(observations.flatMap((result) => result.status === "fulfilled" ? [[result.value.environmentId, result.value] as const] : []));
      if (!cancelled) store.update({ devices: devices.map((device, index) => {
        const result = observations[index];
        return result?.status === "fulfilled" ? result.value : device;
      }) });
      if (!cancelled) await advanceInstallPlan(driver);
    })().catch((error: unknown) => {
      if (!cancelled) store.update({ error: error instanceof Error ? error.message : "Install status could not be read." });
    }).finally(() => {
      running = false;
      if (!cancelled) {
        if (timer !== undefined) clearTimeout(timer);
        timer = setTimeout(wake, dirty ? 0 : 15_000);
      }
    });
  };
  const stopPlan = services.bridge.onInstallPlan?.((native) => {
    if (!cancelled) {
      const previous = store.get().native;
      const changed = previous?.current?.planId !== native.current?.planId || previous?.current?.revision !== native.current?.revision;
      store.update({ native });
      if (running && changed) dirty = true;
      // A repeated read broadcast must not turn the bounded runner into a spin loop.
      if (!running && changed) wake();
    }
  });
  const stopUpdate = services.bridge.onUpdateState((desktop) => {
    eventRevision++;
    if (!cancelled) { store.update({ desktop }); wake(); }
  });
  wake();
  return { wake, close: () => { cancelled = true; if (timer !== undefined) clearTimeout(timer); stopPlan?.(); stopUpdate(); unbind(); } };
}

export function InstallPlanController() {
  const { environments } = useEnvironments();
  const current = useRef(environments);
  // oxlint-disable-next-line t3code/react-effect-budget -- Event callbacks need the latest connection set without restarting the runner on every status push.
  useEffect(() => { current.current = environments; }, [environments]);
  const ids = JSON.stringify(environments.map((e) => e.environmentId).sort());
  // oxlint-disable-next-line t3code/react-effect-budget -- Own the plan runner, observation subscriptions and their exact lifetimes.
  useEffect(() => {
    const bridge = window.desktopBridge;
    if (bridge?.installPlan === undefined) return;
    const controller = startInstallPlanController(productionInstallPlanServices(bridge, () => current.current));
    const stops = current.current.flatMap(({ environmentId }) => [
      appAtomRegistry.subscribe(jonesUpdates.observation(environmentId), controller.wake),
      appAtomRegistry.subscribe(environmentSession.sessionStateAtom(environmentId), controller.wake),
    ]);
    return () => { for (const stop of stops) stop(); controller.close(); };
  }, [ids]);
  return <RecoveryReview />;
}
