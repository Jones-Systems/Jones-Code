import { useEffect } from "react";
import { advanceFleetCampaigns } from "@t3tools/client-runtime/jones/fleet-updates";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import {
  fleetDesktopState,
  fleetHost,
  fleetStatusError,
  refreshFleetDesktopState,
} from "./runtime";

/** Native proof gates activation; ordinary app startup and source equality do not. */
export function FleetController() {
  // oxlint-disable-next-line t3code/react-effect-budget -- Own the desktop fleet campaign advancement loop and cancel its timer when the controller unmounts.
  useEffect(() => {
    const bridge = window.desktopBridge?.fleetUpdates;
    if (bridge === undefined) return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const tick = async () => {
      try {
        await advanceFleetCampaigns({
          desktop: async (request) => {
            const state = await bridge(request);
            if (!cancelled) appAtomRegistry.set(fleetDesktopState, state);
            return state;
          },
          host: fleetHost,
          cancelled: () => cancelled,
          // The native active install's provenance; the renderer only relays it.
          committedSource: readFleetPublicationSource,
        });
        if (!cancelled) await refreshFleetDesktopState();
      } catch {
        if (!cancelled)
          appAtomRegistry.set(
            fleetStatusError,
            "Fleet campaign storage could not be read. Automatic updates are waiting for reconciliation.",
          );
      }
      if (!cancelled) timer = setTimeout(() => void tick(), 60_000);
    };
    void tick();
    return () => {
      cancelled = true;
      if (timer !== undefined) clearTimeout(timer);
    };
  }, []);
  return null;
}

export async function readFleetPublicationSource() {
  const bridge = window.desktopBridge;
  if (bridge?.installPlan === undefined)
    return (await bridge?.getUpdateState())?.jones?.installedSource;
  const result = await bridge.installPlan({ type: "publication" });
  return result.publication?.kind === "committed" ? result.publication.source : undefined;
}
