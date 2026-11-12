import { useEffect } from "react";
import {
  advanceFleetCampaigns,
  type FleetPublication,
} from "@t3tools/client-runtime/jones/fleet-updates";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { fleetInstalledSource, fleetPublication } from "./publicationState";
import {
  fleetDesktop,
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
    let installedReadVersion = 0;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const tick = async () => {
      try {
        const readVersion = ++installedReadVersion;
        // Label lookup cannot hold fleet observation or authorize publication.
        void Promise.resolve()
          .then(() => window.desktopBridge?.getUpdateState())
          .then((installed) => {
            if (!cancelled && readVersion === installedReadVersion)
              appAtomRegistry.set(fleetInstalledSource, installed?.jones?.installedSource ?? null);
          })
          .catch(() => {
            if (!cancelled && readVersion === installedReadVersion)
              appAtomRegistry.set(fleetInstalledSource, null);
          });
        await advanceFleetCampaigns({
          desktop: (request) => fleetDesktop(request, () => cancelled),
          host: fleetHost,
          cancelled: () => cancelled,
          // The native active install's provenance; the renderer only relays it.
          publication: readFleetPublication,
          onPublication: (report) => {
            if (!cancelled) appAtomRegistry.set(fleetPublication, report);
          },
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
  const publication = await readFleetPublication();
  return publication.kind === "withheld" ? undefined : publication.source;
}

export async function readFleetPublication(): Promise<FleetPublication> {
  const bridge = window.desktopBridge;
  if (bridge?.installPlan === undefined)
    return {
      kind: "withheld",
      code: "unavailable",
      message: "Native publication proof is unavailable.",
    };
  try {
    const result = await bridge.installPlan({ type: "publication", acceptOrdinaryRecovery: 1 });
    return result.publication ?? {
      kind: "withheld",
      code: "unavailable",
      message: "Native publication proof is unavailable.",
    };
  } catch {
    return {
      kind: "withheld",
      code: "unavailable",
      message: "Native publication proof could not be read.",
    };
  }
}
