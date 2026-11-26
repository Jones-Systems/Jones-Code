import { useEffect } from "react";
import {
  advanceFleetCampaigns,
  refreshFleetFollowerReport,
  type FleetCampaignDriver,
  type FleetPublication,
} from "@t3tools/client-runtime/jones/fleet-updates";
import type { FleetEnrollment } from "@t3tools/contracts/jones/fleet-updates";
import type { JonesInstallPlanLaptopEvidence } from "@t3tools/contracts/jones/installPlan";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { fleetPublication } from "./publicationState";
import { fleetDesktop, fleetHost, fleetStatusError, refreshFleetDesktopState } from "./runtime";

let refreshMountedFollower: ((enrollment: FleetEnrollment) => Promise<void>) | undefined;

export function refreshFleetFollower(enrollment: FleetEnrollment) {
  return refreshMountedFollower?.(enrollment) ?? Promise.resolve();
}

/** Native proof gates activation; ordinary app startup and source equality do not. */
export function FleetController() {
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Own the desktop fleet campaign advancement loop and cancel its timer when the controller unmounts.
  useEffect(() => {
    const bridge = window.desktopBridge?.fleetUpdates;
    if (bridge === undefined) return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let latestLaptop: JonesInstallPlanLaptopEvidence | undefined;
    let tail = Promise.resolve();
    const refreshes = new Map<string, Promise<void>>();
    const enqueue = (work: () => Promise<void>) => {
      const next = tail.then(async () => {
        if (!cancelled) await work();
      });
      tail = next.catch(() => undefined);
      return next;
    };
    const driver: FleetCampaignDriver = {
      desktop: (request) => fleetDesktop(request, () => cancelled),
      host: fleetHost,
      cancelled: () => cancelled,
      publication: () =>
        readFleetPublication((laptop) => {
          latestLaptop = laptop;
        }),
      onPublication: (report) => {
        if (!cancelled) appAtomRegistry.set(fleetPublication, { ...report, laptop: latestLaptop });
      },
    };
    const refresh = (enrollment: FleetEnrollment) => {
      const { environmentId } = enrollment;
      const key = JSON.stringify([
        environmentId,
        enrollment.enrollmentId,
        enrollment.continueRunningThreads,
      ]);
      const existing = refreshes.get(key);
      if (existing !== undefined) return existing;
      const next = enqueue(() =>
        refreshFleetFollowerReport(
          {
            ...driver,
            onPublication: (report) => {
              if (cancelled) return;
              const others = (appAtomRegistry.get(fleetPublication)?.hosts ?? []).filter(
                (host) => host.enrollment.environmentId !== environmentId,
              );
              return driver.onPublication?.({ ...report, hosts: [...others, ...report.hosts] });
            },
          },
          enrollment,
        ),
      ).finally(() => {
        refreshes.delete(key);
      });
      refreshes.set(key, next);
      return next;
    };
    refreshMountedFollower = refresh;
    const tick = async () => {
      try {
        await enqueue(async () => {
          await advanceFleetCampaigns(driver);
          if (!cancelled) await refreshFleetDesktopState();
        });
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
      if (refreshMountedFollower === refresh) {
        refreshMountedFollower = undefined;
        appAtomRegistry.set(fleetPublication, null);
      }
    };
  }, []);
  return null;
}

export async function readFleetPublicationSource() {
  const publication = await readFleetPublication();
  return publication.kind === "withheld" ? undefined : publication.source;
}

export async function readFleetPublication(
  onLaptop?: (laptop: JonesInstallPlanLaptopEvidence | undefined) => void,
): Promise<FleetPublication> {
  const bridge = window.desktopBridge;
  onLaptop?.(undefined);
  if (bridge?.installPlan === undefined)
    return {
      kind: "withheld",
      code: "unavailable",
      message: "Native publication proof is unavailable.",
    };
  try {
    const result = await bridge.installPlan({ type: "publication", acceptOrdinaryRecovery: 1 });
    if (result.refused !== undefined || result.state?.readOnly !== undefined)
      return {
        kind: "withheld",
        code: "unavailable",
        message: "Native publication proof is unavailable.",
      };
    onLaptop?.(result.state?.laptop);
    return (
      result.publication ?? {
        kind: "withheld",
        code: "unavailable",
        message: "Native publication proof is unavailable.",
      }
    );
  } catch {
    return {
      kind: "withheld",
      code: "unavailable",
      message: "Native publication proof could not be read.",
    };
  }
}
