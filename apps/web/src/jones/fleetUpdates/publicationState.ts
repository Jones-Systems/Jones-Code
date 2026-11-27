import type { FleetPublicationReport } from "@t3tools/client-runtime/jones/fleet-updates";
import { Atom } from "effect/reactivity";
import type { JonesInstallPlanLaptopEvidence } from "@t3tools/contracts/jones/installPlan";

interface FleetPublicationView extends FleetPublicationReport {
  readonly laptop: JonesInstallPlanLaptopEvidence | undefined;
}

export const fleetPublication = Atom.make<FleetPublicationView | null>(null);
