import type {
  FleetCommittedSource,
  FleetPublicationReport,
} from "@t3tools/client-runtime/jones/fleet-updates";
import { Atom } from "effect/reactivity";

export const fleetPublication = Atom.make<FleetPublicationReport | null>(null);
/** Label evidence only; this never supplies a publication grant. */
export const fleetInstalledSource = Atom.make<FleetCommittedSource | null>(null);
