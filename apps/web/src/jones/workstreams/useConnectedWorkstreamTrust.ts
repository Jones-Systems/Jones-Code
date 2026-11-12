import { useAtomValue } from "@effect/atom-react";
import {
  buildConnectedEnvironmentTrust,
  type ConnectedEnvironmentTrust,
} from "@t3tools/client-runtime/state/workstreams";
import type { TrustedT3PlacementEnvironment } from "@t3tools/contracts";

import { environmentCatalog } from "../../connection/catalog";
import { environmentSession } from "../../state/session";
import { createConnectedEnvironmentConfigsAtom } from "./connectedWorkstreamTrust";

const connectedEnvironmentConfigsAtom = createConnectedEnvironmentConfigsAtom({
  catalogValueAtom: environmentCatalog.catalogValueAtom,
  initialConfigValueAtom: environmentSession.initialConfigValueAtom,
});

/** Workstream display trust from the primary's placements and each live environment's descriptor. */
export function useConnectedWorkstreamTrust(
  placementTrustedEnvironments: readonly TrustedT3PlacementEnvironment[] | undefined,
): ConnectedEnvironmentTrust {
  const environments = useAtomValue(connectedEnvironmentConfigsAtom);
  return buildConnectedEnvironmentTrust({ environments, placementTrustedEnvironments });
}
